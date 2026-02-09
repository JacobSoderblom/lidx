//! Extracted handler functions for RPC methods.
//! Each function corresponds to a match arm in `handle_method`.

use super::*;

// ---------------------------------------------------------------------------
// GROUP 1 -- Symbol query handlers
// ---------------------------------------------------------------------------

pub(super) fn handle_find_symbol(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: FindSymbolParams = serde_json::from_value(params)?;
    if params.query.trim().is_empty() {
        return Err(anyhow::anyhow!("find_symbol requires a non-empty query"));
    }
    let limit = params.limit.unwrap_or(50).min(MAX_RESPONSE_LIMIT);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let symbols = indexer.db().find_symbols(
        &params.query,
        limit,
        languages.as_deref(),
        graph_version,
    )?;

    // Check format param
    let format = params.format.as_deref().unwrap_or("full");
    Ok(if format == "signatures" {
        // For signatures format, wrap in consistent shape
        let compact_symbols = apply_compact_format(json!(symbols));
        let next_hops: Vec<serde_json::Value> = if let Some(first_symbol) = symbols.first() {
            vec![
                json!({
                    "method": "open_symbol",
                    "params": {"id": first_symbol.id},
                    "label": format!("Open {}", first_symbol.name)
                })
            ]
        } else {
            vec![]
        };
        json!({
            "data": compact_symbols,
            "next_hops": next_hops
        })
    } else {
        // Full format returns bare array for backward compatibility
        json!(symbols)
    })
}

pub(super) fn handle_suggest_qualnames(indexer: &mut Indexer, params: Value) -> Result<Value> {
    // Helper: Extract 3-character trigrams from a string
    fn extract_trigrams(s: &str) -> Vec<String> {
        let chars: Vec<char> = s.chars().collect();
        chars
            .windows(3)
            .map(|w| w.iter().collect::<String>())
            .collect()
    }

    // Helper: Split camelCase/PascalCase into components
    fn split_camel_case(s: &str) -> Vec<String> {
        let mut components = Vec::new();
        let mut current = String::new();
        for c in s.chars() {
            if c.is_uppercase() && !current.is_empty() {
                components.push(current);
                current = String::new();
            }
            current.push(c);
        }
        if !current.is_empty() {
            components.push(current);
        }
        components
    }

    let params: SuggestQualNamesParams = serde_json::from_value(params)?;
    let limit = params.limit.unwrap_or(10).min(MAX_RESPONSE_LIMIT);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;

    // Fast path: try exact substring match first
    let symbols = indexer.db().find_symbols(
        &params.query,
        limit,
        languages.as_deref(),
        graph_version,
    )?;

    let symbols = if symbols.is_empty() && params.query.len() >= 3 {
        // Fuzzy path: search by name prefix (starts-with), then rank by
        // Levenshtein distance against the full query.
        // Use progressively shorter prefixes to find candidates.
        let mut candidates = Vec::new();
        for prefix_len in (3..=params.query.len()).rev() {
            let prefix = &params.query[..prefix_len];
            candidates = indexer.db().find_symbols_by_name_prefix(
                prefix,
                limit * 20,
                languages.as_deref(),
                graph_version,
            )?;
            if !candidates.is_empty() {
                break;
            }
        }

        // Strategy 2: Trigram search
        // Extract 3-character substrings and search for symbols matching each
        let trigrams = extract_trigrams(&params.query);
        for trigram in trigrams.iter().take(5) {
            let more = indexer.db().find_symbols_by_name_prefix(
                trigram,
                limit * 10,
                languages.as_deref(),
                graph_version,
            )?;
            candidates.extend(more);
        }

        // Strategy 3: CamelCase component search
        // Split query into camelCase components and search for each
        let components = split_camel_case(&params.query);
        if components.len() > 1 {
            for component in &components {
                if component.len() >= 3 {
                    let more = indexer.db().find_symbols_by_name_prefix(
                        component,
                        limit * 5,
                        languages.as_deref(),
                        graph_version,
                    )?;
                    candidates.extend(more);
                }
            }
        }

        // Deduplicate candidates by symbol ID
        candidates.sort_by_key(|s| s.id);
        candidates.dedup_by_key(|s| s.id);

        // Score candidates using case-insensitive Levenshtein distance
        let query_lower = params.query.to_lowercase();
        let max_dist = (params.query.len() / 4).max(2);
        let mut scored: Vec<(crate::model::Symbol, usize)> = candidates
            .into_iter()
            .filter_map(|s| {
                let name_lower = s.name.to_lowercase();
                let dist = search::levenshtein_distance(
                    query_lower.as_bytes(),
                    name_lower.as_bytes(),
                );
                if dist <= max_dist {
                    Some((s, dist))
                } else {
                    None
                }
            })
            .collect();
        scored.sort_by_key(|(_, dist)| *dist);
        scored.truncate(limit);
        scored.into_iter().map(|(s, _)| s).collect()
    } else {
        symbols
    };

    // Return just the qualnames with metadata
    let suggestions: Vec<serde_json::Value> = symbols
        .into_iter()
        .map(|s| {
            json!({
                "qualname": s.qualname,
                "kind": s.kind,
                "file_path": s.file_path,
            })
        })
        .collect();

    Ok(json!(suggestions))
}

pub(super) fn handle_open_symbol(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: OpenSymbolParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let symbol = if let Some(id) = params.id {
        indexer.db().get_symbol_by_id(id)?
    } else if let Some(qualname) = params.qualname {
        indexer
            .db()
            .get_symbol_by_qualname(&qualname, graph_version)?
    } else {
        return Err(anyhow::anyhow!("open_symbol requires id or qualname"));
    };
    let symbol = symbol.ok_or_else(|| anyhow::anyhow!("symbol not found"))?;
    let include_snippet = params.include_snippet.unwrap_or(true);
    let include_symbol = if params.snippet_only.unwrap_or(false) {
        false
    } else {
        params.include_symbol.unwrap_or(true)
    };
    let max_snippet_bytes = params.max_snippet_bytes;
    let snippet = if include_snippet {
        let path = indexer.repo_root().join(&symbol.file_path);
        let content = util::read_to_string(&path)
            .with_context(|| format!("read {}", symbol.file_path))?;
        let snippet = util::slice_bytes(&content, symbol.start_byte, symbol.end_byte)
            .unwrap_or_else(|| {
                util::slice_lines(&content, symbol.start_line, symbol.end_line)
            });
        match max_snippet_bytes {
            Some(max) => util::truncate_str_bytes(&snippet, max),
            None => snippet,
        }
    } else {
        String::new()
    };
    let mut payload = serde_json::Map::new();
    if include_symbol {
        payload.insert("symbol".to_string(), json!(symbol));
    }
    if include_snippet {
        payload.insert("snippet".to_string(), json!(snippet));
    }
    let next_hops = if params.snippet_only.unwrap_or(false) {
        Vec::new()
    } else {
        build_reference_hops(&symbol, graph_version)
    };
    if !next_hops.is_empty() {
        payload.insert("next_hops".to_string(), json!(next_hops));
    }
    Ok(Value::Object(payload))
}

pub(super) fn handle_explain_symbol(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: ExplainSymbolParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let languages = params.languages.clone();

    let max_bytes = params.max_bytes.unwrap_or(40_000).min(200_000);
    let max_refs = params.max_refs.unwrap_or(10);
    let sections = params.sections.clone().unwrap_or_else(||
        vec!["source".into(), "callers".into(), "callees".into(), "tests".into(), "implements".into()]
    );

    // 1. Resolve symbol
    let symbol = if let Some(id) = params.id {
        indexer.db().get_symbol_by_id(id)?
            .ok_or_else(|| anyhow::anyhow!("symbol not found: id={}", id))?
    } else if let Some(ref qn) = params.qualname {
        indexer.db().get_symbol_by_qualname(qn, graph_version)?
            .ok_or_else(|| anyhow::anyhow!("symbol not found: {}", qn))?
    } else if let Some(ref query) = params.query {
        let results = indexer.db().find_symbols(query, 1, languages.as_deref(), graph_version)?;
        results.into_iter().next()
            .ok_or_else(|| anyhow::anyhow!("no symbol found for query: {}", query))?
    } else {
        anyhow::bail!("explain_symbol requires id, qualname, or query");
    };

    // 2. Budget allocation (30% source, 20% callers, 20% callees, 10% tests, 20% expansion) - FIX #4
    let source_budget = max_bytes * 30 / 100;
    let callers_budget = max_bytes * 20 / 100;
    let callees_budget = max_bytes * 20 / 100;
    let tests_budget = max_bytes * 10 / 100;
    let expansion_budget = max_bytes * 20 / 100;
    let mut used_bytes = 0usize;
    let mut truncated = false;

    // 3. Read source (FIX #5: truncate at line boundaries)
    let source = if sections.contains(&"source".to_string()) {
        let repo_root = indexer.repo_root();
        let full_path = repo_root.join(&symbol.file_path);
        if full_path.exists() {
            let content = std::fs::read_to_string(&full_path).unwrap_or_default();
            let lines: Vec<&str> = content.lines().collect();
            let start = (symbol.start_line as usize).saturating_sub(1);
            let end = (symbol.end_line as usize).min(lines.len());
            let snippet = lines[start..end].join("\n");
            let snippet = if snippet.len() > source_budget {
                truncated = true;
                // Find last newline before budget limit to avoid mid-line truncation
                let truncate_pos = snippet[..source_budget].rfind('\n').unwrap_or(source_budget);
                snippet[..truncate_pos].to_string()
            } else {
                snippet
            };
            used_bytes += snippet.len();
            Some(snippet)
        } else {
            None
        }
    } else { None };

    // 4. Get edges for callers/callees
    let edges = indexer.db().edges_for_symbol(
        symbol.id, languages.as_deref(), graph_version
    )?;

    // 5. Build callers (incoming CALLS)
    let mut callers = if sections.contains(&"callers".to_string()) {
        let mut caller_refs = Vec::new();
        let mut caller_bytes = 0usize;
        let mut seen_caller_ids = std::collections::HashSet::new();

        // Determine which symbol IDs to collect callers for
        let is_class_symbol = symbol.kind == "class";
        let target_ids: Vec<(i64, String)> = if is_class_symbol {
            // For class symbols, find all methods and collect callers for each
            let all_symbols = indexer.db().get_symbols_for_file(&symbol.file_path, graph_version)?;
            let mut ids: Vec<(i64, String)> = all_symbols.into_iter()
                .filter(|s| {
                    (s.kind == "method" || s.kind == "function") &&
                    s.start_line >= symbol.start_line &&
                    s.end_line <= symbol.end_line
                })
                .map(|s| { let name = s.name.clone(); (s.id, name) })
                .collect();
            // Also include the class itself
            ids.push((symbol.id, symbol.name.clone()));
            ids
        } else {
            vec![(symbol.id, symbol.name.clone())]
        };

        for (target_id, target_name) in &target_ids {
            if caller_refs.len() >= max_refs || caller_bytes > callers_budget { break; }

            // Get edges for this target
            let target_edges = if *target_id == symbol.id {
                edges.clone()
            } else {
                indexer.db().edges_for_symbol(*target_id, languages.as_deref(), graph_version)?
            };

            // Collect resolved callers
            for edge in &target_edges {
                if edge.kind == "CALLS" && edge.target_symbol_id == Some(*target_id) {
                    if let Some(source_id) = edge.source_symbol_id {
                        if seen_caller_ids.insert(source_id) {
                            if let Ok(Some(caller_sym)) = indexer.db().get_symbol_by_id(source_id) {
                                let evidence = edge.evidence_snippet.clone();
                                let ref_json = serde_json::to_string(&caller_sym).unwrap_or_default();
                                caller_bytes += ref_json.len() + evidence.as_ref().map_or(0, |e| e.len());
                                if caller_bytes > callers_budget {
                                    truncated = true;
                                    break;
                                }
                                caller_refs.push(ExplainRef {
                                    signature: caller_sym.signature.clone(),
                                    symbol: caller_sym,
                                    evidence,
                                    edge_kind: "CALLS".to_string(),
                                });
                                if caller_refs.len() >= max_refs { break; }
                            }
                        }
                    }
                }
            }

            // Check for unresolved callers by qualname
            if caller_refs.len() < max_refs && caller_bytes <= callers_budget {
                let unresolved_edges = indexer.db().incoming_edges_by_qualname_pattern(
                    target_name, "CALLS", languages.as_deref(), graph_version
                )?;

                for edge in &unresolved_edges {
                    if let Some(ref target_qn) = edge.target_qualname {
                        if target_qn.ends_with(target_name) {
                            if let Some(source_id) = edge.source_symbol_id {
                                if seen_caller_ids.insert(source_id) {
                                    if let Ok(Some(caller_sym)) = indexer.db().get_symbol_by_id(source_id) {
                                        let evidence = edge.evidence_snippet.clone();
                                        let ref_json = serde_json::to_string(&caller_sym).unwrap_or_default();
                                        caller_bytes += ref_json.len() + evidence.as_ref().map_or(0, |e| e.len());
                                        if caller_bytes > callers_budget {
                                            truncated = true;
                                            break;
                                        }
                                        caller_refs.push(ExplainRef {
                                            signature: caller_sym.signature.clone(),
                                            symbol: caller_sym,
                                            evidence,
                                            edge_kind: "CALLS".to_string(),
                                        });
                                        if caller_refs.len() >= max_refs { break; }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        used_bytes += caller_bytes;
        Some(caller_refs)
    } else { None };

    // 6. Build callees (outgoing CALLS) - FIX #3: For class symbols, aggregate from methods
    let mut callees = if sections.contains(&"callees".to_string()) {
        let mut callee_refs = Vec::new();
        let mut callee_bytes = 0usize;
        let mut seen_callee_ids = std::collections::HashSet::new();

        // Determine if this is a class-level symbol
        let is_class_symbol = symbol.kind == "class";

        if is_class_symbol {
            // For class symbols, find all methods in the same file within the class's line range
            let all_symbols = indexer.db().get_symbols_for_file(&symbol.file_path, graph_version)?;
            let methods: Vec<_> = all_symbols.into_iter()
                .filter(|s| {
                    (s.kind == "method" || s.kind == "function") &&
                    s.start_line >= symbol.start_line &&
                    s.end_line <= symbol.end_line
                })
                .collect();

            // Get callees from all methods
            for method in methods {
                let method_edges = indexer.db().edges_for_symbol(
                    method.id, languages.as_deref(), graph_version
                )?;

                for edge in &method_edges {
                    if edge.kind == "CALLS" && edge.source_symbol_id == Some(method.id) {
                        // Resolve target_id, with fuzzy fallback for unresolved edges
                        let target_id = match edge.target_symbol_id {
                            Some(id) => Some(id),
                            None => edge.target_qualname.as_deref().and_then(|qn| {
                                indexer.db().lookup_symbol_id_fuzzy(qn, languages.as_deref(), graph_version).ok().flatten()
                            }),
                        };
                        if let Some(target_id) = target_id {
                            if seen_callee_ids.insert(target_id) {
                                if let Ok(Some(callee_sym)) = indexer.db().get_symbol_by_id(target_id) {
                                    let evidence = edge.evidence_snippet.clone();
                                    let ref_json = serde_json::to_string(&callee_sym).unwrap_or_default();
                                    callee_bytes += ref_json.len() + evidence.as_ref().map_or(0, |e| e.len());
                                    if callee_bytes > callees_budget {
                                        truncated = true;
                                        break;
                                    }
                                    callee_refs.push(ExplainRef {
                                        signature: callee_sym.signature.clone(),
                                        symbol: callee_sym,
                                        evidence,
                                        edge_kind: "CALLS".to_string(),
                                    });
                                    if callee_refs.len() >= max_refs { break; }
                                }
                            }
                        }
                    }
                }
                if callee_refs.len() >= max_refs || callee_bytes > callees_budget {
                    break;
                }
            }
        } else {
            // For non-class symbols, use direct edges
            for edge in &edges {
                if edge.kind == "CALLS" && edge.source_symbol_id == Some(symbol.id) {
                    let target_id = match edge.target_symbol_id {
                        Some(id) => Some(id),
                        None => edge.target_qualname.as_deref().and_then(|qn| {
                            indexer.db().lookup_symbol_id_fuzzy(qn, languages.as_deref(), graph_version).ok().flatten()
                        }),
                    };
                    if let Some(target_id) = target_id {
                        if seen_callee_ids.insert(target_id) {
                            if let Ok(Some(callee_sym)) = indexer.db().get_symbol_by_id(target_id) {
                                let evidence = edge.evidence_snippet.clone();
                                let ref_json = serde_json::to_string(&callee_sym).unwrap_or_default();
                                callee_bytes += ref_json.len() + evidence.as_ref().map_or(0, |e| e.len());
                                if callee_bytes > callees_budget {
                                    truncated = true;
                                    break;
                                }
                                callee_refs.push(ExplainRef {
                                    signature: callee_sym.signature.clone(),
                                    symbol: callee_sym,
                                    evidence,
                                    edge_kind: "CALLS".to_string(),
                                });
                                if callee_refs.len() >= max_refs { break; }
                            }
                        }
                    }
                }
            }
        }

        used_bytes += callee_bytes;
        Some(callee_refs)
    } else { None };

    // 7. Find tests (incoming CALLS from test files)
    let mut tests = if sections.contains(&"tests".to_string()) {
        let mut test_refs = Vec::new();
        let mut test_bytes = 0usize;
        for edge in &edges {
            if edge.kind == "CALLS" && edge.target_symbol_id == Some(symbol.id) {
                if let Some(source_id) = edge.source_symbol_id {
                    if let Ok(Some(test_sym)) = indexer.db().get_symbol_by_id(source_id) {
                        let is_test = test_sym.file_path.contains("test")
                            || test_sym.file_path.contains("spec")
                            || test_sym.name.starts_with("test_")
                            || test_sym.name.starts_with("Test");
                        if is_test {
                            let ref_json = serde_json::to_string(&test_sym).unwrap_or_default();
                            test_bytes += ref_json.len();
                            if test_bytes > tests_budget {
                                truncated = true;
                                break;
                            }
                            test_refs.push(ExplainRef {
                                signature: test_sym.signature.clone(),
                                symbol: test_sym,
                                evidence: edge.evidence_snippet.clone(),
                                edge_kind: "CALLS".to_string(),
                            });
                            if test_refs.len() >= max_refs { break; }
                        }
                    }
                }
            }
        }
        used_bytes += test_bytes;
        Some(test_refs)
    } else { None };

    // 8. Find implements (EXTENDS/IMPLEMENTS/INHERITS edges) - FIX #2
    let implements = if sections.contains(&"implements".to_string()) {
        let mut impl_syms = Vec::new();
        for edge in &edges {
            if (edge.kind == "EXTENDS" || edge.kind == "IMPLEMENTS" || edge.kind == "INHERITS")
                && edge.source_symbol_id == Some(symbol.id) {
                if let Some(target_id) = edge.target_symbol_id {
                    if let Ok(Some(impl_sym)) = indexer.db().get_symbol_by_id(target_id) {
                        impl_syms.push(impl_sym);
                    }
                }
            }
        }
        if impl_syms.is_empty() { None } else { Some(impl_syms) }
    } else { None };

    // 9. FIX #4: Budget expansion - if >30% budget remaining, fetch source snippets for refs
    let budget_remaining = max_bytes.saturating_sub(used_bytes);
    let budget_utilization = (used_bytes as f64) / (max_bytes as f64);

    if budget_utilization < 0.70 && budget_remaining > expansion_budget {
        let repo_root = indexer.repo_root();
        let snippet_budget_per_ref = 500; // Max bytes per reference snippet

        // Expand callers with source snippets
        if let Some(ref caller_list) = callers {
            for caller_ref in caller_list.iter() {
                if used_bytes + snippet_budget_per_ref > max_bytes { break; }

                let full_path = repo_root.join(&caller_ref.symbol.file_path);
                if full_path.exists() {
                    if let Ok(content) = std::fs::read_to_string(&full_path) {
                        let lines: Vec<&str> = content.lines().collect();
                        let start = (caller_ref.symbol.start_line as usize).saturating_sub(1);
                        let end = ((caller_ref.symbol.start_line + 3) as usize).min(lines.len());
                        let snippet = lines[start..end].join("\n");
                        let snippet = if snippet.len() > snippet_budget_per_ref {
                            let truncate_pos = snippet[..snippet_budget_per_ref].rfind('\n').unwrap_or(snippet_budget_per_ref);
                            snippet[..truncate_pos].to_string()
                        } else {
                            snippet
                        };
                        used_bytes += snippet.len();
                    }
                }
            }
        }

        // Expand callees with source snippets
        if let Some(ref callee_list) = callees {
            for callee_ref in callee_list.iter() {
                if used_bytes + snippet_budget_per_ref > max_bytes { break; }

                let full_path = repo_root.join(&callee_ref.symbol.file_path);
                if full_path.exists() {
                    if let Ok(content) = std::fs::read_to_string(&full_path) {
                        let lines: Vec<&str> = content.lines().collect();
                        let start = (callee_ref.symbol.start_line as usize).saturating_sub(1);
                        let end = ((callee_ref.symbol.start_line + 3) as usize).min(lines.len());
                        let snippet = lines[start..end].join("\n");
                        let snippet = if snippet.len() > snippet_budget_per_ref {
                            let truncate_pos = snippet[..snippet_budget_per_ref].rfind('\n').unwrap_or(snippet_budget_per_ref);
                            snippet[..truncate_pos].to_string()
                        } else {
                            snippet
                        };
                        used_bytes += snippet.len();
                    }
                }
            }
        }
    }

    // 10. Apply format: "signatures" — strip symbols to compact form
    let format = params.format.as_deref().unwrap_or("full");
    let strip_to_compact = |refs: &mut Vec<ExplainRef>| {
        for r in refs.iter_mut() {
            r.symbol.docstring = None;
            r.symbol.commit_sha = None;
            r.symbol.stable_id = None;
            r.symbol.start_byte = 0;
            r.symbol.end_byte = 0;
            r.symbol.start_col = 0;
            r.symbol.end_col = 0;
        }
    };
    if format == "signatures" {
        if let Some(ref mut c) = callers { strip_to_compact(c); }
        if let Some(ref mut c) = callees { strip_to_compact(c); }
        if let Some(ref mut t) = tests { strip_to_compact(t); }
    }

    // 11. Build next_hops
    let next_hops = vec![
        json!({"method": "analyze_impact", "params": {"id": symbol.id}, "description": "Analyze downstream impact"}),
        json!({"method": "subgraph", "params": {"start_ids": [symbol.id], "depth": 2}, "description": "Explore graph neighborhood"}),
        json!({"method": "gather_context", "params": {"seeds": [{"type": "symbol", "qualname": symbol.qualname}], "max_bytes": 80000}, "description": "Assemble full context"}),
    ];

    let result = ExplainSymbolResult {
        symbol,
        source,
        callers,
        callees,
        tests,
        implements,
        budget: BudgetInfo {
            budget_bytes: max_bytes,
            used_bytes,
            truncated,
        },
        next_hops,
    };

    Ok(serde_json::to_value(&result)?)
}

pub(super) fn handle_open_file(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: OpenFileParams = serde_json::from_value(params)?;
    let (abs_path, rel_path) = resolve_repo_path(indexer.repo_root(), &params.path)?;
    let content = util::read_to_string(&abs_path)?;
    let mut text = if params.start_line.is_some() || params.end_line.is_some() {
        let total_lines = content.lines().count() as i64;
        let start_line = params.start_line.unwrap_or(1);
        let end_line = params.end_line.unwrap_or(total_lines);
        util::slice_lines(&content, start_line, end_line)
    } else {
        content
    };
    if let Some(max_bytes) = params.max_bytes {
        text = util::truncate_str_bytes(&text, max_bytes);
    }
    Ok(json!({ "path": rel_path, "text": text }))
}

// ---------------------------------------------------------------------------
// GROUP 4 -- Metrics handlers
// ---------------------------------------------------------------------------

pub(super) fn handle_repo_overview(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: OverviewParams = serde_json::from_value(params)?;
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let overview = indexer.db().repo_overview(
        indexer.repo_root().clone(),
        languages.as_deref(),
        graph_version,
    )?;
    Ok(apply_field_filters(
        json!(overview),
        params.summary.unwrap_or(false),
        params.fields.as_deref(),
        &["files", "symbols", "edges"],
    ))
}

pub(super) fn handle_repo_insights(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: InsightsParams = serde_json::from_value(params)?;
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
    let complexity_limit = params.complexity_limit.unwrap_or(10);
    let min_complexity = params.min_complexity.unwrap_or(1);
    let duplicate_limit = params.duplicate_limit.unwrap_or(10);
    let duplicate_min_count = params.duplicate_min_count.unwrap_or(2);
    let duplicate_min_loc = params.duplicate_min_loc.unwrap_or(5);
    let duplicate_per_group_limit = params.duplicate_per_group_limit.unwrap_or(10);
    let call_edges = indexer.db().call_edge_count(
        languages.as_deref(),
        paths.as_deref(),
        graph_version,
    )?;
    let top_complexity = indexer.db().top_complexity(
        complexity_limit,
        min_complexity,
        languages.as_deref(),
        paths.as_deref(),
        graph_version,
    )?;
    let duplicate_groups = indexer.db().duplicate_groups(
        duplicate_limit,
        duplicate_min_count,
        duplicate_min_loc,
        duplicate_per_group_limit,
        languages.as_deref(),
        paths.as_deref(),
        graph_version,
    )?;
    let coupling_limit = params.coupling_limit.unwrap_or(10);
    let top_fan_in = indexer.db().top_fan_in(
        coupling_limit,
        languages.as_deref(),
        paths.as_deref(),
        graph_version,
    )?;
    let top_fan_out = indexer.db().top_fan_out(
        coupling_limit,
        languages.as_deref(),
        paths.as_deref(),
        graph_version,
    )?;
    let diagnostics = indexer.db().diagnostics_summary(
        languages.as_deref(),
        paths.as_deref(),
        None,
        None,
        None,
    )?;
    let staleness = if params.include_staleness.unwrap_or(false) {
        let staleness_limit = params.staleness_limit.unwrap_or(1000);
        let dead_symbols_list = indexer.db().dead_symbols(
            staleness_limit,
            languages.as_deref(),
            paths.as_deref(),
            graph_version,
        )?;
        let dead_symbols_count = dead_symbols_list.iter().filter(|s| !is_test_symbol(s)).count() as i64;
        let unused_imports_count = indexer.db().unused_imports(
            staleness_limit,
            languages.as_deref(),
            paths.as_deref(),
            graph_version,
        )?.len() as i64;
        let orphan_tests_count = indexer.db().orphan_tests(
            staleness_limit,
            languages.as_deref(),
            paths.as_deref(),
            graph_version,
        )?.len() as i64;
        Some(crate::model::StalenessMetrics {
            dead_symbols: dead_symbols_count,
            unused_imports: unused_imports_count,
            orphan_tests: orphan_tests_count,
        })
    } else {
        None
    };
    let coupling_hotspots = if params.include_coupling_hotspots.unwrap_or(false) {
        let hotspots_limit = params.coupling_hotspots_limit.unwrap_or(10);
        let min_confidence = params.coupling_hotspots_min_confidence.unwrap_or(0.5);
        Some(indexer.db().coupling_hotspots(hotspots_limit, min_confidence)?)
    } else {
        None
    };
    let last_indexed = indexer.db().get_meta_i64("last_indexed")?;
    let commit_sha = indexer.db().graph_version_commit(graph_version)?;
    let insights = RepoInsights {
        repo_root: indexer.repo_root().to_string_lossy().to_string(),
        call_edges,
        top_complexity,
        duplicate_groups,
        top_fan_in,
        top_fan_out,
        coupling_hotspots,
        diagnostics,
        staleness,
        last_indexed,
        graph_version: Some(graph_version),
        commit_sha,
    };
    Ok(json!(insights))
}

pub(super) fn handle_module_map(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: ModuleMapParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let depth = params.depth.unwrap_or(1).max(1).min(5);
    let include_edges = params.include_edges.unwrap_or(true);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;

    let summary = indexer.db().module_summary(
        depth,
        languages.as_deref(),
        paths.as_deref(),
        graph_version,
    )?;

    let modules: Vec<ModuleNode> = summary
        .into_iter()
        .map(|m| ModuleNode {
            path: m.path,
            file_count: m.file_count,
            symbol_count: m.symbol_count,
            languages: m.languages,
        })
        .collect();

    let edges = if include_edges {
        let edge_data = indexer.db().module_edges(
            depth,
            languages.as_deref(),
            graph_version,
        )?;
        edge_data
            .into_iter()
            .map(|(src, dst, calls, imports)| ModuleEdge {
                source_module: src,
                target_module: dst,
                call_count: calls,
                import_count: imports,
            })
            .collect()
    } else {
        vec![]
    };

    let next_hops: Vec<serde_json::Value> = modules
        .iter()
        .take(5)
        .map(|m| {
            json!({
                "method": "find_symbol",
                "params": {"query": &m.path, "limit": 20},
                "description": format!("Explore {}", m.path)
            })
        })
        .collect();

    let result = ModuleMapResult {
        modules,
        edges,
        next_hops,
    };
    Ok(json!(result))
}

pub(super) fn handle_repo_map(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: RepoMapParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let max_bytes = params.max_bytes.unwrap_or(8000).max(1000).min(50000);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;

    let config = crate::repo_map::RepoMapConfig {
        max_bytes,
        languages,
        paths,
        graph_version,
    };

    let result = crate::repo_map::build_repo_map(indexer.db(), &config)?;

    let next_hops: Vec<serde_json::Value> = vec![
        json!({
            "method": "module_map",
            "params": {"depth": 2, "include_edges": true},
            "description": "Explore full module DAG"
        }),
        json!({
            "method": "search",
            "params": {"query": "main entry", "limit": 10},
            "description": "Find entry points"
        }),
    ];

    Ok(json!({
        "text": result.text,
        "modules": result.modules,
        "symbols": result.symbols,
        "bytes": result.bytes,
        "next_hops": next_hops,
    }))
}

pub(super) fn handle_top_complexity(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: TopComplexityParams = serde_json::from_value(params)?;
    let limit = params.limit.unwrap_or(10);
    let min_complexity = params.min_complexity.unwrap_or(1);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
    let results = indexer.db().top_complexity(
        limit,
        min_complexity,
        languages.as_deref(),
        paths.as_deref(),
        graph_version,
    )?;
    Ok(json!(results))
}

pub(super) fn handle_duplicate_groups(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: DuplicateGroupsParams = serde_json::from_value(params)?;
    let limit = params.limit.unwrap_or(10);
    let min_count = params.min_count.unwrap_or(2);
    let min_loc = params.min_loc.unwrap_or(5);
    let per_group_limit = params.per_group_limit.unwrap_or(10);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
    let groups = indexer.db().duplicate_groups(
        limit,
        min_count,
        min_loc,
        per_group_limit,
        languages.as_deref(),
        paths.as_deref(),
        graph_version,
    )?;
    Ok(json!(groups))
}

pub(super) fn handle_top_coupling(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: TopCouplingParams = serde_json::from_value(params)?;
    let limit = params.limit.unwrap_or(10);
    let direction = params.direction.as_deref().unwrap_or("both");
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;

    let result = match direction {
        "in" => {
            let fan_in = indexer.db().top_fan_in(
                limit,
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;
            json!({
                "fan_in": fan_in,
            })
        }
        "out" => {
            let fan_out = indexer.db().top_fan_out(
                limit,
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;
            json!({
                "fan_out": fan_out,
            })
        }
        "both" => {
            let fan_in = indexer.db().top_fan_in(
                limit,
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;
            let fan_out = indexer.db().top_fan_out(
                limit,
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;
            json!({
                "fan_in": fan_in,
                "fan_out": fan_out,
            })
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Invalid direction: {}. Must be 'in', 'out', or 'both'",
                direction
            ));
        }
    };
    Ok(result)
}

pub(super) fn handle_co_changes(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: CoChangesParams = serde_json::from_value(params)?;
    let limit = params.limit.unwrap_or(50).min(MAX_RESPONSE_LIMIT);
    let min_confidence = params.min_confidence.unwrap_or(0.3);
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;

    // Get paths from either path/paths params or qualname
    let paths = if params.path.is_some() || params.paths.is_some() {
        normalize_search_paths(indexer.repo_root(), params.path, params.paths)?
    } else if let Some(ref qualname) = params.qualname {
        // Resolve qualname to file path
        if let Some(symbol) = indexer.db().get_symbol_by_qualname(qualname, graph_version)? {
            Some(vec![symbol.file_path])
        } else {
            return Err(anyhow::anyhow!("Symbol not found: {}", qualname));
        }
    } else {
        return Err(anyhow::anyhow!("Must provide either path, paths, or qualname"));
    };

    let results = if let Some(ref paths) = paths {
        if paths.len() == 1 {
            indexer.db().co_changes_for_file(&paths[0], limit, min_confidence, graph_version)?
        } else {
            indexer.db().co_changes_for_files(paths, limit, min_confidence, graph_version)?
        }
    } else {
        Vec::new()
    };

    // Generate next_hops
    let mut next_hops = Vec::new();
    if let Some(ref paths) = paths {
        let first_path = &paths[0];
        for (i, result) in results.iter().take(3).enumerate() {
            let other_file = if &result.file_a == first_path {
                &result.file_b
            } else {
                &result.file_a
            };
            next_hops.push(json!({
                "method": "open_file",
                "params": {
                    "path": other_file,
                },
                "label": format!("Open co-changed file #{}", i + 1),
            }));
        }
    }

    Ok(json!({
        "results": results,
        "next_hops": next_hops,
    }))
}

pub(super) fn handle_dead_symbols(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: DeadSymbolsParams = serde_json::from_value(params)?;
    let limit = params.limit.unwrap_or(50).min(MAX_RESPONSE_LIMIT);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
    let mut results = indexer.db().dead_symbols(
        limit,
        languages.as_deref(),
        paths.as_deref(),
        graph_version,
    )?;
    // Filter out test symbols
    results.retain(|s| !is_test_symbol(s));
    let compact: Vec<SymbolCompact> = results.into_iter().map(|s| s.into()).collect();
    Ok(json!(compact))
}

pub(super) fn handle_unused_imports(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: UnusedImportsParams = serde_json::from_value(params)?;
    let limit = params.limit.unwrap_or(50).min(MAX_RESPONSE_LIMIT);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
    let results = indexer.db().unused_imports(
        limit,
        languages.as_deref(),
        paths.as_deref(),
        graph_version,
    )?;
    Ok(json!(results))
}

pub(super) fn handle_orphan_tests(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: OrphanTestsParams = serde_json::from_value(params)?;
    let limit = params.limit.unwrap_or(50).min(MAX_RESPONSE_LIMIT);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
    let results = indexer.db().orphan_tests(
        limit,
        languages.as_deref(),
        paths.as_deref(),
        graph_version,
    )?;
    let compact: Vec<SymbolCompact> = results.into_iter().map(|s| s.into()).collect();
    Ok(json!(compact))
}

// ---------------------------------------------------------------------------
// GROUP 2 -- Graph handlers
// ---------------------------------------------------------------------------

pub(super) fn handle_neighbors(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: NeighborsParams = serde_json::from_value(params)?;
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let edges =
        indexer
            .db()
            .edges_for_symbol(params.id, languages.as_deref(), graph_version)?;
    let mut ids = std::collections::HashSet::new();
    ids.insert(params.id);
    for edge in &edges {
        if edge.source_symbol_id == Some(params.id) {
            if let Some(id) = edge.target_symbol_id {
                ids.insert(id);
            }
        } else if let Some(id) = edge.source_symbol_id {
            ids.insert(id);
        }
    }
    let mut id_list: Vec<i64> = ids.into_iter().collect();
    id_list.sort_unstable();
    let nodes =
        indexer
            .db()
            .symbols_by_ids(&id_list, languages.as_deref(), graph_version)?;
    let allowed: std::collections::HashSet<i64> =
        nodes.iter().map(|symbol| symbol.id).collect();
    let filtered_edges: Vec<_> = if languages.is_some() && allowed.is_empty() {
        Vec::new()
    } else {
        edges
            .into_iter()
            .filter(|edge| {
                let source_ok = edge
                    .source_symbol_id
                    .map(|id| allowed.contains(&id))
                    .unwrap_or(true);
                let target_ok = edge
                    .target_symbol_id
                    .map(|id| allowed.contains(&id))
                    .unwrap_or(true);
                source_ok && target_ok
            })
            .collect()
    };
    let mut value = json!(Subgraph {
        nodes,
        edges: filtered_edges,
    });

    // Check format param
    let format = params.format.as_deref().unwrap_or("full");
    if format == "signatures" {
        value = apply_compact_format(value);
    }

    Ok(value)
}

pub(super) fn handle_subgraph(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: SubgraphParams = serde_json::from_value(params)?;
    let depth = params.depth.unwrap_or(2);
    let max_nodes = params.max_nodes.unwrap_or(50);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let mut start_ids = params.start_ids.unwrap_or_default();
    let mut missing = Vec::new();
    if let Some(roots) = params.start_qualnames {
        for raw in roots {
            let qualname = raw.trim();
            if qualname.is_empty() {
                continue;
            }
            let id = indexer.db().lookup_symbol_id_filtered(
                qualname,
                languages.as_deref(),
                graph_version,
            )?;
            match id {
                Some(id) => start_ids.push(id),
                None => missing.push(qualname.to_string()),
            }
        }
    }
    if start_ids.is_empty() {
        return Err(anyhow::anyhow!("subgraph requires start_ids or roots"));
    }
    if !missing.is_empty() {
        return Err(anyhow::anyhow!(
            "subgraph roots not found: {}",
            missing.join(", ")
        ));
    }
    start_ids.sort_unstable();
    start_ids.dedup();
    let include_kinds = match params.kinds.as_deref() {
        Some(kinds) => normalize_edge_kinds(kinds),
        None => None,
    };
    let (exclude_kinds, exclude_all) = match params.exclude_kinds.as_deref() {
        Some(kinds) => normalize_edge_kinds_exclude(kinds),
        None => (HashSet::new(), false),
    };
    let filter = subgraph::EdgeFilter {
        include: include_kinds,
        exclude: exclude_kinds,
        exclude_all,
        resolved_only: params.resolved_only.unwrap_or(false),
    };
    let graph = subgraph::build_subgraph_filtered(
        indexer.db(),
        &start_ids,
        depth,
        max_nodes,
        languages.as_deref(),
        graph_version,
        Some(&filter),
    )?;
    let mut value = json!(graph);

    // Check format param
    let format = params.format.as_deref().unwrap_or("full");
    if format == "signatures" {
        value = apply_compact_format(value);
    }

    Ok(value)
}

pub(super) fn handle_references(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: ReferencesParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let symbol = if let Some(id) = params.id {
        indexer.db().get_symbol_by_id(id)?
    } else if let Some(qualname) = params.qualname.as_deref() {
        indexer
            .db()
            .get_symbol_by_qualname(qualname, graph_version)?
    } else {
        return Err(anyhow::anyhow!("references requires id or qualname"));
    };
    let symbol = symbol.ok_or_else(|| anyhow::anyhow!("symbol not found"))?;
    let direction = parse_edge_direction(params.direction.as_deref())?;
    let limit = params.limit.unwrap_or(100).min(MAX_RESPONSE_LIMIT);
    let include_symbols = params.include_symbols.unwrap_or(true);
    let include_snippet = params.include_snippet.unwrap_or(true);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let kinds = params.kinds.unwrap_or_else(|| vec!["CALLS".to_string()]);
    let kind_filter = normalize_edge_kinds(&kinds);

    // Detect if this is a container type (class, struct, interface, etc.)
    let is_container = matches!(
        symbol.kind.as_str(),
        "class" | "interface" | "struct" | "enum" | "trait" | "service"
    );

    // Build target_ids list: start with the symbol itself
    let mut target_ids = vec![symbol.id];
    let mut member_count = 0;

    // If it's a container, find all members via CONTAINS edges
    if is_container {
        let container_edges = indexer
            .db()
            .edges_for_symbol(symbol.id, languages.as_deref(), graph_version)?;
        for edge in &container_edges {
            if edge.kind == "CONTAINS" && edge.source_symbol_id == Some(symbol.id) {
                if let Some(target_id) = edge.target_symbol_id {
                    target_ids.push(target_id);
                    member_count += 1;
                }
            }
        }
    }

    // Query edges for all target IDs (class + members)
    let all_edges = if target_ids.len() > 1 {
        // Use batch query for multiple symbols
        let edges_by_symbol = indexer
            .db()
            .edges_for_symbols(&target_ids, languages.as_deref(), graph_version)?;
        // Flatten the HashMap into a single Vec, deduplicating by edge ID
        let mut edge_map: HashMap<i64, Edge> = HashMap::new();
        for edges in edges_by_symbol.values() {
            for edge in edges {
                edge_map.insert(edge.id, edge.clone());
            }
        }
        edge_map.into_values().collect()
    } else {
        // Use single-symbol query (existing path)
        indexer
            .db()
            .edges_for_symbol(symbol.id, languages.as_deref(), graph_version)?
    };

    let mut incoming = Vec::new();
    let mut outgoing = Vec::new();
    let wants_in = matches!(direction, EdgeDirection::In | EdgeDirection::Both);
    let wants_out = matches!(direction, EdgeDirection::Out | EdgeDirection::Both);
    for edge in all_edges {
        if !edge_kind_matches(&edge.kind, &kind_filter) {
            continue;
        }
        // Filter out CONTAINS edges from the results when showing incoming references
        if edge.kind == "CONTAINS" {
            continue;
        }
        let is_out = edge
            .source_symbol_id
            .map_or(false, |id| target_ids.contains(&id));
        let is_in = edge
            .target_symbol_id
            .map_or(false, |id| target_ids.contains(&id));
        let include_out = wants_out && is_out;
        let include_in = wants_in && is_in;
        match (include_in, include_out) {
            (true, true) => {
                incoming.push(edge.clone());
                outgoing.push(edge);
            }
            (true, false) => incoming.push(edge),
            (false, true) => outgoing.push(edge),
            (false, false) => {}
        }
    }
    if limit == 0 {
        incoming.clear();
        outgoing.clear();
    } else {
        incoming.truncate(limit);
        outgoing.truncate(limit);
    }

    let mut symbol_map = HashMap::new();
    if include_symbols {
        let mut ids = HashSet::new();
        for edge in incoming.iter().chain(outgoing.iter()) {
            if let Some(id) = edge.source_symbol_id {
                ids.insert(id);
            }
            if let Some(id) = edge.target_symbol_id {
                ids.insert(id);
            }
        }
        if !ids.is_empty() {
            let mut id_list: Vec<i64> = ids.into_iter().collect();
            id_list.sort_unstable();
            let symbols = indexer.db().symbols_by_ids(&id_list, None, graph_version)?;
            for symbol in symbols {
                symbol_map.insert(symbol.id, symbol);
            }
        }
    }

    let incoming =
        build_edge_references(incoming, &symbol_map, include_symbols, include_snippet);
    let outgoing =
        build_edge_references(outgoing, &symbol_map, include_symbols, include_snippet);

    // Add metadata if we aggregated members
    let metadata = if member_count > 0 {
        Some(ReferencesMetadata {
            aggregated_members: member_count,
            note: format!(
                "Includes references to {} member methods/fields",
                member_count
            ),
        })
    } else {
        None
    };

    let mut value = json!(ReferencesResult {
        symbol,
        incoming,
        outgoing,
        metadata,
    });

    // Check format param
    let format = params.format.as_deref().unwrap_or("full");
    if format == "signatures" {
        value = apply_compact_format(value);
    }

    Ok(value)
}

pub(super) fn handle_trace_flow(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: TraceFlowParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let languages = params.languages.clone();
    let max_hops = params.max_hops.unwrap_or(5).min(10);
    let direction = params.direction.as_deref().unwrap_or("downstream");
    let include_snippets = params.include_snippets.unwrap_or(true);
    let max_bytes = params.max_bytes.unwrap_or(30_000).min(200_000);
    let trace_offset = params.trace_offset.unwrap_or(0);
    let compact_mode = params.format.as_deref() == Some("compact");
    let allowed_kinds: Vec<String> = params.kinds.clone().unwrap_or_else(||
        vec![
            "CALLS".into(), "RPC_IMPL".into(), "RPC_CALL".into(), "XREF".into(),
            "CHANNEL_PUBLISH".into(), "CHANNEL_SUBSCRIBE".into(),
            "HTTP_CALL".into(), "HTTP_ROUTE".into(),
        ]
    );

    // Resolve start symbol
    let start = if let Some(id) = params.start_id {
        indexer.db().get_symbol_by_id(id)?
            .ok_or_else(|| anyhow::anyhow!("start symbol not found: id={}", id))?
    } else if let Some(ref qn) = params.start_qualname {
        let id = indexer.db().lookup_symbol_id(qn, graph_version)?
            .ok_or_else(|| anyhow::anyhow!("start symbol not found: {}", qn))?;
        indexer.db().get_symbol_by_id(id)?
            .ok_or_else(|| anyhow::anyhow!("start symbol not found"))?
    } else {
        anyhow::bail!("trace_flow requires start_id or start_qualname");
    };

    // Resolve optional end symbol
    let end_id = if let Some(id) = params.end_id {
        Some(id)
    } else if let Some(ref qn) = params.end_qualname {
        indexer.db().lookup_symbol_id(qn, graph_version)?
    } else {
        None
    };

    // If start symbol is a container, also seed BFS with its members
    let is_container = matches!(start.kind.as_str(), "class" | "module" | "resource");
    let mut seed_ids: Vec<i64> = vec![start.id];
    if is_container {
        if let Ok(file_symbols) = indexer.db().get_symbols_for_file(&start.file_path, graph_version) {
            for s in &file_symbols {
                if s.id != start.id
                    && s.start_line >= start.start_line
                    && s.end_line <= start.end_line
                    && matches!(s.kind.as_str(), "method" | "function" | "resource" | "var" | "param" | "output")
                {
                    seed_ids.push(s.id);
                }
            }
        }
    }

    // BFS from start (seeded with container members if applicable)
    let mut trace = Vec::new();
    let mut visited = std::collections::HashSet::new();
    let mut queue = std::collections::VecDeque::new();
    for &sid in &seed_ids {
        visited.insert(sid);
        queue.push_back((sid, 0usize, start.file_path.clone()));
    }
    let mut used_bytes = 0;
    let mut truncated = false;
    let mut reached_target = false;

    while let Some((current_id, dist, prev_file)) = queue.pop_front() {
        if dist > max_hops {
            truncated = true;
            break;
        }
        if used_bytes >= max_bytes {
            truncated = true;
            break;
        }

        let mut edges = indexer.db().edges_for_symbol(
            current_id, languages.as_deref(), graph_version
        )?;

        // For upstream direction, also find unresolved callers via qualname pattern
        if direction == "upstream" {
            if let Ok(Some(current_sym)) = indexer.db().get_symbol_by_id(current_id) {
                for kind in &allowed_kinds {
                    let mut unresolved = indexer.db().incoming_edges_by_qualname_pattern(
                        &current_sym.name, kind, languages.as_deref(), graph_version
                    ).unwrap_or_default();
                    edges.append(&mut unresolved);
                }
            }
        }

        // Collect bridgeable edges for a second pass
        let mut bridge_targets: Vec<(String, String)> = Vec::new(); // (target_qualname, edge_kind)

        for edge in &edges {
            if !allowed_kinds.contains(&edge.kind) { continue; }

            // Determine next symbol based on direction
            let next_id = if direction == "downstream" {
                // Follow outgoing calls: we are the source, get target
                if edge.source_symbol_id != Some(current_id) { continue; }
                edge.target_symbol_id
            } else {
                // Follow incoming calls: we are the target, get source
                // For resolved edges, check target matches us
                // For unresolved edges (from qualname pattern), source_symbol_id is the caller
                if edge.target_symbol_id == Some(current_id) {
                    edge.source_symbol_id
                } else if edge.target_symbol_id.is_none() {
                    // Unresolved edge from qualname pattern — source is the caller
                    edge.source_symbol_id
                } else {
                    // This edge doesn't target us
                    continue;
                }
            };

            // Check for bridgeable edge (e.g., CHANNEL_PUBLISH → CHANNEL_SUBSCRIBE)
            if let Some(ref tq) = edge.target_qualname {
                if crate::indexer::channel::bridge_complement(&edge.kind).is_some() {
                    bridge_targets.push((tq.clone(), edge.kind.clone()));
                }
            }

            // Resolve next_id, trying fuzzy lookup if unresolved
            let next_id = match next_id {
                Some(id) => id,
                None => {
                    // Try fuzzy resolve on target_qualname if available
                    if let Some(ref qn) = edge.target_qualname {
                        match indexer.db().lookup_symbol_id_fuzzy(qn, languages.as_deref(), graph_version) {
                            Ok(Some(id)) => id,
                            _ => continue,
                        }
                    } else {
                        continue;
                    }
                }
            };

            if !visited.insert(next_id) { continue; }

            if let Ok(Some(next_sym)) = indexer.db().get_symbol_by_id(next_id) {
                let prev_lang = detect_language(&prev_file);
                let next_lang = detect_language(&next_sym.file_path);
                let cross_lang = prev_lang != next_lang;
                let language = next_lang.clone();

                // Read snippet if requested
                let snippet = if include_snippets {
                    edge.evidence_snippet.clone()
                } else { None };

                // Detect language boundary and add annotations
                let (boundary_type, boundary_detail, protocol_context) = if cross_lang {
                    let b_type = detect_boundary_type(&edge.kind, &prev_lang, &next_lang);
                    let b_detail = build_boundary_detail(&b_type, &prev_lang, &next_lang);
                    let p_context = extract_protocol_context(edge);
                    (Some(b_type), Some(b_detail), p_context)
                } else {
                    (None, None, None)
                };

                let hop = TraceHop {
                    symbol: next_sym.clone(),
                    edge_kind: edge.kind.clone(),
                    distance: dist + 1,
                    language,
                    snippet,
                    cross_language: cross_lang,
                    boundary_type,
                    boundary_detail,
                    protocol_context,
                };

                let hop_size = estimate_hop_size(&hop, compact_mode);
                let hop_idx = trace.len();
                trace.push(hop);
                if hop_idx >= trace_offset {
                    used_bytes += hop_size;
                    if used_bytes >= max_bytes {
                        truncated = true;
                        break;
                    }
                }

                // Check if we reached the target
                if end_id == Some(next_id) {
                    reached_target = true;
                    break;
                }

                queue.push_back((next_id, dist + 1, next_sym.file_path.clone()));
            }
        }

        // Bridge pass: for edges with bridge complements, find cross-service symbols
        if !reached_target && !truncated {
            for (tq, edge_kind) in &bridge_targets {
                if let Some(complement_kinds) = crate::indexer::channel::bridge_complement(edge_kind) {
                    let bridged = indexer.db().edges_by_target_qualname_and_kinds(
                        tq, complement_kinds, languages.as_deref(), graph_version
                    ).unwrap_or_default();
                    let b_type = crate::indexer::channel::boundary_type_for_kind(edge_kind);
                    for bridged_edge in &bridged {
                        let Some(bridged_id) = bridged_edge.source_symbol_id else { continue };
                        if !visited.insert(bridged_id) { continue; }
                        if let Ok(Some(bridged_sym)) = indexer.db().get_symbol_by_id(bridged_id) {
                            let prev_lang = detect_language(&prev_file);
                            let next_lang = detect_language(&bridged_sym.file_path);
                            let b_detail = build_boundary_detail(b_type, &prev_lang, &next_lang);
                            let p_context = extract_protocol_context(bridged_edge);
                            let hop = TraceHop {
                                symbol: bridged_sym.clone(),
                                edge_kind: bridged_edge.kind.clone(),
                                distance: dist + 1,
                                language: next_lang,
                                snippet: if include_snippets { bridged_edge.evidence_snippet.clone() } else { None },
                                cross_language: true,
                                boundary_type: Some(b_type.to_string()),
                                boundary_detail: Some(b_detail),
                                protocol_context: p_context,
                            };
                            let hop_size = estimate_hop_size(&hop, compact_mode);
                            let hop_idx = trace.len();
                            trace.push(hop);
                            if hop_idx >= trace_offset {
                                used_bytes += hop_size;
                                if used_bytes >= max_bytes { truncated = true; break; }
                            }
                            if end_id == Some(bridged_id) { reached_target = true; break; }
                            queue.push_back((bridged_id, dist + 1, bridged_sym.file_path.clone()));
                        }
                    }
                    if reached_target || truncated { break; }
                }
            }
        }

        if reached_target || truncated { break; }
    }

    // Sort trace by distance, then apply offset pagination
    trace.sort_by_key(|h| h.distance);
    let trace: Vec<TraceHop> = trace.into_iter().skip(trace_offset).collect();

    let end_sym = if let Some(eid) = end_id {
        indexer.db().get_symbol_by_id(eid)?
    } else { None };

    // Build next_hops with continuation when truncated
    let mut next_hops: Vec<serde_json::Value> = Vec::new();
    if truncated {
        let next_offset = trace_offset + trace.len();
        let mut continue_params = json!({
            "max_hops": max_hops,
            "include_snippets": include_snippets,
            "trace_offset": next_offset,
        });
        if let Some(ref qn) = params.start_qualname {
            continue_params["start_qualname"] = json!(qn);
        } else if let Some(id) = params.start_id {
            continue_params["start_id"] = json!(id);
        }
        if let Some(ref k) = params.kinds {
            continue_params["kinds"] = json!(k);
        }
        if let Some(ref f) = params.format {
            continue_params["format"] = json!(f);
        }
        next_hops.push(json!({
            "method": "trace_flow",
            "params": continue_params,
            "description": format!("Continue trace (offset {})", next_offset),
        }));
    }
    for h in trace.iter().take(3) {
        next_hops.push(json!({
            "method": "explain_symbol",
            "params": {"id": h.symbol.id},
            "description": format!("Explain {}", h.symbol.name),
        }));
    }

    // Calculate paths_found: 0 if empty and no target reached, 1 if target reached, else count leaf nodes
    let paths_found = if trace.is_empty() {
        0
    } else if end_id.is_some() {
        if reached_target { 1 } else { 0 }
    } else {
        // Count distinct leaf nodes (max distance symbols)
        let max_dist = trace.iter().map(|h| h.distance).max().unwrap_or(0);
        trace.iter().filter(|h| h.distance == max_dist).count()
    };

    let result = TraceFlowResult {
        start,
        end: end_sym,
        trace,
        paths_found,
        reached_target,
        truncated,
        budget: BudgetInfo {
            budget_bytes: max_bytes,
            used_bytes,
            truncated,
        },
        next_hops,
    };

    let mut value = serde_json::to_value(&result)?;
    let format = params.format.as_deref().unwrap_or("full");
    if format == "compact" {
        value = apply_compact_format(value);
    }
    Ok(value)
}

pub(super) fn handle_route_refs(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: RouteRefsParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let limit = params.limit.unwrap_or(200).min(MAX_RESPONSE_LIMIT);
    let include_symbols = params.include_symbols.unwrap_or(true);
    let include_snippet = params.include_snippet.unwrap_or(true);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
    // Try HTTP route first, fall back to partial matching, then gRPC RPC_IMPL
    let (mut edges, normalized) =
        if let Some(norm) = xref::normalize_route_literal(&params.query) {
            let kinds = Some(vec!["ROUTE".to_string()]);
            let edges = indexer.db().list_edges(
                limit, 0, languages.as_deref(), paths.as_deref(),
                kinds.as_deref(), None, None, Some(&norm),
                false, None, graph_version, None, None, None,
            )?;
            (edges, norm)
        } else {
            // Fall back to gRPC service lookup via RPC_IMPL edges.
            // Fetch all RPC_IMPL edges and filter by query (case-insensitive).
            let kinds = Some(vec!["RPC_IMPL".to_string()]);
            let all_rpc = indexer.db().list_edges(
                500, 0, languages.as_deref(), paths.as_deref(),
                kinds.as_deref(), None, None, None,
                false, None, graph_version, None, None, None,
            )?;
            let query_lower = params.query.to_lowercase();
            let edges: Vec<_> = all_rpc
                .into_iter()
                .filter(|e| {
                    e.target_qualname
                        .as_ref()
                        .map_or(false, |qn| qn.to_lowercase().contains(&query_lower))
                })
                .take(limit)
                .collect();
            (edges, params.query.clone())
        };

    // If exact match failed and we have a normalized route, try partial/prefix matching
    if edges.is_empty() {
        if let Some(norm) = xref::normalize_route_literal(&params.query) {
            // Try partial matching on ROUTE and HTTP_ROUTE edges
            let kinds = Some(vec!["ROUTE".to_string(), "HTTP_ROUTE".to_string()]);
            let all_routes = indexer.db().list_edges(
                2000, 0, languages.as_deref(), paths.as_deref(),
                kinds.as_deref(), None, None, None,
                false, None, graph_version, None, None, None,
            )?;
            let query_lower = norm.to_lowercase();
            edges = all_routes
                .into_iter()
                .filter(|e| {
                    e.target_qualname
                        .as_ref()
                        .map_or(false, |qn| qn.to_lowercase().contains(&query_lower))
                })
                .take(limit)
                .collect();
        }
    }
    let mut symbol_map = HashMap::new();
    if include_symbols {
        let mut ids = HashSet::new();
        for edge in &edges {
            if let Some(id) = edge.source_symbol_id {
                ids.insert(id);
            }
            if let Some(id) = edge.target_symbol_id {
                ids.insert(id);
            }
        }
        if !ids.is_empty() {
            let mut id_list: Vec<i64> = ids.into_iter().collect();
            id_list.sort_unstable();
            let symbols = indexer.db().symbols_by_ids(&id_list, None, graph_version)?;
            for symbol in symbols {
                symbol_map.insert(symbol.id, symbol);
            }
        }
    }
    let references =
        build_edge_references(edges, &symbol_map, include_symbols, include_snippet);
    Ok(json!(RouteRefsResult {
        query: params.query,
        normalized,
        references,
    }))
}

pub(super) fn handle_flow_status(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: FlowStatusParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let limit = params.limit.unwrap_or(200).min(MAX_RESPONSE_LIMIT);
    let edge_limit = params.edge_limit.unwrap_or(50_000);
    let include_routes = params.include_routes.unwrap_or(true);
    let include_calls = params.include_calls.unwrap_or(true);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
    let route_kinds = vec![
        http::HTTP_ROUTE_KIND.to_string(),
        proto::RPC_IMPL_KIND.to_string(),
    ];
    let call_kinds = vec![http::HTTP_CALL_KIND.to_string()];
    let routes = indexer.db().list_edges(
        edge_limit,
        0,
        languages.as_deref(),
        paths.as_deref(),
        Some(&route_kinds),
        None,
        None,
        None,
        false,
        None,
        graph_version,
        None,
        None,
        None,
    )?;
    let calls = indexer.db().list_edges(
        edge_limit,
        0,
        languages.as_deref(),
        paths.as_deref(),
        Some(&call_kinds),
        None,
        None,
        None,
        false,
        None,
        graph_version,
        None,
        None,
        None,
    )?;
    let result = build_flow_status(
        routes,
        calls,
        include_routes,
        include_calls,
        limit,
        edge_limit,
    );
    Ok(json!(result))
}

// ---------------------------------------------------------------------------
// GROUP 3 -- Analysis handlers
// ---------------------------------------------------------------------------

pub(super) fn handle_find_tests_for(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: FindTestsForParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let languages = params.languages.clone();
    let include_indirect = params.include_indirect.unwrap_or(true);
    let indirect_depth = params.indirect_depth.unwrap_or(1).min(5);
    let limit = params.limit.unwrap_or(20).min(100);

    // Resolve symbol by id, qualname, or query
    let symbol = if let Some(id) = params.id {
        indexer.db().get_symbol_by_id(id)?
    } else if let Some(qualname) = params.qualname.as_deref() {
        indexer.db().get_symbol_by_qualname(qualname, graph_version)?
    } else if let Some(query) = params.query.as_deref() {
        // Find symbols matching the query, preferring production symbols over test symbols
        let mut results = indexer.db().find_symbols(query, 10, languages.as_deref(), graph_version)?;
        // Sort to deprioritize test symbols (put them at the end)
        results.sort_by_key(|s| is_test_symbol(s));
        results.into_iter().next()
    } else {
        return Err(anyhow::anyhow!("find_tests_for requires id, qualname, or query"));
    };

    let symbol = symbol.ok_or_else(|| {
        if let Some(query) = params.query.as_deref() {
            if let Ok(suggestions) = indexer.db().find_symbols(query, 5, languages.as_deref(), graph_version) {
                if !suggestions.is_empty() {
                    let names: Vec<String> = suggestions.into_iter().map(|s| s.qualname).collect();
                    return anyhow::anyhow!(
                        "Symbol '{}' not found. Did you mean: {}?",
                        query,
                        names.join(", ")
                    );
                }
            }
        }
        anyhow::anyhow!("symbol not found")
    })?;

    // For proto service symbols, find RPC_IMPL edges by service name in detail JSON
    let mut impl_symbols: Vec<Symbol> = Vec::new();
    if symbol.kind == "service" {
        // Extract service name (last segment of qualname, e.g., "TriggerService")
        let service_name = symbol.name.clone();

        // Find all RPC_IMPL edges and match by service name in detail field
        let rpc_impl_edges = indexer.db().list_edges(
            100, 0, languages.as_deref(), None,
            Some(&["RPC_IMPL".to_string()]), None, None, None,
            false, None, graph_version, None, None, None,
        )?;

        for edge in &rpc_impl_edges {
            if let Some(ref detail_str) = edge.detail {
                if let Ok(detail) = serde_json::from_str::<serde_json::Value>(detail_str) {
                    if detail.get("service").and_then(|v| v.as_str()) == Some(&service_name) {
                        if let Some(src_id) = edge.source_symbol_id {
                            if let Ok(Some(impl_sym)) = indexer.db().get_symbol_by_id(src_id) {
                                impl_symbols.push(impl_sym);
                            }
                        }
                    }
                }
            }
        }
    }

    // Build list of symbols to search tests for
    let search_symbols: Vec<&Symbol> = if impl_symbols.is_empty() {
        vec![&symbol]
    } else {
        impl_symbols.iter().collect()
    };

    // Find direct test callers and non-test callers across all search symbols
    let mut direct_tests = Vec::new();
    let mut non_test_callers = Vec::new();
    let mut test_files = HashSet::new();
    let mut seen_caller_ids = HashSet::new();

    for search_symbol in &search_symbols {
        // Get all incoming CALLS edges for the search symbol (resolved edges)
        let edges = indexer.db().edges_for_symbol(
            search_symbol.id,
            languages.as_deref(),
            graph_version
        )?;

        // Also get unresolved edges by qualname pattern
        let unresolved_edges = indexer.db().incoming_edges_by_qualname_pattern(
            &search_symbol.name,
            "CALLS",
            languages.as_deref(),
            graph_version
        )?;

        // Process resolved edges
        for edge in &edges {
            if edge.kind == "CALLS" && edge.target_symbol_id == Some(search_symbol.id) {
                if let Some(source_id) = edge.source_symbol_id {
                    if seen_caller_ids.insert(source_id) {
                        if let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id) {
                            let is_test = is_test_symbol(&caller);
                            if is_test {
                                test_files.insert(caller.file_path.clone());
                                direct_tests.push(TestMatch {
                                    test_symbol: caller.into(),
                                    match_type: "direct".to_string(),
                                    via_symbol: None,
                                    relevance: 1.0,
                                });
                            } else {
                                non_test_callers.push(caller);
                            }
                        }
                    }
                }
            }
        }

        // Process unresolved edges (target_qualname matches)
        for edge in &unresolved_edges {
            if let Some(target_qn) = &edge.target_qualname {
                let matches = target_qn == &search_symbol.qualname
                    || target_qn.ends_with(&format!(".{}", search_symbol.name));

                if matches {
                    if let Some(source_id) = edge.source_symbol_id {
                        if seen_caller_ids.insert(source_id) {
                            if let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id) {
                                let is_test = is_test_symbol(&caller);
                                if is_test {
                                    test_files.insert(caller.file_path.clone());
                                    direct_tests.push(TestMatch {
                                        test_symbol: caller.into(),
                                        match_type: "direct".to_string(),
                                        via_symbol: None,
                                        relevance: 0.9,
                                    });
                                } else {
                                    non_test_callers.push(caller);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Find indirect test callers via multi-level BFS (depth controlled by indirect_depth)
    let mut indirect_tests = Vec::new();
    if include_indirect {
        let mut current_callers = non_test_callers.clone();
        let mut all_seen_ids: HashSet<i64> = seen_caller_ids.clone();
        let mut base_relevance = 0.7;

        for _level in 0..indirect_depth {
            let mut next_level_callers = Vec::new();

            for caller in &current_callers {
                // Get resolved edges for this caller
                let caller_edges = indexer.db().edges_for_symbol(
                    caller.id,
                    languages.as_deref(),
                    graph_version
                )?;

                // Get unresolved edges for this caller
                let unresolved_caller_edges = indexer.db().incoming_edges_by_qualname_pattern(
                    &caller.name,
                    "CALLS",
                    languages.as_deref(),
                    graph_version
                )?;

                // Process resolved edges
                for edge in &caller_edges {
                    if edge.kind == "CALLS" && edge.target_symbol_id == Some(caller.id) {
                        if let Some(source_id) = edge.source_symbol_id {
                            if all_seen_ids.insert(source_id) {
                                if let Ok(Some(upstream)) = indexer.db().get_symbol_by_id(source_id) {
                                    if is_test_symbol(&upstream) {
                                        test_files.insert(upstream.file_path.clone());
                                        indirect_tests.push(TestMatch {
                                            test_symbol: upstream.into(),
                                            match_type: "indirect".to_string(),
                                            via_symbol: Some(caller.clone().into()),
                                            relevance: base_relevance,
                                        });
                                    } else {
                                        next_level_callers.push(upstream);
                                    }
                                }
                            }
                        }
                    }
                }

                // Process unresolved edges
                for edge in &unresolved_caller_edges {
                    if let Some(target_qn) = &edge.target_qualname {
                        let matches = target_qn == &caller.qualname
                            || target_qn.ends_with(&format!(".{}", caller.name));

                        if matches {
                            if let Some(source_id) = edge.source_symbol_id {
                                if all_seen_ids.insert(source_id) {
                                    if let Ok(Some(upstream)) = indexer.db().get_symbol_by_id(source_id) {
                                        if is_test_symbol(&upstream) {
                                            test_files.insert(upstream.file_path.clone());
                                            indirect_tests.push(TestMatch {
                                                test_symbol: upstream.into(),
                                                match_type: "indirect".to_string(),
                                                via_symbol: Some(caller.clone().into()),
                                                relevance: base_relevance * 0.9,
                                            });
                                        } else {
                                            next_level_callers.push(upstream);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if next_level_callers.is_empty() { break; }
            current_callers = next_level_callers;
            base_relevance *= 0.7; // Decay relevance per level
        }
    }

    // Truncate to limit
    direct_tests.truncate(limit);
    indirect_tests.truncate(limit);

    let summary = TestSummary {
        direct_count: direct_tests.len(),
        indirect_count: indirect_tests.len(),
        test_files: test_files.into_iter().collect(),
    };

    let next_hops = vec![
        json!({"method": "explain_symbol", "params": {"id": symbol.id}, "description": "Full symbol explanation"}),
        json!({"method": "analyze_impact", "params": {"id": symbol.id}, "description": "Impact analysis"}),
    ];

    let result = FindTestsResult {
        symbol: symbol.into(),
        direct_tests,
        indirect_tests,
        summary,
        next_hops,
    };

    Ok(serde_json::to_value(&result)?)
}

pub(super) fn handle_analyze_impact(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: AnalyzeImpactParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;

    // Resolve symbol by id or qualname
    let symbol = if let Some(id) = params.id {
        indexer.db().get_symbol_by_id(id)?
    } else if let Some(qualname) = params.qualname.as_deref() {
        indexer
            .db()
            .get_symbol_by_qualname(qualname, graph_version)?
    } else {
        return Err(anyhow::anyhow!("analyze_impact requires id or qualname"));
    };

    let symbol = symbol.ok_or_else(|| {
        if let Some(qualname) = params.qualname.as_deref() {
            if let Ok(suggestions) =
                indexer.db().find_symbols(qualname, 10, None, graph_version)
            {
                if !suggestions.is_empty() {
                    let names: Vec<String> =
                        suggestions.into_iter().map(|s| s.qualname).collect();
                    return anyhow::anyhow!(
                        "Symbol '{}' not found. Did you mean: {}?",
                        qualname,
                        names.join(", ")
                    );
                }
            }
        }
        anyhow::anyhow!("symbol not found")
    })?;

    // Build multi-layer configuration
    let config = crate::impact::config::MultiLayerConfig::builder()
        .max_depth(params.max_depth.unwrap_or(3).min(10))
        .direction(params.direction.unwrap_or_else(|| "both".to_string()))
        .include_tests(params.include_tests.unwrap_or(false))
        .include_paths(params.include_paths.unwrap_or(true))
        .limit(params.limit.unwrap_or(500).min(2000))
        .min_confidence(params.min_confidence.unwrap_or(0.0))
        .build();

    // Apply layer enable/disable overrides if specified
    // If not specified, use config defaults (which are now enabled by default)
    let mut config = config;
    if let Some(enable_direct) = params.enable_direct {
        config.direct.enabled = enable_direct;
    }
    if let Some(enable_test) = params.enable_test {
        config.test.enabled = enable_test;
    }
    if let Some(enable_historical) = params.enable_historical {
        config.historical.enabled = enable_historical;
    }

    // Set languages if specified
    if let Some(languages) = params.languages.as_ref() {
        let normalized = scan::normalize_language_filter(Some(languages.as_slice()))?;
        config.direct.languages = normalized;
    }

    // Set kinds if specified
    if let Some(kinds) = params.kinds {
        config.direct.kinds = kinds;
    }

    // Perform multi-layer impact analysis
    let result = crate::impact::analyze_impact_multi_layer(
        indexer.db(),
        &[symbol.id],
        config,
        graph_version,
    )?;

    Ok(json!(result))
}

pub(super) fn handle_analyze_diff(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: AnalyzeDiffParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let max_bytes = params.max_bytes.unwrap_or(50_000).min(200_000);
    let max_depth = params.max_depth.unwrap_or(1).min(5);
    let include_tests = params.include_tests.unwrap_or(true);
    let include_risk = params.include_risk.unwrap_or(true);
    let languages = params.languages.clone();

    // Step 1: Get changed files with optional line ranges
    let mut warnings: Vec<String> = Vec::new();
    let changed_files: Vec<ChangedFile> = if let Some(ref diff) = params.diff {
        parse_diff_with_ranges(diff)
    } else if let Some(ref paths) = params.paths {
        paths.iter().map(|p| ChangedFile {
            path: p.clone(),
            changed_ranges: Vec::new(),
            added_ranges: Vec::new(),
            deleted_ranges: Vec::new(),
        }).collect()
    } else {
        anyhow::bail!("analyze_diff requires 'diff' or 'paths' parameter");
    };

    if changed_files.is_empty() {
        anyhow::bail!("No changed files found");
    }

    // Step 2: Find symbols in changed files, filtered by hunk ranges
    let mut changed_symbols = Vec::new();
    for cf in &changed_files {
        let symbols = indexer.db().get_symbols_for_file(&cf.path, graph_version).unwrap_or_default();
        if symbols.is_empty() {
            warnings.push(format!("Path not found in index: {}", cf.path));
            continue;
        }
        let has_ranges = !cf.changed_ranges.is_empty();
        for sym in symbols {
            let change_type = if has_ranges {
                // Check if symbol overlaps any changed hunk
                let overlaps = cf.changed_ranges.iter().any(|h| {
                    let hunk_end = h.start_line + h.line_count - 1;
                    sym.start_line <= hunk_end && sym.end_line >= h.start_line
                });
                if !overlaps { continue; }
                // Determine change type: if symbol is fully within added range, it's "added"
                let fully_added = cf.added_ranges.iter().any(|h| {
                    let hunk_end = h.start_line + h.line_count - 1;
                    sym.start_line >= h.start_line && sym.end_line <= hunk_end
                });
                if fully_added { "added".to_string() } else { "modified".to_string() }
            } else {
                "modified".to_string()
            };

            // Step 2a: Detect signature changes by comparing with previous graph version
            let mut old_signature = None;
            let new_signature = sym.signature.clone();
            let mut final_change_type = change_type.clone();

            if change_type == "modified" && sym.stable_id.is_some() && graph_version > 1 {
                // Try to find the symbol in the previous graph version
                if let Ok(Some(old_sym)) = indexer.db().get_symbol_by_stable_id(
                    sym.stable_id.as_ref().unwrap(),
                    graph_version - 1,
                ) {
                    // Compare signatures
                    if old_sym.signature != sym.signature {
                        final_change_type = "signature_changed".to_string();
                        old_signature = old_sym.signature;
                    }
                }
            }

            changed_symbols.push(ChangedSymbol {
                symbol: sym,
                change_type: final_change_type,
                old_signature,
                new_signature,
            });
        }
    }

    // Step 3: Compute downstream impact via multi-level BFS (depth controlled by max_depth)
    let seed_ids: Vec<i64> = changed_symbols.iter().map(|cs| cs.symbol.id).collect();
    let mut downstream = Vec::new();
    let mut seen_ids: HashSet<i64> = seed_ids.iter().copied().collect();
    let max_downstream = 50;

    // BFS: start with changed symbols, expand callers level by level
    let mut current_level: Vec<Symbol> = changed_symbols.iter().map(|cs| cs.symbol.clone()).collect();
    let mut current_distance = 1usize;
    let mut base_confidence = 0.9;

    for _depth in 0..max_depth {
        let mut next_level = Vec::new();

        for sym in &current_level {
            if downstream.len() >= max_downstream { break; }

            let edges = indexer.db().edges_for_symbol(
                sym.id, languages.as_deref(), graph_version
            )?;

            // Find callers via resolved edges
            for edge in &edges {
                if downstream.len() >= max_downstream { break; }
                if edge.kind == "CALLS" && edge.target_symbol_id == Some(sym.id) {
                    if let Some(source_id) = edge.source_symbol_id {
                        if seen_ids.insert(source_id) {
                            if let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id) {
                                next_level.push(caller.clone());
                                downstream.push(DiffImpactEntry {
                                    symbol: caller,
                                    relationship: if current_distance == 1 { "caller".to_string() } else { format!("caller_depth_{}", current_distance) },
                                    distance: current_distance,
                                    confidence: base_confidence,
                                });
                            }
                        }
                    }
                }
            }

            // Qualname fallback for unresolved incoming edges
            if downstream.len() < max_downstream {
                let unresolved = indexer.db().incoming_edges_by_qualname_pattern(
                    &sym.name, "CALLS", languages.as_deref(), graph_version
                ).unwrap_or_default();
                for edge in &unresolved {
                    if downstream.len() >= max_downstream { break; }
                    if let Some(source_id) = edge.source_symbol_id {
                        if seen_ids.insert(source_id) {
                            if let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id) {
                                next_level.push(caller.clone());
                                downstream.push(DiffImpactEntry {
                                    symbol: caller,
                                    relationship: if current_distance == 1 { "caller".to_string() } else { format!("caller_depth_{}", current_distance) },
                                    distance: current_distance,
                                    confidence: base_confidence * 0.8,
                                });
                            }
                        }
                    }
                }
            }
        }

        if next_level.is_empty() || downstream.len() >= max_downstream { break; }
        current_level = next_level;
        current_distance += 1;
        base_confidence *= 0.8; // Decay confidence per level
    }

    // Step 4: Test coverage (with qualname fallback)
    let test_coverage = if include_tests {
        let mut coverage = Vec::new();
        for cs in &changed_symbols {
            let mut tests = Vec::new();
            let mut seen_test_ids = HashSet::new();
            // Check resolved edges
            let edges = indexer.db().edges_for_symbol(
                cs.symbol.id, languages.as_deref(), graph_version
            )?;
            for edge in &edges {
                if edge.kind == "CALLS" && edge.target_symbol_id == Some(cs.symbol.id) {
                    if let Some(source_id) = edge.source_symbol_id {
                        if let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id) {
                            if is_test_symbol(&caller) && seen_test_ids.insert(source_id) {
                                tests.push(TestRef {
                                    test_qualname: caller.qualname.clone(),
                                    test_file: caller.file_path.clone(),
                                    coverage_type: "direct".to_string(),
                                });
                            }
                        }
                    }
                }
            }
            // Qualname fallback for unresolved edges
            let unresolved = indexer.db().incoming_edges_by_qualname_pattern(
                &cs.symbol.name, "CALLS", languages.as_deref(), graph_version
            ).unwrap_or_default();
            for edge in &unresolved {
                if let Some(source_id) = edge.source_symbol_id {
                    if let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id) {
                        if is_test_symbol(&caller) && seen_test_ids.insert(source_id) {
                            tests.push(TestRef {
                                test_qualname: caller.qualname.clone(),
                                test_file: caller.file_path.clone(),
                                coverage_type: "direct".to_string(),
                            });
                        }
                    }
                }
            }
            let status = if tests.is_empty() { "uncovered" } else { "covered" };
            coverage.push(TestCoverageEntry {
                symbol_qualname: cs.symbol.qualname.clone(),
                tests,
                status: status.to_string(),
            });
        }
        Some(coverage)
    } else { None };

    // Step 5: Enhanced risk assessment with review checklist
    let risk = if include_risk {
        let mut factors = Vec::new();
        let mut focus_areas = Vec::new();
        let mut review_checklist = Vec::new();

        // 1. Signature change + high fan-in = CRITICAL risk
        for cs in &changed_symbols {
            if cs.change_type == "signature_changed" {
                let caller_count = downstream.iter()
                    .filter(|d| d.relationship.starts_with("caller"))
                    .count();

                if caller_count > 10 {
                    factors.push(RiskFactor {
                        factor: "Signature changed on high-traffic symbol".to_string(),
                        description: format!(
                            "Signature changed on {} with {} callers",
                            cs.symbol.qualname, caller_count
                        ),
                        severity: "critical".to_string(),
                    });
                    review_checklist.push(format!(
                        "Verify all {} callers of {} handle the new signature: {} → {}",
                        caller_count,
                        cs.symbol.qualname,
                        cs.old_signature.as_deref().unwrap_or("(none)"),
                        cs.new_signature.as_deref().unwrap_or("(none)")
                    ));
                } else if caller_count > 0 {
                    factors.push(RiskFactor {
                        factor: "Signature change".to_string(),
                        description: format!(
                            "Signature changed on {} with {} callers",
                            cs.symbol.qualname, caller_count
                        ),
                        severity: "high".to_string(),
                    });
                    review_checklist.push(format!(
                        "Review callers of {} for signature compatibility",
                        cs.symbol.qualname
                    ));
                }
            }
        }

        // 2. Cross-language callers = HIGH risk
        let mut cross_lang_callers: Vec<String> = Vec::new();
        for impact in &downstream {
            let changed_langs: HashSet<_> = changed_symbols.iter()
                .map(|cs| infer_language(&cs.symbol.file_path))
                .collect();
            let caller_lang = infer_language(&impact.symbol.file_path);
            if !changed_langs.contains(&caller_lang) {
                cross_lang_callers.push(format!(
                    "{}:{} ({})",
                    impact.symbol.file_path, impact.symbol.name, caller_lang
                ));
            }
        }
        if !cross_lang_callers.is_empty() {
            factors.push(RiskFactor {
                factor: "Cross-language impact".to_string(),
                description: format!("{} cross-language callers affected", cross_lang_callers.len()),
                severity: "high".to_string(),
            });
            for caller in cross_lang_callers.iter().take(3) {
                review_checklist.push(format!(
                    "Test cross-language caller: {}",
                    caller
                ));
            }
        }

        // 3. Interface/trait changes = HIGH risk
        for cs in &changed_symbols {
            if matches!(cs.symbol.kind.as_str(), "interface" | "trait" | "abstract_class") {
                factors.push(RiskFactor {
                    factor: "Interface/contract change".to_string(),
                    description: format!(
                        "{} {} changed",
                        cs.symbol.kind, cs.symbol.qualname
                    ),
                    severity: "high".to_string(),
                });
                review_checklist.push(format!(
                    "Review all implementers of {} {}",
                    cs.symbol.kind, cs.symbol.qualname
                ));
            }
        }

        // 4. High fan-in = HIGH risk
        let high_fan_in: Vec<_> = downstream.iter()
            .filter(|d| d.relationship.starts_with("caller"))
            .collect();
        if high_fan_in.len() > 10 {
            factors.push(RiskFactor {
                factor: "High fan-in".to_string(),
                description: format!("{} callers affected", high_fan_in.len()),
                severity: "high".to_string(),
            });
            let caller_files: HashSet<_> = high_fan_in.iter()
                .map(|d| d.symbol.file_path.as_str())
                .collect();
            if caller_files.len() <= 5 {
                for file in caller_files {
                    review_checklist.push(format!("Review callers in {}", file));
                }
            }
        }

        // 5. Wide blast radius = MEDIUM risk
        let affected_files: HashSet<_> = downstream.iter()
            .map(|d| d.symbol.file_path.as_str())
            .collect();
        if affected_files.len() > 3 {
            factors.push(RiskFactor {
                factor: "Wide blast radius".to_string(),
                description: format!("{} files affected", affected_files.len()),
                severity: "medium".to_string(),
            });
            focus_areas.extend(affected_files.iter().map(|f| f.to_string()));
        }

        // 6. Missing test coverage = MEDIUM risk
        if let Some(ref cov) = test_coverage {
            let uncovered: Vec<_> = cov.iter().filter(|c| c.status == "uncovered").collect();
            if !uncovered.is_empty() {
                factors.push(RiskFactor {
                    factor: "Missing test coverage".to_string(),
                    description: format!("{} symbols without tests", uncovered.len()),
                    severity: "medium".to_string(),
                });
                for entry in uncovered.iter().take(5) {
                    review_checklist.push(format!(
                        "Add tests for {} (currently uncovered)",
                        entry.symbol_qualname
                    ));
                }
            }
        }

        // Compute overall risk level
        let level = if factors.iter().any(|f| f.severity == "critical") {
            "critical"
        } else if factors.iter().any(|f| f.severity == "high") {
            "high"
        } else if factors.iter().any(|f| f.severity == "medium") {
            "medium"
        } else {
            "low"
        };

        Some(RiskAssessment {
            level: level.to_string(),
            factors,
            focus_areas,
            review_checklist,
        })
    } else { None };

    let mut used_bytes = 0;
    let result_json = serde_json::to_value(&changed_symbols)?;
    used_bytes += serde_json::to_string(&result_json).unwrap_or_default().len();

    let mut next_hops: Vec<Value> = Vec::new();
    // Add explain_symbol for first changed symbol
    if let Some(cs) = changed_symbols.first() {
        next_hops.push(json!({"method": "explain_symbol", "params": {"id": cs.symbol.id}, "description": format!("Explain {}", cs.symbol.name)}));
    }
    // Add references for top changed symbol
    if let Some(cs) = changed_symbols.iter().find(|cs| cs.symbol.kind == "method" || cs.symbol.kind == "function") {
        next_hops.push(json!({"method": "references", "params": {"id": cs.symbol.id, "direction": "in"}, "description": format!("Callers of {}", cs.symbol.name)}));
    }
    // Add subgraph for exploration
    if let Some(cs) = changed_symbols.first() {
        next_hops.push(json!({"method": "subgraph", "params": {"start_ids": [cs.symbol.id], "depth": 2}, "description": "Explore impact graph"}));
    }

    let result = AnalyzeDiffResult {
        changed_symbols,
        downstream,
        test_coverage,
        risk,
        budget: BudgetInfo {
            budget_bytes: max_bytes,
            used_bytes,
            truncated: false,
        },
        next_hops,
        warnings,
    };

    Ok(serde_json::to_value(&result)?)
}

// ---------------------------------------------------------------------------
// GROUP 5 -- Search handlers
// ---------------------------------------------------------------------------

pub(super) fn handle_search_rg(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: RgParams = serde_json::from_value(params)?;
    validate_pattern_length(&params.query, "search_rg")?;
    let limit = params.limit.unwrap_or(100).min(MAX_RESPONSE_LIMIT);
    let context_lines = normalize_rg_context_lines(params.context_lines);
    let include_text = params.include_text.unwrap_or(true);
    let include_symbol = params.include_symbol.unwrap_or(false);
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let paths = resolve_rg_paths(indexer.repo_root(), params.path, params.paths)?;
    let globs = params.globs.unwrap_or_default();
    let options = RgSearchOptions {
        include_text,
        case_sensitive: params.case_sensitive,
        fixed_string: params.fixed_string.unwrap_or(false),
        hidden: params.hidden.unwrap_or(false),
        no_ignore: params.no_ignore.unwrap_or(false),
        follow: params.follow.unwrap_or(false),
        globs,
        paths,
    };
    let mut results = search_rg(indexer.repo_root(), &params.query, limit, options)?;
    for hit in &mut results {
        if hit.engine.is_none() {
            hit.engine = Some("search_rg".to_string());
        }
    }
    annotate_grep_hits(
        indexer,
        &mut results,
        context_lines,
        include_symbol,
        graph_version,
        Some(&params.query),
    )?;
    Ok(json!(results))
}

pub(super) fn handle_search_text(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: SearchParams = serde_json::from_value(params)?;
    validate_pattern_length(&params.query, "search_text")?;
    let limit = params.limit.unwrap_or(50).min(MAX_RESPONSE_LIMIT);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let scope = search::parse_scope(params.scope.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
    let options = search::SearchOptions {
        languages: languages.as_deref(),
        scope,
        exclude_generated: params.exclude_generated.unwrap_or(false),
        rank: params.rank.unwrap_or(true),
        no_ignore: params.no_ignore.unwrap_or(false),
        paths: paths.as_deref(),
    };
    let mut results =
        search::search_text(indexer.repo_root(), &params.query, limit, options)?;
    for hit in &mut results {
        hit.engine = Some("search_text".to_string());
    }
    let context_lines = normalize_context_lines(params.context_lines, 2);
    let include_symbol = params.include_symbol.unwrap_or(true);
    annotate_search_hits(
        indexer,
        &mut results,
        context_lines,
        include_symbol,
        graph_version,
        Some(&params.query),
    )?;
    // Add capped metadata to response
    let capped = results.len() >= limit;
    Ok(json!({
        "results": results,
        "capped": capped,
        "total_returned": results.len(),
    }))
}

pub(super) fn handle_grep(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: GrepParams = serde_json::from_value(params)?;
    validate_pattern_length(&params.query, "grep")?;
    let limit = params.limit.unwrap_or(50).min(MAX_RESPONSE_LIMIT);
    let include_text = params.include_text.unwrap_or(false);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let scope = search::parse_scope(params.scope.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
    let options = search::SearchOptions {
        languages: languages.as_deref(),
        scope,
        exclude_generated: params.exclude_generated.unwrap_or(false),
        rank: params.rank.unwrap_or(true),
        no_ignore: params.no_ignore.unwrap_or(false),
        paths: paths.as_deref(),
    };
    let mut results = search::grep_text(
        indexer.repo_root(),
        &params.query,
        limit,
        include_text,
        options,
    )?;
    for hit in &mut results {
        hit.engine = Some("grep".to_string());
    }
    let context_lines = normalize_context_lines(params.context_lines, 0);
    let include_symbol = params.include_symbol.unwrap_or(false);
    annotate_grep_hits(
        indexer,
        &mut results,
        context_lines,
        include_symbol,
        graph_version,
        Some(&params.query),
    )?;
    // Add capped metadata to response
    let capped = results.len() >= limit;
    Ok(json!({
        "results": results,
        "capped": capped,
        "total_returned": results.len(),
    }))
}

// ---------------------------------------------------------------------------
// GROUP 6 -- Index/meta handlers
// ---------------------------------------------------------------------------

pub(super) fn handle_index_status(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: IndexStatusParams = serde_json::from_value(params)?;
    let include_paths = params.include_paths.unwrap_or(false);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let changed = indexer.changed_files(languages.as_deref())?;
    let counts = IndexChangeCounts {
        added: changed.added.len(),
        modified: changed.modified.len(),
        deleted: changed.deleted.len(),
    };
    let stale = counts.added > 0 || counts.modified > 0 || counts.deleted > 0;
    let last_indexed = indexer.db().get_meta_i64("last_indexed")?;
    let hint = if last_indexed.is_none() {
        "index missing; run reindex".to_string()
    } else if stale {
        "reindex needed".to_string()
    } else {
        "index current".to_string()
    };
    let commit_sha = indexer.db().graph_version_commit(indexer.graph_version())?;
    let status = IndexStatus {
        repo_root: indexer.repo_root().to_string_lossy().to_string(),
        last_indexed,
        graph_version: Some(indexer.graph_version()),
        commit_sha,
        stale,
        hint,
        counts,
        changed_files: if include_paths { Some(changed) } else { None },
    };
    Ok(json!(status))
}

pub(super) fn handle_reindex(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: ReindexParams = serde_json::from_value(params)?;
    let stats = indexer.reindex()?;

    // Optionally resolve unresolved edges after reindexing
    let mut json_stats = json!(stats);
    if params.resolve_edges.unwrap_or(false) {
        let graph_version = indexer.db().current_graph_version()?;
        let resolved = indexer.db().resolve_null_target_edges(graph_version)?;
        // Add resolved count to stats
        if let Some(obj) = json_stats.as_object_mut() {
            obj.insert("edges_resolved".to_string(), json!(resolved));
        }
    }

    // Optionally mine git co-changes after reindexing
    if params.mine_git.unwrap_or(false) {
        use crate::git_mining;

        eprintln!("lidx: Mining git co-changes...");
        let max_commits = 1000;
        let since_days = 180;

        match git_mining::mine_co_changes(indexer.repo_root(), max_commits, since_days) {
            Ok(entries) => {
                let count = entries.len();
                match indexer.db_mut().insert_co_changes_batch(&entries) {
                    Ok(inserted) => {
                        eprintln!("lidx: Inserted {} co-change patterns", inserted);
                        if let Some(obj) = json_stats.as_object_mut() {
                            obj.insert("co_changes_mined".to_string(), json!(count));
                            obj.insert("co_changes_inserted".to_string(), json!(inserted));
                        }
                    }
                    Err(e) => {
                        eprintln!("lidx: Warning: Failed to insert co-changes: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("lidx: Warning: Git mining failed: {}", e);
            }
        }
    }

    Ok(apply_field_filters(
        json_stats,
        params.summary.unwrap_or(false),
        params.fields.as_deref(),
        &["scanned", "indexed", "skipped", "deleted"],
    ))
}

pub(super) fn handle_gather_context(indexer: &mut Indexer, params: Value) -> Result<Value> {
    use crate::gather_context;

    const MAX_SEEDS: usize = 100;
    const MAX_BYTES_HARD_CAP: usize = 2_000_000; // 2MB

    let params: GatherContextParams = serde_json::from_value(params)?;

    // Validate parameters
    let validation = validate_gather_context_params(&params);
    if !validation.is_valid() {
        return Err(anyhow::anyhow!(
            "Validation failed: {}",
            serde_json::to_string(&validation.errors)?
        ));
    }

    // Moderate Concern #3: Validate seed count
    if params.seeds.len() > MAX_SEEDS {
        anyhow::bail!(
            "Too many seeds: {} (max: {})",
            params.seeds.len(),
            MAX_SEEDS
        );
    }

    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;

    // Moderate Concern #1: Enforce hard cap on max_bytes
    let max_bytes = params.max_bytes.unwrap_or(100_000).min(MAX_BYTES_HARD_CAP);

    // Determine strategy: default to "symbol" if all seeds are symbol seeds
    let strategy = params.strategy.or_else(|| {
        let all_symbol_seeds = params.seeds.iter().all(|seed| {
            matches!(seed, ContextSeed::Symbol { .. })
        });
        if all_symbol_seeds && !params.seeds.is_empty() {
            Some("symbol".to_string())
        } else {
            Some("file".to_string())
        }
    });

    let config = gather_context::GatherConfig {
        max_bytes,
        depth: params.depth.unwrap_or(2),
        max_nodes: params.max_nodes.unwrap_or(50),
        include_snippets: params.include_snippets.unwrap_or(true),
        include_related: params.include_related.unwrap_or(true),
        dry_run: params.dry_run.unwrap_or(false),
        languages,
        paths,
        graph_version,
        semantic_results: HashMap::new(),
        strategy,
    };

    let result = gather_context::gather_context(
        indexer.db(),
        indexer.repo_root(),
        &params.seeds,
        &config,
    )?;

    Ok(json!(result))
}

pub(super) fn handle_onboard(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: OnboardParams = serde_json::from_value(params)?;
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;

    // 1. Repo overview (compact)
    let overview = indexer.db().repo_overview(
        indexer.repo_root().clone(),
        languages.as_deref(),
        graph_version,
    )?;

    // 2. Module summary (depth=1)
    let modules = indexer.db().module_summary(
        1,
        languages.as_deref(),
        None,
        graph_version,
    )?;
    let module_nodes: Vec<Value> = modules
        .into_iter()
        .map(|m| json!({
            "path": m.path,
            "file_count": m.file_count,
            "symbol_count": m.symbol_count,
            "languages": m.languages,
        }))
        .collect();

    // 3. Languages
    let lang_list: Vec<String> = scan::language_specs()
        .iter()
        .map(|s| s.name.to_string())
        .collect();

    // 4. Index status
    let changed = indexer.changed_files(languages.as_deref())?;
    let stale = !changed.added.is_empty()
        || !changed.modified.is_empty()
        || !changed.deleted.is_empty();
    let last_indexed = indexer.db().get_meta_i64("last_indexed")?;
    let hint = if last_indexed.is_none() {
        "index missing; run reindex"
    } else if stale {
        "reindex needed"
    } else {
        "index current"
    };

    // 5. Suggested queries
    let suggested = json!([
        { "method": "explain_symbol", "params": { "query": "<symbol_name>" }, "why": "Understand any symbol deeply" },
        { "method": "repo_map", "params": { "max_bytes": 8000 }, "why": "Get architecture text overview" },
        { "method": "search", "params": { "query": "<topic>" }, "why": "Search by concept" },
        { "method": "analyze_diff", "params": { "paths": ["<file>"] }, "why": "Assess change impact" },
    ]);

    Ok(json!({
        "overview": overview,
        "languages": lang_list,
        "modules": module_nodes,
        "index_status": { "stale": stale, "hint": hint },
        "suggested_queries": suggested,
    }))
}

pub(super) fn handle_changed_since(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: ChangedSinceParams = serde_json::from_value(params)?;
    let repo_root = indexer.repo_root();

    // Determine the base commit
    let since_commit = match params.commit {
        Some(ref c) => {
            // Validate: allow alphanumeric, ~, ^, -, . (for HEAD~5, tags, etc)
            if c.len() > 100 || !c.chars().all(|ch| ch.is_alphanumeric() || "~^-._/".contains(ch)) {
                anyhow::bail!("invalid commit reference: must be alphanumeric with ~^-._/ allowed, max 100 chars");
            }
            c.clone()
        }
        None => {
            // Use the commit from the current graph version
            let gv = indexer.db().current_graph_version()?;
            match indexer.db().graph_version_commit(gv)? {
                Some(sha) => sha,
                None => anyhow::bail!("no commit parameter provided and no commit recorded in current graph version"),
            }
        }
    };

    // Get current HEAD
    let current_commit = crate::util::git_head_sha(repo_root)
        .ok_or_else(|| anyhow::anyhow!("failed to get git HEAD sha — is this a git repository?"))?;

    // Run git diff
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("diff")
        .arg("--name-only")
        .arg(format!("{}..HEAD", since_commit))
        .output()
        .map_err(|e| anyhow::anyhow!("failed to run git diff: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("git diff failed: {stderr}");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let files: Vec<&str> = stdout.lines().filter(|l| !l.is_empty()).collect();
    let file_count = files.len();

    // Build suggested queries
    let paths_for_diff: Vec<&str> = files.iter().take(20).copied().collect();
    let suggested = if file_count > 0 {
        json!([{
            "method": "analyze_diff",
            "params": { "paths": paths_for_diff },
            "why": "Analyze impact of these changes"
        }])
    } else {
        json!([])
    };

    Ok(json!({
        "since_commit": since_commit,
        "current_commit": current_commit,
        "files_changed": files,
        "file_count": file_count,
        "suggested_queries": suggested,
    }))
}

// ---------------------------------------------------------------------------
// GROUP 7 -- Diagnostics
// ---------------------------------------------------------------------------

pub(super) fn handle_diagnostics_import(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: DiagnosticsImportParams = serde_json::from_value(params)?;
    // Security fix: Use resolve_repo_path_for_op to validate path
    let (abs, _rel) =
        resolve_repo_path_for_op(indexer.repo_root(), &params.path, "diagnostics_import")?;
    let content = util::read_to_string(&abs)
        .with_context(|| format!("read diagnostics {}", abs.display()))?;
    let diagnostics = diagnostics::parse_sarif(&content, indexer.repo_root())?;
    let imported = indexer.db_mut().insert_diagnostics(&diagnostics)?;
    Ok(json!({ "imported": imported }))
}

pub(super) fn handle_diagnostics_list(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: DiagnosticsListParams = serde_json::from_value(params)?;
    let limit = params.limit.unwrap_or(100).min(MAX_RESPONSE_LIMIT);
    let offset = params.offset.unwrap_or(0);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
    let diagnostics = indexer.db().list_diagnostics(
        limit,
        offset,
        languages.as_deref(),
        paths.as_deref(),
        params.severity.as_ref(),
        params.rule_id.as_ref(),
        params.tool.as_ref(),
    )?;
    Ok(json!(diagnostics))
}

pub(super) fn handle_diagnostics_summary(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: DiagnosticsSummaryParams = serde_json::from_value(params)?;
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
    let summary = indexer.db().diagnostics_summary(
        languages.as_deref(),
        paths.as_deref(),
        params.severity.as_ref(),
        params.rule_id.as_ref(),
        params.tool.as_ref(),
    )?;
    Ok(json!(summary))
}
