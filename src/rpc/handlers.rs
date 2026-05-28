//! Extracted handler functions for RPC methods.
//! Each function corresponds to a match arm in `handle_method`.

use super::*;
use crate::search::{
    RgSearchOptions, annotate_grep_hits, normalize_rg_context, resolve_rg_paths, search_rg,
};

// ---------------------------------------------------------------------------
// GROUP 1 -- Symbol query handlers
// ---------------------------------------------------------------------------

pub(super) fn handle_explain_symbol(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: ExplainSymbolParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let languages = params.languages.clone();

    let max_bytes = params.max_bytes.unwrap_or(40_000).min(200_000);
    let max_refs = params.max_refs.unwrap_or(10);

    // Normalize sections: resolve aliases and warn on unknowns
    let known_sections: &[&str] = &["source", "callers", "callees", "tests", "implements"];
    let aliases: &[(&str, &str)] = &[
        ("dependencies", "callees"),
        ("dependents", "callers"),
        ("summary", "source"),
        ("body", "source"),
    ];
    let raw_sections = params.sections.clone().unwrap_or_else(|| {
        vec![
            "source".into(),
            "callers".into(),
            "callees".into(),
            "tests".into(),
            "implements".into(),
        ]
    });
    let mut warnings: Vec<String> = Vec::new();
    let sections: Vec<String> = raw_sections.iter().map(|s| {
        let lower = s.to_lowercase();
        for (alias, canonical) in aliases {
            if lower == *alias {
                return canonical.to_string();
            }
        }
        if !known_sections.contains(&lower.as_str()) {
            warnings.push(format!(
                "Unknown section '{}'. Valid: source, callers, callees, tests, implements (aliases: dependencies\u{2192}callees, dependents\u{2192}callers, summary/body\u{2192}source)",
                s
            ));
        }
        lower
    }).collect();

    // 1. Resolve symbol
    let sym_ref = if let Some(id) = params.id {
        crate::resolve::SymbolRef::Id(id)
    } else if let Some(ref qn) = params.qualname {
        crate::resolve::SymbolRef::Qualname(qn.clone())
    } else if let Some(ref query) = params.query {
        crate::resolve::SymbolRef::Query(query.clone())
    } else {
        anyhow::bail!("explain_symbol requires id, qualname, or query");
    };
    let symbol =
        crate::resolve::resolve_symbol(indexer.db(), sym_ref, languages.as_deref(), graph_version)?;

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
                let truncate_pos = snippet[..source_budget]
                    .rfind('\n')
                    .unwrap_or(source_budget);
                snippet[..truncate_pos].to_string()
            } else {
                snippet
            };
            used_bytes += snippet.len();
            Some(snippet)
        } else {
            None
        }
    } else {
        None
    };

    // 4. Get edges for callers/callees
    let edges = indexer
        .db()
        .edges_for_symbol(symbol.id, languages.as_deref(), graph_version)?;

    // 5. Build callers (incoming CALLS)
    let mut callers = if sections.contains(&"callers".to_string()) {
        let mut caller_refs = Vec::new();
        let mut caller_bytes = 0usize;
        let mut seen_caller_ids = std::collections::HashSet::new();

        // Determine which symbol IDs to collect callers for
        let is_class_symbol = symbol.kind == "class";
        let target_ids: Vec<(i64, String)> = if is_class_symbol {
            // For class symbols, find all methods and collect callers for each
            let all_symbols = indexer
                .db()
                .get_symbols_for_file(&symbol.file_path, graph_version)?;
            let mut ids: Vec<(i64, String)> = all_symbols
                .into_iter()
                .filter(|s| {
                    (s.kind == "method" || s.kind == "function")
                        && s.start_line >= symbol.start_line
                        && s.end_line <= symbol.end_line
                })
                .map(|s| {
                    let name = s.name.clone();
                    (s.id, name)
                })
                .collect();
            // Also include the class itself
            ids.push((symbol.id, symbol.name.clone()));
            ids
        } else {
            vec![(symbol.id, symbol.name.clone())]
        };

        for (target_id, target_name) in &target_ids {
            if caller_refs.len() >= max_refs || caller_bytes > callers_budget {
                break;
            }

            // Get edges for this target
            let target_edges = if *target_id == symbol.id {
                edges.clone()
            } else {
                indexer
                    .db()
                    .edges_for_symbol(*target_id, languages.as_deref(), graph_version)?
            };

            // Collect resolved callers
            for edge in &target_edges {
                if edge.kind == "CALLS"
                    && edge.target_symbol_id == Some(*target_id)
                    && let Some(source_id) = edge.source_symbol_id
                    && seen_caller_ids.insert(source_id)
                    && let Ok(Some(caller_sym)) = indexer.db().get_symbol_by_id(source_id)
                {
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
                    if caller_refs.len() >= max_refs {
                        break;
                    }
                }
            }

            // Check for unresolved callers by qualname
            if caller_refs.len() < max_refs && caller_bytes <= callers_budget {
                let unresolved_edges = indexer.db().incoming_edges_by_qualname_pattern(
                    target_name,
                    "CALLS",
                    languages.as_deref(),
                    graph_version,
                )?;

                for edge in &unresolved_edges {
                    if let Some(ref target_qn) = edge.target_qualname
                        && target_qn.ends_with(target_name)
                        && let Some(source_id) = edge.source_symbol_id
                        && seen_caller_ids.insert(source_id)
                        && let Ok(Some(caller_sym)) = indexer.db().get_symbol_by_id(source_id)
                    {
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
                        if caller_refs.len() >= max_refs {
                            break;
                        }
                    }
                }
            }
        }

        used_bytes += caller_bytes;
        Some(caller_refs)
    } else {
        None
    };

    // 6. Build callees (outgoing CALLS) - FIX #3: For class symbols, aggregate from methods
    let mut callees = if sections.contains(&"callees".to_string()) {
        let mut callee_refs = Vec::new();
        let mut callee_bytes = 0usize;
        let mut seen_callee_ids = std::collections::HashSet::new();

        // Determine if this is a class-level symbol
        let is_class_symbol = symbol.kind == "class";

        if is_class_symbol {
            // For class symbols, find all methods in the same file within the class's line range
            let all_symbols = indexer
                .db()
                .get_symbols_for_file(&symbol.file_path, graph_version)?;
            let methods: Vec<_> = all_symbols
                .into_iter()
                .filter(|s| {
                    (s.kind == "method" || s.kind == "function")
                        && s.start_line >= symbol.start_line
                        && s.end_line <= symbol.end_line
                })
                .collect();

            // Get callees from all methods
            for method in methods {
                let method_edges = indexer.db().edges_for_symbol(
                    method.id,
                    languages.as_deref(),
                    graph_version,
                )?;

                for edge in &method_edges {
                    if edge.kind == "CALLS" && edge.source_symbol_id == Some(method.id) {
                        // Resolve target_id, with fuzzy fallback for unresolved edges
                        let target_id = match edge.target_symbol_id {
                            Some(id) => Some(id),
                            None => edge.target_qualname.as_deref().and_then(|qn| {
                                indexer
                                    .db()
                                    .lookup_symbol_id_fuzzy(qn, languages.as_deref(), graph_version)
                                    .ok()
                                    .flatten()
                            }),
                        };
                        if let Some(target_id) = target_id
                            && seen_callee_ids.insert(target_id)
                            && let Ok(Some(callee_sym)) = indexer.db().get_symbol_by_id(target_id)
                        {
                            let evidence = edge.evidence_snippet.clone();
                            let ref_json = serde_json::to_string(&callee_sym).unwrap_or_default();
                            callee_bytes +=
                                ref_json.len() + evidence.as_ref().map_or(0, |e| e.len());
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
                            if callee_refs.len() >= max_refs {
                                break;
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
                            indexer
                                .db()
                                .lookup_symbol_id_fuzzy(qn, languages.as_deref(), graph_version)
                                .ok()
                                .flatten()
                        }),
                    };
                    if let Some(target_id) = target_id
                        && seen_callee_ids.insert(target_id)
                        && let Ok(Some(callee_sym)) = indexer.db().get_symbol_by_id(target_id)
                    {
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
                        if callee_refs.len() >= max_refs {
                            break;
                        }
                    }
                }
            }
        }

        used_bytes += callee_bytes;
        Some(callee_refs)
    } else {
        None
    };

    // 7. Find tests (incoming CALLS from test files)
    let mut tests = if sections.contains(&"tests".to_string()) {
        let mut test_refs = Vec::new();
        let mut test_bytes = 0usize;
        for edge in &edges {
            if edge.kind == "CALLS"
                && edge.target_symbol_id == Some(symbol.id)
                && let Some(source_id) = edge.source_symbol_id
                && let Ok(Some(test_sym)) = indexer.db().get_symbol_by_id(source_id)
            {
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
                    if test_refs.len() >= max_refs {
                        break;
                    }
                }
            }
        }
        used_bytes += test_bytes;
        Some(test_refs)
    } else {
        None
    };

    // 8. Find implements (EXTENDS/IMPLEMENTS/INHERITS edges) - FIX #2
    let implements = if sections.contains(&"implements".to_string()) {
        let mut impl_syms = Vec::new();
        for edge in &edges {
            if (edge.kind == "EXTENDS" || edge.kind == "IMPLEMENTS" || edge.kind == "INHERITS")
                && edge.source_symbol_id == Some(symbol.id)
                && let Some(target_id) = edge.target_symbol_id
                && let Ok(Some(impl_sym)) = indexer.db().get_symbol_by_id(target_id)
            {
                impl_syms.push(impl_sym);
            }
        }
        if impl_syms.is_empty() {
            None
        } else {
            Some(impl_syms)
        }
    } else {
        None
    };

    // 9. FIX #4: Budget expansion - if >30% budget remaining, fetch source snippets for refs
    let budget_remaining = max_bytes.saturating_sub(used_bytes);
    let budget_utilization = (used_bytes as f64) / (max_bytes as f64);

    if budget_utilization < 0.70 && budget_remaining > expansion_budget {
        let repo_root = indexer.repo_root();
        let snippet_budget_per_ref = 500; // Max bytes per reference snippet

        // Expand callers with source snippets
        if let Some(ref caller_list) = callers {
            for caller_ref in caller_list.iter() {
                if used_bytes + snippet_budget_per_ref > max_bytes {
                    break;
                }

                let full_path = repo_root.join(&caller_ref.symbol.file_path);
                if full_path.exists()
                    && let Ok(content) = std::fs::read_to_string(&full_path)
                {
                    let lines: Vec<&str> = content.lines().collect();
                    let start = (caller_ref.symbol.start_line as usize).saturating_sub(1);
                    let end = ((caller_ref.symbol.start_line + 3) as usize).min(lines.len());
                    let snippet = lines[start..end].join("\n");
                    let snippet = if snippet.len() > snippet_budget_per_ref {
                        let truncate_pos = snippet[..snippet_budget_per_ref]
                            .rfind('\n')
                            .unwrap_or(snippet_budget_per_ref);
                        snippet[..truncate_pos].to_string()
                    } else {
                        snippet
                    };
                    used_bytes += snippet.len();
                }
            }
        }

        // Expand callees with source snippets
        if let Some(ref callee_list) = callees {
            for callee_ref in callee_list.iter() {
                if used_bytes + snippet_budget_per_ref > max_bytes {
                    break;
                }

                let full_path = repo_root.join(&callee_ref.symbol.file_path);
                if full_path.exists()
                    && let Ok(content) = std::fs::read_to_string(&full_path)
                {
                    let lines: Vec<&str> = content.lines().collect();
                    let start = (callee_ref.symbol.start_line as usize).saturating_sub(1);
                    let end = ((callee_ref.symbol.start_line + 3) as usize).min(lines.len());
                    let snippet = lines[start..end].join("\n");
                    let snippet = if snippet.len() > snippet_budget_per_ref {
                        let truncate_pos = snippet[..snippet_budget_per_ref]
                            .rfind('\n')
                            .unwrap_or(snippet_budget_per_ref);
                        snippet[..truncate_pos].to_string()
                    } else {
                        snippet
                    };
                    used_bytes += snippet.len();
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
        if let Some(ref mut c) = callers {
            strip_to_compact(c);
        }
        if let Some(ref mut c) = callees {
            strip_to_compact(c);
        }
        if let Some(ref mut t) = tests {
            strip_to_compact(t);
        }
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
        warnings,
    };

    Ok(serde_json::to_value(&result)?)
}

// ---------------------------------------------------------------------------
// GROUP 4 -- Metrics handlers
// ---------------------------------------------------------------------------

pub(super) fn handle_orient(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: OrientParams = serde_json::from_value(params)?;
    let view = params.view.as_deref().unwrap_or("all");
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;

    // Resolve optional focus symbol via resolve module
    let focus_sym = if let Some(ref qn) = params.focus_qualname {
        Some(crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Qualname(qn.clone()),
            languages.as_deref(),
            graph_version,
        )?)
    } else if let Some(ref query) = params.focus_query {
        Some(crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query(query.clone()),
            languages.as_deref(),
            graph_version,
        )?)
    } else {
        None
    };

    let mut result = serde_json::Map::new();

    let include_overview = matches!(view, "all" | "overview");
    let include_map = matches!(view, "all" | "map");
    let include_modules = matches!(view, "all" | "modules");

    if include_overview {
        let overview = indexer.db().repo_overview(
            indexer.repo_root().clone(),
            languages.as_deref(),
            graph_version,
        )?;
        result.insert("overview".to_string(), json!(overview));
    }

    if include_map {
        let max_bytes = params.max_bytes.unwrap_or(8000).clamp(1000, 50000);
        let config = crate::repo_map::RepoMapConfig {
            max_bytes,
            languages: languages.clone(),
            paths: paths.clone(),
            graph_version,
        };
        let map_result = crate::repo_map::build_repo_map(indexer.db(), &config)?;
        result.insert(
            "map".to_string(),
            json!({
                "text": map_result.text,
                "modules": map_result.modules,
                "symbols": map_result.symbols,
                "bytes": map_result.bytes,
            }),
        );
    }

    if include_modules {
        let depth = params.depth.unwrap_or(1).clamp(1, 5);
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
        let edges = indexer
            .db()
            .module_edges(depth, languages.as_deref(), graph_version)?;
        let module_edges: Vec<ModuleEdge> = edges
            .into_iter()
            .map(|(src, dst, calls, imports)| ModuleEdge {
                source_module: src,
                target_module: dst,
                call_count: calls,
                import_count: imports,
            })
            .collect();
        result.insert("modules".to_string(), json!(modules));
        result.insert("module_edges".to_string(), json!(module_edges));
    }

    // Include focus symbol metadata when provided
    if let Some(sym) = focus_sym {
        result.insert(
            "focus_symbol".to_string(),
            json!({
                "id": sym.id,
                "name": sym.name,
                "qualname": sym.qualname,
                "kind": sym.kind,
                "file_path": sym.file_path,
            }),
        );
    }

    Ok(Value::Object(result))
}

pub(super) fn handle_repo_map(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: RepoMapParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let paths = normalize_search_paths(indexer.repo_root(), None, params.paths)?;
    let max_bytes = params.max_bytes.unwrap_or(8000).clamp(1000, 50000);

    let config = crate::repo_map::RepoMapConfig {
        max_bytes,
        languages,
        paths,
        graph_version,
    };
    let map_result = crate::repo_map::build_repo_map(indexer.db(), &config)?;
    Ok(json!({
        "text": map_result.text,
        "modules": map_result.modules,
        "symbols": map_result.symbols,
        "bytes": map_result.bytes,
    }))
}

pub(super) fn handle_dead_symbols(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: DeadSymbolsParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let limit = params.limit.unwrap_or(50);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths.clone())?;
    let include_unused_imports = params.include_unused_imports.unwrap_or(true);
    let include_orphan_tests = params.include_orphan_tests.unwrap_or(true);

    let dead_syms =
        indexer
            .db()
            .dead_symbols(limit, languages.as_deref(), paths.as_deref(), graph_version)?;

    let unused_imports = if include_unused_imports {
        indexer
            .db()
            .unused_imports(limit, languages.as_deref(), paths.as_deref(), graph_version)?
    } else {
        vec![]
    };

    let orphan_tests = if include_orphan_tests {
        indexer
            .db()
            .orphan_tests(limit, languages.as_deref(), paths.as_deref(), graph_version)?
    } else {
        vec![]
    };

    let ds_count = dead_syms.len();
    let ui_count = unused_imports.len();
    let ot_count = orphan_tests.len();

    Ok(json!({
        "dead_symbols": dead_syms,
        "unused_imports": unused_imports,
        "orphan_tests": orphan_tests,
        "counts": {
            "dead_symbols": ds_count,
            "unused_imports": ui_count,
            "orphan_tests": ot_count,
        }
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

pub(super) fn handle_context(indexer: &mut Indexer, params: Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct ContextParams {
        path: String,
        format: Option<String>,
        graph_version: Option<i64>,
    }
    let params: ContextParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let ctx = crate::context::build_file_context(
        indexer.db(),
        indexer.repo_root(),
        &params.path,
        graph_version,
    )?;
    match params.format.as_deref() {
        Some("json") => Ok(crate::context::format_json(&ctx)),
        _ => Ok(json!({ "context": crate::context::format_text(&ctx) })),
    }
}

// ---------------------------------------------------------------------------
// GROUP 2 -- Graph handlers
// ---------------------------------------------------------------------------

pub(super) fn handle_trace_flow(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: TraceFlowParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;
    let languages = params.languages.clone();
    let max_hops = params.max_hops.unwrap_or(5).min(10);
    let include_snippets = params.include_snippets.unwrap_or(true);
    let max_bytes = params.max_bytes.unwrap_or(30_000).min(200_000);
    let trace_offset = params.trace_offset.unwrap_or(0);
    let compact_mode = params.format.as_deref() == Some("compact");
    let direction = match params.direction.as_deref().unwrap_or("downstream") {
        "upstream" => crate::traversal::TraceDirection::Upstream,
        _ => crate::traversal::TraceDirection::Downstream,
    };
    let allowed_kinds: Vec<String> = params
        .kinds
        .clone()
        .unwrap_or_else(|| crate::traversal::TraceConfig::default().allowed_kinds);

    // Config URI resolution: find all symbols connected to the URI
    let config_uri_seeds: Vec<i64> = if let Some(ref qn) = params.start_qualname {
        if crate::indexer::config::is_config_uri(qn) {
            indexer
                .db()
                .source_symbols_for_config_uri(qn, &[], graph_version)?
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    // Resolve start symbol
    let start_ref = if let Some(id) = params.start_id {
        crate::resolve::SymbolRef::Id(id)
    } else if let Some(ref qn) = params.start_qualname {
        if crate::indexer::config::is_config_uri(qn) {
            let first_id = config_uri_seeds
                .first()
                .ok_or_else(|| anyhow::anyhow!("no symbols found for config URI: {}", qn))?;
            crate::resolve::SymbolRef::Id(*first_id)
        } else {
            crate::resolve::SymbolRef::Qualname(qn.clone())
        }
    } else if let Some(ref query) = params.query {
        crate::resolve::SymbolRef::Query(query.clone())
    } else {
        anyhow::bail!("trace_flow requires start_id, start_qualname, or query");
    };
    let start = crate::resolve::resolve_symbol(
        indexer.db(),
        start_ref,
        languages.as_deref(),
        graph_version,
    )?;

    // Resolve optional end symbol
    let end_id = if let Some(id) = params.end_id {
        Some(id)
    } else if let Some(ref qn) = params.end_qualname {
        indexer.db().lookup_symbol_id(qn, graph_version)?
    } else {
        None
    };

    // Expand seeds: container members + config URI seeds
    let mut seed_ids = crate::resolve::expand_seeds(indexer.db(), start.id, graph_version)?;
    for id in &config_uri_seeds {
        if !seed_ids.contains(id) {
            seed_ids.push(*id);
        }
    }

    // BFS traversal via traversal module
    let config = crate::traversal::TraceConfig {
        max_hops,
        max_bytes,
        direction,
        include_snippets,
        allowed_kinds,
        trace_offset,
        compact: compact_mode,
    };
    let trace_result = crate::traversal::trace_flow(
        indexer.db(),
        seed_ids,
        end_id,
        languages.as_deref(),
        graph_version,
        &config,
    )?;

    let trace = &trace_result.hops;
    let truncated = trace_result.truncated;

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
    if truncated && params.kinds.is_none() {
        // Suggest narrowing by edge kind when trace was truncated and no filter was used
        let mut narrow_params = json!({"max_bytes": (max_bytes * 2).min(200_000)});
        if let Some(ref qn) = params.start_qualname {
            narrow_params["start_qualname"] = json!(qn);
        } else if let Some(id) = params.start_id {
            narrow_params["start_id"] = json!(id);
        }
        narrow_params["kinds"] = json!(["CONFIG_BIND", "CONFIG_SOURCE", "CONFIG_READ"]);
        next_hops.push(json!({
            "method": "trace_flow",
            "params": narrow_params,
            "description": "Re-trace with only CONFIG edges (avoids truncation)",
        }));
    }
    for h in trace.iter().take(3) {
        next_hops.push(json!({
            "method": "explain_symbol",
            "params": {"id": h.symbol.id},
            "description": format!("Explain {}", h.symbol.name),
        }));
    }
    // When trace is empty, suggest analyze_impact as an alternative
    if trace.is_empty() {
        let mut impact_params = json!({"id": start.id, "direction": "upstream"});
        if matches!(start.kind.as_str(), "class" | "property") {
            impact_params["kinds"] =
                json!(["CONFIG_BIND", "CONFIG_SOURCE", "CONFIG_READ", "CALLS"]);
        }
        next_hops.push(json!({
            "method": "analyze_impact",
            "params": impact_params,
            "description": format!("Try analyze_impact on {} (finds consumers via CONFIG/DI edges)", start.name),
        }));
        // Also suggest with CONFIG-only kinds if default kinds were used
        if params.kinds.is_none() {
            let mut retry_params = json!({"include_snippets": include_snippets});
            if let Some(ref qn) = params.start_qualname {
                retry_params["start_qualname"] = json!(qn);
            } else {
                retry_params["start_id"] = json!(start.id);
            }
            retry_params["kinds"] = json!(["CONFIG_SOURCE", "CONFIG_READ", "CONFIG_BIND"]);
            next_hops.push(json!({
                "method": "trace_flow",
                "params": retry_params,
                "description": "Re-trace with CONFIG edges only (useful for config/property symbols)",
            }));
        }
    }

    let result = TraceFlowResult {
        start: trace_result.start,
        end: trace_result.end,
        trace: trace_result.hops,
        paths_found: trace_result.paths_found,
        reached_target: trace_result.reached_target,
        truncated,
        budget: BudgetInfo {
            budget_bytes: trace_result.budget_bytes,
            used_bytes: trace_result.used_bytes,
            truncated,
        },
        next_hops,
    };

    let mut value = serde_json::to_value(&result)?;
    if compact_mode {
        value = apply_compact_format(value);
    }
    Ok(value)
}

// ---------------------------------------------------------------------------
// GROUP 3 -- Analysis handlers
// ---------------------------------------------------------------------------

/// Build a MultiLayerConfig from AnalyzeImpactParams with a given limit.
fn build_impact_config(
    params: &AnalyzeImpactParams,
    limit: usize,
) -> crate::impact::config::MultiLayerConfig {
    let mut config = crate::impact::config::MultiLayerConfig::builder()
        .max_depth(params.max_depth.unwrap_or(3).min(10))
        .direction(
            params
                .direction
                .clone()
                .unwrap_or_else(|| "both".to_string()),
        )
        .include_tests(params.include_tests.unwrap_or(false))
        .include_paths(params.include_paths.unwrap_or(true))
        .limit(limit)
        .min_confidence(params.min_confidence.unwrap_or(0.0))
        .build();

    if let Some(enable_direct) = params.enable_direct {
        config.direct.enabled = enable_direct;
    }
    if let Some(enable_test) = params.enable_test {
        config.test.enabled = enable_test;
    }
    if let Some(enable_historical) = params.enable_historical {
        config.historical.enabled = enable_historical;
    }
    if let Some(languages) = params.languages.as_ref()
        && let Ok(normalized) = scan::normalize_language_filter(Some(languages.as_slice()))
    {
        config.direct.languages = normalized;
    }
    if let Some(ref kinds) = params.kinds {
        config.direct.kinds = kinds.clone();
    }
    config
}

/// Resolve a single qualname (or config URI) to seed IDs and run impact analysis.
fn resolve_and_analyze_single(
    indexer: &mut Indexer,
    qualname: &str,
    config: &crate::impact::config::MultiLayerConfig,
    graph_version: i64,
) -> Result<crate::impact::types::UnifiedImpactResult> {
    let dir = config.direct.direction.as_str();

    let seed_ids = if crate::indexer::config::is_config_uri(qualname) {
        let uri_kinds: &[&str] = match dir {
            "downstream" => &["CONFIG_SOURCE"],
            "upstream" => &["CONFIG_READ", "CONFIG_BIND"],
            _ => &[],
        };
        let ids = indexer
            .db()
            .source_symbols_for_config_uri(qualname, uri_kinds, graph_version)?;
        if ids.is_empty() {
            return Err(anyhow::anyhow!(
                "no symbols found for config URI: {}",
                qualname
            ));
        }
        ids
    } else {
        let symbol = indexer
            .db()
            .get_symbol_by_qualname(qualname, graph_version)?
            .ok_or_else(|| anyhow::anyhow!("symbol not found: {}", qualname))?;
        vec![symbol.id]
    };

    crate::impact::analyze_impact_multi_layer(
        indexer.db(),
        &seed_ids,
        config.clone(),
        graph_version,
    )
}

pub(super) fn handle_analyze_impact(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: AnalyzeImpactParams = serde_json::from_value(params)?;
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;

    // ---- Batch path: multiple qualnames in one call ----
    if let Some(ref qualnames) = params.qualnames {
        if qualnames.is_empty() {
            return Err(anyhow::anyhow!("qualnames array must not be empty"));
        }

        // Build config once (shared across all seeds)
        let total_limit = params.limit.unwrap_or(500).min(2000);
        let per_seed_limit = (total_limit / qualnames.len()).max(50);

        let base_config = build_impact_config(&params, per_seed_limit);

        let mut results = Vec::with_capacity(qualnames.len());
        let mut total_affected: usize = 0;
        let mut all_files: std::collections::HashSet<String> = std::collections::HashSet::new();

        for qn in qualnames {
            let entry = match resolve_and_analyze_single(indexer, qn, &base_config, graph_version) {
                Ok(result) => {
                    total_affected += result.summary.total_affected;
                    for fi in &result.summary.by_file {
                        all_files.insert(fi.path.clone());
                    }
                    crate::impact::types::BatchImpactEntry {
                        seed_qualname: qn.clone(),
                        seeds: result.seeds,
                        affected: result.affected,
                        summary: result.summary,
                        truncated: result.truncated,
                        layers: result.layers,
                    }
                }
                Err(e) => {
                    // Include error entry rather than failing the whole batch
                    crate::impact::types::BatchImpactEntry {
                        seed_qualname: qn.clone(),
                        seeds: vec![],
                        affected: vec![],
                        summary: crate::impact::types::ImpactSummary {
                            by_file: vec![],
                            by_relationship: std::collections::HashMap::new(),
                            by_distance: std::collections::HashMap::new(),
                            total_affected: 0,
                        },
                        truncated: false,
                        layers: crate::impact::types::LayerMetadata {
                            direct: Some(crate::impact::types::LayerStats {
                                enabled: false,
                                duration_ms: 0,
                                result_count: 0,
                                truncated: false,
                                error: Some(e.to_string()),
                            }),
                            test: None,
                            historical: None,
                        },
                    }
                }
            };
            results.push(entry);
        }

        let config_display = crate::impact::types::ImpactConfig {
            max_depth: base_config.direct.max_depth,
            direction: base_config.direct.direction.clone(),
            relationship_types: base_config.direct.kinds.clone(),
            include_tests: base_config.direct.include_tests,
            limit: total_limit,
        };

        let batch = crate::impact::types::BatchImpactResult {
            results,
            config: config_display,
            total_affected,
            total_files: all_files.len(),
        };

        return Ok(json!(batch));
    }

    // ---- Single-seed path (unchanged) ----

    // Check for config URI in qualname (e.g., "secret://datamgr-db-conn-str", "env://DATABASE")
    // Direction-aware: downstream seeds from providers (CONFIG_SOURCE),
    // upstream seeds from consumers (CONFIG_READ), both uses all.
    let seed_ids: Vec<i64> = if let Some(qualname) = params.qualname.as_deref() {
        if crate::indexer::config::is_config_uri(qualname) {
            let dir = params.direction.as_deref().unwrap_or("both");
            let uri_kinds: &[&str] = match dir {
                "downstream" => &["CONFIG_SOURCE"],
                "upstream" => &["CONFIG_READ", "CONFIG_BIND"],
                _ => &[],
            };
            let ids =
                indexer
                    .db()
                    .source_symbols_for_config_uri(qualname, uri_kinds, graph_version)?;
            if ids.is_empty() {
                return Err(anyhow::anyhow!(
                    "no symbols found for config URI: {}",
                    qualname
                ));
            }
            ids
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    // Resolve symbol by id, qualname, or fuzzy query (skip if config URI already resolved)
    let seed_ids = if !seed_ids.is_empty() {
        seed_ids
    } else {
        let sym_ref = if let Some(id) = params.id {
            crate::resolve::SymbolRef::Id(id)
        } else if let Some(ref qualname) = params.qualname {
            crate::resolve::SymbolRef::Qualname(qualname.clone())
        } else if let Some(ref query) = params.query {
            crate::resolve::SymbolRef::Query(query.clone())
        } else {
            return Err(anyhow::anyhow!(
                "analyze_impact requires id, qualname, or query"
            ));
        };
        let langs = scan::normalize_language_filter(params.languages.as_deref())?;
        let symbol =
            crate::resolve::resolve_symbol(indexer.db(), sym_ref, langs.as_deref(), graph_version)?;

        // Property→parent expansion: if the seed is a property/field/attribute/const,
        // also add the parent class so CONFIG_BIND consumers are reachable
        let mut ids = vec![symbol.id];
        if matches!(
            symbol.kind.as_str(),
            "property" | "field" | "attribute" | "const"
        ) && let Some(parent_qn) = symbol.qualname.rsplit_once('.').map(|(p, _)| p)
            && let Ok(Some(parent)) = indexer
                .db()
                .get_symbol_by_qualname(parent_qn, graph_version)
            && !ids.contains(&parent.id)
        {
            ids.push(parent.id);
        }
        ids
    };

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
    let result =
        crate::impact::analyze_impact_multi_layer(indexer.db(), &seed_ids, config, graph_version)?;

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
        paths
            .iter()
            .map(|p| ChangedFile {
                path: p.clone(),
                changed_ranges: Vec::new(),
                added_ranges: Vec::new(),
                deleted_ranges: Vec::new(),
            })
            .collect()
    } else {
        anyhow::bail!("analyze_diff requires 'diff' or 'paths' parameter");
    };

    if changed_files.is_empty() {
        anyhow::bail!("No changed files found");
    }

    // Step 2: Find symbols in changed files, filtered by hunk ranges
    let mut changed_symbols = Vec::new();
    for cf in &changed_files {
        let symbols = indexer
            .db()
            .get_symbols_for_file(&cf.path, graph_version)
            .unwrap_or_default();
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
                if !overlaps {
                    continue;
                }
                // Determine change type: if symbol is fully within added range, it's "added"
                let fully_added = cf.added_ranges.iter().any(|h| {
                    let hunk_end = h.start_line + h.line_count - 1;
                    sym.start_line >= h.start_line && sym.end_line <= hunk_end
                });
                if fully_added {
                    "added".to_string()
                } else {
                    "modified".to_string()
                }
            } else {
                "modified".to_string()
            };

            // Step 2a: Detect signature changes by comparing with previous graph version
            let mut old_signature = None;
            let new_signature = sym.signature.clone();
            let mut final_change_type = change_type.clone();

            if change_type == "modified" && graph_version > 1 {
                // Try to find the symbol in the previous graph version
                if let Some(stable_id) = sym.stable_id.as_ref()
                    && let Ok(Some(old_sym)) = indexer
                        .db()
                        .get_symbol_by_stable_id(stable_id, graph_version - 1)
                {
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

    // Step 2b: Deduplicate containment — when a hunk overlaps both a method and its
    // parent class/interface, keep only the more specific (child) symbol. A parent is
    // removed if any other matched symbol's range is strictly within it.
    if changed_symbols.len() > 1 {
        let ranges: Vec<(i64, i64, i64)> = changed_symbols
            .iter()
            .map(|cs| (cs.symbol.id, cs.symbol.start_line, cs.symbol.end_line))
            .collect();
        changed_symbols.retain(|cs| {
            !ranges.iter().any(|(id, start, end)| {
                *id != cs.symbol.id
                    && *start >= cs.symbol.start_line
                    && *end <= cs.symbol.end_line
                    && (*start > cs.symbol.start_line || *end < cs.symbol.end_line)
            })
        });
    }

    // Step 3: Compute downstream impact via multi-level BFS (depth controlled by max_depth)
    let seed_ids: Vec<i64> = changed_symbols.iter().map(|cs| cs.symbol.id).collect();
    let mut downstream = Vec::new();
    let mut seen_ids: HashSet<i64> = seed_ids.iter().copied().collect();
    let max_downstream = 50;

    // BFS: start with changed symbols, expand callers level by level
    let mut current_level: Vec<Symbol> =
        changed_symbols.iter().map(|cs| cs.symbol.clone()).collect();
    let mut base_confidence = 0.9;

    for current_distance in 1..=max_depth {
        let mut next_level = Vec::new();

        for sym in &current_level {
            if downstream.len() >= max_downstream {
                break;
            }

            let edges =
                indexer
                    .db()
                    .edges_for_symbol(sym.id, languages.as_deref(), graph_version)?;

            // Find callers via resolved edges
            for edge in &edges {
                if downstream.len() >= max_downstream {
                    break;
                }
                if edge.kind == "CALLS"
                    && edge.target_symbol_id == Some(sym.id)
                    && let Some(source_id) = edge.source_symbol_id
                    && seen_ids.insert(source_id)
                    && let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id)
                {
                    next_level.push(caller.clone());
                    downstream.push(DiffImpactEntry {
                        symbol: caller,
                        relationship: if current_distance == 1 {
                            "caller".to_string()
                        } else {
                            format!("caller_depth_{}", current_distance)
                        },
                        distance: current_distance,
                        confidence: base_confidence,
                    });
                }
            }

            // Qualname fallback for unresolved incoming edges
            if downstream.len() < max_downstream {
                let unresolved = indexer
                    .db()
                    .incoming_edges_by_qualname_pattern(
                        &sym.name,
                        "CALLS",
                        languages.as_deref(),
                        graph_version,
                    )
                    .unwrap_or_default();
                for edge in &unresolved {
                    if downstream.len() >= max_downstream {
                        break;
                    }
                    if let Some(source_id) = edge.source_symbol_id
                        && seen_ids.insert(source_id)
                        && let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id)
                    {
                        next_level.push(caller.clone());
                        downstream.push(DiffImpactEntry {
                            symbol: caller,
                            relationship: if current_distance == 1 {
                                "caller".to_string()
                            } else {
                                format!("caller_depth_{}", current_distance)
                            },
                            distance: current_distance,
                            confidence: base_confidence * 0.8,
                        });
                    }
                }
            }
        }

        if next_level.is_empty() || downstream.len() >= max_downstream {
            break;
        }
        current_level = next_level;
        base_confidence *= 0.8; // Decay confidence per level
    }

    // Step 4: Test coverage (with qualname fallback)
    let test_coverage = if include_tests {
        let mut coverage = Vec::new();
        for cs in &changed_symbols {
            let mut tests = Vec::new();
            let mut seen_test_ids = HashSet::new();
            // Check resolved edges
            let edges =
                indexer
                    .db()
                    .edges_for_symbol(cs.symbol.id, languages.as_deref(), graph_version)?;
            for edge in &edges {
                if edge.kind == "CALLS"
                    && edge.target_symbol_id == Some(cs.symbol.id)
                    && let Some(source_id) = edge.source_symbol_id
                    && let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id)
                    && is_test_symbol(&caller)
                    && seen_test_ids.insert(source_id)
                {
                    tests.push(TestRef {
                        test_qualname: caller.qualname.clone(),
                        test_file: caller.file_path.clone(),
                        coverage_type: "direct".to_string(),
                    });
                }
            }
            // Qualname fallback for unresolved edges
            let unresolved = indexer
                .db()
                .incoming_edges_by_qualname_pattern(
                    &cs.symbol.name,
                    "CALLS",
                    languages.as_deref(),
                    graph_version,
                )
                .unwrap_or_default();
            for edge in &unresolved {
                if let Some(source_id) = edge.source_symbol_id
                    && let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id)
                    && is_test_symbol(&caller)
                    && seen_test_ids.insert(source_id)
                {
                    tests.push(TestRef {
                        test_qualname: caller.qualname.clone(),
                        test_file: caller.file_path.clone(),
                        coverage_type: "direct".to_string(),
                    });
                }
            }
            let status = if tests.is_empty() {
                "uncovered"
            } else {
                "covered"
            };
            coverage.push(TestCoverageEntry {
                symbol_qualname: cs.symbol.qualname.clone(),
                tests,
                status: status.to_string(),
            });
        }
        Some(coverage)
    } else {
        None
    };

    // Step 5: Enhanced risk assessment with review checklist
    let risk = if include_risk {
        let mut factors = Vec::new();
        let mut focus_areas = Vec::new();
        let mut review_checklist = Vec::new();

        // 1. Signature change + high fan-in = CRITICAL risk
        for cs in &changed_symbols {
            if cs.change_type == "signature_changed" {
                let caller_count = downstream
                    .iter()
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
            let changed_langs: HashSet<_> = changed_symbols
                .iter()
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
                description: format!(
                    "{} cross-language callers affected",
                    cross_lang_callers.len()
                ),
                severity: "high".to_string(),
            });
            for caller in cross_lang_callers.iter().take(3) {
                review_checklist.push(format!("Test cross-language caller: {}", caller));
            }
        }

        // 3. Interface/trait signature changes = HIGH risk
        //    Only flag when the signature actually changed, not just because the
        //    interface appeared in the changed list (e.g. due to a method body edit
        //    in the same file).
        for cs in &changed_symbols {
            if matches!(
                cs.symbol.kind.as_str(),
                "interface" | "trait" | "abstract_class"
            ) && cs.change_type == "signature_changed"
            {
                factors.push(RiskFactor {
                    factor: "Interface/contract change".to_string(),
                    description: format!(
                        "{} {} signature changed",
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
        let high_fan_in: Vec<_> = downstream
            .iter()
            .filter(|d| d.relationship.starts_with("caller"))
            .collect();
        if high_fan_in.len() > 10 {
            factors.push(RiskFactor {
                factor: "High fan-in".to_string(),
                description: format!("{} callers affected", high_fan_in.len()),
                severity: "high".to_string(),
            });
            let caller_files: HashSet<_> = high_fan_in
                .iter()
                .map(|d| d.symbol.file_path.as_str())
                .collect();
            if caller_files.len() <= 5 {
                for file in caller_files {
                    review_checklist.push(format!("Review callers in {}", file));
                }
            }
        }

        // 5. Wide blast radius = MEDIUM risk
        let affected_files: HashSet<_> = downstream
            .iter()
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
    } else {
        None
    };

    let mut used_bytes = 0;
    let result_json = serde_json::to_value(&changed_symbols)?;
    used_bytes += serde_json::to_string(&result_json)
        .unwrap_or_default()
        .len();

    let mut next_hops: Vec<Value> = Vec::new();
    // Add explain_symbol for first changed symbol
    if let Some(cs) = changed_symbols.first() {
        next_hops.push(json!({"method": "explain_symbol", "params": {"id": cs.symbol.id}, "description": format!("Explain {}", cs.symbol.name)}));
    }
    // Add references for top changed symbol
    if let Some(cs) = changed_symbols
        .iter()
        .find(|cs| cs.symbol.kind == "method" || cs.symbol.kind == "function")
    {
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
    let context_lines = normalize_rg_context(params.context_lines);
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

// ---------------------------------------------------------------------------
// GROUP 6 -- Index/meta handlers
// ---------------------------------------------------------------------------

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
        let all_symbol_seeds = params
            .seeds
            .iter()
            .all(|seed| matches!(seed, ContextSeed::Symbol { .. }));
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

    let result =
        gather_context::gather_context(indexer.db(), indexer.repo_root(), &params.seeds, &config)?;

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
    let modules = indexer
        .db()
        .module_summary(1, languages.as_deref(), None, graph_version)?;
    let module_nodes: Vec<Value> = modules
        .into_iter()
        .map(|m| {
            json!({
                "path": m.path,
                "file_count": m.file_count,
                "symbol_count": m.symbol_count,
                "languages": m.languages,
            })
        })
        .collect();

    // 3. Languages
    let lang_list: Vec<String> = scan::language_specs()
        .iter()
        .map(|s| s.name.to_string())
        .collect();

    // 4. Index status
    let changed = indexer.changed_files(languages.as_deref())?;
    let stale =
        !changed.added.is_empty() || !changed.modified.is_empty() || !changed.deleted.is_empty();
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
        { "method": "orient", "params": { "view": "map" }, "why": "Get architecture text overview" },
        { "method": "search", "params": { "query": "<topic>" }, "why": "Search code by pattern" },
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
