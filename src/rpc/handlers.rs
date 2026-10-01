//! Extracted handler functions for RPC methods.
//! Each function corresponds to a match arm in `handle_method`.

use super::reading::is_markdown_path;
use super::*;
use crate::search::{
    RgSearchOptions, annotate_grep_hits, normalize_rg_context, resolve_rg_paths, search_rg,
};

// ---------------------------------------------------------------------------
// GROUP 1 -- Symbol query handlers
// ---------------------------------------------------------------------------

/// One `explain_symbol` section's byte budget and running spend.
struct SectionBudget {
    budget: usize,
    used: usize,
}

impl SectionBudget {
    fn new(share: usize, rollover: usize) -> Self {
        Self {
            budget: share + rollover,
            used: 0,
        }
    }

    /// Unspent budget, which rolls forward into the next section.
    fn rollover(&self) -> usize {
        self.budget.saturating_sub(self.used)
    }

    /// Whether a ref of `ref_bytes` may be added: it fits this section's
    /// budget, or it is the section's first ref and the true remaining
    /// budget (`global_used` bytes of earlier sections plus this section's
    /// own) still fits it.
    ///
    /// Issue #120: without the second arm, a requested section could come
    /// back empty purely because its share happened to be smaller than its
    /// first candidate, even with most of `max_bytes` still unused.
    fn admits(
        &self,
        ref_bytes: usize,
        is_first: bool,
        global_used: usize,
        max_bytes: usize,
    ) -> bool {
        self.used + ref_bytes <= self.budget
            || (is_first && global_used + self.used + ref_bytes <= max_bytes)
    }
}

pub(super) fn handle_explain_symbol(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let raw_params = params.clone();
    let params: ExplainSymbolParams = super::parse_params("explain_symbol", params)?;
    let ctx = HandlerContext::new(indexer, params.common)?;

    // ponytail: 200_000 is a hard ceiling on the internal section budget, not a
    // knob anyone tunes; ceiling exists to bound worst-case response size. If a
    // caller asks for more, we clamp but say so via budget.requested_bytes
    // rather than silently pretending we honored the request.
    let requested_max_bytes = params.max_bytes;
    let max_bytes = requested_max_bytes.unwrap_or(40_000).min(200_000);
    let max_bytes_clamped = requested_max_bytes.is_some_and(|v| v != max_bytes);
    super::validate::require_at_least_one("max_refs", params.max_refs)?;
    super::validate::require_one_of(
        "format",
        params.format.as_deref(),
        super::validate::EXPLAIN_FORMATS,
    )?;
    super::validate::require_at_least_one("max_bytes", params.max_bytes)?;
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

    // Issue #67: resolve `min_resolution` against the resolver's canonical,
    // strongest-to-weakest tier order (`db::resolver::ALL_RESOLUTION_KINDS`,
    // the same single source of truth issue #81's `exclude_resolution_kinds`
    // validates against). An unknown tier is an error: a silently ignored
    // filter returns an unfiltered answer the caller reads as filtered.
    super::validate::require_one_of(
        "min_resolution",
        params.min_resolution.as_deref(),
        &crate::db::resolver::ALL_RESOLUTION_KINDS,
    )?;
    let min_resolution_rank: Option<usize> = params
        .min_resolution
        .as_deref()
        .and_then(resolution_kind_rank);
    // A ref passes when its edge's tier ranks at or above (index <=)
    // `min_resolution_rank`. An edge whose `resolution_kind` is absent
    // (never resolved -- e.g. a String-Targeted Edge Kind whose own target
    // is a config key/secret URI, not a symbol, such as CONFIG_SOURCE)
    // never passes once a tier floor is set, since "absent" is weaker than
    // every named tier.
    let meets_min_resolution = |kind: &Option<String>| -> bool {
        match min_resolution_rank {
            None => true,
            Some(min_rank) => kind
                .as_deref()
                .and_then(resolution_kind_rank)
                .is_some_and(|rank| rank <= min_rank),
        }
    };

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
    let resolved = match crate::resolve::resolve_or_recovery(
        indexer.db(),
        sym_ref,
        ctx.languages.as_deref(),
        ctx.graph_version,
        "explain_symbol",
        &raw_params,
    )? {
        Ok(resolved) => resolved,
        Err(payload) => return Ok(payload),
    };
    let symbol = resolved.symbol.clone();

    // 2. Budget allocation: percentages below are shares of max_bytes (30%
    // source, 20% callers, 20% callees, 10% tests, 10% implements) - FIX #4.
    //
    // Issue #120: these shares used to apply even to sections nobody asked
    // for, so e.g. `sections:["callers"]` alone still only got 20% of
    // max_bytes -- sometimes leaving the section empty even though 80% of
    // the budget went unused. Shares are now renormalized across only the
    // requested sections, and any share a section doesn't spend rolls
    // forward into the next one, in source -> callers -> callees -> tests
    // -> implements order (the same order they're built in below).
    let wants = |s: &str| sections.iter().any(|x| x == s);
    let wants_source = wants("source");
    let wants_callers = wants("callers");
    let wants_callees = wants("callees");
    let wants_tests = wants("tests");
    let wants_implements = wants("implements");

    // 4. Get edges for callers/callees
    let edges = indexer.db().edges_for_symbol_with_dispatch(
        symbol.id,
        ctx.languages.as_deref(),
        ctx.graph_version,
    )?;

    // 4b. Cross-boundary neighbours (RPC/HTTP/channel/config), appended after
    // the CALLS refs in callers/callees/tests below. Same seed set as the
    // CALLS aggregation: a class also speaks for its members.
    let cross_seeds = if symbol.kind == "class" {
        crate::resolve::expand_seeds(indexer.db(), symbol.id, ctx.graph_version)?
    } else {
        vec![symbol.id]
    };
    let incoming_cross = if wants_callers || wants_tests {
        cross_boundary_refs(indexer.db(), &cross_seeds, false, &ctx)?
    } else {
        Vec::new()
    };
    let outgoing_cross = if wants_callees {
        cross_boundary_refs(indexer.db(), &cross_seeds, true, &ctx)?
    } else {
        Vec::new()
    };

    // Sections with no candidates are known before filling, so they are
    // left out of the share renormalization: their share goes to the other
    // requested sections instead of stranding behind an earlier one.
    let has_callers = wants_callers
        && (symbol.kind == "class"
            || !incoming_cross.is_empty()
            || !indexer
                .db()
                .dispatch_peers(symbol.id, ctx.graph_version)?
                .interface_methods
                .is_empty()
            || edges
                .iter()
                .any(|e| e.kind == "CALLS" && e.target_symbol_id == Some(symbol.id)));
    let has_callees = wants_callees
        && (symbol.kind == "class"
            || !outgoing_cross.is_empty()
            || edges.iter().any(|e| {
                e.kind == "CALLS"
                    && e.source_symbol_id == Some(symbol.id)
                    && e.target_symbol_id.is_some()
            }));
    let has_tests = wants_tests
        && (!incoming_cross.is_empty()
            || edges
                .iter()
                .any(|e| e.kind == "CALLS" && e.target_symbol_id == Some(symbol.id)));
    let has_direct_implements = edges.iter().any(|e| {
        matches!(e.kind.as_str(), "EXTENDS" | "IMPLEMENTS" | "INHERITS")
            && e.target_symbol_id.is_some()
            && (e.source_symbol_id == Some(symbol.id)
                || (e.kind == "IMPLEMENTS" && e.target_symbol_id == Some(symbol.id)))
    });
    let has_implements = wants_implements
        && (has_direct_implements
            || !indexer
                .db()
                .implementing_types(symbol.id, ctx.graph_version)?
                .is_empty());

    const SOURCE_PCT: usize = 30;
    const CALLERS_PCT: usize = 20;
    const CALLEES_PCT: usize = 20;
    const TESTS_PCT: usize = 10;
    const IMPLEMENTS_PCT: usize = 10;

    let active_pct: usize = [
        (wants_source, SOURCE_PCT),
        (has_callers, CALLERS_PCT),
        (has_callees, CALLEES_PCT),
        (has_tests, TESTS_PCT),
        (has_implements, IMPLEMENTS_PCT),
    ]
    .into_iter()
    .filter_map(|(active, pct)| active.then_some(pct))
    .sum();
    let alloc_share = |pct: usize, active: bool| -> usize {
        if active && active_pct > 0 {
            max_bytes * pct / active_pct
        } else {
            0
        }
    };

    // Expansion (step 9) only adds source snippets to callers/callees
    // already selected below; it's a headroom check ("is there enough
    // budget left over to bother"), not a user-selectable section, so it
    // keeps a flat share of the raw max_bytes instead of competing with
    // requested sections for renormalized space.
    let expansion_budget = max_bytes * 10 / 100;

    let mut used_bytes = 0usize;
    // Tracks only the source-snippet cut; caller/callee/test/implements
    // truncation is derived honestly below from `returned.len() < total` for
    // each section (see step 9.5), so a section capped by max_refs is never
    // reported as complete just because it didn't also blow its byte budget.
    let mut source_truncated = false;
    // Unused share of one section's budget rolls forward into the next
    // (source -> callers -> callees -> tests -> implements), so a section
    // that's skipped or spends less than its share never strands bytes a
    // later requested section could have used. Source is first, so it only
    // ever carries the initial (always-zero) rollover, kept explicit here
    // for symmetry with every later section's `budget = share + rollover`.
    let mut source_budget = SectionBudget::new(alloc_share(SOURCE_PCT, wants_source), 0);

    // 3. Read source (FIX #5: truncate at line boundaries)
    let source = if wants_source {
        let repo_root = indexer.repo_root();
        let full_path = repo_root.join(&symbol.file_path);
        if full_path.exists() {
            let content = std::fs::read_to_string(&full_path).unwrap_or_default();
            let lines: Vec<&str> = content.lines().collect();
            let start = (symbol.start_line as usize).saturating_sub(1);
            let end = (symbol.end_line as usize).min(lines.len());
            let snippet = lines[start..end].join("\n");
            let snippet = if snippet.len() > source_budget.budget {
                source_truncated = true;
                // Find last newline before budget limit to avoid mid-line truncation
                let truncate_pos = snippet[..source_budget.budget]
                    .rfind('\n')
                    .unwrap_or(source_budget.budget);
                snippet[..truncate_pos].to_string()
            } else {
                snippet
            };
            source_budget.used = snippet.len();
            used_bytes += source_budget.used;
            Some(snippet)
        } else {
            None
        }
    } else {
        None
    };
    let mut rollover = source_budget.rollover();

    // 5. Build callers (incoming CALLS)
    //
    // `callers_total` counts every distinct matching caller, independent of
    // max_refs/byte-budget capping, so the response can honestly say how many
    // were dropped instead of asserting completeness it doesn't have. Once a
    // cap is hit we stop resolving+pushing refs (`still_adding = false`) but
    // keep scanning edges already in hand to finish the count.
    let mut callers_budget = SectionBudget::new(alloc_share(CALLERS_PCT, has_callers), rollover);
    let (mut callers, callers_total) = if wants_callers {
        let mut caller_refs = Vec::new();
        let mut caller_total = 0usize;
        let mut still_adding = true;
        let mut seen_caller_ids = std::collections::HashSet::new();

        // Determine which symbol IDs to collect callers for
        let is_class_symbol = symbol.kind == "class";
        let target_ids: Vec<i64> = if is_class_symbol {
            // For class symbols, find all methods and collect callers for each
            let all_symbols = indexer
                .db()
                .get_symbols_for_file(&symbol.file_path, ctx.graph_version)?;
            let mut ids: Vec<i64> = all_symbols
                .into_iter()
                .filter(|s| {
                    (s.kind == "method" || s.kind == "function")
                        && s.start_line >= symbol.start_line
                        && s.end_line <= symbol.end_line
                })
                .map(|s| s.id)
                .collect();
            // Also include the class itself
            ids.push(symbol.id);
            ids
        } else {
            vec![symbol.id]
        };
        // Issue #122: calls through an interface-typed receiver bind to the
        // interface method; count them as callers of the implementing method.
        let mut target_ids = target_ids;
        let mut via_interface_ids = std::collections::HashSet::new();
        // Closed generic args of the impls each dispatch-only interface
        // method stands in for (issue #185): a call typed `IA<int>` is not a
        // caller of the `IA<string>` explicit impl.
        let own_ids = target_ids.clone();
        let mut via_impl_args: std::collections::HashMap<i64, Vec<Option<String>>> =
            std::collections::HashMap::new();
        for (iface, imp) in indexer.db().dispatch_pairs(&own_ids, ctx.graph_version)? {
            if own_ids.contains(&imp) && !own_ids.contains(&iface) {
                if !target_ids.contains(&iface) {
                    target_ids.push(iface);
                }
                via_interface_ids.insert(iface);
                let args = indexer.db().get_symbol_by_id(imp)?.and_then(|s| {
                    crate::db::closed_impl_args(&s.qualname, &s.name).map(String::from)
                });
                via_impl_args.entry(iface).or_default().push(args);
            }
        }

        for target_id in &target_ids {
            // Get edges for this target
            let target_edges = if *target_id == symbol.id {
                edges.clone()
            } else {
                indexer.db().edges_for_symbol(
                    *target_id,
                    ctx.languages.as_deref(),
                    ctx.graph_version,
                )?
            };

            let receivers = match via_impl_args.get(target_id) {
                Some(_) => indexer
                    .db()
                    .call_receiver_types(&target_edges.iter().map(|e| e.id).collect::<Vec<_>>())?,
                None => Default::default(),
            };
            // Collect resolved callers
            for edge in &target_edges {
                if let Some(impls) = via_impl_args.get(target_id) {
                    let call_args = receivers
                        .get(&edge.id)
                        .and_then(|t| crate::db::type_args(t));
                    if !impls
                        .iter()
                        .any(|args| crate::db::dispatch_compatible(call_args, args.as_deref()))
                    {
                        continue;
                    }
                }
                if edge.kind == "CALLS"
                    && !edge.is_synthetic()
                    && edge.target_symbol_id == Some(*target_id)
                    && meets_min_resolution(&edge.resolution_kind)
                    && let Some(source_id) = edge.source_symbol_id
                    && seen_caller_ids.insert(source_id)
                {
                    caller_total += 1;
                    if !still_adding {
                        continue;
                    }
                    if caller_refs.len() >= max_refs {
                        still_adding = false;
                        continue;
                    }
                    if let Ok(Some(caller_sym)) = indexer.db().get_symbol_by_id(source_id) {
                        let evidence = edge.evidence_snippet.clone();
                        let ref_json = serde_json::to_string(&caller_sym).unwrap_or_default();
                        let ref_bytes = ref_json.len() + evidence.as_ref().map_or(0, |e| e.len());
                        if !callers_budget.admits(
                            ref_bytes,
                            caller_refs.is_empty(),
                            used_bytes,
                            max_bytes,
                        ) {
                            still_adding = false;
                            continue;
                        }
                        callers_budget.used += ref_bytes;
                        caller_refs.push(ExplainRef {
                            symbol: caller_sym,
                            evidence,
                            edge_kind: "CALLS".to_string(),
                            protocol_context: None,
                            resolution_kind: edge.resolution_kind.clone(),
                            via_interface: via_interface_ids.contains(target_id),
                        });
                    }
                }
            }
        }

        for r in &incoming_cross {
            if !meets_min_resolution(&r.resolution_kind) {
                continue;
            }
            if !seen_caller_ids.insert(r.symbol.id) {
                continue;
            }
            caller_total += 1;
            if !still_adding {
                continue;
            }
            if caller_refs.len() >= max_refs {
                still_adding = false;
                continue;
            }
            let ref_bytes = serde_json::to_string(r).map_or(0, |j| j.len());
            if !callers_budget.admits(ref_bytes, caller_refs.is_empty(), used_bytes, max_bytes) {
                still_adding = false;
                continue;
            }
            callers_budget.used += ref_bytes;
            caller_refs.push(r.clone());
        }
        used_bytes += callers_budget.used;
        (Some(caller_refs), caller_total)
    } else {
        (None, 0)
    };
    rollover = callers_budget.rollover();

    // 6. Build callees (outgoing CALLS) - FIX #3: For class symbols, aggregate from methods
    //
    // Same honest-counting shape as callers: `callee_total` counts every
    // distinct match, `still_adding` gates whether we still resolve+push.
    let mut callees_budget = SectionBudget::new(alloc_share(CALLEES_PCT, has_callees), rollover);
    let (mut callees, callees_total) = if wants_callees {
        let mut callee_refs = Vec::new();
        let mut callee_total = 0usize;
        let mut still_adding = true;
        let mut seen_callee_ids = std::collections::HashSet::new();

        // Determine if this is a class-level symbol
        let is_class_symbol = symbol.kind == "class";

        if is_class_symbol {
            // For class symbols, find all methods in the same file within the class's line range
            let all_symbols = indexer
                .db()
                .get_symbols_for_file(&symbol.file_path, ctx.graph_version)?;
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
                let method_edges = indexer.db().edges_for_symbol_with_dispatch(
                    method.id,
                    ctx.languages.as_deref(),
                    ctx.graph_version,
                )?;

                for edge in &method_edges {
                    if edge.kind == "CALLS" && edge.source_symbol_id == Some(method.id) {
                        // `target_symbol_id` is NULL means the write path could not
                        // attribute this call (ambiguous or unresolved receiver) —
                        // the read path must not invent one via fuzzy qualname
                        // lookup (that's how a C# `value.Trim()` call used to
                        // surface a Python `trim` function as its callee).
                        let target_id = edge.target_symbol_id;
                        if let Some(target_id) = target_id
                            && meets_min_resolution(&edge.resolution_kind)
                            && seen_callee_ids.insert(target_id)
                        {
                            callee_total += 1;
                            if !still_adding {
                                continue;
                            }
                            if callee_refs.len() >= max_refs {
                                still_adding = false;
                                continue;
                            }
                            if let Ok(Some(callee_sym)) = indexer.db().get_symbol_by_id(target_id) {
                                let evidence = edge.evidence_snippet.clone();
                                let ref_json =
                                    serde_json::to_string(&callee_sym).unwrap_or_default();
                                let ref_bytes =
                                    ref_json.len() + evidence.as_ref().map_or(0, |e| e.len());
                                if !callees_budget.admits(
                                    ref_bytes,
                                    callee_refs.is_empty(),
                                    used_bytes,
                                    max_bytes,
                                ) {
                                    still_adding = false;
                                    continue;
                                }
                                callees_budget.used += ref_bytes;
                                callee_refs.push(ExplainRef {
                                    symbol: callee_sym,
                                    evidence,
                                    edge_kind: "CALLS".to_string(),
                                    protocol_context: None,
                                    resolution_kind: edge.resolution_kind.clone(),
                                    via_interface: edge.is_synthetic(),
                                });
                            }
                        }
                    }
                }
            }
        } else {
            // For non-class symbols, use direct edges
            for edge in &edges {
                if edge.kind == "CALLS" && edge.source_symbol_id == Some(symbol.id) {
                    // See the class-symbol branch above: NULL target_symbol_id
                    // means the write path deliberately refused to attribute
                    // this call, so the read path must not guess one either.
                    let target_id = edge.target_symbol_id;
                    if let Some(target_id) = target_id
                        && meets_min_resolution(&edge.resolution_kind)
                        && seen_callee_ids.insert(target_id)
                    {
                        callee_total += 1;
                        if !still_adding {
                            continue;
                        }
                        if callee_refs.len() >= max_refs {
                            still_adding = false;
                            continue;
                        }
                        if let Ok(Some(callee_sym)) = indexer.db().get_symbol_by_id(target_id) {
                            let evidence = edge.evidence_snippet.clone();
                            let ref_json = serde_json::to_string(&callee_sym).unwrap_or_default();
                            let ref_bytes =
                                ref_json.len() + evidence.as_ref().map_or(0, |e| e.len());
                            if !callees_budget.admits(
                                ref_bytes,
                                callee_refs.is_empty(),
                                used_bytes,
                                max_bytes,
                            ) {
                                still_adding = false;
                                continue;
                            }
                            callees_budget.used += ref_bytes;
                            callee_refs.push(ExplainRef {
                                symbol: callee_sym,
                                evidence,
                                edge_kind: "CALLS".to_string(),
                                protocol_context: None,
                                resolution_kind: edge.resolution_kind.clone(),
                                via_interface: edge.is_synthetic(),
                            });
                        }
                    }
                }
            }
        }

        for r in &outgoing_cross {
            if !meets_min_resolution(&r.resolution_kind) {
                continue;
            }
            if !seen_callee_ids.insert(r.symbol.id) {
                continue;
            }
            callee_total += 1;
            if !still_adding {
                continue;
            }
            if callee_refs.len() >= max_refs {
                still_adding = false;
                continue;
            }
            let ref_bytes = serde_json::to_string(r).map_or(0, |j| j.len());
            if !callees_budget.admits(ref_bytes, callee_refs.is_empty(), used_bytes, max_bytes) {
                still_adding = false;
                continue;
            }
            callees_budget.used += ref_bytes;
            callee_refs.push(r.clone());
        }
        used_bytes += callees_budget.used;
        (Some(callee_refs), callee_total)
    } else {
        (None, 0)
    };
    rollover = callees_budget.rollover();

    // 7. Find tests (incoming CALLS from test files)
    let mut tests_budget = SectionBudget::new(alloc_share(TESTS_PCT, has_tests), rollover);
    let (mut tests, tests_total) = if wants_tests {
        let mut test_refs = Vec::new();
        let mut test_total = 0usize;
        let mut still_adding = true;
        let mut calls_test_ids = std::collections::HashSet::new();
        // Tests calling the interface method this symbol implements reach it
        // only via dispatch: listed, but marked, after the direct ones.
        let interface_edges = indexer.db().interface_caller_edges(
            symbol.id,
            ctx.languages.as_deref(),
            ctx.graph_version,
        )?;
        for (edge, via_interface) in edges
            .iter()
            .filter(|e| !e.is_synthetic())
            .map(|e| (e, false))
            .chain(interface_edges.iter().map(|e| (e, true)))
        {
            if edge.kind == "CALLS"
                && (via_interface || edge.target_symbol_id == Some(symbol.id))
                && meets_min_resolution(&edge.resolution_kind)
                && let Some(source_id) = edge.source_symbol_id
                && let Ok(Some(test_sym)) = indexer.db().get_symbol_by_id(source_id)
                && is_test_symbol(&test_sym)
            {
                if !calls_test_ids.insert(test_sym.id) {
                    continue;
                }
                test_total += 1;
                if !still_adding {
                    continue;
                }
                let ref_json = serde_json::to_string(&test_sym).unwrap_or_default();
                let ref_bytes = ref_json.len();
                if !tests_budget.admits(ref_bytes, test_refs.is_empty(), used_bytes, max_bytes) {
                    still_adding = false;
                    continue;
                }
                tests_budget.used += ref_bytes;
                test_refs.push(ExplainRef {
                    symbol: test_sym,
                    evidence: edge.evidence_snippet.clone(),
                    edge_kind: "CALLS".to_string(),
                    protocol_context: None,
                    resolution_kind: edge.resolution_kind.clone(),
                    via_interface,
                });
                if test_refs.len() >= max_refs {
                    still_adding = false;
                }
            }
        }
        // Tests reaching the symbol over RPC/HTTP/a channel (e.g. a gRPC
        // client test against a service impl) count too.
        for r in &incoming_cross {
            if !is_test_symbol(&r.symbol)
                || !meets_min_resolution(&r.resolution_kind)
                || !calls_test_ids.insert(r.symbol.id)
            {
                continue;
            }
            test_total += 1;
            if !still_adding {
                continue;
            }
            let ref_bytes = serde_json::to_string(r).map_or(0, |j| j.len());
            if !tests_budget.admits(ref_bytes, test_refs.is_empty(), used_bytes, max_bytes) {
                still_adding = false;
                continue;
            }
            tests_budget.used += ref_bytes;
            test_refs.push(r.clone());
            if test_refs.len() >= max_refs {
                still_adding = false;
            }
        }
        used_bytes += tests_budget.used;
        (Some(test_refs), test_total)
    } else {
        (None, 0)
    };
    rollover = tests_budget.rollover();

    // 7.5. Issue #68: an empty tests list means two different things -- "no
    // test-scope files were ever indexed" or "tests exist but none reach
    // this symbol". Only the first is worth a warning; the second is a
    // genuine "no" and would be noise. Reuses #63's scope-count query, so
    // this only runs when the tests section was requested and came back
    // empty.
    if wants_tests
        && tests_total == 0
        && !indexer
            .db()
            .has_test_scope_files(ctx.languages.as_deref(), ctx.graph_version)?
    {
        warnings.push(
            "No test-scope files exist in this index, so the empty tests list doesn't mean \
             this symbol is untested -- it means lidx found no files it classifies as \
             tests (tests are detected by file path, so tests living inline in an \
             otherwise-non-test file, e.g. Rust's #[cfg(test)] modules, won't count)."
                .to_string(),
        );
    }

    // 8. Find implements (EXTENDS/IMPLEMENTS/INHERITS edges) - FIX #2
    //
    // Same honest-counting shape as callers/callees/tests: `implements_total`
    // counts every distinct match, `still_adding` gates whether we still
    // collect once max_refs or the byte budget is hit.
    let mut implements_budget =
        SectionBudget::new(alloc_share(IMPLEMENTS_PCT, has_implements), rollover);
    let (implements, implements_total) = if wants_implements {
        let mut impl_syms = Vec::new();
        let mut impl_total = 0usize;
        let mut still_adding = true;
        // Outgoing supertypes, plus (issue #122) implementors of an
        // interface: incoming IMPLEMENTS edges name the implementing type.
        let mut related: Vec<i64> = Vec::new();
        for edge in &edges {
            if !matches!(edge.kind.as_str(), "EXTENDS" | "IMPLEMENTS" | "INHERITS") {
                continue;
            }
            let other = if edge.source_symbol_id == Some(symbol.id) {
                edge.target_symbol_id
            } else if edge.kind == "IMPLEMENTS" && edge.target_symbol_id == Some(symbol.id) {
                edge.source_symbol_id
            } else {
                None
            };
            if let Some(id) = other
                && !related.contains(&id)
            {
                related.push(id);
            }
        }
        // The implements edge of an interface may be bound to a same-named
        // module symbol, so ask the shared type lookup as well.
        if matches!(
            symbol.kind.as_str(),
            "interface" | "class" | "struct" | "trait"
        ) {
            for id in indexer
                .db()
                .implementing_types(symbol.id, ctx.graph_version)?
            {
                if !related.contains(&id) {
                    related.push(id);
                }
            }
        }
        for target_id in related {
            if let Ok(Some(impl_sym)) = indexer.db().get_symbol_by_id(target_id) {
                impl_total += 1;
                if !still_adding {
                    continue;
                }
                let ref_bytes = serde_json::to_string(&impl_sym).unwrap_or_default().len();
                if !implements_budget.admits(ref_bytes, impl_syms.is_empty(), used_bytes, max_bytes)
                {
                    still_adding = false;
                    continue;
                }
                implements_budget.used += ref_bytes;
                impl_syms.push(impl_sym);
                if impl_syms.len() >= max_refs {
                    still_adding = false;
                }
            }
        }
        used_bytes += implements_budget.used;
        (Some(impl_syms), impl_total)
    } else {
        (None, 0)
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
    let format = params
        .format
        .as_deref()
        .unwrap_or(super::validate::FORMAT_FULL);
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
    if format == super::validate::FORMAT_SIGNATURES {
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
        json!({"method": "analyze_impact", "params": {"id": symbol.id, "direction": "both"}, "description": "Explore full graph neighborhood via impact analysis"}),
        json!({"method": "gather_context", "params": {"seeds": [{"type": "symbol", "qualname": symbol.qualname}], "max_bytes": 80000}, "description": "Assemble full context"}),
        // Issue #97: point at the exact source for the symbol this call just resolved,
        // so understanding (explain_symbol) leads straight to reading (read_symbol).
        json!({"method": "read_symbol", "params": {"qualname": symbol.qualname}, "description": format!("Read exact source of {}", symbol.name)}),
    ];

    // Honest truncation: true if the source snippet was cut, or if any
    // returned section holds fewer items than actually exist -- regardless of
    // whether the shortfall came from max_refs or the byte budget.
    let truncated = source_truncated
        || callers.as_ref().is_some_and(|c| c.len() < callers_total)
        || callees.as_ref().is_some_and(|c| c.len() < callees_total)
        || tests.as_ref().is_some_and(|t| t.len() < tests_total)
        || implements
            .as_ref()
            .is_some_and(|i| i.len() < implements_total);

    // `commit_sha`/`graph_version` are constant for the whole response --
    // captured once here, before `symbol` moves into the struct below, so
    // they can be stamped onto the envelope instead of every nested symbol.
    let graph_version = symbol.graph_version;
    let commit_sha = symbol.commit_sha.clone();

    let result = ExplainSymbolResult {
        symbol,
        source,
        callers_total: callers.as_ref().map(|_| callers_total),
        callers,
        callees_total: callees.as_ref().map(|_| callees_total),
        callees,
        tests_total: tests.as_ref().map(|_| tests_total),
        tests,
        implements_total: implements.as_ref().map(|_| implements_total),
        implements,
        graph_version,
        commit_sha,
        budget: BudgetInfo {
            budget_bytes: max_bytes,
            used_bytes,
            truncated,
            requested_bytes: if max_bytes_clamped {
                requested_max_bytes
            } else {
                None
            },
        },
        next_hops,
        warnings,
    };

    // `graph_version`/`commit_sha` live once on `ExplainSymbolResult`
    // (issue #66); the generic dispatch-boundary hoist in `rpc::mod`
    // (`hoist_symbol_run_metadata`, applied to every method's result in
    // `handle_method`) strips the copies `Symbol`'s derive still stamps
    // onto every nested symbol -- the main `symbol`, each `ExplainRef.symbol`
    // in `callers`/`callees`/`tests`, and each entry of `implements` -- since
    // the top-level `graph_version` field above is already present.
    let mut response = serde_json::to_value(&result)?;
    // Issue #235: disclose how the symbol was resolved (fuzzy fallback etc.).
    resolved.annotate(&mut response);
    Ok(response)
}

/// Client side of each bridge pair (see `bridge_complement`): the kinds an
/// explained symbol's *outgoing* cross-boundary edges carry.
const CLIENT_BRIDGE_KINDS: &[&str] = &["RPC_CALL", "HTTP_CALL", "CHANNEL_PUBLISH", "CONFIG_READ"];
/// Server side of each bridge pair: the kinds a callee-of-a-bridge carries.
const SERVER_BRIDGE_KINDS: &[&str] = &[
    "RPC_IMPL",
    "HTTP_ROUTE",
    "CHANNEL_SUBSCRIBE",
    "CONFIG_SOURCE",
];

/// One-hop cross-boundary neighbours of `seeds` for explain_symbol, found by
/// running trace_flow's own traversal (direct resolved edges + bridge
/// crossing by exact target_qualname) with `max_hops: 1` (issue #121:
/// `max_hops` now bounds the maximum returned hop distance directly, so
/// "one hop" means `max_hops: 1`, not `0`).
///
/// Every ref is labelled with the *client-side* kind (RPC_CALL, HTTP_CALL,
/// CHANNEL_PUBLISH, CONFIG_READ) or CONFIG_BIND, so a test -> impl gRPC hop
/// reads RPC_CALL from both ends.
///
/// RPC_CALL fan-out: a C# client call emits one RPC_CALL per candidate
/// (package, service) pair, all with a NULL target. Only candidates that
/// bridge to a real RPC_IMPL surface, deduped by target symbol, so the
/// unresolvable guesses never appear or count toward `callees_total`.
fn cross_boundary_refs(
    db: &crate::db::Db,
    seeds: &[i64],
    outgoing: bool,
    ctx: &HandlerContext,
) -> Result<Vec<ExplainRef>> {
    // Outgoing starts from the client kinds and crosses to the server side;
    // incoming starts from the server kinds and crosses back to the clients.
    let mut allowed_kinds: Vec<String> = if outgoing {
        CLIENT_BRIDGE_KINDS
    } else {
        SERVER_BRIDGE_KINDS
    }
    .iter()
    .map(|k| k.to_string())
    .collect();
    if outgoing {
        // Resolved method -> options-class binding; a direct edge, no bridge.
        allowed_kinds.push("CONFIG_BIND".to_string());
    }
    let config = crate::traversal::TraceConfig {
        max_hops: 1,
        max_bytes: usize::MAX,
        allowed_kinds,
        ..Default::default()
    };
    let hops = crate::traversal::trace_flow(
        db,
        seeds.to_vec(),
        None,
        ctx.languages.as_deref(),
        ctx.graph_version,
        &config,
    )?
    .hops;
    let mut refs: Vec<ExplainRef> = hops
        .into_iter()
        .filter_map(|hop| {
            let edge_kind = if config.allowed_kinds.contains(&hop.edge_kind) {
                // Direct edge from a seed. Incoming wants only bridged hops.
                if !outgoing {
                    return None;
                }
                hop.edge_kind
            } else if outgoing {
                // Bridged hop carries the far (server) side's kind.
                crate::indexer::channel::bridge_complement(&hop.edge_kind)?[0].to_string()
            } else if CLIENT_BRIDGE_KINDS.contains(&hop.edge_kind.as_str()) {
                hop.edge_kind
            } else {
                // e.g. RPC_ROUTE: the .proto declaration, not a caller.
                return None;
            };
            Some(ExplainRef {
                symbol: hop.symbol,
                via_interface: false,
                evidence: hop.snippet,
                edge_kind,
                protocol_context: hop.protocol_context,
                resolution_kind: hop.resolution_kind,
            })
        })
        .collect();
    if !outgoing {
        // CONFIG_BIND is resolved and unbridged, so trace_flow's downstream
        // walk never sees it arriving; read it straight off the seeds.
        for &seed in seeds {
            for edge in db.edges_for_symbol(seed, ctx.languages.as_deref(), ctx.graph_version)? {
                if edge.kind == "CONFIG_BIND"
                    && edge.target_symbol_id == Some(seed)
                    && let Some(source_id) = edge.source_symbol_id
                    && let Some(sym) = db.get_symbol_by_id(source_id)?
                {
                    refs.push(ExplainRef {
                        symbol: sym,
                        via_interface: false,
                        evidence: edge.evidence_snippet,
                        edge_kind: edge.kind,
                        protocol_context: None,
                        resolution_kind: edge.resolution_kind,
                    });
                }
            }
        }
    }
    Ok(refs)
}

// ---------------------------------------------------------------------------
// GROUP 4 -- Metrics handlers
// ---------------------------------------------------------------------------

pub(super) fn handle_orient(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: OrientParams = super::parse_params("orient", params)?;
    use super::validate::{VIEW_ALL, VIEW_MAP, VIEW_MODULES, VIEW_OVERVIEW};
    super::validate::require_one_of(
        "view",
        params.view.as_deref(),
        super::validate::ORIENT_VIEWS,
    )?;
    let view = params.view.as_deref().unwrap_or(VIEW_ALL);
    let ctx = HandlerContext::new(indexer, params.common)?;

    // Resolve optional focus symbol via resolve module
    let focus_sym: Option<crate::resolve::Resolved> = if let Some(ref qn) = params.focus_qualname {
        Some(crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Qualname(qn.clone()),
            ctx.languages.as_deref(),
            ctx.graph_version,
        )?)
    } else if let Some(ref query) = params.focus_query {
        Some(crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query(query.clone()),
            ctx.languages.as_deref(),
            ctx.graph_version,
        )?)
    } else {
        None
    };

    let mut result = serde_json::Map::new();

    let include_overview = matches!(view, VIEW_ALL | VIEW_OVERVIEW);
    let include_map = matches!(view, VIEW_ALL | VIEW_MAP);
    let include_modules = matches!(view, VIEW_ALL | VIEW_MODULES);

    if include_overview {
        let overview = indexer.db().repo_overview(
            indexer.repo_root().clone(),
            ctx.languages.as_deref(),
            ctx.graph_version,
        )?;
        result.insert("overview".to_string(), json!(overview));
    }

    if include_map {
        let max_bytes = params.max_bytes.unwrap_or(8000).clamp(1000, 50000);
        let config = crate::repo_map::RepoMapConfig {
            max_bytes,
            languages: ctx.languages.clone(),
            paths: ctx.paths.clone(),
            graph_version: ctx.graph_version,
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
            ctx.languages.as_deref(),
            ctx.paths.as_deref(),
            ctx.graph_version,
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
        let edges =
            indexer
                .db()
                .module_edges(depth, ctx.languages.as_deref(), ctx.graph_version)?;
        let module_edges: Vec<ModuleEdge> = edges
            .into_iter()
            .map(|(src, dst, calls, imports, xrefs)| ModuleEdge {
                source_module: src,
                target_module: dst,
                call_count: calls,
                import_count: imports,
                xref_count: xrefs,
            })
            .collect();
        result.insert("modules".to_string(), json!(modules));
        result.insert("module_edges".to_string(), json!(module_edges));
    }

    // Include focus symbol metadata when provided
    let mut focus_resolution = None;
    if let Some(resolved) = focus_sym {
        let sym = &resolved.symbol;
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
        focus_resolution = Some(resolved);
    }

    let mut response = Value::Object(result);
    // Issue #235: disclosure sits at the top level, like every other method.
    if let Some(resolved) = focus_resolution {
        resolved.annotate(&mut response);
    }
    Ok(response)
}

pub(super) fn handle_repo_map(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: RepoMapParams = super::parse_params("repo_map", params)?;
    let ctx = HandlerContext::new(indexer, params.common)?;
    let max_bytes = params.max_bytes.unwrap_or(8000).clamp(1000, 50000);

    let config = crate::repo_map::RepoMapConfig {
        max_bytes,
        languages: ctx.languages.clone(),
        paths: ctx.paths.clone(),
        graph_version: ctx.graph_version,
    };
    let map_result = crate::repo_map::build_repo_map(indexer.db(), &config)?;

    if map_result.modules == 0 {
        // Issue #65: zero modules is ambiguous -- it could mean "the
        // languages/paths filter matched nothing in an otherwise-populated
        // index" or "nothing is indexed at all". Disambiguate by re-running
        // the same aggregate with every filter dropped: if that's also
        // empty, the index itself is empty.
        let filter_applied = ctx.languages.is_some() || ctx.paths.is_some();
        let index_empty = if filter_applied {
            indexer
                .db()
                .module_summary(1, None, None, ctx.graph_version)?
                .is_empty()
        } else {
            true
        };
        let mut next_hops: Vec<serde_json::Value> = Vec::new();
        let warnings: Vec<String> = if index_empty {
            next_hops.push(json!({
                "method": "reindex",
                "params": {},
                "description": "Nothing is indexed yet -- reindex the repo before calling repo_map",
            }));
            vec![
                "Nothing is indexed for this repo at this graph version -- reindex before calling repo_map."
                    .to_string(),
            ]
        } else {
            next_hops.push(json!({
                "method": "repo_map",
                "params": {},
                "description": "Retry without the languages/paths filter to see the full repo map",
            }));
            vec![
                "The languages/paths filter matched no indexed files -- widen or drop the filter to see the repo map."
                    .to_string(),
            ]
        };
        return Ok(json!({
            "text": map_result.text,
            "modules": map_result.modules,
            "symbols": map_result.symbols,
            "bytes": map_result.bytes,
            "counts": {
                "modules": map_result.modules,
                "symbols": map_result.symbols,
            },
            "index_empty": index_empty,
            "warnings": warnings,
            "next_hops": next_hops,
        }));
    }

    Ok(json!({
        "text": map_result.text,
        "modules": map_result.modules,
        "symbols": map_result.symbols,
        "bytes": map_result.bytes,
    }))
}

pub(super) fn handle_dead_symbols(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: DeadSymbolsParams = super::parse_params("dead_symbols", params)?;
    super::validate::require_at_least_one("limit", params.limit)?;
    let ctx = HandlerContext::new(indexer, params.common)?;
    let limit = params.limit.unwrap_or(50);
    let include_unused_imports = params.include_unused_imports.unwrap_or(true);
    let include_orphan_tests = params.include_orphan_tests.unwrap_or(true);

    let dead_syms = indexer.db().dead_symbols(
        limit,
        ctx.languages.as_deref(),
        ctx.paths.as_deref(),
        ctx.graph_version,
    )?;

    let unused_imports = if include_unused_imports {
        indexer.db().unused_imports(
            limit,
            ctx.languages.as_deref(),
            ctx.paths.as_deref(),
            ctx.graph_version,
            indexer.repo_root(),
        )?
    } else {
        vec![]
    };

    let orphan_tests = if include_orphan_tests {
        indexer.db().orphan_tests(
            limit,
            ctx.languages.as_deref(),
            ctx.paths.as_deref(),
            ctx.graph_version,
        )?
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
    let params: TopComplexityParams = super::parse_params("top_complexity", params)?;
    super::validate::require_at_least_one("limit", params.limit)?;
    let ctx = HandlerContext::new(indexer, params.common)?;
    let limit = params.limit.unwrap_or(10);
    let min_complexity = params.min_complexity.unwrap_or(1);
    let results = indexer.db().top_complexity(
        limit,
        min_complexity,
        ctx.languages.as_deref(),
        ctx.paths.as_deref(),
        ctx.graph_version,
    )?;

    if results.is_empty() {
        // Issue #65: an empty ranking is ambiguous on its own -- it could mean
        // "every function in scope is below min_complexity" (metrics exist) or
        // "no function/method symbols were ever extracted for this scope"
        // (metrics never existed: an unsupported/unindexed language, or a
        // paths filter matching nothing). Report both explicitly instead of a
        // bare `[]`.
        //
        // Whether metrics exist at all is answered by re-running the same
        // query with the complexity floor lifted (`i64::MIN`) and a 1-row
        // limit, rather than a second query carrying its own copy of the
        // join/version/path-filter SQL.
        let metrics_exist = !indexer
            .db()
            .top_complexity(
                1,
                i64::MIN,
                ctx.languages.as_deref(),
                ctx.paths.as_deref(),
                ctx.graph_version,
            )?
            .is_empty();
        let mut next_hops: Vec<serde_json::Value> = Vec::new();
        // `limit:0` is rejected up front, so an empty ranking here is never
        // the limit's doing.
        let warnings: Vec<String> = if metrics_exist {
            if min_complexity > 1 {
                let mut retry_params = serde_json::Map::new();
                retry_params.insert("min_complexity".to_string(), json!(1));
                if let Some(ref langs) = ctx.languages {
                    retry_params.insert("languages".to_string(), json!(langs));
                }
                if let Some(ref paths) = ctx.paths {
                    retry_params.insert("paths".to_string(), json!(paths));
                }
                next_hops.push(json!({
                    "method": "top_complexity",
                    "params": retry_params,
                    "description": "Retry with min_complexity:1 to see the full (unfiltered) ranking",
                }));
            }
            vec![format!(
                "No symbol reached min_complexity:{min_complexity} in this scope -- complexity metrics exist, the scope is just uniformly simple relative to the threshold."
            )]
        } else {
            vec![
                "No complexity metrics exist for this scope at all -- no function/method symbols were extracted for the requested languages/paths (unsupported or unindexed language, or a paths filter with no matches)."
                    .to_string(),
            ]
        };
        return Ok(json!({
            "results": [],
            "counts": { "results": 0 },
            "metrics_exist": metrics_exist,
            "warnings": warnings,
            "next_hops": next_hops,
        }));
    }

    // Issue: this used to be a bare `Ok(json!(results))`, so the response's
    // top-level shape depended on whether `results` was empty (an object
    // above, a bare array here). That made `hoist_symbol_run_metadata`
    // (`rpc/mod.rs`) unable to hoist `graph_version`/`commit_sha` on the
    // non-empty path -- there's nowhere to hoist a field to on a bare array
    // -- so every entry repeated it. Always return an object, mirroring the
    // empty path's `results`/`counts` field names, so the shape is uniform
    // and the generic hoist can do its job.
    let count = results.len();
    Ok(json!({
        "results": results,
        "counts": { "results": count },
    }))
}

pub(super) fn handle_context(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: ContextParams = super::parse_params("context", params)?;
    super::validate::require_one_of(
        "format",
        params.format.as_deref(),
        super::validate::CONTEXT_FORMATS,
    )?;
    let repo_root = indexer.repo_root().clone();
    let validated =
        super::validate::validate_repo_path("context", indexer.db(), &repo_root, &params.path)?;
    let path = validated.path;
    let ctx = HandlerContext::from_version(indexer, params.graph_version)?;
    let file_ctx = crate::context::build_file_context(
        indexer.db(),
        indexer.repo_root(),
        path,
        ctx.graph_version,
    )?;
    match params.format.as_deref() {
        Some(super::validate::FORMAT_JSON) => Ok(crate::context::format_json(&file_ctx)),
        _ => Ok(json!({ "context": crate::context::format_text(&file_ctx) })),
    }
}

// ---------------------------------------------------------------------------
// GROUP 2 -- Graph handlers
// ---------------------------------------------------------------------------

/// Rank of `kind` in `db::resolver::ALL_RESOLUTION_KINDS`'s
/// strongest-to-weakest tier order, or `None` when `kind` isn't one of
/// those values. Shared lookup behind `explain_symbol`'s `min_resolution`
/// filter (issue #67) -- unlike `validate_resolution_kinds` below, a miss
/// here isn't an error, just "unranked".
fn resolution_kind_rank(kind: &str) -> Option<usize> {
    crate::db::resolver::ALL_RESOLUTION_KINDS
        .iter()
        .position(|k| *k == kind)
}

/// Issue #81 (R3): reject an unknown or wrong-case resolution kind
/// (`"BARE_NAME"`, `"bogus"`) up front, rather than silently matching
/// nothing while `next_hops` still offers "retry without the filter" for a
/// filter that quietly did nothing. Validates against
/// `db::resolver::ALL_RESOLUTION_KINDS`, the resolver's own single source
/// of truth for the column's possible values.
fn validate_resolution_kinds(kinds: &[String]) -> Result<()> {
    if let Some(bad) = kinds
        .iter()
        .find(|k| !crate::db::resolver::ALL_RESOLUTION_KINDS.contains(&k.as_str()))
    {
        anyhow::bail!(
            "unknown resolution kind '{bad}' in exclude_resolution_kinds -- valid kinds: {}",
            crate::db::resolver::ALL_RESOLUTION_KINDS.join(", ")
        );
    }
    Ok(())
}

pub(super) fn handle_trace_flow(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let raw_params = params.clone();
    let params: TraceFlowParams = super::parse_params("trace_flow", params)?;
    super::validate::require_at_least_one("max_hops", params.max_hops)?;
    super::validate::require_at_least_one("max_bytes", params.max_bytes)?;
    super::validate::require_one_of(
        "format",
        params.format.as_deref(),
        super::validate::TRACE_FORMATS,
    )?;
    let ctx = HandlerContext::new(indexer, params.common.clone())?;
    let max_hops = params.max_hops.unwrap_or(5).min(10);
    let include_snippets = params.include_snippets.unwrap_or(true);
    let max_bytes = params.max_bytes.unwrap_or(30_000).min(200_000);
    let trace_offset = params.trace_offset.unwrap_or(0);
    let compact_mode = params.format.as_deref() == Some(super::validate::FORMAT_COMPACT);
    let direction = match params.direction.as_deref().unwrap_or("downstream") {
        "upstream" => crate::traversal::TraceDirection::Upstream,
        _ => crate::traversal::TraceDirection::Downstream,
    };
    let allowed_kinds: Vec<String> = params
        .kinds
        .clone()
        .unwrap_or_else(|| crate::traversal::TraceConfig::default().allowed_kinds);
    let exclude_resolution_kinds: Vec<String> =
        params.exclude_resolution_kinds.clone().unwrap_or_default();
    validate_resolution_kinds(&exclude_resolution_kinds)?;

    // Config URI resolution: find all symbols connected to the URI
    let config_uri_seeds: Vec<i64> = if let Some(ref qn) = params.start_qualname {
        if crate::indexer::config::is_config_uri(qn) {
            indexer
                .db()
                .source_symbols_for_config_uri(qn, &[], ctx.graph_version)?
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    // Resolve start symbol.
    // For ID lookups we propagate errors (the ID either exists or it doesn't).
    // For qualname/query lookups, and for a config URI with no connected
    // symbols, we catch resolution failure and return a structured recovery
    // payload instead of a flat {error: ...} so the caller has a path forward.
    let start_ref = if let Some(id) = params.start_id {
        crate::resolve::SymbolRef::Id(id)
    } else if let Some(ref qn) = params.start_qualname {
        if crate::indexer::config::is_config_uri(qn) {
            match config_uri_seeds.first() {
                Some(&first_id) => crate::resolve::SymbolRef::Id(first_id),
                None => {
                    return Ok(crate::resolve::build_resolution_recovery_payload(
                        indexer.db(),
                        qn,
                        &[],
                        ctx.graph_version,
                        "trace_flow",
                        &raw_params,
                    ));
                }
            }
        } else {
            crate::resolve::SymbolRef::Qualname(qn.clone())
        }
    } else if let Some(ref query) = params.query {
        crate::resolve::SymbolRef::Query(query.clone())
    } else {
        anyhow::bail!("trace_flow requires start_id, start_qualname, or query");
    };
    let resolved = match crate::resolve::resolve_or_recovery(
        indexer.db(),
        start_ref,
        ctx.languages.as_deref(),
        ctx.graph_version,
        "trace_flow",
        &raw_params,
    )? {
        Ok(resolved) => resolved,
        Err(payload) => return Ok(payload),
    };
    let start = resolved.symbol.clone();

    // Resolve optional end symbol
    let end_id = if let Some(id) = params.end_id {
        Some(id)
    } else if let Some(ref qn) = params.end_qualname {
        indexer.db().lookup_symbol_id(qn, ctx.graph_version)?
    } else {
        None
    };

    // Expand seeds: container members + config URI seeds
    let mut seed_ids = crate::resolve::expand_seeds(indexer.db(), start.id, ctx.graph_version)?;
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
        exclude_resolution_kinds,
        seed_config_uri: params
            .start_qualname
            .clone()
            .filter(|qn| crate::indexer::config::is_config_uri(qn)),
    };
    let trace_result = crate::traversal::trace_flow(
        indexer.db(),
        seed_ids,
        end_id,
        ctx.languages.as_deref(),
        ctx.graph_version,
        &config,
    )?;

    let trace = &trace_result.hops;
    let truncated = trace_result.truncated;

    // Build next_hops with continuation when truncated
    let mut next_hops: Vec<serde_json::Value> = Vec::new();
    if truncated {
        let next_offset = trace_offset + trace.len();
        // #119: echo every original param (direction, max_bytes,
        // exclude_resolution_kinds, languages, end_qualname, query, ...) by
        // cloning the raw request and overriding only trace_offset, rather
        // than hand-picking a field subset that silently dropped params
        // (and left a query-started trace with no start at all).
        let mut continue_params = raw_params.clone();
        if let Some(obj) = continue_params.as_object_mut() {
            obj.insert("trace_offset".to_string(), json!(next_offset));
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

    let lower_bound = LowerBound {
        is_lower_bound: trace_result.unresolved_reference_count > 0,
        unresolved_count: trace_result.unresolved_reference_count,
    };

    // Issue #81: suggest the filtered/unfiltered counterpart of this call,
    // where useful -- never both, since asking for the opposite of a filter
    // that wasn't applied is a no-op. `trace_flow` already always attaches
    // informational hops to a non-empty trace (the explain_symbol hops
    // above), so this one follows the same convention unconditionally
    // rather than gating on `lower_bound` (unlike `analyze_impact`, which
    // has an existing "unchanged when non-empty" contract to preserve).
    let has_exclude_filter = params
        .exclude_resolution_kinds
        .as_ref()
        .is_some_and(|k| !k.is_empty());
    // R2/R4: a retry hop must reconstruct the same trace, not a bare start --
    // a call started via `query`/`start_query` has neither start_qualname nor
    // start_id, so it falls back to the symbol `resolve_symbol` already
    // resolved it to (`start.id`); end_qualname/end_id/kinds/max_hops/
    // include_snippets all carry over too, so only the filter itself changes.
    let hop_start_params = |extra: &mut serde_json::Map<String, serde_json::Value>| {
        if let Some(ref qn) = params.start_qualname {
            extra.insert("start_qualname".to_string(), json!(qn));
        } else if let Some(id) = params.start_id {
            extra.insert("start_id".to_string(), json!(id));
        } else {
            extra.insert("start_id".to_string(), json!(start.id));
        }
        if let Some(id) = params.end_id {
            extra.insert("end_id".to_string(), json!(id));
        } else if let Some(ref qn) = params.end_qualname {
            extra.insert("end_qualname".to_string(), json!(qn));
        }
        if let Some(ref d) = params.direction {
            extra.insert("direction".to_string(), json!(d));
        }
        if let Some(ref k) = params.kinds {
            extra.insert("kinds".to_string(), json!(k));
        }
        if let Some(h) = params.max_hops {
            extra.insert("max_hops".to_string(), json!(h));
        }
        if let Some(s) = params.include_snippets {
            extra.insert("include_snippets".to_string(), json!(s));
        }
        if let Some(ref langs) = params.common.languages {
            extra.insert("languages".to_string(), json!(langs));
        }
        if let Some(gv) = params.common.graph_version {
            extra.insert("graph_version".to_string(), json!(gv));
        }
    };
    if has_exclude_filter {
        let mut retry_params = serde_json::Map::new();
        hop_start_params(&mut retry_params);
        next_hops.push(json!({
            "method": "trace_flow",
            "params": retry_params,
            "description": "Retry without the resolution-kind filter to see the full (unfiltered) trace, including heuristic edges",
        }));
    } else if trace_result.traversed_heuristic_kind {
        // R5: only suggest the filtered retry when a heuristic-kind edge was
        // actually traversed -- a non-empty trace made entirely of exact/
        // import/receiver_type/inherited edges has nothing for the filter
        // to remove.
        let mut retry_params = serde_json::Map::new();
        hop_start_params(&mut retry_params);
        retry_params.insert(
            "exclude_resolution_kinds".to_string(),
            json!(crate::db::resolver::HEURISTIC_RESOLUTION_KINDS),
        );
        next_hops.push(json!({
            "method": "trace_flow",
            "params": retry_params,
            "description": "Retry excluding heuristic name-fallback edges (bare_name, two_segment) for a higher-confidence trace",
        }));
    }

    let result = TraceFlowResult {
        start: trace_result.start,
        end: trace_result.end,
        trace: trace_result.hops,
        paths_found: trace_result.paths_found,
        reached_target: trace_result.reached_target,
        truncated,
        truncation_reason: trace_result.truncation_reason,
        budget: BudgetInfo {
            budget_bytes: trace_result.budget_bytes,
            used_bytes: trace_result.used_bytes,
            truncated,
            requested_bytes: None,
        },
        lower_bound,
        next_hops,
    };

    let mut value = serde_json::to_value(&result)?;
    if compact_mode {
        value = super::compact::apply_compact_format(value);
    }
    resolved.annotate(&mut value);
    Ok(value)
}

// ---------------------------------------------------------------------------
// GROUP 3 -- Analysis handlers
// ---------------------------------------------------------------------------

/// Build a MultiLayerConfig from AnalyzeImpactParams with a given limit.
/// `languages` is the already-normalized filter from the handler context.
fn build_impact_config(
    params: &AnalyzeImpactParams,
    limit: usize,
    languages: Option<&[String]>,
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
    if let Some(languages) = languages {
        config.direct.languages = Some(languages.to_vec());
    }
    if let Some(ref kinds) = params.kinds {
        config.direct.kinds = kinds.clone();
    }
    if let Some(ref exclude) = params.exclude_resolution_kinds {
        config.direct.exclude_resolution_kinds = exclude.clone();
    }
    config
}

/// Resolve a single batch qualname (or config URI) to seed IDs. Exact match
/// only (no fuzzy fallback) -- a batch entry that doesn't resolve this way
/// is treated as an unresolvable seed by the caller, which builds a
/// recovery payload for it. Kept separate from the analysis step below so
/// the caller can tell "seed not found" (recoverable) apart from "seed
/// found, analysis failed afterward" (a real error, not recoverable) --
/// see issue: batch analyze_impact previously labeled every per-entry
/// error "not found", even analysis failures like a `languages` filter
/// that excludes the seed's own language.
fn resolve_batch_seed_ids(
    indexer: &mut Indexer,
    qualname: &str,
    config: &crate::impact::config::MultiLayerConfig,
    graph_version: i64,
) -> Result<Vec<i64>> {
    let dir = config.direct.direction.as_str();

    if crate::indexer::config::is_config_uri(qualname) {
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
        Ok(ids)
    } else {
        let symbol = indexer
            .db()
            .get_symbol_by_qualname(qualname, graph_version)?
            .ok_or_else(|| anyhow::anyhow!("symbol not found: {}", qualname))?;
        Ok(vec![symbol.id])
    }
}

/// Build a batch entry for a qualname that produced no result, carrying the
/// real error in the legacy `layers.direct.error` field. `recovery` is
/// `Some` only when the seed itself couldn't be resolved -- never for a
/// downstream analysis failure on an already-resolved seed.
fn batch_error_entry(
    qn: &str,
    error: String,
    recovery: Option<Value>,
) -> crate::impact::types::BatchImpactEntry {
    crate::impact::types::BatchImpactEntry {
        seed_qualname: qn.to_string(),
        test_layer: None,
        seeds: vec![],
        affected: vec![],
        summary: crate::impact::types::ImpactSummary {
            by_file: vec![],
            by_relationship: std::collections::HashMap::new(),
            by_distance: std::collections::HashMap::new(),
            total_affected: 0,
        },
        truncated: false,
        truncation_reason: None,
        layers: crate::impact::types::LayerMetadata {
            direct: Some(crate::impact::types::LayerStats {
                enabled: false,
                duration_ms: 0,
                result_count: 0,
                truncated: false,
                error: Some(error),
            }),
            test: None,
            historical: None,
        },
        lower_bound: LowerBound {
            is_lower_bound: false,
            unresolved_count: 0,
        },
        recovery,
    }
}

/// Explanation attached to an `analyze_impact` response whose TEST layer ran
/// and found nothing (issue #231). The layer reports a test only when it
/// reaches the seed through graph edges, so an empty layer says the graph
/// holds no such path -- not that the code is untested. `None` when the
/// layer was disabled, errored, or reported at least one test.
fn empty_test_layer_note(
    result: &crate::impact::types::UnifiedImpactResult,
    seed_ids: &[i64],
) -> Option<serde_json::Value> {
    let seed_id = seed_ids.first().copied();
    let seed_name = result.seeds.first().map(|s| s.name.as_str());
    let max_depth = result.config.max_depth;
    let stats = result.layers.test.as_ref()?;
    if !stats.enabled || stats.error.is_some() || stats.result_count > 0 {
        return None;
    }
    let mut next_hops: Vec<serde_json::Value> = Vec::new();
    let mut explain = serde_json::Map::new();
    if let Some(id) = seed_id {
        explain.insert("id".to_string(), json!(id));
    }
    explain.insert("sections".to_string(), json!(["tests"]));
    next_hops.push(json!({
        "method": "explain_symbol",
        "params": explain,
        "description": "Inspect the seed's callers and tests directly; unresolved calls do not appear as graph edges",
    }));
    if let Some(name) = seed_name {
        next_hops.push(json!({
            "method": "search",
            "params": {"query": name},
            "description": format!(
                "Search for '{name}' in test code -- textual matches are candidates to verify, not graph evidence"
            ),
        }));
    }
    if max_depth < 10 {
        let mut deeper = serde_json::Map::new();
        if let Some(id) = seed_id {
            deeper.insert("id".to_string(), json!(id));
        }
        deeper.insert("max_depth".to_string(), json!(10));
        next_hops.push(json!({
            "method": "analyze_impact",
            "params": deeper,
            "description": format!("Retry with a deeper traversal (max_depth was {max_depth})"),
        }));
    }
    Some(json!({
        "empty": true,
        "reason": format!(
            "No test reaches the seed through graph edges within {max_depth} hops. Tests are reported only when a resolved edge path connects them to the seed (never by name similarity), so this reflects resolution coverage -- calls that could not be resolved, dynamic dispatch, or cross-language calls -- and does not mean the code is untested."
        ),
        "next_hops": next_hops,
    }))
}

pub(super) fn handle_analyze_impact(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let mut resolution = None;
    let mut response = analyze_impact_inner(indexer, params, &mut resolution)?;
    // Issue #235: one annotation covers every exit path of the handler.
    if let Some(resolved) = resolution {
        resolved.annotate(&mut response);
    }
    Ok(response)
}

/// Body of `handle_analyze_impact`. Reports through `resolution` how the seed
/// symbol was resolved, when it was resolved from an id/qualname/query.
fn analyze_impact_inner(
    indexer: &mut Indexer,
    params: Value,
    resolution: &mut Option<crate::resolve::Resolved>,
) -> Result<Value> {
    let raw_params = params.clone();
    let params: AnalyzeImpactParams = super::parse_params("analyze_impact", params)?;
    super::validate::require_at_least_one("limit", params.limit)?;
    super::validate::require_at_least_one("max_depth", params.max_depth)?;
    super::validate::require_unit_interval("min_confidence", params.min_confidence)?;
    let ctx = HandlerContext::new(indexer, params.common.clone())?;
    // Issue #81 (R3): validated once here, ahead of both the batch path
    // (`build_impact_config`) and the single-seed path below -- both read
    // `params.exclude_resolution_kinds`.
    if let Some(ref exclude) = params.exclude_resolution_kinds {
        validate_resolution_kinds(exclude)?;
    }

    // ---- Batch path: multiple qualnames in one call ----
    if let Some(ref qualnames) = params.qualnames {
        if qualnames.is_empty() {
            return Err(anyhow::anyhow!("qualnames array must not be empty"));
        }

        // Build config once (shared across all seeds)
        let total_limit = params.limit.unwrap_or(500).min(2000);
        let per_seed_limit = (total_limit / qualnames.len()).max(50);

        let base_config = build_impact_config(&params, per_seed_limit, ctx.languages.as_deref());

        let mut results = Vec::with_capacity(qualnames.len());
        let mut total_affected: usize = 0;
        let mut all_files: std::collections::HashSet<String> = std::collections::HashSet::new();

        for qn in qualnames {
            let entry = match resolve_batch_seed_ids(indexer, qn, &base_config, ctx.graph_version) {
                Ok(seed_ids) => match crate::impact::analyze_impact_multi_layer(
                    indexer.db(),
                    &seed_ids,
                    {
                        let mut c = base_config.clone();
                        if crate::indexer::config::is_config_uri(qn) {
                            c.direct.seed_config_uri = Some(qn.clone());
                        }
                        c
                    },
                    ctx.graph_version,
                ) {
                    Ok(result) => {
                        total_affected += result.summary.total_affected;
                        for fi in &result.summary.by_file {
                            all_files.insert(fi.path.clone());
                        }
                        let test_layer = empty_test_layer_note(&result, &seed_ids);
                        crate::impact::types::BatchImpactEntry {
                            seed_qualname: qn.clone(),
                            test_layer,
                            seeds: result.seeds,
                            affected: result.affected,
                            summary: result.summary,
                            truncated: result.truncated,
                            truncation_reason: result.truncation_reason,
                            layers: result.layers,
                            lower_bound: result.lower_bound,
                            recovery: None,
                        }
                    }
                    Err(e) => {
                        // The seed resolved fine -- this is a genuine analysis
                        // failure (e.g. a `languages` filter that excludes the
                        // seed's own language, so `load_seeds` finds nothing),
                        // not an unresolvable seed. No recovery payload: that
                        // would misreport a found symbol as "not found".
                        batch_error_entry(qn, e.to_string(), None)
                    }
                },
                Err(e) => {
                    // The seed itself could not be resolved -- build a structured
                    // recovery payload rather than failing the whole batch or
                    // returning a bare error message. The batch loop resolves each
                    // qualname by exact match only (no fuzzy fallback), so
                    // candidates aren't already computed the way resolve_by_query's
                    // failure carries them -- find them the same way (find_candidates
                    // is the one candidate-search algorithm).
                    let candidates =
                        crate::resolve::find_candidates(indexer.db(), qn, ctx.graph_version);
                    let recovery = crate::resolve::build_resolution_recovery_payload(
                        indexer.db(),
                        qn,
                        &candidates,
                        ctx.graph_version,
                        "analyze_impact",
                        &raw_params,
                    );
                    batch_error_entry(qn, e.to_string(), Some(recovery))
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
            let ids = indexer.db().source_symbols_for_config_uri(
                qualname,
                uri_kinds,
                ctx.graph_version,
            )?;
            if ids.is_empty() {
                return Ok(crate::resolve::build_resolution_recovery_payload(
                    indexer.db(),
                    qualname,
                    &[],
                    ctx.graph_version,
                    "analyze_impact",
                    &raw_params,
                ));
            }
            ids
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    // Resolve symbol by id, qualname, or fuzzy query (skip if config URI already resolved).
    // For qualname/query we catch resolution failure and return a structured recovery payload
    // instead of propagating a flat error — giving the caller actionable next_hops.
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
        let resolved = match crate::resolve::resolve_or_recovery(
            indexer.db(),
            sym_ref,
            ctx.languages.as_deref(),
            ctx.graph_version,
            "analyze_impact",
            &raw_params,
        )? {
            Ok(resolved) => resolved,
            Err(payload) => return Ok(payload),
        };
        let symbol = resolved.symbol.clone();
        *resolution = Some(resolved);

        // Property→parent expansion: if the seed is a property/field/attribute/const,
        // also add the parent class so CONFIG_BIND consumers are reachable
        let mut ids = vec![symbol.id];
        if matches!(
            symbol.kind.as_str(),
            "property" | "field" | "attribute" | "const"
        ) && let Some(parent_qn) = symbol.qualname.rsplit_once('.').map(|(p, _)| p)
            && let Ok(Some(parent)) = indexer
                .db()
                .get_symbol_by_qualname(parent_qn, ctx.graph_version)
            && !ids.contains(&parent.id)
        {
            ids.push(parent.id);
        }
        ids
    };

    // Used for both the traversal config and recovery next_hops on zero results.
    let direction = params.direction.unwrap_or_else(|| "both".to_string());
    // Capture before params.kinds is moved into config.
    let original_kinds: Option<Vec<String>> = params.kinds.clone();
    // Issue #81: whether this call already applied a resolution-kind filter,
    // for the filtered/unfiltered next_hops suggestion below.
    let has_exclude_filter = params
        .exclude_resolution_kinds
        .as_ref()
        .is_some_and(|k| !k.is_empty());

    // Build multi-layer configuration
    let config = crate::impact::config::MultiLayerConfig::builder()
        .max_depth(params.max_depth.unwrap_or(3).min(10))
        .direction(direction.clone())
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

    // Set languages if specified (already normalized by HandlerContext)
    if let Some(ref languages) = ctx.languages {
        config.direct.languages = Some(languages.clone());
    }

    if let Some(qn) = params.qualname.as_deref()
        && crate::indexer::config::is_config_uri(qn)
    {
        config.direct.seed_config_uri = Some(qn.to_string());
    }

    // Set kinds if specified
    if let Some(kinds) = params.kinds {
        config.direct.kinds = kinds;
    }
    if let Some(ref exclude) = params.exclude_resolution_kinds {
        config.direct.exclude_resolution_kinds = exclude.clone();
    }

    // Perform multi-layer impact analysis
    let result = crate::impact::analyze_impact_multi_layer(
        indexer.db(),
        &seed_ids,
        config,
        ctx.graph_version,
    )?;

    let test_layer_note = empty_test_layer_note(&result, &seed_ids);

    // Issue #81: suggest the filtered/unfiltered counterpart of this call,
    // where useful -- never both, since asking for the opposite of a filter
    // that wasn't applied is a no-op. Merged into whichever next_hops list
    // below actually gets returned (the zero-result recovery hops, or a
    // fresh one for a normal, non-empty result). R5: the exclude-filter
    // suggestion fires only when the direct layer actually traversed a
    // heuristic-kind edge -- `lower_bound` (pending unresolved references)
    // is a different signal and doesn't imply a heuristic edge was crossed.
    let mut resolution_next_hops: Vec<serde_json::Value> = Vec::new();
    {
        // R4: rebuild the retry from the ORIGINAL request's own identifying
        // and config params, not just the first seed id and direction -- so
        // a config-URI qualname's multi-symbol seeding, `max_depth`,
        // `kinds`, `include_tests`/`include_paths`/`limit`/`min_confidence`
        // and layer toggles all survive the retry; only
        // `exclude_resolution_kinds` itself changes.
        let mut retry_params = serde_json::Map::new();
        if let Some(id) = params.id {
            retry_params.insert("id".to_string(), json!(id));
        } else if let Some(ref qn) = params.qualname {
            retry_params.insert("qualname".to_string(), json!(qn));
        } else if let Some(ref q) = params.query {
            retry_params.insert("query".to_string(), json!(q));
        }
        retry_params.insert("direction".to_string(), json!(direction));
        if let Some(depth) = params.max_depth {
            retry_params.insert("max_depth".to_string(), json!(depth));
        }
        if let Some(ref kinds) = original_kinds {
            retry_params.insert("kinds".to_string(), json!(kinds));
        }
        if let Some(include_tests) = params.include_tests {
            retry_params.insert("include_tests".to_string(), json!(include_tests));
        }
        if let Some(include_paths) = params.include_paths {
            retry_params.insert("include_paths".to_string(), json!(include_paths));
        }
        if let Some(limit) = params.limit {
            retry_params.insert("limit".to_string(), json!(limit));
        }
        if let Some(min_confidence) = params.min_confidence {
            retry_params.insert("min_confidence".to_string(), json!(min_confidence));
        }
        if let Some(enable_direct) = params.enable_direct {
            retry_params.insert("enable_direct".to_string(), json!(enable_direct));
        }
        if let Some(enable_test) = params.enable_test {
            retry_params.insert("enable_test".to_string(), json!(enable_test));
        }
        if let Some(enable_historical) = params.enable_historical {
            retry_params.insert("enable_historical".to_string(), json!(enable_historical));
        }
        if let Some(ref langs) = params.common.languages {
            retry_params.insert("languages".to_string(), json!(langs));
        }
        if let Some(gv) = params.common.graph_version {
            retry_params.insert("graph_version".to_string(), json!(gv));
        }

        if has_exclude_filter {
            resolution_next_hops.push(json!({
                "method": "analyze_impact",
                "params": retry_params,
                "description": "Retry without the resolution-kind filter to see the full (unfiltered) impact set, including heuristic edges",
            }));
        } else if !result.affected.is_empty() && result.traversed_heuristic_kind {
            retry_params.insert(
                "exclude_resolution_kinds".to_string(),
                json!(crate::db::resolver::HEURISTIC_RESOLUTION_KINDS),
            );
            resolution_next_hops.push(json!({
                "method": "analyze_impact",
                "params": retry_params,
                "description": "Retry excluding heuristic name-fallback edges (bare_name, two_segment) for a higher-confidence impact set",
            }));
        }
    }

    // When zero symbols were affected, attach recovery next_hops so the LLM has a path
    // forward instead of a dead-end payload.
    if result.affected.is_empty() {
        let seed_id = seed_ids.first().copied();
        let seed_params = |dir: &str| {
            let mut map = serde_json::Map::new();
            if let Some(id) = seed_id {
                map.insert("id".to_string(), json!(id));
            }
            map.insert("direction".to_string(), json!(dir));
            // Keep a filtered call's filter so following the hop doesn't
            // silently widen it.
            if let Some(ref exclude) = params.exclude_resolution_kinds
                && !exclude.is_empty()
            {
                map.insert("exclude_resolution_kinds".to_string(), json!(exclude));
            }
            map
        };
        let mut next_hops: Vec<serde_json::Value> = Vec::new();

        // Suggest flipping direction. Only meaningful for an explicit upstream/downstream
        // query: "both" already traverses every edge, so a narrower retry cannot find more.
        // Also recognise aliases accepted by TraversalDirection::from (direct.rs ~30-31):
        //   up/upstream/callers/in  → upstream  → flip to "downstream"
        //   down/downstream/callees/out → downstream → flip to "upstream"
        let alt_direction = match direction.to_lowercase().as_str() {
            "upstream" | "up" | "callers" | "in" => Some("downstream"),
            "downstream" | "down" | "callees" | "out" => Some("upstream"),
            _ => None,
        };
        if let Some(alt) = alt_direction {
            next_hops.push(json!({
                "method": "analyze_impact",
                "params": seed_params(alt),
                "description": format!("Flip direction to '{}' (current '{}' found nothing)", alt, direction),
            }));
        }

        // Suggest including CONFIG edge kinds (useful for config/DI consumers).
        // Only emit this hop when the original call passed a restrictive `kinds` filter
        // that excluded some of the CONFIG/CALLS subset. When `kinds` was empty/None
        // (the default: all edge kinds), a restricted retry cannot find more results and
        // would be a guaranteed dead-end — same reasoning as the direction-flip suppression
        // above for the "both" case.
        let config_kind_set = ["CONFIG_BIND", "CONFIG_SOURCE", "CONFIG_READ", "CALLS"];
        let original_kinds_non_empty = original_kinds.as_ref().is_some_and(|k| !k.is_empty());
        let original_already_covers_config = original_kinds
            .as_ref()
            .is_some_and(|k| config_kind_set.iter().all(|ck| k.iter().any(|ok| ok == ck)));
        if original_kinds_non_empty && !original_already_covers_config {
            let mut config_params = seed_params("both");
            config_params.insert(
                "kinds".to_string(),
                json!(["CONFIG_BIND", "CONFIG_SOURCE", "CONFIG_READ", "CALLS"]),
            );
            next_hops.push(json!({
                "method": "analyze_impact",
                "params": config_params,
                "description": "Retry with CONFIG edge kinds (finds DI/config consumers missed by graph walk)",
            }));
        }

        // Suggest seeding the parent (e.g. the class when the seed is a method/property).
        // Only when the parent qualname resolves to a real symbol that was not already
        // seeded — a hop that errors when followed is worse than no hop.
        if let Some(id) = seed_id
            && let Ok(Some(sym)) = indexer.db().get_symbol_by_id(id)
            && let Some(parent_qn) = sym.qualname.rsplit_once('.').map(|(p, _)| p)
            && let Ok(Some(parent)) = indexer
                .db()
                .get_symbol_by_qualname(parent_qn, ctx.graph_version)
            && !seed_ids.contains(&parent.id)
        {
            next_hops.push(json!({
                "method": "analyze_impact",
                "params": {"qualname": parent_qn, "direction": direction},
                "description": format!("Seed parent {} '{}' instead", parent.kind, parent_qn),
            }));
        }

        next_hops.extend(resolution_next_hops);
        let mut value = serde_json::to_value(&result)?;
        if let Some(obj) = value.as_object_mut() {
            obj.insert("next_hops".to_string(), json!(next_hops));
            if let Some(note) = test_layer_note {
                obj.insert("test_layer".to_string(), note);
            }
        }
        return Ok(value);
    }

    if resolution_next_hops.is_empty() && test_layer_note.is_none() {
        Ok(json!(result))
    } else {
        let mut value = serde_json::to_value(&result)?;
        if let Some(obj) = value.as_object_mut() {
            if !resolution_next_hops.is_empty() {
                obj.insert("next_hops".to_string(), json!(resolution_next_hops));
            }
            if let Some(note) = test_layer_note {
                obj.insert("test_layer".to_string(), note);
            }
        }
        Ok(value)
    }
}

pub(super) fn handle_analyze_diff(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: AnalyzeDiffParams = super::parse_params("analyze_diff", params)?;
    super::validate::require_at_least_one("max_depth", params.max_depth)?;
    super::validate::require_at_least_one("max_bytes", params.max_bytes)?;
    // analyze_diff.paths means "changed files", not a search-path filter
    let ctx = HandlerContext::from_version(indexer, params.graph_version)?;
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let max_bytes = params.max_bytes.unwrap_or(50_000).min(200_000);
    let max_depth = params.max_depth.unwrap_or(1).min(5);
    let include_tests = params.include_tests.unwrap_or(true);
    let include_risk = params.include_risk.unwrap_or(true);

    // Step 1: Get changed files with optional line ranges
    let mut warnings: Vec<String> = Vec::new();
    let changed_files: Vec<ChangedFile> = if let Some(ref diff) = params.diff {
        parse_diff_with_ranges(diff)
    } else if let Some(ref paths) = params.paths {
        paths.iter().map(|p| ChangedFile::new(p.clone())).collect()
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
            .get_symbols_for_file(&cf.path, ctx.graph_version)
            .unwrap_or_default();
        if symbols.is_empty() {
            warnings.push(format!("Path not found in index: {}", cf.path));
            continue;
        }
        let has_ranges = cf.has_line_changes();
        // Source lines, read lazily to tell a trailing body deletion from a
        // following sibling's (see `ChangedFile::touches`).
        let mut file_lines: Option<Vec<String>> = None;
        for sym in symbols {
            let change_type = if has_ranges {
                // Only added lines and deletions inside the symbol count;
                // context lines never do.
                let def_indent = || {
                    let text = file_lines
                        .get_or_insert_with(|| {
                            std::fs::read_to_string(indexer.repo_root().join(&cf.path))
                                .map(|t| t.lines().map(str::to_owned).collect())
                                .unwrap_or_default()
                        })
                        .get(usize::try_from(sym.start_line - 1).ok()?)?;
                    text.find(|c: char| !c.is_whitespace())
                };
                if !cf.touches(sym.start_line, sym.end_line, def_indent) {
                    continue;
                }
                // Fully within one run of added lines means the symbol is new.
                let fully_added = cf.fully_added(sym.start_line, sym.end_line);
                if fully_added {
                    "added".to_string()
                } else {
                    "modified".to_string()
                }
            } else {
                // No hunk ranges to compare against (paths-only mode, no diff
                // text) -- there's no evidence any of these symbols actually
                // changed, so "modified" would be a false claim. Label
                // neutrally instead.
                "in_changed_file".to_string()
            };

            // Step 2a: Detect signature changes by comparing with previous graph version
            let mut old_signature = None;
            let new_signature = sym.signature.clone();
            let mut final_change_type = change_type.clone();

            if (change_type == "modified" || change_type == "in_changed_file")
                && ctx.graph_version > 1
            {
                // Try to find the symbol in the previous graph version
                if let Some(stable_id) = sym.stable_id.as_ref()
                    && let Ok(Some(old_sym)) = indexer
                        .db()
                        .get_symbol_by_stable_id(stable_id, ctx.graph_version - 1)
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

    // Step 3: Compute upstream impact (callers of the changed symbols) via
    // multi-level BFS (depth controlled by max_depth)
    let seed_ids: Vec<i64> = changed_symbols.iter().map(|cs| cs.symbol.id).collect();
    let mut upstream = Vec::new();
    let mut seen_ids: HashSet<i64> = seed_ids.iter().copied().collect();
    let max_upstream = 50;

    // BFS: start with changed symbols, expand callers level by level
    let mut current_level: Vec<Symbol> =
        changed_symbols.iter().map(|cs| cs.symbol.clone()).collect();
    let mut base_confidence = 0.9;

    for current_distance in 1..=max_depth {
        let mut next_level = Vec::new();

        for sym in &current_level {
            if upstream.len() >= max_upstream {
                break;
            }

            let edges = indexer.db().edges_for_symbol_with_dispatch(
                sym.id,
                languages.as_deref(),
                ctx.graph_version,
            )?;

            // Find callers via resolved edges
            for edge in &edges {
                if upstream.len() >= max_upstream {
                    break;
                }
                if edge.kind == "CALLS"
                    && edge.target_symbol_id == Some(sym.id)
                    && let Some(source_id) = edge.source_symbol_id
                    && seen_ids.insert(source_id)
                    && let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id)
                {
                    next_level.push(caller.clone());
                    upstream.push(DiffImpactEntry {
                        symbol: caller,
                        relationship: if current_distance == 1 {
                            "caller".to_string()
                        } else {
                            format!("caller_depth_{}", current_distance)
                        },
                        distance: current_distance,
                        confidence: base_confidence,
                        resolution_kind: edge.resolution_kind.clone(),
                    });
                }
            }
        }

        if next_level.is_empty() || upstream.len() >= max_upstream {
            break;
        }
        current_level = next_level;
        base_confidence *= 0.8; // Decay confidence per level
    }

    // Step 4: Test coverage
    let test_coverage = if include_tests {
        let mut coverage = Vec::new();
        for cs in &changed_symbols {
            let mut tests = Vec::new();
            let mut seen_test_ids = HashSet::new();
            // Direct callers, then tests that call the interface method this
            // one implements: those reach every implementor, so they are
            // reported as "via_interface", not direct coverage.
            let direct = indexer.db().edges_for_symbol(
                cs.symbol.id,
                languages.as_deref(),
                ctx.graph_version,
            )?;
            let via_interface = indexer.db().interface_caller_edges(
                cs.symbol.id,
                languages.as_deref(),
                ctx.graph_version,
            )?;
            for (edge, coverage_type) in direct
                .iter()
                .filter(|e| e.target_symbol_id == Some(cs.symbol.id))
                .map(|e| (e, "direct"))
                .chain(via_interface.iter().map(|e| (e, "via_interface")))
            {
                if edge.kind == "CALLS"
                    && let Some(source_id) = edge.source_symbol_id
                    && let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id)
                    && is_test_symbol(&caller)
                    && seen_test_ids.insert(source_id)
                {
                    tests.push(TestRef {
                        test_qualname: caller.qualname.clone(),
                        test_file: caller.file_path.clone(),
                        coverage_type: coverage_type.to_string(),
                    });
                }
            }
            let status = if tests.iter().any(|t| t.coverage_type == "direct") {
                "covered"
            } else if tests.is_empty() {
                "uncovered"
            } else {
                "covered_via_interface"
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
                let caller_count = upstream
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
        for impact in &upstream {
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
        let high_fan_in: Vec<_> = upstream
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
        let affected_files: HashSet<_> = upstream
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
    // Add upstream callers for top changed method/function
    if let Some(cs) = changed_symbols
        .iter()
        .find(|cs| cs.symbol.kind == "method" || cs.symbol.kind == "function")
    {
        next_hops.push(json!({"method": "analyze_impact", "params": {"id": cs.symbol.id, "direction": "upstream"}, "description": format!("Callers of {}", cs.symbol.name)}));
    }
    // Add bidirectional impact exploration for first changed symbol
    if let Some(cs) = changed_symbols.first() {
        next_hops.push(json!({"method": "analyze_impact", "params": {"id": cs.symbol.id, "direction": "both"}, "description": "Explore full impact graph"}));
    }

    let result = AnalyzeDiffResult {
        changed_symbols,
        upstream,
        test_coverage,
        risk,
        budget: BudgetInfo {
            budget_bytes: max_bytes,
            used_bytes,
            truncated: false,
            requested_bytes: None,
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
    let params: RgParams = super::parse_params("search", params)?;
    super::validate::validate_pattern_length(&params.query, "search_rg")?;
    super::validate::require_at_least_one("limit", params.limit)?;
    let limit = params.limit.unwrap_or(100).min(MAX_RESPONSE_LIMIT);
    let context_lines = normalize_rg_context(params.context_lines);
    let include_text = params.include_text.unwrap_or(true);
    let include_symbol = params.include_symbol.unwrap_or(false);
    let ctx = HandlerContext::from_version(indexer, params.graph_version)?;
    // resolve_rg_paths handles path/paths with its own normalization for ripgrep
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
        scope: params.scope,
        languages: scan::normalize_language_filter(params.languages.as_deref())?,
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
        ctx.graph_version,
        Some(&params.query),
    )?;

    if results.is_empty() {
        // Recovery next_hops: guide the LLM toward broadening the search.
        let query = &params.query;
        let fixed_string = params.fixed_string == Some(true);
        // Suggested retries must stay valid calls: a fixed-string query may not parse
        // as a regex, so carry the flag through to every re-search hop.
        let search_params = |q: &str| {
            let mut map = serde_json::Map::new();
            map.insert("query".to_string(), json!(q));
            if fixed_string {
                map.insert("fixed_string".to_string(), json!(true));
            }
            map
        };
        let mut next_hops: Vec<serde_json::Value> = Vec::new();

        // If case_sensitive was set, suggest dropping it. Explicitly set case_sensitive:false
        // to get genuinely case-insensitive matching (-i flag). Using search_params alone
        // yields case_sensitive:None which keeps ripgrep's default case-sensitive mode —
        // the same as the original query.
        if params.case_sensitive == Some(true) {
            let mut ci_params = search_params(query);
            ci_params.insert("case_sensitive".to_string(), json!(false));
            next_hops.push(json!({
                "method": "search",
                "params": ci_params,
                "description": "Retry with case_sensitive:false (matches any casing)",
            }));
        }

        // Suggest explain_symbol using the query as a best-effort symbol lookup.
        // Note: if the query does not name a known symbol this call may return an error.
        next_hops.push(json!({
            "method": "explain_symbol",
            "params": {"query": query},
            "description": format!("Try explain_symbol in case '{}' names a symbol", query),
        }));

        // Suggest a broader search using the first whitespace-separated token. Skip when
        // truncating a regex could leave an invalid fragment (e.g. an unclosed group).
        let first_token = query.split_whitespace().next().unwrap_or(query);
        let token_is_valid_pattern =
            fixed_string || !first_token.contains(['(', ')', '[', ']', '{', '}', '\\']);
        if first_token != query.as_str() && token_is_valid_pattern {
            next_hops.push(json!({
                "method": "search",
                "params": search_params(first_token),
                "description": format!("Widen pattern to first token '{}'", first_token),
            }));
        }

        return Ok(json!({
            "results": [],
            "query": query,
            "next_hops": next_hops,
        }));
    }

    // Issue #97: point each hit toward an `outline` of its file -- one hop per
    // distinct file, not per hit, since a file with several matching lines only
    // needs one skeleton. Only emitted when the file is something `outline`
    // can actually handle: an indexed language (reused from the scanner's own
    // extension-to-language detection, not a hand-rolled extension list) or
    // Markdown (read straight off disk -- see `is_markdown_path`). A hop
    // toward e.g. Cargo.toml or a .json file would just error.
    let mut hopped_paths: HashSet<String> = HashSet::new();
    for hit in results.iter_mut() {
        if !hopped_paths.insert(hit.path.clone()) {
            continue;
        }
        let path = hit.path.clone();
        let outlineable = is_markdown_path(&path)
            || scan::language_for_path(std::path::Path::new(&path)).is_some();
        if !outlineable {
            continue;
        }
        hit.next_hops = Some(vec![RpcSuggestion {
            method: "outline".to_string(),
            params: json!({"path": path}),
            description: Some(format!("Outline {}", path)),
        }]);
    }

    Ok(json!(results))
}

// ---------------------------------------------------------------------------
// GROUP 6 -- Index/meta handlers
// ---------------------------------------------------------------------------

pub(super) fn handle_reindex(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: ReindexParams = super::parse_params("reindex", params)?;
    let stats = indexer.reindex()?;

    // Optionally force another repair pass after reindexing, beyond the one
    // `reindex()` already runs when it detects work to do (see
    // `Indexer::reindex`'s `needs_repair` gate) -- issue #79 retired the
    // older, untargeted `resolve_null_target_edges` full-edge-table rescan
    // this param used to run (nothing was left for it to find once
    // `insert_edges` stopped writing a NULL-target edge for any kind but a
    // Bridge Edge), so this now forces the same store-driven repair
    // (`Db::repair_unresolved`) reindex's own gate would otherwise skip on
    // a purely-carried-forward run.
    let mut json_stats = json!(stats);
    if params.resolve_edges.unwrap_or(false) {
        let graph_version = indexer.db().current_graph_version()?;
        let (reconciled, store_resolved) =
            indexer
                .db()
                .repair_unresolved(graph_version, true, "manual resolve_edges request")?;
        // Add resolved count to stats
        if let Some(obj) = json_stats.as_object_mut() {
            obj.insert(
                "edges_resolved".to_string(),
                json!(reconciled + store_resolved),
            );
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

    Ok(super::format::apply_field_filters(
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

    let params: GatherContextParams = super::parse_params("gather_context", params)?;

    // Validate parameters
    let validation = super::validate::validate_gather_context_params(&params);
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

    let ctx = HandlerContext::new(indexer, params.common)?;

    // Moderate Concern #1: Enforce hard cap on max_bytes
    let max_bytes = params.max_bytes.unwrap_or(100_000).min(MAX_BYTES_HARD_CAP);

    // Determine strategy: default to "symbol" if all seeds are symbol seeds
    let strategy = params.strategy.or_else(|| {
        let all_symbol_seeds = params
            .seeds
            .iter()
            .all(|seed| matches!(seed, ContextSeed::Symbol { .. }));
        if all_symbol_seeds && !params.seeds.is_empty() {
            Some(gather_context::STRATEGY_SYMBOL.to_string())
        } else {
            Some(gather_context::STRATEGY_FILE.to_string())
        }
    });

    let config = gather_context::GatherConfig {
        max_bytes,
        depth: params.depth.unwrap_or(2),
        max_nodes: params.max_nodes.unwrap_or(50),
        include_snippets: params.include_snippets.unwrap_or(true),
        include_related: params.include_related.unwrap_or(true),
        dry_run: params.dry_run.unwrap_or(false),
        languages: ctx.languages,
        paths: ctx.paths,
        graph_version: ctx.graph_version,
        strategy,
    };

    let result =
        gather_context::gather_context(indexer.db(), indexer.repo_root(), &params.seeds, &config)?;

    Ok(json!(result))
}

pub(super) fn handle_onboard(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: OnboardParams = super::parse_params("onboard", params)?;
    let ctx = HandlerContext::new(indexer, params.common)?;

    // 1. Repo overview (compact)
    let overview = indexer.db().repo_overview(
        indexer.repo_root().clone(),
        ctx.languages.as_deref(),
        ctx.graph_version,
    )?;

    // 2. Module summary (depth=1)
    let modules =
        indexer
            .db()
            .module_summary(1, ctx.languages.as_deref(), None, ctx.graph_version)?;
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
    let lang_list = indexer.db().list_languages(ctx.graph_version)?;

    // 4. Index status
    let changed = indexer.changed_files(ctx.languages.as_deref())?;
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

#[cfg(test)]
mod explain_symbol_cross_boundary_tests {
    use super::*;
    use tempfile::TempDir;

    /// A python gRPC server, its .proto, and a test that calls it both
    /// directly (`helper()`, a CALLS edge) and over gRPC (an RPC_CALL edge
    /// with a NULL target that only trace_flow's bridge resolves).
    fn grpc_repo() -> (TempDir, Indexer) {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        std::fs::write(
            root.join("users.proto"),
            "syntax = \"proto3\";\n\nservice UserService {\n  rpc GetUser (GetUserRequest) returns (User);\n}\n",
        )
        .unwrap();
        std::fs::write(
            root.join("server.py"),
            "import users_pb2_grpc\n\n\nclass UserService(users_pb2_grpc.UserServiceServicer):\n    def GetUser(self, request, context):\n        return None\n",
        )
        .unwrap();
        std::fs::write(
            root.join("test_client.py"),
            "import users_pb2_grpc\n\n\ndef helper():\n    return 1\n\n\ndef test_get_user(channel):\n    helper()\n    users_pb2_grpc.UserServiceStub(channel).GetUser(None)\n",
        )
        .unwrap();
        let mut indexer =
            Indexer::new(root.to_path_buf(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
        indexer.reindex().unwrap();
        (dir, indexer)
    }

    fn explain(indexer: &mut Indexer, qualname: &str) -> Value {
        handle_explain_symbol(indexer, json!({"qualname": qualname})).unwrap()
    }

    fn refs<'a>(v: &'a Value, section: &str) -> Vec<(&'a str, &'a str)> {
        v[section]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                (
                    r["symbol"]["qualname"].as_str().unwrap(),
                    r["edge_kind"].as_str().unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn callees_include_rpc_hop_after_calls_with_kind_and_protocol_context() {
        let (_dir, mut indexer) = grpc_repo();
        let v = explain(&mut indexer, "test_client.test_get_user");
        // `users_pb2_grpc.UserServiceStub(channel)` is a call into a
        // generated-protobuf import (issue #80: known-external, so it binds
        // to that stub symbol instead of staying unresolved) between the
        // plain CALLS callee and the RPC_CALL hop.
        assert_eq!(
            refs(&v, "callees"),
            vec![
                ("test_client.helper", "CALLS"),
                ("ext:users_pb2_grpc.UserServiceStub", "CALLS"),
                ("server.UserService.GetUser", "RPC_CALL"),
            ],
            "{v:#}"
        );
        assert_eq!(v["callees_total"], 3);
        let ctx = &v["callees"][2]["protocol_context"];
        assert_eq!(ctx["service"], "UserService", "{v:#}");
        assert_eq!(ctx["rpc"], "GetUser", "{v:#}");
        assert!(v["callees"][0].get("protocol_context").is_none());
        assert!(v["callees"][1].get("protocol_context").is_none());
    }

    #[test]
    fn callers_and_tests_include_rpc_clients_but_not_the_proto_declaration() {
        let (_dir, mut indexer) = grpc_repo();
        let v = explain(&mut indexer, "server.UserService.GetUser");
        assert_eq!(
            refs(&v, "callers"),
            vec![("test_client.test_get_user", "RPC_CALL")],
            "{v:#}"
        );
        assert_eq!(v["callers_total"], 1);
        assert_eq!(
            refs(&v, "tests"),
            vec![("test_client.test_get_user", "RPC_CALL")],
            "{v:#}"
        );
        assert_eq!(v["tests_total"], 1);
    }
}
