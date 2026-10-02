use crate::db::Db;
use crate::indexer::channel::{
    WalkDirection, boundary_type_for_kind, bridge_complement, bridge_complements_for,
    bridge_crossing_allowed, bridge_pair_is_upstream,
};
use crate::indexer::config::{
    BridgeOutcome, BridgeTarget, CAP_TRUNCATION_REASON, CROSS_SERVICE_KIND, ConfigScope, Entry,
    config_edge_allowed, prefer_same_service,
};
use crate::indexer::scan::language_for_path;
use crate::model::{Edge, Symbol, TraceHop};
use anyhow::Result;
use std::collections::{HashMap, HashSet, VecDeque};

/// Direction of a BFS trace through the symbol graph.
#[derive(Debug, Clone)]
pub enum TraceDirection {
    Downstream,
    Upstream,
}

/// Configuration for a `trace_flow` traversal.
#[derive(Debug, Clone)]
pub struct TraceConfig {
    pub max_hops: usize,
    pub max_bytes: usize,
    pub direction: TraceDirection,
    pub include_snippets: bool,
    pub allowed_kinds: Vec<String>,
    pub trace_offset: usize,
    pub compact: bool,
    /// Resolution kinds to refuse to traverse (issue #81), e.g.
    /// `["bare_name", "two_segment"]` to exclude the guarded name-fallback
    /// tier's heuristic edges. An edge with no resolution kind at all (a
    /// Bridge Edge kind, governed separately by `bridge_complement`) is
    /// always traversable regardless of this list. Empty by default:
    /// unchanged behaviour.
    pub exclude_resolution_kinds: Vec<String>,
    /// Config URI (`secret://...`/`env://...`) the seeds were resolved from,
    /// if any (issue #131). Seed nodes then only follow config edges
    /// carrying that URI; see `ConfigScope`.
    pub seed_config_uri: Option<String>,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            max_hops: 5,
            max_bytes: 30_000,
            direction: TraceDirection::Downstream,
            include_snippets: true,
            // XREF is listed, but only its qualified grade is ever crossed --
            // see `xref_is_traversable`. RPC_ROUTE must be listed or a trace
            // from a .proto rpc is filtered out before bridge_complement is
            // consulted, silently yielding paths_found: 0.
            allowed_kinds: vec![
                "CALLS".into(),
                "RPC_IMPL".into(),
                "RPC_CALL".into(),
                "RPC_ROUTE".into(),
                "XREF".into(),
                "CHANNEL_PUBLISH".into(),
                "CHANNEL_SUBSCRIBE".into(),
                "HTTP_CALL".into(),
                "HTTP_ROUTE".into(),
                "CONFIG_SOURCE".into(),
                "CONFIG_READ".into(),
                "CONFIG_BIND".into(),
            ],
            trace_offset: 0,
            compact: false,
            exclude_resolution_kinds: Vec::new(),
            seed_config_uri: None,
        }
    }
}

/// Result of a `trace_flow` BFS traversal.
#[derive(Debug)]
pub struct TraceResult {
    pub start: Symbol,
    pub end: Option<Symbol>,
    pub hops: Vec<TraceHop>,
    pub paths_found: usize,
    pub reached_target: bool,
    pub truncated: bool,
    /// Why `truncated` is set when it is not a depth/byte limit.
    pub truncation_reason: Option<String>,
    pub budget_bytes: usize,
    pub used_bytes: usize,
    /// Count of `unresolved_references` rows touching the traversed symbols
    /// (issue #81) -- see `Db::unresolved_reference_count_for_symbols`'s doc.
    pub unresolved_reference_count: i64,
    /// Issue #81 (R5): whether at least one edge that produced a hop has a
    /// heuristic (`bare_name`/`two_segment`) resolution kind -- gates the
    /// "retry excluding heuristics" next_hops suggestion in
    /// `handle_trace_flow`.
    pub traversed_heuristic_kind: bool,
}

/// Every config URI `id`'s own config edges carry: the only URIs a config
/// bridge can enter it on.
fn is_config_edge_kind(kind: &str) -> bool {
    matches!(kind, "CONFIG_SOURCE" | "CONFIG_READ" | "CONFIG_BIND")
}

/// Content-based tie-break for two arrivals at the same (node, entry) pair
/// and level: independent of edge ids and processing order.
type TieKey = (String, String, Option<String>, Option<String>, String);

fn tie_key(parent_qualname: &str, edge: &Edge) -> TieKey {
    (
        parent_qualname.to_string(),
        edge.kind.clone(),
        edge.evidence_snippet.clone(),
        edge.detail.clone(),
        edge.file_path.clone(),
    )
}

/// Where a (node, entry) pair's hop sits in the trace, and the tie key of the
/// arrival that produced it.
struct HopSlot {
    idx: usize,
    dist: usize,
    key: TieKey,
}

type HopSlots = HashMap<(i64, Entry), HopSlot>;

/// A later arrival at an already-reported pair replaces its hop when it is
/// at the same distance and has a smaller tie key, so the reported parent,
/// edge kind and snippet do not depend on the order edges were processed.
fn retie(
    trace: &mut [TraceHop],
    slots: &mut HopSlots,
    pair: &(i64, Entry),
    dist: usize,
    key: TieKey,
    build: impl FnOnce() -> Option<TraceHop>,
) -> bool {
    let Some(slot) = slots.get_mut(pair) else {
        return false;
    };
    if slot.dist != dist || key >= slot.key {
        return false;
    }
    let Some(hop) = build() else {
        return false;
    };
    trace[slot.idx] = hop;
    slot.key = key;
    true
}

/// One BFS frontier entry: a node to expand under `entry`.
struct QueueItem {
    id: i64,
    dist: usize,
    prev_file: String,
    entry: Entry,
}

/// BFS traversal of the symbol graph from `seeds`, following edges in the
/// configured direction with bridge-edge crossing and byte budgeting.
pub fn trace_flow(
    db: &Db,
    seeds: Vec<i64>,
    end_id: Option<i64>,
    languages: Option<&[String]>,
    graph_version: i64,
    config: &TraceConfig,
) -> Result<TraceResult> {
    let start_sym = db
        .get_symbol_by_id(
            *seeds
                .first()
                .ok_or_else(|| anyhow::anyhow!("empty seeds"))?,
        )?
        .ok_or_else(|| anyhow::anyhow!("start symbol not found"))?;

    let mut trace: Vec<TraceHop> = Vec::new();
    let mut visited = HashSet::new();
    let mut slots: HopSlots = HashMap::new();
    let mut queue: VecDeque<QueueItem> = VecDeque::new();

    // Config URI each node was entered through (issue #131): it then only
    // continues along config edges carrying that URI.
    let mut scope = ConfigScope::new(&seeds, config.seed_config_uri.as_deref());
    for &sid in &seeds {
        visited.insert(sid);
        queue.push_back(QueueItem {
            id: sid,
            dist: 0,
            prev_file: start_sym.file_path.clone(),
            entry: scope.seed_entry(),
        });
    }

    // Byte budget is applied to the settled, canonically ordered hops (at
    // level boundaries and once at the end), never per arrival, so a hop
    // replaced by a same-level tie-break cannot change truncation.
    let mut last_level: usize = 0;
    let mut truncated = false;
    let mut reached_target = false;
    let is_upstream = matches!(config.direction, TraceDirection::Upstream);
    let walk = walk_direction(is_upstream);
    // Issue #81 (R5): every edge that actually produced a hop -- checked
    // once, after the BFS, against `HEURISTIC_RESOLUTION_KINDS` to decide
    // whether suggesting the exclude-heuristics retry is useful at all.
    let mut traversed_edge_ids: Vec<i64> = Vec::new();
    // Receiver type arguments each node was entered with (issue #185): a
    // dispatch edge to a closed explicit impl only follows a matching call.
    let mut entry_args = crate::db::EntryArgs::default();

    while let Some(QueueItem {
        id: current_id,
        dist,
        prev_file,
        entry,
    }) = queue.pop_front()
    {
        // A node at `dist == max_hops` was already recorded as a hop when
        // its parent expanded (below); it must not itself expand, or its
        // children would be recorded at `max_hops + 1`. BFS pops in
        // non-decreasing `dist` order (all seeds start at 0, children are
        // always enqueued at `dist + 1`), so once one node hits the depth
        // limit every remaining queued node does too -- the rest of the
        // queue at this point *is* the frontier sitting at the ceiling, so
        // it's safe to stop the whole loop rather than skip node by node.
        //
        // Whether stopping here is a truncation depends on whether that
        // frontier actually has more graph beyond it. A trace whose
        // reachable graph happens to end exactly at `max_hops` (the
        // ceiling nodes are leaves, or their only further edges are
        // filtered out) is complete, not truncated -- reporting truncation
        // there would be a false positive. But a frontier that still has
        // edges we're declining to follow genuinely lost information to
        // the depth cutoff, so `truncated` must reflect that: it gates the
        // "continue trace" next_hops in `handle_trace_flow`, one of lidx's
        // most valuable affordances.
        if dist >= config.max_hops {
            if !truncated {
                let ceiling_frontier = std::iter::once((current_id, entry.clone()))
                    .chain(queue.iter().map(|q| (q.id, q.entry.clone())));
                for (candidate, candidate_entry) in ceiling_frontier {
                    if has_further_edges(
                        db,
                        candidate,
                        is_upstream,
                        config,
                        &candidate_entry,
                        languages,
                        graph_version,
                    )? {
                        truncated = true;
                        break;
                    }
                }
            }
            break;
        }
        if dist > last_level {
            last_level = dist;
            if budget_exhausted(&trace, config) {
                truncated = true;
                break;
            }
        }

        let edges = db.edges_for_symbol_with_dispatch(current_id, languages, graph_version)?;
        let current_qn = db
            .get_symbol_by_id(current_id)?
            .map(|s| s.qualname)
            .unwrap_or_default();

        let mut bridge_targets: Vec<BridgeTarget> = Vec::new();
        let allowed = ConfigScope::allowed(&entry, &edges);

        for edge in &edges {
            if !config.allowed_kinds.contains(&edge.kind)
                || !crate::model::xref_is_traversable(edge)
                || !config_edge_allowed(edge, allowed.as_ref())
                || !entry_args.allows(current_id, edge)
            {
                continue;
            }
            // An edge with no resolution kind (a Bridge Edge kind) is always
            // traversable here -- bridging is governed separately below via
            // `bridge_targets`/`bridge_complement`.
            if crate::model::is_resolution_excluded(
                edge.resolution_kind.as_deref(),
                &config.exclude_resolution_kinds,
            ) {
                continue;
            }

            let next_id = if is_upstream {
                if edge.target_symbol_id == Some(current_id) || edge.target_symbol_id.is_none() {
                    // An outgoing edge with an unresolved target is not an
                    // incoming one: its source is `current_id` itself.
                    edge.source_symbol_id.filter(|s| *s != current_id)
                } else {
                    continue;
                }
            } else {
                if edge.source_symbol_id != Some(current_id) {
                    continue;
                }
                edge.target_symbol_id
            };

            if let Some(ref tq) = edge.target_qualname
                && bridge_complement(&edge.kind).is_some()
                && bridge_crossing_allowed(&edge.kind, walk)
            {
                bridge_targets.extend(ConfigScope::bridges_for(
                    &entry, &edges, edge, tq, current_id, walk,
                ));
            }

            // `next_id` is None when the write path left this edge's
            // target_symbol_id (or, for upstream, source_symbol_id) NULL --
            // it could not attribute the edge. The read path must not
            // invent an attribution via fuzzy qualname lookup here (that's
            // how a C# `value.Trim()` call used to surface a Python `trim`
            // function as its callee/caller); see the equivalent fix in
            // subgraph.rs / rpc/handlers.rs. Bridge-kind edges (message
            // bus, RPC, HTTP) still cross language boundaries below via
            // `bridge_targets`, which binds by an exact target_qualname
            // match, not a fuzzy one.
            let Some(next_id) = next_id else {
                continue;
            };

            let widened = !is_upstream && entry_args.record(next_id, edge, db)?;
            let Some(admission) = scope.admit_plain(next_id) else {
                // Reached again through a call with other type arguments:
                // expand it again so those closed impls are reached too.
                if widened && let Ok(Some(sym)) = db.get_symbol_by_id(next_id) {
                    queue.push_back(QueueItem {
                        id: next_id,
                        dist: dist + 1,
                        prev_file: sym.file_path.clone(),
                        entry: Entry::Unscoped,
                    });
                }
                let key = tie_key(&current_qn, edge);
                if retie(
                    &mut trace,
                    &mut slots,
                    &(next_id, Entry::Unscoped),
                    dist + 1,
                    key,
                    || {
                        let sym = db.get_symbol_by_id(next_id).ok()??;
                        Some(build_hop(
                            &sym,
                            edge,
                            dist + 1,
                            (current_id, &current_qn),
                            &prev_file,
                            config.include_snippets,
                        ))
                    },
                ) {
                    traversed_edge_ids.push(edge.id);
                }
                continue;
            };

            if let Ok(Some(next_sym)) = db.get_symbol_by_id(next_id) {
                // One hop per newly reached (node, entry) pair.
                let hop = build_hop(
                    &next_sym,
                    edge,
                    dist + 1,
                    (current_id, &current_qn),
                    &prev_file,
                    config.include_snippets,
                );

                let hop_idx = trace.len();
                trace.push(hop);
                slots.insert(
                    (next_id, admission.entry.clone()),
                    HopSlot {
                        idx: hop_idx,
                        dist: dist + 1,
                        key: tie_key(&current_qn, edge),
                    },
                );
                traversed_edge_ids.push(edge.id);
                visited.insert(next_id);
                if end_id == Some(next_id) {
                    reached_target = true;
                    break;
                }

                // An external stub is a leaf: its other callers are unrelated
                // to this trace (issue #175), so never expand through it.
                if admission.expand && !next_sym.is_external() {
                    queue.push_back(QueueItem {
                        id: next_id,
                        dist: dist + 1,
                        prev_file: next_sym.file_path.clone(),
                        entry: admission.entry,
                    });
                }
            }
        }

        if !reached_target && !truncated {
            bridge_targets.sort();
            for bridge in &bridge_targets {
                let BridgeTarget {
                    uri: tq,
                    edge_kind,
                    origin_path,
                    key,
                    method,
                    walk: bridge_walk,
                    ..
                } = bridge;
                let complement_kinds = bridge_complements_for(edge_kind, *bridge_walk);
                if !complement_kinds.is_empty() {
                    let bridged = db
                        .edges_by_target_qualname_and_kinds(
                            tq,
                            &complement_kinds,
                            languages,
                            graph_version,
                        )
                        .unwrap_or_default();
                    let b_type = boundary_type_for_kind(edge_kind);
                    for (bridged_edge, speculative) in
                        prefer_same_service(tq, origin_path, method.as_deref(), &bridged)
                    {
                        let Some(bridged_id) = bridged_edge.source_symbol_id else {
                            continue;
                        };
                        let make_hop = |bridged_sym: &Symbol| {
                            let prev_lang = detect_language(&prev_file);
                            let next_lang = detect_language(&bridged_sym.file_path);
                            let mut b_detail =
                                build_boundary_detail(b_type, &prev_lang, &next_lang);
                            if speculative {
                                b_detail.push_str(" (speculative: other service)");
                            }
                            let mut hop = build_hop(
                                bridged_sym,
                                bridged_edge,
                                dist + 1,
                                (current_id, &current_qn),
                                &prev_file,
                                config.include_snippets,
                            );
                            hop.bridge_direction =
                                Some(if bridge_pair_is_upstream(edge_kind, &bridged_edge.kind) {
                                    WalkDirection::Upstream
                                } else {
                                    WalkDirection::Downstream
                                });
                            hop.cross_language = true;
                            hop.boundary_type = Some(b_type.to_string());
                            hop.boundary_detail = Some(b_detail);
                            hop.protocol_context = extract_protocol_context(bridged_edge);
                            if speculative {
                                hop.resolution_kind = Some(CROSS_SERVICE_KIND.to_string());
                            }
                            hop
                        };
                        let admission = match scope.admit_bridged(bridge, bridged_id, || {
                            db.edges_for_symbol(bridged_id, languages, graph_version)
                                .unwrap_or_default()
                        }) {
                            BridgeOutcome::Skipped => continue,
                            BridgeOutcome::Admitted(a) => a,
                            BridgeOutcome::Refused => {
                                let pair = (
                                    bridged_id,
                                    if is_config_edge_kind(edge_kind) {
                                        Entry::scoped(tq, key.as_deref())
                                    } else {
                                        Entry::Unscoped
                                    },
                                );
                                let key = tie_key(&current_qn, bridged_edge);
                                if retie(&mut trace, &mut slots, &pair, dist + 1, key, || {
                                    let sym = db.get_symbol_by_id(bridged_id).ok()??;
                                    Some(make_hop(&sym))
                                }) {
                                    traversed_edge_ids.push(bridged_edge.id);
                                }
                                continue;
                            }
                        };
                        visited.insert(bridged_id);
                        if let Ok(Some(bridged_sym)) = db.get_symbol_by_id(bridged_id) {
                            let hop = make_hop(&bridged_sym);
                            let hop_idx = trace.len();
                            trace.push(hop);
                            slots.insert(
                                (bridged_id, admission.entry.clone()),
                                HopSlot {
                                    idx: hop_idx,
                                    dist: dist + 1,
                                    key: tie_key(&current_qn, bridged_edge),
                                },
                            );
                            traversed_edge_ids.push(bridged_edge.id);
                            if end_id == Some(bridged_id) {
                                reached_target = true;
                                break;
                            }
                            if admission.expand {
                                queue.push_back(QueueItem {
                                    id: bridged_id,
                                    dist: dist + 1,
                                    prev_file: bridged_sym.file_path.clone(),
                                    entry: admission.entry,
                                });
                            }
                        }
                    }
                    if reached_target || truncated {
                        break;
                    }
                }
            }
        }

        if reached_target || truncated {
            break;
        }
    }

    let truncation_reason = scope.capped().then(|| CAP_TRUNCATION_REASON.to_string());
    truncated |= scope.capped();

    // With an end target the answer is the path to it, not the visited
    // frontier: keep only the hops on the predecessor chain, or none when
    // the target was never reached.
    if let Some(eid) = end_id {
        let on_path: HashSet<usize> = if reached_target {
            path_indices(&trace, eid).into_iter().collect()
        } else {
            HashSet::new()
        };
        let mut idx = 0;
        trace.retain(|_| {
            idx += 1;
            on_path.contains(&(idx - 1))
        });
    }

    // Canonical order: independent of the order edges were processed in.
    trace.sort_by_cached_key(canonical_key);
    let mut trace: Vec<TraceHop> = trace.into_iter().skip(config.trace_offset).collect();

    // Apply the byte budget to the settled hops: keep hops while they fit,
    // so `used_bytes` stays within the budget (#221). The one exception is the
    // first hop, which is always kept: a budget smaller than a single hop would
    // otherwise return nothing and the continuation (offset + 0) would never
    // advance. Only then can `used_bytes` exceed `budget_bytes`.
    let mut used_bytes = 0usize;
    let mut keep = 0usize;
    for h in &trace {
        let size = estimate_hop_size(h, config.compact);
        if keep > 0 && used_bytes + size > config.max_bytes {
            break;
        }
        used_bytes += size;
        keep += 1;
    }
    if keep < trace.len() {
        trace.truncate(keep);
        truncated = true;
    }

    let end_sym = if let Some(eid) = end_id {
        db.get_symbol_by_id(eid)?
    } else {
        None
    };

    let paths_found = if trace.is_empty() {
        0
    } else if end_id.is_some() {
        if reached_target { 1 } else { 0 }
    } else {
        let max_dist = trace.iter().map(|h| h.distance).max().unwrap_or(0);
        trace.iter().filter(|h| h.distance == max_dist).count()
    };

    // Issue #81: lower-bound signal over every symbol this traversal
    // actually visited (seeds included), regardless of direction -- see
    // `Db::unresolved_reference_count_for_symbols`'s doc for the exact
    // per-direction semantics.
    let visited_ids: Vec<i64> = visited.into_iter().collect();
    let unresolved_reference_count =
        db.unresolved_reference_count_for_symbols(&visited_ids, graph_version)?;

    // Issue #81 (R5): one batched query over every edge that produced a hop,
    // rather than per-node -- paid only once, and only when there was
    // anything to check at all.
    let traversed_heuristic_kind = if traversed_edge_ids.is_empty() {
        false
    } else {
        let resolution_kinds = db.edge_resolution_kinds(&traversed_edge_ids)?;
        resolution_kinds
            .values()
            .any(|rk| crate::db::resolver::HEURISTIC_RESOLUTION_KINDS.contains(&rk.as_str()))
    };

    Ok(TraceResult {
        start: start_sym,
        end: end_sym,
        hops: trace,
        paths_found,
        reached_target,
        truncated,
        truncation_reason,
        budget_bytes: config.max_bytes,
        used_bytes,
        unresolved_reference_count,
        traversed_heuristic_kind,
    })
}

/// Whether `id` has at least one further edge that the BFS in
/// [`trace_flow`] would follow -- i.e. whether stopping expansion at `id`
/// (because it sits at the `max_hops` ceiling) actually discards reachable
/// graph. Mirrors the edge filtering and direction resolution used inside
/// the main loop (`allowed_kinds`, `xref_is_traversable`,
/// `exclude_resolution_kinds`, direct edge resolution, and bridge-kind
/// edges via `bridge_complement`), but only checks for existence -- it does
/// not build hops, consult `visited`, or resolve bridge targets against the
/// database, so it stays cheap even for a wide final frontier.
fn walk_direction(is_upstream: bool) -> WalkDirection {
    if is_upstream {
        WalkDirection::Upstream
    } else {
        WalkDirection::Downstream
    }
}

fn has_further_edges(
    db: &Db,
    id: i64,
    is_upstream: bool,
    config: &TraceConfig,
    entry: &Entry,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<bool> {
    let edges = db.edges_for_symbol_with_dispatch(id, languages, graph_version)?;
    let walk = walk_direction(is_upstream);
    let allowed = ConfigScope::allowed(entry, &edges);
    for edge in &edges {
        if !config.allowed_kinds.contains(&edge.kind)
            || !crate::model::xref_is_traversable(edge)
            || !config_edge_allowed(edge, allowed.as_ref())
        {
            continue;
        }
        if crate::model::is_resolution_excluded(
            edge.resolution_kind.as_deref(),
            &config.exclude_resolution_kinds,
        ) {
            continue;
        }

        let next_id = if is_upstream {
            if (edge.target_symbol_id == Some(id) || edge.target_symbol_id.is_none())
                && edge.source_symbol_id != Some(id)
            {
                edge.source_symbol_id
            } else {
                continue;
            }
        } else {
            if edge.source_symbol_id != Some(id) {
                continue;
            }
            edge.target_symbol_id
        };
        if next_id.is_some() {
            return Ok(true);
        }

        if edge.target_qualname.is_some()
            && bridge_complement(&edge.kind).is_some()
            && bridge_crossing_allowed(&edge.kind, walk)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Indices of the hops on the chain from the hop reaching `end_id` back to
/// the first hop after a seed, found by following predecessor ids. The walk
/// ends naturally there: seeds are never hops, so the lookup for a
/// distance-1 hop's predecessor (at distance 0) finds nothing.
fn path_indices(trace: &[TraceHop], end_id: i64) -> Vec<usize> {
    let mut by_id_dist: HashMap<(i64, usize), usize> = HashMap::new();
    for (i, h) in trace.iter().enumerate() {
        by_id_dist.entry((h.symbol.id, h.distance)).or_insert(i);
    }
    let mut path = Vec::new();
    let mut cur = trace.iter().position(|h| h.symbol.id == end_id);
    while let Some(i) = cur {
        path.push(i);
        let h = &trace[i];
        cur = h
            .distance
            .checked_sub(1)
            .and_then(|d| by_id_dist.get(&(h.predecessor_id, d)).copied());
    }
    path
}

fn build_hop(
    next_sym: &Symbol,
    edge: &Edge,
    distance: usize,
    predecessor: (i64, &str),
    prev_file: &str,
    include_snippets: bool,
) -> TraceHop {
    let prev_lang = detect_language(prev_file);
    let next_lang = detect_language(&next_sym.file_path);
    // External stubs (issue #121) live in a synthetic `<external>` file with
    // no real language of its own -- `next_lang` would otherwise come back
    // "unknown" and get reported as a bogus cross-language boundary (e.g.
    // "C# -> unknown" for `ext:SHA256.Create`).
    let cross_lang = prev_lang != next_lang && !next_sym.is_external();

    let snippet = if include_snippets {
        edge.evidence_snippet.clone()
    } else {
        None
    };

    let (boundary_type, boundary_detail, protocol_context) = if cross_lang {
        let b_type = detect_boundary_type(&edge.kind, &prev_lang, &next_lang);
        let b_detail = build_boundary_detail(&b_type, &prev_lang, &next_lang);
        let p_context = extract_protocol_context(edge);
        (Some(b_type), Some(b_detail), p_context)
    } else {
        (None, None, None)
    };

    TraceHop {
        symbol: next_sym.clone(),
        edge_kind: edge.kind.clone(),
        distance,
        predecessor: predecessor.1.to_string(),
        predecessor_id: predecessor.0,
        language: next_lang,
        snippet,
        cross_language: cross_lang,
        boundary_type,
        boundary_detail,
        protocol_context,
        resolution_kind: edge.resolution_kind.clone(),
        bridge_direction: None,
    }
}

fn detect_language(file_path: &str) -> String {
    let lang = language_for_path(std::path::Path::new(file_path)).unwrap_or("unknown");
    // Normalize tsx → typescript so .ts/.tsx files are never treated as
    // different languages for cross-language boundary detection.
    match lang {
        "tsx" => "typescript".to_string(),
        other => other.to_string(),
    }
}

fn detect_boundary_type(edge_kind: &str, source_lang: &str, target_lang: &str) -> String {
    match edge_kind {
        "RPC_IMPL" | "RPC_CALL" | "RPC_ROUTE" => "grpc".to_string(),
        "HTTP_CALL" | "HTTP_ROUTE" => "http".to_string(),
        "CHANNEL_PUBLISH" | "CHANNEL_SUBSCRIBE" => "message_bus".to_string(),
        "CONFIG_SOURCE" | "CONFIG_READ" => "config".to_string(),
        "XREF" if source_lang == "csharp" && target_lang == "sql" => "stored_procedure".to_string(),
        "XREF" if source_lang == "sql" && target_lang == "csharp" => "stored_procedure".to_string(),
        "XREF" => "xref".to_string(),
        _ => "other".to_string(),
    }
}

fn display_language(lang: &str) -> String {
    match lang {
        "csharp" => "C#".to_string(),
        "javascript" => "JavaScript".to_string(),
        "typescript" | "tsx" => "TypeScript".to_string(),
        other => other.to_string(),
    }
}

fn build_boundary_detail(boundary_type: &str, source_lang: &str, target_lang: &str) -> String {
    let source_display = display_language(source_lang);
    let target_display = display_language(target_lang);

    match boundary_type {
        "grpc" => format!("{} \u{2192} {} via gRPC", source_display, target_display),
        "http" => format!("{} \u{2192} {} via HTTP", source_display, target_display),
        "message_bus" => format!(
            "{} \u{2192} {} via message bus",
            source_display, target_display
        ),
        "config" => format!(
            "{} \u{2192} {} via config/env",
            source_display, target_display
        ),
        "stored_procedure" => format!(
            "{} \u{2192} {} via stored procedure",
            source_display, target_display
        ),
        "xref" => format!(
            "{} \u{2192} {} via cross-reference",
            source_display, target_display
        ),
        _ => format!("{} \u{2192} {}", source_display, target_display),
    }
}

fn extract_protocol_context(edge: &Edge) -> Option<serde_json::Value> {
    let detail_str = edge.detail.as_ref()?;
    let detail: serde_json::Value = serde_json::from_str(detail_str).ok()?;

    match edge.kind.as_str() {
        "RPC_IMPL" | "RPC_CALL" | "RPC_ROUTE" => {
            let service = detail.get("service")?.as_str()?;
            let rpc = detail.get("rpc")?.as_str()?;
            let package = detail.get("package").and_then(|p| p.as_str());
            let framework = detail
                .get("framework")
                .and_then(|f| f.as_str())
                .unwrap_or("grpc");
            Some(serde_json::json!({
                "framework": framework,
                "service": service,
                "rpc": rpc,
                "package": package,
            }))
        }
        "CHANNEL_PUBLISH" | "CHANNEL_SUBSCRIBE" => {
            let channel_name = detail.get("channel").and_then(|c| c.as_str());
            let framework = detail
                .get("framework")
                .and_then(|f| f.as_str())
                .unwrap_or("unknown");
            let role = detail
                .get("role")
                .and_then(|r| r.as_str())
                .unwrap_or("unknown");
            Some(serde_json::json!({
                "framework": framework,
                "channel": channel_name,
                "role": role,
            }))
        }
        "CONFIG_SOURCE" | "CONFIG_READ" => {
            let config_uri = detail.get("config_uri").and_then(|c| c.as_str());
            let source_type = detail
                .get("source_type")
                .and_then(|s| s.as_str())
                .unwrap_or("env");
            let role = detail
                .get("role")
                .and_then(|r| r.as_str())
                .unwrap_or("unknown");
            Some(serde_json::json!({
                "source_type": source_type,
                "config_uri": config_uri,
                "role": role,
            }))
        }
        "HTTP_CALL" | "HTTP_ROUTE" => {
            let method = detail.get("method").and_then(|m| m.as_str());
            let path = detail.get("path").and_then(|p| p.as_str());
            let framework = detail
                .get("framework")
                .and_then(|f| f.as_str())
                .unwrap_or("http");
            Some(serde_json::json!({
                "framework": framework,
                "method": method,
                "path": path,
            }))
        }
        _ => None,
    }
}

fn canonical_key(h: &TraceHop) -> (usize, String, String, String) {
    (
        h.distance,
        h.symbol.qualname.clone(),
        h.edge_kind.clone(),
        serde_json::to_string(h).unwrap_or_default(),
    )
}

/// Whether the settled hops so far (after `trace_offset`, in canonical
/// order) already reach the byte budget.
fn budget_exhausted(trace: &[TraceHop], config: &TraceConfig) -> bool {
    let mut sorted: Vec<&TraceHop> = trace.iter().collect();
    sorted.sort_by_cached_key(|h| canonical_key(h));
    let mut used = 0usize;
    sorted.into_iter().skip(config.trace_offset).any(|h| {
        used += estimate_hop_size(h, config.compact);
        used >= config.max_bytes
    })
}

fn estimate_hop_size(hop: &TraceHop, compact: bool) -> usize {
    if compact {
        let mut hop_val = serde_json::to_value(hop).unwrap_or_default();
        if let Some(sym) = hop_val.get("symbol").cloned()
            && let Some(obj) = hop_val.as_object_mut()
        {
            let compact_sym = crate::rpc::compact_symbol_value(&sym);
            obj.insert("symbol".to_string(), compact_sym);
        }
        serde_json::to_string(&hop_val).unwrap_or_default().len()
    } else {
        serde_json::to_string(hop).unwrap_or_default().len()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::Indexer;
    use crate::model::Edge;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn sample_edge(kind: &str) -> Edge {
        Edge {
            id: 1,
            file_path: "Service.cs".to_string(),
            kind: kind.to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: Some(200),
            target_qualname: None,
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            resolution_kind: None,
            graph_version: 1,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
            dispatch_args: None,
        }
    }

    /// RPC_ROUTE must be in the default kind set, or a trace from a .proto rpc
    /// is filtered out before bridge_complement is ever consulted and the
    /// proto->impl linkage silently returns paths_found: 0.
    #[test]
    fn default_allowed_kinds_include_both_sides_of_the_rpc_bridge() {
        let kinds = TraceConfig::default().allowed_kinds;
        assert!(
            kinds.contains(&"RPC_ROUTE".to_string()),
            "RPC_ROUTE must be traversable by default so proto rpcs reach their impls: {kinds:?}"
        );
        assert!(kinds.contains(&"RPC_IMPL".to_string()));
        assert!(kinds.contains(&"CALLS".to_string()));
    }

    /// XREF is listed in the defaults, but the *grade* gates it: a bare
    /// `name_exact` match (one shared word, confidence 0.7) is never crossed,
    /// while a qualified `qualname_exact` match (a SQL literal naming that
    /// exact table) is. Without this, `trace_flow` upstream from one C# method
    /// fabricated an 18-step trace of which 16 steps were phantom.
    #[test]
    fn only_qualified_xref_is_traversable() {
        let bare = r#"{"confidence":0.7,"match":"name_exact","source":"string_literal","token":"Deserialize"}"#;
        let qualified = r#"{"confidence":1.0,"match":"qualname_exact","source":"string_literal","token":"dpb.pipeline_run"}"#;

        let mut edge = sample_edge("XREF");
        edge.detail = Some(bare.to_string());
        assert!(
            !crate::model::xref_is_traversable(&edge),
            "a bare name_exact XREF must never be crossed"
        );

        edge.detail = Some(qualified.to_string());
        assert!(
            crate::model::xref_is_traversable(&edge),
            "a qualified XREF is real evidence and must be crossed"
        );

        // An XREF with no detail at all cannot prove its grade, so it is refused.
        edge.detail = None;
        assert!(!crate::model::xref_is_traversable(&edge));

        // Non-XREF kinds are unaffected, detail or not.
        let mut calls = sample_edge("CALLS");
        calls.detail = None;
        assert!(crate::model::xref_is_traversable(&calls));
        calls.detail = Some(bare.to_string());
        assert!(crate::model::xref_is_traversable(&calls));
    }

    static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join(name)
    }

    fn temp_repo_dir(label: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
        dir.push(format!("lidx-traversal-{label}-{nanos}-{counter}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn copy_dir(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).unwrap();
        for entry in std::fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let target = dst.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&path, &target);
            } else {
                std::fs::copy(&path, &target).unwrap();
            }
        }
    }

    struct TempRepo {
        pub repo_root: PathBuf,
        pub db_path: PathBuf,
    }

    impl Drop for TempRepo {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.repo_root);
        }
    }

    impl TempRepo {
        fn new(fixture: &str) -> Self {
            let src = fixture_path(fixture);
            let repo_root = temp_repo_dir(fixture);
            copy_dir(&src, &repo_root);
            let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
            Self { repo_root, db_path }
        }
    }

    fn indexed_repo(fixture: &str) -> (TempRepo, Indexer) {
        let temp = TempRepo::new(fixture);
        let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
        indexer.reindex().unwrap();
        (temp, indexer)
    }

    // -- Unit tests for boundary helpers (migrated from rpc/mod.rs) --

    #[test]
    fn test_detect_language() {
        assert_eq!(detect_language("test.py"), "python");
        assert_eq!(detect_language("test.cs"), "csharp");
        assert_eq!(detect_language("test.rs"), "rust");
        assert_eq!(detect_language("test.proto"), "proto");
        assert_eq!(detect_language("test.ts"), "typescript");
        assert_eq!(detect_language("test.tsx"), "typescript");
        assert_eq!(detect_language("test.js"), "javascript");
        assert_eq!(detect_language("test.jsx"), "javascript");
        assert_eq!(detect_language("test.sql"), "sql");
        assert_eq!(detect_language("test.go"), "go");
        assert_eq!(detect_language("test.yaml"), "yaml");
        assert_eq!(detect_language("test.yml"), "yaml");
        assert_eq!(detect_language("test.bicep"), "bicep");
        assert_eq!(detect_language("test.psql"), "postgres");
        assert_eq!(detect_language("test.pgsql"), "postgres");
        assert_eq!(detect_language("test.md"), "markdown");
        assert_eq!(detect_language("test.txt"), "unknown");
        // Paths with directories
        assert_eq!(detect_language("src/services/api.py"), "python");
        assert_eq!(detect_language("deep/nested/path/file.ts"), "typescript");
        // Edge cases
        assert_eq!(detect_language("no_extension"), "unknown");
        assert_eq!(detect_language(""), "unknown");
    }

    #[test]
    fn test_detect_boundary_type() {
        assert_eq!(detect_boundary_type("RPC_IMPL", "proto", "csharp"), "grpc");
        assert_eq!(detect_boundary_type("RPC_CALL", "csharp", "proto"), "grpc");
        assert_eq!(detect_boundary_type("RPC_ROUTE", "proto", "csharp"), "grpc");
        assert_eq!(
            detect_boundary_type("XREF", "csharp", "sql"),
            "stored_procedure"
        );
        assert_eq!(
            detect_boundary_type("XREF", "sql", "csharp"),
            "stored_procedure"
        );
        assert_eq!(detect_boundary_type("XREF", "python", "csharp"), "xref");
        assert_eq!(
            detect_boundary_type("HTTP_CALL", "typescript", "python"),
            "http"
        );
        assert_eq!(
            detect_boundary_type("HTTP_ROUTE", "python", "typescript"),
            "http"
        );
        assert_eq!(
            detect_boundary_type("CHANNEL_PUBLISH", "python", "csharp"),
            "message_bus"
        );
        assert_eq!(
            detect_boundary_type("CHANNEL_SUBSCRIBE", "csharp", "python"),
            "message_bus"
        );
        assert_eq!(
            detect_boundary_type("CONFIG_SOURCE", "python", "typescript"),
            "config"
        );
        assert_eq!(
            detect_boundary_type("CONFIG_READ", "typescript", "python"),
            "config"
        );
        assert_eq!(detect_boundary_type("CALLS", "python", "python"), "other");
    }

    #[test]
    fn test_build_boundary_detail() {
        assert_eq!(
            build_boundary_detail("grpc", "proto", "csharp"),
            "proto \u{2192} C# via gRPC"
        );
        assert_eq!(
            build_boundary_detail("stored_procedure", "csharp", "sql"),
            "C# \u{2192} sql via stored procedure"
        );
        assert_eq!(
            build_boundary_detail("xref", "python", "csharp"),
            "python \u{2192} C# via cross-reference"
        );
        assert_eq!(
            build_boundary_detail("http", "typescript", "python"),
            "TypeScript \u{2192} python via HTTP"
        );
        assert_eq!(
            build_boundary_detail("message_bus", "python", "csharp"),
            "python \u{2192} C# via message bus"
        );
        assert_eq!(
            build_boundary_detail("config", "javascript", "python"),
            "JavaScript \u{2192} python via config/env"
        );
        assert_eq!(
            build_boundary_detail("other", "rust", "python"),
            "rust \u{2192} python"
        );
    }

    #[test]
    fn test_extract_protocol_context() {
        let rpc_impl_edge = Edge {
            id: 1,
            file_path: "test.cs".to_string(),
            kind: "RPC_IMPL".to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: Some(200),
            target_qualname: Some("myservice.MyService.GetUser".to_string()),
            detail: Some(r#"{"framework":"grpc-csharp","role":"server","service":"MyService","rpc":"GetUser","package":"myservice","raw":"/myservice.MyService/GetUser"}"#.to_string()),
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            resolution_kind: None,
            graph_version: 1,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
            dispatch_args: None,
        };

        let context = extract_protocol_context(&rpc_impl_edge);
        assert!(context.is_some());
        let context = context.unwrap();
        assert_eq!(context["service"], "MyService");
        assert_eq!(context["rpc"], "GetUser");
        assert_eq!(context["package"], "myservice");
        assert_eq!(context["framework"], "grpc-csharp");

        let call_edge = Edge {
            id: 2,
            file_path: "test.rs".to_string(),
            kind: "CALLS".to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: Some(200),
            target_qualname: Some("module::function".to_string()),
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            resolution_kind: None,
            graph_version: 1,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
            dispatch_args: None,
        };

        let context = extract_protocol_context(&call_edge);
        assert!(context.is_none());
    }

    #[test]
    fn test_extract_protocol_context_channel() {
        let edge = Edge {
            id: 1,
            file_path: "test.py".to_string(),
            kind: "CHANNEL_PUBLISH".to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: None,
            target_qualname: Some("events.user_created".to_string()),
            detail: Some(
                r#"{"framework":"rabbitmq","channel":"user_created","role":"publisher"}"#
                    .to_string(),
            ),
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            resolution_kind: None,
            graph_version: 1,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
            dispatch_args: None,
        };

        let ctx = extract_protocol_context(&edge).unwrap();
        assert_eq!(ctx["framework"], "rabbitmq");
        assert_eq!(ctx["channel"], "user_created");
        assert_eq!(ctx["role"], "publisher");
    }

    #[test]
    fn test_extract_protocol_context_config() {
        let edge = Edge {
            id: 1,
            file_path: "test.py".to_string(),
            kind: "CONFIG_READ".to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: None,
            target_qualname: Some("env.DATABASE_URL".to_string()),
            detail: Some(
                r#"{"source_type":"env","config_uri":"DATABASE_URL","role":"reader"}"#.to_string(),
            ),
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            resolution_kind: None,
            graph_version: 1,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
            dispatch_args: None,
        };

        let ctx = extract_protocol_context(&edge).unwrap();
        assert_eq!(ctx["source_type"], "env");
        assert_eq!(ctx["config_uri"], "DATABASE_URL");
        assert_eq!(ctx["role"], "reader");
    }

    #[test]
    fn test_extract_protocol_context_http() {
        let edge = Edge {
            id: 1,
            file_path: "test.ts".to_string(),
            kind: "HTTP_CALL".to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: None,
            target_qualname: Some("api.users".to_string()),
            detail: Some(
                r#"{"framework":"express","method":"GET","path":"/api/users"}"#.to_string(),
            ),
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            resolution_kind: None,
            graph_version: 1,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
            dispatch_args: None,
        };

        let ctx = extract_protocol_context(&edge).unwrap();
        assert_eq!(ctx["framework"], "express");
        assert_eq!(ctx["method"], "GET");
        assert_eq!(ctx["path"], "/api/users");
    }

    #[test]
    fn test_extract_protocol_context_malformed_json() {
        let edge = Edge {
            id: 1,
            file_path: "test.py".to_string(),
            kind: "RPC_IMPL".to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: None,
            target_qualname: None,
            detail: Some("not valid json".to_string()),
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            resolution_kind: None,
            graph_version: 1,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
            dispatch_args: None,
        };

        assert!(extract_protocol_context(&edge).is_none());
    }

    #[test]
    fn test_extract_protocol_context_missing_required_fields() {
        let edge = Edge {
            id: 1,
            file_path: "test.cs".to_string(),
            kind: "RPC_IMPL".to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: None,
            target_qualname: None,
            detail: Some(r#"{"framework":"grpc"}"#.to_string()),
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            resolution_kind: None,
            graph_version: 1,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
            dispatch_args: None,
        };

        assert!(
            extract_protocol_context(&edge).is_none(),
            "should return None when required service/rpc fields are missing"
        );
    }

    #[test]
    fn display_language_renders_tsx_as_typescript() {
        assert_eq!(display_language("tsx"), "TypeScript");
        assert_eq!(display_language("typescript"), "TypeScript");
        assert_eq!(display_language("csharp"), "C#");
        assert_eq!(display_language("javascript"), "JavaScript");
        assert_eq!(display_language("python"), "python");
        assert_eq!(display_language("unknown"), "unknown");
    }

    // -- Integration tests for trace_flow --

    #[test]
    fn downstream_bfs_traces_calls_edges() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("run".into()),
            None,
            gv,
        )
        .unwrap()
        .symbol;
        let seeds = crate::resolve::expand_seeds(indexer.db(), start.id, gv).unwrap();

        let config = TraceConfig {
            max_hops: 5,
            direction: TraceDirection::Downstream,
            allowed_kinds: vec!["CALLS".into()],
            ..Default::default()
        };

        let result = trace_flow(indexer.db(), seeds, None, None, gv, &config).unwrap();

        assert!(!result.hops.is_empty(), "should find downstream hops");
        for hop in &result.hops {
            assert_eq!(hop.edge_kind, "CALLS");
            assert!(hop.distance >= 1);
            assert!(hop.distance <= 5);
        }
    }

    #[test]
    fn upstream_bfs_traces_incoming_edges() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let target = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("helper".into()),
            None,
            gv,
        )
        .unwrap()
        .symbol;
        let seeds = vec![target.id];

        let config = TraceConfig {
            max_hops: 5,
            direction: TraceDirection::Upstream,
            allowed_kinds: vec!["CALLS".into()],
            ..Default::default()
        };

        let result = trace_flow(indexer.db(), seeds, None, None, gv, &config).unwrap();

        assert!(!result.hops.is_empty(), "should find upstream callers");
        for hop in &result.hops {
            assert!(hop.distance >= 1);
        }
    }

    #[test]
    fn bridge_edge_crossing_produces_cross_language_hops() {
        let (_temp, indexer) = indexed_repo("poly_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("GetUser".into()),
            None,
            gv,
        )
        .unwrap()
        .symbol;
        let seeds = crate::resolve::expand_seeds(indexer.db(), start.id, gv).unwrap();

        let config = TraceConfig {
            max_hops: 5,
            direction: TraceDirection::Downstream,
            ..Default::default()
        };

        let result = trace_flow(indexer.db(), seeds, None, None, gv, &config).unwrap();

        let cross_lang_hops: Vec<&TraceHop> =
            result.hops.iter().filter(|h| h.cross_language).collect();
        assert!(
            !cross_lang_hops.is_empty(),
            "should find cross-language hops via bridge edges"
        );
        for hop in &cross_lang_hops {
            assert!(
                hop.boundary_type.is_some(),
                "cross-language hop should have boundary_type"
            );
            assert!(
                hop.boundary_detail.is_some(),
                "cross-language hop should have boundary_detail"
            );
        }
    }

    #[test]
    fn byte_budget_truncation() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("run".into()),
            None,
            gv,
        )
        .unwrap()
        .symbol;
        let seeds = crate::resolve::expand_seeds(indexer.db(), start.id, gv).unwrap();

        let config = TraceConfig {
            max_bytes: 1,
            direction: TraceDirection::Downstream,
            ..Default::default()
        };

        let result = trace_flow(indexer.db(), seeds, None, None, gv, &config).unwrap();

        assert!(result.truncated, "should be truncated with 1-byte budget");
    }

    #[test]
    fn max_hops_limit_respected() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("run".into()),
            None,
            gv,
        )
        .unwrap()
        .symbol;
        let seeds = crate::resolve::expand_seeds(indexer.db(), start.id, gv).unwrap();

        let deep_config = TraceConfig {
            max_hops: 10,
            direction: TraceDirection::Downstream,
            ..Default::default()
        };
        let deep = trace_flow(indexer.db(), seeds.clone(), None, None, gv, &deep_config).unwrap();

        let shallow_config = TraceConfig {
            max_hops: 1,
            direction: TraceDirection::Downstream,
            ..Default::default()
        };
        let shallow = trace_flow(indexer.db(), seeds, None, None, gv, &shallow_config).unwrap();

        assert!(
            shallow.hops.len() <= deep.hops.len(),
            "shallow trace should have fewer or equal hops"
        );
        let max_dist = shallow.hops.iter().map(|h| h.distance).max().unwrap_or(0);
        assert!(
            max_dist <= shallow_config.max_hops,
            "no hop should exceed max_hops={}; got max_dist={}",
            shallow_config.max_hops,
            max_dist
        );
        let deep_max_dist = deep.hops.iter().map(|h| h.distance).max().unwrap_or(0);
        assert!(
            deep_max_dist <= deep_config.max_hops,
            "no hop should exceed max_hops={}; got max_dist={}",
            deep_config.max_hops,
            deep_max_dist
        );
    }

    #[test]
    fn empty_trace_when_no_matching_edges() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("helper".into()),
            None,
            gv,
        )
        .unwrap()
        .symbol;
        let seeds = vec![start.id];

        let config = TraceConfig {
            max_hops: 5,
            direction: TraceDirection::Downstream,
            allowed_kinds: vec!["NONEXISTENT_KIND".into()],
            ..Default::default()
        };

        let result = trace_flow(indexer.db(), seeds, None, None, gv, &config).unwrap();

        assert!(
            result.hops.is_empty(),
            "should have no hops with non-matching edge kinds"
        );
        assert_eq!(result.paths_found, 0);
    }

    #[test]
    fn trace_offset_pagination_skips_hops() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("run".into()),
            None,
            gv,
        )
        .unwrap()
        .symbol;
        let seeds = crate::resolve::expand_seeds(indexer.db(), start.id, gv).unwrap();

        let config_full = TraceConfig {
            direction: TraceDirection::Downstream,
            ..Default::default()
        };
        let full_result =
            trace_flow(indexer.db(), seeds.clone(), None, None, gv, &config_full).unwrap();

        if full_result.hops.len() > 1 {
            let config_offset = TraceConfig {
                direction: TraceDirection::Downstream,
                trace_offset: 1,
                ..Default::default()
            };
            let offset_result =
                trace_flow(indexer.db(), seeds, None, None, gv, &config_offset).unwrap();

            assert_eq!(
                offset_result.hops.len(),
                full_result.hops.len() - 1,
                "offset=1 should skip 1 hop"
            );
        }
    }

    #[test]
    fn empty_seeds_returns_error() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let config = TraceConfig::default();
        let err = trace_flow(indexer.db(), vec![], None, None, gv, &config).unwrap_err();
        assert!(
            err.to_string().contains("empty seeds"),
            "empty seeds should fail, got: {}",
            err
        );
    }

    #[test]
    fn max_hops_zero_returns_no_hops() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("run".into()),
            None,
            gv,
        )
        .unwrap()
        .symbol;
        let seeds = crate::resolve::expand_seeds(indexer.db(), start.id, gv).unwrap();

        let config = TraceConfig {
            max_hops: 0,
            direction: TraceDirection::Downstream,
            allowed_kinds: vec!["CALLS".into()],
            ..Default::default()
        };

        let result = trace_flow(indexer.db(), seeds, None, None, gv, &config).unwrap();
        // Issue #121: max_hops bounds the maximum returned hop distance
        // directly, and every hop is at least distance 1 (the seed itself
        // is never reported as a hop), so max_hops=0 must return nothing.
        assert!(
            result.hops.is_empty(),
            "with max_hops=0, no hops should be returned, got {:?}",
            result.hops.iter().map(|h| h.distance).collect::<Vec<_>>()
        );
    }

    #[test]
    fn max_hops_one_returns_direct_neighbors_only() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("run".into()),
            None,
            gv,
        )
        .unwrap()
        .symbol;
        let seeds = crate::resolve::expand_seeds(indexer.db(), start.id, gv).unwrap();

        let config = TraceConfig {
            max_hops: 1,
            direction: TraceDirection::Downstream,
            allowed_kinds: vec!["CALLS".into()],
            ..Default::default()
        };

        let result = trace_flow(indexer.db(), seeds, None, None, gv, &config).unwrap();
        assert!(
            !result.hops.is_empty(),
            "with max_hops=1, direct neighbors should still be returned"
        );
        for hop in &result.hops {
            assert_eq!(
                hop.distance, 1,
                "with max_hops=1, every hop should be at distance 1, got {}",
                hop.distance
            );
        }
    }

    #[test]
    fn trace_offset_larger_than_results_produces_empty_hops() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("run".into()),
            None,
            gv,
        )
        .unwrap()
        .symbol;
        let seeds = crate::resolve::expand_seeds(indexer.db(), start.id, gv).unwrap();

        let config = TraceConfig {
            trace_offset: 10000,
            direction: TraceDirection::Downstream,
            ..Default::default()
        };

        let result = trace_flow(indexer.db(), seeds, None, None, gv, &config).unwrap();
        assert!(
            result.hops.is_empty(),
            "large offset should produce no hops"
        );
        assert_eq!(result.paths_found, 0);
    }

    #[test]
    fn nonexistent_seed_id_returns_error() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let config = TraceConfig::default();
        let err = trace_flow(indexer.db(), vec![999999], None, None, gv, &config).unwrap_err();
        assert!(
            err.to_string().contains("start symbol not found"),
            "nonexistent seed should fail, got: {}",
            err
        );
    }

    #[test]
    fn trace_with_end_id_same_as_start_finds_nothing() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("run".into()),
            None,
            gv,
        )
        .unwrap()
        .symbol;
        let seeds = vec![start.id];

        let config = TraceConfig {
            direction: TraceDirection::Downstream,
            ..Default::default()
        };

        // end_id == start_id: the start is in visited, so it can never be "reached"
        // as a neighbor. The trace should run normally but not reach target.
        let result = trace_flow(indexer.db(), seeds, Some(start.id), None, gv, &config).unwrap();
        assert!(
            !result.reached_target,
            "should not reach target when end_id == start_id (already visited)"
        );
    }

    #[test]
    fn downstream_and_upstream_on_leaf_node() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        // helper() is a leaf function - nothing calls from it downstream
        let leaf = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("helper".into()),
            None,
            gv,
        )
        .unwrap()
        .symbol;
        let seeds = vec![leaf.id];

        let down_config = TraceConfig {
            direction: TraceDirection::Downstream,
            allowed_kinds: vec!["CALLS".into()],
            ..Default::default()
        };
        let down = trace_flow(indexer.db(), seeds.clone(), None, None, gv, &down_config).unwrap();
        // helper() doesn't call anything, so downstream should be empty
        assert!(
            down.hops.is_empty(),
            "leaf node downstream should have no hops"
        );

        let up_config = TraceConfig {
            direction: TraceDirection::Upstream,
            allowed_kinds: vec!["CALLS".into()],
            ..Default::default()
        };
        let up = trace_flow(indexer.db(), seeds, None, None, gv, &up_config).unwrap();
        // helper() should have at least one upstream caller (call() in a.py)
        assert!(
            !up.hops.is_empty(),
            "leaf node upstream should find callers"
        );
    }

    #[test]
    fn compact_estimate_differs_from_full_estimate() {
        let hop = TraceHop {
            symbol: crate::model::Symbol {
                id: 1,
                file_path: "test.py".to_string(),
                kind: "function".to_string(),
                name: "test_func".to_string(),
                qualname: "module.test_func".to_string(),
                start_line: 1,
                start_col: 0,
                end_line: 10,
                end_col: 0,
                start_byte: 0,
                end_byte: 100,
                signature: Some("def test_func():".to_string()),
                docstring: Some(
                    "A test function with a long docstring for size testing".to_string(),
                ),
                graph_version: 1,
                commit_sha: None,
                stable_id: None,
            },
            edge_kind: "CALLS".to_string(),
            distance: 1,
            predecessor: String::new(),
            predecessor_id: 0,
            language: "python".to_string(),
            snippet: Some("test_func()".to_string()),
            cross_language: false,
            boundary_type: None,
            boundary_detail: None,
            protocol_context: None,
            resolution_kind: None,
            bridge_direction: None,
        };

        let full_size = estimate_hop_size(&hop, false);
        let compact_size = estimate_hop_size(&hop, true);

        // Compact should be smaller because it strips fields from the symbol
        assert!(
            compact_size <= full_size,
            "compact ({}) should be <= full ({})",
            compact_size,
            full_size
        );
        assert!(full_size > 0, "hop size should be positive");
        assert!(compact_size > 0, "compact hop size should be positive");
    }
}

#[cfg(test)]
mod tsx_normalization_tests {
    use super::*;
    use crate::model::{Edge, Symbol};

    fn dummy_symbol(file_path: &str) -> Symbol {
        Symbol {
            id: 1,
            file_path: file_path.to_string(),
            kind: "function".to_string(),
            name: "foo".to_string(),
            qualname: "mod.foo".to_string(),
            start_line: 1,
            start_col: 0,
            end_line: 5,
            end_col: 0,
            start_byte: 0,
            end_byte: 50,
            signature: None,
            docstring: None,
            graph_version: 1,
            commit_sha: None,
            stable_id: None,
        }
    }

    fn dummy_edge() -> Edge {
        Edge {
            id: 1,
            file_path: "src/a.ts".to_string(),
            kind: "CALLS".to_string(),
            source_symbol_id: Some(1),
            target_symbol_id: Some(2),
            target_qualname: Some("mod.bar".to_string()),
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            resolution_kind: None,
            graph_version: 1,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
            dispatch_args: None,
        }
    }

    // External stubs (issue #121) are attributed to a synthetic `<external>`
    // file, which `detect_language` can't map to a real language -- see
    // `Symbol::is_external`'s doc.
    fn dummy_external_symbol(qualname: &str) -> Symbol {
        Symbol {
            id: 2,
            file_path: "<external>".to_string(),
            kind: "external".to_string(),
            name: qualname.rsplit('.').next().unwrap_or(qualname).to_string(),
            qualname: qualname.to_string(),
            start_line: 0,
            start_col: 0,
            end_line: 0,
            end_col: 0,
            start_byte: 0,
            end_byte: 0,
            signature: None,
            docstring: None,
            graph_version: 1,
            commit_sha: None,
            stable_id: None,
        }
    }

    #[test]
    fn ts_tsx_variants_are_same_language() {
        let edge = dummy_edge();

        for (source, target, label) in [
            ("src/util.ts", "components/App.tsx", ".ts -> .tsx"),
            ("components/App.tsx", "src/util.ts", ".tsx -> .ts"),
            ("components/App.tsx", "components/Bar.tsx", ".tsx -> .tsx"),
        ] {
            let target_sym = dummy_symbol(target);
            let hop = build_hop(&target_sym, &edge, 1, (0, "p"), source, true);
            assert!(!hop.cross_language, "{label} should not be cross-language");
            assert!(
                hop.boundary_type.is_none(),
                "{label} should have no boundary type"
            );
            assert_eq!(hop.language, "typescript", "{label}");
        }
    }

    #[test]
    fn ts_tsx_to_other_language_is_cross_language() {
        let edge = dummy_edge();

        for (source, label) in [("frontend/util.ts", ".ts"), ("frontend/App.tsx", ".tsx")] {
            let target_sym = dummy_symbol("backend/app.py");
            let hop = build_hop(&target_sym, &edge, 1, (0, "p"), source, true);
            assert!(
                hop.cross_language,
                "{label} -> .py should be cross-language"
            );
            assert!(
                hop.boundary_type.is_some(),
                "{label} -> .py should have boundary type"
            );
        }
    }

    #[test]
    fn external_stub_is_not_cross_language() {
        let edge = dummy_edge();

        for (source, target_qualname, label) in [
            ("src/Program.cs", "ext:SHA256.Create", "C# -> BCL external"),
            ("src/main.rs", "ext:std::fs::read", "Rust -> std external"),
        ] {
            let target_sym = dummy_external_symbol(target_qualname);
            let hop = build_hop(&target_sym, &edge, 1, (0, "p"), source, true);
            assert!(
                !hop.cross_language,
                "{label}: external stub should not be reported as cross-language"
            );
            assert!(
                hop.boundary_type.is_none(),
                "{label}: external stub should have no boundary type"
            );
            assert!(
                hop.boundary_detail.is_none(),
                "{label}: external stub should have no boundary detail"
            );
        }
    }
}

// Regression tests for the read path no longer re-attributing edges the
// write path refused to resolve. Since issue #79, a genuinely unresolved
// non-Bridge-Edge-kind reference has no edge at all (only a store row) --
// these build a minimal DB directly (not through a fixture repo) so the
// ambiguity guard in `insert_edges` deterministically leaves a reference
// unresolved.
#[cfg(test)]
mod null_target_regression_tests {
    use super::*;
    use crate::db::Db;
    use crate::indexer::extract::{EdgeInput, ReceiverType, SymbolInput};
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn test_db() -> (Db, TempDir) {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("test.db");
        let db = Db::new(&db_path).unwrap();
        (db, temp)
    }

    fn symbol(qualname: &str, kind: &str, start_line: i64) -> SymbolInput {
        SymbolInput {
            kind: kind.to_string(),
            name: qualname.rsplit('.').next().unwrap_or(qualname).to_string(),
            qualname: qualname.to_string(),
            start_line,
            start_col: 0,
            end_line: start_line + 5,
            end_col: 0,
            start_byte: 0,
            end_byte: 100,
            signature: None,
            docstring: None,
            identity: None,
        }
    }

    fn calls_edge(source_qualname: &str, target_qualname: &str) -> EdgeInput {
        EdgeInput {
            kind: "CALLS".to_string(),
            source_qualname: Some(source_qualname.to_string()),
            target_qualname: Some(target_qualname.to_string()),
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: Some(1.0),
            trace_id: None,
            span_id: None,
            event_ts: None,
            receiver_type: ReceiverType::NotTracked,
            import_candidates: Vec::new(),
            bare_call: false,
            call_shape: None,
            source_start_byte: None,
            target_start_byte: None,
        }
    }

    /// A bare-name call with two same-language candidates is genuinely
    /// ambiguous, so `insert_edges`' ambiguity guard leaves it unresolved --
    /// no edge is written at all (issue #79), only a store row. `trace_flow`
    /// must not traverse it via a fuzzy qualname guess -- neither candidate
    /// should appear as a downstream hop from the caller.
    #[test]
    fn downstream_does_not_traverse_null_target_edge() {
        let (mut db, _temp) = test_db();
        let file_id = db
            .upsert_file("pkg/store.py", "h1", "python", 100, 0)
            .unwrap();
        let symbols = vec![
            symbol("builtins.list.append", "method", 1),
            symbol("pkg.store.EventStore.append", "method", 10),
            symbol("pkg.store.caller", "function", 20),
        ];
        let inserted = db
            .insert_symbols(file_id, "pkg/store.py", &symbols, 1, None)
            .unwrap();
        let caller_id = inserted
            .iter()
            .find(|s| s.qualname == "pkg.store.caller")
            .unwrap()
            .id;

        let edges = vec![calls_edge("pkg.store.caller", "append")];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        // Issue #79: an ambiguous CALLS edge is no longer written at all --
        // confirm the write path really did refuse to attribute it by
        // checking the unresolved-reference store instead of a NULL-target
        // edge row.
        let edge_count: i64 = db
            .read_conn()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE graph_version = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            edge_count, 0,
            "an ambiguous, unresolved CALLS edge must not be written at all"
        );
        let unresolved: i64 = db
            .read_conn()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM unresolved_references WHERE graph_version = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            unresolved, 1,
            "edge should be unresolved (ambiguous bare name)"
        );

        let config = TraceConfig {
            direction: TraceDirection::Downstream,
            allowed_kinds: vec!["CALLS".into()],
            ..Default::default()
        };
        let result = trace_flow(&db, vec![caller_id], None, None, 1, &config).unwrap();
        assert!(
            result.hops.is_empty(),
            "an unresolved reference must not be traversed downstream, got {:?}",
            result.hops
        );
    }

    /// Sanity check that the fix didn't throw out the happy path: an edge
    /// the write path genuinely resolved (exact qualname match, no
    /// ambiguity) must still be traversed.
    #[test]
    fn downstream_still_traverses_genuinely_resolved_edge() {
        let (mut db, _temp) = test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![
            symbol("mod.Caller", "function", 1),
            symbol("mod.Callee", "function", 10),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();
        let caller_id = inserted
            .iter()
            .find(|s| s.qualname == "mod.Caller")
            .unwrap()
            .id;
        let callee_id = inserted
            .iter()
            .find(|s| s.qualname == "mod.Callee")
            .unwrap()
            .id;

        let edges = vec![calls_edge("mod.Caller", "mod.Callee")];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let config = TraceConfig {
            direction: TraceDirection::Downstream,
            allowed_kinds: vec!["CALLS".into()],
            ..Default::default()
        };
        let result = trace_flow(&db, vec![caller_id], None, None, 1, &config).unwrap();
        assert_eq!(
            result.hops.len(),
            1,
            "resolved edge should still produce a hop"
        );
        assert_eq!(result.hops[0].symbol.id, callee_id);
    }
}
