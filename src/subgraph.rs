use crate::db::Db;
use crate::model::{Edge, Subgraph, Symbol};
use anyhow::Result;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Clone, Default)]
pub struct EdgeFilter {
    pub include: Option<HashSet<String>>,
    pub exclude: HashSet<String>,
    pub exclude_all: bool,
    pub resolved_only: bool,
}

pub fn build_subgraph(
    db: &Db,
    start_ids: &[i64],
    depth: usize,
    max_nodes: usize,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<Subgraph> {
    build_subgraph_filtered(
        db,
        start_ids,
        depth,
        max_nodes,
        languages,
        graph_version,
        None,
    )
}

pub fn build_subgraph_filtered(
    db: &Db,
    start_ids: &[i64],
    depth: usize,
    max_nodes: usize,
    languages: Option<&[String]>,
    graph_version: i64,
    filter: Option<&EdgeFilter>,
) -> Result<Subgraph> {
    let mut visited: HashSet<i64> = HashSet::new();
    let mut queue: VecDeque<(i64, usize)> = VecDeque::new();
    let mut sorted_start: Vec<i64> = start_ids.iter().copied().collect();
    sorted_start.sort_unstable();
    sorted_start.dedup();

    let mut edge_ids: HashSet<i64> = HashSet::new();
    let mut edges: Vec<Edge> = Vec::new();
    let mut module_target_cache: HashMap<String, Option<i64>> = HashMap::new();
    let mut calls_target_cache: HashMap<String, Option<i64>> = HashMap::new();
    let mut symbol_cache: HashMap<i64, String> = HashMap::new();
    let mut symbol_checked: HashSet<i64> = HashSet::new();

    cache_symbols(
        db,
        &mut symbol_cache,
        &mut symbol_checked,
        &sorted_start,
        languages,
        graph_version,
    )?;

    if languages.is_some() {
        sorted_start.retain(|id| symbol_cache.contains_key(id));
    }

    for id in sorted_start.iter().copied() {
        if visited.insert(id) {
            queue.push_back((id, 0));
        }
    }

    while let Some((id, dist)) = queue.pop_front() {
        if dist >= depth {
            continue;
        }
        let mut neighbors = db.edges_for_symbol(id, languages, graph_version)?;
        for edge in neighbors.iter_mut() {
            if (edge.kind == "MODULE_FILE" || edge.kind == "IMPORTS_FILE")
                && edge.target_symbol_id.is_none()
            {
                if let Some(target_qualname) = edge.target_qualname.as_deref() {
                    let resolved = if let Some(cached) = module_target_cache.get(target_qualname) {
                        *cached
                    } else {
                        let id = db.lookup_symbol_id_filtered(
                            target_qualname,
                            languages,
                            graph_version,
                        )?;
                        module_target_cache.insert(target_qualname.to_string(), id);
                        id
                    };
                    if let Some(resolved_id) = resolved {
                        edge.target_symbol_id = Some(resolved_id);
                    }
                }
            }
            // Resolve CALLS edges with NULL target_symbol_id
            if edge.kind == "CALLS" && edge.target_symbol_id.is_none() {
                if let Some(target_qualname) = edge.target_qualname.as_deref() {
                    let resolved = if let Some(cached) = calls_target_cache.get(target_qualname) {
                        *cached
                    } else {
                        let id = db.lookup_symbol_id_fuzzy(
                            target_qualname,
                            languages,
                            graph_version,
                        )?;
                        calls_target_cache.insert(target_qualname.to_string(), id);
                        id
                    };
                    if let Some(resolved_id) = resolved {
                        edge.target_symbol_id = Some(resolved_id);
                    }
                }
            }
        }
        if let Some(filter) = filter {
            if filter.exclude_all {
                neighbors.clear();
            } else {
                neighbors.retain(|edge| edge_allowed(edge, filter));
            }
        }
        let mut lookup_ids = Vec::new();
        for edge in &neighbors {
            if let Some(source_id) = edge.source_symbol_id {
                lookup_ids.push(source_id);
            }
            if let Some(target_id) = edge.target_symbol_id {
                lookup_ids.push(target_id);
            }
        }
        cache_symbols(
            db,
            &mut symbol_cache,
            &mut symbol_checked,
            &lookup_ids,
            languages,
            graph_version,
        )?;

        neighbors
            .sort_by(|a, b| edge_sort_key(a, &symbol_cache).cmp(&edge_sort_key(b, &symbol_cache)));

        for edge in neighbors {
            let source_ok = edge
                .source_symbol_id
                .map(|sid| symbol_cache.contains_key(&sid))
                .unwrap_or(true);
            let target_ok = edge
                .target_symbol_id
                .map(|tid| symbol_cache.contains_key(&tid))
                .unwrap_or(true);
            if !source_ok || !target_ok {
                continue;
            }
            if edge_ids.insert(edge.id) {
                edges.push(edge.clone());
            }
            let neighbor_id = if edge.source_symbol_id == Some(id) {
                edge.target_symbol_id
            } else {
                edge.source_symbol_id
            };
            if let Some(nid) = neighbor_id {
                if !symbol_cache.contains_key(&nid) {
                    continue;
                }
                if visited.len() < max_nodes && visited.insert(nid) {
                    queue.push_back((nid, dist + 1));
                }
            }
        }
        if visited.len() >= max_nodes {
            break;
        }
    }

    let mut ids: Vec<i64> = visited.into_iter().collect();
    ids.sort_unstable();
    let mut nodes: Vec<Symbol> = db.symbols_by_ids(&ids, languages, graph_version)?;
    nodes.sort_by(|a, b| a.qualname.cmp(&b.qualname).then_with(|| a.id.cmp(&b.id)));

    edges.sort_by(|a, b| edge_sort_key(a, &symbol_cache).cmp(&edge_sort_key(b, &symbol_cache)));

    Ok(Subgraph { nodes, edges })
}

fn cache_symbols(
    db: &Db,
    cache: &mut HashMap<i64, String>,
    checked: &mut HashSet<i64>,
    ids: &[i64],
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<()> {
    let mut missing: Vec<i64> = ids
        .iter()
        .copied()
        .filter(|id| !checked.contains(id))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    missing.sort_unstable();
    missing.dedup();
    let symbols = db.symbols_by_ids(&missing, languages, graph_version)?;
    for symbol in symbols {
        cache.insert(symbol.id, symbol.qualname);
    }
    for id in missing {
        checked.insert(id);
    }
    Ok(())
}

fn edge_sort_key(edge: &Edge, cache: &HashMap<i64, String>) -> (u8, String, String, String) {
    let source = edge
        .source_symbol_id
        .and_then(|id| cache.get(&id))
        .cloned()
        .unwrap_or_default();
    let target = edge
        .target_symbol_id
        .and_then(|id| cache.get(&id))
        .cloned()
        .or_else(|| edge.target_qualname.clone())
        .unwrap_or_default();
    let detail = edge.detail.clone().unwrap_or_default();
    (edge_rank(&edge.kind), source, target, detail)
}

fn edge_rank(kind: &str) -> u8 {
    match kind {
        "CONTAINS" => 0,
        "MODULE_FILE" => 1,
        "EXTENDS" => 2,
        "IMPLEMENTS" => 3,
        "IMPORTS_FILE" => 4,
        "IMPORTS" => 5,
        "CALLS" => 6,
        "XREF" => 7,
        _ => 10,
    }
}

fn edge_allowed(edge: &Edge, filter: &EdgeFilter) -> bool {
    if filter.resolved_only && (edge.source_symbol_id.is_none() || edge.target_symbol_id.is_none())
    {
        return false;
    }
    if let Some(include) = filter.include.as_ref() {
        if include.is_empty() {
            return false;
        }
        if !include.contains(&edge.kind) {
            return false;
        }
    }
    if !filter.exclude.is_empty() && filter.exclude.contains(&edge.kind) {
        return false;
    }
    true
}
