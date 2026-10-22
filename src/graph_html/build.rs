//! Scoped graphs for `lidx graph`: a seeded symbol neighbourhood (plus a
//! bounded one-hop halo the viewer can expand into) and file/module
//! aggregations of either that neighbourhood or the whole repository.
//!
//! Every edge is normalised to its *flow* direction before anything else
//! looks at it: the receiving side of a bridge pair (`CHANNEL_SUBSCRIBE`,
//! `RPC_IMPL`, `HTTP_ROUTE`, `CONFIG_READ`) is stored handler -> key, but
//! data flows key -> handler, so those edges are flipped (and flagged
//! [`EDGE_REVERSED`]). An unresolved bridge edge's target is its join key,
//! which becomes one shared endpoint node: a publisher and a subscriber of
//! the same channel then meet on it, in whatever languages they are written.

use crate::db::Db;
use crate::edge_views;
use crate::indexer::channel::boundary_type_for_kind;
use crate::indexer::test_detection::is_test_symbol;
use crate::model::{Edge, Symbol, xref_is_traversable};
use crate::search::{SearchScope, scope_allows};
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

pub const FLAG_SEED: u32 = 1;
pub const FLAG_HALO: u32 = 2;
pub const FLAG_EXTERNAL: u32 = 4;
pub const FLAG_UNRESOLVED: u32 = 8;
pub const FLAG_ENDPOINT: u32 = 16;
pub const FLAG_TEST: u32 = 32;
/// Outside `--scope` (tests, docs, examples, generated or tooling files when
/// the scope is `code`): embedded, but hidden until the viewer shows it.
pub const FLAG_SCOPE_OUT: u32 = 64;

pub const EDGE_REVERSED: u32 = 1;
pub const EDGE_HALO: u32 = 2;
/// A folder -> folder bridge edge derived from a shared endpoint (one side
/// publishes, calls, or sources a key the other side receives).
pub const EDGE_DERIVED: u32 = 4;

pub const GROUP_EXTERNAL: &str = "(external)";
pub const GROUP_BRIDGES: &str = "(bridges)";
pub const GROUP_UNRESOLVED: &str = "(unresolved)";

/// Halo neighbours embedded per scoped node, and in total.
const HALO_PER_NODE: usize = 24;
const HALO_MAX_TOTAL: usize = 2_000;
/// SQLite parameter batches for the per-level edge fetch.
const ID_BATCH: usize = 400;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    Symbol,
    File,
    Module,
    /// Applications and libraries (the module graph underneath).
    App,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Symbol => "symbol",
            Level::File => "file",
            Level::Module => "module",
            Level::App => "app",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
    Both,
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Up => "up",
            Direction::Down => "down",
            Direction::Both => "both",
        }
    }
}

/// Which edges a graph shows: an explicit `--kinds` list, or everything but
/// the structural `CONTAINS`/`MODULE_FILE` nesting (clusters show that).
/// Bare-name `XREF` matches are never shown; see [`xref_is_traversable`].
#[derive(Clone, Debug, Default)]
pub struct KindFilter {
    pub include: Option<HashSet<String>>,
}

impl KindFilter {
    pub fn allows(&self, edge: &Edge) -> bool {
        let kind_ok = match &self.include {
            Some(set) => set.contains(&edge.kind),
            None => !edge_views::contains(edge_views::STRUCTURAL, &edge.kind),
        };
        kind_ok && xref_is_traversable(edge)
    }
}

/// A node of the built graph before serialisation.
#[derive(Clone, Debug)]
pub struct GNode {
    pub name: String,
    pub qual: String,
    pub kind: String,
    pub lang: Option<String>,
    pub file: Option<String>,
    pub module: String,
    pub line: i64,
    pub flags: u32,
    pub sig: Option<String>,
    pub doc: Option<String>,
    /// Symbols a file/module node stands for; 1 for a symbol.
    pub size: i64,
}

#[derive(Clone, Debug)]
pub struct GEdge {
    pub from: usize,
    pub to: usize,
    pub kind: String,
    pub weight: u32,
    pub flags: u32,
    pub ev_line: i64,
    pub ev: Option<String>,
    pub ev_file: Option<String>,
    pub res: Option<String>,
    /// For a derived bridge edge, the endpoint keys it stands for; its
    /// weight is their count.
    pub keys: Vec<String>,
}

#[derive(Debug, Default)]
pub struct Graph {
    pub nodes: Vec<GNode>,
    pub edges: Vec<GEdge>,
    /// Leaves start collapsed into their module clusters.
    pub collapse_modules: bool,
    pub truncated: bool,
    pub notes: Vec<String>,
}

/// Inputs shared by every builder.
pub struct Ctx<'a> {
    pub db: &'a Db,
    pub graph_version: i64,
    pub languages: Option<Vec<String>>,
    pub filter: KindFilter,
    /// Directory depth a module is cut at (0: a file's whole directory).
    pub module_depth: usize,
    /// Live file path -> language.
    pub file_lang: HashMap<String, String>,
    /// Which files are in scope (`search`'s classifier).
    pub scope: SearchScope,
}

impl Ctx<'_> {
    fn langs(&self) -> Option<&[String]> {
        self.languages.as_deref()
    }

    pub fn module_of(&self, path: &str) -> String {
        module_of(path, self.module_depth)
    }

    /// Whether `path` is in `--scope`. `code` also leaves out generated and
    /// vendored files (`node_modules/`, `dist/`, ...).
    pub fn in_scope(&self, path: &str) -> bool {
        scope_allows(
            path,
            Some(self.scope),
            self.scope == SearchScope::Code,
            None,
        )
    }
}

/// `n` and `one` or `many`: "1 node", "2 nodes".
pub(crate) fn count(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

/// The module (directory, trailing `/`) a repo-relative file belongs to;
/// `./` at the root. A non-zero `depth` cuts it after that many directories.
pub fn module_of(path: &str, depth: usize) -> String {
    let mut dirs: Vec<&str> = path.split('/').collect();
    dirs.pop();
    if dirs.is_empty() {
        return "./".to_string();
    }
    if depth > 0 && dirs.len() > depth {
        dirs.truncate(depth);
    }
    dirs.join("/") + "/"
}

/// Live file path -> language for the current graph version.
pub fn file_languages(db: &Db, graph_version: i64) -> Result<HashMap<String, String>> {
    let conn = db.read_conn()?;
    let mut stmt = conn.prepare(
        "SELECT path, language FROM files
         WHERE deleted_version IS NULL OR deleted_version > ?",
    )?;
    let rows = stmt.query_map([graph_version], |r| Ok((r.get(0)?, r.get(1)?)))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Endpoint identity of a graph node while it is being built.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Key {
    Sym(i64),
    /// An unresolved bridge edge's join key (channel, route, config URI).
    Endpoint(String),
    /// An unresolved non-bridge target, by its written qualname.
    Unresolved(String),
}

/// Receiving side of a bridge pair: stored handler -> key, flows key -> handler.
pub(super) fn receiving(kind: &str) -> bool {
    matches!(
        kind,
        "CHANNEL_SUBSCRIBE" | "RPC_IMPL" | "HTTP_ROUTE" | "CONFIG_READ"
    )
}

fn is_bridge_kind(kind: &str) -> bool {
    kind != edge_views::XREF && edge_views::contains(edge_views::BRIDGE, kind)
}

/// `(from, to, flags)` of `edge` in flow direction; `None` for an edge with
/// no usable endpoints.
fn display(edge: &Edge) -> Option<(Key, Key, u32)> {
    let src = Key::Sym(edge.source_symbol_id?);
    let tgt = match edge.target_symbol_id {
        Some(t) => Key::Sym(t),
        None => {
            let tq = edge.target_qualname.as_deref()?.trim();
            if tq.is_empty() {
                return None;
            }
            if is_bridge_kind(&edge.kind) {
                Key::Endpoint(tq.to_string())
            } else {
                Key::Unresolved(tq.to_string())
            }
        }
    };
    if src == tgt {
        return None;
    }
    Some(if receiving(&edge.kind) {
        (tgt, src, EDGE_REVERSED)
    } else {
        (src, tgt, 0)
    })
}

/// One name per bridge endpoint. An RPC edge may store a guessed route
/// (`/Service/Method` without the proto package); the route it binds to is
/// a property of the index, not of the order edges are met in, so every edge
/// is rewritten to it before anything keys on its target.
///
/// A channel key is matched lowercased (`channel://orderstarted`); it is
/// shown by the name the code writes for it (`channel://OrderStarted`), so
/// every edge on one channel still meets on one key.
#[derive(Default)]
pub(crate) struct RouteCanon {
    cache: HashMap<String, String>,
    topics: Option<HashMap<String, String>>,
}

/// The name people read for each channel key: the one its edges write most
/// often, a capitalised one first on a tie, then the first in order.
pub(crate) fn topic_names(pairs: &[(String, String)]) -> HashMap<String, String> {
    let mut seen: HashMap<&str, BTreeMap<&str, usize>> = HashMap::new();
    for (key, name) in pairs {
        if crate::indexer::channel::normalize_channel_name(name).as_deref() == Some(key.as_str()) {
            *seen.entry(key).or_default().entry(name).or_default() += 1;
        }
    }
    seen.into_iter()
        .filter_map(|(key, names)| {
            let caps = |n: &str| n.chars().any(|c| c.is_ascii_uppercase());
            let best = names
                .into_iter()
                .max_by(|a, b| {
                    a.1.cmp(&b.1)
                        .then_with(|| caps(a.0).cmp(&caps(b.0)))
                        .then_with(|| b.0.cmp(a.0))
                })?
                .0;
            Some((key.to_string(), format!("channel://{best}")))
        })
        .collect()
}

impl RouteCanon {
    pub(crate) fn canonical(
        &mut self,
        db: &Db,
        graph_version: i64,
        kind: &str,
        tq: &str,
    ) -> String {
        if edge_views::contains(edge_views::CHANNEL_FAMILY, kind) {
            return self.topic(db, graph_version, tq);
        }
        if !edge_views::contains(edge_views::RPC_FAMILY, kind) || !tq.starts_with('/') {
            return tq.to_string();
        }
        if let Some(c) = self.cache.get(tq) {
            return c.clone();
        }
        let c = db
            .resolve_rpc_route(tq, graph_version)
            .ok()
            .flatten()
            .unwrap_or_else(|| tq.to_string());
        self.cache.insert(tq.to_string(), c.clone());
        c
    }

    /// A channel key as it is shown (see [`topic_names`]).
    pub(crate) fn topic(&mut self, db: &Db, graph_version: i64, key: &str) -> String {
        let topics = self.topics.get_or_insert_with(|| {
            let pairs = channel_names(db, graph_version).unwrap_or_default();
            topic_names(&pairs)
        });
        topics.get(key).cloned().unwrap_or_else(|| key.to_string())
    }

    /// Rewrite the target of every unresolved bridge edge in `edges`.
    fn normalise(&mut self, db: &Db, graph_version: i64, edges: &mut [Edge]) {
        for e in edges {
            if e.target_symbol_id.is_none()
                && is_bridge_kind(&e.kind)
                && let Some(tq) = e.target_qualname.as_deref()
            {
                let c = self.canonical(db, graph_version, &e.kind, tq.trim());
                e.target_qualname = Some(c);
            }
        }
    }
}

/// Every channel edge's key and the name its detail records.
fn channel_names(db: &Db, graph_version: i64) -> Result<Vec<(String, String)>> {
    let conn = db.read_conn()?;
    let mut stmt = conn.prepare(
        "SELECT target_qualname, detail FROM edges
         WHERE graph_version = ?1 AND kind IN ('CHANNEL_PUBLISH', 'CHANNEL_SUBSCRIBE')
           AND target_qualname IS NOT NULL AND detail IS NOT NULL",
    )?;
    let rows = stmt.query_map([graph_version], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (key, detail) = row?;
        if let Some(name) = serde_json::from_str::<serde_json::Value>(&detail)
            .ok()
            .and_then(|d| d.get("name").and_then(|n| n.as_str()).map(str::to_string))
        {
            out.push((key.trim().to_string(), name));
        }
    }
    Ok(out)
}

fn edge_rank(kind: &str) -> u8 {
    match kind {
        "CALLS" | "DISPATCH" => 0,
        k if is_bridge_kind(k) => 1,
        "EXTENDS" | "IMPLEMENTS" | "INHERITS" => 2,
        "USES" | "REFERENCES" => 3,
        "IMPORTS" | "IMPORTS_FILE" => 4,
        _ => 5,
    }
}

/// Symbol and edge lookups, cached across BFS levels.
struct Fetcher<'c, 'a> {
    ctx: &'c Ctx<'a>,
    edges: HashMap<Key, Vec<Edge>>,
    symbols: HashMap<i64, Option<Symbol>>,
    routes: RouteCanon,
}

impl<'c, 'a> Fetcher<'c, 'a> {
    fn new(ctx: &'c Ctx<'a>) -> Self {
        Self {
            ctx,
            edges: HashMap::new(),
            symbols: HashMap::new(),
            routes: RouteCanon::default(),
        }
    }

    fn load_symbols(&mut self, ids: &[i64]) -> Result<()> {
        let mut missing: Vec<i64> = ids
            .iter()
            .copied()
            .filter(|id| !self.symbols.contains_key(id))
            .collect();
        missing.sort_unstable();
        missing.dedup();
        for chunk in missing.chunks(ID_BATCH) {
            for id in chunk {
                self.symbols.insert(*id, None);
            }
            for sym in
                self.ctx
                    .db
                    .symbols_by_ids(chunk, self.ctx.langs(), self.ctx.graph_version)?
            {
                self.symbols.insert(sym.id, Some(sym));
            }
        }
        Ok(())
    }

    fn symbol(&self, id: i64) -> Option<&Symbol> {
        self.symbols.get(&id).and_then(Option::as_ref)
    }

    /// Whether a node may be walked through: not an external stub (whose
    /// only edges are every caller in the repo) and not an unresolved name.
    fn expandable(&self, key: &Key) -> bool {
        match key {
            Key::Sym(id) => self.symbol(*id).is_some_and(|s| !s.is_external()),
            Key::Endpoint(_) => true,
            Key::Unresolved(_) => false,
        }
    }

    /// Fetch (and cache) the filtered edges of every expandable key.
    fn load_edges(&mut self, keys: &[Key]) -> Result<()> {
        let ids: Vec<i64> = keys
            .iter()
            .filter(|k| !self.edges.contains_key(*k) && self.expandable(k))
            .filter_map(|k| match k {
                Key::Sym(id) => Some(*id),
                _ => None,
            })
            .collect();
        for chunk in ids.chunks(ID_BATCH) {
            let mut map = self.ctx.db.edges_for_symbols_with_dispatch(
                chunk,
                self.ctx.langs(),
                self.ctx.graph_version,
            )?;
            for id in chunk {
                let mut list = map.remove(id).unwrap_or_default();
                list.retain(|e| self.ctx.filter.allows(e));
                self.routes
                    .normalise(self.ctx.db, self.ctx.graph_version, &mut list);
                list.sort_by(|a, b| {
                    (edge_rank(&a.kind), a.id, &a.kind, a.source_symbol_id).cmp(&(
                        edge_rank(&b.kind),
                        b.id,
                        &b.kind,
                        b.source_symbol_id,
                    ))
                });
                self.edges.insert(Key::Sym(*id), list);
            }
        }
        let bridge_kinds: Vec<&str> = edge_views::BRIDGE
            .iter()
            .copied()
            .filter(|k| is_bridge_kind(k))
            .collect();
        for key in keys {
            let Key::Endpoint(tq) = key else { continue };
            if self.edges.contains_key(key) {
                continue;
            }
            let mut list = self.ctx.db.edges_by_target_qualname_and_kinds(
                tq,
                &bridge_kinds,
                self.ctx.langs(),
                self.ctx.graph_version,
            )?;
            list.retain(|e| self.ctx.filter.allows(e) && e.target_symbol_id.is_none());
            // A route guess that binds to this route is rewritten to it.
            self.routes
                .normalise(self.ctx.db, self.ctx.graph_version, &mut list);
            list.retain(|e| e.target_qualname.as_deref() == Some(tq.as_str()));
            list.sort_by_key(|e| (edge_rank(&e.kind), e.id));
            self.edges.insert(key.clone(), list);
        }
        Ok(())
    }

    fn edges_of(&self, key: &Key) -> &[Edge] {
        self.edges.get(key).map(Vec::as_slice).unwrap_or(&[])
    }
}

/// Scope of a symbol-level graph.
pub struct SymbolScope {
    pub seeds: Vec<i64>,
    /// Symbols that belong to the scope regardless of the walk
    /// (`--diff`'s callers from `analyze_diff`).
    pub extra: Vec<i64>,
    pub depth: usize,
    pub direction: Direction,
    pub max_nodes: usize,
}

/// One raw edge in flow direction, deduplicated across both endpoints' lists.
struct FlowEdge {
    from: Key,
    to: Key,
    edge: Edge,
    flags: u32,
}

/// Seeded neighbourhood: a BFS from the seeds in `direction`, up to `depth`
/// hops and `max_nodes` nodes, plus a bounded halo of one-hop neighbours.
pub fn symbol_graph(ctx: &Ctx, scope: &SymbolScope) -> Result<Graph> {
    let mut fetch = Fetcher::new(ctx);
    let mut seeds: Vec<i64> = scope.seeds.clone();
    seeds.sort_unstable();
    seeds.dedup();
    fetch.load_symbols(&seeds)?;
    fetch.load_symbols(&scope.extra)?;
    let mut graph = Graph::default();
    seeds.retain(|id| fetch.symbol(*id).is_some());
    // `max_nodes` bounds every node emitted: seeds, `extra`, the walk and the
    // halo (externals, endpoints and unresolved names included). The walk
    // leaves a fifth of it for the halo; the halo then takes what is left.
    let max_nodes = scope.max_nodes.max(seeds.len());
    if max_nodes > scope.max_nodes {
        graph.notes.push(format!(
            "--max-nodes {} raised to {max_nodes} to fit the seeds",
            scope.max_nodes
        ));
    }
    let walk_cap = (max_nodes - (max_nodes / 5).min(HALO_MAX_TOTAL)).max(seeds.len());

    let mut core: Vec<Key> = Vec::new();
    let mut in_core: HashSet<Key> = HashSet::new();
    for id in &seeds {
        let key = Key::Sym(*id);
        if in_core.insert(key.clone()) {
            core.push(key);
        }
    }
    let frontier_seeds = core.clone();
    let mut extra_trimmed = 0usize;
    for id in &scope.extra {
        let key = Key::Sym(*id);
        if fetch.symbol(*id).is_none() || in_core.contains(&key) {
            continue;
        }
        if core.len() >= walk_cap {
            extra_trimmed += 1;
            continue;
        }
        in_core.insert(key.clone());
        core.push(key);
    }
    if extra_trimmed > 0 {
        graph.truncated = true;
        graph.notes.push(format!(
            "{} of the diff left out (--max-nodes {max_nodes})",
            count(extra_trimmed, "caller", "callers")
        ));
    }
    let mut frontier = frontier_seeds;
    let mut capped = false;
    for _ in 0..scope.depth {
        if frontier.is_empty() {
            break;
        }
        fetch.load_edges(&frontier)?;
        let mut next: Vec<Key> = Vec::new();
        let mut new_ids: Vec<i64> = Vec::new();
        for node in &frontier {
            for edge in fetch.edges_of(node) {
                let Some((from, to, _)) = display(edge) else {
                    continue;
                };
                let down = matches!(scope.direction, Direction::Down | Direction::Both);
                let up = matches!(scope.direction, Direction::Up | Direction::Both);
                let other = if down && &from == node {
                    to
                } else if up && &to == node {
                    from
                } else {
                    continue;
                };
                if in_core.contains(&other) {
                    continue;
                }
                if core.len() >= walk_cap {
                    capped = true;
                    break;
                }
                if let Key::Sym(id) = other {
                    new_ids.push(id);
                }
                in_core.insert(other.clone());
                core.push(other.clone());
                next.push(other);
            }
        }
        fetch.load_symbols(&new_ids)?;
        // A symbol the language filter (or a deletion) hides is dropped.
        // So is one outside --scope: it is not walked through, but stays
        // eligible for the halo below.
        let dropped: HashSet<Key> = next
            .iter()
            .filter(|k| match k {
                Key::Sym(id) => match fetch.symbol(*id) {
                    None => true,
                    Some(sym) => !sym.is_external() && !ctx.in_scope(&sym.file_path),
                },
                _ => false,
            })
            .cloned()
            .collect();
        if !dropped.is_empty() {
            core.retain(|k| !dropped.contains(k));
            in_core.retain(|k| !dropped.contains(k));
            next.retain(|k| !dropped.contains(k));
        }
        frontier = next;
    }
    if capped {
        graph.truncated = true;
        graph.notes.push(format!(
            "walk stopped at {} (--max-nodes {max_nodes}, a fifth kept for the halo)",
            count(walk_cap, "node", "nodes")
        ));
    }

    // Every edge touching the scope; edges leaving it seed the halo.
    fetch.load_edges(&core)?;
    let halo_total = max_nodes.saturating_sub(core.len()).min(HALO_MAX_TOTAL);
    let mut halo: Vec<Key> = Vec::new();
    let mut in_halo: HashSet<Key> = HashSet::new();
    let mut flow: Vec<FlowEdge> = Vec::new();
    let mut seen_edges: HashSet<(i64, Key, Key, String)> = HashSet::new();
    let mut halo_skipped = 0usize;
    for node in &core {
        let mut per_node = 0usize;
        for edge in fetch.edges_of(node) {
            let Some((from, to, flags)) = display(edge) else {
                continue;
            };
            if &from != node && &to != node {
                continue;
            }
            let other = if &from == node { &to } else { &from };
            let both_core = in_core.contains(other);
            if !both_core && !in_halo.contains(other) {
                if per_node >= HALO_PER_NODE || halo.len() >= halo_total {
                    halo_skipped += 1;
                    continue;
                }
                per_node += 1;
                in_halo.insert(other.clone());
                halo.push(other.clone());
            }
            if seen_edges.insert((edge.id, from.clone(), to.clone(), edge.kind.clone())) {
                let halo_flag = if both_core { 0 } else { EDGE_HALO };
                flow.push(FlowEdge {
                    from,
                    to,
                    edge: edge.clone(),
                    flags: flags | halo_flag,
                });
            }
        }
    }
    if halo_skipped > 0 {
        graph.truncated = true;
        graph.notes.push(format!(
            "{} to further neighbours not embedded (halo budget: {halo_total} of --max-nodes {max_nodes})",
            count(halo_skipped, "edge", "edges")
        ));
    }
    let halo_ids: Vec<i64> = halo
        .iter()
        .filter_map(|k| match k {
            Key::Sym(id) => Some(*id),
            _ => None,
        })
        .collect();
    fetch.load_symbols(&halo_ids)?;

    let seed_set: HashSet<i64> = seeds.iter().copied().collect();
    let mut keys: Vec<(Key, u32)> = core.iter().map(|k| (k.clone(), 0)).collect();
    keys.extend(halo.iter().map(|k| (k.clone(), FLAG_HALO)));
    let mut index: HashMap<Key, usize> = HashMap::new();
    // Identical external stubs (one target qualname) become one node.
    let mut external_index: HashMap<String, usize> = HashMap::new();
    let endpoint_kinds = endpoint_kinds(&flow);
    for (key, extra) in keys {
        let node = match &key {
            Key::Sym(id) => {
                let Some(sym) = fetch.symbol(*id) else {
                    continue;
                };
                if sym.is_external()
                    && let Some(&i) = external_index.get(&sym.qualname)
                {
                    graph.nodes[i].flags &= extra | !FLAG_HALO;
                    index.insert(key, i);
                    continue;
                }
                if sym.is_external() {
                    external_index.insert(sym.qualname.clone(), graph.nodes.len());
                }
                let mut node = symbol_node(ctx, sym);
                if seed_set.contains(id) {
                    node.flags |= FLAG_SEED;
                }
                node
            }
            Key::Endpoint(tq) => endpoint_node(tq, endpoint_kinds.get(tq).map(String::as_str)),
            Key::Unresolved(tq) => unresolved_node(tq),
        };
        let mut node = node;
        node.flags |= extra;
        index.insert(key, graph.nodes.len());
        graph.nodes.push(node);
    }
    let mut seen_pairs: HashMap<(usize, usize, String), usize> = HashMap::new();
    for f in flow {
        let (Some(&from), Some(&to)) = (index.get(&f.from), index.get(&f.to)) else {
            continue;
        };
        if from == to {
            continue;
        }
        // Merged externals can make two edges one.
        let pair = (from, to, f.edge.kind.clone());
        if graph.nodes[to].flags & FLAG_EXTERNAL != 0
            || graph.nodes[from].flags & FLAG_EXTERNAL != 0
        {
            if let Some(&i) = seen_pairs.get(&pair) {
                graph.edges[i].weight += 1;
                graph.edges[i].flags &= f.flags;
                continue;
            }
            seen_pairs.insert(pair, graph.edges.len());
        }
        graph.edges.push(GEdge {
            from,
            to,
            kind: f.edge.kind.clone(),
            weight: 1,
            flags: f.flags,
            ev_line: f.edge.evidence_start_line.unwrap_or(0),
            ev: f.edge.evidence_snippet.clone(),
            ev_file: Some(f.edge.file_path.clone()),
            res: f.edge.resolution_kind.clone(),
            keys: Vec::new(),
        });
    }
    propagate_scope(&mut graph);
    Ok(graph)
}

/// An external or bridge-endpoint node has no file of its own: it is out of
/// scope when everything it is connected to is.
fn propagate_scope(graph: &mut Graph) {
    let mut in_scope_neighbour = vec![false; graph.nodes.len()];
    let mut has_edge = vec![false; graph.nodes.len()];
    for e in &graph.edges {
        for (me, other) in [(e.from, e.to), (e.to, e.from)] {
            has_edge[me] = true;
            if graph.nodes[other].flags & FLAG_SCOPE_OUT == 0 {
                in_scope_neighbour[me] = true;
            }
        }
    }
    for (i, n) in graph.nodes.iter_mut().enumerate() {
        if n.flags & (FLAG_EXTERNAL | FLAG_ENDPOINT) != 0 && has_edge[i] && !in_scope_neighbour[i] {
            n.flags |= FLAG_SCOPE_OUT;
        }
    }
}

/// The bridge kind each endpoint key was first reached through.
fn endpoint_kinds(flow: &[FlowEdge]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for f in flow {
        for key in [&f.from, &f.to] {
            if let Key::Endpoint(tq) = key {
                out.entry(tq.clone()).or_insert_with(|| f.edge.kind.clone());
            }
        }
    }
    out
}

fn symbol_node(ctx: &Ctx, sym: &Symbol) -> GNode {
    let external = sym.is_external();
    let mut flags = if external { FLAG_EXTERNAL } else { 0 };
    if !external && is_test_symbol(sym) {
        flags |= FLAG_TEST;
    }
    if !external && !ctx.in_scope(&sym.file_path) {
        flags |= FLAG_SCOPE_OUT;
    }
    GNode {
        name: if external {
            owner_and_name(&sym.qualname)
        } else {
            sym.name.clone()
        },
        qual: sym.qualname.clone(),
        kind: sym.kind.clone(),
        lang: if external {
            Some("external".to_string())
        } else {
            ctx.file_lang.get(&sym.file_path).cloned()
        },
        file: (!external).then(|| sym.file_path.clone()),
        module: if external {
            GROUP_EXTERNAL.to_string()
        } else {
            ctx.module_of(&sym.file_path)
        },
        line: if external { 0 } else { sym.start_line },
        flags,
        sig: crate::model::public_signature(sym.signature.clone()),
        doc: sym.docstring.clone(),
        size: 1,
    }
}

fn endpoint_node(tq: &str, kind: Option<&str>) -> GNode {
    GNode {
        name: tq.to_string(),
        qual: tq.to_string(),
        kind: kind
            .map(boundary_type_for_kind)
            .unwrap_or("bridge")
            .to_string(),
        lang: None,
        file: None,
        module: GROUP_BRIDGES.to_string(),
        line: 0,
        flags: FLAG_ENDPOINT,
        sig: None,
        doc: None,
        size: 1,
    }
}

/// The last two segments of a qualname with the separator between them
/// (`ext:std::collections::HashMap::new` -> `HashMap::new`,
/// `ext:./base:query` -> `base:query`), so `new` and `new` are told apart.
pub fn owner_and_name(qualname: &str) -> String {
    let rest = qualname.strip_prefix("ext:").unwrap_or(qualname);
    let last_sep = |text: &str| -> Option<(usize, usize)> {
        ["::", ":", ".", "/", "#"]
            .iter()
            .filter_map(|sep| text.rfind(sep).map(|i| (i, sep.len())))
            // Rightmost end; on a tie the longer separator (`::` over `:`).
            .max_by_key(|(i, len)| (i + len, *len))
    };
    let Some((i, len)) = last_sep(rest) else {
        return rest.to_string();
    };
    let (head, tail) = (&rest[..i], &rest[i + len..]);
    if tail.is_empty() {
        return rest.to_string();
    }
    let owner = match last_sep(head) {
        Some((j, l)) => &head[j + l..],
        None => head,
    };
    if owner.is_empty() {
        return tail.to_string();
    }
    format!("{owner}{}{tail}", &rest[i..i + len])
}

fn unresolved_node(tq: &str) -> GNode {
    GNode {
        name: owner_and_name(tq),
        qual: tq.to_string(),
        kind: "unresolved".to_string(),
        lang: None,
        file: None,
        module: GROUP_UNRESOLVED.to_string(),
        line: 0,
        flags: FLAG_UNRESOLVED,
        sig: None,
        doc: None,
        size: 1,
    }
}

/// Leaf of an aggregated graph.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) enum Leaf {
    File(String),
    Module(String),
    Endpoint(String),
    /// External stubs: in a whole-repo view, one chip per consumer folder
    /// (so no central hub forms); in a seeded view, one per package.
    External(String),
}

/// The package an `ext:` stub qualname belongs to.
fn external_package(qualname: &str) -> String {
    let rest = qualname.strip_prefix("ext:").unwrap_or(qualname);
    let cut = rest
        .find("::")
        .into_iter()
        .chain(rest.find(['.', '/']))
        .min();
    match cut {
        Some(i) if i > 0 => rest[..i].to_string(),
        _ => rest.to_string(),
    }
}

/// One edge to aggregate: its endpoints' leaves and evidence.
pub(super) struct AggInput {
    pub(super) from: Leaf,
    pub(super) to: Leaf,
    pub(super) kind: String,
    pub(super) flags: u32,
    pub(super) ev_line: i64,
    pub(super) ev: Option<String>,
    pub(super) ev_file: String,
    /// Endpoint keys a derived bridge edge goes through.
    pub(super) keys: Vec<String>,
}

#[derive(Default)]
struct LeafInfo {
    lang_counts: BTreeMap<String, i64>,
    symbols: i64,
    halo_only: bool,
    /// Some file (or symbol) of the leaf is in scope.
    in_scope: bool,
    seed: bool,
    endpoint_kind: Option<String>,
    /// External packages a consumer-folder chip stands for, with call counts.
    ext_packages: BTreeMap<String, i64>,
}

/// Group edges into leaf -> leaf weighted edges, one per kind, keeping the
/// first edge's evidence as a sample. Edges inside one leaf are dropped.
fn aggregate(
    ctx: &Ctx,
    leaves: BTreeMap<Leaf, LeafInfo>,
    inputs: Vec<AggInput>,
    collapse_modules: bool,
) -> Graph {
    let mut graph = Graph {
        collapse_modules,
        ..Graph::default()
    };
    let mut index: HashMap<Leaf, usize> = HashMap::new();
    for (leaf, info) in leaves {
        let lang = info
            .lang_counts
            .iter()
            .max_by_key(|(l, c)| (**c, std::cmp::Reverse(l.as_str())))
            .map(|(l, _)| l.clone());
        let mut flags = 0;
        if info.halo_only {
            flags |= FLAG_HALO;
        }
        let repo_leaf = matches!(leaf, Leaf::File(_) | Leaf::Module(_));
        if repo_leaf && !info.in_scope {
            flags |= FLAG_SCOPE_OUT;
        }
        if info.seed {
            flags |= FLAG_SEED;
        }
        let node = match &leaf {
            Leaf::File(path) => GNode {
                name: path.rsplit('/').next().unwrap_or(path).to_string(),
                qual: path.clone(),
                kind: "file".to_string(),
                lang: ctx.file_lang.get(path).cloned().or(lang),
                file: Some(path.clone()),
                module: ctx.module_of(path),
                line: 0,
                flags,
                sig: None,
                doc: None,
                size: info.symbols,
            },
            Leaf::Module(path) => GNode {
                name: path.clone(),
                qual: path.clone(),
                kind: "module".to_string(),
                lang,
                file: None,
                module: module_of(path.trim_end_matches('/'), 1),
                line: 0,
                flags,
                sig: None,
                doc: None,
                size: info.symbols,
            },
            Leaf::Endpoint(tq) => {
                let mut n = endpoint_node(tq, info.endpoint_kind.as_deref());
                n.flags |= flags;
                n
            }
            Leaf::External(key) if !info.ext_packages.is_empty() => {
                // A consumer folder's chip: it lives inside that folder.
                let mut pkgs: Vec<(&String, &i64)> = info.ext_packages.iter().collect();
                pkgs.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
                let listed: Vec<String> = pkgs
                    .iter()
                    .take(12)
                    .map(|(p, n)| format!("{p} ×{n}"))
                    .collect();
                let more = pkgs.len().saturating_sub(12);
                GNode {
                    name: count(pkgs.len(), "external package", "external packages"),
                    qual: format!("ext:{key}"),
                    kind: "external".to_string(),
                    lang: Some("external".to_string()),
                    file: None,
                    module: key.clone(),
                    line: 0,
                    flags: flags | FLAG_EXTERNAL,
                    sig: Some(format!(
                        "{}{}",
                        listed.join(", "),
                        if more > 0 {
                            format!(", +{more} more")
                        } else {
                            String::new()
                        }
                    )),
                    doc: None,
                    size: info.symbols,
                }
            }
            Leaf::External(pkg) => GNode {
                name: pkg.clone(),
                qual: format!("ext:{pkg}"),
                kind: "external".to_string(),
                lang: Some("external".to_string()),
                file: None,
                module: GROUP_EXTERNAL.to_string(),
                line: 0,
                flags: flags | FLAG_EXTERNAL,
                sig: None,
                doc: None,
                size: info.symbols,
            },
        };
        index.insert(leaf, graph.nodes.len());
        graph.nodes.push(node);
    }
    let mut agg: BTreeMap<(usize, usize, String), GEdge> = BTreeMap::new();
    let mut keys: BTreeMap<(usize, usize, String), BTreeSet<String>> = BTreeMap::new();
    for input in inputs {
        let (Some(&from), Some(&to)) = (index.get(&input.from), index.get(&input.to)) else {
            continue;
        };
        if from == to {
            continue;
        }
        let key = (from, to, input.kind.clone());
        if !input.keys.is_empty() {
            keys.entry(key.clone())
                .or_default()
                .extend(input.keys.iter().cloned());
        }
        agg.entry(key)
            .and_modify(|e| {
                e.weight += 1;
                e.flags &= input.flags;
            })
            .or_insert(GEdge {
                from,
                to,
                kind: input.kind,
                weight: 1,
                flags: input.flags,
                ev_line: input.ev_line,
                ev: input.ev,
                ev_file: (!input.ev_file.is_empty()).then_some(input.ev_file),
                res: None,
                keys: Vec::new(),
            });
    }
    // A derived edge weighs as many distinct keys as it stands for.
    for (key, set) in keys {
        if let Some(e) = agg.get_mut(&key) {
            let list: Vec<String> = set.into_iter().collect();
            let more = list.len().saturating_sub(4);
            e.ev = Some(format!(
                "{}{}",
                list.iter().take(4).cloned().collect::<Vec<_>>().join(", "),
                if more > 0 {
                    format!(" (+{more})")
                } else {
                    String::new()
                }
            ));
            e.weight = list.len() as u32;
            e.keys = list;
        }
    }
    graph.edges = agg.into_values().collect();
    propagate_scope(&mut graph);
    graph
}

/// Bridge kinds that send to an endpoint (the caller side), and the
/// transport each one names.
fn sender_transport(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "CHANNEL_PUBLISH" => "BUS",
        "RPC_CALL" => "RPC",
        "HTTP_CALL" => "HTTP",
        "CONFIG_SOURCE" => "CONFIG",
        _ => return None,
    })
}

/// Bridge kinds that receive from an endpoint (after the flow-direction
/// flip: the endpoint is their source).
fn receives(kind: &str) -> bool {
    matches!(
        kind,
        "CHANNEL_SUBSCRIBE" | "RPC_IMPL" | "HTTP_ROUTE" | "CONFIG_READ"
    )
}

/// Leaf -> leaf bridge edges through shared endpoints:
///
/// - `VIA_BUS` / `VIA_RPC` / `VIA_HTTP` / `VIA_CONFIG`: each leaf that sends
///   to an endpoint (publishes, calls, sources a key) joined to each leaf
///   that receives from it (subscribes, implements, serves, reads). This is
///   how a service map draws a cross-process call: between the owners, not
///   through a central hub.
/// - `DECLARES_RPC`: a proto's `RPC_ROUTE` declares the route; it joins the
///   proto to the implementers, never as a caller.
///
/// Every edge carries the endpoint key it goes through, so merging (here
/// into a pair, in the viewer into folders) counts distinct keys, not
/// files. A leaf is never joined to itself. Derived before any
/// `--max-nodes` trim, so a trimmed endpoint still links its owners.
pub(super) fn derive_leaf_bridges(inputs: &[AggInput]) -> Vec<AggInput> {
    let mut senders: BTreeMap<&str, BTreeSet<(&Leaf, &'static str)>> = BTreeMap::new();
    let mut receivers: BTreeMap<&str, BTreeSet<&Leaf>> = BTreeMap::new();
    let mut declarers: BTreeMap<&str, BTreeSet<&Leaf>> = BTreeMap::new();
    for i in inputs {
        match (&i.from, &i.to) {
            (Leaf::Endpoint(_), Leaf::Endpoint(_)) => {}
            (from, Leaf::Endpoint(p)) => {
                if let Some(t) = sender_transport(&i.kind) {
                    senders.entry(p).or_default().insert((from, t));
                } else if i.kind == "RPC_ROUTE" {
                    declarers.entry(p).or_default().insert(from);
                }
            }
            (Leaf::Endpoint(p), to) if receives(&i.kind) => {
                receivers.entry(p).or_default().insert(to);
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    let mut push = |from: &Leaf, to: &Leaf, kind: String, key: &str| {
        if from != to {
            out.push(AggInput {
                from: from.clone(),
                to: to.clone(),
                kind,
                flags: EDGE_DERIVED,
                ev_line: 0,
                ev: None,
                ev_file: String::new(),
                keys: vec![key.to_string()],
            });
        }
    };
    for (p, ss) in &senders {
        let Some(rs) = receivers.get(p) else { continue };
        for (a, t) in ss {
            for b in rs {
                push(a, b, format!("VIA_{t}"), p);
            }
        }
    }
    for (p, ds) in &declarers {
        let Some(rs) = receivers.get(p) else { continue };
        for d in ds {
            for b in rs {
                push(d, b, "DECLARES_RPC".to_string(), p);
            }
        }
    }
    out
}

/// Aggregate a symbol graph into files (or modules).
pub fn aggregate_symbol_graph(ctx: &Ctx, sym: Graph, level: Level) -> Graph {
    let leaf_of = |n: &GNode| -> Option<Leaf> {
        if n.flags & FLAG_ENDPOINT != 0 {
            return Some(Leaf::Endpoint(n.qual.clone()));
        }
        if n.flags & FLAG_EXTERNAL != 0 {
            return Some(Leaf::External(external_package(&n.qual)));
        }
        let file = n.file.as_ref()?;
        Some(match level {
            Level::Module => Leaf::Module(ctx.module_of(file)),
            _ => Leaf::File(file.clone()),
        })
    };
    let mut leaves: BTreeMap<Leaf, LeafInfo> = BTreeMap::new();
    let node_leaf: Vec<Option<Leaf>> = sym.nodes.iter().map(leaf_of).collect();
    for (n, leaf) in sym.nodes.iter().zip(&node_leaf) {
        let Some(leaf) = leaf else { continue };
        let info = leaves.entry(leaf.clone()).or_insert_with(|| LeafInfo {
            halo_only: true,
            ..LeafInfo::default()
        });
        info.symbols += 1;
        info.halo_only &= n.flags & FLAG_HALO != 0;
        info.in_scope |= n.flags & FLAG_SCOPE_OUT == 0;
        info.seed |= n.flags & FLAG_SEED != 0;
        if let Some(lang) = &n.lang {
            *info.lang_counts.entry(lang.clone()).or_default() += 1;
        }
        if n.flags & FLAG_ENDPOINT != 0 {
            info.endpoint_kind.get_or_insert_with(|| n.kind.clone());
        }
    }
    let mut inputs: Vec<AggInput> = sym
        .edges
        .into_iter()
        .filter_map(|e| {
            Some(AggInput {
                from: node_leaf[e.from].clone()?,
                to: node_leaf[e.to].clone()?,
                kind: e.kind,
                flags: e.flags,
                ev_line: e.ev_line,
                ev: e.ev,
                ev_file: e.ev_file.unwrap_or_default(),
                keys: Vec::new(),
            })
        })
        .collect();
    let derived = derive_leaf_bridges(&inputs);
    inputs.extend(derived);
    let mut graph = aggregate(ctx, leaves, inputs, false);
    graph.truncated = sym.truncated;
    graph.notes = sym.notes;
    graph
}

/// A live file: path, language and symbol count.
pub(crate) type RepoFile = (String, String, i64);

/// One stored edge leaving a live symbol, with what is known of its target.
pub(crate) struct RepoRow {
    pub kind: String,
    pub src_path: String,
    pub src_qual: String,
    pub src_line: i64,
    pub src_id: i64,
    pub tgt_id: Option<i64>,
    pub tgt_path: Option<String>,
    pub tgt_lang: Option<String>,
    pub tgt_qual: Option<String>,
    pub tq: Option<String>,
    pub ev: Option<String>,
    pub ev_line: Option<i64>,
    pub ev_file: String,
    /// The edge's detail, kept for HTTP calls: the raw URL names its host.
    pub detail: Option<String>,
}

fn lang_clause(ctx: &Ctx, alias: &str) -> String {
    match ctx.langs() {
        Some(langs) if !langs.is_empty() => format!(
            " AND {alias}.language IN ({})",
            langs
                .iter()
                .map(|l| format!("'{}'", l.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(",")
        ),
        _ => String::new(),
    }
}

/// Live files (in `--lang`) and their symbol counts.
pub(crate) fn repo_files(ctx: &Ctx) -> Result<Vec<RepoFile>> {
    let conn = ctx.db.read_conn()?;
    let sql = format!(
        "SELECT f.path, f.language, COUNT(s.id)
         FROM files f
         LEFT JOIN symbols s ON s.file_id = f.id AND s.graph_version = ?1
         WHERE (f.deleted_version IS NULL OR f.deleted_version > ?1)
           AND f.language != 'external'{}
         GROUP BY f.id ORDER BY f.path",
        lang_clause(ctx, "f")
    );
    let mut stmt = conn.prepare(&sql)?;
    let files = stmt
        .query_map([ctx.graph_version], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(files)
}

/// Every edge leaving a live symbol (in `--lang`) that `--kinds` allows.
pub(crate) fn repo_rows(ctx: &Ctx) -> Result<Vec<RepoRow>> {
    let conn = ctx.db.read_conn()?;
    let gv = ctx.graph_version;
    let sql = format!(
        "SELECT e.kind, sf.path, tf.path, tf.language, ts.qualname, e.target_qualname,
                e.evidence_snippet, e.evidence_start_line, ef.path,
                CASE WHEN e.kind IN ('XREF', 'HTTP_CALL') THEN e.detail END, ss.qualname, ss.start_line,
                ss.id, ts.id
         FROM edges e
         JOIN symbols ss ON e.source_symbol_id = ss.id
         JOIN files sf ON ss.file_id = sf.id
         JOIN files ef ON e.file_id = ef.id
         LEFT JOIN symbols ts ON e.target_symbol_id = ts.id
         LEFT JOIN files tf ON ts.file_id = tf.id
         WHERE e.graph_version = ?1
           AND (sf.deleted_version IS NULL OR sf.deleted_version > ?1)
           AND (tf.id IS NULL OR tf.deleted_version IS NULL OR tf.deleted_version > ?1)
           AND sf.language != 'external'{}{}",
        lang_clause(ctx, "sf"),
        crate::db::RPC_NAME_ONLY_FILTER
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut out = Vec::new();
    let mut rows = stmt.query([gv])?;
    while let Some(r) = rows.next()? {
        let kind: String = r.get(0)?;
        let detail: Option<String> = r.get(9)?;
        let http_detail = if kind == "HTTP_CALL" {
            detail.clone()
        } else {
            None
        };
        let probe = Edge {
            id: 0,
            file_path: String::new(),
            kind: kind.clone(),
            source_symbol_id: None,
            target_symbol_id: None,
            target_qualname: None,
            detail: if kind == "XREF" { detail.clone() } else { None },
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            resolution_kind: None,
            graph_version: gv,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
            dispatch_args: None,
            go_scope: None,
        };
        if !ctx.filter.allows(&probe) {
            continue;
        }
        out.push(RepoRow {
            kind,
            src_path: r.get(1)?,
            tgt_path: r.get(2)?,
            tgt_lang: r.get(3)?,
            tgt_qual: r.get(4)?,
            tq: r.get(5)?,
            ev: r.get(6)?,
            ev_line: r.get(7)?,
            ev_file: r.get(8)?,
            src_qual: r.get(10)?,
            src_line: r.get::<_, Option<i64>>(11)?.unwrap_or(0),
            src_id: r.get(12)?,
            tgt_id: r.get(13)?,
            detail: http_detail,
        });
    }
    Ok(out)
}

/// Whole-repository overview: every live file (or module) and the weighted
/// edges between them. `max_leaves` bounds the node count: a module
/// overview past it falls back to module leaves, a file overview keeps the
/// most connected files.
pub fn repo_graph(ctx: &Ctx, level: Level, max_leaves: usize) -> Result<Graph> {
    let files = repo_files(ctx)?;
    let rows = repo_rows(ctx)?;
    repo_graph_from(ctx, level, max_leaves, &files, &rows)
}

pub(crate) fn repo_graph_from(
    ctx: &Ctx,
    level: Level,
    max_leaves: usize,
    files: &[RepoFile],
    rows: &[RepoRow],
) -> Result<Graph> {
    let file_leaves = level == Level::File || files.len() <= max_leaves;
    let leaf_for_file = |path: &str| -> Leaf {
        if file_leaves {
            Leaf::File(path.to_string())
        } else {
            Leaf::Module(ctx.module_of(path))
        }
    };
    let mut leaves: BTreeMap<Leaf, LeafInfo> = BTreeMap::new();
    for (path, lang, count) in files {
        let info = leaves.entry(leaf_for_file(path)).or_default();
        info.in_scope |= ctx.in_scope(path);
        info.symbols += count;
        *info.lang_counts.entry(lang.clone()).or_default() += 1;
    }

    let mut inputs = Vec::new();
    let mut unresolved = 0usize;
    let mut routes = RouteCanon::default();
    for row in rows {
        let kind = row.kind.clone();
        let src = &row.src_path;
        let tgt = row.tgt_path.clone();
        let tgt_lang = row.tgt_lang.clone();
        let tgt_qual = row.tgt_qual.clone();
        let tq = row.tq.clone();
        let ev = row.ev.clone();
        let ev_line = row.ev_line;
        let ev_file = row.ev_file.clone();
        let from = leaf_for_file(src);
        if !leaves.contains_key(&from) {
            continue;
        }
        let to = match (tgt, tgt_lang) {
            (Some(_), Some(lang)) if lang == "external" => {
                // One chip per consumer folder, not a repo-wide hub.
                let consumer = match &from {
                    Leaf::Module(m) => m.clone(),
                    _ => ctx.module_of(src),
                };
                let pkg = external_package(tgt_qual.as_deref().unwrap_or("?"));
                *leaves
                    .entry(Leaf::External(consumer.clone()))
                    .or_default()
                    .ext_packages
                    .entry(pkg)
                    .or_default() += 1;
                Leaf::External(consumer)
            }
            (Some(path), _) => {
                let leaf = leaf_for_file(&path);
                if !leaves.contains_key(&leaf) {
                    // Outside the language filter.
                    continue;
                }
                leaf
            }
            (None, _) => match tq.as_deref().map(str::trim) {
                Some(tq) if !tq.is_empty() && is_bridge_kind(&kind) => {
                    Leaf::Endpoint(routes.canonical(ctx.db, ctx.graph_version, &kind, tq))
                }
                _ => {
                    unresolved += 1;
                    continue;
                }
            },
        };
        if let Leaf::External(_) | Leaf::Endpoint(_) = &to {
            let info = leaves.entry(to.clone()).or_default();
            if let Leaf::External(_) = &to {
                info.symbols += 1;
            }
            if let Leaf::Endpoint(_) = &to {
                info.endpoint_kind.get_or_insert_with(|| kind.clone());
            }
        }
        let (from, to, flags) = if receiving(&kind) {
            (to, from, EDGE_REVERSED)
        } else {
            (from, to, 0)
        };
        inputs.push(AggInput {
            from,
            to,
            kind,
            flags,
            ev_line: ev_line.unwrap_or(0),
            ev,
            ev_file,
            keys: Vec::new(),
        });
    }
    // Bridges between owners are derived before the trim below, so a small
    // --max-nodes keeps them even when it drops the endpoints.
    let derived = derive_leaf_bridges(&inputs);
    inputs.extend(derived);

    // Stored DISPATCH edges (a Go call through an interface) are in `rows`
    // above. The synthetic interface-method -> implementation hops that
    // `edges_for_symbols_with_dispatch` adds per visited node are not: the
    // whole-repo form of that query (`Db::all_dispatch_pairs`) takes ~25 s on
    // syft, ~250x the rest of this overview. Seeded views include them; the
    // page says the overview does not.
    let mut notes = vec![
        "interface-dispatch hops (interface method -> implementation) appear in seeded views only"
            .to_string(),
    ];
    if unresolved > 0 {
        notes.push(format!(
            "{} not shown in an aggregated view",
            count(
                unresolved,
                "unresolved non-bridge edge",
                "unresolved non-bridge edges"
            )
        ));
    }
    let mut truncated = !file_leaves;
    if !file_leaves {
        notes.push(format!(
            "{} exceed --max-nodes {max_leaves}: showing modules",
            count(files.len(), "file", "files")
        ));
    }
    // Every leaf counts against --max-nodes, externals and bridge endpoints
    // included: keep the most connected ones (repo files first on a tie).
    let total = leaves.len();
    if total > max_leaves {
        let mut degree: HashMap<&Leaf, u64> = HashMap::new();
        for i in &inputs {
            *degree.entry(&i.from).or_default() += 1;
            *degree.entry(&i.to).or_default() += 1;
        }
        let repo_leaf = |l: &Leaf| matches!(l, Leaf::File(_) | Leaf::Module(_));
        // Endpoints rank last: the derived bridges already join their owners.
        let endpoint = |l: &Leaf| matches!(l, Leaf::Endpoint(_));
        let mut ranked: Vec<&Leaf> = leaves.keys().collect();
        ranked.sort_by(|a, b| {
            endpoint(a)
                .cmp(&endpoint(b))
                .then_with(|| degree.get(b).unwrap_or(&0).cmp(degree.get(a).unwrap_or(&0)))
                .then_with(|| repo_leaf(b).cmp(&repo_leaf(a)))
                .then_with(|| a.cmp(b))
        });
        let drop: HashSet<Leaf> = ranked[max_leaves..].iter().map(|l| (*l).clone()).collect();
        let lost: BTreeSet<(&Leaf, &Leaf)> = inputs
            .iter()
            .filter(|i| !i.keys.is_empty() && (drop.contains(&i.from) || drop.contains(&i.to)))
            .map(|i| (&i.from, &i.to))
            .collect();
        if !lost.is_empty() {
            notes.push(format!(
                "{} dropped with their nodes by --max-nodes {max_leaves}",
                count(lost.len(), "bridge link", "bridge links")
            ));
        }
        leaves.retain(|l, _| !drop.contains(l));
        notes.push(format!(
            "kept the {max_leaves} most connected of {} (--max-nodes; externals and bridge endpoints count)",
            count(total, "node", "nodes")
        ));
        truncated = true;
    }
    let collapse = level == Level::Module && file_leaves;
    let mut graph = aggregate(ctx, leaves, inputs, collapse);
    graph.truncated = truncated;
    graph.notes = notes;
    Ok(graph)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_pluralises() {
        assert_eq!(count(1, "node", "nodes"), "1 node");
        assert_eq!(count(0, "node", "nodes"), "0 nodes");
        assert_eq!(count(2, "caller", "callers"), "2 callers");
    }

    #[test]
    fn module_of_uses_directory_and_depth() {
        assert_eq!(module_of("main.rs", 0), "./");
        assert_eq!(module_of("src/db/mod.rs", 0), "src/db/");
        assert_eq!(module_of("src/db/mod.rs", 1), "src/");
        assert_eq!(module_of("src/lib.rs", 3), "src/");
    }

    #[test]
    fn owner_and_name_keeps_the_owner() {
        assert_eq!(owner_and_name("ext:Vec::new"), "Vec::new");
        assert_eq!(
            owner_and_name("ext:std::collections::HashMap::new"),
            "HashMap::new"
        );
        assert_eq!(
            owner_and_name("ext:./base:baseQueryOptions"),
            "base:baseQueryOptions"
        );
        assert_eq!(owner_and_name("ext:fmt.Println"), "fmt.Println");
        assert_eq!(owner_and_name("ext:len"), "len");
        assert_eq!(owner_and_name("self.repo.save"), "repo.save");
    }

    #[test]
    fn external_package_takes_first_segment() {
        assert_eq!(external_package("ext:std::fs::read"), "std");
        assert_eq!(external_package("ext:SHA256.Create"), "SHA256");
        assert_eq!(external_package("ext:app/page.useState"), "app");
        assert_eq!(external_package("ext:fmt"), "fmt");
    }

    #[test]
    fn a_channel_is_shown_by_the_name_its_code_writes_most() {
        let pairs: Vec<(String, String)> = [
            ("channel://orderpaid", "OrderPaid"),
            ("channel://orderpaid", "orderpaid"),
            ("channel://orderpaid", "OrderPaid"),
            ("channel://jobqueue", "job-queue"),
            ("channel://jobqueue", "jobQueue"),
            // A name that is not the key's is ignored.
            ("channel://jobqueue", "other"),
            ("channel://other", "x"),
        ]
        .iter()
        .map(|(k, n)| (k.to_string(), n.to_string()))
        .collect();
        let names = topic_names(&pairs);
        assert_eq!(names["channel://orderpaid"], "channel://OrderPaid");
        // A tie: the capitalised one, though it sorts after.
        assert_eq!(names["channel://jobqueue"], "channel://jobQueue");
        assert!(!names.contains_key("channel://other"));
        assert_eq!(names.len(), 2);
    }
}
