use super::{Db, RPC_NAME_ONLY_FILTER, edge_from_row, symbol_from_row};
use crate::edge_views;
use crate::model::{
    CONTAINMENT_KIND, DISPATCH_CALL_DETAIL, DISPATCH_HOP_DETAIL, Edge, EdgeSnapshotRow,
    INTERFACE_DISPATCH_KIND, Symbol,
};
use anyhow::Result;
use rusqlite::OptionalExtension;
use std::collections::{HashMap, HashSet};

/// Max interface -> interface hops followed by dispatch.
const MAX_IFACE_CHAIN_DEPTH: i64 = 5;

/// Closed generic arguments an explicit impl names (`C.IA<int>.Run` ->
/// `int`); `None` for an implicit impl or an open one (issue #185).
pub fn closed_impl_args<'a>(qualname: &'a str, name: &str) -> Option<&'a str> {
    let head = qualname.strip_suffix(name)?.strip_suffix('.')?;
    type_args(head)
}

/// `N1.IA<int>` -> `int`: the type arguments of a receiver type / identity.
pub fn type_args(text: &str) -> Option<&str> {
    let open = text.find('<')?;
    let close = text.rfind('>')?;
    (close > open + 1).then(|| &text[open + 1..close])
}

/// Whether a call through a receiver typed with `call_args` reaches an impl
/// closed over `impl_args`: an open/unknown side pairs with everything.
pub fn dispatch_compatible(call_args: Option<&str>, impl_args: Option<&str>) -> bool {
    match (call_args, impl_args) {
        (Some(c), Some(i)) => c == i,
        _ => true,
    }
}

/// The receiver-type arguments each node of a downstream traversal was
/// entered with (issue #185), so a dispatch edge to a closed explicit impl
/// only follows a call whose receiver has the same ones. A node reached
/// several times keeps every distinct set, and one arrival without
/// arguments (or with a plain edge) makes it open to every closure.
///
/// Go (#482) uses the same shape for interfaces: a call whose receiver is
/// typed as an interface embedding the one declaring the method enters the
/// method spec with that interface, and the spec then fans out only to
/// implementations of it; walking upstream, a spec entered from an
/// implementation only continues to the callers whose receiver interface that
/// implementation's type satisfies (`upstream`).
#[derive(Debug, Default)]
pub struct EntryArgs {
    args: HashMap<i64, Option<Vec<String>>>,
    impls: HashMap<i64, Option<Vec<String>>>,
}

fn merge_entry(
    map: &mut HashMap<i64, Option<Vec<String>>>,
    node: i64,
    arg: Option<String>,
) -> bool {
    match (map.get_mut(&node), arg) {
        (None, arg) => {
            map.insert(node, arg.map(|a| vec![a]));
            true
        }
        (Some(None), _) => false,
        (Some(entry), None) => {
            *entry = None;
            true
        }
        (Some(Some(seen)), Some(a)) => {
            let new = !seen.contains(&a);
            if new {
                seen.push(a);
            }
            new
        }
    }
}

impl EntryArgs {
    /// Note an arrival at `node` through `edge`. `true` when that widens what
    /// the node reaches, so it must be (re-)expanded even if already visited.
    pub fn record(&mut self, node: i64, edge: &Edge, db: &Db, upstream: bool) -> Result<bool> {
        if upstream {
            let scope = edge
                .is_dispatch_hop()
                .then(|| edge.go_scope.clone())
                .flatten();
            return Ok(merge_entry(&mut self.impls, node, scope));
        }
        Ok(merge_entry(&mut self.args, node, edge.call_args(db)?))
    }

    /// Whether `edge`, expanded from `node`, may be followed.
    pub fn allows(&self, node: i64, edge: &Edge, upstream: bool) -> bool {
        if upstream {
            let Some(receiver) = edge
                .go_scope
                .as_deref()
                .filter(|_| edge.id > 0 && edge.kind == edge_views::DISPATCH)
            else {
                return true;
            };
            return match self.impls.get(&node) {
                Some(Some(lists)) => lists.iter().any(|l| in_scope(l, receiver)),
                _ => true,
            };
        }
        if edge.is_dispatch_hop()
            && let Some(scope) = edge.go_scope.as_deref()
        {
            return match self.args.get(&node) {
                Some(Some(seen)) => seen.iter().any(|a| in_scope(scope, a)),
                _ => true,
            };
        }
        let Some(impl_args) = edge.dispatch_args.as_deref() else {
            return true;
        };
        match self.args.get(&node) {
            Some(Some(seen)) => seen.iter().any(|a| a == impl_args),
            _ => true,
        }
    }
}

/// Whether `interface` is one of the `GO_SCOPE_SEP`-joined names in `list`.
fn in_scope(list: &str, interface: &str) -> bool {
    list.split(crate::model::GO_SCOPE_SEP)
        .any(|x| x == interface)
}

impl Edge {
    /// The closed generic arguments of this CALLS edge's receiver type, or,
    /// for a Go `DISPATCH` edge, the interface its receiver is typed as.
    pub fn call_args(&self, db: &Db) -> Result<Option<String>> {
        if self.id > 0 && self.kind == edge_views::DISPATCH {
            return Ok(self.go_scope.clone());
        }
        if !edge_views::is_call_like(&self.kind) || self.id <= 0 {
            return Ok(None);
        }
        let types = db.call_receiver_types(&[self.id])?;
        Ok(types
            .get(&self.id)
            .and_then(|t| type_args(t))
            .map(str::to_string))
    }
}

/// SQL: the closed generic arguments of an explicit impl `m` (the text
/// between the first `<` and the `>` ending its identity segment), else NULL.
fn explicit_args(m: &str, c: &str) -> String {
    let head = format!("substr({m}.qualname, 1, length({m}.qualname) - length({m}.name) - 1)");
    format!(
        "(CASE WHEN {explicit} AND substr({head}, -1) = '>' AND instr({head}, '<') > 0
               THEN substr({head}, instr({head}, '<') + 1, length({head}) - instr({head}, '<') - 1) END)",
        explicit = explicit_impl(m, c)
    )
}

/// SQL: the CALLS edge `ce` reaches the member `cm` of class `c` given its
/// closed generic arguments (see [`dispatch_compatible`]).
pub(super) fn call_reaches_impl_sql(ce: &str) -> String {
    format!(
        "({ce}.receiver_type IS NULL OR instr({ce}.receiver_type, '<') = 0
          OR {impl_args} IS NULL
          OR substr({ce}.receiver_type, instr({ce}.receiver_type, '<') + 1,
                    length({ce}.receiver_type) - instr({ce}.receiver_type, '<') - 1)
             = {impl_args})",
        impl_args = explicit_args("cm", "c")
    )
}

/// SQL: symbol `alias` carries `modifier` in the space-separated
/// `symbols.visibility` list (`private`, `static`, `override`), recorded by
/// the extractors (issue #238).
pub(crate) fn has_modifier_sql(alias: &str, modifier: &str) -> String {
    format!("((' ' || COALESCE({alias}.visibility, '') || ' ') LIKE '% {modifier} %')")
}

/// The `symbols.visibility` modifier of a function declared inside another
/// function (TypeScript/JavaScript `mod.outer.f`): bound only by a bare call
/// or its parent's CONTAINS edge, never by a name tier or as a member of a
/// same-named type; no top-level symbol (`dead_symbols`, `repo_map`).
pub(crate) const LOCAL_MODIFIER: &str = "local";

/// The `symbols.visibility` modifier of a bodiless member declaration
/// (TypeScript interface / type-literal members, `declare class` method
/// signatures): paired with its implementations by dispatch and bound by an
/// exact typed-receiver call, but never by a name tier, and never dead code.
pub(crate) const DECL_MODIFIER: &str = "decl";

/// The `symbols.visibility` modifier of a JS/TS `get` / `set` accessor: a
/// property read or write runs it, which no value reference to a function
/// means. Such an access is a `USES` edge with the `member` call shape,
/// bound only to an accessor; `dead_symbols` judges an accessor by it.
pub(crate) const ACCESSOR_MODIFIER: &str = "accessor";

/// The `symbols.visibility` modifier of a declaration a test framework runs
/// or collects by convention (a pytest fixture, a `TestCase` subclass, a C#
/// method with a test attribute and the class that holds it): a root of
/// `dead_symbols` while `tests_are_roots` is on.
pub(crate) const TEST_ENTRY_MODIFIER: &str = "test_entry";

/// The `symbols.visibility` modifier of a JS/TS declaration every file sees
/// as a global (a script file's top level, `declare global { }`).
pub(crate) const GLOBAL_MODIFIER: &str = "global";

/// Rust form of [`has_modifier_sql`]: whether a `symbols.visibility` value
/// carries `modifier`.
pub(crate) fn has_modifier(visibility: Option<&str>, modifier: &str) -> bool {
    visibility.is_some_and(|v| v.split_whitespace().any(|m| m == modifier))
}

/// SQL: the C# member `m`'s containing type has an EXTENDS/IMPLEMENTS
/// reference that never resolved to an in-repo type (an external base or
/// interface), so the framework may call the member (issue #238).
pub(super) fn external_base_member_sql(m: &str, gv: i64) -> String {
    format!(
        "EXISTS (
           SELECT 1 FROM edges xc
           JOIN unresolved_references xr ON xr.source_symbol_id = xc.source_symbol_id
           WHERE xc.target_symbol_id = {m}.id
             AND xc.kind = 'CONTAINS'
             AND xc.graph_version = {gv}
             AND xr.edge_kind IN ({subtyping})
             AND xr.graph_version = {gv})",
        subtyping = edge_views::sql_list(edge_views::SUBTYPING)
    )
}

/// SQL: the type `t` has a live member anywhere below it in the CONTAINS
/// tree (nested types included; issue #238). Live means an incoming
/// non-structural edge, an `override`, or a `const` / non-private `static`
/// field -- a field read leaves no edge, so its use cannot be disproved.
pub(super) fn live_nested_member_sql(t: &str, gv: i64) -> String {
    let is_override = has_modifier_sql("m", "override");
    let is_static = has_modifier_sql("m", "static");
    format!(
        "EXISTS (
           WITH RECURSIVE nested(id) AS (
             SELECT ce.target_symbol_id FROM edges ce
             WHERE ce.source_symbol_id = {t}.id AND ce.kind = 'CONTAINS'
               AND ce.graph_version = {gv} AND ce.target_symbol_id IS NOT NULL
             UNION
             SELECT ce.target_symbol_id FROM edges ce
             JOIN nested n ON ce.source_symbol_id = n.id
             WHERE ce.kind = 'CONTAINS' AND ce.graph_version = {gv}
               AND ce.target_symbol_id IS NOT NULL
           )
           SELECT 1 FROM nested n JOIN symbols m ON m.id = n.id
           WHERE EXISTS (
                   SELECT 1 FROM edges me
                   WHERE me.target_symbol_id = m.id
                     AND me.kind NOT IN ({structural})
                     AND me.graph_version = {gv})
              OR {is_override}
              OR (m.kind = 'field' AND {is_static}
                  AND (SELECT mf.language FROM files mf WHERE mf.id = m.file_id) = 'csharp'))",
        structural = edge_views::sql_list(edge_views::STRUCTURAL)
    )
}

/// `member` (alias `m`) declared on class `c` as an explicit interface
/// implementation (`C.<Iface>.<name>`, issue #181): its qualname is the
/// class's, a `.`, an identity segment, `.` and the member's own name.
fn explicit_impl(m: &str, c: &str) -> String {
    format!(
        "(length({m}.qualname) >= length({c}.qualname) + length({m}.name) + 3
          AND substr({m}.qualname, 1, length({c}.qualname) + 1) = {c}.qualname || '.'
          AND substr({m}.qualname, -(length({m}.name) + 1)) = '.' || {m}.name)"
    )
}

/// The interface name an explicit impl `m` on `c` names, as written and
/// without generic arguments (`C.N1.IA<int>.Run` -> `N1.IA`, issue #185).
fn explicit_base(m: &str, c: &str) -> String {
    let spec = format!(
        "substr({m}.qualname, length({c}.qualname) + 2,
                length({m}.qualname) - length({c}.qualname) - length({m}.name) - 2)"
    );
    format!(
        "(CASE WHEN instr({spec}, '<') > 0 THEN substr({spec}, 1, instr({spec}, '<') - 1) ELSE {spec} END)"
    )
}

/// Whether interface `i` is the one the written name `base` (from an
/// explicit impl on class `c`) denotes: `i`'s qualname is `base` or ends in
/// `.base`, unless `c` lists a *different* interface under that very text
/// in its base list (`class C : IA, N2.IA` -- the short `IA` is the first).
fn names_interface(base: &str, i: &str, c: &str, gv: i64) -> String {
    format!(
        "(({i}.qualname = {base} OR substr({i}.qualname, -(length({base}) + 1)) = '.' || {base})
          AND NOT EXISTS (SELECT 1 FROM edges e2
                           WHERE e2.source_symbol_id = {c}.id AND e2.kind = 'IMPLEMENTS'
                             AND e2.graph_version = {gv} AND e2.target_qualname = {base}
                             AND e2.target_symbol_id <> {i}.id))"
    )
}

/// What a dispatch query is about, so the ancestor recursion starts from the
/// few classes involved instead of every `IMPLEMENTS` edge in the repo.
#[derive(Debug, Clone)]
pub(super) enum DispatchSeed {
    /// Every class (`dead_symbols` runs one correlated probe per symbol).
    All,
    /// Implementing members: only their containing classes are expanded.
    Impls(String),
    /// Base members: the containing interfaces / classes, their implementors
    /// (a reverse closure) and those classes' ancestors are expanded.
    Bases(String),
    /// [`Self::Bases`] for members the classes reach by `CONTAINS` edge from
    /// another file than the class's (a Go method declared apart from its
    /// receiver type): their qualname is not under the class's.
    BasesCross(String),
}

impl DispatchSeed {
    /// `id, id, ..` list of the queried members.
    pub(super) fn impls(ids: &[i64]) -> Self {
        Self::Impls(id_list(ids))
    }

    pub(super) fn bases(ids: &[i64]) -> Self {
        Self::Bases(id_list(ids))
    }

    pub(super) fn bases_cross(ids: &[i64]) -> Self {
        Self::BasesCross(id_list(ids))
    }

    /// Whether the query starts from the classes (and walks to members).
    fn starts_from_classes(&self) -> bool {
        matches!(self, Self::Bases(_) | Self::BasesCross(_))
    }

    /// Containers of the members in `list`: qualname minus `<sep><name>` or
    /// the CONTAINS parent (see `dispatch_pairs_from`).
    fn containers(list: &str, gv: i64) -> String {
        let name = "length(m.name)";
        format!(
            "SELECT k.id FROM symbols m JOIN symbols k ON k.graph_version = {gv}
                 AND k.qualname IN (substr(m.qualname, 1, length(m.qualname) - {name} - 1),
                                    substr(m.qualname, 1, length(m.qualname) - {name} - 2))
              WHERE m.id IN ({list})
             UNION
             SELECT ce.source_symbol_id FROM edges ce
              WHERE ce.target_symbol_id IN ({list}) AND ce.kind = 'CONTAINS'
                AND ce.graph_version = {gv}"
        )
    }

    /// `(extra CTE before anc, extra filter on anc's first step)`.
    fn ctes(&self, gv: i64) -> (String, String) {
        match self {
            Self::All => (String::new(), String::new()),
            Self::Impls(list) => (
                String::new(),
                format!("AND source_symbol_id IN ({})", Self::containers(list, gv)),
            ),
            Self::Bases(list) | Self::BasesCross(list) => (
                format!(
                    "rel(id, d) AS (
                         SELECT id, 0 FROM ({seeds})
                         UNION
                         SELECT e.source_symbol_id, rel.d + 1
                           FROM rel JOIN edges e ON e.target_symbol_id = rel.id
                                                AND +e.kind IN ({subtyping})
                                                AND e.graph_version = {gv}
                          WHERE rel.d <= {MAX_IFACE_CHAIN_DEPTH}),",
                    seeds = Self::containers(list, gv),
                    subtyping = edge_views::sql_list(edge_views::SUBTYPING)
                ),
                "AND source_symbol_id IN (SELECT id FROM rel)".to_string(),
            ),
        }
    }
}

fn id_list(ids: &[i64]) -> String {
    ids.iter().map(i64::to_string).collect::<Vec<_>>().join(",")
}

/// `FROM` clause of the one dispatch query (issues #122, #185), shared by
/// [`Db::dispatch_pairs`] and `dead_symbols`: yields one row per
/// `(im.id = base member, cm.id = implementing member)` -- `cm`'s class `c`
/// IMPLEMENTS the interface (or EXTENDS the base class) `i`, and `im` is
/// `i`'s member of the same kind and name. `cm` is a method, property or
/// event. `c` is `cm`'s container: its qualname minus `<sep><name>` for 1-
/// and 2-char separators (`.` / `::`), or its CONTAINS parent (an explicit
/// impl's qualname carries an identity segment, so no fixed offset works).
/// An explicit impl pairs only with the interface its identity names; an
/// implicit one with every other one; through a base *class* only an
/// `override` pairs. Callers append their own `WHERE`/`JOIN`s.
/// `graph_version` is inlined (an `i64`, so injection-safe) so callers can
/// mix it into queries with their own positional parameters.
pub(super) fn dispatch_pairs_from(graph_version: i64, seed: &DispatchSeed) -> String {
    let gv = graph_version;
    let (rel_cte, anc_filter) = seed.ctes(gv);
    let subtyping = edge_views::sql_list(edge_views::SUBTYPING);
    // Every type a class reaches: its direct IMPLEMENTS/EXTENDS targets,
    // then interface -> interface and class -> base hops (issues #173,
    // #185), depth-bounded (cycle-safe: `UNION` dedups and `d` caps the
    // recursion). `ov` marks paths through a base class, whose members
    // only an `override` implements.
    let ancestors = format!(
        "WITH RECURSIVE {rel_cte} anc(cid, iid, d, ov) AS (
             SELECT source_symbol_id, target_symbol_id, 1, kind = 'EXTENDS' FROM edges
              WHERE kind IN ({subtyping}) AND graph_version = {gv}
                AND target_symbol_id IS NOT NULL {anc_filter}
             UNION
             SELECT anc.cid, e.target_symbol_id, anc.d + 1, anc.ov
               FROM anc JOIN edges e ON e.source_symbol_id = anc.iid
                                    -- `+` keeps the planner on idx_edges_source: with
                                    -- `kind IN (..)` it scanned every IMPLEMENTS edge per row
                                    AND +e.kind IN ({subtyping})
                                    AND e.graph_version = {gv}
                                    AND e.target_symbol_id IS NOT NULL
              WHERE anc.d <= {MAX_IFACE_CHAIN_DEPTH})
         SELECT cid, iid, ov FROM anc"
    );
    let parent = "substr(cm.qualname, 1, length(cm.qualname) - length(cm.name)";
    let cm_explicit = explicit_impl("cm", "c");
    let cm_names_i = names_interface(&explicit_base("cm", "c"), "i", "c", gv);
    let x_names_i = names_interface(&explicit_base("x", "c"), "i", "c", gv);
    let x_explicit = explicit_impl("x", "c");
    let x_args = explicit_args("x", "c");
    // An explicit twin of `i`'s member on `c` covering the closed arguments
    // `edge_args` a base-list entry `IA<..>` declared (NULL: any twin does).
    let twin_covers = |edge_args: &str| {
        format!(
            "EXISTS (SELECT 1 FROM symbols x
                      WHERE x.graph_version = {gv} AND +x.kind = cm.kind AND +x.name = cm.name
                        AND x.qualname > c.qualname || '.' AND x.qualname < c.qualname || '/'
                        AND {x_explicit} AND i.kind = 'interface' AND {x_names_i}
                        AND ({edge_args} IS NULL OR {x_args} IS NULL OR {x_args} = {edge_args}))"
        )
    };
    let twin_any = twin_covers("NULL");
    let twin_per_entry = twin_covers("de.detail");
    // Excluded once every closure of `i` the class lists is covered by an
    // explicit twin (`class C : IA<int>, IA<string>` with only
    // `IA<string>.Run` explicit still serves `IA<int>` implicitly).
    let twin_excludes = format!(
        "CASE WHEN EXISTS (SELECT 1 FROM edges de WHERE de.source_symbol_id = c.id
                              AND de.target_symbol_id = i.id AND de.kind = 'IMPLEMENTS'
                              AND de.graph_version = {gv})
              THEN NOT EXISTS (SELECT 1 FROM edges de WHERE de.source_symbol_id = c.id
                                  AND de.target_symbol_id = i.id AND de.kind = 'IMPLEMENTS'
                                  AND de.graph_version = {gv} AND NOT {twin_per_entry})
              ELSE {twin_any} END"
    );
    let c_is_parent = format!(
        "(c.qualname IN (
              {parent} - 1),
              {parent} - 2))
          OR c.id IN (SELECT ce.source_symbol_id FROM edges ce
                       WHERE ce.target_symbol_id = cm.id AND ce.kind = 'CONTAINS'
                         AND ce.graph_version = {gv}))"
    );
    // Queried by base member: start from the (few) implementing classes and
    // walk to their members by qualname range, in this order (`CROSS JOIN`
    // pins it); otherwise start from the queried member `cm`.
    let head = if matches!(seed, DispatchSeed::BasesCross(_)) {
        format!(
            "FROM ({ancestors}) a
             CROSS JOIN symbols c ON c.id = a.cid AND c.graph_version = {gv}
             CROSS JOIN symbols i ON i.id = a.iid
             CROSS JOIN edges ce ON ce.source_symbol_id = c.id AND ce.kind = 'CONTAINS'
                                AND ce.graph_version = {gv}
             CROSS JOIN symbols cm ON cm.id = ce.target_symbol_id AND cm.graph_version = {gv}
                                  AND cm.file_id <> c.file_id"
        )
    } else if seed.starts_from_classes() {
        format!(
            "FROM ({ancestors}) a
             CROSS JOIN symbols c ON c.id = a.cid AND c.graph_version = {gv}
             CROSS JOIN symbols i ON i.id = a.iid
             CROSS JOIN symbols cm INDEXED BY idx_symbols_qualname ON cm.graph_version = {gv}
                                  AND cm.qualname > c.qualname AND cm.qualname < c.qualname || '~'
                                  AND {c_is_parent}"
        )
    } else {
        format!(
            "FROM symbols cm
             JOIN symbols c ON c.graph_version = {gv} AND {c_is_parent}
             JOIN ({ancestors}) a ON a.cid = c.id
             JOIN symbols i ON i.id = a.iid"
        )
    };
    let join = if seed.starts_from_classes() {
        "CROSS JOIN"
    } else {
        "JOIN"
    };
    // JavaScript/TypeScript: `override` is optional, so a base-class member
    // pairs with every same-named one; a method and a function-valued field
    // are one member kind.
    // A constructor is no override; neither is an interface redeclaring a
    // member (only a body or a class declaration implements one).
    let cm_decl = has_modifier_sql("cm", DECL_MODIFIER);
    let cm_static = has_modifier_sql("cm", "static");
    let im_static = has_modifier_sql("im", "static");
    let js = "fc.language IN ('typescript', 'tsx', 'javascript')";
    let js_kinds =
        format!("({js} AND +im.kind IN ('method', 'field') AND +cm.kind IN ('method', 'field'))");
    format!(
        "{head}
         -- One indexed `im.qualname = <expr>` equality: an explicit impl pairs
         -- only with the interface it names, an implicit one keeps the tail.
         {join} symbols im ON im.qualname = CASE WHEN {cm_explicit}
                                               THEN i.qualname || '.' || cm.name
                                               WHEN substr(cm.qualname, 1, length(c.qualname)) = c.qualname
                                               THEN i.qualname || substr(cm.qualname, length(c.qualname) + 1)
                                               -- a member declared apart from its class
                                               -- (Go): its qualname is not under the class's
                                               ELSE i.qualname || '.' || cm.name END
                        AND +im.name = +cm.name AND im.graph_version = {gv}
         JOIN files fc ON fc.id = cm.file_id
                      AND (fc.deleted_version IS NULL OR fc.deleted_version > {gv})
         JOIN files fi ON fi.id = im.file_id
                      AND (fi.deleted_version IS NULL OR fi.deleted_version > {gv})
         WHERE (cm.kind IN ('method', 'property', 'event') OR ({js} AND cm.kind = 'field'))
           AND cm.graph_version = {gv}
           AND (+im.kind = +cm.kind OR {js_kinds})
           AND NOT ({js} AND ({cm_static} OR {im_static} OR cm.name = 'constructor' OR ({cm_decl} AND c.kind IN ('interface', 'type'))))
           AND CASE WHEN {cm_explicit}
                    THEN i.kind = 'interface' AND {cm_names_i}
                    ELSE (a.ov = 0 OR {js} OR (' ' || COALESCE(cm.visibility, '') || ' ') LIKE '% override %')
                         -- an implicit impl is not paired with an interface whose closures all have explicit twins
                         AND NOT ({twin_excludes})
               END"
    )
}

impl Db {
    /// The one lookup behind interface dispatch (issue #122): a call through
    /// an interface-typed receiver only ever resolves to the *interface*
    /// method, so the implementing method looks uncalled. Returns every
    /// `(interface_method_id, impl_method_id)` pair where either side is in
    /// `ids`, via class IMPLEMENTS edges + same method name. Language-
    /// agnostic. Follows interface inheritance chains and, for C#, an
    /// `override` reached through EXTENDS (issue #185).
    pub fn dispatch_pairs(&self, ids: &[i64], graph_version: i64) -> Result<Vec<(i64, i64)>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let list = ids.iter().map(i64::to_string).collect::<Vec<_>>().join(",");
        // One statement per side: an `OR` across `cm.id` / `im.id` would
        // defeat the rowid lookup and scan every symbol.
        let conn = self.read_conn()?;
        let mut pairs = Vec::new();
        for (side, seed) in [
            ("cm", DispatchSeed::impls(ids)),
            ("im", DispatchSeed::bases(ids)),
            ("im", DispatchSeed::bases_cross(ids)),
        ] {
            let from = dispatch_pairs_from(graph_version, &seed);
            let sql = format!("SELECT DISTINCT im.id, cm.id {from} AND {side}.id IN ({list})");
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
            for row in rows {
                pairs.push(row?);
            }
        }
        // Rust: an `impl Trait for Ty` method carries its own member-level
        // `IMPLEMENTS` edge to the trait's method, also when `Ty` is no symbol
        // of the repository (`Option<T>`, `&T`, a blanket `T`), which the
        // class-based query above cannot see.
        for side in ["e.source_symbol_id", "e.target_symbol_id"] {
            let sql = format!(
                "SELECT DISTINCT e.target_symbol_id, e.source_symbol_id
                   FROM edges e
                   JOIN symbols a ON a.id = e.source_symbol_id
                   JOIN symbols b ON b.id = e.target_symbol_id
                   JOIN files fa ON fa.id = a.file_id
                  WHERE e.kind = 'IMPLEMENTS' AND e.graph_version = ?1
                    AND a.kind = 'method' AND b.kind = 'method' AND fa.language = 'rust'
                    AND {side} IN ({list})"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([graph_version], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
            })?;
            for row in rows {
                pairs.push(row?);
            }
        }
        pairs.sort_unstable();
        pairs.dedup();
        Ok(pairs)
    }

    /// Every `(base member id, implementing member id)` pair of the dispatch
    /// query ([`DispatchSeed::All`]): the same rows `dead_symbols` uses to
    /// keep implementations live. Dumped by `lidx dispatch-pairs` so the
    /// oracle benchmark can grade the pairing against compiler output.
    pub fn all_dispatch_pairs(&self, graph_version: i64) -> Result<Vec<(i64, i64)>> {
        let from = dispatch_pairs_from(graph_version, &DispatchSeed::All);
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&format!("SELECT DISTINCT im.id, cm.id {from}"))?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
        let mut pairs = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        pairs.sort_unstable();
        Ok(pairs)
    }

    /// [`Db::dispatch_pairs`] for one method, split by side.
    pub fn dispatch_peers(&self, symbol_id: i64, graph_version: i64) -> Result<DispatchPeers> {
        let mut peers = DispatchPeers::default();
        for (iface, imp) in self.dispatch_pairs(&[symbol_id], graph_version)? {
            if imp == symbol_id {
                peers.interface_methods.push(iface);
            }
            if iface == symbol_id {
                peers.impl_methods.push(imp);
            }
        }
        Ok(peers)
    }

    /// Types that IMPLEMENT `type_id` (incoming IMPLEMENTS edges).
    pub fn implementing_types(&self, type_id: i64, graph_version: i64) -> Result<Vec<i64>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT source_symbol_id FROM edges
             WHERE target_symbol_id = ?1 AND kind = 'IMPLEMENTS' AND graph_version = ?2
               AND source_symbol_id IS NOT NULL
             ORDER BY source_symbol_id",
        )?;
        let rows = stmt.query_map(rusqlite::params![type_id, graph_version], |r| {
            r.get::<_, i64>(0)
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// [`Db::edges_for_symbols`] plus, for every id, the synthetic
    /// interface-dispatch edges around it: CALLS edges from an interface
    /// method to each implementing method (`resolution_kind =
    /// "interface_dispatch"`, id 0), listed under both endpoints. Downstream
    /// from the interface method reaches the impls; upstream from an impl
    /// reaches the interface method and, through its real edges, its
    /// callers. Used by graph traversals (trace_flow, analyze_impact);
    /// every other `edges_for_symbol(s)` caller keeps raw-edge semantics.
    pub fn edges_for_symbols_with_dispatch(
        &self,
        ids: &[i64],
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<HashMap<i64, Vec<Edge>>> {
        let mut map = self.edges_for_symbols(ids, languages, graph_version)?;
        self.annotate_go_receivers(&mut map)?;
        for (iface, imp) in self.dispatch_pairs(ids, graph_version)? {
            let peer_file = |id: i64| -> Result<String> {
                Ok(self
                    .get_symbol_by_id(id)?
                    .map(|s| s.file_path)
                    .unwrap_or_default())
            };
            let imp_sym = self.get_symbol_by_id(imp)?;
            let closed = imp_sym
                .as_ref()
                .and_then(|s| closed_impl_args(&s.qualname, &s.name))
                .map(str::to_string);
            let go_scope = match imp_sym.as_ref() {
                Some(sym) if sym.file_path.ends_with(".go") => self.go_impl_interfaces(imp)?,
                _ => None,
            };
            let make = |file_path: String, source: i64| Edge {
                id: 0,
                file_path,
                kind: "CALLS".to_string(),
                source_symbol_id: Some(source),
                target_symbol_id: Some(imp),
                target_qualname: None,
                detail: Some(
                    if source == iface {
                        DISPATCH_HOP_DETAIL
                    } else {
                        DISPATCH_CALL_DETAIL
                    }
                    .to_string(),
                ),
                evidence_snippet: None,
                evidence_start_line: None,
                evidence_end_line: None,
                confidence: None,
                resolution_kind: Some(INTERFACE_DISPATCH_KIND.to_string()),
                graph_version,
                commit_sha: None,
                trace_id: None,
                span_id: None,
                event_ts: None,
                dispatch_args: closed.clone(),
                go_scope: if source == iface {
                    go_scope.clone()
                } else {
                    None
                },
            };
            if ids.contains(&iface) {
                map.entry(iface)
                    .or_default()
                    .push(make(peer_file(imp)?, iface));
            }
            if !ids.contains(&imp) {
                continue;
            }
            let Some(args) = closed.clone() else {
                map.entry(imp)
                    .or_default()
                    .push(make(peer_file(iface)?, iface));
                continue;
            };
            // A closed explicit impl is reached by the calls whose receiver
            // type matches its arguments, not by every call to the
            // interface method: link those callers straight to it.
            let callers = self.edges_for_symbol(iface, languages, graph_version)?;
            let recv =
                self.call_receiver_types(&callers.iter().map(|e| e.id).collect::<Vec<_>>())?;
            let mut seen = HashSet::new();
            for e in callers {
                let Some(src) = e.source_symbol_id else {
                    continue;
                };
                let call_args = recv.get(&e.id).and_then(|t| type_args(t));
                if edge_views::is_call_like(&e.kind)
                    && e.target_symbol_id == Some(iface)
                    && dispatch_compatible(call_args, Some(&args))
                    && seen.insert(src)
                {
                    map.entry(imp)
                        .or_default()
                        .push(make(e.file_path.clone(), src));
                }
            }
        }
        // A function declared inside another is reached through its parent
        // (nothing calls a returned closure): list the parent as its caller.
        for (local, parent, file_path) in self.local_function_parents(ids, graph_version)? {
            let entry = map.entry(local).or_default();
            let called = entry.iter().any(|e| {
                e.kind == "CALLS"
                    && e.source_symbol_id == Some(parent)
                    && e.target_symbol_id == Some(local)
            });
            if called {
                continue;
            }
            entry.push(Edge {
                id: 0,
                file_path,
                kind: "CALLS".to_string(),
                source_symbol_id: Some(parent),
                target_symbol_id: Some(local),
                target_qualname: None,
                detail: Some("contained by".to_string()),
                evidence_snippet: None,
                evidence_start_line: None,
                evidence_end_line: None,
                confidence: None,
                resolution_kind: Some(CONTAINMENT_KIND.to_string()),
                graph_version,
                commit_sha: None,
                trace_id: None,
                span_id: None,
                event_ts: None,
                dispatch_args: None,
                go_scope: None,
            });
        }
        Ok(map)
    }

    /// `(local function id, CONTAINS parent id, file path)` for every `local`
    /// function among `ids`.
    fn local_function_parents(
        &self,
        ids: &[i64],
        graph_version: i64,
    ) -> Result<Vec<(i64, i64, String)>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let is_local = has_modifier_sql("s", LOCAL_MODIFIER);
        let placeholders = vec!["?"; ids.len()].join(",");
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT s.id, c.source_symbol_id, f.path
             FROM symbols s
             JOIN files f ON f.id = s.file_id
             JOIN edges c ON c.target_symbol_id = s.id AND c.kind = 'CONTAINS'
                         AND c.graph_version = ? AND c.source_symbol_id IS NOT NULL
             WHERE s.id IN ({placeholders}) AND {is_local}"
        ))?;
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version];
        params.extend(ids.iter().map(|id| id as &dyn rusqlite::ToSql));
        let rows = stmt.query_map(&*params, |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Real edges into the interface-method peers of `id`: the callers that
    /// reach `id` only *through* the interface, not by calling it directly.
    /// Empty when `id` implements no interface method. Callers filter by
    /// kind (`CALLS`) and use `source_symbol_id`; results are never
    /// synthetic. Consumers that report coverage or callers should label
    /// these as via-interface rather than direct: a call to `IFoo.Run`
    /// reaches every implementor.
    pub fn interface_caller_edges(
        &self,
        id: i64,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Edge>> {
        let mut out = Vec::new();
        for peer in self.dispatch_peers(id, graph_version)?.interface_methods {
            out.extend(
                self.edges_for_symbol(peer, languages, graph_version)?
                    .into_iter()
                    .filter(|e| e.target_symbol_id == Some(peer)),
            );
        }
        Ok(out)
    }

    /// Stored `receiver_type` of the given edges that carry generic
    /// arguments (`IA<int>`); the value dispatch matches against closed
    /// explicit impls.
    pub fn call_receiver_types(&self, edge_ids: &[i64]) -> Result<HashMap<i64, String>> {
        let ids: Vec<String> = edge_ids
            .iter()
            .filter(|id| **id > 0)
            .map(i64::to_string)
            .collect();
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT id, receiver_type FROM edges
             WHERE id IN ({}) AND receiver_type LIKE '%<%'",
            ids.join(",")
        ))?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
        Ok(rows.collect::<rusqlite::Result<HashMap<_, _>>>()?)
    }

    /// Sets [`Edge::go_scope`] on the stored `DISPATCH` edges of `map`: the
    /// interface the call's receiver is typed as, kept in `receiver_scope`
    /// when it embeds the interface declaring the method.
    fn annotate_go_receivers(&self, map: &mut HashMap<i64, Vec<Edge>>) -> Result<()> {
        let mut ids: Vec<i64> = map
            .values()
            .flatten()
            .filter(|e| e.id > 0 && e.kind == edge_views::DISPATCH)
            .map(|e| e.id)
            .collect();
        if ids.is_empty() {
            return Ok(());
        }
        ids.sort_unstable();
        ids.dedup();
        let conn = self.read_conn()?;
        let mut scopes: HashMap<i64, String> = HashMap::new();
        for chunk in ids.chunks(500) {
            let list = chunk
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let mut stmt = conn.prepare(&format!(
                "SELECT id, receiver_scope FROM edges
                 WHERE id IN ({list}) AND receiver_scope IS NOT NULL"
            ))?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
            for row in rows {
                let (id, scope) = row?;
                scopes.insert(id, scope);
            }
        }
        for edge in map.values_mut().flatten() {
            if let Some(scope) = scopes.get(&edge.id) {
                edge.go_scope = Some(scope.clone());
            }
        }
        Ok(())
    }

    /// The interfaces the Go type declaring method `imp` satisfies, and those
    /// of the types that reach `imp` by embedding (directly or through other
    /// embedders; their method sets hold it), joined by
    /// [`crate::model::GO_SCOPE_SEP`]. An embedder that declares a method of
    /// the same name itself shadows `imp`: it reaches it only from its own
    /// body, so neither it nor the types embedding it count. `None` when the
    /// type is unknown: then nothing is narrowed.
    fn go_impl_interfaces(&self, imp: i64) -> Result<Option<String>> {
        let conn = self.read_conn()?;
        let gv = self.current_graph_version()?;
        let name: String = conn.query_row(
            "SELECT name FROM symbols WHERE id = ?1",
            rusqlite::params![imp],
            |r| r.get(0),
        )?;
        let owners: Vec<i64> = {
            let mut stmt = conn.prepare(
                "SELECT source_symbol_id FROM edges
                 WHERE target_symbol_id = ?1 AND kind = 'CONTAINS' AND graph_version = ?2
                   AND source_symbol_id IS NOT NULL",
            )?;
            let rows = stmt.query_map(rusqlite::params![imp, gv], |r| r.get::<_, i64>(0))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        if owners.is_empty() {
            return Ok(None);
        }
        let mut embedders = conn.prepare(
            "SELECT source_symbol_id FROM edges
             WHERE target_symbol_id = ?1 AND kind = 'EXTENDS' AND graph_version = ?2
               AND source_symbol_id IS NOT NULL",
        )?;
        let mut declares = conn.prepare(
            "SELECT 1 FROM edges e JOIN symbols m ON m.id = e.target_symbol_id
             WHERE e.source_symbol_id = ?1 AND e.kind = 'CONTAINS' AND e.graph_version = ?2
               AND m.kind = 'method' AND m.name = ?3 AND m.id != ?4 LIMIT 1",
        )?;
        let mut seen: HashSet<i64> = owners.iter().copied().collect();
        let mut frontier = owners;
        while let Some(owner) = frontier.pop() {
            let ids = embedders
                .query_map(rusqlite::params![owner, gv], |r| r.get::<_, i64>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for id in ids {
                if seen.contains(&id) {
                    continue;
                }
                if declares.exists(rusqlite::params![id, gv, name, imp])? {
                    continue;
                }
                seen.insert(id);
                frontier.push(id);
            }
        }
        let mut names: Vec<String> = Vec::new();
        let mut stmt = conn.prepare(
            "SELECT t.qualname FROM edges e JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.source_symbol_id = ?1 AND e.kind = 'IMPLEMENTS' AND e.graph_version = ?2",
        )?;
        for owner in &seen {
            let rows = stmt.query_map(rusqlite::params![owner, gv], |r| r.get::<_, String>(0))?;
            for row in rows {
                names.push(row?);
            }
        }
        names.sort();
        names.dedup();
        Ok(Some(names.join(&crate::model::GO_SCOPE_SEP.to_string())))
    }

    /// Single-symbol form of [`Db::edges_for_symbols_with_dispatch`].
    pub fn edges_for_symbol_with_dispatch(
        &self,
        id: i64,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Edge>> {
        Ok(self
            .edges_for_symbols_with_dispatch(&[id], languages, graph_version)?
            .remove(&id)
            .unwrap_or_default())
    }
}

/// Result of [`Db::dispatch_peers`].
#[derive(Debug, Default, Clone)]
pub struct DispatchPeers {
    /// Same-named methods on interfaces the method's class implements.
    pub interface_methods: Vec<i64>,
    /// Same-named methods on classes implementing the method's interface.
    pub impl_methods: Vec<i64>,
}

impl Db {
    pub fn find_symbols(
        &self,
        query: &str,
        limit: usize,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Symbol>> {
        let tokens: Vec<&str> = query.split_whitespace().collect();
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        // Build per-token LIKE patterns
        let patterns: Vec<String> = tokens.iter().map(|t| format!("%{}%", t)).collect();
        // For ORDER BY exact-name match, use the longest token
        let longest_token = tokens.iter().max_by_key(|t| t.len()).unwrap_or(&query);
        let longest_lower = longest_token.to_lowercase();
        let limit = limit as i64;

        let mut sql = String::from(
            "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE ",
        );

        // Each token must match name OR qualname (AND across tokens)
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        for (i, pat) in patterns.iter().enumerate() {
            if i > 0 {
                sql.push_str(" AND ");
            }
            sql.push_str("(s.name LIKE ? OR s.qualname LIKE ?)");
            params.push(Box::new(pat.clone()));
            params.push(Box::new(pat.clone()));
        }

        sql.push_str(
            " AND s.graph_version = ? AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        params.push(Box::new(graph_version));
        params.push(Box::new(graph_version));

        if let Some(languages) = languages
            && !languages.is_empty()
        {
            sql.push_str(" AND f.language IN (");
            for (idx, _) in languages.iter().enumerate() {
                if idx > 0 {
                    sql.push(',');
                }
                sql.push('?');
            }
            sql.push(')');
            for language in languages {
                params.push(Box::new(language.clone()));
            }
        }
        // Relevance-based ordering:
        // 1. Exact name match (longest token) first
        // 2. Code symbols before doc/heading symbols
        // 3. Demote changelog/migration files
        // 4. Shorter qualnames (less nesting) first
        sql.push_str(
            " ORDER BY \
             CASE WHEN LOWER(s.name) = ? THEN 0 ELSE 1 END, \
             CASE WHEN s.kind IN ('class','function','method','struct','interface','enum','trait','service','message') THEN 0 \
                  WHEN s.kind IN ('module','namespace','package') THEN 1 \
                  WHEN s.kind IN ('heading','section') THEN 3 \
                  ELSE 2 END, \
             CASE WHEN f.path LIKE '%changelog%' OR f.path LIKE '%migration%' THEN 1 ELSE 0 END, \
             LENGTH(s.qualname), \
             s.name \
             LIMIT ?",
        );
        params.push(Box::new(longest_lower));
        params.push(Box::new(limit));

        let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*param_refs, symbol_from_row)?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Bounded candidate scan for fuzzy "did you mean": symbols whose
    /// lowercased name contains any of `patterns` (plain alphanumeric tokens),
    /// those matching the most patterns first, at most `cap` rows.
    pub fn fuzzy_symbol_rows(
        &self,
        patterns: &[String],
        cap: usize,
        graph_version: i64,
    ) -> Result<Vec<Symbol>> {
        if patterns.is_empty() {
            return Ok(Vec::new());
        }
        let likes: Vec<String> = patterns.iter().map(|p| format!("%{}%", p)).collect();
        let cond = vec!["LOWER(s.name) LIKE ?"; likes.len()].join(" OR ");
        let score = vec!["(LOWER(s.name) LIKE ?)"; likes.len()].join(" + ");
        let sql = format!(
            "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE ({cond})
               AND s.kind NOT IN ('heading','section')
               AND s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)
             ORDER BY ({score}) DESC, LENGTH(s.name), s.id
             LIMIT ?"
        );
        let cap = cap as i64;
        let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
        params.extend(likes.iter().map(|l| l as &dyn rusqlite::ToSql));
        params.push(&graph_version);
        params.push(&graph_version);
        params.extend(likes.iter().map(|l| l as &dyn rusqlite::ToSql));
        params.push(&cap);
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, symbol_from_row)?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Search symbols where name starts with the given prefix.
    /// Used for fuzzy matching candidate retrieval.
    pub fn find_symbols_by_name_prefix(
        &self,
        prefix: &str,
        limit: usize,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Symbol>> {
        let pattern = format!("{}%", prefix);
        let limit = limit as i64;
        let mut sql = String::from(
            "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.name LIKE ?
               AND s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&pattern, &graph_version, &graph_version];
        if let Some(languages) = languages
            && !languages.is_empty()
        {
            sql.push_str(" AND f.language IN (");
            for (idx, _) in languages.iter().enumerate() {
                if idx > 0 {
                    sql.push(',');
                }
                sql.push('?');
            }
            sql.push(')');
            for language in languages {
                params.push(language as &dyn rusqlite::ToSql);
            }
        }
        sql.push_str(" ORDER BY s.name LIMIT ?");
        params.push(&limit);

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, symbol_from_row)?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn lookup_symbol_id(&self, qualname: &str, graph_version: i64) -> Result<Option<i64>> {
        self.lookup_symbol_id_filtered(qualname, None, graph_version)
    }

    pub fn lookup_symbol_id_filtered(
        &self,
        qualname: &str,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Option<i64>> {
        let mut sql = String::from(
            "SELECT s.id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.qualname = ?
               AND s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&qualname, &graph_version, &graph_version];
        if let Some(languages) = languages
            && !languages.is_empty()
        {
            sql.push_str(" AND f.language IN (");
            for (idx, _) in languages.iter().enumerate() {
                if idx > 0 {
                    sql.push(',');
                }
                sql.push('?');
            }
            sql.push(')');
            for language in languages {
                params.push(language as &dyn rusqlite::ToSql);
            }
        }
        sql.push_str(" LIMIT 1");
        self.read_conn()?
            .query_row(&sql, &*params, |row| row.get(0))
            .optional()
            .map_err(Into::into)
    }

    pub fn edges_for_symbol(
        &self,
        id: i64,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Edge>> {
        let mut sql = String::from(
            "SELECT e.id, f.path, e.kind, e.source_symbol_id, e.target_symbol_id,
                    e.target_qualname, e.detail, e.evidence_snippet,
                    e.evidence_start_line, e.evidence_end_line, e.confidence,
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                    e.resolution_kind
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE (e.source_symbol_id = ? OR e.target_symbol_id = ?)
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        sql.push_str(RPC_NAME_ONLY_FILTER);
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&id, &id, &graph_version, &graph_version];
        if let Some(languages) = languages
            && !languages.is_empty()
        {
            sql.push_str(" AND f.language IN (");
            for (idx, _) in languages.iter().enumerate() {
                if idx > 0 {
                    sql.push(',');
                }
                sql.push('?');
            }
            sql.push(')');
            for language in languages {
                params.push(language as &dyn rusqlite::ToSql);
            }
        }
        sql.push_str(" ORDER BY e.id");
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, edge_from_row)?;
        let mut edges = Vec::new();
        for row in rows {
            edges.push(row?);
        }
        Ok(edges)
    }

    /// Find edges by exact target_qualname match and edge kind filter.
    /// Used for traversal bridging: given a channel/route qualname, find all
    /// edges pointing at it with complementary kinds.
    ///
    /// RPC paths additionally bind by `service/method` suffix when a side
    /// guessed the proto package wrong (see `resolve_rpc_route`).
    pub fn edges_by_target_qualname_and_kinds(
        &self,
        target_qualname: &str,
        kinds: &[&str],
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Edge>> {
        let is_rpc = kinds
            .iter()
            .any(|k| edge_views::contains(edge_views::RPC_FAMILY, k));
        if !is_rpc || !target_qualname.starts_with('/') {
            return self.edges_by_exact_target(
                target_qualname,
                target_qualname,
                kinds,
                languages,
                graph_version,
            );
        }
        // Our own path may be a wrong guess: bind it to the one real route.
        let own = self.resolve_rpc_route(target_qualname, graph_version)?;
        let route = own.as_deref().unwrap_or(target_qualname);
        let mut edges =
            self.edges_by_exact_target(route, route, kinds, languages, graph_version)?;
        // The other side may be the wrong guess: pull in its edges whose
        // guessed path resolves to our route.
        // Symmetric in edge kind: callers (RPC_CALL) and implementers
        // (RPC_IMPL) both store guessed paths. Pull in the other side's
        // edges whose guessed path resolves to our route, whether or not
        // our own path was itself a guess.
        {
            let (_, svc_method) = rpc_split(target_qualname);
            let conn = self.read_conn()?;
            let mut stmt = conn.prepare(
                "SELECT DISTINCT target_qualname FROM edges
                 WHERE kind = ?4 AND graph_version = ?1
                   AND target_qualname != ?2
                   AND (target_qualname = '/' || ?3
                        OR substr(target_qualname, -length(?3) - 1) = '.' || ?3)",
            )?;
            for &kind in edge_views::RPC_BOUND {
                if !kinds.contains(&kind) {
                    continue;
                }
                let guesses: Vec<String> = stmt
                    .query_map(
                        rusqlite::params![graph_version, route, svc_method, kind],
                        |r| r.get(0),
                    )?
                    .collect::<rusqlite::Result<_>>()?;
                for guess in guesses {
                    if self.resolve_rpc_route(&guess, graph_version)?.as_deref() == Some(route) {
                        edges.extend(self.edges_by_exact_target(
                            &guess,
                            route,
                            &[kind],
                            languages,
                            graph_version,
                        )?);
                    }
                }
            }
        }
        Ok(edges)
    }

    /// Bind a guessed RPC path (`/pkg.service/method`, package possibly wrong
    /// or missing) to a real `.proto` RPC_ROUTE path. `None` when the path
    /// already has an exact route, or no single route can be chosen. A
    /// package-less guess binds to the one `service/method` suffix match; a
    /// guess that carries a package binds only to routes whose package agrees
    /// (one a dotted suffix of the other), so a contradicting package never
    /// binds; still ambiguous means unbound.
    fn resolve_rpc_route(&self, guess: &str, graph_version: i64) -> Result<Option<String>> {
        let (guess_pkg, svc_method) = rpc_split(guess);
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT target_qualname FROM edges
             WHERE kind = 'RPC_ROUTE' AND graph_version = ?1
               AND (target_qualname = '/' || ?2
                    OR substr(target_qualname, -length(?2) - 1) = '.' || ?2)",
        )?;
        let routes: Vec<String> = stmt
            .query_map(rusqlite::params![graph_version, svc_method], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        if routes.iter().any(|r| r == guess) {
            return Ok(None);
        }
        let agreeing: Vec<&String> = routes
            .iter()
            .filter(|r| {
                let (pkg, _) = rpc_split(r);
                guess_pkg.is_empty()
                    || pkg == guess_pkg
                    || pkg.ends_with(&format!(".{guess_pkg}"))
                    || guess_pkg.ends_with(&format!(".{pkg}"))
            })
            .collect();
        Ok(match agreeing.as_slice() {
            [only] => Some((*only).clone()),
            _ => None,
        })
    }

    /// `kind` (RPC_IMPL or RPC_CALL) edges bound to the route(s) of proto
    /// `rpc` symbol `symbol_id`, guessed-package edges included; one per
    /// source symbol, in edge order.
    pub fn rpc_bound_edges(
        &self,
        symbol_id: i64,
        kind: &str,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Edge>> {
        let paths: Vec<String> = {
            let conn = self.read_conn()?;
            let mut stmt = conn.prepare(
                "SELECT DISTINCT target_qualname FROM edges
                 WHERE kind = 'RPC_ROUTE' AND source_symbol_id = ?1
                   AND graph_version = ?2 AND target_qualname IS NOT NULL",
            )?;
            stmt.query_map(rusqlite::params![symbol_id, graph_version], |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?
        };
        let mut seen = HashSet::new();
        let mut found = Vec::new();
        for path in paths {
            for edge in
                self.edges_by_target_qualname_and_kinds(&path, &[kind], languages, graph_version)?
            {
                if let Some(id) = edge.source_symbol_id
                    && seen.insert(id)
                {
                    found.push(edge);
                }
            }
        }
        Ok(found)
    }

    /// Cross-file reconciliation of RPC_IMPL / RPC_CALL edges, recomputed
    /// from current rows only so incremental sync equals a fresh index:
    /// - `detail.package`, when the extractor had none, is the package of the
    ///   one `.proto` route the edge's path binds to (flagged
    ///   `package_from_route`, and cleared again when the binding goes);
    /// - an RPC_IMPL edge that lists `handler_candidates` (a wrapped or
    ///   imported handler) is sourced at the first candidate that is a
    ///   function or method, else at its enclosing scope (`detail.enclosing`).
    pub fn reconcile_rpc_edges(&self, graph_version: i64) -> Result<usize> {
        struct Row {
            id: i64,
            kind: String,
            file_id: i64,
            source: Option<i64>,
            target: String,
            detail: String,
        }
        let rows: Vec<Row> = {
            let conn = self.read_conn()?;
            let mut stmt = conn.prepare(&format!(
                "SELECT e.id, e.kind, e.file_id, e.source_symbol_id, e.target_qualname, e.detail
                 FROM edges e JOIN files f ON f.id = e.file_id
                 WHERE e.graph_version = ?1 AND e.kind IN ({rpc_bound})
                   AND e.target_qualname LIKE '/%' AND e.detail IS NOT NULL
                   AND (f.deleted_version IS NULL OR f.deleted_version > ?1)",
                rpc_bound = edge_views::sql_list(edge_views::RPC_BOUND)
            ))?;
            stmt.query_map([graph_version], |r| {
                Ok(Row {
                    id: r.get(0)?,
                    kind: r.get(1)?,
                    file_id: r.get(2)?,
                    source: r.get(3)?,
                    target: r.get(4)?,
                    detail: r.get(5)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?
        };
        let mut route_packages: HashMap<String, Option<String>> = HashMap::new();
        let mut updates: Vec<(i64, Option<i64>, String)> = Vec::new();
        for row in rows {
            let Ok(mut detail) = serde_json::from_str::<serde_json::Value>(&row.detail) else {
                continue;
            };
            let before = detail.clone();
            let mut source = row.source;
            let flagged = detail["package_from_route"] == true;
            if detail["package"].is_null() || flagged {
                let package = match route_packages.get(&row.target) {
                    Some(cached) => cached.clone(),
                    None => {
                        let found = self.bound_route_package(&row.target, graph_version)?;
                        route_packages.insert(row.target.clone(), found.clone());
                        found
                    }
                };
                match package {
                    Some(pkg) => {
                        detail["package"] = pkg.into();
                        detail["package_from_route"] = true.into();
                    }
                    None => {
                        detail["package"] = serde_json::Value::Null;
                        if let Some(map) = detail.as_object_mut() {
                            map.remove("package_from_route");
                        }
                    }
                }
            }
            if row.kind == "RPC_IMPL"
                && let Some(candidates) = detail["handler_candidates"].as_array()
            {
                let conn = self.read_conn()?;
                let mut chosen = None;
                for candidate in candidates.iter().filter_map(|c| c.as_str()) {
                    chosen = conn
                        .query_row(
                            "SELECT s.id FROM symbols s JOIN files f ON f.id = s.file_id
                             WHERE s.qualname = ?1 AND s.graph_version = ?2
                               AND s.kind IN ('function', 'method')
                               AND (f.deleted_version IS NULL OR f.deleted_version > ?2)
                             ORDER BY s.id LIMIT 1",
                            rusqlite::params![candidate, graph_version],
                            |r| r.get(0),
                        )
                        .optional()?;
                    if chosen.is_some() {
                        break;
                    }
                }
                if chosen.is_none()
                    && let Some(scope) = detail["enclosing"].as_str()
                {
                    chosen = conn
                        .query_row(
                            "SELECT id FROM symbols WHERE qualname = ?1 AND file_id = ?2
                               AND graph_version = ?3 ORDER BY id LIMIT 1",
                            rusqlite::params![scope, row.file_id, graph_version],
                            |r| r.get(0),
                        )
                        .optional()?;
                }
                if chosen.is_some() {
                    source = chosen;
                }
            }
            if detail != before || source != row.source {
                updates.push((row.id, source, detail.to_string()));
            }
        }
        let changed = updates.len();
        if changed > 0 {
            let mut conn = self.conn();
            let tx = conn.transaction()?;
            for (id, source, detail) in updates {
                tx.execute(
                    "UPDATE edges SET source_symbol_id = ?1, detail = ?2 WHERE id = ?3",
                    rusqlite::params![source, detail, id],
                )?;
            }
            tx.commit()?;
        }
        Ok(changed)
    }

    /// Package of the `.proto` route `path` is, or binds to.
    fn bound_route_package(&self, path: &str, graph_version: i64) -> Result<Option<String>> {
        let route = match self.resolve_rpc_route(path, graph_version)? {
            Some(real) => real,
            None => path.to_string(),
        };
        let conn = self.read_conn()?;
        let detail: Option<String> = conn
            .query_row(
                "SELECT detail FROM edges
                 WHERE kind = 'RPC_ROUTE' AND target_qualname = ?1 AND graph_version = ?2
                 LIMIT 1",
                rusqlite::params![route, graph_version],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        Ok(detail
            .and_then(|d| serde_json::from_str::<serde_json::Value>(&d).ok())
            .and_then(|d| d["package"].as_str().map(str::to_string)))
    }

    /// `route` is the RPC_ROUTE path that must back RPC edges (the target
    /// itself, or the real route a guessed target resolved to).
    fn edges_by_exact_target(
        &self,
        target_qualname: &str,
        route: &str,
        kinds: &[&str],
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Edge>> {
        if kinds.is_empty() {
            return Ok(Vec::new());
        }
        let mut kind_placeholders = String::new();
        for (idx, _) in kinds.iter().enumerate() {
            if idx > 0 {
                kind_placeholders.push(',');
            }
            kind_placeholders.push('?');
        }
        let mut sql = format!(
            "SELECT e.id, f.path, e.kind, e.source_symbol_id, e.target_symbol_id,
                    e.target_qualname, e.detail, e.evidence_snippet,
                    e.evidence_start_line, e.evidence_end_line, e.confidence,
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                    e.resolution_kind
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE e.target_qualname = ?
               AND e.kind IN ({kind_placeholders})
               AND e.source_symbol_id IS NOT NULL
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)
               AND (e.kind NOT IN ({rpc_bound})
                    OR EXISTS (SELECT 1 FROM edges r
                               WHERE r.target_qualname = ?
                                 AND r.kind = 'RPC_ROUTE' AND r.graph_version = ?)
                    OR NOT EXISTS (SELECT 1 FROM edges r
                                   WHERE r.kind = 'RPC_ROUTE' AND r.graph_version = ?))",
            rpc_bound = edge_views::sql_list(edge_views::RPC_BOUND)
        );
        // ponytail: RPC_CALL and RPC_IMPL both fan out one edge per
        // *guessed* proto package (bare `using`s), so two wrong guesses can
        // share a path and bridge unrelated services. Only bridge on a path
        // a real `.proto` RPC_ROUTE backs; repos with no RPC_ROUTE at all
        // (protos live elsewhere) keep the unguarded behaviour. Ceiling: a
        // route whose .proto isn't indexed is still exposed to that noise.
        sql.push_str(RPC_NAME_ONLY_FILTER);
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&target_qualname as &dyn rusqlite::ToSql];
        for kind in kinds {
            params.push(kind as &dyn rusqlite::ToSql);
        }
        params.push(&graph_version);
        params.push(&graph_version);
        params.push(&route as &dyn rusqlite::ToSql);
        params.push(&graph_version);
        params.push(&graph_version);
        if let Some(languages) = languages
            && !languages.is_empty()
        {
            sql.push_str(" AND f.language IN (");
            for (idx, _) in languages.iter().enumerate() {
                if idx > 0 {
                    sql.push(',');
                }
                sql.push('?');
            }
            sql.push(')');
            for language in languages {
                params.push(language as &dyn rusqlite::ToSql);
            }
        }
        sql.push_str(" ORDER BY e.id LIMIT 100");
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, edge_from_row)?;
        let mut edges = Vec::new();
        for row in rows {
            edges.push(row?);
        }
        Ok(edges)
    }

    /// Find unique source symbol IDs from edges targeting a config URI.
    /// If `kinds` is empty, searches all CONFIG edge kinds.
    pub fn source_symbols_for_config_uri(
        &self,
        uri: &str,
        kinds: &[&str],
        graph_version: i64,
    ) -> Result<Vec<i64>> {
        let default_kinds: &[&str] = edge_views::CONFIG;
        let search_kinds = if kinds.is_empty() {
            default_kinds
        } else {
            kinds
        };
        let edges =
            self.edges_by_target_qualname_and_kinds(uri, search_kinds, None, graph_version)?;
        let mut seen = HashSet::new();
        let mut ids = Vec::new();
        for e in &edges {
            if let Some(id) = e.source_symbol_id
                && seen.insert(id)
            {
                ids.push(id);
            }
        }
        Ok(ids)
    }

    pub fn edges_for_symbols(
        &self,
        ids: &[i64],
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<HashMap<i64, Vec<Edge>>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut placeholders = String::new();
        for (idx, _) in ids.iter().enumerate() {
            if idx > 0 {
                placeholders.push(',');
            }
            placeholders.push('?');
        }

        // First query: resolved edges (existing behavior)
        let mut sql = format!(
            "SELECT e.id, f.path, e.kind, e.source_symbol_id, e.target_symbol_id,
                    e.target_qualname, e.detail, e.evidence_snippet,
                    e.evidence_start_line, e.evidence_end_line, e.confidence,
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                    e.resolution_kind
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE (e.source_symbol_id IN ({}) OR e.target_symbol_id IN ({}))
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
            placeholders, placeholders
        );

        sql.push_str(RPC_NAME_ONLY_FILTER);
        let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
        // Add IDs twice (for source and target)
        for id in ids {
            params.push(id as &dyn rusqlite::ToSql);
        }
        for id in ids {
            params.push(id as &dyn rusqlite::ToSql);
        }
        params.push(&graph_version);
        params.push(&graph_version);

        if let Some(languages) = languages
            && !languages.is_empty()
        {
            sql.push_str(" AND f.language IN (");
            for (idx, _) in languages.iter().enumerate() {
                if idx > 0 {
                    sql.push(',');
                }
                sql.push('?');
            }
            sql.push(')');
            for language in languages {
                params.push(language as &dyn rusqlite::ToSql);
            }
        }
        sql.push_str(" ORDER BY e.id");

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, edge_from_row)?;

        // Group edges by symbol ID
        let mut result: HashMap<i64, Vec<Edge>> = HashMap::new();
        for id in ids {
            result.insert(*id, Vec::new());
        }

        for row in rows {
            let edge = row?;
            // Add edge to both source and target symbol lists
            if let Some(source_id) = edge.source_symbol_id
                && ids.contains(&source_id)
            {
                result.entry(source_id).or_default().push(edge.clone());
            }
            if let Some(target_id) = edge.target_symbol_id
                && ids.contains(&target_id)
            {
                result.entry(target_id).or_default().push(edge.clone());
            }
        }

        // A second query used to widen this result set by matching
        // `target_symbol_id IS NULL` edges against symbol *names* (via a
        // `LIKE '%.Name'` suffix pattern) — i.e. it re-attributed edges the
        // write path had deliberately left unresolved. That's exactly the
        // guess the write path already refused to make: SQLite's LIKE is
        // case-insensitive for ASCII (`'value.Trim' LIKE '%.trim'` is true)
        // and the pattern carried no language scoping, so it matched same-
        // named methods across unrelated classes and even unrelated
        // languages (see the C#-`Trim`-to-Python-`trim` false callee).
        // `target_symbol_id IS NULL` now means "could not be attributed" —
        // the read path must not invent one. A target that *can* be bound
        // legitimately (e.g. an exact qualname match) belongs in the write
        // path (`insert_edges`) or its repair passes (`db::resolver`'s
        // `retry_unresolved_references`/`reconcile_unresolved_reference_store`),
        // not here.
        Ok(result)
    }

    pub fn symbols_by_ids(
        &self,
        ids: &[i64],
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Symbol>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut placeholders = String::new();
        for (idx, _) in ids.iter().enumerate() {
            if idx > 0 {
                placeholders.push(',');
            }
            placeholders.push('?');
        }
        let mut sql = format!(
            "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.id IN ({})
               AND s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
            placeholders
        );
        let mut params: Vec<&dyn rusqlite::ToSql> =
            ids.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
        params.push(&graph_version);
        params.push(&graph_version);
        if let Some(languages) = languages
            && !languages.is_empty()
        {
            sql.push_str(" AND f.language IN (");
            for (idx, _) in languages.iter().enumerate() {
                if idx > 0 {
                    sql.push(',');
                }
                sql.push('?');
            }
            sql.push(')');
            for language in languages {
                params.push(language as &dyn rusqlite::ToSql);
            }
        }
        sql.push_str(" ORDER BY s.id");
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, symbol_from_row)?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Read every edge in `graph_version` as a normalized
    /// `(source qualname, kind, target qualname | None, resolution kind)`
    /// row, joining `source_symbol_id`/`target_symbol_id` to their symbols'
    /// *current* qualnames rather than the edge's raw stored
    /// `target_qualname` text. Issue #79: a pending (non-Bridge-Edge-kind)
    /// `unresolved_references` row -- one with no edge at all -- is unioned
    /// in too, as `target qualname = None`/`resolution kind = None` (i.e.
    /// `UNRESOLVED`, same as a Bridge Edge kind's still-NULL-target edge
    /// already renders via the first half of this query), so the scoreboard
    /// keeps seeing every unresolved reference regardless of which of the
    /// two shapes holds it.
    ///
    /// Test support for the golden-corpus correctness scoreboard (see
    /// `tests/common/golden.rs`): the one seam a test needs to compare the
    /// graph against an expected-edges fixture without touching SQL or
    /// resolver internals directly. A reference with no resolved source
    /// symbol (file-level edges such as `IMPORTS`/`MODULE_FILE`, or a
    /// pending row whose own caller never resolved) is omitted — the
    /// scoreboard only covers edges attributable to a real symbol.
    pub fn edges_snapshot(&self, graph_version: i64) -> Result<Vec<EdgeSnapshotRow>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT src.qualname, e.kind, tgt.qualname, e.resolution_kind, e.confidence
             FROM edges e
             JOIN files f ON e.file_id = f.id
             JOIN symbols src ON e.source_symbol_id = src.id
             LEFT JOIN symbols tgt ON e.target_symbol_id = tgt.id
             WHERE e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)

             UNION ALL

             SELECT src.qualname, ur.edge_kind, NULL, NULL, NULL
             FROM unresolved_references ur
             JOIN files f ON ur.file_id = f.id
             JOIN symbols src ON ur.source_symbol_id = src.id
             WHERE ur.edge_id IS NULL
               AND ur.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        )?;
        let rows = stmt.query_map(
            rusqlite::params![graph_version, graph_version, graph_version, graph_version],
            |row| {
                Ok(EdgeSnapshotRow {
                    source_qualname: row.get(0)?,
                    kind: row.get(1)?,
                    target_qualname: row.get(2)?,
                    resolution_kind: row.get(3)?,
                    confidence_milli: row
                        .get::<_, Option<f64>>(4)?
                        .map(|c| (c * 1000.0).round() as i64),
                })
            },
        )?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        results.sort();
        Ok(results)
    }

    /// `edges.resolution_kind` for a batch of edge ids, keyed by edge id.
    /// Since issue #62, `Edge::resolution_kind` (via `edge_from_row`) also
    /// carries this column for any caller that already has full `Edge`
    /// values in hand -- prefer reading `edge.resolution_kind` directly in
    /// that case, as `trace_flow`'s BFS (`traversal.rs`) and
    /// `analyze_direct_impact`'s BFS (`impact/layers/direct.rs`) now do for
    /// their per-node `exclude_resolution_kinds` filter (issue #81). This
    /// batch, id-keyed form stays for the one remaining case where a caller
    /// only has bare edge ids: those same two BFS functions' post-traversal
    /// `traversed_heuristic_kind` summary, which checks every edge that
    /// actually produced a hop/neighbor in one query rather than loading a
    /// full `Edge` per id just to read one field. An edge id absent from the
    /// returned map has no resolution kind at all -- a Bridge Edge kind
    /// (its target is a cross-process join key, resolved separately from
    /// the name tiers) or any edge kind the resolver never labels.
    pub fn edge_resolution_kinds(&self, edge_ids: &[i64]) -> Result<HashMap<i64, String>> {
        if edge_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut placeholders = String::new();
        for (idx, _) in edge_ids.iter().enumerate() {
            if idx > 0 {
                placeholders.push(',');
            }
            placeholders.push('?');
        }
        let sql = format!(
            "SELECT id, resolution_kind FROM edges \
             WHERE id IN ({placeholders}) AND resolution_kind IS NOT NULL"
        );
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let params: Vec<&dyn rusqlite::ToSql> = edge_ids
            .iter()
            .map(|id| id as &dyn rusqlite::ToSql)
            .collect();
        let rows = stmt.query_map(&*params, |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut map = HashMap::new();
        for row in rows {
            let (id, kind) = row?;
            map.insert(id, kind);
        }
        Ok(map)
    }
}

/// Split `/pkg.Service/method` into (`pkg`, `service/method`); `pkg` is empty
/// when the path has none.
fn rpc_split(path: &str) -> (&str, &str) {
    let path = path.trim_start_matches('/');
    let svc_path = path.split_once('/').map_or(path, |(svc, _)| svc);
    match svc_path.rfind('.') {
        Some(i) => (&path[..i], &path[i + 1..]),
        None => ("", path),
    }
}
