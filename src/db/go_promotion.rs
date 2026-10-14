//! Methods a Go struct promotes from the types it embeds (issue #503).
//!
//! `type S struct{ T }` gives `S` every method of `T` it does not declare
//! itself. Two consequences for `dead_symbols`:
//!
//! - a method promoted into an exported type of an importable package is
//!   public API through that type, whatever the package its own type is in
//!   (syft's `internal/packagemetadata.CompletionTester`, embedded by the
//!   exported `testutil.PackageMetadataCompletionTester`);
//! - a call through an interface that `S` implements reaches the promoted
//!   method of `T` (`cyclonedxxml.encoder` embeds `cyclonedxutil.Encoder` and
//!   is returned as `sbom.FormatEncoder`, whose `Encode` calls reach
//!   `Encoder.Encode`), which the dispatch pairs, keyed on `S`'s own members,
//!   cannot see.
//!
//! Both are read from the derived edges the resolver keeps: the `EXTENDS`
//! edge of each embedding (`DERIVED_GO_EMBED`), the structural `IMPLEMENTS`
//! edges, and the `CONTAINS` edges from a type to its methods.

use super::graph_query::has_modifier;
use crate::indexer::go::GO_PACKAGE_MAIN;
use anyhow::Result;
use std::collections::{HashMap, HashSet};

/// How deep an embedding chain is followed.
const MAX_EMBED_DEPTH: usize = 4;

/// What embedding does for Go methods, computed once per `dead_symbols`.
#[derive(Default)]
pub(crate) struct GoPromotion {
    /// Method id -> why it is public API through the type embedding it.
    pub public: HashMap<i64, String>,
    /// Methods reached by interface dispatch through an embedding type.
    pub live: HashSet<i64>,
}

struct TypeRow {
    name: String,
    qualname: String,
    path: String,
    visibility: Option<String>,
}

impl TypeRow {
    /// An exported type of a package other modules can import.
    fn importable_exported(&self) -> bool {
        self.name.chars().next().is_some_and(char::is_uppercase)
            && !has_modifier(self.visibility.as_deref(), GO_PACKAGE_MAIN)
            && !self.path.ends_with("_test.go")
            && !self
                .path
                .rsplit_once('/')
                .map_or("", |(d, _)| d)
                .split('/')
                .any(|seg| seg == "internal")
    }
}

/// `test_guard` is `dead_symbols`' SQL excluding edges from test code
/// (over an `edges` alias `e`); empty when tests are roots.
pub(crate) fn go_promotion(
    conn: &rusqlite::Connection,
    graph_version: i64,
    test_guard: &str,
) -> Result<GoPromotion> {
    let mut out = GoPromotion::default();
    // embedded type -> the types embedding it
    let mut embedders: HashMap<i64, Vec<i64>> = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT source_symbol_id, target_symbol_id FROM edges
             WHERE kind = 'EXTENDS' AND derived = ?1 AND graph_version = ?2
               AND source_symbol_id IS NOT NULL AND target_symbol_id IS NOT NULL",
        )?;
        let rows = stmt.query_map(
            rusqlite::params![crate::model::DERIVED_GO_EMBED, graph_version],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        )?;
        for row in rows {
            let (embedder, embedded) = row?;
            embedders.entry(embedded).or_default().push(embedder);
        }
    }
    if embedders.is_empty() {
        return Ok(out);
    }
    // type -> its own methods (name, id); the interfaces' members included
    let mut methods: HashMap<i64, Vec<(String, i64)>> = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT c.source_symbol_id, m.name, m.id FROM edges c
             JOIN symbols m ON m.id = c.target_symbol_id
             JOIN files f ON f.id = m.file_id
             WHERE c.kind = 'CONTAINS' AND c.graph_version = ?1 AND m.kind = 'method'
               AND f.language = 'go' AND c.source_symbol_id IS NOT NULL",
        )?;
        let rows = stmt.query_map([graph_version], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?;
        for row in rows {
            let (parent, name, id) = row?;
            methods.entry(parent).or_default().push((name, id));
        }
    }
    let embedder_ids: HashSet<i64> = embedders.values().flatten().copied().collect();
    let mut types: HashMap<i64, TypeRow> = HashMap::new();
    let mut implements: HashMap<i64, Vec<i64>> = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT s.id, s.name, s.qualname, f.path, s.visibility FROM symbols s
             JOIN files f ON f.id = s.file_id
             WHERE s.graph_version = ?1 AND f.language = 'go'
               AND s.kind IN ('class', 'type', 'interface')",
        )?;
        let rows = stmt.query_map([graph_version], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, Option<String>>(4)?,
            ))
        })?;
        for row in rows {
            let (id, name, qualname, path, visibility) = row?;
            if embedder_ids.contains(&id) {
                types.insert(
                    id,
                    TypeRow {
                        name,
                        qualname,
                        path,
                        visibility,
                    },
                );
            }
        }
        let mut stmt = conn.prepare(
            "SELECT source_symbol_id, target_symbol_id FROM edges
             WHERE kind = 'IMPLEMENTS' AND graph_version = ?1
               AND source_symbol_id IS NOT NULL AND target_symbol_id IS NOT NULL",
        )?;
        let rows = stmt.query_map([graph_version], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (ty, iface) = row?;
            if embedder_ids.contains(&ty) {
                implements.entry(ty).or_default().push(iface);
            }
        }
    }
    // interface members some call reaches (outside test code when tests
    // are not roots)
    let called: HashSet<i64> = {
        let mut stmt = conn.prepare(&format!(
            "SELECT DISTINCT e.target_symbol_id FROM edges e
             JOIN symbols im ON im.id = e.target_symbol_id
             JOIN edges c ON c.target_symbol_id = im.id AND c.kind = 'CONTAINS'
                           AND c.graph_version = ?1
             JOIN symbols i ON i.id = c.source_symbol_id
             WHERE e.kind IN ('CALLS', 'DISPATCH') AND e.graph_version = ?1
               AND im.kind = 'method' AND i.kind = 'interface' {test_guard}"
        ))?;
        let rows = stmt.query_map([graph_version], |r| r.get::<_, i64>(0))?;
        rows.collect::<rusqlite::Result<_>>()?
    };
    let own_method = |ty: i64, name: &str| -> Option<i64> {
        methods
            .get(&ty)
            .and_then(|ms| ms.iter().find(|(n, _)| n == name).map(|(_, id)| *id))
    };
    for (&embedded, first) in &embedders {
        let Some(own) = methods.get(&embedded) else {
            continue;
        };
        for (name, method_id) in own {
            // every type that promotes this method, embedding chains followed
            let mut frontier: Vec<i64> = first.clone();
            let mut seen: HashSet<i64> = HashSet::new();
            for _ in 0..MAX_EMBED_DEPTH {
                let mut next = Vec::new();
                for ty in frontier.drain(..) {
                    if !seen.insert(ty) || own_method(ty, name).is_some() {
                        // declared by the embedding type itself: not promoted
                        continue;
                    }
                    if let Some(row) = types.get(&ty)
                        && name.chars().next().is_some_and(char::is_uppercase)
                        && row.importable_exported()
                    {
                        out.public.entry(*method_id).or_insert_with(|| {
                            format!(
                                "promoted by embedding into the exported type {} of an importable package",
                                row.qualname
                            )
                        });
                    }
                    if let Some(ifaces) = implements.get(&ty)
                        && ifaces.iter().any(|iface| {
                            own_method(*iface, name).is_some_and(|im| called.contains(&im))
                        })
                    {
                        out.live.insert(*method_id);
                    }
                    if let Some(up) = embedders.get(&ty) {
                        next.extend(up.iter().copied());
                    }
                }
                if next.is_empty() {
                    break;
                }
                frontier = next;
            }
        }
    }
    Ok(out)
}
