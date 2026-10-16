//! The compact JSON document the viewer reads.
//!
//! Every string (names, qualnames, paths, kinds, languages, signatures,
//! evidence lines) lives once in the `s` table and is referenced by index;
//! `-1` means absent. Nodes and edges are positional arrays:
//!
//! - node: `[name, qualname, kind, lang, file, module, line, flags, sig, doc, size]`
//! - edge: `[from, to, kind, weight, flags, ev_line, ev, ev_file, resolution]`
//!
//! `from`/`to` index `nodes`. Flag bits are the `FLAG_*` / `EDGE_*`
//! constants in `build`, repeated in `meta.flags` so the viewer never
//! hard-codes them.

use super::build::{self, Graph};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::HashMap;

pub const SCHEMA_VERSION: u32 = 1;
const MAX_DOC: usize = 600;
const MAX_SIG: usize = 400;
const MAX_EVIDENCE: usize = 240;

#[derive(Serialize, Debug, Clone)]
pub struct Meta {
    pub repo: String,
    pub commit: Option<String>,
    pub graph_version: i64,
    pub level: String,
    pub scope: String,
    pub generated_by: String,
    pub generated_at: u64,
}

#[derive(Default)]
struct Strings {
    index: HashMap<String, i64>,
    list: Vec<String>,
}

impl Strings {
    fn id(&mut self, s: &str) -> i64 {
        if let Some(&i) = self.index.get(s) {
            return i;
        }
        let i = self.list.len() as i64;
        self.list.push(s.to_string());
        self.index.insert(s.to_string(), i);
        i
    }

    fn opt(&mut self, s: Option<&str>, max: usize) -> i64 {
        match s.map(str::trim) {
            Some(s) if !s.is_empty() => self.id(&clip(s, max)),
            _ => -1,
        }
    }
}

/// `s` cut to at most `max` bytes on a char boundary, with `…` when cut.
fn clip(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

/// Counts shown in the header (and asserted by tests).
#[derive(Serialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct Counts {
    pub nodes: usize,
    pub edges: usize,
    pub halo_nodes: usize,
    pub seeds: usize,
    pub external: usize,
    pub endpoints: usize,
}

pub fn counts(graph: &Graph) -> Counts {
    let has = |f: u32| graph.nodes.iter().filter(|n| n.flags & f != 0).count();
    Counts {
        nodes: graph.nodes.len(),
        edges: graph.edges.len(),
        halo_nodes: has(build::FLAG_HALO),
        seeds: has(build::FLAG_SEED),
        external: has(build::FLAG_EXTERNAL),
        endpoints: has(build::FLAG_ENDPOINT),
    }
}

/// Serialise `graph` into the viewer document.
pub fn to_json(graph: &Graph, meta: &Meta) -> Value {
    let mut s = Strings::default();
    let nodes: Vec<Value> = graph
        .nodes
        .iter()
        .map(|n| {
            json!([
                s.id(&n.name),
                s.id(&n.qual),
                s.id(&n.kind),
                s.opt(n.lang.as_deref(), usize::MAX),
                s.opt(n.file.as_deref(), usize::MAX),
                s.id(&n.module),
                n.line,
                n.flags,
                s.opt(n.sig.as_deref(), MAX_SIG),
                s.opt(n.doc.as_deref(), MAX_DOC),
                n.size,
            ])
        })
        .collect();
    let edges: Vec<Value> = graph
        .edges
        .iter()
        .map(|e| {
            json!([
                e.from,
                e.to,
                s.id(&e.kind),
                e.weight,
                e.flags,
                e.ev_line,
                s.opt(e.ev.as_deref(), MAX_EVIDENCE),
                s.opt(e.ev_file.as_deref(), usize::MAX),
                s.opt(e.res.as_deref(), usize::MAX),
            ])
        })
        .collect();
    json!({
        "schema": SCHEMA_VERSION,
        "meta": {
            "repo": meta.repo,
            "commit": meta.commit,
            "graph_version": meta.graph_version,
            "level": meta.level,
            "scope": meta.scope,
            "generated_by": meta.generated_by,
            "generated_at": meta.generated_at,
            "collapse_modules": graph.collapse_modules,
            "truncated": graph.truncated,
            "notes": graph.notes,
            "counts": counts(graph),
            "flags": {
                "seed": build::FLAG_SEED,
                "halo": build::FLAG_HALO,
                "external": build::FLAG_EXTERNAL,
                "unresolved": build::FLAG_UNRESOLVED,
                "endpoint": build::FLAG_ENDPOINT,
                "test": build::FLAG_TEST,
                "edge_reversed": build::EDGE_REVERSED,
                "edge_halo": build::EDGE_HALO,
            },
        },
        "s": s.list,
        "nodes": nodes,
        "edges": edges,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clip_respects_char_boundaries() {
        assert_eq!(clip("abc", 5), "abc");
        assert_eq!(clip("ååå", 3), "å…");
    }

    #[test]
    fn strings_are_interned_once() {
        let mut s = Strings::default();
        assert_eq!(s.id("a"), 0);
        assert_eq!(s.id("b"), 1);
        assert_eq!(s.id("a"), 0);
        assert_eq!(s.opt(None, 10), -1);
        assert_eq!(s.opt(Some("  "), 10), -1);
    }
}
