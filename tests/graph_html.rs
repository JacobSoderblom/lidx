//! `lidx graph`: the written page embeds a parseable graph document whose
//! nodes and edges match the requested scope, on a fixture indexed by the
//! real indexer.
mod common;

use serde_json::Value;
use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;

/// `alpha`'s docstring would end the page's data block if embedded raw.
const MAIN: &str = concat!(
    "from lib.util import beta\n",
    "from lib.helpers import gamma\n",
    "\n\n",
    "def alpha():\n",
    "    \"\"\"Calls </script><script>alert(1)</script> <!-- x --> a\u{2028}b\u{2029}c.\"\"\"\n",
    "    beta()\n",
    "    gamma()\n",
);
const UTIL: &str = "from lib.helpers import delta\n\n\ndef beta():\n    delta()\n";
const HELPERS: &str = "def gamma():\n    return 1\n\n\ndef delta():\n    return 2\n";

fn fixture(prefix: &str) -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    common::index_repo(
        prefix,
        &[
            ("app/main.py", MAIN),
            ("lib/util.py", UTIL),
            ("lib/helpers.py", HELPERS),
        ],
    )
}

fn graph_cmd(root: &Path, db: &Path, out: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_lidx"))
        .arg("graph")
        .arg("--repo")
        .arg(root)
        .arg("--db")
        .arg(db)
        .arg("--out")
        .arg(out)
        .args(args)
        .output()
        .unwrap()
}

/// Run `lidx graph`, returning the page and its embedded document.
fn graph(root: &Path, db: &Path, args: &[&str]) -> (String, Value) {
    let out = root.join("graph-out.html");
    let res = graph_cmd(root, db, &out, args);
    assert!(
        res.status.success(),
        "lidx graph {args:?} failed: {}",
        String::from_utf8_lossy(&res.stderr)
    );
    let html = std::fs::read_to_string(&out).unwrap();
    let doc = lidx::graph_html::extract_document(&html).expect("embedded JSON parses");
    (html, doc)
}

struct Doc {
    strings: Vec<String>,
    nodes: Vec<Vec<Value>>,
    edges: Vec<Vec<Value>>,
    flag_halo: u64,
    flag_seed: u64,
}

impl Doc {
    fn new(doc: &Value) -> Self {
        let arr = |k: &str| -> Vec<Vec<Value>> {
            doc[k]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r.as_array().unwrap().clone())
                .collect()
        };
        Doc {
            strings: doc["s"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| s.as_str().unwrap().to_string())
                .collect(),
            nodes: arr("nodes"),
            edges: arr("edges"),
            flag_halo: doc["meta"]["flags"]["halo"].as_u64().unwrap(),
            flag_seed: doc["meta"]["flags"]["seed"].as_u64().unwrap(),
        }
    }
    fn s(&self, v: &Value) -> &str {
        &self.strings[v.as_u64().unwrap() as usize]
    }
    fn qual(&self, node: usize) -> &str {
        self.s(&self.nodes[node][1])
    }
    fn kind(&self, node: usize) -> &str {
        self.s(&self.nodes[node][2])
    }
    fn flags(&self, node: usize) -> u64 {
        self.nodes[node][7].as_u64().unwrap()
    }
    /// Last qualname segment of every node that is (not) in the halo.
    fn names(&self, halo: bool) -> BTreeSet<String> {
        (0..self.nodes.len())
            .filter(|&i| (self.flags(i) & self.flag_halo != 0) == halo)
            .map(|i| short(self.qual(i)))
            .collect()
    }
    /// `(from, to, kind, weight)` with endpoints by short name.
    fn edge_set(&self, include_halo: bool) -> BTreeSet<(String, String, String, u64)> {
        self.edges
            .iter()
            .filter(|e| {
                let (a, b) = (
                    e[0].as_u64().unwrap() as usize,
                    e[1].as_u64().unwrap() as usize,
                );
                include_halo
                    || (self.flags(a) & self.flag_halo == 0 && self.flags(b) & self.flag_halo == 0)
            })
            .map(|e| {
                (
                    short(self.qual(e[0].as_u64().unwrap() as usize)),
                    short(self.qual(e[1].as_u64().unwrap() as usize)),
                    self.s(&e[2]).to_string(),
                    e[3].as_u64().unwrap(),
                )
            })
            .collect()
    }
}

/// A symbol's last qualname segment, a file's name, or a module path as is.
fn short(qual: &str) -> String {
    if qual.ends_with('/') {
        return qual.to_string();
    }
    if let Some((_, file)) = qual.rsplit_once('/') {
        return file.to_string();
    }
    qual.rsplit(['.', ':']).next().unwrap_or(qual).to_string()
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

fn calls(edges: &[(&str, &str)]) -> BTreeSet<(String, String, String, u64)> {
    edges
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string(), "CALLS".to_string(), 1))
        .collect()
}

#[test]
fn seeded_graph_honours_depth_direction_and_kinds() {
    let (_tmp, root, db) = fixture("lidx-graph-seed-");

    // depth 1 down: the seed and its callees; delta (one hop further) is halo.
    let (_, doc) = graph(
        &root,
        &db,
        &[
            "--seed",
            "alpha",
            "--depth",
            "1",
            "--direction",
            "down",
            "--kinds",
            "CALLS",
        ],
    );
    let d = Doc::new(&doc);
    assert_eq!(doc["meta"]["level"], "symbol");
    assert_eq!(d.names(false), set(&["alpha", "beta", "gamma"]));
    assert_eq!(d.names(true), set(&["delta"]), "halo embeds the next hop");
    assert_eq!(
        d.edge_set(false),
        calls(&[("alpha", "beta"), ("alpha", "gamma")])
    );
    assert_eq!(
        d.edge_set(true),
        calls(&[("alpha", "beta"), ("alpha", "gamma"), ("beta", "delta")])
    );
    let seeds: Vec<String> = (0..d.nodes.len())
        .filter(|&i| d.flags(i) & d.flag_seed != 0)
        .map(|i| short(d.qual(i)))
        .collect();
    assert_eq!(seeds, vec!["alpha".to_string()]);
    assert_eq!(doc["meta"]["counts"]["nodes"], 4);
    assert_eq!(doc["meta"]["counts"]["halo_nodes"], 1);
    assert_eq!(doc["meta"]["counts"]["edges"], 3);

    // depth 2 reaches delta; nothing is left for the halo.
    let (_, doc) = graph(
        &root,
        &db,
        &[
            "--seed",
            "alpha",
            "--depth",
            "2",
            "--direction",
            "down",
            "--kinds",
            "CALLS",
        ],
    );
    let d = Doc::new(&doc);
    assert_eq!(d.names(false), set(&["alpha", "beta", "gamma", "delta"]));
    assert!(d.names(true).is_empty());

    // Upstream from delta: its callers, not alpha's other callee.
    let (_, doc) = graph(
        &root,
        &db,
        &[
            "--seed",
            "delta",
            "--depth",
            "2",
            "--direction",
            "up",
            "--kinds",
            "CALLS",
        ],
    );
    let d = Doc::new(&doc);
    assert_eq!(d.names(false), set(&["delta", "beta", "alpha"]));
    assert_eq!(
        d.edge_set(false),
        calls(&[("alpha", "beta"), ("beta", "delta")])
    );
    assert_eq!(d.names(true), set(&["gamma"]));

    // A kinds filter that excludes CALLS leaves no CALLS edge anywhere.
    let (_, doc) = graph(&root, &db, &["--seed", "alpha", "--kinds", "IMPORTS"]);
    let d = Doc::new(&doc);
    assert!(d.edges.iter().all(|e| d.s(&e[2]) == "IMPORTS"));
    assert!(!d.names(false).contains("beta"));

    // --max-nodes bounds the walk and says so.
    let (_, doc) = graph(
        &root,
        &db,
        &[
            "--seed",
            "alpha",
            "--depth",
            "3",
            "--kinds",
            "CALLS",
            "--max-nodes",
            "2",
        ],
    );
    let d = Doc::new(&doc);
    assert_eq!(d.names(false).len(), 2);
    assert_eq!(doc["meta"]["truncated"], true);
}

#[test]
fn file_and_module_levels_aggregate_weighted_edges() {
    let (_tmp, root, db) = fixture("lidx-graph-levels-");

    // Whole repo, file leaves that start collapsed into modules.
    let (_, doc) = graph(&root, &db, &["--kinds", "CALLS"]);
    let d = Doc::new(&doc);
    assert_eq!(doc["meta"]["level"], "module");
    assert_eq!(doc["meta"]["collapse_modules"], true);
    let files: BTreeSet<&str> = (0..d.nodes.len())
        .filter(|&i| d.kind(i) == "file")
        .map(|i| d.qual(i))
        .collect();
    assert_eq!(
        files,
        ["app/main.py", "lib/helpers.py", "lib/util.py"]
            .into_iter()
            .collect()
    );
    let modules: BTreeSet<&str> = d.nodes.iter().map(|n| d.s(&n[5])).collect();
    assert_eq!(modules, ["app/", "lib/"].into_iter().collect());
    assert_eq!(
        d.edge_set(true),
        [
            ("main.py", "util.py"),
            ("main.py", "helpers.py"),
            ("util.py", "helpers.py"),
        ]
        .into_iter()
        .map(|(a, b)| (a.to_string(), b.to_string(), "CALLS".to_string(), 1))
        .collect(),
        "one weighted CALLS edge per file pair; no self loops"
    );

    // Past --max-nodes a module overview falls back to module leaves: the
    // two cross-module calls merge into one edge of weight 2 and the
    // lib-internal call disappears.
    let (_, doc) = graph(&root, &db, &["--kinds", "CALLS", "--max-nodes", "2"]);
    let d = Doc::new(&doc);
    let kinds: BTreeSet<&str> = (0..d.nodes.len()).map(|i| d.kind(i)).collect();
    assert_eq!(kinds, ["module"].into_iter().collect());
    let edges: Vec<(String, String, String, u64)> = d.edge_set(true).into_iter().collect();
    assert_eq!(
        edges,
        vec![("app/".into(), "lib/".into(), "CALLS".into(), 2)],
        "{edges:?}"
    );

    // A seeded file-level view aggregates the seeded walk the same way.
    let (_, doc) = graph(
        &root,
        &db,
        &[
            "--seed",
            "alpha",
            "--depth",
            "2",
            "--direction",
            "down",
            "--kinds",
            "CALLS",
            "--level",
            "file",
        ],
    );
    let d = Doc::new(&doc);
    assert_eq!(doc["meta"]["level"], "file");
    let files: BTreeSet<&str> = (0..d.nodes.len())
        .filter(|&i| d.flags(i) & d.flag_halo == 0)
        .map(|i| d.qual(i))
        .collect();
    assert_eq!(
        files,
        ["app/main.py", "lib/helpers.py", "lib/util.py"]
            .into_iter()
            .collect()
    );
}

#[test]
fn embedded_json_cannot_break_out_of_its_script_block() {
    let (_tmp, root, db) = fixture("lidx-graph-escape-");
    let (html, doc) = graph(&root, &db, &["--seed", "alpha", "--depth", "1"]);
    let tag = r#"<script type="application/json" id="lidx-graph-data">"#;
    let start = html.find(tag).unwrap() + tag.len();
    let block = &html[start..start + html[start..].find("</script>").unwrap()];
    for bad in ["</", "<!--", "<script", "\u{2028}", "\u{2029}"] {
        assert!(
            !block.contains(bad),
            "{bad:?} appears raw in the data block"
        );
    }
    let d = Doc::new(&doc);
    assert!(
        d.strings.iter().any(
            |s| s.contains("</script><script>alert(1)</script> <!-- x --> a\u{2028}b\u{2029}c")
        ),
        "the docstring round-trips unchanged"
    );
    // Exactly the two script blocks of the template.
    assert_eq!(html.matches("</script>").count(), 2);
}

#[test]
fn missing_index_is_a_clear_error_and_nothing_is_created() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("nope").join("index.sqlite");
    let out = tmp.path().join("g.html");
    let res = graph_cmd(tmp.path(), &db, &out, &[]);
    assert!(!res.status.success());
    let err = String::from_utf8_lossy(&res.stderr);
    assert!(err.contains("no lidx index"), "{err}");
    assert!(err.contains("lidx reindex"), "{err}");
    assert!(!db.exists(), "graph must never create an index");
    assert!(!out.exists());
}

#[test]
fn whole_repo_symbol_level_is_refused() {
    let (_tmp, root, db) = fixture("lidx-graph-symlevel-");
    let res = graph_cmd(&root, &db, &root.join("x.html"), &["--level", "symbol"]);
    assert!(!res.status.success());
    assert!(String::from_utf8_lossy(&res.stderr).contains("--seed or --diff"));
}

#[test]
fn diff_view_seeds_changed_symbols_and_walks_to_callers() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-graph-diff-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(
        &root,
        &[
            ("app/main.py", MAIN),
            ("lib/util.py", UTIL),
            ("lib/helpers.py", HELPERS),
        ],
    );
    let git = |args: &[&str]| {
        let st = Command::new("git")
            .arg("-C")
            .arg(&root)
            .args(["-c", "user.name=t", "-c", "user.email=t@example.com"])
            .args(args)
            .output()
            .unwrap();
        assert!(
            st.status.success(),
            "{}",
            String::from_utf8_lossy(&st.stderr)
        );
    };
    git(&["init", "-q"]);
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "init"]);
    std::fs::write(
        root.join("lib/util.py"),
        UTIL.replace("    delta()\n", "    delta()\n    delta()\n"),
    )
    .unwrap();
    let db = root.join(".lidx").join(".lidx.sqlite");
    lidx::indexer::Indexer::new(root.clone(), db.clone())
        .unwrap()
        .reindex()
        .unwrap();

    let (_, doc) = graph(
        &root,
        &db,
        &["--diff", "HEAD", "--depth", "1", "--kinds", "CALLS"],
    );
    let d = Doc::new(&doc);
    let seeds: BTreeSet<String> = (0..d.nodes.len())
        .filter(|&i| d.flags(i) & d.flag_seed != 0)
        .map(|i| short(d.qual(i)))
        .collect();
    assert_eq!(seeds, set(&["beta"]));
    assert_eq!(d.names(false), set(&["beta", "alpha"]));
    assert!(
        doc["meta"]["scope"]
            .as_str()
            .unwrap()
            .starts_with("diff HEAD")
    );
}
