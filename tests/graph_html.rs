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
    flag_scope_out: u64,
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
            flag_scope_out: doc["meta"]["flags"]["scope_out"].as_u64().unwrap(),
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

    // Whole repo: the app view, over file leaves that start collapsed
    // into modules (the page's Folders level).
    let (_, doc) = graph(&root, &db, &["--kinds", "CALLS"]);
    let d = Doc::new(&doc);
    assert_eq!(doc["meta"]["level"], "app");
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

/// A test caller and a fixture under `testdata/` sit outside the default
/// `code` scope: embedded (so the page can show them) but flagged, and not
/// walked through. `--scope all` takes them in.
#[test]
fn scope_flags_non_code_files_and_keeps_them_out_of_the_walk() {
    let (_tmp, root, db) = common::index_repo(
        "lidx-graph-scope-",
        &[
            ("app/main.py", MAIN),
            ("lib/util.py", UTIL),
            ("lib/helpers.py", HELPERS),
            (
                "tests/test_main.py",
                "from app.main import alpha\n\n\ndef test_alpha():\n    alpha()\n",
            ),
            ("lib/testdata/sample.py", "def sample():\n    return 3\n"),
        ],
    );
    let out_of_scope = |d: &Doc| -> BTreeSet<String> {
        (0..d.nodes.len())
            .filter(|&i| d.flags(i) & d.flag_scope_out != 0)
            .map(|i| d.qual(i).to_string())
            .collect()
    };

    let (_, doc) = graph(&root, &db, &["--kinds", "CALLS"]);
    let d = Doc::new(&doc);
    assert_eq!(doc["meta"]["file_scope"], "code");
    assert_eq!(
        out_of_scope(&d),
        ["lib/testdata/sample.py", "tests/test_main.py"]
            .into_iter()
            .map(String::from)
            .collect()
    );
    assert_eq!(doc["meta"]["counts"]["out_of_scope"], 2);

    let (_, doc) = graph(&root, &db, &["--kinds", "CALLS", "--scope", "all"]);
    assert!(out_of_scope(&Doc::new(&doc)).is_empty());

    // Upward from beta: alpha is walked, the test caller only embedded as
    // an out-of-scope halo node.
    let (_, doc) = graph(
        &root,
        &db,
        &[
            "--seed",
            "beta",
            "--depth",
            "3",
            "--direction",
            "up",
            "--kinds",
            "CALLS",
        ],
    );
    let d = Doc::new(&doc);
    assert_eq!(d.names(false), set(&["beta", "alpha"]));
    // The halo holds both directions' next hops; the test caller is the one
    // out-of-scope node.
    assert_eq!(d.names(true), set(&["test_alpha", "gamma", "delta"]));
    let outs = out_of_scope(&d);
    assert_eq!(outs.len(), 1);
    assert!(outs.iter().all(|q| q.ends_with("test_alpha")), "{outs:?}");

    let (_, doc) = graph(
        &root,
        &db,
        &[
            "--seed",
            "beta",
            "--depth",
            "3",
            "--direction",
            "up",
            "--kinds",
            "CALLS",
            "--scope",
            "all",
        ],
    );
    assert_eq!(
        Doc::new(&doc).names(false),
        set(&["beta", "alpha", "test_alpha"])
    );
}

/// The header names the repo after its git remote, not the folder it was
/// cloned into; `--title` overrides it.
#[test]
fn header_uses_the_remote_name_or_title() {
    let (_tmp, root, db) = fixture("lidx-graph-name-");
    let git = |args: &[&str]| {
        let st = Command::new("git")
            .arg("-C")
            .arg(&root)
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
    let (_, doc) = graph(&root, &db, &[]);
    let folder = root
        .canonicalize()
        .unwrap()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    assert_eq!(
        doc["meta"]["repo"],
        folder.as_str(),
        "no remote: the top-level folder"
    );

    git(&[
        "remote",
        "add",
        "origin",
        "https://example.com/acme/widgets.git",
    ]);
    let (_, doc) = graph(&root, &db, &[]);
    assert_eq!(doc["meta"]["repo"], "widgets");

    let (html, doc) = graph(&root, &db, &["--title", "Widgets <core>"]);
    assert_eq!(doc["meta"]["repo"], "Widgets <core>");
    assert!(html.contains("<title>lidx graph · Widgets &lt;core&gt;</title>"));
}

/// A wider fixture: four packages of chained functions that also call into
/// the standard library, so walks, halos, externals and file leaves all
/// outgrow a small `--max-nodes`.
fn wide_files() -> Vec<(String, String)> {
    let mut files = Vec::new();
    for p in 0..4 {
        let mut src = String::from("import os\nimport json\n");
        if p > 0 {
            src.push_str(&format!("from pkg{}.mod import f{}_0\n", p - 1, p - 1));
        }
        for f in 0..8 {
            src.push_str(&format!("\n\ndef f{p}_{f}():\n"));
            if f + 1 < 8 {
                src.push_str(&format!("    f{p}_{}()\n", f + 1));
            }
            if p > 0 && f == 0 {
                src.push_str(&format!("    f{}_0()\n", p - 1));
            }
            src.push_str(&format!(
                "    os.path.join('a{f}', 'b')\n    json.dumps({f})\n"
            ));
        }
        files.push((format!("pkg{p}/mod.py"), src));
        files.push((
            format!("pkg{p}/extra.py"),
            format!("from pkg{p}.mod import f{p}_3\n\n\ndef helper{p}():\n    f{p}_3()\n"),
        ));
    }
    files
}

/// `--max-nodes` bounds every node the page holds: walk, halo, `--diff`
/// callers, externals, endpoints, files and modules.
#[test]
fn max_nodes_bounds_every_emitted_node() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-graph-cap-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    let files = wide_files();
    let refs: Vec<(&str, &str)> = files
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    common::write_files(&root, &refs);
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
    // Touch the bottom of the chain so the diff's callers are many.
    let changed = files[6]
        .1
        .replace("def f3_7():\n", "def f3_7():\n    x = 1\n");
    std::fs::write(root.join("pkg3/mod.py"), changed).unwrap();
    let db = root.join(".lidx").join(".lidx.sqlite");
    lidx::indexer::Indexer::new(root.clone(), db.clone())
        .unwrap()
        .reindex()
        .unwrap();

    let views: &[&[&str]] = &[
        &["--seed", "f0_0", "--depth", "9", "--direction", "both"],
        &["--seed", "f0_0", "--depth", "9", "--level", "file"],
        &["--level", "file"],
        &["--level", "module"],
        &["--diff", "HEAD", "--depth", "5"],
    ];
    for view in views {
        let (_, full) = graph(&root, &db, view);
        let total = full["meta"]["counts"]["nodes"].as_u64().unwrap();
        for cap in [3u64, 5, 7] {
            if total <= cap {
                continue;
            }
            let mut args: Vec<&str> = view.to_vec();
            let cap_s = cap.to_string();
            args.extend(["--max-nodes", cap_s.as_str()]);
            let (_, doc) = graph(&root, &db, &args);
            let n = doc["meta"]["counts"]["nodes"].as_u64().unwrap();
            assert!(n <= cap, "{view:?} --max-nodes {cap}: {n} nodes");
            assert_eq!(doc["meta"]["truncated"], true, "{view:?} --max-nodes {cap}");
            assert!(
                !doc["meta"]["notes"].as_array().unwrap().is_empty(),
                "{view:?}: a trimmed page says so"
            );
        }
        assert!(
            total > 3,
            "{view:?} is too small to exercise the cap: {total}"
        );
    }
}

/// An RPC call and its implementation store a guessed route; the proto
/// declares the real one. All three meet on one endpoint node, whichever
/// side the walk starts from and in the whole-repo view.
#[test]
fn rpc_route_endpoint_is_the_same_node_from_either_side() {
    const PROTO: &str = "syntax = \"proto3\";\npackage datacatalog.v1;\n\
service DataCatalogService {\n  rpc GetTables (Req) returns (Res);\n}\n\
message Req {}\nmessage Res {}\n";
    const HANDLER: &str = "export function getTables(call, cb) { cb(null, {}); }\n";
    const PLUGIN: &str = r#"
import { getTables } from "../features/datacatalog/tables/get";
import { DataCatalogServiceService } from "../gen/datacatalog_grpc_pb";
export function grpcPlugin(fastify) {
  fastify.grpcServer.addService(DataCatalogServiceService, {
    getTables: grpcHandler(getTables, fastify),
  });
}
"#;
    const CLIENT: &str = r#"
import { DataCatalogServiceClient } from "../gen/datacatalog_grpc_pb";
const client = new DataCatalogServiceClient("h");
const dataCatalogClient = client;
export function loadTables(req) {
  return dataCatalogClient.getTables.bind(dataCatalogClient)(req);
}
"#;
    let (_tmp, root, db) = common::index_repo(
        "lidx-graph-rpc-",
        &[
            ("protos/datacatalog.proto", PROTO),
            ("src/features/datacatalog/tables/get.ts", HANDLER),
            ("src/plugins/grpc.ts", PLUGIN),
            ("src/client/tables.ts", CLIENT),
        ],
    );
    let endpoints = |doc: &Value| -> Vec<String> {
        let d = Doc::new(doc);
        let flag = doc["meta"]["flags"]["endpoint"].as_u64().unwrap();
        let mut out: Vec<String> = (0..d.nodes.len())
            .filter(|&i| d.flags(i) & flag != 0)
            .map(|i| d.qual(i).to_string())
            .collect();
        out.sort();
        out
    };
    let route = "/datacatalog.v1.datacatalogservice/gettables".to_string();
    let views: &[&[&str]] = &[
        &[
            "--seed",
            "loadTables",
            "--depth",
            "3",
            "--direction",
            "down",
        ],
        &["--seed", "getTables", "--depth", "3", "--direction", "up"],
        &["--seed", "GetTables", "--depth", "2", "--direction", "both"],
        &["--level", "file", "--scope", "all"],
    ];
    for view in views {
        let (_, doc) = graph(&root, &db, view);
        assert_eq!(endpoints(&doc), vec![route.clone()], "{view:?}");
    }
    // From the client, the walk crosses the endpoint to the handler.
    let (_, doc) = graph(
        &root,
        &db,
        &[
            "--seed",
            "loadTables",
            "--depth",
            "3",
            "--direction",
            "down",
        ],
    );
    let names = Doc::new(&doc).names(false);
    assert!(names.iter().any(|n| n.ends_with(".getTables")), "{names:?}");
}

/// A `--seed` matching several symbols equally well names the one taken and
/// the others, on stderr and in the page.
#[test]
fn ambiguous_seed_warns_with_the_other_candidates() {
    let (_tmp, root, db) = common::index_repo(
        "lidx-graph-ambiguous-",
        &[
            ("a/one.py", "def run():\n    return 1\n"),
            ("b/two.py", "def run():\n    return 2\n"),
        ],
    );
    let out = root.join("g.html");
    let res = graph_cmd(&root, &db, &out, &["--seed", "run"]);
    assert!(res.status.success());
    let err = String::from_utf8_lossy(&res.stderr);
    assert!(err.contains("ambiguous"), "{err}");
    assert!(
        err.contains("a.one.run") && err.contains("b.two.run"),
        "{err}"
    );
    let doc = lidx::graph_html::extract_document(&std::fs::read_to_string(&out).unwrap()).unwrap();
    let notes = doc["meta"]["notes"].to_string();
    assert!(notes.contains("ambiguous"), "{notes}");
}

/// More seeds than `--max-nodes`: every seed is kept and the page says the
/// cap was raised, rather than only showing the raised value.
#[test]
fn seeds_over_the_cap_raise_it_and_say_so() {
    let (_tmp, root, db) = fixture("lidx-graph-seedcap-");
    let (_, doc) = graph(
        &root,
        &db,
        &["--seed", "alpha", "--seed", "beta", "--max-nodes", "1"],
    );
    assert_eq!(Doc::new(&doc).names(false), set(&["alpha", "beta"]));
    let notes = doc["meta"]["notes"].to_string();
    assert!(
        notes.contains("--max-nodes 1 raised to 2 to fit the seeds"),
        "{notes}"
    );
}

/// A bus publisher in one folder and its subscriber in another: the
/// overview joins the two folders with one derived `VIA_BUS` edge naming the
/// key, instead of routing both through a central endpoint hub.
#[test]
fn bridges_are_drawn_between_the_folders_that_own_them() {
    let (_tmp, root, db) = common::index_repo(
        "lidx-graph-bridges-",
        &[
            (
                "sender/send.py",
                "def send(msg):\n    _bus.publish(\"order-created\", msg)\n",
            ),
            (
                "worker/handle.py",
                "@router.subscribe('order-created')\ndef handle(msg):\n    pass\n",
            ),
        ],
    );
    let (_, doc) = graph(&root, &db, &["--level", "file"]);
    let d = Doc::new(&doc);
    let derived = doc["meta"]["flags"]["edge_derived"].as_u64().unwrap();
    let via: Vec<(String, String, String, u64, u64, String)> = d
        .edges
        .iter()
        .filter(|e| d.s(&e[2]).starts_with("VIA_"))
        .map(|e| {
            (
                d.qual(e[0].as_u64().unwrap() as usize).to_string(),
                d.qual(e[1].as_u64().unwrap() as usize).to_string(),
                d.s(&e[2]).to_string(),
                e[3].as_u64().unwrap(),
                e[4].as_u64().unwrap() & derived,
                d.s(&e[6]).to_string(),
            )
        })
        .collect();
    assert_eq!(
        via,
        vec![(
            "sender/send.py".to_string(),
            "worker/handle.py".to_string(),
            "VIA_BUS".to_string(),
            1,
            derived,
            "channel://ordercreated".to_string(),
        )],
        "{via:?}"
    );
    // The endpoint itself is still in the data, for the viewer to show on
    // request; nothing joins a folder to itself.
    let endpoint = doc["meta"]["flags"]["endpoint"].as_u64().unwrap();
    assert_eq!(
        (0..d.nodes.len())
            .filter(|&i| d.flags(i) & endpoint != 0)
            .count(),
        1
    );
}

/// Externals become one chip per consumer folder, listing its packages,
/// rather than a repo-wide `(external)` hub.
#[test]
fn externals_are_chips_inside_their_consumer_folder() {
    let (_tmp, root, db) = common::index_repo(
        "lidx-graph-extchips-",
        &[
            (
                "a/one.rs",
                "pub fn one() -> Vec<u8> { let v = Vec::new(); std::mem::drop(1); v }\n",
            ),
            ("b/two.rs", "pub fn two() -> String { String::new() }\n"),
        ],
    );
    let (_, doc) = graph(&root, &db, &["--level", "file"]);
    let d = Doc::new(&doc);
    let external = doc["meta"]["flags"]["external"].as_u64().unwrap();
    let chips: BTreeSet<(String, String)> = (0..d.nodes.len())
        .filter(|&i| d.flags(i) & external != 0)
        .map(|i| (d.qual(i).to_string(), d.s(&d.nodes[i][5]).to_string()))
        .collect();
    assert!(!chips.is_empty());
    for (qual, module) in &chips {
        assert_eq!(
            qual,
            &format!("ext:{module}"),
            "a chip lives in its consumer folder"
        );
        assert!(module == "a/" || module == "b/", "{module}");
    }
}

/// Derived bridge edges of a document: `(from qual, to qual, kind, weight,
/// keys)`.
fn bridge_edges(doc: &Value) -> Vec<(String, String, String, u64, Vec<String>)> {
    let d = Doc::new(doc);
    let derived = doc["meta"]["flags"]["edge_derived"].as_u64().unwrap();
    let mut out: Vec<_> = d
        .edges
        .iter()
        .filter(|e| e[4].as_u64().unwrap() & derived != 0)
        .map(|e| {
            let keys = e
                .get(9)
                .and_then(Value::as_array)
                .map(|ks| ks.iter().map(|k| d.s(k).to_string()).collect())
                .unwrap_or_default();
            (
                d.qual(e[0].as_u64().unwrap() as usize).to_string(),
                d.qual(e[1].as_u64().unwrap() as usize).to_string(),
                d.s(&e[2]).to_string(),
                e[3].as_u64().unwrap(),
                keys,
            )
        })
        .collect();
    out.sort();
    out
}

fn publisher(keys: &[&str]) -> String {
    let mut s = String::from("def send(msg):\n");
    for k in keys {
        s.push_str(&format!("    _bus.publish(\"{k}\", msg)\n"));
    }
    s
}

fn subscriber(keys: &[&str]) -> String {
    keys.iter()
        .enumerate()
        .map(|(i, k)| format!("@router.subscribe('{k}')\ndef handle_{i}(msg):\n    pass\n\n\n"))
        .collect()
}

/// Two senders in one folder share a key with a receiver in another: the
/// folder edge weighs the distinct keys (3), not the file pairs (2 + 2).
/// A key published and received inside one file, or inside one folder, never
/// draws an edge from a node to itself. Bridges are derived before the
/// --max-nodes trim, so a cap that drops every endpoint keeps them.
#[test]
fn bridge_weights_count_distinct_keys_and_survive_the_cap() {
    let a = publisher(&["alpha", "beta"]);
    let b = publisher(&["beta", "gamma"]);
    let w = subscriber(&["alpha", "beta", "gamma"]);
    let selfish = format!("{}\n\n{}", publisher(&["delta"]), subscriber(&["delta"]));
    let pub_e = publisher(&["epsilon"]);
    let sub_e = subscriber(&["epsilon"]);
    let (_tmp, root, db) = common::index_repo(
        "lidx-graph-bridgekeys-",
        &[
            ("sender/a.py", a.as_str()),
            ("sender/b.py", b.as_str()),
            ("worker/w.py", w.as_str()),
            ("loop/self.py", selfish.as_str()),
            ("pair/p.py", pub_e.as_str()),
            ("pair/s.py", sub_e.as_str()),
        ],
    );
    let ch = |k: &str| format!("channel://{k}");

    // File level: one edge per file pair, each with its own keys.
    let (_, doc) = graph(&root, &db, &["--level", "file"]);
    let edges = bridge_edges(&doc);
    assert_eq!(
        edges,
        vec![
            (
                "pair/p.py".into(),
                "pair/s.py".into(),
                "VIA_BUS".into(),
                1,
                vec![ch("epsilon")]
            ),
            (
                "sender/a.py".into(),
                "worker/w.py".into(),
                "VIA_BUS".into(),
                2,
                vec![ch("alpha"), ch("beta")]
            ),
            (
                "sender/b.py".into(),
                "worker/w.py".into(),
                "VIA_BUS".into(),
                2,
                vec![ch("beta"), ch("gamma")]
            ),
        ],
        "{edges:?}"
    );
    assert!(edges.iter().all(|e| e.0 != e.1), "no self loops");

    // Module leaves (6 files over --max-nodes 4) and a cap that also drops
    // every endpoint: one sender/ -> worker/ edge of 3 distinct keys; the
    // same-folder pair/ key and the same-file loop/ key draw nothing.
    let (_, doc) = graph(&root, &db, &["--level", "module", "--max-nodes", "4"]);
    let edges = bridge_edges(&doc);
    assert_eq!(
        edges,
        vec![(
            "sender/".into(),
            "worker/".into(),
            "VIA_BUS".into(),
            3,
            vec![ch("alpha"), ch("beta"), ch("gamma")]
        )],
        "{edges:?}"
    );
    let endpoint = doc["meta"]["flags"]["endpoint"].as_u64().unwrap();
    let d = Doc::new(&doc);
    assert_eq!(
        (0..d.nodes.len())
            .filter(|&i| d.flags(i) & endpoint != 0)
            .count(),
        0,
        "endpoints trimmed"
    );
    assert!(d.nodes.len() <= 4);

    // A cap that drops an owner says how many bridge links went with it.
    let (_, doc) = graph(&root, &db, &["--level", "file", "--max-nodes", "1"]);
    let notes = doc["meta"]["notes"].to_string();
    assert!(notes.contains("bridge link"), "{notes}");
}

/// A proto declares the route; it is not a caller. The client calls the
/// implementer over RPC, and the proto gets a `DECLARES_RPC` link to the
/// implementer instead of a `VIA_RPC` one.
#[test]
fn a_proto_declares_its_route_and_does_not_call_the_implementer() {
    const PROTO: &str = "syntax = \"proto3\";\npackage datacatalog.v1;\n\
service DataCatalogService {\n  rpc GetTables (Req) returns (Res);\n}\n\
message Req {}\nmessage Res {}\n";
    const HANDLER: &str = "export function getTables(call, cb) { cb(null, {}); }\n";
    const PLUGIN: &str = r#"
import { getTables } from "../features/datacatalog/tables/get";
import { DataCatalogServiceService } from "../gen/datacatalog_grpc_pb";
export function grpcPlugin(fastify) {
  fastify.grpcServer.addService(DataCatalogServiceService, {
    getTables: grpcHandler(getTables, fastify),
  });
}
"#;
    const CLIENT: &str = r#"
import { DataCatalogServiceClient } from "../gen/datacatalog_grpc_pb";
const client = new DataCatalogServiceClient("h");
const dataCatalogClient = client;
export function loadTables(req) {
  return dataCatalogClient.getTables.bind(dataCatalogClient)(req);
}
"#;
    let (_tmp, root, db) = common::index_repo(
        "lidx-graph-protobridge-",
        &[
            ("protos/datacatalog.proto", PROTO),
            ("src/features/datacatalog/tables/get.ts", HANDLER),
            ("src/plugins/grpc.ts", PLUGIN),
            ("src/client/tables.ts", CLIENT),
        ],
    );
    let (_, doc) = graph(&root, &db, &["--level", "file", "--scope", "all"]);
    let edges: Vec<(String, String, String)> = bridge_edges(&doc)
        .into_iter()
        .map(|(a, b, k, _, _)| (a, b, k))
        .collect();
    let route = "src/features/datacatalog/tables/get.ts".to_string();
    assert!(
        edges.contains(&(
            "src/client/tables.ts".into(),
            route.clone(),
            "VIA_RPC".into()
        )),
        "{edges:?}"
    );
    assert!(
        edges.contains(&(
            "protos/datacatalog.proto".into(),
            route.clone(),
            "DECLARES_RPC".into()
        )),
        "{edges:?}"
    );
    assert!(
        !edges
            .iter()
            .any(|(a, _, k)| a == "protos/datacatalog.proto" && k == "VIA_RPC"),
        "a proto is never a caller: {edges:?}"
    );
}

/// A consumer folder's chip lists its external packages with call counts.
#[test]
fn external_chips_list_their_packages() {
    let (_tmp, root, db) = common::index_repo(
        "lidx-graph-chiplist-",
        &[(
            "a/one.rs",
            "pub fn one() -> Vec<u8> { let v = Vec::new(); let w: Vec<u8> = Vec::new(); drop(w); std::mem::drop(1); v }\n",
        )],
    );
    let (_, doc) = graph(&root, &db, &["--level", "file"]);
    let d = Doc::new(&doc);
    let external = doc["meta"]["flags"]["external"].as_u64().unwrap();
    let chip = (0..d.nodes.len())
        .find(|&i| d.flags(i) & external != 0)
        .expect("a chip");
    assert_eq!(d.qual(chip), "ext:a/");
    let sig = d.s(&d.nodes[chip][8]).to_string();
    assert!(sig.contains("Vec ×"), "{sig}");
    let name = d.s(&d.nodes[chip][0]).to_string();
    assert!(
        name.ends_with("external package") || name.ends_with("external packages"),
        "{name}"
    );
}
