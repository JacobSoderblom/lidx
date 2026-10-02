//! Issue #222: decorator-derived edges on a nested `def` must hang off an
//! indexed symbol (the nearest enclosing one), never a dangling qualname.

use lidx::indexer::Indexer;
use lidx::indexer::extract::{EdgeInput, ExtractedFile, LanguageExtractor};
use lidx::indexer::python::PythonExtractor;
use lidx::rpc;
use serde_json::Value;

fn extract(source: &str) -> ExtractedFile {
    PythonExtractor::new()
        .unwrap()
        .extract(source, "m")
        .unwrap()
}

fn decorator_edges(file: &ExtractedFile) -> Vec<&EdgeInput> {
    file.edges
        .iter()
        .filter(|e| {
            matches!(
                e.kind.as_str(),
                "CHANNEL_PUBLISH" | "CHANNEL_SUBSCRIBE" | "HTTP_ROUTE"
            )
        })
        .collect()
}

fn assert_sources_indexed(file: &ExtractedFile) {
    let edges = decorator_edges(file);
    assert!(!edges.is_empty());
    for e in edges {
        let src = e.source_qualname.as_deref().unwrap_or("");
        assert!(
            file.symbols.iter().any(|s| s.qualname == src),
            "{} edge source {src:?} is not an indexed symbol",
            e.kind
        );
    }
}

fn sources_of_kind<'a>(file: &'a ExtractedFile, kind: &str) -> Vec<&'a str> {
    file.edges
        .iter()
        .filter(|e| e.kind == kind)
        .map(|e| {
            e.source_qualname
                .as_deref()
                .expect("edge has a source qualname")
        })
        .collect()
}

const NESTED: &str = r#"
def make_router():
    @router.publish(topic="out")
    @router.subscribe(topic="in")
    async def handle(msg): ...

@router.subscribe(topic="top")
async def handle(msg): ...
"#;

#[test]
fn nested_handler_edges_attach_to_enclosing_symbol_and_top_level_is_unchanged() {
    let f = extract(NESTED);
    assert_sources_indexed(&f);
    assert_eq!(sources_of_kind(&f, "CHANNEL_PUBLISH"), ["m.make_router"]);
    // Nested and top-level `handle` do not collide.
    assert_eq!(
        sources_of_kind(&f, "CHANNEL_SUBSCRIBE"),
        ["m.make_router", "m.handle"]
    );
}

#[test]
fn two_levels_deep_and_method_factories_attach_to_nearest_indexed_symbol() {
    let f = extract(
        r#"
def outer():
    def mid():
        @router.subscribe(topic="deep")
        def leaf(msg): ...

class C:
    def meth(self):
        @router.subscribe(topic="m")
        def h(msg): ...
        @app.get("/x")
        def route(): ...
"#,
    );
    assert_sources_indexed(&f);
    assert_eq!(
        sources_of_kind(&f, "CHANNEL_SUBSCRIBE"),
        ["m.outer", "m.C.meth"]
    );
    assert_eq!(sources_of_kind(&f, "HTTP_ROUTE"), ["m.C.meth"]);
}

const CHANNEL_SNIPPETS: &[(&str, &str)] = &[
    (
        "a.py",
        "def make():\n    @r.subscribe(topic=\"t\")\n    def h(m): ...\n@r.publish(topic=\"t\")\ndef top(m): ...\n",
    ),
    (
        "b.cs",
        "class P { void R() { _bus.PublishAsync(\"x\", 1); } }",
    ),
    ("c.go", "package main\nfunc a() { bus.Publish(\"x\", e) }\n"),
    ("d.js", "function a() { bus.publish(\"x\", e); }"),
    ("e.ts", "function a() { bus.subscribe(\"x\", e); }"),
    ("f.rs", "fn a() { bus.publish(\"x\", e); }"),
];

fn assert_no_null_source_channel_edges(root: &std::path::Path, db: &std::path::Path) {
    let mut indexer = Indexer::new(root.to_path_buf(), db.to_path_buf()).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges WHERE graph_version = ? \
             AND kind IN ('CHANNEL_PUBLISH','CHANNEL_SUBSCRIBE') AND source_symbol_id IS NULL",
            rusqlite::params![gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 0, "NULL-source channel edges in {}", root.display());
}

#[test]
fn no_channel_edge_has_null_source_in_any_language_or_fixture() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-null-source-")
        .tempdir()
        .unwrap();
    let root = tmp.path().join("repo");
    std::fs::create_dir_all(&root).unwrap();
    for (name, src) in CHANNEL_SNIPPETS {
        std::fs::write(root.join(name), src).unwrap();
    }
    assert_no_null_source_channel_edges(&root, &tmp.path().join("db.sqlite"));

    // Every checked-in fixture repo too.
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    for entry in std::fs::read_dir(fixtures).unwrap() {
        let dir = entry.unwrap().path();
        if dir.is_dir() {
            assert_no_null_source_channel_edges(&dir, &tmp.path().join("fx.sqlite"));
            let _ = std::fs::remove_file(tmp.path().join("fx.sqlite"));
        }
    }
}

#[test]
fn indexing_fails_loudly_on_channel_edge_with_unresolved_source() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-fail-loud-")
        .tempdir()
        .unwrap();
    let mut db = lidx::db::Db::new(&tmp.path().join("db.sqlite")).unwrap();
    let file_id = db.upsert_file("a.py", "h", "python", 1, 1).unwrap();
    let edge = EdgeInput {
        kind: "CHANNEL_SUBSCRIBE".to_string(),
        source_qualname: Some("m.ghost".to_string()),
        target_qualname: Some("channel://t".to_string()),
        ..Default::default()
    };
    let err = db
        .insert_edges(file_id, &[edge], &Default::default(), 1, None)
        .unwrap_err();
    assert!(err.to_string().contains("unresolved source"), "{err}");
}

#[test]
fn nested_body_calls_attach_once_to_enclosing_symbol() {
    let f = extract("def outer():\n    def inner():\n        helper()\n    return 1\n");
    let calls: Vec<_> = f
        .edges
        .iter()
        .filter(|e| {
            e.kind == "CALLS"
                && e.target_qualname
                    .as_deref()
                    .is_some_and(|t| t.ends_with("helper"))
        })
        .collect();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(calls[0].source_qualname.as_deref(), Some("m.outer"));
}

#[test]
fn trace_flow_reaches_factory_registered_handler_across_the_bus() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-nested-channel-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    std::fs::write(
        root.join("publisher.py"),
        "def send(msg):\n    _bus.publish(\"orders\", msg)\n",
    )
    .unwrap();
    std::fs::write(
        root.join("subscriber.py"),
        "def make_router():\n    @router.subscribe(topic=\"orders\")\n    def handle(msg):\n        pass\n",
    )
    .unwrap();
    let db = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let raw = rpc::call(
        root,
        db,
        "trace_flow".to_string(),
        r#"{"start_qualname":"publisher.send","direction":"downstream","max_hops":5}"#,
        "1",
    )
    .unwrap();
    let v: Value = serde_json::from_str(&raw).unwrap();
    let hop = v["result"]["trace"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|p| {
            p["hops"]
                .as_array()
                .cloned()
                .unwrap_or_else(|| vec![p.clone()])
        })
        .find(|h| h["symbol"]["qualname"] == "subscriber.make_router")
        .unwrap_or_else(|| panic!("trace did not reach the nested handler: {raw}"));
    assert_eq!(hop["edge_kind"], "CHANNEL_SUBSCRIBE", "{hop}");
}
