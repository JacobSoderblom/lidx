//! Issue #364: a typo in the last segment of a dotted qualname must suggest the
//! existing sibling member first, even when many unrelated symbols share the
//! query's other tokens and fill the fuzzy prefilter cap.
use lidx::indexer::Indexer;
use lidx::resolve::find_candidates;

fn indexed() -> (tempfile::TempDir, Indexer) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(
        root.join("ports.py"),
        "class PortTableDef:\n    columns: list = []\n    filter: str = \"\"\n",
    )
    .unwrap();
    // Well over FUZZY_SCAN_CAP (500) symbols matching several query tokens.
    for file in 0..8 {
        let body: String = (0..100)
            .map(|i| format!("def port_table_def_{file}_{i}():\n    pass\n\n"))
            .collect();
        std::fs::write(root.join(format!("decoys_{file}.py")), body).unwrap();
    }
    let mut indexer =
        Indexer::new(root.to_path_buf(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    (dir, indexer)
}

fn first(indexer: &Indexer, q: &str) -> Option<String> {
    find_candidates(indexer.db(), q, indexer.graph_version())
        .into_iter()
        .next()
        .map(|s| s.qualname)
}

#[test]
fn last_segment_typo_suggests_sibling_first() {
    let (_dir, indexer) = indexed();
    assert_eq!(
        first(&indexer, "ports.PortTableDef.colums").as_deref(),
        Some("ports.PortTableDef.columns")
    );
    assert_eq!(
        first(&indexer, "ports.PortTableDef.filtr").as_deref(),
        Some("ports.PortTableDef.filter")
    );
}

#[test]
fn rust_path_qualname_typo_suggests_sibling() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::write(
        root.join("src/lib.rs"),
        "pub struct Table;\nimpl Table {\n    pub fn columns(&self) {}\n}\n",
    )
    .unwrap();
    let mut indexer =
        Indexer::new(root.to_path_buf(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    let real = find_candidates(indexer.db(), "columns", indexer.graph_version())
        .into_iter()
        .find(|s| s.name == "columns")
        .unwrap()
        .qualname;
    assert!(real.contains("::Table::"), "{real}");
    let typo = real.replace("::columns", "::colums");
    let got = find_candidates(indexer.db(), &typo, indexer.graph_version());
    assert!(
        got.first().is_some_and(|s| s.qualname == real),
        "{:?}",
        got.iter().map(|s| &s.qualname).collect::<Vec<_>>()
    );
}

#[test]
fn large_parent_does_not_hide_nearest_sibling() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut body = String::from("class Big:\n");
    for i in 0..600 {
        body.push_str(&format!("    attr_{i:04}: int = 0\n"));
    }
    body.push_str("    zebra_stripes: int = 0\n");
    std::fs::write(root.join("big.py"), body).unwrap();
    let mut indexer =
        Indexer::new(root.to_path_buf(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    assert_eq!(
        first(&indexer, "big.Big.zebra_stripe").as_deref(),
        Some("big.Big.zebra_stripes")
    );
}

#[test]
fn short_last_segment_does_not_match_unrelated_sibling() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(root.join("foo.py"), "class Foo:\n    id: int = 0\n").unwrap();
    let mut indexer =
        Indexer::new(root.to_path_buf(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    let got = find_candidates(indexer.db(), "foo.Foo.x", indexer.graph_version());
    assert!(got.iter().all(|s| s.qualname != "foo.Foo.id"), "{got:?}");
}
