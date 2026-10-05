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
