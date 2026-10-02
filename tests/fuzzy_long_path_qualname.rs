//! Issue #245: a long path-style qualname must not lose its final segment to
//! the fuzzy prefilter's token cap.
use lidx::indexer::Indexer;
use lidx::resolve::find_candidates;
use std::path::PathBuf;

fn indexed(label: &str) -> (PathBuf, Indexer) {
    let mut root = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    root.push(format!("lidx-fuzzy-long-{label}-{nanos}"));
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::write(
        root.join("src/get.ts"),
        "export function findCatalogItem(id: string) { return id; }\n",
    )
    .unwrap();
    std::fs::write(
        root.join("src/Validator.cs"),
        "namespace Dpb.Real { public class DeployRequestValidator { public void ValidateAsync() {} } }\n",
    )
    .unwrap();
    // Unrelated symbols sharing path-prefix words.
    let decoys: String = (0..20)
        .map(|i| format!("export function datacatalogTables{i}() {{}}\nexport function featuresApi{i}() {{}}\n"))
        .collect();
    std::fs::write(root.join("src/decoys.ts"), decoys).unwrap();
    let mut indexer = Indexer::new(root.clone(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    (root, indexer)
}

fn names(indexer: &Indexer, q: &str) -> Vec<String> {
    find_candidates(indexer.db(), q, indexer.graph_version())
        .into_iter()
        .map(|s| s.qualname)
        .collect()
}

fn has(names: &[String], bare: &str) -> bool {
    names.iter().any(|n| n.ends_with(bare))
}

#[test]
fn long_path_qualname_still_suggests_final_segment() {
    let (root, indexer) = indexed("ts");
    let long = "node/datacatalog-api/src/features/datacatalog/tables/get.findCatalogItem";
    assert!(has(&names(&indexer, long), "findCatalogItem"), "11 tokens");
    let short = "features/get.findCatalogItem";
    assert!(has(&names(&indexer, short), "findCatalogItem"), "5 tokens");
    for n in [3, 6, 9, 12] {
        let prefix: Vec<String> = (0..n).map(|i| format!("datacatalog{i}")).collect();
        let q = format!("{}/get.findCatalogItem", prefix.join("/"));
        let r = names(&indexer, &q);
        assert!(has(&r, "findCatalogItem"), "{n} prefix tokens: {r:?}");
    }
    // Pathological 50-token query still answers (prefilter stays bounded).
    let huge = format!("{}/findCatalogItem", vec!["segment"; 50].join("/"));
    assert!(has(&names(&indexer, &huge), "findCatalogItem"));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn markdown_style_path_qualname_suggests_final_segment() {
    let (root, indexer) = indexed("md");
    let q = "docs/guides/api/reference/catalog/tables/getting-started.md.findCatalogItem";
    assert!(has(&names(&indexer, q), "findCatalogItem"));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn dotted_csharp_qualname_unchanged() {
    let (root, indexer) = indexed("cs");
    let r = names(
        &indexer,
        "Dpb.Wrong.Namespace.DeployRequestValidator.ValidateAsync",
    );
    assert!(
        r.first().is_some_and(|n| n.ends_with("ValidateAsync")),
        "{r:?}"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn no_matching_final_segment_returns_nothing() {
    let (root, indexer) = indexed("none");
    let q = "a/b/c/d/e/f/g/h/i/j/k/Zzyzx.Qqqrrsttuv";
    assert!(names(&indexer, q).is_empty());
    let _ = std::fs::remove_dir_all(root);
}
