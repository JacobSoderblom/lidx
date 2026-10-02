//! Issue #245: a long path-style qualname must not lose its final segment to
//! the fuzzy prefilter's token cap.
use lidx::indexer::Indexer;
use lidx::resolve::find_candidates;

fn indexed() -> (tempfile::TempDir, Indexer) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
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
    let mut indexer =
        Indexer::new(root.to_path_buf(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    (dir, indexer)
}

fn names(indexer: &Indexer, q: &str) -> Vec<String> {
    find_candidates(indexer.db(), q, indexer.graph_version())
        .into_iter()
        .map(|s| s.qualname)
        .collect()
}

fn suggests(names: &[String], bare: &str) -> bool {
    names.iter().any(|n| n.ends_with(bare))
}

#[test]
fn eleven_token_path_qualname_suggests_final_segment() {
    let (_dir, indexer) = indexed();
    let q = "node/datacatalog-api/src/features/datacatalog/tables/get.findCatalogItem";
    assert!(suggests(&names(&indexer, q), "findCatalogItem"));
}

#[test]
fn five_token_path_qualname_suggests_final_segment() {
    let (_dir, indexer) = indexed();
    let q = "features/get.findCatalogItem";
    assert!(suggests(&names(&indexer, q), "findCatalogItem"));
}

#[test]
fn suggestion_is_monotonic_in_prefix_length() {
    let (_dir, indexer) = indexed();
    for n in [3, 6, 9, 12] {
        let prefix: Vec<String> = (0..n).map(|i| format!("datacatalog{i}")).collect();
        let q = format!("{}/get.findCatalogItem", prefix.join("/"));
        let r = names(&indexer, &q);
        assert!(suggests(&r, "findCatalogItem"), "{n} prefix tokens: {r:?}");
    }
}

#[test]
fn fifty_token_query_still_suggests_final_segment() {
    let (_dir, indexer) = indexed();
    let huge = format!("{}/findCatalogItem", vec!["segment"; 50].join("/"));
    assert!(suggests(&names(&indexer, &huge), "findCatalogItem"));
}

#[test]
fn markdown_style_path_qualname_suggests_final_segment() {
    let (_dir, indexer) = indexed();
    let q = "docs/guides/api/reference/catalog/tables/getting-started.md.findCatalogItem";
    assert!(suggests(&names(&indexer, q), "findCatalogItem"));
}

#[test]
fn dotted_csharp_qualname_unchanged() {
    let (_dir, indexer) = indexed();
    let r = names(
        &indexer,
        "Dpb.Wrong.Namespace.DeployRequestValidator.ValidateAsync",
    );
    assert!(
        r.first().is_some_and(|n| n.ends_with("ValidateAsync")),
        "{r:?}"
    );
}

#[test]
fn no_matching_final_segment_returns_nothing() {
    let (_dir, indexer) = indexed();
    let q = "a/b/c/d/e/f/g/h/i/j/k/Zzyzx.Qqqrrsttuv";
    assert!(names(&indexer, q).is_empty());
}
