use lidx::indexer::rust::RustExtractor;
use lidx::metrics;

#[test]
fn duplication_hash_matches_identical_methods() {
    let source = r#"
struct A;
struct B;

impl A {
    fn foo(&self) { let x = 1; }
}

impl B {
    fn foo(&self) { let x = 1; }
}
"#;
    let mut extractor = RustExtractor::new().unwrap();
    let extracted = extractor.extract(source, "crate::mod").unwrap();
    let metrics = metrics::compute_symbol_metrics(source, "rust", &extracted.symbols);
    let mut hashes: Vec<_> = metrics
        .iter()
        .filter(|m| m.qualname.ends_with("::foo"))
        .filter_map(|m| m.duplication_hash.clone())
        .collect();
    hashes.sort();
    hashes.dedup();
    assert_eq!(hashes.len(), 1);
}
