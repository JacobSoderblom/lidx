//! Issue #348: Rust symbol spans start at the first contiguous outer
//! attribute (`#[derive]`, `#[cfg]`, `#[test]`), like Python decorators.

use lidx::indexer::Indexer;
use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::rust::RustExtractor;
use lidx::rpc;
use std::collections::BTreeMap;

const FIXTURE: &str = "#[derive(Debug)]
pub struct A {
    pub x: u8,
}

#[cfg(test)]
mod tests {
    #[test]
    fn t() {}
}
";

/// qualname -> (start_line, end_line) for every symbol in `source`.
fn spans(source: &str) -> BTreeMap<String, (i64, i64)> {
    let mut extractor = RustExtractor::new().unwrap();
    let extracted = extractor.extract(source, "crate").unwrap();
    extracted
        .symbols
        .iter()
        .map(|s| (s.qualname.clone(), (s.start_line, s.end_line)))
        .collect()
}

#[test]
fn issue_fixture_spans_start_at_attributes() {
    let s = spans(FIXTURE);
    assert_eq!(s["crate::A"], (1, 4));
    assert_eq!(s["crate::tests"], (6, 10));
    assert_eq!(s["crate::tests::t"], (8, 9));
}

#[test]
fn stacked_multiline_and_comment_separated_attributes() {
    let src = "/// docs
#[derive(Debug)]
#[cfg_attr(
    feature = \"x\",
    derive(Clone)
)]
// a comment between attributes and item
pub struct A;

struct Plain;

/// doc
fn free() {}
";
    let s = spans(src);
    assert_eq!(s["crate::A"].0, 2);
    assert_eq!(s["crate::Plain"], (10, 10));
    assert_eq!(s["crate::free"], (13, 13));
}

#[test]
fn item_kinds_include_attributes() {
    let src = "#[allow(dead_code)]
enum E { A }
#[allow(dead_code)]
trait T {
    #[cfg(unix)]
    fn sig(&self);
    #[inline]
    fn dflt(&self) {}
}
#[allow(dead_code)]
const C: u8 = 1;
#[allow(dead_code)]
static S: u8 = 1;
#[allow(dead_code)]
type Alias = u8;
struct X;
impl X {
    #[test]
    fn m(&self) {}
}
";
    let s = spans(src);
    assert_eq!(s["crate::E"].0, 1);
    assert_eq!(s["crate::T"].0, 3);
    assert_eq!(s["crate::T::sig"].0, 5);
    assert_eq!(s["crate::T::dflt"].0, 7);
    assert_eq!(s["crate::C"].0, 10);
    assert_eq!(s["crate::S"].0, 12);
    assert_eq!(s["crate::Alias"].0, 14);
    assert_eq!(s["crate::X::m"].0, 18);
}

#[test]
fn nested_fn_does_not_inherit_outer_attribute_start() {
    let src = "#[test]
fn outer() {
    fn inner() {}
}
";
    let s = spans(src);
    assert_eq!(s["crate::outer"].0, 1);
    assert_eq!(s["crate::inner"].0, 3);
}

#[test]
fn cfg_twins_do_not_overlap() {
    let src = "#[cfg(unix)]
pub fn go() {}

#[cfg(windows)]
pub fn go() {}
";
    let mut extractor = RustExtractor::new().unwrap();
    let extracted = extractor.extract(src, "crate").unwrap();
    let mut twins: Vec<_> = extracted
        .symbols
        .iter()
        .filter(|s| s.qualname == "crate::go")
        .map(|s| (s.start_line, s.end_line))
        .collect();
    twins.sort();
    assert_eq!(twins, vec![(1, 2), (4, 5)]);
}

#[test]
fn read_symbol_includes_attributes_and_incremental_matches_fresh() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::write(root.join("src/lib.rs"), FIXTURE).unwrap();
    let db = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();

    for (qn, start, needle) in [
        ("crate::A", 1, "#[derive(Debug)]"),
        ("crate::tests", 6, "#[cfg(test)]"),
        ("crate::tests::t", 8, "#[test]"),
    ] {
        let raw = rpc::call(
            root.clone(),
            db.clone(),
            "read_symbol".to_string(),
            &serde_json::json!({"qualname": qn}).to_string(),
            "1",
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let r = &v["result"];
        assert!(r.to_string().contains(needle), "{qn}: {r}");
        assert_eq!(r["start_line"], start, "{qn}: {r}");
    }

    let snapshot = |indexer: &Indexer| -> Vec<(String, i64, i64)> {
        let gv = indexer.db().current_graph_version().unwrap();
        let conn = indexer.db().read_conn().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT qualname, start_line, end_line FROM symbols
                 WHERE graph_version = ? ORDER BY qualname",
            )
            .unwrap();
        stmt.query_map([gv], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };
    // Incremental: shift everything down by editing the file.
    std::fs::write(root.join("src/lib.rs"), format!("// top\n{FIXTURE}")).unwrap();
    indexer.reindex().unwrap();
    let incremental = snapshot(&indexer);

    let tmp2 = tempfile::tempdir().unwrap();
    let root2 = tmp2.path().to_path_buf();
    std::fs::create_dir_all(root2.join("src")).unwrap();
    std::fs::write(root2.join("src/lib.rs"), format!("// top\n{FIXTURE}")).unwrap();
    let mut fresh = Indexer::new(root2.clone(), root2.join(".lidx").join(".lidx.sqlite")).unwrap();
    fresh.reindex().unwrap();
    assert_eq!(incremental, snapshot(&fresh));
    assert!(incremental.contains(&("crate::A".to_string(), 2, 5)));
}

#[test]
fn bodiless_mod_file_edge_evidence_starts_at_attribute() {
    let mut extractor = RustExtractor::new().unwrap();
    let extracted = extractor
        .extract("#[cfg(unix)]\nmod foo;\n", "crate")
        .unwrap();
    let edge = extracted
        .edges
        .iter()
        .find(|e| e.kind == "MODULE_FILE")
        .unwrap();
    assert_eq!(edge.evidence_start_line, Some(1));
    assert_eq!(edge.evidence_end_line, Some(2));
}
