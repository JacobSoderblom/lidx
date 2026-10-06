//! A receiver type declared in the caller's own module resolves to that
//! type's member, even when other crates (separate integration-test files)
//! declare a same-named type with the same method.

mod common;

const FIXTURE: &str = "\
struct Fixture;\n\
impl Fixture {\n\
    fn targets(&self, q: &str) -> usize {\n        q.len()\n    }\n\
}\n\
fn index() -> Fixture {\n    Fixture\n}\n";
const CALLER: &str = "\
fn check() -> usize {\n    index().targets(\"x\")\n}\n";

fn targets(files: &[(&str, &str)], source: &str) -> Vec<Option<String>> {
    let (_tmp, snap) = common::index_files(files);
    snap.into_iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == source)
        .map(|e| e.target_qualname)
        .collect()
}

fn caller_file() -> String {
    format!("{FIXTURE}{CALLER}")
}

#[test]
fn receiver_type_declared_in_caller_file_wins_over_same_named_type_in_other_crate() {
    let a = caller_file();
    let files = [
        (
            "Cargo.toml",
            "[package]\nname = \"app\"\nversion = \"0.1.0\"\n",
        ),
        ("src/lib.rs", ""),
        ("tests/a.rs", a.as_str()),
        ("tests/b.rs", FIXTURE),
        ("tests/c.rs", FIXTURE),
    ];
    let t = targets(&files, "crate::tests::a::check");
    assert!(
        t.contains(&Some("crate::tests::a::Fixture::targets".to_string())),
        "{t:?}"
    );
}

#[test]
fn receiver_type_declared_only_elsewhere_stays_ambiguous() {
    let caller = "fn check(f: Fixture) -> usize {\n    f.targets(\"x\")\n}\n";
    let files = [
        (
            "Cargo.toml",
            "[package]\nname = \"app\"\nversion = \"0.1.0\"\n",
        ),
        ("src/lib.rs", ""),
        ("tests/a.rs", caller),
        ("tests/b.rs", FIXTURE),
        ("tests/c.rs", FIXTURE),
    ];
    assert_eq!(targets(&files, "crate::tests::a::check"), vec![None]);
}
