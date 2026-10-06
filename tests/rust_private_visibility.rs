//! Rust module privacy: a private (no `pub`) item is visible in the module
//! that declares it (the module containing the `impl`, for a method) and in
//! every descendant module, never in a sibling.

mod common;

const LIB_RS: &str = "mod walk;\nmod other;\n\
pub enum Error {\n    Io(String),\n    Partial(Vec<Error>),\n}\n\
impl Error {\n    fn with_depth(self, depth: usize) -> Error {\n        self\n    }\n\
    fn with_path(self, p: &str) -> Error {\n        self\n    }\n}\n";

const WALK_RS: &str = "use crate::Error;\n\
pub struct Walk;\n\
impl Walk {\n\
    pub fn direct(&self, err: String) -> Error {\n        Error::Io(err).with_depth(1)\n    }\n\
    pub fn chained(&self, err: String) -> Error {\n        Error::Io(err).with_depth(1).with_path(\"x\")\n    }\n\
    pub fn mapped(&self, r: Result<(), String>) -> Result<(), Error> {\n        r.map_err(|err| Error::Io(err).with_depth(2))\n    }\n\
    pub fn typed(&self, e: Error) -> Error {\n        e.with_depth(3)\n    }\n}\n\
pub fn free(err: String) -> Error {\n    Error::Io(err).with_depth(4)\n}\n";

const OTHER_RS: &str = "pub fn outside() {}\n";

fn call_targets(files: &[(&str, &str)], source: &str, name: &str) -> Vec<Option<String>> {
    let (_tmp, snap) = common::index_files(files);
    snap.into_iter()
        .filter(|e| {
            e.kind == "CALLS"
                && e.source_qualname == source
                && e.target_qualname
                    .as_deref()
                    .is_some_and(|t| t.ends_with(&format!("::{name}")))
        })
        .map(|e| e.target_qualname)
        .collect()
}

#[test]
fn private_method_resolves_from_child_module() {
    let files = [
        ("src/lib.rs", LIB_RS),
        ("src/walk.rs", WALK_RS),
        ("src/other.rs", OTHER_RS),
    ];
    for source in [
        "crate::walk::Walk::direct",
        "crate::walk::Walk::chained",
        "crate::walk::Walk::mapped",
        "crate::walk::Walk::typed",
        "crate::walk::free",
    ] {
        let t = call_targets(&files, source, "with_depth");
        assert_eq!(
            t,
            vec![Some("crate::Error::with_depth".to_string())],
            "{source}"
        );
    }
    let t = call_targets(&files, "crate::walk::Walk::chained", "with_path");
    assert_eq!(t, vec![Some("crate::Error::with_path".to_string())]);
}

#[test]
fn private_item_stays_unresolved_from_sibling_module() {
    let files = [
        ("src/lib.rs", "mod a;\nmod b;\n"),
        (
            "src/a.rs",
            "fn helper() {}\nstruct T;\nimpl T {\n    fn secret(&self) {}\n}\n",
        ),
        ("src/b.rs", "pub fn caller() {\n    helper();\n}\n"),
    ];
    let (_tmp, edges) = common::index_files(&files);
    for e in edges
        .iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == "crate::b::caller")
    {
        assert!(
            e.target_qualname
                .as_deref()
                .is_none_or(|t| !t.starts_with("crate::a::")),
            "sibling must not see private: {e:?}"
        );
    }
}

#[test]
fn private_item_resolves_from_nested_descendant_module() {
    // Positive control for the sibling test above: same shapes, but the
    // caller sits in a module nested under the declaring module.
    let files = [
        ("src/lib.rs", "mod a;\n"),
        (
            "src/a.rs",
            "mod child;\nfn helper() {}\nstruct T;\nimpl T {\n    fn secret(&self) {}\n}\n",
        ),
        ("src/a/child.rs", "pub fn caller() {\n    helper();\n}\n"),
    ];
    let (_tmp, edges) = common::index_files(&files);
    let targets: Vec<_> = edges
        .iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == "crate::a::child::caller")
        .filter_map(|e| e.target_qualname.as_deref())
        .collect();
    assert!(targets.contains(&"crate::a::helper"), "{targets:?}");
}
