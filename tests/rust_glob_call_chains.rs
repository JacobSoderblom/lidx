//! Issue #497: a call written against a name a glob import brings in
//! (`use super::*;`), possibly re-exported by the module the glob names
//! (`pub(crate) use tree::Tree;`), types the value it returns, so a method
//! called on it binds that type's method and not a same-named method of
//! another type.

mod common;

use std::collections::BTreeSet;

fn targets(files: &[(&str, &str)], caller: &str) -> BTreeSet<String> {
    common::call_targets(files, caller).into_iter().collect()
}

const MANIFEST: &str = "[package]\nname = \"x\"\n";

const LIB: &str = "mod node;\nmod tree;\nmod other;\npub(crate) use crate::tree::Tree;\n";
const TREE: &str = "pub(crate) struct Tree(u8);\nimpl Tree {\n    pub(crate) fn atom(x: u8) -> Self { Tree(x) }\n    pub(crate) fn extend(self, _y: u8) -> Self { self }\n}\n";
const OTHER: &str = "pub(crate) struct Other(u8);\nimpl Other {\n    pub(crate) fn extend(self, _y: u8) -> Self { self }\n}\n";

#[test]
fn associated_function_through_a_glob_and_a_re_export() {
    let node = "use super::*;\npub fn go() -> Tree {\n    Tree::atom(1).extend(2)\n}\n";
    let got = targets(
        &[
            ("Cargo.toml", MANIFEST),
            ("src/lib.rs", LIB),
            ("src/tree.rs", TREE),
            ("src/other.rs", OTHER),
            ("src/node.rs", node),
        ],
        "crate::node::go",
    );
    assert!(got.contains("crate::tree::Tree::extend"), "{got:#?}");
    assert!(!got.contains("crate::other::Other::extend"), "{got:#?}");
}

#[test]
fn field_of_a_value_a_call_returns() {
    let tree = "pub(crate) struct Holder { pub(crate) inner: Inner }\npub(crate) struct Inner(u8);\nimpl Inner {\n    pub(crate) fn poke(&mut self) {}\n}\nimpl Holder {\n    pub(crate) fn make() -> Holder { Holder { inner: Inner(0) } }\n}\n";
    let other =
        "pub(crate) struct Other(u8);\nimpl Other {\n    pub(crate) fn poke(&mut self) {}\n}\n";
    let node = "use crate::tree::Holder;\npub fn go() {\n    let mut h = Holder::make();\n    h.inner.poke();\n}\n";
    let got = targets(
        &[
            ("Cargo.toml", MANIFEST),
            ("src/lib.rs", "mod node;\nmod tree;\nmod other;\n"),
            ("src/tree.rs", tree),
            ("src/other.rs", other),
            ("src/node.rs", node),
        ],
        "crate::node::go",
    );
    assert!(got.contains("crate::tree::Inner::poke"), "{got:#?}");
    assert!(!got.contains("crate::other::Other::poke"), "{got:#?}");
}
