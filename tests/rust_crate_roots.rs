//! Cargo `[lib]`/`[[bin]]`/`[[test]]` `path` entries name crate roots, so the
//! files under them get qualnames relative to that root (`crate::search`, not
//! `crate::crates::core::search`), and a call through a `pub use .. as alias`
//! re-export reaches the aliased item.

mod common;

const ROOT_TOML: &str = "[package]\nname = \"app\"\nversion = \"0.1.0\"\n\n\
[[bin]]\nname = \"app\"\npath = \"crates/core/main.rs\"\n\n\
[[test]]\nname = \"integration\"\npath = \"tests/tests.rs\"\n";

const MAIN_RS: &str = "mod flags;\nmod index;\n\
fn search(n: u32) -> u32 {\n    n\n}\n\
fn main() {\n    flags::generate_version_short();\n}\n";
const INDEX_MOD_RS: &str = "mod enabled;\n";
const ENABLED_RS: &str = "pub fn read() -> u32 {\n    crate::search(1)\n}\n";
const FLAGS_MOD_RS: &str = "pub mod doc;\n\
pub(crate) use crate::flags::{\n    doc::{\n        help::{generate_short as generate_help_short},\n        \
version::{\n            generate_long as generate_version_long,\n            \
generate_short as generate_version_short,\n        },\n    },\n};\n";
const DOC_MOD_RS: &str = "pub mod help;\npub mod version;\n";
const HELP_RS: &str = "pub fn generate_short() {}\n";
const VERSION_RS: &str = "pub fn generate_short() {}\npub fn generate_long() {}\n";

// A second crate declaring the same `crate::search`, so an unscoped exact
// lookup of `crate::search` is ambiguous.
const OTHER_TOML: &str = "[package]\nname = \"other\"\nversion = \"0.1.0\"\n";
const OTHER_LIB_RS: &str = "pub fn search(n: u32) -> u32 {\n    n\n}\n";

const TESTS_RS: &str = "mod misc;\nfn helper() {}\n";
const MISC_RS: &str = "fn check() {\n    crate::helper();\n}\n";

fn files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Cargo.toml", ROOT_TOML),
        ("crates/core/main.rs", MAIN_RS),
        ("crates/core/index/mod.rs", INDEX_MOD_RS),
        ("crates/core/index/enabled.rs", ENABLED_RS),
        ("crates/core/flags/mod.rs", FLAGS_MOD_RS),
        ("crates/core/flags/doc/mod.rs", DOC_MOD_RS),
        ("crates/core/flags/doc/help.rs", HELP_RS),
        ("crates/core/flags/doc/version.rs", VERSION_RS),
        ("crates/other/Cargo.toml", OTHER_TOML),
        ("crates/other/src/lib.rs", OTHER_LIB_RS),
        ("tests/tests.rs", TESTS_RS),
        ("tests/misc.rs", MISC_RS),
    ]
}

fn targets(source: &str) -> Vec<Option<String>> {
    let (_tmp, snap) = common::index_files(&files());
    snap.into_iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == source)
        .map(|e| e.target_qualname)
        .collect()
}

#[test]
fn bin_path_root_gets_crate_relative_qualnames() {
    let (_tmp, snap) = common::index_files(&files());
    let qualnames: std::collections::BTreeSet<_> =
        snap.iter().map(|e| e.source_qualname.clone()).collect();
    assert!(qualnames.contains("crate::main"), "{qualnames:#?}");
    assert!(qualnames.contains("crate::index::enabled::read"));
    assert!(
        !qualnames.iter().any(|q| q.contains("crates::core")),
        "{qualnames:#?}"
    );
}

#[test]
fn crate_path_call_resolves_to_root_fn_despite_other_crate_twin() {
    assert_eq!(
        targets("crate::index::enabled::read"),
        vec![Some("crate::search".to_string())]
    );
}

#[test]
fn mod_qualified_call_follows_nested_group_alias_reexport() {
    assert_eq!(
        targets("crate::main"),
        vec![Some(
            "crate::flags::doc::version::generate_short".to_string()
        )]
    );
}

#[test]
fn test_path_root_is_crate_root_of_its_modules() {
    assert_eq!(
        targets("crate::misc::check"),
        vec![Some("crate::helper".to_string())]
    );
}

#[test]
fn lib_path_root_is_crate_root() {
    let toml = "[package]\nname = \"x\"\n[lib]\npath = \"rust/entry.rs\"\n";
    let (_tmp, snap) = common::index_files(&[
        ("Cargo.toml", toml),
        (
            "rust/entry.rs",
            "mod util;\npub fn top() {\n    util::inner();\n}\n",
        ),
        ("rust/util.rs", "pub fn inner() {}\n"),
    ]);
    let qualnames: std::collections::BTreeSet<_> =
        snap.iter().map(|e| e.source_qualname.clone()).collect();
    assert!(qualnames.contains("crate::top"), "{qualnames:#?}");
    assert!(snap.iter().any(|e| e.kind == "CALLS"
        && e.source_qualname == "crate::top"
        && e.target_qualname.as_deref() == Some("crate::util::inner")));
}

#[test]
fn sibling_bin_roots_are_each_their_own_crate_root() {
    let toml = "[package]\nname = \"x\"\n[[bin]]\nname = \"a\"\npath = \"src/bin/a.rs\"\n\
[[bin]]\nname = \"b\"\npath = \"src/bin/b.rs\"\n";
    let (_tmp, snap) = common::index_files(&[
        ("Cargo.toml", toml),
        ("src/bin/a.rs", "fn main() {\n    ha();\n}\nfn ha() {}\n"),
        ("src/bin/b.rs", "fn main() {\n    hb();\n}\nfn hb() {}\n"),
    ]);
    let sources: Vec<_> = snap
        .iter()
        .filter(|e| e.kind == "CALLS")
        .map(|e| (e.source_qualname.clone(), e.target_qualname.clone()))
        .collect();
    assert!(
        sources.iter().all(|(s, _)| s == "crate::main"),
        "{sources:?}"
    );
    assert_eq!(sources.len(), 2, "{sources:?}");
}

#[test]
fn inline_table_lib_path_root_is_crate_root() {
    let toml = "lib = { name = \"xx\", path = \"rust/entry.rs\" }\n[package]\nname = \"x\"\n";
    let (_tmp, snap) = common::index_files(&[
        ("Cargo.toml", toml),
        (
            "rust/entry.rs",
            "mod util;\npub fn top() {\n    util::inner();\n}\n",
        ),
        ("rust/util.rs", "pub fn inner() {}\n"),
    ]);
    assert!(snap.iter().any(|e| e.kind == "CALLS"
        && e.source_qualname == "crate::top"
        && e.target_qualname.as_deref() == Some("crate::util::inner")));
}

#[test]
fn inline_array_bin_roots_are_each_their_own_crate_root() {
    let toml = "bin = [{ name = \"a\", path = \"src/bin/a.rs\" }, { name = \"b\", path = \"src/bin/b.rs\" }]\n[package]\nname = \"x\"\n";
    let (_tmp, snap) = common::index_files(&[
        ("Cargo.toml", toml),
        ("src/bin/a.rs", "fn main() {\n    ha();\n}\nfn ha() {}\n"),
        ("src/bin/b.rs", "fn main() {\n    hb();\n}\nfn hb() {}\n"),
    ]);
    let sources: Vec<_> = snap
        .iter()
        .filter(|e| e.kind == "CALLS")
        .map(|e| e.source_qualname.clone())
        .collect();
    assert_eq!(sources, ["crate::main", "crate::main"], "{sources:?}");
}
