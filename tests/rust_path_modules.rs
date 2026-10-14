//! Issue #497: `#[path = ".."] mod name;` is the module of the named file, so
//! a path through `name` (or an alias of it) reaches that file's items.

mod common;

use std::collections::BTreeSet;

fn targets(files: &[(&str, &str)], caller: &str) -> BTreeSet<String> {
    common::call_targets(files, caller).into_iter().collect()
}

#[test]
fn path_attribute_module_declared_in_the_file() {
    let files = [
        ("Cargo.toml", "[package]\nname = \"x\"\n"),
        (
            "src/lib.rs",
            "#[path = \"platform/unix_impl.rs\"]\nmod os;\npub fn go() -> u8 { os::setup() }\n",
        ),
        ("src/platform/unix_impl.rs", "pub fn setup() -> u8 { 1 }\n"),
        ("src/os.rs", "pub fn setup() -> u8 { 2 }\n"),
    ];
    let got = targets(&files, "crate::go");
    assert!(
        got.contains("crate::platform::unix_impl::setup"),
        "{got:#?}"
    );
    assert!(!got.contains("crate::os::setup"), "{got:#?}");
}

#[test]
fn path_attribute_module_in_a_cfg_if_branch_and_aliased() {
    let files = [
        ("Cargo.toml", "[package]\nname = \"x\"\n"),
        ("src/lib.rs", "mod imp;\npub fn go() -> u8 { imp::run() }\n"),
        (
            "src/imp.rs",
            "cfg_if::cfg_if! {\n    if #[cfg(unix)] {\n        #[path = \"unix.rs\"]\n        mod unix;\n        use unix as os;\n    } else {\n        compile_error!(\"unsupported\");\n    }\n}\n\
pub fn run() -> u8 { os::setup() }\n",
        ),
        ("src/unix.rs", "pub fn setup() -> u8 { 1 }\n"),
    ];
    let got = targets(&files, "crate::imp::run");
    assert!(got.contains("crate::unix::setup"), "{got:#?}");
}

#[test]
fn path_attribute_module_names_its_file_in_the_module_file_edge() {
    let files = [
        ("Cargo.toml", "[package]\nname = \"x\"\n"),
        (
            "src/lib.rs",
            "#[path = \"platform/unix_impl.rs\"]\nmod os;\n",
        ),
        ("src/platform/unix_impl.rs", "pub fn setup() -> u8 { 1 }\n"),
    ];
    let edges = common::edge_targets(&files, "crate", "MODULE_FILE");
    assert!(
        edges.contains(&"crate::platform::unix_impl".to_string()),
        "{edges:#?}"
    );
}
