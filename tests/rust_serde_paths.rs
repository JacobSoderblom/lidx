//! Issue #497: a serde attribute names a function by a string path
//! (`deserialize_with = "super::f"`), read relative to the module the
//! attribute is written in, through the re-exports of the modules on the way.

mod common;

#[test]
fn super_path_reaches_a_function_a_parent_re_exports() {
    let files = [
        ("Cargo.toml", "[package]\nname = \"x\"\n"),
        ("src/lib.rs", "pub mod config;\n"),
        (
            "src/config/mod.rs",
            "mod leak_timeout;\nmod overrides;\npub use leak_timeout::*;\npub use overrides::*;\n",
        ),
        (
            "src/config/leak_timeout.rs",
            "pub(super) fn deserialize_leak_timeout() {}\n",
        ),
        (
            "src/config/overrides.rs",
            "pub struct S {\n    #[serde(default, deserialize_with = \"super::deserialize_leak_timeout\")]\n    pub x: u8,\n}\n",
        ),
    ];
    let targets = common::edge_targets(&files, "crate::config::overrides", "REFERENCES");
    assert!(
        targets.contains(&"crate::config::leak_timeout::deserialize_leak_timeout".to_string()),
        "{targets:#?}"
    );
}
