//! Issue #497: a chain of method calls rooted at a function written by its full
//! path from a crate outside the repository (`std::env::vars().init()`,
//! `tracing_subscriber::registry().with(l).init()`) is a chain on that crate's
//! values: a method of the same name in the repository is not what it calls.

mod common;

use std::collections::BTreeSet;

fn targets(files: &[(&str, &str)], caller: &str) -> BTreeSet<String> {
    common::call_targets(files, caller).into_iter().collect()
}

const MANIFEST: &str = "[package]\nname = \"x\"\n[dependencies]\ntracing-subscriber = \"0.3\"\n";

const OWN: &str = "pub struct Opts;\nimpl Opts {\n    pub fn init(&self) {}\n}\n";

#[test]
fn full_path_std_call_chain_stays_external() {
    let src = format!("{OWN}pub fn go() {{\n    std::env::vars().init();\n}}\n");
    let got = targets(
        &[("Cargo.toml", MANIFEST), ("src/lib.rs", &src)],
        "crate::go",
    );
    assert!(!got.contains("crate::Opts::init"), "{got:#?}");
}

#[test]
fn full_path_dependency_call_chain_stays_external() {
    let src = format!(
        "{OWN}pub fn go(layer: u8) {{\n    tracing_subscriber::registry().with(layer).init();\n}}\n"
    );
    let got = targets(
        &[("Cargo.toml", MANIFEST), ("src/lib.rs", &src)],
        "crate::go",
    );
    assert!(!got.contains("crate::Opts::init"), "{got:#?}");
}

#[test]
fn repo_module_path_chain_still_binds() {
    let src = format!(
        "{OWN}pub mod build {{\n    pub fn make() -> super::Opts {{ super::Opts }}\n}}\npub fn go() {{\n    crate::build::make().init();\n}}\n"
    );
    let got = targets(
        &[("Cargo.toml", MANIFEST), ("src/lib.rs", &src)],
        "crate::go",
    );
    assert!(got.contains("crate::Opts::init"), "{got:#?}");
}
