//! Issue #497: a name that a repo module re-exports from a dependency
//! (`use uom::si; pub use si::f64::Time;`) is the dependency's item wherever it
//! is imported from the module, in a function body or at the top of a file: a
//! call written against it is recorded as an external stub, like one written
//! against the dependency itself, not dropped as unresolved.

mod common;

const MANIFEST: &str = "[package]\nname = \"x\"\n[dependencies]\nuom = \"0.36\"\n";
const QUANTITY: &str = "use uom::si;\npub use si::f64::Time;\npub use si::time::second;\n";
const OWN: &str = "pub struct Mine;\nimpl Mine {\n    pub fn new() -> Mine { Mine }\n}\n";

fn targets(user: &str, caller: &str) -> Vec<String> {
    let lib = format!("pub mod quantity;\npub mod user;\n{OWN}");
    common::call_targets(
        &[
            ("Cargo.toml", MANIFEST),
            ("src/lib.rs", &lib),
            ("src/quantity.rs", QUANTITY),
            ("src/user.rs", user),
        ],
        caller,
    )
}

#[test]
fn function_local_use_of_a_re_exported_dependency_item() {
    let got = targets(
        "pub fn go() {\n    use crate::quantity::{second, Time};\n    let _t = Time::new::<second>(7.0);\n}\n",
        "crate::user::go",
    );
    assert_eq!(got, ["ext:Time::new"], "{got:#?}");
}

#[test]
fn file_level_use_of_a_re_exported_dependency_item() {
    let got = targets(
        "use crate::quantity::{second, Time};\npub fn go() {\n    let _t = Time::new::<second>(7.0);\n}\n",
        "crate::user::go",
    );
    assert_eq!(got, ["ext:Time::new"], "{got:#?}");
}

#[test]
fn a_repo_type_of_the_same_name_still_binds() {
    let got = targets(
        "pub fn go() {\n    use crate::Mine;\n    let _t = Mine::new();\n}\n",
        "crate::user::go",
    );
    assert_eq!(got, ["crate::Mine::new"], "{got:#?}");
}
