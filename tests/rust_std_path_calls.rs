//! Issue #454: a path call whose first segment is read through a `use` into
//! std (`use std::io;` ... `io::Error::new(..)`) leaves the crate, so it
//! never binds a repo type that shares the type's name (a crate's own
//! `Error::new`). Paths into the repo still bind.

mod common;

use common::call_targets;

const MANIFEST: (&str, &str) = ("Cargo.toml", "[package]\nname = \"g\"\n");
const LIB: (&str, &str) = ("src/lib.rs", "pub mod error;\npub mod user;\n");
const ERROR: (&str, &str) = (
    "src/error.rs",
    "pub struct Error;\nimpl Error {\n    pub fn new(_m: &str) -> Error { Error }\n}\n\
     pub struct Formatter;\nimpl Formatter {\n    pub fn new() -> Formatter { Formatter }\n}\n",
);

fn targets(user: &'static str, caller: &str) -> Vec<String> {
    call_targets(&[MANIFEST, LIB, ERROR, ("src/user.rs", user)], caller)
}

fn binds_repo(t: &[String]) -> bool {
    t.iter().any(|q| q.starts_with("crate::error::"))
}

#[test]
fn module_imported_from_std_never_binds_a_repo_type() {
    let t = targets(
        "use std::io;\npub fn a() { let _ = io::Error::new(io::ErrorKind::Other, \"x\"); }\n",
        "crate::user::a",
    );
    assert!(!binds_repo(&t), "{t:?}");
}

#[test]
fn type_imported_from_std_never_binds_a_repo_type() {
    let t = targets(
        "use std::io::Error;\npub fn a() { let _ = Error::new(std::io::ErrorKind::Other, \"x\"); }\n",
        "crate::user::a",
    );
    assert!(!binds_repo(&t), "{t:?}");
}

#[test]
fn core_and_alloc_roots_leave_the_crate_too() {
    let t = targets(
        "use core::fmt;\nuse alloc::string as astr;\n\
         pub fn a() { let _ = fmt::Formatter::new(); let _ = astr::String::new(); }\n",
        "crate::user::a",
    );
    assert!(!binds_repo(&t), "{t:?}");
}

#[test]
fn fully_qualified_std_path_never_binds_a_repo_type() {
    let t = targets(
        "pub fn a() { let _ = std::io::Error::new(std::io::ErrorKind::Other, \"x\"); }\n",
        "crate::user::a",
    );
    assert!(!binds_repo(&t), "{t:?}");
}

#[test]
fn repo_paths_still_bind() {
    let t = targets(
        "use crate::error;\nuse crate::error::Formatter;\n\
         pub fn a() { let _ = error::Error::new(\"x\"); }\n\
         pub fn b() { let _ = crate::error::Error::new(\"x\"); }\n\
         pub fn c() { let _ = Formatter::new(); }\n",
        "crate::user::a",
    );
    assert_eq!(t, ["crate::error::Error::new"], "{t:?}");
    let t = targets(
        "use crate::error::Formatter;\npub fn c() { let _ = Formatter::new(); }\n",
        "crate::user::c",
    );
    assert_eq!(t, ["crate::error::Formatter::new"], "{t:?}");
}

#[test]
fn a_local_module_named_like_a_std_one_is_not_std() {
    // `mod io;` declared in the repo: `io::Error::new` is the repo's.
    let files = [
        MANIFEST,
        ("src/lib.rs", "pub mod io;\npub mod user;\n"),
        (
            "src/io.rs",
            "pub struct Error;\nimpl Error {\n    pub fn new(_m: &str) -> Error { Error }\n}\n",
        ),
        (
            "src/user.rs",
            "use crate::io;\npub fn a() { let _ = io::Error::new(\"x\"); }\n",
        ),
    ];
    let t = call_targets(&files, "crate::user::a");
    assert_eq!(t, ["crate::io::Error::new"], "{t:?}");
}
