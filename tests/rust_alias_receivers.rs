//! Issue #497: a value typed through a type alias (`type CliResult<T> =
//! Result<T, E>`, declared in another file), a guard (`MutexGuard<T>`) or a
//! `Deref` wrapper is a value of the type behind it, so a method called on it
//! binds that type's method and not a same-named method of another type.

mod common;

use std::collections::BTreeSet;

fn targets(files: &[(&str, &str)], caller: &str) -> BTreeSet<String> {
    common::call_targets(files, caller).into_iter().collect()
}

const TYPES: &str = "pub struct Entry { p: u32 }\nimpl Entry {\n    pub fn path(&self) -> u32 { self.p }\n}\n\
pub struct Other { p: u32 }\nimpl Other {\n    pub fn path(&self) -> u32 { self.p }\n}\n";

fn binds_entry(files: &[(&str, &str)], caller: &str) {
    let got = targets(files, caller);
    assert!(got.contains("crate::Entry::path"), "{caller}: {got:#?}");
    assert!(!got.contains("crate::Other::path"), "{caller}: {got:#?}");
}

fn crate_of(lib: &str, other: &'static str) -> Vec<(&'static str, String)> {
    vec![
        ("Cargo.toml", "[package]\nname = \"x\"\n".to_string()),
        ("src/lib.rs", lib.to_string()),
        ("src/other.rs", other.to_string()),
    ]
}

fn run(files: &[(&str, String)], caller: &str) {
    let borrowed: Vec<(&str, &str)> = files.iter().map(|(p, s)| (*p, s.as_str())).collect();
    binds_entry(&borrowed, caller);
}

#[test]
fn alias_declared_in_another_file_is_unwrapped_by_try() {
    let lib = format!("mod other;\npub type CliResult<T> = Result<T, u8>;\n{TYPES}");
    let other = "use crate::{CliResult, Entry};\n\
pub fn fetch() -> CliResult<Entry> { Ok(Entry { p: 1 }) }\n\
pub fn go() -> CliResult<u32> {\n    let ent = fetch()?;\n    Ok(ent.path())\n}\n";
    run(&crate_of(&lib, other), "crate::other::go");
}

#[test]
fn alias_reached_through_a_glob_import() {
    let lib = format!("mod other;\npub type Res<T = ()> = Result<T, u8>;\n{TYPES}");
    let other = "use super::*;\n\
pub fn fetch() -> Res<Entry> { Ok(Entry { p: 1 }) }\n\
pub fn go() -> Res<u32> {\n    let ent = fetch()?;\n    Ok(ent.path())\n}\n";
    run(&crate_of(&lib, other), "crate::other::go");
}

#[test]
fn method_returning_an_alias_is_unwrapped_by_try() {
    let lib = format!("mod other;\npub type CliResult<T> = Result<T, u8>;\n{TYPES}");
    let other = "use crate::{CliResult, Entry};\n\
pub struct Args;\nimpl Args {\n    fn make(&self) -> CliResult<Entry> { Ok(Entry { p: 1 }) }\n}\n\
pub fn go(args: Args) -> CliResult<u32> {\n    let ent = args.make()?;\n    Ok(ent.path())\n}\n";
    run(&crate_of(&lib, other), "crate::other::go");
}

#[test]
fn alias_of_an_alias_and_an_alias_without_parameters() {
    let lib = format!(
        "mod other;\npub type Inner<T> = Result<T, u8>;\npub type Outer<T> = Inner<T>;\npub type EntryResult = Result<Entry, u8>;\n{TYPES}"
    );
    let other = "use crate::{EntryResult, Outer, Entry};\n\
pub fn fetch() -> Outer<Entry> { Ok(Entry { p: 1 }) }\n\
pub fn bare() -> EntryResult { Ok(Entry { p: 1 }) }\n\
pub fn go() -> Outer<u32> {\n    let ent = fetch()?;\n    Ok(ent.path())\n}\n\
pub fn go_bare() -> Outer<u32> {\n    let ent = bare()?;\n    Ok(ent.path())\n}\n";
    let files = crate_of(&lib, other);
    run(&files, "crate::other::go_bare");
    run(&files, "crate::other::go");
}

#[test]
fn a_generic_struct_is_not_mistaken_for_an_alias() {
    let lib = format!("mod other;\n{TYPES}pub struct Wrap<T>(pub T);\n");
    let other = "use crate::{Entry, Wrap};\n\
pub fn fetch() -> Wrap<Entry> { Wrap(Entry { p: 1 }) }\n\
pub fn go() -> u32 {\n    let w = fetch();\n    w.0.path()\n}\n";
    // `Wrap` is no alias: the value stays a `Wrap`, whose field is untyped
    // here; the call must not bind the wrong `path` either way.
    let got = targets(
        &crate_of(&lib, other)
            .iter()
            .map(|(p, s)| (*p, s.as_str()))
            .collect::<Vec<_>>(),
        "crate::other::go",
    );
    assert!(!got.contains("crate::Other::path"), "{got:#?}");
}

#[test]
fn mutex_guard_derefs_to_the_protected_value() {
    let lib = format!("mod other;\n{TYPES}");
    let other = "use std::sync::{Mutex, MutexGuard};\nuse crate::Entry;\n\
static SLOT: Mutex<Entry> = Mutex::new(Entry { p: 0 });\n\
pub struct Holder;\nimpl Holder {\n    fn grab() -> MutexGuard<'static, Entry> { SLOT.lock().unwrap() }\n\
    pub fn go() -> u32 {\n        let guard = Self::grab();\n        guard.path()\n    }\n}\n";
    run(&crate_of(&lib, other), "crate::other::Holder::go");
}

#[test]
fn deref_target_declared_in_the_repo() {
    let lib = format!(
        "mod other;\n{TYPES}pub struct Wrapper {{ e: Entry }}\nimpl std::ops::Deref for Wrapper {{\n    type Target = Entry;\n    fn deref(&self) -> &Entry {{ &self.e }}\n}}\n"
    );
    let other = "use crate::Wrapper;\n\
pub fn go(w: Wrapper) -> u32 {\n    w.path()\n}\n";
    run(&crate_of(&lib, other), "crate::other::go");
}

#[test]
fn deref_target_is_not_used_when_the_wrapper_declares_the_method() {
    let lib = format!(
        "mod other;\n{TYPES}pub struct Wrapper {{ e: Entry }}\nimpl Wrapper {{\n    pub fn path(&self) -> u32 {{ 9 }}\n}}\nimpl std::ops::Deref for Wrapper {{\n    type Target = Entry;\n    fn deref(&self) -> &Entry {{ &self.e }}\n}}\n"
    );
    let other = "use crate::Wrapper;\n\
pub fn go(w: Wrapper) -> u32 {\n    w.path()\n}\n";
    let files = crate_of(&lib, other);
    let borrowed: Vec<(&str, &str)> = files.iter().map(|(p, s)| (*p, s.as_str())).collect();
    let got = targets(&borrowed, "crate::other::go");
    assert!(got.contains("crate::Wrapper::path"), "{got:#?}");
    assert!(!got.contains("crate::Entry::path"), "{got:#?}");
}

#[test]
fn alias_of_a_named_type_binds_the_aliased_types_methods() {
    let lib = format!("mod other;\npub type Range = Entry;\n{TYPES}");
    let other = "use crate::Range;\n\
pub fn make() -> Range { crate::Entry { p: 1 } }\n\
pub fn go() -> u32 {\n    let r: Range = make();\n    r.path()\n}\n";
    run(&crate_of(&lib, other), "crate::other::go");
}
