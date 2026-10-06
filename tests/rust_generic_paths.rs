//! Paths through a generic parameter (`S::Error::error_message`, `T::make`,
//! `x.hi()` with `x: T`) bind to the bound trait's method declaration, never
//! to an impl of it; a concrete `io::Error::error_message` binds to the impl;
//! functions passed as values (`.map_err(f)`) are recorded as calls.

mod common;

const TOML: &str = "[package]\nname = \"app\"\nversion = \"0.1.0\"\n";
const LIB: &str = "pub mod sink;\npub mod core;\npub mod util;\n";
const SINK: &str = "use std::io;\n\
pub trait SinkError: Sized {\n\
    fn error_message<T: std::fmt::Display>(message: T) -> Self;\n\
    fn error_io(err: u8) -> Self {\n\
        Self::error_message(err)\n\
    }\n\
}\n\
impl SinkError for io::Error {\n\
    fn error_message<T: std::fmt::Display>(message: T) -> io::Error {\n\
        io::Error::new(io::ErrorKind::Other, message.to_string())\n\
    }\n\
}\n\
impl SinkError for Box<dyn std::error::Error> {\n\
    fn error_message<T: std::fmt::Display>(message: T) -> Self {\n\
        Box::from(message.to_string())\n\
    }\n\
}\n\
pub trait Sink {\n\
    type Error: SinkError;\n\
    fn matched(&mut self) -> Result<bool, Self::Error>;\n\
}\n\
pub trait Greet {\n\
    fn hi(&self);\n\
    fn make() -> Self;\n\
}\n\
pub struct A;\n\
impl Greet for A {\n\
    fn hi(&self) {}\n\
    fn make() -> Self { A }\n\
}\n\
pub struct B;\n\
impl Greet for B {\n\
    fn hi(&self) {}\n\
    fn make() -> Self { B }\n\
}\n";
const CORE: &str = "use crate::sink::{A, Greet, Sink, SinkError};\n\
pub struct Core<S> {\n    sink: S,\n}\n\
impl<S: Sink> Core<S> {\n\
    pub fn direct(&mut self, e: u8) -> Result<(), S::Error> {\n\
        Err(S::Error::error_message(e))\n\
    }\n\
    pub fn by_ref(&mut self, r: Result<(), u8>) -> Result<(), S::Error> {\n\
        r.map_err(S::Error::error_message)\n\
    }\n\
}\n\
pub struct W<G> {\n    g: G,\n}\n\
impl<G: Greet> W<G> {\n\
    pub fn run(&self, g: G) {\n\
        g.hi();\n\
    }\n\
}\n\
pub fn bound<T: Greet>(x: T) {\n\
    x.hi();\n\
    let _y = T::make();\n\
}\n\
pub fn wherey<T>(x: T)\nwhere\n    T: Greet,\n{\n\
    x.hi();\n\
}\n\
pub fn where_assoc<S>(r: Result<(), u8>) -> Result<(), S::Error>\n\
where\n    S: Sink,\n    S::Error: SinkError,\n{\n\
    r.map_err(S::Error::error_message)\n\
}\n\
pub fn rebound<T: Greet>(x: T) {\n\
    let x = A;\n\
    x.hi();\n\
}\n\
pub fn unbound<T>(x: T) {\n\
    x.hi();\n\
    let _y = T::make();\n\
}\n";
const UTIL: &str = "use std::io;\n\
use crate::sink::SinkError;\n\
pub fn concrete(r: Result<(), u8>) -> io::Result<()> {\n\
    r.map_err(io::Error::error_message)\n\
}\n\
pub fn concrete_call() -> io::Error {\n\
    io::Error::error_message(1)\n\
}\n\
pub fn local_fn(r: Result<(), u8>) -> Result<(), String> {\n\
    r.map_err(convert)\n\
}\n\
pub fn convert(e: u8) -> String {\n\
    e.to_string()\n\
}\n\
pub fn passes_local(r: Result<(), u8>) {\n\
    let convert2 = 1;\n\
    let _ = r.map(|v| v + convert2);\n\
    let _ = std::mem::drop(convert2);\n\
}\n\
pub fn noise(r: Result<(), u8>) {\n\
    let _ = std::sync::atomic::Ordering::SeqCst;\n\
    let _ = r.map_err(Wrapper);\n\
    let _ = Some(None::<u8>);\n\
}\n\
pub struct Wrapper(u8);\n";

fn files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Cargo.toml", TOML),
        ("src/lib.rs", LIB),
        ("src/sink.rs", SINK),
        ("src/core.rs", CORE),
        ("src/util.rs", UTIL),
    ]
}

fn calls(source: &str) -> Vec<(Option<String>, Option<String>)> {
    let (_tmp, snap) = common::index_files(&files());
    snap.into_iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == source)
        .map(|e| (e.target_qualname, e.resolution_kind))
        .collect()
}

fn has(source: &str, target: &str) -> bool {
    calls(source)
        .iter()
        .any(|(t, _)| t.as_deref() == Some(target))
}

const TRAIT_EM: &str = "crate::sink::SinkError::error_message";
const IMPL_EM: &str = "io::Error::error_message";

#[test]
fn assoc_type_path_binds_to_trait_decl() {
    let c = calls("crate::core::Core::direct");
    assert!(
        c.iter().any(|(t, _)| t.as_deref() == Some(TRAIT_EM)),
        "{c:?}"
    );
    assert!(
        c.iter().all(|(t, _)| t.as_deref() != Some(IMPL_EM)),
        "{c:?}"
    );
}

#[test]
fn assoc_type_path_as_value_is_a_call_to_trait_decl() {
    assert!(
        has("crate::core::Core::by_ref", TRAIT_EM),
        "{:?}",
        calls("crate::core::Core::by_ref")
    );
}

#[test]
fn self_path_in_trait_default_body_binds_to_trait_decl() {
    assert!(has("crate::sink::SinkError::error_io", TRAIT_EM));
}

#[test]
fn concrete_path_as_value_is_a_call_to_the_impl() {
    let c = calls("crate::util::concrete");
    assert!(
        c.iter().any(|(t, _)| t.as_deref() == Some(IMPL_EM)),
        "{c:?}"
    );
    assert!(
        c.iter().all(|(t, _)| t.as_deref() != Some(TRAIT_EM)),
        "{c:?}"
    );
}

#[test]
fn concrete_direct_call_still_binds_to_the_impl() {
    assert!(has("crate::util::concrete_call", IMPL_EM));
}

#[test]
fn generic_receiver_method_binds_to_trait_decl() {
    for src in [
        "crate::core::bound",
        "crate::core::wherey",
        "crate::core::W::run",
    ] {
        assert!(
            has(src, "crate::sink::Greet::hi"),
            "{src}: {:?}",
            calls(src)
        );
        assert!(!has(src, "crate::sink::A::hi") && !has(src, "crate::sink::B::hi"));
    }
}

#[test]
fn generic_path_call_binds_to_trait_decl() {
    assert!(
        has("crate::core::bound", "crate::sink::Greet::make"),
        "{:?}",
        calls("crate::core::bound")
    );
}

#[test]
fn unbound_generic_stays_unresolved() {
    for (target, _) in calls("crate::core::unbound") {
        assert!(
            target.as_deref().is_none_or(|t| !t.contains("Greet")
                && !t.ends_with("::A::hi")
                && !t.ends_with("::B::hi")
                && !t.ends_with("::make")),
            "{target:?}"
        );
    }
}

#[test]
fn non_function_value_paths_are_not_calls() {
    let c = calls("crate::util::noise");
    assert!(
        c.iter().all(|(t, _)| t
            .as_deref()
            .is_none_or(|t| !t.contains("SeqCst") && !t.contains("Wrapper") && t != "None")),
        "{c:?}"
    );
}

#[test]
fn where_clause_bound_on_assoc_type_binds_to_trait_decl() {
    assert!(
        has("crate::core::where_assoc", TRAIT_EM),
        "{:?}",
        calls("crate::core::where_assoc")
    );
}

#[test]
fn rebound_parameter_name_does_not_type_the_receiver() {
    assert!(
        !has("crate::core::rebound", "crate::sink::Greet::hi"),
        "{:?}",
        calls("crate::core::rebound")
    );
}

#[test]
fn bare_function_name_passed_as_value_is_a_call() {
    assert!(
        has("crate::util::local_fn", "crate::util::convert"),
        "{:?}",
        calls("crate::util::local_fn")
    );
}

#[test]
fn local_variable_passed_as_argument_is_not_a_call() {
    let c = calls("crate::util::passes_local");
    assert!(
        c.iter()
            .all(|(t, _)| t.as_deref().is_none_or(|t| !t.contains("convert2"))),
        "{c:?}"
    );
}
