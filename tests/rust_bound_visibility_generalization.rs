//! Associated-type bounds (`C::Out::blank()` with `type Out: Render;`) and
//! restricted visibility (`pub(super)`, `pub(in path)`, `use super::helper`)
//! on fixtures that share no names or shapes with the ripgrep ones: the
//! bound trait lives in another module (imported plainly, through
//! `use .. as Alias`, or from another workspace crate) and the bound is
//! written inline, in a `where` clause, or on an impl.

mod common;

const TOML: &str = "[package]\nname = \"app\"\nversion = \"0.1.0\"\n";

fn calls(files: &[(&str, &str)], source: &str) -> Vec<Option<String>> {
    let (_tmp, snap) = common::index_files(files);
    snap.into_iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == source)
        .map(|e| e.target_qualname)
        .collect()
}

fn has(files: &[(&str, &str)], source: &str, target: &str) -> bool {
    calls(files, source)
        .iter()
        .any(|t| t.as_deref() == Some(target))
}

const SHAPES: &str = "pub trait Render {\n    fn blank_slate() -> Self;\n}\n\
pub trait Canvas {\n    type Out: Render;\n}\n\
pub struct Dot;\npub struct Line;\n\
impl Render for Dot {\n    fn blank_slate() -> Self { Dot }\n}\n\
impl Render for Line {\n    fn blank_slate() -> Self { Line }\n}\n";
const DRAW: &str = "use crate::shapes::Canvas as Surface;\n\
use crate::shapes::{Canvas, Render as Paint};\n\
pub trait Ext {\n    type Tool: Paint;\n}\n\
pub fn plain<C: Canvas>() {\n    let _o = C::Out::blank_slate();\n}\n\
pub fn aliased<C: Surface>() {\n    let _o = C::Out::blank_slate();\n}\n\
pub fn in_where<C>()\nwhere\n    C: Surface,\n{\n    let _o = C::Out::blank_slate();\n}\n\
pub fn via_alias_decl<E: Ext>() {\n    let _o = E::Tool::blank_slate();\n}\n\
pub struct Holder<C> {\n    c: C,\n}\n\
impl<C> Holder<C>\nwhere\n    C: Surface,\n{\n    pub fn go(&self) {\n        let _o = C::Out::blank_slate();\n    }\n}\n\
pub fn unbound<C>() {\n    let _o = C::Out::blank_slate();\n}\n";

fn single_crate() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Cargo.toml", TOML),
        ("src/lib.rs", "pub mod shapes;\npub mod draw;\n"),
        ("src/shapes.rs", SHAPES),
        ("src/draw.rs", DRAW),
    ]
}

const TRAIT_DECL: &str = "crate::shapes::Render::blank_slate";

#[test]
fn assoc_bound_trait_from_another_module_binds_to_trait_decl() {
    let f = single_crate();
    for source in [
        "crate::draw::plain",
        "crate::draw::aliased",
        "crate::draw::in_where",
        "crate::draw::Holder::go",
    ] {
        assert!(
            has(&f, source, TRAIT_DECL),
            "{source}: {:?}",
            calls(&f, source)
        );
    }
}

#[test]
fn assoc_decl_bound_written_through_use_alias_binds_to_trait_decl() {
    let f = single_crate();
    let source = "crate::draw::via_alias_decl";
    assert!(has(&f, source, TRAIT_DECL), "{:?}", calls(&f, source));
}

#[test]
fn assoc_path_without_a_bound_binds_nothing() {
    let f = single_crate();
    let t = calls(&f, "crate::draw::unbound");
    assert!(
        t.iter()
            .all(|t| t.as_deref().is_none_or(|t| !t.contains("blank_slate"))),
        "{t:?}"
    );
}

const KIT_TOML: &str = "[package]\nname = \"kit\"\nversion = \"0.1.0\"\n";
const KIT_LIB: &str = "pub trait Render {\n    fn blank_slate() -> Self;\n}\n\
pub trait Canvas {\n    type Out: Render;\n}\n\
pub struct Dot;\nimpl Render for Dot {\n    fn blank_slate() -> Self { Dot }\n}\n";
const APP_LIB: &str = "use kit::Canvas;\n\
pub fn from_kit<C: Canvas>() {\n    let _o = C::Out::blank_slate();\n}\n\
pub struct Mine;\nimpl Mine {\n    pub fn blank_slate() -> Self { Mine }\n}\n";

#[test]
fn assoc_bound_trait_from_another_workspace_crate_binds_to_trait_decl() {
    let f = [
        ("Cargo.toml", TOML),
        ("src/lib.rs", APP_LIB),
        ("crates/kit/Cargo.toml", KIT_TOML),
        ("crates/kit/src/lib.rs", KIT_LIB),
    ];
    let t = calls(&f, "crate::from_kit");
    assert!(
        t.iter()
            .any(|t| t.as_deref() == Some("crate::Render::blank_slate")),
        "{t:?}"
    );
    assert!(
        !t.iter()
            .any(|t| t.as_deref() == Some("crate::Mine::blank_slate")),
        "{t:?}"
    );
}

const VIS_LIB: &str = "pub mod a;\npub mod z;\n";
const VIS_A: &str = "pub mod b;\nuse crate::a::b;\n\
pub fn from_parent() {\n    b::sup();\n    b::inside();\n}\n\
pub fn method_from_parent() {\n    let _f = |x| {\n        x.sup_m();\n        x.inside_m();\n    };\n}\n";
const VIS_B: &str = "pub(super) fn sup() {}\n\
pub(in crate::a) fn inside() {}\n\
fn helper() {}\n\
pub struct S;\n\
impl S {\n    pub(super) fn sup_m(&self) {}\n    pub(in crate::a) fn inside_m(&self) {}\n}\n\
mod kid {\n    use super::helper;\n    pub fn run() {\n        helper();\n    }\n}\n";
const VIS_Z: &str = "use crate::a::b;\n\
pub fn cousin() {\n    b::sup();\n    b::inside();\n}\n\
pub fn method_from_cousin() {\n    let _f = |x| {\n        x.sup_m();\n        x.inside_m();\n    };\n}\n";

fn vis_files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Cargo.toml", TOML),
        ("src/lib.rs", VIS_LIB),
        ("src/a.rs", VIS_A),
        ("src/a/b.rs", VIS_B),
        ("src/z.rs", VIS_Z),
    ]
}

#[test]
fn restricted_items_resolve_from_inside_their_scope() {
    let f = vis_files();
    for target in ["crate::a::b::sup", "crate::a::b::inside"] {
        assert!(has(&f, "crate::a::from_parent", target), "{target}");
    }
    for target in ["crate::a::b::S::sup_m", "crate::a::b::S::inside_m"] {
        assert!(
            has(&f, "crate::a::method_from_parent", target),
            "{target}: {:?}",
            calls(&f, "crate::a::method_from_parent")
        );
    }
}

#[test]
fn restricted_methods_stay_unresolved_from_outside_their_scope() {
    let f = vis_files();
    let t = calls(&f, "crate::z::method_from_cousin");
    assert!(
        t.iter()
            .all(|t| t.as_deref().is_none_or(|t| !t.contains("::S::"))),
        "{t:?}"
    );
}

#[test]
fn private_fn_reached_through_use_super_from_child_module_resolves() {
    let f = vis_files();
    assert!(
        has(&f, "crate::a::b::kid::run", "crate::a::b::helper"),
        "{:?}",
        calls(&f, "crate::a::b::kid::run")
    );
}
