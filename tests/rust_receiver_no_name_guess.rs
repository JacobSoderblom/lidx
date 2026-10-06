//! A Rust method call whose receiver type is unknown never binds by the
//! method's bare name (`n.as_str()` on a `String` is not `Category::as_str`);
//! the receiver inference must instead type the receivers that are knowable:
//! enum-variant constructors, `(*self)` in a blanket impl over a bound
//! generic, a facade-rooted builder chain, block-valued locals, map keys and
//! `to_string()`.

mod common;

const TOML: &str = "[package]\nname = \"app\"\nversion = \"0.1.0\"\n";

fn targets(files: &[(&str, &str)], source: &str) -> Vec<Option<String>> {
    let (_tmp, snap) = common::index_files(files);
    snap.into_iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == source)
        .map(|e| e.target_qualname)
        .collect()
}

fn count(files: &[(&str, &str)], source: &str, target: &str) -> usize {
    targets(files, source)
        .iter()
        .filter(|t| t.as_deref() == Some(target))
        .count()
}

fn base(extra: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
    let mut v = vec![("Cargo.toml", TOML)];
    v.extend_from_slice(extra);
    v
}

const CATEGORY: &str = "\
pub enum Category { A }\n\
impl Category {\n\
    pub fn as_str(&self) -> &'static str { \"a\" }\n\
}\n\
pub struct Stringy;\n\
impl Stringy {\n\
    pub fn as_str(&self) -> &str { \"s\" }\n\
}\n";

#[test]
fn unknown_receiver_never_binds_by_bare_name() {
    let files = base(&[
        ("src/lib.rs", "pub mod cat;\npub mod user;\n"),
        ("src/cat.rs", CATEGORY),
        (
            "src/user.rs",
            "pub fn param(x: ext::T) -> usize { x.as_str().len() }\n\
             pub fn untyped() -> usize { let y = ext::make(); y.as_str().len() }\n",
        ),
    ]);
    for src in ["crate::user::param", "crate::user::untyped"] {
        assert_eq!(count(&files, src, "crate::cat::Category::as_str"), 0);
        assert_eq!(count(&files, src, "crate::cat::Stringy::as_str"), 0);
    }
}

const ERR: &str = "\
pub enum Error { Io(u8), Other }\n\
impl Error {\n\
    pub fn with_depth(self, d: usize) -> Error { let _ = d; self }\n\
    pub fn with_path(self, p: &str) -> Error { let _ = p; self }\n\
}\n\
pub struct Decoy;\n\
impl Decoy {\n\
    pub fn with_depth(self, d: usize) -> Decoy { let _ = d; self }\n\
    pub fn with_path(self, p: &str) -> Decoy { let _ = p; self }\n\
}\n\
pub fn same_file(e: u8) -> Error {\n\
    Error::Io(e).with_depth(1).with_path(\"p\")\n\
}\n";

#[test]
fn enum_variant_constructor_receiver_binds_the_enum_methods() {
    let files = base(&[
        ("src/lib.rs", "pub mod err;\npub mod user;\n"),
        ("src/err.rs", ERR),
        (
            "src/user.rs",
            "use crate::err::Error;\n\
             pub fn imported(e: u8) -> Error {\n\
                 Error::Io(e).with_depth(1).with_path(\"p\")\n\
             }\n",
        ),
    ]);
    for src in ["crate::user::imported", "crate::err::same_file"] {
        assert_eq!(
            count(&files, src, "crate::err::Error::with_depth"),
            1,
            "{src}: {:?}",
            targets(&files, src)
        );
        assert_eq!(
            count(&files, src, "crate::err::Error::with_path"),
            1,
            "{src}: {:?}",
            targets(&files, src)
        );
        assert_eq!(count(&files, src, "crate::err::Decoy::with_depth"), 0);
    }
}

const MATCHER: &str = "\
pub trait Matcher {\n\
    fn find(&self) -> u8;\n\
    fn find_iter(&self) -> u8 { self.find() }\n\
}\n\
impl<'a, M: Matcher> Matcher for &'a M {\n\
    fn find(&self) -> u8 { (*self).find() }\n\
    fn find_iter(&self) -> u8 { (*self).find_iter() }\n\
}\n\
pub struct Decoy;\n\
impl Decoy {\n\
    pub fn find_iter(&self) -> u8 { 0 }\n\
}\n";

#[test]
fn deref_self_in_blanket_ref_impl_binds_the_bound_trait() {
    let files = base(&[("src/lib.rs", MATCHER)]);
    // `Self` here is `&M`; `(*self)` is the `M: Matcher`.
    let src = "crate::<impl Matcher for &'a M>::find_iter";
    let all: Vec<_> = {
        let (_tmp, snap) = common::index_files(&files);
        snap.into_iter()
            .filter(|e| e.kind == "CALLS")
            .map(|e| (e.source_qualname, e.target_qualname))
            .collect()
    };
    let _ = src;
    let hit = |t: &str| {
        all.iter()
            .filter(|(s, tt)| s.ends_with("find_iter") && tt.as_deref() == Some(t))
            .count()
    };
    assert_eq!(hit("crate::Matcher::find_iter"), 1, "{all:?}");
    assert_eq!(hit("crate::Decoy::find_iter"), 0, "{all:?}");
}

const FACADE_FILES: [(&str, &str); 6] = [
    ("Cargo.toml", TOML),
    (
        "crates/grep/Cargo.toml",
        "[package]\nname = \"grep\"\nversion = \"0.1.0\"\n",
    ),
    (
        "crates/grep/src/lib.rs",
        "pub extern crate grep_searcher as searcher;\n",
    ),
    (
        "crates/searcher/Cargo.toml",
        "[package]\nname = \"grep-searcher\"\nversion = \"0.1.0\"\n",
    ),
    (
        "crates/searcher/src/lib.rs",
        "mod inner;\npub use crate::inner::{Searcher, SearcherBuilder};\n",
    ),
    (
        "crates/searcher/src/inner.rs",
        "pub struct Searcher;\npub struct SearcherBuilder;\n\
         impl SearcherBuilder {\n\
             pub fn new() -> SearcherBuilder { SearcherBuilder }\n\
             pub fn line_number(&mut self, _b: bool) -> &mut SearcherBuilder { self }\n\
             pub fn build(&self) -> Searcher { Searcher }\n\
         }\n\
         impl Searcher {\n\
             pub fn search_path(&mut self) -> u8 { 1 }\n\
         }\n\
         pub struct Decoy;\n\
         impl Decoy { pub fn search_path(&mut self) -> u8 { 2 } }\n",
    ),
];

#[test]
fn facade_rooted_builder_chain_binds_each_link() {
    let mut files = FACADE_FILES.to_vec();
    files.push((
        "src/main.rs",
        "use grep::searcher::SearcherBuilder;\n\
         fn main() {\n\
             let mut s = SearcherBuilder::new().line_number(false).build();\n\
             s.search_path();\n\
         }\n",
    ));
    assert_eq!(
        count(&files, "crate::main", "crate::inner::Searcher::search_path"),
        1,
        "{:?}",
        targets(&files, "crate::main")
    );
    assert_eq!(
        count(&files, "crate::main", "crate::inner::Decoy::search_path"),
        0
    );
}

const STATS: &str = "\
pub struct Stats;\n\
impl Stats {\n\
    pub fn bytes(&self) -> u64 { 1 }\n\
}\n\
pub struct Decoy;\n\
impl Decoy {\n\
    pub fn bytes(&self) -> u64 { 2 }\n\
}\n\
pub struct Sink { s: Stats }\n\
impl Sink {\n\
    pub fn stats(&self) -> Option<&Stats> { Some(&self.s) }\n\
}\n\
pub struct Printer;\n\
pub struct PrinterBuilder;\n\
impl PrinterBuilder {\n\
    pub fn new() -> PrinterBuilder { PrinterBuilder }\n\
    pub fn stats(&mut self, _b: bool) -> &mut PrinterBuilder { self }\n\
    pub fn build(&self) -> Printer { Printer }\n\
}\n\
impl Printer {\n\
    pub fn sink(&mut self) -> Sink { Sink { s: Stats } }\n\
}\n";

#[test]
fn block_valued_local_takes_the_tail_expression_type() {
    let files = base(&[
        ("src/lib.rs", "pub mod st;\npub mod user;\n"),
        ("src/st.rs", STATS),
        (
            "src/user.rs",
            "use crate::st::PrinterBuilder;\n\
             pub fn f() -> u64 {\n\
                 let mut printer = PrinterBuilder::new().stats(true).build();\n\
                 let stats = {\n\
                     let mut sink = printer.sink();\n\
                     sink.stats().unwrap().clone()\n\
                 };\n\
                 stats.bytes()\n\
             }\n",
        ),
    ]);
    assert_eq!(
        count(&files, "crate::user::f", "crate::st::Stats::bytes"),
        1,
        "{:?}",
        targets(&files, "crate::user::f")
    );
    assert_eq!(
        count(&files, "crate::user::f", "crate::st::Decoy::bytes"),
        0
    );
}

const FLAGS: &str = "\
pub enum Category { A }\n\
impl Category {\n\
    pub fn as_str(&self) -> &'static str { \"a\" }\n\
}\n\
pub struct Decoy;\n\
impl Decoy {\n\
    pub fn as_str(&self) -> &'static str { \"d\" }\n\
}\n\
pub trait Flag {\n\
    fn doc_category(&self) -> Category;\n\
}\n\
pub struct F1;\n\
impl Flag for F1 {\n\
    fn doc_category(&self) -> Category { Category::A }\n\
}\n\
pub static FLAGS: &[&dyn Flag] = &[&F1];\n";

#[test]
fn map_key_type_comes_from_the_declaration_or_the_first_entry_call() {
    let files = base(&[
        ("src/lib.rs", "pub mod fl;\npub mod docs;\n"),
        ("src/fl.rs", FLAGS),
        (
            "src/docs.rs",
            "use std::collections::BTreeMap;\n\
             use crate::fl::{Category, FLAGS};\n\
             pub fn typed() -> usize {\n\
                 let mut cats: BTreeMap<Category, Vec<u8>> = BTreeMap::new();\n\
                 cats.insert(Category::A, vec![]);\n\
                 let mut n = 0;\n\
                 for (cat, _v) in cats.iter() {\n\
                     n += cat.as_str().len();\n\
                 }\n\
                 n\n\
             }\n\
             pub fn untyped() -> usize {\n\
                 let mut cats = BTreeMap::new();\n\
                 for flag in FLAGS.iter().copied() {\n\
                     cats.entry(flag.doc_category()).or_insert(String::new());\n\
                 }\n\
                 let mut n = 0;\n\
                 for (cat, _v) in cats.iter() {\n\
                     n += cat.as_str().len();\n\
                 }\n\
                 n\n\
             }\n",
        ),
    ]);
    for src in ["crate::docs::typed", "crate::docs::untyped"] {
        assert_eq!(
            count(&files, src, "crate::fl::Category::as_str"),
            1,
            "{src}: {:?}",
            targets(&files, src)
        );
        assert_eq!(count(&files, src, "crate::fl::Decoy::as_str"), 0);
    }
}

#[test]
fn enum_struct_variant_expression_receiver_binds_the_enum_methods() {
    let files = base(&[(
        "src/lib.rs",
        "pub enum Error { Loop { a: u8 }, Other }\n\
             impl Error {\n\
                 pub fn with_depth(self, d: usize) -> Error { let _ = d; self }\n\
             }\n\
             pub struct Decoy;\n\
             impl Decoy {\n\
                 pub fn with_depth(self, d: usize) -> Decoy { let _ = d; self }\n\
             }\n\
             pub fn f() -> Error {\n\
                 Error::Loop { a: 1 }.with_depth(1)\n\
             }\n",
    )]);
    assert_eq!(count(&files, "crate::f", "crate::Error::with_depth"), 1);
    assert_eq!(count(&files, "crate::f", "crate::Decoy::with_depth"), 0);
}

#[test]
fn unwrap_or_else_default_value_types_the_destructured_local() {
    let files = base(&[(
        "src/lib.rs",
        "pub struct M;\n\
         impl M {\n\
             pub fn none() -> M { M }\n\
             pub fn tag(self) -> M { self }\n\
         }\n\
         pub struct Decoy;\n\
         impl Decoy {\n\
             pub fn tag(self) -> Decoy { self }\n\
         }\n\
         fn lookup() -> Option<M> { None }\n\
         pub fn f() -> M {\n\
             let (mut m, ok) = lookup()\n\
                 .map(|m| (m, true))\n\
                 .unwrap_or_else(|| (M::none(), false));\n\
             if ok { m = m.tag(); }\n\
             m\n\
         }\n",
    )]);
    assert_eq!(
        count(&files, "crate::f", "crate::M::tag"),
        1,
        "{:?}",
        targets(&files, "crate::f")
    );
    assert_eq!(count(&files, "crate::f", "crate::Decoy::tag"), 0);
}
