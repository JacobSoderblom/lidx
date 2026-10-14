//! Issue #489: a method call on a value that came out of an enum-typed
//! field, a `Result`/`Option` (`?`, `match Ok(x)`/`Some(x)`, `if let`,
//! `while let`), an iterator (`for`, a user `impl Iterator`, a channel) or a
//! `ref`/`ref mut` pattern binds the method of the concrete repo type, not
//! nothing and not a same-named method of another type.

mod common;

use std::collections::BTreeSet;

fn targets(files: &[(&str, &str)], caller: &str) -> BTreeSet<String> {
    common::call_targets(files, caller).into_iter().collect()
}

fn lib(src: &str) -> Vec<(&str, &str)> {
    vec![
        ("Cargo.toml", "[package]\nname = \"x\"\n"),
        ("src/lib.rs", src),
    ]
}

/// Two types with a `path` method, so only typing picks the right one.
const TYPES: &str = "pub struct Entry { p: u32 }\nimpl Entry {\n    pub fn path(&self) -> u32 { self.p }\n}\n\
pub struct Other { p: u32 }\nimpl Other {\n    pub fn path(&self) -> u32 { self.p }\n}\n";

fn binds_entry(body: &str, caller: &str) {
    let src = format!("{TYPES}{body}");
    let got = targets(&lib(&src), caller);
    assert!(got.contains("crate::Entry::path"), "{caller}: {got:#?}");
    assert!(!got.contains("crate::Other::path"), "{caller}: {got:#?}");
}

#[test]
fn enum_typed_field_receiver() {
    let src = format!(
        "{TYPES}pub enum Inner {{ A(Entry), B }}\nimpl Inner {{\n    pub fn path(&self) -> u32 {{ 0 }}\n}}\n\
pub struct Holder {{ dent: Inner }}\nimpl Holder {{\n    pub fn go(&self) -> u32 {{\n        self.dent.path()\n    }}\n}}\n"
    );
    let got = targets(&lib(&src), "crate::Holder::go");
    assert!(got.contains("crate::Inner::path"), "{got:#?}");
    assert!(!got.contains("crate::Entry::path"), "{got:#?}");
    assert!(!got.contains("crate::Other::path"), "{got:#?}");
}

#[test]
fn variant_ref_pattern_through_a_glob_import() {
    binds_entry(
        "pub enum Inner { Raw(Entry), Gone }\nimpl Inner {\n    pub fn path(&self) -> u32 {\n        use self::Inner::*;\n        match *self {\n            Gone => 0,\n            Raw(ref x) => x.path(),\n        }\n    }\n}\n",
        "crate::Inner::path",
    );
}

#[test]
fn variant_ref_mut_pattern_through_a_glob_import() {
    binds_entry(
        "pub enum Inner { Raw(Entry), Gone }\nimpl Inner {\n    pub fn poke(&mut self) -> u32 {\n        use self::Inner::*;\n        match *self {\n            Gone => 0,\n            Raw(ref mut x) => x.path(),\n        }\n    }\n}\n",
        "crate::Inner::poke",
    );
}

#[test]
fn variant_pattern_on_a_self_reference() {
    binds_entry(
        "pub enum Inner { Raw(Entry), Gone }\nimpl Inner {\n    pub fn poke(&self) -> u32 {\n        match self {\n            Inner::Raw(x) => x.path(),\n            Inner::Gone => 0,\n        }\n    }\n}\n",
        "crate::Inner::poke",
    );
}

#[test]
fn try_operator_on_a_result_returning_call() {
    binds_entry(
        "pub fn fetch() -> Result<Entry, u8> { Ok(Entry { p: 1 }) }\npub fn go() -> Result<u32, u8> {\n    let ent = fetch()?;\n    Ok(ent.path())\n}\n",
        "crate::go",
    );
}

#[test]
fn match_ok_arm() {
    binds_entry(
        "pub fn fetch() -> Result<Entry, u8> { Ok(Entry { p: 1 }) }\npub fn go() -> u32 {\n    match fetch() {\n        Ok(ent) => ent.path(),\n        Err(_) => 0,\n    }\n}\n",
        "crate::go",
    );
}

#[test]
fn match_some_arm_with_ref() {
    binds_entry(
        "pub fn fetch() -> Option<Entry> { None }\npub fn go() -> u32 {\n    match fetch() {\n        Some(ref ent) => ent.path(),\n        None => 0,\n    }\n}\n",
        "crate::go",
    );
}

#[test]
fn if_let_some() {
    binds_entry(
        "pub fn fetch() -> Option<Entry> { None }\npub fn go() -> u32 {\n    if let Some(ent) = fetch() {\n        return ent.path();\n    }\n    0\n}\n",
        "crate::go",
    );
}

#[test]
fn if_let_ok_with_ref_mut() {
    binds_entry(
        "pub fn fetch() -> Result<Entry, u8> { Ok(Entry { p: 1 }) }\npub fn go() -> u32 {\n    if let Ok(ref mut ent) = fetch() {\n        return ent.path();\n    }\n    0\n}\n",
        "crate::go",
    );
}

#[test]
fn while_let_pop() {
    binds_entry(
        "pub fn go(mut stack: Vec<Entry>) -> u32 {\n    let mut n = 0;\n    while let Some(ent) = stack.pop() {\n        n += ent.path();\n    }\n    n\n}\n",
        "crate::go",
    );
}

#[test]
fn for_over_a_vec_and_its_iter() {
    binds_entry(
        "pub fn go(all: Vec<Entry>) -> u32 {\n    let mut n = 0;\n    for ent in all.iter() {\n        n += ent.path();\n    }\n    for ent in &all {\n        n += ent.path();\n    }\n    n\n}\n",
        "crate::go",
    );
}

#[test]
fn for_over_results_then_try() {
    binds_entry(
        "pub fn go(all: Vec<Result<Entry, u8>>) -> Result<u32, u8> {\n    let mut n = 0;\n    for r in all {\n        let ent = r?;\n        n += ent.path();\n    }\n    Ok(n)\n}\n",
        "crate::go",
    );
}

#[test]
fn for_over_a_user_iterator_of_the_item_type() {
    binds_entry(
        "pub struct Walk { left: u32 }\nimpl Iterator for Walk {\n    type Item = Entry;\n    fn next(&mut self) -> Option<Entry> { None }\n}\n\
pub struct Tree;\nimpl Tree {\n    pub fn walk(&self) -> Walk { Walk { left: 1 } }\n}\n\
pub fn go(t: &Tree) -> u32 {\n    let mut n = 0;\n    for ent in t.walk().take_while(|_| true) {\n        n += ent.path();\n    }\n    n\n}\n",
        "crate::go",
    );
}

#[test]
fn for_over_a_channel_receiver() {
    binds_entry(
        "pub fn go() -> u32 {\n    let (tx, rx) = crossbeam_channel::bounded::<Entry>(4);\n    drop(tx);\n    let mut n = 0;\n    for ent in rx {\n        n += ent.path();\n    }\n    n\n}\n",
        "crate::go",
    );
}

#[test]
fn std_channel_receiver_iter() {
    binds_entry(
        "pub fn go() -> u32 {\n    let (tx, rx) = std::sync::mpsc::channel::<Entry>();\n    drop(tx);\n    let mut n = 0;\n    for ent in rx.iter() {\n        n += ent.path();\n    }\n    n\n}\n",
        "crate::go",
    );
}

#[test]
fn a_rebinding_of_the_name_is_not_typed_by_the_outer_binding() {
    let src = format!(
        "{TYPES}pub fn fetch() -> Option<Entry> {{ None }}\npub fn go(other: Other) -> u32 {{\n    if let Some(ent) = fetch() {{\n        let ent = other;\n        return ent.path();\n    }}\n    0\n}}\n"
    );
    let got = targets(&lib(&src), "crate::go");
    assert!(got.contains("crate::Other::path"), "{got:#?}");
    assert!(!got.contains("crate::Entry::path"), "{got:#?}");
}

#[test]
fn enum_field_and_variant_across_files() {
    let files = vec![
        ("Cargo.toml", "[package]\nname = \"x\"\n"),
        (
            "src/lib.rs",
            "mod inner;\nmod holder;\npub use inner::Inner;\n",
        ),
        (
            "src/inner.rs",
            "pub struct Entry { p: u32 }\nimpl Entry {\n    pub fn path(&self) -> u32 { self.p }\n}\n\
pub struct Other;\nimpl Other {\n    pub fn path(&self) -> u32 { 0 }\n}\n\
pub enum Inner { Raw(Entry), Gone }\nimpl Inner {\n    pub fn path(&self) -> u32 {\n        use self::Inner::*;\n        match *self {\n            Gone => 0,\n            Raw(ref x) => x.path(),\n        }\n    }\n}\n",
        ),
        (
            "src/holder.rs",
            "use crate::inner::Inner;\npub struct Holder { dent: Inner }\nimpl Holder {\n    pub fn go(&self) -> u32 {\n        self.dent.path()\n    }\n}\n",
        ),
    ];
    let got = targets(&files, "crate::holder::Holder::go");
    assert!(got.contains("crate::inner::Inner::path"), "{got:#?}");
    let got = targets(&files, "crate::inner::Inner::path");
    assert!(got.contains("crate::inner::Entry::path"), "{got:#?}");
    assert!(!got.contains("crate::inner::Other::path"), "{got:#?}");
}

const ITER_TYPE: &str = "pub struct Walk { left: u32 }\nimpl Iterator for Walk {\n    type Item = Entry;\n    fn next(&mut self) -> Option<Self::Item> { None }\n}\n";

#[test]
fn user_iterator_item_through_self_item() {
    binds_entry(
        &format!(
            "{ITER_TYPE}pub fn go(w: Walk) -> u32 {{\n    let mut n = 0;\n    for ent in w {{\n        n += ent.path();\n    }}\n    n\n}}\n"
        ),
        "crate::go",
    );
}

#[test]
fn user_iterator_next_in_a_while_let() {
    binds_entry(
        &format!(
            "{ITER_TYPE}pub fn go(mut w: Walk) -> u32 {{\n    let mut n = 0;\n    while let Some(ent) = w.next() {{\n        n += ent.path();\n    }}\n    n\n}}\n"
        ),
        "crate::go",
    );
}

#[test]
fn user_iterator_declared_in_another_file() {
    let files = vec![
        ("Cargo.toml", "[package]\nname = \"x\"\n"),
        ("src/lib.rs", "mod dir;\nmod walk;\n"),
        (
            "src/dir.rs",
            "pub struct Entry { p: u32 }\nimpl Entry {\n    pub fn path(&self) -> u32 { self.p }\n}\n\
pub struct Other;\nimpl Other {\n    pub fn path(&self) -> u32 { 0 }\n}\n\
pub struct Parents<'a>(Option<&'a Entry>);\n\
impl<'a> Iterator for Parents<'a> {\n    type Item = &'a Entry;\n    fn next(&mut self) -> Option<Self::Item> { None }\n}\n\
pub struct Chain { p: u32 }\nimpl Chain {\n    pub fn parents(&self) -> Parents<'_> { Parents(None) }\n}\n",
        ),
        (
            "src/walk.rs",
            "use crate::dir::Chain;\npub fn check(chain: &Chain) -> u32 {\n    let mut n = 0;\n    for ig in chain.parents().take_while(|_| true) {\n        n += ig.path();\n    }\n    n\n}\n",
        ),
    ];
    let got = targets(&files, "crate::walk::check");
    assert!(got.contains("crate::dir::Entry::path"), "{got:#?}");
    assert!(!got.contains("crate::dir::Other::path"), "{got:#?}");
}

#[test]
fn a_type_that_is_not_an_iterator_is_not_iterated() {
    let src = format!(
        "{TYPES}pub struct Bag;\nimpl Bag {{\n    pub fn items(&self) -> Entry {{ Entry {{ p: 1 }} }}\n}}\n\
pub fn go(b: Bag) -> u32 {{\n    let mut n = 0;\n    for ent in b.items().take(2) {{\n        n += ent.path();\n    }}\n    n\n}}\n"
    );
    let got = targets(&lib(&src), "crate::go");
    assert!(!got.contains("crate::Other::path"), "{got:#?}");
}

#[test]
fn a_type_declared_in_the_callers_module_wins_over_a_same_named_type_elsewhere() {
    let files = vec![
        ("Cargo.toml", "[package]\nname = \"x\"\n"),
        ("src/lib.rs", "pub mod lib_entry;\nmod example;\n"),
        (
            "src/lib_entry.rs",
            "pub enum DirEntry { A }\nimpl DirEntry {\n    pub fn path(&self) -> u32 { 0 }\n}\n",
        ),
        (
            "src/example.rs",
            "pub enum DirEntry { X, Y }\nimpl DirEntry {\n    fn path(&self) -> u32 { 1 }\n}\n\
pub fn go() -> u32 {\n    let (tx, rx) = crossbeam_channel::bounded::<DirEntry>(4);\n    drop(tx);\n    let mut n = 0;\n    for dent in rx {\n        n += dent.path();\n    }\n    n\n}\n",
        ),
    ];
    let got = targets(&files, "crate::example::go");
    assert!(got.contains("crate::example::DirEntry::path"), "{got:#?}");
    assert!(
        !got.contains("crate::lib_entry::DirEntry::path"),
        "{got:#?}"
    );
}

// Incremental sync equals a fresh reindex when the declaration a receiver is
// typed from changes in another file.

const INC_LIB: &str = "mod dir;\nmod walk;\n";
const INC_TYPES: &str = "pub struct Entry { p: u32 }\nimpl Entry {\n    pub fn path(&self) -> u32 { self.p }\n}\n\
pub struct Other;\nimpl Other {\n    pub fn path(&self) -> u32 { 0 }\n}\n";
const INC_WALK: &str = "use crate::dir::{Chain, Holder};\n\
pub fn iter(chain: &Chain) -> u32 {\n    let mut n = 0;\n    for ig in chain.parents().take_while(|_| true) {\n        n += ig.path();\n    }\n    n\n}\n\
pub fn field(h: &Holder) -> u32 {\n    h.dent.path()\n}\n";

fn inc_dir(item: &str, field: &str) -> String {
    format!(
        "{INC_TYPES}pub struct Parents(u32);\n\
impl Iterator for Parents {{\n    type Item = {item};\n    fn next(&mut self) -> Option<Self::Item> {{ None }}\n}}\n\
pub struct Chain;\nimpl Chain {{\n    pub fn parents(&self) -> Parents {{ Parents(0) }}\n}}\n\
pub enum Inner {{ Raw({field}), Gone }}\nimpl Inner {{\n    pub fn path(&self) -> u32 {{\n        use self::Inner::*;\n        match *self {{\n            Gone => 0,\n            Raw(ref x) => x.path(),\n        }}\n    }}\n}}\n\
pub struct Holder {{ pub dent: Inner }}\n"
    )
}

fn inc_files(dir: &str) -> Vec<(&str, &str)> {
    vec![
        ("Cargo.toml", "[package]\nname = \"x\"\n"),
        ("src/lib.rs", INC_LIB),
        ("src/dir.rs", dir),
        ("src/walk.rs", INC_WALK),
    ]
}

#[test]
fn incremental_sync_retypes_receivers_when_the_declaration_changes() {
    let first = inc_dir("Entry", "Entry");
    let second = inc_dir("Other", "Other");
    let (_tmp, root, mut indexer) = common::live_indexer("rust-result-recv-", &inc_files(&first));
    indexer.reindex().unwrap();
    for (dir, expect_item) in [(&second, "Other"), (&first, "Entry")] {
        common::write_files(&root, &[("src/dir.rs", dir)]);
        indexer.sync_rel_paths(&["src/dir.rs".to_string()]).unwrap();
        common::assert_no_dangling_edge_targets(indexer.db());
        let gv = indexer.db().current_graph_version().unwrap();
        let snapshot = lidx_snapshot(&indexer, gv);
        let (_t, fresh) = common::index_files(&inc_files(dir));
        common::assert_matches_fresh(&snapshot, &fresh);
        // The loop variable follows the iterator's new item type.
        let want = format!("crate::dir::{expect_item}::path");
        let got = targets(&inc_files(dir), "crate::walk::iter");
        assert!(got.contains(&want), "{expect_item}: {got:#?}");
    }
}

#[test]
fn incremental_sync_types_receivers_when_the_declaring_file_is_added_later() {
    let full = inc_dir("Entry", "Entry");
    let mut files = inc_files(&full);
    files.retain(|(p, _)| *p != "src/dir.rs");
    files[1] = ("src/lib.rs", "mod walk;\n");
    let (_tmp, root, mut indexer) = common::live_indexer("rust-result-recv-", &files);
    indexer.reindex().unwrap();
    common::write_files(
        &root,
        &[("src/lib.rs", INC_LIB), ("src/dir.rs", full.as_str())],
    );
    indexer
        .sync_rel_paths(&["src/lib.rs".to_string(), "src/dir.rs".to_string()])
        .unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = lidx_snapshot(&indexer, gv);
    let (_t, fresh) = common::index_files(&inc_files(&full));
    common::assert_matches_fresh(&snapshot, &fresh);
}

fn lidx_snapshot(indexer: &lidx::indexer::Indexer, gv: i64) -> BTreeSet<common::golden::EdgeKey> {
    common::golden::snapshot_edges(indexer.db(), gv).unwrap()
}

fn two_foos() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Cargo.toml", "[package]\nname = \"x\"\n"),
        ("src/lib.rs", "pub mod a;\npub mod b;\n"),
        (
            "src/a.rs",
            "pub struct Foo;\nimpl Foo { pub fn go(&self) {} }\npub fn make() -> Foo { Foo }\npub fn many() -> Vec<Foo> { vec![] }\npub struct Holder { pub foo: Foo }\n",
        ),
        (
            "src/b.rs",
            "use crate::a;\npub struct Foo;\nimpl Foo { pub fn go(&self) {} }\n\
pub fn via_fn() { let f = a::make(); f.go(); }\n\
pub fn via_field(h: &a::Holder) { h.foo.go(); }\n\
pub fn via_iter() { for f in a::many() { f.go(); } }\n\
pub fn via_param(f: &a::Foo) { f.go(); }\n\
pub fn via_new() { let f = a::Foo::new(); f.go(); }\n\
pub fn local_param(f: &Foo) { f.go(); }\n\
pub fn local_ctor() { let f = Foo::new(); f.go(); }\n\
impl Foo { pub fn new() -> Foo { Foo } }\n",
        ),
    ]
}

#[test]
fn a_receiver_typed_through_another_module_never_binds_the_callers_same_named_type() {
    let files = two_foos();
    for caller in [
        "crate::b::via_fn",
        "crate::b::via_field",
        "crate::b::via_iter",
        "crate::b::via_param",
        "crate::b::via_new",
    ] {
        let got = targets(&files, caller);
        assert!(!got.contains("crate::b::Foo::go"), "{caller}: {got:#?}");
    }
}

#[test]
fn a_bare_type_declared_in_the_callers_module_binds_that_modules_method() {
    let files = two_foos();
    for caller in ["crate::b::local_param", "crate::b::local_ctor"] {
        let got = targets(&files, caller);
        assert!(got.contains("crate::b::Foo::go"), "{caller}: {got:#?}");
        assert!(!got.contains("crate::a::Foo::go"), "{caller}: {got:#?}");
    }
}

#[test]
fn annotated_channel_halves_type_the_receiver() {
    binds_entry(
        "use std::sync::mpsc::{Receiver, Sender, channel};\npub fn go() -> u32 {\n    let (tx, rx): (Sender<Entry>, Receiver<Entry>) = channel();\n    drop(tx);\n    let mut n = 0;\n    for ent in rx {\n        n += ent.path();\n    }\n    n\n}\npub fn go2(rx: std::sync::mpsc::Receiver<Entry>) -> u32 {\n    let mut n = 0;\n    for ent in rx.iter() {\n        n += ent.path();\n    }\n    n\n}\n",
        "crate::go",
    );
    binds_entry(
        "pub fn go2(rx: crossbeam_channel::Receiver<Entry>) -> u32 {\n    let mut n = 0;\n    for ent in rx.iter() {\n        n += ent.path();\n    }\n    n\n}\n",
        "crate::go2",
    );
}

#[test]
fn a_repo_type_named_receiver_is_not_a_channel() {
    let src = format!(
        "{TYPES}pub struct Receiver<T>(T);\nimpl<T> Receiver<T> {{\n    pub fn path(&self) -> u32 {{ 9 }}\n}}\n\
pub fn go(rx: Receiver<Entry>) -> u32 {{\n    rx.path()\n}}\n"
    );
    let got = targets(&lib(&src), "crate::go");
    assert!(got.contains("crate::Receiver::path"), "{got:#?}");
    assert!(!got.contains("crate::Entry::path"), "{got:#?}");
}

fn two_packages() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "Cargo.toml",
            "[workspace]\nmembers = [\"regex\", \"pcre\"]\n",
        ),
        ("regex/Cargo.toml", "[package]\nname = \"regex\"\n"),
        ("regex/src/lib.rs", "pub mod matcher;\n"),
        (
            "regex/src/matcher.rs",
            "pub struct RegexMatcher;\nimpl RegexMatcher {\n    pub fn find_candidate_line(&self) {}\n    pub fn own(&self) {}\n}\n",
        ),
        ("pcre/Cargo.toml", "[package]\nname = \"pcre\"\n"),
        ("pcre/src/lib.rs", "pub mod matcher;\n"),
        (
            "pcre/src/matcher.rs",
            "pub struct RegexMatcher;\nimpl RegexMatcher {\n    pub fn own(&self) {}\n}\n\
pub trait Matcher {\n    fn find_candidate_line(&self) {}\n}\nimpl Matcher for RegexMatcher {}\n\
pub fn trait_default(m: &RegexMatcher) { m.find_candidate_line(); }\n\
pub fn inherent(m: &RegexMatcher) { m.own(); }\n\
pub fn nothing(m: &RegexMatcher) { m.absent(); }\n",
        ),
    ]
}

#[test]
fn a_bare_type_never_binds_the_method_of_a_same_named_type_in_another_package() {
    let files = two_packages();
    let got = targets(&files, "crate::matcher::trait_default");
    assert!(
        got.contains("crate::matcher::Matcher::find_candidate_line"),
        "{got:#?}"
    );
    assert!(
        !got.iter()
            .any(|t| t.contains("RegexMatcher::find_candidate_line")),
        "{got:#?}"
    );
    let got = targets(&files, "crate::matcher::nothing");
    assert!(got.iter().all(|t| !t.ends_with("::absent")), "{got:#?}");
}
