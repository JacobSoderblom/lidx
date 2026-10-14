//! Issue #497: a collection of a dependency named like a map or a set
//! (`iddqd::IdOrdMap<Item>`) is read like std's, so a value taken out of it
//! (`get`, `iter`, `first`) is a value of the item type and a method called on
//! it binds that type's method, not a same-named method of another type.

mod common;

const MANIFEST: &str = "[package]\nname = \"x\"\n[dependencies]\niddqd = \"0.3\"\n";

const TYPES: &str = "use iddqd::IdOrdMap;\npub struct Item { n: u32 }\nimpl Item {\n    pub fn matches(&self, k: u32) -> bool { self.n == k }\n}\n\
pub struct Other { n: u32 }\nimpl Other {\n    pub fn matches(&self, k: u32) -> bool { self.n == k }\n}\n";

fn binds_item(body: &str) {
    let lib = format!("{TYPES}{body}");
    let got = common::call_targets(
        &[("Cargo.toml", MANIFEST), ("src/lib.rs", &lib)],
        "crate::go",
    );
    assert!(
        got.contains(&"crate::Item::matches".to_string()),
        "{got:#?}"
    );
    assert!(
        !got.contains(&"crate::Other::matches".to_string()),
        "{got:#?}"
    );
}

#[test]
fn get_on_a_dependency_map_of_items() {
    binds_item(
        "pub fn go(items: &IdOrdMap<Item>, k: u32) -> bool {\n    if let Some(item) = items.get(&k) {\n        return item.matches(k);\n    }\n    false\n}\n",
    );
}

#[test]
fn iteration_over_a_dependency_map_of_items() {
    binds_item(
        "pub fn go(items: &IdOrdMap<Item>, k: u32) -> bool {\n    let mut any = false;\n    for item in items.iter() {\n        any |= item.matches(k);\n    }\n    any\n}\n",
    );
}

#[test]
fn a_repo_map_named_like_one_is_not_read_as_std() {
    let lib = "pub struct Item { n: u32 }\nimpl Item {\n    pub fn matches(&self, k: u32) -> bool { self.n == k }\n}\n\
pub struct FooMap<T> { v: Vec<T> }\nimpl<T> FooMap<T> {\n    pub fn get(&self, _k: u32) -> Option<&Other> { None }\n}\n\
pub struct Other { n: u32 }\nimpl Other {\n    pub fn matches(&self, k: u32) -> bool { self.n == k }\n}\n\
pub fn go(m: &FooMap<Item>, k: u32) -> bool {\n    if let Some(x) = m.get(k) {\n        return x.matches(k);\n    }\n    false\n}\n";
    let got = common::call_targets(
        &[("Cargo.toml", MANIFEST), ("src/lib.rs", lib)],
        "crate::go",
    );
    assert!(
        !got.contains(&"crate::Item::matches".to_string()),
        "{got:#?}"
    );
}
