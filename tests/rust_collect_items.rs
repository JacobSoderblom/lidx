//! Issue #497: the items of an iterator that a closure builds
//! (`.map(|c| Builder::new(c))`) type the value it is collected into
//! (`let v: io::Result<Vec<_>> = ..collect();`), so a method called on an
//! element binds that type's method and not a same-named method of another.

mod common;

const TYPES: &str = "pub struct Builder { n: u32 }\nimpl Builder {\n    pub fn new(n: u32) -> std::io::Result<Builder> { Ok(Builder { n }) }\n    pub fn push(&mut self, k: u32) { self.n += k; }\n}\n\
pub struct Other { n: u32 }\nimpl Other {\n    pub fn push(&mut self, k: u32) { self.n += k; }\n}\n";

fn targets(body: &str) -> Vec<String> {
    let lib = format!("{TYPES}{body}");
    common::call_targets(
        &[
            ("Cargo.toml", "[package]\nname = \"x\"\n"),
            ("src/lib.rs", &lib),
        ],
        "crate::go",
    )
}

fn binds_builder(body: &str) {
    let got = targets(body);
    assert!(
        got.contains(&"crate::Builder::push".to_string()),
        "{got:#?}"
    );
    assert!(!got.contains(&"crate::Other::push".to_string()), "{got:#?}");
}

#[test]
fn result_of_a_vec_collected_from_a_mapping_closure() {
    binds_builder(
        "pub fn go(xs: &[u32]) {\n    let builders: std::io::Result<Vec<_>> = xs.iter().map(|c| Builder::new(*c)).collect();\n    match builders {\n        Ok(mut builders) => {\n            for builder in &mut builders {\n                builder.push(1);\n            }\n        }\n        Err(_) => {}\n    }\n}\n",
    );
}

#[test]
fn vec_collected_from_a_mapping_closure() {
    binds_builder(
        "pub fn go(xs: &[u32]) {\n    let mut all: Vec<_> = xs.iter().map(|c| Builder::new(*c).unwrap()).collect();\n    for b in all.iter_mut() {\n        b.push(1);\n    }\n}\n",
    );
}
