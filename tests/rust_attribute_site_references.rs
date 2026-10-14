//! Issue #514: a function named where rust-analyzer (run without proc macros)
//! emits no occurrence, in an attribute argument or in the body of a
//! `macro_rules!` definition, is still a value reference. The oracle benchmark
//! leaves such a pair ungradable on the strength of this: the index must hold
//! the right edge at each shape, from the right source, bound to the right
//! target.

mod common;

const FILES: &[(&str, &str)] = &[
    ("Cargo.toml", "[package]\nname = \"x\"\n"),
    (
        "src/lib.rs",
        "pub mod cli;\npub mod keyed;\npub mod filter;\n",
    ),
    (
        "src/filter.rs",
        "pub struct OwnerFilter;\nimpl OwnerFilter {\n    pub fn from_string(s: &str) -> Option<OwnerFilter> {\n        let _ = s;\n        None\n    }\n}\n",
    ),
    ("src/keyed.rs", "pub fn serialize() {}\n"),
    (
        "src/cli.rs",
        r#"use crate::filter::OwnerFilter;

fn parse_millis(s: &str) -> u64 {
    s.len() as u64
}

fn default_true() -> bool {
    true
}

pub struct Opts {
    #[arg(long, hide = true, value_parser = parse_millis)]
    pub a: u64,
    #[arg(long, short = 'o', value_parser = OwnerFilter::from_string)]
    pub b: Option<OwnerFilter>,
    #[serde(default = "default_true")]
    pub c: bool,
    #[serde(serialize_with = "crate::keyed::serialize")]
    pub d: u8,
}
"#,
    ),
    (
        "build.rs",
        r#"fn generate() {
    let mut v = vec!["a"];
    macro_rules! sort_prop {
        ($p:expr) => {{
            v.sort_unstable_by(compare_len)
        }};
    }
    sort_prop!("quotes");
}

fn compare_len(a: &&str, b: &&str) -> std::cmp::Ordering {
    a.len().cmp(&b.len())
}
"#,
    ),
];

#[test]
fn attribute_arguments_are_references_from_the_module() {
    let t = common::ref_targets(FILES, "crate::cli");
    for want in [
        "crate::cli::parse_millis",
        "crate::filter::OwnerFilter::from_string",
        "crate::cli::default_true",
        "crate::keyed::serialize",
    ] {
        assert!(t.contains(&want.to_string()), "{want} missing from {t:#?}");
    }
}

#[test]
fn a_macro_rules_body_names_a_function_the_enclosing_function_references() {
    let t = common::ref_targets(FILES, "crate::build::generate");
    assert!(
        t.contains(&"crate::build::compare_len".to_string()),
        "{t:#?}"
    );
}
