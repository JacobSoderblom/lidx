//! A lowercase non-function binding passed or stored as a value (`static`,
//! `const`, a struct-field shorthand, a captured local) is never a CALLS
//! edge; a real function reference still is.

mod common;

const LIB_RS: &str = r#"
pub mod cfg {
    pub static other_counter: u32 = 1;
    pub const other_limit: u32 = 2;
}
pub fn real_fn(x: u32) -> u32 { x }
static counter: u32 = 0;
const limit: u32 = 1;
pub struct Foo { pub f: u32 }
fn take<T>(_t: T) {}
pub fn run(xs: Vec<u32>) {
    take(counter);
    take(limit);
    take(cfg::other_counter);
    take(cfg::other_limit);
    let _a = counter;
    let outer = 5u32;
    let _c = xs.iter().map(|x| take(outer + *x)).count();
    let f = 3u32;
    let _s = Foo { f };
    let _m = xs.iter().map(real_fn).count();
}
"#;

#[test]
fn non_fn_values_are_never_calls() {
    let (_tmp, snap) = common::index_files(&[
        ("Cargo.toml", "[package]\nname = \"x\"\n"),
        ("src/lib.rs", LIB_RS),
    ]);
    let calls: Vec<String> = snap
        .iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == "crate::run")
        .map(|e| e.to_string())
        .collect();
    for bad in [
        "counter",
        "limit",
        "other_counter",
        "other_limit",
        "outer",
        "::f ",
    ] {
        assert!(
            !calls.iter().any(|c| c.contains(bad)),
            "{bad} became a call: {calls:#?}"
        );
    }
    assert!(
        calls.iter().any(|c| c.contains("crate::real_fn")),
        "{calls:#?}"
    );
}
