//! A call with a receiver (`x.m()`) is answered by a method, never by a free
//! function that shares the name (issue #463 review: nextest's
//! `"..".style(..)` bound to `output::clap_styles::style`).

mod common;

fn call_targets(files: &[(&str, &str)], source: &str) -> Vec<String> {
    let (_tmp, snapshot) = common::index_files(files);
    snapshot
        .into_iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == source)
        .filter_map(|e| e.target_qualname)
        .collect()
}

const CARGO: (&str, &str) = (
    "Cargo.toml",
    "[package]\nname = \"m\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
);

#[test]
fn rust_receiver_call_never_binds_a_free_function() {
    let lib = r#"mod styles {
    pub fn style(x: u32) -> u32 { x }
    pub fn is_zero(x: u32) -> bool { x == 0 }
}
pub struct Painter;
impl Painter { pub fn paint(&self) -> u32 { 1 } }
pub fn run(p: Painter) -> u32 {
    let _ = "text".style(1);
    let _ = make().is_zero();
    p.paint()
}
"#;
    let targets = call_targets(&[CARGO, ("src/lib.rs", lib)], "crate::run");
    assert!(
        !targets
            .iter()
            .any(|t| t.ends_with("styles::style") || t.ends_with("styles::is_zero")),
        "{targets:?}"
    );
}

#[test]
fn rust_receiver_call_still_binds_a_unique_method() {
    let lib = r#"pub struct Painter;
impl Painter { pub fn paint(&self) -> u32 { 1 } }
pub fn run() -> u32 {
    make().paint()
}
"#;
    let targets = call_targets(&[CARGO, ("src/lib.rs", lib)], "crate::run");
    assert!(
        targets.iter().any(|t| t.ends_with("Painter::paint")),
        "{targets:?}"
    );
}

#[test]
fn rust_receiver_call_with_a_free_function_twin_stays_unresolved() {
    let lib = r#"pub fn walk(x: u32) -> u32 { x }
pub struct Walker;
impl Walker { pub fn walk(&self) -> u32 { 1 } }
pub fn run() -> u32 {
    make().walk()
}
"#;
    let targets = call_targets(&[CARGO, ("src/lib.rs", lib)], "crate::run");
    assert!(
        !targets.iter().any(|t| t.ends_with("walk")),
        "a free function with the name keeps the call ambiguous: {targets:?}"
    );
}

#[test]
fn rust_path_call_to_a_free_function_still_binds() {
    let lib = r#"mod styles { pub fn style(x: u32) -> u32 { x } }
pub fn run() -> u32 { styles::style(1) }
"#;
    let targets = call_targets(&[CARGO, ("src/lib.rs", lib)], "crate::run");
    assert!(
        targets.iter().any(|t| t.ends_with("styles::style")),
        "{targets:?}"
    );
}

#[test]
fn other_languages_receiver_calls_do_not_bind_free_functions() {
    let py = "def style(x):\n    return x\n\ndef run(unknown):\n    return unknown.style(1)\n";
    let targets = call_targets(&[("m.py", py)], "m.run");
    assert!(
        !targets.iter().any(|t| t.ends_with("style")),
        "python {targets:?}"
    );

    let ts = "export function style(x: number) { return x; }\nexport function run(unknown: any) { return unknown.style(1); }\n";
    let targets = call_targets(&[("m.ts", ts)], "m.run");
    assert!(
        !targets.iter().any(|t| t.ends_with("style")),
        "ts {targets:?}"
    );

    let go = "package m\n\nfunc style(x int) int { return x }\n\nfunc run(unknown Foreign) int { return unknown.style(1) }\n";
    let targets = call_targets(
        &[
            ("go.mod", "module example.com/m\n\ngo 1.22\n"),
            ("m.go", go),
        ],
        "m.run",
    );
    assert!(
        !targets.iter().any(|t| t.ends_with("style")),
        "go {targets:?}"
    );
}
