//! `read_symbol` must return whole real file lines when a symbol's span starts or
//! ends mid-line and no other symbol shares that line (issue #343).
mod common;

use lidx::indexer::Indexer;
use lidx::rpc;

fn index(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let (tmp, root, db_path) = common::index_repo("lidx-midline-", files);
    (tmp, Indexer::new(root, db_path).unwrap())
}

fn read(indexer: &mut Indexer, qualname: &str) -> String {
    let r = rpc::handle_method(
        indexer,
        "read_symbol",
        serde_json::json!({"qualname": qualname}),
    )
    .unwrap();
    r["source"]
        .as_str()
        .unwrap_or_else(|| panic!("{r:#}"))
        .to_string()
}

/// First line of `explain_symbol`'s source section.
fn explain_first_line(indexer: &mut Indexer, qualname: &str) -> String {
    let r = rpc::handle_method(
        indexer,
        "explain_symbol",
        serde_json::json!({"qualname": qualname, "sections": ["source"]}),
    )
    .unwrap();
    let src = r["source"]
        .as_str()
        .unwrap_or_else(|| panic!("{r:#}"))
        .to_string();
    src.lines().next().unwrap().to_string()
}

/// First line of a `read_symbol` source without its `N: ` prefix.
fn first_line_text(src: &str) -> &str {
    let line = src.lines().next().unwrap();
    line.split_once(": ").map_or(line, |(_, t)| t)
}

const TS: &str =
    "export async function buildApp() {\n  return 1;\n}\n\nexport const meter = getMeter('x');\n";
const CS: &str = "namespace Demo;\n\npublic class B\n{\n    private readonly string _name;\n    private const int Cap = 64 * 1024;\n}\n";
const SQL: &str = "CREATE TABLE dpb.t\n(\n  id INT\n);\n";

#[test]
fn ts_export_prefix_and_trailing_semicolon_included() {
    let (_tmp, mut ix) = index(&[("a.ts", TS)]);
    assert_eq!(
        read(&mut ix, "a.buildApp").lines().next().unwrap(),
        "1: export async function buildApp() {"
    );
    assert_eq!(
        read(&mut ix, "a.meter"),
        "5: export const meter = getMeter('x');"
    );
    for qn in ["a.buildApp", "a.meter"] {
        assert_eq!(
            first_line_text(&read(&mut ix, qn)),
            explain_first_line(&mut ix, qn),
            "{qn}"
        );
    }
}

#[test]
fn csharp_field_modifiers_included() {
    let (_tmp, mut ix) = index(&[("B.cs", CS)]);
    assert_eq!(
        read(&mut ix, "Demo.B._name"),
        "5:     private readonly string _name;"
    );
    assert_eq!(
        read(&mut ix, "Demo.B.Cap"),
        "6:     private const int Cap = 64 * 1024;"
    );
    for qn in ["Demo.B._name", "Demo.B.Cap"] {
        assert_eq!(
            first_line_text(&read(&mut ix, qn)),
            explain_first_line(&mut ix, qn),
            "{qn}"
        );
    }
}

#[test]
fn tsql_create_table_last_line_has_semicolon() {
    let (_tmp, mut ix) = index(&[("t.sql", SQL)]);
    let src = read(&mut ix, "dpb.t");
    assert_eq!(src.lines().last().unwrap(), "4: );", "{src}");
    assert_eq!(first_line_text(&src), explain_first_line(&mut ix, "dpb.t"));
}

#[test]
fn shared_line_keeps_exact_byte_span() {
    let cs = "namespace Demo;\n\npublic class C\n{\n    int a; int b;\n}\n";
    let (_tmp, mut ix) = index(&[("C.cs", cs)]);
    assert_eq!(read(&mut ix, "Demo.C.a"), "5: a");
    assert_eq!(read(&mut ix, "Demo.C.b"), "5: b");
}

#[test]
fn python_and_rust_spans_unchanged() {
    let py = "def f():\n    x = 1  # note\n    return x\n";
    let rs = "fn f() {\n    1\n} // end\n";
    let (_tmp, mut ix) = index(&[("m.py", py), ("m.rs", rs)]);
    assert_eq!(
        read(&mut ix, "m.f"),
        "1: def f():\n2:     x = 1  # note\n3:     return x"
    );
    assert_eq!(read(&mut ix, "m::f"), "1: fn f() {\n2:     1\n3: }");
}
