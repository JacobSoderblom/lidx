//! Issue #214: TypeScript interface heritage, namespaces, class fields and
//! abstract members, and anonymous default exports.

mod common;

use lidx::indexer::Indexer;
use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::javascript::TypescriptExtractor;
use lidx::rpc;

fn extract(source: &str, module: &str) -> lidx::indexer::extract::ExtractedFile {
    TypescriptExtractor::new()
        .unwrap()
        .extract(source, module)
        .unwrap()
}

fn sym_kind<'a>(f: &'a lidx::indexer::extract::ExtractedFile, qualname: &str) -> Option<&'a str> {
    f.symbols
        .iter()
        .find(|s| s.qualname == qualname)
        .map(|s| s.kind.as_str())
}

fn qualnames(f: &lidx::indexer::extract::ExtractedFile) -> Vec<String> {
    f.symbols.iter().map(|s| s.qualname.clone()).collect()
}

/// `(source qualname, resolved-or-raw target qualname)` for edges of `kind`.
fn indexed_edges(files: &[(&str, &str)], kind: &str) -> Vec<(String, String)> {
    let (tmp, _) = common::index_files(files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, COALESCE(t.qualname, 'UNRESOLVED:' || e.target_qualname) FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = ?",
        )
        .unwrap();
    let mut rows: Vec<(String, String)> = stmt
        .query_map(rusqlite::params![gv, kind], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    rows.sort();
    rows
}

#[test]
fn interface_extends_resolves_to_base() {
    let edges = indexed_edges(
        &[(
            "a.ts",
            "export interface Base {}\nexport interface Child extends Base {}\nexport class Impl implements Child {}\nexport class Sub extends Impl {}\n",
        )],
        "EXTENDS",
    );
    assert!(
        edges.contains(&("a.Child".into(), "a.Base".into())),
        "{edges:?}"
    );
    assert!(
        edges.contains(&("a.Sub".into(), "a.Impl".into())),
        "{edges:?}"
    );
    let impls = indexed_edges(
        &[(
            "a.ts",
            "export interface Child {}\nexport class Impl implements Child {}\n",
        )],
        "IMPLEMENTS",
    );
    assert_eq!(impls, vec![("a.Impl".into(), "a.Child".into())]);
}

#[test]
fn interface_multiple_extends_yield_one_edge_each() {
    let f = extract(
        "interface A {}\ninterface B<T> {}\ninterface C extends A, B<string>, ns.D {}\n",
        "m",
    );
    let targets: Vec<&str> = f
        .edges
        .iter()
        .filter(|e| e.kind == "EXTENDS" && e.source_qualname.as_deref() == Some("m.C"))
        .filter_map(|e| e.target_qualname.as_deref())
        .collect();
    assert_eq!(targets, vec!["A", "B", "ns.D"]);
}

#[test]
fn namespace_qualifies_members() {
    let f = extract(
        "namespace NS { export function inner() {} export const k = 1; export class K {} }\n",
        "m",
    );
    assert_eq!(sym_kind(&f, "m.NS"), Some("namespace"));
    assert_eq!(sym_kind(&f, "m.NS.inner"), Some("function"));
    assert_eq!(sym_kind(&f, "m.NS.k"), Some("const"));
    assert_eq!(sym_kind(&f, "m.NS.K"), Some("class"));
    assert!(sym_kind(&f, "m.inner").is_none());
    assert!(f.edges.iter().any(|e| e.kind == "CONTAINS"
        && e.source_qualname.as_deref() == Some("m.NS")
        && e.target_qualname.as_deref() == Some("m.NS.inner")));
    assert!(f.edges.iter().any(|e| e.kind == "CONTAINS"
        && e.source_qualname.as_deref() == Some("m")
        && e.target_qualname.as_deref() == Some("m.NS")));
}

#[test]
fn same_named_functions_in_two_namespaces_do_not_collide() {
    let f = extract(
        "namespace A { export function f() {} }\nmodule B { export function f() {} }\n",
        "m",
    );
    let q = qualnames(&f);
    assert!(q.contains(&"m.A.f".to_string()), "{q:?}");
    assert!(q.contains(&"m.B.f".to_string()), "{q:?}");
    let mut dedup = q.clone();
    dedup.sort();
    dedup.dedup();
    assert_eq!(dedup.len(), q.len(), "duplicate qualnames: {q:?}");
}

#[test]
fn nested_and_merged_namespaces() {
    let f = extract(
        "namespace Outer { export namespace Inner { export function g() {} } }\nnamespace P.Q { export function h() {} }\nnamespace Outer { export function more() {} }\n",
        "m",
    );
    let q = qualnames(&f);
    for want in [
        "m.Outer",
        "m.Outer.Inner",
        "m.Outer.Inner.g",
        "m.P",
        "m.P.Q",
        "m.P.Q.h",
        "m.Outer.more",
    ] {
        assert!(q.contains(&want.to_string()), "missing {want}: {q:?}");
    }
    assert_eq!(q.iter().filter(|s| *s == "m.Outer").count(), 1, "{q:?}");
}

#[test]
fn class_fields_and_parameter_properties_yield_symbols() {
    let f = extract(
        "class Impl {\n  id: number = 1;\n  readonly name: string;\n  opt?: string;\n  static count = 0;\n  #secret = 1;\n  constructor(private x: number, readonly y: string, public z?: number, plain: number) {}\n}\n",
        "m",
    );
    for want in [
        "m.Impl.id",
        "m.Impl.name",
        "m.Impl.opt",
        "m.Impl.count",
        "m.Impl.#secret",
        "m.Impl.x",
        "m.Impl.y",
        "m.Impl.z",
    ] {
        assert_eq!(sym_kind(&f, want), Some("field"), "{want}");
        assert!(
            f.edges.iter().any(|e| e.kind == "CONTAINS"
                && e.source_qualname.as_deref() == Some("m.Impl")
                && e.target_qualname.as_deref() == Some(want)),
            "CONTAINS for {want}"
        );
    }
    assert!(sym_kind(&f, "m.Impl.plain").is_none());
    assert_eq!(sym_kind(&f, "m.Impl.constructor"), Some("method"));
    assert!(f.private_qualnames.contains(&"m.Impl.x".to_string()));
}

#[test]
fn abstract_method_yields_symbol() {
    let f = extract(
        "export abstract class Abs {\n  abstract run(): void;\n  abstract helper(a: number): string;\n  concrete() {}\n}\n",
        "m",
    );
    assert_eq!(sym_kind(&f, "m.Abs.run"), Some("method"));
    assert_eq!(sym_kind(&f, "m.Abs.helper"), Some("method"));
    assert_eq!(sym_kind(&f, "m.Abs.concrete"), Some("method"));
}

#[test]
fn anonymous_default_export_yields_symbol() {
    for (src, kind) in [
        ("export default function () {}\n", "function"),
        ("export default () => 1;\n", "function"),
        ("export default class { run() {} }\n", "class"),
        ("export default { a: 1 };\n", "const"),
    ] {
        let f = extract(src, "m");
        assert_eq!(sym_kind(&f, "m.default"), Some(kind), "{src}");
        assert!(
            !f.private_qualnames.contains(&"m.default".to_string()),
            "{src}"
        );
    }
    let f = extract("export default class { run() {} }\n", "m");
    assert_eq!(sym_kind(&f, "m.default.run"), Some("method"));
}

#[test]
fn named_default_export_unchanged() {
    let f = extract("export default function foo() {}\n", "m");
    assert_eq!(sym_kind(&f, "m.foo"), Some("function"));
    assert!(sym_kind(&f, "m.default").is_none());
    let f = extract("class A {}\nexport default A;\n", "m");
    assert_eq!(sym_kind(&f, "m.A"), Some("class"));
    assert!(sym_kind(&f, "m.default").is_none());
}

#[test]
fn import_of_anonymous_default_resolves_to_it() {
    let edges = indexed_edges(
        &[
            (
                "lib/widget.ts",
                "export default function () { return 1; }\n",
            ),
            (
                "use.ts",
                "import w from './lib/widget';\nexport function go() { return w(); }\n",
            ),
        ],
        "CALLS",
    );
    assert!(
        edges.contains(&("use.go".into(), "lib/widget.default".into())),
        "{edges:?}"
    );
}

#[test]
fn outline_lists_new_symbols() {
    let (tmp, _) = common::index_files(&[(
        "a.ts",
        "export interface Base {}\nexport interface Child extends Base {}\nexport namespace NS { export function inner() {} }\nexport abstract class Abs { id: number; abstract run(): void; }\nexport default () => 1;\n",
    )]);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    let result =
        rpc::handle_method(&mut indexer, "outline", serde_json::json!({"path": "a.ts"})).unwrap();
    let q: Vec<&str> = result["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["qualname"].as_str())
        .collect();
    for want in ["a.NS", "a.NS.inner", "a.Abs.id", "a.Abs.run", "a.default"] {
        assert!(q.contains(&want), "missing {want}: {q:?}");
    }
}

#[test]
fn namespace_then_same_named_class_or_function_has_unique_qualnames() {
    for src in [
        "namespace Foo { export const a = 1; }\nclass Foo {}\n",
        "namespace Foo { export const a = 1; }\nfunction Foo() {}\n",
        "class Foo {}\nnamespace Foo { export const a = 1; }\n",
        "function Foo() {}\nnamespace Foo { export const a = 1; }\n",
    ] {
        let f = extract(src, "m");
        let q = qualnames(&f);
        assert_eq!(
            q.iter().filter(|s| *s == "m.Foo").count(),
            1,
            "{src}: {q:?}"
        );
        assert!(q.contains(&"m.Foo.a".to_string()), "{src}: {q:?}");
        let contains = f
            .edges
            .iter()
            .filter(|e| {
                e.kind == "CONTAINS"
                    && e.source_qualname.as_deref() == Some("m")
                    && e.target_qualname.as_deref() == Some("m.Foo")
            })
            .count();
        assert_eq!(contains, 1, "{src}");
    }
}

#[test]
fn bare_call_in_namespace_still_resolves_module_level_helper() {
    let edges = indexed_edges(
        &[(
            "a.ts",
            "function helper() {}\nnamespace NS {\n  export function a() { helper(); }\n}\n",
        )],
        "CALLS",
    );
    assert!(
        edges.contains(&("a.NS.a".into(), "a.helper".into())),
        "{edges:?}"
    );
}

#[test]
fn bare_call_in_namespace_prefers_namespace_sibling() {
    let edges = indexed_edges(
        &[(
            "a.ts",
            "function helper() {}\nnamespace NS {\n  function helper() {}\n  export function a() { helper(); }\n}\n",
        )],
        "CALLS",
    );
    assert!(
        edges.contains(&("a.NS.a".into(), "a.NS.helper".into())),
        "{edges:?}"
    );
}

#[test]
fn arrow_const_in_namespace_attributes_calls() {
    let edges = indexed_edges(
        &[(
            "a.ts",
            "function target() {}\nnamespace NS {\n  export const h = () => { target(); };\n}\n",
        )],
        "CALLS",
    );
    assert!(
        edges.contains(&("a.NS.h".into(), "a.target".into())),
        "{edges:?}"
    );
}

#[test]
fn declare_module_string_name_qualifies_members() {
    let f = extract("declare module \"x/y\" { export class K {} }\n", "m");
    assert_eq!(sym_kind(&f, "m.x/y"), Some("namespace"));
    assert_eq!(sym_kind(&f, "m.x/y.K"), Some("class"));
}

#[test]
fn imported_namespace_member_call_resolves() {
    let edges = indexed_edges(
        &[
            (
                "lib.ts",
                "export namespace NS { export function fn() {} }\n",
            ),
            (
                "use.ts",
                "import { NS } from './lib';\nexport function go() { NS.fn(); }\n",
            ),
        ],
        "CALLS",
    );
    assert!(
        edges.contains(&("use.go".into(), "lib.NS.fn".into())),
        "{edges:?}"
    );
}

#[test]
fn reexported_anonymous_default_resolves() {
    let edges = indexed_edges(
        &[
            ("x.ts", "export default function () { return 1; }\n"),
            ("re.ts", "export { default } from './x';\n"),
            (
                "use.ts",
                "import w from './re';\nexport function go() { return w(); }\n",
            ),
        ],
        "CALLS",
    );
    assert!(
        edges.contains(&("use.go".into(), "x.default".into())),
        "{edges:?}"
    );
}

#[test]
fn interface_in_namespace_extends_sibling() {
    let edges = indexed_edges(
        &[(
            "a.ts",
            "namespace NS {\n  export interface Base {}\n  export interface Child extends Base {}\n}\n",
        )],
        "EXTENDS",
    );
    assert!(
        edges.contains(&("a.NS.Child".into(), "a.NS.Base".into())),
        "{edges:?}"
    );
}

#[test]
fn class_generic_heritage_strips_type_arguments() {
    let f = extract(
        "class Base<T> {}\ninterface I<T> {}\nclass C extends Base<string> implements I<number> {}\n",
        "m",
    );
    let of = |kind: &str| -> Vec<&str> {
        f.edges
            .iter()
            .filter(|e| e.kind == kind && e.source_qualname.as_deref() == Some("m.C"))
            .filter_map(|e| e.target_qualname.as_deref())
            .collect()
    };
    assert_eq!(of("EXTENDS"), vec!["Base"]);
    assert_eq!(of("IMPLEMENTS"), vec!["I"]);
}

#[test]
fn javascript_class_fields_and_anonymous_default() {
    use lidx::indexer::javascript::JavascriptExtractor;
    let f = JavascriptExtractor::new()
        .unwrap()
        .extract(
            "export default class {\n  count = 0;\n  static s = 1;\n  #p = 2;\n  run() {}\n}\n",
            "m",
        )
        .unwrap();
    assert_eq!(sym_kind(&f, "m.default"), Some("class"));
    assert_eq!(sym_kind(&f, "m.default.count"), Some("field"));
    assert_eq!(sym_kind(&f, "m.default.s"), Some("field"));
    assert_eq!(sym_kind(&f, "m.default.#p"), Some("field"));
    assert_eq!(sym_kind(&f, "m.default.run"), Some("method"));
}
