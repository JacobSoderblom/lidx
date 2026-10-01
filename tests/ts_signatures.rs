//! Issue #215: TypeScript signatures carry type parameters and the return
//! annotation as written, and a const/let bound to an arrow function or
//! function expression is a function with a signature.

mod common;

use lidx::indexer::Indexer;
use lidx::indexer::extract::{ExtractedFile, LanguageExtractor};
use lidx::indexer::javascript::TypescriptExtractor;
use lidx::rpc;

fn extract(source: &str) -> ExtractedFile {
    TypescriptExtractor::new()
        .unwrap()
        .extract(source, "m")
        .unwrap()
}

fn sym<'a>(f: &'a ExtractedFile, qualname: &str) -> (&'a str, Option<&'a str>) {
    let s = f
        .symbols
        .iter()
        .find(|s| s.qualname == qualname)
        .unwrap_or_else(|| panic!("missing {qualname}"));
    (s.kind.as_str(), s.signature.as_deref())
}

#[test]
fn annotated_return_type_is_in_signature() {
    let f = extract(
        "export function classifyCatalogError(err: unknown, deliveryCount = 1): string { return ''; }\n\
         export function createNewestCursor(cursorItem: string): string { return cursorItem; }\n",
    );
    assert_eq!(
        sym(&f, "m.classifyCatalogError"),
        (
            "function",
            Some("(err: unknown, deliveryCount = 1): string")
        )
    );
    assert_eq!(
        sym(&f, "m.createNewestCursor"),
        ("function", Some("(cursorItem: string): string"))
    );
}

#[test]
fn generic_function_includes_type_parameters() {
    let f = extract(
        "export function first<T>(items: T[]): T | undefined { return items[0]; }\n\
         export function bound<T extends object, K extends keyof T>(o: T, k: K): T[K] { return o[k]; }\n",
    );
    assert_eq!(sym(&f, "m.first").1, Some("<T>(items: T[]): T | undefined"));
    assert_eq!(
        sym(&f, "m.bound").1,
        Some("<T extends object, K extends keyof T>(o: T, k: K): T[K]")
    );
}

#[test]
fn arrow_const_is_a_function_with_signature() {
    let f = extract(
        "export const decodeCursor = (c: string): number => 1;\n\
         export let gen = <T,>(x: T): T => x;\n\
         export const bare = x => x;\n",
    );
    assert_eq!(
        sym(&f, "m.decodeCursor"),
        ("function", Some("(c: string): number"))
    );
    assert_eq!(sym(&f, "m.gen"), ("function", Some("<T,>(x: T): T")));
    assert_eq!(sym(&f, "m.bare"), ("function", Some("(x)")));
}

#[test]
fn function_expression_const_behaves_like_arrow() {
    let f = extract(
        "export const fe = function (a: number): string { return ''; };\n\
         export const named = function inner<T>(a: T): T { return a; };\n",
    );
    assert_eq!(sym(&f, "m.fe"), ("function", Some("(a: number): string")));
    assert_eq!(sym(&f, "m.named"), ("function", Some("<T>(a: T): T")));
}

#[test]
fn async_arrow_reflects_annotated_return_type() {
    let f = extract("export const load = async (id: string): Promise<string[]> => [];\n");
    assert_eq!(
        sym(&f, "m.load"),
        ("function", Some("(id: string): Promise<string[]>"))
    );
}

#[test]
fn const_holding_plain_value_keeps_kind_and_has_no_signature() {
    let f = extract(
        "export const cfg = { a: 1 };\nexport const s = 'x';\nexport const n = 5;\n\
         export const call = make();\nlet v = [1];\nexport const { d } = cfg;\n",
    );
    for (q, kind) in [
        ("m.cfg", "const"),
        ("m.s", "const"),
        ("m.n", "const"),
        ("m.call", "const"),
        ("m.v", "variable"),
        ("m.d", "const"),
    ] {
        assert_eq!(sym(&f, q), (kind, None), "{q}");
    }
}

#[test]
fn class_and_object_literal_methods_include_return_types() {
    let f = extract(
        "export class C<T> { m<U>(a: U): T | null { return null; } }\n\
         export abstract class A { abstract foo(x: number): string; }\n\
         export const obj = { m(x: number): string { return ''; }, f: (y: number): void => {}, g: function (z: string): number { return 1; } };\n",
    );
    assert_eq!(sym(&f, "m.C.m"), ("method", Some("<U>(a: U): T | null")));
    assert_eq!(sym(&f, "m.A.foo"), ("method", Some("(x: number): string")));
    assert_eq!(sym(&f, "m.obj"), ("const", None));
    assert_eq!(sym(&f, "m.obj.m"), ("method", Some("(x: number): string")));
    assert_eq!(sym(&f, "m.obj.f"), ("method", Some("(y: number): void")));
    assert_eq!(sym(&f, "m.obj.g"), ("method", Some("(z: string): number")));
}

#[test]
fn unannotated_function_has_no_guessed_return_type() {
    let f = extract(
        "export function a(x: number) { return x; }\nexport const b = (x: number) => x;\n\
         export class C { m(x: number) { return x; } }\n",
    );
    assert_eq!(sym(&f, "m.a").1, Some("(x: number)"));
    assert_eq!(sym(&f, "m.b").1, Some("(x: number)"));
    assert_eq!(sym(&f, "m.C.m").1, Some("(x: number)"));
}

#[test]
fn same_named_arrow_consts_in_one_file_stay_distinct() {
    let (tmp, _) = common::index_files(&[(
        "a.ts",
        "var f = (a: number) => 1;\nvar f = (a: string) => 2;\nvar g = () => 1;\nvar g = () => 2;\n",
    )]);
    let conn = rusqlite::Connection::open(tmp.path().join(".lidx").join(".lidx.sqlite")).unwrap();
    for name in ["f", "g"] {
        let ids: Vec<String> = conn
            .prepare("SELECT stable_id FROM symbols WHERE qualname = ?")
            .unwrap()
            .query_map([format!("a.{name}")], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(ids.len(), 2, "{name}: {ids:?}");
        assert_ne!(ids[0], ids[1], "{name}");
    }
}

#[test]
fn outline_shows_improved_signatures() {
    let (tmp, _) = common::index_files(&[(
        "a.ts",
        "export const decodeCursor = (c: string): number => 1;\nexport function first<T>(items: T[]): T | undefined { return items[0]; }\n",
    )]);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    let result =
        rpc::handle_method(&mut indexer, "outline", serde_json::json!({"path": "a.ts"})).unwrap();
    let text = result.to_string();
    assert!(text.contains("(c: string): number"), "{text}");
    assert!(text.contains("<T>(items: T[]): T | undefined"), "{text}");
    assert!(text.contains("function"), "{text}");
}
