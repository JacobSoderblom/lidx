//! TypeScript call resolution (#426): bare callees qualified by module and
//! refused when locally bound, merged type + value declarations, static
//! member calls, object-literal methods, runtime global objects, generic
//! and arrow-const return types. Each fixture asserts the exact edge or its
//! absence; the two incremental tests prove sync == fresh reindex.

mod common;

use common::call_targets;
use lidx::indexer::Indexer;
use std::path::PathBuf;

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

const MERGE: &str = r#"export interface ZodType { _def: number }
export const ZodType = function (this: ZodType) { return this; } as unknown as { new (): ZodType };
type DIRTY = { status: string };
const DIRTY = (value: number): DIRTY => ({ status: String(value) });
export interface Counter { n: number }
export class Counter { n = 0 }
export function useMerged() {
  DIRTY(1);
  new ZodType();
  new Counter();
}
"#;

#[test]
fn merged_type_and_value_declarations_bind_the_value() {
    let t = sorted(call_targets(&[("src/merge.ts", MERGE)], "src/merge.useMerged"));
    assert_eq!(
        t,
        ["src/merge.Counter", "src/merge.DIRTY", "src/merge.ZodType"]
    );
}

const STATICS: &str = r#"export class ZodString {
  static create(): ZodString { return new ZodString(); }
  static fromParts(): ZodString { return ZodString.create(); }
  clone(): ZodString { return new ZodString(); }
}
export class ZodNumber {
  static create(): ZodNumber { return new ZodNumber(); }
}
export class Context {
  header = (name: string): void => {};
  text = (body: string): string => body;
  render(): string {
    this.header('x');
    return this.text('y');
  }
}
export function client(c: Context) {
  c.header('a');
}
export function useStatics() {
  ZodString.create();
  return ZodNumber.create();
}
"#;

#[test]
fn bare_new_and_static_member_calls_bind_same_file_declarations() {
    let f = [("src/statics.ts", STATICS)];
    assert_eq!(
        call_targets(&f, "src/statics.ZodString.create"),
        ["src/statics.ZodString"]
    );
    assert_eq!(
        call_targets(&f, "src/statics.ZodString.fromParts"),
        ["src/statics.ZodString.create"]
    );
    assert_eq!(
        call_targets(&f, "src/statics.ZodString.clone"),
        ["src/statics.ZodString"]
    );
    assert_eq!(
        sorted(call_targets(&f, "src/statics.useStatics")),
        ["src/statics.ZodNumber.create", "src/statics.ZodString.create"]
    );
}

#[test]
fn arrow_valued_fields_are_methods_callers_can_bind() {
    let f = [("src/statics.ts", STATICS)];
    assert_eq!(
        sorted(call_targets(&f, "src/statics.Context.render")),
        ["src/statics.Context.header", "src/statics.Context.text"]
    );
    assert_eq!(
        call_targets(&f, "src/statics.client"),
        ["src/statics.Context.header"]
    );
}

const OBJLIT: &str = r#"function helper() {}
function other() {}
export const api = {
  get: () => helper(),
  post() { return other(); },
};
export const plain = () => helper();
"#;

#[test]
fn object_literal_methods_own_their_calls() {
    let f = [("src/objlit.ts", OBJLIT)];
    assert_eq!(
        call_targets(&f, "src/objlit.api.get"),
        ["src/objlit.helper"]
    );
    assert_eq!(
        call_targets(&f, "src/objlit.api.post"),
        ["src/objlit.other"]
    );
    assert!(call_targets(&f, "src/objlit.api").is_empty());
    assert_eq!(call_targets(&f, "src/objlit.plain"), ["src/objlit.helper"]);
}

const SHADOW_LIB: &str = "export function res(x: string) { return x; }\n";
const SHADOW: &str = r#"import { res } from './lib';
export class Context {
  get res(): string { return ''; }
  html(text: string) {
    const res = (html: string) => html + text;
    return res(text);
  }
  viaThis() { return this.res; }
}
export function param(res: (x: string) => string) {
  return res('a');
}
export function imported() {
  return res('b');
}
export function nestedShadow(items: string[]) {
  items.map((res) => res.length);
  return res('c');
}
"#;

#[test]
fn locally_bound_callees_are_not_class_members_or_imports() {
    let f = [("src/lib.ts", SHADOW_LIB), ("src/shadow.ts", SHADOW)];
    assert!(call_targets(&f, "src/shadow.Context.html").is_empty());
    assert!(call_targets(&f, "src/shadow.param").is_empty());
    assert_eq!(call_targets(&f, "src/shadow.imported"), ["src/lib.res"]);
    // A callback parameter named like the import does not hide the import.
    assert_eq!(call_targets(&f, "src/shadow.nestedShadow"), ["src/lib.res"]);
}

const GLOBALS: &str = r#"export class Registry {
  parse(x: string) { return x; }
  open() {}
  isFinite(x: number) { return true; }
  createElement() {}
}
export function useGlobals(s: string) {
  JSON.parse(s);
  Number.isFinite(1);
  document.createElement('a');
  caches.open('x');
}
"#;

#[test]
fn runtime_global_receivers_never_bind_repo_methods() {
    let t = sorted(call_targets(
        &[("src/globals.ts", GLOBALS)],
        "src/globals.useGlobals",
    ));
    assert!(
        t.iter().all(|x| !x.starts_with("src/globals.Registry")),
        "{t:?}"
    );
}

const GENERIC: &str = r#"export abstract class Processor<E> {
  createRequest(event: E): string { return String(event); }
}
class LambdaProcessor extends Processor<number> {}
export function getProcessor(event: number): Processor<number> {
  return new LambdaProcessor();
}
export const makeProcessor = (event: number): Processor<number> => new LambdaProcessor();
export function handle(event: number) {
  const processor = getProcessor(event);
  return processor.createRequest(event);
}
export function viaArrow(event: number) {
  return makeProcessor(event).createRequest(event);
}
export function annotated(p: Processor<number>) {
  return p.createRequest(1);
}
"#;

#[test]
fn generic_and_arrow_const_return_types_type_the_receiver() {
    let f = [("src/generic_return.ts", GENERIC)];
    for caller in ["handle", "viaArrow", "annotated"] {
        let t = call_targets(&f, &format!("src/generic_return.{caller}"));
        assert!(
            t.contains(&"src/generic_return.Processor.createRequest".to_string()),
            "{caller}: {t:?}"
        );
    }
}

fn indexed(label: &str, files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix(&format!("lidx-{label}-"))
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, files);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, root, indexer)
}

/// `kind` of the symbol the first `CALLS` edge leaving `caller` is bound to.
fn target_kind(indexer: &Indexer, caller: &str) -> Option<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    conn.query_row(
        "SELECT t.kind FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
         JOIN symbols t ON t.id = e.target_symbol_id
         WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.qualname = ?",
        rusqlite::params![gv, caller],
        |r| r.get(0),
    )
    .ok()
}

fn snapshot(indexer: &Indexer) -> std::collections::BTreeSet<common::golden::EdgeKey> {
    let gv = indexer.db().current_graph_version().unwrap();
    common::golden::snapshot_edges(indexer.db(), gv).unwrap()
}

/// The merged `type X` + `const X` target file is edited in place: the
/// value half is dropped (the call falls back to the lone type) and
/// restored (it binds the value again).
#[test]
fn incremental_merged_declaration_follows_target_edits() {
    let target = "export type Status = { a: number };\nexport const Status = () => 1;\n";
    let caller = "import { Status } from './target';\nexport function run() { return Status(); }\n";
    let local = "type Local = { a: number };\nconst Local = () => 1;\nexport function localRun() { return Local(); }\n";
    let files = [
        ("src/target.ts", target),
        ("src/caller.ts", caller),
        ("src/local.ts", local),
    ];
    let (_tmp, root, mut indexer) = indexed("ts-merge-inc", &files);
    for caller in ["src/caller.run", "src/local.localRun"] {
        assert_eq!(target_kind(&indexer, caller).as_deref(), Some("function"));
    }

    let edited = "export type Status = { a: number };\n";
    let edited_local = "type Local = { a: number };\nexport function localRun() { return Local(); }\n";
    std::fs::write(root.join("src/target.ts"), edited).unwrap();
    std::fs::write(root.join("src/local.ts"), edited_local).unwrap();
    indexer
        .sync_rel_paths(&["src/target.ts".to_string(), "src/local.ts".to_string()])
        .unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    for caller in ["src/caller.run", "src/local.localRun"] {
        assert_ne!(target_kind(&indexer, caller).as_deref(), Some("function"));
    }
    let (_ft, fresh) = common::index_files(&[
        ("src/target.ts", edited),
        ("src/caller.ts", caller),
        ("src/local.ts", edited_local),
    ]);
    common::assert_matches_fresh(&snapshot(&indexer), &fresh);

    std::fs::write(root.join("src/target.ts"), target).unwrap();
    std::fs::write(root.join("src/local.ts"), local).unwrap();
    indexer
        .sync_rel_paths(&["src/target.ts".to_string(), "src/local.ts".to_string()])
        .unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    for caller in ["src/caller.run", "src/local.localRun"] {
        assert_eq!(target_kind(&indexer, caller).as_deref(), Some("function"));
    }
    let (_ft, fresh) = common::index_files(&files);
    common::assert_matches_fresh(&snapshot(&indexer), &fresh);
}

/// A static member call into another file follows edits of the class file.
#[test]
fn incremental_static_member_follows_target_edits() {
    let shapes = "export class Shape {\n  static create(): Shape { return new Shape(); }\n}\n";
    let caller = "import { Shape } from './shapes';\nexport function run() { return Shape.create(); }\n";
    let own = "export class Own {\n  static make(): Own { return new Own(); }\n}\nexport function useOwn() { return Own.make(); }\n";
    let files = [
        ("src/shapes.ts", shapes),
        ("src/caller.ts", caller),
        ("src/own.ts", own),
    ];
    let (_tmp, root, mut indexer) = indexed("ts-static-inc", &files);
    let resolved = |snap: &std::collections::BTreeSet<common::golden::EdgeKey>, src: &str| {
        snap.iter()
            .find(|e| e.source_qualname == src && e.kind == "CALLS" && e.target_qualname.is_some())
            .and_then(|e| e.target_qualname.clone())
    };
    let snap = snapshot(&indexer);
    assert_eq!(
        resolved(&snap, "src/caller.run").as_deref(),
        Some("src/shapes.Shape.create")
    );
    assert_eq!(
        resolved(&snap, "src/own.useOwn").as_deref(),
        Some("src/own.Own.make")
    );

    let renamed = "export class Shape {\n  static build(): Shape { return new Shape(); }\n}\n";
    let renamed_own = "export class Own {\n  static build(): Own { return new Own(); }\n}\nexport function useOwn() { return Own.make(); }\n";
    std::fs::write(root.join("src/shapes.ts"), renamed).unwrap();
    std::fs::write(root.join("src/own.ts"), renamed_own).unwrap();
    indexer
        .sync_rel_paths(&["src/shapes.ts".to_string(), "src/own.ts".to_string()])
        .unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let snap = snapshot(&indexer);
    assert_eq!(resolved(&snap, "src/caller.run"), None);
    assert_eq!(resolved(&snap, "src/own.useOwn"), None);
    let (_ft, fresh) = common::index_files(&[
        ("src/shapes.ts", renamed),
        ("src/caller.ts", caller),
        ("src/own.ts", renamed_own),
    ]);
    common::assert_matches_fresh(&snap, &fresh);

    std::fs::write(root.join("src/shapes.ts"), shapes).unwrap();
    std::fs::write(root.join("src/own.ts"), own).unwrap();
    indexer
        .sync_rel_paths(&["src/shapes.ts".to_string(), "src/own.ts".to_string()])
        .unwrap();
    let snap = snapshot(&indexer);
    assert_eq!(
        resolved(&snap, "src/caller.run").as_deref(),
        Some("src/shapes.Shape.create")
    );
    let (_ft, fresh) = common::index_files(&files);
    common::assert_matches_fresh(&snap, &fresh);
}
