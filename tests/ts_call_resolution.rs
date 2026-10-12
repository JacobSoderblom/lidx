//! TypeScript call resolution (#426): bare callees qualified by module and
//! refused when locally bound, merged type + value declarations, static
//! member calls, object-literal methods, runtime global objects, generic
//! and arrow-const return types. Each fixture asserts the exact edge or its
//! absence; the two incremental tests prove sync == fresh reindex.

mod common;

use common::call_targets;
use lidx::indexer::Indexer;

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
    let t = sorted(call_targets(
        &[("src/merge.ts", MERGE)],
        "src/merge.useMerged",
    ));
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
        [
            "src/statics.ZodNumber.create",
            "src/statics.ZodString.create"
        ]
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
    // The local arrow is a symbol of its own: the call binds to it, never to
    // the import or the `res` getter.
    assert_eq!(
        call_targets(&f, "src/shadow.Context.html"),
        ["src/shadow.Context.html.res"]
    );
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

const SUPER_BASE: &str = "export class HonoBase {\n  constructor(options?: number) {}\n  fetch(): number { return 1; }\n}\n";
const SUPER_DERIVED: &str = r#"import { HonoBase as Base } from './base';
class Local {
  constructor(n: number) {}
}
export class Hono extends Base {
  constructor(options?: number) {
    super(options);
  }
}
export class Child extends Local {
  constructor() {
    super(1);
  }
}
export function run(app: Hono) {
  return app.fetch();
}
"#;

#[test]
fn super_calls_construct_the_base_and_aliased_bases_resolve() {
    let f = [
        ("src/base.ts", SUPER_BASE),
        ("src/derived.ts", SUPER_DERIVED),
    ];
    assert_eq!(
        call_targets(&f, "src/derived.Hono.constructor"),
        ["src/base.HonoBase.constructor"]
    );
    assert_eq!(
        call_targets(&f, "src/derived.Child.constructor"),
        ["src/derived.Local.constructor"]
    );
    // `Hono extends Base` (an aliased import) reaches HonoBase.fetch.
    assert_eq!(
        call_targets(&f, "src/derived.run"),
        ["src/base.HonoBase.fetch"]
    );
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
    let (_tmp, root, mut indexer) = common::live_indexer("lidx-ts-merge-inc-", &files);
    for caller in ["src/caller.run", "src/local.localRun"] {
        assert_eq!(target_kind(&indexer, caller).as_deref(), Some("function"));
    }

    let edited = "export type Status = { a: number };\n";
    let edited_local =
        "type Local = { a: number };\nexport function localRun() { return Local(); }\n";
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
    let caller =
        "import { Shape } from './shapes';\nexport function run() { return Shape.create(); }\n";
    let own = "export class Own {\n  static make(): Own { return new Own(); }\n}\nexport function useOwn() { return Own.make(); }\n";
    let files = [
        ("src/shapes.ts", shapes),
        ("src/caller.ts", caller),
        ("src/own.ts", own),
    ];
    let (_tmp, root, mut indexer) = common::live_indexer("lidx-ts-static-inc-", &files);
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

/// Sorted `kind`s of the symbols the `edge_kind` edges leaving `caller` bind.
fn target_kinds(files: &[(&str, &str)], caller: &str, edge_kind: &str) -> Vec<String> {
    let (_tmp, root, db_path) = common::index_repo("lidx-ts-kinds-", files);
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.kind FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = ? AND s.qualname = ?",
        )
        .unwrap();
    let kinds = stmt
        .query_map(rusqlite::params![gv, edge_kind, caller], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    sorted(kinds)
}

const LOCAL_SHADOW: &str = r#"function helper() { return 1; }
export function localConst() {
  const helper = () => 2;
  return helper();
}
export function param(helper: () => number) {
  return helper();
}
export function viaModule() {
  return helper();
}
"#;

#[test]
fn a_local_binding_shadows_a_module_declaration() {
    let f = [("src/shadow2.ts", LOCAL_SHADOW)];
    assert_eq!(
        call_targets(&f, "src/shadow2.localConst"),
        ["src/shadow2.localConst.helper"]
    );
    assert!(call_targets(&f, "src/shadow2.param").is_empty());
    assert_eq!(
        call_targets(&f, "src/shadow2.viaModule"),
        ["src/shadow2.helper"]
    );
}

const POSITIONS: &str = r#"type DIRTY = { status: string };
const DIRTY = (value: number): DIRTY => ({ status: String(value) });
interface Shape { n: number }
class Shape { n = 0 }
interface Counter { n: number }
class Counter { n = 0 }
class Sub extends Counter {}
class Impl implements Shape { n = 1 }
interface Wide extends Shape { m: number }
export function typed(d: DIRTY, s: Shape) {
  return [d, s];
}
export function called() {
  DIRTY(1);
  return new Shape();
}
"#;

#[test]
fn merged_declarations_bind_the_half_the_position_means() {
    let f = [("src/pos.ts", POSITIONS)];
    // Type annotations reach the type half ...
    assert_eq!(
        target_kinds(&f, "src/pos.typed", "USES"),
        ["interface", "type"]
    );
    // ... calls and `new` the value half.
    assert_eq!(
        target_kinds(&f, "src/pos.called", "CALLS"),
        ["class", "function"]
    );
    // `class extends` is a value position, `implements` and an interface's
    // `extends` take the type half.
    assert_eq!(target_kinds(&f, "src/pos.Sub", "EXTENDS"), ["class"]);
    assert_eq!(
        target_kinds(&f, "src/pos.Impl", "IMPLEMENTS"),
        ["interface"]
    );
    assert_eq!(target_kinds(&f, "src/pos.Wide", "EXTENDS"), ["interface"]);
}

const CHAIN_REQ: &str = r#"export class HonoRequest {
  header(name: string): string | undefined { return name; }
  json<T>(): Promise<T> { return Promise.resolve({} as T); }
}
export class Decoy {
  header(name: string): string { return name; }
  json(): number { return 1; }
}
"#;
const CHAIN_CTX: &str = r#"import { HonoRequest } from './req';
export class Context {
  get req(): HonoRequest { return new HonoRequest(); }
  get raw(): Context { return this; }
  header = (name: string): void => {};
  json = (o: unknown): void => {};
}
"#;
const CHAIN_USE: &str = r#"import type { Context } from './ctx';
export async function handler(c: Context) {
  const a = c.req.header('x');
  const b = await c.req.json<{ name: string }>();
  const d = (await c.raw.req.json<number>());
  return [a, b, d];
}
export const arrow = (c: Context) => c.req.header('y');
"#;

#[test]
fn typed_parameter_getter_chains_bind_through_declared_return_types() {
    let f = [
        ("src/req.ts", CHAIN_REQ),
        ("src/ctx.ts", CHAIN_CTX),
        ("src/use.ts", CHAIN_USE),
    ];
    let t = sorted(call_targets(&f, "src/use.handler"));
    assert_eq!(
        t,
        [
            "src/req.HonoRequest.header",
            "src/req.HonoRequest.json",
            "src/req.HonoRequest.json"
        ]
    );
    assert_eq!(
        call_targets(&f, "src/use.arrow"),
        ["src/req.HonoRequest.header"]
    );
}
