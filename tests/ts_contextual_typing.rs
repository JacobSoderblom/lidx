//! TypeScript contextual typing (#431): callback parameters typed by the
//! callable type they are written against (a function type alias through an
//! import, an annotated const, a declared return type, the callee's declared
//! parameter), and functions declared inside functions indexed as symbols
//! qualified under their parent. The incremental tests prove sync == fresh
//! reindex.

mod common;

use common::{call_targets, golden};
use lidx::indexer::Indexer;

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

const TYPES: &str = r#"import type { Context } from './ctx';
export type Next = () => Promise<void>;
export type Handler<E = any> = (c: Context<E>, next: Next) => Response | Promise<Response>;
export type MiddlewareHandler<E = any> = (c: Context<E>, next: Next) => Promise<Response | void>;
export type H<E = any> = Handler<E> | MiddlewareHandler<E>;
export type NotFoundHandler<E = any> = (c: Context<E>) => Response | Promise<Response>;
export interface ErrorLike { (err: Error, c: Context): Response }
"#;
const CTX: &str = r#"import { HonoRequest } from './req';
export class Context<E = any> {
  get req(): HonoRequest { return new HonoRequest(); }
  header(name: string, value: string): void {}
  text(body: string): Response { return new Response(body); }
}
"#;
const REQ: &str = r#"export class HonoRequest {
  header(name: string): string | undefined { return name; }
  query(name: string): string | undefined { return name; }
}
"#;
const DECOY: &str = r#"export class Decoy {
  header(name: string, value: string): void {}
  text(body: string): string { return body; }
  query(name: string): number { return 1; }
}
"#;
const FACTORY: &str = r#"import type { MiddlewareHandler, H } from './types';
export const createMiddleware = <E = any>(middleware: MiddlewareHandler<E>): MiddlewareHandler<E> => middleware;
export function use(...handlers: H[]): void {}
"#;
const USE: &str = r#"import type { MiddlewareHandler, NotFoundHandler } from './types';
import { createMiddleware, use } from './factory';

export const mw = createMiddleware(async (c, next) => {
  c.header('x', 'y');
  const q = c.req.query('q');
  await next();
});

export const notFound: NotFoundHandler = (c) => c.text('404');

export const cors = (): MiddlewareHandler => {
  return async function cors(c, next) {
    c.header('a', 'b');
    await next();
  };
};

export const viaExpression = (): MiddlewareHandler => async (c, next) => {
  c.req.header('h');
};

use(async (c, next) => {
  c.text('x');
});

export const asserted = ((c) => c.text('z')) as NotFoundHandler;
"#;

fn files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("src/types.ts", TYPES),
        ("src/ctx.ts", CTX),
        ("src/req.ts", REQ),
        ("src/decoy.ts", DECOY),
        ("src/factory.ts", FACTORY),
        ("src/use.ts", USE),
    ]
}

#[test]
fn callback_passed_to_a_function_takes_the_declared_parameter_type() {
    assert_eq!(
        sorted(call_targets(&files(), "src/use.mw")),
        ["src/ctx.Context.header", "src/req.HonoRequest.query"]
    );
}

#[test]
fn const_annotated_with_a_function_alias_types_its_parameters() {
    assert_eq!(
        call_targets(&files(), "src/use.notFound"),
        ["src/ctx.Context.text"]
    );
}

#[test]
fn callback_returned_from_a_function_declared_to_return_the_alias_is_typed() {
    assert_eq!(
        call_targets(&files(), "src/use.cors"),
        ["src/ctx.Context.header"]
    );
    assert_eq!(
        call_targets(&files(), "src/use.viaExpression"),
        ["src/req.HonoRequest.header"]
    );
}

#[test]
fn rest_parameter_and_union_alias_and_assertion_type_a_callback() {
    let t = sorted(call_targets(&files(), "src/use"));
    assert!(t.contains(&"src/ctx.Context.text".to_string()), "{t:?}");
    assert_eq!(
        call_targets(&files(), "src/use.asserted"),
        ["src/ctx.Context.text"]
    );
}

#[test]
fn disagreeing_alias_declarations_leave_the_parameter_untyped() {
    let mut f = files();
    f.push((
        "src/other.ts",
        "import type { Decoy } from './decoy';\nexport type NotFoundHandler = (c: Decoy) => void;\n",
    ));
    let t = call_targets(&f, "src/use.notFound");
    assert!(
        t.iter()
            .all(|t| !t.starts_with("src/ctx.") && !t.starts_with("src/decoy.")),
        "{t:?}"
    );
}

#[test]
fn an_unknown_callee_does_not_type_its_callback() {
    let f = [
        ("src/ctx.ts", CTX),
        ("src/req.ts", REQ),
        ("src/decoy.ts", DECOY),
        (
            "src/app.ts",
            "declare function register(cb: unknown): void;\nexport function run() {\n  register((c) => c.header('a', 'b'));\n  [1].forEach((c) => c.text('z'));\n}\n",
        ),
    ];
    let t = call_targets(&f, "src/app.run");
    assert!(
        t.iter()
            .all(|t| !t.starts_with("src/ctx.") && !t.starts_with("src/decoy.")),
        "{t:?}"
    );
}

const LOCALS: &str = r#"function helper() {}
function other() {}
export function outer() {
  const f = () => helper();
  function g() {
    f();
    other();
  }
  g();
  f();
  [1].forEach((n) => {
    const inner = () => g();
    inner();
  });
}
export class Box {
  run() {
    const step = () => helper();
    step();
  }
}
"#;

fn dedup(v: Vec<String>) -> Vec<String> {
    let mut v = sorted(v);
    v.dedup();
    v
}

#[test]
fn local_functions_are_symbols_qualified_under_their_parent() {
    let f = [("src/loc.ts", LOCALS)];
    let outer = dedup(call_targets(&f, "src/loc.outer"));
    for want in ["src/loc.outer.f", "src/loc.outer.g", "src/loc.outer.inner"] {
        assert!(outer.contains(&want.to_string()), "{want} in {outer:?}");
    }
    assert_eq!(call_targets(&f, "src/loc.outer.f"), ["src/loc.helper"]);
    assert_eq!(
        dedup(call_targets(&f, "src/loc.outer.g")),
        ["src/loc.other", "src/loc.outer.f"]
    );
    assert_eq!(call_targets(&f, "src/loc.outer.inner"), ["src/loc.outer.g"]);
    assert_eq!(call_targets(&f, "src/loc.Box.run.step"), ["src/loc.helper"]);
    assert!(call_targets(&f, "src/loc.Box.run").contains(&"src/loc.Box.run.step".to_string()));
}

#[test]
fn duplicate_local_names_get_no_symbol() {
    let src = "function helper() {}\nexport function outer(flag: boolean) {\n  if (flag) {\n    const f = () => helper();\n    f();\n  } else {\n    const f = () => 1;\n    f();\n  }\n}\n";
    let f = [("src/dup.ts", src)];
    let t = call_targets(&f, "src/dup.outer");
    assert!(t.iter().all(|t| !t.starts_with("src/dup.outer.")), "{t:?}");
}

fn snapshot_equals_fresh(indexer: &Indexer, final_files: &[(&str, &str)]) {
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(final_files);
    common::assert_matches_fresh(&snapshot, &fresh);
}

fn live(files: &[(&str, &str)]) -> (tempfile::TempDir, std::path::PathBuf, Indexer) {
    common::live_indexer("lidx-ts-ctx-", files)
}

fn targets(indexer: &Indexer, caller: &str) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(t.qualname, e.target_qualname, '') FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.qualname = ?",
        )
        .unwrap();
    let rows = stmt
        .query_map(rusqlite::params![gv, caller], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    dedup(rows)
}

#[test]
fn editing_the_parent_of_local_functions_matches_a_fresh_index() {
    let base = [("src/loc.ts", LOCALS)];
    let (_tmp, root, mut indexer) = live(&base);
    assert_eq!(targets(&indexer, "src/loc.outer.f"), ["src/loc.helper"]);
    snapshot_equals_fresh(&indexer, &base);

    // The local function is renamed: the old symbol and its edges go.
    let renamed = LOCALS
        .replace("const f = () => helper();", "const f2 = () => other();")
        .replace("f();", "f2();");
    common::write_files(&root, &[("src/loc.ts", &renamed)]);
    indexer.sync_rel_paths(&["src/loc.ts".to_string()]).unwrap();
    assert!(targets(&indexer, "src/loc.outer.f").is_empty());
    assert_eq!(targets(&indexer, "src/loc.outer.f2"), ["src/loc.other"]);
    snapshot_equals_fresh(&indexer, &[("src/loc.ts", &renamed)]);

    // A second local of the same name makes the name ambiguous.
    let twin = format!(
        "{renamed}export function twin() {{\n  const a = () => 1;\n  a();\n  {{ const a = () => 2; a(); }}\n}}\n"
    );
    common::write_files(&root, &[("src/loc.ts", &twin)]);
    indexer.sync_rel_paths(&["src/loc.ts".to_string()]).unwrap();
    snapshot_equals_fresh(&indexer, &[("src/loc.ts", &twin)]);

    // Back to the original.
    common::write_files(&root, &[("src/loc.ts", LOCALS)]);
    indexer.sync_rel_paths(&["src/loc.ts".to_string()]).unwrap();
    assert_eq!(targets(&indexer, "src/loc.outer.f"), ["src/loc.helper"]);
    snapshot_equals_fresh(&indexer, &base);
}

#[test]
fn editing_the_alias_file_retypes_the_callbacks_like_a_fresh_index() {
    let base = files();
    let (_tmp, root, mut indexer) = live(&base);
    assert_eq!(
        targets(&indexer, "src/use.notFound"),
        ["src/ctx.Context.text"]
    );
    snapshot_equals_fresh(&indexer, &base);

    // The alias now hands its callback a different class.
    let retyped = TYPES.replace(
        "export type NotFoundHandler<E = any> = (c: Context<E>) =>",
        "export type NotFoundHandler<E = any> = (c: import('./decoy').Decoy) =>",
    );
    let retyped = format!(
        "import type {{ Decoy }} from './decoy';\n{}",
        retyped.replace("import('./decoy').Decoy", "Decoy")
    );
    common::write_files(&root, &[("src/types.ts", &retyped)]);
    indexer
        .sync_rel_paths(&["src/types.ts".to_string()])
        .unwrap();
    assert_eq!(
        targets(&indexer, "src/use.notFound"),
        ["src/decoy.Decoy.text"]
    );
    let mut after = files();
    after[0] = ("src/types.ts", &retyped);
    snapshot_equals_fresh(&indexer, &after);

    // The alias disappears: the callback is untyped again.
    let gone = TYPES.replace(
        "export type NotFoundHandler<E = any> = (c: Context<E>) => Response | Promise<Response>;\n",
        "",
    );
    common::write_files(&root, &[("src/types.ts", &gone)]);
    indexer
        .sync_rel_paths(&["src/types.ts".to_string()])
        .unwrap();
    assert!(
        targets(&indexer, "src/use.notFound")
            .iter()
            .all(|t| !t.starts_with("src/ctx.") && !t.starts_with("src/decoy."))
    );
    let mut after = files();
    after[0] = ("src/types.ts", &gone);
    snapshot_equals_fresh(&indexer, &after);

    // And it comes back.
    common::write_files(&root, &[("src/types.ts", TYPES)]);
    indexer
        .sync_rel_paths(&["src/types.ts".to_string()])
        .unwrap();
    assert_eq!(
        targets(&indexer, "src/use.notFound"),
        ["src/ctx.Context.text"]
    );
    snapshot_equals_fresh(&indexer, &base);
}

const HIDDEN: &str = r#"export function widget() {
  const unusedLocal = () => 1;
  function neverCalled() {}
  const used = () => 2;
  return used();
}
export function deadTopLevel() {}
"#;

fn rpc_call(files: &[(&str, &str)], method: &str, params: serde_json::Value) -> serde_json::Value {
    let (_tmp, root, db_path) = common::index_repo("lidx-ts-ctx-rpc-", files);
    let mut indexer = Indexer::new(root, db_path).unwrap();
    lidx::rpc::handle_method(&mut indexer, method, params).unwrap()
}

#[test]
fn local_functions_are_not_dead_and_not_listed_at_the_top_level() {
    let files = [
        ("src/hidden.ts", HIDDEN),
        (
            "src/user.ts",
            "import { widget } from './hidden';\nwidget();\n",
        ),
    ];
    let dead = rpc_call(
        &files,
        "dead_symbols",
        serde_json::json!({"include_unused_imports": false, "include_orphan_tests": false}),
    );
    let dead: Vec<&str> = dead["dead_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["qualname"].as_str())
        .collect();
    assert!(dead.contains(&"src/hidden.deadTopLevel"), "{dead:?}");
    assert!(
        dead.iter().all(|d| !d.starts_with("src/hidden.widget.")),
        "{dead:?}"
    );

    let map = rpc_call(&files, "repo_map", serde_json::json!({}));
    let text = map["text"].as_str().unwrap();
    assert!(
        !text.contains("unusedLocal") && !text.contains("neverCalled"),
        "{text}"
    );
    assert!(!text.contains("**used**"), "{text}");

    // The outline still shows them, nested under their parent.
    let outline = rpc_call(
        &files,
        "outline",
        serde_json::json!({"path": "src/hidden.ts"}),
    );
    let entries = outline["entries"]
        .as_array()
        .or_else(|| outline["symbols"].as_array())
        .unwrap();
    let local = entries
        .iter()
        .find(|e| e["qualname"] == "src/hidden.widget.used")
        .unwrap_or_else(|| panic!("{outline}"));
    assert_eq!(local["parent"], "src/hidden.widget");
}

#[test]
fn a_local_function_never_stands_in_for_a_member_of_a_same_named_type() {
    let src = r#"export interface $constructor { init(a: number): void }
export function $constructor(name: string): $constructor {
  function init(a: number) {}
  return { init } as $constructor;
}
export function user(x: $constructor) {
  x.init(1);
}
"#;
    let (_tmp, _root, indexer) = live(&[("src/ctor.ts", src)]);
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let bound: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.qualname = 'src/ctor.user'",
            [gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(bound, 0);
}

#[test]
fn an_interface_with_one_call_signature_types_its_callback() {
    let f = [
        ("src/ctx.ts", CTX),
        ("src/req.ts", REQ),
        ("src/decoy.ts", DECOY),
        (
            "src/h.ts",
            "import type { Context } from './ctx';\nexport interface Handler { (c: Context): Response }\n",
        ),
        (
            "src/use.ts",
            "import type { Handler } from './h';\nexport const h: Handler = (c) => c.text('x');\n",
        ),
    ];
    assert_eq!(call_targets(&f, "src/use.h"), ["src/ctx.Context.text"]);
}
