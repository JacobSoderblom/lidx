//! A function or const used as a value (not called) counts as a use: an
//! in-repo import or a same-file module-level declaration passed, assigned,
//! returned or picked in a ternary / `??` emits a `CALLS` edge from the
//! enclosing symbol.

mod common;

use common::{edge_targets, ref_targets};

const FILES: &[(&str, &str)] = &[
    (
        "src/utils/url.ts",
        "export const getPath = (r: string) => r;\nexport const getPathNoStrict = (r: string) => r.trim();\n",
    ),
    (
        "src/hono-base.ts",
        "import { getPath, getPathNoStrict } from './utils/url';\nexport class Hono {\n  getPath: (r: string) => string;\n  constructor(options: { getPath?: (r: string) => string }, strict?: boolean) {\n    this.getPath = (strict ?? true) ? (options.getPath ?? getPath) : getPathNoStrict;\n  }\n}\n",
    ),
    (
        "src/path.ts",
        "export const defaultJoin = (...p: string[]) => p.join('/');\n",
    ),
    (
        "src/serve.ts",
        "import { defaultJoin } from './path';\nexport const serve = (options: { join?: (...p: string[]) => string }) => {\n  const join = options.join ?? defaultJoin;\n  return join('a', 'b');\n};\n",
    ),
    (
        "src/local.ts",
        "import { useState } from 'react';\nimport { getPath } from './utils/url';\nconst handler = () => 1;\nfunction register(cb: unknown) {}\nexport function setup() {\n  register(handler);\n  register(useState);\n  register(Math.max);\n}\nexport function shadowed(getPath: () => void) {\n  register(getPath);\n}\nexport function called() {\n  handler();\n  return [handler, handler];\n}\nexport function calledToo() {\n  return getPath('x');\n}\n",
    ),
];

#[test]
fn ternary_and_nullish_operands_are_references() {
    let mut t = ref_targets(FILES, "src/hono-base.Hono.constructor");
    t.sort();
    assert_eq!(
        t,
        ["src/utils/url.getPath", "src/utils/url.getPathNoStrict"],
        "{t:?}"
    );
}

#[test]
fn nullish_fallback_to_an_imported_function_is_a_reference() {
    let t = ref_targets(FILES, "src/serve.serve");
    assert!(t.contains(&"src/path.defaultJoin".to_string()), "{t:?}");
}

#[test]
fn same_file_const_is_referenced_once_per_site_and_externals_stay_out() {
    let mut t = ref_targets(FILES, "src/local.setup");
    t.sort();
    // `register(handler)` resolves; `useState` (react) and `Math.max` emit nothing.
    assert_eq!(t, ["src/local.handler"], "{t:?}");
    let calls = edge_targets(FILES, "src/local.setup", "CALLS");
    assert!(
        calls.contains(&"src/local.register".to_string()),
        "{calls:?}"
    );
    assert!(!t.iter().any(|q| q.contains("useState")), "{t:?}");
    assert!(!t.iter().any(|q| q.contains("max")), "{t:?}");
}

#[test]
fn locals_shadow_imports_and_calls_are_not_doubled() {
    let t = ref_targets(FILES, "src/local.shadowed");
    assert!(!t.iter().any(|q| q.contains("getPath")), "{t:?}");
    let t = edge_targets(FILES, "src/local.calledToo", "CALLS");
    assert_eq!(
        t.iter()
            .filter(|q| q.as_str() == "src/utils/url.getPath")
            .count(),
        1,
        "{t:?}"
    );
    // One call plus two references on another line: the pair has a CALLS edge,
    // so it keeps no REFERENCES edge at all.
    let t = ref_targets(FILES, "src/local.called");
    assert!(t.is_empty(), "{t:?}");
    let t = edge_targets(FILES, "src/local.called", "CALLS");
    assert_eq!(
        t.iter()
            .filter(|q| q.as_str() == "src/local.handler")
            .count(),
        1,
        "{t:?}"
    );
}

#[test]
fn externals_do_not_flood_unresolved_references() {
    let (_tmp, root, db_path) = common::index_repo("lidx-ts-valref-", FILES);
    let indexer = lidx::indexer::Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM unresolved_references WHERE graph_version = ?
               AND (reference_name LIKE '%useState%' OR import_candidates LIKE '%useState%')",
            [gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 0);
}

const SURFACES: &[(&str, &str)] = &[
    (
        "src/err.ts",
        "export class Failure extends Error {}\nexport const op = () => 1;\n",
    ),
    (
        "src/use.ts",
        "import { Failure, op } from './err';\nclass Local {}\nconst check = (x: unknown) => x;\nexport function run(e: unknown) {\n  check(Failure);\n  check(Local);\n  check(op);\n}\nexport const Api = { op, check };\n",
    ),
];

#[test]
fn classes_are_references_and_exported_object_tables_are_too() {
    // `check(Failure)` / `check(Local)` pass classes, which are values
    // (DI registration, `new Map([[k, Foo]])`); `check(op)` a function. The
    // three `check(..)` calls are CALLS edges.
    let t = ref_targets(SURFACES, "src/use.run");
    assert_eq!(
        t,
        ["src/err.Failure", "src/err.op", "src/use.Local"],
        "{t:?}"
    );
    let t = edge_targets(SURFACES, "src/use.run", "CALLS");
    assert_eq!(t, ["src/use.check"; 3], "{t:?}");
    // `export const Api = { op, check }` is a table the compiler reads `op`
    // and `check` in: references from the module.
    let t = ref_targets(SURFACES, "src/use");
    assert_eq!(t, ["src/err.op", "src/use.check"], "{t:?}");
}

#[test]
fn functions_inside_an_exported_object_still_use_what_they_call() {
    let files: &[(&str, &str)] = &[
        (
            "src/h.ts",
            "export const helper = () => 1;\nexport const settings = { port: 1 };\n",
        ),
        (
            "src/api.ts",
            "import { helper, settings } from './h';\nexport const Api = { run: () => take(helper), port: settings.port };\ndeclare function take(f: unknown): void;\n",
        ),
    ];
    // `Api.run` owns what its body uses (#426).
    let t = ref_targets(files, "src/api.Api.run");
    assert!(t.contains(&"src/h.helper".to_string()), "{t:?}");
    // `settings.port` reads the object: a use, not a call.
    let t = ref_targets(files, "src/api.Api");
    assert!(!t.contains(&"src/h.settings".to_string()), "{t:?}");
}

const TABLES: &[(&str, &str)] = &[
    (
        "src/jwt.ts",
        "export function sign() {}\nexport function verify() {}\nexport function decode() {}\n",
    ),
    (
        "src/table.ts",
        r#"import { sign, verify, decode } from './jwt';
function numberProcessor() {}
function setProcessor() {}
export const Jwt = { sign, verify, decode };
export const allProcessors: Record<string, unknown> = {
  number: numberProcessor,
  set: setProcessor,
};
export function build() {
  const createContext = () => 1;
  const getOrCreate = () => 2;
  return { createContext, getOrCreate, other: 3 };
}
"#,
    ),
];

#[test]
fn module_level_object_tables_reference_their_entries() {
    let mut t = ref_targets(TABLES, "src/table");
    t.sort();
    assert_eq!(
        t,
        [
            "src/jwt.decode",
            "src/jwt.sign",
            "src/jwt.verify",
            "src/table.numberProcessor",
            "src/table.setProcessor",
        ],
        "{t:?}"
    );
}

#[test]
fn shorthand_entries_name_the_function_declared_in_the_enclosing_function() {
    let mut t = ref_targets(TABLES, "src/table.build");
    t.sort();
    assert_eq!(
        t,
        [
            "src/table.build.createContext",
            "src/table.build.getOrCreate"
        ],
        "{t:?}"
    );
}

const MEMBERS: &[(&str, &str)] = &[
    (
        "src/types.ts",
        r#"export class ZodNumber {
  static create = (x: number) => x;
  static make() { return 1; }
  parse() { return 1; }
  safeParse() { return 2; }
  constructor() {
    this.parse = this.parse.bind(this);
    this.safeParse = this.safeParse.bind(this);
  }
}
export class Unpromise {
  static resolve(x: number) { return x; }
}
export class Immer {
  produce = (base: number) => base;
  createDraft(x: number) { return x; }
}
"#,
    ),
    (
        "src/use.ts",
        r#"import { ZodNumber, Unpromise, Immer } from './types';
const immer = new Immer();
export const numberType = ZodNumber.create;
export const makeType = ZodNumber.make;
export const produce = immer.produce;
export const createDraft = immer.createDraft.bind(immer);
export function race(values: number[]) {
  return values.map(Unpromise.resolve);
}
export function called() {
  return ZodNumber.make();
}
"#,
    ),
];

#[test]
fn bound_members_in_the_constructor_are_references() {
    let mut t = ref_targets(MEMBERS, "src/types.ZodNumber.constructor");
    t.sort();
    assert_eq!(
        t,
        ["src/types.ZodNumber.parse", "src/types.ZodNumber.safeParse"],
        "{t:?}"
    );
}

#[test]
fn static_and_typed_member_reads_are_references() {
    let mut t = ref_targets(MEMBERS, "src/use");
    t.sort();
    assert_eq!(
        t,
        [
            "src/types.Immer.createDraft",
            "src/types.Immer.produce",
            "src/types.ZodNumber.create",
            "src/types.ZodNumber.make",
        ],
        "{t:?}"
    );
    let t = ref_targets(MEMBERS, "src/use.race");
    assert_eq!(t, ["src/types.Unpromise.resolve"], "{t:?}");
    // A call is a call: no reference for the pair.
    let t = ref_targets(MEMBERS, "src/use.called");
    assert!(t.is_empty(), "{t:?}");
}

const LOCALS: &[(&str, &str)] = &[
    (
        "src/lib.ts",
        "export function lazy() {}\nexport function isDir() {}\n",
    ),
    (
        "src/local.ts",
        r#"import { lazy, isDir } from './lib';
export function run(p: Promise<void>, res: any) {
  const handleError = (e: unknown) => e;
  function onDrain() {}
  function wrapped() {}
  p.catch(handleError);
  res.off('drain', onDrain);
  return wrapped;
}
export function selfRef(signal: any) {
  const onAbort = () => {
    signal.removeEventListener('abort', onAbort);
  };
  signal.addEventListener('abort', onAbort);
}
export function shadow() {
  const isDir = (path: string) => {
    let isDir;
    try {
      isDir = path;
    } catch {}
    return isDir;
  };
  const lazy: Record<string, number> = {};
  delete lazy['a'];
  return isDir;
}
"#,
    ),
];

#[test]
fn local_functions_used_as_callbacks_are_references() {
    let mut t = ref_targets(LOCALS, "src/local.run");
    t.sort();
    assert_eq!(
        t,
        [
            "src/local.run.handleError",
            "src/local.run.onDrain",
            "src/local.run.wrapped"
        ],
        "{t:?}"
    );
}

#[test]
fn a_local_function_may_name_itself() {
    let t = ref_targets(LOCALS, "src/local.selfRef.onAbort");
    assert_eq!(t, ["src/local.selfRef.onAbort"], "{t:?}");
    let t = ref_targets(LOCALS, "src/local.selfRef");
    assert_eq!(t, ["src/local.selfRef.onAbort"], "{t:?}");
}

#[test]
fn locals_inside_a_local_function_shadow_it_and_the_import() {
    // `return isDir` inside `isDir` is its own `let isDir`; `lazy` is a record.
    let t = ref_targets(LOCALS, "src/local.shadow.isDir");
    assert!(t.is_empty(), "{t:?}");
    let t = ref_targets(LOCALS, "src/local.shadow");
    assert_eq!(t, ["src/local.shadow.isDir"], "{t:?}");
    let t = edge_targets(LOCALS, "src/local.shadow", "USES");
    assert!(!t.iter().any(|q| q.ends_with("lib.lazy")), "{t:?}");
}

const WRONG: &[(&str, &str)] = &[
    (
        "src/checks.ts",
        "export function minLength(n: number) { return n; }\nexport function bigint() {}\nexport function retry() {}\nexport const DEFAULTS = { a: 1 };\n",
    ),
    (
        "src/model.ts",
        "export class Model {\n  private _closed = false;\n  get closed() { return this._closed; }\n  get shape() { return {}; }\n  set shape(v: unknown) {}\n  copy() { return this.shape; }\n  check() { return this.closed; }\n}\n",
    ),
    (
        "src/use.ts",
        r#"import * as checks from './checks';
import { bigint, retry, DEFAULTS } from './checks';
import { Model } from './model';
export function viaCast(...args: unknown[]) {
  return (checks.minLength as any)(...args);
}
export function viaParens(n: number) {
  return ((checks.minLength))(n);
}
export function read(m: Model) {
  return m.shape;
}
export interface Options {
  readonly retry: typeof retry;
  value: bigint;
  keys: keyof typeof DEFAULTS;
}
export type R = ReturnType<typeof retry>;
"#,
    ),
];

#[test]
fn a_call_through_a_cast_is_a_call_not_a_reference() {
    let calls = edge_targets(WRONG, "src/use.viaCast", "CALLS");
    assert_eq!(calls, ["src/checks.minLength"], "{calls:?}");
    let calls = edge_targets(WRONG, "src/use.viaParens", "CALLS");
    assert_eq!(calls, ["src/checks.minLength"], "{calls:?}");
    for caller in ["src/use.viaCast", "src/use.viaParens"] {
        for kind in ["REFERENCES", "USES"] {
            let t = edge_targets(WRONG, caller, kind);
            assert!(!t.iter().any(|q| q.contains("minLength")), "{kind} {t:?}");
        }
    }
}

#[test]
fn accessor_reads_are_not_references_to_the_accessor() {
    for caller in [
        "src/model.Model.copy",
        "src/model.Model.check",
        "src/use.read",
    ] {
        let t = ref_targets(WRONG, caller);
        assert!(t.is_empty(), "{caller}: {t:?}");
    }
}

#[test]
fn type_positions_do_not_reference_functions() {
    let mut t = edge_targets(WRONG, "src/use.Options", "USES");
    t.sort();
    // `typeof retry` and the `bigint` keyword name no function; the constant
    // behind `keyof typeof DEFAULTS` is still used.
    assert!(!t.iter().any(|q| q.ends_with(".retry")), "{t:?}");
    assert!(!t.iter().any(|q| q.ends_with(".bigint")), "{t:?}");
    assert!(t.iter().any(|q| q.ends_with(".DEFAULTS")), "{t:?}");
    let t = edge_targets(WRONG, "src/use", "USES");
    assert!(!t.iter().any(|q| q.ends_with(".retry")), "{t:?}");
}
