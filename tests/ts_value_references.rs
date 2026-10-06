//! A function or const used as a value (not called) counts as a use: an
//! in-repo import or a same-file module-level declaration passed, assigned,
//! returned or picked in a ternary / `??` emits a `CALLS` edge from the
//! enclosing symbol.

mod common;

use common::call_targets;

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
    let mut t = call_targets(FILES, "src/hono-base.Hono.constructor");
    t.sort();
    assert_eq!(
        t,
        ["src/utils/url.getPath", "src/utils/url.getPathNoStrict"],
        "{t:?}"
    );
}

#[test]
fn nullish_fallback_to_an_imported_function_is_a_reference() {
    let t = call_targets(FILES, "src/serve.serve");
    assert!(t.contains(&"src/path.defaultJoin".to_string()), "{t:?}");
}

#[test]
fn same_file_const_is_referenced_once_per_site_and_externals_stay_out() {
    let mut t = call_targets(FILES, "src/local.setup");
    t.sort();
    // `register(handler)` resolves; `useState` (react) and `Math.max` emit nothing.
    assert!(t.contains(&"src/local.handler".to_string()), "{t:?}");
    assert!(t.contains(&"src/local.register".to_string()), "{t:?}");
    assert!(!t.iter().any(|q| q.contains("useState")), "{t:?}");
    assert!(!t.iter().any(|q| q.contains("max")), "{t:?}");
}

#[test]
fn locals_shadow_imports_and_calls_are_not_doubled() {
    let t = call_targets(FILES, "src/local.shadowed");
    assert!(!t.iter().any(|q| q.contains("getPath")), "{t:?}");
    let t = call_targets(FILES, "src/local.calledToo");
    assert_eq!(
        t.iter()
            .filter(|q| q.as_str() == "src/utils/url.getPath")
            .count(),
        1,
        "{t:?}"
    );
    // One call plus two references on another line: the same-line pair counts once.
    let t = call_targets(FILES, "src/local.called");
    assert_eq!(
        t.iter()
            .filter(|q| q.as_str() == "src/local.handler")
            .count(),
        2,
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
fn classes_and_exported_object_surfaces_are_not_references() {
    let mut t = call_targets(SURFACES, "src/use.run");
    t.sort();
    assert_eq!(
        t,
        [
            "src/err.op",
            "src/use.check",
            "src/use.check",
            "src/use.check"
        ],
        "{t:?}"
    );
    let t = call_targets(SURFACES, "src/use.Api");
    assert!(t.is_empty(), "{t:?}");
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
    let t = call_targets(files, "src/api.Api");
    assert!(t.contains(&"src/h.helper".to_string()), "{t:?}");
    // `settings.port` reads the object: a use, not a call.
    assert!(!t.contains(&"src/h.settings".to_string()), "{t:?}");
}
