//! A TypeScript/JavaScript function used as a value -- argument, array
//! element, object value (plain and shorthand), return, declarator,
//! assignment, `export default`, `this.method`, JSX attribute -- is a
//! `REFERENCES` edge from the enclosing symbol. Locals, parameters and names
//! already called are not.

mod common;

use common::{edge_targets, ref_targets, unresolved_accounting};

const FILES: &[(&str, &str)] = &[
    (
        "src/lib.ts",
        "export function helper(x: number) { return x; }\nexport function other(x: number) { return x; }\nexport const LIMIT = 10;\n",
    ),
    (
        "src/shapes.ts",
        r#"import { helper, other, LIMIT } from './lib';

function handler(x: number) { return x; }
function register(cb: unknown, extra?: unknown) {}

export function argument() {
  register(handler);
}

export function keyword() {
  register(1, helper);
}

export function arrayElement() {
  return [handler, helper];
}

export function objectValue() {
  const o = { run: handler, other: other };
  return o;
}

export function shorthand() {
  const o = { handler };
  return o;
}

export function returned() {
  return handler;
}

export function declarator() {
  const cb = handler;
  return cb;
}

export function assignment() {
  let cb: unknown;
  cb = helper;
  return cb;
}

export class Bus {
  handle(x: number) { return x; }
  start() {
    register(this.handle);
  }
  table() {
    return { h: this.handle };
  }
}

export function negatives(param: (x: number) => number, n: number) {
  const local = (x: number) => x;
  register(LIMIT);
  register(param);
  register(local);
  register(n);
  register(console.log);
  register(Math.max);
}

export function called() {
  handler(1);
  register(handler);
}

export function shadowed(handler: unknown) {
  register(handler);
}
"#,
    ),
    (
        "src/default.ts",
        "function main() { return 1; }\nexport default main;\n",
    ),
    (
        "src/view.tsx",
        r#"import { helper } from './lib';
function onClick() {}
export function View() {
  const local = () => 1;
  return <button onClick={onClick} onFocus={helper} onBlur={local} title="t" />;
}
"#,
    ),
];

#[test]
fn argument_collection_and_object_shapes() {
    assert_eq!(
        ref_targets(FILES, "src/shapes.argument"),
        ["src/shapes.handler"]
    );
    assert_eq!(ref_targets(FILES, "src/shapes.keyword"), ["src/lib.helper"]);
    assert_eq!(
        ref_targets(FILES, "src/shapes.arrayElement"),
        ["src/lib.helper", "src/shapes.handler"]
    );
    assert_eq!(
        ref_targets(FILES, "src/shapes.objectValue"),
        ["src/lib.other", "src/shapes.handler"]
    );
    assert_eq!(
        ref_targets(FILES, "src/shapes.shorthand"),
        ["src/shapes.handler"]
    );
}

#[test]
fn return_declarator_and_assignment_shapes() {
    assert_eq!(
        ref_targets(FILES, "src/shapes.returned"),
        ["src/shapes.handler"]
    );
    assert_eq!(
        ref_targets(FILES, "src/shapes.declarator"),
        ["src/shapes.handler"]
    );
    assert_eq!(
        ref_targets(FILES, "src/shapes.assignment"),
        ["src/lib.helper"]
    );
}

#[test]
fn this_method_default_export_and_jsx() {
    assert_eq!(
        ref_targets(FILES, "src/shapes.Bus.start"),
        ["src/shapes.Bus.handle"]
    );
    assert_eq!(
        ref_targets(FILES, "src/shapes.Bus.table"),
        ["src/shapes.Bus.handle"]
    );
    assert_eq!(ref_targets(FILES, "src/default"), ["src/default.main"]);
    // `onBlur={local}` names the arrow declared inside `View`.
    assert_eq!(
        ref_targets(FILES, "src/view.View"),
        ["src/lib.helper", "src/view.View.local", "src/view.onClick"]
    );
}

#[test]
fn locals_params_externals_and_calls_are_not_references() {
    // Only the arrow declared inside the function is a reference: the
    // parameters, the constant and the globals are not.
    let t = ref_targets(FILES, "src/shapes.negatives");
    assert_eq!(t, ["src/shapes.negatives.local"], "{t:?}");
    // A constant is data, not a function: a plain USES edge, no REFERENCES.
    assert_eq!(
        edge_targets(FILES, "src/shapes.negatives", "USES"),
        ["src/lib.LIMIT"]
    );
    assert!(ref_targets(FILES, "src/shapes.called").is_empty());
    assert!(
        edge_targets(FILES, "src/shapes.called", "CALLS")
            .contains(&"src/shapes.handler".to_string())
    );
    assert!(ref_targets(FILES, "src/shapes.shadowed").is_empty());
}

#[test]
fn unresolved_value_references_are_not_counted() {
    unresolved_accounting(FILES).assert_value_references_not_counted();
}

const CLASSES: &[(&str, &str)] = &[
    (
        "src/svc.ts",
        "export class Service {}\nexport interface Shape { a: number }\n",
    ),
    (
        "src/di.ts",
        r#"import { Service, Shape } from './svc';
class Local {}
declare function provide(c: unknown): void;
export function di() {
  provide(Service);
  provide(Local);
}
export function mapped() {
  return new Map([['k', Service]]);
}
export function typeOnly(s: Shape) {
  return s;
}
"#,
    ),
    (
        "src/routes.tsx",
        r#"import { Service } from './svc';
declare function Route(p: { component: unknown }): unknown;
export function Routes() {
  return <Route component={Service} />;
}
"#,
    ),
];

#[test]
fn classes_passed_as_values_are_references() {
    assert_eq!(
        ref_targets(CLASSES, "src/di.di"),
        ["src/di.Local", "src/svc.Service"]
    );
    assert_eq!(ref_targets(CLASSES, "src/di.mapped"), ["src/svc.Service"]);
    assert_eq!(
        ref_targets(CLASSES, "src/routes.Routes"),
        ["src/svc.Service"]
    );
    // An interface is a type, never a value.
    assert!(ref_targets(CLASSES, "src/di.typeOnly").is_empty());
}

const PARAMS_AND_PROTOTYPES: &[(&str, &str)] = &[(
    "src/a.ts",
    r#"export function canOverride(a: string): boolean { return !!a; }
export type Pred = (a: string) => boolean;

export function createMatcher(find: string, pred: Pred = canOverride): () => boolean {
  return () => pred(find);
}

export class Core {
  handler(e: string): unknown { return e; }
  other() { return 1; }
}

export function dispatcher(app: Core) {
  return app.other !== Core.prototype.other;
}

export function toHandler(h: unknown) {
  if (typeof (h as Core)?.handler === "function") {
    return (h as Core).handler as unknown;
  }
  return h;
}

export class Resp { x = 1; }
(Resp.prototype as any)["k"] = true;
"#,
)];

#[test]
fn a_parameter_default_reads_a_function() {
    assert_eq!(
        ref_targets(PARAMS_AND_PROTOTYPES, "src/a.createMatcher"),
        ["src/a.canOverride"]
    );
}

#[test]
fn a_class_prototype_reads_the_class() {
    assert_eq!(
        ref_targets(PARAMS_AND_PROTOTYPES, "src/a.dispatcher"),
        ["src/a.Core"]
    );
    assert_eq!(ref_targets(PARAMS_AND_PROTOTYPES, "src/a"), ["src/a.Resp"]);
}

#[test]
fn a_member_read_through_a_cast_is_typed_by_the_cast() {
    assert_eq!(
        ref_targets(PARAMS_AND_PROTOTYPES, "src/a.toHandler"),
        ["src/a.Core.handler", "src/a.Core.handler"]
    );
}

const IIFE_CLASS: &[(&str, &str)] = &[(
    "src/a.ts",
    r#"export class Core {}

export const Named = (() => {
  class Inner extends Core {}
  return Inner;
})() as unknown as { new (): Core };

export const Same = (() => {
  class Same extends Core {}
  return Same;
})() as unknown as { new (): Core };
export type Same = Core;

export function shadowed() {
  const Core = 1;
  return Core;
}
"#,
)];

#[test]
fn a_class_declared_in_an_iife_and_returned_is_a_reference() {
    assert_eq!(ref_targets(IIFE_CLASS, "src/a.Named"), ["src/a.Inner"]);
    // The const and the class share a qualname; the read inside the const
    // means the class.
    let (_tmp, root, db_path) = common::index_repo("lidx-iife-class-", IIFE_CLASS);
    let indexer = lidx::indexer::Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let kinds: Vec<(String, String)> = conn
        .prepare(
            "SELECT s.kind, t.kind FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = 'REFERENCES' AND s.qualname = 'src/a.Same'",
        )
        .unwrap()
        .query_map([gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(kinds, [("const".to_string(), "class".to_string())]);
    // A local of the same name still hides the class.
    assert!(ref_targets(IIFE_CLASS, "src/a.shadowed").is_empty());
}
