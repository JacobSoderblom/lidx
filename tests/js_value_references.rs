//! A TypeScript/JavaScript function used as a value -- argument, array
//! element, object value (plain and shorthand), return, declarator,
//! assignment, `export default`, `this.method`, JSX attribute -- is a
//! `REFERENCES` edge from the enclosing symbol. Locals, parameters and names
//! already called are not.

mod common;

use common::{edge_targets, ref_targets, unresolved_reference_rows};

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
    assert_eq!(
        ref_targets(FILES, "src/view.View"),
        ["src/lib.helper", "src/view.onClick"]
    );
}

#[test]
fn locals_params_externals_and_calls_are_not_references() {
    let t = ref_targets(FILES, "src/shapes.negatives");
    assert!(t.is_empty(), "{t:?}");
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
fn unresolved_value_references_leave_no_rows() {
    assert_eq!(unresolved_reference_rows(FILES), 0);
}
