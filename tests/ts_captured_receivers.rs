//! Issue #483: a call on a variable of an enclosing scope (a closure's
//! captured parameter or local, a module-level `const`) is typed like a call
//! on the function's own locals. A builtin or unknown type never binds the
//! repo's same-named method through the bare-name tier (`promise.catch(..)`,
//! `middleware.map(..)`, `opts.maxAge.toString()`, `registry.get(..)`), and a
//! captured value of a repo type still binds that type's method.

mod common;

use common::call_targets;

const LIB: (&str, &str) = (
    "lib.ts",
    "export class Unpromise {\n\
     \x20 catch(f: any) { return f; }\n\
     \x20 map(f: any) { return f; }\n\
     \x20 get(k: string) { return k; }\n\
     \x20 toString() { return 'x'; }\n\
     \x20 run() { return 1; }\n\
     }\n",
);

fn targets(body: &str, caller: &str) -> Vec<String> {
    let src = format!("import {{ Unpromise }} from './lib';\n{body}\n");
    call_targets(&[LIB, ("use.ts", &src)], caller)
}

fn binds_lib(t: &[String]) -> bool {
    t.iter().any(|q| q.starts_with("lib.Unpromise."))
}

#[test]
fn captured_parameters_of_builtin_type_bind_nothing() {
    let t = targets(
        "export function outer(promise: Promise<number>, ...mw: any[]) {\n\
         \x20 function inner() {\n\
         \x20   promise.catch((c) => c);\n\
         \x20   mw.map((m) => m);\n\
         \x20 }\n\
         \x20 return inner;\n}",
        "use.outer.inner",
    );
    assert!(!binds_lib(&t), "{t:?}");
}

#[test]
fn captured_locals_and_property_chains_bind_nothing() {
    let t = targets(
        "export function outer(opts: any) {\n\
         \x20 const cfg = { maxAge: 3 };\n\
         \x20 function inner() {\n\
         \x20   opts.maxAge.toString();\n\
         \x20   cfg.maxAge.toString();\n\
         \x20 }\n\
         \x20 return inner;\n}",
        "use.outer.inner",
    );
    assert!(!binds_lib(&t), "{t:?}");
}

#[test]
fn module_level_builtin_constants_bind_nothing_inside_functions() {
    let t = targets(
        "const registry = new WeakMap<object, number>();\n\
         const cache = new Map();\n\
         export function read(k: object) { registry.get(k); cache.get('a'); }",
        "use.read",
    );
    assert!(!binds_lib(&t), "{t:?}");
}

#[test]
fn captured_values_of_a_repo_type_still_bind() {
    let t = targets(
        "const shared = new Unpromise();\n\
         export function outer(own: Unpromise) {\n\
         \x20 function inner() {\n\
         \x20   shared.catch(1);\n\
         \x20   own.run();\n\
         \x20 }\n\
         \x20 return inner;\n}",
        "use.outer.inner",
    );
    assert!(t.contains(&"lib.Unpromise.catch".to_string()), "{t:?}");
    assert!(t.contains(&"lib.Unpromise.run".to_string()), "{t:?}");
}

#[test]
fn an_inner_binding_shadows_the_captured_one() {
    let t = targets(
        "const shared = new Unpromise();\n\
         export function outer() {\n\
         \x20 function inner(shared: Promise<number>) {\n\
         \x20   shared.catch(1);\n\
         \x20 }\n\
         \x20 return inner;\n}",
        "use.outer.inner",
    );
    assert!(!binds_lib(&t), "{t:?}");
}

#[test]
fn an_imported_class_reference_is_not_a_captured_variable() {
    let t = targets(
        "export function outer() {\n\
         \x20 function inner() { return Unpromise.name; }\n\
         \x20 return inner;\n}",
        "use.outer.inner",
    );
    assert!(!binds_lib(&t), "{t:?}");
}

#[test]
fn a_captured_unknown_receiver_keeps_non_builtin_names() {
    // `run` is no builtin method name: the unique repo method still binds
    // (the data behind the list is on `JS_TS_BUILTIN_METHODS`).
    let t = targets(
        "export function outer(x: any) {\n\
         \x20 function inner() { x.run(); }\n\
         \x20 return inner;\n}",
        "use.outer.inner",
    );
    assert!(t.contains(&"lib.Unpromise.run".to_string()), "{t:?}");
}
