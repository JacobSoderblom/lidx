//! Issue #113: `x.m()` where `x` is imported from a repo module binds to the
//! member of `x` the repo declares, never an `ext:` stub. A member it does not
//! declare stays unresolved: it never binds to `x` itself.

mod common;

use common::call_targets;

const API_CLIENT: &str = "export const apiClient = {\n  get(url: string) { return url; },\n  post(url: string) { return url; },\n};\n";

#[test]
fn relative_import_member_call_binds_to_imported_object() {
    let targets = call_targets(
        &[
            ("lib/api-client.ts", API_CLIENT),
            (
                "queries/client.ts",
                "import { apiClient } from '../lib/api-client';\n\nexport function load() {\n  return apiClient.get('/x');\n}\n",
            ),
        ],
        "queries/client.load",
    );
    assert_eq!(targets, vec!["lib/api-client.apiClient.get".to_string()]);
}

#[test]
fn alias_import_member_call_binds_to_imported_object() {
    let targets = call_targets(
        &[
            (
                "tsconfig.json",
                "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"./*\"]}}}",
            ),
            ("lib/api-client.ts", API_CLIENT),
            (
                "queries/client.ts",
                "import { apiClient } from '@/lib/api-client';\n\nexport function load() {\n  return apiClient.get('/x');\n}\n",
            ),
        ],
        "queries/client.load",
    );
    assert_eq!(targets, vec!["lib/api-client.apiClient.get".to_string()]);
}

fn single_call_target(files: &[(&str, &str)]) -> Vec<String> {
    call_targets(files, "use.go")
}

const NS_LIB: &str = "export function fn() {}\n";

#[test]
fn namespace_import_missing_member_stays_unbound() {
    let t = single_call_target(&[
        ("x.ts", NS_LIB),
        (
            "use.ts",
            "import * as ns from './x';\nexport function go() {\n  return ns.missing();\n}\n",
        ),
    ]);
    assert!(!t.iter().any(|q| q == "x"), "{t:?}");
}

#[test]
fn namespace_import_existing_member_binds() {
    let t = single_call_target(&[
        ("x.ts", NS_LIB),
        (
            "use.ts",
            "import * as ns from './x';\nexport function go() {\n  return ns.fn();\n}\n",
        ),
    ]);
    assert_eq!(t, vec!["x.fn".to_string()]);
}

#[test]
fn third_party_default_import_stays_external() {
    let t = single_call_target(&[(
        "use.ts",
        "import axios from 'axios';\nexport function go() {\n  return axios.get('/x');\n}\n",
    )]);
    assert_eq!(t, vec!["ext:axios.get".to_string()]);
}

#[test]
fn imported_class_missing_static_does_not_bind_to_class() {
    let t = single_call_target(&[
        ("c.ts", "export class Foo {}\n"),
        (
            "use.ts",
            "import { Foo } from './c';\nexport function go() {\n  return Foo.missingStatic();\n}\n",
        ),
    ]);
    assert!(!t.iter().any(|q| q == "c.Foo"), "{t:?}");
}

#[test]
fn local_instance_call_is_not_import_bound() {
    let t = single_call_target(&[
        ("c.ts", "export class Client { get() {} }\n"),
        (
            "use.ts",
            "import { Client } from './c';\nexport function go() {\n  const c = new Client();\n  return c.get();\n}\n",
        ),
    ]);
    // `new Client()` -> the class; `c.get()` -> the real method, unchanged.
    assert!(t.contains(&"c.Client.get".to_string()), "{t:?}");
    assert!(!t.iter().any(|q| q.starts_with("ext:")), "{t:?}");
}

#[test]
fn deep_member_chain_on_imported_object_never_binds_to_the_object() {
    let t = single_call_target(&[
        (
            "lib/api.ts",
            "export const api = { users: { list() { return 1; } } };\n",
        ),
        (
            "use.ts",
            "import { api } from './lib/api';\nexport function go() {\n  return api.users.list();\n}\n",
        ),
    ]);
    assert!(t.is_empty(), "{t:?}");
}

#[test]
fn deep_member_chain_on_imported_class_stays_unbound() {
    let t = single_call_target(&[
        ("c.ts", "export class Foo {}\n"),
        (
            "use.ts",
            "import { Foo } from './c';\nexport function go() {\n  return Foo.a.b();\n}\n",
        ),
    ]);
    assert!(t.is_empty(), "{t:?}");
}

#[test]
fn namespace_import_deep_chain_does_not_bind_to_module() {
    let t = single_call_target(&[
        ("lib/api.ts", "export const a = { b() {} };\n"),
        (
            "use.ts",
            "import * as ns from './lib/api';\nexport function go() {\n  return ns.a.b();\n}\n",
        ),
    ]);
    assert_eq!(t, vec!["lib/api.a.b".to_string()]);
}

// Issue #187: default-import aliases, re-export chains, unexported members.

const TSCONFIG: (&str, &str) = (
    "tsconfig.json",
    "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"./*\"]}}}",
);

fn caller(spec: &str, import: &str, call: &str) -> String {
    format!("import {import} from '{spec}';\nexport function go() {{\n  return {call};\n}}\n")
}

#[test]
fn default_import_alias_binds_to_differently_named_default_export() {
    for spec in ["./lib/api", "@/lib/api"] {
        let t = single_call_target(&[
            TSCONFIG,
            (
                "lib/api.ts",
                "const apiClient = { get() { return 1; } };\nexport default apiClient;\n",
            ),
            ("use.ts", &caller(spec, "api", "api.get()")),
        ]);
        assert_eq!(t, vec!["lib/api.apiClient.get".to_string()], "{spec}");
    }
}

#[test]
fn default_import_alias_binds_to_inline_default_declaration() {
    let t = single_call_target(&[
        ("lib/f.ts", "export default function realName() {}\n"),
        ("use.ts", &caller("./lib/f", "other", "other()")),
    ]);
    assert_eq!(t, vec!["lib/f.realName".to_string()]);
}

#[test]
fn barrel_named_and_star_reexports_bind_to_original() {
    for (spec, cfg) in [("./lib", false), ("@/lib", true)] {
        let mut files = vec![
            (
                "lib/named.ts",
                "export function viaNamed() {}\nexport function renamedOrig() {}\n",
            ),
            ("lib/star.ts", "export function viaStar() {}\n"),
            (
                "lib/index.ts",
                "export { viaNamed, renamedOrig as renamed } from './named';\nexport * from './star';\n",
            ),
        ];
        if cfg {
            files.push(TSCONFIG);
        }
        let src = format!(
            "import {{ viaNamed, viaStar, renamed }} from '{spec}';\nexport function go() {{\n  viaNamed();\n  viaStar();\n  renamed();\n}}\n"
        );
        files.push(("use.ts", &src));
        let mut t = single_call_target(&files);
        t.sort();
        assert_eq!(
            t,
            vec![
                "lib/named.renamedOrig".to_string(),
                "lib/named.viaNamed".to_string(),
                "lib/star.viaStar".to_string()
            ],
            "{spec}"
        );
    }
}

#[test]
fn nested_barrel_chain_and_default_reexport_bind_to_original() {
    let t = single_call_target(&[
        (
            "a/deep.ts",
            "export function deepFn() {}\nexport default function dflt() {}\n",
        ),
        (
            "a/index.ts",
            "export * from './deep';\nexport { default as Widget } from './deep';\n",
        ),
        ("index.ts", "export * from './a';\n"),
        (
            "use.ts",
            "import { deepFn, Widget } from './index';\nexport function go() {\n  deepFn();\n  Widget();\n}\n",
        ),
    ]);
    let mut t = t;
    t.sort();
    assert_eq!(
        t,
        vec!["a/deep.deepFn".to_string(), "a/deep.dflt".to_string()]
    );
}

#[test]
fn cyclic_star_reexports_terminate_and_stay_unbound() {
    let t = single_call_target(&[
        ("a.ts", "export * from './b';\n"),
        ("b.ts", "export * from './a';\n"),
        (
            "use.ts",
            "import { ghost } from './a';\nexport function go() {\n  return ghost();\n}\n",
        ),
    ]);
    assert!(
        !t.iter().any(|q| q.starts_with("a.") || q.starts_with("b.")),
        "{t:?}"
    );
}

#[test]
fn unexported_class_members_are_not_cross_file_fallback_candidates() {
    let t = single_call_target(&[
        (
            "hidden.ts",
            "class Hidden { secretMethod() {} }\nexport const x = 1;\n",
        ),
        (
            "use.ts",
            "export function go() {\n  return svc.secretMethod();\n}\n",
        ),
    ]);
    assert!(!t.iter().any(|q| q.contains("Hidden")), "{t:?}");
    let t = single_call_target(&[
        ("shown.ts", "export class Shown { openMethod() {} }\n"),
        (
            "use.ts",
            "export function go() {\n  return svc.openMethod();\n}\n",
        ),
    ]);
    assert!(t.contains(&"shown.Shown.openMethod".to_string()), "{t:?}");
}

#[test]
fn star_reexport_collision_stays_unbound() {
    let t = single_call_target(&[
        (
            "lib/a.ts",
            "export function foo() {}\nexport function onlyA() {}\n",
        ),
        ("lib/b.ts", "export function foo() {}\n"),
        (
            "lib/index.ts",
            "export * from './a';\nexport * from './b';\n",
        ),
        (
            "use.ts",
            "import { foo, onlyA } from './lib';\nexport function go() {\n  foo();\n  onlyA();\n}\n",
        ),
    ]);
    assert!(t.contains(&"lib/a.onlyA".to_string()), "{t:?}");
    assert!(
        !t.iter().any(|q| q == "lib/a.foo" || q == "lib/b.foo"),
        "{t:?}"
    );
}

#[test]
fn bare_default_reexport_binds_to_original_default() {
    let t = single_call_target(&[
        ("lib/w.ts", "export default function Widget() {}\n"),
        ("lib/index.ts", "export { default } from './w';\n"),
        ("use.ts", &caller("./lib", "Other", "Other()")),
    ]);
    assert_eq!(t, vec!["lib/w.Widget".to_string()]);
}

#[test]
fn type_only_reexport_binds_to_original() {
    let t = single_call_target(&[
        ("lib/a.ts", "export function foo() {}\n"),
        ("lib/index.ts", "export type { foo } from './a';\n"),
        ("use.ts", &caller("./lib", "{ foo }", "foo()")),
    ]);
    assert_eq!(t, vec!["lib/a.foo".to_string()]);
}

#[test]
fn directory_import_resolves_through_index_tsx_and_index_js() {
    for (idx, file) in [("index.tsx", "lib/index.tsx"), ("index.js", "lib/index.js")] {
        let t = single_call_target(&[
            (file, "export function foo() {}\n"),
            ("use.ts", &caller("./lib", "{ foo }", "foo()")),
        ]);
        assert_eq!(t, vec!["lib.foo".to_string()], "{idx}");
    }
}

// tsconfig `extends` (relative, array, package-style, override, cycle).

const ALIAS_USE: (&str, &str) = (
    "src/use.ts",
    "import { foo } from '@/foo';\nexport function go() {\n  return foo();\n}\n",
);
const FOO_LIB: (&str, &str) = ("lib/foo.ts", "export function foo() {}\n");
const BASE_PATHS: &str =
    "{\"compilerOptions\":{\"baseUrl\":\".\",\"paths\":{\"@/*\":[\"lib/*\"]}}}";

fn alias_target(files: &[(&str, &str)]) -> Vec<String> {
    call_targets(files, "src/use.go")
}

#[test]
fn alias_defined_only_in_relative_base_config_resolves() {
    for extends in ["./tsconfig.base.json", "./tsconfig.base"] {
        let cfg = format!("{{\"extends\":\"{extends}\"}}");
        let t = alias_target(&[
            ("tsconfig.base.json", BASE_PATHS),
            ("tsconfig.json", &cfg),
            FOO_LIB,
            ALIAS_USE,
        ]);
        assert_eq!(t, vec!["lib/foo.foo".to_string()], "{extends}");
    }
}

#[test]
fn base_config_base_url_resolves_against_declaring_config() {
    // baseUrl "." in configs/base.json is configs/, so `@/*` -> configs/lib/*.
    let t = alias_target(&[
        (
            "configs/base.json",
            "{\"compilerOptions\":{\"baseUrl\":\".\",\"paths\":{\"@/*\":[\"lib/*\"]}}}",
        ),
        ("tsconfig.json", "{\"extends\":\"./configs/base.json\"}"),
        ("configs/lib/foo.ts", "export function foo() {}\n"),
        FOO_LIB,
        ALIAS_USE,
    ]);
    assert_eq!(t, vec!["configs/lib/foo.foo".to_string()]);
}

#[test]
fn extends_array_later_entries_and_child_paths_override() {
    let t = alias_target(&[
        (
            "a.json",
            "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"nope/*\"]}}}",
        ),
        ("b.json", BASE_PATHS),
        ("tsconfig.json", "{\"extends\":[\"./a.json\",\"./b.json\"]}"),
        FOO_LIB,
        ALIAS_USE,
    ]);
    assert_eq!(t, vec!["lib/foo.foo".to_string()]);
    // A child's own `paths` replaces the parent's wholesale.
    let t = alias_target(&[
        ("base.json", BASE_PATHS),
        (
            "tsconfig.json",
            "{\"extends\":\"./base.json\",\"compilerOptions\":{\"paths\":{\"@/*\":[\"other/*\"]}}}",
        ),
        ("other/foo.ts", "export function foo() {}\n"),
        FOO_LIB,
        ALIAS_USE,
    ]);
    assert_eq!(t, vec!["other/foo.foo".to_string()]);
}

#[test]
fn package_style_extends_resolves_through_node_modules() {
    let t = alias_target(&[
        // `paths` resolve against the declaring config's own directory.
        (
            "node_modules/@shared/tsconfig/tsconfig.json",
            "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"../../../lib/*\"]}}}",
        ),
        (
            "tsconfig.json",
            "{\"extends\":\"@shared/tsconfig/tsconfig.json\"}",
        ),
        FOO_LIB,
        ALIAS_USE,
    ]);
    assert_eq!(t, vec!["lib/foo.foo".to_string()]);
}

#[test]
fn cyclic_extends_terminates() {
    let t = alias_target(&[
        ("a.json", "{\"extends\":\"./tsconfig.json\"}"),
        ("tsconfig.json", "{\"extends\":\"./a.json\"}"),
        FOO_LIB,
        ALIAS_USE,
    ]);
    assert!(!t.contains(&"lib/foo.foo".to_string()), "{t:?}");
}

// Import-then-export barrels, namespace re-exports, diamonds.

#[test]
fn import_then_export_barrel_binds_to_original() {
    let t = single_call_target(&[
        ("lib/foo.ts", "export function foo() {}\n"),
        (
            "lib/index.ts",
            "import { foo } from './foo';\nexport { foo };\n",
        ),
        ("use.ts", &caller("./lib", "{ foo }", "foo()")),
    ]);
    assert_eq!(t, vec!["lib/foo.foo".to_string()]);
}

#[test]
fn import_then_export_default_barrel_binds_to_original() {
    let t = single_call_target(&[
        (
            "lib/client.ts",
            "const apiClient = { get() { return 1; } };\nexport default apiClient;\n",
        ),
        (
            "lib/index.ts",
            "import apiClient from './client';\nexport default apiClient;\n",
        ),
        ("use.ts", &caller("./lib", "api", "api.get()")),
    ]);
    assert_eq!(t, vec!["lib/client.apiClient.get".to_string()]);
}

#[test]
fn export_star_as_namespace_binds_member_to_original() {
    let t = single_call_target(&[
        ("lib/x.ts", "export function fn() {}\n"),
        ("lib/index.ts", "export * as ns from './x';\n"),
        ("use.ts", &caller("./lib", "{ ns }", "ns.fn()")),
    ]);
    assert_eq!(t, vec!["lib/x.fn".to_string()]);
}

#[test]
fn imported_namespace_exported_through_barrel_binds_member() {
    let t = single_call_target(&[
        ("lib/x.ts", "export function fn() {}\n"),
        (
            "lib/index.ts",
            "import * as ns from './x';\nexport { ns };\n",
        ),
        ("use.ts", &caller("./lib", "{ ns }", "ns.fn()")),
    ]);
    assert_eq!(t, vec!["lib/x.fn".to_string()]);
}

#[test]
fn namespace_import_of_barrel_binds_star_member() {
    for spec in ["./lib", "@/lib"] {
        let t = single_call_target(&[
            TSCONFIG,
            ("lib/x.ts", "export function viaStar() {}\n"),
            ("lib/index.ts", "export * from './x';\n"),
            ("use.ts", &caller(spec, "* as ns", "ns.viaStar()")),
        ]);
        assert_eq!(t, vec!["lib/x.viaStar".to_string()], "{spec}");
    }
}

#[test]
fn diamond_star_reexports_bind_once_to_the_shared_leaf() {
    let t = single_call_target(&[
        ("leaf.ts", "export function foo() {}\n"),
        ("a.ts", "export * from './leaf';\n"),
        ("b.ts", "export * from './leaf';\n"),
        ("index.ts", "export * from './a';\nexport * from './b';\n"),
        ("use.ts", &caller("./index", "{ foo }", "foo()")),
    ]);
    assert_eq!(t, vec!["leaf.foo".to_string()]);
}
