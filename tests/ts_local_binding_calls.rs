//! Issue #232: a TS/JS bare call must resolve against lexical scope: a
//! local binding (destructured hook result, parameter, `let`) shadows any
//! same-named export, and in an ES module an unbound name cannot reach
//! another file's export at all.

mod common;

use common::call_targets;
use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::json;

const TAURI: (&str, &str) = (
    "lib/tauri.ts",
    "export function login() { return fetch('/api/login'); }\nexport function doLogin() { return 1; }\n",
);
const USE_AUTH: (&str, &str) = (
    "hooks/use-auth.ts",
    "export function useAuth() { const login = () => true; return { login }; }\n",
);
const IMPORT_USE_AUTH: &str = "import { useAuth } from '../hooks/use-auth';\n";

fn no_tauri(t: &[String]) {
    assert!(!t.iter().any(|q| q.contains("tauri")), "{t:?}");
}

fn comp(body: &str) -> String {
    format!("{IMPORT_USE_AUTH}{body}\n")
}

#[test]
fn destructured_hook_result_does_not_bind_to_unimported_export() {
    let src = comp(
        "export function LoginPrompt() { const { login } = useAuth(); login(); return null; }",
    );
    let t = call_targets(
        &[TAURI, USE_AUTH, ("components/login-prompt.tsx", &src)],
        "components/login-prompt.LoginPrompt",
    );
    no_tauri(&t);
    assert!(t.contains(&"hooks/use-auth.useAuth".to_string()), "{t:?}");
}

#[test]
fn explain_symbol_on_exported_login_reports_no_destructuring_caller() {
    let tmp = tempfile::tempdir().unwrap();
    let src = comp(
        "export function LoginPrompt() { const { login } = useAuth(); login(); return null; }",
    );
    common::write_files(
        tmp.path(),
        &[TAURI, USE_AUTH, ("components/login-prompt.tsx", &src)],
    );
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    let out = rpc::handle_method(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "lib/tauri.login"}),
    )
    .unwrap();
    assert!(!out.to_string().contains("login-prompt"), "{out}");
}

#[test]
fn renamed_destructuring_does_not_bind_to_either_export_name() {
    let src = comp(
        "export function P() { const { login: doLogin } = useAuth(); doLogin(); return null; }",
    );
    let t = call_targets(
        &[TAURI, USE_AUTH, ("components/p.tsx", &src)],
        "components/p.P",
    );
    // Neither `login` nor `doLogin` in lib/tauri; only the hook call remains.
    assert_eq!(t, vec!["hooks/use-auth.useAuth".to_string()], "{t:?}");
}

#[test]
fn parameter_shadows_export() {
    let t = call_targets(
        &[
            TAURI,
            (
                "a.ts",
                "export function run(login: () => void) { login(); }\n",
            ),
        ],
        "a.run",
    );
    assert!(t.is_empty(), "{t:?}");
}

#[test]
fn local_arrow_call_is_unresolved() {
    // The arrow is not a symbol, so there is nothing to resolve to.
    let t = call_targets(
        &[
            TAURI,
            (
                "a.ts",
                "export function run() { const login = () => 1; login(); }\n",
            ),
        ],
        "a.run",
    );
    assert!(t.is_empty(), "{t:?}");
}

#[test]
fn nested_named_function_sees_enclosing_function_locals() {
    let src = comp(
        "export function Outer() {\n  const { login } = useAuth();\n  function inner() { login(); }\n  inner();\n}",
    );
    let t = call_targets(
        &[TAURI, USE_AUTH, ("components/n.tsx", &src)],
        "components/n.Outer",
    );
    no_tauri(&t);
}

#[test]
fn nested_named_function_own_local_does_not_bind() {
    let src = comp(
        "export function Outer() {\n  function inner() { const { login } = useAuth(); login(); }\n  inner();\n}",
    );
    let t = call_targets(
        &[TAURI, USE_AUTH, ("components/n.tsx", &src)],
        "components/n.Outer",
    );
    no_tauri(&t);
}

#[test]
fn module_level_destructured_binding_used_in_function_does_not_bind() {
    let src = comp("const { login } = useAuth();\nexport function run() { login(); }");
    let t = call_targets(
        &[TAURI, USE_AUTH, ("components/m.tsx", &src)],
        "components/m.run",
    );
    no_tauri(&t);
}

#[test]
fn imported_call_still_resolves() {
    let t = call_targets(
        &[
            TAURI,
            (
                "a.ts",
                "import { login } from './lib/tauri';\nexport function run() { login(); }\n",
            ),
        ],
        "a.run",
    );
    assert_eq!(t, vec!["lib/tauri.login".to_string()], "{t:?}");
}

#[test]
fn same_file_function_declaration_resolves() {
    let t = call_targets(
        &[(
            "a.ts",
            "function helper() { return 1; }\nexport function run() { helper(); }\n",
        )],
        "a.run",
    );
    assert_eq!(t, vec!["a.helper".to_string()], "{t:?}");
}

#[test]
fn same_file_const_arrow_resolves() {
    let t = call_targets(
        &[(
            "a.ts",
            "const viaConst = () => 2;\nexport function run() { viaConst(); }\n",
        )],
        "a.run",
    );
    assert_eq!(t, vec!["a.viaConst".to_string()], "{t:?}");
}

#[test]
fn same_file_function_resolves_from_class_method() {
    let t = call_targets(
        &[(
            "a.ts",
            "function helper() { return 1; }\nexport class K { m() { helper(); } }\n",
        )],
        "a.K.m",
    );
    assert_eq!(t, vec!["a.helper".to_string()], "{t:?}");
}

#[test]
fn unimported_name_in_esm_file_stays_unresolved() {
    let t = call_targets(
        &[TAURI, ("a.ts", "export function run() { login(); }\n")],
        "a.run",
    );
    assert!(t.is_empty(), "{t:?}");
}

#[test]
fn unbound_name_in_classic_script_still_resolves_globally() {
    let t = call_targets(
        &[
            ("a.js", "function boot() {}\n"),
            ("b.js", "function start() { boot(); }\n"),
        ],
        "b.start",
    );
    assert_eq!(t, vec!["a.boot".to_string()], "{t:?}");
}

#[test]
fn function_local_require_binding_still_resolves() {
    let t = call_targets(
        &[
            ("x.js", "function f() {}\nmodule.exports = { f };\n"),
            (
                "b.js",
                "function start() { const { f } = require('./x'); f(); }\n",
            ),
        ],
        "b.start",
    );
    assert!(t.contains(&"x.f".to_string()), "{t:?}");
}
