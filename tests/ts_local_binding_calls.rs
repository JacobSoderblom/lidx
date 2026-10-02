//! Issue #232: a TS/JS bare call to a locally bound name (destructured hook
//! result, parameter, `let`) must not bind to a same-named export elsewhere.

mod common;

use lidx::indexer::Indexer;
use rusqlite::params;

fn call_targets(files: &[(&str, &str)], caller: &str) -> Vec<String> {
    let tmp = tempfile::tempdir().unwrap();
    common::write_files(tmp.path(), files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    conn.prepare(
        "SELECT t.qualname FROM edges e
         JOIN symbols s ON s.id = e.source_symbol_id
         JOIN symbols t ON t.id = e.target_symbol_id
         WHERE e.kind = 'CALLS' AND s.name = ? AND e.graph_version = ?
           AND t.kind != 'external'",
    )
    .unwrap()
    .query_map(params![caller, gv], |r| r.get(0))
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

const TAURI: (&str, &str) = (
    "lib/tauri.ts",
    "export function login() { return fetch('/api/login'); }\nexport function doLogin() { return 1; }\n",
);
const USE_AUTH: (&str, &str) = (
    "hooks/use-auth.ts",
    "export function useAuth() { const login = () => true; return { login }; }\n",
);

#[test]
fn destructured_hook_result_does_not_bind_to_unimported_export() {
    let t = call_targets(
        &[
            TAURI,
            USE_AUTH,
            (
                "components/login-prompt.tsx",
                "import { useAuth } from '../hooks/use-auth';\nexport function LoginPrompt() { const { login } = useAuth(); login(); return null; }\n",
            ),
        ],
        "LoginPrompt",
    );
    assert!(!t.iter().any(|q| q.contains("tauri")), "{t:?}");
}

#[test]
fn renamed_destructuring_binds_to_local() {
    let t = call_targets(
        &[
            TAURI,
            USE_AUTH,
            (
                "components/p.tsx",
                "import { useAuth } from '../hooks/use-auth';\nexport function P() { const { login: doLogin } = useAuth(); doLogin(); return null; }\n",
            ),
        ],
        "P",
    );
    assert!(!t.iter().any(|q| q.contains("tauri")), "{t:?}");
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
        "run",
    );
    assert!(t.is_empty(), "{t:?}");
}

#[test]
fn nested_named_function_matches_top_level() {
    let t = call_targets(
        &[
            TAURI,
            USE_AUTH,
            (
                "components/n.tsx",
                "import { useAuth } from '../hooks/use-auth';\nexport function Outer() {\n  function inner() { const { login } = useAuth(); login(); }\n  inner();\n}\n",
            ),
        ],
        "inner",
    );
    assert!(!t.iter().any(|q| q.contains("tauri")), "{t:?}");
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
        "run",
    );
    assert_eq!(t, vec!["lib/tauri.login".to_string()], "{t:?}");
}

#[test]
fn same_file_declaration_still_resolves() {
    let t = call_targets(
        &[(
            "a.ts",
            "function helper() { return 1; }\nconst viaConst = () => 2;\nexport function run() { helper(); viaConst(); }\nexport class K { m() { helper(); } }\n",
        )],
        "run",
    );
    assert_eq!(t.len(), 2, "{t:?}");
    let t = call_targets(
        &[(
            "a.ts",
            "function helper() { return 1; }\nexport class K { m() { helper(); } }\n",
        )],
        "m",
    );
    assert_eq!(t, vec!["a.helper".to_string()], "{t:?}");
}
