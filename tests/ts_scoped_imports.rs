//! Issue #519: a scoped npm package (`@scope/pkg`) stays a package. Its
//! external stub is shared across importing files, while a tsconfig path alias
//! that also starts with `@` still resolves to repo files.

mod common;

use common::call_targets;

const TSCONFIG: (&str, &str) = (
    "tsconfig.json",
    "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"./*\"],\"@app/*\":[\"./app/*\"]}}}",
);

#[test]
fn scoped_package_calls_share_one_external_stub() {
    let files = [
        (
            "a.ts",
            "import { invoke } from '@tauri-apps/api/core';\nexport function a() { return invoke('x'); }\n",
        ),
        (
            "b.ts",
            "import { invoke } from '@tauri-apps/api/core';\nexport function b() { return invoke('y'); }\n",
        ),
    ];
    let a = call_targets(&files, "a.a");
    let b = call_targets(&files, "b.b");
    assert_eq!(a, vec!["ext:@tauri-apps/api/core:invoke".to_string()]);
    assert_eq!(a, b);
}

#[test]
fn bare_scoped_package_import_is_shared() {
    let files = [
        (
            "a.ts",
            "import { readFile } from '@scope/pkg';\nexport function a() { return readFile('x'); }\n",
        ),
        (
            "b.ts",
            "import { readFile } from '@scope/pkg';\nexport function b() { return readFile('y'); }\n",
        ),
    ];
    let a = call_targets(&files, "a.a");
    assert_eq!(a, vec!["ext:@scope/pkg:readFile".to_string()]);
    assert_eq!(a, call_targets(&files, "b.b"));
}

#[test]
fn at_prefixed_tsconfig_aliases_still_resolve_to_repo_files() {
    let files = [
        TSCONFIG,
        ("lib/utils.ts", "export function cn() {}\n"),
        ("app/thing.ts", "export function thing() {}\n"),
        (
            "use.ts",
            "import { cn } from '@/lib/utils';\nimport { thing } from '@app/thing';\nexport function go() { cn(); thing(); }\n",
        ),
    ];
    let mut t = call_targets(&files, "use.go");
    t.sort();
    assert_eq!(
        t,
        vec!["app/thing.thing".to_string(), "lib/utils.cn".to_string()]
    );
}

#[test]
fn scoped_package_next_to_at_alias_stays_external() {
    let files = [
        TSCONFIG,
        (
            "use.ts",
            "import { invoke } from '@tauri-apps/api/core';\nexport function go() { invoke(); }\n",
        ),
    ];
    assert_eq!(
        call_targets(&files, "use.go"),
        vec!["ext:@tauri-apps/api/core:invoke".to_string()]
    );
}

#[test]
fn dollar_alias_and_other_marker_lookalikes_stay_shared_externals() {
    for spec in ["$lib/util", "^weird/pkg", "%odd/pkg", "!bang/pkg"] {
        let files = [
            (
                "a.ts",
                format!("import {{ f }} from '{spec}';\nexport function a() {{ return f(); }}\n"),
            ),
            (
                "b.ts",
                format!("import {{ f }} from '{spec}';\nexport function b() {{ return f(); }}\n"),
            ),
        ];
        let refs: Vec<(&str, &str)> = files.iter().map(|(p, s)| (*p, s.as_str())).collect();
        let a = call_targets(&refs, "a.a");
        assert_eq!(a, vec![format!("ext:{spec}:f")], "{spec}");
        assert_eq!(a, call_targets(&refs, "b.b"), "{spec}");
    }
}

#[test]
fn dollar_tsconfig_alias_resolves_to_repo_files() {
    let files = [
        (
            "tsconfig.json",
            "{\"compilerOptions\":{\"paths\":{\"$lib/*\":[\"./src/lib/*\"]}}}",
        ),
        ("src/lib/util.ts", "export function f() {}\n"),
        (
            "use.ts",
            "import { f } from '$lib/util';\nexport function go() { f(); }\n",
        ),
    ];
    assert_eq!(
        call_targets(&files, "use.go"),
        vec!["src/lib/util.f".to_string()]
    );
}

const SERVER_PKG: &str = "{\"name\":\"@acme/server\",\"main\":\"./dist/index.cjs\",\"exports\":{\".\":{\"import\":{\"types\":\"./dist/index.d.mts\",\"default\":\"./dist/index.mjs\"}},\"./adapters/express\":{\"import\":{\"types\":\"./dist/adapters/express.d.mts\",\"default\":\"./dist/adapters/express.mjs\"}}}}";

fn workspace_files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("packages/server/package.json", SERVER_PKG),
        (
            "packages/server/src/index.ts",
            "export function initServer() {}\n",
        ),
        (
            "packages/server/src/adapters/express.ts",
            "export function createMiddleware() {}\n",
        ),
        (
            "packages/util/package.json",
            "{\"name\":\"@acme/util\",\"main\":\"index.js\"}",
        ),
        (
            "packages/util/src/index.ts",
            "export function helper() {}\n",
        ),
        (
            "packages/util/src/deep/thing.ts",
            "export function thing() {}\n",
        ),
        (
            "examples/app/src/main.ts",
            "import { initServer } from '@acme/server';\nimport { createMiddleware } from '@acme/server/adapters/express';\nimport { helper } from '@acme/util';\nimport { thing } from '@acme/util/deep/thing';\nimport { nope } from '@acme/missing';\nexport function go() { initServer(); createMiddleware(); helper(); thing(); nope(); }\n",
        ),
    ]
}

#[test]
fn workspace_packages_resolve_to_their_source() {
    let mut t = call_targets(&workspace_files(), "examples/app/src/main.go");
    t.sort();
    assert_eq!(
        t,
        vec![
            "ext:@acme/missing:nope".to_string(),
            "packages/server/src.initServer".to_string(),
            "packages/server/src/adapters/express.createMiddleware".to_string(),
            "packages/util/src.helper".to_string(),
            "packages/util/src/deep/thing.thing".to_string(),
        ]
    );
}

#[test]
fn a_package_name_two_workspaces_share_stays_external() {
    let mut files = workspace_files();
    files.push(("other/server/package.json", "{\"name\":\"@acme/server\"}"));
    let t = call_targets(&files, "examples/app/src/main.go");
    assert!(
        t.contains(&"ext:@acme/server:initServer".to_string()),
        "{t:?}"
    );
}
