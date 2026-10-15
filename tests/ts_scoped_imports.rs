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
