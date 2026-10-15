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

const F: &str = "export function f() {}\n";

fn resolve(spec: &str, mut files: Vec<(&str, String)>) -> Vec<String> {
    files.push((
        "use.ts",
        format!("import {{ f }} from '{spec}';\nexport function go() {{ f(); }}\n"),
    ));
    let refs: Vec<(&str, &str)> = files.iter().map(|(a, b)| (*a, b.as_str())).collect();
    call_targets(&refs, "use.go")
}

fn s(x: &str) -> String {
    x.to_string()
}

#[test]
fn undeclared_fixture_and_example_manifests_are_not_packages() {
    for dir in [
        "test/fixtures/e",
        "examples/e",
        "__fixtures__/e",
        "testdata/e",
    ] {
        let manifest = format!("{dir}/package.json");
        let source = format!("{dir}/index.ts");
        let t = resolve(
            "express",
            vec![(&manifest, s("{\"name\":\"express\"}")), (&source, s(F))],
        );
        assert_eq!(t, vec!["ext:express:f".to_string()], "{dir}");
    }
}

#[test]
fn declared_workspaces_decide_which_manifests_are_packages() {
    let files = |decl: (&'static str, String)| {
        vec![
            decl,
            ("pkgs/a/package.json", s("{\"name\":\"@w/a\"}")),
            ("pkgs/a/src/index.ts", s(F)),
            ("examples/b/package.json", s("{\"name\":\"@w/b\"}")),
            ("examples/b/src/index.ts", s(F)),
            ("loose/c/package.json", s("{\"name\":\"@w/c\"}")),
            ("loose/c/src/index.ts", s(F)),
        ]
    };
    let npm = files((
        "package.json",
        s("{\"name\":\"r\",\"workspaces\":[\"pkgs/*\",\"examples/*\"]}"),
    ));
    assert_eq!(
        resolve("@w/a", npm.clone()),
        vec!["pkgs/a/src.f".to_string()]
    );
    // A declared member is a package even under `examples`.
    assert_eq!(
        resolve("@w/b", npm.clone()),
        vec!["examples/b/src.f".to_string()]
    );
    // An undeclared directory is not, once workspaces are declared.
    assert_eq!(resolve("@w/c", npm), vec!["ext:@w/c:f".to_string()]);

    let pnpm = files((
        "pnpm-workspace.yaml",
        s("packages:\n  - 'pkgs/**'\n  - '!pkgs/skip'\n"),
    ));
    assert_eq!(
        resolve("@w/a", pnpm.clone()),
        vec!["pkgs/a/src.f".to_string()]
    );
    assert_eq!(resolve("@w/c", pnpm), vec!["ext:@w/c:f".to_string()]);

    let lerna = files(("lerna.json", s("{\"packages\":[\"loose/*\"]}")));
    assert_eq!(
        resolve("@w/c", lerna.clone()),
        vec!["loose/c/src.f".to_string()]
    );
    assert_eq!(resolve("@w/a", lerna), vec!["ext:@w/a:f".to_string()]);
}

#[test]
fn escaping_and_unlisted_targets_never_resolve() {
    type Case<'a> = (&'a str, &'a str, Vec<(&'a str, String)>);
    let cases: Vec<Case> = vec![
        (
            "@a/p",
            "dotdot exports",
            vec![
                (
                    "p/package.json",
                    s("{\"name\":\"@a/p\",\"exports\":{\".\":\"../secret/x.ts\"}}"),
                ),
                ("secret/x.ts", s(F)),
            ],
        ),
        (
            "@a/p",
            "dotdot main",
            vec![
                (
                    "p/package.json",
                    s("{\"name\":\"@a/p\",\"main\":\"../secret/x.js\"}"),
                ),
                ("secret/x.ts", s(F)),
            ],
        ),
        (
            "@a/p/../../secret/x",
            "dotdot subpath",
            vec![
                ("p/package.json", s("{\"name\":\"@a/p\"}")),
                ("secret/x.ts", s(F)),
            ],
        ),
        (
            "@a/p/../../secret/x",
            "dotdot pattern",
            vec![
                (
                    "p/package.json",
                    s("{\"name\":\"@a/p\",\"exports\":{\"./*\":\"./src/*.ts\"}}"),
                ),
                ("secret/x.ts", s(F)),
            ],
        ),
        (
            "@a/p",
            "gitignored dist",
            vec![
                (".gitignore", s("dist\n")),
                (
                    "p/package.json",
                    s("{\"name\":\"@a/p\",\"main\":\"dist/index.js\"}"),
                ),
                ("p/dist/index.js", s(F)),
            ],
        ),
    ];
    for (spec, label, files) in cases {
        let t = resolve(spec, files);
        assert!(t.iter().all(|q| q.starts_with("ext:")), "{label}: {t:?}");
    }
}

#[cfg(unix)]
#[test]
fn symlinked_package_source_never_resolves() {
    let tmp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("index.ts"), F).unwrap();
    std::fs::create_dir_all(tmp.path().join("p")).unwrap();
    std::fs::write(tmp.path().join("p/package.json"), "{\"name\":\"@a/p\"}").unwrap();
    std::os::unix::fs::symlink(outside.path(), tmp.path().join("p/src")).unwrap();
    std::fs::write(
        tmp.path().join("use.ts"),
        "import { f } from '@a/p';\nexport function go() { f(); }\n",
    )
    .unwrap();
    let db = tmp.path().join(".lidx/.lidx.sqlite");
    let mut ix = lidx::indexer::Indexer::new(tmp.path().to_path_buf(), db).unwrap();
    ix.reindex().unwrap();
    let gv = ix.db().current_graph_version().unwrap();
    let conn = ix.db().read_conn().unwrap();
    let internal: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges e JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ?1 AND e.kind = 'CALLS' AND t.qualname NOT LIKE 'ext:%'",
            [gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(internal, 0);
}

#[test]
fn oversized_manifest_is_skipped() {
    let mut big = String::from("{\"name\":\"@a/p\",\"dependencies\":{");
    for i in 0..60_000 {
        big.push_str(&format!("\"dependency-number-{i}\":\"1.0.0\","));
    }
    big.push_str("\"z\":\"1\"}}");
    assert!(big.len() > 1_000_000);
    let t = resolve(
        "@a/p",
        vec![("p/package.json", big), ("p/src/index.ts", s(F))],
    );
    assert_eq!(t, vec!["ext:@a/p:f".to_string()]);
}

#[test]
fn pattern_exports_use_the_longest_prefix() {
    let files = vec![
        (
            "p/package.json",
            s(
                "{\"name\":\"@a/p\",\"exports\":{\"./*\":\"./dist/*.js\",\"./x/*\":\"./dist/other/*.js\"}}",
            ),
        ),
        ("p/src/x/y.ts", s("export function wrong() {}\n")),
        ("p/src/other/y.ts", s(F)),
    ];
    assert_eq!(
        resolve("@a/p/x/y", files),
        vec!["p/src/other/y.f".to_string()]
    );
}

#[test]
fn unlisted_subpath_with_exports_stays_external() {
    let files = vec![
        (
            "p/package.json",
            s("{\"name\":\"@a/p\",\"exports\":{\".\":\"./dist/index.js\"}}"),
        ),
        ("p/src/index.ts", s("export function g() {}\n")),
        ("p/src/internal.ts", s(F)),
    ];
    assert_eq!(
        resolve("@a/p/internal", files),
        vec!["ext:@a/p/internal:f".to_string()]
    );
}

#[test]
fn export_conditions_prefer_types_import_default_require() {
    // `require` is written first, `types` last: priority still picks types.
    let files = vec![
        (
            "p/package.json",
            s(
                "{\"name\":\"@a/p\",\"exports\":{\".\":{\"require\":\"./dist/cjs.cjs\",\"default\":\"./dist/b.js\",\"types\":\"./dist/a.d.ts\"}}}",
            ),
        ),
        ("p/src/cjs.ts", s(F)),
        ("p/src/b.ts", s(F)),
        ("p/src/a.ts", s(F)),
    ];
    assert_eq!(resolve("@a/p", files), vec!["p/src/a.f".to_string()]);
    let files = vec![
        (
            "p/package.json",
            s(
                "{\"name\":\"@a/p\",\"exports\":{\".\":{\"require\":\"./dist/cjs.cjs\",\"import\":{\"default\":\"./dist/esm.mjs\"}}}}",
            ),
        ),
        ("p/src/cjs.ts", s(F)),
        ("p/src/esm.ts", s(F)),
    ];
    assert_eq!(resolve("@a/p", files), vec!["p/src/esm.f".to_string()]);
}
