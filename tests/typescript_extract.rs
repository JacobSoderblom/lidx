mod common;

use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::javascript::{
    TsxExtractor, TypescriptExtractor, module_name_from_rel_path, resolve_import_file_edges,
};
use std::path::Path;

/// Writes each `(rel_path, content)` pair under `root`, creating parent
/// directories as needed. Used to build a small on-disk fixture for
/// `resolve_import_file_edges`, which resolves both relative imports and
/// tsconfig path aliases by checking real files on disk.
fn write_fixture(root: &Path, files: &[(&str, &str)]) {
    for (rel, content) in files {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, content).unwrap();
    }
}

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("types/foo.d.ts"), "types/foo");
    assert_eq!(module_name_from_rel_path("pkg/index.ts"), "pkg");
}

#[test]
fn extract_symbols_and_edges() {
    let source = r#"
import type { Foo } from "./foo";

export interface Greeter {
    greet(name: string): void;
}

export type Id = string | number;

export enum Kind { A, B }

export class Impl implements Greeter {
    helper() {}
    greet(name: string) { this.helper(); }
}
"#;
    let mut extractor = TypescriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/types").unwrap();

    let names: Vec<_> = extracted
        .symbols
        .iter()
        .map(|s| (s.kind.as_str(), s.qualname.as_str()))
        .collect();

    assert!(names.contains(&("interface", "src/types.Greeter")));
    assert!(names.contains(&("type", "src/types.Id")));
    assert!(names.contains(&("enum", "src/types.Kind")));
    assert!(names.contains(&("class", "src/types.Impl")));
    assert!(names.contains(&("method", "src/types.Impl.helper")));
    assert!(names.contains(&("method", "src/types.Impl.greet")));

    let edge_kinds: Vec<_> = extracted.edges.iter().map(|e| e.kind.as_str()).collect();
    assert!(edge_kinds.contains(&"IMPORTS"));
    assert!(edge_kinds.contains(&"IMPLEMENTS"));
    assert!(edge_kinds.contains(&"CALLS"));

    let call_edges: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CALLS")
        .collect();
    assert!(
        call_edges
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("src/types.Impl.helper"))
    );
}

#[test]
fn multiline_chained_call_resolves_like_single_line() {
    let source = "
function caller() {
    UniqueName
        .Create();
}
";
    let mut extractor = TypescriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.detail.is_none())
        .expect("UniqueName.Create() call edge");
    assert_eq!(
        call.target_qualname.as_deref(),
        Some("UniqueName.Create"),
        "multi-line chain must resolve to the same qualname as the single-line form"
    );
}

// tsconfig `@/*` path-alias resolution. Modeled on dpb's
// `node/datacatalog-ui/tsconfig.json` (`{"paths": {"@/*": ["./*"]}}`, no
// `baseUrl`) and the real `ProductPage` case: a `.tsx` file several
// directories deep importing first-party helpers via `@/lib/...` and
// `@/components/...`. Before this fix, `resolve_import_path` bailed out on
// any non-relative specifier, so these never produced IMPORTS_FILE edges no
// matter how concrete the target file was.

#[test]
fn tsconfig_alias_resolves_to_imports_file_edge() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write_fixture(
        root,
        &[
            (
                "tsconfig.json",
                r#"{"compilerOptions": {"paths": {"@/*": ["./*"]}}}"#,
            ),
            (
                "lib/datacatalog-service-client.ts",
                "export function getDataCatalogService() {}",
            ),
            (
                "app/(workspace)/product/[uniqueName]/page.tsx",
                "import { getDataCatalogService } from '@/lib/datacatalog-service-client';\n",
            ),
        ],
    );

    let file_rel = "app/(workspace)/product/[uniqueName]/page.tsx";
    let source = std::fs::read_to_string(root.join(file_rel)).unwrap();
    let mut extractor = TsxExtractor::new().unwrap();
    let module = module_name_from_rel_path(file_rel);
    let mut extracted = extractor.extract(&source, &module).unwrap();
    assert!(
        extracted.edges.iter().any(|e| e.kind == "IMPORTS"
            && e.target_qualname.as_deref() == Some("@/lib/datacatalog-service-client")),
        "extraction must still record the raw @/ specifier as an IMPORTS edge"
    );

    resolve_import_file_edges(root, file_rel, &module, &mut extracted.edges);

    let imports_file = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "IMPORTS_FILE")
        .collect::<Vec<_>>();
    assert_eq!(
        imports_file.len(),
        1,
        "expected exactly one IMPORTS_FILE edge, got {:?}",
        imports_file
    );
    assert_eq!(
        imports_file[0].target_qualname.as_deref(),
        Some("lib/datacatalog-service-client")
    );
    let detail: serde_json::Value =
        serde_json::from_str(imports_file[0].detail.as_ref().unwrap()).unwrap();
    assert_eq!(
        detail["dst_path"].as_str().unwrap(),
        "lib/datacatalog-service-client.ts"
    );
    assert_eq!(detail["confidence"].as_f64().unwrap(), 1.0);
}

#[test]
fn tsconfig_alias_honors_base_url_src_mapping() {
    // dpb-app's shape: `baseUrl: "."`, `paths: {"@/*": ["./src/*"]}`.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write_fixture(
        root,
        &[
            (
                "tsconfig.json",
                r#"{"compilerOptions": {"baseUrl": ".", "paths": {"@/*": ["./src/*"]}}}"#,
            ),
            ("src/lib/bar.ts", "export const bar = 1;\n"),
            (
                "src/components/Foo.tsx",
                "import { bar } from '@/lib/bar';\n",
            ),
        ],
    );

    let file_rel = "src/components/Foo.tsx";
    let source = std::fs::read_to_string(root.join(file_rel)).unwrap();
    let mut extractor = TsxExtractor::new().unwrap();
    let module = module_name_from_rel_path(file_rel);
    let mut extracted = extractor.extract(&source, &module).unwrap();
    resolve_import_file_edges(root, file_rel, &module, &mut extracted.edges);

    let imports_file = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "IMPORTS_FILE")
        .collect::<Vec<_>>();
    assert_eq!(
        imports_file
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("src/lib/bar")]
    );
}

#[test]
fn tsconfig_alias_is_scoped_per_owning_project_not_global() {
    // Two sibling projects, each with its own tsconfig.json mapping the
    // *same* `@/*` alias to a different root — mirrors dpb's
    // `node/datacatalog-ui` (`@/*` -> `./*`) sitting next to `node/dpb-app`
    // (`@/*` -> `./src/*`). A file in one project must resolve `@/shared`
    // against its own tsconfig only, never the sibling's.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write_fixture(
        root,
        &[
            (
                "apps/app-a/tsconfig.json",
                r#"{"compilerOptions": {"paths": {"@/*": ["./*"]}}}"#,
            ),
            ("apps/app-a/shared.ts", "export const a = 1;\n"),
            ("apps/app-a/entry.ts", "import { a } from '@/shared';\n"),
            (
                "apps/app-b/tsconfig.json",
                r#"{"compilerOptions": {"baseUrl": ".", "paths": {"@/*": ["./src/*"]}}}"#,
            ),
            ("apps/app-b/src/shared.ts", "export const b = 1;\n"),
            ("apps/app-b/entry.ts", "import { b } from '@/shared';\n"),
        ],
    );

    for (file_rel, expected_dst) in [
        ("apps/app-a/entry.ts", "apps/app-a/shared"),
        ("apps/app-b/entry.ts", "apps/app-b/src/shared"),
    ] {
        let source = std::fs::read_to_string(root.join(file_rel)).unwrap();
        let mut extractor = TypescriptExtractor::new().unwrap();
        let module = module_name_from_rel_path(file_rel);
        let mut extracted = extractor.extract(&source, &module).unwrap();
        resolve_import_file_edges(root, file_rel, &module, &mut extracted.edges);

        let imports_file = extracted
            .edges
            .iter()
            .filter(|e| e.kind == "IMPORTS_FILE")
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>();
        assert_eq!(
            imports_file,
            vec![Some(expected_dst)],
            "{file_rel} must resolve @/shared against its own tsconfig.json only"
        );
    }
}

#[test]
fn unmapped_alias_and_third_party_specifier_stay_unresolved() {
    // Precision constraint: an alias with no matching `paths` entry, and a
    // genuine third-party bare specifier (`next/navigation`, which no
    // tsconfig here maps), must both produce zero IMPORTS_FILE edges — no
    // fuzzy fallback onto a same-named file elsewhere in the tree.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write_fixture(
        root,
        &[
            (
                "tsconfig.json",
                r#"{"compilerOptions": {"paths": {"@/*": ["./*"]}}}"#,
            ),
            // A same-named decoy that a fuzzy/bare-name fallback could
            // wrongly latch onto; the precise resolver must ignore it since
            // no `paths` entry maps `next/navigation` or `@/does/not/exist`.
            (
                "vendor/next/navigation.ts",
                "export function notFound() {}\n",
            ),
            (
                "app/page.tsx",
                "import { notFound } from 'next/navigation';\nimport { missing } from '@/does/not/exist';\n",
            ),
        ],
    );

    let file_rel = "app/page.tsx";
    let source = std::fs::read_to_string(root.join(file_rel)).unwrap();
    let mut extractor = TsxExtractor::new().unwrap();
    let module = module_name_from_rel_path(file_rel);
    let mut extracted = extractor.extract(&source, &module).unwrap();
    assert_eq!(
        extracted
            .edges
            .iter()
            .filter(|e| e.kind == "IMPORTS")
            .count(),
        2,
        "extraction itself must still record both raw specifiers honestly"
    );

    resolve_import_file_edges(root, file_rel, &module, &mut extracted.edges);

    // The mapped-but-missing alias keeps an *unresolved* (confidence 0)
    // edge so a later-added file re-resolves it; the third-party import
    // gets none.
    let import_files: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "IMPORTS_FILE")
        .collect();
    assert_eq!(import_files.len(), 1, "{import_files:?}");
    assert_eq!(
        import_files[0].target_qualname.as_deref(),
        Some("does/not/exist")
    );
    assert!(
        import_files[0]
            .detail
            .as_deref()
            .is_some_and(|d| d.contains("\"confidence\":0.0")),
        "{import_files:?}"
    );
}

#[test]
fn function_local_consts_and_destructuring_are_not_symbols() {
    let source = r#"
export const top = 1;
const { a, b } = require("x");
const [c, d] = [1, 2];

export async function run() {
    const controller = new AbortController();
    const response = await fetch("a");
    const { timeout = 10, ...rest } = opts;
    if (x) {
        const inner = 1;
    }
}

export const handler = async () => {
    const response = await fetch("b");
    const { q } = opts;
};

export class K {
    m() {
        const local = 1;
    }
}
"#;
    let mut extractor = TypescriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/mod").unwrap();
    let names: Vec<_> = extracted.symbols.iter().map(|s| s.name.as_str()).collect();
    for bad in ["controller", "response", "inner", "local", "q", "rest"] {
        assert!(!names.contains(&bad), "{bad} leaked: {names:?}");
    }
    assert!(
        names.iter().all(|n| !n.contains('{') && !n.contains('[')),
        "pattern symbol: {names:?}"
    );
    assert!(names.contains(&"top") && names.contains(&"handler") && names.contains(&"run"));
}

#[test]
fn top_level_destructuring_emits_one_symbol_per_binding() {
    let source = r#"
const { a, b: c } = x;
const [d, e] = y;
export const { f } = z;
const { g = 1, h: { i }, ...r } = w;
const [j, [k], ...l] = v;
function fn() {
    const { p, q: s } = o;
    const [t] = o;
}
"#;
    let mut extractor = TypescriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/mod").unwrap();
    let names: Vec<_> = extracted.symbols.iter().map(|s| s.name.as_str()).collect();
    for good in ["a", "c", "d", "e", "f", "g", "i", "r", "j", "k", "l"] {
        assert!(names.contains(&good), "{good} missing: {names:?}");
    }
    for bad in ["b", "h", "p", "q", "s", "t"] {
        assert!(!names.contains(&bad), "{bad} leaked: {names:?}");
    }
    assert!(
        names.iter().all(|n| !n.contains(['{', '[', ' '])),
        "{names:?}"
    );
}

#[test]
fn destructured_require_and_dynamic_import_are_imports_not_symbols() {
    let source = r#"
const { a } = require('./m');
const { m1 } = await import('./n');
const { b } = obj;
"#;
    let mut extractor = TypescriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/mod").unwrap();
    let names: Vec<_> = extracted.symbols.iter().map(|s| s.name.as_str()).collect();
    assert!(!names.contains(&"a") && !names.contains(&"m1"), "{names:?}");
    assert!(names.contains(&"b"), "{names:?}");
}

#[test]
fn loop_switch_locals_are_not_symbols_but_namespace_members_are() {
    let source = r#"
for (let i = 0; i < 3; i++) {}
switch (k) {
    case 1:
        const inCase = 1;
        break;
    default:
        const inDefault = 2;
}
namespace X {
    export const y = 1;
}
export namespace Z {
    export const w = 1;
}
export const response = 1;
function f() {
    const response = 2;
}
"#;
    let mut extractor = TypescriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/mod").unwrap();
    let names: Vec<_> = extracted.symbols.iter().map(|s| s.name.as_str()).collect();
    for bad in ["i", "inCase", "inDefault"] {
        assert!(!names.contains(&bad), "{bad} leaked: {names:?}");
    }
    for good in ["y", "w"] {
        assert!(names.contains(&good), "{good} missing: {names:?}");
    }
    assert_eq!(names.iter().filter(|n| **n == "response").count(), 1);
}

/// Issue #114: a nested Next.js App Router handler and a template-literal
/// `fetch` client must meet on the same normalized `/api/tables` target.
#[test]
fn nextjs_route_handlers_and_template_fetch_link_up() {
    let tmp = tempfile::tempdir().unwrap();
    common::write_files(
        tmp.path(),
        &[
            (
                "web/src/app/api/tables/route.ts",
                "export async function GET() { return Response.json([]); }\n\
                 export async function POST(req: Request) { return Response.json({}); }\n",
            ),
            (
                "web/src/lib/client.ts",
                "const BASE = process.env.API_BASE;\n\
                 export async function listTables(n: number) {\n  \
                 return fetch(`${BASE}/api/tables?limit=${n}`);\n}\n",
            ),
        ],
    );
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = lidx::indexer::Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let rows = |kind: &str| -> Vec<(String, Option<String>, String)> {
        let mut stmt = conn
            .prepare(
                "SELECT target_qualname, detail FROM edges \
                 WHERE graph_version = ? AND kind = ? ORDER BY id",
            )
            .unwrap();
        stmt.query_map(rusqlite::params![gv, kind], |r| {
            let detail: String = r.get(1)?;
            let detail: serde_json::Value = serde_json::from_str(&detail).unwrap();
            Ok((
                detail["method"].as_str().unwrap().to_string(),
                r.get::<_, Option<String>>(0)?,
                detail["path"].as_str().unwrap().to_string(),
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
    };
    let routes = rows("HTTP_ROUTE");
    let calls = rows("HTTP_CALL");
    let methods: Vec<&str> = routes.iter().map(|r| r.0.as_str()).collect();
    assert_eq!(methods, ["GET", "POST"], "{routes:?}");
    assert!(routes.iter().all(|r| r.1.as_deref() == Some("/api/tables")));
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(calls[0].1, routes[0].1);
}

/// Issue #211: calls through an in-repo `fetch` wrapper (object method over a
/// plain function, defined in another file) are HTTP calls to the URL the
/// call site supplies.
#[test]
fn fetch_wrapper_calls_link_to_next_routes() {
    let tmp = tempfile::tempdir().unwrap();
    common::write_files(
        tmp.path(),
        &[
            (
                "app/api/tables/route.ts",
                "export async function GET() { return Response.json([]); }\n\
                 export async function POST(req: Request) { return Response.json({}); }\n",
            ),
            (
                "lib/api-client.ts",
                "export async function apiClientFetch(endpoint: string, options: RequestInit = {}) {\n  \
                 return fetch(`${process.env.API}${endpoint}`, options);\n}\n\
                 export const apiClient = {\n  \
                 get: (endpoint: string, options?: RequestInit) =>\n    \
                 apiClientFetch(endpoint, { ...options, method: \"GET\" }),\n  \
                 post: (endpoint: string, body: unknown) =>\n    \
                 apiClientFetch(endpoint, { method: \"POST\", body: JSON.stringify(body) }),\n};\n\
                 export function log(msg: string) { console.log(msg); }\n",
            ),
            (
                "queries/tables/client.ts",
                "import { apiClient, apiClientFetch, log } from \"../../lib/api-client\";\n\
                 export function listTables(kind: string) {\n  \
                 return apiClient.get(`/api/tables?type=${kind}`);\n}\n\
                 export function createTable(body: unknown) {\n  \
                 return apiClient.post(\"/api/tables\", body);\n}\n\
                 export function viaPlain() {\n  \
                 return apiClientFetch(\"/api/tables\", { method: \"POST\" });\n}\n\
                 export function dynamicUrl(url: string) {\n  \
                 return apiClient.get(url);\n}\n\
                 export function notHttp() {\n  log(\"/api/tables\");\n}\n",
            ),
        ],
    );
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer =
        lidx::indexer::Indexer::new(tmp.path().to_path_buf(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, e.target_qualname, e.detail FROM edges e \
             JOIN symbols s ON s.id = e.source_symbol_id \
             WHERE e.graph_version = ? AND e.kind = 'HTTP_CALL' ORDER BY e.id",
        )
        .unwrap();
    let mut calls: Vec<(String, String, String)> = stmt
        .query_map(rusqlite::params![gv], |r| {
            let detail: serde_json::Value = serde_json::from_str(&r.get::<_, String>(2)?).unwrap();
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                detail["method"].as_str().unwrap().to_string(),
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    calls.sort();
    let c = "queries/tables/client";
    let want = |f: &str, m: &str| (format!("{c}.{f}"), "/api/tables".to_string(), m.to_string());
    assert_eq!(
        calls,
        [
            want("createTable", "POST"),
            want("listTables", "GET"),
            want("viaPlain", "POST"),
        ],
        "{calls:?}"
    );

    let (tmp_root, db_path2) = (tmp.path().to_path_buf(), db_path.clone());
    let resp = lidx::rpc::call(
        tmp.path().to_path_buf(),
        db_path,
        "explain_symbol".to_string(),
        r#"{"query":"app/api/tables/route.GET"}"#,
        "1",
    )
    .unwrap();
    assert!(resp.contains("listTables"), "GET has no caller: {resp}");
    assert!(!resp.contains("createTable"), "POST caller on GET: {resp}");
    let resp = lidx::rpc::call(
        tmp_root,
        db_path2,
        "explain_symbol".to_string(),
        r#"{"query":"app/api/tables/route.POST"}"#,
        "2",
    )
    .unwrap();
    assert!(
        resp.contains("createTable") && resp.contains("viaPlain"),
        "{resp}"
    );
    assert!(!resp.contains("listTables"), "GET caller on POST: {resp}");
}
