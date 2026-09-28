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

    assert!(
        extracted.edges.iter().all(|e| e.kind != "IMPORTS_FILE"),
        "neither the unmapped alias nor the third-party import may resolve, got {:?}",
        extracted
            .edges
            .iter()
            .filter(|e| e.kind == "IMPORTS_FILE")
            .collect::<Vec<_>>()
    );
}
