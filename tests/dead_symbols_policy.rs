//! Issues #461 and #462: `dead_symbols` leaves public API and conventional
//! roots out of its claims, sets the former aside in an opt-in group, and tags
//! every claim with a confidence tier and a reason.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};

fn call(indexer: &mut Indexer, public_api: bool) -> Value {
    rpc::handle_method(
        indexer,
        "dead_symbols",
        json!({"include_unused_imports": false, "include_orphan_tests": false,
               "include_public_api": public_api, "limit": 1000}),
    )
    .unwrap()
}

fn names(result: &Value, group: &str) -> Vec<String> {
    result[group]
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|s| s["qualname"].as_str().unwrap().to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn row<'a>(result: &'a Value, group: &str, suffix: &str) -> &'a Value {
    result[group]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["qualname"].as_str().unwrap().ends_with(suffix))
        .unwrap_or_else(|| panic!("{suffix} not in {group}: {result}"))
}

fn has(names: &[String], suffix: &str) -> bool {
    names.iter().any(|q| q.ends_with(suffix))
}

const GO_LIB: &str = "package lib\n\nfunc Exported() {}\nfunc unexported() {}\n\
type Widget struct{}\nfunc (w Widget) Run() {}\nfunc (w Widget) stop() {}\n\
type hidden struct{}\nfunc (h hidden) Run() {}\n";

#[test]
fn go_exported_in_importable_package_is_public_api_not_a_claim() {
    let (_tmp, _root, mut ix) = common::live_indexer(
        "lidx-dead-go-",
        &[
            ("go.mod", "module example.com/m\n\ngo 1.22\n"),
            ("lib/lib.go", GO_LIB),
        ],
    );
    let r = call(&mut ix, false);
    let dead = names(&r, "dead_symbols");
    assert!(!has(&dead, "lib.Exported"), "{dead:?}");
    assert!(!has(&dead, "Widget"), "{dead:?}");
    assert!(!has(&dead, "Widget.Run"), "{dead:?}");
    assert!(has(&dead, "lib.unexported"), "{dead:?}");
    assert!(has(&dead, "Widget.stop"), "{dead:?}");
    assert!(has(&dead, "hidden"), "{dead:?}");
    // an exported method of an unexported type may satisfy an interface held outside
    assert!(!has(&dead, "hidden.Run"), "{dead:?}");
    assert!(r.get("public_api").is_none(), "group only on request");
    assert!(r["counts"]["public_api"].as_u64().unwrap() >= 3);
    assert_eq!(
        row(&r, "dead_symbols", "lib.unexported")["confidence"],
        "high"
    );
}

#[test]
fn opt_in_returns_public_api_group_tagged_low_with_reason() {
    let (_tmp, _root, mut ix) = common::live_indexer(
        "lidx-dead-go-",
        &[
            ("go.mod", "module example.com/m\n\ngo 1.22\n"),
            ("lib/lib.go", GO_LIB),
        ],
    );
    let r = call(&mut ix, true);
    let api = names(&r, "public_api");
    assert!(has(&api, "lib.Exported"), "{api:?}");
    assert!(has(&api, "Widget.Run"), "{api:?}");
    assert!(!has(&api, "lib.unexported"), "{api:?}");
    let e = row(&r, "public_api", "lib.Exported");
    assert_eq!(e["confidence"], "low");
    assert!(e["reason"].as_str().unwrap().contains("public API"));
    // the claims list is unchanged by the flag
    assert!(!has(&names(&r, "dead_symbols"), "lib.Exported"));
    assert_eq!(
        r["counts"]["public_api"].as_u64().unwrap() as usize,
        api.len()
    );
}

#[test]
fn go_exported_in_main_or_internal_is_judged_at_medium() {
    let (_tmp, _root, mut ix) = common::live_indexer(
        "lidx-dead-go-",
        &[
            ("go.mod", "module example.com/m\n\ngo 1.22\n"),
            (
                "cmd/app/main.go",
                "package main\n\nfunc main() { run() }\nfunc run() {}\nfunc Helper() {}\n",
            ),
            (
                "internal/util/util.go",
                "package util\n\nfunc Orphan() {}\n",
            ),
        ],
    );
    let r = call(&mut ix, false);
    let dead = names(&r, "dead_symbols");
    assert!(has(&dead, "main.Helper"), "{dead:?}");
    assert!(has(&dead, "util.Orphan"), "{dead:?}");
    assert_eq!(
        row(&r, "dead_symbols", "main.Helper")["confidence"],
        "medium"
    );
    assert_eq!(
        row(&r, "dead_symbols", "util.Orphan")["confidence"],
        "medium"
    );
    // `main` is a root
    assert!(!has(&dead, "main.main"), "{dead:?}");
}

#[test]
fn go_test_entry_points_init_and_main_are_roots() {
    let (_tmp, _root, mut ix) = common::live_indexer(
        "lidx-dead-go-",
        &[
            ("go.mod", "module example.com/m\n\ngo 1.22\n"),
            (
                "pkg/a.go",
                "package pkg\n\nfunc init() {}\nfunc helper() {}\n",
            ),
            (
                "pkg/a_test.go",
                "package pkg\n\nimport \"testing\"\n\nfunc TestA(t *testing.T) {}\n\
func BenchmarkA(b *testing.B) {}\nfunc FuzzA(f *testing.F) {}\nfunc ExampleA() {\n\t// Output:\n\t// hi\n}\n\
func TestMain(m *testing.M) {}\nfunc Testify() {}\nfunc ExampleNoOutput() {\n\t_ = 1\n}\nfunc ExampleUnordered() {\n\t// Unordered output:\n\t// b\n}\nfunc testlower() {}\nfunc helperInTest() {}\n",
            ),
            (
                "cmd/x/main.go",
                "package main\n\nfunc main() {}\nfunc init() {}\n",
            ),
        ],
    );
    let r = call(&mut ix, true);
    let all: Vec<String> = names(&r, "dead_symbols")
        .into_iter()
        .chain(names(&r, "public_api"))
        .collect();
    for root in [
        "TestA",
        "BenchmarkA",
        "FuzzA",
        "ExampleA",
        "TestMain",
        "init",
        "main",
    ] {
        assert!(
            !all.iter().any(|q| q.ends_with(&format!(".{root}"))),
            "{root} claimed: {all:?}"
        );
    }
    // lowercase after the prefix is not a test entry point; ordinary helpers still count
    let dead = names(&r, "dead_symbols");
    assert!(has(&dead, "a_test.Testify"), "{dead:?}");
    // an example with no Output comment is never run: judged like any function
    assert!(has(&dead, "a_test.ExampleNoOutput"), "{dead:?}");
    assert!(!has(&dead, "a_test.ExampleUnordered"), "{dead:?}");
    assert!(has(&dead, "a_test.testlower"), "{dead:?}");
    assert!(has(&dead, "a_test.helperInTest"), "{dead:?}");
    assert!(has(&dead, "a.helper"), "{dead:?}");
}

#[test]
fn test_functions_outside_test_files_are_not_roots() {
    let (_tmp, _root, mut ix) = common::live_indexer(
        "lidx-dead-go-",
        &[
            ("go.mod", "module example.com/m\n\ngo 1.22\n"),
            ("internal/x/x.go", "package x\n\nfunc TestNotATest() {}\n"),
        ],
    );
    let r = call(&mut ix, false);
    assert!(has(&names(&r, "dead_symbols"), "x.TestNotATest"));
}

const RUST_LIB: &str = "pub trait Shape { fn area(&self) -> u32; fn name(&self) -> u32 { 0 } }\nfn only_here() {}\npub fn api() {}\nfn private_fn() {}\npub(crate) fn crate_fn() {}\n\
pub struct Api;\nimpl Api { pub fn method(&self) {} fn hidden(&self) {} }\n\
struct Hidden;\nimpl Hidden { pub fn method(&self) {} }\n\
#[bench]\nfn bench_it() {}\n";

#[test]
fn rust_pub_items_of_a_library_crate_are_public_api() {
    let (_tmp, _root, mut ix) = common::live_indexer(
        "lidx-dead-rs-",
        &[
            (
                "Cargo.toml",
                "[package]\nname = \"c\"\nversion = \"0.1.0\"\n",
            ),
            ("src/lib.rs", RUST_LIB),
        ],
    );
    let r = call(&mut ix, true);
    let dead = names(&r, "dead_symbols");
    let api = names(&r, "public_api");
    assert!(has(&api, "::api"), "{api:?}");
    assert!(has(&api, "Api"), "{api:?}");
    assert!(has(&api, "Api::method"), "{api:?}");
    // methods of a pub trait are as public as the trait
    assert!(has(&api, "Shape::name"), "{api:?}");
    for private in [
        "private_fn",
        "crate_fn",
        "Api::hidden",
        "Hidden",
        "Hidden::method",
    ] {
        assert!(has(&dead, private), "{private} missing from {dead:?}");
        assert!(
            !has(&api, &format!("::{private}")) || private == "Api::method",
            "{private} in api"
        );
    }
    assert!(!has(&dead, "bench_it") && !has(&api, "bench_it"));
    assert_eq!(row(&r, "dead_symbols", "private_fn")["confidence"], "high");
}

#[test]
fn rust_pub_items_of_a_binary_crate_stay_judged() {
    let (_tmp, _root, mut ix) = common::live_indexer(
        "lidx-dead-rs-",
        &[
            (
                "Cargo.toml",
                "[package]\nname = \"b\"\nversion = \"0.1.0\"\n",
            ),
            ("src/main.rs", "fn main() {}\npub fn unused_pub() {}\n"),
        ],
    );
    let r = call(&mut ix, false);
    let dead = names(&r, "dead_symbols");
    assert!(has(&dead, "unused_pub"), "{dead:?}");
    assert_eq!(r["counts"]["public_api"], 0);
}

const TS_INDEX: &str = "export { used, reexported } from './lib';\nexport * from './star';\n";
const TS_LIB: &str = "export function used() { return 1; }\nexport function reexported() {}\n\
export function deepOnly() {}\nfunction localDead() {}\n\
export class Api { run() {} private secret() {} }\n";
const TS_STAR: &str = "export const fromStar = 1;\n";

fn ts_package() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "package.json",
            "{\"name\":\"p\",\"main\":\"dist/index.js\",\"types\":\"dist/index.d.ts\"}",
        ),
        ("src/index.ts", TS_INDEX),
        ("src/lib.ts", TS_LIB),
        ("src/star.ts", TS_STAR),
    ]
}

#[test]
fn ts_entry_exports_are_public_api_other_exports_are_judged() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-ts-", &ts_package());
    let r = call(&mut ix, true);
    let dead = names(&r, "dead_symbols");
    let api = names(&r, "public_api");
    assert!(
        has(&api, "lib.used") && has(&api, "lib.reexported"),
        "{api:?}"
    );
    assert!(has(&api, "star.fromStar"), "{api:?}");
    assert!(has(&dead, "lib.deepOnly"), "{dead:?}");
    assert!(has(&dead, "lib.localDead"), "{dead:?}");
    assert_eq!(
        row(&r, "dead_symbols", "lib.localDead")["confidence"],
        "high"
    );
    assert_eq!(
        row(&r, "dead_symbols", "lib.deepOnly")["confidence"],
        "medium"
    );
}

#[test]
fn ts_without_package_json_treats_every_export_as_public() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-ts-", &[("src/lib.ts", TS_LIB)]);
    let r = call(&mut ix, true);
    let dead = names(&r, "dead_symbols");
    let api = names(&r, "public_api");
    assert!(has(&api, "lib.deepOnly"), "{api:?}");
    assert!(has(&dead, "lib.localDead"), "{dead:?}");
    assert!(!has(&dead, "lib.deepOnly"), "{dead:?}");
}

#[test]
fn language_without_modelled_visibility_is_medium_with_reason() {
    let (_tmp, _root, mut ix) = common::live_indexer(
        "lidx-dead-py-",
        &[(
            "m.py",
            "def lonely():\n    pass\n\ndef caller():\n    thing.lonely_ext()\n    thing.mystery()\n\ndef mystery():\n    pass\n",
        )],
    );
    let r = call(&mut ix, false);
    // Python's visibility is not modelled: tier medium either way, reason says so.
    let lonely = row(&r, "dead_symbols", "m.lonely");
    assert_eq!(lonely["confidence"], "medium");
    assert!(lonely["reason"].as_str().unwrap().contains("not modelled"));
}

#[test]
fn private_with_unresolved_same_name_reference_is_medium_otherwise_high() {
    let files = [
        (
            "src/a.ts",
            "function plainDead() {}\nfunction shadowed() {}\nexport function keep() {}\n",
        ),
        (
            "src/b.ts",
            "declare const o: any;\nexport function use() { o.shadowed(); }\n",
        ),
    ];
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-ts-", &files);
    let r = call(&mut ix, false);
    let plain = row(&r, "dead_symbols", "a.plainDead");
    assert_eq!(plain["confidence"], "high", "{plain}");
    assert!(
        plain["reason"]
            .as_str()
            .unwrap()
            .contains("no unresolved reference")
    );
    let shadowed = row(&r, "dead_symbols", "a.shadowed");
    assert_eq!(shadowed["confidence"], "medium", "{shadowed}");
    assert!(
        shadowed["reason"]
            .as_str()
            .unwrap()
            .contains("unresolved reference names it")
    );
}

#[test]
fn export_flip_after_incremental_sync_moves_the_symbol_between_groups() {
    let (_tmp, root, mut ix) = common::live_indexer(
        "lidx-dead-go-",
        &[
            ("go.mod", "module example.com/m\n\ngo 1.22\n"),
            ("lib/lib.go", "package lib\n\nfunc Thing() {}\n"),
        ],
    );
    let r = call(&mut ix, true);
    assert!(has(&names(&r, "public_api"), "lib.Thing"));
    assert!(!has(&names(&r, "dead_symbols"), "lib.Thing"));

    // unexport it: the (renamed) function becomes an ordinary claim
    std::fs::write(root.join("lib/lib.go"), "package lib\n\nfunc thing() {}\n").unwrap();
    ix.sync_rel_paths(&["lib/lib.go".to_string()]).unwrap();
    let r = call(&mut ix, true);
    assert!(has(&names(&r, "dead_symbols"), "lib.thing"), "{r}");
    assert!(names(&r, "public_api").is_empty(), "{r}");

    // `package main` makes every exported identifier unimportable
    std::fs::write(root.join("lib/lib.go"), "package main\n\nfunc Thing() {}\n").unwrap();
    ix.sync_rel_paths(&["lib/lib.go".to_string()]).unwrap();
    let r = call(&mut ix, true);
    assert!(has(&names(&r, "dead_symbols"), "lib.Thing"), "{r}");
    assert!(names(&r, "public_api").is_empty(), "{r}");

    // back to an importable package
    std::fs::write(root.join("lib/lib.go"), "package lib\n\nfunc Thing() {}\n").unwrap();
    ix.sync_rel_paths(&["lib/lib.go".to_string()]).unwrap();
    let r = call(&mut ix, true);
    assert!(has(&names(&r, "public_api"), "lib.Thing"), "{r}");
}

#[test]
fn rust_pub_flip_after_incremental_sync() {
    let (_tmp, root, mut ix) = common::live_indexer(
        "lidx-dead-rs-",
        &[
            (
                "Cargo.toml",
                "[package]\nname = \"c\"\nversion = \"0.1.0\"\n",
            ),
            ("src/lib.rs", "pub fn thing() {}\n"),
        ],
    );
    let r = call(&mut ix, true);
    assert!(has(&names(&r, "public_api"), "thing"));
    std::fs::write(root.join("src/lib.rs"), "pub(crate) fn thing() {}\n").unwrap();
    ix.sync_rel_paths(&["src/lib.rs".to_string()]).unwrap();
    let r = call(&mut ix, true);
    assert!(has(&names(&r, "dead_symbols"), "thing"), "{r}");
    assert!(names(&r, "public_api").is_empty(), "{r}");
    std::fs::write(root.join("src/lib.rs"), "pub fn thing() {}\n").unwrap();
    ix.sync_rel_paths(&["src/lib.rs".to_string()]).unwrap();
    let r = call(&mut ix, true);
    assert!(has(&names(&r, "public_api"), "thing"), "{r}");
}

#[test]
fn ts_export_flip_after_incremental_sync() {
    let (_tmp, root, mut ix) = common::live_indexer("lidx-dead-ts-", &ts_package());
    let r = call(&mut ix, true);
    assert!(has(&names(&r, "public_api"), "lib.used"));
    // drop `used` from the entry barrel: still exported by lib.ts, no longer public
    std::fs::write(
        root.join("src/index.ts"),
        "export { reexported } from './lib';\nexport * from './star';\n",
    )
    .unwrap();
    ix.sync_rel_paths(&["src/index.ts".to_string()]).unwrap();
    let r = call(&mut ix, true);
    assert!(has(&names(&r, "dead_symbols"), "lib.used"), "{r}");
    assert!(has(&names(&r, "public_api"), "lib.reexported"), "{r}");
}

#[test]
fn counts_report_withheld_public_api_by_default() {
    let (_tmp, _root, mut ix) = common::live_indexer(
        "lidx-dead-go-",
        &[
            ("go.mod", "module example.com/m\n\ngo 1.22\n"),
            ("lib/lib.go", GO_LIB),
        ],
    );
    let r = call(&mut ix, false);
    assert!(r["counts"]["public_api"].as_u64().unwrap() > 0);
}

#[test]
fn rust_bench_targets_are_roots() {
    let (_tmp, _root, mut ix) = common::live_indexer(
        "lidx-dead-rs-",
        &[
            (
                "Cargo.toml",
                "[package]\nname = \"c\"\nversion = \"0.1.0\"\n",
            ),
            ("src/lib.rs", "fn helper() {}\n"),
            (
                "benches/b.rs",
                "fn bench_group() {}\nfn criterion_main_like() {}\n",
            ),
        ],
    );
    let r = call(&mut ix, true);
    let all: Vec<String> = names(&r, "dead_symbols")
        .into_iter()
        .chain(names(&r, "public_api"))
        .collect();
    assert!(has(&all, "helper"), "{all:?}");
    assert!(
        !has(&all, "bench_group") && !has(&all, "criterion_main_like"),
        "{all:?}"
    );
}

#[test]
fn rust_pub_item_in_a_private_module_counts_as_public_api() {
    // Documented over-approximation: the module path is not checked.
    let (_tmp, _root, mut ix) = common::live_indexer(
        "lidx-dead-rs-",
        &[
            (
                "Cargo.toml",
                "[package]\nname = \"c\"\nversion = \"0.1.0\"\n",
            ),
            ("src/lib.rs", "mod inner;\n"),
            ("src/inner.rs", "pub fn deep() {}\nfn shallow() {}\n"),
        ],
    );
    let r = call(&mut ix, true);
    assert!(has(&names(&r, "public_api"), "inner::deep"), "{r}");
    assert!(has(&names(&r, "dead_symbols"), "inner::shallow"), "{r}");
}

#[test]
fn editing_package_json_changes_the_public_set_after_sync() {
    let (_tmp, root, mut ix) = common::live_indexer("lidx-dead-ts-", &ts_package());
    let r = call(&mut ix, true);
    assert!(has(&names(&r, "dead_symbols"), "lib.deepOnly"));
    assert!(!has(&names(&r, "public_api"), "lib.deepOnly"));
    // point the entry at lib.ts directly: its exports become the API
    std::fs::write(
        root.join("package.json"),
        "{\"name\":\"p\",\"main\":\"dist/lib.js\"}",
    )
    .unwrap();
    ix.sync_rel_paths(&["package.json".to_string()]).unwrap();
    let r = call(&mut ix, true);
    assert!(has(&names(&r, "public_api"), "lib.deepOnly"), "{r}");
    assert!(!has(&names(&r, "public_api"), "star.fromStar"), "{r}");
}

/// A declaration the grammar cannot parse (a long overload list of generic
/// call signatures, from Hono's `helper/factory`, MIT) swallows the export
/// statements after it; the package entry must still show them.
#[test]
fn ts_exports_after_a_parse_error_are_still_public_api() {
    let source = include_str!("fixtures/ts_error_recovery/factory.ts.txt");
    let (_tmp, _root, mut ix) = common::live_indexer(
        "lidx-dead-ts-parse-error-",
        &[
            (
                "package.json",
                "{\"name\":\"p\",\"main\":\"src/factory.ts\"}\n",
            ),
            ("src/factory.ts", source),
        ],
    );
    let r = call(&mut ix, true);
    assert!(
        has(&names(&r, "public_api"), "factory.createFactory"),
        "{r}"
    );
    assert!(
        has(&names(&r, "public_api"), "factory.createMiddleware"),
        "{r}"
    );
    assert!(
        !has(&names(&r, "dead_symbols"), "factory.createFactory"),
        "{r}"
    );
}

#[test]
fn ts_entry_in_a_source_directory_is_mapped_from_the_built_path() {
    let (_tmp, _root, mut ix) = common::live_indexer(
        "lidx-dead-ts-source-dir-",
        &[
            (
                "package.json",
                "{\"name\":\"p\",\"main\":\"./distribution/index.js\",\"types\":\"./distribution/index.d.ts\"}\n",
            ),
            (
                "source/index.ts",
                "export {helper} from './utils.js';\nexport const api = 1;\n",
            ),
            (
                "source/utils.ts",
                "export const helper = () => 1;\nexport const internal = () => 2;\n",
            ),
        ],
    );
    let r = call(&mut ix, true);
    assert!(has(&names(&r, "public_api"), "utils.helper"), "{r}");
    assert!(has(&names(&r, "public_api"), "source.api"), "{r}");
    assert!(has(&names(&r, "dead_symbols"), "utils.internal"), "{r}");
}
