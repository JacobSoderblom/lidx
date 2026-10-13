//! A `get` / `set` accessor is run by a property read that no edge records,
//! so `dead_symbols` never reports one; the methods and local callbacks
//! around it are still judged.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;

const FILES: &[(&str, &str)] = &[(
    "src/model.ts",
    "export class Model {\n  get shape() { return 1; }\n  get unread() { return 2; }\n  dead() { return 3; }\n  copy() { return this.shape; }\n}\nexport function make() {\n  const orphan = () => 1;\n  return new Model().copy();\n}\n",
)];

#[test]
fn accessors_are_never_dead_but_methods_still_are() {
    let (_tmp, root, db_path) = common::index_repo("lidx-ts-dead-accessor-", FILES);
    let mut indexer = Indexer::new(root, db_path).unwrap();
    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"include_unused_imports": false, "include_orphan_tests": false, "include_public_api": true}),
    )
    .unwrap();
    let dead: Vec<String> = result["dead_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .chain(result["public_api"].as_array().unwrap())
        .filter_map(|s| s["qualname"].as_str().map(str::to_string))
        .collect();
    assert!(dead.iter().any(|d| d == "src/model.Model.dead"), "{dead:?}");
    for q in ["src/model.Model.shape", "src/model.Model.unread"] {
        assert!(!dead.iter().any(|d| d == q), "{q}: {dead:?}");
    }
}
