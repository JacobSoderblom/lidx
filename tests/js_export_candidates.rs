//! Issue #151: non-exported JS/TS top-level symbols are not name-fallback
//! resolution candidates for callers in other files. Same-file resolution,
//! ESM `export`, `export { a, b as c }` lists and CommonJS exports still are.

mod common;

fn call_targets(
    caller: &str,
    snapshot: &std::collections::BTreeSet<common::golden::EdgeKey>,
) -> Vec<String> {
    snapshot
        .iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == caller)
        .filter_map(|e| e.target_qualname.clone())
        .collect()
}

#[test]
fn non_exported_symbols_are_not_cross_file_candidates() {
    let (_tmp, snap) = common::index_files(&[
        (
            "src/priv.ts",
            "function secretHelper() {}\nexport function inFile() { secretHelper(); }\n",
        ),
        (
            "src/esm.ts",
            "export function esmHelper() {}\nfunction listed() {}\nfunction renamed() {}\nexport { listed, renamed as other };\n",
        ),
        (
            "src/cjs.js",
            "function cjsOne() {}\nfunction cjsTwo() {}\nfunction cjsThree() {}\nmodule.exports = { cjsOne };\nexports.two = cjsTwo;\nmodule.exports.three = cjsThree;\n",
        ),
        (
            "src/caller.ts",
            "export function run() {\n  secretHelper();\n  esmHelper();\n  listed();\n  renamed();\n  cjsOne();\n  cjsTwo();\n  cjsThree();\n}\n",
        ),
    ]);
    let run = call_targets("src/caller.run", &snap);
    for want in [
        "src/esm.esmHelper",
        "src/esm.listed",
        "src/esm.renamed",
        "src/cjs.cjsOne",
        "src/cjs.cjsTwo",
        "src/cjs.cjsThree",
    ] {
        assert!(
            run.iter().any(|t| t == want),
            "{want} should resolve: {run:?}"
        );
    }
    assert!(
        !run.iter().any(|t| t == "src/priv.secretHelper"),
        "non-exported symbol must not bind cross-file: {run:?}"
    );
    let same = call_targets("src/priv.inFile", &snap);
    assert!(
        same.iter().any(|t| t == "src/priv.secretHelper"),
        "same-file resolution must still work: {same:?}"
    );
}
