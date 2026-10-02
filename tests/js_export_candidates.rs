//! Issue #151: non-exported JS/TS top-level symbols are not name-fallback
//! resolution candidates for callers in other files. Same-file resolution,
//! exported symbols, and classic-script globals still resolve.

mod common;

use std::collections::BTreeSet;

use common::golden::EdgeKey;

fn targets(caller: &str, snap: &BTreeSet<EdgeKey>) -> Vec<String> {
    snap.iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == caller)
        .filter_map(|e| e.target_qualname.clone())
        .collect()
}

/// Indexes `defs` plus `src/caller.ts` calling each of `names` bare, and
/// returns the resolved targets of `src/caller.run`. The caller is a classic
/// script (no `import`/`export`): in an ES module an unbound bare name cannot
/// reach another file's export at all (issue #232), see
/// `esm_caller_does_not_reach_unimported_exports`.
fn resolved_from_caller(defs: &[(&str, &str)], names: &[&str]) -> Vec<String> {
    let calls: String = names.iter().map(|n| format!("  {n}();\n")).collect();
    let caller = format!("function run() {{\n{calls}}}\n");
    let mut files = defs.to_vec();
    files.push(("src/caller.ts", caller.as_str()));
    let (_tmp, snap) = common::index_files(&files);
    targets("src/caller.run", &snap)
}

fn assert_resolves(got: &[String], want: &[&str]) {
    for w in want {
        assert!(got.iter().any(|t| t == w), "{w} should resolve: {got:?}");
    }
}

#[test]
fn esm_exports_resolve_cross_file() {
    let got = resolved_from_caller(
        &[
            (
                "src/esm.ts",
                "export function esmHelper() {}\nfunction listed() {}\nfunction renamed() {}\nexport { listed, renamed as other };\n",
            ),
            (
                "src/def.ts",
                "function dflt() {}\nexport default dflt;\nexport const {a, b} = obj;\n",
            ),
            (
                "src/obj.ts",
                "function inObj() {}\nfunction hidden() {}\nexport default { inObj };\n",
            ),
        ],
        &[
            "esmHelper",
            "listed",
            "renamed",
            "dflt",
            "a",
            "b",
            "plain",
            "inObj",
        ],
    );
    assert_resolves(
        &got,
        &[
            "src/esm.esmHelper",
            "src/esm.listed",
            "src/esm.renamed",
            "src/def.dflt",
            "src/obj.inObj",
        ],
    );
}

#[test]
fn esm_caller_does_not_reach_unimported_exports() {
    let (_tmp, snap) = common::index_files(&[
        ("src/esm.ts", "export function esmHelper() {}\n"),
        (
            "src/caller.ts",
            "export function run() {\n  esmHelper();\n}\n",
        ),
    ]);
    assert!(
        !targets("src/caller.run", &snap)
            .iter()
            .any(|t| t == "src/esm.esmHelper"),
        "{:?}",
        targets("src/caller.run", &snap)
    );
}

#[test]
fn commonjs_exports_resolve_cross_file() {
    let got = resolved_from_caller(
        &[(
            "src/cjs.js",
            "function cjsOne() {}\nfunction cjsTwo() {}\nfunction cjsThree() {}\nmodule.exports = { cjsOne };\nexports.two = cjsTwo;\nmodule.exports.three = cjsThree;\n",
        )],
        &["cjsOne", "cjsTwo", "cjsThree"],
    );
    assert_resolves(
        &got,
        &["src/cjs.cjsOne", "src/cjs.cjsTwo", "src/cjs.cjsThree"],
    );
}

#[test]
fn non_exported_symbol_is_not_a_cross_file_candidate() {
    let got = resolved_from_caller(
        &[(
            "src/priv.ts",
            "function secretHelper() {}\nexport function pub() {}\n",
        )],
        &["secretHelper"],
    );
    assert!(!got.iter().any(|t| t == "src/priv.secretHelper"), "{got:?}");
}

#[test]
fn non_exported_symbol_still_resolves_in_its_own_file() {
    let (_tmp, snap) = common::index_files(&[(
        "src/priv.ts",
        "function secretHelper() {}\nexport function inFile() { secretHelper(); }\n",
    )]);
    assert_resolves(
        &targets("src/priv.inFile", &snap),
        &["src/priv.secretHelper"],
    );
}

#[test]
fn classic_script_globals_resolve_cross_file() {
    let (_tmp, snap) = common::index_files(&[
        ("a.js", "function boot() {}\n"),
        ("b.js", "function start() { boot(); }\n"),
    ]);
    assert_resolves(&targets("b.start", &snap), &["a.boot"]);
}

#[test]
fn unanalysable_export_forms_mark_nothing_private() {
    let got = resolved_from_caller(
        &[
            (
                "src/wrap.js",
                "function wrapped() {}\nmodule.exports = wrap(wrapped);\n",
            ),
            (
                "src/spread.js",
                "function spreadFn() {}\nmodule.exports = { ...other };\n",
            ),
            (
                "src/assign.js",
                "function assigned() {}\nObject.assign(module.exports, { assigned });\n",
            ),
        ],
        &["wrapped", "spreadFn", "assigned"],
    );
    assert_resolves(
        &got,
        &[
            "src/wrap.wrapped",
            "src/spread.spreadFn",
            "src/assign.assigned",
        ],
    );
}
