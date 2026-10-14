//! Issue #472: a method that implements an interface or trait of the
//! standard library or a dependency is called through it, so it is live when
//! its receiver type is live; the same method of a dead type stays dead.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};

fn call(indexer: &mut Indexer) -> Value {
    rpc::handle_method(
        indexer,
        "dead_symbols",
        json!({"include_unused_imports": false, "include_orphan_tests": false,
               "include_public_api": true, "include_external_interface": true,
               "limit": 1000}),
    )
    .unwrap()
}

fn group(result: &Value, name: &str) -> Vec<String> {
    result[name]
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|s| s["qualname"].as_str().unwrap().to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// Every claim, public or not: these tests judge liveness, not the policy.
fn claimed(result: &Value) -> Vec<String> {
    let mut all = group(result, "dead_symbols");
    all.extend(group(result, "public_api"));
    all
}

fn has(names: &[String], suffix: &str) -> bool {
    names.iter().any(|q| q.ends_with(suffix))
}

fn listed(result: &Value, suffix: &str) -> Value {
    result["implements_external_interface"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["qualname"].as_str().unwrap().ends_with(suffix))
        .unwrap_or_else(|| panic!("{suffix} not listed: {result}"))
        .clone()
}

const GO_FILES: &[(&str, &str)] = &[
    ("go.mod", "module example.com/app\n\ngo 1.22\n"),
    (
        "main.go",
        "package main\n\nimport \"example.com/app/internal/app\"\n\n\
         func main() {\n\t_ = app.NewLive()\n\t_ = app.Failure{}\n\t_ = app.Sorted{}\n}\n",
    ),
    (
        "internal/app/app.go",
        "package app\n\nimport \"fmt\"\n\n\
         type Live struct{ n int }\n\n\
         func NewLive() *Live { return &Live{} }\n\n\
         func (l *Live) String() string { return fmt.Sprint(l.n) }\n\
         func (l *Live) Read(p []byte) (int, error) { return 0, nil }\n\
         func (l *Live) PostLoad() error { return nil }\n\
         func (l *Live) Len() int { return l.n }\n\
         func (l *Live) Helper() int { return 1 }\n\
         func (l *Live) Name() string { return \"x\" }\n\n\
         type Failure struct{}\n\n\
         func (Failure) Error() string { return \"f\" }\n\n\
         type Sorted []int\n\n\
         func (s Sorted) Len() int { return len(s) }\n\
         func (s Sorted) Less(i, j int) bool { return s[i] < s[j] }\n\
         func (s Sorted) Swap(i, j int) { s[i], s[j] = s[j], s[i] }\n\n\
         type Unused struct{ n int }\n\n\
         func (u *Unused) String() string { return \"u\" }\n\
         func (u *Unused) Read(p []byte) (int, error) { return 0, nil }\n\
         func (u *Unused) PostLoad() error { return nil }\n",
    ),
];

#[test]
fn go_methods_of_a_live_type_matching_an_external_interface_are_live() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-ext-go-", GO_FILES);
    let r = call(&mut ix);
    let dead = claimed(&r);
    for live in [
        "Live.String",
        "Live.Read",
        "Failure.Error",
        "Sorted.Len",
        "Sorted.Less",
        "Sorted.Swap",
    ] {
        assert!(!has(&dead, live), "{live} claimed: {dead:?}");
    }
    let row = listed(&r, "Live.String");
    assert_eq!(row["confidence"], "medium", "{row}");
    assert!(
        row["reason"]
            .as_str()
            .unwrap()
            .starts_with("implements_external_interface"),
        "{row}"
    );
    assert_eq!(r["counts"]["implements_external_interface"], 6, "{r}");
}

#[test]
fn go_other_methods_and_dead_types_stay_dead() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-ext-go-", GO_FILES);
    let dead = claimed(&call(&mut ix));
    // not part of any external interface (`Len` alone is not sort.Interface)
    assert!(has(&dead, "Live.Helper"), "{dead:?}");
    assert!(has(&dead, "Live.Len"), "{dead:?}");
    assert!(has(&dead, "Live.Name"), "{dead:?}");
    // a dependency interface is no evidence without an assertion or embedding
    assert!(has(&dead, "Live.PostLoad"), "{dead:?}");
    // the same methods on an unused type
    for method in ["Unused.String", "Unused.Read", "Unused.PostLoad"] {
        assert!(has(&dead, method), "{method} not claimed: {dead:?}");
    }
}

#[test]
fn go_signature_must_match() {
    let files = [
        ("go.mod", "module example.com/app\n\ngo 1.22\n"),
        (
            "main.go",
            "package main\n\ntype T struct{}\n\nfunc (T) String(n int) string { return \"\" }\n\
             func (T) Close() int { return 0 }\n\nfunc main() { _ = T{} }\n",
        ),
    ];
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-ext-go-sig-", &files);
    let dead = claimed(&call(&mut ix));
    assert!(has(&dead, "T.String"), "{dead:?}");
    assert!(has(&dead, "T.Close"), "{dead:?}");
}

const TS_FILES: &[(&str, &str)] = &[
    (
        "package.json",
        "{\"name\":\"app\",\"main\":\"src/index.ts\"}\n",
    ),
    (
        "src/index.ts",
        "import { Live } from './live';\nexport const x = new Live();\n",
    ),
    (
        "src/live.ts",
        "import { Disposable } from 'vscode';\n\
         export class Live implements Disposable, Iterable<number> {\n\
         \x20 toString(): string { return 'live'; }\n\
         \x20 dispose(): void {}\n\
         \x20 [Symbol.iterator](): Iterator<number> { return [1][Symbol.iterator](); }\n\
         \x20 helper(): void {}\n\
         }\n\
         export class Unused implements Disposable {\n\
         \x20 toString(): string { return 'u'; }\n\
         \x20 dispose(): void {}\n\
         }\n",
    ),
];

#[test]
fn ts_methods_of_a_live_class_satisfying_an_external_interface_are_live() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-ext-ts-", TS_FILES);
    let r = call(&mut ix);
    let dead = claimed(&r);
    for live in ["Live.toString", "Live.dispose", "Live.[Symbol.iterator]"] {
        assert!(!has(&dead, live), "{live} claimed: {dead:?}");
    }
    assert!(has(&dead, "Live.helper"), "{dead:?}");
    for method in ["Unused.toString", "Unused.dispose"] {
        assert!(has(&dead, method), "{method} not claimed: {dead:?}");
    }
    assert_eq!(listed(&r, "Live.dispose")["confidence"], "medium");
}

const RUST_FILES: &[(&str, &str)] = &[
    (
        "Cargo.toml",
        "[package]\nname = \"c\"\nversion = \"0.1.0\"\n",
    ),
    (
        "src/lib.rs",
        "use std::fmt;\n\n\
         struct Used {\n    n: u8,\n}\nstruct Orphan {\n    n: u8,\n}\n\n\
         impl fmt::Display for Used {\n    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(\"u\") }\n}\n\
         impl fmt::Display for Orphan {\n    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(\"o\") }\n}\n\
         impl Used {\n    fn inherent(&self) {}\n}\n\n\
         pub fn make() -> String { Used { n: 1 }.to_string() }\n",
    ),
];

#[test]
fn rust_external_trait_impl_of_a_live_type_is_live_and_of_a_dead_type_is_dead() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-ext-rs-", RUST_FILES);
    let r = call(&mut ix);
    let dead = claimed(&r);
    assert!(!has(&dead, "Used::fmt"), "{dead:?}");
    assert!(has(&dead, "Orphan::fmt"), "{dead:?}");
    assert!(has(&dead, "Orphan"), "{dead:?}");
    assert!(has(&dead, "Used::inherent"), "{dead:?}");
    assert_eq!(listed(&r, "Used::fmt")["confidence"], "medium");
}

/// Everything `dead_symbols` answers, for comparing two indexes.
fn answer(indexer: &mut Indexer) -> Value {
    let r = call(indexer);
    json!({
        "dead": group(&r, "dead_symbols"),
        "public": group(&r, "public_api"),
        "external": group(&r, "implements_external_interface"),
        "counts": r["counts"],
    })
}

fn fresh_answer(files: &[(&str, &str)], edits: &[(&str, &str)]) -> Value {
    let merged: Vec<(&str, &str)> = files
        .iter()
        .map(|(p, s)| {
            let s = edits.iter().find(|(e, _)| e == p).map_or(*s, |(_, s)| *s);
            (*p, s)
        })
        .collect();
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-ext-fresh-", &merged);
    answer(&mut ix)
}

fn assert_flip(files: &[(&str, &str)], edit: (&str, &str), method: &str) {
    let (_tmp, root, mut ix) = common::live_indexer("lidx-dead-ext-inc-", files);
    let before = answer(&mut ix);
    assert!(has(&strings(&before["dead"]), method), "{before}");

    std::fs::write(root.join(edit.0), edit.1).unwrap();
    ix.sync_rel_paths(&[edit.0.to_string()]).unwrap();
    let after = answer(&mut ix);
    assert!(!has(&strings(&after["dead"]), method), "{after}");
    assert!(has(&strings(&after["external"]), method), "{after}");
    assert_eq!(after, fresh_answer(files, &[edit]), "incremental != fresh");
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect()
}

#[test]
fn go_type_becoming_live_after_incremental_sync_matches_fresh() {
    let main_with_unused = "package main\n\nimport \"example.com/app/internal/app\"\n\n\
         func main() {\n\t_ = app.NewLive()\n\t_ = app.Failure{}\n\t_ = app.Sorted{}\n\t_ = &app.Unused{}\n}\n";
    assert_flip(GO_FILES, ("main.go", main_with_unused), "Unused.String");
}

#[test]
fn ts_type_becoming_live_after_incremental_sync_matches_fresh() {
    let index = "import { Live, Unused } from './live';\nexport const x = new Live();\nexport const y = new Unused();\n";
    assert_flip(TS_FILES, ("src/index.ts", index), "Unused.dispose");
}

#[test]
fn rust_type_becoming_live_after_incremental_sync_matches_fresh() {
    let lib = RUST_FILES[1].1.replace(
        "Used { n: 1 }.to_string() }",
        "Used { n: 1 }.to_string() + &Orphan { n: 2 }.to_string() }",
    );
    assert_flip(RUST_FILES, ("src/lib.rs", &lib), "Orphan::fmt");
}

const GO_GHOST: &[(&str, &str)] = &[
    ("go.mod", "module example.com/app\n\ngo 1.22\n"),
    (
        "main.go",
        "package main\n\nimport \"example.com/app/internal/app\"\n\nfunc main() { _ = app.NewLive() }\n",
    ),
    (
        "internal/app/app.go",
        "package app\n\ntype Live struct{}\n\nfunc NewLive() *Live { return &Live{} }\n\n\
         func (l *Live) String() string { return \"l\" }\n\n\
         type Ghost struct{ n int }\n\n\
         func (g *Ghost) String() string { return \"g\" }\n\
         func (g *Ghost) Read(p []byte) (int, error) { return 0, nil }\n\n\
         func buildGhost() Ghost {\n\tg := Ghost{n: 1}\n\treturn g\n}\n",
    ),
];

#[test]
fn go_type_referenced_only_from_dead_code_is_dead_and_so_are_its_methods() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-ext-ghost-", GO_GHOST);
    let dead = claimed(&call(&mut ix));
    assert!(has(&dead, "app.buildGhost"), "{dead:?}");
    for method in ["Ghost.String", "Ghost.Read"] {
        assert!(has(&dead, method), "{method} not claimed: {dead:?}");
    }
    assert!(!has(&dead, "Live.String"), "{dead:?}");
}

#[test]
fn go_ghost_type_revived_by_a_live_caller_matches_fresh() {
    let main_calls = "package main\n\nimport \"example.com/app/internal/app\"\n\nfunc main() { _ = app.NewLive(); _ = app.Make() }\n";
    let app = GO_GHOST[2]
        .1
        .replace("func buildGhost() Ghost", "func Make() Ghost");
    let (_tmp, root, mut ix) = common::live_indexer("lidx-dead-ext-ghost-inc-", GO_GHOST);
    std::fs::write(root.join("main.go"), main_calls).unwrap();
    std::fs::write(root.join("internal/app/app.go"), &app).unwrap();
    ix.sync_rel_paths(&["main.go".to_string(), "internal/app/app.go".to_string()])
        .unwrap();
    let after = answer(&mut ix);
    assert!(!has(&strings(&after["dead"]), "Ghost.String"), "{after}");
    let merged = [
        GO_GHOST[0],
        ("main.go", main_calls),
        ("internal/app/app.go", app.as_str()),
    ];
    let (_t2, _r2, mut fresh) = common::live_indexer("lidx-dead-ext-ghost-fresh-", &merged);
    assert_eq!(after, answer(&mut fresh), "incremental != fresh");
}

const GO_ASSERTED: &[(&str, &str)] = &[
    ("go.mod", "module example.com/app\n\ngo 1.22\n"),
    (
        "main.go",
        "package main\n\nimport \"example.com/app/internal/app\"\n\n\
         func main() {\n\t_ = app.NewSvc()\n\t_ = app.Wrapper{}\n\t_ = app.Plain{}\n\t_ = app.Local{}\n\t_ = app.NewLater()\n}\n",
    ),
    (
        "internal/app/app.go",
        "package app\n\nimport (\n\t\"github.com/acme/ui\"\n\t\"example.com/app/internal/iface\"\n)\n\n\
         type Svc struct{}\n\n\
         func NewSvc() *Svc { return &Svc{} }\n\n\
         func (s *Svc) Setup(n int) error { return nil }\n\
         func (s *Svc) Teardown() error { return nil }\n\
         func (s *Svc) helper() {}\n\n\
         var _ ui.UI = (*Svc)(nil)\n\n\
         type Wrapper struct {\n\tui.Base\n}\n\n\
         func (Wrapper) Render() string { return \"\" }\n\
         func (Wrapper) tidy() {}\n\n\
         type Plain struct{}\n\n\
         func (Plain) Render() string { return \"\" }\n\n\
         type Local struct{}\n\n\
         func (Local) Run() {}\n\n\
         var _ iface.Runner = Local{}\n\n\
         type Ghost struct{}\n\n\
         func (Ghost) Render() string { return \"\" }\n\n\
         var _ ui.UI = Ghost{}\n",
    ),
    (
        "internal/app/more.go",
        "package app\n\nimport \"github.com/acme/ui\"\n\ntype Later struct{}\n\n\
         func NewLater() *Later { return &Later{} }\n\n\
         func (l *Later) Draw() {}\n\n\
         var _ ui.Drawer = &Later{}\n",
    ),
    (
        "internal/iface/iface.go",
        "package iface\n\ntype Runner interface{ Run() }\n",
    ),
];

#[test]
fn go_assertion_or_embedding_of_an_external_interface_rescues_exported_methods() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-ext-go-assert-", GO_ASSERTED);
    let r = call(&mut ix);
    let dead = claimed(&r);
    // `var _ ui.UI = (*Svc)(nil)`
    for live in ["Svc.Setup", "Svc.Teardown", "Wrapper.Render"] {
        assert!(!has(&dead, live), "{live} claimed: {dead:?}");
    }
    // an assertion in another file of the package
    assert!(!has(&dead, "Later.Draw"), "{dead:?}");
    // only exported methods can satisfy another package's interface
    assert!(has(&dead, "Svc.helper"), "{dead:?}");
    assert!(has(&dead, "Wrapper.tidy"), "{dead:?}");
    // no evidence: not asserted (the repository's own interface is
    // handled by its implements edges, not by this rule)
    assert!(has(&dead, "Plain.Render"), "{dead:?}");
    assert!(!has(&dead, "Local.Run"), "{dead:?}");
    // asserted but never used: the type is dead, so is the method
    assert!(has(&dead, "Ghost.Render"), "{dead:?}");
    assert!(
        listed(&r, "Svc.Setup")["reason"]
            .as_str()
            .unwrap()
            .contains("ui.UI"),
        "{r}"
    );
}

#[test]
fn go_assertion_evidence_follows_incremental_sync() {
    let (_tmp, root, mut ix) = common::live_indexer("lidx-dead-ext-go-assert-inc-", GO_ASSERTED);
    let app = GO_ASSERTED[2].1.replace("var _ ui.UI = (*Svc)(nil)\n", "");
    std::fs::write(root.join("internal/app/app.go"), &app).unwrap();
    ix.sync_rel_paths(&["internal/app/app.go".to_string()])
        .unwrap();
    let after = answer(&mut ix);
    assert!(has(&strings(&after["dead"]), "Svc.Setup"), "{after}");
    let merged = [
        GO_ASSERTED[0],
        GO_ASSERTED[1],
        ("internal/app/app.go", app.as_str()),
        GO_ASSERTED[3],
        GO_ASSERTED[4],
    ];
    let (_t2, _r2, mut fresh) = common::live_indexer("lidx-dead-ext-go-assert-fresh-", &merged);
    assert_eq!(after, answer(&mut fresh), "incremental != fresh");
}
