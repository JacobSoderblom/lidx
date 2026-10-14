//! Issue #503: the four Go uses `dead_symbols` missed, each through the
//! real indexer: a method promoted by embedding into an exported type of an
//! importable package is public API; a call through an interface an
//! embedding type implements reaches the promoted method; a `:=` binding
//! does not shadow the call on its own right-hand side; an untyped member
//! call whose method name is declared only in the calling package is
//! ambiguous among those methods, never a call into a dependency.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};

const FILES: &[(&str, &str)] = &[
    ("go.mod", "module example.com/app\n\ngo 1.22\n"),
    (
        "main.go",
        "package main\n\nimport (\n\t\"example.com/app/internal/enc\"\n\t\"example.com/app/internal/enc/xml\"\n\
         \t\"example.com/app/internal/files\"\n\t\"example.com/app/internal/img\"\n\t\"example.com/app/pkg/public\"\n)\n\n\
         func main() {\n\t_ = public.NewTester()\n\t_ = enc.Use(xml.New())\n\t_ = files.Collect(\".\")\n\t_ = img.Fetch()\n}\n",
    ),
    // promotion into an exported type of an importable package
    (
        "internal/core/tester.go",
        "package core\n\ntype Tester struct{ n int }\n\n\
         func NewTester() *Tester { return &Tester{} }\n\n\
         func (t *Tester) Expect(x any) {}\n\
         func (t *Tester) Ignore(x any) {}\n\
         func (t *Tester) helper() {}\n",
    ),
    (
        "pkg/public/tester.go",
        "package public\n\nimport \"example.com/app/internal/core\"\n\n\
         type Tester struct {\n\t*core.Tester\n}\n\n\
         func NewTester() *Tester { return &Tester{Tester: core.NewTester()} }\n",
    ),
    // interface dispatch through an embedded field
    (
        "internal/enc/iface.go",
        "package enc\n\ntype Encoder interface {\n\tEncode(s string) string\n}\n\n\
         func Use(e Encoder) string { return e.Encode(\"x\") }\n",
    ),
    (
        "internal/enc/util/util.go",
        "package util\n\ntype Base struct{ pretty bool }\n\n\
         func (b Base) Encode(s string) string { return s }\n\
         func (b Base) Other() string { return \"\" }\n",
    ),
    (
        "internal/enc/xml/xml.go",
        "package xml\n\nimport (\n\t\"example.com/app/internal/enc\"\n\t\"example.com/app/internal/enc/util\"\n)\n\n\
         type encoder struct {\n\tutil.Base\n}\n\n\
         func New() enc.Encoder { return encoder{} }\n",
    ),
    // `x, err := x(..)`
    (
        "internal/files/files.go",
        "package files\n\n\
         func mainFiles(dir string) ([]string, error) { return nil, nil }\n\n\
         func Collect(dir string) []string {\n\
         \tif mainFiles, err := mainFiles(dir); err == nil {\n\t\treturn mainFiles\n\t}\n\treturn nil\n}\n",
    ),
    // an untyped receiver whose method name is package-local
    (
        "internal/img/img.go",
        "package img\n\n\
         type image interface {\n\tfetch() []string\n}\n\n\
         type elf struct{}\n\nfunc (elf) fetch() []string { return nil }\n\n\
         type pe struct{}\n\nfunc (pe) fetch() []string { return nil }\n\n\
         func newElf() (image, error) { return elf{}, nil }\n\
         func newPE() (image, error) { return pe{}, nil }\n\n\
         func Fetch() []string {\n\
         \tformats := []func() (image, error){newElf, newPE}\n\
         \tvar out []string\n\
         \tfor _, build := range formats {\n\
         \t\tni, err := build()\n\
         \t\tif err != nil {\n\t\t\tcontinue\n\t\t}\n\
         \t\tout = append(out, ni.fetch()...)\n\
         \t}\n\treturn out\n}\n",
    ),
];

fn call(indexer: &mut Indexer) -> Value {
    rpc::handle_method(
        indexer,
        "dead_symbols",
        json!({"include_unused_imports": false, "include_orphan_tests": false,
               "include_public_api": true, "limit": 1000}),
    )
    .unwrap()
}

fn group(result: &Value, name: &str) -> Vec<Value> {
    result[name].as_array().cloned().unwrap_or_default()
}

fn names(rows: &[Value]) -> Vec<String> {
    rows.iter()
        .map(|s| s["qualname"].as_str().unwrap().to_string())
        .collect()
}

fn has(names: &[String], suffix: &str) -> bool {
    names.iter().any(|q| q.ends_with(suffix))
}

#[test]
fn a_method_promoted_into_an_exported_type_of_an_importable_package_is_public_api() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-go-promo-", FILES);
    let r = call(&mut ix);
    let dead = names(&group(&r, "dead_symbols"));
    let public = group(&r, "public_api");
    for promoted in ["tester.Tester.Expect", "tester.Tester.Ignore"] {
        assert!(!has(&dead, promoted), "{promoted} claimed: {dead:?}");
        let row = public
            .iter()
            .find(|s| s["qualname"].as_str().unwrap().ends_with(promoted))
            .unwrap_or_else(|| panic!("{promoted} not in the public_api group: {r}"));
        assert_eq!(
            row["reason"].as_str().unwrap(),
            "public API: promoted by embedding into the exported type pkg/public/tester.Tester of an importable package",
            "{row}"
        );
    }
    // an unexported method is not promoted into the API
    assert!(has(&dead, "tester.Tester.helper"), "{dead:?}");
}

#[test]
fn a_call_through_an_interface_the_embedding_type_implements_reaches_the_promoted_method() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-go-promo-dispatch-", FILES);
    let r = call(&mut ix);
    let dead = names(&group(&r, "dead_symbols"));
    assert!(!has(&dead, "util.Base.Encode"), "{dead:?}");
    // the interface does not declare it: nothing reaches it
    assert!(has(&dead, "util.Base.Other"), "{dead:?}");
}

#[test]
fn a_short_var_declaration_does_not_shadow_the_call_on_its_own_right_hand_side() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-go-promo-shadow-", FILES);
    let r = call(&mut ix);
    let dead = names(&group(&r, "dead_symbols"));
    assert!(!has(&dead, "files.mainFiles"), "{dead:?}");
    let targets = common::call_targets(FILES, "internal/files/files.Collect");
    assert!(
        targets.iter().any(|t| t.ends_with("files.mainFiles")),
        "Collect must call the package function mainFiles: {targets:?}"
    );
}

#[test]
fn an_untyped_member_call_on_a_package_local_method_name_is_ambiguous_among_those_methods() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-go-promo-untyped-", FILES);
    let r = call(&mut ix);
    let dead = names(&group(&r, "dead_symbols"));
    assert!(!has(&dead, "img.elf.fetch"), "{dead:?}");
    assert!(!has(&dead, "img.pe.fetch"), "{dead:?}");
    let conn = ix.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare("SELECT edge_kind, reason FROM unresolved_references WHERE name_tail = 'fetch' ORDER BY edge_kind")
        .unwrap();
    let rows: Vec<(String, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    // the call is ambiguous among the package's `fetch` methods, and only a
    // call is: a value reference `x.fetch` may read a field of that name
    assert_eq!(rows, [("CALLS".to_string(), "ambiguous".to_string())]);
}
