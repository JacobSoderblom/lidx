//! Issue #255: the resolver's name-fallback SQL prefilters on `symbols.name`
//! (index seek) instead of scanning on a leading-wildcard `LIKE`. That is
//! only result-preserving while every extractor records a symbol's `name`
//! as the final segment of its `qualname`, except C# constructors
//! (`.ctor`) and TS/JS computed or quoted keys (tail ends in `]` or a
//! quote), which the resolver sends to the unfiltered statement. This pins
//! that rule per extractor.

mod common;

use rusqlite::Connection;

#[test]
fn symbol_name_is_the_trailing_qualname_segment() {
    let (_tmp, _root, db_path) = common::index_repo(
        "lidx-name-invariant-",
        &[
            (
                "a.py",
                "class Svc:\n    def run(self):\n        pass\n\ndef util():\n    pass\n",
            ),
            (
                "b.rs",
                "pub struct S;\nimpl S {\n    pub fn run(&self) {}\n}\npub mod m {\n    pub fn helper() {}\n}\n",
            ),
            (
                "c.cs",
                "namespace N\n{\n    public class C\n    {\n        public C(int x) { }\n        static C() { }\n        public void Run() { }\n        public int P { get; set; }\n    }\n}\n",
            ),
            (
                "d.go",
                "package p\ntype T struct{}\nfunc (t T) Run() {}\nfunc Helper() {}\n",
            ),
            (
                "e.ts",
                "export class K {\n  run(): void {}\n  [Symbol.iterator]() {}\n  \"a.b\"() {}\n}\nexport function f() {}\n",
            ),
            ("f.lua", "local M = {}\nfunction M.run() end\nreturn M\n"),
        ],
    );
    let conn = Connection::open(db_path).unwrap();
    let mut stmt = conn
        .prepare("SELECT kind, name, qualname FROM symbols WHERE kind != 'module'")
        .unwrap();
    let rows: Vec<(String, String, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(rows.len() > 10, "fixture should index symbols: {rows:?}");
    for (kind, name, qualname) in &rows {
        let tail = qualname.rsplit(['.']).next().unwrap();
        let tail = tail.rsplit("::").next().unwrap();
        let non_plain = tail.ends_with([']', '\'', '"', '`']);
        assert!(
            *name == tail || *name == format!(".{tail}") || non_plain,
            "{kind} `{name}` breaks the name-prefilter rule for `{qualname}`"
        );
    }
    // The odd shapes the rule exists for must actually be indexed.
    for odd in [".ctor", ".cctor", "[Symbol.iterator]"] {
        assert!(rows.iter().any(|r| r.1 == odd), "missing {odd}: {rows:?}");
    }
}
