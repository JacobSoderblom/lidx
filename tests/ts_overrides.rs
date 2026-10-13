//! Issue #458: TypeScript override pairs. Interface and type-literal members
//! are `decl` symbols, `override` is optional on a class member, a
//! `declare class` method signature is a member, and a heritage name the
//! module never binds (a `lib` global) resolves to nothing.

mod common;

use common::golden::{self, EdgeKey};
use lidx::db::Db;
use lidx::indexer::Indexer;
use lidx::rpc;
use std::collections::BTreeSet;

type Files<'a> = &'a [(&'a str, &'a str)];

/// `base.qualname -> impl.qualname` for every dispatch pair.
fn pairs(db: &Db) -> BTreeSet<String> {
    let gv = db.current_graph_version().unwrap();
    let conn = db.read_conn().unwrap();
    let name = |id: i64| -> String {
        conn.query_row("SELECT qualname FROM symbols WHERE id = ?", [id], |r| {
            r.get(0)
        })
        .unwrap()
    };
    db.all_dispatch_pairs(gv)
        .unwrap()
        .into_iter()
        .map(|(base, imp)| format!("{} -> {}", name(base), name(imp)))
        .collect()
}

fn pairs_of(files: Files) -> BTreeSet<String> {
    let (_tmp, root, db_path) = common::index_repo("lidx-ts-over-", files);
    let indexer = Indexer::new(root, db_path).unwrap();
    pairs(indexer.db())
}

/// Symbols, edges and pairs of an index, comparable between an incremental
/// sync and a fresh reindex.
fn state(db: &Db) -> (BTreeSet<String>, BTreeSet<EdgeKey>, BTreeSet<String>) {
    let gv = db.current_graph_version().unwrap();
    let conn = db.read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, s.kind, COALESCE(s.visibility, ''), s.start_line
             FROM symbols s JOIN files f ON f.id = s.file_id
             WHERE s.graph_version = ? AND f.language != 'external'",
        )
        .unwrap();
    let symbols = stmt
        .query_map([gv], |r| {
            Ok(format!(
                "{} {} [{}] @{}",
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?
            ))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    (symbols, golden::snapshot_edges(db, gv).unwrap(), pairs(db))
}

fn fresh_state(files: Files) -> (BTreeSet<String>, BTreeSet<EdgeKey>, BTreeSet<String>) {
    let (_tmp, root, db_path) = common::index_repo("lidx-ts-over-fresh-", files);
    let indexer = Indexer::new(root, db_path).unwrap();
    state(indexer.db())
}

fn has(set: &BTreeSet<String>, wanted: &str) -> bool {
    set.contains(wanted)
}

const ROUTER: &str = "export interface Router<T> {\n  name: string\n  add(method: string, path: string): void\n  match: (method: string) => T | null\n  readonly parent?: Router<T>\n  \"~find\"(): void\n}\n\
export type Opts = { retries: number; run(): void }\n";

const IMPL: &str = "import type { Router, Opts } from './router'\n\
export class Trie<T> implements Router<T> {\n  name = 'trie'\n  add(method: string, path: string) {}\n  match = (method: string) => null\n  parent?: Router<T>\n  \"~find\"() {}\n  static name2 = 1\n}\n\
export class Runner implements Opts {\n  retries = 1\n  run() {}\n}\n";

#[test]
fn interface_and_type_literal_members_pair_with_implementations() {
    let got = pairs_of(&[("src/router.ts", ROUTER), ("src/trie.ts", IMPL)]);
    for want in [
        "src/router.Router.name -> src/trie.Trie.name",
        "src/router.Router.add -> src/trie.Trie.add",
        "src/router.Router.match -> src/trie.Trie.match",
        "src/router.Router.parent -> src/trie.Trie.parent",
        "src/router.Router.\"~find\" -> src/trie.Trie.\"~find\"",
        "src/router.Opts.retries -> src/trie.Runner.retries",
        "src/router.Opts.run -> src/trie.Runner.run",
    ] {
        assert!(has(&got, want), "missing {want}: {got:#?}");
    }
}

#[test]
fn base_class_member_pairs_without_the_override_keyword() {
    let got = pairs_of(&[(
        "src/a.ts",
        "export abstract class Base {\n  abstract parse(): void\n  hook() {}\n  constructor() {}\n  static make() {}\n  static shared = 1\n  kept() {}\n}\n\
export class Mid extends Base {\n  constructor() { super() }\n  parse() {}\n  static make() {}\n  static shared = 2\n  override hook() {}\n}\n\
export class Leaf extends Mid {\n  parse() {}\n}\n",
    )]);
    for want in [
        "src/a.Base.parse -> src/a.Mid.parse",
        "src/a.Base.hook -> src/a.Mid.hook",
        "src/a.Base.parse -> src/a.Leaf.parse",
    ] {
        assert!(has(&got, want), "missing {want}: {got:#?}");
    }
    for unwanted in ["constructor", "make", "shared", "kept"] {
        assert!(
            !got.iter().any(|p| p.contains(&format!(".{unwanted}"))),
            "{unwanted} is no override: {got:#?}"
        );
    }
}

#[test]
fn interface_redeclaring_a_member_is_no_override() {
    let got = pairs_of(&[(
        "src/a.ts",
        "export interface Base { serialize(o: any): any; id: number }\n\
export interface Sub extends Base { serialize(o: any): any; id: number }\n\
export class Impl implements Sub {\n  id = 1\n  serialize(o: any) { return o }\n}\n",
    )]);
    assert!(
        !got.iter()
            .any(|p| p.ends_with("-> src/a.Sub.serialize") || p.ends_with("-> src/a.Sub.id")),
        "{got:#?}"
    );
    // The class reaches both interfaces' members.
    assert!(has(&got, "src/a.Base.serialize -> src/a.Impl.serialize"));
    assert!(has(&got, "src/a.Sub.serialize -> src/a.Impl.serialize"));
}

#[test]
fn declare_class_method_signatures_are_members_and_overloads_are_not() {
    let (_tmp, root, db_path) = common::index_repo(
        "lidx-ts-declare-",
        &[(
            "src/h3.ts",
            "export declare class H3 {\n  fetch(req: Request): Response\n  static create(): H3\n  \"~addRoute\"(r: string): void\n}\n\
export class Impl implements H3 {\n  fetch(req: Request) { return new Response() }\n  \"~addRoute\"(r: string) {}\n}\n\
export class Over {\n  get(a: string): string\n  get(a: number): number\n  get(a: any): any { return a }\n}\n",
        )],
    );
    let indexer = Indexer::new(root, db_path).unwrap();
    let got = pairs(indexer.db());
    assert!(
        has(&got, "src/h3.H3.fetch -> src/h3.Impl.fetch"),
        "{got:#?}"
    );
    assert!(has(
        &got,
        "src/h3.H3.\"~addRoute\" -> src/h3.Impl.\"~addRoute\""
    ));
    let conn = indexer.db().read_conn().unwrap();
    let n = |q: &str| -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM symbols WHERE qualname = ?",
            [q],
            |r| r.get(0),
        )
        .unwrap()
    };
    assert_eq!(n("src/h3.H3.fetch"), 1);
    assert_eq!(
        n("src/h3.Over.get"),
        1,
        "overload signatures are not symbols"
    );
    let vis: String = conn
        .query_row(
            "SELECT COALESCE(visibility, '') FROM symbols WHERE qualname = 'src/h3.H3.fetch'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(vis.split_whitespace().any(|m| m == "decl"), "{vis}");
}

#[test]
fn unbound_heritage_names_resolve_to_nothing_and_imported_ones_resolve() {
    let (_tmp, root, db_path) = common::index_repo(
        "lidx-ts-global-",
        &[
            (
                "src/jsx.ts",
                "export type Event = { kind: string }\nexport interface Real { x: number }\n",
            ),
            (
                "src/sw.ts",
                "import type { Real } from './jsx'\ninterface ExtendableEvent extends Event {\n  waitUntil(): void\n}\nexport interface Fetch extends ExtendableEvent, Real {}\nexport class Bad extends Error {}\n",
            ),
        ],
    );
    let indexer = Indexer::new(root, db_path).unwrap();
    let snapshot =
        golden::snapshot_edges(indexer.db(), indexer.db().current_graph_version().unwrap())
            .unwrap();
    let extends = |source: &str| -> Vec<Option<String>> {
        snapshot
            .iter()
            .filter(|e| e.kind == "EXTENDS" && e.source_qualname == source)
            .map(|e| e.target_qualname.clone())
            .collect()
    };
    assert!(
        !extends("src/sw.ExtendableEvent")
            .iter()
            .flatten()
            .any(|t| t == "src/jsx.Event"),
        "global Event must not bind to the repo alias: {snapshot:#?}"
    );
    let fetch = extends("src/sw.Fetch");
    assert!(
        fetch.iter().flatten().any(|t| t == "src/jsx.Real"),
        "{fetch:?}"
    );
    assert!(
        fetch
            .iter()
            .flatten()
            .any(|t| t == "src/sw.ExtendableEvent"),
        "{fetch:?}"
    );
}

#[test]
fn calls_bind_to_interface_members_only_through_an_exact_typed_receiver() {
    let files: Files = &[
        (
            "src/store.ts",
            "export interface Store {\n  get(k: string): number\n  fetch?: typeof fetch\n}\n",
        ),
        (
            "src/use.ts",
            "import type { Store } from './store'\n\
export function typed(s: Store) { return s.get('a') }\n\
export function untyped(x: any) { return x.get('a') }\n\
export function viaField(s: Store) { return s.fetch!('u') }\n",
        ),
    ];
    assert_eq!(
        common::call_targets(files, "src/use.typed"),
        vec!["src/store.Store.get".to_string()]
    );
    for caller in ["src/use.untyped", "src/use.viaField"] {
        let got = common::call_targets(files, caller);
        assert!(
            !got.iter().any(|t| t.starts_with("src/store.Store")),
            "{caller}: {got:?}"
        );
    }
}

#[test]
fn interface_members_are_not_dead_code_and_show_in_the_outline() {
    let (_tmp, root, db_path) = common::index_repo(
        "lidx-ts-dead-",
        &[(
            "src/router.ts",
            "export interface Router {\n  add(path: string): void\n  name: string\n}\n\
export type Hooks = { onStart(): void; label: string }\n\
export declare class Core {\n  start(): void\n}\n",
        )],
    );
    let mut indexer = Indexer::new(root, db_path).unwrap();
    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"include_unused_imports": false, "include_orphan_tests": false}),
    )
    .unwrap();
    let dead: Vec<&str> = result["dead_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["qualname"].as_str())
        .collect();
    for member in [
        "src/router.Router.add",
        "src/router.Hooks.onStart",
        "src/router.Core.start",
    ] {
        assert!(
            !dead.contains(&member),
            "{member} is a declaration: {dead:?}"
        );
    }
    let outline = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "src/router.ts"}),
    )
    .unwrap()
    .to_string();
    for member in ["Router.add", "Router.name", "Hooks.onStart", "Core.start"] {
        assert!(
            outline.contains(member),
            "outline lacks {member}: {outline}"
        );
    }
    let repo_map = rpc::handle_method(&mut indexer, "repo_map", serde_json::json!({})).unwrap();
    // The members count as symbols of their file, by kind.
    let text = repo_map["text"].as_str().unwrap();
    assert!(
        text.contains("9 symbols") && text.contains("method: 3"),
        "{text}"
    );
}

fn assert_incremental_matches_fresh(
    label: &str,
    before: Files,
    changes: &[(&str, &str)],
    after: Files,
) {
    let (_tmp, root, mut indexer) = common::live_indexer(label, before);
    common::write_files(&root, changes);
    let paths: Vec<String> = changes.iter().map(|(p, _)| p.to_string()).collect();
    indexer.sync_rel_paths(&paths).unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let incremental = state(indexer.db());
    let fresh = fresh_state(after);
    assert_eq!(incremental.0, fresh.0, "symbols");
    common::assert_matches_fresh(&incremental.1, &fresh.1);
    assert_eq!(incremental.2, fresh.2, "dispatch pairs");
}

#[test]
fn incremental_sync_matches_fresh_when_an_override_is_added() {
    let base = ("src/base.ts", "export class Base {\n  run() {}\n}\n");
    let before = (
        "src/sub.ts",
        "import { Base } from './base'\nexport class Sub extends Base {\n}\n",
    );
    let after = (
        "src/sub.ts",
        "import { Base } from './base'\nexport class Sub extends Base {\n  run() {}\n}\n",
    );
    assert_incremental_matches_fresh("ts-add-override", &[base, before], &[after], &[base, after]);
    assert!(pairs_of(&[base, after]).contains("src/base.Base.run -> src/sub.Sub.run"));
}

#[test]
fn incremental_sync_matches_fresh_when_an_interface_member_is_added() {
    let iface0 = (
        "src/i.ts",
        "export interface Router {\n  add(p: string): void\n}\n",
    );
    let iface1 = (
        "src/i.ts",
        "export interface Router {\n  add(p: string): void\n  match(p: string): void\n}\n",
    );
    let imp = (
        "src/c.ts",
        "import type { Router } from './i'\nexport class Trie implements Router {\n  add(p: string) {}\n  match(p: string) {}\n}\n",
    );
    assert_incremental_matches_fresh("ts-add-member", &[iface0, imp], &[iface1], &[iface1, imp]);
    assert!(pairs_of(&[iface1, imp]).contains("src/i.Router.match -> src/c.Trie.match"));
}

#[test]
fn incremental_sync_matches_fresh_when_an_interface_member_is_renamed() {
    let iface0 = (
        "src/i.ts",
        "export interface Router {\n  add(p: string): void\n  name: string\n}\n",
    );
    let iface1 = (
        "src/i.ts",
        "export interface Router {\n  insert(p: string): void\n  name: string\n}\n",
    );
    let imp = (
        "src/c.ts",
        "import type { Router } from './i'\nexport class Trie implements Router {\n  add(p: string) {}\n  insert(p: string) {}\n  name = 'x'\n}\n",
    );
    assert_incremental_matches_fresh(
        "ts-rename-member",
        &[iface0, imp],
        &[iface1],
        &[iface1, imp],
    );
    let got = pairs_of(&[iface1, imp]);
    assert!(got.contains("src/i.Router.insert -> src/c.Trie.insert"));
    assert!(!got.contains("src/i.Router.add -> src/c.Trie.add"));
}

#[test]
fn incremental_sync_matches_fresh_when_an_overriding_member_is_renamed() {
    let base = ("src/base.ts", "export class Base {\n  run() {}\n}\n");
    let sub0 = (
        "src/sub.ts",
        "import { Base } from './base'\nexport class Sub extends Base {\n  run() {}\n}\n",
    );
    let sub1 = (
        "src/sub.ts",
        "import { Base } from './base'\nexport class Sub extends Base {\n  go() {}\n}\n",
    );
    assert_incremental_matches_fresh("ts-rename-override", &[base, sub0], &[sub1], &[base, sub1]);
    assert!(pairs_of(&[base, sub1]).is_empty());
}
