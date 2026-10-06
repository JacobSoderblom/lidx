//! `dead_symbols` judges module-level TS/JS constants and type aliases: a
//! value use (argument, operand, member access), a type use (annotation,
//! `typeof X`, `keyof typeof X`, generic argument), an import or a re-export
//! keeps one live; an unreferenced one is reported.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;

const FILES: &[(&str, &str)] = &[
    (
        "src/consts.ts",
        "export const USED_ARG = 1;\n\
export const USED_COMPARE = 2;\n\
export const DEAD_CONST = 3;\n\
export const config = { port: 80 };\n\
export const TABLE = { a: 1, b: 2 };\n\
export const TYPEOF_ONLY = { k: 'v' };\n\
export const VIA_BARREL = 4;\n\
export const NS_CONST = 5;\n\
export const SAME_FILE_USE = 6;\n\
export const sameFileUser = () => SAME_FILE_USE;\n\
export type UsedType = { a: number };\n\
export type DeadType = { b: number };\n\
export type GenericArg = string;\n\
export type NsType = number;\n\
export type Keys = keyof typeof TABLE;\n\
export type Shape = typeof TYPEOF_ONLY;\n\
export type Recursive = { next?: Recursive };\n\
export type ViaAs = { v: string };\n\
export type Param<T> = T;\n\
export function noop() {\n  const LOCAL = 1;\n  return LOCAL;\n}\n\
export type MappedKeys = 'a' | 'b';\n\
export const SHARED = 7;\n",
    ),
    ("src/barrel.ts", "export { VIA_BARREL } from './consts';\n"),
    (
        "src/user.ts",
        "import { SHARED, type MappedKeys, USED_ARG, USED_COMPARE, config, VIA_BARREL as vb, type UsedType, type GenericArg, type Keys, type Shape, type ViaAs, type Param } from './consts';\n\
import * as ns from './consts';\n\
import { VIA_BARREL } from './barrel';\n\
declare function take(...a: unknown[]): void;\n\
export type Mapped = { [M in MappedKeys]: number };\n\
declare function it(name: string, fn: () => void): void;\n\
it('uses the module const', () => {\n  take(SHARED);\n});\n\
it('shadows it', () => {\n  const SHARED = 1;\n  take(SHARED);\n});\n\
export function run(x: UsedType, k: Keys, s: Shape): Array<GenericArg> {\n\
  take(USED_ARG, vb, VIA_BARREL, ns.NS_CONST);\n\
  if (x.a > USED_COMPARE) take(config.port);\n\
  const y = {} as ViaAs;\n\
  const z: ns.NsType = 1;\n\
  const p: Param<number> = 1;\n\
  return [];\n\
}\n",
    ),
    (
        "app/page.tsx",
        "export const metadata = { title: 't' };\nexport const dynamic = 'force-dynamic';\nexport const unusedInApp = 1;\nexport default function Page() { return null; }\n",
    ),
];

fn dead() -> Vec<String> {
    let (_tmp, root, db_path) = common::index_repo("lidx-ts-deadconst-", FILES);
    let mut indexer = Indexer::new(root, db_path).unwrap();
    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"include_unused_imports": false, "include_orphan_tests": false}),
    )
    .unwrap();
    result["dead_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["qualname"].as_str().map(str::to_string))
        .collect()
}

#[test]
fn unreferenced_consts_and_types_are_dead_and_used_ones_are_not() {
    let dead = dead();
    for q in [
        "src/consts.DEAD_CONST",
        "src/consts.DeadType",
        "app/page.unusedInApp",
    ] {
        assert!(dead.iter().any(|d| d == q), "{q} should be dead: {dead:?}");
    }
    for q in [
        "src/consts.USED_ARG",
        "src/consts.USED_COMPARE",
        "src/consts.config",
        "src/consts.TABLE",
        "src/consts.TYPEOF_ONLY",
        "src/consts.VIA_BARREL",
        "src/consts.NS_CONST",
        "src/consts.SAME_FILE_USE",
        "src/consts.UsedType",
        "src/consts.GenericArg",
        "src/consts.NsType",
        "src/consts.Keys",
        "src/consts.Shape",
        "src/consts.ViaAs",
        "src/consts.Param",
        "src/consts.MappedKeys",
        "src/consts.SHARED",
        "app/page.metadata",
        "app/page.dynamic",
    ] {
        assert!(!dead.iter().any(|d| d == q), "{q} is used: {dead:?}");
    }
}

#[test]
fn a_type_referring_only_to_itself_stays_dead() {
    let dead = dead();
    assert!(dead.iter().any(|d| d == "src/consts.Recursive"), "{dead:?}");
}

#[test]
fn nested_consts_are_not_judged() {
    let dead = dead();
    assert!(!dead.iter().any(|d| d.contains("LOCAL")), "{dead:?}");
}
