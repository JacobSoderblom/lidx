//! (Also `T | undefined` variables and `(x as T).m()`.)
//!
//! A parameter declared `Partial<T>`, `Required<T>`, `Readonly<T>` or
//! `NonNullable<T>` has the members of `T`: a method call on it binds `T`'s
//! method, not nothing (the callbacks of an observable are typed
//! `Partial<Observer<..>>`).

mod common;

use lidx::indexer::Indexer;
use rusqlite::params;

fn calls_from(indexer: &Indexer, source: &str) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND s.name = ? AND e.graph_version = ?
               AND t.kind != 'external'
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map(params![source, gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn utility_wrapped_parameter_types_bind_the_wrapped_types_methods() {
    let files = [
        (
            "types.ts",
            "export class Observer {\n  next(v: number): void {}\n  done(): void {}\n}\nexport class Decoy {\n  next(v: number): void {}\n  done(): void {}\n}\n",
        ),
        (
            "use.ts",
            "import { Observer } from './types';\nexport function a(o: Partial<Observer>) { o.next(1); }\nexport function b(o: Readonly<Observer>) { o.done(); }\nexport function c(o: NonNullable<Observer>) { o.done(); }\nexport function d(o: Partial<Record<string, Observer>>) { o.next(1); }\nlet lazy: Observer | undefined;\nexport function e() { lazy!.next(1); }\nexport function f(o: unknown) { (o as Observer).done(); }\nexport function g(o: Observer | null) { o?.done(); }\n",
        ),
    ];
    let (_tmp, root, db_path) = common::index_repo("lidx-ts-utility-", &files);
    let indexer = Indexer::new(root, db_path).unwrap();
    assert_eq!(calls_from(&indexer, "a"), ["types.Observer.next"]);
    assert_eq!(calls_from(&indexer, "b"), ["types.Observer.done"]);
    assert_eq!(calls_from(&indexer, "c"), ["types.Observer.done"]);
    assert!(calls_from(&indexer, "d").is_empty());
    assert_eq!(calls_from(&indexer, "e"), ["types.Observer.next"]);
    assert_eq!(calls_from(&indexer, "f"), ["types.Observer.done"]);
    assert_eq!(calls_from(&indexer, "g"), ["types.Observer.done"]);
}
