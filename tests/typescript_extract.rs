use lidx::indexer::javascript::{TypescriptExtractor, module_name_from_rel_path};

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("types/foo.d.ts"), "types/foo");
    assert_eq!(module_name_from_rel_path("pkg/index.ts"), "pkg");
}

#[test]
fn extract_symbols_and_edges() {
    let source = r#"
import type { Foo } from "./foo";

export interface Greeter {
    greet(name: string): void;
}

export type Id = string | number;

export enum Kind { A, B }

export class Impl implements Greeter {
    helper() {}
    greet(name: string) { this.helper(); }
}
"#;
    let mut extractor = TypescriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/types").unwrap();

    let names: Vec<_> = extracted
        .symbols
        .iter()
        .map(|s| (s.kind.as_str(), s.qualname.as_str()))
        .collect();

    assert!(names.contains(&("interface", "src/types.Greeter")));
    assert!(names.contains(&("type", "src/types.Id")));
    assert!(names.contains(&("enum", "src/types.Kind")));
    assert!(names.contains(&("class", "src/types.Impl")));
    assert!(names.contains(&("method", "src/types.Impl.helper")));
    assert!(names.contains(&("method", "src/types.Impl.greet")));

    let edge_kinds: Vec<_> = extracted.edges.iter().map(|e| e.kind.as_str()).collect();
    assert!(edge_kinds.contains(&"IMPORTS"));
    assert!(edge_kinds.contains(&"IMPLEMENTS"));
    assert!(edge_kinds.contains(&"CALLS"));

    let call_edges: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CALLS")
        .collect();
    assert!(
        call_edges
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("src/types.Impl.helper"))
    );
}
