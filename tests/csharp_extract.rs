use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::csharp::{CSharpExtractor, module_name_from_rel_path};

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("src/App.cs"), "src/App");
    assert_eq!(module_name_from_rel_path("App.csx"), "App");
}

#[test]
fn extract_symbols_and_edges() {
    let source = r#"
using System;
using Foo.Bar;

namespace Acme.App;

public interface Greeter {
    void Greet(string name);
}

public class Base {}

public class Impl : Base, Greeter {
    public void Helper() {}
    public void Greet(string name) { Helper(); }
    public int Count { get; }
    private int field;
}
"#;
    let module = module_name_from_rel_path("src/app.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let names: Vec<_> = extracted
        .symbols
        .iter()
        .map(|s| (s.kind.as_str(), s.qualname.as_str()))
        .collect();

    assert!(names.contains(&("module", "src/app")));
    assert!(names.contains(&("namespace", "Acme.App")));
    assert!(names.contains(&("interface", "Acme.App.Greeter")));
    assert!(names.contains(&("class", "Acme.App.Base")));
    assert!(names.contains(&("class", "Acme.App.Impl")));
    assert!(names.contains(&("method", "Acme.App.Impl.Helper")));
    assert!(names.contains(&("method", "Acme.App.Impl.Greet")));
    assert!(names.contains(&("property", "Acme.App.Impl.Count")));
    assert!(names.contains(&("field", "Acme.App.Impl.field")));

    let edge_kinds: Vec<_> = extracted.edges.iter().map(|e| e.kind.as_str()).collect();
    assert!(edge_kinds.contains(&"CONTAINS"));
    assert!(edge_kinds.contains(&"IMPORTS"));
    assert!(edge_kinds.contains(&"EXTENDS"));
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
            .any(|edge| edge.target_qualname.as_deref() == Some("Acme.App.Impl.Helper"))
    );
}
