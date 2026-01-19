use lidx::indexer::python::{PythonExtractor, module_name_from_rel_path};

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("foo.py"), "foo");
    assert_eq!(module_name_from_rel_path("pkg/__init__.py"), "pkg");
    assert_eq!(module_name_from_rel_path("pkg/sub/mod.py"), "pkg.sub.mod");
    assert_eq!(module_name_from_rel_path("__init__.py"), "__init__");
}

#[test]
fn extract_symbols_and_edges() {
    let source = r#"
"""module doc"""
import os, sys as system
from pkg import mod, util as u

class Base:
    pass

class Foo(Base):
    """Foo doc"""
    def method(self, x):
        "method doc"
        return x

def func(a, b):
    return a + b

func(1, 2)
"#;
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, "pkg.mod").unwrap();

    let names: Vec<_> = extracted
        .symbols
        .iter()
        .map(|s| (s.kind.as_str(), s.qualname.as_str()))
        .collect();

    assert!(names.contains(&("module", "pkg.mod")));
    assert!(names.contains(&("class", "pkg.mod.Base")));
    assert!(names.contains(&("class", "pkg.mod.Foo")));
    assert!(names.contains(&("method", "pkg.mod.Foo.method")));
    assert!(names.contains(&("function", "pkg.mod.func")));

    let edge_kinds: Vec<_> = extracted.edges.iter().map(|e| e.kind.as_str()).collect();
    assert!(edge_kinds.contains(&"CONTAINS"));
    assert!(edge_kinds.contains(&"IMPORTS"));
    assert!(edge_kinds.contains(&"EXTENDS"));
    assert!(edge_kinds.contains(&"CALLS"));

    let call_edges: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CALLS")
        .collect();
    assert!(
        call_edges
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("pkg.mod.func"))
    );
}
