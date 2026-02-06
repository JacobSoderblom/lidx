use lidx::indexer::csharp::CSharpExtractor;
use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::javascript::JavascriptExtractor;
use lidx::indexer::python::PythonExtractor;
use lidx::indexer::rust::RustExtractor;
use lidx::metrics;

#[test]
fn python_complexity_counts() {
    let source = r#"
def func(x, y):
    if x and y:
        return 1
    elif x:
        return 2
    return 3
"#;
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, "mod").unwrap();
    let metrics = metrics::compute_symbol_metrics(source, "python", &extracted.symbols);
    let func = metrics.iter().find(|m| m.qualname == "mod.func").unwrap();
    assert_eq!(func.complexity, 4);
}

#[test]
fn javascript_complexity_counts() {
    let source = r#"
function f(x, y) {
  if (x && y) { return 1; }
  if (x || y) { return 2; }
}
"#;
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();
    let metrics = metrics::compute_symbol_metrics(source, "javascript", &extracted.symbols);
    let func = metrics.iter().find(|m| m.qualname == "src/app.f").unwrap();
    assert_eq!(func.complexity, 5);
}

#[test]
fn rust_complexity_counts() {
    let source = r#"
fn f(x: i32) -> i32 {
    if x > 0 { 1 } else { 0 }
}
"#;
    let mut extractor = RustExtractor::new().unwrap();
    let extracted = extractor.extract(source, "crate::mod").unwrap();
    let metrics = metrics::compute_symbol_metrics(source, "rust", &extracted.symbols);
    let func = metrics
        .iter()
        .find(|m| m.qualname == "crate::mod::f")
        .unwrap();
    assert_eq!(func.complexity, 2);
}

#[test]
fn csharp_complexity_counts() {
    let source = r#"
namespace Acme;

public class Foo {
    public void Bar(bool x, bool y) {
        if (x && y) {}
        if (x || y) {}
    }
}
"#;
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();
    let metrics = metrics::compute_symbol_metrics(source, "csharp", &extracted.symbols);
    let method = metrics
        .iter()
        .find(|m| m.qualname == "Acme.Foo.Bar")
        .unwrap();
    assert_eq!(method.complexity, 5);
}
