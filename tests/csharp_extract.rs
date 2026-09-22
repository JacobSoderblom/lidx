use lidx::indexer::csharp::{CSharpExtractor, module_name_from_rel_path};
use lidx::indexer::extract::{LanguageExtractor, ReceiverType};

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("src/App.cs"), "src/App");
    assert_eq!(module_name_from_rel_path("App.csx"), "App");
}

// Receiver-type-inference regression tests (mirrors the Python mechanism's
// own test shapes; see `python::infer_receiver_type`'s doc comment). Each
// of these fails if the corresponding change in `csharp.rs` is reverted:
// `this_method_resolves_to_enclosing_class` and
// `bare_method_call_still_resolves` only prove `receiver_type` stays
// `NotTracked` in cases already exact/unresolved-receiver-free before this
// change; `typed_parameter_method_resolves_to_declared_type` and
// `unresolvable_var_receiver_does_not_bind` are the discriminating ones —
// both assert a `receiver_type` value (`Known(..)` / `Unresolved`) that the
// pre-change extractor could never produce (every edge defaulted to
// `NotTracked`).

#[test]
fn this_method_resolves_to_enclosing_class() {
    let source = r#"
namespace Acme.App;
public class Foo {
    public void Helper() {}
    public void Method() {
        this.Helper();
    }
}
"#;
    let module = module_name_from_rel_path("src/app.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("Acme.App.Foo.Helper"))
        .expect("this.Helper() call edge");
    assert_eq!(call.receiver_type, ReceiverType::NotTracked);
}

#[test]
fn typed_parameter_method_resolves_to_declared_type() {
    let source = r#"
namespace Acme.App;
public class Foo {
    public void Method(EventStore store) {
        store.Append(1);
    }
}
"#;
    let module = module_name_from_rel_path("src/app.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("store.Append"))
        .expect("store.Append() call edge");
    assert_eq!(
        call.receiver_type,
        ReceiverType::Known("EventStore".to_string())
    );
}

#[test]
fn unresolvable_var_receiver_does_not_bind() {
    let source = r#"
namespace Acme.App;
public class Foo {
    public void Method() {
        var store = GetStore();
        store.Append(1);
    }
}
"#;
    let module = module_name_from_rel_path("src/app.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("store.Append"))
        .expect("store.Append() call edge");
    assert_eq!(call.receiver_type, ReceiverType::Unresolved);
}

#[test]
fn bare_field_reference_without_this_does_not_bind_to_unrelated_type() {
    // C# lets a method reference its own class's field by bare name (no
    // `this.` prefix) — and a `static` field can *only* be reached that
    // way. Found via real-corpus evidence on dpb: `NumberToCode.TryGetValue(...)`
    // (a `Dictionary<int, T>` field referenced bare) was colliding with an
    // unrelated `Result<T>.TryGetValue` domain method until
    // `class_attr_types` was also consulted for bare identifiers.
    let source = r#"
namespace Acme.App;
public class Lookup {
    private static readonly System.Collections.Generic.Dictionary<int, string> NumberToCode = new();
    public void Method() {
        NumberToCode.TryGetValue(1, out var mapped);
    }
}
"#;
    let module = module_name_from_rel_path("src/app.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| {
            e.kind == "CALLS" && e.target_qualname.as_deref() == Some("NumberToCode.TryGetValue")
        })
        .expect("NumberToCode.TryGetValue() call edge");
    assert_eq!(call.receiver_type, ReceiverType::Unresolved);
}

#[test]
fn bare_method_call_still_resolves() {
    let source = r#"
namespace Acme.App;
public class Foo {
    public void Helper() {}
    public void Method() {
        Helper();
    }
}
"#;
    let module = module_name_from_rel_path("src/app.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("Acme.App.Foo.Helper"))
        .expect("Helper() call edge");
    assert_eq!(call.receiver_type, ReceiverType::NotTracked);
}

// Import-aware receiver resolution regression tests (issue: twin
// `Dpb.DataMgr.DataProduct.Domain.UniqueName` / `Dpb.DataMgr.Datasource.Domain.UniqueName`
// classes both defining `Create`, which made every `UniqueName.Create(...)`
// call site ambiguous and unbound). Each of these fails if
// `import_qualified_candidates`/`collect_import_context` in `csharp.rs` is
// reverted back to always producing an empty `import_candidates` list —
// confirmed by temporarily reverting that change and rerunning.

#[test]
fn using_imported_bare_type_resolves_to_its_namespace() {
    let source = r#"
using Dpb.DataMgr.DataProduct.Domain;

public class Caller {
    public void Method() {
        UniqueName.Create();
    }
}
"#;
    let module = module_name_from_rel_path("src/Caller.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("UniqueName.Create"))
        .expect("UniqueName.Create() call edge");
    assert_eq!(
        call.import_candidates,
        vec!["Dpb.DataMgr.DataProduct.Domain.UniqueName.Create".to_string()],
        "a single `using` naming the receiver's namespace must produce exactly \
         that one qualified candidate"
    );
}

#[test]
fn aliased_using_resolves_to_its_target() {
    let source = r#"
using DPUniqueName = Dpb.DataMgr.DataProduct.Domain.UniqueName;

public class Caller {
    public void Method() {
        DPUniqueName.Create();
    }
}
"#;
    let module = module_name_from_rel_path("src/Caller.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("DPUniqueName.Create"))
        .expect("DPUniqueName.Create() call edge");
    assert_eq!(
        call.import_candidates,
        vec!["Dpb.DataMgr.DataProduct.Domain.UniqueName.Create".to_string()],
        "an alias must resolve directly to its aliased target, not a namespace guess"
    );
}

#[test]
fn two_usings_supplying_same_bare_name_both_remain_candidates() {
    // Neither namespace is picked as a winner here -- the extractor cannot
    // know, from this file alone, which one (if either) actually declares
    // `UniqueName`. Both candidates are kept so the DB layer can try them
    // against the real symbol table and refuse to bind if both turn out to
    // name a real symbol (see `db::resolve_import_candidate`).
    let source = r#"
using Dpb.DataMgr.DataProduct.Domain;
using Dpb.DataMgr.Datasource.Domain;

public class Caller {
    public void Method() {
        UniqueName.Create();
    }
}
"#;
    let module = module_name_from_rel_path("src/Caller.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("UniqueName.Create"))
        .expect("UniqueName.Create() call edge");
    let mut candidates = call.import_candidates.clone();
    candidates.sort();
    let mut expected = vec![
        "Dpb.DataMgr.DataProduct.Domain.UniqueName.Create".to_string(),
        "Dpb.DataMgr.Datasource.Domain.UniqueName.Create".to_string(),
    ];
    expected.sort();
    assert_eq!(
        candidates, expected,
        "two usings that could both supply the name must both remain \
         candidates -- the extractor must not pick one"
    );
}

#[test]
fn type_in_own_namespace_resolves_without_using() {
    let source = r#"
namespace Dpb.DataMgr.DataProduct.Domain;

public class Caller {
    public void Method() {
        UniqueName.Create();
    }
}
"#;
    let module = module_name_from_rel_path("src/Caller.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("UniqueName.Create"))
        .expect("UniqueName.Create() call edge");
    assert_eq!(
        call.import_candidates,
        vec!["Dpb.DataMgr.DataProduct.Domain.UniqueName.Create".to_string()],
        "a type in the call site's own enclosing namespace must resolve without any using"
    );
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

#[test]
fn extract_environment_get_variable_config_read() {
    let source = r#"
using System;

namespace Acme.App;

public class Startup {
    public void Configure() {
        var dbUrl = Environment.GetEnvironmentVariable("DATABASE_URL");
    }
}
"#;
    let module = module_name_from_rel_path("src/Startup.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://DATABASE_URL") }),
        "expected CONFIG_READ for env://DATABASE_URL, found: {:?}",
        config_reads
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>()
    );
}

#[test]
fn extract_config_bind_from_constructor_injection() {
    let source = r#"
namespace Acme.Data;

public class MssqlRepositoryBase {
    public MssqlRepositoryBase(IOptions<DatabaseOptions> options) {
        _options = options;
    }
}
"#;
    let module = module_name_from_rel_path("src/MssqlRepositoryBase.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_binds: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_BIND")
        .collect();
    assert_eq!(
        config_binds.len(),
        1,
        "expected 1 CONFIG_BIND edge, found: {:?}",
        config_binds
            .iter()
            .map(|e| (&e.source_qualname, &e.target_qualname))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        config_binds[0].source_qualname.as_deref(),
        Some("Acme.Data.MssqlRepositoryBase")
    );
    assert_eq!(
        config_binds[0].target_qualname.as_deref(),
        Some("DatabaseOptions")
    );
}

#[test]
fn extract_config_bind_multiple_wrappers() {
    let source = r#"
namespace Acme.App;

public class MyService {
    public MyService(IOptions<DatabaseOptions> db, IOptionsMonitor<LoggingOptions> log) {
    }
}
"#;
    let module = module_name_from_rel_path("src/MyService.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_binds: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_BIND")
        .collect();
    assert_eq!(
        config_binds.len(),
        2,
        "expected 2 CONFIG_BIND edges, found: {:?}",
        config_binds
            .iter()
            .map(|e| (&e.source_qualname, &e.target_qualname))
            .collect::<Vec<_>>()
    );
    assert!(
        config_binds
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("DatabaseOptions"))
    );
    assert!(
        config_binds
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("LoggingOptions"))
    );
}

#[test]
fn extract_config_bind_from_configure_call() {
    let source = r#"
namespace Acme.App;

public class Startup {
    public void ConfigureServices(IServiceCollection services) {
        services.Configure<DatabaseOptions>(config.GetSection("Database"));
        services.AddOptions<LoggingOptions>();
    }
}
"#;
    let module = module_name_from_rel_path("src/Startup.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_binds: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_BIND")
        .collect();
    assert_eq!(
        config_binds.len(),
        2,
        "expected 2 CONFIG_BIND edges, found: {:?}",
        config_binds
            .iter()
            .map(|e| (&e.source_qualname, &e.target_qualname))
            .collect::<Vec<_>>()
    );
    assert!(
        config_binds
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("DatabaseOptions"))
    );
    assert!(
        config_binds
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("LoggingOptions"))
    );
}

#[test]
fn extract_bind_configuration_config_read() {
    let source = r#"
namespace Acme.App;

public class Startup {
    public void ConfigureServices(IServiceCollection services) {
        services.AddOptions<DatabaseOptions>().BindConfiguration("Database");
    }
}
"#;
    let module = module_name_from_rel_path("src/Startup.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://DATABASE") }),
        "expected CONFIG_READ for env://DATABASE, found: {:?}",
        config_reads
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>()
    );
}

// Multi-line call-target regression tests. tree-sitter hands `node_text` the
// call target's raw span verbatim, newline and indentation included, when a
// call is written as a chain across lines (dpb's
// `UniqueName\n    .Create(...)` in DataProductMapper.cs is the real
// example). `is_simple_call_target` rejects any embedded whitespace, so
// before the `collapse_call_target_whitespace` fix every one of these
// `target_qualname_is_some` tests failed (NULL instead of the qualname).
// The `target_qualname_is_none` tests guard the other direction: a target
// that is genuinely not a simple dotted path must stay NULL even once
// interior whitespace is stripped, because collapsing whitespace never
// removes the parens/brackets/operators that disqualify it.

#[test]
fn multiline_chained_call_resolves_like_single_line() {
    let source = "
namespace Acme.App;
public class Foo {
    public void Method() {
        UniqueName
            .Create(1);
    }
}
";
    let module = module_name_from_rel_path("src/app.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| {
            e.kind == "CALLS"
                && e.detail.is_none()
                && e.evidence_snippet
                    .as_deref()
                    .is_some_and(|s| s.contains("UniqueName"))
        })
        .expect("UniqueName.Create(...) call edge");
    assert_eq!(
        call.target_qualname.as_deref(),
        Some("UniqueName.Create"),
        "multi-line chain must resolve to the same qualname as the single-line form"
    );
    // Positive control for the reject-shape tests below: the import-aware
    // resolution tier (main's `two_segment_receiver_and_method` /
    // `import_qualified_candidates`) reads the same collapsed call-target
    // text `resolve_call_target` does. A genuine two-segment `Ident.Ident`
    // shape like this one is accepted by that tier too — here it produces
    // a sibling-namespace candidate (`UniqueName` qualified against the
    // enclosing `Acme.App` namespace) purely because the whitespace was
    // collapsed first. This proves the fix reaches that tier; the reject
    // tests below prove it doesn't reach it for shapes that aren't
    // actually simple.
    assert_eq!(
        call.import_candidates,
        vec!["Acme.App.UniqueName.Create".to_string()],
        "multi-line chain must also reach the import-candidate builder, got {:?}",
        call.import_candidates
    );
}

#[test]
fn call_in_receiver_position_stays_null_even_when_split_across_lines() {
    // `foo(bar).Baz` — the receiver is itself a call, not a simple path.
    let source = "
namespace Acme.App;
public class Foo {
    public void Method() {
        foo(bar)
            .Baz();
    }
}
";
    let module = module_name_from_rel_path("src/app.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| {
            e.kind == "CALLS"
                && e.evidence_snippet
                    .as_deref()
                    .is_some_and(|s| s.contains("Baz"))
        })
        .expect("foo(bar).Baz() call edge");
    assert!(
        call.target_qualname.is_none(),
        "call-in-receiver-position must not bind, got {:?}",
        call.target_qualname
    );
    assert!(
        call.import_candidates.is_empty(),
        "call-in-receiver-position must not produce import candidates either, got {:?}",
        call.import_candidates
    );
}

#[test]
fn parenthesized_expression_receiver_stays_null_even_when_split_across_lines() {
    // `(a + b).ToString` — parenthesized arithmetic expression as receiver.
    let source = "
namespace Acme.App;
public class Foo {
    public void Method() {
        (a + b)
            .ToString();
    }
}
";
    let module = module_name_from_rel_path("src/app.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| {
            e.kind == "CALLS"
                && e.evidence_snippet
                    .as_deref()
                    .is_some_and(|s| s.contains("ToString"))
        })
        .expect("(a + b).ToString() call edge");
    assert!(
        call.target_qualname.is_none(),
        "parenthesized-expression receiver must not bind, got {:?}",
        call.target_qualname
    );
    assert!(
        call.import_candidates.is_empty(),
        "parenthesized-expression receiver must not produce import candidates either, got {:?}",
        call.import_candidates
    );
}

#[test]
fn indexer_receiver_stays_null_even_when_split_across_lines() {
    // `arr[0].Method` — indexer access as receiver.
    let source = "
namespace Acme.App;
public class Foo {
    public void Method() {
        arr[0]
            .Method();
    }
}
";
    let module = module_name_from_rel_path("src/app.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| {
            e.kind == "CALLS"
                && e.evidence_snippet
                    .as_deref()
                    .is_some_and(|s| s.contains("Method"))
        })
        .expect("arr[0].Method() call edge");
    assert!(
        call.target_qualname.is_none(),
        "indexer receiver must not bind, got {:?}",
        call.target_qualname
    );
    assert!(
        call.import_candidates.is_empty(),
        "indexer receiver must not produce import candidates either, got {:?}",
        call.import_candidates
    );
}

#[test]
fn null_conditional_receiver_stays_null_even_when_split_across_lines() {
    // `x?.Method` — null-conditional access.
    let source = "
namespace Acme.App;
public class Foo {
    public void Method() {
        x
            ?.Method();
    }
}
";
    let module = module_name_from_rel_path("src/app.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| {
            e.kind == "CALLS"
                && e.evidence_snippet
                    .as_deref()
                    .is_some_and(|s| s.contains("Method"))
        })
        .expect("x?.Method() call edge");
    assert!(
        call.target_qualname.is_none(),
        "null-conditional receiver must not bind, got {:?}",
        call.target_qualname
    );
    assert!(
        call.import_candidates.is_empty(),
        "null-conditional receiver must not produce import candidates either, got {:?}",
        call.import_candidates
    );
}

#[test]
fn await_expression_receiver_stays_null_even_when_split_across_lines() {
    // `await foo().Bar` — the receiver is an awaited call, not a simple path.
    let source = "
namespace Acme.App;
public class Foo {
    public async System.Threading.Tasks.Task Method() {
        await foo()
            .Bar();
    }
}
";
    let module = module_name_from_rel_path("src/app.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| {
            e.kind == "CALLS"
                && e.evidence_snippet
                    .as_deref()
                    .is_some_and(|s| s.contains("Bar"))
        })
        .expect("await foo().Bar() call edge");
    assert!(
        call.target_qualname.is_none(),
        "awaited-call receiver must not bind, got {:?}",
        call.target_qualname
    );
    assert!(
        call.import_candidates.is_empty(),
        "awaited-call receiver must not produce import candidates either, got {:?}",
        call.import_candidates
    );
}
