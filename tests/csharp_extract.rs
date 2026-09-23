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

// Extension-method CALLS resolution regression tests (issue: `public static
// X Foo(this T t, ...)` extension methods were effectively invisible to
// CALLS resolution -- a call like `row.ToDomain(...)` never produced any
// candidate naming the extension method's real declaring class, because the
// receiver text (`row`) is an instance, never the class name, unlike the
// `UniqueName.Create()` static-call shape `import_qualified_candidates`
// already handled). Each of these fails if `record_extension_method` /
// `extension_method_candidates` in `csharp.rs` is reverted (confirmed by
// temporarily neutering that logic and rerunning).
//
// `CSharpExtractor` accumulates its `extension_registry` across every
// `extract()` call on the same instance (see `Context::extension_registry`'s
// doc) -- these tests call `extract()` twice on one extractor to simulate
// the declaring file being processed before the calling file, exactly as a
// real cold reindex would (in whichever relative order the two files
// happen to be scanned in).

#[test]
fn extension_method_call_resolves_via_cross_file_registry() {
    // Mirrors the real dpb shape: `PipelineMapper.ToDomain` is declared as
    // an extension method in one file and called as `row.ToDomain(...)` in
    // another, reachable only because the calling file's `using` names the
    // declaring namespace.
    let mut extractor = CSharpExtractor::new().unwrap();

    let declaring = r#"
namespace Dpb.DataMgr.DataProduct.Mappers {
  internal static class PipelineMapper {
    public static PipelineRun ToDomain(this PipelineRunRow row, UniqueName dataProduct) {
      return null;
    }
  }
}
"#;
    extractor.extract(declaring, "declaring").unwrap();

    let caller = r#"
using Dpb.DataMgr.DataProduct.Mappers;

namespace Dpb.DataMgr.DataProduct.Persistence {
  internal class MssqlPipelineRepository {
    public PipelineRun Method(PipelineRunRow row, UniqueName dataProduct) {
      return row.ToDomain(dataProduct);
    }
  }
}
"#;
    let extracted = extractor.extract(caller, "caller").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("row.ToDomain"))
        .expect("row.ToDomain(...) call edge");
    assert_eq!(
        call.import_candidates,
        vec!["Dpb.DataMgr.DataProduct.Mappers.PipelineMapper.ToDomain".to_string()],
        "row.ToDomain() must resolve to the extension method declared (in an earlier file \
         this run) under an imported namespace, got {:?}",
        call.import_candidates
    );
}

#[test]
fn extension_method_candidates_filtered_by_known_receiver_type() {
    // Two extension methods named `Convert`, declared for two different
    // receiver types, both imported into scope -- name-and-scope alone
    // would leave this ambiguous (two distinct real symbols), but the
    // call's own known receiver type (`TypeA`, a typed parameter) narrows
    // it to exactly one.
    let mut extractor = CSharpExtractor::new().unwrap();

    let declaring_a = r#"
namespace Dpb.Mappers.A {
  public static class ConverterA {
    public static string Convert(this TypeA value) { return null; }
  }
}
"#;
    extractor.extract(declaring_a, "declaring_a").unwrap();

    let declaring_b = r#"
namespace Dpb.Mappers.B {
  public static class ConverterB {
    public static string Convert(this TypeB value) { return null; }
  }
}
"#;
    extractor.extract(declaring_b, "declaring_b").unwrap();

    let caller = r#"
using Dpb.Mappers.A;
using Dpb.Mappers.B;

namespace Dpb.App {
  public class Caller {
    public string Method(TypeA value) {
      return value.Convert();
    }
  }
}
"#;
    let extracted = extractor.extract(caller, "caller").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("value.Convert"))
        .expect("value.Convert() call edge");
    assert_eq!(
        call.import_candidates,
        vec!["Dpb.Mappers.A.ConverterA.Convert".to_string()],
        "a known receiver type (TypeA) must exclude the TypeB-typed overload from another \
         in-scope extension class, got {:?}",
        call.import_candidates
    );
}

#[test]
fn extension_method_not_imported_produces_no_candidates() {
    // Same extension method as the cross-file test above, but the calling
    // file never imports its namespace and isn't in it either -- it must
    // not be offered as a candidate (an out-of-scope extension method
    // wouldn't even compile as `value.Convert()` in real C#).
    let mut extractor = CSharpExtractor::new().unwrap();

    let declaring = r#"
namespace Dpb.Mappers.A {
  public static class ConverterA {
    public static string Convert(this TypeA value) { return null; }
  }
}
"#;
    extractor.extract(declaring, "declaring").unwrap();

    let caller = r#"
namespace Dpb.App {
  public class Caller {
    public string Method(TypeA value) {
      return value.Convert();
    }
  }
}
"#;
    let extracted = extractor.extract(caller, "caller").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("value.Convert"))
        .expect("value.Convert() call edge");
    assert!(
        call.import_candidates.is_empty(),
        "an extension method whose declaring namespace isn't imported (and isn't the caller's \
         own namespace) must not be offered as a candidate, got {:?}",
        call.import_candidates
    );
}

// Static-field-then-method CALLS resolution regression test (issue: a call
// like `HealthMeters.CheckDuration.Record(...)` -- `HealthMeters` a type,
// `CheckDuration` one of its static fields holding a `Histogram<double>`,
// `Record` called on *that field's value* -- produced no import candidates
// at all, because the pre-fix two-segment-only tier rejected any dotted
// suffix outright. With no candidates, the `1d6a5a7` guard (which only acts
// on a non-empty, all-failing candidate list) never saw evidence to act on,
// so the call fell through unguarded to the bare-name tier and wrongly
// bound to an unrelated same-named `Record` method elsewhere in the repo.
// The real target, `System.Diagnostics.Metrics.Histogram<double>.Record`,
// is external and can never resolve from this repo's own symbol table --
// the correct outcome is that this call stays unresolved, which requires
// only that a candidate exists for the DB-layer guard to fail on, not that
// it succeeds.

#[test]
fn static_field_then_method_chain_produces_guard_evidence_not_empty() {
    let source = r#"
using Dpb.Common.Telemetry;

namespace Dpb.Common.Telemetry {
  public static class HealthMeters {
    public static readonly Histogram<double> CheckDuration = null;
  }
}

namespace Dpb.Common.Health {
  public class HealthPublisher {
    public void Method() {
      HealthMeters.CheckDuration.Record(1.0);
    }
  }
}
"#;
    let module = module_name_from_rel_path("src/HealthPublisher.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| {
            e.kind == "CALLS"
                && e.target_qualname.as_deref() == Some("HealthMeters.CheckDuration.Record")
        })
        .expect("HealthMeters.CheckDuration.Record(...) call edge");
    let mut candidates = call.import_candidates.clone();
    candidates.sort();
    let mut expected = vec![
        "Dpb.Common.Health.HealthMeters.CheckDuration.Record".to_string(),
        "Dpb.Common.Telemetry.HealthMeters.CheckDuration.Record".to_string(),
    ];
    expected.sort();
    assert_eq!(
        candidates, expected,
        "a static-field-then-method chain rooted at a type name must produce import \
         candidates (even though none can ever resolve) so the existing 1d6a5a7 guard has \
         evidence to refuse binding on, got {:?}",
        call.import_candidates
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

// Regression test for the dpb-corpus finding that `_connection.EnsureOpenAsync`
// had zero CALLS edges anywhere in the index: every one of its call sites sat
// inside a lambda passed to a Polly resilience pipeline
// (`_pipeline.ExecuteAsync(async () => { ... })`), and `walk_node` returned
// immediately at the lambda boundary (`is_lambda_node`) without ever
// descending into its body. Fails if that early return is restored (or if
// `collect_statement_bindings`'s matching lambda-parameter fold-in is
// reverted, per this test's sibling below).
#[test]
fn call_inside_lambda_body_attributes_to_enclosing_method() {
    let source = r#"
namespace Acme.App;
public class ConnectionManager {
    private readonly IDbConnection _connection;
    private readonly ResiliencePipeline _pipeline;
    public async System.Threading.Tasks.Task EnsureOpenAsync() {
        await _pipeline.ExecuteAsync(async () => {
            await _connection.EnsureOpenAsync();
        });
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
            e.kind == "CALLS" && e.target_qualname.as_deref() == Some("_connection.EnsureOpenAsync")
        })
        .expect("_connection.EnsureOpenAsync() call edge inside the lambda body");
    assert_eq!(
        call.source_qualname.as_deref(),
        Some("Acme.App.ConnectionManager.EnsureOpenAsync"),
        "a call inside a lambda is a nested scope, not a new symbol — it must \
         attribute to the enclosing named method, not be dropped or attributed \
         to a synthetic lambda symbol"
    );
    assert_eq!(
        call.receiver_type,
        ReceiverType::Known("IDbConnection".to_string()),
        "the field's type must still resolve correctly from inside the lambda"
    );
}

// A lambda's own parameter must be folded into the enclosing method's
// `local_types` (mirrors `python_receiver_type_resolution.rs`'s
// `lambda_parameter_call_does_not_bind`), or a reference to it inside the
// body would be mistaken for an outer name — e.g. here, a lambda parameter
// named `_connection` shadows the class field of the same name, and must
// NOT resolve to the field's `IDbConnection` type. Fails if
// `collect_lambda_parameter_bindings` is not called (the reference would
// then fall through to `class_attr_types` and wrongly resolve `Known`).
#[test]
fn lambda_parameter_shadowing_field_does_not_bind_to_field_type() {
    let source = r#"
namespace Acme.App;
public class ConnectionManager {
    private readonly IDbConnection _connection;
    public void Subscribe(System.Action<IDbConnection> onEach) {
        Register(_connection => { onEach(_connection); _connection.Close(); });
    }
    public void Register(System.Action<IDbConnection> handler) {}
}
"#;
    let module = module_name_from_rel_path("src/app.cs");
    let mut extractor = CSharpExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("_connection.Close"))
        .expect("_connection.Close() call edge inside the lambda body");
    assert_eq!(
        call.receiver_type,
        ReceiverType::Unresolved,
        "the lambda's own parameter shadows the class field of the same name \
         and must not be resolved via the field's type"
    );
}
