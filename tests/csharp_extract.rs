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
    assert!(config_reads.iter().any(|e| {
        e.target_qualname.as_deref() == Some("env://DATABASE_URL")
    }), "expected CONFIG_READ for env://DATABASE_URL, found: {:?}",
    config_reads.iter().map(|e| e.target_qualname.as_deref()).collect::<Vec<_>>());
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
    assert_eq!(config_binds.len(), 1, "expected 1 CONFIG_BIND edge, found: {:?}",
        config_binds.iter().map(|e| (&e.source_qualname, &e.target_qualname)).collect::<Vec<_>>());
    assert_eq!(config_binds[0].source_qualname.as_deref(), Some("Acme.Data.MssqlRepositoryBase"));
    assert_eq!(config_binds[0].target_qualname.as_deref(), Some("DatabaseOptions"));
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
    assert_eq!(config_binds.len(), 2, "expected 2 CONFIG_BIND edges, found: {:?}",
        config_binds.iter().map(|e| (&e.source_qualname, &e.target_qualname)).collect::<Vec<_>>());
    assert!(config_binds.iter().any(|e| e.target_qualname.as_deref() == Some("DatabaseOptions")));
    assert!(config_binds.iter().any(|e| e.target_qualname.as_deref() == Some("LoggingOptions")));
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
    assert_eq!(config_binds.len(), 2, "expected 2 CONFIG_BIND edges, found: {:?}",
        config_binds.iter().map(|e| (&e.source_qualname, &e.target_qualname)).collect::<Vec<_>>());
    assert!(config_binds.iter().any(|e| e.target_qualname.as_deref() == Some("DatabaseOptions")));
    assert!(config_binds.iter().any(|e| e.target_qualname.as_deref() == Some("LoggingOptions")));
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
    assert!(config_reads.iter().any(|e| {
        e.target_qualname.as_deref() == Some("env://DATABASE")
    }), "expected CONFIG_READ for env://DATABASE, found: {:?}",
    config_reads.iter().map(|e| e.target_qualname.as_deref()).collect::<Vec<_>>());
}
