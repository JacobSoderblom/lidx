use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::python::{PythonExtractor, module_name_from_rel_path};
use lidx::indexer::python_eval::{Outcome, PyEval, PyTypeTable};
use lidx::indexer::python_expr::PyExpr;

/// Bind every call of `caller_src` (at `caller_path`) against a table of
/// `files` plus the caller, as `(callee text, bound qualname)` pairs.
fn bound_in(files: &[(&str, &str)], caller_path: &str, caller_src: &str) -> Vec<(String, String)> {
    let mut decls = Vec::new();
    let mut sites = Vec::new();
    let all: Vec<(&str, &str)> = files
        .iter()
        .copied()
        .chain(std::iter::once((caller_path, caller_src)))
        .collect();
    for (path, src) in all {
        let module = module_name_from_rel_path(path);
        let mut x = PythonExtractor::new().unwrap();
        x.set_current_path(path);
        let out = x.extract(src, &module).unwrap();
        decls.push(out.py_decls.unwrap());
        if path == caller_path {
            for e in out.edges.into_iter().filter(|e| e.kind == "CALLS") {
                sites.push((module.clone(), e));
            }
        }
    }
    let table = PyTypeTable::build(decls);
    sites
        .into_iter()
        .filter_map(|(module, e)| {
            let site = e.py_site?;
            match PyEval::new(&table).resolve_site(&site, &module) {
                Outcome::Bound { target, .. } => {
                    Some((e.target_qualname.or(e.detail).unwrap_or_default(), target))
                }
                _ => None,
            }
        })
        .collect()
}

/// The lowered callee of the call whose target text is `text`.
fn callee_of(source: &str, path: &str, text: &str) -> PyExpr {
    let mut x = PythonExtractor::new().unwrap();
    x.set_current_path(path);
    let out = x.extract(source, &module_name_from_rel_path(path)).unwrap();
    out.edges
        .into_iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some(text))
        .unwrap_or_else(|| panic!("{text}(...) call must be extracted"))
        .py_site
        .expect("every CALLS edge carries its site")
        .callee
}

fn is_attr_of_unknown(e: &PyExpr, name: &str) -> bool {
    matches!(e, PyExpr::Attr(inner, n) if n == name && matches!(**inner, PyExpr::Unknown(_)))
}

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

#[test]
fn extract_os_getenv_config_read() {
    let source = r#"
import os

def main():
    db_url = os.getenv("DATABASE_URL")
    api_key = os.environ.get("API_KEY")
"#;
    let module = module_name_from_rel_path("app/config.py");
    let mut extractor = PythonExtractor::new().unwrap();
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
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://API_KEY") }),
        "expected CONFIG_READ for env://API_KEY"
    );
}

#[test]
fn extract_os_environ_subscript_config_read() {
    let source = r#"
import os

secret = os.environ["SECRET_KEY"]
"#;
    let module = module_name_from_rel_path("app/config.py");
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://SECRET_KEY") }),
        "expected CONFIG_READ for env://SECRET_KEY, found: {:?}",
        config_reads
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>()
    );
}

#[test]
fn pydantic_base_settings_subclass_emits_config_read_per_field() {
    let source = r#"
from pydantic_settings import BaseSettings

class DatabaseSettings(BaseSettings):
    database_url: str
    secret_key: str
"#;
    let module = module_name_from_rel_path("app/settings.py");
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    // CONFIG_READ edges per field: class → env://FIELD_UPPER
    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert!(
        config_reads.iter().any(|e| e.source_qualname.as_deref()
            == Some("app.settings.DatabaseSettings")
            && e.target_qualname.as_deref() == Some("env://DATABASE_URL")),
        "expected CONFIG_READ from DatabaseSettings to env://DATABASE_URL, found: {:?}",
        config_reads
            .iter()
            .map(|e| (&e.source_qualname, &e.target_qualname))
            .collect::<Vec<_>>()
    );
    assert!(
        config_reads.iter().any(|e| e.source_qualname.as_deref()
            == Some("app.settings.DatabaseSettings")
            && e.target_qualname.as_deref() == Some("env://SECRET_KEY")),
        "expected CONFIG_READ from DatabaseSettings to env://SECRET_KEY, found: {:?}",
        config_reads
            .iter()
            .map(|e| (&e.source_qualname, &e.target_qualname))
            .collect::<Vec<_>>()
    );

    // CONFIG_BIND edges per field: class → env://FIELD_UPPER (non-self-loop, marks class as config consumer)
    let config_binds: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_BIND")
        .collect();
    assert!(
        config_binds.iter().any(|e| e.source_qualname.as_deref()
            == Some("app.settings.DatabaseSettings")
            && e.target_qualname.as_deref() == Some("env://DATABASE_URL")),
        "expected CONFIG_BIND from DatabaseSettings to env://DATABASE_URL, found: {:?}",
        config_binds
            .iter()
            .map(|e| (&e.source_qualname, &e.target_qualname))
            .collect::<Vec<_>>()
    );
    assert!(
        config_binds.iter().any(|e| e.source_qualname.as_deref()
            == Some("app.settings.DatabaseSettings")
            && e.target_qualname.as_deref() == Some("env://SECRET_KEY")),
        "expected CONFIG_BIND from DatabaseSettings to env://SECRET_KEY, found: {:?}",
        config_binds
            .iter()
            .map(|e| (&e.source_qualname, &e.target_qualname))
            .collect::<Vec<_>>()
    );
}

#[test]
fn plain_class_does_not_emit_config_bind() {
    let source = r#"
class PlainModel:
    name: str
    value: int

class AnotherClass(SomeOtherBase):
    foo: str
"#;
    let module = module_name_from_rel_path("app/models.py");
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_binds: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_BIND")
        .collect();
    assert!(
        config_binds.is_empty(),
        "expected no CONFIG_BIND edges for plain classes, found: {:?}",
        config_binds
            .iter()
            .map(|e| (&e.source_qualname, &e.target_qualname))
            .collect::<Vec<_>>()
    );
}

/// Helper: collect (source, target) for edges of a given kind.
fn config_edge_targets(
    extracted: &lidx::indexer::extract::ExtractedFile,
    kind: &str,
) -> Vec<String> {
    let mut v: Vec<String> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == kind)
        .filter_map(|e| e.target_qualname.clone())
        .collect();
    v.sort();
    v
}

#[test]
fn pydantic_settings_skips_non_field_statements() {
    // model_config, plain (non-annotated) assignments, methods, properties,
    // docstrings/comments and the nested Config class must NOT produce edges.
    let source = r#"
from pydantic_settings import BaseSettings

class S(BaseSettings):
    """A settings class docstring."""
    # a leading comment
    real_field: str
    not_annotated = 42
    model_config = {"env_prefix": "APP_"}

    class Config:
        env_prefix = "FOO_"

    def helper(self):
        return self.real_field

    @property
    def computed(self) -> str:
        return "x"
"#;
    let module = module_name_from_rel_path("app/s.py");
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    assert_eq!(
        config_edge_targets(&extracted, "CONFIG_READ"),
        vec!["env://REAL_FIELD".to_string()],
        "only the annotated field should emit a CONFIG_READ"
    );
    assert_eq!(
        config_edge_targets(&extracted, "CONFIG_BIND"),
        vec!["env://REAL_FIELD".to_string()],
        "only the annotated field should emit a CONFIG_BIND"
    );
}

#[test]
fn pydantic_settings_skips_classvar_constants() {
    // ClassVar-annotated attributes are class constants, excluded from the model
    // and never bound from the environment.
    let source = r#"
from pydantic_settings import BaseSettings
from typing import ClassVar
import typing

class S(BaseSettings):
    real_field: str
    plain_classvar: ClassVar
    subscripted: ClassVar[int] = 5
    qualified: typing.ClassVar[str] = "x"
"#;
    let module = module_name_from_rel_path("app/s.py");
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    assert_eq!(
        config_edge_targets(&extracted, "CONFIG_READ"),
        vec!["env://REAL_FIELD".to_string()],
        "ClassVar attributes must not produce CONFIG_READ edges"
    );
    assert_eq!(
        config_edge_targets(&extracted, "CONFIG_BIND"),
        vec!["env://REAL_FIELD".to_string()],
        "ClassVar attributes must not produce CONFIG_BIND edges"
    );
}

#[test]
fn pydantic_settings_skips_private_attributes() {
    // Leading-underscore names are pydantic private attributes, not env-bound fields.
    let source = r#"
from pydantic_settings import BaseSettings

class S(BaseSettings):
    public_field: str
    _private: str
    __dunder: str
"#;
    let module = module_name_from_rel_path("app/s.py");
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    assert_eq!(
        config_edge_targets(&extracted, "CONFIG_READ"),
        vec!["env://PUBLIC_FIELD".to_string()],
        "leading-underscore names must not produce CONFIG_READ edges"
    );
    assert_eq!(
        config_edge_targets(&extracted, "CONFIG_BIND"),
        vec!["env://PUBLIC_FIELD".to_string()],
        "leading-underscore names must not produce CONFIG_BIND edges"
    );
}

#[test]
fn pydantic_settings_handles_complex_type_annotations() {
    // Optional/dict/generic annotations and a field literally named `env` are all valid fields.
    let source = r#"
from pydantic_settings import BaseSettings
from typing import Optional

class S(BaseSettings):
    optional_field: Optional[str] = None
    dict_field: dict[str, int] = {}
    env: str
"#;
    let module = module_name_from_rel_path("app/s.py");
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    assert_eq!(
        config_edge_targets(&extracted, "CONFIG_READ"),
        vec![
            "env://DICT_FIELD".to_string(),
            "env://ENV".to_string(),
            "env://OPTIONAL_FIELD".to_string(),
        ],
        "complex type annotations and a field named `env` should all be emitted"
    );
}

#[test]
fn pydantic_settings_empty_body_emits_nothing() {
    let source = r#"
from pydantic_settings import BaseSettings

class Empty(BaseSettings):
    pass

class OnlyMethods(BaseSettings):
    def m(self):
        return 1
"#;
    let module = module_name_from_rel_path("app/s.py");
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    assert!(
        config_edge_targets(&extracted, "CONFIG_READ").is_empty(),
        "a settings class with no annotated fields should emit no CONFIG_READ"
    );
    assert!(
        config_edge_targets(&extracted, "CONFIG_BIND").is_empty(),
        "a settings class with no annotated fields should emit no CONFIG_BIND"
    );
}

#[test]
fn pydantic_multiple_settings_classes_each_emit_their_fields() {
    // Multiple settings classes in one module; edges are scoped to the right class
    // and there is no cross-contamination or duplication.
    let source = r#"
from pydantic_settings import BaseSettings

class DbSettings(BaseSettings):
    db_url: str

class CacheSettings(BaseSettings):
    cache_url: str
"#;
    let module = module_name_from_rel_path("app/s.py");
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let pairs = |kind: &str| -> Vec<(String, String)> {
        extracted
            .edges
            .iter()
            .filter(|e| e.kind == kind)
            .map(|e| {
                (
                    e.source_qualname.clone().unwrap_or_default(),
                    e.target_qualname.clone().unwrap_or_default(),
                )
            })
            .collect()
    };
    let binds = pairs("CONFIG_BIND");
    assert!(binds.contains(&("app.s.DbSettings".to_string(), "env://DB_URL".to_string())));
    assert!(binds.contains(&(
        "app.s.CacheSettings".to_string(),
        "env://CACHE_URL".to_string()
    )));
    // No cross-contamination between sibling settings classes.
    assert!(!binds.contains(&(
        "app.s.DbSettings".to_string(),
        "env://CACHE_URL".to_string()
    )));
    assert!(!binds.contains(&(
        "app.s.CacheSettings".to_string(),
        "env://DB_URL".to_string()
    )));
    // Exactly one READ + one BIND per field (no double-counting).
    assert_eq!(
        binds.len(),
        2,
        "expected exactly one CONFIG_BIND per field, got {binds:?}"
    );
    assert_eq!(
        pairs("CONFIG_READ").len(),
        2,
        "expected exactly one CONFIG_READ per field, got {:?}",
        pairs("CONFIG_READ")
    );
}

#[test]
fn pydantic_inheritance_chain_only_direct_subclass_detected() {
    // Documents a known limitation: the extractor matches the literal base name,
    // so an indirect subclass (Child -> Base -> BaseSettings) is NOT detected.
    let source = r#"
from pydantic_settings import BaseSettings

class Base(BaseSettings):
    shared: str

class Child(Base):
    extra: str
"#;
    let module = module_name_from_rel_path("app/s.py");
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let reads = config_edge_targets(&extracted, "CONFIG_READ");
    assert!(
        reads.contains(&"env://SHARED".to_string()),
        "direct BaseSettings subclass fields should be emitted"
    );
    assert!(
        !reads.contains(&"env://EXTRA".to_string()),
        "indirect subclass is not detected (known limitation): {reads:?}"
    );
}

#[test]
fn pydantic_settings_class_inside_function_emits_nothing() {
    // Classes nested inside functions are not indexed at all (fn_depth guard),
    // so a function-local settings class produces no config edges.
    let source = r#"
from pydantic_settings import BaseSettings

def make():
    class Local(BaseSettings):
        field_x: str
    return Local
"#;
    let module = module_name_from_rel_path("app/s.py");
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    assert!(
        config_edge_targets(&extracted, "CONFIG_READ").is_empty(),
        "function-local settings class should not emit config edges"
    );
    assert!(
        config_edge_targets(&extracted, "CONFIG_BIND").is_empty(),
        "function-local settings class should not emit config edges"
    );
}

#[test]
fn tuple_unpacking_target_lowers_to_an_unknown_receiver() {
    // `base_var, body = split()` then `body.append(x)`: `body` is bound by
    // tuple-unpacking, so its value is unknown and the call can never bind
    // by the name `append` (it was observed binding to a domain method).
    let source = r#"
def render():
    base_var, body = split()
    body.append("line")
"#;
    let callee = callee_of(source, "app/gen.py", "body.append");
    assert!(is_attr_of_unknown(&callee, "append"), "{callee:?}");
}

#[test]
fn for_loop_unpacking_target_lowers_to_an_unknown_receiver() {
    let source = r#"
def render():
    for key, body in pairs():
        body.append("line")
"#;
    let callee = callee_of(source, "app/gen.py", "body.append");
    assert!(is_attr_of_unknown(&callee, "append"), "{callee:?}");
}

#[test]
fn module_level_local_is_inlined_like_a_function_local() {
    // A call at module top level through a module-level local (the
    // namespace-package idiom `__path__ = pkgutil.extend_path(...)` then
    // `__path__.append(...)`) is its own scope: the local is inlined, so the
    // receiver is the call's value, not a free name.
    let source = r#"
import pkgutil

__path__ = pkgutil.extend_path(__path__, __name__)
__path__.append("/extra")
"#;
    let callee = callee_of(source, "app/__init__.py", "__path__.append");
    assert!(
        matches!(&callee, PyExpr::Attr(inner, n) if n == "append" && matches!(**inner, PyExpr::Call(_))),
        "{callee:?}"
    );
}

// Import-aware receiver resolution regression tests, mirroring the C#
// mechanism (see `csharp.rs`'s equivalent tests) — `from x import Y`
// already binds a specific name to a specific target, so (unlike C#'s
// `using NS;`) there is normally exactly one candidate; ambiguity only
// shows up if the same name is bound twice in the same file.

#[test]
fn from_import_bare_name_resolves_to_its_module() {
    let domain = "class Widget:\n    @staticmethod\n    def create():\n        return 1\n";
    let source = "from pkg.domain import Widget\n\ndef run():\n    Widget.create()\n";
    let bound = bound_in(&[("pkg/domain.py", domain)], "app/caller.py", source);
    assert_eq!(
        bound,
        vec![(
            "Widget.create".to_string(),
            "pkg.domain.Widget.create".to_string()
        )],
        "`from x import Y` must bind the name to exactly that module's definition"
    );
}

#[test]
fn import_module_as_alias_resolves_to_its_target() {
    let domain = "def create():\n    return 1\n";
    let source = "import pkg.domain as dom\n\ndef run():\n    dom.create()\n";
    let bound = bound_in(&[("pkg/domain.py", domain)], "app/caller.py", source);
    assert_eq!(
        bound,
        vec![("dom.create".to_string(), "pkg.domain.create".to_string())],
        "`import x.y as z` must bind the alias to its dotted module"
    );
}

#[test]
fn multiline_chained_call_resolves_like_single_line() {
    // Python needs the chain wrapped in parens for the newline to be legal
    // (a bare `pkg\n.mod` outside brackets is a SyntaxError), but the
    // `function` field's raw text still carries the interior newlines and
    // indentation exactly like the C# `UniqueName\n    .Create(...)` case
    // this fix targets.
    let source = "
def caller():
    (
        pkg
        .mod
        .helper()
    )
";
    let module = module_name_from_rel_path("app/caller.py");
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.detail.is_none())
        .expect("pkg.mod.helper() call edge");
    assert_eq!(
        call.target_qualname.as_deref(),
        Some("pkg.mod.helper"),
        "multi-line chain must resolve to the same qualname as the single-line form"
    );
}

// Parenthesized `from x import (a, b)` regression tests (issue #117) —
// `parse_imports`/`parse_import_bindings` used to split the statement's raw
// text on commas, so the `(`/`)` tokens and interior newlines produced
// bogus `pkg.mod.(`/`pkg.mod.)` IMPORTS targets, dropped the first name,
// and made the whole statement's bindings unusable (any text containing
// `(` was skipped outright). Walking the tree-sitter node's `name` field
// children sidesteps punctuation and whitespace entirely.

#[test]
fn from_import_parenthesized_multiline_trailing_comma_emits_clean_targets() {
    let source = "
from pkg.engine import (
    compute_frontier,
    other_fn as of,
)
";
    let module = module_name_from_rel_path("app/caller.py");
    let mut extractor = PythonExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let targets: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "IMPORTS")
        .filter_map(|e| e.target_qualname.as_deref())
        .collect();

    assert!(
        targets.contains(&"pkg.engine.compute_frontier"),
        "the first name in a parenthesized import list must not be dropped, got: {targets:?}"
    );
    assert!(
        targets.contains(&"pkg.engine.other_fn"),
        "an aliased name inside the parens must use its pre-alias name as the IMPORTS target, \
         got: {targets:?}"
    );
    assert!(
        targets
            .iter()
            .all(|t| !t.ends_with('(') && !t.ends_with(')')),
        "no IMPORTS target may be the bare paren punctuation, got: {targets:?}"
    );
}

#[test]
fn from_import_parenthesized_names_bind_for_call_resolution() {
    // `parse_import_bindings` once skipped any import statement whose text
    // contained `(`, so its calls never bound.
    let engine = "def compute_frontier():\n    return 1\n\ndef other_fn():\n    return 2\n";
    let source = "
from pkg.engine import (
    compute_frontier,
    other_fn as of,
)

def run():
    compute_frontier()
    of()
";
    let mut bound = bound_in(&[("pkg/engine.py", engine)], "app/caller.py", source);
    bound.sort();
    assert_eq!(
        bound,
        vec![
            (
                "app.caller.compute_frontier".to_string(),
                "pkg.engine.compute_frontier".to_string()
            ),
            (
                "app.caller.of".to_string(),
                "pkg.engine.other_fn".to_string()
            ),
        ],
        "a parenthesized import list must bind both the plain and the aliased name"
    );
}
