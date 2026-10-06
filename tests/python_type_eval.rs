//! The Python type evaluator over multi-file fixtures: real extractor
//! output, no database. Each pattern has a positive test and a precision
//! trap (a call that must NOT bind).

use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::python::{PythonExtractor, module_name_from_rel_path};
use lidx::indexer::python_eval::{How, Outcome, PyTypeTable, Value, Why, resolve_site, type_of};
use lidx::indexer::python_expr::{PyCallSite, PyExpr};
use lidx::indexer::python_types::PyFileDecls;

struct Fx {
    table: PyTypeTable,
    /// (path, module, line, site) for every CALLS edge.
    sites: Vec<(String, String, i64, PyCallSite)>,
}

fn fixture(files: &[(&str, &str)]) -> Fx {
    let mut decls: Vec<PyFileDecls> = Vec::new();
    let mut sites = Vec::new();
    for (path, src) in files {
        let module = module_name_from_rel_path(path);
        let mut x = PythonExtractor::new().unwrap();
        x.set_current_path(path);
        let out = x.extract(src, &module).unwrap();
        decls.push(out.py_decls.expect("python decls"));
        for e in out.edges.into_iter().filter(|e| e.kind == "CALLS") {
            if let (Some(site), Some(line)) = (e.py_site, e.evidence_start_line) {
                sites.push((path.to_string(), module.clone(), line, *site));
            }
        }
    }
    Fx {
        table: PyTypeTable::build(decls),
        sites,
    }
}

fn callee_name(site: &PyCallSite) -> Option<&str> {
    match &site.callee {
        PyExpr::Name(n) => Some(n),
        PyExpr::Attr(_, n) => Some(n),
        PyExpr::Imported(t) => t.rsplit('.').next(),
        _ => None,
    }
}

/// Outcome of the call named `name` on the line of `path` containing `needle`.
fn outcome(fx: &Fx, path: &str, needle: &str, name: &str, files: &[(&str, &str)]) -> Outcome {
    let src = files.iter().find(|(p, _)| *p == path).unwrap().1;
    let line = src
        .lines()
        .position(|l| l.contains(needle))
        .unwrap_or_else(|| panic!("no line with {needle:?}")) as i64
        + 1;
    let found: Vec<_> = fx
        .sites
        .iter()
        .filter(|(p, _, l, s)| p == path && *l == line && callee_name(s) == Some(name))
        .collect();
    assert_eq!(found.len(), 1, "sites on line {line} named {name}");
    resolve_site(&fx.table, &found[0].3, &found[0].1)
}

fn bound(o: Outcome) -> String {
    match o {
        Outcome::Bound { target, .. } => target,
        other => panic!("expected Bound, got {other:?}"),
    }
}

fn not_bound(o: &Outcome) {
    assert!(
        !matches!(o, Outcome::Bound { .. } | Outcome::Ambiguous(_)),
        "must not bind: {o:?}"
    );
}

const LIB: &[(&str, &str)] = &[
    (
        "pkg/__init__.py",
        "from ._client import *\nfrom ._auth import *\nfrom ._models import Request, Response, Headers, Cookies\nfrom .broker import Broker\n",
    ),
    (
        "pkg/_client.py",
        r#"import typing
from ._models import Request, Response, Cookies

T = typing.TypeVar("T", bound="Client")
__all__ = ["Client", "AsyncClient"]


class BaseClient:
    def build_request(self) -> Request: ...

    @property
    def cookies(self) -> Cookies: ...

    def poke(self):
        self.cookies.extract_cookies()


class Client(BaseClient):
    def __enter__(self: T) -> T: ...

    def request(self) -> Response: ...

    def setup(self):
        self._cookies = Cookies()

    def use(self):
        self.cookies.extract_cookies()
        self._cookies.extract_cookies()

    def sup(self):
        super().build_request()


class AsyncClient(BaseClient):
    async def __aenter__(self: T) -> T: ...

    async def request(self) -> Response: ...

    def build_request(self) -> Request: ...
"#,
    ),
    (
        "pkg/_auth.py",
        "__all__ = [\"Auth\", \"DigestAuth\"]\n\nclass Auth:\n    def sync_auth_flow(self): ...\n\nclass DigestAuth(Auth):\n    pass\n",
    ),
    (
        "pkg/_models.py",
        r#"class Headers:
    def get_list(self): ...
    def multi_items(self): ...

class Cookies:
    def extract_cookies(self): ...

class Request:
    def __init__(self, headers=None):
        self.headers = Headers(headers)

class Response:
    headers: Headers

    def raise_for_status(self): ...

class Wrapper:
    def __init__(self, response: Response):
        self.response = response

    def go(self):
        self.response.headers.multi_items()
"#,
    ),
    (
        "pkg/broker.py",
        r#"class TaskSupervisor:
    def shutdown(self): ...

class Broker:
    def __init__(self):
        self._supervisor = TaskSupervisor()

    def stop(self):
        self._supervisor.shutdown()
"#,
    ),
    ("pkg/ops/__init__.py", "from .evolve import evolve\n"),
    (
        "pkg/ops/evolve/__init__.py",
        "from .operation import evolve, EvolveOperation\n",
    ),
    (
        "pkg/ops/evolve/operation.py",
        r#"from typing import overload

class EvolveOperation:
    def __init__(self, name): ...

    @overload
    def apply_plan(self, p: int) -> int: ...
    @overload
    def apply_plan(self, p: str) -> str: ...
    def apply_plan(self, p): ...

def evolve(name) -> EvolveOperation: ...
"#,
    ),
    ("pkg/strategies/__init__.py", "from .tw import tw\n"),
    (
        "pkg/strategies/tw.py",
        "class Thing:\n    def run(self): ...\n\ndef tw(x) -> Thing: ...\n",
    ),
];

fn with_app(app: &str) -> (Fx, Vec<(&'static str, String)>) {
    let mut files: Vec<(&str, String)> = LIB.iter().map(|(p, s)| (*p, s.to_string())).collect();
    files.push(("app/main.py", app.to_string()));
    let refs: Vec<(&str, &str)> = files.iter().map(|(p, s)| (*p, s.as_str())).collect();
    (fixture(&refs), files)
}

/// Evaluate `needle`'s call named `name` in `app/main.py`.
fn app_outcome(app: &str, needle: &str, name: &str) -> Outcome {
    let (fx, files) = with_app(app);
    let refs: Vec<(&str, &str)> = files.iter().map(|(p, s)| (*p, s.as_str())).collect();
    outcome(&fx, "app/main.py", needle, name, &refs)
}

fn lib_outcome(path: &str, needle: &str, name: &str) -> Outcome {
    let (fx, files) = with_app("x = 1\n");
    let refs: Vec<(&str, &str)> = files.iter().map(|(p, s)| (*p, s.as_str())).collect();
    outcome(&fx, path, needle, name, &refs)
}

// ---- P1: star re-export through a package __init__ -------------------------

#[test]
fn p1_client_inherits_build_request_through_star_reexport() {
    let o = app_outcome(
        "import pkg\n\ndef f():\n    client = pkg.Client()\n    client.build_request()\n",
        "client.build_request",
        "build_request",
    );
    assert_eq!(
        o,
        Outcome::Bound {
            target: "pkg._client.BaseClient.build_request".into(),
            how: How::Inherited
        }
    );
}

#[test]
fn p1_digest_auth_inherits_sync_auth_flow() {
    let o = app_outcome(
        "import pkg\n\ndef f():\n    auth = pkg.DigestAuth()\n    auth.sync_auth_flow()\n",
        "auth.sync_auth_flow",
        "sync_auth_flow",
    );
    assert_eq!(bound(o), "pkg._auth.Auth.sync_auth_flow");
}

#[test]
fn p1_trap_name_excluded_by_all_is_not_reexported() {
    // `BaseClient` is not in `_client.__all__`, so `pkg.BaseClient` is absent.
    let o = app_outcome(
        "import pkg\n\ndef f():\n    pkg.BaseClient().build_request()\n",
        "pkg.BaseClient()",
        "build_request",
    );
    not_bound(&o);
}

#[test]
fn constructor_call_binds_to_the_class() {
    let o = app_outcome("import pkg\n\npkg.Client()\n", "pkg.Client()", "Client");
    assert_eq!(
        o,
        Outcome::Bound {
            target: "pkg._client.Client".into(),
            how: How::Import
        }
    );
}

// ---- P2: context managers ---------------------------------------------------

#[test]
fn p2_with_enter_returns_the_receiver_through_a_self_typevar() {
    let o = app_outcome(
        "import pkg\n\ndef f():\n    with pkg.Client() as c:\n        c.build_request()\n",
        "c.build_request",
        "build_request",
    );
    assert_eq!(bound(o), "pkg._client.BaseClient.build_request");
}

#[test]
fn p2_async_with_uses_aenter() {
    let o = app_outcome(
        "import pkg\n\nasync def f():\n    async with pkg.AsyncClient() as c:\n        c.build_request()\n",
        "c.build_request",
        "build_request",
    );
    // `AsyncClient` overrides it: the receiver's own declaration.
    assert_eq!(bound(o), "pkg._client.AsyncClient.build_request");
}

#[test]
fn p2_trap_no_enter_in_repo_mro_is_unknown() {
    let o = app_outcome(
        "import pkg\n\ndef f():\n    with pkg.Headers() as h:\n        h.get_list()\n",
        "h.get_list",
        "get_list",
    );
    not_bound(&o);
}

#[test]
fn p2_trap_sync_with_on_async_only_class_is_unknown() {
    let o = app_outcome(
        "import pkg\n\ndef f():\n    with pkg.AsyncClient() as c:\n        c.build_request()\n",
        "c.build_request",
        "build_request",
    );
    not_bound(&o);
}

// ---- P3: awaited results ----------------------------------------------------

#[test]
fn p3_awaited_async_return_annotation() {
    let o = app_outcome(
        "import pkg\n\nasync def f():\n    client = pkg.AsyncClient()\n    response = await client.request()\n    response.raise_for_status()\n",
        "response.raise_for_status",
        "raise_for_status",
    );
    assert_eq!(bound(o), "pkg._models.Response.raise_for_status");
}

#[test]
fn p3_sync_return_annotation() {
    let o = app_outcome(
        "import pkg\n\ndef f():\n    client = pkg.Client()\n    response = client.request()\n    response.raise_for_status()\n",
        "response.raise_for_status",
        "raise_for_status",
    );
    assert_eq!(bound(o), "pkg._models.Response.raise_for_status");
}

#[test]
fn p3_trap_missing_await_is_a_coroutine() {
    let o = app_outcome(
        "import pkg\n\nasync def f():\n    client = pkg.AsyncClient()\n    response = client.request()\n    response.raise_for_status()\n",
        "response.raise_for_status",
        "raise_for_status",
    );
    not_bound(&o);
}

// ---- P4: properties and attributes assigned in methods ----------------------

#[test]
fn p4_inherited_property_return_type() {
    // In `BaseClient.poke`: the property is declared on the same class.
    let o = lib_outcome(
        "pkg/_client.py",
        "self.cookies.extract_cookies()",
        "extract_cookies",
    );
    assert_eq!(bound(o), "pkg._models.Cookies.extract_cookies");
}

#[test]
fn p4_property_via_self_on_subclass() {
    let (fx, _) = with_app("x = 1\n");
    // Every `extract_cookies` site in `_client.py`: `BaseClient.poke`,
    // `Client.use` (property inherited from the base) and `Client.use`
    // (attribute set in `setup`).
    let hits: Vec<_> = fx
        .sites
        .iter()
        .filter(|(p, _, _, s)| p == "pkg/_client.py" && callee_name(s) == Some("extract_cookies"))
        .map(|(_, m, _, s)| resolve_site(&fx.table, s, m))
        .collect();
    assert_eq!(hits.len(), 3);
    for h in &hits[..2] {
        assert_eq!(
            bound(h.clone()),
            "pkg._models.Cookies.extract_cookies",
            "{hits:?}"
        );
    }
}

#[test]
fn p4_attr_assigned_in_a_non_init_method() {
    let o = app_outcome(
        "import pkg\n\ndef f():\n    c = pkg.Client()\n    c._cookies.extract_cookies()\n",
        "c._cookies.extract_cookies",
        "extract_cookies",
    );
    assert_eq!(bound(o), "pkg._models.Cookies.extract_cookies");
}

#[test]
fn p4_attr_assigned_in_init() {
    let o = lib_outcome("pkg/broker.py", "self._supervisor.shutdown()", "shutdown");
    assert_eq!(bound(o), "pkg.broker.TaskSupervisor.shutdown");
}

#[test]
fn p4_trap_property_read_on_a_base_typed_receiver_for_unknown_member() {
    let o = app_outcome(
        "import pkg\n\ndef f():\n    c = pkg.Client()\n    c.cookies.nope()\n",
        "c.cookies.nope",
        "nope",
    );
    assert_eq!(o, Outcome::NoCandidates);
}

// ---- P5: attribute chains ----------------------------------------------------

#[test]
fn p5_annotated_param_attr_set_from_constructor() {
    let o = app_outcome(
        "import pkg\n\ndef f(request: pkg.Request):\n    request.headers.get_list()\n",
        "request.headers.get_list",
        "get_list",
    );
    assert_eq!(bound(o), "pkg._models.Headers.get_list");
}

#[test]
fn p5_attr_assigned_from_annotated_init_param() {
    let o = lib_outcome(
        "pkg/_models.py",
        "self.response.headers.multi_items()",
        "multi_items",
    );
    assert_eq!(bound(o), "pkg._models.Headers.multi_items");
}

#[test]
fn p5_attr_of_a_local_instance() {
    let o = app_outcome(
        "import pkg\n\ndef f():\n    broker = pkg.Broker()\n    broker._supervisor.shutdown()\n",
        "broker._supervisor.shutdown",
        "shutdown",
    );
    assert_eq!(bound(o), "pkg.broker.TaskSupervisor.shutdown");
}

// ---- P6: re-exports, shadowed submodules, overloads --------------------------

#[test]
fn p6_reexported_function_wins_over_same_named_submodule() {
    let o = app_outcome(
        "from pkg.ops import evolve\n\ndef f(p):\n    evolve(\"x\").apply_plan(p)\n",
        "evolve(\"x\").apply_plan",
        "apply_plan",
    );
    assert_eq!(
        bound(o),
        "pkg.ops.evolve.operation.EvolveOperation.apply_plan"
    );
}

#[test]
fn p6_the_function_itself_binds_not_the_submodule() {
    let o = app_outcome(
        "from pkg.ops import evolve\n\ndef f():\n    evolve(\"x\")\n",
        "evolve(\"x\")",
        "evolve",
    );
    assert_eq!(bound(o), "pkg.ops.evolve.operation.evolve");
}

#[test]
fn p6_package_function_shadowing_its_submodule() {
    let o = app_outcome(
        "from pkg.strategies import tw\n\ndef f():\n    tw(1).run()\n",
        "tw(1).run",
        "run",
    );
    assert_eq!(bound(o), "pkg.strategies.tw.Thing.run");
    let o = app_outcome(
        "from pkg.strategies import tw\n\ndef f():\n    tw(1)\n",
        "tw(1)",
        "tw",
    );
    assert_eq!(bound(o), "pkg.strategies.tw.tw");
}

#[test]
fn p6_submodule_still_reachable_when_nothing_shadows_it() {
    let o = app_outcome(
        "import pkg.ops.evolve.operation as op\n\ndef f():\n    op.evolve(\"x\")\n",
        "op.evolve",
        "evolve",
    );
    assert_eq!(bound(o), "pkg.ops.evolve.operation.evolve");
}

#[test]
fn p6_overloads_bind_to_the_single_implementation() {
    let o = app_outcome(
        "from pkg.ops.evolve import EvolveOperation\n\ndef f(p):\n    EvolveOperation(\"b\").apply_plan(p)\n",
        "EvolveOperation(\"b\").apply_plan",
        "apply_plan",
    );
    assert_eq!(
        bound(o),
        "pkg.ops.evolve.operation.EvolveOperation.apply_plan"
    );
}

// ---- Super, classmethods, Self ------------------------------------------------

#[test]
fn super_call_starts_after_the_class() {
    let o = lib_outcome("pkg/_client.py", "super().build_request()", "build_request");
    assert_eq!(
        o,
        Outcome::Bound {
            target: "pkg._client.BaseClient.build_request".into(),
            how: How::Inherited
        }
    );
}

#[test]
fn classmethod_cls_is_the_class_and_self_return_is_an_instance() {
    let files = [(
        "m.py",
        "class K:\n    @classmethod\n    def make(cls) -> \"K\":\n        cls.helper()\n        return cls()\n    @classmethod\n    def helper(cls) -> \"Self\": ...\n    def go(self): ...\n\ndef f():\n    K.make().go()\n    K.helper().go()\n",
    )];
    let fx = fixture(&files);
    let o = outcome(&fx, "m.py", "K.make().go()", "go", &files);
    assert_eq!(bound(o), "m.K.go");
    let o = outcome(&fx, "m.py", "K.helper().go()", "go", &files);
    assert_eq!(bound(o), "m.K.go");
    let o = outcome(&fx, "m.py", "cls.helper()", "helper", &files);
    assert_eq!(
        o,
        Outcome::Bound {
            target: "m.K.helper".into(),
            how: How::Exact
        }
    );
}

// ---- precision traps -----------------------------------------------------------

#[test]
fn trap_unannotated_param_receiver() {
    let o = app_outcome(
        "def f(c):\n    c.build_request()\n",
        "c.build_request",
        "build_request",
    );
    not_bound(&o);
}

#[test]
fn trap_rebound_local() {
    let o = app_outcome(
        "import pkg\n\ndef f(other):\n    c = pkg.Client()\n    c = other()\n    c.build_request()\n",
        "c.build_request",
        "build_request",
    );
    not_bound(&o);
}

#[test]
fn trap_builtin_type_is_external() {
    let o = app_outcome(
        "def f():\n    d = dict()\n    d.get(\"a\")\n",
        "d.get",
        "get",
    );
    assert_eq!(o, Outcome::External);
}

#[test]
fn trap_stdlib_import_is_external() {
    let o = app_outcome(
        "import json\n\ndef f():\n    json.dumps(1)\n",
        "json.dumps",
        "dumps",
    );
    assert_eq!(o, Outcome::External);
}

#[test]
fn trap_attribute_sources_that_disagree() {
    let files = [(
        "m.py",
        "class A:\n    def run(self): ...\nclass B:\n    def run(self): ...\nclass H:\n    def __init__(self):\n        self.x = A()\n    def other(self):\n        self.x = B()\n    def go(self):\n        self.x.run()\n",
    )];
    let fx = fixture(&files);
    not_bound(&outcome(&fx, "m.py", "self.x.run()", "run", &files));
}

#[test]
fn attribute_sources_that_agree_bind() {
    let files = [(
        "m.py",
        "class A:\n    def run(self): ...\nclass H:\n    x: A\n    def __init__(self):\n        self.x = A()\n    def other(self):\n        self.x = A()\n    def go(self):\n        self.x.run()\n",
    )];
    let fx = fixture(&files);
    assert_eq!(
        bound(outcome(&fx, "m.py", "self.x.run()", "run", &files)),
        "m.A.run"
    );
}

#[test]
fn trap_ambiguous_method_name_on_untyped_receiver() {
    let files = [(
        "m.py",
        "class A:\n    def run(self): ...\nclass B:\n    def run(self): ...\ndef f(p):\n    p.run()\n",
    )];
    let fx = fixture(&files);
    not_bound(&outcome(&fx, "m.py", "p.run()", "run", &files));
}

#[test]
fn same_short_class_name_binds_by_import() {
    let files = [
        ("a/models.py", "class Item:\n    def ping(self): ...\n"),
        ("b/models.py", "class Item:\n    def pong(self): ...\n"),
        (
            "app.py",
            "from a.models import Item\nfrom b.models import Item as Other\n\ndef f():\n    Item().ping()\n    Other().pong()\n    Item().pong()\n",
        ),
    ];
    let fx = fixture(&files);
    assert_eq!(
        bound(outcome(&fx, "app.py", "Item().ping()", "ping", &files)),
        "a.models.Item.ping"
    );
    assert_eq!(
        bound(outcome(&fx, "app.py", "Other().pong()", "pong", &files)),
        "b.models.Item.pong"
    );
    assert_eq!(
        outcome(&fx, "app.py", "Item().pong()", "pong", &files),
        Outcome::NoCandidates
    );
}

#[test]
fn trap_subclass_only_method_on_base_type() {
    let o = app_outcome(
        "import pkg\nfrom pkg._client import BaseClient\n\ndef f(c: BaseClient):\n    c.request()\n",
        "c.request()",
        "request",
    );
    assert_eq!(o, Outcome::NoCandidates);
}

#[test]
fn base_typed_receiver_binds_to_the_base_declaration_only() {
    let o = app_outcome(
        "from pkg._client import BaseClient\n\ndef f(c: BaseClient):\n    c.build_request()\n",
        "c.build_request()",
        "build_request",
    );
    assert_eq!(bound(o), "pkg._client.BaseClient.build_request");
}

#[test]
fn mro_cycle_terminates() {
    let files = [(
        "m.py",
        "class A(B):\n    def a(self): ...\nclass B(A):\n    def b(self): ...\ndef f():\n    A().b()\n    A().zzz()\n",
    )];
    let fx = fixture(&files);
    not_bound(&outcome(&fx, "m.py", "A().zzz()", "zzz", &files));
    // Whatever it binds, it must terminate; `b` is reachable from `A`.
    let _ = outcome(&fx, "m.py", "A().b()", "b", &files);
}

#[test]
fn reexport_cycle_terminates() {
    let files = [
        ("pkg/__init__.py", ""),
        ("pkg/c1.py", "from .c2 import X\n"),
        ("pkg/c2.py", "from .c1 import X\n"),
        ("app.py", "from pkg.c1 import X\n\nX()\nX().m()\n"),
    ];
    let fx = fixture(&files);
    not_bound(&outcome(&fx, "app.py", "X()", "X", &files));
}

#[test]
fn diamond_mro_is_c3() {
    let files = [(
        "m.py",
        "class Root:\n    def hi(self): ...\nclass L(Root):\n    pass\nclass R(Root):\n    def hi(self): ...\nclass D(L, R):\n    pass\ndef f():\n    D().hi()\n",
    )];
    let fx = fixture(&files);
    assert_eq!(
        bound(outcome(&fx, "m.py", "D().hi()", "hi", &files)),
        "m.R.hi"
    );
}

#[test]
fn function_local_import_resolves_member_or_module() {
    let o = app_outcome(
        "def f():\n    from pkg.strategies import tw\n    tw(1)\n    import pkg.broker as b\n    b.Broker()\n",
        "tw(1)",
        "tw",
    );
    assert_eq!(bound(o), "pkg.strategies.tw.tw");
    let o = app_outcome(
        "def f():\n    import pkg.broker as b\n    b.Broker()\n",
        "b.Broker()",
        "Broker",
    );
    assert_eq!(bound(o), "pkg.broker.Broker");
}

#[test]
fn type_of_and_trace_are_exposed() {
    let (fx, _) = with_app("import pkg\n\ndef f():\n    pkg.Client().request()\n");
    let (_, m, _, site) = fx
        .sites
        .iter()
        .find(|(p, _, _, s)| p == "app/main.py" && callee_name(s) == Some("request"))
        .unwrap();
    let PyExpr::Attr(recv, _) = &site.callee else {
        panic!()
    };
    assert_eq!(
        type_of(&fx.table, recv, m),
        Value::Instance("pkg._client.Client".into())
    );
    let t = lidx::indexer::python_eval::trace_site(&fx.table, site, m);
    assert!(t.contains("Instance(\"pkg._client.Client\")"), "{t}");
    assert!(matches!(
        type_of(&fx.table, &PyExpr::Name("nothing_here".into()), m),
        Value::Unknown(Why::UnresolvedName)
    ));
}

// ---- unannotated `return self` ---------------------------------------------

const RETURNS_SELF: &[(&str, &str)] = &[
    ("pkg/__init__.py", ""),
    (
        "pkg/res.py",
        r#"class Res:
    def __enter__(self):
        return self

    def close(self):
        return None

    def mixed_enter(self, x):
        if x:
            return self
        return 1


class Sub(Res):
    def extra(self):
        return 1


class Other:
    def close(self):
        return 2
"#,
    ),
    (
        "app.py",
        r#"from pkg.res import Res, Sub


def use():
    with Res() as r:
        r.close()
    with Sub() as s:
        s.extra()
    Res().__enter__().close()


def trap(res: Res):
    res.mixed_enter(1).close()
"#,
    ),
];

#[test]
fn unannotated_return_self_types_a_with_target_and_a_chain() {
    let fx = fixture(RETURNS_SELF);
    let b = |needle: &str, name: &str| bound(outcome(&fx, "app.py", needle, name, RETURNS_SELF));
    assert_eq!(b("r.close()", "close"), "pkg.res.Res.close");
    // The receiver is the subclass: `self` is whatever the method was reached on.
    assert_eq!(b("s.extra()", "extra"), "pkg.res.Sub.extra");
    assert_eq!(b(".__enter__().close()", "close"), "pkg.res.Res.close");
}

#[test]
fn mixed_returns_do_not_count_as_returns_self() {
    let fx = fixture(RETURNS_SELF);
    let o = outcome(
        &fx,
        "app.py",
        "res.mixed_enter(1).close()",
        "close",
        RETURNS_SELF,
    );
    not_bound(&o);
}

// ---- a local assigned several times ----------------------------------------

const REBOUND: &[(&str, &str)] = &[
    ("pkg/__init__.py", ""),
    (
        "pkg/things.py",
        r#"class Widget:
    def run(self):
        return 1


class Gadget:
    def run(self):
        return 2


def unknown():
    return None
"#,
    ),
    (
        "app.py",
        r#"from pkg.things import Widget, Gadget, unknown


def agree():
    w = Widget()
    w.run()
    w = Widget()
    w.run()


def disagree():
    w = Widget()
    w = Gadget()
    w.run()


def one_unknown():
    w = Widget()
    w = unknown()
    w.run()


def looped(items):
    w = Widget()
    for w in items:
        pass
    w.run()


def augmented():
    w = Widget()
    w += 1
    w.run()
"#,
    ),
];

#[test]
fn a_local_assigned_the_same_type_every_time_binds() {
    let fx = fixture(REBOUND);
    // Both calls (lines of `w.run()` inside `agree`) bind.
    let src = REBOUND[2].1;
    let lines: Vec<i64> = src
        .lines()
        .enumerate()
        .filter(|(_, l)| l.trim() == "w.run()")
        .map(|(i, _)| i as i64 + 1)
        .collect();
    let agree: Vec<_> = fx
        .sites
        .iter()
        .filter(|(p, _, l, s)| p == "app.py" && *l <= lines[1] && callee_name(s) == Some("run"))
        .collect();
    assert_eq!(agree.len(), 2);
    for (_, module, _, site) in agree {
        assert_eq!(
            bound(resolve_site(&fx.table, site, module)),
            "pkg.things.Widget.run"
        );
    }
}

#[test]
fn a_local_assigned_different_types_does_not_bind() {
    let fx = fixture(REBOUND);
    let src = REBOUND[2].1;
    let line_of = |func: &str| -> i64 {
        let start = src.find(&format!("def {func}(")).unwrap();
        let rest = &src[start..];
        let at = rest.find("w.run()").unwrap();
        src[..start + at].matches('\n').count() as i64 + 1
    };
    for func in ["disagree", "one_unknown", "looped", "augmented"] {
        let line = line_of(func);
        let found: Vec<_> = fx
            .sites
            .iter()
            .filter(|(p, _, l, s)| p == "app.py" && *l == line && callee_name(s) == Some("run"))
            .collect();
        assert_eq!(found.len(), 1, "{func}");
        not_bound(&resolve_site(&fx.table, &found[0].3, &found[0].1));
    }
}

// ---- the value a generator receives from `yield` ----------------------------

const SENT: &[(&str, &str)] = &[
    ("pkg/__init__.py", ""),
    (
        "pkg/msgs.py",
        r#"class Request:
    pass


class Response:
    def status(self):
        return 1
"#,
    ),
    (
        "app.py",
        r#"import typing
from typing import Generator, AsyncGenerator
from pkg.msgs import Request, Response


def flow(request: Request) -> typing.Generator[Request, Response, None]:
    response = yield request
    response.status()


async def aflow(request: Request) -> AsyncGenerator[Request, Response]:
    response = yield request
    response.status()


def untyped(request: Request):
    response = yield request
    response.status()


def only_yields(request: Request) -> Generator[Request]:
    response = yield request
    response.status()
"#,
    ),
];

#[test]
fn a_yield_assignment_takes_the_declared_send_type() {
    let fx = fixture(SENT);
    let status_at = |func: &str| -> Outcome {
        let src = SENT[2].1;
        let start = src.find(&format!("def {func}(")).unwrap();
        let at = src[start..].find("response.status()").unwrap();
        let line = src[..start + at].matches('\n').count() as i64 + 1;
        let found: Vec<_> = fx
            .sites
            .iter()
            .filter(|(p, _, l, s)| p == "app.py" && *l == line && callee_name(s) == Some("status"))
            .collect();
        assert_eq!(found.len(), 1, "{func}");
        resolve_site(&fx.table, &found[0].3, &found[0].1)
    };
    assert_eq!(bound(status_at("flow")), "pkg.msgs.Response.status");
    assert_eq!(bound(status_at("aflow")), "pkg.msgs.Response.status");
    // Nothing declares what is sent: unknown, never a guess.
    not_bound(&status_at("untyped"));
    not_bound(&status_at("only_yields"));
}

// ---- scripts importing their sibling modules --------------------------------

const SCRIPT_DIR: &[(&str, &str)] = &[
    (
        "scripts/main.py",
        "import util\nfrom util import helper\n\n\ndef start(n):\n    helper(n)\n    util.helper(n)\n",
    ),
    ("scripts/util.py", "def helper(x):\n    return x\n"),
    (
        "lib/other.py",
        "import util\n\n\ndef go():\n    util.helper(1)\n",
    ),
    // In a regular package a sibling `json.py` does not hijack the stdlib's.
    ("pkg/__init__.py", ""),
    ("pkg/json.py", "def dumps(x):\n    return x\n"),
    (
        "pkg/main.py",
        "from json import dumps\n\n\ndef go(n):\n    dumps(n)\n",
    ),
];

#[test]
fn a_script_imports_the_sibling_module_but_not_a_distant_one() {
    let fx = fixture(SCRIPT_DIR);
    let at = |path: &str, needle: &str, name: &str| outcome(&fx, path, needle, name, SCRIPT_DIR);
    assert_eq!(
        bound(at("scripts/main.py", "    helper(n)", "helper")),
        "scripts.util.helper"
    );
    assert_eq!(
        bound(at("scripts/main.py", "util.helper(n)", "helper")),
        "scripts.util.helper"
    );
    // `lib/other.py` has no sibling `util`: the import names something
    // outside the repo, never `scripts.util`.
    not_bound(&at("lib/other.py", "util.helper(1)", "helper"));
    // A package's sibling is not importable by its bare name.
    not_bound(&at("pkg/main.py", "dumps(n)", "dumps"));
}
