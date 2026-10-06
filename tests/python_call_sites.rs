//! Lowering of Python call nodes to `PyCallSite`: locals inlined, `self`
//! and `super()` tracked, unfollowable values explicit `Unknown(..)`.

use lidx::indexer::extract::{EdgeInput, LanguageExtractor};
use lidx::indexer::python::PythonExtractor;
use lidx::indexer::python_expr::{PyCallSite, PyExpr, Why};
use lidx::indexer::python_types::PyTypeRef;

fn edges(src: &str) -> Vec<EdgeInput> {
    let mut x = PythonExtractor::new().unwrap();
    x.set_current_path("app/mod.py");
    x.extract(src, "app.mod")
        .unwrap()
        .edges
        .into_iter()
        .filter(|e| e.kind == "CALLS")
        .collect()
}

/// The site of the call on the (single) source line containing `needle`
/// whose callee ends in `callee_name`.
fn site(src: &str, needle: &str, callee_name: &str) -> PyCallSite {
    let line = src
        .lines()
        .position(|l| l.contains(needle))
        .unwrap_or_else(|| panic!("no line with {needle:?}")) as i64
        + 1;
    let found: Vec<_> = edges(src)
        .into_iter()
        .filter(|e| e.evidence_start_line == Some(line))
        .filter(|e| {
            let t = e.target_qualname.as_deref().unwrap_or("");
            let d = e.detail.as_deref().unwrap_or("");
            t.ends_with(callee_name) || d.ends_with(callee_name)
        })
        .collect();
    assert_eq!(found.len(), 1, "calls on line {line} ending {callee_name}");
    *found[0].py_site.clone().expect("CALLS edge has a site")
}

fn n(s: &str) -> PyExpr {
    PyExpr::Name(s.into())
}
fn attr(e: PyExpr, a: &str) -> PyExpr {
    PyExpr::Attr(Box::new(e), a.into())
}
fn call(e: PyExpr) -> PyExpr {
    PyExpr::Call(Box::new(e))
}
fn unknown(w: Why) -> PyExpr {
    PyExpr::Unknown(w)
}
fn ty(path: &[&str]) -> PyTypeRef {
    PyTypeRef::Name {
        path: path.iter().map(|s| s.to_string()).collect(),
        args: vec![],
    }
}

#[test]
fn local_assigned_from_a_call_is_inlined() {
    let src = "import httpx\n\ndef f():\n    client = httpx.Client()\n    client.build_request()\n";
    let s = site(src, "client.build_request", "build_request");
    assert_eq!(
        s.callee,
        attr(call(attr(n("httpx"), "Client")), "build_request")
    );
    assert_eq!(s.arg_count, 0);
}

#[test]
fn with_target_is_an_enter_of_its_context_manager() {
    let src = "import httpx\n\ndef f():\n    with httpx.Client() as c:\n        c.get(1, a=2)\n";
    let s = site(src, "c.get", "get");
    assert_eq!(
        s.callee,
        attr(
            PyExpr::Enter {
                value: Box::new(call(attr(n("httpx"), "Client"))),
                is_async: false
            },
            "get"
        )
    );
    assert_eq!(s.arg_count, 2);
}

#[test]
fn async_with_target_is_an_async_enter() {
    let src = "async def f():\n    async with X() as c:\n        c.get()\n";
    let s = site(src, "c.get", "get");
    assert_eq!(
        s.callee,
        attr(
            PyExpr::Enter {
                value: Box::new(call(n("X"))),
                is_async: true
            },
            "get"
        )
    );
}

#[test]
fn awaited_result_keeps_the_await() {
    let src = "import httpx\n\nasync def f(client: httpx.AsyncClient):\n    response = await client.request()\n    response.raise_for_status()\n";
    let s = site(src, "response.raise_for_status", "raise_for_status");
    assert_eq!(
        s.callee,
        attr(
            PyExpr::Await(Box::new(call(attr(
                PyExpr::Declared(ty(&["httpx", "AsyncClient"])),
                "request"
            )))),
            "raise_for_status"
        )
    );
}

#[test]
fn parenthesized_await_is_followed() {
    let src = "async def f():\n    return (await x()).y()\n";
    let s = site(src, "(await x())", "y");
    assert_eq!(s.callee, attr(PyExpr::Await(Box::new(call(n("x")))), "y"));
}

#[test]
fn self_attribute_chains_start_at_the_class() {
    let src = "class C:\n    def m(self):\n        self.cookies.extract_cookies()\n        self.response.headers.multi_items()\n";
    let self_ref = PyExpr::SelfRef {
        class: "app.mod.C".into(),
        cls: false,
    };
    assert_eq!(
        site(src, "extract_cookies", "extract_cookies").callee,
        attr(attr(self_ref.clone(), "cookies"), "extract_cookies")
    );
    assert_eq!(
        site(src, "multi_items", "multi_items").callee,
        attr(attr(attr(self_ref, "response"), "headers"), "multi_items")
    );
}

#[test]
fn cls_in_a_classmethod_is_a_cls_ref() {
    let src = "class C:\n    @classmethod\n    def make(cls):\n        cls.build()\n    @staticmethod\n    def s(self):\n        self.build()\n";
    assert_eq!(
        site(src, "cls.build", "build").callee,
        attr(
            PyExpr::SelfRef {
                class: "app.mod.C".into(),
                cls: true
            },
            "build"
        )
    );
    // A static method's first parameter is an ordinary, untyped parameter.
    assert_eq!(
        site(src, "self.build", "build").callee,
        attr(unknown(Why::Untracked), "build")
    );
}

#[test]
fn annotated_parameter_is_declared() {
    let src = "import httpx\n\ndef f(request: httpx.Request):\n    request.headers.get_list()\n";
    assert_eq!(
        site(src, "get_list", "get_list").callee,
        attr(
            attr(PyExpr::Declared(ty(&["httpx", "Request"])), "headers"),
            "get_list"
        )
    );
}

#[test]
fn annotation_wins_over_the_assigned_value() {
    let src = "def f():\n    x: Foo = make()\n    x.run()\n";
    assert_eq!(
        site(src, "x.run", "run").callee,
        attr(PyExpr::Declared(ty(&["Foo"])), "run")
    );
}

#[test]
fn chained_calls_nest() {
    let src = "def f(p):\n    evolve(\"x\").apply_plan(p).generate()\n";
    let s = site(src, "evolve", "generate");
    assert_eq!(
        s.callee,
        attr(call(attr(call(n("evolve")), "apply_plan")), "generate")
    );
    let inner = site(src, "evolve", "apply_plan");
    assert_eq!(inner.callee, attr(call(n("evolve")), "apply_plan"));
    assert_eq!(inner.arg_count, 1);
    assert_eq!(site(src, "evolve", "evolve").callee, n("evolve"));
}

#[test]
fn super_call_is_a_super_of_the_class() {
    let src = "class C(B):\n    def __init__(self):\n        super().__init__()\n";
    let s = site(src, "super().__init__", "__init__");
    assert_eq!(
        s.callee,
        attr(
            PyExpr::Super {
                class: "app.mod.C".into()
            },
            "__init__"
        )
    );
    // The `super()` call itself is a call too.
    assert_eq!(site(src, "super().__init__", "super").callee, n("super"));
}

#[test]
fn local_assigned_several_times_agrees_or_is_unknown() {
    let src =
        "def f():\n    x = A()\n    x = B()\n    x.run()\n    y = A()\n    y += 1\n    y.run2()\n";
    // Plain assignments are all kept: evaluation binds only if they agree.
    assert_eq!(
        site(src, "x.run", "run").callee,
        attr(PyExpr::Agree(vec![call(n("A")), call(n("B"))]), "run")
    );
    // An augmented assignment is a rebinding nothing can follow.
    assert_eq!(
        site(src, "y.run2", "run2").callee,
        attr(unknown(Why::Rebound), "run2")
    );
}

#[test]
fn loop_lambda_comprehension_nested_def_and_unpacking_are_unknown() {
    let src = "\
def f(xs, untyped):
    for x in xs:
        x.loop_call()
    g = lambda p: p.lambda_call()
    [v.comp_call() for v in xs]
    def inner():
        pass
    inner()
    a, b = xs
    a.unpack_call()
    untyped.param_call()
    try:
        pass
    except E as err:
        err.except_call()
";
    let c = |needle: &str, name: &str| site(src, needle, name).callee;
    assert_eq!(
        c("x.loop_call", "loop_call"),
        attr(unknown(Why::LoopVar), "loop_call")
    );
    assert_eq!(
        c("p.lambda_call", "lambda_call"),
        attr(unknown(Why::Lambda), "lambda_call")
    );
    assert_eq!(
        c("v.comp_call", "comp_call"),
        attr(unknown(Why::Comprehension), "comp_call")
    );
    assert_eq!(c("    inner()", "inner"), unknown(Why::Complex));
    assert_eq!(
        c("a.unpack_call", "unpack_call"),
        attr(unknown(Why::Complex), "unpack_call")
    );
    assert_eq!(
        c("untyped.param_call", "param_call"),
        attr(unknown(Why::Untracked), "param_call")
    );
    assert_eq!(
        c("err.except_call", "except_call"),
        attr(unknown(Why::Complex), "except_call")
    );
}

#[test]
fn walrus_global_and_nonlocal() {
    let src = "\
def f():
    global G
    nonlocal_free = 1
    if (w := make()):
        w.walrus_call()
    G.global_call()
";
    assert_eq!(
        site(src, "w.walrus_call", "walrus_call").callee,
        attr(call(n("make")), "walrus_call")
    );
    assert_eq!(
        site(src, "G.global_call", "global_call").callee,
        attr(unknown(Why::Global), "global_call")
    );
}

#[test]
fn nested_def_sees_closure_locals() {
    let src = "import httpx\n\ndef f():\n    c = httpx.Client()\n    def g():\n        c.get()\n";
    assert_eq!(
        site(src, "c.get", "get").callee,
        attr(call(attr(n("httpx"), "Client")), "get")
    );
}

#[test]
fn module_level_single_binding_is_inlined() {
    let src =
        "import httpx\nclient = httpx.Client()\nclient.get()\nother = 1\nother = 2\nother.m()\n";
    assert_eq!(
        site(src, "client.get", "get").callee,
        attr(call(attr(n("httpx"), "Client")), "get")
    );
    // Both assignments are literals: they collapse to one unknown value.
    assert_eq!(
        site(src, "other.m", "m").callee,
        attr(unknown(Why::Literal), "m")
    );
}

#[test]
fn module_level_defs_and_imports_stay_free_names() {
    let src = "import os\nfrom .sib import helper\ndef util():\n    pass\nutil()  # a\nhelper()  # b\nos.getcwd()  # c\n";
    assert_eq!(site(src, "# a", "util").callee, n("util"));
    assert_eq!(site(src, "# b", "helper").callee, n("helper"));
    assert_eq!(site(src, "# c", "getcwd").callee, attr(n("os"), "getcwd"));
}

#[test]
fn functions_leave_module_names_free() {
    let src = "import httpx\nclient = httpx.Client()\n\ndef f():\n    client.get()\n";
    assert_eq!(
        site(src, "client.get", "get").callee,
        attr(n("client"), "get")
    );
}

#[test]
fn global_statement_unbinds_module_inlining() {
    let src = "client = A()\n\ndef reset():\n    global client\n    client = B()\n\nclient.get()\n";
    assert_eq!(
        site(src, "client.get", "get").callee,
        attr(unknown(Why::Global), "get")
    );
}

#[test]
fn function_local_import_binds_an_imported_name() {
    let src = "def f():\n    import a.b as x\n    x.run()\n    from .m import F\n    F.go()\n    from p import q\n    q.go2()\n    import os\n    import os.path\n    os.getcwd()\n";
    assert_eq!(
        site(src, "x.run", "run").callee,
        attr(PyExpr::Imported("a.b".into()), "run")
    );
    assert_eq!(
        site(src, "F.go", "go").callee,
        attr(PyExpr::Imported("app.m.F".into()), "go")
    );
    assert_eq!(
        site(src, "q.go2", "go2").callee,
        attr(PyExpr::Imported("p.q".into()), "go2")
    );
    assert_eq!(
        site(src, "os.getcwd", "getcwd").callee,
        attr(PyExpr::Imported("os".into()), "getcwd")
    );
}

#[test]
fn function_local_import_shadows_module_scope_only_there() {
    let src = "import a\n\ndef f():\n    import b as a\n    a.one()\n\ndef g():\n    a.two()\n";
    assert_eq!(
        site(src, "a.one", "one").callee,
        attr(PyExpr::Imported("b".into()), "one")
    );
    assert_eq!(site(src, "a.two", "two").callee, attr(n("a"), "two"));
}

#[test]
fn subscripts_and_literals_are_unknown() {
    let src = "def f(xs):\n    xs[0].sub()\n    \"abc\".join(xs)\n    [1].pop()\n";
    assert_eq!(
        site(src, "xs[0].sub", "sub").callee,
        attr(unknown(Why::Subscript), "sub")
    );
    assert_eq!(
        site(src, "\"abc\"", "join").callee,
        attr(unknown(Why::Literal), "join")
    );
    assert_eq!(
        site(src, "[1].pop", "pop").callee,
        attr(unknown(Why::Literal), "pop")
    );
}

#[test]
fn deep_chains_are_capped() {
    let src = "def f(x):\n    a.b.c.d.e.f.g.h.i.j.k.l()\n";
    let mut e = site(src, "a.b.c", "l").callee;
    let mut depth = 0;
    while let PyExpr::Attr(inner, _) = e {
        e = *inner;
        depth += 1;
    }
    assert_eq!(e, unknown(Why::Depth));
    assert!(depth <= 9, "kept {depth} levels");
}

#[test]
fn long_local_chains_are_capped() {
    let mut src = String::from("def f():\n    x0 = make()\n");
    for i in 1..14 {
        src.push_str(&format!("    x{i} = x{}.next()\n", i - 1));
    }
    src.push_str("    x13.last()\n");
    let s = site(&src, "x13.last", "last");
    let mut e = s.callee;
    let mut found_depth = false;
    loop {
        match e {
            PyExpr::Attr(inner, _) | PyExpr::Call(inner) => e = *inner,
            PyExpr::Unknown(Why::Depth) => {
                found_depth = true;
                break;
            }
            _ => break,
        }
    }
    assert!(found_depth);
}

#[test]
fn argument_counts_cover_keywords_splats_and_generators() {
    let src = "def f(a, b):\n    g(1, *a, k=2, **b)\n    h(x for x in a)\n    i()\n";
    assert_eq!(site(src, "g(1", "g").arg_count, 4);
    assert_eq!(site(src, "h(x", "h").arg_count, 1);
    assert_eq!(site(src, "i()", "i").arg_count, 0);
}

#[test]
fn json_shape_is_pinned() {
    let s = PyCallSite {
        callee: attr(call(attr(n("httpx"), "Client")), "get"),
        arg_count: 2,
    };
    assert_eq!(
        serde_json::to_string(&s).unwrap(),
        r#"{"f":{"a":[{"c":{"a":[{"n":"httpx"},"Client"]}},"get"]},"n":2}"#
    );
    let s = PyCallSite {
        callee: attr(PyExpr::Imported("a.b".into()), "run"),
        arg_count: 0,
    };
    assert_eq!(
        serde_json::to_string(&s).unwrap(),
        r#"{"f":{"a":[{"i":"a.b"},"run"]},"n":0}"#
    );
    let back: PyCallSite = serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
    assert_eq!(back, s);
}

#[test]
fn calls_outside_function_bodies_are_lowered_in_the_enclosing_scope() {
    let src = "\
import httpx
shared = httpx.Client()

@decorate(shared.get())
def f(a=default_value(), b: Annotated[int, Marker()] = 1) -> Ret(): ...

class C(make_base(), metaclass=Meta()):
    attr = build()
    other = attr.go()
";
    assert_eq!(
        site(src, "@decorate", "get").callee,
        attr(call(attr(n("httpx"), "Client")), "get")
    );
    for (needle, name) in [
        ("@decorate", "decorate"),
        ("default_value", "default_value"),
        ("Marker()", "Marker"),
        ("-> Ret", "Ret"),
        ("make_base", "make_base"),
        ("Meta()", "Meta"),
    ] {
        let _ = site(src, needle, name);
    }
    // Class-body names are scope-local: `attr` inlines to its value.
    assert_eq!(
        site(src, "attr.go", "go").callee,
        attr(call(n("build")), "go")
    );
}

#[test]
fn local_class_bodies_are_walked() {
    let src = "def f():\n    class Local(base()):\n        def m(self):\n            self.go()\n        x = made()\n";
    let _ = site(src, "base()", "base");
    let _ = site(src, "made()", "made");
    assert_eq!(
        site(src, "self.go", "go").callee,
        attr(unknown(Why::Untracked), "go")
    );
}
