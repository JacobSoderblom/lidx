//! Declared Python types (`PyFileDecls`): extraction, payload shape, and
//! persistence in `py_decls` through real index runs.

mod common;

use lidx::db::Db;
use lidx::indexer::Indexer;
use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::python::PythonExtractor;
use lidx::indexer::python_expr::{PyExpr, Why};
use lidx::indexer::python_types::{
    PyAttrSource, PyFileDecls, PyFuncKind, PyImport, PyTypeRef, PyVarDecl,
};

fn decls_at(src: &str, module: &str, path: &str) -> PyFileDecls {
    let mut x = PythonExtractor::new().unwrap();
    x.set_current_path(path);
    x.extract(src, module).unwrap().py_decls.unwrap()
}

fn decls(src: &str) -> PyFileDecls {
    decls_at(src, "app.mod", "app/mod.py")
}

fn name(path: &[&str]) -> PyTypeRef {
    PyTypeRef::Name {
        path: path.iter().map(|s| s.to_string()).collect(),
        args: vec![],
    }
}

fn imp(bound: &str, target: &str, member: bool) -> PyImport {
    PyImport {
        bound: bound.into(),
        target: target.into(),
        member,
    }
}

#[test]
fn imports_absolute_relative_aliased_and_dotted() {
    let d = decls(
        "import os\nimport a.b\nimport a.b as x\nfrom p.q import C as D, E\nfrom . import sib\nfrom .m import F\nfrom ..up import G\nfrom .. import H\n",
    );
    assert_eq!(d.module, "app.mod");
    assert!(!d.is_package);
    assert_eq!(
        d.imports,
        vec![
            imp("os", "os", false),
            imp("a", "a", false),
            imp("x", "a.b", false),
            imp("D", "p.q.C", true),
            imp("E", "p.q.E", true),
            imp("sib", "app.sib", true),
            imp("F", "app.m.F", true),
            // `..up` from package `app` is one level above it; the
            // existing absolutizer keeps the remainder. `from .. import H`
            // has nothing left and is dropped.
            imp("G", "up.G", true),
        ]
    );
}

#[test]
fn package_init_is_package_and_relative_imports_use_the_package() {
    let d = decls_at(
        "from . import sib\nfrom .m import F\n__all__ = [\"sib\", \"F\"]\n",
        "app",
        "app/__init__.py",
    );
    assert!(d.is_package);
    assert_eq!(d.module, "app");
    assert_eq!(
        d.imports,
        vec![imp("sib", "app.sib", true), imp("F", "app.m.F", true)]
    );
    assert_eq!(d.all, Some(vec!["sib".to_string(), "F".to_string()]));
}

#[test]
fn all_only_recorded_when_a_literal_of_strings() {
    assert_eq!(
        decls("__all__ = (\"a\", \"b\")\n").all,
        Some(vec!["a".into(), "b".into()])
    );
    assert_eq!(decls("__all__ = [n for n in dir()]\n").all, None);
    assert_eq!(decls("__all__ = [a, \"b\"]\n").all, None);
    assert_eq!(decls("x = 1\n").all, None);
}

#[test]
fn star_imports_are_absolutized_modules() {
    let d = decls("from m import *\nfrom .rel import *\n");
    assert_eq!(d.star_imports, vec!["m".to_string(), "app.rel".to_string()]);
    assert!(d.imports.is_empty());
}

#[test]
fn class_bases_dotted_and_generic() {
    let d = decls("class K(Base[T], httpx.Client, metaclass=Meta):\n    pass\nclass Plain: pass\n");
    let k = &d.classes[0];
    assert_eq!(k.qualname, "app.mod.K");
    assert_eq!(k.name, "K");
    assert_eq!(
        k.bases,
        vec![
            PyTypeRef::Name {
                path: vec!["Base".into()],
                args: vec![name(&["T"])]
            },
            name(&["httpx", "Client"]),
        ]
    );
    assert_eq!(k.start_line, 1);
    assert!(d.classes[1].bases.is_empty());
}

#[test]
fn nested_classes_get_dotted_qualnames() {
    let d = decls("class A:\n    class B:\n        def m(self): ...\n");
    let q: Vec<_> = d.classes.iter().map(|c| c.qualname.as_str()).collect();
    assert_eq!(q, vec!["app.mod.A", "app.mod.A.B"]);
    assert_eq!(d.functions[0].qualname, "app.mod.A.B.m");
    assert_eq!(d.functions[0].owner, Some(1));
}

fn attr<'a>(d: &'a PyFileDecls, class: usize, n: &str) -> &'a [PyAttrSource] {
    &d.classes[class]
        .attrs
        .iter()
        .find(|a| a.name == n)
        .unwrap_or_else(|| panic!("no attr {n}"))
        .sources
}

#[test]
fn class_level_annotations_and_values() {
    let d = decls("class K:\n    z: int = 3\n    w: \"Bar\"\n    y = Foo()\n");
    assert_eq!(attr(&d, 0, "z"), &[PyAttrSource::Declared(name(&["int"]))]);
    assert_eq!(attr(&d, 0, "w"), &[PyAttrSource::Declared(name(&["Bar"]))]);
    assert_eq!(
        attr(&d, 0, "y"),
        &[PyAttrSource::Value(PyExpr::Call(Box::new(PyExpr::Name(
            "Foo".into()
        ))))]
    );
}

#[test]
fn self_attrs_from_init_and_any_other_method() {
    let d = decls(
        "class K:\n    def __init__(self, repo: Repo, raw):\n        self.repo = repo\n        self.raw = raw\n        self.c = Foo()\n        self.t: Baz = make()\n    def setup(self):\n        self.late = Late()\n        self.chain = self.c.inner\n",
    );
    assert_eq!(
        attr(&d, 0, "repo"),
        &[PyAttrSource::Value(PyExpr::Declared(name(&["Repo"])))]
    );
    assert_eq!(
        attr(&d, 0, "raw"),
        &[PyAttrSource::Value(PyExpr::Unknown(Why::Untracked))]
    );
    assert_eq!(
        attr(&d, 0, "c"),
        &[PyAttrSource::Value(PyExpr::Call(Box::new(PyExpr::Name(
            "Foo".into()
        ))))]
    );
    assert_eq!(attr(&d, 0, "t"), &[PyAttrSource::Declared(name(&["Baz"]))]);
    assert_eq!(
        attr(&d, 0, "late"),
        &[PyAttrSource::Value(PyExpr::Call(Box::new(PyExpr::Name(
            "Late".into()
        ))))]
    );
    assert_eq!(
        attr(&d, 0, "chain"),
        &[PyAttrSource::Value(PyExpr::Attr(
            Box::new(PyExpr::Attr(
                Box::new(PyExpr::SelfRef {
                    class: "app.mod.K".into(),
                    cls: false
                }),
                "c".into()
            )),
            "inner".into()
        ))]
    );
}

#[test]
fn property_getter_contributes_its_return_annotation() {
    let d = decls(
        "import functools\nclass K:\n    @property\n    def pp(self) -> Dict[str, \"X\"]: ...\n    @functools.cached_property\n    def cp(self) -> Cfg: ...\n    @pp.setter\n    def pp(self, v): ...\n",
    );
    assert_eq!(
        attr(&d, 0, "pp"),
        &[PyAttrSource::Property(PyTypeRef::Name {
            path: vec!["Dict".into()],
            args: vec![name(&["str"]), name(&["X"])]
        })]
    );
    assert_eq!(attr(&d, 0, "cp"), &[PyAttrSource::Property(name(&["Cfg"]))]);
    let kinds: Vec<_> = d
        .functions
        .iter()
        .map(|f| (f.name.as_str(), f.kind))
        .collect();
    // The setter is not a lookup target.
    assert_eq!(
        kinds,
        vec![("pp", PyFuncKind::Property), ("cp", PyFuncKind::Property)]
    );
}

#[test]
fn overloads_async_and_kinds_are_flagged() {
    let d = decls(
        "class K:\n    @overload\n    def f(self, a: int) -> int: ...\n    @typing.overload\n    def f(self, a: str) -> str: ...\n    def f(self, a): return a\n    async def g(self): ...\n    @staticmethod\n    def s(*args, **kw): ...\n    @classmethod\n    def c(cls) -> \"K\": ...\nasync def top(): ...\ndef plain(): ...\n",
    );
    let f: Vec<_> = d.functions.iter().collect();
    assert_eq!(f[0].qualname, "app.mod.K.f");
    assert!(f[0].is_overload && f[1].is_overload && !f[2].is_overload);
    assert_eq!(f[0].params[1].ty, Some(name(&["int"])));
    assert_eq!(f[0].returns, Some(name(&["int"])));
    assert!(f[3].is_async && f[3].name == "g" && !f[3].is_overload);
    assert_eq!(f[4].kind, PyFuncKind::StaticMethod);
    assert_eq!(
        f[4].params
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>(),
        vec!["args", "kw"]
    );
    assert_eq!(f[5].kind, PyFuncKind::ClassMethod);
    assert_eq!(f[5].returns, Some(name(&["K"])));
    assert_eq!(f[6].kind, PyFuncKind::Function);
    assert!(f[6].is_async && f[6].owner.is_none());
    assert_eq!(f[7].kind, PyFuncKind::Function);
    assert!(!f[7].is_async);
    assert_eq!(f[2].kind, PyFuncKind::Method);
    assert_eq!(f[2].owner, Some(0));
}

#[test]
fn self_annotation_is_captured() {
    let d = decls("class K:\n    def __enter__(self: T) -> T: ...\n");
    let f = &d.functions[0];
    assert_eq!(f.params[0].name, "self");
    assert_eq!(f.params[0].ty, Some(name(&["T"])));
    assert_eq!(f.returns, Some(name(&["T"])));
}

#[test]
fn module_vars_single_binding_only() {
    let d = decls(
        "import x\nA: int = 4\nB = Foo()\nC = 1\nC = 2\nD: \"Bar\"\ntry:\n    import y\nexcept ImportError:\n    y = None\n",
    );
    let var = |n: &str| -> &PyVarDecl { d.vars.iter().find(|v| v.name == n).unwrap() };
    assert_eq!(var("A").ty, Some(name(&["int"])));
    assert_eq!(
        var("B").value,
        Some(PyExpr::Call(Box::new(PyExpr::Name("Foo".into()))))
    );
    // Rebinding: no value.
    assert_eq!(var("C").value, None);
    assert_eq!(var("D").ty, Some(name(&["Bar"])));
    assert_eq!(var("D").value, None);
    // Import then assignment is a rebinding too.
    assert_eq!(var("y").value, None);
}

#[test]
fn optional_and_union_normalize() {
    let d = decls(
        "def f(a: Optional[int], b: int | None, c: Union[A, None], d: Union[A, B], e: \"Foo | None\", f: typing.Optional[X], g: None, h: A | B | None, i: List[Optional[int]]): ...\n",
    );
    let t: Vec<_> = d.functions[0]
        .params
        .iter()
        .map(|p| p.ty.clone().unwrap())
        .collect();
    assert_eq!(t[0], name(&["int"]));
    assert_eq!(t[1], name(&["int"]));
    assert_eq!(t[2], name(&["A"]));
    assert_eq!(t[3], PyTypeRef::Union(vec![name(&["A"]), name(&["B"])]));
    assert_eq!(t[4], name(&["Foo"]));
    assert_eq!(t[5], name(&["X"]));
    assert_eq!(t[6], PyTypeRef::None);
    assert_eq!(t[7], PyTypeRef::Union(vec![name(&["A"]), name(&["B"])]));
    assert_eq!(
        t[8],
        PyTypeRef::Name {
            path: vec!["List".into()],
            args: vec![name(&["int"])]
        }
    );
}

#[test]
fn string_forward_refs_parse_and_garbage_is_unknown() {
    let d = decls("def f(a: \"pkg.Foo[int]\", b: \"not a type!\", c: \"\"): ...\n");
    let t: Vec<_> = d.functions[0]
        .params
        .iter()
        .map(|p| p.ty.clone().unwrap())
        .collect();
    assert_eq!(
        t[0],
        PyTypeRef::Name {
            path: vec!["pkg".into(), "Foo".into()],
            args: vec![name(&["int"])]
        }
    );
    assert_eq!(t[1], PyTypeRef::Unknown);
    assert_eq!(t[2], PyTypeRef::Unknown);
}

#[test]
fn payload_round_trips_and_hash_tracks_content() {
    let a = decls("class K:\n    x: int\n");
    let b = PyFileDecls::from_payload(&a.to_payload()).unwrap();
    assert_eq!(a, b);
    assert_eq!(a.hash(), b.hash());
    assert_ne!(a.hash(), decls("class K:\n    x: str\n").hash());
}

#[test]
fn pyexpr_json_shape_is_pinned() {
    let e = PyExpr::Await(Box::new(PyExpr::Call(Box::new(PyExpr::Attr(
        Box::new(PyExpr::SelfRef {
            class: "m.K".into(),
            cls: false,
        }),
        "run".into(),
    )))));
    assert_eq!(
        serde_json::to_string(&e).unwrap(),
        r#"{"w":{"c":{"a":[{"s":{"c":"m.K"}},"run"]}}}"#
    );
    let cases: Vec<(PyExpr, &str)> = vec![
        (PyExpr::Name("x".into()), r#"{"n":"x"}"#),
        (PyExpr::Declared(name(&["T"])), r#"{"d":{"N":{"p":["T"]}}}"#),
        (
            PyExpr::SelfRef {
                class: "m.K".into(),
                cls: true,
            },
            r#"{"s":{"c":"m.K","k":true}}"#,
        ),
        (
            PyExpr::Enter {
                value: Box::new(PyExpr::Name("f".into())),
                is_async: true,
            },
            r#"{"e":{"v":{"n":"f"},"a":true}}"#,
        ),
        (
            PyExpr::Super {
                class: "m.K".into(),
            },
            r#"{"u":{"c":"m.K"}}"#,
        ),
        (PyExpr::Unknown(Why::Rebound), r#"{"?":"rebound"}"#),
    ];
    for (e, json) in cases {
        assert_eq!(serde_json::to_string(&e).unwrap(), json);
        assert_eq!(serde_json::from_str::<PyExpr>(json).unwrap(), e);
    }
}

#[test]
fn lowering_covers_await_calls_and_unknowns() {
    let d = decls(
        "class K:\n    def __init__(self, s: Svc):\n        self.a = await s.get()\n        self.b = [1]\n        self.c = s.x[0]\n        self.d = lambda: 1\n        self.e = a if s else b\n        self.f = super().make()\n",
    );
    let v = |n: &str| match &attr(&d, 0, n)[0] {
        PyAttrSource::Value(e) => e.clone(),
        other => panic!("{other:?}"),
    };
    assert_eq!(
        v("a"),
        PyExpr::Await(Box::new(PyExpr::Call(Box::new(PyExpr::Attr(
            Box::new(PyExpr::Declared(name(&["Svc"]))),
            "get".into()
        )))))
    );
    assert_eq!(v("b"), PyExpr::Unknown(Why::Literal));
    assert_eq!(v("c"), PyExpr::Unknown(Why::Subscript));
    assert_eq!(v("d"), PyExpr::Unknown(Why::Lambda));
    assert_eq!(v("e"), PyExpr::Unknown(Why::Complex));
    assert_eq!(
        v("f"),
        PyExpr::Call(Box::new(PyExpr::Attr(
            Box::new(PyExpr::Super {
                class: "app.mod.K".into()
            }),
            "make".into()
        )))
    );
}

#[test]
fn locals_bound_once_are_inlined_and_rebound_are_unknown() {
    let d = decls(
        "class K:\n    def __init__(self):\n        once = Foo()\n        twice = A()\n        twice = B()\n        self.x = once\n        self.y = twice\n        for it in items:\n            self.z = it\n",
    );
    let v = |n: &str| match &attr(&d, 0, n)[0] {
        PyAttrSource::Value(e) => e.clone(),
        other => panic!("{other:?}"),
    };
    assert_eq!(v("x"), PyExpr::Call(Box::new(PyExpr::Name("Foo".into()))));
    assert_eq!(v("y"), PyExpr::Unknown(Why::Rebound));
    assert_eq!(v("z"), PyExpr::Unknown(Why::LoopVar));
}

// ---------------------------------------------------------------------------
// Persistence through real index runs
// ---------------------------------------------------------------------------

const PKG_INIT: &str = "from .core import Engine\n__all__ = [\"Engine\"]\n";
const CORE: &str = "class Engine:\n    def __init__(self, repo: Repo):\n        self.repo = repo\n";
const OTHER: &str = "import pkg\nclass User:\n    def run(self) -> None: ...\n";

fn files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("pkg/__init__.py", PKG_INIT),
        ("pkg/core.py", CORE),
        ("other.py", OTHER),
        ("note.md", "# not python\n"),
    ]
}

fn dump(db: &Db) -> Vec<(String, String)> {
    let gv = db.current_graph_version().unwrap();
    let conn = db.read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT f.path, c.payload FROM py_decls c JOIN files f ON f.id = c.file_id
             WHERE c.graph_version = ?1 AND (f.deleted_version IS NULL OR f.deleted_version > ?1)
             ORDER BY f.path",
        )
        .unwrap();
    stmt.query_map([gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn loaded(db: &Db) -> Vec<(String, PyFileDecls)> {
    let gv = db.current_graph_version().unwrap();
    let conn = db.read_conn().unwrap();
    db.py_decls(&conn, gv)
        .unwrap()
        .into_iter()
        .map(|f| (f.path, f.decls))
        .collect()
}

#[test]
fn index_persists_decls_per_python_file() {
    let (_tmp, root, db_path) = common::index_repo("lidx-py-decls-", &files());
    let indexer = Indexer::new(root, db_path).unwrap();
    let rows = dump(indexer.db());
    let paths: Vec<_> = rows.iter().map(|(p, _)| p.as_str()).collect();
    assert_eq!(paths, vec!["other.py", "pkg/__init__.py", "pkg/core.py"]);

    let table = loaded(indexer.db());
    let init = &table
        .iter()
        .find(|(p, _)| p == "pkg/__init__.py")
        .unwrap()
        .1;
    assert!(init.is_package);
    assert_eq!(init.module, "pkg");
    assert_eq!(init.imports, vec![imp("Engine", "pkg.core.Engine", true)]);
    let core = &table.iter().find(|(p, _)| p == "pkg/core.py").unwrap().1;
    assert_eq!(core.module, "pkg.core");
    assert_eq!(core.classes[0].qualname, "pkg.core.Engine");
}

#[test]
fn unchanged_files_carry_forward_and_edits_replace() {
    let (_tmp, root, db_path) = common::index_repo("lidx-py-decls-cf-", &files());
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    let before = dump(indexer.db());

    // Edit one file; the others are carried forward byte-identical.
    let new_core =
        "class Engine:\n    def __init__(self, repo: Other):\n        self.repo = repo\n";
    common::write_files(&root, &[("pkg/core.py", new_core)]);
    indexer.reindex().unwrap();
    let after = dump(indexer.db());
    assert_eq!(after.len(), 3);
    for (p, payload) in &after {
        let old = &before.iter().find(|(q, _)| q == p).unwrap().1;
        if p == "pkg/core.py" {
            assert_ne!(old, payload);
            assert!(payload.contains("Other"));
        } else {
            assert_eq!(old, payload, "{p} not carried forward");
        }
    }

    // An incremental sync of the edited file agrees with a fresh reindex.
    let newer = "class Engine:\n    def __init__(self, repo: Third):\n        self.repo = repo\n";
    common::write_files(&root, &[("pkg/core.py", newer)]);
    indexer
        .sync_rel_paths(&["pkg/core.py".to_string()])
        .unwrap();
    let synced = dump(indexer.db());
    let (_t2, r2, d2) = common::index_repo(
        "lidx-py-decls-fresh-",
        &[
            ("pkg/__init__.py", PKG_INIT),
            ("pkg/core.py", newer),
            ("other.py", OTHER),
            ("note.md", "# not python\n"),
        ],
    );
    let fresh = Indexer::new(r2, d2).unwrap();
    assert_eq!(synced, dump(fresh.db()));
}

#[test]
fn deleted_and_renamed_files_drop_their_rows() {
    let (_tmp, root, db_path) = common::index_repo("lidx-py-decls-del-", &files());
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();

    std::fs::remove_file(root.join("other.py")).unwrap();
    indexer.sync_rel_paths(&["other.py".to_string()]).unwrap();
    let paths: Vec<_> = dump(indexer.db()).into_iter().map(|(p, _)| p).collect();
    assert_eq!(paths, vec!["pkg/__init__.py", "pkg/core.py"]);

    // Rename: old path gone, new path present, via a full reindex.
    std::fs::rename(root.join("pkg/core.py"), root.join("pkg/engine.py")).unwrap();
    indexer.reindex().unwrap();
    let paths: Vec<_> = dump(indexer.db()).into_iter().map(|(p, _)| p).collect();
    assert_eq!(paths, vec!["pkg/__init__.py", "pkg/engine.py"]);
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let live = indexer.db().py_decls(&conn, gv).unwrap();
    assert_eq!(live.len(), 2);
}

// ---- unannotated `return self` ---------------------------------------------

fn returns_self_of(src: &str, name: &str) -> bool {
    let d = decls(src);
    d.functions
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("no function {name}"))
        .returns_self
}

#[test]
fn unannotated_function_returning_only_self_is_returns_self() {
    let src = "class C:
    def __enter__(self):
        return self

    def many(self, x):
        if x:
            return self
        return self
";
    assert!(returns_self_of(src, "__enter__"));
    assert!(returns_self_of(src, "many"));
}

#[test]
fn returns_self_traps() {
    let src = "class C:
    def mixed(self, x):
        if x:
            return self
        return 1

    def bare(self, x):
        if x:
            return self
        return

    def none_only(self):
        pass

    def other(self, o):
        return o

    def annotated(self) -> \"C\":
        return self

    def attr(self):
        return self.x

    def nested(self):
        def inner():
            return 1
        return self

    def nested_other(self):
        def inner():
            return self
        return 1

    def generator(self):
        yield 1
        return self

    @staticmethod
    def static(self):
        return self
";
    for n in [
        "mixed",
        "bare",
        "none_only",
        "other",
        "annotated",
        "attr",
        "nested_other",
        "generator",
        "static",
    ] {
        assert!(!returns_self_of(src, n), "{n} must not be returns_self");
    }
    // A return inside a nested def belongs to that def, not to the method.
    assert!(returns_self_of(src, "nested"));
}
