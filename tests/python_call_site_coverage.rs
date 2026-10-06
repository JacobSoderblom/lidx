//! No Python call site is silently dropped. Every tree-sitter `call` node
//! of a file must come out of extraction as exactly one `CALLS` edge
//! carrying its lowered `PyCallSite`.
//!
//! There are no documented non-CALLS cases: a call that also yields an
//! HTTP / gRPC / channel / config / route edge (decorator routes, `requests.get`,
//! `os.getenv`) gets that edge in addition to its `CALLS` edge, and a call
//! in a position the walk once skipped (decorator, default value, parameter
//! or return annotation, class base list, a class defined inside a
//! function) is walked too. Conversely every `CALLS` edge of a Python file
//! comes from a call node and carries a site.
//!
//! The expected set is read straight off each file's syntax tree, parsed
//! independently of the extractor; the actual set off the extracted edges.
//! Keys are `(start line, end line, argument count)`, compared as multisets
//! (two identical calls on one line must both appear).

use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::python::PythonExtractor;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tree_sitter::{Node, Parser};

type Key = (i64, i64, u32);

fn py_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in rd.flatten() {
        let p = entry.path();
        if p.is_dir() {
            if p.file_name().is_some_and(|n| n == ".lidx" || n == ".git") {
                continue;
            }
            py_files(&p, out);
        } else if p.extension().is_some_and(|e| e == "py") {
            out.push(p);
        }
    }
}

fn walk(node: Node<'_>, out: &mut BTreeMap<Key, usize>) {
    if node.kind() == "call" {
        let args = node.child_by_field_name("arguments").unwrap();
        let count = if args.kind() == "argument_list" {
            let mut cursor = args.walk();
            args.named_children(&mut cursor)
                .filter(|a| a.kind() != "comment")
                .count() as u32
        } else {
            1
        };
        *out.entry((
            node.start_position().row as i64 + 1,
            node.end_position().row as i64 + 1,
            count,
        ))
        .or_default() += 1;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk(child, out);
    }
}

fn check(src: &str, module: &str, path: &str, label: &str) {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_python::LANGUAGE.into())
        .unwrap();
    let tree = parser.parse(src, None).unwrap();
    let mut want = BTreeMap::new();
    walk(tree.root_node(), &mut want);

    let mut x = PythonExtractor::new().unwrap();
    x.set_current_path(path);
    let out = x.extract(src, module).unwrap();
    let mut got: BTreeMap<Key, usize> = BTreeMap::new();
    for e in out.edges.iter().filter(|e| e.kind == "CALLS") {
        let site = e
            .py_site
            .as_ref()
            .unwrap_or_else(|| panic!("{label}: CALLS edge {:?} has no py_site", e.detail));
        *got.entry((
            e.evidence_start_line.unwrap(),
            e.evidence_end_line.unwrap(),
            site.arg_count,
        ))
        .or_default() += 1;
    }
    assert_eq!(
        want, got,
        "{label}: call nodes (left) vs CALLS edges with a site (right)"
    );
}

#[test]
fn every_call_of_the_fixture_files_has_a_site() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut files = Vec::new();
    py_files(&fixtures, &mut files);
    assert!(!files.is_empty());
    for path in files {
        let src = std::fs::read_to_string(&path).unwrap();
        let rel = path.strip_prefix(&fixtures).unwrap().to_string_lossy();
        let module = rel.trim_end_matches(".py").replace('/', ".");
        check(&src, &module, &rel, &format!("tests/fixtures/{rel}"));
    }
}

/// Shapes the legacy walk skipped or that produce side edges.
const SHAPES: &str = r#"
import os
import requests
import httpx
from fastapi import FastAPI, Depends
from typing import Annotated

app = FastAPI()
bus = make_bus()
TOPIC = "orders"


@app.get("/items")
def list_items(db=Depends(get_db), q: Annotated[int, Query()] = lookup()) -> Resp():
    r = requests.get("http://x/y")
    key = os.getenv("KEY")
    return r.json()


@bus.subscribe(topic=TOPIC)
async def on_order(msg):
    await publish(msg)


@decorate(arg())
@other
class Handler(make_base(), metaclass=Meta()):
    default = build()

    @property
    def p(self):
        return super().p.strip()

    @staticmethod
    def s(a=helper()):
        return a

    def m(self, x):
        def inner(y=dflt()):
            return y.go()

        class Local(lbase()):
            attr = lattr()

            def lm(self):
                return self.lcall()

        f = lambda z: z.lam()
        g = [i.comp() for i in x if i.ok()]
        h = {k: v.val() for k, v in x.items()}
        if (w := compute()):
            w.use()
        print(f"{x.fmt()} and {y_fmt()}")
        with open("p") as fh, httpx.Client() as c:
            fh.read(); c.get("u")
        return inner(1)(2).chain().more(x for x in range(3))


def make(*args, **kwargs):
    return ident(ident(ident(1)))
make(
    1,
    2,
)
make(1); make(2)
client = httpx.AsyncClient()
"#;

#[test]
fn every_call_of_the_awkward_shapes_has_a_site() {
    check(SHAPES, "demo.shapes", "demo/shapes.py", "SHAPES");
}

#[test]
fn route_and_http_calls_keep_their_calls_edge_alongside() {
    let mut x = PythonExtractor::new().unwrap();
    x.set_current_path("demo/shapes.py");
    let out = x.extract(SHAPES, "demo.shapes").unwrap();
    let kinds: std::collections::BTreeSet<_> = out.edges.iter().map(|e| e.kind.as_str()).collect();
    for kind in [
        "HTTP_ROUTE",
        "HTTP_CALL",
        "CHANNEL_SUBSCRIBE",
        "CONFIG_READ",
    ] {
        assert!(kinds.contains(kind), "fixture should produce {kind}");
    }
    let calls_on = |line: i64| {
        out.edges
            .iter()
            .filter(|e| e.kind == "CALLS" && e.evidence_start_line == Some(line))
            .count()
    };
    let line_of =
        |needle: &str| SHAPES.lines().position(|l| l.contains(needle)).unwrap() as i64 + 1;
    // `requests.get(..)` is an HTTP_CALL and still a call.
    assert_eq!(calls_on(line_of("requests.get(")), 1);
    assert_eq!(calls_on(line_of("os.getenv(")), 1);
    // The route decorator's own call is a call; its route edge is separate.
    assert_eq!(calls_on(line_of("@app.get")), 1);
}
