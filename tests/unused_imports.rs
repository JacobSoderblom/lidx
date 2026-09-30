//! Issue #116: `dead_symbols`' `unused_imports` reported every Python
//! import as unused. Root cause: `src/db/analytics.rs`'s old "is it used"
//! check compared a same-file `CALLS` edge's `target_qualname` text
//! *literally* against the IMPORTS edge's own `target_qualname` text --
//! but a Python call's `target_qualname` is the extractor's own local
//! guess (e.g. a bare `helper_used()` call becomes `<this
//! module>.helper_used`, not the imported symbol's real qualname), an
//! attribute use (`json.dumps(...)`) never matched a bare `import json`,
//! and an annotation-only use emits no `CALLS` edge at all.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;

fn temp_indexer(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-unused-imports-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    (tmp, indexer)
}

fn unused_import_qualnames(indexer: &mut Indexer) -> Vec<String> {
    let result = rpc::handle_method(indexer, "dead_symbols", serde_json::json!({})).unwrap();
    result["unused_imports"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|edge| edge["target_qualname"].as_str().map(String::from))
        .collect()
}

/// The dominant real-world case (issue #116's dpb evidence): a same-repo
/// import resolved via `target_symbol_id`, but used only through a call
/// whose own `target_qualname` is a *different* guessed string
/// (`resolve_call_target` qualifies a bare call with the *caller's* own
/// module, not the callee's). Both `helper_used` (used) and `sys` (never
/// referenced at all) live in the same file so the fix can't just stop
/// flagging everything.
#[test]
fn unused_imports_excludes_resolved_import_used_via_mismatched_call_guess() {
    let (_tmp, mut indexer) = temp_indexer(&[
        (
            "app.py",
            "import sys\nfrom pkg.utils import helper_used\n\n\ndef run():\n    return helper_used()\n",
        ),
        ("pkg/utils.py", "def helper_used():\n    return 1\n"),
    ]);
    indexer.reindex().unwrap();

    let unused = unused_import_qualnames(&mut indexer);
    assert!(
        !unused.iter().any(|q| q == "pkg.utils.helper_used"),
        "helper_used is called (via a bare, differently-guessed call site) and must NOT \
         appear in unused_imports, got: {unused:?}"
    );
    assert!(
        unused.iter().any(|q| q == "sys"),
        "sys is never referenced anywhere in the file and must appear in unused_imports, \
         got: {unused:?}"
    );
}

/// Attribute access on an unresolved external import (`json.dumps(...)`
/// uses `json`) never matched the old text-equality check at all.
#[test]
fn unused_imports_excludes_attribute_access_on_external_import() {
    let (_tmp, mut indexer) = temp_indexer(&[(
        "app.py",
        "import json\nimport sys\n\n\ndef dump():\n    return json.dumps({})\n",
    )]);
    indexer.reindex().unwrap();

    let unused = unused_import_qualnames(&mut indexer);
    assert!(
        !unused.iter().any(|q| q == "json"),
        "json is used via json.dumps(...) and must NOT appear in unused_imports, got: {unused:?}"
    );
    assert!(
        unused.iter().any(|q| q == "sys"),
        "sys is never referenced and must still appear in unused_imports, got: {unused:?}"
    );
}

/// A bare call on a name bound by an unresolved external import
/// (`from fastapi import FastAPI` + `FastAPI()`) resolves to an external
/// stub symbol whose guessed `target_qualname` text never matched the
/// IMPORTS edge's own reference text either.
#[test]
fn unused_imports_excludes_bare_call_on_external_import() {
    let (_tmp, mut indexer) = temp_indexer(&[(
        "app.py",
        "from fastapi import FastAPI\nimport sys\n\napp = FastAPI()\n",
    )]);
    indexer.reindex().unwrap();

    let unused = unused_import_qualnames(&mut indexer);
    assert!(
        !unused.iter().any(|q| q == "fastapi.FastAPI"),
        "FastAPI is used (via FastAPI()) and must NOT appear in unused_imports, got: {unused:?}"
    );
    assert!(
        unused.iter().any(|q| q == "sys"),
        "sys is never referenced and must still appear in unused_imports, got: {unused:?}"
    );
}

/// An annotation-only use (`x: Optional[int]`) never emits any edge at
/// all -- the fix must fall back to the symbol's own recorded `signature`
/// text.
#[test]
fn unused_imports_excludes_annotation_only_use() {
    let (_tmp, mut indexer) = temp_indexer(&[(
        "app.py",
        "from typing import Optional\nimport sys\n\n\ndef f(x: Optional[int]) -> None:\n    pass\n",
    )]);
    indexer.reindex().unwrap();

    let unused = unused_import_qualnames(&mut indexer);
    assert!(
        !unused.iter().any(|q| q == "typing.Optional"),
        "Optional is used only in a type annotation and must NOT appear in unused_imports, \
         got: {unused:?}"
    );
    assert!(
        unused.iter().any(|q| q == "sys"),
        "sys is never referenced and must still appear in unused_imports, got: {unused:?}"
    );
}

/// A module-level `__all__` re-export makes an otherwise-unreferenced
/// import "used" -- the common `pkg/__init__.py` re-export pattern from
/// issue #116's evidence.
#[test]
fn unused_imports_excludes_dunder_all_reexport() {
    let (_tmp, mut indexer) = temp_indexer(&[
        (
            "__init__.py",
            "from pkg.thing import thing\nimport sys\n\n__all__ = [\"thing\"]\n",
        ),
        ("pkg/thing.py", "def thing():\n    return 1\n"),
    ]);
    indexer.reindex().unwrap();

    let unused = unused_import_qualnames(&mut indexer);
    assert!(
        !unused.iter().any(|q| q == "pkg.thing.thing"),
        "thing is re-exported via __all__ and must NOT appear in unused_imports, got: {unused:?}"
    );
    assert!(
        unused.iter().any(|q| q == "sys"),
        "sys is never referenced and must still appear in unused_imports, got: {unused:?}"
    );
}

/// A genuinely unused, cleanly-resolved same-repo import (no call, no
/// attribute access, no annotation, no `__all__` entry) must still be
/// reported -- the fix must not over-correct into flagging nothing.
#[test]
fn unused_imports_still_flags_a_resolved_but_unreferenced_import() {
    let (_tmp, mut indexer) = temp_indexer(&[
        ("app.py", "from pkg.utils import helper_unused\n"),
        ("pkg/utils.py", "def helper_unused():\n    return 1\n"),
    ]);
    indexer.reindex().unwrap();

    let unused = unused_import_qualnames(&mut indexer);
    assert!(
        unused.iter().any(|q| q == "pkg.utils.helper_unused"),
        "helper_unused is imported but never referenced anywhere and must appear in \
         unused_imports, got: {unused:?}"
    );
}

/// The used-check must look for the name an import *binds*, not the
/// imported module's own last segment: `import numpy as np` binds `np`.
#[test]
fn unused_imports_uses_as_alias_of_module_import() {
    let (_tmp, mut indexer) = temp_indexer(&[(
        "app.py",
        "import numpy as np\nimport sys as system\n\n\ndef f():\n    return np.array([1])\n",
    )]);
    indexer.reindex().unwrap();

    let unused = unused_import_qualnames(&mut indexer);
    assert!(
        !unused.iter().any(|q| q == "numpy"),
        "np.array() uses the numpy alias, got: {unused:?}"
    );
    assert!(
        unused.iter().any(|q| q == "sys"),
        "`system` is never used, got: {unused:?}"
    );
}

/// `from x import y as z` binds `z`; a use of `z` counts, a bare `y`
/// elsewhere in the file (a different, unrelated name) must not.
#[test]
fn unused_imports_uses_as_alias_of_from_import() {
    let (_tmp, mut indexer) = temp_indexer(&[(
        "app.py",
        "from pkg.mod import used_orig as used_alias\nfrom pkg.mod import other_orig as other_alias\n\n\ndef f():\n    other_orig = 1\n    return used_alias() + other_orig\n",
    )]);
    indexer.reindex().unwrap();

    let unused = unused_import_qualnames(&mut indexer);
    assert!(
        !unused.iter().any(|q| q == "pkg.mod.used_orig"),
        "used_alias() is called, got: {unused:?}"
    );
    assert!(
        unused.iter().any(|q| q == "pkg.mod.other_orig"),
        "other_alias is never used (a local named other_orig is unrelated), got: {unused:?}"
    );
}

/// `import os.path` binds `os`, so `os.getcwd()` uses it even though the
/// imported target's trailing segment is `path`.
#[test]
fn unused_imports_dotted_import_binds_first_segment() {
    let (_tmp, mut indexer) = temp_indexer(&[(
        "app.py",
        "import os.path\nimport xml.dom\n\n\ndef f():\n    return os.getcwd()\n",
    )]);
    indexer.reindex().unwrap();

    let unused = unused_import_qualnames(&mut indexer);
    assert!(
        !unused.iter().any(|q| q == "os.path"),
        "os.getcwd() uses the `os` binding, got: {unused:?}"
    );
    assert!(
        unused.iter().any(|q| q == "xml.dom"),
        "xml is never used, got: {unused:?}"
    );
}

/// Parenthesized, multi-line `from x import (a, b as c)` binds `a` and `c`;
/// a use of the alias counts and an unused alias is still flagged.
#[test]
fn unused_imports_parenthesized_from_import_binds_aliases() {
    let (_tmp, mut indexer) = temp_indexer(&[(
        "app.py",
        "from pkg import (\n    json_util as ju,\n    other_orig as other_alias,\n)\n\n\ndef f():\n    return ju.x()\n",
    )]);
    indexer.reindex().unwrap();

    let unused = unused_import_qualnames(&mut indexer);
    assert!(
        !unused.iter().any(|q| q == "pkg.json_util"),
        "ju.x() uses the `ju` binding, got: {unused:?}"
    );
    assert!(
        unused.iter().any(|q| q == "pkg.other_orig"),
        "other_alias is never used, got: {unused:?}"
    );
}

// ---- Issue #242: value uses (no edge emitted) must count as usage ----

/// All six false-positive shapes from the report, plus one genuinely
/// unused import that must still be flagged.
#[test]
fn unused_imports_counts_python_value_uses_and_still_flags_unused() {
    let src = "\
import dlt
from a import RunLifeCycleState
from b import InputKind
from c import dpb_runtime
from d import advanced_dataproduct
from e import ALL_DATASOURCES
from f import ValidationOption
from g import PipelineSpec


@dlt.table(name=\"x\")
def go(obj):
    states = (
        RunLifeCycleState.TERMINATED,
        RunLifeCycleState.SKIPPED,
    )
    spec = make(kind=InputKind.Datasource)
    dpb_runtime.run(advanced_dataproduct)
    dpb_runtime.run(*ALL_DATASOURCES)
    obj.bk_dup(ValidationOption.Flag)
    return states, spec
";
    let (_tmp, mut indexer) = temp_indexer(&[("app.py", src)]);
    indexer.reindex().unwrap();
    let unused = unused_import_qualnames(&mut indexer);
    for used in [
        "dlt",
        "a.RunLifeCycleState",
        "b.InputKind",
        "c.dpb_runtime",
        "d.advanced_dataproduct",
        "e.ALL_DATASOURCES",
        "f.ValidationOption",
    ] {
        assert!(
            !unused.iter().any(|q| q == used),
            "{used} is used: {unused:?}"
        );
    }
    assert!(
        unused.iter().any(|q| q == "g.PipelineSpec"),
        "PipelineSpec is unused and must be flagged: {unused:?}"
    );
}

#[test]
fn unused_imports_counts_membership_bare_expr_and_fstring_uses() {
    let src = "\
from m import Kind, Flag, Other, Name

def f(x):
    if x in (Kind.A, Kind.B):
        pass
    Flag
    return f\"hello {Name.value!r} and {{Other}}\"
";
    let (_tmp, mut indexer) = temp_indexer(&[("app.py", src)]);
    indexer.reindex().unwrap();
    let unused = unused_import_qualnames(&mut indexer);
    for used in ["m.Kind", "m.Flag", "m.Name"] {
        assert!(
            !unused.iter().any(|q| q == used),
            "{used} is used: {unused:?}"
        );
    }
    // `{{Other}}` is an escaped literal, not an interpolation.
    assert!(unused.iter().any(|q| q == "m.Other"), "{unused:?}");
}

#[test]
fn unused_imports_token_scan_alias_and_whole_identifier() {
    let src = "\
import x as y
import p as q
from m import InputKind
from n import Foo

def f():
    InputKindOther = 1
    return y.go(), InputKindOther
";
    let (_tmp, mut indexer) = temp_indexer(&[("app.py", src)]);
    indexer.reindex().unwrap();
    let unused = unused_import_qualnames(&mut indexer);
    assert!(!unused.iter().any(|q| q == "x"), "y is used: {unused:?}");
    assert!(unused.iter().any(|q| q == "p"), "q unused: {unused:?}");
    assert!(
        unused.iter().any(|q| q == "m.InputKind"),
        "InputKindOther must not count as InputKind: {unused:?}"
    );
}

#[test]
fn unused_imports_ignores_names_in_import_block_strings_and_comments() {
    let src = "\
import os
import os.path as osp
from typing import (
    List,  # os used here? no
)
from pkg import Ghost

# Ghost in a comment isn't's a use
DOC = \"Ghost in a string\"
'''
Ghost in a docstring, os too
'''

def f():
    return List
";
    let (_tmp, mut indexer) = temp_indexer(&[("app.py", src)]);
    indexer.reindex().unwrap();
    let unused = unused_import_qualnames(&mut indexer);
    assert!(unused.iter().any(|q| q == "os"), "{unused:?}");
    assert!(unused.iter().any(|q| q == "pkg.Ghost"), "{unused:?}");
    assert!(!unused.iter().any(|q| q == "typing.List"), "{unused:?}");
}

/// Non-Python behavior is unchanged: a JS import mentioned only in a
/// string/comment is still reported (the token scan is Python-only).
#[test]
fn unused_imports_token_scan_is_python_only() {
    let (_tmp, mut indexer) = temp_indexer(&[
        (
            "a.ts",
            "import { thing } from './b';\nexport const x = 1;\n",
        ),
        ("b.ts", "export const thing = 1;\n"),
    ]);
    indexer.reindex().unwrap();
    let unused = unused_import_qualnames(&mut indexer);
    assert!(unused.iter().any(|q| q == "./b"), "{unused:?}");
}
