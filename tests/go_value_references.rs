//! A Go function used as a value -- passed as an argument, placed in a
//! map / struct / slice literal, or taken as a method value -- is a
//! `REFERENCES` edge from the enclosing symbol. Locals, plain data and
//! names already called are not.

mod common;

use common::{edge_targets, edge_targets_with_resolution, ref_targets, unresolved_accounting};

const FILES: &[(&str, &str)] = &[
    ("go.mod", "module example.com/app\n"),
    (
        "internal/handlers/users.go",
        r#"package handlers

func Do(x int) int { return x }

func Other(x int) int { return x }

const Limit = 10

type Router struct{}

func (r *Router) Handle(x int) int { return x }
"#,
    ),
    (
        "cmd/app/helpers.go",
        r#"package main

func sibling(x int) int { return x }

type Server struct {
	OnDone func(int) int
}

func (s *Server) Handle(x int) int { return x }
"#,
    ),
    (
        "cmd/app/main.go",
        r#"package main

import (
	"example.com/app/internal/handlers"
	h "example.com/app/internal/handlers"
	"strings"
)

func handler(x int) int { return x }

func apply(f func(int) int, v int) int { return f(v) }

var routes = map[string]func(int) int{
	"a": handler,
	"b": h.Do,
}

func positional() int {
	return apply(handler, 1)
}

func sameFolderSibling() int {
	return apply(sibling, 1)
}

func packageQualified() int {
	return apply(handlers.Other, 1) + apply(h.Do, 2)
}

func mapLiteral() map[string]func(int) int {
	return map[string]func(int) int{"x": handler, "y": h.Other}
}

func structLiteral() Server {
	return Server{OnDone: handler}
}

func positionalStruct() Server {
	return Server{handler}
}

func sliceLiteral() []func(int) int {
	return []func(int) int{handler, sibling}
}

func (s *Server) methodValue() int {
	return apply(s.Handle, 1)
}

func localMethodValue() int {
	srv := &Server{}
	return apply(srv.Handle, 1)
}

func nestedInCall() int {
	return apply(func(x int) int { return apply(handler, x) }, 1)
}

func negatives(param func(int) int, n int) int {
	local := 3
	handler := func(x int) int { return x + 1 }
	_ = strings.ToUpper
	return apply(param, local) + apply(handler, n) + handlers.Limit + apply(strings.ToUpper2, n)
}

func called() int {
	handler(1)
	return apply(handler, 2)
}
"#,
    ),
];

#[test]
fn arguments_same_package_and_imported() {
    let m = "cmd/app/main";
    assert_eq!(
        ref_targets(FILES, &format!("{m}.positional")),
        [format!("{m}.handler")]
    );
    assert_eq!(
        ref_targets(FILES, &format!("{m}.sameFolderSibling")),
        ["cmd/app/helpers.sibling"]
    );
    assert_eq!(
        ref_targets(FILES, &format!("{m}.packageQualified")),
        [
            "internal/handlers/users.Do",
            "internal/handlers/users.Other"
        ]
    );
}

#[test]
fn composite_literal_values() {
    let m = "cmd/app/main";
    assert_eq!(
        ref_targets(FILES, &format!("{m}.mapLiteral")),
        [
            format!("{m}.handler"),
            "internal/handlers/users.Other".to_string()
        ]
    );
    assert_eq!(
        ref_targets(FILES, &format!("{m}.structLiteral")),
        [format!("{m}.handler")]
    );
    assert_eq!(
        ref_targets(FILES, &format!("{m}.positionalStruct")),
        [format!("{m}.handler")]
    );
    assert_eq!(
        ref_targets(FILES, &format!("{m}.sliceLiteral")),
        [
            "cmd/app/helpers.sibling".to_string(),
            format!("{m}.handler")
        ]
    );
    // The package-level dispatch table is sourced from the module.
    assert_eq!(
        ref_targets(FILES, "cmd/app/main"),
        [
            format!("{m}.handler"),
            "internal/handlers/users.Do".to_string()
        ]
    );
}

#[test]
fn method_values() {
    let m = "cmd/app/main";
    assert_eq!(
        ref_targets(FILES, &format!("{m}.Server.methodValue")),
        ["cmd/app/helpers.Server.Handle"]
    );
    assert_eq!(
        ref_targets(FILES, &format!("{m}.localMethodValue")),
        ["cmd/app/helpers.Server.Handle"]
    );
}

#[test]
fn arguments_nested_inside_calls() {
    let m = "cmd/app/main";
    assert_eq!(
        ref_targets(FILES, &format!("{m}.nestedInCall")),
        [format!("{m}.handler")]
    );
}

#[test]
fn locals_params_data_and_calls_are_not_references() {
    let m = "cmd/app/main";
    // `param`, `local` and the shadowing `handler` are variables; `Limit` is
    // a constant; `strings` is an external package.
    assert!(ref_targets(FILES, &format!("{m}.negatives")).is_empty());
    assert!(ref_targets(FILES, &format!("{m}.called")).is_empty());
    assert!(edge_targets(FILES, &format!("{m}.called"), "CALLS").contains(&format!("{m}.handler")));
}

#[test]
fn unresolved_value_references_are_not_counted() {
    unresolved_accounting(FILES).assert_value_references_not_counted();
}

#[test]
fn same_package_references_are_exact_and_imported_ones_import() {
    let m = "cmd/app/main";
    assert_eq!(
        edge_targets_with_resolution(FILES, &format!("{m}.sameFolderSibling"), "REFERENCES"),
        [("cmd/app/helpers.sibling".to_string(), "exact".to_string())]
    );
    assert_eq!(
        edge_targets_with_resolution(FILES, &format!("{m}.packageQualified"), "REFERENCES"),
        [
            (
                "internal/handlers/users.Do".to_string(),
                "import".to_string()
            ),
            (
                "internal/handlers/users.Other".to_string(),
                "import".to_string()
            )
        ]
    );
}

const FOREIGN: &[(&str, &str)] = &[
    ("go.mod", "module example.com/app\n"),
    (
        "internal/util/util.go",
        "package util\n\nfunc Do(x int) int { return x }\n",
    ),
    (
        "cmd/cmd.go",
        r#"package cmd

import "github.com/other/dep/internal/util"

func apply(f func(int) int) int { return f(1) }

func useForeign() int { return apply(util.Do) }
"#,
    ),
];

#[test]
fn an_import_outside_the_module_never_binds_a_repo_directory() {
    // `github.com/other/dep/internal/util` ends with this repo's
    // `internal/util`, but it is another module's package.
    assert!(ref_targets(FOREIGN, "cmd/cmd.useForeign").is_empty());
}

const ROOT: &[(&str, &str)] = &[
    ("go.mod", "module github.com/spf13/cobra\n"),
    (
        "cobra.go",
        "package cobra\n\nfunc NoArgs(x int) int { return x }\n",
    ),
    (
        "doc/doc.go",
        r#"package doc

import "github.com/spf13/cobra"

func apply(f func(int) int) int { return f(1) }

func useRoot() int { return apply(cobra.NoArgs) }
"#,
    ),
];

#[test]
fn the_module_root_package_is_resolvable() {
    assert_eq!(ref_targets(ROOT, "doc/doc.useRoot"), ["cobra.NoArgs"]);
}

const NESTED: &[(&str, &str)] = &[
    ("services/api/go.mod", "module example.com/api\n"),
    (
        "services/api/store/store.go",
        "package store\n\nfunc Open(x int) int { return x }\n",
    ),
    (
        "services/api/main.go",
        r#"package main

import "example.com/api/store"

func apply(f func(int) int) int { return f(1) }

func run() int { return apply(store.Open) }
"#,
    ),
];

#[test]
fn a_nested_module_maps_import_paths_below_its_own_directory() {
    assert_eq!(
        ref_targets(NESTED, "services/api/main.run"),
        ["services/api/store/store.Open"]
    );
}
