//! A Go function used as a value -- passed as an argument, placed in a
//! map / struct / slice literal, or taken as a method value -- is a
//! `REFERENCES` edge from the enclosing symbol. Locals, plain data and
//! names already called are not.

mod common;

use common::{edge_targets, ref_targets, unresolved_reference_rows};

const FILES: &[(&str, &str)] = &[
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
fn unresolved_value_references_leave_no_rows() {
    assert_eq!(unresolved_reference_rows(FILES), 0);
}
