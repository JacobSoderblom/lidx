use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::go::{GoExtractor, module_name_from_rel_path};

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("main.go"), "main");
    assert_eq!(module_name_from_rel_path("cmd/server/main.go"), "cmd/server/main");
    assert_eq!(module_name_from_rel_path("pkg/auth/handler.go"), "pkg/auth/handler");
    assert_eq!(module_name_from_rel_path("internal/models/user.go"), "internal/models/user");
}

#[test]
fn extract_symbols_and_edges() {
    let source = r#"
package main

import (
    "fmt"
    "net/http"
)

type User struct {
    ID   int
    Name string
}

type Greeter interface {
    Greet(name string) string
}

func NewUser(id int, name string) *User {
    return &User{ID: id, Name: name}
}

func (u *User) String() string {
    return fmt.Sprintf("User(%d, %s)", u.ID, u.Name)
}

const MaxUsers = 100

var defaultUser = NewUser(0, "nobody")

func helper() {}
func caller() { helper() }
"#;
    let mut extractor = GoExtractor::new().unwrap();
    let extracted = extractor.extract(source, "pkg/models/user").unwrap();

    let names: Vec<_> = extracted
        .symbols
        .iter()
        .map(|s| (s.kind.as_str(), s.qualname.as_str()))
        .collect();

    assert!(names.contains(&("module", "pkg/models/user")));
    assert!(names.contains(&("class", "pkg/models/user.User")));
    assert!(names.contains(&("interface", "pkg/models/user.Greeter")));
    assert!(names.contains(&("function", "pkg/models/user.NewUser")));
    assert!(names.contains(&("method", "pkg/models/user.User.String")));
    assert!(names.contains(&("variable", "pkg/models/user.MaxUsers")));
    assert!(names.contains(&("variable", "pkg/models/user.defaultUser")));
    assert!(names.contains(&("function", "pkg/models/user.helper")));
    assert!(names.contains(&("function", "pkg/models/user.caller")));

    let edge_kinds: Vec<_> = extracted.edges.iter().map(|e| e.kind.as_str()).collect();
    assert!(edge_kinds.contains(&"CONTAINS"));
    assert!(edge_kinds.contains(&"IMPORTS"));
    assert!(edge_kinds.contains(&"CALLS"));

    let imports: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "IMPORTS")
        .collect();
    assert!(imports.iter().any(|e| e.target_qualname.as_deref() == Some("fmt")));
    assert!(imports.iter().any(|e| e.target_qualname.as_deref() == Some("net/http")));

    let calls: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CALLS")
        .collect();
    assert!(
        calls
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("pkg/models/user.helper"))
    );
}
