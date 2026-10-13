//! Methods that implement an interface or trait of the standard library or a
//! dependency (issue #472).
//!
//! The runtime or the dependency calls such a method through the interface,
//! so no call edge in the repository reaches it although it is used -- when
//! its receiver type is live. `dead_symbols` therefore treats it as reached
//! through its type, with the reason [`REASON`], and keeps the same method of
//! a dead type dead.
//!
//! - Go: `go_external_interface` (structural: name and signature against the
//!   standard-library and dependency interface tables).
//! - Rust: the extractor links `impl ExternalTrait for T` methods to the
//!   trait's external stub with an `IMPLEMENTS` edge; `dead_symbols` reads it.
//! - TypeScript: the protocols the runtime calls on any object (`toString`,
//!   `Symbol.iterator`, ...) and the methods of the well-known external
//!   interfaces a class names in `implements` (`Disposable`, `Iterator`, ...).

/// The reason on the liveness path.
pub(crate) const REASON: &str = "implements_external_interface";

/// Methods the JavaScript runtime calls on any object, whatever the class
/// declares it implements.
const TS_RUNTIME_PROTOCOL: &[&str] = &[
    "toString",
    "toJSON",
    "valueOf",
    "[Symbol.iterator]",
    "[Symbol.asyncIterator]",
    "[Symbol.toPrimitive]",
    "[Symbol.dispose]",
    "[Symbol.asyncDispose]",
];

/// Well-known external interfaces (ECMAScript, DOM, Node, VS Code) and the
/// methods a class implementing them declares.
const TS_EXTERNAL_INTERFACES: &[(&str, &[&str])] = &[
    ("Disposable", &["dispose"]),
    ("AsyncDisposable", &["disposeAsync"]),
    ("IDisposable", &["dispose"]),
    ("Iterator", &["next", "return", "throw"]),
    ("AsyncIterator", &["next", "return", "throw"]),
    ("IterableIterator", &["next", "return", "throw"]),
    ("AsyncIterableIterator", &["next", "return", "throw"]),
    ("Generator", &["next", "return", "throw"]),
    ("PromiseLike", &["then"]),
    ("Promise", &["then", "catch", "finally"]),
    ("EventListenerObject", &["handleEvent"]),
    ("Comparable", &["compareTo"]),
    ("Transformer", &["start", "transform", "flush"]),
    ("UnderlyingSource", &["start", "pull", "cancel"]),
    ("UnderlyingSink", &["start", "write", "close", "abort"]),
    (
        "ProxyHandler",
        &["get", "set", "has", "deleteProperty", "apply", "construct"],
    ),
];

/// The external interface a TypeScript class method implements: a runtime
/// protocol method, or a method of an interface in `implemented` (the simple
/// names the class lists after `implements`). Generic arguments are ignored.
pub(crate) fn ts_external_interface(name: &str, implemented: &[String]) -> Option<String> {
    if TS_RUNTIME_PROTOCOL.contains(&name) {
        return Some(format!("runtime protocol {name}"));
    }
    implemented.iter().find_map(|iface| {
        let simple = iface.split('<').next().unwrap_or(iface);
        let simple = simple.rsplit('.').next().unwrap_or(simple).trim();
        TS_EXTERNAL_INTERFACES
            .iter()
            .find(|(n, methods)| *n == simple && methods.contains(&name))
            .map(|(n, _)| (*n).to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ts_protocol_and_named_interface() {
        assert!(ts_external_interface("toString", &[]).is_some());
        assert!(ts_external_interface("dispose", &[]).is_none());
        assert_eq!(
            ts_external_interface("dispose", &["Disposable".into()]).as_deref(),
            Some("Disposable")
        );
        assert!(ts_external_interface("next", &["vscode.Disposable".into()]).is_none());
        assert!(ts_external_interface("next", &["Iterator<number>".into()]).is_some());
    }
}
