//! Named edge-kind sets shared by every consumer of the symbol graph.
//!
//! Each consumer used to carry its own hard-coded list of edge kinds, so
//! adding a kind meant hunting every list and features silently disagreed on
//! what "used" means. The sets below are the single place that answers:
//!
//! - [`EXECUTION`]: control can flow along the edge (what `trace_flow` walks,
//!   minus `CONFIG_BIND`). Never contains `REFERENCES`.
//! - [`SYMBOL_USE`]: the source depends on the target symbol in some way:
//!   [`EXECUTION`] plus type relations, `USES`, `CONFIG_BIND` and `REFERENCES`.
//! - [`REACHABILITY`]: what keeps a symbol alive for `dead_symbols`.
//! - [`FILE_DEPENDENCY`]: file/module level dependency edges.
//!
//! The remaining sets are the exact lists individual consumers carried before
//! this module existed (named after what they are for), so migrating a
//! consumer never changes its behaviour.

pub const CALLS: &str = "CALLS";
pub const IMPORTS: &str = "IMPORTS";
pub const CONTAINS: &str = "CONTAINS";
pub const EXTENDS: &str = "EXTENDS";
pub const IMPLEMENTS: &str = "IMPLEMENTS";
pub const INHERITS: &str = "INHERITS";
pub const USES: &str = "USES";
pub const REFERENCES: &str = "REFERENCES";
pub const XREF: &str = "XREF";
pub const RPC_IMPL: &str = "RPC_IMPL";
pub const RPC_CALL: &str = "RPC_CALL";
pub const RPC_ROUTE: &str = "RPC_ROUTE";
pub const HTTP_CALL: &str = "HTTP_CALL";
pub const HTTP_ROUTE: &str = "HTTP_ROUTE";
pub const CHANNEL_PUBLISH: &str = "CHANNEL_PUBLISH";
pub const CHANNEL_SUBSCRIBE: &str = "CHANNEL_SUBSCRIBE";
pub const CONFIG_SOURCE: &str = "CONFIG_SOURCE";
pub const CONFIG_READ: &str = "CONFIG_READ";
pub const CONFIG_BIND: &str = "CONFIG_BIND";
pub const MODULE_FILE: &str = "MODULE_FILE";
pub const IMPORTS_FILE: &str = "IMPORTS_FILE";

/// Control can flow along the edge. Excludes `REFERENCES` (a function passed
/// as a value is not a call) and `CONFIG_BIND` (a binding, not a flow).
/// `XREF` is listed but `trace_flow` only crosses its qualified grade;
/// `RPC_ROUTE` must be listed so a trace from a `.proto` rpc is not filtered
/// out before the bridge is consulted.
pub const EXECUTION: &[&str] = &[
    CALLS,
    RPC_IMPL,
    RPC_CALL,
    RPC_ROUTE,
    XREF,
    CHANNEL_PUBLISH,
    CHANNEL_SUBSCRIBE,
    HTTP_CALL,
    HTTP_ROUTE,
    CONFIG_SOURCE,
    CONFIG_READ,
];

/// The source depends on the target symbol: [`EXECUTION`] plus type
/// relations, `USES`, `CONFIG_BIND` and `REFERENCES`.
pub const SYMBOL_USE: &[&str] = &[
    CALLS,
    RPC_IMPL,
    RPC_CALL,
    RPC_ROUTE,
    XREF,
    CHANNEL_PUBLISH,
    CHANNEL_SUBSCRIBE,
    HTTP_CALL,
    HTTP_ROUTE,
    CONFIG_SOURCE,
    CONFIG_READ,
    EXTENDS,
    IMPLEMENTS,
    INHERITS,
    USES,
    CONFIG_BIND,
    REFERENCES,
];

/// Incoming edges that keep a symbol alive for `dead_symbols`: the
/// [`SYMBOL_USE`] kinds whose targets are real symbols today, plus `IMPORTS`.
/// Deliberately narrower than `SYMBOL_USE + IMPORTS` so `dead_symbols` keeps
/// its exact pre-existing behaviour; add a kind here (as `REFERENCES` was)
/// to make it count as a use.
pub const REACHABILITY: &[&str] = &[
    CALLS, IMPORTS, RPC_IMPL, IMPLEMENTS, EXTENDS, USES, REFERENCES,
];

/// File/module level dependency edges.
pub const FILE_DEPENDENCY: &[&str] = &[IMPORTS, IMPORTS_FILE, MODULE_FILE];

/// `trace_flow`'s default `allowed_kinds`: everything it walks.
pub const TRACE_DEFAULT: &[&str] = &[
    CALLS,
    RPC_IMPL,
    RPC_CALL,
    RPC_ROUTE,
    XREF,
    CHANNEL_PUBLISH,
    CHANNEL_SUBSCRIBE,
    HTTP_CALL,
    HTTP_ROUTE,
    CONFIG_SOURCE,
    CONFIG_READ,
    CONFIG_BIND,
];

/// Edges `gather_context` follows when expanding seeds into a subgraph.
pub const CONTEXT_EXPANSION: &[&str] = &[CALLS, CONTAINS, IMPLEMENTS, EXTENDS, IMPORTS, RPC_IMPL];

/// Edges counted as module-to-module coupling in the repo overview.
pub const MODULE_COUPLING: &[&str] = &[CALLS, IMPORTS, XREF];

/// Edges that make their *source* an entry point (server side of a bridge),
/// so `dead_symbols` never reports it.
pub const ENTRYPOINT: &[&str] = &[HTTP_ROUTE, RPC_IMPL, CHANNEL_SUBSCRIBE];

/// Subtype relations whose class hierarchy is walked (`EXTENDS`, `IMPLEMENTS`).
pub const SUBTYPING: &[&str] = &[EXTENDS, IMPLEMENTS];

/// Type hierarchy relations, including `INHERITS`.
pub const TYPE_HIERARCHY: &[&str] = &[EXTENDS, IMPLEMENTS, INHERITS];

/// Edges that carry a config key/secret URI.
pub const CONFIG: &[&str] = &[CONFIG_SOURCE, CONFIG_READ, CONFIG_BIND];

/// Config edges plus `CALLS`: the kinds that surface config/DI consumers in
/// `analyze_impact` and `trace_flow` retry suggestions.
pub const CONFIG_CONSUMER_RETRY: &[&str] = &[CONFIG_BIND, CONFIG_SOURCE, CONFIG_READ, CALLS];

/// The two config edge kinds that cross a bridge (source and reader of a URI).
pub const CONFIG_BRIDGE: &[&str] = &[CONFIG_SOURCE, CONFIG_READ];

/// Config edges that identify a symbol as a *consumer* of a config URI.
pub const CONFIG_CONSUMER: &[&str] = &[CONFIG_READ, CONFIG_BIND];

/// Client side of each bridge pair: the kinds an explained symbol's
/// *outgoing* cross-boundary edges carry.
pub const BRIDGE_CLIENT: &[&str] = &[RPC_CALL, HTTP_CALL, CHANNEL_PUBLISH, CONFIG_READ];

/// Server side of each bridge pair: the kinds a callee-of-a-bridge carries.
pub const BRIDGE_SERVER: &[&str] = &[RPC_IMPL, HTTP_ROUTE, CHANNEL_SUBSCRIBE, CONFIG_SOURCE];

/// Edges that call out of a file in the per-file context view.
pub const CALL_SITE: &[&str] = &[CALLS, RPC_CALL, HTTP_CALL, CHANNEL_PUBLISH];

/// gRPC protocol family.
pub const RPC_FAMILY: &[&str] = &[RPC_IMPL, RPC_CALL, RPC_ROUTE];
/// HTTP protocol family.
pub const HTTP_FAMILY: &[&str] = &[HTTP_CALL, HTTP_ROUTE];
/// Message-bus protocol family.
pub const CHANNEL_FAMILY: &[&str] = &[CHANNEL_PUBLISH, CHANNEL_SUBSCRIBE];

/// RPC edges that bind to a route: implementation and call sides.
pub const RPC_BOUND: &[&str] = &[RPC_IMPL, RPC_CALL];

/// Every edge kind the indexer can write to the `edges` table, so `kinds`
/// can never reject a kind that exists in the graph. Kinds with a `*_KIND`
/// const in the indexer reference it; the rest are literals. Matching is
/// case-insensitive; see `normalize_kinds` in `rpc::validate`. `tests/direction_kinds_validation.rs`
/// asserts every kind found in indexed fixtures is accepted.
pub const ALL: &[&str] = &[
    "CALLS",
    "IMPORTS",
    "CONTAINS",
    "EXTENDS",
    "IMPLEMENTS",
    "INHERITS",
    "USES",
    "MODULE_FILE",
    "IMPORTS_FILE",
    crate::indexer::proto::RPC_IMPL_KIND,
    crate::indexer::proto::RPC_CALL_KIND,
    crate::indexer::proto::RPC_ROUTE_KIND,
    crate::indexer::http::HTTP_ROUTE_KIND,
    crate::indexer::http::HTTP_CALL_KIND,
    crate::indexer::http::PAGE_ROUTE_KIND,
    crate::indexer::channel::CHANNEL_PUBLISH_KIND,
    crate::indexer::channel::CHANNEL_SUBSCRIBE_KIND,
    crate::indexer::config::CONFIG_SOURCE_KIND,
    crate::indexer::config::CONFIG_READ_KIND,
    crate::indexer::config::CONFIG_BIND_KIND,
    crate::indexer::xref::XREF_KIND,
    crate::indexer::xref::ROUTE_KIND,
    crate::indexer::python::MODULE_EXPORT_KIND,
    crate::indexer::sql_extractor::REFERENCES_KIND,
];

/// Whether `kind` belongs to `set`.
pub fn contains(set: &[&str], kind: &str) -> bool {
    set.contains(&kind)
}

/// Owned copy of a set, for APIs taking `Vec<String>`.
pub fn to_strings(set: &[&str]) -> Vec<String> {
    set.iter().map(|k| k.to_string()).collect()
}

/// A set rendered as a SQL list body: `'CALLS', 'IMPORTS'`. Kinds are
/// compile-time constants, never user input, so no escaping is needed.
pub fn sql_list(set: &[&str]) -> String {
    set.iter()
        .map(|k| format!("'{k}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn members(set: &[&'static str]) -> BTreeSet<&'static str> {
        let m: BTreeSet<&'static str> = set.iter().copied().collect();
        assert_eq!(m.len(), set.len(), "duplicate kind in {set:?}");
        m
    }

    fn of(kinds: &[&'static str]) -> BTreeSet<&'static str> {
        kinds.iter().copied().collect()
    }

    #[test]
    fn execution_membership() {
        assert_eq!(
            members(EXECUTION),
            of(&[
                "CALLS",
                "RPC_IMPL",
                "RPC_CALL",
                "RPC_ROUTE",
                "XREF",
                "CHANNEL_PUBLISH",
                "CHANNEL_SUBSCRIBE",
                "HTTP_CALL",
                "HTTP_ROUTE",
                "CONFIG_SOURCE",
                "CONFIG_READ",
            ])
        );
        assert!(!contains(EXECUTION, REFERENCES));
        assert!(!contains(EXECUTION, CONFIG_BIND));
    }

    #[test]
    fn symbol_use_membership() {
        let mut expected = members(EXECUTION);
        expected.extend([
            "EXTENDS",
            "IMPLEMENTS",
            "INHERITS",
            "USES",
            "CONFIG_BIND",
            "REFERENCES",
        ]);
        assert_eq!(members(SYMBOL_USE), expected);
    }

    #[test]
    fn reachability_membership() {
        assert_eq!(
            members(REACHABILITY),
            of(&[
                "CALLS",
                "IMPORTS",
                "RPC_IMPL",
                "IMPLEMENTS",
                "EXTENDS",
                "USES",
                "REFERENCES",
            ])
        );
        let mut universe = members(SYMBOL_USE);
        universe.insert(IMPORTS);
        assert!(members(REACHABILITY).is_subset(&universe));
    }

    #[test]
    fn file_dependency_membership() {
        assert_eq!(
            members(FILE_DEPENDENCY),
            of(&["IMPORTS", "IMPORTS_FILE", "MODULE_FILE"])
        );
    }

    #[test]
    fn references_is_symbol_use_and_reachability_but_not_execution() {
        assert!(contains(SYMBOL_USE, REFERENCES));
        assert!(contains(REACHABILITY, REFERENCES));
        assert!(!contains(EXECUTION, REFERENCES));
        assert!(!contains(TRACE_DEFAULT, REFERENCES));
        assert!(!contains(FILE_DEPENDENCY, REFERENCES));
    }

    #[test]
    fn trace_default_is_execution_plus_config_bind() {
        let mut expected = members(EXECUTION);
        expected.insert(CONFIG_BIND);
        assert_eq!(members(TRACE_DEFAULT), expected);
    }

    #[test]
    fn consumer_set_membership() {
        assert_eq!(
            members(CONTEXT_EXPANSION),
            of(&[
                "CALLS",
                "CONTAINS",
                "IMPLEMENTS",
                "EXTENDS",
                "IMPORTS",
                "RPC_IMPL"
            ])
        );
        assert_eq!(members(MODULE_COUPLING), of(&["CALLS", "IMPORTS", "XREF"]));
        assert_eq!(
            members(ENTRYPOINT),
            of(&["HTTP_ROUTE", "RPC_IMPL", "CHANNEL_SUBSCRIBE"])
        );
        assert_eq!(members(SUBTYPING), of(&["EXTENDS", "IMPLEMENTS"]));
        assert_eq!(
            members(TYPE_HIERARCHY),
            of(&["EXTENDS", "IMPLEMENTS", "INHERITS"])
        );
        assert_eq!(
            members(CONFIG),
            of(&["CONFIG_SOURCE", "CONFIG_READ", "CONFIG_BIND"])
        );
        assert_eq!(
            members(CONFIG_CONSUMER_RETRY),
            of(&["CONFIG_BIND", "CONFIG_SOURCE", "CONFIG_READ", "CALLS"])
        );
        assert_eq!(
            members(CONFIG_CONSUMER),
            of(&["CONFIG_READ", "CONFIG_BIND"])
        );
        assert_eq!(
            members(BRIDGE_CLIENT),
            of(&["RPC_CALL", "HTTP_CALL", "CHANNEL_PUBLISH", "CONFIG_READ"])
        );
        assert_eq!(
            members(BRIDGE_SERVER),
            of(&[
                "RPC_IMPL",
                "HTTP_ROUTE",
                "CHANNEL_SUBSCRIBE",
                "CONFIG_SOURCE"
            ])
        );
        assert_eq!(
            members(CALL_SITE),
            of(&["CALLS", "RPC_CALL", "HTTP_CALL", "CHANNEL_PUBLISH"])
        );
        assert_eq!(
            members(RPC_FAMILY),
            of(&["RPC_IMPL", "RPC_CALL", "RPC_ROUTE"])
        );
        assert_eq!(members(HTTP_FAMILY), of(&["HTTP_CALL", "HTTP_ROUTE"]));
        assert_eq!(
            members(CHANNEL_FAMILY),
            of(&["CHANNEL_PUBLISH", "CHANNEL_SUBSCRIBE"])
        );
        assert_eq!(members(RPC_BOUND), of(&["RPC_IMPL", "RPC_CALL"]));
    }

    #[test]
    fn sql_list_quotes_and_joins() {
        assert_eq!(sql_list(SUBTYPING), "'EXTENDS', 'IMPLEMENTS'");
    }
}
