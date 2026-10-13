//! Named edge-kind sets shared by every consumer of the symbol graph.
//!
//! Each consumer used to carry its own hard-coded list of edge kinds, so
//! adding a kind meant hunting every list and features silently disagreed on
//! what "used" means. The sets below are the single place that answers:
//!
//! - [`EXECUTION`]: control can flow along the edge (what `trace_flow` walks,
//!   minus `CONFIG_BIND`). Never contains `REFERENCES`.
//! - [`CONSUMER`]: [`EXECUTION`] plus `CONFIG_BIND`: the edges whose source
//!   consumes the target at runtime. `trace_flow` defaults to it and
//!   `explain_symbol` builds `callers`/`callees`/`tests` from it.
//! - [`SYMBOL_USE`]: the source depends on the target symbol in some way:
//!   [`CONSUMER`] plus type hierarchy edges, `USES` and `REFERENCES`.
//!   Whatever it holds beyond [`CONSUMER`] is what `explain_symbol` reports
//!   under `other_edges` (see [`is_other_edge`]).
//! - [`REACHABILITY`]: what keeps a symbol alive for `dead_symbols`.
//!
//! The remaining sets are the lists individual consumers carried before this
//! module existed (named after what they are for), so migrating a consumer
//! never changes its behaviour. Rust consts cannot be composed at const
//! level, so the unit tests below assert how the sets relate to each other.

use crate::indexer::{channel, config, http, proto, python, sql_extractor, xref};

pub const CALLS: &str = "CALLS";
/// A call on an interface-typed receiver whose implementation cannot be told
/// (Go: several implementations, none provable at the call site): the target is
/// the interface method, and traversals fan out from it to the implementations
/// through `IMPLEMENTS` (`Db::edges_for_symbols_with_dispatch`). Never a
/// `CALLS` edge: a graph that names one callee for the call is not
/// describing it. See [`CALL_LIKE`].
pub const DISPATCH: &str = "DISPATCH";
pub const IMPORTS: &str = "IMPORTS";
pub const CONTAINS: &str = "CONTAINS";
pub const EXTENDS: &str = "EXTENDS";
pub const IMPLEMENTS: &str = "IMPLEMENTS";
pub const INHERITS: &str = "INHERITS";
pub const USES: &str = "USES";
pub const MODULE_FILE: &str = "MODULE_FILE";
pub const IMPORTS_FILE: &str = "IMPORTS_FILE";
pub const REFERENCES: &str = sql_extractor::REFERENCES_KIND;
pub const XREF: &str = xref::XREF_KIND;
pub const ROUTE: &str = xref::ROUTE_KIND;
pub const PAGE_ROUTE: &str = http::PAGE_ROUTE_KIND;
pub const MODULE_EXPORT: &str = python::MODULE_EXPORT_KIND;
pub const RPC_IMPL: &str = proto::RPC_IMPL_KIND;
pub const RPC_CALL: &str = proto::RPC_CALL_KIND;
pub const RPC_ROUTE: &str = proto::RPC_ROUTE_KIND;
pub const HTTP_CALL: &str = http::HTTP_CALL_KIND;
pub const HTTP_ROUTE: &str = http::HTTP_ROUTE_KIND;
pub const CHANNEL_PUBLISH: &str = channel::CHANNEL_PUBLISH_KIND;
pub const CHANNEL_SUBSCRIBE: &str = channel::CHANNEL_SUBSCRIBE_KIND;
pub const CONFIG_SOURCE: &str = config::CONFIG_SOURCE_KIND;
pub const CONFIG_READ: &str = config::CONFIG_READ_KIND;
pub const CONFIG_BIND: &str = config::CONFIG_BIND_KIND;

/// Control can flow along the edge. Excludes `REFERENCES` (a function passed
/// as a value is not a call) and `CONFIG_BIND` (a binding, not a flow).
/// `XREF` is listed but `trace_flow` only crosses its qualified grade;
/// `RPC_ROUTE` must be listed so a trace from a `.proto` rpc is not filtered
/// out before the bridge is consulted.
pub const EXECUTION: &[&str] = &[
    CALLS,
    DISPATCH,
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

/// [`EXECUTION`] plus `CONFIG_BIND`: edges whose source consumes the target
/// at runtime. `explain_symbol`'s `callers`/`callees`/`tests` and
/// `trace_flow`'s default kinds are built from it.
pub const CONSUMER: &[&str] = &[
    CALLS,
    DISPATCH,
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

/// The source depends on the target symbol: [`CONSUMER`] plus type hierarchy
/// edges, `USES` and `REFERENCES`.
pub const SYMBOL_USE: &[&str] = &[
    CALLS,
    DISPATCH,
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
    EXTENDS,
    IMPLEMENTS,
    INHERITS,
    USES,
    REFERENCES,
];

/// Incoming edges that keep a symbol alive for `dead_symbols`: the
/// [`SYMBOL_USE`] kinds whose targets are real symbols, plus `IMPORTS`.
/// Narrower than `SYMBOL_USE + IMPORTS` only in leaving out the string-
/// targeted bridge kinds (their targets are route/channel/config URIs, not
/// symbols). `CONFIG_BIND` and `INHERITS` count since #408: an options class
/// bound only through `IOptions<T>` consumers is live.
pub const REACHABILITY: &[&str] = &[
    CALLS,
    DISPATCH,
    IMPORTS,
    RPC_IMPL,
    IMPLEMENTS,
    EXTENDS,
    INHERITS,
    USES,
    CONFIG_BIND,
    REFERENCES,
];

/// `trace_flow`'s default `allowed_kinds`: everything it walks.
pub const TRACE_DEFAULT: &[&str] = CONSUMER;

/// Edges `gather_context` follows when expanding seeds into a subgraph.
pub const CONTEXT_EXPANSION: &[&str] = &[
    CALLS, DISPATCH, CONTAINS, IMPLEMENTS, EXTENDS, IMPORTS, RPC_IMPL,
];

/// Edges counted as module-to-module coupling in the repo overview.
pub const MODULE_COUPLING: &[&str] = &[CALLS, DISPATCH, IMPORTS, XREF];

/// Edges that make their *source* an entry point (server side of a bridge),
/// so `dead_symbols` never reports it.
pub const ENTRYPOINT: &[&str] = &[HTTP_ROUTE, RPC_IMPL, CHANNEL_SUBSCRIBE];

/// The edges whose source *calls* the target: a call with a named callee and a
/// dispatch through an interface method. What `callers`/`callees` logic that
/// special-cases calls (as opposed to bridge edges) tests with [`is_call_like`].
pub const CALL_LIKE: &[&str] = &[CALLS, DISPATCH];

/// Subtype relations whose class hierarchy is walked (`EXTENDS`, `IMPLEMENTS`).
pub const SUBTYPING: &[&str] = &[EXTENDS, IMPLEMENTS];

/// Type hierarchy relations, including `INHERITS`.
pub const TYPE_HIERARCHY: &[&str] = &[EXTENDS, IMPLEMENTS, INHERITS];

/// Edges that carry a config key/secret URI.
pub const CONFIG: &[&str] = &[CONFIG_SOURCE, CONFIG_READ, CONFIG_BIND];

/// The two config edge kinds that cross a bridge (source and reader of a URI).
pub const CONFIG_BRIDGE: &[&str] = &[CONFIG_SOURCE, CONFIG_READ];

/// Config edges that identify a symbol as a *consumer* of a config URI.
pub const CONFIG_CONSUMER: &[&str] = &[CONFIG_READ, CONFIG_BIND];

/// Config edges plus `CALLS`: the kinds that surface config/DI consumers in
/// `analyze_impact` and `trace_flow` retry suggestions.
pub const CONFIG_CONSUMER_RETRY: &[&str] = &[CONFIG_BIND, CONFIG_SOURCE, CONFIG_READ, CALLS];

/// Client side of each bridge pair: the kinds an explained symbol's
/// *outgoing* cross-boundary edges carry.
pub const BRIDGE_CLIENT: &[&str] = &[RPC_CALL, HTTP_CALL, CHANNEL_PUBLISH, CONFIG_READ];

/// Server side of each bridge pair: the kinds a callee-of-a-bridge carries.
pub const BRIDGE_SERVER: &[&str] = &[RPC_IMPL, HTTP_ROUTE, CHANNEL_SUBSCRIBE, CONFIG_SOURCE];

/// Edges that call out of a file in the per-file context view.
pub const CALL_SITE: &[&str] = &[CALLS, DISPATCH, RPC_CALL, HTTP_CALL, CHANNEL_PUBLISH];

/// gRPC protocol family.
pub const RPC_FAMILY: &[&str] = &[RPC_IMPL, RPC_CALL, RPC_ROUTE];
/// HTTP protocol family.
pub const HTTP_FAMILY: &[&str] = &[HTTP_CALL, HTTP_ROUTE];
/// Message-bus protocol family.
pub const CHANNEL_FAMILY: &[&str] = &[CHANNEL_PUBLISH, CHANNEL_SUBSCRIBE];

/// RPC edges that bind to a route: implementation and call sides.
pub const RPC_BOUND: &[&str] = &[RPC_IMPL, RPC_CALL];

/// Bridge edges: kinds whose target is a route/channel/config key or a
/// cross-reference rather than a plain symbol (see
/// `indexer::channel::is_bridge_edge_kind`): the RPC, HTTP, channel and
/// config kinds plus `XREF`.
pub const BRIDGE: &[&str] = &[
    RPC_IMPL,
    RPC_CALL,
    RPC_ROUTE,
    HTTP_CALL,
    HTTP_ROUTE,
    CHANNEL_PUBLISH,
    CHANNEL_SUBSCRIBE,
    CONFIG_SOURCE,
    CONFIG_READ,
    CONFIG_BIND,
    XREF,
];

/// Import machinery: edges that declare an import rather than use the
/// imported name.
pub const IMPORT_MACHINERY: &[&str] = &[IMPORTS, IMPORTS_FILE];

/// Edges that never indicate a name is used in a file (`unused_imports`).
pub const NON_USAGE: &[&str] = &[IMPORTS, IMPORTS_FILE, CONTAINS];

/// Structural edges: declaration nesting, not a dependency on the target.
pub const STRUCTURAL: &[&str] = &[CONTAINS, MODULE_FILE];

/// Every edge kind the indexer can write to the `edges` table, so `kinds`
/// can never reject a kind that exists in the graph. Matching is
/// case-insensitive; see `normalize_kinds` in `rpc::validate`.
/// `tests/direction_kinds_validation.rs` asserts every kind found in indexed
/// fixtures is accepted.
pub const ALL: &[&str] = &[
    CALLS,
    DISPATCH,
    IMPORTS,
    CONTAINS,
    EXTENDS,
    IMPLEMENTS,
    INHERITS,
    USES,
    MODULE_FILE,
    IMPORTS_FILE,
    RPC_IMPL,
    RPC_CALL,
    RPC_ROUTE,
    HTTP_ROUTE,
    HTTP_CALL,
    PAGE_ROUTE,
    CHANNEL_PUBLISH,
    CHANNEL_SUBSCRIBE,
    CONFIG_SOURCE,
    CONFIG_READ,
    CONFIG_BIND,
    XREF,
    ROUTE,
    MODULE_EXPORT,
    REFERENCES,
];

/// Whether `kind` is a symbol-use edge that is not a [`CONSUMER`] edge: the
/// kinds `explain_symbol` reports under `other_edges` instead of
/// `callers`/`callees`.
pub fn is_other_edge(kind: &str) -> bool {
    contains(SYMBOL_USE, kind) && !contains(CONSUMER, kind)
}

/// Whether `kind` is [`CALLS`] or [`DISPATCH`].
pub fn is_call_like(kind: &str) -> bool {
    contains(CALL_LIKE, kind)
}

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

    /// Union of several sets plus loose kinds.
    fn union(sets: &[&[&'static str]], extra: &[&'static str]) -> BTreeSet<&'static str> {
        let mut out = of(extra);
        for s in sets {
            out.extend(s.iter().copied());
        }
        out
    }

    #[test]
    fn execution_membership() {
        assert_eq!(
            members(EXECUTION),
            of(&[
                "CALLS",
                "DISPATCH",
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
        assert_eq!(
            members(SYMBOL_USE),
            of(&[
                "CALLS",
                "DISPATCH",
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
                "CONFIG_BIND",
                "EXTENDS",
                "IMPLEMENTS",
                "INHERITS",
                "USES",
                "REFERENCES",
            ])
        );
    }

    #[test]
    fn reachability_membership() {
        assert_eq!(
            members(REACHABILITY),
            of(&[
                "CALLS",
                "DISPATCH",
                "IMPORTS",
                "RPC_IMPL",
                "IMPLEMENTS",
                "EXTENDS",
                "INHERITS",
                "USES",
                "CONFIG_BIND",
                "REFERENCES",
            ])
        );
    }

    #[test]
    fn references_is_symbol_use_and_reachability_but_not_execution() {
        assert!(contains(SYMBOL_USE, REFERENCES));
        assert!(contains(REACHABILITY, REFERENCES));
        assert!(!contains(EXECUTION, REFERENCES));
        assert!(!contains(CONSUMER, REFERENCES));
        assert!(!contains(TRACE_DEFAULT, REFERENCES));
    }

    #[test]
    fn composition_relationships() {
        let execution = members(EXECUTION);
        let symbol_use = members(SYMBOL_USE);
        assert!(execution.is_subset(&symbol_use));
        assert_eq!(members(CONSUMER), union(&[EXECUTION], &[CONFIG_BIND]));
        assert_eq!(members(TRACE_DEFAULT), members(CONSUMER));
        assert_eq!(
            symbol_use,
            union(
                &[CONSUMER],
                &[EXTENDS, IMPLEMENTS, INHERITS, USES, REFERENCES]
            )
        );
        assert!(members(REACHABILITY).is_subset(&union(&[SYMBOL_USE], &[IMPORTS])));
        assert_eq!(members(NON_USAGE), union(&[IMPORT_MACHINERY], &[CONTAINS]));
        assert_eq!(members(TYPE_HIERARCHY), union(&[SUBTYPING], &[INHERITS]));
        assert_eq!(members(CONFIG), union(&[CONFIG_BRIDGE], &[CONFIG_BIND]));
        assert_eq!(members(CONFIG_CONSUMER_RETRY), union(&[CONFIG], &[CALLS]));
        assert_eq!(
            members(BRIDGE),
            union(&[RPC_FAMILY, HTTP_FAMILY, CHANNEL_FAMILY, CONFIG], &[XREF])
        );
        assert!(members(BRIDGE).is_subset(&symbol_use));
        assert!(members(BRIDGE_CLIENT).is_subset(&members(BRIDGE)));
        assert!(members(BRIDGE_SERVER).is_subset(&members(BRIDGE)));
        assert!(members(RPC_BOUND).is_subset(&members(RPC_FAMILY)));
        assert!(members(ENTRYPOINT).is_subset(&members(BRIDGE)));
        assert!(members(CALL_SITE).is_subset(&execution));
        assert!(symbol_use.is_subset(&members(ALL)));
        assert!(members(STRUCTURAL).is_subset(&members(ALL)));
    }

    #[test]
    fn consumer_set_membership() {
        assert_eq!(
            members(CONTEXT_EXPANSION),
            of(&[
                "CALLS",
                "DISPATCH",
                "CONTAINS",
                "IMPLEMENTS",
                "EXTENDS",
                "IMPORTS",
                "RPC_IMPL"
            ])
        );
        assert_eq!(
            members(MODULE_COUPLING),
            of(&["CALLS", "DISPATCH", "IMPORTS", "XREF"])
        );
        assert_eq!(
            members(ENTRYPOINT),
            of(&["HTTP_ROUTE", "RPC_IMPL", "CHANNEL_SUBSCRIBE"])
        );
        assert_eq!(members(SUBTYPING), of(&["EXTENDS", "IMPLEMENTS"]));
        assert_eq!(
            members(CONFIG_BRIDGE),
            of(&["CONFIG_SOURCE", "CONFIG_READ"])
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
            of(&[
                "CALLS",
                "DISPATCH",
                "RPC_CALL",
                "HTTP_CALL",
                "CHANNEL_PUBLISH"
            ])
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
        assert_eq!(members(IMPORT_MACHINERY), of(&["IMPORTS", "IMPORTS_FILE"]));
        assert_eq!(members(STRUCTURAL), of(&["CONTAINS", "MODULE_FILE"]));
        assert_eq!(members(RPC_BOUND), of(&["RPC_IMPL", "RPC_CALL"]));
    }

    #[test]
    fn other_edges_are_symbol_use_outside_consumer() {
        let other: BTreeSet<&str> = SYMBOL_USE
            .iter()
            .copied()
            .filter(|k| is_other_edge(k))
            .collect();
        assert_eq!(
            other,
            of(&["EXTENDS", "IMPLEMENTS", "INHERITS", "USES", "REFERENCES"])
        );
        assert!(!is_other_edge("CALLS"));
        assert!(!is_other_edge("CONFIG_BIND"));
        assert!(!is_other_edge("IMPORTS"));
    }

    #[test]
    fn sql_list_quotes_and_joins() {
        assert_eq!(sql_list(SUBTYPING), "'EXTENDS', 'IMPLEMENTS'");
    }
}
