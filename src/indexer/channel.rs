use crate::indexer::string_consts::{LocalBinding, StringConsts};
use serde_json::json;

pub const CHANNEL_PUBLISH_KIND: &str = "CHANNEL_PUBLISH";
pub const CHANNEL_SUBSCRIBE_KIND: &str = "CHANNEL_SUBSCRIBE";

/// Known topic container prefixes (C# class names, Python enum names, etc.)
const TOPIC_CONTAINERS: &[&str] = &[
    "Topics",
    "TopicName",
    "TopicNames",
    "Topic",
    "Channels",
    "Channel",
    "Queues",
    "Queue",
    "QueueName",
    "QueueNames",
    "EventType",
    "EventTypes",
    "Subjects",
    "Subject",
];

/// Known bus receiver name patterns (last segment of receiver expression)
const BUS_RECEIVER_PATTERNS: &[&str] = &[
    "_bus",
    "_messages",
    "_messageBus",
    "bus",
    "Bus",
    "messageBus",
    "MessageBus",
    "_publisher",
    "publisher",
    "_eventBus",
    "eventBus",
    "_serviceBus",
    "serviceBus",
    "_queue",
    "_channel",
];

/// Known publish method names
const PUBLISH_METHODS: &[&str] = &[
    "PublishAsync",
    "Publish",
    "publish",
    "publish_async",
    "SendAsync",
    "Send",
    "send",
    "emit",
    "Emit",
    "dispatch",
    "Dispatch",
];

/// Known subscribe method names
const SUBSCRIBE_METHODS: &[&str] = &[
    "SubscribeAsync",
    "Subscribe",
    "subscribe",
    "subscribe_async",
    "on",
    "On",
    "AddHandler",
    "add_handler",
    "listen",
    "Listen",
];

/// Azure Service Bus naming prefixes (topic, queue, subscription) stripped from
/// a channel name so infrastructure names and code literals share one key.
const AZURE_NAME_PREFIXES: &[&str] = &["sbt-", "sbts-", "sbq-"];

/// Normalize a channel/topic name to a canonical form.
///
/// The single normalizer for the Bicep extractor and every code extractor.
/// Strips the container prefix (Topics., TopicName., etc.) and an Azure
/// Service Bus prefix (`sbt-`, `sbq-`, `sbts-`), removes hyphens and
/// underscores, and lowercases, so Bicep resource names, C# PascalCase,
/// string literals and Python SCREAMING_SNAKE produce identical keys.
///
/// # Examples
/// - `sbt-dataproxy-commands` → `channel://dataproxycommands`
/// - `Topics.DataProxyCommands` → `channel://dataproxycommands`
/// - `TopicName.ORCHESTRATOR_TRIGGERS` → `channel://orchestratortriggers`
/// - `DATAPROXY_COMMANDS` → `channel://dataproxycommands`
pub fn normalize_channel_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || !is_plausible_topic(trimmed) {
        return None;
    }

    // Strip known container prefix (Topics.X → X)
    let topic_part = strip_topic_container(trimmed);
    let topic_part = strip_azure_prefix(topic_part);

    // Remove hyphens/underscores and lowercase
    let normalized: String = topic_part
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-')
        .flat_map(|ch| ch.to_lowercase())
        .collect();

    if normalized.is_empty() {
        return None;
    }

    Some(format!("channel://{normalized}"))
}

/// A topic name never contains quote, bracket, or call-syntax characters.
/// Source text like `"x"`, `Foo()` or `Arg.Any<string>()` is an expression,
/// not a topic, so it is rejected rather than turned into a fabricated name.
fn is_plausible_topic(s: &str) -> bool {
    !s.chars().any(|ch| {
        matches!(
            ch,
            '"' | '\'' | '`' | '(' | ')' | '<' | '>' | '{' | '}' | '[' | ']' | '\n' | '\r'
        )
    })
}

/// Resolve a topic argument to a normalized `channel://` name.
///
/// A topic is derived from a *value*: a string literal's content or a
/// same-file constant (see `string_consts`). A call, parameter, unknown
/// name, foreign member access, interpolation with holes or mock matcher
/// yields `None`; a name is never derived from expression text.
pub fn resolve_topic(raw: &str, consts: &StringConsts, local: &LocalBinding) -> Option<String> {
    normalize_channel_name(&consts.resolve_arg(raw, local)?)
}

/// Strip known topic container prefix from a dotted expression.
/// "Topics.Foo" → "Foo", "TopicName.FOO_BAR" → "FOO_BAR", "Foo" → "Foo"
fn strip_topic_container(raw: &str) -> &str {
    if let Some((prefix, suffix)) = raw.split_once('.') {
        let prefix = prefix.rsplit('.').next().unwrap_or(prefix);
        if TOPIC_CONTAINERS.contains(&prefix) {
            return suffix;
        }
    }
    raw
}

/// Strip a leading Azure Service Bus prefix (`sbt-`, `sbts-`, `sbq-`), ignoring case.
fn strip_azure_prefix(name: &str) -> &str {
    AZURE_NAME_PREFIXES
        .iter()
        .find(|p| {
            name.as_bytes()
                .get(..p.len())
                .is_some_and(|head| head.eq_ignore_ascii_case(p.as_bytes()))
        })
        .map_or(name, |p| &name[p.len()..])
}

/// Check if a receiver expression looks like a message bus.
pub fn is_bus_receiver(receiver: &str) -> bool {
    let last = receiver.rsplit('.').next().unwrap_or(receiver).trim();
    if last.is_empty() {
        return false;
    }
    BUS_RECEIVER_PATTERNS.contains(&last)
}

/// Check if a method name is a publish method.
pub fn is_publish_method(name: &str) -> bool {
    PUBLISH_METHODS.contains(&name)
}

/// Check if a method name is a subscribe method.
pub fn is_subscribe_method(name: &str) -> bool {
    SUBSCRIBE_METHODS.contains(&name)
}

/// Check if a raw topic value looks like a topic container member access.
/// Returns the normalized channel name if it does.
pub fn topic_from_member_access(raw: &str) -> Option<String> {
    normalize_channel_name(raw)
}

pub fn build_publish_detail(channel: &str, raw: &str, framework: &str) -> String {
    json!({
        "channel": channel,
        "raw": raw,
        "framework": framework,
        "role": "publisher",
    })
    .to_string()
}

pub fn build_subscribe_detail(channel: &str, raw: &str, framework: &str) -> String {
    json!({
        "channel": channel,
        "raw": raw,
        "framework": framework,
        "role": "subscriber",
    })
    .to_string()
}

/// Bridge pair: given an edge kind, return the complementary kind(s) for traversal bridging.
///
/// `RPC_IMPL` bridges to *two* things: `RPC_CALL` (a cross-service caller
/// invoking this RPC) and `RPC_ROUTE` (the `.proto` definition this method
/// implements) -- unlike `HTTP_ROUTE`/`HTTP_CALL`, gRPC has a third edge
/// kind (the route/definition side) distinct from the call side, so it
/// needs both. `RPC_ROUTE` sits in the middle (callers -> route -> impl):
/// it bridges downstream to `RPC_IMPL` and upstream to `RPC_CALL`.
pub fn bridge_complement(kind: &str) -> Option<Vec<&'static str>> {
    bridge_entry(kind).map(|pairs| pairs.iter().map(|(complement, _)| *complement).collect())
}

/// The one table of bridge kinds: each kind's complement(s), each with the
/// walk direction it is crossed in (true = upstream, toward callers /
/// publishers / sources; false = downstream). Adding a bridge kind is one
/// edit here.
fn bridge_entry(kind: &str) -> Option<&'static [(&'static str, bool)]> {
    Some(match kind {
        "CHANNEL_PUBLISH" => &[("CHANNEL_SUBSCRIBE", false)],
        "CHANNEL_SUBSCRIBE" => &[("CHANNEL_PUBLISH", true)],
        "RPC_CALL" => &[("RPC_IMPL", false)],
        "RPC_IMPL" => &[("RPC_CALL", true), ("RPC_ROUTE", true)],
        "RPC_ROUTE" => &[("RPC_IMPL", false), ("RPC_CALL", true)],
        "HTTP_CALL" => &[("HTTP_ROUTE", false)],
        "HTTP_ROUTE" => &[("HTTP_CALL", true)],
        "CONFIG_SOURCE" => &[("CONFIG_READ", false)],
        "CONFIG_READ" => &[("CONFIG_SOURCE", true)],
        _ => return None,
    })
}

/// Which way a walk (or a bridged hop) runs through the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WalkDirection {
    Upstream,
    Downstream,
    /// A walk following both directions; never recorded on a hop.
    Both,
}

/// Whether crossing from a symbol holding an `edge_kind` edge to its
/// `complement` walks against caller/publisher -> callee/subscriber order. The
/// hop's parent is the symbol holding `edge_kind`, so an upstream pair means
/// the bridged symbol is the caller (issue #103).
pub fn bridge_pair_is_upstream(edge_kind: &str, complement: &str) -> bool {
    bridge_entry(edge_kind).is_some_and(|pairs| pairs.iter().any(|(c, up)| *c == complement && *up))
}

/// Whether any of `edge_kind`'s bridges runs upstream (a callee-side kind).
pub fn bridge_hop_is_reversed(edge_kind: &str) -> bool {
    bridge_entry(edge_kind).is_some_and(|pairs| pairs.iter().any(|(_, up)| *up))
}

/// The complements of `edge_kind` a `walk` may cross to: the one direction
/// gate shared by `trace_flow` and `analyze_impact` (issue #201). Upstream
/// walks cross to callers/publishers/sources, downstream walks to
/// callees/subscribers.
pub fn bridge_complements_for(edge_kind: &str, walk: WalkDirection) -> Vec<&'static str> {
    bridge_entry(edge_kind)
        .into_iter()
        .flatten()
        .filter(|(_, up)| match walk {
            WalkDirection::Both => true,
            WalkDirection::Upstream => *up,
            WalkDirection::Downstream => !*up,
        })
        .map(|(c, _)| *c)
        .collect()
}

/// Whether a `walk` may cross any bridge from an `edge_kind` edge.
pub fn bridge_crossing_allowed(edge_kind: &str, walk: WalkDirection) -> bool {
    !bridge_complements_for(edge_kind, walk).is_empty()
}

/// Returns true for an edge kind that `Db::insert_edges` always keeps a live
/// edge for, even when unresolved -- issue #79's exemption from "every other
/// kind's unresolved reference lives only in `unresolved_references`". Not
/// one uniform reason: two different groups of consumers need the edge's
/// `target_qualname` (or `detail`) text to still be there at rest, not just
/// a resolved `target_symbol_id`:
///
/// - The three actual **Bridge Edge** pairs/triple (CONTEXT.md glossary):
///   `RPC_CALL`↔`RPC_IMPL` (plus `RPC_ROUTE`, the `.proto` definition side --
///   `channel::bridge_complement`'s doc), `CHANNEL_PUBLISH`↔`CHANNEL_SUBSCRIBE`,
///   `HTTP_CALL`↔`HTTP_ROUTE`. Their target is a cross-process join key, not
///   necessarily a symbol in this graph; `trace_flow`'s traversal bridging
///   (`traversal.rs`, `bridge_complement`) looks up the complementary side by
///   `target_qualname` text regardless of resolution.
/// - `CONFIG_SOURCE`/`CONFIG_READ`/`CONFIG_BIND`: not a Bridge Edge pair (no
///   `bridge_complement` entry for `CONFIG_BIND`, and traversal never crosses
///   these three to each other), but their target is a config key/secret URI
///   (`secret://...`, `env://...`), not a symbol either.
///   `graph_query::source_symbols_for_config_uri` looks these up by that URI
///   text so a config-URI-rooted `trace_flow`/`analyze_impact` can find
///   their source symbols before any symbol-based resolution applies.
/// - `XREF`: unlike the above, its target usually *is* a real, already-known
///   symbol qualname (`indexer::xref::collect_xref_edges` reads it from a
///   same-graph-version symbol index) -- but confidence-gated, evidence-based
///   consumers (`model::xref_is_traversable`, `context.rs`'s cross-ref
///   listing) read its `target_qualname`/`detail` text directly off the edge
///   itself regardless of whether `target_symbol_id` bound, so the edge (and
///   that text) must survive even an Unresolved/Ambiguous outcome.
pub fn is_bridge_edge_kind(kind: &str) -> bool {
    matches!(
        kind,
        "RPC_IMPL"
            | "RPC_CALL"
            | "RPC_ROUTE"
            | "HTTP_ROUTE"
            | "HTTP_CALL"
            | "CHANNEL_PUBLISH"
            | "CHANNEL_SUBSCRIBE"
            | "CONFIG_SOURCE"
            | "CONFIG_READ"
            | "CONFIG_BIND"
            | "XREF"
    )
}

/// Determine the boundary type string for a bridged edge kind.
pub fn boundary_type_for_kind(kind: &str) -> &'static str {
    match kind {
        "CHANNEL_PUBLISH" | "CHANNEL_SUBSCRIBE" => "message_bus",
        "RPC_CALL" | "RPC_IMPL" | "RPC_ROUTE" => "grpc",
        "HTTP_CALL" | "HTTP_ROUTE" => "http",
        "CONFIG_SOURCE" | "CONFIG_READ" => "config",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_csharp_pascal_case() {
        assert_eq!(
            normalize_channel_name("Topics.OrchestratorTriggers"),
            Some("channel://orchestratortriggers".to_string())
        );
    }

    #[test]
    fn normalize_python_screaming_snake() {
        assert_eq!(
            normalize_channel_name("TopicName.ORCHESTRATOR_TRIGGERS"),
            Some("channel://orchestratortriggers".to_string())
        );
    }

    #[test]
    fn normalize_csharp_data_proxy() {
        assert_eq!(
            normalize_channel_name("Topics.DataProxyCommands"),
            Some("channel://dataproxycommands".to_string())
        );
    }

    #[test]
    fn normalize_python_data_proxy() {
        assert_eq!(
            normalize_channel_name("TopicName.DATAPROXY_COMMANDS"),
            Some("channel://dataproxycommands".to_string())
        );
    }

    #[test]
    fn normalize_bare_name() {
        assert_eq!(
            normalize_channel_name("DataProxyCommands"),
            Some("channel://dataproxycommands".to_string())
        );
    }

    #[test]
    fn normalize_empty() {
        assert_eq!(normalize_channel_name(""), None);
    }

    #[test]
    fn normalize_rejects_expression_text() {
        for raw in ["\"orders\"", "Foo()", "Arg.Any<string>()", "'x'"] {
            assert_eq!(normalize_channel_name(raw), None, "{raw}");
        }
    }

    #[test]
    fn bus_receiver_detection() {
        assert!(is_bus_receiver("_bus"));
        assert!(is_bus_receiver("self._messages"));
        assert!(is_bus_receiver("_messageBus"));
        assert!(!is_bus_receiver("_client"));
        assert!(!is_bus_receiver("httpClient"));
    }

    #[test]
    fn publish_subscribe_methods() {
        assert!(is_publish_method("PublishAsync"));
        assert!(is_publish_method("publish"));
        assert!(is_publish_method("emit"));
        assert!(!is_publish_method("SubscribeAsync"));

        assert!(is_subscribe_method("SubscribeAsync"));
        assert!(is_subscribe_method("subscribe"));
        assert!(is_subscribe_method("on"));
        assert!(!is_subscribe_method("PublishAsync"));
    }

    #[test]
    fn bridge_pairs() {
        assert_eq!(
            bridge_complement("CHANNEL_PUBLISH"),
            Some(vec!["CHANNEL_SUBSCRIBE"])
        );
        assert_eq!(
            bridge_complement("CHANNEL_SUBSCRIBE"),
            Some(vec!["CHANNEL_PUBLISH"])
        );
        assert_eq!(bridge_complement("RPC_CALL"), Some(vec!["RPC_IMPL"]));
        assert_eq!(
            bridge_complement("RPC_IMPL"),
            Some(vec!["RPC_CALL", "RPC_ROUTE"])
        );
        assert_eq!(
            bridge_complement("RPC_ROUTE"),
            Some(vec!["RPC_IMPL", "RPC_CALL"])
        );
        assert_eq!(bridge_complement("HTTP_CALL"), Some(vec!["HTTP_ROUTE"]));
        assert_eq!(bridge_complement("HTTP_ROUTE"), Some(vec!["HTTP_CALL"]));
        assert_eq!(
            bridge_complement("CONFIG_SOURCE"),
            Some(vec!["CONFIG_READ"])
        );
        assert_eq!(
            bridge_complement("CONFIG_READ"),
            Some(vec!["CONFIG_SOURCE"])
        );
        assert_eq!(bridge_complement("CALLS"), None);
    }

    #[test]
    fn normalize_azure_prefixes_and_hyphens() {
        for (raw, want) in [
            ("sbt-x-y", "channel://xy"),
            ("sbq-x", "channel://x"),
            ("sbts-x", "channel://x"),
            ("sbt-dataproxy-commands", "channel://dataproxycommands"),
            (
                "sbt-orchestrator-triggers",
                "channel://orchestratortriggers",
            ),
            ("sbq-dead-letter", "channel://deadletter"),
            ("sbts-my-subscription", "channel://mysubscription"),
            ("my-topic-name", "channel://mytopicname"),
            ("MyTopic", "channel://mytopic"),
            ("Topics.XY", "channel://xy"),
            ("TOPIC_NAME", "channel://topicname"),
        ] {
            assert_eq!(normalize_channel_name(raw), Some(want.to_string()), "{raw}");
        }
    }

    #[test]
    fn normalize_azure_prefix_is_case_insensitive() {
        assert_eq!(
            normalize_channel_name("SBT-Foo"),
            Some("channel://foo".to_string())
        );
    }

    #[test]
    fn normalize_empty_inputs_yield_none() {
        assert_eq!(normalize_channel_name(""), None);
        assert_eq!(normalize_channel_name("   "), None);
        assert_eq!(normalize_channel_name("sbt-"), None);
    }
}
