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

/// Normalize a channel/topic name to a canonical form.
///
/// Strips the container prefix (Topics., TopicName., etc.), removes underscores,
/// and lowercases everything so that C# PascalCase and Python SCREAMING_SNAKE
/// produce identical keys.
///
/// # Examples
/// - `Topics.OrchestratorTriggers` → `channel://orchestratortriggers`
/// - `TopicName.ORCHESTRATOR_TRIGGERS` → `channel://orchestratortriggers`
/// - `Topics.DataProxyCommands` → `channel://dataproxycommands`
/// - `DATAPROXY_COMMANDS` → `channel://dataproxycommands`
pub fn normalize_channel_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    // Strip known container prefix (Topics.X → X)
    let topic_part = strip_topic_container(trimmed);
    if topic_part.is_empty() {
        return None;
    }

    // Remove underscores and lowercase
    let normalized: String = topic_part
        .chars()
        .filter(|ch| *ch != '_')
        .flat_map(|ch| ch.to_lowercase())
        .collect();

    if normalized.is_empty() {
        return None;
    }

    Some(format!("channel://{normalized}"))
}

/// Strip known topic container prefix from a dotted expression.
/// "Topics.Foo" → "Foo", "TopicName.FOO_BAR" → "FOO_BAR", "Foo" → "Foo"
fn strip_topic_container(raw: &str) -> &str {
    if let Some((prefix, suffix)) = raw.split_once('.') {
        let prefix = prefix.rsplit('.').next().unwrap_or(prefix);
        if TOPIC_CONTAINERS.iter().any(|c| *c == prefix) {
            return suffix;
        }
    }
    raw
}

/// Check if a receiver expression looks like a message bus.
pub fn is_bus_receiver(receiver: &str) -> bool {
    let last = receiver.rsplit('.').next().unwrap_or(receiver).trim();
    if last.is_empty() {
        return false;
    }
    BUS_RECEIVER_PATTERNS.iter().any(|pat| last == *pat)
}

/// Check if a method name is a publish method.
pub fn is_publish_method(name: &str) -> bool {
    PUBLISH_METHODS.iter().any(|m| *m == name)
}

/// Check if a method name is a subscribe method.
pub fn is_subscribe_method(name: &str) -> bool {
    SUBSCRIBE_METHODS.iter().any(|m| *m == name)
}

/// Check if a raw topic value looks like a topic container member access.
/// Returns the normalized channel name if it does.
pub fn topic_from_member_access(raw: &str) -> Option<String> {
    normalize_channel_name(raw)
}

pub fn build_publish_detail(
    channel: &str,
    raw: &str,
    framework: &str,
) -> String {
    json!({
        "channel": channel,
        "raw": raw,
        "framework": framework,
        "role": "publisher",
    })
    .to_string()
}

pub fn build_subscribe_detail(
    channel: &str,
    raw: &str,
    framework: &str,
) -> String {
    json!({
        "channel": channel,
        "raw": raw,
        "framework": framework,
        "role": "subscriber",
    })
    .to_string()
}

/// Bridge pair: given an edge kind, return the complementary kind(s) for traversal bridging.
pub fn bridge_complement(kind: &str) -> Option<&'static [&'static str]> {
    match kind {
        "CHANNEL_PUBLISH" => Some(&["CHANNEL_SUBSCRIBE"]),
        "CHANNEL_SUBSCRIBE" => Some(&["CHANNEL_PUBLISH"]),
        "RPC_CALL" => Some(&["RPC_IMPL"]),
        "RPC_IMPL" => Some(&["RPC_CALL"]),
        "HTTP_CALL" => Some(&["HTTP_ROUTE"]),
        "HTTP_ROUTE" => Some(&["HTTP_CALL"]),
        _ => None,
    }
}

/// Determine the boundary type string for a bridged edge kind.
pub fn boundary_type_for_kind(kind: &str) -> &'static str {
    match kind {
        "CHANNEL_PUBLISH" | "CHANNEL_SUBSCRIBE" => "message_bus",
        "RPC_CALL" | "RPC_IMPL" | "RPC_ROUTE" => "grpc",
        "HTTP_CALL" | "HTTP_ROUTE" => "http",
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
        assert_eq!(bridge_complement("CHANNEL_PUBLISH"), Some(&["CHANNEL_SUBSCRIBE"] as &[&str]));
        assert_eq!(bridge_complement("CHANNEL_SUBSCRIBE"), Some(&["CHANNEL_PUBLISH"] as &[&str]));
        assert_eq!(bridge_complement("RPC_CALL"), Some(&["RPC_IMPL"] as &[&str]));
        assert_eq!(bridge_complement("RPC_IMPL"), Some(&["RPC_CALL"] as &[&str]));
        assert_eq!(bridge_complement("HTTP_CALL"), Some(&["HTTP_ROUTE"] as &[&str]));
        assert_eq!(bridge_complement("HTTP_ROUTE"), Some(&["HTTP_CALL"] as &[&str]));
        assert_eq!(bridge_complement("CALLS"), None);
    }
}
