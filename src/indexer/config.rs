use serde_json::json;

pub const CONFIG_SOURCE_KIND: &str = "CONFIG_SOURCE";
pub const CONFIG_READ_KIND: &str = "CONFIG_READ";
pub const CONFIG_BIND_KIND: &str = "CONFIG_BIND";

/// Normalize an env var name to a canonical URI: `env://VARNAME`
/// Trims whitespace, rejects empty, uppercases.
pub fn normalize_env_var_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let upper = trimmed.to_uppercase();
    Some(format!("env://{upper}"))
}

/// Normalize a K8s secret name to a canonical URI: `secret://name`
/// Trims whitespace, rejects empty, lowercases (preserves hyphens).
pub fn normalize_secret_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_lowercase();
    Some(format!("secret://{lower}"))
}

pub fn build_config_source_detail(
    source_type: &str,
    config_uri: &str,
    raw: &str,
    extra: Option<&serde_json::Value>,
) -> String {
    let mut obj = json!({
        "config_uri": config_uri,
        "raw": raw,
        "source_type": source_type,
        "role": "source",
    });
    if let Some(extra) = extra
        && let Some(map) = extra.as_object()
    {
        for (k, v) in map {
            obj[k] = v.clone();
        }
    }
    obj.to_string()
}

pub fn build_config_read_detail(
    source_type: &str,
    config_uri: &str,
    raw: &str,
    framework: &str,
) -> String {
    json!({
        "config_uri": config_uri,
        "raw": raw,
        "source_type": source_type,
        "framework": framework,
        "role": "reader",
    })
    .to_string()
}

/// Check if a string is a config URI (secret://, env://).
pub fn is_config_uri(s: &str) -> bool {
    s.starts_with("secret://") || s.starts_with("env://")
}

pub fn build_config_bind_detail(
    options_type: &str,
    wrapper_type: &str,
    binding_kind: &str,
    framework: &str,
) -> String {
    json!({
        "options_type": options_type,
        "wrapper_type": wrapper_type,
        "binding_kind": binding_kind,
        "framework": framework,
        "role": "consumer",
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_env_var_basic() {
        assert_eq!(
            normalize_env_var_name("DATABASE_URL"),
            Some("env://DATABASE_URL".to_string())
        );
    }

    #[test]
    fn normalize_env_var_lowercased() {
        assert_eq!(
            normalize_env_var_name("database_url"),
            Some("env://DATABASE_URL".to_string())
        );
    }

    #[test]
    fn normalize_env_var_whitespace() {
        assert_eq!(
            normalize_env_var_name("  FOO_BAR  "),
            Some("env://FOO_BAR".to_string())
        );
    }

    #[test]
    fn normalize_env_var_empty() {
        assert_eq!(normalize_env_var_name(""), None);
        assert_eq!(normalize_env_var_name("   "), None);
    }

    #[test]
    fn normalize_secret_basic() {
        assert_eq!(
            normalize_secret_name("datamgr-db-conn"),
            Some("secret://datamgr-db-conn".to_string())
        );
    }

    #[test]
    fn normalize_secret_uppercased() {
        assert_eq!(
            normalize_secret_name("MySecret"),
            Some("secret://mysecret".to_string())
        );
    }

    #[test]
    fn normalize_secret_empty() {
        assert_eq!(normalize_secret_name(""), None);
        assert_eq!(normalize_secret_name("   "), None);
    }

    #[test]
    fn is_config_uri_detects_schemes() {
        assert!(is_config_uri("secret://datamgr-db-conn-str"));
        assert!(is_config_uri("env://DATABASE_URL"));
        assert!(is_config_uri("env://DATABASE"));
        assert!(!is_config_uri("Foo.Bar.Baz"));
        assert!(!is_config_uri("DatabaseOptions"));
        assert!(!is_config_uri(""));
    }

    #[test]
    fn build_source_detail_json() {
        let detail = build_config_source_detail("env", "env://FOO", "FOO", None);
        let parsed: serde_json::Value = serde_json::from_str(&detail).unwrap();
        assert_eq!(parsed["config_uri"], "env://FOO");
        assert_eq!(parsed["role"], "source");
    }

    #[test]
    fn build_read_detail_json() {
        let detail = build_config_read_detail("env", "env://FOO", "FOO", "dotnet");
        let parsed: serde_json::Value = serde_json::from_str(&detail).unwrap();
        assert_eq!(parsed["config_uri"], "env://FOO");
        assert_eq!(parsed["framework"], "dotnet");
        assert_eq!(parsed["role"], "reader");
    }
}
