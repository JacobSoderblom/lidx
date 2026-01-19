use crate::indexer::xref;
use serde_json::json;

pub const HTTP_ROUTE_KIND: &str = "HTTP_ROUTE";
pub const HTTP_CALL_KIND: &str = "HTTP_CALL";
pub const PAGE_ROUTE_KIND: &str = "PAGE_ROUTE";
pub const HTTP_ANY: &str = "ANY";

const HTTP_METHODS: &[&str] = &["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"];

pub fn normalize_method(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut upper = trimmed.trim_matches('"').to_ascii_uppercase();
    if upper == "ALL" || upper == "ANY" {
        return Some(HTTP_ANY.to_string());
    }
    if HTTP_METHODS.iter().any(|method| *method == upper) {
        return Some(upper);
    }
    if upper.ends_with("ASYNC") && upper.len() > 5 {
        upper.truncate(upper.len() - 5);
        if HTTP_METHODS.iter().any(|method| *method == upper) {
            return Some(upper);
        }
    }
    None
}

pub fn normalize_path(raw: &str) -> Option<String> {
    xref::normalize_route_literal(raw)
}

pub fn build_route_detail(
    method: &str,
    normalized_path: &str,
    raw_path: &str,
    framework: &str,
) -> String {
    json!({
        "method": method,
        "path": normalized_path,
        "raw": raw_path,
        "framework": framework,
    })
    .to_string()
}

pub fn build_call_detail(
    method: &str,
    normalized_path: &str,
    raw_path: &str,
    client: &str,
) -> String {
    json!({
        "method": method,
        "path": normalized_path,
        "raw": raw_path,
        "client": client,
    })
    .to_string()
}

pub fn join_paths(prefix: &str, suffix: &str) -> String {
    let mut left = prefix.trim().to_string();
    let mut right = suffix.trim().to_string();
    if left.is_empty() {
        left = "/".to_string();
    }
    if right.is_empty() {
        right = "/".to_string();
    }
    if !left.starts_with('/') {
        left = format!("/{left}");
    }
    let left = left.trim_end_matches('/');
    let right = right.trim_start_matches('/');
    if left.is_empty() {
        if right.is_empty() {
            "/".to_string()
        } else {
            format!("/{right}")
        }
    } else if right.is_empty() {
        left.to_string()
    } else {
        format!("{left}/{right}")
    }
}
