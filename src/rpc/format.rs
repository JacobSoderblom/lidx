use serde_json::Value;

use super::{RpcError, RpcResponse};

/// Methods whose params already use `max_bytes` for their own internal
/// content budget (source snippet sizing, section allocation, etc.), with
/// their own truncation bookkeeping (e.g. explain_symbol's budget.truncated).
/// For these, `max_bytes` must NOT also be read here as the outer
/// response-size cap: that would silently apply the same number a second
/// time via generic array-slicing, on top of (and uncoordinated with) the
/// method's own budget -- reintroducing the exact "truncation the caller
/// can't see" bug this module exists to close, just one layer out.
const METHODS_WITH_OWN_MAX_BYTES: &[&str] = &[
    "repo_map",
    "analyze_diff",
    "orient",
    "gather_context",
    "explain_symbol",
    "trace_flow",
];

/// Extract the response byte budget from params. Supports three names:
/// `max_response_bytes` (preferred, most explicit), `max_bytes` (alias --
/// this is what fixes analyze_impact silently ignoring `max_bytes`, since it
/// has no params field of its own for it; skipped for methods in
/// `METHODS_WITH_OWN_MAX_BYTES`, see there), then `max_tokens` (converted at
/// ~4 bytes/token).
pub(super) fn extract_max_response_bytes(
    method: &str,
    params: &serde_json::Value,
) -> Option<usize> {
    params
        .get("max_response_bytes")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .or_else(|| {
            if METHODS_WITH_OWN_MAX_BYTES.contains(&method) {
                None
            } else {
                params
                    .get("max_bytes")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as usize)
            }
        })
        .or_else(|| {
            params
                .get("max_tokens")
                .and_then(|v| v.as_u64())
                .map(|v| (v as usize) * 4) // ~4 bytes per token
        })
}

/// Truncate a JSON response to fit within a byte budget.
/// If value is an array, removes tail elements.
/// If value is an object with common array fields, truncates those arrays.
/// Returns (truncated_value, was_truncated, total_available)
pub(super) fn truncate_response(
    value: serde_json::Value,
    max_bytes: usize,
) -> (serde_json::Value, bool, Option<usize>) {
    let serialized = serde_json::to_string(&value).unwrap_or_default();
    if serialized.len() <= max_bytes {
        return (value, false, None);
    }

    match value {
        serde_json::Value::Array(arr) => {
            // Binary search for how many elements fit
            let original_len = arr.len();
            let mut low = 0usize;
            let mut high = arr.len();
            while low < high {
                let mid = (low + high).div_ceil(2);
                let slice = serde_json::Value::Array(arr[..mid].to_vec());
                let size = serde_json::to_string(&slice).unwrap_or_default().len();
                if size <= max_bytes {
                    low = mid;
                } else {
                    high = mid - 1;
                }
            }
            (
                serde_json::Value::Array(arr[..low].to_vec()),
                true,
                Some(original_len),
            )
        }
        serde_json::Value::Object(mut map) => {
            // Check if this object has a top-level array field that we can track
            let mut total_available: Option<usize> = None;

            // Look for common array fields and truncate them
            let array_keys: Vec<String> = map
                .iter()
                .filter(|(_, v)| v.is_array())
                .map(|(k, _)| k.clone())
                .collect();

            if array_keys.is_empty() {
                return (serde_json::Value::Object(map), false, None);
            }

            // If there's a single top-level array (common pattern), capture its length
            if array_keys.len() == 1
                && let Some(serde_json::Value::Array(arr)) = map.get(&array_keys[0])
            {
                total_available = Some(arr.len());
            }

            // Truncate each array field proportionally
            let overhead = {
                let mut temp = map.clone();
                for key in &array_keys {
                    temp.insert(key.clone(), serde_json::Value::Array(vec![]));
                }
                serde_json::to_string(&serde_json::Value::Object(temp))
                    .unwrap_or_default()
                    .len()
            };

            let available = max_bytes.saturating_sub(overhead);
            let per_array = available / array_keys.len().max(1);

            let mut did_truncate = false;
            for key in &array_keys {
                if let Some(serde_json::Value::Array(arr)) = map.remove(key) {
                    let (truncated_arr, was_truncated, _) =
                        truncate_response(serde_json::Value::Array(arr), per_array);
                    did_truncate = did_truncate || was_truncated;
                    map.insert(key.clone(), truncated_arr);
                }
            }

            // A handler's own result can carry a sibling `truncated` bool that
            // it computed for its own reasons (e.g. analyze_impact's internal
            // depth/limit truncation). Slicing an array field here is *also*
            // truncation -- so OR it in rather than leaving a stale `false`
            // sitting next to an array that was just cut. Never flip a true
            // back to false: this only ever adds honesty, never removes it.
            if did_truncate
                && let Some(serde_json::Value::Bool(existing)) = map.get("truncated")
                && !*existing
            {
                map.insert("truncated".to_string(), serde_json::Value::Bool(true));
            }

            (
                serde_json::Value::Object(map),
                did_truncate,
                total_available,
            )
        }
        other => (other, false, None),
    }
}

pub(super) fn error_response(id: Value, message: &str) -> RpcResponse {
    RpcResponse {
        id,
        result: None,
        error: Some(RpcError {
            message: message.to_string(),
        }),
    }
}

pub(super) fn parse_value(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_string()))
}

pub(super) fn apply_field_filters(
    value: Value,
    summary: bool,
    fields: Option<&[String]>,
    summary_fields: &[&str],
) -> Value {
    if let Some(fields) = fields {
        return filter_fields(value, fields.iter().map(|s| s.as_str()));
    }
    if summary {
        return filter_fields(value, summary_fields.iter().copied());
    }
    value
}

pub(super) fn filter_fields<'a, I>(value: Value, fields: I) -> Value
where
    I: IntoIterator<Item = &'a str>,
{
    let Value::Object(mut map) = value else {
        return value;
    };
    let mut filtered = serde_json::Map::new();
    for key in fields {
        if let Some(value) = map.remove(key) {
            filtered.insert(key.to_string(), value);
        }
    }
    Value::Object(filtered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- extract_max_response_bytes ---

    #[test]
    fn extract_max_response_bytes_from_max_response_bytes() {
        let params = json!({"max_response_bytes": 5000});
        assert_eq!(
            extract_max_response_bytes("analyze_impact", &params),
            Some(5000)
        );
    }

    #[test]
    fn extract_max_response_bytes_from_max_tokens() {
        let params = json!({"max_tokens": 1000});
        assert_eq!(
            extract_max_response_bytes("analyze_impact", &params),
            Some(4000)
        );
    }

    #[test]
    fn extract_max_response_bytes_prefers_max_response_bytes() {
        let params = json!({"max_response_bytes": 5000, "max_tokens": 1000});
        assert_eq!(
            extract_max_response_bytes("analyze_impact", &params),
            Some(5000)
        );
    }

    #[test]
    fn extract_max_response_bytes_none_when_absent() {
        let params = json!({"other": 42});
        assert_eq!(extract_max_response_bytes("analyze_impact", &params), None);
    }

    #[test]
    fn extract_max_response_bytes_from_max_bytes_alias() {
        // analyze_impact has no max_bytes field of its own, so the outer
        // wrapper is the only place `max_bytes` can take effect for it.
        let params = json!({"max_bytes": 12345});
        assert_eq!(
            extract_max_response_bytes("analyze_impact", &params),
            Some(12345)
        );
    }

    #[test]
    fn extract_max_response_bytes_prefers_max_response_bytes_over_max_bytes() {
        let params = json!({"max_response_bytes": 5000, "max_bytes": 9999});
        assert_eq!(
            extract_max_response_bytes("analyze_impact", &params),
            Some(5000)
        );
    }

    #[test]
    fn extract_max_response_bytes_prefers_max_bytes_over_max_tokens() {
        let params = json!({"max_bytes": 3000, "max_tokens": 1000});
        assert_eq!(
            extract_max_response_bytes("analyze_impact", &params),
            Some(3000)
        );
    }

    #[test]
    fn extract_max_response_bytes_ignores_max_bytes_alias_for_methods_with_their_own() {
        // gather_context (and repo_map, explain_symbol, trace_flow,
        // analyze_diff, orient) already use `max_bytes` for their own
        // internal content budget. If the outer wrapper also read it as the
        // response-size cap, a caller's internal-budget request would
        // silently double as a second, uncoordinated truncation layer --
        // this regressed real gather_context tests during development
        // (fixed by scoping the alias to methods that lack their own
        // max_bytes param).
        let params = json!({"max_bytes": 50});
        for method in [
            "repo_map",
            "analyze_diff",
            "orient",
            "gather_context",
            "explain_symbol",
            "trace_flow",
        ] {
            assert_eq!(
                extract_max_response_bytes(method, &params),
                None,
                "{method} should not have max_bytes read as the outer response cap"
            );
        }
        // Sanity: the same params DO resolve for a method without its own
        // max_bytes, proving the alias itself still works.
        assert_eq!(
            extract_max_response_bytes("analyze_impact", &params),
            Some(50)
        );
    }

    // --- truncate_response ---

    #[test]
    fn truncate_response_small_array_no_truncation() {
        let arr = json!([1, 2, 3]);
        let (result, truncated, total) = truncate_response(arr.clone(), 10000);
        assert_eq!(result, arr);
        assert!(!truncated);
        assert_eq!(total, None);
    }

    #[test]
    fn truncate_response_array_exceeds_budget() {
        let arr: Vec<serde_json::Value> = (0..100).map(|i| json!({"x": i})).collect();
        let val = Value::Array(arr);
        let (result, truncated, total) = truncate_response(val, 50);
        assert!(truncated);
        assert_eq!(total, Some(100));
        let result_arr = result.as_array().unwrap();
        assert!(result_arr.len() < 100);
        // Result serialized should fit within budget
        let size = serde_json::to_string(&result).unwrap().len();
        assert!(size <= 50, "truncated result {} > budget 50", size);
    }

    #[test]
    fn truncate_response_single_huge_element() {
        let val = json!([{"data": "x".repeat(1000)}]);
        let (result, truncated, total) = truncate_response(val, 10);
        assert!(truncated);
        assert_eq!(total, Some(1));
        assert_eq!(result.as_array().unwrap().len(), 0);
    }

    #[test]
    fn truncate_response_object_corrects_stale_false_truncated_sibling() {
        // Regression for the analyze_impact self-contradiction: a handler
        // (e.g. src/impact/) computes its own `truncated: false` because ITS
        // internal depth/limit budget wasn't hit, but the outer response-size
        // budget here still has to cut the `affected` array. The stale
        // `false` must not survive next to an array that just got sliced.
        let big_affected: Vec<serde_json::Value> = (0..182).map(|i| json!({"id": i})).collect();
        let val = json!({"affected": big_affected, "truncated": false});
        let (result, was_truncated, _) = truncate_response(val, 500);
        assert!(was_truncated);
        let obj = result.as_object().unwrap();
        assert!(
            obj["affected"].as_array().unwrap().len() < 182,
            "affected array should have been sliced"
        );
        assert_eq!(
            obj["truncated"],
            json!(true),
            "sibling `truncated` must be corrected to true once the array was cut, \
             not left as the stale false the handler wrote before slicing"
        );
    }

    #[test]
    fn truncate_response_object_leaves_true_truncated_sibling_alone() {
        // If the handler already knew it truncated, we must never flip that
        // back to false -- OR-ing only ever adds honesty, never removes it.
        let items: Vec<serde_json::Value> = (0..5).map(|i| json!({"id": i})).collect();
        let val = json!({"affected": items, "truncated": true});
        let (result, _, _) = truncate_response(val, 10000);
        assert_eq!(result["truncated"], json!(true));
    }

    #[test]
    fn truncate_response_object_no_truncated_field_unaffected() {
        // Objects without a `truncated` sibling (e.g. explain_symbol's nested
        // shape) must not gain one out of nowhere.
        let big: Vec<serde_json::Value> = (0..100).map(|i| json!({"x": i})).collect();
        let val = json!({"items": big});
        let (result, was_truncated, _) = truncate_response(val, 50);
        assert!(was_truncated);
        assert!(result.as_object().unwrap().get("truncated").is_none());
    }

    #[test]
    fn truncate_response_object_with_array_field() {
        let items: Vec<serde_json::Value> = (0..50).map(|i| json!({"n": i})).collect();
        let val = json!({"results": items, "meta": "info"});
        let (result, truncated, total) = truncate_response(val, 100);
        assert!(truncated);
        assert!(total.is_some());
        assert!(result.get("results").unwrap().as_array().unwrap().len() < 50);
    }

    #[test]
    fn truncate_response_object_no_arrays() {
        let val = json!({"a": 1, "b": "long string but not array"});
        let (result, truncated, _) = truncate_response(val.clone(), 10);
        // No arrays to truncate, returns as-is even though it exceeds
        assert!(!truncated);
        assert_eq!(result, val);
    }

    #[test]
    fn truncate_response_empty_array() {
        let val = json!([]);
        let (result, truncated, _) = truncate_response(val, 1);
        // "[]" is 2 bytes > 1, but array is empty so nothing to truncate
        assert_eq!(result.as_array().unwrap().len(), 0);
        assert!(truncated); // technically enters truncation path
        assert_eq!(result, json!([]));
    }

    // --- parse_value ---

    #[test]
    fn parse_value_valid_json() {
        assert_eq!(parse_value("42"), json!(42));
        assert_eq!(parse_value("\"hello\""), json!("hello"));
        assert_eq!(parse_value("[1,2]"), json!([1, 2]));
    }

    #[test]
    fn parse_value_invalid_json_becomes_string() {
        assert_eq!(parse_value("not json"), json!("not json"));
        assert_eq!(parse_value(""), json!(""));
    }

    // --- error_response ---

    #[test]
    fn error_response_structure() {
        let resp = error_response(json!(1), "boom");
        assert_eq!(resp.id, json!(1));
        assert!(resp.result.is_none());
        assert_eq!(resp.error.as_ref().unwrap().message, "boom");
    }

    // --- filter_fields / apply_field_filters ---

    #[test]
    fn filter_fields_selects_named_fields() {
        let val = json!({"a": 1, "b": 2, "c": 3});
        let result = filter_fields(val, ["a", "c"]);
        let obj = result.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert_eq!(obj.get("a").unwrap(), &json!(1));
        assert_eq!(obj.get("c").unwrap(), &json!(3));
    }

    #[test]
    fn filter_fields_missing_field_ignored() {
        let val = json!({"a": 1});
        let result = filter_fields(val, ["a", "nonexistent"]);
        let obj = result.as_object().unwrap();
        assert_eq!(obj.len(), 1);
    }

    #[test]
    fn filter_fields_non_object_returns_as_is() {
        let val = json!(42);
        assert_eq!(filter_fields(val, ["a"]), json!(42));

        let val = json!([1, 2]);
        assert_eq!(filter_fields(val, ["a"]), json!([1, 2]));
    }

    #[test]
    fn apply_field_filters_with_explicit_fields() {
        let val = json!({"a": 1, "b": 2, "c": 3});
        let fields = vec!["a".to_string(), "c".to_string()];
        let result = apply_field_filters(val, false, Some(&fields), &["b"]);
        let obj = result.as_object().unwrap();
        // Explicit fields take precedence over summary
        assert_eq!(obj.len(), 2);
        assert!(obj.contains_key("a"));
        assert!(obj.contains_key("c"));
    }

    #[test]
    fn apply_field_filters_summary_mode() {
        let val = json!({"a": 1, "b": 2, "c": 3});
        let result = apply_field_filters(val, true, None, &["b"]);
        let obj = result.as_object().unwrap();
        assert_eq!(obj.len(), 1);
        assert!(obj.contains_key("b"));
    }

    #[test]
    fn apply_field_filters_no_filters() {
        let val = json!({"a": 1, "b": 2});
        let result = apply_field_filters(val.clone(), false, None, &["a"]);
        assert_eq!(result, val);
    }
}
