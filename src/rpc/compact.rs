/// Convert a Symbol JSON value to compact format by keeping only essential fields
pub fn compact_symbol_value(symbol_value: &serde_json::Value) -> serde_json::Value {
    let keep_fields = [
        "id",
        "kind",
        "name",
        "qualname",
        "file_path",
        "start_line",
        "signature",
    ];
    if let serde_json::Value::Object(map) = symbol_value {
        let compact: serde_json::Map<String, serde_json::Value> = map
            .iter()
            .filter(|(k, _)| keep_fields.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        serde_json::Value::Object(compact)
    } else {
        symbol_value.clone()
    }
}

/// Apply compact format to a response value by converting all symbol objects to compact form.
pub(super) fn apply_compact_format(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(
                arr.into_iter()
                    .map(|item| {
                        if let serde_json::Value::Object(ref map) = item {
                            // If it looks like a symbol (has qualname field), compact it
                            if map.contains_key("qualname") {
                                return compact_symbol_value(&item);
                            }
                            // If it has a "symbol" field, compact that
                            if map.contains_key("symbol") {
                                let mut new_map = map.clone();
                                if let Some(sym) = new_map.get("symbol") {
                                    new_map.insert("symbol".to_string(), compact_symbol_value(sym));
                                }
                                return serde_json::Value::Object(new_map);
                            }
                        }
                        item
                    })
                    .collect(),
            )
        }
        serde_json::Value::Object(mut map) => {
            // Process known array fields
            for key in [
                "results", "nodes", "incoming", "outgoing", "edges", "trace", "items", "affected",
            ] {
                if let Some(arr) = map.remove(key) {
                    map.insert(key.to_string(), apply_compact_format(arr));
                }
            }
            // Process symbol field if present
            if let Some(sym) = map.remove("symbol") {
                if sym.is_object() && sym.get("qualname").is_some() {
                    map.insert("symbol".to_string(), compact_symbol_value(&sym));
                } else {
                    map.insert("symbol".to_string(), sym);
                }
            }
            // Process start/end symbol fields (trace_flow)
            for key in ["start", "end"] {
                if let Some(sym) = map.remove(key) {
                    if sym.is_object() && sym.get("qualname").is_some() {
                        map.insert(key.to_string(), compact_symbol_value(&sym));
                    } else {
                        map.insert(key.to_string(), sym);
                    }
                }
            }
            serde_json::Value::Object(map)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compact_symbol_keeps_essential_fields() {
        let sym = json!({
            "id": 1,
            "kind": "function",
            "name": "foo",
            "qualname": "mod::foo",
            "file_path": "src/lib.rs",
            "start_line": 10,
            "signature": "fn foo()",
            "end_line": 20,
            "docstring": "docs",
            "start_byte": 100,
        });
        let compact = compact_symbol_value(&sym);
        let obj = compact.as_object().unwrap();
        assert_eq!(obj.len(), 7);
        assert!(obj.contains_key("id"));
        assert!(obj.contains_key("qualname"));
        assert!(!obj.contains_key("end_line"));
        assert!(!obj.contains_key("docstring"));
        assert!(!obj.contains_key("start_byte"));
    }

    #[test]
    fn compact_symbol_non_object_returns_clone() {
        let val = json!("just a string");
        assert_eq!(compact_symbol_value(&val), val);

        let val = json!(42);
        assert_eq!(compact_symbol_value(&val), val);

        let val = json!(null);
        assert_eq!(compact_symbol_value(&val), val);
    }

    #[test]
    fn compact_symbol_no_matching_fields() {
        let val = json!({"extra": 1, "other": "x"});
        let compact = compact_symbol_value(&val);
        assert_eq!(compact.as_object().unwrap().len(), 0);
    }

    #[test]
    fn apply_compact_array_of_symbols() {
        let arr = json!([
            {"qualname": "A", "name": "A", "docstring": "remove me"},
            {"qualname": "B", "name": "B", "end_line": 99},
        ]);
        let result = apply_compact_format(arr);
        let items = result.as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert!(!items[0].as_object().unwrap().contains_key("docstring"));
        assert!(!items[1].as_object().unwrap().contains_key("end_line"));
    }

    #[test]
    fn apply_compact_array_with_symbol_field() {
        let arr = json!([
            {
                "score": 5,
                "symbol": {
                    "qualname": "X",
                    "name": "X",
                    "end_line": 99
                }
            }
        ]);
        let result = apply_compact_format(arr);
        let item = &result.as_array().unwrap()[0];
        let sym = item.get("symbol").unwrap();
        assert!(!sym.as_object().unwrap().contains_key("end_line"));
        assert!(sym.as_object().unwrap().contains_key("qualname"));
        // Non-symbol fields preserved
        assert_eq!(item.get("score").unwrap(), &json!(5));
    }

    #[test]
    fn apply_compact_array_non_symbol_items_pass_through() {
        let arr = json!([42, "string", null, {"other": true}]);
        let result = apply_compact_format(arr.clone());
        assert_eq!(result, arr);
    }

    #[test]
    fn apply_compact_object_with_results_array() {
        let val = json!({
            "total": 2,
            "results": [
                {"qualname": "A", "docstring": "gone"},
                {"qualname": "B", "end_line": 5}
            ]
        });
        let result = apply_compact_format(val);
        assert_eq!(result.get("total").unwrap(), &json!(2));
        let results = result.get("results").unwrap().as_array().unwrap();
        assert!(!results[0].as_object().unwrap().contains_key("docstring"));
    }

    #[test]
    fn apply_compact_object_symbol_field_compacted() {
        let val = json!({
            "symbol": {"qualname": "Foo", "docstring": "gone"},
            "other": "kept"
        });
        let result = apply_compact_format(val);
        let sym = result.get("symbol").unwrap().as_object().unwrap();
        assert!(!sym.contains_key("docstring"));
        assert_eq!(result.get("other").unwrap(), &json!("kept"));
    }

    #[test]
    fn apply_compact_object_symbol_without_qualname_kept() {
        let val = json!({
            "symbol": {"raw": "not a real symbol"}
        });
        let result = apply_compact_format(val);
        let sym = result.get("symbol").unwrap().as_object().unwrap();
        assert!(sym.contains_key("raw"));
    }

    #[test]
    fn apply_compact_start_end_fields() {
        let val = json!({
            "start": {"qualname": "A", "docstring": "gone"},
            "end": {"qualname": "B", "end_line": 99}
        });
        let result = apply_compact_format(val);
        let start = result.get("start").unwrap().as_object().unwrap();
        assert!(!start.contains_key("docstring"));
        let end = result.get("end").unwrap().as_object().unwrap();
        assert!(!end.contains_key("end_line"));
    }

    #[test]
    fn apply_compact_primitive_pass_through() {
        assert_eq!(apply_compact_format(json!(42)), json!(42));
        assert_eq!(apply_compact_format(json!("hello")), json!("hello"));
        assert_eq!(apply_compact_format(json!(null)), json!(null));
        assert_eq!(apply_compact_format(json!(true)), json!(true));
    }
}
