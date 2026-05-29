use serde_json::{Value, json};

/// Generate a JSON Schema value for a type that implements `schemars::JsonSchema`.
pub(super) fn schema_value<T: schemars::JsonSchema>() -> Value {
    let schema = schemars::schema_for!(T);
    let raw = serde_json::to_value(schema).unwrap_or_else(|_| json!({"type": "object"}));
    simplify_schema(raw)
}

/// Return a simplified JSON Schema for the params struct of the given method.
pub fn method_param_schema(method: &str) -> Value {
    use super::{
        AnalyzeDiffParams, AnalyzeImpactParams, DeadSymbolsParams, ExplainSymbolParams,
        GatherContextParams, OnboardParams, OrientParams, ReindexParams, RepoMapParams, RgParams,
        TopComplexityParams, TraceFlowParams,
    };
    match method {
        "search" => schema_value::<RgParams>(),
        "explain_symbol" => schema_value::<ExplainSymbolParams>(),
        "trace_flow" => schema_value::<TraceFlowParams>(),
        "analyze_impact" => schema_value::<AnalyzeImpactParams>(),
        "analyze_diff" => schema_value::<AnalyzeDiffParams>(),
        "gather_context" => schema_value::<GatherContextParams>(),
        "orient" => schema_value::<OrientParams>(),
        "onboard" => schema_value::<OnboardParams>(),
        "reindex" => schema_value::<ReindexParams>(),
        "top_complexity" => schema_value::<TopComplexityParams>(),
        "repo_map" => schema_value::<RepoMapParams>(),
        "dead_symbols" => schema_value::<DeadSymbolsParams>(),
        _ => json!({"type": "object"}),
    }
}

/// Post-process schemars output into compact, LLM-friendly JSON Schema.
fn simplify_schema(mut schema: Value) -> Value {
    // 1. Collect definitions for inlining $ref
    let definitions = schema
        .get("definitions")
        .cloned()
        .unwrap_or_else(|| json!({}));

    // 2. Recursively inline $ref and clean up
    inline_refs(&mut schema, &definitions);

    // 3. Strip root-level noise
    if let Some(obj) = schema.as_object_mut() {
        obj.remove("$schema");
        obj.remove("definitions");
        obj.remove("title");
    }

    schema
}

/// Recursively inline `$ref` references and collapse `Option<T>` patterns.
fn inline_refs(value: &mut Value, definitions: &Value) {
    match value {
        Value::Object(map) => {
            // Handle $ref: inline the definition
            if let Some(ref_val) = map.get("$ref").cloned()
                && let Some(ref_str) = ref_val.as_str()
            {
                // Extract definition name from "#/definitions/Name"
                if let Some(name) = ref_str.strip_prefix("#/definitions/")
                    && let Some(def) = definitions.get(name)
                {
                    let mut inlined = def.clone();
                    inline_refs(&mut inlined, definitions);
                    *value = inlined;
                    return;
                }
            }

            // Handle anyOf with null (Option<T> pattern): collapse to inner schema
            if let Some(any_of) = map.get("anyOf").cloned()
                && let Some(variants) = any_of.as_array()
                && variants.len() == 2
            {
                let null_idx = variants
                    .iter()
                    .position(|v| v.get("type").and_then(|t| t.as_str()) == Some("null"));
                if let Some(idx) = null_idx {
                    let inner_idx = 1 - idx;
                    let mut inner = variants[inner_idx].clone();
                    inline_refs(&mut inner, definitions);
                    *value = inner;
                    return;
                }
            }

            // Recurse into all values
            let keys: Vec<String> = map.keys().cloned().collect();
            for key in keys {
                if let Some(v) = map.get_mut(&key) {
                    inline_refs(v, definitions);
                }
            }

            // Strip format on integers (e.g. "format": "uint", "format": "int64")
            if map.get("type").and_then(|t| t.as_str()) == Some("integer") {
                map.remove("format");
                map.remove("minimum");
            }
            // Strip format on numbers
            if map.get("type").and_then(|t| t.as_str()) == Some("number") {
                map.remove("format");
            }
        }
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                inline_refs(item, definitions);
            }
        }
        _ => {}
    }
}
