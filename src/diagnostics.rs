use crate::util;
use anyhow::{Context, Result};
use blake3::Hasher;
use serde_json::Value;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct DiagnosticInput {
    pub path: Option<String>,
    pub line: Option<i64>,
    pub column: Option<i64>,
    pub end_line: Option<i64>,
    pub end_column: Option<i64>,
    pub severity: Option<String>,
    pub message: String,
    pub rule_id: Option<String>,
    pub tool: Option<String>,
    pub snippet: Option<String>,
}

impl DiagnosticInput {
    pub fn fingerprint(&self) -> String {
        let mut hasher = Hasher::new();
        push_opt_str(&mut hasher, self.path.as_deref());
        push_opt_i64(&mut hasher, self.line);
        push_opt_i64(&mut hasher, self.column);
        push_opt_i64(&mut hasher, self.end_line);
        push_opt_i64(&mut hasher, self.end_column);
        push_opt_str(&mut hasher, self.severity.as_deref());
        push_str(&mut hasher, &self.message);
        push_opt_str(&mut hasher, self.rule_id.as_deref());
        push_opt_str(&mut hasher, self.tool.as_deref());
        push_opt_str(&mut hasher, self.snippet.as_deref());
        hasher.finalize().to_hex().to_string()
    }
}

pub fn parse_sarif(source: &str, repo_root: &Path) -> Result<Vec<DiagnosticInput>> {
    let root: Value = serde_json::from_str(source).with_context(|| "parse SARIF JSON")?;
    let runs = root
        .get("runs")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    let mut diagnostics = Vec::new();
    for run in runs {
        let tool_name = run
            .get("tool")
            .and_then(|tool| tool.get("driver"))
            .and_then(|driver| driver.get("name"))
            .and_then(|name| name.as_str())
            .map(|value| value.to_string());
        let rules = run
            .get("tool")
            .and_then(|tool| tool.get("driver"))
            .and_then(|driver| driver.get("rules"))
            .and_then(|rules| rules.as_array())
            .cloned()
            .unwrap_or_default();
        let results = run
            .get("results")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        for result in results {
            let message = result_message(&result);
            if message.is_empty() {
                continue;
            }
            let severity = result
                .get("level")
                .and_then(|value| value.as_str())
                .or_else(|| result.get("kind").and_then(|value| value.as_str()))
                .map(|value| value.to_string());
            let rule_id = result
                .get("ruleId")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string())
                .or_else(|| {
                    result
                        .get("rule")
                        .and_then(|rule| rule.get("id"))
                        .and_then(|value| value.as_str())
                        .map(|value| value.to_string())
                })
                .or_else(|| {
                    let index = result.get("ruleIndex").and_then(|value| value.as_u64())?;
                    let rule = rules.get(index as usize)?;
                    rule.get("id")
                        .and_then(|value| value.as_str())
                        .map(|value| value.to_string())
                });

            let mut path = None;
            let mut line = None;
            let mut column = None;
            let mut end_line = None;
            let mut end_column = None;
            let mut snippet = None;

            if let Some(location) = first_location(&result) {
                if let Some(uri) = location
                    .get("physicalLocation")
                    .and_then(|value| value.get("artifactLocation"))
                    .and_then(|value| value.get("uri"))
                    .and_then(|value| value.as_str())
                {
                    path = normalize_uri(uri, repo_root);
                }
                if let Some(region) = location
                    .get("physicalLocation")
                    .and_then(|value| value.get("region"))
                {
                    line = region.get("startLine").and_then(|value| value.as_i64());
                    column = region.get("startColumn").and_then(|value| value.as_i64());
                    end_line = region.get("endLine").and_then(|value| value.as_i64());
                    end_column = region.get("endColumn").and_then(|value| value.as_i64());
                    snippet = region
                        .get("snippet")
                        .and_then(|value| value.get("text"))
                        .and_then(|value| value.as_str())
                        .map(|value| value.to_string());
                }
            }

            diagnostics.push(DiagnosticInput {
                path,
                line,
                column,
                end_line,
                end_column,
                severity,
                message,
                rule_id,
                tool: tool_name.clone(),
                snippet,
            });
        }
    }
    Ok(diagnostics)
}

fn result_message(result: &Value) -> String {
    result
        .get("message")
        .and_then(|value| value.get("text"))
        .and_then(|value| value.as_str())
        .or_else(|| {
            result
                .get("message")
                .and_then(|value| value.get("markdown"))
                .and_then(|value| value.as_str())
        })
        .or_else(|| result.get("message").and_then(|value| value.as_str()))
        .unwrap_or("")
        .to_string()
}

fn first_location(result: &Value) -> Option<&Value> {
    result
        .get("locations")
        .and_then(|value| value.as_array())
        .and_then(|value| value.first())
}

fn normalize_uri(uri: &str, repo_root: &Path) -> Option<String> {
    let trimmed = uri.trim();
    if trimmed.is_empty() {
        return None;
    }
    let cleaned = trimmed
        .strip_prefix("file://")
        .or_else(|| trimmed.strip_prefix("file:"))
        .unwrap_or(trimmed);
    let path = PathBuf::from(cleaned);
    if path.is_absolute() {
        if let Ok(rel) = path.strip_prefix(repo_root) {
            return Some(util::normalize_path(rel));
        }
    }
    Some(util::normalize_path(Path::new(cleaned)))
}

fn push_opt_str(hasher: &mut Hasher, value: Option<&str>) {
    match value {
        Some(value) => push_str(hasher, value),
        None => push_str(hasher, "-"),
    }
}

fn push_opt_i64(hasher: &mut Hasher, value: Option<i64>) {
    match value {
        Some(value) => push_str(hasher, &value.to_string()),
        None => push_str(hasher, "-"),
    }
}

fn push_str(hasher: &mut Hasher, value: &str) {
    hasher.update(value.as_bytes());
    hasher.update(b"\n");
}
