use crate::indexer::extract::{EdgeInput, ExtractedFile, LanguageExtractor, SymbolInput};
use anyhow::Result;
use serde_yaml_ng::Value;
use std::path::Path;

pub struct YamlExtractor;

impl YamlExtractor {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }
}

impl LanguageExtractor for YamlExtractor {
    fn module_name_from_rel_path(&self, rel_path: &str) -> String {
        module_name_from_rel_path(rel_path)
    }

    fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile> {
        let mut output = ExtractedFile::default();
        output
            .symbols
            .push(module_symbol_with_span(module_name, span_whole(source)));

        let documents = split_documents(source);
        for doc in &documents {
            if doc.text.trim().is_empty() {
                continue;
            }
            let value: Value = match serde_yaml_ng::from_str(&doc.text) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let Some(resource) = parse_k8s_resource(&value) else {
                continue;
            };
            resource_to_symbols(
                &resource,
                &value,
                module_name,
                doc,
                source,
                &mut output,
            );
        }

        Ok(output)
    }
}

// --- K8s resource parsing ---

struct K8sResource {
    api_version: String,
    kind: String,
    name: String,
    namespace: String,
    labels: Vec<(String, String)>,
    annotations: Vec<(String, String)>,
}

fn parse_k8s_resource(value: &Value) -> Option<K8sResource> {
    let map = value.as_mapping()?;
    let api_version = map.get(&Value::String("apiVersion".into()))?.as_str()?;
    if !is_k8s_api_version(api_version) {
        return None;
    }
    let kind = map.get(&Value::String("kind".into()))?.as_str()?;
    let metadata = map.get(&Value::String("metadata".into()))?.as_mapping()?;
    let name = metadata
        .get(&Value::String("name".into()))?
        .as_str()?;
    let namespace = metadata
        .get(&Value::String("namespace".into()))
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();
    let labels = extract_string_map(metadata.get(&Value::String("labels".into())));
    let annotations = extract_string_map(metadata.get(&Value::String("annotations".into())));

    Some(K8sResource {
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        name: name.to_string(),
        namespace,
        labels,
        annotations,
    })
}

fn is_k8s_api_version(version: &str) -> bool {
    if version == "v1" {
        return true;
    }
    if version.contains('/') {
        return true;
    }
    false
}

fn extract_string_map(value: Option<&Value>) -> Vec<(String, String)> {
    let Some(map) = value.and_then(|v| v.as_mapping()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (k, v) in map {
        if let (Some(key), Some(val)) = (k.as_str(), v.as_str()) {
            out.push((key.to_string(), val.to_string()));
        }
    }
    out
}

// --- Symbol generation ---

fn resource_to_symbols(
    resource: &K8sResource,
    value: &Value,
    module_name: &str,
    doc: &YamlDocument,
    source: &str,
    output: &mut ExtractedFile,
) {
    let kind_lower = resource.kind.to_ascii_lowercase();
    let qualname = format!(
        "k8s://{}/{}/{}",
        resource.namespace, kind_lower, resource.name
    );
    let signature = format!(
        "{} {} {}/{}",
        resource.api_version, resource.kind, resource.namespace, resource.name
    );
    let docstring = build_docstring(&resource.labels, &resource.annotations);

    let (start_line, end_line, start_byte, end_byte) = doc_span(doc, source);

    let symbol = SymbolInput {
        kind: kind_lower.clone(),
        name: resource.name.clone(),
        qualname: qualname.clone(),
        start_line,
        start_col: 1,
        end_line,
        end_col: 1,
        start_byte,
        end_byte,
        signature: Some(signature),
        docstring,
    };
    output.symbols.push(symbol);
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(module_name.to_string()),
        target_qualname: Some(qualname.clone()),
        ..Default::default()
    });

    // Extract containers
    let containers = find_containers(&kind_lower, value);
    for container in containers {
        let container_qualname =
            format!("{}/container/{}", qualname, container.name);
        let container_symbol = SymbolInput {
            kind: "container".to_string(),
            name: container.name.clone(),
            qualname: container_qualname.clone(),
            start_line,
            start_col: 1,
            end_line,
            end_col: 1,
            start_byte,
            end_byte,
            signature: container.image,
            docstring: None,
        };
        output.symbols.push(container_symbol);
        output.edges.push(EdgeInput {
            kind: "CONTAINS".to_string(),
            source_qualname: Some(qualname.clone()),
            target_qualname: Some(container_qualname),
            ..Default::default()
        });
    }
}

fn build_docstring(
    labels: &[(String, String)],
    annotations: &[(String, String)],
) -> Option<String> {
    let mut parts = Vec::new();
    if !labels.is_empty() {
        let items: Vec<String> = labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
        parts.push(format!("labels: {}", items.join(", ")));
    }
    if !annotations.is_empty() {
        let items: Vec<String> = annotations
            .iter()
            .filter(|(k, _)| !k.starts_with("kubectl.kubernetes.io/"))
            .map(|(k, v)| {
                let short = if v.len() > 60 {
                    format!("{}...", &v[..57])
                } else {
                    v.clone()
                };
                format!("{k}={short}")
            })
            .collect();
        if !items.is_empty() {
            parts.push(format!("annotations: {}", items.join(", ")));
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

// --- Container extraction ---

struct ContainerInfo {
    name: String,
    image: Option<String>,
}

fn find_containers(kind: &str, value: &Value) -> Vec<ContainerInfo> {
    let pod_spec = match kind {
        "deployment" | "statefulset" | "daemonset" | "replicaset" => {
            // spec.template.spec
            value
                .get("spec")
                .and_then(|v| v.get("template"))
                .and_then(|v| v.get("spec"))
        }
        "job" => {
            // spec.template.spec
            value
                .get("spec")
                .and_then(|v| v.get("template"))
                .and_then(|v| v.get("spec"))
        }
        "cronjob" => {
            // spec.jobTemplate.spec.template.spec
            value
                .get("spec")
                .and_then(|v| v.get("jobTemplate"))
                .and_then(|v| v.get("spec"))
                .and_then(|v| v.get("template"))
                .and_then(|v| v.get("spec"))
        }
        "pod" => {
            // spec
            value.get("spec")
        }
        _ => None,
    };
    let Some(pod_spec) = pod_spec else {
        return Vec::new();
    };
    let mut containers = Vec::new();
    for key in &["containers", "initContainers"] {
        if let Some(list) = pod_spec.get(*key).and_then(|v| v.as_sequence()) {
            for item in list {
                if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
                    let image = item.get("image").and_then(|v| v.as_str()).map(|s| s.to_string());
                    containers.push(ContainerInfo {
                        name: name.to_string(),
                        image,
                    });
                }
            }
        }
    }
    containers
}

// --- Multi-document splitting ---

struct YamlDocument {
    text: String,
    byte_offset: usize,
    line_offset: i64,
}

fn split_documents(source: &str) -> Vec<YamlDocument> {
    let mut documents = Vec::new();
    let mut current_start = 0usize;
    let mut current_line = 1i64;
    let mut i = 0;
    let bytes = source.as_bytes();

    while i < bytes.len() {
        if bytes[i] == b'\n' {
            let next = i + 1;
            // Check if next line starts with "---"
            if next < bytes.len() && is_doc_separator(source, next) {
                // End current document at end of this line (including newline)
                let doc_text = &source[current_start..next];
                if !doc_text.trim().is_empty() {
                    documents.push(YamlDocument {
                        text: doc_text.to_string(),
                        byte_offset: current_start,
                        line_offset: current_line,
                    });
                }
                // Find end of separator line
                let sep_end = find_line_end(source, next);
                current_start = sep_end;
                current_line = current_line
                    + doc_text.bytes().filter(|b| *b == b'\n').count() as i64
                    + 1; // +1 for the separator line
                i = sep_end;
                continue;
            }
            i = next;
            continue;
        }
        i += 1;
    }

    // Final document
    let doc_text = &source[current_start..];
    // Check if starts with separator
    let (text, byte_off, line_off) = if is_doc_separator(source, current_start) {
        let sep_end = find_line_end(source, current_start);
        (&source[sep_end..], sep_end, current_line + 1)
    } else {
        (doc_text, current_start, current_line)
    };
    if !text.trim().is_empty() {
        documents.push(YamlDocument {
            text: text.to_string(),
            byte_offset: byte_off,
            line_offset: line_off,
        });
    }

    // Handle leading separator (first line is ---)
    if documents.is_empty() && !source.trim().is_empty() {
        // Entire source might start with --- on first line
        if is_doc_separator(source, 0) {
            let sep_end = find_line_end(source, 0);
            let rest = &source[sep_end..];
            if !rest.trim().is_empty() {
                documents.push(YamlDocument {
                    text: rest.to_string(),
                    byte_offset: sep_end,
                    line_offset: 2,
                });
            }
        }
    }

    documents
}

fn is_doc_separator(source: &str, pos: usize) -> bool {
    let rest = &source[pos..];
    if !rest.starts_with("---") {
        return false;
    }
    let after = &rest[3..];
    after.is_empty() || after.starts_with('\n') || after.starts_with('\r') || after.starts_with(' ')
}

fn find_line_end(source: &str, pos: usize) -> usize {
    match source[pos..].find('\n') {
        Some(offset) => pos + offset + 1,
        None => source.len(),
    }
}

// --- Span helpers ---

fn doc_span(doc: &YamlDocument, _source: &str) -> (i64, i64, i64, i64) {
    let start_line = doc.line_offset;
    let line_count = doc.text.bytes().filter(|b| *b == b'\n').count() as i64;
    let end_line = start_line + line_count.max(0);
    let start_byte = doc.byte_offset as i64;
    let end_byte = start_byte + doc.text.len() as i64;
    (start_line, end_line, start_byte, end_byte)
}

fn span_whole(source: &str) -> (i64, i64, i64, i64, i64, i64) {
    let lines = source.lines().count().max(1) as i64;
    (1, 1, lines, 1, 0, source.len() as i64)
}

fn module_symbol_with_span(
    module_name: &str,
    span: (i64, i64, i64, i64, i64, i64),
) -> SymbolInput {
    let name = module_name
        .rsplit('/')
        .next()
        .unwrap_or(module_name)
        .to_string();
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span;
    SymbolInput {
        kind: "module".to_string(),
        name,
        qualname: module_name.to_string(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature: None,
        docstring: None,
    }
}

pub fn module_name_from_rel_path(rel_path: &str) -> String {
    let path = Path::new(rel_path);
    let mut parts: Vec<String> = path
        .components()
        .filter_map(|comp| comp.as_os_str().to_str().map(|s| s.to_string()))
        .collect();
    if parts.is_empty() {
        return "yaml".to_string();
    }
    let file = parts.pop().unwrap_or_default();
    let stem = Path::new(&file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(&file)
        .to_string();
    if !stem.is_empty() {
        parts.push(stem);
    }
    if parts.is_empty() {
        "yaml".to_string()
    } else {
        parts.join("/")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_documents_basic() {
        let source = "apiVersion: v1\nkind: Service\n---\napiVersion: apps/v1\nkind: Deployment\n";
        let docs = split_documents(source);
        assert_eq!(docs.len(), 2);
        assert!(docs[0].text.contains("Service"));
        assert!(docs[1].text.contains("Deployment"));
        assert_eq!(docs[0].line_offset, 1);
        assert_eq!(docs[0].byte_offset, 0);
    }

    #[test]
    fn k8s_detection_positive() {
        let yaml = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: test\n";
        let value: Value = serde_yaml_ng::from_str(yaml).unwrap();
        let resource = parse_k8s_resource(&value);
        assert!(resource.is_some());
        let r = resource.unwrap();
        assert_eq!(r.kind, "Deployment");
        assert_eq!(r.name, "test");
        assert_eq!(r.namespace, "default");
    }

    #[test]
    fn k8s_detection_negative_no_api_version() {
        let yaml = "kind: Deployment\nmetadata:\n  name: test\n";
        let value: Value = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(parse_k8s_resource(&value).is_none());
    }

    #[test]
    fn k8s_detection_negative_docker_compose() {
        let yaml = "version: '3'\nservices:\n  web:\n    image: nginx\n";
        let value: Value = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(parse_k8s_resource(&value).is_none());
    }
}
