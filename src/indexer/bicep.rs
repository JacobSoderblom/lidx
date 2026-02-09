use crate::indexer::channel::{CHANNEL_PUBLISH_KIND, CHANNEL_SUBSCRIBE_KIND};
use crate::indexer::extract::{EdgeInput, ExtractedFile, LanguageExtractor, SymbolInput};
use anyhow::Result;
use serde_json::json;
use std::path::Path;

pub struct BicepExtractor;

impl BicepExtractor {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }
}

impl LanguageExtractor for BicepExtractor {
    fn module_name_from_rel_path(&self, rel_path: &str) -> String {
        module_name_from_rel_path(rel_path)
    }

    fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile> {
        let mut output = ExtractedFile::default();
        let (decls, target_scope, using_path) = parse_bicep(source);

        // File-level module symbol
        let file_sig = target_scope.clone();
        let file_doc = decls.iter().find_map(|d| {
            if d.kind == DeclKind::Metadata {
                d.description.clone()
            } else {
                None
            }
        });
        output.symbols.push(module_symbol(module_name, source, file_sig, file_doc));

        // Store using_path for resolve_imports
        if let Some(ref path) = using_path {
            output.edges.push(EdgeInput {
                kind: "IMPORTS_FILE".to_string(),
                source_qualname: Some(module_name.to_string()),
                target_qualname: Some(path.clone()),
                ..Default::default()
            });
        }

        for decl in &decls {
            if decl.kind == DeclKind::Metadata {
                continue;
            }
            let (kind_str, qualname) = decl_kind_and_qualname(decl, module_name);
            let signature = build_signature(decl);
            let symbol = SymbolInput {
                kind: kind_str.to_string(),
                name: decl.name.clone(),
                qualname: qualname.clone(),
                start_line: decl.start_line,
                start_col: 1,
                end_line: decl.end_line,
                end_col: 1,
                start_byte: decl.start_byte,
                end_byte: decl.end_byte,
                signature,
                docstring: decl.description.clone(),
            };
            output.symbols.push(symbol);
            output.edges.push(EdgeInput {
                kind: "CONTAINS".to_string(),
                source_qualname: Some(module_name.to_string()),
                target_qualname: Some(qualname.clone()),
                ..Default::default()
            });

            // IMPORTS_FILE edge for module declarations
            if decl.kind == DeclKind::ModuleRef {
                if let Some(ref path) = decl.type_or_path {
                    output.edges.push(EdgeInput {
                        kind: "IMPORTS_FILE".to_string(),
                        source_qualname: Some(qualname.clone()),
                        target_qualname: Some(path.clone()),
                        ..Default::default()
                    });
                }
            }

            // Channel edges for Service Bus topics/queues
            if decl.kind == DeclKind::Resource {
                if let Some(ref azure_type) = decl.type_or_path {
                    if is_service_bus_topic_or_queue(azure_type) {
                        if let Some(ref resource_name) = decl.resource_name {
                            if let Some(channel) = normalize_azure_channel_name(resource_name) {
                                let detail = json!({
                                    "channel": channel,
                                    "raw": resource_name,
                                    "framework": "azure-service-bus",
                                    "role": "infrastructure",
                                })
                                .to_string();
                                output.edges.push(EdgeInput {
                                    kind: CHANNEL_PUBLISH_KIND.to_string(),
                                    source_qualname: Some(qualname.clone()),
                                    target_qualname: Some(channel.clone()),
                                    detail: Some(detail.clone()),
                                    evidence_start_line: Some(decl.start_line),
                                    evidence_end_line: Some(decl.end_line),
                                    ..Default::default()
                                });
                                output.edges.push(EdgeInput {
                                    kind: CHANNEL_SUBSCRIBE_KIND.to_string(),
                                    source_qualname: Some(qualname.clone()),
                                    target_qualname: Some(channel),
                                    detail: Some(detail),
                                    evidence_start_line: Some(decl.start_line),
                                    evidence_end_line: Some(decl.end_line),
                                    ..Default::default()
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(output)
    }

    fn resolve_imports(
        &self,
        _repo_root: &Path,
        file_rel_path: &str,
        _module_name: &str,
        edges: &mut Vec<EdgeInput>,
    ) {
        let file_dir = Path::new(file_rel_path)
            .parent()
            .unwrap_or(Path::new(""));

        for edge in edges.iter_mut() {
            if edge.kind != "IMPORTS_FILE" {
                continue;
            }
            let Some(ref target) = edge.target_qualname else {
                continue;
            };
            // Only resolve raw .bicep/.bicepparam paths
            if !target.ends_with(".bicep") && !target.ends_with(".bicepparam") {
                continue;
            }
            let raw = target.trim_matches('\'').trim_matches('"');
            let resolved = file_dir.join(raw);
            let normalized = crate::util::normalize_path(&resolved);
            edge.target_qualname = Some(normalized);
        }
    }
}

// --- Public helpers ---

pub fn module_name_from_rel_path(rel_path: &str) -> String {
    let path = Path::new(rel_path);
    let mut parts: Vec<String> = path
        .components()
        .filter_map(|comp| comp.as_os_str().to_str().map(|s| s.to_string()))
        .collect();
    if parts.is_empty() {
        return "bicep".to_string();
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
        "bicep".to_string()
    } else {
        parts.join("/")
    }
}

// --- Parser types ---

#[derive(Debug, Clone, PartialEq, Eq)]
enum DeclKind {
    Resource,
    ModuleRef,
    Param,
    Var,
    Output,
    Type,
    Func,
    Metadata, // internal: for `metadata description = '...'`
}

#[derive(Debug, Clone)]
struct BicepDecl {
    kind: DeclKind,
    name: String,
    type_or_path: Option<String>,
    start_line: i64,
    start_byte: i64,
    end_line: i64,
    end_byte: i64,
    is_existing: bool,
    description: Option<String>,
    is_secure: bool,
    resource_name: Option<String>,
}

// --- Main parser ---

fn parse_bicep(source: &str) -> (Vec<BicepDecl>, Option<String>, Option<String>) {
    let lines: Vec<&str> = source.lines().collect();
    let mut decls: Vec<BicepDecl> = Vec::new();
    let mut target_scope: Option<String> = None;
    let mut using_path: Option<String> = None;

    let mut line_idx = 0;
    let mut byte_offset: i64 = 0;
    let mut brace_depth: i32 = 0;
    let mut bracket_depth: i32 = 0;
    let mut in_block_comment = false;

    // Pending decorator state
    let mut pending_description: Option<String> = None;
    let mut pending_secure = false;
    let mut decl_start_line: Option<i64> = None;

    // Track which decl owns the current block (for extracting name: property)
    let mut current_resource_idx: Option<usize> = None;

    while line_idx < lines.len() {
        let line = lines[line_idx];
        let line_num = (line_idx + 1) as i64;
        let line_byte_start = byte_offset;
        let line_byte_end = byte_offset + line.len() as i64;

        // Handle block comments
        if in_block_comment {
            if let Some(pos) = line.find("*/") {
                in_block_comment = false;
                // Process rest of line after block comment end
                let rest = &line[pos + 2..];
                // Count braces/brackets in rest
                count_depth(rest, &mut brace_depth, &mut bracket_depth);
            }
            byte_offset = line_byte_end + 1; // +1 for newline
            line_idx += 1;
            continue;
        }

        let trimmed = line.trim();

        // Skip empty lines
        if trimmed.is_empty() {
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // Skip line comments
        if trimmed.starts_with("//") {
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // Check for block comment start
        if trimmed.starts_with("/*") {
            if let Some(pos) = trimmed[2..].find("*/") {
                // Single-line block comment
                let rest = &trimmed[pos + 4..];
                count_depth(rest, &mut brace_depth, &mut bracket_depth);
            } else {
                in_block_comment = true;
            }
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // Inside a block — track depth
        if brace_depth > 0 || bracket_depth > 0 {
            // At depth 1 inside a resource/module block, extract name: 'value'
            if brace_depth == 1 && bracket_depth == 0 {
                if let Some(idx) = current_resource_idx {
                    if decls[idx].resource_name.is_none() {
                        if let Some(rn) = extract_resource_name_property(trimmed) {
                            decls[idx].resource_name = Some(rn);
                        }
                    }
                }
            }

            let (bd, kd) = count_depth_line(trimmed);
            brace_depth += bd;
            bracket_depth += kd;
            // Clamp
            if brace_depth < 0 { brace_depth = 0; }
            if bracket_depth < 0 { bracket_depth = 0; }

            // If we've returned to depth 0, finalize the last decl's end
            if brace_depth == 0 && bracket_depth == 0 {
                if let Some(last) = decls.last_mut() {
                    let last_decl: &mut BicepDecl = last;
                    last_decl.end_line = line_num;
                    last_decl.end_byte = line_byte_end;
                }
                current_resource_idx = None;
            }
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // At depth 0: parse declarations
        // Handle decorators
        if trimmed.starts_with('@') {
            if decl_start_line.is_none() {
                decl_start_line = Some(line_num);
            }
            if let Some(desc) = extract_description_decorator(trimmed) {
                pending_description = Some(desc);
            }
            if trimmed.starts_with("@secure()") || trimmed.starts_with("@secure()")  {
                pending_secure = true;
            }
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // targetScope
        if trimmed.starts_with("targetScope") {
            if let Some(scope) = extract_single_quoted_string(trimmed) {
                target_scope = Some(scope);
            }
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // using (bicepparam)
        if trimmed.starts_with("using ") || trimmed.starts_with("using\t") {
            if let Some(path) = extract_single_quoted_string(trimmed) {
                using_path = Some(path);
            }
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // metadata description = '...'
        if trimmed.starts_with("metadata ") {
            let tokens = split_tokens(trimmed);
            if tokens.len() >= 4 && tokens[0] == "metadata" && tokens[1] == "description" && tokens[2] == "=" {
                if let Some(val) = extract_single_quoted_string(trimmed) {
                    decls.push(BicepDecl {
                        kind: DeclKind::Metadata,
                        name: "description".to_string(),
                        type_or_path: None,
                        start_line: line_num,
                        start_byte: line_byte_start,
                        end_line: line_num,
                        end_byte: line_byte_end,
                        is_existing: false,
                        description: Some(val),
                        is_secure: false,
                        resource_name: None,
                    });
                }
            }
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // resource <name> '<azureType>' [existing] = ...
        if trimmed.starts_with("resource ") {
            let start = decl_start_line.take().unwrap_or(line_num);
            let start_byte_actual = if start < line_num {
                line_byte_offset(source, start)
            } else {
                line_byte_start
            };
            let tokens = split_tokens(trimmed);
            if tokens.len() >= 2 {
                let name = tokens[1].to_string();
                let azure_type = extract_single_quoted_string(trimmed);
                let is_existing = tokens.iter().any(|t| *t == "existing");

                // Determine if this opens a block
                let (bd, kd) = count_depth_line(trimmed);
                brace_depth += bd;
                bracket_depth += kd;
                if brace_depth < 0 { brace_depth = 0; }
                if bracket_depth < 0 { bracket_depth = 0; }

                let end_line = if brace_depth == 0 && bracket_depth == 0 {
                    line_num
                } else {
                    line_num // will be updated when block closes
                };

                decls.push(BicepDecl {
                    kind: DeclKind::Resource,
                    name,
                    type_or_path: azure_type,
                    start_line: start,
                    start_byte: start_byte_actual,
                    end_line,
                    end_byte: line_byte_end,
                    is_existing,
                    description: pending_description.take(),
                    is_secure: false,
                    resource_name: None,
                });
                if brace_depth > 0 || bracket_depth > 0 {
                    current_resource_idx = Some(decls.len() - 1);
                }
                pending_secure = false;
            }
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // module <name> '<path>' = ...
        if trimmed.starts_with("module ") {
            let start = decl_start_line.take().unwrap_or(line_num);
            let start_byte_actual = if start < line_num {
                line_byte_offset(source, start)
            } else {
                line_byte_start
            };
            let tokens = split_tokens(trimmed);
            if tokens.len() >= 2 {
                let name = tokens[1].to_string();
                let file_path = extract_single_quoted_string(trimmed);

                let (bd, kd) = count_depth_line(trimmed);
                brace_depth += bd;
                bracket_depth += kd;
                if brace_depth < 0 { brace_depth = 0; }
                if bracket_depth < 0 { bracket_depth = 0; }

                let end_line = if brace_depth == 0 && bracket_depth == 0 {
                    line_num
                } else {
                    line_num
                };

                decls.push(BicepDecl {
                    kind: DeclKind::ModuleRef,
                    name,
                    type_or_path: file_path,
                    start_line: start,
                    start_byte: start_byte_actual,
                    end_line,
                    end_byte: line_byte_end,
                    is_existing: false,
                    description: pending_description.take(),
                    is_secure: false,
                    resource_name: None,
                });
                if brace_depth > 0 || bracket_depth > 0 {
                    current_resource_idx = Some(decls.len() - 1);
                }
                pending_secure = false;
            }
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // param <name> <type> [= <default>]
        if trimmed.starts_with("param ") {
            let start = decl_start_line.take().unwrap_or(line_num);
            let start_byte_actual = if start < line_num {
                line_byte_offset(source, start)
            } else {
                line_byte_start
            };
            let tokens = split_tokens(trimmed);
            if tokens.len() >= 2 {
                let name = tokens[1].to_string();
                // Type is the token after name, before = if present
                let param_type = if tokens.len() >= 3 && tokens[2] != "=" {
                    Some(tokens[2].to_string())
                } else {
                    None
                };

                // Check for multi-line (block) params
                let (bd, kd) = count_depth_line(trimmed);
                brace_depth += bd;
                bracket_depth += kd;
                if brace_depth < 0 { brace_depth = 0; }
                if bracket_depth < 0 { bracket_depth = 0; }

                let secure = pending_secure;
                decls.push(BicepDecl {
                    kind: DeclKind::Param,
                    name,
                    type_or_path: param_type,
                    start_line: start,
                    start_byte: start_byte_actual,
                    end_line: line_num,
                    end_byte: line_byte_end,
                    is_existing: false,
                    description: pending_description.take(),
                    is_secure: secure,
                    resource_name: None,
                });
                pending_secure = false;
            }
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // var <name> = <expr>
        if trimmed.starts_with("var ") {
            let start = decl_start_line.take().unwrap_or(line_num);
            let start_byte_actual = if start < line_num {
                line_byte_offset(source, start)
            } else {
                line_byte_start
            };
            let tokens = split_tokens(trimmed);
            if tokens.len() >= 2 {
                let name = tokens[1].to_string();

                let (bd, kd) = count_depth_line(trimmed);
                brace_depth += bd;
                bracket_depth += kd;
                if brace_depth < 0 { brace_depth = 0; }
                if bracket_depth < 0 { bracket_depth = 0; }

                decls.push(BicepDecl {
                    kind: DeclKind::Var,
                    name,
                    type_or_path: None,
                    start_line: start,
                    start_byte: start_byte_actual,
                    end_line: line_num,
                    end_byte: line_byte_end,
                    is_existing: false,
                    description: pending_description.take(),
                    is_secure: false,
                    resource_name: None,
                });
                pending_secure = false;
            }
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // output <name> <type> = <expr>
        if trimmed.starts_with("output ") {
            let start = decl_start_line.take().unwrap_or(line_num);
            let start_byte_actual = if start < line_num {
                line_byte_offset(source, start)
            } else {
                line_byte_start
            };
            let tokens = split_tokens(trimmed);
            if tokens.len() >= 2 {
                let name = tokens[1].to_string();
                let output_type = if tokens.len() >= 3 && tokens[2] != "=" {
                    Some(tokens[2].to_string())
                } else {
                    None
                };

                decls.push(BicepDecl {
                    kind: DeclKind::Output,
                    name,
                    type_or_path: output_type,
                    start_line: start,
                    start_byte: start_byte_actual,
                    end_line: line_num,
                    end_byte: line_byte_end,
                    is_existing: false,
                    description: pending_description.take(),
                    is_secure: false,
                    resource_name: None,
                });
                pending_secure = false;
            }
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // type <name> = <type-expr>
        if trimmed.starts_with("type ") {
            let start = decl_start_line.take().unwrap_or(line_num);
            let start_byte_actual = if start < line_num {
                line_byte_offset(source, start)
            } else {
                line_byte_start
            };
            let tokens = split_tokens(trimmed);
            if tokens.len() >= 2 {
                let name = tokens[1].to_string();

                let (bd, kd) = count_depth_line(trimmed);
                brace_depth += bd;
                bracket_depth += kd;
                if brace_depth < 0 { brace_depth = 0; }
                if bracket_depth < 0 { bracket_depth = 0; }

                decls.push(BicepDecl {
                    kind: DeclKind::Type,
                    name,
                    type_or_path: None,
                    start_line: start,
                    start_byte: start_byte_actual,
                    end_line: line_num,
                    end_byte: line_byte_end,
                    is_existing: false,
                    description: pending_description.take(),
                    is_secure: false,
                    resource_name: None,
                });
                pending_secure = false;
            }
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // func <name>(<params>) <returnType> => <expr>
        if trimmed.starts_with("func ") {
            let start = decl_start_line.take().unwrap_or(line_num);
            let start_byte_actual = if start < line_num {
                line_byte_offset(source, start)
            } else {
                line_byte_start
            };
            let name = trimmed[5..].split(|c: char| c == '(' || c.is_whitespace()).next().unwrap_or("").to_string();
            let signature = extract_func_signature(trimmed);

            if !name.is_empty() {
                decls.push(BicepDecl {
                    kind: DeclKind::Func,
                    name,
                    type_or_path: signature,
                    start_line: start,
                    start_byte: start_byte_actual,
                    end_line: line_num,
                    end_byte: line_byte_end,
                    is_existing: false,
                    description: pending_description.take(),
                    is_secure: false,
                    resource_name: None,
                });
                pending_secure = false;
            }
            byte_offset = line_byte_end + 1;
            line_idx += 1;
            continue;
        }

        // Any other line at depth 0: track depth changes
        let (bd, kd) = count_depth_line(trimmed);
        brace_depth += bd;
        bracket_depth += kd;
        if brace_depth < 0 { brace_depth = 0; }
        if bracket_depth < 0 { bracket_depth = 0; }

        // Clear pending decorators if line doesn't match any declaration
        pending_description = None;
        pending_secure = false;
        decl_start_line = None;

        byte_offset = line_byte_end + 1;
        line_idx += 1;
    }

    (decls, target_scope, using_path)
}

// --- Helpers ---

fn module_symbol(module_name: &str, source: &str, signature: Option<String>, docstring: Option<String>) -> SymbolInput {
    let name = module_name
        .rsplit('/')
        .next()
        .unwrap_or(module_name)
        .to_string();
    let lines = source.lines().count().max(1) as i64;
    SymbolInput {
        kind: "module".to_string(),
        name,
        qualname: module_name.to_string(),
        start_line: 1,
        start_col: 1,
        end_line: lines,
        end_col: 1,
        start_byte: 0,
        end_byte: source.len() as i64,
        signature,
        docstring,
    }
}

fn decl_kind_and_qualname<'a>(decl: &BicepDecl, module_name: &str) -> (&'a str, String) {
    let kind_str = match decl.kind {
        DeclKind::Resource => "resource",
        DeclKind::ModuleRef => "module_ref",
        DeclKind::Param => "param",
        DeclKind::Var => "var",
        DeclKind::Output => "output",
        DeclKind::Type => "type",
        DeclKind::Func => "function",
        DeclKind::Metadata => "metadata",
    };
    let qualname = format!("{}.{}", module_name, decl.name);
    (kind_str, qualname)
}

fn build_signature(decl: &BicepDecl) -> Option<String> {
    match decl.kind {
        DeclKind::Resource => {
            let mut sig = decl.type_or_path.clone().unwrap_or_default();
            if decl.is_existing {
                sig.push_str(" existing");
            }
            if sig.is_empty() { None } else { Some(sig) }
        }
        DeclKind::ModuleRef => decl.type_or_path.clone(),
        DeclKind::Param => {
            let mut sig = decl.type_or_path.clone().unwrap_or_default();
            if decl.is_secure {
                if sig.is_empty() {
                    sig = "(secure)".to_string();
                } else {
                    sig.push_str(" (secure)");
                }
            }
            if sig.is_empty() { None } else { Some(sig) }
        }
        DeclKind::Output => decl.type_or_path.clone(),
        DeclKind::Func => decl.type_or_path.clone(),
        DeclKind::Var | DeclKind::Type | DeclKind::Metadata => None,
    }
}

/// Normalize an Azure Service Bus topic/queue name to a channel:// target.
/// Strips common naming prefixes (sbt-, sbq-, sbts-), removes hyphens/underscores, lowercases.
fn normalize_azure_channel_name(azure_name: &str) -> Option<String> {
    let trimmed = azure_name.trim();
    let stripped = trimmed
        .strip_prefix("sbt-")
        .or_else(|| trimmed.strip_prefix("sbq-"))
        .or_else(|| trimmed.strip_prefix("sbts-"))
        .unwrap_or(trimmed);
    let normalized: String = stripped
        .chars()
        .filter(|ch| *ch != '-' && *ch != '_')
        .flat_map(|ch| ch.to_lowercase())
        .collect();
    if normalized.is_empty() {
        return None;
    }
    Some(format!("channel://{normalized}"))
}

/// Check if an Azure resource type is a Service Bus topic or queue (not a topic subscription).
fn is_service_bus_topic_or_queue(azure_type: &str) -> bool {
    let lower = azure_type.to_lowercase();
    if lower.contains("servicebus/namespaces/topics/subscriptions") {
        return false;
    }
    lower.contains("servicebus/namespaces/topics")
        || lower.contains("servicebus/namespaces/queues")
}

/// Extract the `name:` property value from a resource body line.
/// Matches `name: 'value'` (single-quoted literal only, not expressions).
fn extract_resource_name_property(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if !trimmed.starts_with("name:") {
        return None;
    }
    extract_single_quoted_string(trimmed)
}

/// Extract the first single-quoted string from a line: 'value'
fn extract_single_quoted_string(line: &str) -> Option<String> {
    let start = line.find('\'')?;
    let rest = &line[start + 1..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

/// Extract @description('...') text from a decorator line.
/// Handles both single-quoted and multi-word descriptions.
fn extract_description_decorator(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if !trimmed.starts_with("@description(") && !trimmed.starts_with("@sys.description(") {
        return None;
    }
    // Find the opening paren
    let open = trimmed.find('(')?;
    let rest = &trimmed[open + 1..];
    // Find single-quoted string
    let sq_start = rest.find('\'')?;
    let after = &rest[sq_start + 1..];
    let sq_end = after.find('\'')?;
    Some(after[..sq_end].to_string())
}

/// Extract function signature: everything from ( to the => token
fn extract_func_signature(line: &str) -> Option<String> {
    let open = line.find('(')?;
    let arrow = line.find("=>")?;
    if arrow <= open {
        return None;
    }
    let sig = line[open..arrow].trim();
    if sig.is_empty() { None } else { Some(sig.to_string()) }
}

/// Split a line into whitespace-separated tokens (simple, not quote-aware for keywords)
fn split_tokens(line: &str) -> Vec<&str> {
    line.split_whitespace().collect()
}

/// Count net brace and bracket depth changes in a line, ignoring strings and comments.
fn count_depth_line(line: &str) -> (i32, i32) {
    let mut brace: i32 = 0;
    let mut bracket: i32 = 0;
    let mut in_single_quote = false;
    let mut in_line_comment = false;
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if in_line_comment {
            break;
        }
        let b = bytes[i];
        if in_single_quote {
            if b == b'\'' {
                in_single_quote = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' => {
                in_single_quote = true;
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'/' => {
                in_line_comment = true;
            }
            b'{' => brace += 1,
            b'}' => brace -= 1,
            b'[' => bracket += 1,
            b']' => bracket -= 1,
            _ => {}
        }
        i += 1;
    }
    (brace, bracket)
}

/// Count depth changes and apply them to mutable counters.
fn count_depth(line: &str, brace: &mut i32, bracket: &mut i32) {
    let (bd, kd) = count_depth_line(line);
    *brace += bd;
    *bracket += kd;
    if *brace < 0 { *brace = 0; }
    if *bracket < 0 { *bracket = 0; }
}

/// Compute the byte offset for the start of a given 1-based line number.
fn line_byte_offset(source: &str, target_line: i64) -> i64 {
    let mut offset: i64 = 0;
    for (idx, line) in source.lines().enumerate() {
        if (idx + 1) as i64 == target_line {
            return offset;
        }
        offset += line.len() as i64 + 1; // +1 for newline
    }
    offset
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_resource_line() {
        let source = "resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {\n  name: 'kv'\n}\n";
        let (decls, _, _) = parse_bicep(source);
        let res = decls.iter().find(|d| d.kind == DeclKind::Resource).unwrap();
        assert_eq!(res.name, "keyVault");
        assert_eq!(res.type_or_path.as_deref(), Some("Microsoft.KeyVault/vaults@2023-07-01"));
    }

    #[test]
    fn parse_module_line() {
        let source = "module logAnalytics 'modules/logAnalytics.bicep' = {\n  name: 'la'\n}\n";
        let (decls, _, _) = parse_bicep(source);
        let m = decls.iter().find(|d| d.kind == DeclKind::ModuleRef).unwrap();
        assert_eq!(m.name, "logAnalytics");
        assert_eq!(m.type_or_path.as_deref(), Some("modules/logAnalytics.bicep"));
    }

    #[test]
    fn parse_param_line() {
        let source = "param location string = 'eastus'\n";
        let (decls, _, _) = parse_bicep(source);
        let p = decls.iter().find(|d| d.kind == DeclKind::Param).unwrap();
        assert_eq!(p.name, "location");
        assert_eq!(p.type_or_path.as_deref(), Some("string"));
    }

    #[test]
    fn brace_and_bracket_tracking() {
        let (brace, bracket) = count_depth_line("resource x 'type' = {");
        assert_eq!(brace, 1);
        assert_eq!(bracket, 0);

        let (brace, bracket) = count_depth_line("}");
        assert_eq!(brace, -1);
        assert_eq!(bracket, 0);

        let (brace, bracket) = count_depth_line("resource x 'type' = [for i in range: {");
        assert_eq!(brace, 1);
        assert_eq!(bracket, 1);
    }

    #[test]
    fn single_quoted_string_extraction() {
        assert_eq!(
            extract_single_quoted_string("resource x 'Microsoft.KeyVault/vaults@2023-07-01' = {"),
            Some("Microsoft.KeyVault/vaults@2023-07-01".to_string())
        );
        assert_eq!(
            extract_single_quoted_string("targetScope = 'subscription'"),
            Some("subscription".to_string())
        );
        assert_eq!(extract_single_quoted_string("var x = 42"), None);
    }

    #[test]
    fn description_extraction() {
        assert_eq!(
            extract_description_decorator("@description('The location for resources')"),
            Some("The location for resources".to_string())
        );
        assert_eq!(
            extract_description_decorator("@sys.description('System desc')"),
            Some("System desc".to_string())
        );
        assert_eq!(extract_description_decorator("@secure()"), None);
    }

    #[test]
    fn normalize_azure_channel_name_strips_prefix() {
        assert_eq!(
            normalize_azure_channel_name("sbt-orchestrator-triggers"),
            Some("channel://orchestratortriggers".to_string())
        );
        assert_eq!(
            normalize_azure_channel_name("sbq-dead-letter"),
            Some("channel://deadletter".to_string())
        );
        assert_eq!(
            normalize_azure_channel_name("sbts-my-subscription"),
            Some("channel://mysubscription".to_string())
        );
    }

    #[test]
    fn normalize_azure_channel_name_no_prefix() {
        assert_eq!(
            normalize_azure_channel_name("my-topic-name"),
            Some("channel://mytopicname".to_string())
        );
        assert_eq!(
            normalize_azure_channel_name("MyTopic"),
            Some("channel://mytopic".to_string())
        );
    }

    #[test]
    fn normalize_azure_channel_name_empty() {
        assert_eq!(normalize_azure_channel_name(""), None);
        assert_eq!(normalize_azure_channel_name("   "), None);
    }

    #[test]
    fn resource_name_extraction() {
        let source = "resource topic 'Microsoft.ServiceBus/namespaces/topics@2022-10-01' = {\n  name: 'sbt-foo-bar'\n  properties: {\n    maxSizeInMegabytes: 1024\n  }\n}\n";
        let (decls, _, _) = parse_bicep(source);
        let res = decls.iter().find(|d| d.kind == DeclKind::Resource).unwrap();
        assert_eq!(res.resource_name.as_deref(), Some("sbt-foo-bar"));
    }

    #[test]
    fn resource_name_not_extracted_from_nested() {
        // name: at depth 2 (inside properties) should NOT be captured
        let source = "resource kv 'Microsoft.KeyVault/vaults@2023-07-01' = {\n  properties: {\n    name: 'nested'\n  }\n}\n";
        let (decls, _, _) = parse_bicep(source);
        let res = decls.iter().find(|d| d.kind == DeclKind::Resource).unwrap();
        assert_eq!(res.resource_name, None);
    }

    #[test]
    fn is_service_bus_topic_or_queue_checks() {
        assert!(is_service_bus_topic_or_queue("Microsoft.ServiceBus/namespaces/topics@2022-10-01"));
        assert!(is_service_bus_topic_or_queue("Microsoft.ServiceBus/namespaces/queues@2022-10-01"));
        assert!(!is_service_bus_topic_or_queue("Microsoft.ServiceBus/namespaces/topics/subscriptions@2022-10-01"));
        assert!(!is_service_bus_topic_or_queue("Microsoft.KeyVault/vaults@2023-07-01"));
    }
}
