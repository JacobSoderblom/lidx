use crate::indexer::extract::{EdgeInput, ExtractedFile, SymbolInput};
use crate::indexer::http;
use crate::util;
use anyhow::Result;
use serde_json::json;
use std::path::Path;

pub const RPC_ROUTE_KIND: &str = "RPC_ROUTE";
pub const RPC_CALL_KIND: &str = "RPC_CALL";
pub const RPC_IMPL_KIND: &str = "RPC_IMPL";

#[derive(Clone)]
struct Token {
    kind: TokenKind,
    text: String,
    start_line: i64,
    start_col: i64,
    start_byte: i64,
    end_byte: i64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TokenKind {
    Ident,
    Punct(char),
}

impl Token {
    fn is_ident(&self, value: &str) -> bool {
        self.kind == TokenKind::Ident && self.text == value
    }

    fn is_ident_any(&self) -> bool {
        self.kind == TokenKind::Ident
    }

    fn is_punct(&self, ch: char) -> bool {
        self.kind == TokenKind::Punct(ch)
    }
}

struct ServiceDef {
    name: String,
    name_token: Token,
    rpcs: Vec<RpcDef>,
}

struct RpcDef {
    name: String,
    name_token: Token,
    start_token: Token,
    end_token: Token,
    request: Option<String>,
    response: Option<String>,
}

struct ActiveService {
    service: ServiceDef,
    depth: usize,
}

pub struct ProtoExtractor;

impl ProtoExtractor {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }

    pub fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile> {
        let mut output = ExtractedFile::default();
        output
            .symbols
            .push(module_symbol_with_span(module_name, span_whole(source)));

        let tokens = tokenize_proto(source);
        let package = find_package(&tokens);
        let services = parse_services(&tokens);

        for service in services {
            let service_name = service.name.clone();
            let service_qualname = build_service_qualname(package.as_deref(), &service_name);
            let service_symbol = symbol_from_token(
                "service",
                &service_name,
                &service_qualname,
                &service.name_token,
            );
            output.symbols.push(service_symbol);
            output.edges.push(EdgeInput {
                kind: "CONTAINS".to_string(),
                source_qualname: Some(module_name.to_string()),
                target_qualname: Some(service_qualname.clone()),
                detail: None,
                evidence_snippet: None,
                ..Default::default()
            });

            for rpc in service.rpcs {
                let rpc_qualname = format!("{service_qualname}.{}", rpc.name);
                let rpc_symbol =
                    symbol_from_token("rpc", &rpc.name, &rpc_qualname, &rpc.name_token);
                output.symbols.push(rpc_symbol);
                output.edges.push(EdgeInput {
                    kind: "CONTAINS".to_string(),
                    source_qualname: Some(service_qualname.clone()),
                    target_qualname: Some(rpc_qualname.clone()),
                    detail: None,
                    evidence_snippet: None,
                    ..Default::default()
                });

                let Some((raw_path, normalized)) =
                    normalize_rpc_path(package.as_deref(), &service_name, &rpc.name)
                else {
                    continue;
                };
                let detail = json!({
                    "protocol": "grpc",
                    "package": package.as_deref(),
                    "service": service_name.as_str(),
                    "rpc": rpc.name,
                    "request": rpc.request,
                    "response": rpc.response,
                    "path": normalized,
                    "raw": raw_path,
                })
                .to_string();
                let snippet = util::edge_evidence_snippet(
                    source,
                    rpc.start_token.start_byte,
                    rpc.end_token.end_byte,
                    rpc.start_token.start_line,
                    rpc.end_token.start_line,
                );
                output.edges.push(EdgeInput {
                    kind: RPC_ROUTE_KIND.to_string(),
                    source_qualname: Some(rpc_qualname),
                    target_qualname: Some(normalized),
                    detail: Some(detail),
                    evidence_snippet: snippet,
                    evidence_start_line: Some(rpc.start_token.start_line),
                    evidence_end_line: Some(rpc.end_token.start_line),
                    ..Default::default()
                });
            }
        }

        Ok(output)
    }
}

pub fn module_name_from_rel_path(rel_path: &str) -> String {
    let path = Path::new(rel_path);
    let mut parts: Vec<String> = path
        .components()
        .filter_map(|comp| comp.as_os_str().to_str().map(|s| s.to_string()))
        .collect();
    if parts.is_empty() {
        return "proto".to_string();
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
        "proto".to_string()
    } else {
        parts.join("/")
    }
}

fn span_whole(source: &str) -> (i64, i64, i64, i64, i64, i64) {
    (1, 1, line_count(source), 1, 0, source.len() as i64)
}

fn module_symbol_with_span(module_name: &str, span: (i64, i64, i64, i64, i64, i64)) -> SymbolInput {
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

fn symbol_from_token(kind: &str, name: &str, qualname: &str, token: &Token) -> SymbolInput {
    SymbolInput {
        kind: kind.to_string(),
        name: name.to_string(),
        qualname: qualname.to_string(),
        start_line: token.start_line,
        start_col: token.start_col,
        end_line: token.start_line,
        end_col: token.start_col + token.text.len() as i64,
        start_byte: token.start_byte,
        end_byte: token.end_byte,
        signature: None,
        docstring: None,
    }
}

fn build_service_qualname(package: Option<&str>, service: &str) -> String {
    match package {
        Some(package) if !package.is_empty() => format!("{package}.{service}"),
        _ => service.to_string(),
    }
}

pub fn normalize_rpc_path(
    package: Option<&str>,
    service: &str,
    rpc: &str,
) -> Option<(String, String)> {
    let service = match package {
        Some(package) if !package.is_empty() => format!("{package}.{service}"),
        _ => service.to_string(),
    };
    let raw = format!("/{service}/{rpc}");
    let normalized = http::normalize_path(&raw)?;
    Some((raw, normalized))
}

fn find_package(tokens: &[Token]) -> Option<String> {
    let mut idx = 0;
    while idx + 1 < tokens.len() {
        if tokens[idx].is_ident("package") {
            if let Some(name) = tokens.get(idx + 1).filter(|t| t.is_ident_any()) {
                return Some(name.text.clone());
            }
        }
        idx += 1;
    }
    None
}

fn parse_services(tokens: &[Token]) -> Vec<ServiceDef> {
    let mut services = Vec::new();
    let mut idx = 0;
    let mut pending_service: Option<Token> = None;
    let mut active: Option<ActiveService> = None;
    while idx < tokens.len() {
        let token = &tokens[idx];
        if token.is_ident("service") {
            if let Some(name_token) = tokens.get(idx + 1).filter(|t| t.is_ident_any()) {
                pending_service = Some(name_token.clone());
            }
            idx += 1;
            continue;
        }

        if token.is_punct('{') {
            if let Some(name_token) = pending_service.take() {
                let service = ServiceDef {
                    name: name_token.text.clone(),
                    name_token,
                    rpcs: Vec::new(),
                };
                active = Some(ActiveService { service, depth: 1 });
                idx += 1;
                continue;
            }
            if let Some(active) = active.as_mut() {
                active.depth += 1;
            }
        } else if token.is_punct('}') {
            if let Some(active_service) = active.as_mut() {
                if active_service.depth > 0 {
                    active_service.depth -= 1;
                }
                if active_service.depth == 0 {
                    if let Some(active) = active.take() {
                        services.push(active.service);
                    }
                }
            }
        } else if token.is_ident("rpc") {
            if let Some(active) = active.as_mut() {
                if let Some(rpc) = parse_rpc(tokens, idx) {
                    active.service.rpcs.push(rpc);
                }
            }
        }
        idx += 1;
    }
    if let Some(active) = active {
        services.push(active.service);
    }
    services
}

fn parse_rpc(tokens: &[Token], start_idx: usize) -> Option<RpcDef> {
    let start_token = tokens.get(start_idx)?.clone();
    let name_token = tokens.get(start_idx + 1)?.clone();
    if !name_token.is_ident_any() {
        return None;
    }
    let mut end_token = name_token.clone();
    let mut request = None;
    let mut response = None;
    let mut idx = start_idx + 2;
    while idx < tokens.len() {
        let token = &tokens[idx];
        if token.is_punct('(') {
            let (value, end_idx) = parse_type_in_parens(tokens, idx);
            request = value;
            idx = end_idx + 1;
            break;
        }
        if token.is_punct(';') || token.is_punct('{') {
            end_token = token.clone();
            return Some(RpcDef {
                name: name_token.text.clone(),
                name_token,
                start_token,
                end_token,
                request,
                response,
            });
        }
        idx += 1;
    }
    while idx < tokens.len() {
        let token = &tokens[idx];
        if token.is_ident("returns") {
            if let Some(next) = tokens.get(idx + 1) {
                if next.is_punct('(') {
                    let (value, end_idx) = parse_type_in_parens(tokens, idx + 1);
                    response = value;
                    idx = end_idx + 1;
                    if let Some(token) = tokens
                        .get(idx)
                        .filter(|t| t.is_punct(';') || t.is_punct('{'))
                    {
                        end_token = token.clone();
                    }
                    break;
                }
            }
        }
        if token.is_punct(';') || token.is_punct('{') {
            end_token = token.clone();
            break;
        }
        idx += 1;
    }
    Some(RpcDef {
        name: name_token.text.clone(),
        name_token,
        start_token,
        end_token,
        request,
        response,
    })
}

fn parse_type_in_parens(tokens: &[Token], open_idx: usize) -> (Option<String>, usize) {
    let mut parts = Vec::new();
    let mut idx = open_idx + 1;
    while idx < tokens.len() {
        let token = &tokens[idx];
        if token.is_punct(')') {
            break;
        }
        if token.is_ident_any() {
            parts.push(token.text.clone());
        }
        idx += 1;
    }
    let value = if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    };
    (value, idx)
}

fn tokenize_proto(source: &str) -> Vec<Token> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut idx = 0usize;
    let mut line = 1i64;
    let mut col = 1i64;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if byte == b'/' && idx + 1 < bytes.len() {
            let next = bytes[idx + 1];
            if next == b'/' {
                idx += 2;
                col += 2;
                while idx < bytes.len() && bytes[idx] != b'\n' {
                    idx += 1;
                    col += 1;
                }
                continue;
            }
            if next == b'*' {
                idx += 2;
                col += 2;
                while idx + 1 < bytes.len() {
                    if bytes[idx] == b'*' && bytes[idx + 1] == b'/' {
                        idx += 2;
                        col += 2;
                        break;
                    }
                    if bytes[idx] == b'\n' {
                        line += 1;
                        col = 1;
                        idx += 1;
                        continue;
                    }
                    idx += 1;
                    col += 1;
                }
                continue;
            }
        }

        if byte.is_ascii_whitespace() {
            if byte == b'\n' {
                line += 1;
                col = 1;
            } else {
                col += 1;
            }
            idx += 1;
            continue;
        }

        let ch = byte as char;
        if is_ident_start(byte, bytes.get(idx + 1).copied()) {
            let start = idx;
            let start_line = line;
            let start_col = col;
            idx += 1;
            col += 1;
            while idx < bytes.len() && is_ident_continue(bytes[idx]) {
                idx += 1;
                col += 1;
            }
            let text = source.get(start..idx).unwrap_or("").to_string();
            tokens.push(Token {
                kind: TokenKind::Ident,
                text,
                start_line,
                start_col,
                start_byte: start as i64,
                end_byte: idx as i64,
            });
            continue;
        }

        if matches!(ch, '{' | '}' | '(' | ')' | ';') {
            tokens.push(Token {
                kind: TokenKind::Punct(ch),
                text: ch.to_string(),
                start_line: line,
                start_col: col,
                start_byte: idx as i64,
                end_byte: (idx + 1) as i64,
            });
        }
        idx += 1;
        col += 1;
    }
    tokens
}

fn is_ident_start(current: u8, next: Option<u8>) -> bool {
    if current.is_ascii_alphabetic() || current == b'_' {
        return true;
    }
    if current == b'.' {
        if let Some(next) = next {
            return next.is_ascii_alphabetic() || next == b'_';
        }
    }
    false
}

fn is_ident_continue(current: u8) -> bool {
    current.is_ascii_alphanumeric() || current == b'_' || current == b'.'
}

fn line_count(source: &str) -> i64 {
    let count = source.lines().count();
    if count == 0 { 1 } else { count as i64 }
}

#[cfg(test)]
mod tests {
    use super::{ProtoExtractor, RPC_ROUTE_KIND};

    #[test]
    fn extracts_proto_services_and_rpcs() {
        let source = r#"
syntax = "proto3";
package example.v1;

service UserService {
  rpc GetUser (GetUserRequest) returns (GetUserResponse);
  rpc StreamUsers (stream UserRequest) returns (stream UserResponse) {}
}
"#;
        let mut extractor = ProtoExtractor::new().unwrap();
        let file = extractor.extract(source, "proto").unwrap();
        let routes = file
            .edges
            .iter()
            .filter(|edge| edge.kind == RPC_ROUTE_KIND)
            .collect::<Vec<_>>();
        assert!(
            routes
                .iter()
                .any(|edge| edge.target_qualname.as_deref()
                    == Some("/example.v1.userservice/getuser"))
        );
        assert!(routes.iter().any(|edge| {
            edge.target_qualname.as_deref() == Some("/example.v1.userservice/streamusers")
        }));
    }
}
