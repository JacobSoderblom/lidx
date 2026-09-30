use crate::indexer::extract::{EdgeInput, ExtractedFile, SymbolInput};
use crate::indexer::http;
use crate::indexer::tree_helpers::module_symbol_fallback;
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
    start_token: Token,
    end_token: Token,
    rpcs: Vec<RpcDef>,
}

struct RpcDef {
    name: String,
    start_token: Token,
    end_token: Token,
    request: Option<String>,
    response: Option<String>,
}

struct ActiveService {
    service: ServiceDef,
    depth: usize,
}

/// A `message` declaration, top-level or nested inside another message.
struct MessageDef {
    name: String,
    start_token: Token,
    end_token: Token,
    fields: Vec<MemberDef>,
    nested_messages: Vec<MessageDef>,
    nested_enums: Vec<EnumDef>,
}

/// An `enum` declaration, top-level or nested inside a message.
struct EnumDef {
    name: String,
    start_token: Token,
    end_token: Token,
    values: Vec<MemberDef>,
}

/// A message field or enum value: `<...> name = N [options];`. Shared shape
/// because both are just "an identifier bound to a number", which is all
/// `parse_member_statement` needs to locate one.
struct MemberDef {
    name: String,
    start_token: Token,
    end_token: Token,
}

pub struct ProtoExtractor;

impl ProtoExtractor {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }
}

impl crate::indexer::extract::LanguageExtractor for ProtoExtractor {
    fn module_name_from_rel_path(&self, rel_path: &str) -> String {
        module_name_from_rel_path(rel_path)
    }

    fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile> {
        let mut output = ExtractedFile::default();
        output
            .symbols
            .push(module_symbol_fallback(module_name, source, "/", None));

        let tokens = tokenize_proto(source);
        let package = find_package(&tokens);
        let services = parse_services(&tokens);
        let (messages, enums) = parse_top_level_types(&tokens);

        for service in services {
            let service_name = service.name.clone();
            let service_qualname = build_package_qualname(package.as_deref(), &service_name);
            let service_symbol = symbol_from_span(
                "service",
                &service_name,
                &service_qualname,
                &service.start_token,
                &service.end_token,
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
                let rpc_symbol = symbol_from_span(
                    "rpc",
                    &rpc.name,
                    &rpc_qualname,
                    &rpc.start_token,
                    &rpc.end_token,
                );
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

        for message in &messages {
            emit_message(message, package.as_deref(), module_name, None, &mut output);
        }
        for enum_def in &enums {
            emit_enum(enum_def, package.as_deref(), module_name, None, &mut output);
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

/// Builds a symbol spanning from `start_token` through `end_token` — e.g.
/// the `service`/`rpc`/`message`/`enum` keyword through the declaration's
/// closing brace (or terminating `;`), rather than collapsing the whole
/// symbol onto the name token's own line.
fn symbol_from_span(
    kind: &str,
    name: &str,
    qualname: &str,
    start_token: &Token,
    end_token: &Token,
) -> SymbolInput {
    SymbolInput {
        kind: kind.to_string(),
        name: name.to_string(),
        qualname: qualname.to_string(),
        start_line: start_token.start_line,
        start_col: start_token.start_col,
        end_line: end_token.start_line,
        end_col: end_token.start_col + end_token.text.len() as i64,
        start_byte: start_token.start_byte,
        end_byte: end_token.end_byte,
        signature: None,
        docstring: None,
        identity: None,
    }
}

fn build_package_qualname(package: Option<&str>, name: &str) -> String {
    match package {
        Some(package) if !package.is_empty() => format!("{package}.{name}"),
        _ => name.to_string(),
    }
}

/// Emits a `message` symbol (top-level or nested), its `CONTAINS` edge from
/// its container, and recurses into its fields and nested types. Nested
/// qualnames dot-join onto the parent, matching this repo's convention for
/// nested types (see e.g. `csharp::build_qualname`).
fn emit_message(
    message: &MessageDef,
    package: Option<&str>,
    module_name: &str,
    parent_qualname: Option<&str>,
    output: &mut ExtractedFile,
) {
    let qualname = match parent_qualname {
        Some(parent) => format!("{parent}.{}", message.name),
        None => build_package_qualname(package, &message.name),
    };
    output.symbols.push(symbol_from_span(
        "message",
        &message.name,
        &qualname,
        &message.start_token,
        &message.end_token,
    ));
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(parent_qualname.unwrap_or(module_name).to_string()),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });

    for field in &message.fields {
        emit_member(field, "field", &qualname, output);
    }
    for nested in &message.nested_messages {
        emit_message(nested, package, module_name, Some(&qualname), output);
    }
    for nested_enum in &message.nested_enums {
        emit_enum(nested_enum, package, module_name, Some(&qualname), output);
    }
}

/// Emits an `enum` symbol (top-level or nested inside a message), its
/// `CONTAINS` edge from its container, and its enum values.
fn emit_enum(
    enum_def: &EnumDef,
    package: Option<&str>,
    module_name: &str,
    parent_qualname: Option<&str>,
    output: &mut ExtractedFile,
) {
    let qualname = match parent_qualname {
        Some(parent) => format!("{parent}.{}", enum_def.name),
        None => build_package_qualname(package, &enum_def.name),
    };
    output.symbols.push(symbol_from_span(
        "enum",
        &enum_def.name,
        &qualname,
        &enum_def.start_token,
        &enum_def.end_token,
    ));
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(parent_qualname.unwrap_or(module_name).to_string()),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });

    for value in &enum_def.values {
        emit_member(value, "enum_value", &qualname, output);
    }
}

/// Emits a message field or enum value symbol plus its `CONTAINS` edge from
/// `parent_qualname`.
fn emit_member(member: &MemberDef, kind: &str, parent_qualname: &str, output: &mut ExtractedFile) {
    let qualname = format!("{parent_qualname}.{}", member.name);
    output.symbols.push(symbol_from_span(
        kind,
        &member.name,
        &qualname,
        &member.start_token,
        &member.end_token,
    ));
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(parent_qualname.to_string()),
        target_qualname: Some(qualname),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
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
        if tokens[idx].is_ident("package")
            && let Some(name) = tokens.get(idx + 1).filter(|t| t.is_ident_any())
        {
            return Some(name.text.clone());
        }
        idx += 1;
    }
    None
}

fn parse_services(tokens: &[Token]) -> Vec<ServiceDef> {
    let mut services = Vec::new();
    let mut idx = 0;
    let mut pending_service: Option<(Token, Token)> = None;
    let mut active: Option<ActiveService> = None;
    while idx < tokens.len() {
        let token = &tokens[idx];
        if token.is_ident("service") {
            if let Some(name_token) = tokens.get(idx + 1).filter(|t| t.is_ident_any()) {
                pending_service = Some((token.clone(), name_token.clone()));
            }
            idx += 1;
            continue;
        }

        if token.is_punct('{') {
            if let Some((start_token, name_token)) = pending_service.take() {
                let service = ServiceDef {
                    name: name_token.text,
                    start_token,
                    end_token: token.clone(),
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
                active_service.service.end_token = token.clone();
                if active_service.depth == 0
                    && let Some(active) = active.take()
                {
                    services.push(active.service);
                }
            }
        } else if token.is_ident("rpc")
            && let Some(active) = active.as_mut()
            && let Some(rpc) = parse_rpc(tokens, idx)
        {
            active.service.rpcs.push(rpc);
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
            end_token = statement_end_token(tokens, idx);
            return Some(RpcDef {
                name: name_token.text,
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
        if token.is_ident("returns")
            && let Some(next) = tokens.get(idx + 1)
            && next.is_punct('(')
        {
            let (value, end_idx) = parse_type_in_parens(tokens, idx + 1);
            response = value;
            idx = end_idx + 1;
            if tokens
                .get(idx)
                .is_some_and(|t| t.is_punct(';') || t.is_punct('{'))
            {
                end_token = statement_end_token(tokens, idx);
            }
            break;
        }
        if token.is_punct(';') || token.is_punct('{') {
            end_token = statement_end_token(tokens, idx);
            break;
        }
        idx += 1;
    }
    Some(RpcDef {
        name: name_token.text,
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

/// Finds the `}` matching the `{` at `open_idx` by brace-depth counting.
/// Falls back to the last token if unterminated (malformed input) so
/// callers always get a usable, in-bounds index rather than panicking.
fn matching_close_brace(tokens: &[Token], open_idx: usize) -> usize {
    let mut depth = 0i32;
    let mut idx = open_idx;
    while idx < tokens.len() {
        if tokens[idx].is_punct('{') {
            depth += 1;
        } else if tokens[idx].is_punct('}') {
            depth -= 1;
            if depth == 0 {
                return idx;
            }
        }
        idx += 1;
    }
    tokens.len().saturating_sub(1)
}

/// Resolves the true end of a statement whose terminator token is at
/// `idx`: a `;` terminates immediately, but a `{` (e.g. an rpc's inline
/// options body) only terminates at its *matching* `}`, which may be many
/// tokens — and lines — later.
fn statement_end_token(tokens: &[Token], idx: usize) -> Token {
    let token = &tokens[idx];
    if token.is_punct('{') {
        tokens[matching_close_brace(tokens, idx)].clone()
    } else {
        token.clone()
    }
}

/// Scans the whole token stream for top-level `message`/`enum`
/// declarations, skipping over any other block (e.g. a `service` body,
/// which is parsed separately by `parse_services`) by brace-balancing
/// past it.
fn parse_top_level_types(tokens: &[Token]) -> (Vec<MessageDef>, Vec<EnumDef>) {
    let mut messages = Vec::new();
    let mut enums = Vec::new();
    let mut idx = 0;
    while idx < tokens.len() {
        let token = &tokens[idx];
        if token.is_ident("message") {
            if let Some((def, next_idx)) = parse_message(tokens, idx) {
                messages.push(def);
                idx = next_idx;
                continue;
            }
        } else if token.is_ident("enum") {
            if let Some((def, next_idx)) = parse_enum(tokens, idx) {
                enums.push(def);
                idx = next_idx;
                continue;
            }
        } else if token.is_punct('{') {
            idx = matching_close_brace(tokens, idx) + 1;
            continue;
        }
        idx += 1;
    }
    (messages, enums)
}

/// Parses a `message Name { ... }` declaration starting at the `message`
/// keyword token (`keyword_idx`), including nested messages/enums/fields.
/// Returns the parsed definition and the index just past its closing `}`.
fn parse_message(tokens: &[Token], keyword_idx: usize) -> Option<(MessageDef, usize)> {
    let start_token = tokens.get(keyword_idx)?.clone();
    let name_token = tokens.get(keyword_idx + 1).filter(|t| t.is_ident_any())?;
    let name = name_token.text.clone();
    let open_idx = keyword_idx + 2;
    if !tokens.get(open_idx)?.is_punct('{') {
        return None;
    }
    let close_idx = matching_close_brace(tokens, open_idx);
    let end_token = tokens[close_idx].clone();
    let (fields, nested_messages, nested_enums) =
        parse_block_members(tokens, open_idx + 1, close_idx);
    Some((
        MessageDef {
            name,
            start_token,
            end_token,
            fields,
            nested_messages,
            nested_enums,
        },
        close_idx + 1,
    ))
}

/// Parses an `enum Name { ... }` declaration starting at the `enum`
/// keyword token, including its values. Returns the parsed definition and
/// the index just past its closing `}`.
fn parse_enum(tokens: &[Token], keyword_idx: usize) -> Option<(EnumDef, usize)> {
    let start_token = tokens.get(keyword_idx)?.clone();
    let name_token = tokens.get(keyword_idx + 1).filter(|t| t.is_ident_any())?;
    let name = name_token.text.clone();
    let open_idx = keyword_idx + 2;
    if !tokens.get(open_idx)?.is_punct('{') {
        return None;
    }
    let close_idx = matching_close_brace(tokens, open_idx);
    let end_token = tokens[close_idx].clone();
    let (values, _, _) = parse_block_members(tokens, open_idx + 1, close_idx);
    Some((
        EnumDef {
            name,
            start_token,
            end_token,
            values,
        },
        close_idx + 1,
    ))
}

/// Scans the body of a message or enum (the token range `[start, end)`,
/// `end` being the index of the closing `}`) for nested messages, nested
/// enums, and members (fields/enum values). `option`/`reserved`/
/// `extensions`/`extend` statements are recognized and skipped rather than
/// misparsed as members; `oneof` bodies are flattened into `fields` since a
/// oneof is not itself a symbol kind this extractor models.
fn parse_block_members(
    tokens: &[Token],
    start: usize,
    end: usize,
) -> (Vec<MemberDef>, Vec<MessageDef>, Vec<EnumDef>) {
    let mut fields = Vec::new();
    let mut nested_messages = Vec::new();
    let mut nested_enums = Vec::new();
    let mut idx = start;
    while idx < end {
        let token = &tokens[idx];
        if token.is_ident("message") {
            if let Some((def, next_idx)) = parse_message(tokens, idx) {
                nested_messages.push(def);
                idx = next_idx;
                continue;
            }
        } else if token.is_ident("enum") {
            if let Some((def, next_idx)) = parse_enum(tokens, idx) {
                nested_enums.push(def);
                idx = next_idx;
                continue;
            }
        } else if token.is_ident("oneof") {
            if let Some(next_idx) = parse_oneof_into(tokens, idx, &mut fields) {
                idx = next_idx;
                continue;
            }
        } else if token.is_ident("option")
            || token.is_ident("reserved")
            || token.is_ident("extensions")
            || token.is_ident("extend")
        {
            idx = skip_statement(tokens, idx, end);
            continue;
        } else if token.is_ident_any() {
            if let Some((member, next_idx)) = parse_member_statement(tokens, idx, end) {
                fields.push(member);
                idx = next_idx;
                continue;
            }
            idx = skip_statement(tokens, idx, end);
            continue;
        }
        idx += 1;
    }
    (fields, nested_messages, nested_enums)
}

/// Parses a `oneof name { ... }` block starting at the `oneof` keyword
/// token, appending any fields found inside it to `fields` (a oneof isn't
/// modeled as its own symbol, just a grouping of fields). Returns the
/// index just past its closing `}`.
fn parse_oneof_into(
    tokens: &[Token],
    keyword_idx: usize,
    fields: &mut Vec<MemberDef>,
) -> Option<usize> {
    tokens.get(keyword_idx + 1).filter(|t| t.is_ident_any())?;
    let open_idx = keyword_idx + 2;
    if !tokens.get(open_idx)?.is_punct('{') {
        return None;
    }
    let close_idx = matching_close_brace(tokens, open_idx);
    let (mut oneof_fields, _, _) = parse_block_members(tokens, open_idx + 1, close_idx);
    fields.append(&mut oneof_fields);
    Some(close_idx + 1)
}

/// Parses a single field or enum-value statement starting at `start`
/// (bounded by `end`): `<label/type tokens...> name = number [opts];`.
/// The name is the identifier immediately preceding the *first* top-level
/// `=`, which is robust to trailing bracketed field options (which may
/// themselves contain identifiers and `=` signs, e.g.
/// `[deprecated = true]`) since those come after the field's own `=` and
/// are never examined. Returns `None` (not a member statement) for
/// anything without a top-level `=` before its terminator, or that hits an
/// unexpected `{` (e.g. a deprecated proto2 group field) — callers skip
/// such statements instead.
fn parse_member_statement(
    tokens: &[Token],
    start: usize,
    end: usize,
) -> Option<(MemberDef, usize)> {
    let start_token = tokens.get(start)?.clone();
    let mut last_ident: Option<Token> = None;
    let mut idx = start;
    while idx < end {
        let token = &tokens[idx];
        if token.is_punct('=') {
            let name_token = last_ident?;
            let mut term_idx = idx + 1;
            while term_idx < end {
                let term = &tokens[term_idx];
                if term.is_punct(';') {
                    return Some((
                        MemberDef {
                            name: name_token.text.clone(),
                            start_token,
                            end_token: term.clone(),
                        },
                        term_idx + 1,
                    ));
                }
                if term.is_punct('{') {
                    return None;
                }
                term_idx += 1;
            }
            return None;
        }
        if token.is_punct(';') || token.is_punct('{') {
            return None;
        }
        if token.is_ident_any() {
            last_ident = Some(token.clone());
        }
        idx += 1;
    }
    None
}

/// Advances past one statement without extracting anything from it: to the
/// token right after its terminating `;`, brace-balancing past any `{...}`
/// encountered along the way (e.g. an `option (foo) = { ... };` message
/// literal, or an `extend Foo { ... }` block).
fn skip_statement(tokens: &[Token], start: usize, end: usize) -> usize {
    let mut idx = start;
    while idx < end {
        if tokens[idx].is_punct(';') {
            return idx + 1;
        }
        if tokens[idx].is_punct('{') {
            idx = matching_close_brace(tokens, idx) + 1;
            continue;
        }
        idx += 1;
    }
    end
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

        if matches!(ch, '{' | '}' | '(' | ')' | ';' | '=') {
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
    if current == b'.'
        && let Some(next) = next
    {
        return next.is_ascii_alphabetic() || next == b'_';
    }
    false
}

fn is_ident_continue(current: u8) -> bool {
    current.is_ascii_alphanumeric() || current == b'_' || current == b'.'
}

#[cfg(test)]
mod tests {
    use super::{ProtoExtractor, RPC_ROUTE_KIND};
    use crate::indexer::extract::{ExtractedFile, LanguageExtractor, SymbolInput};

    /// Fixture covering: top-level + nested messages, a nested enum, a
    /// top-level enum, message fields, enum values, and a service whose
    /// second rpc has a multi-line option body — used to pin down exact
    /// line spans below. Line numbers in the assertions are 1-indexed and
    /// match this literal exactly (line 1 is the blank line right after
    /// the opening `r#"`), so keep them in sync if this fixture changes.
    const FIXTURE: &str = r#"
syntax = "proto3";
package example.v1;

message Address {
  string street = 1;
  string city = 2;
}

message User {
  string name = 1;
  int32 age = 2;
  Address address = 3;

  message Preferences {
    bool newsletter = 1;
  }

  enum Status {
    ACTIVE = 0;
    INACTIVE = 1;
  }
}

enum Role {
  ROLE_UNSPECIFIED = 0;
  ROLE_ADMIN = 1;
}

service UserService {
  rpc GetUser (GetUserRequest) returns (GetUserResponse);
  rpc StreamUsers (stream UserRequest) returns (stream UserResponse) {
    option (google.api.http) = {
      get: "/v1/users"
    };
  }
}
"#;

    fn find_symbol<'a>(file: &'a ExtractedFile, qualname: &str) -> &'a SymbolInput {
        file.symbols
            .iter()
            .find(|s| s.qualname == qualname)
            .unwrap_or_else(|| panic!("missing symbol: {qualname}"))
    }

    fn has_contains_edge(file: &ExtractedFile, source: &str, target: &str) -> bool {
        file.edges.iter().any(|e| {
            e.kind == "CONTAINS"
                && e.source_qualname.as_deref() == Some(source)
                && e.target_qualname.as_deref() == Some(target)
        })
    }

    #[test]
    fn extracts_top_level_and_nested_messages_with_full_spans() {
        let mut extractor = ProtoExtractor::new().unwrap();
        let file = extractor.extract(FIXTURE, "proto").unwrap();

        let address = find_symbol(&file, "example.v1.Address");
        assert_eq!(address.kind, "message");
        assert_eq!(address.start_line, 5);
        assert_eq!(address.end_line, 8);

        let user = find_symbol(&file, "example.v1.User");
        assert_eq!(user.kind, "message");
        assert_eq!(user.start_line, 10);
        assert_eq!(user.end_line, 23);

        let preferences = find_symbol(&file, "example.v1.User.Preferences");
        assert_eq!(preferences.kind, "message");
        assert_eq!(preferences.start_line, 15);
        assert_eq!(preferences.end_line, 17);

        assert!(has_contains_edge(&file, "proto", "example.v1.Address"));
        assert!(has_contains_edge(&file, "proto", "example.v1.User"));
        assert!(has_contains_edge(
            &file,
            "example.v1.User",
            "example.v1.User.Preferences"
        ));
    }

    #[test]
    fn extracts_enum_symbols_with_full_spans() {
        let mut extractor = ProtoExtractor::new().unwrap();
        let file = extractor.extract(FIXTURE, "proto").unwrap();

        let status = find_symbol(&file, "example.v1.User.Status");
        assert_eq!(status.kind, "enum");
        assert_eq!(status.start_line, 19);
        assert_eq!(status.end_line, 22);

        let role = find_symbol(&file, "example.v1.Role");
        assert_eq!(role.kind, "enum");
        assert_eq!(role.start_line, 25);
        assert_eq!(role.end_line, 28);

        assert!(has_contains_edge(
            &file,
            "example.v1.User",
            "example.v1.User.Status"
        ));
        assert!(has_contains_edge(&file, "proto", "example.v1.Role"));
    }

    #[test]
    fn extracts_message_fields_and_enum_values() {
        let mut extractor = ProtoExtractor::new().unwrap();
        let file = extractor.extract(FIXTURE, "proto").unwrap();

        let street = find_symbol(&file, "example.v1.Address.street");
        assert_eq!(street.kind, "field");
        assert_eq!(street.start_line, 6);
        assert_eq!(street.end_line, 6);

        let newsletter = find_symbol(&file, "example.v1.User.Preferences.newsletter");
        assert_eq!(newsletter.kind, "field");
        assert_eq!(newsletter.start_line, 16);

        let active = find_symbol(&file, "example.v1.User.Status.ACTIVE");
        assert_eq!(active.kind, "enum_value");
        assert_eq!(active.start_line, 20);

        assert!(has_contains_edge(
            &file,
            "example.v1.Address",
            "example.v1.Address.street"
        ));
        assert!(has_contains_edge(
            &file,
            "example.v1.User.Status",
            "example.v1.User.Status.ACTIVE"
        ));
    }

    #[test]
    fn service_and_rpc_symbol_spans_cover_full_declaration() {
        let mut extractor = ProtoExtractor::new().unwrap();
        let file = extractor.extract(FIXTURE, "proto").unwrap();

        let service = find_symbol(&file, "example.v1.UserService");
        assert_eq!(service.kind, "service");
        assert_eq!(service.start_line, 30);
        assert_eq!(service.end_line, 37);
        assert_ne!(
            service.start_line, service.end_line,
            "service span regressed to name-line-only"
        );

        let get_user = find_symbol(&file, "example.v1.UserService.GetUser");
        assert_eq!(get_user.start_line, 31);
        assert_eq!(get_user.end_line, 31);

        let stream_users = find_symbol(&file, "example.v1.UserService.StreamUsers");
        assert_eq!(stream_users.start_line, 32);
        assert_eq!(stream_users.end_line, 36);
        assert_ne!(
            stream_users.start_line, stream_users.end_line,
            "rpc with a multi-line option body must span the whole declaration"
        );
    }

    #[test]
    fn handles_map_oneof_reserved_and_bracketed_field_options_without_misparsing() {
        let source = r#"
syntax = "proto3";
package example.v1;

message Complex {
  reserved 2, 15, 9 to 11;
  reserved "foo", "bar";
  option deprecated = true;

  map<string, string> attributes = 1;

  oneof kind {
    string text = 2;
    int32 number = 3 [deprecated = true];
  }

  option (custom.thing) = {
    nested: true
  };
}
"#;
        let mut extractor = ProtoExtractor::new().unwrap();
        let file = extractor.extract(source, "proto").unwrap();

        let field_names: std::collections::BTreeSet<&str> = file
            .symbols
            .iter()
            .filter(|s| s.kind == "field")
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(
            field_names,
            std::collections::BTreeSet::from(["attributes", "text", "number"]),
            "reserved/option statements must not be misparsed as fields, and \
             map/oneof fields must still be found"
        );

        let number = find_symbol(&file, "example.v1.Complex.number");
        assert_eq!(number.name, "number");
    }

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
