//! Same-file string constants: "this name is really this string".
//!
//! One lookup shared by every consumer that needs a static string from an
//! identifier (channel topics today, env names for #225). Module- and
//! class-level constants only; function-local bindings are the caller's
//! concern. A name declared twice with different values is ambiguous and is
//! dropped rather than guessed.

use crate::indexer::channel::{StringConsts, string_literal_value};
use crate::indexer::tree_helpers::node_text;
use std::collections::HashMap;
use tree_sitter::Node;

#[derive(Clone, Copy)]
pub enum ConstLang {
    Python,
    CSharp,
    Go,
    JavaScript,
    Rust,
}

pub fn collect_string_consts(lang: ConstLang, root: Node<'_>, source: &str) -> StringConsts {
    let mut found: HashMap<String, Option<String>> = HashMap::new();
    walk(lang, root, source, &mut found);
    found
        .into_iter()
        .filter_map(|(k, v)| v.map(|v| (k, v)))
        .collect()
}

fn record(found: &mut HashMap<String, Option<String>>, name: &str, value_text: &str) {
    let name = name.trim();
    if name.is_empty() {
        return;
    }
    let value = string_literal_value(value_text);
    match found.get(name) {
        Some(existing) if *existing != value => {
            found.insert(name.to_string(), None);
        }
        Some(_) => {}
        None => {
            found.insert(name.to_string(), value);
        }
    }
}

fn is_function_scope(kind: &str) -> bool {
    kind.contains("function")
        || kind.contains("method")
        || kind.contains("lambda")
        || kind.contains("arrow")
        || kind.contains("constructor")
        || kind.contains("accessor")
        || kind == "closure_expression"
}

fn walk(
    lang: ConstLang,
    node: Node<'_>,
    source: &str,
    found: &mut HashMap<String, Option<String>>,
) {
    let kind = node.kind();
    if is_function_scope(kind) {
        return;
    }
    match (lang, kind) {
        (ConstLang::Python, "assignment") => {
            if let (Some(left), Some(right)) = (
                node.child_by_field_name("left"),
                node.child_by_field_name("right"),
            ) && left.kind() == "identifier"
            {
                record(found, &node_text(left, source), &node_text(right, source));
            }
        }
        (ConstLang::CSharp, "field_declaration") => {
            let text = node_text(node, source);
            let head = text.split('=').next().unwrap_or("");
            let constant = head
                .split(|c: char| !c.is_alphanumeric())
                .any(|w| w == "const" || w == "readonly");
            if constant {
                let mut cursor = node.walk();
                for decl in node.named_children(&mut cursor) {
                    collect_cs_declarators(decl, source, found);
                }
            }
            return;
        }
        (ConstLang::Go, "const_spec") | (ConstLang::Go, "var_spec") => {
            if let (Some(name), Some(value)) = (
                node.child_by_field_name("name"),
                node.child_by_field_name("value"),
            ) {
                record(found, &node_text(name, source), &node_text(value, source));
            }
        }
        (ConstLang::JavaScript, "variable_declarator") => {
            if let (Some(name), Some(value)) = (
                node.child_by_field_name("name"),
                node.child_by_field_name("value"),
            ) && name.kind() == "identifier"
            {
                record(found, &node_text(name, source), &node_text(value, source));
            }
        }
        (ConstLang::JavaScript, "public_field_definition" | "field_definition") => {
            let name = node
                .child_by_field_name("name")
                .or_else(|| node.child_by_field_name("property"));
            if let (Some(name), Some(value)) = (name, node.child_by_field_name("value")) {
                record(found, &node_text(name, source), &node_text(value, source));
            }
        }
        (ConstLang::Rust, "const_item" | "static_item") => {
            if let (Some(name), Some(value)) = (
                node.child_by_field_name("name"),
                node.child_by_field_name("value"),
            ) {
                record(found, &node_text(name, source), &node_text(value, source));
            }
        }
        _ => {}
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk(lang, child, source, found);
    }
}

fn collect_cs_declarators(
    node: Node<'_>,
    source: &str,
    found: &mut HashMap<String, Option<String>>,
) {
    if node.kind() == "variable_declarator" {
        let text = node_text(node, source);
        if let Some((name, value)) = text.split_once('=') {
            record(found, name, value);
        }
        return;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_cs_declarators(child, source, found);
    }
}
