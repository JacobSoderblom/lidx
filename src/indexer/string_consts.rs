//! Same-file string constants: "this name is really this string".
//!
//! The one shared resolver for any consumer that needs a static string from
//! an argument expression (channel topics today, env names for #225). It
//! knows nothing about channels: it answers "what string does this
//! expression statically denote?", and answers `None` rather than guessing.
//!
//! Scope is module- and class-level constants of one file. Every constant is
//! registered under its class-qualified path and each shorter suffix of it
//! (`Outer.Inner.NAME`, `Inner.NAME`, `NAME`); a key declared more than once
//! with different values is ambiguous and dropped, never guessed.
//! Function-local bindings are the caller's concern (see [`LocalTally`]).

use crate::indexer::tree_helpers::node_text;
use std::collections::HashMap;
use tree_sitter::Node;

/// Languages whose constant declarations [`collect_string_consts`] understands.
#[derive(Clone, Copy)]
pub enum ConstLang {
    /// Module/class-level `NAME = "x"` assignments.
    Python,
    /// `const`/`readonly` fields.
    CSharp,
    /// Package-level `const` declarations.
    Go,
    /// `const` declarations and class fields (JavaScript and TypeScript).
    JavaScript,
    /// `const`/`static` items, including those in `impl`/`mod` blocks.
    Rust,
}

/// A file's string constants, keyed by (suffixes of) their qualified path.
/// A `None` value marks an ambiguous key. Build with [`collect_string_consts`].
#[derive(Debug, Default, Clone)]
pub struct StringConsts {
    map: HashMap<String, Option<String>>,
}

/// What an enclosing function says about a bare identifier argument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalBinding {
    /// Not declared in the enclosing function: fall back to file constants.
    NotLocal,
    /// A parameter, reassigned, or otherwise not statically known.
    Unknown,
    /// Declared exactly once, with this initializer expression text.
    Value(String),
}

/// Tally of how a name is bound inside one function body. Language-specific
/// scanners fill it in; [`LocalTally::finish`] turns it into a verdict.
#[derive(Debug, Default)]
pub struct LocalTally {
    /// Declarations/assignments that introduce the name with an initializer.
    pub declarations: usize,
    /// Bindings with no static value: parameters, loop variables, patterns.
    pub other_bindings: usize,
    /// Later mutations of the name (`+=`, plain re-assignment in C#).
    pub reassignments: usize,
    /// Initializer text of the most recent declaration.
    pub initializer: Option<String>,
}

impl LocalTally {
    /// Fold the tally into a [`LocalBinding`]: only a single declaration
    /// with an initializer, and nothing else touching the name, is static.
    pub fn finish(self) -> LocalBinding {
        if self.declarations + self.other_bindings + self.reassignments == 0 {
            return LocalBinding::NotLocal;
        }
        match self.initializer {
            Some(init)
                if self.declarations == 1
                    && self.other_bindings == 0
                    && self.reassignments == 0 =>
            {
                LocalBinding::Value(init)
            }
            _ => LocalBinding::Unknown,
        }
    }
}

/// Scan the outermost function enclosing `call` (a node whose kind is in
/// `function_kinds`), calling `visit` on every node in it, and report what
/// the body says about a bare name. No enclosing function: `NotLocal`.
pub fn scan_enclosing_function<'a>(
    call: Node<'a>,
    function_kinds: &[&str],
    mut visit: impl FnMut(Node<'a>, &mut LocalTally),
) -> LocalBinding {
    let mut func = None;
    let mut cur = call.parent();
    while let Some(n) = cur {
        if function_kinds.contains(&n.kind()) {
            func = Some(n);
        }
        cur = n.parent();
    }
    let Some(func) = func else {
        return LocalBinding::NotLocal;
    };
    let mut tally = LocalTally::default();
    let mut stack = vec![func];
    while let Some(n) = stack.pop() {
        visit(n, &mut tally);
        let mut c = n.walk();
        stack.extend(n.named_children(&mut c));
    }
    tally.finish()
}

/// Value of a string-literal source text, for every quoting form the
/// supported languages allow: single/double quotes, Python triple quotes and
/// `r`/`u`/`b`/`f` prefixes, C# verbatim (`@"..."`), interpolated (`$"..."`)
/// and raw (`"""..."""`) strings, Go/JS backtick strings, Rust `r#"..."#`.
/// Interpolated/template strings with holes (`{`/`${`) are not static and
/// yield `None`, as does anything that is not a single complete literal.
pub fn string_literal_value(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let qpos = raw.find(['"', '\'', '`'])?;
    let prefix = &raw[..qpos];
    if !prefix.chars().all(|c| {
        matches!(
            c,
            'r' | 'R' | 'u' | 'U' | 'b' | 'B' | 'f' | 'F' | '$' | '@' | '#'
        )
    }) {
        return None;
    }
    let interpolated = prefix.contains(['f', 'F', '$']);
    let hashes = prefix.chars().filter(|c| *c == '#').count();
    let rest = &raw[qpos..];
    let q = rest.chars().next()?;
    let triple: String = std::iter::repeat_n(q, 3).collect();
    let delim = if q != '`' && rest.starts_with(&triple) && rest.len() >= 6 {
        triple
    } else {
        q.to_string()
    };
    let mut tail = rest.strip_suffix(&"#".repeat(hashes))?;
    if tail.len() < delim.len() * 2 {
        return None;
    }
    tail = tail.strip_suffix(delim.as_str())?;
    let body = &tail[delim.len()..];
    if body.is_empty() || body.contains(q) {
        return None;
    }
    if (interpolated || q == '`') && body.contains('{') {
        return None;
    }
    Some(body.to_string())
}

/// True for `a`, `a.b`, `a::b`: dotted/scoped identifier paths, nothing else.
pub fn is_identifier_path(s: &str) -> bool {
    let s = s.trim();
    !s.is_empty()
        && s.split(['.', ':']).all(|seg| {
            let mut chars = seg.chars();
            chars.next().is_some_and(|c| c.is_alphabetic() || c == '_')
                && chars.all(|c| c.is_alphanumeric() || c == '_')
        })
}

impl StringConsts {
    /// Value of the constant a dotted path names (`NAME`, `Class.NAME`,
    /// `Outer.Inner.NAME`; `self.`/`cls.`/`this.`/`Self::` receivers are
    /// dropped). `None` when unknown or ambiguous. Arbitrary receivers
    /// (`settings.TOPIC`) are never resolved by simple name.
    pub fn value_of_path(&self, path: &str) -> Option<&str> {
        let path = path.trim().replace("::", ".");
        let key = ["self.", "cls.", "this.", "Self."]
            .iter()
            .find_map(|p| path.strip_prefix(p))
            .unwrap_or(&path);
        self.map.get(key)?.as_deref()
    }

    /// The string an argument expression statically denotes: a string
    /// literal's content, a same-function local bound once to such a value,
    /// or a same-file constant. Anything else (call, parameter, interpolation
    /// with holes, unknown name, foreign receiver) is `None`.
    pub fn resolve_arg(&self, raw: &str, local: &LocalBinding) -> Option<String> {
        let raw = raw.trim();
        if let Some(v) = string_literal_value(raw) {
            return Some(v);
        }
        if !is_identifier_path(raw) {
            return None;
        }
        if !raw.contains(['.', ':']) {
            match local {
                LocalBinding::Unknown => return None,
                LocalBinding::Value(expr) => {
                    return string_literal_value(expr)
                        .or_else(|| self.value_of_path(expr).map(str::to_string));
                }
                LocalBinding::NotLocal => {}
            }
        }
        self.value_of_path(raw).map(str::to_string)
    }

    fn record(&mut self, scope: &[String], name: &str, value_text: &str) {
        let name = name.trim();
        if name.is_empty() {
            return;
        }
        let value = string_literal_value(value_text);
        let mut path: Vec<&str> = scope.iter().map(String::as_str).collect();
        path.push(name);
        for start in 0..path.len() {
            let key = path[start..].join(".");
            match self.map.get(&key) {
                Some(existing) if *existing != value => {
                    self.map.insert(key, None);
                }
                Some(_) => {}
                None => {
                    self.map.insert(key, value.clone());
                }
            }
        }
    }
}

/// Collect a file's module- and class-level string constants.
pub fn collect_string_consts(lang: ConstLang, root: Node<'_>, source: &str) -> StringConsts {
    let mut out = StringConsts::default();
    walk(lang, root, source, &mut Vec::new(), &mut out);
    out
}

fn function_kinds(lang: ConstLang) -> &'static [&'static str] {
    match lang {
        ConstLang::Python => &["function_definition", "lambda"],
        ConstLang::CSharp => &[
            "method_declaration",
            "constructor_declaration",
            "destructor_declaration",
            "operator_declaration",
            "conversion_operator_declaration",
            "local_function_statement",
            "accessor_declaration",
            "lambda_expression",
            "anonymous_method_expression",
        ],
        ConstLang::Go => &["function_declaration", "method_declaration", "func_literal"],
        ConstLang::JavaScript => &[
            "function_declaration",
            "function_expression",
            "function",
            "generator_function_declaration",
            "generator_function",
            "arrow_function",
            "method_definition",
        ],
        ConstLang::Rust => &["function_item", "closure_expression"],
    }
}

/// Name a container node contributes to the qualified path, if it is one.
fn container_name(lang: ConstLang, node: Node<'_>, source: &str) -> Option<String> {
    let kind = node.kind();
    let field = match (lang, kind) {
        (ConstLang::Python, "class_definition")
        | (
            ConstLang::CSharp,
            "class_declaration"
            | "struct_declaration"
            | "record_declaration"
            | "record_struct_declaration"
            | "interface_declaration",
        )
        | (ConstLang::JavaScript, "class_declaration" | "class")
        | (ConstLang::Rust, "mod_item" | "trait_item") => "name",
        (ConstLang::Rust, "impl_item") => "type",
        _ => return None,
    };
    node.child_by_field_name(field)
        .map(|n| node_text(n, source))
}

fn field_of<'a>(n: Node<'a>, f: &str) -> Option<Node<'a>> {
    n.child_by_field_name(f)
}

fn walk(
    lang: ConstLang,
    node: Node<'_>,
    source: &str,
    scope: &mut Vec<String>,
    out: &mut StringConsts,
) {
    let kind = node.kind();
    if function_kinds(lang).contains(&kind) {
        return;
    }
    match (lang, kind) {
        (ConstLang::Python, "assignment") => {
            if let (Some(left), Some(right)) = (field_of(node, "left"), field_of(node, "right"))
                && left.kind() == "identifier"
            {
                out.record(scope, &node_text(left, source), &node_text(right, source));
            }
        }
        (ConstLang::CSharp, "field_declaration") => {
            let mut cursor = node.walk();
            let constant = node.named_children(&mut cursor).any(|c| {
                c.kind() == "modifier"
                    && matches!(node_text(c, source).as_str(), "const" | "readonly")
            });
            if constant {
                let mut cursor = node.walk();
                for decl in node.named_children(&mut cursor) {
                    record_cs_declarators(decl, source, scope, out);
                }
            }
            return;
        }
        (ConstLang::Go, "const_spec") => {
            if let (Some(name), Some(value)) = (field_of(node, "name"), field_of(node, "value")) {
                out.record(scope, &node_text(name, source), &node_text(value, source));
            }
        }
        (ConstLang::JavaScript, "variable_declarator") => {
            let is_const = node
                .parent()
                .and_then(|p| p.child(0))
                .is_some_and(|k| node_text(k, source) == "const");
            if is_const
                && let (Some(name), Some(value)) = (field_of(node, "name"), field_of(node, "value"))
                && name.kind() == "identifier"
            {
                out.record(scope, &node_text(name, source), &node_text(value, source));
            }
        }
        (ConstLang::JavaScript, "public_field_definition" | "field_definition") => {
            let name = field_of(node, "name").or_else(|| field_of(node, "property"));
            if let (Some(name), Some(value)) = (name, field_of(node, "value")) {
                out.record(scope, &node_text(name, source), &node_text(value, source));
            }
        }
        (ConstLang::Rust, "const_item" | "static_item") => {
            if let (Some(name), Some(value)) = (field_of(node, "name"), field_of(node, "value")) {
                out.record(scope, &node_text(name, source), &node_text(value, source));
            }
        }
        _ => {}
    }
    let pushed = container_name(lang, node, source)
        .map(|n| scope.push(n))
        .is_some();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk(lang, child, source, scope, out);
    }
    if pushed {
        scope.pop();
    }
}

/// Initializer expression of a C# `variable_declarator` (its last named
/// child other than the `name` field), unwrapping `equals_value_clause`.
pub fn csharp_declarator_initializer<'a>(declarator: Node<'a>) -> Option<Node<'a>> {
    let name = declarator.child_by_field_name("name")?;
    let mut cursor = declarator.walk();
    let init = declarator
        .named_children(&mut cursor)
        .filter(|c| c.id() != name.id())
        .last()?;
    if init.kind() == "equals_value_clause" {
        return init.named_child(0);
    }
    Some(init)
}

fn record_cs_declarators(node: Node<'_>, source: &str, scope: &[String], out: &mut StringConsts) {
    if node.kind() == "variable_declarator" {
        if let (Some(name), Some(init)) = (
            node.child_by_field_name("name"),
            csharp_declarator_initializer(node),
        ) {
            out.record(scope, &node_text(name, source), &node_text(init, source));
        }
        return;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        record_cs_declarators(child, source, scope, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_literal_forms() {
        for (raw, want) in [
            ("\"a\"", Some("a")),
            ("'a'", Some("a")),
            ("\"\"\"a\"\"\"", Some("a")),
            ("'''a'''", Some("a")),
            ("@\"a\"", Some("a")),
            ("$\"a\"", Some("a")),
            ("$\"a{x}\"", None),
            ("f\"a{x}\"", None),
            ("`a`", Some("a")),
            ("`a${x}`", None),
            ("r#\"a\"#", Some("a")),
            ("topic", None),
            ("f()", None),
            ("\"a\" \"b\"", None),
        ] {
            assert_eq!(string_literal_value(raw).as_deref(), want, "{raw}");
        }
    }
}
