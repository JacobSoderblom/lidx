//! Go receiver typing, resolver side: finishing a [`GoDeferred`] receiver
//! (`s := NewServer(); s.Handle()`, `c.conn.Close()`, `a.B().C()`) from the
//! declarations it names, and finding methods a struct gets from its
//! embedded types.
//!
//! The extractor writes every type it can name in one file as a package
//! candidate; what it can only name through another file's declaration is a
//! [`GoValue`] expression. Here each is evaluated over the symbol index:
//! a function's or method's declared result type is read from its stored
//! signature (`(params) -> result`), a field's from the struct's stored
//! field list (`{name Type; ~Embedded}`), each in the *declaring* file's
//! imports. A type that cannot be told stays unknown and binds nothing --
//! the name of the method is never evidence.

use super::*;
use crate::indexer::extract::{GoDeferred, GoValue};
use crate::indexer::go_build::{BUILD_HOST_MODIFIER, BUILD_TAG_MODIFIER};
use crate::model::go_type_data;

/// How deep embedded types are searched for a promoted member.
const MAX_EMBEDDING_DEPTH: usize = 4;

/// A Go type an expression evaluates to.
#[derive(Debug, Clone, PartialEq, Eq)]
enum GoType {
    /// A named type declared in a package directory of this repository.
    Repo { dir: String, name: String },
    /// A named type of a package outside it (an import path).
    Foreign { path: String, name: String },
    /// A slice, array or map of these.
    Seq(Box<GoType>),
}

/// A symbol a Go lookup matched.
struct GoSymbol {
    id: i64,
    qualname: String,
    kind: String,
    signature: Option<String>,
    file_id: i64,
    /// `symbols.visibility`: carries the file's build constraint, if any.
    visibility: Option<String>,
}

/// A file's build constraint as its symbols record it (`go_build`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BuildTag {
    key: String,
    host: bool,
}

impl BuildTag {
    /// The constraint a `symbols.visibility` value records, if any.
    pub(super) fn parse(visibility: Option<&str>) -> Option<BuildTag> {
        let visibility = visibility?;
        let key = visibility
            .split_whitespace()
            .find_map(|word| word.strip_prefix(BUILD_TAG_MODIFIER))?;
        Some(BuildTag {
            key: key.to_string(),
            host: visibility
                .split_whitespace()
                .any(|w| w == BUILD_HOST_MODIFIER),
        })
    }
}

/// Which of same-package twins -- one declaration written in several files
/// whose build constraints exclude each other (`stdlib_regex.go` is
/// `!gore2regex`, `wasilibs_regex.go` is `gore2regex`) -- a call means.
/// `tags` is each twin's file constraint (`None`: unconstrained), `caller`
/// the calling file's. In order:
/// 1. a caller in a constrained file takes the twin with its own constraint
///    (a call inside `x_windows.go` means the windows variant);
/// 2. else the twin that holds on the default host (linux/amd64, cgo off, no
///    extra tags -- see `indexer::go_build`) or has no constraint;
/// 3. if that is still several, the unconstrained one.
///
/// `None` when no twin is told apart: the name stays ambiguous.
pub(super) fn pick_build_twin(
    tags: &[Option<BuildTag>],
    caller: Option<&BuildTag>,
) -> Option<usize> {
    if let Some(caller) = caller {
        let mut own =
            (0..tags.len()).filter(|&i| tags[i].as_ref().is_some_and(|t| t.key == caller.key));
        if let (Some(only), None) = (own.next(), own.next()) {
            return Some(only);
        }
    }
    let on_host: Vec<usize> = (0..tags.len())
        .filter(|&i| tags[i].as_ref().is_none_or(|t| t.host))
        .collect();
    if let [only] = on_host[..] {
        return Some(only);
    }
    let mut plain = on_host.iter().copied().filter(|&i| tags[i].is_none());
    match (plain.next(), plain.next()) {
        (Some(only), None) => Some(only),
        _ => None,
    }
}

/// `items` with every set of build-constraint twins -- same `kind` and
/// member key, different file, at least one file constrained -- reduced to
/// the twin `pick_build_twin` chooses. A set no twin is picked from stays as
/// it is (ambiguous when named). `view` gives `(kind, member key,
/// visibility)`.
pub(super) fn collapse_build_twins<T>(
    items: Vec<T>,
    view: impl Fn(&T) -> (String, String, Option<String>),
    caller: Option<&BuildTag>,
) -> Vec<T> {
    if items.len() < 2 {
        return items;
    }
    let mut dropped = HashSet::new();
    {
        let mut groups: HashMap<(&str, &str), Vec<usize>> = HashMap::new();
        let views: Vec<_> = items.iter().map(&view).collect();
        for (i, (kind, member, _)) in views.iter().enumerate() {
            groups.entry((kind, member)).or_default().push(i);
        }
        for group in groups.values().filter(|g| g.len() > 1) {
            let tags: Vec<Option<BuildTag>> = group
                .iter()
                .map(|&i| BuildTag::parse(views[i].2.as_deref()))
                .collect();
            if tags.iter().all(Option::is_none) {
                continue;
            }
            if let Some(kept) = pick_build_twin(&tags, caller) {
                dropped.extend(
                    group
                        .iter()
                        .enumerate()
                        .filter(|&(n, _)| n != kept)
                        .map(|(_, &i)| i),
                );
            }
        }
    }
    items
        .into_iter()
        .enumerate()
        .filter_map(|(i, item)| (!dropped.contains(&i)).then_some(item))
        .collect()
}

impl GoSymbol {
    /// The member the symbol declares in its package: `Name`, or
    /// `Type.Method` for a method (the qualname minus its module, whose file
    /// stem may itself hold dots).
    fn name_in_package(&self) -> &str {
        let mut dots = self.qualname.rmatch_indices('.').map(|(i, _)| i);
        let last = dots.next();
        let cut = if self.kind == "method" {
            dots.next()
        } else {
            last
        };
        cut.map_or(self.qualname.as_str(), |i| &self.qualname[i + 1..])
    }

    /// The package directory the symbol is declared in.
    fn dir(&self) -> &str {
        self.qualname.rsplit_once('/').map_or("", |(dir, _)| dir)
    }
}

/// `LanguageProfile::deferred_candidate` for Go.
pub(crate) fn go_deferred_candidate(
    resolver: &mut Resolver<'_>,
    marker: &DeferredMarker,
) -> Result<Option<Option<(String, String)>>> {
    match marker {
        DeferredMarker::Go(call) => resolver.go_deferred_candidate(call).map(Some),
        _ => Ok(None),
    }
}

impl Resolver<'_> {
    /// The package candidate and call target text for the method call `d`,
    /// or `None` when its receiver's type cannot be told.
    pub(super) fn go_deferred_candidate(
        &mut self,
        d: &GoDeferred,
    ) -> Result<Option<(String, String)>> {
        Ok(match self.go_eval(&d.recv, 0)? {
            Some(GoType::Repo { dir, name }) => Some((
                format!("{GO_IMPORTED_PACKAGE}{dir}|{name}.{}", d.method),
                format!("{name}.{}", d.method),
            )),
            Some(GoType::Foreign { path, name }) => Some((
                format!("{path}|{name}.{}", d.method),
                format!("{path}.{name}.{}", d.method),
            )),
            Some(GoType::Seq(_)) | None => None,
        })
    }

    fn go_eval(&mut self, value: &GoValue, depth: usize) -> Result<Option<GoType>> {
        if depth > crate::indexer::extract::MAX_DEFERRED_DEPTH {
            return Ok(None);
        }
        match value {
            GoValue::Type { place, name } => Ok(Some(go_type_at(place, name))),
            // Anonymous structs are read in the extractor, never stored.
            GoValue::Struct(_) => Ok(None),
            GoValue::Seq(inner) => Ok(self
                .go_eval(inner, depth + 1)?
                .map(|t| GoType::Seq(Box::new(t)))),
            GoValue::Elem(of) => Ok(match self.go_eval(of, depth + 1)? {
                Some(GoType::Seq(element)) => Some(*element),
                _ => None,
            }),
            GoValue::Var { place, name } => {
                let Some(dir) = repo_dir(place) else {
                    return Ok(None);
                };
                let mut found = self.go_package_symbols(dir, name)?;
                found.retain(|s| s.kind == "variable");
                let [decl] = &found[..] else {
                    return Ok(None);
                };
                let Some(signature) = decl.signature.as_deref().and_then(go_type_data) else {
                    return Ok(None);
                };
                let imports = self.go_file_imports(decl.file_id)?;
                match signature.strip_prefix("= ") {
                    // `var x = NewT()`: the initializer's value, written by
                    // the extractor with the file's own import paths.
                    Some(json) => {
                        let Ok(mut value) = serde_json::from_str::<GoValue>(json) else {
                            return Ok(None);
                        };
                        rebind_places(&mut value, &imports);
                        self.go_eval(&value, depth + 1)
                    }
                    None => Ok(canonical_type(signature, decl.dir(), &imports)),
                }
            }
            GoValue::Func { place, name, index } => {
                let Some(dir) = repo_dir(place) else {
                    return Ok(None);
                };
                let found = self.go_package_symbols(dir, name)?;
                match &found[..] {
                    [only] if only.kind == "function" => {
                        self.go_result_type(only, usize::from(*index))
                    }
                    // `T(x)` converts to the repo type T.
                    [only] if matches!(only.kind.as_str(), "class" | "type") && *index == 0 => {
                        Ok(Some(GoType::Repo {
                            dir: dir.to_string(),
                            name: name.clone(),
                        }))
                    }
                    _ => Ok(None),
                }
            }
            GoValue::Method { recv, name, index } => {
                let Some(GoType::Repo { dir, name: ty }) = self.go_eval(recv, depth + 1)? else {
                    return Ok(None);
                };
                match self.go_member(&dir, &ty, name, 0)? {
                    Some(method) if method.kind == "method" => {
                        self.go_result_type(&method, usize::from(*index))
                    }
                    _ => Ok(None),
                }
            }
            GoValue::Field { owner, name } => {
                let Some(GoType::Repo { dir, name: ty }) = self.go_eval(owner, depth + 1)? else {
                    return Ok(None);
                };
                self.go_field_type(&dir, &ty, name, 0)
            }
        }
    }

    /// Declarations named `name` in package directory `dir`: functions,
    /// types and variables (never methods).
    fn go_package_symbols(&mut self, dir: &str, name: &str) -> Result<Vec<GoSymbol>> {
        let mut stmt = self.conn.prepare_cached(&format!(
            "SELECT s.id, s.qualname, s.kind, s.signature, s.file_id, s.visibility
             FROM symbols s JOIN files f ON s.file_id = f.id
             WHERE s.name = ?1 AND f.language = 'go' AND s.kind != 'method' AND {}",
            live_symbols(2)
        ))?;
        let rows = stmt
            .query_map(params![name, self.graph_version], go_symbol_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let caller = self.go_caller_tag()?;
        Ok(collapse_build_twins(
            rows.into_iter().filter(|s| s.dir() == dir).collect(),
            |s: &GoSymbol| {
                (
                    s.kind.clone(),
                    s.name_in_package().to_string(),
                    s.visibility.clone(),
                )
            },
            caller.as_ref(),
        ))
    }

    /// The build constraint of the file making the reference being
    /// resolved.
    pub(super) fn go_caller_tag(&mut self) -> Result<Option<BuildTag>> {
        if let Some(known) = self.caller_build_tags.get(&self.caller_file) {
            return Ok(known.clone());
        }
        let visibility: Option<String> = self
            .conn
            .prepare_cached(
                "SELECT s.visibility FROM symbols s JOIN files f ON f.id = s.file_id
                 WHERE f.path = ?1 AND s.graph_version = ?2 AND s.visibility LIKE '%buildtag:%'
                 LIMIT 1",
            )?
            .query_row(params![self.caller_file, self.graph_version], |row| {
                row.get(0)
            })
            .optional()?;
        let tag = BuildTag::parse(visibility.as_deref());
        self.caller_build_tags
            .insert(self.caller_file.clone(), tag.clone());
        Ok(tag)
    }

    /// The method `name` of type `ty` of package `dir`, declared on it or
    /// promoted from the types it embeds. `None` when there is none or two
    /// candidates at the shallowest depth that has any.
    fn go_member(
        &mut self,
        dir: &str,
        ty: &str,
        name: &str,
        depth: usize,
    ) -> Result<Option<GoSymbol>> {
        let own = {
            let suffix = format!(".{ty}.{name}");
            let mut stmt = self.conn.prepare_cached(&format!(
                "SELECT s.id, s.qualname, s.kind, s.signature, s.file_id, s.visibility
                 FROM symbols s JOIN files f ON s.file_id = f.id
                 WHERE s.name = ?1 AND f.language = 'go' AND s.kind = 'method' AND {}",
                live_symbols(2)
            ))?;
            let rows = stmt
                .query_map(params![name, self.graph_version], go_symbol_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let caller = self.go_caller_tag()?;
            collapse_build_twins(
                rows.into_iter()
                    .filter(|s| s.qualname.ends_with(&suffix) && s.dir() == dir)
                    .collect::<Vec<_>>(),
                |s: &GoSymbol| {
                    (
                        s.kind.clone(),
                        s.name_in_package().to_string(),
                        s.visibility.clone(),
                    )
                },
                caller.as_ref(),
            )
        };
        if own.len() > 1 {
            self.saw_ambiguous = true;
            return Ok(None);
        }
        if let Some(only) = own.into_iter().next() {
            return Ok(Some(only));
        }
        if depth >= MAX_EMBEDDING_DEPTH {
            return Ok(None);
        }
        let mut found: Option<GoSymbol> = None;
        for embedded in self.go_embedded(dir, ty)? {
            let GoType::Repo {
                dir: edir,
                name: ename,
            } = embedded
            else {
                continue;
            };
            if let Some(hit) = self.go_member(&edir, &ename, name, depth + 1)? {
                match &found {
                    Some(existing) if existing.id != hit.id => {
                        self.saw_ambiguous = true;
                        return Ok(None);
                    }
                    _ => found = Some(hit),
                }
            }
        }
        Ok(found)
    }

    /// The id of method `name` of `ty` of package `dir` -- declared on it or
    /// promoted from an embedded type.
    pub(super) fn go_method_id(&mut self, dir: &str, ty: &str, name: &str) -> Result<Option<i64>> {
        Ok(self.go_member(dir, ty, name, 0)?.map(|m| m.id))
    }

    /// The struct `ty` of package `dir`: its stored field list.
    fn go_struct(&mut self, dir: &str, ty: &str) -> Result<Option<GoSymbol>> {
        let mut found = self.go_package_symbols(dir, ty)?;
        found.retain(|s| matches!(s.kind.as_str(), "class" | "interface"));
        if found.len() > 1 {
            self.saw_ambiguous = true;
            return Ok(None);
        }
        Ok(found.pop())
    }

    /// The types struct `ty` of `dir` embeds.
    fn go_embedded(&mut self, dir: &str, ty: &str) -> Result<Vec<GoType>> {
        let Some(decl) = self.go_struct(dir, ty)? else {
            return Ok(Vec::new());
        };
        let imports = self.go_file_imports(decl.file_id)?;
        Ok(struct_fields(decl.signature.as_deref())
            .into_iter()
            .filter(|(name, _)| name.is_none())
            .filter_map(|(_, text)| canonical_type(&text, decl.dir(), &imports))
            .collect())
    }

    /// The declared type of field `name` of struct `ty` of `dir`: its own,
    /// the embedded type of that name, or one promoted from an embedded type.
    fn go_field_type(
        &mut self,
        dir: &str,
        ty: &str,
        name: &str,
        depth: usize,
    ) -> Result<Option<GoType>> {
        let Some(decl) = self.go_struct(dir, ty)? else {
            return Ok(None);
        };
        let imports = self.go_file_imports(decl.file_id)?;
        let fields = struct_fields(decl.signature.as_deref());
        if let Some((_, text)) = fields
            .iter()
            .find(|(field, _)| field.as_deref() == Some(name))
        {
            return Ok(canonical_type(text, decl.dir(), &imports));
        }
        // An embedded field is named after its type.
        let embedded: Vec<GoType> = fields
            .iter()
            .filter(|(field, _)| field.is_none())
            .filter_map(|(_, text)| canonical_type(text, decl.dir(), &imports))
            .collect();
        if let Some(own) = embedded.iter().find(|t| match t {
            GoType::Repo { name: n, .. } | GoType::Foreign { name: n, .. } => n == name,
            GoType::Seq(_) => false,
        }) {
            return Ok(Some(own.clone()));
        }
        if depth >= MAX_EMBEDDING_DEPTH {
            return Ok(None);
        }
        let mut found: Option<GoType> = None;
        for embedded in embedded {
            let GoType::Repo {
                dir: edir,
                name: ename,
            } = embedded
            else {
                continue;
            };
            if let Some(hit) = self.go_field_type(&edir, &ename, name, depth + 1)? {
                if found.as_ref().is_some_and(|f| *f != hit) {
                    return Ok(None);
                }
                found = Some(hit);
            }
        }
        Ok(found)
    }

    /// The type of result number `index` of function or method `decl`, in
    /// its declaring file's terms.
    fn go_result_type(&mut self, decl: &GoSymbol, index: usize) -> Result<Option<GoType>> {
        let results = signature_results(decl.signature.as_deref());
        let Some(text) = results.get(index) else {
            return Ok(None);
        };
        let imports = self.go_file_imports(decl.file_id)?;
        Ok(canonical_type(text, decl.dir(), &imports))
    }

    /// Import alias -> package (`><dir>` or an import path) of a file, from
    /// the `IMPORTS` references its extractor wrote.
    fn go_file_imports(&mut self, file_id: i64) -> Result<GoImports> {
        // An import binds no symbol, so it is an `unresolved_references` row
        // (or, if some symbol happens to carry its path, an edge).
        let mut stmt = self.conn.prepare_cached(
            "SELECT detail FROM edges
             WHERE file_id = ?1 AND kind = 'IMPORTS' AND graph_version = ?2
             UNION ALL
             SELECT detail FROM unresolved_references
             WHERE file_id = ?1 AND edge_kind = 'IMPORTS' AND graph_version = ?2",
        )?;
        let rows = stmt
            .query_map(params![file_id, self.graph_version], |row| {
                row.get::<_, Option<String>>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut imports = GoImports::default();
        for detail in rows.into_iter().flatten() {
            let Ok(json) = serde_json::from_str::<serde_json::Value>(&detail) else {
                continue;
            };
            let Some(place) = json["place"].as_str() else {
                continue;
            };
            if let Some(alias) = json["bound_name"].as_str() {
                imports
                    .by_alias
                    .insert(alias.to_string(), place.to_string());
            }
            if let Some(path) = json["import_path"].as_str() {
                imports.by_path.insert(path.to_string(), place.to_string());
            }
        }
        Ok(imports)
    }
}

/// The packages a file imports, by the name it uses and by import path.
#[derive(Default)]
struct GoImports {
    by_alias: HashMap<String, String>,
    by_path: HashMap<String, String>,
}

/// Replace the raw import paths in `value` (written without the repository's
/// module map) by the packages the declaring file's imports stand for.
fn rebind_places(value: &mut GoValue, imports: &GoImports) {
    match value {
        GoValue::Type { place, .. } | GoValue::Func { place, .. } | GoValue::Var { place, .. } => {
            if !place.starts_with([GO_SAME_PACKAGE, GO_IMPORTED_PACKAGE])
                && let Some(bound) = imports.by_path.get(place.as_str())
            {
                *place = bound.clone();
            }
        }
        GoValue::Method { recv, .. } => rebind_places(recv, imports),
        GoValue::Field { owner, .. } => rebind_places(owner, imports),
        GoValue::Seq(inner) | GoValue::Elem(inner) => rebind_places(inner, imports),
        GoValue::Struct(fields) => {
            for (_, field) in fields {
                rebind_places(field, imports);
            }
        }
    }
}

fn go_symbol_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<GoSymbol> {
    Ok(GoSymbol {
        id: row.get(0)?,
        qualname: row.get(1)?,
        kind: row.get(2)?,
        signature: row.get(3)?,
        file_id: row.get(4)?,
        visibility: row.get(5)?,
    })
}

/// The directory of an in-repo package `place` (`=dir` / `>dir`).
fn repo_dir(place: &str) -> Option<&str> {
    place.strip_prefix([GO_SAME_PACKAGE, GO_IMPORTED_PACKAGE])
}

fn go_type_at(place: &str, name: &str) -> GoType {
    match repo_dir(place) {
        Some(dir) => GoType::Repo {
            dir: dir.to_string(),
            name: name.to_string(),
        },
        None => GoType::Foreign {
            path: place.to_string(),
            name: name.to_string(),
        },
    }
}

/// The type a declared type text names, read in a file of package `dir`
/// with `imports`: `*Server`, `Set[T]`, `net.Conn`. `None` for a slice,
/// map, channel, function, anonymous or predeclared type.
fn canonical_type(text: &str, dir: &str, imports: &GoImports) -> Option<GoType> {
    let text = text.trim().trim_start_matches('*').trim();
    // A slice, array or map is a sequence of its element type.
    let element = if let Some(rest) = text.strip_prefix("[]").or_else(|| text.strip_prefix("...")) {
        Some(rest)
    } else if let Some(rest) = text.strip_prefix('[') {
        // `[N]T`: a length (digits or a name) then the element.
        rest.split_once(']').map(|(_, element)| element)
    } else if let Some(rest) = text.strip_prefix("map[") {
        // `map[K]V`: skip the key, which may itself hold brackets.
        let mut depth = 1;
        rest.char_indices()
            .find(|&(_, c)| {
                match c {
                    '[' => depth += 1,
                    ']' => depth -= 1,
                    _ => {}
                }
                depth == 0
            })
            .map(|(i, _)| &rest[i + 1..])
    } else {
        None
    };
    if let Some(element) = element {
        return canonical_type(element, dir, imports).map(|t| GoType::Seq(Box::new(t)));
    }
    if text.is_empty()
        || text.starts_with(['[', '(', '<', '.', '?'])
        || ["map[", "chan ", "func(", "func ", "interface", "struct"]
            .iter()
            .any(|p| text.starts_with(p))
    {
        return None;
    }
    // Type arguments are not part of the name.
    let text = text.split('[').next().unwrap_or(text).trim();
    match text.split_once('.') {
        Some((qualifier, name)) => {
            if !is_identifier(qualifier) || !is_identifier(name) {
                return None;
            }
            Some(go_type_at(imports.by_alias.get(qualifier)?, name))
        }
        None => {
            const PREDECLARED: &[&str] = &[
                "any",
                "bool",
                "byte",
                "comparable",
                "complex64",
                "complex128",
                "error",
                "float32",
                "float64",
                "int",
                "int8",
                "int16",
                "int32",
                "int64",
                "rune",
                "string",
                "uint",
                "uint8",
                "uint16",
                "uint32",
                "uint64",
                "uintptr",
            ];
            (is_identifier(text) && !PREDECLARED.contains(&text)).then(|| GoType::Repo {
                dir: dir.to_string(),
                name: text.to_string(),
            })
        }
    }
}

fn is_identifier(text: &str) -> bool {
    !text.is_empty() && text.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// The result types of a stored function signature `(params) -> results`.
fn signature_results(signature: Option<&str>) -> Vec<String> {
    let Some((_, results)) = signature.and_then(|s| s.rsplit_once(" -> ")) else {
        return Vec::new();
    };
    let results = results.trim();
    let Some(inner) = results.strip_prefix('(').and_then(|r| r.strip_suffix(')')) else {
        return vec![results.to_string()];
    };
    let parts = split_top_level(inner, ',');
    // Named results (`(s *Server, err error)`, `(a, b int)`): every part
    // names something, and a bare name takes the next type.
    let named = parts
        .iter()
        .any(|p| split_top_level(p.trim(), ' ').len() > 1);
    if !named {
        return parts.into_iter().map(|p| p.trim().to_string()).collect();
    }
    let mut types = Vec::with_capacity(parts.len());
    let mut carry = String::new();
    for part in parts.iter().rev() {
        let part = part.trim();
        if let Some((_, ty)) = part.split_once(' ') {
            carry = ty.trim().to_string();
        }
        types.push(carry.clone());
    }
    types.reverse();
    types
}

/// Split `text` at `sep` outside any bracket pair.
fn split_top_level(text: &str, sep: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut current = String::new();
    for c in text.chars() {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            _ => {}
        }
        if c == sep && depth == 0 {
            parts.push(std::mem::take(&mut current));
        } else {
            current.push(c);
        }
    }
    parts.push(current);
    parts.into_iter().filter(|p| !p.trim().is_empty()).collect()
}

/// `(field name, type text)` of each field in a stored struct signature
/// `{name Type; ~Embedded}`; an embedded field has no name.
fn struct_fields(signature: Option<&str>) -> Vec<(Option<String>, String)> {
    let Some(inner) = signature
        .and_then(go_type_data)
        .and_then(|s| s.strip_prefix('{'))
        .and_then(|s| s.strip_suffix('}'))
    else {
        return Vec::new();
    };
    inner
        .split("; ")
        .filter_map(|field| {
            if let Some(ty) = field.strip_prefix('~') {
                return Some((None, ty.to_string()));
            }
            let (name, ty) = field.split_once(' ')?;
            Some((Some(name.to_string()), ty.to_string()))
        })
        .collect()
}

/// `edges.derived` value of the `IMPLEMENTS` edges `Db::rederive_go_implements`
/// writes (1 is the deferred-RPC `RPC_CALL` edges).
pub(super) const GO_STRUCTURAL_DERIVED: i64 = 2;

/// Parameter and result counts of a method.
type Arity = (usize, usize);

/// The methods of a Go type, promoted ones included.
#[derive(Default, Clone)]
struct MethodSet {
    methods: HashMap<String, Arity>,
    /// False when an embedded type's methods could not be told.
    complete: bool,
}

/// A Go type declaration, as the structural-implements pass sees it.
struct GoTypeRow {
    id: i64,
    qualname: String,
    name: String,
    kind: String,
    signature: Option<String>,
    file_id: i64,
    visibility: Option<String>,
}

impl GoTypeRow {
    fn dir(&self) -> &str {
        self.qualname.rsplit_once('/').map_or("", |(dir, _)| dir)
    }
}

/// `(params, results)` counts of a stored signature `(params) -> results`.
fn signature_arity(signature: Option<&str>) -> Arity {
    let Some(sig) = signature else {
        return (0, 0);
    };
    let (params, results) = match sig.rsplit_once(" -> ") {
        Some((p, r)) => (p, Some(r.trim())),
        None => (sig, None),
    };
    let count = |list: &str| {
        let inner = list
            .trim()
            .strip_prefix('(')
            .and_then(|l| l.strip_suffix(')'));
        inner.map_or(1, |inner| split_top_level(inner, ',').len())
    };
    (count(params), results.map_or(0, count))
}

/// The methods of the standard-library interfaces repositories embed most;
/// any other foreign embedded interface leaves the embedding one incomplete.
fn std_interface(path: &str, name: &str) -> Option<&'static [(&'static str, usize, usize)]> {
    Some(match (path, name) {
        ("io", "Reader") => &[("Read", 1, 2)],
        ("io", "Writer") => &[("Write", 1, 2)],
        ("io", "Closer") => &[("Close", 0, 1)],
        ("io", "Seeker") => &[("Seek", 2, 2)],
        ("io", "ReaderAt") => &[("ReadAt", 2, 2)],
        ("io", "WriterAt") => &[("WriteAt", 2, 2)],
        ("io", "ReaderFrom") => &[("ReadFrom", 1, 2)],
        ("io", "WriterTo") => &[("WriteTo", 1, 2)],
        ("io", "ByteReader") => &[("ReadByte", 0, 2)],
        ("io", "ByteWriter") => &[("WriteByte", 1, 1)],
        ("io", "RuneReader") => &[("ReadRune", 0, 3)],
        ("io", "StringWriter") => &[("WriteString", 1, 2)],
        ("io", "ReadWriter") => &[("Read", 1, 2), ("Write", 1, 2)],
        ("io", "ReadCloser") => &[("Read", 1, 2), ("Close", 0, 1)],
        ("io", "WriteCloser") => &[("Write", 1, 2), ("Close", 0, 1)],
        ("io", "ReadWriteCloser") => &[("Read", 1, 2), ("Write", 1, 2), ("Close", 0, 1)],
        ("fmt", "Stringer") => &[("String", 0, 1)],
        ("sort", "Interface") => &[("Len", 0, 1), ("Less", 2, 1), ("Swap", 2, 0)],
        ("net/http", "Handler") => &[("ServeHTTP", 2, 0)],
        ("context", "Context") => &[
            ("Deadline", 0, 2),
            ("Done", 0, 1),
            ("Err", 0, 1),
            ("Value", 1, 1),
        ],
        _ => return None,
    })
}

impl Resolver<'_> {
    /// Every `(type, interface)` pair where the type's method set (own and
    /// promoted from embedded types) holds each method of the interface's
    /// (own and embedded interfaces') by name and arity -- Go's implicit
    /// `implements`. An interface with an embedded interface whose methods
    /// are not known is skipped, so no pair rests on a guess.
    fn go_structural_implements(&mut self) -> Result<Vec<(i64, i64, i64)>> {
        let rows: Vec<GoTypeRow> = {
            let mut stmt = self.conn.prepare_cached(&format!(
                "SELECT s.id, s.qualname, s.name, s.kind, s.signature, s.file_id, s.visibility
                 FROM symbols s JOIN files f ON s.file_id = f.id
                 WHERE f.language = 'go' AND s.kind IN ('class', 'interface', 'type', 'method')
                   AND {}",
                live_symbols(1)
            ))?;
            stmt.query_map(params![self.graph_version], |row| {
                Ok(GoTypeRow {
                    id: row.get(0)?,
                    qualname: row.get(1)?,
                    name: row.get(2)?,
                    kind: row.get(3)?,
                    signature: row.get(4)?,
                    file_id: row.get(5)?,
                    visibility: row.get(6)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut own_methods: HashMap<String, Vec<(String, Arity)>> = HashMap::new();
        let mut types: Vec<GoTypeRow> = Vec::new();
        for row in rows {
            if row.kind == "method" {
                if let Some((container, _)) = row.qualname.rsplit_once('.') {
                    own_methods
                        .entry(container.to_string())
                        .or_default()
                        .push((row.name.clone(), signature_arity(row.signature.as_deref())));
                }
            } else {
                types.push(row);
            }
        }
        let mut by_name: HashMap<(String, String), Vec<usize>> = HashMap::new();
        for (i, t) in types.iter().enumerate() {
            by_name
                .entry((t.dir().to_string(), t.name.clone()))
                .or_default()
                .push(i);
        }
        let mut memo: HashMap<usize, MethodSet> = HashMap::new();
        let mut sets: Vec<MethodSet> = Vec::with_capacity(types.len());
        for i in 0..types.len() {
            sets.push(self.go_method_set(i, &types, &own_methods, &by_name, &mut memo, 0)?);
        }
        // Implementers by method name, to start each interface from its
        // rarest method.
        let mut by_method: HashMap<&str, Vec<usize>> = HashMap::new();
        for (i, t) in types.iter().enumerate() {
            if t.kind != "interface" {
                for name in sets[i].methods.keys() {
                    by_method.entry(name).or_default().push(i);
                }
            }
        }
        let mut pairs = Vec::new();
        for (ii, iface) in types.iter().enumerate() {
            let want = &sets[ii];
            if iface.kind != "interface" || !want.complete || want.methods.is_empty() {
                continue;
            }
            let rarest = want
                .methods
                .keys()
                .min_by_key(|m| by_method.get(m.as_str()).map_or(0, Vec::len));
            let Some(candidates) = rarest.and_then(|m| by_method.get(m.as_str())) else {
                continue;
            };
            for &ci in candidates {
                let has = &sets[ci].methods;
                if want.methods.iter().all(|(m, a)| has.get(m) == Some(a)) {
                    pairs.push((types[ci].id, iface.id, types[ci].file_id));
                }
            }
        }
        Ok(pairs)
    }

    fn go_method_set(
        &mut self,
        idx: usize,
        types: &[GoTypeRow],
        own: &HashMap<String, Vec<(String, Arity)>>,
        by_name: &HashMap<(String, String), Vec<usize>>,
        memo: &mut HashMap<usize, MethodSet>,
        depth: usize,
    ) -> Result<MethodSet> {
        if let Some(done) = memo.get(&idx) {
            return Ok(done.clone());
        }
        let t = &types[idx];
        let mut set = MethodSet {
            methods: HashMap::new(),
            complete: true,
        };
        for (name, arity) in own.get(&t.qualname).into_iter().flatten() {
            set.methods.insert(name.clone(), *arity);
        }
        let embedded: Vec<String> = struct_fields(t.signature.as_deref())
            .into_iter()
            .filter(|(name, _)| name.is_none())
            .map(|(_, text)| text)
            .collect();
        if !embedded.is_empty() {
            if depth >= MAX_EMBEDDING_DEPTH {
                set.complete = false;
            } else {
                let imports = self.go_file_imports(t.file_id)?;
                let dir = t.dir().to_string();
                for text in embedded {
                    if text == "error" {
                        set.methods.entry("Error".into()).or_insert((0, 1));
                        continue;
                    }
                    match canonical_type(&text, &dir, &imports) {
                        Some(GoType::Repo { dir, name }) => {
                            let found: Vec<usize> =
                                by_name.get(&(dir, name)).cloned().unwrap_or_default();
                            let found = collapse_build_twins(
                                found,
                                |&i: &usize| {
                                    (
                                        types[i].kind.clone(),
                                        types[i].name.clone(),
                                        types[i].visibility.clone(),
                                    )
                                },
                                None,
                            );
                            let [only] = found[..] else {
                                set.complete = false;
                                continue;
                            };
                            if only == idx {
                                set.complete = false;
                                continue;
                            }
                            let inner =
                                self.go_method_set(only, types, own, by_name, memo, depth + 1)?;
                            set.complete &= inner.complete;
                            for (m, a) in inner.methods {
                                set.methods.entry(m).or_insert(a);
                            }
                        }
                        Some(GoType::Foreign { path, name }) => match std_interface(&path, &name) {
                            Some(methods) => {
                                for (m, p, r) in methods {
                                    set.methods.entry((*m).to_string()).or_insert((*p, *r));
                                }
                            }
                            None => set.complete = false,
                        },
                        _ => set.complete = false,
                    }
                }
            }
        }
        memo.insert(idx, set.clone());
        Ok(set)
    }
}

impl Db {
    /// Rebuild the `IMPLEMENTS` edges of Go's structural typing (a type has
    /// the methods of an interface, so it implements it): the same edges C#
    /// writes for `class C : IA`, so a call bound to `Iface.Method` reaches
    /// every implementing method through interface dispatch
    /// (`edges_for_symbols_with_dispatch`). They depend only on the current
    /// symbols, and only the pairs that changed are written, so incremental
    /// sync equals a fresh reindex and an unchanged repo gets no new edge ids.
    pub fn rederive_go_implements(&self, graph_version: i64) -> Result<usize> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let wanted: Vec<(i64, i64, i64)> = {
            let mut resolver = Resolver::new(&tx, graph_version)?;
            resolver.go_structural_implements()?
        };
        let existing: Vec<(i64, i64, i64)> = {
            let mut stmt = tx.prepare(
                "SELECT id, source_symbol_id, target_symbol_id FROM edges
                 WHERE graph_version = ?1 AND derived = ?2 AND kind = 'IMPLEMENTS'",
            )?;
            stmt.query_map(params![graph_version, GO_STRUCTURAL_DERIVED], |row| {
                Ok((
                    row.get(0)?,
                    row.get::<_, Option<i64>>(1)?.unwrap_or(0),
                    row.get::<_, Option<i64>>(2)?.unwrap_or(0),
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let wanted_set: HashSet<(i64, i64)> = wanted.iter().map(|&(c, i, _)| (c, i)).collect();
        let mut kept: HashSet<(i64, i64)> = HashSet::new();
        let mut changed = 0;
        for (id, source, target) in existing {
            if wanted_set.contains(&(source, target)) && kept.insert((source, target)) {
                continue;
            }
            tx.execute("DELETE FROM edges WHERE id = ?1", params![id])?;
            changed += 1;
        }
        {
            let mut insert = tx.prepare(
                "INSERT INTO edges
                 (file_id, source_symbol_id, target_symbol_id, kind, target_qualname, detail,
                  confidence, graph_version, resolution_kind, derived)
                 VALUES (?1, ?2, ?3, 'IMPLEMENTS', (SELECT qualname FROM symbols WHERE id = ?3),
                         'structural', 1.0, ?4, 'structural', ?5)",
            )?;
            for (class, iface, file_id) in wanted {
                if kept.contains(&(class, iface)) {
                    continue;
                }
                insert.execute(params![
                    file_id,
                    class,
                    iface,
                    graph_version,
                    GO_STRUCTURAL_DERIVED
                ])?;
                changed += 1;
            }
        }
        tx.commit()?;
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn results_are_read_from_plain_and_named_lists() {
        assert_eq!(signature_results(Some("() -> *Server")), ["*Server"]);
        assert_eq!(
            signature_results(Some("(a int) -> (*Server, error)")),
            ["*Server", "error"]
        );
        assert_eq!(
            signature_results(Some("() -> (s *Server, err error)")),
            ["*Server", "error"]
        );
        assert_eq!(
            signature_results(Some("() -> (a, b int, err error)")),
            ["int", "int", "error"]
        );
        assert!(signature_results(Some("(a int)")).is_empty());
    }

    #[test]
    fn declared_types_are_read_in_the_declaring_files_terms() {
        let imports = GoImports {
            by_alias: HashMap::from([
                ("net".to_string(), "net".to_string()),
                ("cfg".to_string(), ">config".to_string()),
            ]),
            by_path: HashMap::new(),
        };
        assert_eq!(
            canonical_type("*Server", "srv", &imports),
            Some(GoType::Repo {
                dir: "srv".into(),
                name: "Server".into()
            })
        );
        assert_eq!(
            canonical_type("Set[T]", "srv", &imports),
            Some(GoType::Repo {
                dir: "srv".into(),
                name: "Set".into()
            })
        );
        assert_eq!(
            canonical_type("net.Conn", "srv", &imports),
            Some(GoType::Foreign {
                path: "net".into(),
                name: "Conn".into()
            })
        );
        assert_eq!(
            canonical_type("*cfg.Config", "srv", &imports),
            Some(GoType::Repo {
                dir: "config".into(),
                name: "Config".into()
            })
        );
        for text in [
            "[]int",
            "map[string]int",
            "error",
            "func(int)",
            "x.Y",
            "chan Server",
        ] {
            assert_eq!(canonical_type(text, "srv", &imports), None, "{text}");
        }
        let server = GoType::Repo {
            dir: "srv".into(),
            name: "Server".into(),
        };
        for text in [
            "[]*Server",
            "[4]Server",
            "map[string][]*Server",
            "...Server",
        ] {
            let seq = canonical_type(text, "srv", &imports);
            assert!(matches!(&seq, Some(GoType::Seq(_))), "{text}: {seq:?}");
        }
        assert_eq!(
            canonical_type("[]*Server", "srv", &imports),
            Some(GoType::Seq(Box::new(server)))
        );
    }

    #[test]
    fn struct_fields_split_named_and_embedded() {
        assert_eq!(
            struct_fields(Some(
                "#[go_type({conn net.Conn; ~Animal; m map[string]int})]"
            )),
            [
                (Some("conn".to_string()), "net.Conn".to_string()),
                (None, "Animal".to_string()),
                (Some("m".to_string()), "map[string]int".to_string()),
            ]
        );
    }
}
