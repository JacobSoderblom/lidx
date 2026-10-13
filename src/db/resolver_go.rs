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
use std::rc::Rc;

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

/// What `collapse_build_twins` groups a declaration by: twins share `kind` and
/// `member` (`Name`, or `Type.Method`); `visibility` carries the file's
/// build constraint. The one place a call site builds it.
pub(super) struct TwinKey {
    kind: String,
    member: String,
    visibility: Option<String>,
}

impl TwinKey {
    pub(super) fn new(kind: &str, member: &str, visibility: Option<&str>) -> Self {
        Self {
            kind: kind.to_string(),
            member: member.to_string(),
            visibility: visibility.map(str::to_string),
        }
    }
}

/// `items` with every set of build-constraint twins -- same `kind` and
/// member key, different file, at least one file constrained -- reduced to
/// the twin `pick_build_twin` chooses. A set no twin is picked from stays as
/// it is (ambiguous when named). `view` gives each item's [`TwinKey`].
pub(super) fn collapse_build_twins<T>(
    items: Vec<T>,
    view: impl Fn(&T) -> TwinKey,
    caller: Option<&BuildTag>,
) -> Vec<T> {
    if items.len() < 2 {
        return items;
    }
    let mut dropped = HashSet::new();
    {
        let mut groups: HashMap<(&str, &str), Vec<usize>> = HashMap::new();
        let views: Vec<_> = items.iter().map(&view).collect();
        for (i, key) in views.iter().enumerate() {
            groups.entry((&key.kind, &key.member)).or_default().push(i);
        }
        for group in groups.values().filter(|g| g.len() > 1) {
            let tags: Vec<Option<BuildTag>> = group
                .iter()
                .map(|&i| BuildTag::parse(views[i].visibility.as_deref()))
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

    /// The symbol as `collapse_build_twins` groups it.
    fn twin_key(&self) -> TwinKey {
        TwinKey::new(
            &self.kind,
            self.name_in_package(),
            self.visibility.as_deref(),
        )
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
            GoSymbol::twin_key,
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
                GoSymbol::twin_key,
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

/// A method's parameter and result types, each normalized (`method_type_key`).
type MethodSig = (Vec<String>, Vec<String>);

/// The methods of a Go type, promoted ones included.
#[derive(Default, Clone)]
struct MethodSet {
    methods: HashMap<String, MethodSig>,
    /// False when an embedded type's methods could not be told.
    complete: bool,
}

/// A Go type declaration, as the structural-implements pass sees it.
#[derive(Clone)]
pub(super) struct GoTypeRow {
    pub(super) id: i64,
    pub(super) qualname: String,
    pub(super) name: String,
    kind: String,
    signature: Option<String>,
    pub(super) file_id: i64,
    visibility: Option<String>,
    /// Path of the declaring file.
    path: String,
}

impl GoTypeRow {
    pub(super) fn dir(&self) -> &str {
        self.qualname.rsplit_once('/').map_or("", |(dir, _)| dir)
    }
}

/// The types implementing a Go interface.
pub(super) struct Implementers {
    pub(super) types: Vec<GoTypeRow>,
    /// A method owner with every method name of the interface was not a
    /// declaration we could judge (a type alias, a type with no symbol):
    /// there may be an implementer this list lacks, so it proves no
    /// uniqueness.
    pub(super) uncertain: bool,
    /// False when the search stopped at its limit.
    complete: bool,
}

/// What one `Resolver` has read of the Go declarations, memoized: the
/// symbols do not change while one transaction resolves.
#[derive(Default)]
pub(super) struct GoCaches {
    types: HashMap<(String, String), Rc<Vec<GoTypeRow>>>,
    methods: HashMap<String, Rc<Vec<(String, MethodSig)>>>,
    sets: HashMap<i64, MethodSet>,
    implementers: HashMap<(i64, bool), Rc<Implementers>>,
}

/// A type text with the parts two declarations of one signature may spell
/// differently dropped: whitespace, package qualifiers (`sbom.FormatID` and
/// `FormatID` are the same simple name) and `interface{}` for `any`. Two
/// methods have the same signature when their normalized texts are equal --
/// `ID() sbom.FormatID` is not `ID() artifact.ID`.
fn method_type_key(text: &str) -> String {
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let chars: Vec<char> = text.chars().collect();
    let is_word = |c: char| c.is_alphanumeric() || c == '_';
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        if is_word(chars[i]) {
            let start = i;
            while i < chars.len() && is_word(chars[i]) {
                i += 1;
            }
            // `pkg.Name`: drop the qualifier and its dot.
            let qualifier = chars.get(i) == Some(&'.')
                && chars
                    .get(i + 1)
                    .is_some_and(|&c| c.is_alphabetic() || c == '_');
            if qualifier {
                i += 1;
            } else {
                out.extend(&chars[start..i]);
            }
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out.replace("interface{}", "any")
}

/// The parameter types of a stored signature `(params) -> results`, one per
/// parameter (`(a, b int, c string)` is `int, int, string`).
fn signature_params(signature: &str) -> Vec<String> {
    let params = signature.rsplit_once(" -> ").map_or(signature, |(p, _)| p);
    let Some(inner) = params
        .trim()
        .strip_prefix('(')
        .and_then(|p| p.strip_suffix(')'))
    else {
        return Vec::new();
    };
    let parts = split_top_level(inner, ',');
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

/// The normalized parameter and result types of a stored method signature.
fn method_signature(signature: Option<&str>) -> MethodSig {
    let Some(sig) = signature else {
        return (Vec::new(), Vec::new());
    };
    (
        signature_params(sig)
            .iter()
            .map(|t| method_type_key(t))
            .collect(),
        signature_results(Some(sig))
            .iter()
            .map(|t| method_type_key(t))
            .collect(),
    )
}

/// The methods of the standard-library interfaces repositories embed most
/// (name and Go signature); any other foreign embedded interface leaves the
/// embedding one incomplete.
fn std_interface(path: &str, name: &str) -> Option<&'static [(&'static str, &'static str)]> {
    const READ: (&str, &str) = ("Read", "(p []byte) -> (n int, err error)");
    const WRITE: (&str, &str) = ("Write", "(p []byte) -> (n int, err error)");
    const CLOSE: (&str, &str) = ("Close", "() -> error");
    Some(match (path, name) {
        ("io", "Reader") => &[READ],
        ("io", "Writer") => &[WRITE],
        ("io", "Closer") => &[CLOSE],
        ("io", "Seeker") => &[("Seek", "(offset int64, whence int) -> (int64, error)")],
        ("io", "ReaderAt") => &[("ReadAt", "(p []byte, off int64) -> (n int, err error)")],
        ("io", "WriterAt") => &[("WriteAt", "(p []byte, off int64) -> (n int, err error)")],
        ("io", "ReaderFrom") => &[("ReadFrom", "(r io.Reader) -> (n int64, err error)")],
        ("io", "WriterTo") => &[("WriteTo", "(w io.Writer) -> (n int64, err error)")],
        ("io", "ByteReader") => &[("ReadByte", "() -> (byte, error)")],
        ("io", "ByteWriter") => &[("WriteByte", "(c byte) -> error")],
        ("io", "RuneReader") => &[("ReadRune", "() -> (r rune, size int, err error)")],
        ("io", "StringWriter") => &[("WriteString", "(s string) -> (n int, err error)")],
        ("io", "ReadWriter") => &[READ, WRITE],
        ("io", "ReadCloser") => &[READ, CLOSE],
        ("io", "WriteCloser") => &[WRITE, CLOSE],
        ("io", "ReadWriteCloser") => &[READ, WRITE, CLOSE],
        ("fmt", "Stringer") => &[("String", "() -> string")],
        ("sort", "Interface") => &[
            ("Len", "() -> int"),
            ("Less", "(i, j int) -> bool"),
            ("Swap", "(i, j int)"),
        ],
        ("net/http", "Handler") => &[("ServeHTTP", "(http.ResponseWriter, *http.Request)")],
        ("net/http", "ResponseWriter") => &[
            ("Header", "() -> http.Header"),
            WRITE,
            ("WriteHeader", "(statusCode int)"),
        ],
        ("net/http", "Flusher") => &[("Flush", "()")],
        ("net/http", "Hijacker") => &[("Hijack", "() -> (net.Conn, *bufio.ReadWriter, error)")],
        ("net/http", "Pusher") => &[("Push", "(target string, opts *http.PushOptions) -> error")],
        // A struct, not an interface: the exported methods of *exec.Cmd
        // (Go 1.24), which an embedding type promotes.
        ("os/exec", "Cmd") => &[
            ("CombinedOutput", "() -> ([]byte, error)"),
            ("Environ", "() -> []string"),
            ("Output", "() -> ([]byte, error)"),
            ("Run", "() -> error"),
            ("Start", "() -> error"),
            ("StderrPipe", "() -> (io.ReadCloser, error)"),
            ("StdinPipe", "() -> (io.WriteCloser, error)"),
            ("StdoutPipe", "() -> (io.ReadCloser, error)"),
            ("String", "() -> string"),
            ("Wait", "() -> error"),
        ],
        ("context", "Context") => &[
            ("Deadline", "() -> (deadline time.Time, ok bool)"),
            ("Done", "() -> <-chan struct{}"),
            ("Err", "() -> error"),
            ("Value", "(key any) -> any"),
        ],
        _ => return None,
    })
}

impl Resolver<'_> {
    /// The type declarations (struct, interface, named type) called `name`
    /// in package directory `dir`; build-constraint twins reduced.
    fn go_types_named(&mut self, dir: &str, name: &str) -> Result<Rc<Vec<GoTypeRow>>> {
        let key = (dir.to_string(), name.to_string());
        if let Some(known) = self.go.types.get(&key) {
            return Ok(known.clone());
        }
        let rows = self.go_type_query(
            "s.name = ?1 AND s.kind IN ('class', 'interface', 'type')",
            params![name, self.graph_version],
        )?;
        let rows: Vec<GoTypeRow> = rows.into_iter().filter(|t| t.dir() == dir).collect();
        let rows = collapse_build_twins(
            rows,
            |t: &GoTypeRow| TwinKey::new(&t.kind, &t.name, t.visibility.as_deref()),
            None,
        );
        let rows = Rc::new(rows);
        self.go.types.insert(key, rows.clone());
        Ok(rows)
    }

    fn go_type_query(&self, filter: &str, args: &[&dyn rusqlite::ToSql]) -> Result<Vec<GoTypeRow>> {
        let gv_param = args.len();
        let mut stmt = self.conn.prepare_cached(&format!(
            "SELECT s.id, s.qualname, s.name, s.kind, s.signature, s.file_id, s.visibility, f.path
             FROM symbols s JOIN files f ON s.file_id = f.id
             WHERE f.language = 'go' AND {filter} AND {}",
            live_symbols(gv_param)
        ))?;
        let rows = stmt
            .query_map(args, |row| {
                Ok(GoTypeRow {
                    id: row.get(0)?,
                    qualname: row.get(1)?,
                    name: row.get(2)?,
                    kind: row.get(3)?,
                    signature: row.get(4)?,
                    file_id: row.get(5)?,
                    visibility: row.get(6)?,
                    path: row.get(7)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// The methods declared on the type `ty` of package directory `dir` (or
    /// the specs of that interface), whichever file of the package declares
    /// them, with their normalized signatures.
    fn go_own_methods(&mut self, dir: &str, ty: &str) -> Result<Rc<Vec<(String, MethodSig)>>> {
        let key = format!("{dir}|{ty}");
        if let Some(known) = self.go.methods.get(&key) {
            return Ok(known.clone());
        }
        let mut stmt = self.conn.prepare_cached(&format!(
            "SELECT s.qualname, s.name, s.signature FROM symbols s JOIN files f ON s.file_id = f.id
             WHERE s.kind = 'method' AND s.qualname LIKE ?1 ESCAPE '\\'
               AND f.language = 'go' AND {}",
            live_symbols(2)
        ))?;
        let escaped = ty
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let rows = stmt
            .query_map(
                params![format!("%.{escaped}.%"), self.graph_version],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let rows = Rc::new(
            rows.into_iter()
                .filter(|(qualname, name, _)| {
                    qualname
                        .strip_suffix(&format!(".{ty}.{name}"))
                        .is_some_and(|module| module.rsplit_once('/').map_or("", |(d, _)| d) == dir)
                })
                .map(|(_, name, sig)| (name, method_signature(sig.as_deref())))
                .collect::<Vec<_>>(),
        );
        self.go.methods.insert(key, rows.clone());
        Ok(rows)
    }

    /// The method set of `t`: its own methods and those of the types it embeds.
    fn go_method_set(&mut self, t: &GoTypeRow, depth: usize) -> Result<MethodSet> {
        if let Some(done) = self.go.sets.get(&t.id) {
            return Ok(done.clone());
        }
        let mut set = MethodSet {
            methods: HashMap::new(),
            complete: true,
        };
        for (name, sig) in self.go_own_methods(t.dir(), &t.name)?.iter() {
            set.methods.insert(name.clone(), sig.clone());
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
                        set.methods
                            .entry("Error".into())
                            .or_insert_with(|| method_signature(Some("() -> string")));
                        continue;
                    }
                    match canonical_type(&text, &dir, &imports) {
                        Some(GoType::Repo { dir, name }) => {
                            let found = self.go_types_named(&dir, &name)?;
                            let [only] = &found[..] else {
                                set.complete = false;
                                continue;
                            };
                            if only.id == t.id {
                                set.complete = false;
                                continue;
                            }
                            let inner = self.go_method_set(only, depth + 1)?;
                            set.complete &= inner.complete;
                            for (m, a) in inner.methods {
                                set.methods.entry(m).or_insert(a);
                            }
                        }
                        Some(GoType::Foreign { path, name }) => match std_interface(&path, &name) {
                            Some(methods) => {
                                for (m, sig) in methods {
                                    set.methods
                                        .entry((*m).to_string())
                                        .or_insert_with(|| method_signature(Some(sig)));
                                }
                            }
                            None => set.complete = false,
                        },
                        _ => set.complete = false,
                    }
                }
            }
        }
        self.go.sets.insert(t.id, set.clone());
        Ok(set)
    }

    /// The types (structs and named types, never interfaces) whose method
    /// sets hold each method of interface `iface` by name and signature --
    /// Go's implicit `implements`. Empty for an interface with no methods or
    /// with an embedded interface whose methods are not known, so no
    /// implementer rests on a guess. Found through the types that declare
    /// one of the interface's methods.
    pub(super) fn go_implementers(
        &mut self,
        iface: &GoTypeRow,
        limit: Option<usize>,
        from_test: bool,
    ) -> Result<Rc<Implementers>> {
        if let Some(known) = self.go.implementers.get(&(iface.id, from_test))
            && (known.complete || limit.is_some_and(|l| known.types.len() >= l))
        {
            return Ok(known.clone());
        }
        let want = self.go_method_set(iface, 0)?;
        let mut found: Vec<GoTypeRow> = Vec::new();
        let mut uncertain = !want.complete;
        let mut complete = true;
        if want.complete && !want.methods.is_empty() {
            let mut seen: HashSet<String> = HashSet::new();
            let mut names: Vec<&String> = want.methods.keys().collect();
            names.sort();
            'names: for name in names {
                let owners: Vec<String> = {
                    let mut stmt = self.conn.prepare_cached(&format!(
                        "SELECT s.qualname FROM symbols s JOIN files f ON s.file_id = f.id
                         WHERE s.name = ?1 AND s.kind = 'method' AND f.language = 'go' AND {}",
                        live_symbols(2)
                    ))?;
                    stmt.query_map(params![name, self.graph_version], |row| row.get(0))?
                        .collect::<rusqlite::Result<Vec<String>>>()?
                };
                for qualname in owners {
                    let Some((module, ty)) = qualname
                        .strip_suffix(name.as_str())
                        .and_then(|c| c.strip_suffix('.'))
                        .and_then(|c| c.rsplit_once('.'))
                    else {
                        continue;
                    };
                    let dir = module.rsplit_once('/').map_or("", |(d, _)| d).to_string();
                    let ty = ty.to_string();
                    if !seen.insert(format!("{dir}|{ty}")) {
                        continue;
                    }
                    let rows = self.go_types_named(&dir, &ty)?;
                    if rows.is_empty() {
                        // methods of a receiver type with no declaration here
                        let own = self.go_own_methods(&dir, &ty)?;
                        if want.methods.keys().all(|m| own.iter().any(|(n, _)| n == m)) {
                            uncertain = true;
                        }
                        continue;
                    }
                    for t in rows.iter().filter(|t| t.kind != "interface") {
                        let has = self.go_method_set(t, 0)?;
                        if want
                            .methods
                            .iter()
                            .all(|(m, a)| has.methods.get(m) == Some(a))
                        {
                            found.push(t.clone());
                        }
                    }
                    // the limit counts what a caller can reach: a production
                    // caller never reaches a type declared in a test file
                    let reachable = found
                        .iter()
                        .filter(|t| from_test || !t.path.ends_with("_test.go"))
                        .count();
                    if limit.is_some_and(|l| reachable >= l) {
                        complete = false;
                        break 'names;
                    }
                }
            }
        }
        found.sort_by_key(|t| t.id);
        let found = Rc::new(Implementers {
            types: found,
            uncertain,
            complete,
        });
        self.go
            .implementers
            .insert((iface.id, from_test), found.clone());
        Ok(found)
    }

    /// The types with a struct field embedding a type of that simple name.
    fn go_embedders(&mut self) -> Result<HashMap<String, Vec<GoTypeRow>>> {
        let rows = self.go_type_query(
            "s.kind IN ('class', 'type') AND s.signature LIKE '%~%'",
            params![self.graph_version],
        )?;
        let mut out: HashMap<String, Vec<GoTypeRow>> = HashMap::new();
        for row in rows {
            for (name, text) in struct_fields(row.signature.as_deref()) {
                if name.is_none() {
                    let simple = text
                        .trim_start_matches('*')
                        .rsplit('.')
                        .next()
                        .unwrap_or("")
                        .split('[')
                        .next()
                        .unwrap_or("")
                        .to_string();
                    out.entry(simple).or_default().push(row.clone());
                }
            }
        }
        Ok(out)
    }

    /// The embedding types (of `embedders`) with a field embedding a
    /// standard-library type `std_interface` knows.
    fn go_std_embedders(
        &mut self,
        embedders: &HashMap<String, Vec<GoTypeRow>>,
    ) -> Result<Vec<GoTypeRow>> {
        let mut seen: HashSet<i64> = HashSet::new();
        let mut out = Vec::new();
        for row in embedders.values().flatten() {
            if !seen.insert(row.id) {
                continue;
            }
            let imports = self.go_file_imports(row.file_id)?;
            let dir = row.dir().to_string();
            let known = struct_fields(row.signature.as_deref())
                .into_iter()
                .filter(|(name, _)| name.is_none())
                .any(|(_, text)| {
                    matches!(
                        canonical_type(&text, &dir, &imports),
                        Some(GoType::Foreign { path, name }) if std_interface(&path, &name).is_some()
                    )
                });
            if known {
                out.push(row.clone());
            }
        }
        out.sort_by_key(|t| t.id);
        Ok(out)
    }

    /// A Go call that resolved to a method *spec* of an interface
    /// (`Doer.Do`), narrowed to the method that runs when the call site or
    /// the repository say so:
    /// 1. the receiver's dynamic type is proven at the call site (a
    ///    `GoDeferred::dynamic`: `var d Doer = &Impl{}`, `d` never assigned
    ///    again) and has the method: bind `Impl.Do`;
    /// 2. exactly one type of the repository implements the interface
    ///    (`go_implementers`) and has the method: bind it;
    /// 3. otherwise the call stays on the spec, and is stored as a
    ///    `DISPATCH` edge (`stored_edge_kind`), not a `CALLS` one.
    pub(super) fn refine_go_interface_call(
        &mut self,
        r: &Reference<'_>,
        resolution: Resolution,
    ) -> Result<Resolution> {
        let Resolution::Resolved { target_id, .. } = resolution else {
            return Ok(resolution);
        };
        if !self.narrow_interface_calls
            || r.source_lang != "go"
            || r.edge_kind != edge_views::CALLS
            || r.call_shape.is_some_and(|shape| shape.is_ref)
        {
            return Ok(resolution);
        }
        let Some((spec, iface)) = self.go_interface_spec(target_id)? else {
            return Ok(resolution);
        };
        if let Some(DeferredMarker::Go(GoDeferred {
            dynamic: Some(dynamic),
            method,
            ..
        })) = r.deferred
            && let Some(GoType::Repo { dir, name }) = self.go_eval(dynamic, 0)?
            && let Some(found) = self.go_member(&dir, &name, method, 0)?
            && found.kind == "method"
            && found.id != spec.id
        {
            return Ok(resolved(found.id, ResolutionKind::ReceiverType));
        }
        let from_test = r.source_file_path.ends_with("_test.go");
        // A production caller never reaches a type declared in a test file.
        let implementers = self.go_implementers(&iface, Some(2), from_test)?;
        let visible: Vec<&GoTypeRow> = implementers
            .types
            .iter()
            .filter(|t| from_test || !t.path.ends_with("_test.go"))
            .collect();
        if !implementers.uncertain
            && let [only] = &visible[..]
            && let Some(found) = self.go_member(only.dir(), &only.name, &spec.name, 0)?
            && found.kind == "method"
            && found.id != spec.id
        {
            return Ok(resolved(found.id, ResolutionKind::ReceiverType));
        }
        Ok(resolution)
    }

    /// `(spec, interface)` when symbol `id` is a method spec of a Go
    /// interface.
    fn go_interface_spec(&mut self, id: i64) -> Result<Option<(GoTypeRow, GoTypeRow)>> {
        let specs = self.go_type_query(
            "s.id = ?1 AND s.kind = 'method'",
            params![id, self.graph_version],
        )?;
        let Some(spec) = specs.into_iter().next() else {
            return Ok(None);
        };
        let Some(container) = spec
            .qualname
            .strip_suffix(spec.name.as_str())
            .and_then(|c| c.strip_suffix('.'))
        else {
            return Ok(None);
        };
        let ifaces = self.go_type_query(
            "s.qualname = ?1 AND s.kind = 'interface'",
            params![container, self.graph_version],
        )?;
        Ok(ifaces.into_iter().next().map(|iface| (spec, iface)))
    }

    /// Every `(type, interface)` pair, by type id, interface id and the
    /// type's file: the types declaring the interface's methods, and those
    /// embedding such a type (or the interface) a few levels out.
    fn go_structural_implements(&mut self) -> Result<Vec<(i64, i64, i64)>> {
        let interfaces = self.go_type_query("s.kind = 'interface'", params![self.graph_version])?;
        let embedders = self.go_embedders()?;
        let foreign = self.go_std_embedders(&embedders)?;
        let mut pairs = Vec::new();
        for iface in interfaces {
            let base = self.go_implementers(&iface, None, true)?;
            if base.uncertain && base.types.is_empty() {
                continue;
            }
            let want = self.go_method_set(&iface, 0)?;
            let mut seen: HashSet<i64> = base.types.iter().map(|t| t.id).collect();
            let mut all: Vec<GoTypeRow> = base.types.clone();
            let mut frontier: Vec<String> = base
                .types
                .iter()
                .map(|t| t.name.clone())
                .chain([iface.name.clone()])
                .collect();
            if want.complete && !want.methods.is_empty() {
                // A type with no method of its own can still hold every one
                // of them through an embedded standard-library type; the
                // types declaring a method never reach it.
                for row in &foreign {
                    if !seen.insert(row.id) {
                        continue;
                    }
                    frontier.push(row.name.clone());
                    let has = self.go_method_set(row, 0)?;
                    if want
                        .methods
                        .iter()
                        .all(|(m, a)| has.methods.get(m) == Some(a))
                    {
                        all.push(row.clone());
                    }
                }
                for _ in 0..MAX_EMBEDDING_DEPTH {
                    let mut next = Vec::new();
                    for ty in frontier.drain(..) {
                        for row in embedders.get(&ty).into_iter().flatten() {
                            if !seen.insert(row.id) {
                                continue;
                            }
                            next.push(row.name.clone());
                            let has = self.go_method_set(row, 0)?;
                            if want
                                .methods
                                .iter()
                                .all(|(m, a)| has.methods.get(m) == Some(a))
                            {
                                all.push(row.clone());
                            }
                        }
                    }
                    if next.is_empty() {
                        break;
                    }
                    frontier = next;
                }
            }
            for t in all {
                pairs.push((t.id, iface.id, t.file_id));
            }
        }
        Ok(pairs)
    }
}

impl Resolver<'_> {
    /// Every `(struct, embedded type, struct's file)` where a struct of the
    /// repository embeds a type of the repository (`type S struct{ T }`,
    /// `*T`, `T[A]`, an interface alike): the embedded type's declaration,
    /// found in the struct's file's imports and package. A type the
    /// repository declares more than once (and no build constraint tells
    /// apart), or one outside it, gets no pair.
    fn go_embeds(&mut self) -> Result<Vec<(i64, i64, i64)>> {
        let structs = self.go_type_query(
            "s.kind IN ('class', 'type') AND s.signature LIKE '%~%'",
            params![self.graph_version],
        )?;
        let mut pairs = Vec::new();
        for row in structs {
            let embedded: Vec<String> = struct_fields(row.signature.as_deref())
                .into_iter()
                .filter(|(name, _)| name.is_none())
                .map(|(_, text)| text)
                .collect();
            if embedded.is_empty() {
                continue;
            }
            let imports = self.go_file_imports(row.file_id)?;
            let dir = row.dir().to_string();
            for text in embedded {
                let Some(GoType::Repo { dir, name }) = canonical_type(&text, &dir, &imports) else {
                    continue;
                };
                let found = self.go_types_named(&dir, &name)?;
                if let [only] = &found[..]
                    && only.id != row.id
                {
                    pairs.push((row.id, only.id, row.file_id));
                }
            }
        }
        Ok(pairs)
    }

    /// Every `(receiver type, method, method's file)` for a method whose
    /// qualname names its own file's module (`altsrc/flag.IntSliceFlag.Apply`)
    /// although its receiver type is declared in another file of the package
    /// (`altsrc/flag_generated.IntSliceFlag`): Go allows methods in any file,
    /// so the qualname's container names no symbol and the extractor's
    /// `CONTAINS` edge has no source. The receiver is found by name in the
    /// package directory, build-constraint twins reduced to the one the
    /// method's own constraint (or the default host) selects.
    fn go_receiver_contains(&mut self) -> Result<Vec<(i64, i64, i64)>> {
        let methods = self.go_type_query("s.kind = 'method'", params![self.graph_version])?;
        if methods.is_empty() {
            return Ok(Vec::new());
        }
        let types = self.go_type_query(
            "s.kind IN ('class', 'interface', 'type')",
            params![self.graph_version],
        )?;
        let declared: HashSet<&str> = types.iter().map(|t| t.qualname.as_str()).collect();
        let mut by_name: HashMap<(String, &str), Vec<&GoTypeRow>> = HashMap::new();
        for t in &types {
            by_name
                .entry((t.dir().to_string(), t.name.as_str()))
                .or_default()
                .push(t);
        }
        let mut pairs = Vec::new();
        for m in &methods {
            let Some((container, _)) = m.qualname.rsplit_once('.') else {
                continue;
            };
            if declared.contains(container) {
                continue;
            }
            let Some((module, ty)) = container.rsplit_once('.') else {
                continue;
            };
            let dir = module.rsplit_once('/').map_or("", |(d, _)| d).to_string();
            let Some(candidates) = by_name.get(&(dir, ty)) else {
                continue;
            };
            let candidates = collapse_build_twins(
                candidates.clone(),
                |t: &&GoTypeRow| TwinKey::new(&t.kind, &t.name, t.visibility.as_deref()),
                BuildTag::parse(m.visibility.as_deref()).as_ref(),
            );
            if let [only] = &candidates[..] {
                pairs.push((only.id, m.id, m.file_id));
            }
        }
        Ok(pairs)
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
    ///
    /// Two more edge sets are rebuilt the same way, from the same symbols:
    /// the `EXTENDS` edge of each struct embedding a type of the repository
    /// (`DERIVED_GO_EMBED`), and the `CONTAINS` edge of a method declared in
    /// another file than its receiver type, whose qualname names the
    /// method's file (`DERIVED_GO_RECEIVER`).
    pub fn rederive_go_implements(&self, graph_version: i64) -> Result<usize> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let (implements, embeds, receivers) = {
            let mut resolver = Resolver::new(&tx, graph_version)?;
            (
                resolver.go_structural_implements()?,
                resolver.go_embeds()?,
                resolver.go_receiver_contains()?,
            )
        };
        let mut changed = 0;
        for (kind, derived, detail, wanted) in [
            (
                "IMPLEMENTS",
                crate::model::DERIVED_GO_STRUCTURAL,
                "structural",
                implements,
            ),
            (
                "EXTENDS",
                crate::model::DERIVED_GO_EMBED,
                "embedded",
                embeds,
            ),
            (
                "CONTAINS",
                crate::model::DERIVED_GO_RECEIVER,
                "receiver",
                receivers,
            ),
        ] {
            changed += sync_derived_edges(&tx, graph_version, kind, derived, detail, wanted)?;
        }
        tx.commit()?;
        Ok(changed)
    }
}

/// Make the `kind` edges a pass owns (`derived`) exactly `wanted`
/// (`(source, target, file_id)`): delete the rest, insert what is missing,
/// leave the others untouched. Returns how many rows changed.
fn sync_derived_edges(
    tx: &rusqlite::Transaction<'_>,
    graph_version: i64,
    kind: &str,
    derived: i64,
    detail: &str,
    wanted: Vec<(i64, i64, i64)>,
) -> Result<usize> {
    let existing: Vec<(i64, i64, i64)> = {
        let mut stmt = tx.prepare(
            "SELECT id, source_symbol_id, target_symbol_id FROM edges
             WHERE graph_version = ?1 AND derived = ?2 AND kind = ?3",
        )?;
        stmt.query_map(params![graph_version, derived, kind], |row| {
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
    let mut insert = tx.prepare(
        "INSERT INTO edges
         (file_id, source_symbol_id, target_symbol_id, kind, target_qualname, detail,
          confidence, graph_version, resolution_kind, derived)
         VALUES (?1, ?2, ?3, ?4, (SELECT qualname FROM symbols WHERE id = ?3),
                 ?5, 1.0, ?6, 'structural', ?7)",
    )?;
    let mut written: HashSet<(i64, i64)> = HashSet::new();
    for (source, target, file_id) in wanted {
        if kept.contains(&(source, target)) || !written.insert((source, target)) {
            continue;
        }
        insert.execute(params![
            file_id,
            source,
            target,
            kind,
            detail,
            graph_version,
            derived
        ])?;
        changed += 1;
    }
    Ok(changed)
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

    #[test]
    fn method_signatures_compare_by_normalized_simple_type_names() {
        let sig = |s: &str| method_signature(Some(s));
        // package qualifiers and spacing do not matter ...
        assert_eq!(
            sig("(r io.Reader, n int) -> (*sbom.SBOM, error)"),
            sig("(in  Reader, count int) -> (*SBOM,error)")
        );
        assert_eq!(sig("(a, b int) -> bool"), sig("(int, int) -> bool"));
        assert_eq!(sig("(v interface{}) -> any"), sig("(v any) -> any"));
        // ... but the types do
        assert_ne!(sig("() -> sbom.FormatID"), sig("() -> artifact.ID"));
        assert_ne!(sig("(x int) -> error"), sig("(x string) -> error"));
        assert_ne!(sig("(x int) -> error"), sig("(x int)"));
        assert_ne!(sig("(x ...int)"), sig("(x []int)"));
    }
}
