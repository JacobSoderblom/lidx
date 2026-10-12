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

/// How deep embedded types are searched for a promoted member.
const MAX_EMBEDDING_DEPTH: usize = 4;

/// A Go type an expression evaluates to.
#[derive(Debug, Clone, PartialEq, Eq)]
enum GoType {
    /// A named type declared in a package directory of this repository.
    Repo { dir: String, name: String },
    /// A named type of a package outside it (an import path).
    Foreign { path: String, name: String },
}

/// A symbol a Go lookup matched.
struct GoSymbol {
    id: i64,
    qualname: String,
    kind: String,
    signature: Option<String>,
    file_id: i64,
}

impl GoSymbol {
    /// The package directory the symbol is declared in.
    fn dir(&self) -> &str {
        self.qualname.rsplit_once('/').map_or("", |(dir, _)| dir)
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
            None => None,
        })
    }

    fn go_eval(&mut self, value: &GoValue, depth: usize) -> Result<Option<GoType>> {
        if depth > crate::indexer::extract::MAX_DEFERRED_DEPTH {
            return Ok(None);
        }
        match value {
            GoValue::Type { place, name } => Ok(Some(go_type_at(place, name))),
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
            "SELECT s.id, s.qualname, s.kind, s.signature, s.file_id
             FROM symbols s JOIN files f ON s.file_id = f.id
             WHERE s.name = ?1 AND f.language = 'go' AND s.kind != 'method' AND {}",
            live_symbols(2)
        ))?;
        let rows = stmt
            .query_map(params![name, self.graph_version], go_symbol_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows.into_iter().filter(|s| s.dir() == dir).collect())
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
                "SELECT s.id, s.qualname, s.kind, s.signature, s.file_id
                 FROM symbols s JOIN files f ON s.file_id = f.id
                 WHERE s.name = ?1 AND f.language = 'go' AND s.kind = 'method' AND {}",
                live_symbols(2)
            ))?;
            let rows = stmt
                .query_map(params![name, self.graph_version], go_symbol_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows.into_iter()
                .filter(|s| s.qualname.ends_with(&suffix) && s.dir() == dir)
                .collect::<Vec<_>>()
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
        found.retain(|s| s.kind == "class");
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
    fn go_file_imports(&mut self, file_id: i64) -> Result<HashMap<String, String>> {
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
        let mut imports = HashMap::new();
        for detail in rows.into_iter().flatten() {
            let Ok(json) = serde_json::from_str::<serde_json::Value>(&detail) else {
                continue;
            };
            if let (Some(alias), Some(place)) = (json["alias"].as_str(), json["place"].as_str()) {
                imports.insert(alias.to_string(), place.to_string());
            }
        }
        Ok(imports)
    }
}

fn go_symbol_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<GoSymbol> {
    Ok(GoSymbol {
        id: row.get(0)?,
        qualname: row.get(1)?,
        kind: row.get(2)?,
        signature: row.get(3)?,
        file_id: row.get(4)?,
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
fn canonical_type(text: &str, dir: &str, imports: &HashMap<String, String>) -> Option<GoType> {
    let text = text.trim().trim_start_matches('*').trim();
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
            Some(go_type_at(imports.get(qualifier)?, name))
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
    let Some(inner) = results
        .strip_prefix('(')
        .and_then(|r| r.strip_suffix(')'))
    else {
        return vec![results.to_string()];
    };
    let parts = split_top_level(inner, ',');
    // Named results (`(s *Server, err error)`, `(a, b int)`): every part
    // names something, and a bare name takes the next type.
    let named = parts.iter().any(|p| split_top_level(p.trim(), ' ').len() > 1);
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
    parts
        .into_iter()
        .filter(|p| !p.trim().is_empty())
        .collect()
}

/// `(field name, type text)` of each field in a stored struct signature
/// `{name Type; ~Embedded}`; an embedded field has no name.
fn struct_fields(signature: Option<&str>) -> Vec<(Option<String>, String)> {
    let Some(inner) = signature
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
        let imports = HashMap::from([
            ("net".to_string(), "net".to_string()),
            ("cfg".to_string(), ">config".to_string()),
        ]);
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
        for text in ["[]Server", "map[string]int", "error", "func(int)", "x.Y"] {
            assert_eq!(canonical_type(text, "srv", &imports), None, "{text}");
        }
    }

    #[test]
    fn struct_fields_split_named_and_embedded() {
        assert_eq!(
            struct_fields(Some("{conn net.Conn; ~Animal; m map[string]int}")),
            [
                (Some("conn".to_string()), "net.Conn".to_string()),
                (None, "Animal".to_string()),
                (Some("m".to_string()), "map[string]int".to_string()),
            ]
        );
    }
}
