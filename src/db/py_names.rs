//! Whole-identifier name scan of a Python source file, for `unused_imports`
//! (issue #242). Value uses of an imported name (decorators, arguments,
//! attribute reads, unpacking, membership tests) emit no edge, so the
//! edge-based checks miss them.
//!
//! Deliberately self-contained: it does NOT reuse the XREF literal scanner.
//! It lexes just enough to (a) skip comments and string literals, (b) scan
//! f-string `{...}` interpolations as code, and (c) skip import statements
//! so an import never counts as its own use.

use std::collections::HashSet;

/// Every identifier that appears as code in `src`, outside import
/// statements, comments and string literals (f-string interpolations count).
pub(super) fn used_names(src: &str) -> HashSet<String> {
    let chars: Vec<char> = src.chars().collect();
    let mut out = HashSet::new();
    scan(&chars, true, &mut out);
    out
}

fn is_ident_start(c: char) -> bool {
    c == '_' || c.is_alphabetic()
}

fn is_ident_char(c: char) -> bool {
    c == '_' || c.is_alphanumeric()
}

fn is_string_prefix(ident: &str) -> Option<bool> {
    let lower = ident.to_ascii_lowercase();
    match lower.as_str() {
        "r" | "u" | "b" | "br" | "rb" => Some(false),
        "f" | "fr" | "rf" => Some(true),
        _ => None,
    }
}

/// Index just past the string literal whose opening quote is at `i`
/// (`chars[i]` is `'` or `"`). Unterminated literals run to the end.
fn skip_string(chars: &[char], i: usize) -> usize {
    let q = chars[i];
    let triple = chars.get(i + 1) == Some(&q) && chars.get(i + 2) == Some(&q);
    let mut j = i + if triple { 3 } else { 1 };
    while j < chars.len() {
        let c = chars[j];
        if c == '\\' {
            j += 2;
        } else if c == q {
            if !triple {
                return j + 1;
            }
            if chars.get(j + 1) == Some(&q) && chars.get(j + 2) == Some(&q) {
                return j + 3;
            }
            j += 1;
        } else if c == '\n' && !triple {
            return j; // unterminated single-line string
        } else {
            j += 1;
        }
    }
    chars.len()
}

/// Scan an f-string literal (opening quote at `i`), collecting names from
/// its `{expr}` interpolations. Returns the index past the literal.
fn scan_fstring(chars: &[char], i: usize, out: &mut HashSet<String>) -> usize {
    let end = skip_string(chars, i);
    let q = chars[i];
    let triple = chars.get(i + 1) == Some(&q) && chars.get(i + 2) == Some(&q);
    let body_start = i + if triple { 3 } else { 1 };
    let body_end = end
        .saturating_sub(if triple { 3 } else { 1 })
        .max(body_start);
    let mut j = body_start;
    while j < body_end {
        match chars[j] {
            '\\' => j += 2,
            '{' if chars.get(j + 1) == Some(&'{') => j += 2,
            '{' => {
                let mut depth = 1;
                let mut k = j + 1;
                while k < body_end && depth > 0 {
                    match chars[k] {
                        '{' => depth += 1,
                        '}' => depth -= 1,
                        '\'' | '"' => {
                            k = skip_string(chars, k);
                            continue;
                        }
                        _ => {}
                    }
                    k += 1;
                }
                let expr_end = if depth == 0 { k - 1 } else { k };
                scan(&chars[j + 1..expr_end.max(j + 1)], false, out);
                j = k;
            }
            _ => j += 1,
        }
    }
    end
}

/// Skip an import statement starting at `i`: to the end of the logical line
/// (past balanced brackets and backslash continuations) or a `;`.
fn skip_import(chars: &[char], mut i: usize) -> usize {
    let mut depth = 0i32;
    while i < chars.len() {
        match chars[i] {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            '#' => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
                continue;
            }
            '\\' => i += 1,
            '\n' if depth <= 0 => return i,
            ';' if depth <= 0 => return i,
            _ => {}
        }
        i += 1;
    }
    chars.len()
}

fn scan(chars: &[char], mut stmt_start: bool, out: &mut HashSet<String>) {
    let mut i = 0;
    let mut depth = 0i32;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '#' => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '\n' => {
                if depth <= 0 {
                    stmt_start = true;
                }
                i += 1;
            }
            ';' => {
                stmt_start = true;
                i += 1;
            }
            ' ' | '\t' | '\r' | '\x0c' => i += 1,
            '\\' => i += 2,
            '\'' | '"' => {
                i = skip_string(chars, i);
                stmt_start = false;
            }
            '(' | '[' | '{' => {
                depth += 1;
                stmt_start = false;
                i += 1;
            }
            ')' | ']' | '}' => {
                depth -= 1;
                stmt_start = false;
                i += 1;
            }
            c if is_ident_start(c) => {
                let start = i;
                while i < chars.len() && is_ident_char(chars[i]) {
                    i += 1;
                }
                let ident: String = chars[start..i].iter().collect();
                if matches!(chars.get(i), Some('\'' | '"'))
                    && let Some(is_f) = is_string_prefix(&ident)
                {
                    i = if is_f {
                        scan_fstring(chars, i, out)
                    } else {
                        skip_string(chars, i)
                    };
                    stmt_start = false;
                    continue;
                }
                if stmt_start && (ident == "import" || ident == "from") {
                    i = skip_import(chars, i);
                    continue;
                }
                stmt_start = false;
                out.insert(ident);
            }
            _ => {
                stmt_start = false;
                i += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::used_names;

    #[test]
    fn skips_imports_comments_strings_but_scans_fstrings() {
        let n = used_names(
            "import a\nfrom b import (c,\n  d)\nx = 1  # e's\ns = 'f'\nt = f'{g} {{h}}'\n'''i\n'''\nj\n",
        );
        for used in ["x", "s", "t", "g", "j"] {
            assert!(n.contains(used), "{used}");
        }
        for unused in ["a", "b", "c", "d", "e", "f", "h", "i"] {
            assert!(!n.contains(unused), "{unused}");
        }
    }

    #[test]
    fn indented_import_and_semicolon() {
        let n = used_names("def f():\n    import q\n    return r; import s\n");
        assert!(n.contains("r") && !n.contains("q") && !n.contains("s"));
    }
}
