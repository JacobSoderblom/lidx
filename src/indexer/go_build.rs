//! Go build constraints: what a file's `//go:build` line, legacy `// +build`
//! lines and `_GOOS` / `_GOARCH` filename suffix say about the builds it is
//! part of.
//!
//! A package may declare one name in several files when their constraints
//! exclude each other (`stdlib_regex.go` is `!gore2regex`, `wasilibs_regex.go`
//! is `gore2regex`). The extractor records, on every symbol of such a file, a
//! `buildtag:<key>` modifier (the constraint, canonically printed) and
//! `buildhost` when the constraint holds on the **default host**; the
//! resolver picks among same-package twins with them (see
//! `db::resolver_go::pick_build_twin`).
//!
//! Default host, the one choice every file is judged against: `GOOS=linux`,
//! `GOARCH=amd64`, cgo off (what the oracle benchmark builds with), no
//! `-tags`. The tags true there are `linux`, `amd64`, `unix`, `gc` and every
//! release tag `go1.1` .. `go1.24`; every other tag (`windows`,
//! `gore2regex`, `ignore`, `cgo`, ...) is false.

/// `symbols.visibility` modifier prefix: the file's constraint key follows.
pub const BUILD_TAG_MODIFIER: &str = "buildtag:";
/// `symbols.visibility` modifier: the file's constraint holds on the default host.
pub const BUILD_HOST_MODIFIER: &str = "buildhost";

const HOST_OS: &str = "linux";
const HOST_ARCH: &str = "amd64";
/// Release tags `go1.1` .. `go1.<N>` are true.
const MAX_RELEASE: u32 = 24;

const KNOWN_OS: &[&str] = &[
    "aix",
    "android",
    "darwin",
    "dragonfly",
    "freebsd",
    "hurd",
    "illumos",
    "ios",
    "js",
    "linux",
    "nacl",
    "netbsd",
    "openbsd",
    "plan9",
    "solaris",
    "wasip1",
    "windows",
    "zos",
];

const KNOWN_ARCH: &[&str] = &[
    "386",
    "amd64",
    "amd64p32",
    "arm",
    "armbe",
    "arm64",
    "arm64be",
    "loong64",
    "mips",
    "mipsle",
    "mips64",
    "mips64le",
    "mips64p32",
    "mips64p32le",
    "ppc",
    "ppc64",
    "ppc64le",
    "riscv",
    "riscv64",
    "s390",
    "s390x",
    "sparc",
    "sparc64",
    "wasm",
];

/// A build constraint expression.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Expr {
    Tag(String),
    Not(Box<Expr>),
    And(Vec<Expr>),
    Or(Vec<Expr>),
}

impl Expr {
    fn holds_on_host(&self) -> bool {
        match self {
            Expr::Tag(tag) => host_tag(tag),
            Expr::Not(inner) => !inner.holds_on_host(),
            Expr::And(parts) => parts.iter().all(Expr::holds_on_host),
            Expr::Or(parts) => parts.iter().any(Expr::holds_on_host),
        }
    }

    /// Canonical text: no spaces, so it can sit inside a modifier word.
    fn print(&self, out: &mut String) {
        match self {
            Expr::Tag(tag) => out.push_str(tag),
            Expr::Not(inner) => {
                out.push('!');
                if matches!(**inner, Expr::And(_) | Expr::Or(_)) {
                    out.push('(');
                    inner.print(out);
                    out.push(')');
                } else {
                    inner.print(out);
                }
            }
            Expr::And(parts) | Expr::Or(parts) => {
                let is_and = matches!(self, Expr::And(_));
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        out.push_str(if is_and { "&&" } else { "||" });
                    }
                    // `||` binds looser than `&&`: an `||` inside an `&&` needs parens.
                    let wrap = is_and && matches!(part, Expr::Or(_));
                    if wrap {
                        out.push('(');
                    }
                    part.print(out);
                    if wrap {
                        out.push(')');
                    }
                }
            }
        }
    }
}

fn host_tag(tag: &str) -> bool {
    if matches!(tag, "unix" | "gc") || tag == HOST_OS || tag == HOST_ARCH {
        return true;
    }
    tag.strip_prefix("go1.")
        .and_then(|n| n.parse::<u32>().ok())
        .is_some_and(|n| (1..=MAX_RELEASE).contains(&n))
}

/// What a file's constraints say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildConstraint {
    /// The constraint, canonically printed (no whitespace).
    pub key: String,
    /// Whether it holds on the default host.
    pub host: bool,
}

impl BuildConstraint {
    /// The `symbols.visibility` words recording this constraint.
    pub fn modifiers(&self) -> Vec<String> {
        let mut words = vec![format!("{BUILD_TAG_MODIFIER}{}", self.key)];
        if self.host {
            words.push(BUILD_HOST_MODIFIER.to_string());
        }
        words
    }
}

/// The constraint of the file whose module stem (`name` of `name.go`) is
/// `stem` and whose text is `source`; `None` when it has none.
pub fn file_constraint(stem: &str, source: &str) -> Option<BuildConstraint> {
    let from_name = name_constraint(stem);
    let from_header = header_constraint(source);
    let mut key = String::new();
    let mut host = true;
    if let Some((name_key, name_host)) = &from_name {
        key.push_str(name_key);
        host &= name_host;
    }
    if let Some(expr) = &from_header {
        if !key.is_empty() {
            key.push('&');
        }
        expr.print(&mut key);
        host &= expr.holds_on_host();
    }
    (!key.is_empty()).then_some(BuildConstraint { key, host })
}

/// `x_linux`, `x_amd64`, `x_linux_amd64` (`_test` already allowed for):
/// the key `file:<os>[_<arch>]` and whether it matches the host.
fn name_constraint(stem: &str) -> Option<(String, bool)> {
    let stem = stem.rsplit('/').next().unwrap_or(stem);
    let stem = stem.strip_suffix("_test").unwrap_or(stem);
    let parts: Vec<&str> = stem.split('_').collect();
    if parts.len() < 2 {
        return None;
    }
    let last = parts[parts.len() - 1];
    if KNOWN_ARCH.contains(&last) {
        if parts.len() >= 3 && KNOWN_OS.contains(&parts[parts.len() - 2]) {
            let os = parts[parts.len() - 2];
            return Some((
                format!("file:{os}_{last}"),
                os == HOST_OS && last == HOST_ARCH,
            ));
        }
        return Some((format!("file:{last}"), last == HOST_ARCH));
    }
    if KNOWN_OS.contains(&last) {
        return Some((format!("file:{last}"), last == HOST_OS));
    }
    None
}

/// The constraint of the header comments before the `package` clause:
/// `//go:build` when present, else the `// +build` lines (all must hold).
fn header_constraint(source: &str) -> Option<Expr> {
    let mut plus_build: Vec<Expr> = Vec::new();
    let mut in_block = false;
    for line in source.lines() {
        let line = line.trim();
        if in_block {
            if line.contains("*/") {
                in_block = false;
            }
            continue;
        }
        if line.is_empty() {
            continue;
        }
        if line.starts_with("/*") {
            in_block = !line.contains("*/");
            continue;
        }
        let Some(comment) = line.strip_prefix("//") else {
            break;
        };
        if let Some(expr) = comment.strip_prefix("go:build") {
            if expr.starts_with([' ', '\t']) {
                return parse_expr(expr);
            }
        } else if let Some(args) = comment.trim_start().strip_prefix("+build")
            && args.starts_with([' ', '\t'])
            && let Some(expr) = parse_plus_build(args)
        {
            plus_build.push(expr);
        }
    }
    match plus_build.len() {
        0 => None,
        1 => plus_build.pop(),
        _ => Some(Expr::And(plus_build)),
    }
}

/// `// +build linux,amd64 darwin !cgo`: spaces are OR, commas AND.
fn parse_plus_build(args: &str) -> Option<Expr> {
    let mut alternatives = Vec::new();
    for option in args.split_whitespace() {
        let mut terms = Vec::new();
        for term in option.split(',') {
            let (negated, tag) = match term.strip_prefix('!') {
                Some(tag) => (true, tag),
                None => (false, term),
            };
            if !is_tag(tag) {
                return None;
            }
            let tag = Expr::Tag(tag.to_string());
            terms.push(if negated {
                Expr::Not(Box::new(tag))
            } else {
                tag
            });
        }
        alternatives.push(if terms.len() == 1 {
            terms.remove(0)
        } else {
            Expr::And(terms)
        });
    }
    match alternatives.len() {
        0 => None,
        1 => alternatives.pop(),
        _ => Some(Expr::Or(alternatives)),
    }
}

fn is_tag(tag: &str) -> bool {
    !tag.is_empty()
        && tag
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '_' | '.'))
}

/// `//go:build` expression grammar: `||` below `&&` below `!` and parens.
fn parse_expr(text: &str) -> Option<Expr> {
    let mut tokens = tokenize(text)?;
    tokens.reverse();
    let expr = parse_or(&mut tokens)?;
    tokens.is_empty().then_some(expr)
}

#[derive(Debug, PartialEq, Eq)]
enum Token {
    Tag(String),
    Not,
    And,
    Or,
    Open,
    Close,
}

fn tokenize(text: &str) -> Option<Vec<Token>> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            c if c.is_whitespace() => i += 1,
            '(' => {
                tokens.push(Token::Open);
                i += 1;
            }
            ')' => {
                tokens.push(Token::Close);
                i += 1;
            }
            '!' => {
                tokens.push(Token::Not);
                i += 1;
            }
            '&' if chars.get(i + 1) == Some(&'&') => {
                tokens.push(Token::And);
                i += 2;
            }
            '|' if chars.get(i + 1) == Some(&'|') => {
                tokens.push(Token::Or);
                i += 2;
            }
            c if c.is_alphanumeric() || c == '_' || c == '.' => {
                let start = i;
                while i < chars.len()
                    && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.')
                {
                    i += 1;
                }
                tokens.push(Token::Tag(chars[start..i].iter().collect()));
            }
            _ => return None,
        }
    }
    Some(tokens)
}

fn parse_or(tokens: &mut Vec<Token>) -> Option<Expr> {
    let mut parts = vec![parse_and(tokens)?];
    while tokens.last() == Some(&Token::Or) {
        tokens.pop();
        parts.push(parse_and(tokens)?);
    }
    Some(if parts.len() == 1 {
        parts.remove(0)
    } else {
        Expr::Or(parts)
    })
}

fn parse_and(tokens: &mut Vec<Token>) -> Option<Expr> {
    let mut parts = vec![parse_not(tokens)?];
    while tokens.last() == Some(&Token::And) {
        tokens.pop();
        parts.push(parse_not(tokens)?);
    }
    Some(if parts.len() == 1 {
        parts.remove(0)
    } else {
        Expr::And(parts)
    })
}

fn parse_not(tokens: &mut Vec<Token>) -> Option<Expr> {
    match tokens.pop()? {
        Token::Not => Some(Expr::Not(Box::new(parse_not(tokens)?))),
        Token::Open => {
            let inner = parse_or(tokens)?;
            (tokens.pop()? == Token::Close).then_some(inner)
        }
        Token::Tag(tag) => Some(Expr::Tag(tag)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn constraint(stem: &str, source: &str) -> Option<(String, bool)> {
        file_constraint(stem, source).map(|c| (c.key, c.host))
    }

    #[test]
    fn go_build_line_is_judged_against_the_default_host() {
        let src = "//go:build !gore2regex\n\npackage regexp\n";
        assert_eq!(
            constraint("regexp/stdlib_regex", src),
            Some(("!gore2regex".into(), true))
        );
        let src = "//go:build gore2regex\n\npackage regexp\n";
        assert_eq!(
            constraint("regexp/wasilibs_regex", src),
            Some(("gore2regex".into(), false))
        );
        let src = "// Copyright\n\n//go:build (linux && amd64) || darwin\n\npackage p\n";
        assert_eq!(
            constraint("p/x", src),
            Some(("linux&&amd64||darwin".into(), true))
        );
        let src = "//go:build windows && !cgo\npackage p\n";
        assert_eq!(
            constraint("p/x", src),
            Some(("windows&&!cgo".into(), false))
        );
    }

    #[test]
    fn filename_suffixes_constrain_like_tags() {
        assert_eq!(
            constraint("p/x_linux", "package p\n"),
            Some(("file:linux".into(), true))
        );
        assert_eq!(
            constraint("p/x_windows", "package p\n"),
            Some(("file:windows".into(), false))
        );
        assert_eq!(
            constraint("p/x_linux_arm64", "package p\n"),
            Some(("file:linux_arm64".into(), false))
        );
        assert_eq!(
            constraint("p/x_windows_test", "package p\n"),
            Some(("file:windows".into(), false))
        );
        assert_eq!(
            constraint("p/x_amd64", "package p\n"),
            Some(("file:amd64".into(), true))
        );
        // No underscore, or an unknown word: not a constraint.
        assert_eq!(constraint("p/linux", "package p\n"), None);
        assert_eq!(constraint("p/x_helper", "package p\n"), None);
    }

    #[test]
    fn name_and_header_both_apply() {
        let src = "//go:build !cgo\npackage p\n";
        assert_eq!(
            constraint("p/x_linux", src),
            Some(("file:linux&!cgo".into(), true))
        );
        assert_eq!(
            constraint("p/x_windows", src),
            Some(("file:windows&!cgo".into(), false))
        );
    }

    #[test]
    fn plus_build_lines_are_read_when_there_is_no_go_build_line() {
        let src = "// +build linux,amd64 darwin\n// +build !cgo\n\npackage p\n";
        assert_eq!(
            constraint("p/x", src),
            Some(("(linux&&amd64||darwin)&&!cgo".into(), true))
        );
        let src = "// +build ignore\n\npackage p\n";
        assert_eq!(constraint("p/x", src), Some(("ignore".into(), false)));
    }

    #[test]
    fn constraints_after_the_package_clause_are_ignored() {
        let src = "package p\n\n//go:build windows\n";
        assert_eq!(constraint("p/x", src), None);
    }

    #[test]
    fn release_tags_hold_up_to_the_modelled_toolchain() {
        let src = "//go:build go1.18\npackage p\n";
        assert_eq!(constraint("p/x", src), Some(("go1.18".into(), true)));
        let src = "//go:build !go1.18\npackage p\n";
        assert_eq!(constraint("p/x", src), Some(("!go1.18".into(), false)));
        let src = "//go:build go1.99\npackage p\n";
        assert_eq!(constraint("p/x", src), Some(("go1.99".into(), false)));
    }

    #[test]
    fn a_malformed_expression_is_no_constraint() {
        assert_eq!(constraint("p/x", "//go:build linux &&\npackage p\n"), None);
    }
}
