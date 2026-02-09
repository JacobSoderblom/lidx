use anyhow::{Context, Result, bail};
use blake3::Hasher;
use ignore::WalkBuilder;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct ScannedFile {
    pub rel_path: String,
    pub abs_path: PathBuf,
    pub hash: String,
    pub size: i64,
    pub modified: i64,
    pub language: String,
}

#[derive(Debug, Clone)]
pub struct LanguageSpec {
    pub name: &'static str,
    pub extensions: &'static [&'static str],
}

#[derive(Debug, Clone)]
pub struct LanguageFilter {
    pub name: &'static str,
    pub languages: &'static [&'static str],
}

#[derive(Debug, Clone, Copy)]
pub struct ScanOptions {
    pub no_ignore: bool,
}

impl ScanOptions {
    pub fn new(no_ignore: bool) -> Self {
        Self { no_ignore }
    }
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self { no_ignore: false }
    }
}

static LANGUAGE_SPECS: &[LanguageSpec] = &[
    LanguageSpec {
        name: "python",
        extensions: &["py", "pyi"],
    },
    LanguageSpec {
        name: "rust",
        extensions: &["rs"],
    },
    LanguageSpec {
        name: "javascript",
        extensions: &["js", "jsx", "mjs", "cjs"],
    },
    LanguageSpec {
        name: "typescript",
        extensions: &["ts", "mts", "cts"],
    },
    LanguageSpec {
        name: "tsx",
        extensions: &["tsx"],
    },
    LanguageSpec {
        name: "csharp",
        extensions: &["cs", "csx"],
    },
    LanguageSpec {
        name: "sql",
        extensions: &["sql"],
    },
    LanguageSpec {
        name: "postgres",
        extensions: &["psql", "pgsql"],
    },
    LanguageSpec {
        name: "tsql",
        extensions: &["tsql"],
    },
    LanguageSpec {
        name: "markdown",
        extensions: &["md", "markdown", "mdx"],
    },
    LanguageSpec {
        name: "proto",
        extensions: &["proto"],
    },
    LanguageSpec {
        name: "go",
        extensions: &["go"],
    },
    LanguageSpec {
        name: "lua",
        extensions: &["lua"],
    },
    LanguageSpec {
        name: "yaml",
        extensions: &["yaml", "yml"],
    },
    LanguageSpec {
        name: "bicep",
        extensions: &["bicep", "bicepparam"],
    },
];

static LANGUAGE_FILTERS: &[LanguageFilter] = &[
    LanguageFilter {
        name: "python",
        languages: &["python"],
    },
    LanguageFilter {
        name: "py",
        languages: &["python"],
    },
    LanguageFilter {
        name: "rust",
        languages: &["rust"],
    },
    LanguageFilter {
        name: "rs",
        languages: &["rust"],
    },
    LanguageFilter {
        name: "javascript",
        languages: &["javascript"],
    },
    LanguageFilter {
        name: "js",
        languages: &["javascript"],
    },
    LanguageFilter {
        name: "node",
        languages: &["javascript"],
    },
    LanguageFilter {
        name: "typescript",
        languages: &["typescript", "tsx"],
    },
    LanguageFilter {
        name: "ts",
        languages: &["typescript", "tsx"],
    },
    LanguageFilter {
        name: "tsx",
        languages: &["tsx"],
    },
    LanguageFilter {
        name: "csharp",
        languages: &["csharp"],
    },
    LanguageFilter {
        name: "c#",
        languages: &["csharp"],
    },
    LanguageFilter {
        name: "cs",
        languages: &["csharp"],
    },
    LanguageFilter {
        name: "dotnet",
        languages: &["csharp"],
    },
    LanguageFilter {
        name: "dot-net",
        languages: &["csharp"],
    },
    LanguageFilter {
        name: "sql",
        languages: &["sql", "postgres", "tsql"],
    },
    LanguageFilter {
        name: "markdown",
        languages: &["markdown"],
    },
    LanguageFilter {
        name: "md",
        languages: &["markdown"],
    },
    LanguageFilter {
        name: "mdx",
        languages: &["markdown"],
    },
    LanguageFilter {
        name: "postgres",
        languages: &["postgres"],
    },
    LanguageFilter {
        name: "postgresql",
        languages: &["postgres"],
    },
    LanguageFilter {
        name: "psql",
        languages: &["postgres"],
    },
    LanguageFilter {
        name: "pgsql",
        languages: &["postgres"],
    },
    LanguageFilter {
        name: "tsql",
        languages: &["tsql"],
    },
    LanguageFilter {
        name: "mssql",
        languages: &["tsql"],
    },
    LanguageFilter {
        name: "sqlserver",
        languages: &["tsql"],
    },
    LanguageFilter {
        name: "sql-server",
        languages: &["tsql"],
    },
    LanguageFilter {
        name: "proto",
        languages: &["proto"],
    },
    LanguageFilter {
        name: "protobuf",
        languages: &["proto"],
    },
    LanguageFilter {
        name: "grpc",
        languages: &["proto"],
    },
    LanguageFilter {
        name: "go",
        languages: &["go"],
    },
    LanguageFilter {
        name: "golang",
        languages: &["go"],
    },
    LanguageFilter {
        name: "lua",
        languages: &["lua"],
    },
    LanguageFilter {
        name: "yaml",
        languages: &["yaml"],
    },
    LanguageFilter {
        name: "yml",
        languages: &["yaml"],
    },
    LanguageFilter {
        name: "k8s",
        languages: &["yaml"],
    },
    LanguageFilter {
        name: "kubernetes",
        languages: &["yaml"],
    },
    LanguageFilter {
        name: "bicep",
        languages: &["bicep"],
    },
    LanguageFilter {
        name: "arm",
        languages: &["bicep"],
    },
    LanguageFilter {
        name: "azure",
        languages: &["bicep"],
    },
];

pub fn language_specs() -> &'static [LanguageSpec] {
    LANGUAGE_SPECS
}

pub fn language_filters() -> &'static [LanguageFilter] {
    LANGUAGE_FILTERS
}

pub fn scan_repo(repo_root: &Path) -> Result<Vec<ScannedFile>> {
    scan_repo_with_options(repo_root, ScanOptions::default())
}

pub fn scan_repo_with_options(repo_root: &Path, options: ScanOptions) -> Result<Vec<ScannedFile>> {
    let mut files = Vec::new();
    let mut builder = WalkBuilder::new(repo_root);
    if options.no_ignore {
        builder
            .ignore(false)
            .git_ignore(false)
            .git_global(false)
            .git_exclude(false)
            .parents(false);
    } else {
        builder
            .ignore(true)
            .git_ignore(true)
            .git_global(true)
            .git_exclude(true)
            .parents(true)
            .require_git(false);
    }
    let walker = builder
        .hidden(false)
        .filter_entry(|entry| !is_ignored_entry(entry))
        .build();

    for entry in walker {
        let entry = match entry {
            Ok(value) => value,
            Err(err) => {
                eprintln!("walk error: {err}");
                continue;
            }
        };
        if !entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
            continue;
        }
        let path = entry.path();
        let language = match detect_language(path) {
            Some(value) => value,
            None => continue,
        };
        let rel_path = crate::util::normalize_rel_path(repo_root, path)?;
        let metadata = fs::metadata(path)?;
        let modified = metadata
            .modified()
            .ok()
            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let size = metadata.len() as i64;
        let hash = hash_file(path).with_context(|| format!("hash {}", path.display()))?;
        files.push(ScannedFile {
            rel_path,
            abs_path: path.to_path_buf(),
            hash,
            size,
            modified,
            language: language.to_string(),
        });
    }
    files.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    Ok(files)
}

fn is_ignored_entry(entry: &ignore::DirEntry) -> bool {
    match entry.file_name() {
        name if name == OsStr::new(".lidx") => true,
        name if name == OsStr::new(".git") => true,
        _ => false,
    }
}

pub fn scan_path(repo_root: &Path, path: &Path) -> Result<Option<ScannedFile>> {
    if !path.is_file() {
        return Ok(None);
    }
    let language = match detect_language(path) {
        Some(value) => value,
        None => return Ok(None),
    };
    let rel_path = match crate::util::normalize_rel_path(repo_root, path) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let metadata = fs::metadata(path)?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let size = metadata.len() as i64;
    let hash = hash_file(path).with_context(|| format!("hash {}", path.display()))?;
    Ok(Some(ScannedFile {
        rel_path,
        abs_path: path.to_path_buf(),
        hash,
        size,
        modified,
        language: language.to_string(),
    }))
}

fn detect_language(path: &Path) -> Option<&'static str> {
    let ext = path.extension().and_then(|ext| ext.to_str())?;
    for spec in LANGUAGE_SPECS {
        if spec.extensions.iter().any(|candidate| *candidate == ext) {
            return Some(spec.name);
        }
    }
    None
}

pub fn language_for_path(path: &Path) -> Option<&'static str> {
    detect_language(path)
}

pub fn normalize_language_filter(raw: Option<&[String]>) -> Result<Option<Vec<String>>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let mut expanded = Vec::new();
    for lang in raw {
        let key = lang.trim().to_ascii_lowercase();
        if key.is_empty() {
            continue;
        }
        let values = match expand_language(&key) {
            Some(value) => value,
            None => bail!("unknown language filter: {lang}"),
        };
        expanded.extend(values.iter().copied());
    }
    if expanded.is_empty() {
        return Ok(None);
    }
    expanded.sort_unstable();
    expanded.dedup();
    Ok(Some(expanded.into_iter().map(|s| s.to_string()).collect()))
}

pub fn extensions_for_languages(languages: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for language in languages {
        for ext in extensions_for_language(language) {
            if !out.iter().any(|value| value == ext) {
                out.push(ext.to_string());
            }
        }
    }
    out
}

fn expand_language(name: &str) -> Option<&'static [&'static str]> {
    LANGUAGE_FILTERS
        .iter()
        .find(|entry| entry.name == name)
        .map(|entry| entry.languages)
}

fn extensions_for_language(language: &str) -> &'static [&'static str] {
    LANGUAGE_SPECS
        .iter()
        .find(|spec| spec.name == language)
        .map(|spec| spec.extensions)
        .unwrap_or(&[])
}

fn hash_file(path: &Path) -> Result<String> {
    let data = fs::read(path)?;
    let mut hasher = Hasher::new();
    hasher.update(&data);
    Ok(hasher.finalize().to_hex().to_string())
}
