use anyhow::{Context, Result};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

pub fn read_to_string(path: &Path) -> Result<String> {
    fs::read_to_string(path).with_context(|| format!("read {}", path.display()))
}

pub fn normalize_rel_path(repo_root: &Path, path: &Path) -> Result<String> {
    let rel = path.strip_prefix(repo_root).with_context(|| {
        format!(
            "strip prefix {} from {}",
            repo_root.display(),
            path.display()
        )
    })?;
    Ok(normalize_path(rel))
}

pub fn normalize_path(path: &Path) -> String {
    let mut parts = Vec::new();
    for comp in path.components() {
        match comp {
            Component::Normal(os) => parts.push(os.to_string_lossy().to_string()),
            Component::ParentDir => parts.push("..".to_string()),
            Component::CurDir => {}
            _ => {}
        }
    }
    if parts.is_empty() {
        ".".to_string()
    } else {
        parts.join("/")
    }
}

pub fn slice_lines(content: &str, start_line: i64, end_line: i64) -> String {
    if content.is_empty() {
        return String::new();
    }
    let lines: Vec<&str> = content.lines().collect();
    if lines.is_empty() {
        return String::new();
    }
    let start = (start_line.max(1) - 1) as usize;
    let mut end = end_line.max(1) as usize;
    if start >= lines.len() {
        return String::new();
    }
    if end > lines.len() {
        end = lines.len();
    }
    if end <= start {
        end = start + 1;
    }
    lines[start..end].join("\n")
}

pub fn slice_bytes(content: &str, start_byte: i64, end_byte: i64) -> Option<String> {
    if start_byte < 0 || end_byte <= start_byte {
        return None;
    }
    let len = content.len();
    if len == 0 {
        return Some(String::new());
    }
    let start = start_byte as usize;
    if start > len {
        return None;
    }
    let mut end = end_byte as usize;
    if end > len {
        end = len;
    }
    content.get(start..end).map(|value| value.to_string())
}

pub fn truncate_str_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

pub fn edge_evidence_snippet(
    source: &str,
    start_byte: i64,
    end_byte: i64,
    start_line: i64,
    end_line: i64,
) -> Option<String> {
    let raw = match slice_bytes(source, start_byte, end_byte) {
        Some(value) if !value.is_empty() => Some(value),
        _ => {
            let value = slice_lines(source, start_line, end_line);
            if value.is_empty() { None } else { Some(value) }
        }
    }?;

    let mut out = String::new();
    let mut last_space = false;
    for ch in raw.chars() {
        if ch.is_whitespace() {
            if !last_space {
                out.push(' ');
                last_space = true;
            }
        } else {
            out.push(ch);
            last_space = false;
        }
    }
    let trimmed = out.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(truncate_str_bytes(trimmed, 200))
    }
}

pub fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create dir {}", parent.display()))?;
    }
    Ok(())
}

pub fn to_abs_path(repo_root: &Path, rel: &str) -> PathBuf {
    repo_root.join(rel)
}

pub fn git_head_sha(repo_root: &Path) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8_lossy(&output.stdout);
    let trimmed = sha.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
