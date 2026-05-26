//! `lidx init` — index repo, install Claude Code hooks, update gitignore.

use anyhow::{Context, Result};
use std::path::Path;

const POST_READ_HOOK: &str = r#"#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || exit 0)"
INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
[ -z "$FILE_PATH" ] && exit 0
REL_PATH="${FILE_PATH#"$REPO_ROOT"/}"
CONTEXT=$(lidx context --repo "$REPO_ROOT" "$REL_PATH" 2>/dev/null) || exit 0
[ -z "$CONTEXT" ] && exit 0
jq -n --arg ctx "$CONTEXT" \
  '{"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":$ctx}}'
"#;

const PRE_EDIT_HOOK: &str = r#"#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || exit 0)"
INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
[ -z "$FILE_PATH" ] && exit 0
REL_PATH="${FILE_PATH#"$REPO_ROOT"/}"
CONTEXT=$(lidx context --repo "$REPO_ROOT" "$REL_PATH" 2>/dev/null) || exit 0
[ -z "$CONTEXT" ] && exit 0
jq -n --arg ctx "Cross-file context (callers may be affected by this edit):\n$CONTEXT" \
  '{"hookSpecificOutput":{"hookEventName":"PreToolUse","additionalContext":$ctx}}'
"#;

pub fn run_init(
    repo: &Path,
    db_path: &Path,
    skip_index: bool,
    skip_hooks: bool,
) -> Result<()> {
    let repo = std::fs::canonicalize(repo).context("canonicalize repo path")?;

    // 1. Index (unless --skip-index)
    if !skip_index {
        eprintln!("Indexing {}...", repo.display());
        let mut indexer = crate::indexer::Indexer::new(repo.clone(), db_path.to_path_buf())?;
        let stats = indexer.reindex()?;
        eprintln!("Indexed: {}", serde_json::to_string(&stats)?);
    }

    // 2. Install hooks (unless --skip-hooks)
    if !skip_hooks {
        install_hooks(&repo)?;
    }

    // 3. Update .gitignore
    update_gitignore(&repo)?;

    eprintln!("lidx init complete.");
    Ok(())
}

fn claude_home() -> Result<std::path::PathBuf> {
    let home = std::env::var("HOME").context("HOME not set")?;
    Ok(std::path::PathBuf::from(home).join(".claude"))
}

fn install_hooks(_repo: &Path) -> Result<()> {
    let claude_dir = claude_home()?;
    let hooks_dir = claude_dir.join("hooks");
    std::fs::create_dir_all(&hooks_dir).context("create ~/.claude/hooks dir")?;

    // Write hook scripts
    let post_read_path = hooks_dir.join("lidx-post-read.sh");
    std::fs::write(&post_read_path, POST_READ_HOOK).context("write post-read hook")?;
    make_executable(&post_read_path)?;
    eprintln!("Wrote {}", post_read_path.display());

    let pre_edit_path = hooks_dir.join("lidx-pre-edit.sh");
    std::fs::write(&pre_edit_path, PRE_EDIT_HOOK).context("write pre-edit hook")?;
    make_executable(&pre_edit_path)?;
    eprintln!("Wrote {}", pre_edit_path.display());

    // Update ~/.claude/settings.json
    update_settings()?;

    Ok(())
}

fn update_settings() -> Result<()> {
    let settings_path = claude_home()?.join("settings.json");
    let mut settings: serde_json::Value = if settings_path.exists() {
        let content = std::fs::read_to_string(&settings_path).context("read settings.json")?;
        serde_json::from_str(&content).unwrap_or_else(|_| serde_json::json!({}))
    } else {
        serde_json::json!({})
    };

    let obj = settings.as_object_mut().unwrap();

    // Ensure hooks object exists
    if !obj.contains_key("hooks") {
        obj.insert("hooks".to_string(), serde_json::json!({}));
    }
    let hooks = obj.get_mut("hooks").unwrap().as_object_mut().unwrap();

    // Add PostToolUse hook (append, don't overwrite)
    let post_hook = serde_json::json!({
        "matcher": "Read",
        "hooks": [{
            "type": "command",
            "command": "~/.claude/hooks/lidx-post-read.sh",
            "timeout": 10
        }]
    });
    merge_hook_entry(hooks, "PostToolUse", post_hook);

    // Add PreToolUse hook
    let pre_hook = serde_json::json!({
        "matcher": "Edit",
        "hooks": [{
            "type": "command",
            "command": "~/.claude/hooks/lidx-pre-edit.sh",
            "timeout": 10
        }]
    });
    merge_hook_entry(hooks, "PreToolUse", pre_hook);

    std::fs::write(
        &settings_path,
        serde_json::to_string_pretty(&settings)?,
    )
    .context("write settings.json")?;
    eprintln!("Updated {}", settings_path.display());

    Ok(())
}

fn merge_hook_entry(
    hooks: &mut serde_json::Map<String, serde_json::Value>,
    event: &str,
    entry: serde_json::Value,
) {
    let arr = hooks
        .entry(event.to_string())
        .or_insert_with(|| serde_json::json!([]));
    let arr = arr.as_array_mut().unwrap();

    // Check if our hook is already installed (by command path)
    let our_command = entry["hooks"][0]["command"].as_str().unwrap_or("");
    let already_installed = arr.iter().any(|existing| {
        existing["hooks"]
            .as_array()
            .map(|h| {
                h.iter()
                    .any(|hook| hook["command"].as_str() == Some(our_command))
            })
            .unwrap_or(false)
    });

    if !already_installed {
        arr.push(entry);
    }
}

fn update_gitignore(repo: &Path) -> Result<()> {
    let gitignore_path = repo.join(".gitignore");
    let content = if gitignore_path.exists() {
        std::fs::read_to_string(&gitignore_path).context("read .gitignore")?
    } else {
        String::new()
    };

    // Check if .lidx is already ignored
    let already_ignored = content.lines().any(|line| {
        let trimmed = line.trim();
        trimmed == ".lidx" || trimmed == ".lidx/" || trimmed == "/.lidx" || trimmed == "/.lidx/"
    });

    if !already_ignored {
        let mut new_content = content;
        if !new_content.ends_with('\n') && !new_content.is_empty() {
            new_content.push('\n');
        }
        new_content.push_str(".lidx\n");
        std::fs::write(&gitignore_path, new_content).context("write .gitignore")?;
        eprintln!("Added .lidx to .gitignore");
    }

    Ok(())
}

#[cfg(unix)]
fn make_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms)?;
    Ok(())
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) -> Result<()> {
    Ok(())
}
