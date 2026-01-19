//! Git Co-Change Mining
//!
//! Mines git history to discover files that frequently change together.
//! This provides historical coupling evidence for impact analysis.
//!
//! ## Algorithm
//!
//! 1. Run `git log --numstat` to get file changes per commit
//! 2. For each commit, track pairs of files changed together
//! 3. Apply time decay to weight recent co-changes higher
//! 4. Compute confidence = weighted_co_changes / min(total_a, total_b)
//!
//! ## Performance
//!
//! - Limit to max 1000 commits by default
//! - Skip merge commits (>50 files changed)
//! - Time decay: exp(-age_days / 90.0)
//! - Target: <10s for 1000 commits

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;

/// Maximum files in a commit to analyze (skip large merges)
const MAX_FILES_PER_COMMIT: usize = 50;

/// Time decay half-life in days
const DECAY_HALF_LIFE_DAYS: f64 = 90.0;

/// Co-change entry for database insertion
#[derive(Debug, Clone)]
pub struct CoChangeEntry {
    pub file_a: String,
    pub file_b: String,
    pub co_change_count: i64,
    pub total_commits_a: i64,
    pub total_commits_b: i64,
    pub confidence: f64,
    pub last_commit_sha: Option<String>,
    pub last_commit_ts: Option<i64>,
}

/// Mine co-change patterns from git history
///
/// # Arguments
/// * `repo_root` - Path to git repository
/// * `max_commits` - Maximum number of commits to analyze (default: 1000)
/// * `since_days` - How many days back to look (default: 180)
///
/// # Returns
/// List of co-change entries sorted by confidence (descending)
pub fn mine_co_changes(
    repo_root: &Path,
    max_commits: usize,
    since_days: u32,
) -> Result<Vec<CoChangeEntry>> {
    // Run git log with numstat format
    let output = Command::new("git")
        .arg("log")
        .arg("--numstat")
        .arg("--format=%H %at")
        .arg(format!("--since={} days ago", since_days))
        .arg(format!("-n{}", max_commits))
        .arg("--no-merges")
        .current_dir(repo_root)
        .output()
        .context("Failed to run git log")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("git log failed: {}", stderr);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Parse git log output
    let commits = parse_git_log(&stdout)?;

    // Get current timestamp for time decay
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    // Count co-occurrences with time decay
    let mut co_occurrence_counts: HashMap<(String, String), f64> = HashMap::new();
    let mut total_changes: HashMap<String, i64> = HashMap::new();
    let mut last_commit_info: HashMap<(String, String), (String, i64)> = HashMap::new();

    for commit in &commits {
        // Skip commits with too many files (likely merges)
        if commit.files.len() > MAX_FILES_PER_COMMIT {
            continue;
        }

        // Calculate time decay weight
        let age_days = ((now - commit.timestamp) as f64) / 86400.0;
        let weight = (-age_days / DECAY_HALF_LIFE_DAYS).exp();

        // Count individual file changes
        for file in &commit.files {
            *total_changes.entry(file.clone()).or_insert(0) += 1;
        }

        // Count co-occurrences between file pairs
        for i in 0..commit.files.len() {
            for j in (i + 1)..commit.files.len() {
                let file_a = &commit.files[i];
                let file_b = &commit.files[j];

                // Create ordered pair (alphabetically)
                let pair = if file_a < file_b {
                    (file_a.clone(), file_b.clone())
                } else {
                    (file_b.clone(), file_a.clone())
                };

                // Add weighted co-occurrence
                *co_occurrence_counts.entry(pair.clone()).or_insert(0.0) += weight;

                // Track most recent commit for this pair
                last_commit_info
                    .entry(pair)
                    .or_insert((commit.sha.clone(), commit.timestamp));
            }
        }
    }

    // Build co-change entries
    let mut entries: Vec<CoChangeEntry> = Vec::new();

    for ((file_a, file_b), weighted_count) in co_occurrence_counts {
        let total_a = total_changes.get(&file_a).copied().unwrap_or(0);
        let total_b = total_changes.get(&file_b).copied().unwrap_or(0);

        // Confidence = weighted_co_changes / min(total_a, total_b)
        let min_changes = total_a.min(total_b).max(1); // Avoid division by zero
        let confidence = (weighted_count / min_changes as f64).min(1.0);

        let (last_sha, last_ts) = last_commit_info
            .get(&(file_a.clone(), file_b.clone()))
            .cloned()
            .unwrap_or_else(|| (String::new(), 0));

        entries.push(CoChangeEntry {
            file_a: file_a.clone(),
            file_b: file_b.clone(),
            co_change_count: weighted_count.round() as i64,
            total_commits_a: total_a,
            total_commits_b: total_b,
            confidence,
            last_commit_sha: if last_sha.is_empty() { None } else { Some(last_sha) },
            last_commit_ts: if last_ts > 0 { Some(last_ts) } else { None },
        });
    }

    // Sort by confidence descending
    entries.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    Ok(entries)
}

/// Parsed git commit information
#[derive(Debug)]
struct GitCommit {
    sha: String,
    timestamp: i64,
    files: Vec<String>,
}

/// Parse git log --numstat output
///
/// Format:
/// ```text
/// HASH TIMESTAMP
/// N1\tN2\tfile_path
/// N1\tN2\tfile_path
///
/// HASH TIMESTAMP
/// N1\tN2\tfile_path
/// ...
/// ```
fn parse_git_log(output: &str) -> Result<Vec<GitCommit>> {
    let mut commits = Vec::new();
    let mut current_commit: Option<GitCommit> = None;

    for line in output.lines() {
        let line = line.trim();

        if line.is_empty() {
            // Empty line separates commits (but only save if it has files)
            if let Some(ref commit) = current_commit {
                if !commit.files.is_empty() {
                    // This commit is complete, save it
                    commits.push(current_commit.take().unwrap());
                }
                // Otherwise keep it open to collect files
            }
            continue;
        }

        // Check if this is a commit header (hash + timestamp)
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 2 && parts[0].len() == 40 && parts[1].parse::<i64>().is_ok() {
            // Save previous commit if exists
            if let Some(commit) = current_commit.take() {
                if !commit.files.is_empty() {
                    commits.push(commit);
                }
            }

            // Start new commit
            current_commit = Some(GitCommit {
                sha: parts[0].to_string(),
                timestamp: parts[1].parse()?,
                files: Vec::new(),
            });
        } else if line.contains('\t') {
            // This is a file change line (numstat format)
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() >= 3 {
                let file_path = parts[2].to_string();
                // Add to current commit
                if let Some(ref mut commit) = current_commit {
                    commit.files.push(file_path);
                }
            }
        }
    }

    // Don't forget the last commit
    if let Some(commit) = current_commit {
        if !commit.files.is_empty() {
            commits.push(commit);
        }
    }

    Ok(commits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_git_log_basic() {
        // Use raw git log format - commit hash + timestamp on one line, then numstat lines
        let output = "\
abc1234567890123456789012345678901234567 1234567890

1\t2\tsrc/main.rs
3\t4\tsrc/lib.rs

def4567890123456789012345678901234567890 1234567900

5\t6\tsrc/main.rs
";

        let commits = parse_git_log(output).unwrap();
        assert_eq!(commits.len(), 2, "Expected 2 commits");
        assert_eq!(commits[0].sha, "abc1234567890123456789012345678901234567");
        assert_eq!(commits[0].files.len(), 2);
        assert_eq!(commits[1].sha, "def4567890123456789012345678901234567890");
        assert_eq!(commits[1].files.len(), 1);
    }

    #[test]
    fn test_time_decay() {
        // Recent change (1 day ago) should have high weight
        let age_days = 1.0;
        let weight = (-age_days / DECAY_HALF_LIFE_DAYS).exp();
        assert!(weight > 0.98);

        // Old change (180 days ago) should have lower weight
        let age_days = 180.0;
        let weight = (-age_days / DECAY_HALF_LIFE_DAYS).exp();
        assert!(weight < 0.2);
    }

    #[test]
    fn test_max_files_per_commit_reasonable() {
        assert!(MAX_FILES_PER_COMMIT >= 20);
        assert!(MAX_FILES_PER_COMMIT <= 100);
    }
}
