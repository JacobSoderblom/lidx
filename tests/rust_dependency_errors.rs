//! `.map_err(From::from)` as the tail of a function: the target is the
//! `impl From<X> for E` of the declared error type `E`, `X` being the error
//! the receiver returns, which for a dependency's method is read from the
//! dependency's sources on disk (never built, never downloaded).

mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::OnceLock;

/// A cargo home with one crate unpacked in its registry, set once for this
/// test process.
fn cargo_home() -> &'static PathBuf {
    static HOME: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    &HOME
        .get_or_init(|| {
            let tmp = tempfile::tempdir().unwrap();
            let home = tmp.path().to_path_buf();
            let krate = home.join("registry/src/index.crates.io-test/fakecsv-1.2.3");
            std::fs::create_dir_all(krate.join("src")).unwrap();
            std::fs::write(
                krate.join("Cargo.toml"),
                "[package]\nname = \"fakecsv\"\nversion = \"1.2.3\"\n",
            )
            .unwrap();
            std::fs::write(
                krate.join("src/lib.rs"),
                "use std::{io, result};\n\
                 pub enum Error { Bad }\n\
                 pub type Result<T> = result::Result<T, Error>;\n\
                 pub struct Writer<W>(W);\n\
                 impl<W: io::Write> Writer<W> {\n\
                     pub fn flush(&mut self) -> io::Result<()> { Ok(()) }\n\
                     pub fn record(&mut self) -> Result<()> { Ok(()) }\n\
                 }\n",
            )
            .unwrap();
            for (version, ret) in [("1.0.0", "io::Result<()>"), ("2.0.0", "Result<()>")] {
                let krate = home.join(format!("registry/src/index.crates.io-test/bumpy-{version}"));
                std::fs::create_dir_all(krate.join("src")).unwrap();
                std::fs::write(
                    krate.join("Cargo.toml"),
                    format!("[package]\nname = \"bumpy\"\nversion = \"{version}\"\n"),
                )
                .unwrap();
                std::fs::write(
                    krate.join("src/lib.rs"),
                    format!(
                        "use std::{{io, result}};\npub enum Error {{ Bad }}\n\
                         pub type Result<T> = result::Result<T, Error>;\n\
                         pub struct Thing;\nimpl Thing {{\n    pub fn go(&mut self) -> {ret} {{ Ok(()) }}\n}}\n"
                    ),
                )
                .unwrap();
            }
            // SAFETY: set once, before any thread of this test binary reads it
            // through the indexer, to the same value every time.
            unsafe { std::env::set_var("CARGO_HOME", &home) };
            (tmp, home)
        })
        .1
}

fn lock(bumpy: &str) -> String {
    format!(
        "{LOCK}\n[[package]]\nname = \"bumpy\"\nversion = \"{bumpy}\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\n\n[[package]]\nname = \"late\"\nversion = \"7.7.7\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\n"
    )
}

const LOCK: &str = "version = 3\n\n[[package]]\nname = \"fakecsv\"\nversion = \"1.2.3\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\n\n[[package]]\nname = \"ghost\"\nversion = \"9.9.9\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\n";

fn files() -> Vec<(&'static str, String)> {
    vec![
        (
            "Cargo.toml",
            "[package]\nname = \"app\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\nfakecsv = \"1\"\nghost = \"9\"\nbumpy = \"1\"\nlate = \"7\"\n".into(),
        ),
        ("Cargo.lock", lock("1.0.0")),
        (
            "src/lib.rs",
            r#"pub mod config;
pub mod cmd;

pub enum App {
    Io(std::io::Error),
    Csv(fakecsv::Error),
    Text(String),
    Bumpy(bumpy::Error),
}

impl From<std::io::Error> for App {
    fn from(e: std::io::Error) -> Self {
        App::Io(e)
    }
}

impl From<fakecsv::Error> for App {
    fn from(e: fakecsv::Error) -> Self {
        App::Csv(e)
    }
}

impl From<bumpy::Error> for App {
    fn from(e: bumpy::Error) -> Self {
        App::Bumpy(e)
    }
}

impl From<String> for App {
    fn from(e: String) -> Self {
        App::Text(e)
    }
}

pub type AppResult<T> = Result<T, App>;
"#
            .into(),
        ),
        (
            "src/config.rs",
            r#"use std::io;

pub struct Config;

impl Config {
    pub fn new() -> Config {
        Config
    }

    pub fn writer(&self) -> io::Result<fakecsv::Writer<Vec<u8>>> {
        unimplemented!()
    }

    pub fn thing(&self) -> io::Result<bumpy::Thing> {
        unimplemented!()
    }

    pub fn late(&self) -> io::Result<late::Thing> {
        unimplemented!()
    }

    pub fn ghost(&self) -> io::Result<ghost::Thing> {
        unimplemented!()
    }
}

pub fn texts() -> Result<u8, String> {
    Ok(1)
}
"#
            .into(),
        ),
        (
            "src/cmd.rs",
            r#"use crate::config::{self, Config};
use crate::AppResult;

pub fn flushes() -> AppResult<()> {
    let mut w = Config::new().writer()?;
    w.flush().map_err(From::from)
}

pub fn records() -> AppResult<()> {
    let mut w = Config::new().writer()?;
    w.record().map_err(From::from)
}

pub fn texts() -> AppResult<u8> {
    config::texts().map_err(From::from)
}

pub fn from_an_absent_crate() -> AppResult<()> {
    let mut g = Config::new().ghost()?;
    g.go().map_err(From::from)
}

pub fn bumps() -> AppResult<()> {
    let mut t = Config::new().thing()?;
    t.go().map_err(From::from)
}

pub fn arrives_later() -> AppResult<()> {
    let mut t = Config::new().late()?;
    t.go().map_err(From::from)
}

pub fn not_the_tail() -> AppResult<u8> {
    let r = config::texts().map_err(From::from);
    r
}
"#
            .into(),
        ),
    ]
}

fn bound(indexer: &lidx::indexer::Indexer, caller: &str) -> BTreeSet<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    conn.prepare(
        "SELECT t.signature FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
         JOIN symbols t ON t.id = e.target_symbol_id
         WHERE e.graph_version = ? AND e.kind = 'REFERENCES' AND s.qualname = ?",
    )
    .unwrap()
    .query_map(rusqlite::params![gv, caller], |r| r.get::<_, String>(0))
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

#[test]
fn bare_from_is_the_impl_of_the_declared_error_and_the_receivers_error() {
    cargo_home();
    let owned = files();
    let refs: Vec<(&str, &str)> = owned.iter().map(|(p, c)| (*p, c.as_str())).collect();
    let (_tmp, root, db_path) = common::index_repo("lidx-dep-errors-", &refs);
    let indexer = lidx::indexer::Indexer::new(root, db_path).unwrap();
    let only = |caller: &str, param: &str| {
        let got = bound(&indexer, caller);
        assert_eq!(got.len(), 1, "{caller}: {got:?}");
        assert!(
            got.iter().next().unwrap().contains(param),
            "{caller}: {got:?}"
        );
    };
    // `io::Result<()>` of a method of the dependency.
    only("crate::cmd::flushes", "(e: std::io::Error)");
    // The dependency's own `Result<()>` alias.
    only("crate::cmd::records", "(e: fakecsv::Error)");
    // A repo function's `Result<u8, String>`.
    only("crate::cmd::texts", "(e: String)");
    // Sources that are not on disk: ambiguous, never a guess.
    assert!(bound(&indexer, "crate::cmd::from_an_absent_crate").is_empty());
    // Not the tail of the function: the target error type is not the declared one.
    assert!(bound(&indexer, "crate::cmd::not_the_tail").is_empty());
}

/// `(caller, parameter text)` of every bound `REFERENCES` edge of the current version.
fn all_bound(indexer: &lidx::indexer::Indexer) -> BTreeSet<(String, String)> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    conn.prepare(
        "SELECT s.qualname, t.signature FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
         JOIN symbols t ON t.id = e.target_symbol_id
         WHERE e.graph_version = ? AND e.kind = 'REFERENCES'",
    )
    .unwrap()
    .query_map([gv], |r| Ok((r.get(0)?, r.get(1)?)))
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

#[test]
fn a_lock_only_change_rejudges_the_dependency_bound_edges() {
    let home = cargo_home().clone();
    let owned = files();
    let refs: Vec<(&str, &str)> = owned.iter().map(|(p, c)| (*p, c.as_str())).collect();
    let (_tmp, root, db_path) = common::index_repo("lidx-dep-lock-", &refs);
    let mut indexer = lidx::indexer::Indexer::new(root.clone(), db_path).unwrap();
    let bumps = |i: &lidx::indexer::Indexer| {
        let got = bound(i, "crate::cmd::bumps");
        assert_eq!(got.len(), 1, "{got:?}");
        got.into_iter().next().unwrap()
    };
    assert!(bumps(&indexer).contains("(e: std::io::Error)"));

    // `cargo update`: only Cargo.lock changes, to the version whose `go`
    // returns the crate's own error.
    std::fs::write(root.join("Cargo.lock"), lock("2.0.0")).unwrap();
    indexer.sync_rel_paths(&["Cargo.lock".to_string()]).unwrap();
    assert!(bumps(&indexer).contains("(e: bumpy::Error)"));

    let mut after = files();
    for (path, content) in after.iter_mut() {
        if *path == "Cargo.lock" {
            *content = lock("2.0.0");
        }
    }
    let after_refs: Vec<(&str, &str)> = after.iter().map(|(p, c)| (*p, c.as_str())).collect();
    let (_f, froot, fdb) = common::index_repo("lidx-dep-lock-fresh-", &after_refs);
    let fresh = lidx::indexer::Indexer::new(froot, fdb).unwrap();
    assert_eq!(all_bound(&indexer), all_bound(&fresh));
    let _ = home;
}

#[test]
fn sources_that_appear_later_are_seen_by_a_running_indexer() {
    let home = cargo_home().clone();
    let owned = files();
    let refs: Vec<(&str, &str)> = owned.iter().map(|(p, c)| (*p, c.as_str())).collect();
    let (_tmp, root, db_path) = common::index_repo("lidx-dep-late-", &refs);
    let mut indexer = lidx::indexer::Indexer::new(root.clone(), db_path).unwrap();
    // `late 7.7.7` is locked but not unpacked: nothing is bound, and that miss
    // is not remembered.
    assert!(bound(&indexer, "crate::cmd::arrives_later").is_empty());
    let krate = home.join("registry/src/index.crates.io-test/late-7.7.7");
    std::fs::create_dir_all(krate.join("src")).unwrap();
    std::fs::write(
        krate.join("Cargo.toml"),
        "[package]\nname = \"late\"\nversion = \"7.7.7\"\n",
    )
    .unwrap();
    std::fs::write(
        krate.join("src/lib.rs"),
        "use std::io;\npub struct Thing;\nimpl Thing {\n    pub fn go(&mut self) -> io::Result<()> { Ok(()) }\n}\n",
    )
    .unwrap();
    // Any later sync judges it again.
    std::fs::write(
        root.join("src/cmd.rs"),
        format!(
            "{}\n// touched\n",
            owned.iter().find(|(p, _)| *p == "src/cmd.rs").unwrap().1
        ),
    )
    .unwrap();
    indexer.sync_rel_paths(&["src/cmd.rs".to_string()]).unwrap();
    let got = bound(&indexer, "crate::cmd::arrives_later");
    assert_eq!(got.len(), 1, "{got:?}");
    assert!(got.iter().next().unwrap().contains("(e: std::io::Error)"));
}
