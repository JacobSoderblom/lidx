//! Python package-root detection (issue #202): the directory a file's module
//! name is relative to, i.e. what Python would have on `sys.path` for it.
//! Every lookup is memoized per directory; `clear` drops the memo between
//! runs, since a marker file may have changed.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

/// Files that mark a project root, or whose edits change a declared root.
pub const PROJECT_MARKERS: [&str; 3] = ["pyproject.toml", "setup.py", "setup.cfg"];

/// Whether an edit to `rel_path` can change some Python file's import root.
pub fn is_layout_marker(rel_path: &str) -> bool {
    let name = Path::new(rel_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");
    name == "__init__.py" || PROJECT_MARKERS.contains(&name)
}

#[derive(Default)]
struct ProjectRoots {
    /// Roots the build config declares (`package-dir`, `where`, poetry
    /// `from`), authoritative for files under them.
    declared: Vec<PathBuf>,
    /// Structurally detected source containers (see `containers`).
    containers: Vec<PathBuf>,
}

pub struct PyLayout {
    repo_root: PathBuf,
    projects: RefCell<HashMap<PathBuf, Option<PathBuf>>>,
    project_roots: RefCell<HashMap<PathBuf, Rc<ProjectRoots>>>,
    import_roots: RefCell<HashMap<PathBuf, PathBuf>>,
    search_roots: RefCell<HashMap<PathBuf, Rc<Vec<PathBuf>>>>,
}

impl PyLayout {
    pub fn new(repo_root: PathBuf) -> Self {
        Self {
            repo_root,
            projects: Default::default(),
            project_roots: Default::default(),
            import_roots: Default::default(),
            search_roots: Default::default(),
        }
    }

    pub fn clear(&self) {
        self.projects.borrow_mut().clear();
        self.project_roots.borrow_mut().clear();
        self.import_roots.borrow_mut().clear();
        self.search_roots.borrow_mut().clear();
    }

    fn has_init(&self, dir: &Path) -> bool {
        self.repo_root.join(dir).join("__init__.py").is_file()
    }

    /// Nearest ancestor of `dir` (inclusive) holding a project-root marker.
    fn project_of(&self, dir: &Path) -> Option<PathBuf> {
        if let Some(hit) = self.projects.borrow().get(dir) {
            return hit.clone();
        }
        let found = if PROJECT_MARKERS
            .iter()
            .any(|m| self.repo_root.join(dir).join(m).is_file())
        {
            Some(dir.to_path_buf())
        } else {
            dir.parent().and_then(|parent| self.project_of(parent))
        };
        self.projects
            .borrow_mut()
            .insert(dir.to_path_buf(), found.clone());
        found
    }

    fn roots_of(&self, project: &Path) -> Rc<ProjectRoots> {
        if let Some(hit) = self.project_roots.borrow().get(project) {
            return hit.clone();
        }
        let roots = Rc::new(ProjectRoots {
            declared: declared_roots(&self.repo_root, project),
            containers: self.containers(project),
        });
        self.project_roots
            .borrow_mut()
            .insert(project.to_path_buf(), roots.clone());
        roots
    }

    /// Source containers of `project`: direct child directories without an
    /// `__init__.py` that hold a regular package. When there is none, a lone
    /// child directory with no `.py` of its own that holds Python files in
    /// subdirectories (a pure namespace layout such as `src/ns/a.py`). Found
    /// structurally, never by name.
    fn containers(&self, project: &Path) -> Vec<PathBuf> {
        let children: Vec<PathBuf> = subdirs(&self.repo_root.join(project))
            .into_iter()
            .map(|name| project.join(name))
            .filter(|dir| !self.has_init(dir))
            .collect();
        let regular: Vec<PathBuf> = children
            .iter()
            .filter(|dir| {
                subdirs(&self.repo_root.join(dir))
                    .iter()
                    .any(|c| self.has_init(&dir.join(c)))
            })
            .cloned()
            .collect();
        if !regular.is_empty() {
            return regular;
        }
        let namespace: Vec<PathBuf> = children
            .into_iter()
            .filter(|dir| {
                let abs = self.repo_root.join(dir);
                !has_py_file(&abs) && subdirs(&abs).iter().any(|c| has_py_tree(&abs.join(c), 3))
            })
            .collect();
        if namespace.len() == 1 {
            namespace
        } else {
            Vec::new()
        }
    }

    /// `project`, or the declared root / source container `dir` lies under.
    fn project_import_root(&self, project: &Path, dir: &Path) -> PathBuf {
        let roots = self.roots_of(project);
        roots
            .containers
            .iter()
            .find(|c| dir.starts_with(c))
            .cloned()
            .unwrap_or_else(|| project.to_path_buf())
    }

    /// The directory the module name of the file at `rel_path` is relative
    /// to. A declared root the file lies under wins. Otherwise: for a file in
    /// a regular package, the parent of the topmost contiguous `__init__.py`
    /// directory, widened to the project root / source container when
    /// namespace directories sit in between; for any other file, the project
    /// root (`pyproject.toml`/`setup.py`/`setup.cfg`) or source container,
    /// else the repo root (a loose script keeps its path).
    pub fn import_root(&self, rel_path: &str) -> PathBuf {
        let dir = Path::new(rel_path).parent().unwrap_or(Path::new(""));
        if let Some(hit) = self.import_roots.borrow().get(dir) {
            return hit.clone();
        }
        let root = self.compute_import_root(dir);
        self.import_roots
            .borrow_mut()
            .insert(dir.to_path_buf(), root.clone());
        root
    }

    fn compute_import_root(&self, dir: &Path) -> PathBuf {
        let project = self.project_of(dir);
        if let Some(p) = &project
            && let Some(declared) = self
                .roots_of(p)
                .declared
                .iter()
                .filter(|d| dir.starts_with(d))
                .max_by_key(|d| d.components().count())
        {
            return declared.clone();
        }
        if self.has_init(dir) {
            let mut top = dir.to_path_buf();
            while let Some(parent) = top.parent()
                && self.has_init(parent)
            {
                top = parent.to_path_buf();
            }
            let root = top.parent().unwrap_or(Path::new("")).to_path_buf();
            match project {
                Some(p)
                    if root.starts_with(&p)
                        && root.components().count() > p.components().count() + 1 =>
                {
                    self.project_import_root(&p, dir)
                }
                _ => root,
            }
        } else {
            match project {
                Some(p) => self.project_import_root(&p, dir),
                None => PathBuf::new(),
            }
        }
    }

    /// `rel_path` with its import root stripped, ready for dotted-name
    /// conversion.
    pub fn strip_root(&self, rel_path: &str) -> String {
        let root = self.import_root(rel_path);
        match Path::new(rel_path).strip_prefix(&root) {
            Ok(rest) => rest
                .components()
                .filter_map(|c| c.as_os_str().to_str())
                .collect::<Vec<_>>()
                .join("/"),
            Err(_) => rel_path.to_string(),
        }
    }

    /// Directories an absolute import from `file_rel_path` may be rooted at:
    /// the file's own import root, its project's declared roots, source
    /// containers and root, then the repo root.
    pub fn search_roots(&self, file_rel_path: &str) -> Rc<Vec<PathBuf>> {
        let dir = Path::new(file_rel_path).parent().unwrap_or(Path::new(""));
        if let Some(hit) = self.search_roots.borrow().get(dir) {
            return hit.clone();
        }
        let mut roots = vec![self.import_root(file_rel_path)];
        if let Some(project) = self.project_of(dir) {
            let info = self.roots_of(&project);
            roots.extend(info.declared.iter().cloned());
            roots.extend(info.containers.iter().cloned());
            roots.push(project);
        }
        roots.push(PathBuf::new());
        let mut seen = std::collections::HashSet::new();
        roots.retain(|r| seen.insert(r.clone()));
        let roots = Rc::new(roots);
        self.search_roots
            .borrow_mut()
            .insert(dir.to_path_buf(), roots.clone());
        roots
    }
}

fn subdirs(abs: &Path) -> Vec<std::ffi::OsString> {
    let mut out: Vec<_> = std::fs::read_dir(abs)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name())
        .filter(|n| !n.to_string_lossy().starts_with('.'))
        .collect();
    out.sort();
    out
}

fn has_py_file(abs: &Path) -> bool {
    std::fs::read_dir(abs)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| e.path().extension().is_some_and(|x| x == "py"))
}

fn has_py_tree(abs: &Path, depth: usize) -> bool {
    has_py_file(abs)
        || (depth > 0
            && subdirs(abs)
                .iter()
                .any(|c| has_py_tree(&abs.join(c), depth - 1)))
}

/// Source roots the project at `project` declares in its build config.
fn declared_roots(repo_root: &Path, project: &Path) -> Vec<PathBuf> {
    let read = |name: &str| std::fs::read_to_string(repo_root.join(project).join(name)).ok();
    let mut raw = Vec::new();
    if let Some(text) = read("pyproject.toml") {
        raw.extend(pyproject_roots(&text));
    }
    if let Some(text) = read("setup.cfg") {
        raw.extend(setup_cfg_roots(&text));
    }
    if let Some(text) = read("setup.py") {
        raw.extend(setup_py_roots(&text));
    }
    let mut out: Vec<PathBuf> = Vec::new();
    for dir in raw {
        let dir = dir.trim().trim_start_matches("./").trim_end_matches('/');
        if dir.is_empty() || dir == "." {
            continue;
        }
        let root = project.join(dir);
        if !out.contains(&root) {
            out.push(root);
        }
    }
    out
}

/// Quoted string literals (`"x"` or `'x'`) in `s`, in order.
fn quoted_strings(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '"' || c == '\'' {
            let lit: String = chars.by_ref().take_while(|&d| d != c).collect();
            out.push(lit);
        }
    }
    out
}

/// The source root a `package_dir` entry `pkg = dir` implies: `dir` minus
/// the package's own trailing path (`real = lib/real` roots at `lib`); the
/// empty package maps the root directly.
fn package_dir_root(pkg: &str, dir: &str) -> String {
    let pkg = pkg.trim().trim_matches(|c| c == '"' || c == '\'');
    let dir = dir.trim().trim_matches(|c| c == '"' || c == '\'');
    let suffix = format!("/{}", pkg.replace('.', "/"));
    if pkg.is_empty() {
        dir.to_string()
    } else if let Some(root) = dir.strip_suffix(&suffix) {
        root.to_string()
    } else if dir == pkg {
        String::new()
    } else {
        dir.to_string()
    }
}

/// `[tool.setuptools] package-dir`, `[tool.setuptools.packages.find]
/// where`, and poetry `packages = [{include, from}]`.
fn pyproject_roots(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut section = String::new();
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.starts_with('[') {
            section = line
                .trim_matches(|c| c == '[' || c == ']')
                .trim()
                .to_string();
            continue;
        }
        let key = line
            .split_once('=')
            .map(|(k, _)| k.trim().trim_matches(|c| c == '"' || c == '\''));
        let value = line.split_once('=').map_or("", |(_, v)| v);
        match (section.as_str(), key) {
            ("tool.setuptools", Some("package-dir")) => {
                for entry in value
                    .trim()
                    .trim_matches(|c| c == '{' || c == '}')
                    .split(',')
                {
                    if let Some((k, v)) = entry.split_once('=') {
                        out.push(package_dir_root(k, v));
                    }
                }
            }
            ("tool.setuptools.package-dir", Some(k)) => out.push(package_dir_root(k, value)),
            ("tool.setuptools.packages.find", Some("where")) => out.extend(quoted_strings(value)),
            (s, _) if s.starts_with("tool.poetry") => {
                for part in line.split([',', '{']) {
                    if let Some((k, v)) = part.split_once('=')
                        && k.trim() == "from"
                    {
                        out.extend(quoted_strings(v));
                    }
                }
            }
            _ => {}
        }
    }
    out
}

/// `[options] package_dir` and `[options.packages.find] where`.
fn setup_cfg_roots(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut section = String::new();
    let mut key = String::new();
    for raw in text.lines() {
        let line = raw.split(['#', ';']).next().unwrap_or("");
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let continuation = line.starts_with([' ', '\t']);
        if trimmed.starts_with('[') {
            section = trimmed.trim_matches(|c| c == '[' || c == ']').to_string();
            key.clear();
            continue;
        }
        let value = if continuation {
            trimmed
        } else if let Some((k, v)) = trimmed.split_once('=') {
            key = k.trim().to_string();
            v.trim()
        } else {
            continue;
        };
        match (section.as_str(), key.as_str()) {
            ("options", "package_dir") => {
                let (k, v) = if continuation {
                    trimmed.split_once('=').unwrap_or(("", trimmed))
                } else {
                    // `package_dir = =src` / `package_dir = pkg = lib/pkg`
                    value.split_once('=').unwrap_or(("", value))
                };
                if !v.trim().is_empty() {
                    out.push(package_dir_root(k, v));
                }
            }
            ("options.packages.find", "where") => out.extend(
                value
                    .split([',', ' '])
                    .filter(|s| !s.is_empty())
                    .map(String::from),
            ),
            _ => {}
        }
    }
    out
}

/// `package_dir={'': 'src'}` in `setup.py`.
fn setup_py_roots(text: &str) -> Vec<String> {
    let Some(at) = text.find("package_dir") else {
        return Vec::new();
    };
    let rest = &text[at..];
    let end = rest.find('}').unwrap_or(rest.len());
    quoted_strings(&rest[..end])
        .chunks_exact(2)
        .map(|kv| package_dir_root(&kv[0], &kv[1]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_declared_roots() {
        assert_eq!(
            pyproject_roots("[tool.setuptools]\npackage-dir = {\"\" = \"lib\"}\n"),
            ["lib"]
        );
        assert_eq!(
            pyproject_roots("[tool.setuptools.package-dir]\nreal = \"lib/real\"\n"),
            ["lib"]
        );
        assert_eq!(
            pyproject_roots("[tool.setuptools.packages.find]\nwhere = [\"src\"]\n"),
            ["src"]
        );
        assert_eq!(
            pyproject_roots("[tool.poetry]\npackages = [{include = \"x\", from = \"src\"}]\n"),
            ["src"]
        );
        assert_eq!(
            setup_cfg_roots(
                "[options]\npackage_dir =\n    =lib\n[options.packages.find]\nwhere = src\n"
            ),
            ["lib", "src"]
        );
        assert_eq!(setup_py_roots("setup(package_dir={'': 'lib'})"), ["lib"]);
    }
}
