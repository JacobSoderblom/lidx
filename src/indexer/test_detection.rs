//! Test symbol and file detection
//!
//! This module provides functions to detect whether symbols and files are tests
//! based on language-specific conventions and naming patterns.

use crate::model::Symbol;
use std::path::Path;

/// Detects if a symbol is a test based on language-specific conventions
///
/// # Detection Rules by Language
///
/// ## Python
/// - Function/method name starts with `test_`
/// - File path contains `test`
/// - Symbol kind is "test" (pytest fixtures, etc.)
///
/// ## Rust
/// - Symbol kind is "test"
/// - Has `#[test]` attribute (detected by tree-sitter)
/// - Has `#[tokio::test]` or `#[actix_rt::test]` attribute
///
/// ## JavaScript/TypeScript
/// - Function name contains "test", "it", "describe", "spec"
/// - File ends with `.test.js`, `.test.ts`, `.spec.js`, `.spec.ts`
///
/// ## Java
/// - Has `@Test` annotation (kind should be "test")
/// - Method name starts with `test`
/// - File ends with `Test.java` or `TestCase.java`
///
/// ## C#
/// - Method/class in a test file (NUnit, xUnit, MSTest)
/// - Qualname contains `.Tests.` or `.Test.` namespace
///
/// ## Go
/// - Function name starts with `Test` (Go convention)
/// - File ends with `_test.go`
///
pub fn is_test_symbol(symbol: &Symbol) -> bool {
    // Check symbol kind first (most reliable)
    if symbol.kind.to_lowercase() == "test" {
        return true;
    }

    // Infer language from file path
    let file_lower = symbol.file_path.to_lowercase();
    let name_lower = symbol.name.to_lowercase();

    // Python tests
    if file_lower.ends_with(".py") {
        if name_lower.starts_with("test_") {
            return true;
        }
        if is_test_file(&symbol.file_path) {
            // In test files, functions/classes are likely tests
            return matches!(symbol.kind.as_str(), "function" | "method" | "class");
        }
    }

    // Rust tests
    if file_lower.ends_with(".rs") {
        // Check for test attributes in signature
        if let Some(sig) = &symbol.signature {
            let sig_lower = sig.to_lowercase();
            if sig_lower.contains("#[test]")
                || sig_lower.contains("#[tokio::test]")
                || sig_lower.contains("#[actix_rt::test]")
            {
                return true;
            }
        }

        // Test functions in tests/ directory
        // In Rust, tests/ directory typically contains integration tests
        let in_tests_dir = file_lower.contains("/tests/")
            || file_lower.contains("/test/")
            || file_lower.starts_with("tests/")
            || file_lower.starts_with("test/");

        if in_tests_dir && symbol.kind == "function" {
            // In tests/ directory, functions starting with test_ are tests
            // But also, files in tests/ are integration tests by convention
            if name_lower.starts_with("test_")
                || name_lower.ends_with("_test")
                || name_lower.contains("_test_")
            {
                return true;
            }
        }
    }

    // JavaScript/TypeScript tests
    if file_lower.ends_with(".js")
        || file_lower.ends_with(".ts")
        || file_lower.ends_with(".jsx")
        || file_lower.ends_with(".tsx")
    {
        // Test functions
        if name_lower.starts_with("test")
            || name_lower == "it"
            || name_lower == "describe"
            || name_lower == "beforeeach"
            || name_lower == "aftereach"
            || name_lower.contains("spec")
        {
            return true;
        }
        // Symbols in test files
        if is_test_file(&symbol.file_path) {
            return matches!(
                symbol.kind.as_str(),
                "function" | "method" | "arrow_function"
            );
        }
    }

    // Java tests
    if file_lower.ends_with(".java") {
        if name_lower.starts_with("test") {
            return true;
        }
        // Symbols in test files (JUnit, TestNG)
        if is_test_file(&symbol.file_path) && symbol.kind == "method" {
            return true;
        }
    }

    // C# tests
    if file_lower.ends_with(".cs") {
        // Methods/classes in test files (NUnit, xUnit, MSTest)
        if is_test_file(&symbol.file_path)
            && matches!(symbol.kind.as_str(), "method" | "function" | "class")
        {
            return true;
        }
        // Qualname contains .Tests. namespace (common C# convention)
        if symbol.qualname.contains(".Tests.") || symbol.qualname.contains(".Test.") {
            return true;
        }
    }

    // Go tests
    if file_lower.ends_with(".go")
        && file_lower.ends_with("_test.go")
        && name_lower.starts_with("test")
    {
        return true;
    }

    false
}

/// Detects if a file path appears to be a test file
///
/// This is a more lenient check than `is_test_symbol` - used for filtering
/// files from direct impact analysis results, and by the search-scope
/// classifier (issue #61) to decide the `tests` scope.
///
/// Directory rules match any path segment; filename rules (issue #67
/// finding 2) match only the file name component, as an anchored
/// prefix/suffix -- never a substring of the whole path -- so a module
/// that merely contains "test" as part of a longer word (`latest_version.py`,
/// `contest_rules.py`) isn't swept in.
pub fn is_test_file(path: &str) -> bool {
    let path_lower = path.to_lowercase();
    if is_test_directory(&path_lower) {
        return true;
    }

    let file_name = Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(path);
    let name_lower = file_name.to_lowercase();

    name_lower.starts_with("test_")
        || name_lower.ends_with("_test.rs")
        || name_lower.ends_with("_test.py")
        || name_lower.ends_with("_test.go")
        || name_lower.ends_with(".test.ts")
        || name_lower.ends_with(".test.tsx")
        || name_lower.ends_with(".test.js")
        || name_lower.ends_with(".test.jsx")
        || name_lower.ends_with(".spec.ts")
        || name_lower.ends_with(".spec.tsx")
        || name_lower.ends_with(".spec.js")
        || name_lower.ends_with(".spec.jsx")
        || name_lower.ends_with("_spec.rb")
        // Java's test convention is PascalCase ("FooTest.java" /
        // "FooTestCase.java") and needs the original casing here: matched
        // case-insensitively it would also catch a plain word that merely
        // ends in "test" ("Latest.java").
        || file_name.ends_with("Test.java")
        || file_name.ends_with("TestCase.java")
}

fn is_test_directory(path_lower: &str) -> bool {
    path_lower.contains("/test/")
        || path_lower.contains("/tests/")
        || path_lower.contains("/_test/")
        || path_lower.contains("/__tests__/")
        || path_lower.contains("/spec/")
        || path_lower.contains("/specs/")
        // A repo-relative path has no leading slash, so a top-level test
        // directory (`tests/foo.rs`, not `/tests/foo.rs`) needs its own
        // check -- `contains("/tests/")` alone never matches it.
        || path_lower.starts_with("test/")
        || path_lower.starts_with("tests/")
        || path_lower.starts_with("_test/")
        || path_lower.starts_with("__tests__/")
        || path_lower.starts_with("spec/")
        || path_lower.starts_with("specs/")
}

/// Extract test name from a test symbol
///
/// Examples:
/// - `test_calculate` -> `calculate`
/// - `TestCalculate` -> `Calculate`
/// - `calculateSpec` -> `calculate`
pub fn extract_test_target_name(test_name: &str) -> Option<String> {
    let lower = test_name.to_lowercase();

    // Pattern: test_something -> something
    if let Some(stripped) = lower.strip_prefix("test_") {
        return Some(stripped.to_string());
    }

    // Pattern: something_test -> something
    if let Some(stripped) = lower.strip_suffix("_test") {
        return Some(stripped.to_string());
    }

    // Pattern: testSomething -> Something (camelCase)
    if test_name.starts_with("test") && test_name.len() > 4 {
        let target = &test_name[4..]; // Skip "test"
        if target.chars().next().is_some_and(|c| c.is_uppercase()) {
            return Some(target.to_string());
        }
    }

    // Pattern: TestSomething -> Something (PascalCase)
    if test_name.starts_with("Test") && test_name.len() > 4 {
        let target = &test_name[4..]; // Skip "Test"
        return Some(target.to_string());
    }

    // Pattern: somethingTest -> something (camelCase)
    if test_name.ends_with("Test") && test_name.len() > 4 {
        let target = &test_name[..test_name.len() - 4]; // Remove "Test"
        return Some(target.to_string());
    }

    // Pattern: somethingSpec -> something
    if lower.ends_with("spec") && test_name.len() > 4 {
        let target = &test_name[..test_name.len() - 4]; // Remove "Spec"
        return Some(target.to_string());
    }

    None
}

/// Classify test type based on file path and symbol properties
///
/// Returns "unit", "integration", or "e2e"
pub fn classify_test_type(symbol: &Symbol) -> &'static str {
    let path_lower = symbol.file_path.to_lowercase();

    // E2E tests
    if path_lower.contains("/e2e/")
        || path_lower.contains("/e2e_tests/")
        || path_lower.contains("/end_to_end/")
        || path_lower.contains("/functional/")
    {
        return "e2e";
    }

    // Integration tests
    if path_lower.contains("/integration/")
        || path_lower.contains("/integration_tests/")
        || path_lower.contains("/integrationtests/")
        || symbol.name.to_lowercase().contains("integration")
    {
        return "integration";
    }

    // Default to unit tests
    "unit"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_symbol(file_path: &str, kind: &str, name: &str, signature: Option<&str>) -> Symbol {
        Symbol {
            id: 1,
            file_path: file_path.to_string(),
            kind: kind.to_string(),
            name: name.to_string(),
            qualname: format!("test::{}", name),
            start_line: 0,
            start_col: 0,
            end_line: 0,
            end_col: 0,
            start_byte: 0,
            end_byte: 0,
            signature: signature.map(|s| s.to_string()),
            docstring: None,
            graph_version: 0,
            commit_sha: None,
            stable_id: None,
        }
    }

    #[test]
    fn test_is_test_symbol_python() {
        let sym = make_symbol("tests/test_core.py", "function", "test_calculate", None);
        assert!(is_test_symbol(&sym), "Python test_ prefix");

        let sym = make_symbol("src/tests/core.py", "function", "calculate", None);
        assert!(is_test_symbol(&sym), "Function in test directory");

        let sym = make_symbol("src/core.py", "function", "calculate", None);
        assert!(!is_test_symbol(&sym), "Not a test");
    }

    #[test]
    fn test_is_test_symbol_rust() {
        let sym = make_symbol(
            "src/lib.rs",
            "function",
            "test_something",
            Some("#[test]\nfn test_something()"),
        );
        assert!(is_test_symbol(&sym), "Rust #[test] attribute");

        let sym = make_symbol("tests/integration.rs", "function", "test_integration", None);
        assert!(is_test_symbol(&sym), "Test function in tests/ directory");

        let sym = make_symbol("src/lib.rs", "function", "regular_function", None);
        assert!(!is_test_symbol(&sym), "Not a test");
    }

    #[test]
    fn test_is_test_symbol_javascript() {
        let sym = make_symbol("src/core.test.ts", "function", "it", None);
        assert!(is_test_symbol(&sym), "Jest it() test");

        let sym = make_symbol("tests/core.spec.js", "function", "describe", None);
        assert!(is_test_symbol(&sym), "describe() block");

        let sym = make_symbol("src/utils.ts", "function", "formatDate", None);
        assert!(!is_test_symbol(&sym), "Not a test");
    }

    #[test]
    fn test_is_test_symbol_csharp() {
        let sym = make_symbol(
            "dotnet/tests/Dpb.Tests/ServiceTests.cs",
            "method",
            "DeploySimpleZipFile",
            None,
        );
        assert!(is_test_symbol(&sym), "C# method in test file");

        let mut sym = make_symbol("dotnet/src/Dpb/Service.cs", "method", "Deploy", None);
        sym.qualname = "Dpb.Tests.ServiceTests.Deploy".to_string();
        assert!(is_test_symbol(&sym), "C# qualname with .Tests. namespace");

        let sym = make_symbol("dotnet/src/Dpb/Service.cs", "method", "Deploy", None);
        assert!(!is_test_symbol(&sym), "C# production method");
    }

    #[test]
    fn test_is_test_file() {
        assert!(is_test_file("tests/test_core.py"));
        assert!(is_test_file("src/core.test.ts"));
        assert!(is_test_file("lib/__tests__/util.js"));
        assert!(is_test_file("src/main_test.go"));
        assert!(!is_test_file("src/core.py"));
        assert!(!is_test_file("lib/utils.js"));
    }

    /// Issue #61: this predicate is now also the search-scope classifier's
    /// rule (`search::classify_path` used to have its own directory-segment
    /// match that didn't require a leading slash). A repo-relative path like
    /// `tests/common.rs` -- no leading slash, no `test_`/`_test` filename
    /// marker -- must still be recognized as a test file, or unifying onto
    /// this predicate would have silently narrowed what `search`'s `tests`
    /// scope matches.
    #[test]
    fn test_is_test_file_bare_directory_prefix() {
        assert!(is_test_file("tests/common.rs"));
        assert!(is_test_file("test/helpers.py"));
        assert!(is_test_file("__tests__/setup.js"));
        assert!(is_test_file("spec/support.rb"));
        assert!(is_test_file("specs/support.rb"));
        assert!(!is_test_file("testimony.py"));
        assert!(!is_test_file("attestation.rs"));
    }

    /// Issue #67 finding 2: filename conventions must match the file name
    /// component (prefix/suffix), not a substring of the whole path -- a
    /// module whose name merely contains "test" as part of a longer word
    /// must not be swept in. `.java` isn't an indexable extension in this
    /// codebase (see `indexer::scan::LANGUAGE_SPECS`), so the PascalCase
    /// `FooTest.java` convention can only be exercised at this unit seam,
    /// not through an RPC call against a reindexed fixture.
    #[test]
    fn test_is_test_file_filename_substring_false_positives() {
        assert!(!is_test_file("src/latest_version.py"));
        assert!(!is_test_file("src/contest_rules.py"));
        assert!(!is_test_file("com/example/Latest.java"));
    }

    /// Java's test convention is PascalCase (`FooTest.java` /
    /// `FooTestCase.java`) and needs the original casing to avoid the
    /// false positive above -- matched case-insensitively, "Latest.java"
    /// would also end in "test.java".
    #[test]
    fn test_is_test_file_java_pascal_case_convention() {
        assert!(is_test_file("FooTest.java"));
        assert!(is_test_file("com/example/FooTestCase.java"));
        assert!(!is_test_file("com/example/Latest.java"));
    }

    #[test]
    fn test_extract_test_target_name() {
        assert_eq!(
            extract_test_target_name("test_calculate"),
            Some("calculate".to_string())
        );
        assert_eq!(
            extract_test_target_name("TestCalculate"),
            Some("Calculate".to_string())
        );
        assert_eq!(
            extract_test_target_name("calculateTest"),
            Some("calculate".to_string())
        );
        assert_eq!(
            extract_test_target_name("calculate_test"),
            Some("calculate".to_string())
        );
        assert_eq!(
            extract_test_target_name("calculateSpec"),
            Some("calculate".to_string())
        );
        assert_eq!(extract_test_target_name("calculate"), None);
    }

    #[test]
    fn test_classify_test_type() {
        let sym = make_symbol("tests/unit/core.py", "function", "test_foo", None);
        assert_eq!(classify_test_type(&sym), "unit");

        let sym = make_symbol("tests/integration/api.py", "function", "test_api", None);
        assert_eq!(classify_test_type(&sym), "integration");

        let sym = make_symbol("tests/e2e/checkout.py", "function", "test_checkout", None);
        assert_eq!(classify_test_type(&sym), "e2e");
    }
}
