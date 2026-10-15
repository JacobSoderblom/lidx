//! Issue #521: with `tests_are_roots` on (the default), the entry points of
//! Python (pytest, unittest) and C# (xUnit, NUnit, MSTest) test suites are
//! roots; with it off they are judged like any other declaration.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};

fn call(indexer: &mut Indexer, tests_are_roots: Option<bool>) -> Value {
    let mut params = json!({"include_unused_imports": false, "include_orphan_tests": false,
                            "include_public_api": true, "limit": 1000});
    if let Some(flag) = tests_are_roots {
        params["tests_are_roots"] = json!(flag);
    }
    rpc::handle_method(indexer, "dead_symbols", params).unwrap()
}

fn claimed(result: &Value, qualname: &str) -> bool {
    ["dead_symbols", "public_api"]
        .iter()
        .flat_map(|g| result[*g].as_array().into_iter().flatten())
        .any(|s| s["qualname"].as_str() == Some(qualname))
}

const PYTHON: &[(&str, &str)] = &[
    (
        "app/widgets.py",
        "def used():\n    return 1\n\n\ndef orphan_prod():\n    return 2\n\n\ndef test_looking_prod():\n    return 3\n",
    ),
    ("app/main.py", "from app.widgets import used\n\nused()\n"),
    (
        "tests/test_widgets.py",
        "import pytest\n\n\n\
         def test_make():\n    pass\n\n\n\
         def helper_unused():\n    pass\n\n\n\
         @pytest.fixture\ndef client():\n    return 1\n\n\n\
         @pytest.fixture(scope=\"module\")\ndef database():\n    return 2\n\n\n\
         @pytest.fixture\nasync def lonely():\n    return 3\n\n\n\
         class TestWidget:\n    def setup_method(self):\n        pass\n\n    def test_a(self):\n        pass\n\n    def helper_method(self):\n        pass\n\n\n\
         class Plain:\n    def test_not_collected(self):\n        pass\n",
    ),
    (
        "tests/conftest.py",
        "def pytest_configure(config):\n    pass\n",
    ),
    (
        "tests/test_cases.py",
        "import unittest\n\n\n\
         class CaseBase(unittest.TestCase):\n    def setUp(self):\n        pass\n\n    def tearDown(self):\n        pass\n\n    def test_x(self):\n        pass\n\n    def _unused_helper(self):\n        pass\n",
    ),
    (
        "app/helpers_test_data.py",
        "class TestLooking:\n    def test_m(self):\n        pass\n",
    ),
];

#[test]
fn python_test_entry_points_are_roots() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-pytest-", PYTHON);
    let default = call(&mut ix, None);
    for rooted in [
        "tests.test_widgets.test_make",
        "tests.test_widgets.client",
        "tests.test_widgets.database",
        "tests.test_widgets.lonely",
        "tests.test_widgets.TestWidget",
        "tests.test_widgets.TestWidget.test_a",
        "tests.test_widgets.TestWidget.setup_method",
        "tests.conftest.pytest_configure",
        "tests.test_cases.CaseBase",
        "tests.test_cases.CaseBase.setUp",
        "tests.test_cases.CaseBase.tearDown",
        "tests.test_cases.CaseBase.test_x",
    ] {
        assert!(!claimed(&default, rooted), "{rooted} claimed: {default}");
    }
    // what no framework calls stays claimed
    for dead in [
        "app.widgets.orphan_prod",
        "app.widgets.test_looking_prod",
        "tests.test_widgets.helper_unused",
        "tests.test_widgets.TestWidget.helper_method",
        "tests.test_widgets.Plain.test_not_collected",
        "tests.test_cases.CaseBase._unused_helper",
        "app.helpers_test_data.TestLooking.test_m",
    ] {
        assert!(claimed(&default, dead), "{dead} not claimed: {default}");
    }
}

#[test]
fn python_tests_are_judged_like_any_declaration_when_the_option_is_off() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-pytest-off-", PYTHON);
    let strict = call(&mut ix, Some(false));
    for name in [
        "tests.test_widgets.test_make",
        "tests.test_widgets.client",
        "tests.test_widgets.TestWidget.test_a",
        "tests.test_cases.CaseBase.test_x",
    ] {
        assert!(claimed(&strict, name), "{name} not claimed: {strict}");
    }
}

const CSHARP: &[(&str, &str)] = &[
    (
        "src/Lib.cs",
        "namespace App;\npublic class Lib\n{\n    public int Used() { return 1; }\n    public int Orphan() { return 2; }\n}\n",
    ),
    (
        "tests/App.Tests/WidgetTests.cs",
        "using System;\nusing Xunit;\nnamespace App.Tests;\n\
         public class WidgetTests : IDisposable, IAsyncLifetime\n{\n\
         \x20   public WidgetTests() { }\n\
         \x20   [Fact]\n    public void Works() { }\n\
         \x20   [Theory]\n    [InlineData(1)]\n    public void Sums(int x) { }\n\
         \x20   public Task InitializeAsync() { return Task.CompletedTask; }\n\
         \x20   public Task DisposeAsync() { return Task.CompletedTask; }\n\
         \x20   public void Dispose() { }\n\
         \x20   private void UnusedHelper() { }\n}\n",
    ),
    (
        "tests/App.Tests/NUnitTests.cs",
        "using NUnit.Framework;\nnamespace App.Tests;\n\
         [TestFixture]\npublic class NUnitTests\n{\n\
         \x20   [SetUp]\n    public void Prepare() { }\n\
         \x20   [Test]\n    public void Runs() { }\n\
         \x20   [TestCase(1)]\n    public void Cases(int x) { }\n\
         \x20   [OneTimeSetUp]\n    public void Once() { }\n}\n",
    ),
    (
        "tests/App.Tests/MsTests.cs",
        "using Microsoft.VisualStudio.TestTools.UnitTesting;\nnamespace App.Tests;\n\
         [TestClass]\npublic class MsTests\n{\n\
         \x20   [TestInitialize]\n    public void Init() { }\n\
         \x20   [TestMethod]\n    public void Checks() { }\n}\n",
    ),
    (
        "tests/App.Tests/SimpleTests.cs",
        "using Xunit;\nnamespace App.Tests;\npublic class SimpleTests\n{\n    public SimpleTests() { }\n    [Fact]\n    public void Simple() { }\n}\n",
    ),
    (
        "tests/App.Tests/Plain.cs",
        "namespace App.Tests;\npublic class PlainHelper\n{\n    public PlainHelper() { }\n    public void Forgotten() { }\n}\n",
    ),
];

#[test]
fn csharp_test_entry_points_are_roots() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-xunit-", CSHARP);
    let default = call(&mut ix, None);
    for rooted in [
        "App.Tests.WidgetTests",
        "App.Tests.WidgetTests..ctor",
        "App.Tests.WidgetTests.Works",
        "App.Tests.WidgetTests.Sums",
        "App.Tests.WidgetTests.InitializeAsync",
        "App.Tests.WidgetTests.DisposeAsync",
        "App.Tests.WidgetTests.Dispose",
        "App.Tests.SimpleTests",
        "App.Tests.SimpleTests..ctor",
        "App.Tests.SimpleTests.Simple",
        "App.Tests.NUnitTests",
        "App.Tests.NUnitTests.Prepare",
        "App.Tests.NUnitTests.Runs",
        "App.Tests.NUnitTests.Cases",
        "App.Tests.NUnitTests.Once",
        "App.Tests.MsTests",
        "App.Tests.MsTests.Init",
        "App.Tests.MsTests.Checks",
    ] {
        assert!(!claimed(&default, rooted), "{rooted} claimed: {default}");
    }
    for dead in [
        "App.Lib.Orphan",
        "App.Tests.WidgetTests.UnusedHelper",
        "App.Tests.PlainHelper.Forgotten",
    ] {
        assert!(claimed(&default, dead), "{dead} not claimed: {default}");
    }
}

#[test]
fn csharp_tests_are_judged_like_any_declaration_when_the_option_is_off() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-xunit-off-", CSHARP);
    let strict = call(&mut ix, Some(false));
    for name in [
        "App.Tests.SimpleTests..ctor",
        "App.Tests.SimpleTests.Simple",
        "App.Tests.NUnitTests.Runs",
        "App.Tests.MsTests.Checks",
    ] {
        assert!(claimed(&strict, name), "{name} not claimed: {strict}");
    }
}
