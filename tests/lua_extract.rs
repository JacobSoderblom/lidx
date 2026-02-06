use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::lua::{LuaExtractor, module_name_from_rel_path};

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("init.lua"), "init");
    assert_eq!(module_name_from_rel_path("lib/utils.lua"), "lib/utils");
    assert_eq!(
        module_name_from_rel_path("src/game/player.lua"),
        "src/game/player"
    );
    assert_eq!(
        module_name_from_rel_path("scripts/setup.lua"),
        "scripts/setup"
    );
}

#[test]
fn extract_symbols_and_edges() {
    let source = r#"
local json = require("cjson")
local utils = require("lib.utils")

local MyClass = {}

function MyClass:new(name)
    local obj = {}
    setmetatable(obj, self)
    self.__index = self
    obj.name = name
    return obj
end

function MyClass:greet()
    print("Hello, " .. self.name)
end

function MyClass.static_method()
    return "static"
end

local function helper(x)
    return x * 2
end

function top_level_func(a, b)
    helper(a)
    MyClass:new("test")
    return a + b
end

MAX_SIZE = 100
local config = { debug = true }
"#;
    let mut extractor = LuaExtractor::new().unwrap();
    let extracted = extractor.extract(source, "lib/mymod").unwrap();

    let names: Vec<_> = extracted
        .symbols
        .iter()
        .map(|s| (s.kind.as_str(), s.qualname.as_str()))
        .collect();

    // Module symbol
    assert!(names.contains(&("module", "lib/mymod")));

    // Class (table assigned to local)
    assert!(names.contains(&("class", "lib/mymod.MyClass")));

    // Methods (colon syntax)
    assert!(names.contains(&("method", "lib/mymod.MyClass.new")));
    assert!(names.contains(&("method", "lib/mymod.MyClass.greet")));

    // Method (dot syntax)
    assert!(names.contains(&("method", "lib/mymod.MyClass.static_method")));

    // Functions
    assert!(names.contains(&("function", "lib/mymod.helper")));
    assert!(names.contains(&("function", "lib/mymod.top_level_func")));

    // Variables
    assert!(names.contains(&("variable", "lib/mymod.MAX_SIZE")));
    assert!(names.contains(&("variable", "lib/mymod.config")));

    // Edge kinds
    let edge_kinds: Vec<_> = extracted.edges.iter().map(|e| e.kind.as_str()).collect();
    assert!(edge_kinds.contains(&"CONTAINS"));
    assert!(edge_kinds.contains(&"IMPORTS"));
    assert!(edge_kinds.contains(&"CALLS"));

    // Imports
    let imports: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "IMPORTS")
        .collect();
    assert!(
        imports
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("cjson")),
        "Expected cjson import, got: {:?}",
        imports
    );
    assert!(
        imports
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("lib/utils")),
        "Expected lib/utils import (dots→slashes), got: {:?}",
        imports
    );

    // Calls
    let calls: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CALLS")
        .collect();
    assert!(
        calls
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("lib/mymod.helper")),
        "Expected call to helper, got: {:?}",
        calls
    );
}

#[test]
fn extract_require_imports() {
    let source = r#"
local mod1 = require("mypackage.module1")
local mod2 = require("simple")
require("side_effect")
"#;
    let mut extractor = LuaExtractor::new().unwrap();
    let extracted = extractor.extract(source, "main").unwrap();

    let imports: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "IMPORTS")
        .collect();

    assert!(
        imports
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("mypackage/module1")),
        "Expected mypackage/module1 import"
    );
    assert!(
        imports
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("simple")),
        "Expected simple import"
    );
    assert!(
        imports
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("side_effect")),
        "Expected side_effect import"
    );
}

#[test]
fn extract_method_calls() {
    let source = r#"
function foo()
    bar()
    obj:method()
    obj.func()
end
"#;
    let mut extractor = LuaExtractor::new().unwrap();
    let extracted = extractor.extract(source, "test").unwrap();

    let calls: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CALLS")
        .collect();

    assert!(
        calls
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("test.bar")),
        "Expected call to bar"
    );
    // method calls via : become qualified
    assert!(
        calls
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("obj.method")),
        "Expected call to obj.method, got: {:?}",
        calls
    );
    // dot calls
    assert!(
        calls
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("obj.func")),
        "Expected call to obj.func, got: {:?}",
        calls
    );
}

#[test]
fn extract_contains_edges() {
    let source = r#"
local Cls = {}

function Cls:method1() end

function top_func() end
"#;
    let mut extractor = LuaExtractor::new().unwrap();
    let extracted = extractor.extract(source, "mod").unwrap();

    let contains: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONTAINS")
        .collect();

    // module contains class
    assert!(
        contains
            .iter()
            .any(|e| e.source_qualname.as_deref() == Some("mod")
                && e.target_qualname.as_deref() == Some("mod.Cls")),
        "Expected module → Cls CONTAINS edge"
    );

    // class contains method
    assert!(
        contains
            .iter()
            .any(|e| e.source_qualname.as_deref() == Some("mod.Cls")
                && e.target_qualname.as_deref() == Some("mod.Cls.method1")),
        "Expected Cls → method1 CONTAINS edge"
    );

    // module contains function
    assert!(
        contains
            .iter()
            .any(|e| e.source_qualname.as_deref() == Some("mod")
                && e.target_qualname.as_deref() == Some("mod.top_func")),
        "Expected module → top_func CONTAINS edge"
    );
}
