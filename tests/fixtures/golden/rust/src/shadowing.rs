mod a {
    /// Decoy: same name as `b::helper`; imported by the file-scope
    /// `use crate::shadowing::a::helper` below.
    pub fn helper() -> &'static str {
        "a"
    }
}

mod b {
    /// Decoy: same name as `a::helper`; `use`d only inside individual
    /// functions below, never at file scope.
    pub fn helper() -> &'static str {
        "b"
    }
}

/// Destructuring target for `shadow_struct_shorthand`'s `let H { helper }`.
struct H {
    helper: u32,
}

use crate::shadowing::a::helper;

/// Plain bare call: the file-scope `use` binds it to `a::helper`.
pub fn f1() -> &'static str {
    helper()
}

/// A function-body `use` of `helper` is a non-callee occurrence, so the
/// import candidate is suppressed (over-suppression: real target is `b`).
pub fn f2() -> &'static str {
    use crate::shadowing::b::helper;
    helper()
}

/// A local `let`-bound closure of the same name shadows the file-scope
/// `use` entirely — this call must never bind to `a::helper`.
pub fn f3() -> &'static str {
    let helper = || "local";
    helper()
}

/// Control: no occurrence of `helper` in this body besides the call
/// itself, so the file-scope `use` still applies normally.
pub fn ok() -> &'static str {
    helper()
}

/// Struct-shorthand destructuring (a `shorthand_field_identifier`).
pub fn shadow_struct_shorthand() -> &'static str {
    let H { helper } = x;
    helper()
}

/// Untyped closure parameter.
pub fn shadow_closure_param() -> &'static str {
    v.iter().for_each(|helper| helper());
    "done"
}

/// `match` pattern binding.
pub fn shadow_match_pattern() -> &'static str {
    match o {
        Some(helper) => helper(),
        _ => "none",
    }
}

/// `if let` pattern binding.
pub fn shadow_if_let() -> &'static str {
    if let Some(helper) = o {
        helper();
    }
    "done"
}

/// `while let` pattern binding.
pub fn shadow_while_let() -> &'static str {
    while let Some(helper) = v.pop() {
        helper();
    }
    "done"
}

/// `for` loop pattern binding.
pub fn shadow_for_loop() -> &'static str {
    for helper in v {
        helper();
    }
    "done"
}

/// A `use` nested in a block — suppressed like `f2`.
pub fn shadow_block_use() -> &'static str {
    {
        use crate::shadowing::b::helper;
        helper();
    }
    "done"
}

/// A `use` nested in a closure body — suppressed like `f2`.
pub fn shadow_closure_use() -> &'static str {
    let f = || {
        use crate::shadowing::b::helper;
        helper()
    };
    f()
}

/// A value use of `helper` also suppresses the call (over-suppression:
/// real target is `a`).
pub fn uses_helper_as_value() -> &'static str {
    let _ = helper;
    helper()
}

/// A glob `use` inside the body shadows the file-scope `use`.
pub fn shadow_glob_use() -> &'static str {
    use crate::shadowing::b::*;
    helper()
}
