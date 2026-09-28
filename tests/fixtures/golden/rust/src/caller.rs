use crate::ambiguous_a::run;
use crate::animals::{Dog, Speak};
use crate::greeter::Greeter;
use crate::helper::format_greeting;

/// Private item: only callable from within `caller`.
fn local_util() -> &'static str {
    "local"
}

/// Plain call: a bare same-module function call.
pub fn entry() -> &'static str {
    local_util()
}

/// `use crate::x::y` call: `format_greeting` is bound by the `use` only.
pub fn call_imported_helper(name: &str) -> &'static str {
    format_greeting(name)
}

/// Module path call: fully qualified through `crate::`.
pub fn call_crate_path(name: &str) -> &'static str {
    crate::helper::format_greeting(name)
}

/// Module path call through `super::` (the crate root, here).
pub fn call_super_path(name: &str) -> &'static str {
    super::helper::format_greeting(name)
}

/// Module path call through `self::`.
pub fn call_self_path() -> &'static str {
    self::local_util()
}

/// Receiver-typed call: `g: &Greeter` pins `.greet` to `Greeter::greet`.
pub fn call_receiver_typed(g: &Greeter) -> &'static str {
    g.greet("world")
}

/// Trait default method: `Dog` implements `Speak` without overriding
/// `speak`, so the call dispatches to `Speak::speak`.
pub fn call_inherited(d: &Dog) -> &'static str {
    d.speak()
}

/// Ambiguous name: `run` exists in `ambiguous_a` and `ambiguous_b`; the
/// `use` above picks `ambiguous_a`.
pub fn call_ambiguous() {
    run()
}

/// Call into an external crate path that is never indexed.
pub fn call_external() -> u32 {
    std::process::id()
}
