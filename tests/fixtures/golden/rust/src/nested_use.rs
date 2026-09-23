/// Target of `inner::calls_helper`'s `use super::helper` below.
pub fn helper() -> &'static str {
    "helper"
}

pub mod inner {
    use super::helper;

    /// `use super::x` written inside a *nested* module (not at file
    /// scope): the `super::` rewrite must use this module's own enclosing
    /// module, not the file's root, and this module must not inherit its
    /// parent's `use` bindings.
    pub fn calls_helper() -> &'static str {
        helper()
    }
}
