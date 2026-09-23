use crate::caller::entry;

/// Cross-module incoming call into `caller::entry`.
pub fn use_entry() -> &'static str {
    entry()
}
