/// Decoy: same name as `caller::local_util`.
pub fn local_util() -> &'static str {
    "other"
}

/// Decoy: same name as `helper::format_greeting`.
pub fn format_greeting(_name: &str) -> &'static str {
    "Other"
}

/// Decoy: same name as the private `helper::decorate`.
fn decorate(_name: &str) -> &'static str {
    "other"
}
