mod child_visibility_child;

/// Private item: visible to `child_visibility_owner` itself and to every
/// descendant module -- including `child_visibility_child`, declared in a
/// separate file (issue #75 follow-up, finding D). A same-file-only check
/// under-refuses this real Rust visibility rule. Named distinctively so
/// the bare-name fallback below has exactly one candidate (several other
/// fixtures already declare their own `helper`).
fn owner_secret() -> &'static str {
    "helper"
}
