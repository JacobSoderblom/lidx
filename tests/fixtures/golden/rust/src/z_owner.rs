/// Private item: only `z_owner` may call it directly. Sorts alphabetically
/// *after* `a_prober.rs` — see that file's doc.
fn zsecret() -> &'static str {
    "secret"
}

pub fn call_locally() -> &'static str {
    zsecret()
}
