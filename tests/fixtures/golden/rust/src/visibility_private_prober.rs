/// Calls `secret_helper`, which exists only in `visibility_private_owner`
/// as a private (non-`pub`) function -- no `use` brings it into scope
/// here, so this is a bare, unqualified call. Before #75, the bare-name
/// fallback found exactly one same-language candidate in the whole fixture
/// (the only `secret_helper`) and bound to it despite it being private and
/// declared in a different file. #75's visibility guard must refuse that
/// cross-file bind and leave the call `UNRESOLVED(private)`.
pub fn call_remotely() -> &'static str {
    secret_helper()
}
