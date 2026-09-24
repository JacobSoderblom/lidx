/// Ordering regression (issue #75 follow-up, finding A): this file sorts
/// alphabetically *before* `z_owner.rs`, so `Indexer::reindex`'s file scan
/// (sorted by `rel_path`) processes it first. Before the fix, marking a
/// file's private symbols was interleaved with resolving its edges, so
/// `zsecret`'s `visibility` was still unset (NULL, "unrestricted") at the
/// moment this call was resolved, and it wrongly bound. The fix marks
/// every file's private symbols before resolving *any* file's edges, so
/// processing order can't matter.
pub fn call_before_owner_is_marked() -> &'static str {
    zsecret()
}
