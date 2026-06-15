/// Backward-compatible re-export. The extractor previously named
/// `PostgresExtractor` has been consolidated into `sql_extractor::SqlExtractor`,
/// which now handles all SQL dialects (`.sql`, `.psql`, `.pgsql`, `.tsql`).
pub use crate::indexer::sql_extractor::SqlExtractor as PostgresExtractor;
pub use crate::indexer::sql_extractor::module_name_from_rel_path;
