use crate::indexer::extract::{ExtractedFile, LanguageExtractor};
use anyhow::Result;

/// Markdown is indexed as a `files` row only (issue #133) so scope counts
/// and language reporting see it. Headings are not stored as symbols:
/// `outline` parses them straight off disk (`rpc::reading`).
pub struct MarkdownExtractor;

impl LanguageExtractor for MarkdownExtractor {
    fn module_name_from_rel_path(&self, rel_path: &str) -> String {
        rel_path.to_string()
    }

    fn extract(&mut self, _source: &str, _module_name: &str) -> Result<ExtractedFile> {
        Ok(ExtractedFile::default())
    }
}
