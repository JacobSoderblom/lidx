#[derive(Debug, Clone)]
pub struct SymbolInput {
    pub kind: String,
    pub name: String,
    pub qualname: String,
    pub start_line: i64,
    pub start_col: i64,
    pub end_line: i64,
    pub end_col: i64,
    pub start_byte: i64,
    pub end_byte: i64,
    pub signature: Option<String>,
    pub docstring: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct EdgeInput {
    pub kind: String,
    pub source_qualname: Option<String>,
    pub target_qualname: Option<String>,
    pub detail: Option<String>,
    pub evidence_snippet: Option<String>,
    pub evidence_start_line: Option<i64>,
    pub evidence_end_line: Option<i64>,
    pub confidence: Option<f64>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub event_ts: Option<i64>,
}

#[derive(Debug, Default)]
pub struct ExtractedFile {
    pub symbols: Vec<SymbolInput>,
    pub edges: Vec<EdgeInput>,
    pub file_metrics: Option<FileMetricsInput>,
    pub symbol_metrics: Vec<SymbolMetricsInput>,
}
use crate::metrics::{FileMetricsInput, SymbolMetricsInput};
use anyhow::Result;
use std::path::Path;

pub trait LanguageExtractor {
    fn module_name_from_rel_path(&self, rel_path: &str) -> String;
    fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile>;
    fn resolve_imports(
        &self,
        _repo_root: &Path,
        _file_rel_path: &str,
        _module_name: &str,
        _edges: &mut Vec<EdgeInput>,
    ) {
        // default no-op
    }
}
