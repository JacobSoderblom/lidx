use lidx::diagnostics::parse_sarif;
use std::path::Path;

#[test]
fn parse_sarif_basic() {
    let sarif = r#"
{
  "version": "2.1.0",
  "runs": [
    {
      "tool": {
        "driver": {
          "name": "demo",
          "rules": [{ "id": "R1" }]
        }
      },
      "results": [
        {
          "ruleId": "R1",
          "level": "error",
          "message": { "text": "Bad thing" },
          "locations": [
            {
              "physicalLocation": {
                "artifactLocation": { "uri": "src/main.rs" },
                "region": {
                  "startLine": 3,
                  "startColumn": 5,
                  "endLine": 3,
                  "endColumn": 10,
                  "snippet": { "text": "bad" }
                }
              }
            }
          ]
        }
      ]
    }
  ]
}
"#;
    let diagnostics = parse_sarif(sarif, Path::new("/repo")).unwrap();
    assert_eq!(diagnostics.len(), 1);
    let diag = &diagnostics[0];
    assert_eq!(diag.path.as_deref(), Some("src/main.rs"));
    assert_eq!(diag.line, Some(3));
    assert_eq!(diag.column, Some(5));
    assert_eq!(diag.end_line, Some(3));
    assert_eq!(diag.end_column, Some(10));
    assert_eq!(diag.severity.as_deref(), Some("error"));
    assert_eq!(diag.message, "Bad thing");
    assert_eq!(diag.rule_id.as_deref(), Some("R1"));
    assert_eq!(diag.tool.as_deref(), Some("demo"));
    assert_eq!(diag.snippet.as_deref(), Some("bad"));
}
