//! Issue #257: `mcp-serve` must answer every non-notification input line, and
//! `serve` must echo the `id` of a request it cannot deserialize. Drives the
//! real `lidx` binary over stdin and counts output lines.
use serde_json::{Value, json};
use std::io::Write;
use std::process::{Command, Stdio};

fn run(subcommand: &str, input: &str) -> Vec<String> {
    let repo = tempfile::tempdir().unwrap();
    std::fs::write(repo.path().join("a.py"), "def f():\n    pass\n").unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_lidx"))
        .args([subcommand, "--repo"])
        .arg(repo.path())
        .args(["--watch", "off"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn lidx");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

fn parse(line: &str) -> Value {
    serde_json::from_str(line).unwrap()
}

#[test]
fn mcp_serve_answers_every_non_notification_line() {
    // (input line, expected number of output lines)
    let matrix: [(&str, usize); 16] = [
        (r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#, 1),
        (r#"[{"jsonrpc":"2.0","id":2,"method":"tools/list"}]"#, 1),
        ("[]", 1),
        (r#"{"jsonrpc":"2.0","id":3,"method":"ping"}"#, 1),
        ("{not json", 1),
        (
            r#"{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"nope","arguments":{}}}"#,
            1,
        ),
        (r#"{"jsonrpc":"2.0","id":7,"method":"nope/nope"}"#, 1),
        (r#"{"jsonrpc":"2.0","id":8}"#, 1),
        ("42", 1),
        (r#""hello""#, 1),
        ("true", 1),
        ("null", 1),
        (r#"{"jsonrpc":"2.0","method":"ping"}"#, 0),
        (
            r#"[{"jsonrpc":"2.0","method":"ping"},{"jsonrpc":"2.0","method":"ping"}]"#,
            0,
        ),
        (
            r#"[{"jsonrpc":"2.0","id":9,"method":"ping"},42,{"x":1}]"#,
            1,
        ),
        (r#"{"jsonrpc":"2.0","id":null,"method":"ping"}"#, 1),
    ];
    let input: String = matrix.iter().map(|(l, _)| format!("{l}\n")).collect();
    let out = run("mcp-serve", &input);
    let expected: usize = matrix.iter().map(|(_, n)| n).sum();
    assert_eq!(out.len(), expected, "output lines: {out:#?}");

    let mut it = out.iter().map(|l| parse(l));
    let mut next = || it.next().unwrap();
    // Valid single-object response is byte-identical to the pre-fix format.
    assert_eq!(out[0], r#"{"id":1,"jsonrpc":"2.0","result":{}}"#);
    next();
    let batch = next();
    assert_eq!(batch.as_array().unwrap()[0]["id"], 2);
    let empty = next();
    assert_eq!(
        (&empty["id"], &empty["error"]["code"]),
        (&Value::Null, &json!(-32600))
    );
    assert_eq!(next()["id"], 3);
    assert_eq!(next()["error"]["code"], -32700);
    assert_eq!(next()["error"]["code"], -32602);
    assert_eq!(next()["error"]["code"], -32601);
    let no_method = next();
    assert_eq!(
        (&no_method["id"], &no_method["error"]["code"]),
        (&json!(8), &json!(-32600))
    );
    for _ in 0..4 {
        let resp = next();
        assert_eq!(
            (&resp["id"], &resp["error"]["code"]),
            (&Value::Null, &json!(-32600))
        );
    }
    let mixed = next();
    let arr = mixed.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["id"], 9);
    assert_eq!(arr[1]["error"]["code"], -32600);
    assert_eq!(arr[2]["error"]["code"], -32600);
    let null_id = next();
    assert_eq!(null_id["id"], Value::Null);
    assert!(null_id["result"].is_object());
}

#[test]
fn serve_echoes_id_of_undeserializable_request() {
    let out = run("serve", "{\"id\":3}\n[]\n{\"id\":2,\"method\":\"nope\"}\n");
    assert_eq!(out.len(), 3, "{out:#?}");
    let first = parse(&out[0]);
    assert_eq!(first["id"], 3);
    assert!(
        first["error"]["message"]
            .as_str()
            .unwrap()
            .contains("missing field `method`")
    );
    let second = parse(&out[1]);
    assert_eq!(second["id"], Value::Null);
    assert_eq!(second["error"]["message"], "invalid request: empty array");
    assert_eq!(parse(&out[2])["id"], 2);
}
