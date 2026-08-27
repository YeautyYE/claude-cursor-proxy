//! Regression coverage for Claude Code's Anthropic-defined text editor.
//!
//! Claude Code 2.1.193 advertises the schema-less pair
//! `text_editor_20250728` / `str_replace_based_edit_tool`.  Cursor's native
//! Pi Edit events use a different (`file_path`/`old_string`/`new_string`)
//! shape, so the proxy has to resolve the advertised alias and normalize the
//! payload before returning a client-only tool_use.  Keep these tests at the
//! public API boundary: they model the exact request/event shapes seen by the
//! running Claude Code client instead of reaching into private live-driver
//! state.

use std::collections::BTreeSet;

use claude_cursor_proxy::anthropic::schema::MessagesRequest;
use claude_cursor_proxy::providers::cursor::native_tools::{
    adapt_tool_input_for_client, map_exec_server_message,
};
use claude_cursor_proxy::providers::cursor::proto::{
    ExecServerMessage, PiEditExecArgs, PiEditReplacement,
};
use claude_cursor_proxy::providers::cursor::request::claude_local_mcp_tools;
use claude_cursor_proxy::providers::cursor::tool_bridge::advertised_tool_names;
use claude_cursor_proxy::providers::cursor::tool_use_xml::{
    CursorToolUseXmlParser, RecoveredCursorEvent,
};

fn text_editor_request(tool: serde_json::Value) -> MessagesRequest {
    serde_json::from_value(serde_json::json!({
        "model": "claude-fable-5",
        "messages": [{"role": "user", "content": "edit the file"}],
        "tools": [tool]
    }))
    .expect("valid MessagesRequest")
}

fn string_field<'a>(
    fields: &'a std::collections::BTreeMap<String, prost_types::Value>,
    key: &str,
) -> &'a str {
    match fields.get(key).and_then(|value| value.kind.as_ref()) {
        Some(prost_types::value::Kind::StringValue(value)) => value,
        other => panic!("expected string field {key}, got {other:?}"),
    }
}

/// Read the command enum from Cursor's protobuf `google.protobuf.Value`
/// wrapper.  This deliberately checks the wire shape as well as the values:
/// sending a raw Struct here causes Cursor to reject the catalog frame.
fn text_editor_command_enum(schema: &prost_types::Value) -> Vec<String> {
    let Some(prost_types::value::Kind::StructValue(root)) = schema.kind.as_ref() else {
        panic!("text editor schema must be a struct Value: {schema:?}");
    };
    assert_eq!(string_field(&root.fields, "type"), "object");
    let Some(prost_types::value::Kind::StructValue(properties)) = root
        .fields
        .get("properties")
        .and_then(|value| value.kind.as_ref())
    else {
        panic!("text editor schema is missing properties: {root:?}");
    };
    let Some(prost_types::value::Kind::StructValue(command)) = properties
        .fields
        .get("command")
        .and_then(|value| value.kind.as_ref())
    else {
        panic!("text editor schema is missing command property: {properties:?}");
    };
    let Some(prost_types::value::Kind::ListValue(values)) = command
        .fields
        .get("enum")
        .and_then(|value| value.kind.as_ref())
    else {
        panic!("command property is missing enum: {command:?}");
    };
    values
        .values
        .iter()
        .map(|value| match value.kind.as_ref() {
            Some(prost_types::value::Kind::StringValue(value)) => value.clone(),
            other => panic!("command enum member must be a string: {other:?}"),
        })
        .collect()
}

#[test]
fn claude_20250728_editor_is_registered_with_exact_wire_name() {
    let req = text_editor_request(serde_json::json!({
        "type": "text_editor_20250728",
        "name": "str_replace_based_edit_tool"
    }));
    let catalog = claude_local_mcp_tools(&req).expect("text editor must be in MCP catalog");
    assert_eq!(catalog.tools.len(), 1);
    let editor = &catalog.tools[0];
    assert_eq!(editor.name, "str_replace_based_edit_tool");
    assert_eq!(editor.tool_name, "str_replace_based_edit_tool");
    assert_eq!(editor.provider_identifier, "claude-local");
    assert!(
        editor.input_schema.is_some(),
        "Cursor requires a Value schema"
    );
    // The text editor is Anthropic-defined and only supports these four
    // commands.  `delete`/`rename` belong to the separate memory tool and
    // making them visible causes Claude Code to emit calls it cannot execute.
    assert_eq!(
        text_editor_command_enum(editor.input_schema.as_ref().unwrap()),
        ["view", "create", "str_replace", "insert"]
    );
    let root = match editor.input_schema.as_ref().unwrap().kind.as_ref() {
        Some(prost_types::value::Kind::StructValue(root)) => root,
        other => panic!("text editor schema must be a struct Value: {other:?}"),
    };
    let properties = match root.fields.get("properties").and_then(|v| v.kind.as_ref()) {
        Some(prost_types::value::Kind::StructValue(properties)) => properties,
        other => panic!("text editor schema properties must be a struct Value: {other:?}"),
    };
    assert!(
        !properties.fields.contains_key("new_path"),
        "20250728 editor has no rename/new_path input"
    );
    assert!(!editor.description.contains("delete"));
    assert!(!editor.description.contains("rename"));
}

#[test]
fn advertised_tool_names_keeps_schema_less_editor_in_allowlist() {
    let req = text_editor_request(serde_json::json!({
        "type": "text_editor_20250728",
        "name": "str_replace_based_edit_tool"
    }));
    let names = advertised_tool_names(&req).expect("explicit tools catalog");
    assert!(
        names.contains("str_replace_based_edit_tool"),
        "schema-less Anthropic editor must participate in native tool admission"
    );
    assert_eq!(names.len(), 1);
}

#[test]
fn pi_edit_single_replacement_is_normalized_for_modern_editor() {
    let exec = ExecServerMessage {
        id: 91,
        exec_id: Some("pi-edit-91".into()),
        pi_edit_args: Some(PiEditExecArgs {
            path: "src/lib.rs".into(),
            edits: vec![PiEditReplacement {
                old_text: "old value".into(),
                new_text: "new value".into(),
            }],
        }),
        ..Default::default()
    };
    let mapped = map_exec_server_message(&exec).expect("Pi Edit event must map");
    assert_eq!(
        mapped.name, "Edit",
        "native mapping keeps legacy shape first"
    );
    let normalized = adapt_tool_input_for_client("str_replace_based_edit_tool", mapped.input);
    assert_eq!(
        normalized,
        serde_json::json!({
            "command": "str_replace",
            "path": "src/lib.rs",
            "old_str": "old value",
            "new_str": "new value"
        })
    );
}

#[test]
fn modern_editor_aliases_accept_legacy_edit_keys_and_drop_transport_metadata() {
    let normalized = adapt_tool_input_for_client(
        "str_replace_based_edit_tool",
        serde_json::json!({
            "command": "replace",
            "file_path": "src/main.rs",
            "old_string": "A",
            "new_string": "B",
            "tool_use_id": "transport-only",
            "provider_identifier": "claude-local",
            "debug": true
        }),
    );
    assert_eq!(
        normalized,
        serde_json::json!({
            "command": "str_replace",
            "path": "src/main.rs",
            "old_str": "A",
            "new_str": "B"
        })
    );
}

#[test]
fn modern_editor_commands_preserve_their_operation_specific_fields() {
    let view = adapt_tool_input_for_client(
        "str_replace_based_edit_tool",
        serde_json::json!({
            "command": "view",
            "file_path": "README.md",
            "range": [1, 8],
            "maxChars": 2048,
            "extra": "drop"
        }),
    );
    assert_eq!(
        view,
        serde_json::json!({
            "command": "view",
            "path": "README.md",
            "view_range": [1, 8],
            "max_characters": 2048
        })
    );

    let create = adapt_tool_input_for_client(
        "str_replace_based_edit_tool",
        serde_json::json!({
            "command": "write",
            "path": "new.txt",
            "file_text": "hello\n",
            "trace": 1
        }),
    );
    assert_eq!(
        create,
        serde_json::json!({
            "command": "create",
            "path": "new.txt",
            "file_text": "hello\n"
        })
    );

    let insert = adapt_tool_input_for_client(
        "str_replace_based_edit_tool",
        serde_json::json!({
            "command": "insert",
            "path": "new.txt",
            "line": 0,
            "text": "header\n"
        }),
    );
    assert_eq!(
        insert,
        serde_json::json!({
            "command": "insert",
            "path": "new.txt",
            "insert_line": 0,
            "insert_text": "header\n"
        })
    );
}

#[test]
fn modern_editor_drops_legacy_rename_fields_and_does_not_infer_view() {
    // `rename`/`new_path` are not part of text_editor_20250728. Keep an
    // unsupported command invalid so the bridge validator can discard it;
    // never turn it into a `view` call by guessing from the remaining path.
    let renamed = adapt_tool_input_for_client(
        "str_replace_based_edit_tool",
        serde_json::json!({
            "command": "rename",
            "path": "old.txt",
            "new_path": "new.txt",
            "destination": "new.txt",
            "to": "new.txt",
            "provider_identifier": "claude-local"
        }),
    );
    assert_eq!(
        renamed,
        serde_json::json!({"command": "rename", "path": "old.txt"})
    );

    let path_only = adapt_tool_input_for_client(
        "str_replace_based_edit_tool",
        serde_json::json!({
            "path": "old.txt",
            "new_path": "new.txt"
        }),
    );
    assert_eq!(path_only, serde_json::json!({"path": "old.txt"}));
}

#[test]
fn modern_editor_preserves_empty_old_str_for_insert_semantics() {
    // Claude's editor implementation intentionally accepts an empty old_str
    // (it inserts new_str at the beginning); do not treat an empty string as
    // a missing field while normalizing Cursor PiEdit payloads.
    let normalized = adapt_tool_input_for_client(
        "str_replace_based_edit_tool",
        serde_json::json!({
            "command": "str_replace",
            "path": "src/lib.rs",
            "old_str": "",
            "new_str": "header\\n"
        }),
    );
    assert_eq!(
        normalized,
        serde_json::json!({
            "command": "str_replace",
            "path": "src/lib.rs",
            "old_str": "",
            "new_str": "header\\n"
        })
    );
}

#[test]
fn xml_edit_alias_is_canonicalized_to_advertised_modern_name() {
    let allowed: BTreeSet<String> = ["str_replace_based_edit_tool".to_string()]
        .into_iter()
        .collect();
    let mut parser =
        CursorToolUseXmlParser::new_with_id_factory(Some(allowed), || "edit-call-1".to_string());
    let mut events = parser.push(r#"<tool_use name="Edit">{"#);
    events.extend(
        parser.push(r#""file_path":"src/lib.rs","old_string":"x","new_string":"y"}</tool_use>"#),
    );
    let tool = events
        .into_iter()
        .find_map(|event| match event {
            RecoveredCursorEvent::ToolUse(tool) => Some(tool),
            RecoveredCursorEvent::Text(_) => None,
        })
        .expect("XML Edit call should be recovered");
    assert_eq!(tool.name, "str_replace_based_edit_tool");
    let normalized = adapt_tool_input_for_client(&tool.name, serde_json::Value::Object(tool.input));
    assert_eq!(normalized["command"], "str_replace");
    assert_eq!(normalized["path"], "src/lib.rs");
    assert_eq!(normalized["old_str"], "x");
    assert_eq!(normalized["new_str"], "y");
}

#[test]
fn xml_edit_prefers_modern_handler_when_legacy_edit_is_also_advertised() {
    // This is the exact failure shape seen by Claude Code: the upstream
    // Cursor event is labelled `Edit`, while the client advertises the
    // Anthropic-defined 20250728 handler.  Emitting the legacy name makes the
    // CLI print "Edit unavailable" and switch to StrReplace; the proxy must
    // resolve the event to the exact modern name instead.
    let allowed: BTreeSet<String> = [
        "Edit".to_string(),
        "str_replace_based_edit_tool".to_string(),
    ]
    .into_iter()
    .collect();
    let mut parser =
        CursorToolUseXmlParser::new_with_id_factory(Some(allowed), || "edit-call-both".to_string());
    let events = parser.push(
        r#"<tool_use name="Edit">{"file_path":"src/lib.rs","old_string":"a","new_string":"b"}</tool_use>"#,
    );
    let tool = events
        .into_iter()
        .find_map(|event| match event {
            RecoveredCursorEvent::ToolUse(tool) => Some(tool),
            RecoveredCursorEvent::Text(_) => None,
        })
        .expect("Edit event should be recovered");
    assert_eq!(tool.name, "str_replace_based_edit_tool");
    let input = adapt_tool_input_for_client(&tool.name, serde_json::Value::Object(tool.input));
    assert_eq!(
        input,
        serde_json::json!({
            "command": "str_replace",
            "path": "src/lib.rs",
            "old_str": "a",
            "new_str": "b"
        })
    );
}

#[test]
fn xml_modern_editor_call_survives_split_chunks_without_markup_leak() {
    let allowed: BTreeSet<String> = ["str_replace_based_edit_tool".to_string()]
        .into_iter()
        .collect();
    let mut parser =
        CursorToolUseXmlParser::new_with_id_factory(Some(allowed), || "edit-call-2".to_string());
    let first = parser.push(r#"prefix <tool_use name="str_replace_based_"#);
    assert!(
        first
            .iter()
            .all(|event| !matches!(event, RecoveredCursorEvent::ToolUse(_)))
    );
    let second = parser.push(
        r#"edit_tool">{"command":"str_replace","path":"a","old_str":"1","new_str":"2"}</tool_use> suffix"#,
    );
    let mut all = first;
    all.extend(second);
    all.extend(parser.flush());
    assert!(all.iter().all(|event| match event {
        RecoveredCursorEvent::Text(text) =>
            !text.contains("<tool_use") && !text.contains("</tool_use"),
        RecoveredCursorEvent::ToolUse(_) => true,
    }));
    assert!(all.iter().any(|event| matches!(
        event,
        RecoveredCursorEvent::ToolUse(tool)
            if tool.name == "str_replace_based_edit_tool"
                && tool.input.get("command").and_then(|v| v.as_str()) == Some("str_replace")
    )));
}
