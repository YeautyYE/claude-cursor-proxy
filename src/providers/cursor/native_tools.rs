//! Map Cursor Agent native tool calls (InteractionUpdate / ExecServerMessage)
//! onto Claude Code Anthropic tool_use shapes.

use crate::providers::cursor::proto::{ExecServerMessage, ShellArgs, ToolCall, ToolCallStarted};

/// A tool call ready for Anthropic `tool_use` emission.
#[derive(Debug, Clone)]
pub struct MappedClaudeTool {
    pub tool_use_id: String,
    pub name: String,
    pub input: serde_json::Value,
}

/// Map `tool_call_started` → Claude tool (if we know the shape).
pub fn map_tool_call_started(started: &ToolCallStarted) -> Option<MappedClaudeTool> {
    let call_id = if started.call_id.is_empty() {
        format!("call_cursor_{}", uuid::Uuid::new_v4().simple())
    } else {
        started.call_id.clone()
    };
    let tc = started.tool_call.as_ref()?;
    map_tool_call(tc, call_id)
}

/// Map ExecServerMessage tool args (BiDi exec path) → Claude tool.
pub fn map_exec_server_message(exec: &ExecServerMessage) -> Option<MappedClaudeTool> {
    let id = exec
        .exec_id
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("exec_{}", exec.id));

    if let Some(ref args) = exec.shell_args {
        return Some(map_shell_args(args, id));
    }
    if let Some(ref args) = exec.shell_stream_args {
        return Some(map_shell_args(args, id));
    }
    if let Some(ref args) = exec.read_args {
        let tool_id = if !args.tool_call_id.is_empty() {
            args.tool_call_id.clone()
        } else {
            id
        };
        let mut input = serde_json::json!({ "file_path": args.path });
        if let Some(o) = args.offset {
            input["offset"] = serde_json::json!(o);
        }
        if let Some(l) = args.limit {
            input["limit"] = serde_json::json!(l);
        }
        return Some(MappedClaudeTool {
            tool_use_id: tool_id,
            name: "Read".into(),
            input,
        });
    }
    if let Some(ref args) = exec.write_args {
        return Some(MappedClaudeTool {
            tool_use_id: id,
            name: "Write".into(),
            input: serde_json::json!({
                "file_path": args.path,
                "content": args.file_text,
            }),
        });
    }
    if let Some(ref args) = exec.delete_args {
        // Claude Code often has no Delete — use Bash.
        return Some(MappedClaudeTool {
            tool_use_id: id,
            name: "Bash".into(),
            input: serde_json::json!({
                "command": format!("rm -f -- {}", shell_single_quote(&args.path)),
            }),
        });
    }
    if let Some(ref args) = exec.grep_args {
        return Some(map_grep(
            &args.pattern,
            args.path.as_deref(),
            args.glob.as_deref(),
            args.case_insensitive.unwrap_or(false),
            id,
        ));
    }
    if let Some(ref args) = exec.ls_args {
        return Some(MappedClaudeTool {
            tool_use_id: id,
            name: "Bash".into(),
            input: serde_json::json!({
                "command": format!("ls -la -- {}", shell_single_quote(&args.path)),
            }),
        });
    }
    // request_context_args handled elsewhere — not a user-visible tool.
    None
}

fn map_tool_call(tc: &ToolCall, call_id: String) -> Option<MappedClaudeTool> {
    if let Some(ref shell) = tc.shell_tool_call {
        let args = shell.args.as_ref()?;
        return Some(map_shell_args(args, call_id));
    }
    if let Some(ref read) = tc.read_tool_call {
        let args = read.args.as_ref()?;
        let mut input = serde_json::json!({ "file_path": args.path });
        if let Some(offset) = args.offset {
            input["offset"] = serde_json::json!(offset);
        }
        if let Some(limit) = args.limit {
            input["limit"] = serde_json::json!(limit);
        }
        return Some(MappedClaudeTool {
            tool_use_id: call_id,
            name: "Read".into(),
            input,
        });
    }
    if let Some(ref edit) = tc.edit_tool_call {
        let args = edit.args.as_ref()?;
        // Cursor edit streams content; map to Write when we have content, else Edit-like Write.
        let content = args.stream_content.clone().unwrap_or_default();
        if content.is_empty() {
            // Incomplete edit — still expose as Write with empty content so agent can recover.
            return Some(MappedClaudeTool {
                tool_use_id: call_id,
                name: "Read".into(),
                input: serde_json::json!({ "file_path": args.path }),
            });
        }
        return Some(MappedClaudeTool {
            tool_use_id: call_id,
            name: "Write".into(),
            input: serde_json::json!({
                "file_path": args.path,
                "content": content,
            }),
        });
    }
    if let Some(ref grep) = tc.grep_tool_call {
        let args = grep.args.as_ref()?;
        return Some(map_grep(
            &args.pattern,
            args.path.as_deref(),
            args.glob.as_deref(),
            args.case_insensitive.unwrap_or(false),
            call_id,
        ));
    }
    if let Some(ref glob) = tc.glob_tool_call {
        let args = glob.args.as_ref()?;
        let pattern = args.glob_pattern.clone();
        let dir = args.target_directory.clone().unwrap_or_else(|| ".".into());
        // Prefer Glob if Claude advertises it; Bash find is universal fallback via name Glob
        // (Claude Code has Glob tool) — use Glob-shaped input.
        return Some(MappedClaudeTool {
            tool_use_id: call_id,
            name: "Glob".into(),
            input: serde_json::json!({
                "pattern": pattern,
                "path": dir,
            }),
        });
    }
    if let Some(ref ls) = tc.ls_tool_call {
        let args = ls.args.as_ref()?;
        return Some(MappedClaudeTool {
            tool_use_id: call_id,
            name: "Bash".into(),
            input: serde_json::json!({
                "command": format!("ls -la -- {}", shell_single_quote(&args.path)),
            }),
        });
    }
    if let Some(ref del) = tc.delete_tool_call {
        let args = del.args.as_ref()?;
        return Some(MappedClaudeTool {
            tool_use_id: call_id,
            name: "Bash".into(),
            input: serde_json::json!({
                "command": format!("rm -f -- {}", shell_single_quote(&args.path)),
            }),
        });
    }
    if let Some(ref mcp) = tc.mcp_tool_call {
        let args = mcp.args.as_ref()?;
        let name = if !args.tool_name.is_empty() {
            args.tool_name.clone()
        } else if !args.name.is_empty() {
            args.name.clone()
        } else {
            "mcp_tool".into()
        };
        let mut input = serde_json::Map::new();
        for (k, v) in &args.args {
            input.insert(k.clone(), decode_mcp_arg_value(v));
        }
        if !args.provider_identifier.is_empty() {
            input.insert(
                "provider_identifier".to_string(),
                serde_json::json!(args.provider_identifier),
            );
        }
        return Some(MappedClaudeTool {
            tool_use_id: if args.tool_call_id.is_empty() {
                call_id
            } else {
                args.tool_call_id.clone()
            },
            name,
            input: serde_json::Value::Object(input),
        });
    }
    if let Some(ref todos) = tc.update_todos_tool_call {
        let args = todos.args.as_ref()?;
        let items: Vec<serde_json::Value> = args
            .todos
            .iter()
            .map(|t| {
                serde_json::json!({
                    "id": t.id,
                    "content": t.content,
                    "status": todo_status_name(t.status),
                })
            })
            .collect();
        return Some(MappedClaudeTool {
            tool_use_id: call_id,
            name: "TodoWrite".into(),
            input: serde_json::json!({
                "todos": items,
                "merge": args.merge,
            }),
        });
    }
    if let Some(ref todos) = tc.read_todos_tool_call {
        let args = todos.args.as_ref();
        let mut input = serde_json::json!({});
        if let Some(args) = args
            && !args.id_filter.is_empty()
        {
            input["id_filter"] = serde_json::json!(args.id_filter);
        }
        return Some(MappedClaudeTool {
            tool_use_id: call_id,
            name: "TodoRead".into(),
            input,
        });
    }
    if let Some(ref plan) = tc.create_plan_tool_call {
        let args = plan.args.as_ref()?;
        return Some(MappedClaudeTool {
            tool_use_id: call_id,
            name: "CreatePlan".into(),
            input: serde_json::json!({
                "name": args.name,
                "overview": args.overview,
                "plan": args.plan,
                "is_project": args.is_project,
                "todos": args.todos.iter().map(|t| serde_json::json!({
                    "id": t.id,
                    "content": t.content,
                    "status": todo_status_name(t.status),
                })).collect::<Vec<_>>(),
            }),
        });
    }
    if let Some(ref search) = tc.web_search_tool_call {
        let args = search.args.as_ref()?;
        return Some(MappedClaudeTool {
            tool_use_id: if args.tool_call_id.is_empty() {
                call_id
            } else {
                args.tool_call_id.clone()
            },
            name: "WebSearch".into(),
            input: serde_json::json!({ "query": args.search_term }),
        });
    }
    if let Some(ref fetch) = tc.fetch_tool_call {
        let args = fetch.args.as_ref()?;
        return Some(MappedClaudeTool {
            tool_use_id: if args.tool_call_id.is_empty() {
                call_id
            } else {
                args.tool_call_id.clone()
            },
            name: "WebFetch".into(),
            input: serde_json::json!({ "url": args.url }),
        });
    }
    if let Some(ref ask) = tc.ask_question_tool_call {
        let args = ask.args.as_ref()?;
        let questions: Vec<serde_json::Value> = args
            .questions
            .iter()
            .map(|q| {
                serde_json::json!({
                    "id": q.id,
                    "prompt": q.prompt,
                })
            })
            .collect();
        return Some(MappedClaudeTool {
            tool_use_id: call_id,
            name: "AskUserQuestion".into(),
            input: serde_json::json!({
                "title": args.title,
                "questions": questions,
            }),
        });
    }
    None
}

fn decode_mcp_arg_value(raw: &[u8]) -> serde_json::Value {
    if let Ok(s) = std::str::from_utf8(raw) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            return v;
        }
        return serde_json::Value::String(s.to_string());
    }
    serde_json::Value::String(format!("base64:{}", base64_std(raw)))
}

fn base64_std(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn todo_status_name(status: i32) -> &'static str {
    match status {
        1 => "in_progress",
        2 => "completed",
        _ => "pending",
    }
}

fn map_shell_args(args: &ShellArgs, call_id: String) -> MappedClaudeTool {
    let mut input = serde_json::json!({
        "command": args.command,
    });
    if !args.working_directory.is_empty() {
        // Claude Bash often takes cwd via command prefix; keep both for flexibility.
        input["command"] = serde_json::json!(format!(
            "cd {} && {}",
            shell_single_quote(&args.working_directory),
            args.command
        ));
    }
    if args.timeout > 0 {
        // Cursor ShellArgs.timeout is milliseconds on the exec path.
        input["timeout"] = serde_json::json!(args.timeout as u64);
    }
    MappedClaudeTool {
        tool_use_id: call_id,
        name: "Bash".into(),
        input,
    }
}

fn map_grep(
    pattern: &str,
    path: Option<&str>,
    glob: Option<&str>,
    case_insensitive: bool,
    call_id: String,
) -> MappedClaudeTool {
    let mut input = serde_json::json!({ "pattern": pattern });
    if let Some(p) = path
        && !p.is_empty()
    {
        input["path"] = serde_json::json!(p);
    }
    if let Some(g) = glob
        && !g.is_empty()
    {
        input["glob"] = serde_json::json!(g);
    }
    if case_insensitive {
        input["case_insensitive"] = serde_json::json!(true);
    }
    MappedClaudeTool {
        tool_use_id: call_id,
        name: "Grep".into(),
        input,
    }
}

fn shell_single_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cursor::proto::{ReadToolArgs, ReadToolCall, ShellToolCall};

    #[test]
    fn maps_shell_to_bash() {
        let started = ToolCallStarted {
            call_id: "c1".into(),
            tool_call: Some(ToolCall {
                shell_tool_call: Some(ShellToolCall {
                    args: Some(ShellArgs {
                        command: "ls -la".into(),
                        working_directory: "/tmp".into(),
                        timeout: 30,
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&started).unwrap();
        assert_eq!(m.name, "Bash");
        assert_eq!(m.input["command"], "cd '/tmp' && ls -la");
    }

    #[test]
    fn maps_read_to_read() {
        let started = ToolCallStarted {
            call_id: "r1".into(),
            tool_call: Some(ToolCall {
                read_tool_call: Some(ReadToolCall {
                    args: Some(ReadToolArgs {
                        path: "/a/b.rs".into(),
                        offset: Some(1),
                        limit: Some(50),
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&started).unwrap();
        assert_eq!(m.name, "Read");
        assert_eq!(m.input["file_path"], "/a/b.rs");
    }

    #[test]
    fn maps_read_without_range_omits_optional_fields() {
        let started = ToolCallStarted {
            call_id: "r2".into(),
            tool_call: Some(ToolCall {
                read_tool_call: Some(ReadToolCall {
                    args: Some(ReadToolArgs {
                        path: "/a/README.md".into(),
                        offset: None,
                        limit: None,
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };

        let mapped = map_tool_call_started(&started).unwrap();
        assert_eq!(mapped.name, "Read");
        assert_eq!(mapped.input["file_path"], "/a/README.md");
        assert!(mapped.input.get("offset").is_none());
        assert!(mapped.input.get("limit").is_none());
    }

    #[test]
    fn maps_web_search_and_todos() {
        let search = ToolCallStarted {
            call_id: "s1".into(),
            tool_call: Some(ToolCall {
                web_search_tool_call: Some(crate::providers::cursor::proto::WebSearchToolCall {
                    args: Some(crate::providers::cursor::proto::WebSearchArgs {
                        search_term: "rust async".into(),
                        tool_call_id: "s1".into(),
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&search).unwrap();
        assert_eq!(m.name, "WebSearch");
        assert_eq!(m.input["query"], "rust async");

        let todos = ToolCallStarted {
            call_id: "t1".into(),
            tool_call: Some(ToolCall {
                update_todos_tool_call: Some(
                    crate::providers::cursor::proto::UpdateTodosToolCall {
                        args: Some(crate::providers::cursor::proto::UpdateTodosArgs {
                            todos: vec![crate::providers::cursor::proto::TodoItem {
                                id: "1".into(),
                                content: "ship".into(),
                                status: 1,
                            }],
                            merge: true,
                        }),
                    },
                ),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&todos).unwrap();
        assert_eq!(m.name, "TodoWrite");
        assert_eq!(m.input["merge"], true);
        assert_eq!(m.input["todos"][0]["status"], "in_progress");
    }

    #[test]
    fn maps_mcp_tool_args_as_json() {
        let mut args_map = std::collections::HashMap::new();
        args_map.insert("query".into(), b"\"hello\"".to_vec());
        let started = ToolCallStarted {
            call_id: "m1".into(),
            tool_call: Some(ToolCall {
                mcp_tool_call: Some(crate::providers::cursor::proto::McpToolCall {
                    args: Some(crate::providers::cursor::proto::McpArgs {
                        name: "unused".into(),
                        args: args_map,
                        tool_call_id: "m1".into(),
                        provider_identifier: "plugin".into(),
                        tool_name: "mcp__plugin__search".into(),
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&started).unwrap();
        assert_eq!(m.name, "mcp__plugin__search");
        assert_eq!(m.input["query"], "hello");
    }
}
