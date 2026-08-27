//! Map Cursor Agent native tool calls (InteractionUpdate / ExecServerMessage)
//! onto Claude Code Anthropic tool_use shapes.

use crate::providers::cursor::proto::{
    AskQuestionArgs, ExecServerMessage, FetchArgs, PiEditExecArgs, PiWriteExecArgs, ShellArgs,
    ToolCall, ToolCallStarted,
};
use crate::providers::cursor::request::{is_claude_local_mcp_spelling, is_text_editor_tool_name};

const ASK_USER_QUESTION_HEADER_MAX: usize = 12;

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
        if let Some(offset) = args.offset.filter(|offset| *offset >= 0) {
            // Claude Code's Read contract is non-negative for offset and
            // strictly positive for limit. Cursor's exec protobuf uses a
            // signed offset, so malformed negative values must be omitted.
            input["offset"] = serde_json::json!(offset);
        }
        if let Some(limit) = args.limit.filter(|limit| *limit > 0) {
            input["limit"] = serde_json::json!(limit);
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
    if let Some(ref args) = exec.pi_edit_args {
        return map_pi_edit_args(args, id);
    }
    if let Some(ref args) = exec.pi_write_args {
        return Some(map_pi_write_args(args, id));
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
            name: "LS".into(),
            input: serde_json::json!({ "path": args.path }),
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
        if let Some(offset) = args.offset.filter(|offset| *offset >= 0) {
            input["offset"] = serde_json::json!(offset);
        }
        if let Some(limit) = args.limit.filter(|limit| *limit > 0) {
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
        // Cursor Edit is a full-file overwrite (stream_content), not Claude's
        // old_string/new_string Edit. `stream_content` is optional because a
        // tool_call_started frame can precede the streamed field, but an
        // explicitly present empty string is a valid full-file truncation.
        // Distinguish `None` (incomplete stream) from `Some("")` (clear file)
        // so a legitimate empty overwrite is not silently dropped.
        let Some(content) = args.stream_content.clone() else {
            // Incomplete stream — do not invent a Read/Write; wait for content.
            return None;
        };
        return Some(MappedClaudeTool {
            tool_use_id: call_id,
            name: "Write".into(),
            input: serde_json::json!({
                "file_path": args.path,
                "content": content,
            }),
        });
    }
    if let Some(ref write) = tc.pi_write_tool_call {
        let args = write.args.as_ref()?;
        return Some(map_pi_write_args(
            &crate::providers::cursor::proto::PiWriteExecArgs {
                path: args.path.clone(),
                content: args.content.clone(),
            },
            call_id,
        ));
    }
    if let Some(ref edit) = tc.pi_edit_tool_call {
        let args = edit.args.as_ref()?;
        return map_pi_edit_args(
            &PiEditExecArgs {
                path: args.path.clone(),
                edits: args.edits.clone(),
            },
            call_id,
        );
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
            name: "LS".into(),
            input: serde_json::json!({ "path": args.path }),
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
        let fallback = crate::providers::cursor::proto::McpArgs::default();
        let args = mcp.args.as_ref().unwrap_or(&fallback);
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
        // Cursor's provider_identifier is a wire field, not tool input.
        // Workflow schema is `{name, args}` with additionalProperties: false.
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
        return map_fetch_args(fetch.args.as_ref()?, call_id);
    }
    if let Some(ref fetch) = tc.web_fetch_tool_call {
        return map_fetch_args(fetch.args.as_ref()?, call_id);
    }
    if let Some(ref task) = tc.task_tool_call {
        let mut input = serde_json::Map::new();
        if let Some(args) = task.args.as_ref() {
            if !args.description.is_empty() {
                input.insert("description".into(), serde_json::json!(args.description));
            }
            if !args.prompt.is_empty() {
                input.insert("prompt".into(), serde_json::json!(args.prompt));
            }
            if !args.subagent_type.is_empty() {
                input.insert(
                    "subagent_type".into(),
                    serde_json::json!(args.subagent_type),
                );
            }
            if let Some(model) = args
                .model
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                // Preserve the native model override. The Claude-facing
                // adapter validates its enum; the Grok spawn adapter keeps
                // only model IDs it can route downstream.
                input.insert("model".into(), serde_json::json!(model));
            }
            if let Some(resume) = args
                .resume
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                // Keep Cursor's resume identity in the intermediate mapped
                // shape. The Claude-facing adapter removes it, while the
                // Grok spawn adapter translates it to `resume_from`.
                input.insert("resume".into(), serde_json::json!(resume));
            }
            if let Some(background) = args.run_in_background {
                input.insert("run_in_background".into(), serde_json::json!(background));
            }
        }
        return Some(MappedClaudeTool {
            tool_use_id: call_id,
            name: "Task".into(),
            input: serde_json::Value::Object(input),
        });
    }
    if let Some(ref ask) = tc.ask_question_tool_call {
        let args = ask.args.as_ref()?;
        return Some(MappedClaudeTool {
            tool_use_id: call_id,
            name: "AskUserQuestion".into(),
            input: map_ask_question_args(args),
        });
    }
    None
}

/// Convert Cursor's native AskQuestion args to Claude Code 2.1.193's
/// AskUserQuestion schema. Cursor carries stable option ids and an
/// `allow_multiple` flag; Claude Code presents labels and calls the latter
/// `multiSelect`. The title is a Cursor UI field and is folded into the short
/// question header because Claude's schema has no top-level title.
pub(crate) fn map_ask_question_args(args: &AskQuestionArgs) -> serde_json::Value {
    let title = args.title.trim();
    let title_header = truncate_ask_header(title);
    let mut questions = Vec::new();

    for item in args.questions.iter().take(4) {
        let mut question = item.prompt.trim().to_string();
        if question.is_empty() {
            question = title.to_string();
        }
        if question.is_empty() {
            question = "Continue?".into();
        }
        if !question.ends_with('?') {
            question.push('?');
        }
        let header = if title_header.is_empty() {
            truncate_ask_header(&question)
        } else {
            title_header.clone()
        };
        let options = map_ask_question_options(&item.options);
        questions.push(serde_json::json!({
            "question": question,
            "header": header,
            "options": options,
            "multiSelect": item.allow_multiple,
        }));
    }

    if questions.is_empty() {
        let question = if title.is_empty() {
            "Continue?".to_string()
        } else if title.ends_with('?') {
            title.to_string()
        } else {
            format!("{title}?")
        };
        questions.push(serde_json::json!({
            "question": question,
            "header": if title_header.is_empty() {
                truncate_ask_header(&question)
            } else {
                title_header
            },
            "options": default_ask_question_options(),
            "multiSelect": false,
        }));
    }

    serde_json::json!({ "questions": questions })
}

fn map_ask_question_options(
    options: &[crate::providers::cursor::proto::AskQuestionOption],
) -> Vec<serde_json::Value> {
    if !(2..=4).contains(&options.len()) {
        return default_ask_question_options();
    }
    options
        .iter()
        .enumerate()
        .map(|(index, option)| {
            let label = if option.label.trim().is_empty() {
                option.id.trim()
            } else {
                option.label.trim()
            };
            let label = if label.is_empty() {
                format!("Option {}", index + 1)
            } else {
                label.to_string()
            };
            serde_json::json!({
                "label": label,
                "description": format!("Select {label}"),
            })
        })
        .collect()
}

fn default_ask_question_options() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "label": "Continue",
            "description": "Accept this option and continue",
        }),
        serde_json::json!({
            "label": "Skip",
            "description": "Skip this question",
        }),
    ]
}

fn truncate_ask_header(text: &str) -> String {
    text.chars().take(ASK_USER_QUESTION_HEADER_MAX).collect()
}

fn map_pi_write_args(args: &PiWriteExecArgs, tool_use_id: String) -> MappedClaudeTool {
    MappedClaudeTool {
        tool_use_id,
        name: "Write".into(),
        input: serde_json::json!({
            "file_path": args.path,
            "content": args.content,
        }),
    }
}

/// Map Cursor's modern Pi string-replacement edit into Claude Code's native
/// Edit/MultiEdit contract.  A single replacement uses `Edit`; preserving a
/// multi-replacement call as `MultiEdit` avoids dropping atomic edits or
/// forcing the model to issue several independent tool calls.
fn map_pi_edit_args(args: &PiEditExecArgs, tool_use_id: String) -> Option<MappedClaudeTool> {
    if args.path.trim().is_empty() || args.edits.is_empty() {
        // Empty Pi edits/path values are incomplete or invalid stream
        // markers. Returning no tool prevents a fabricated editor call with
        // missing replacement data (an empty old/new string itself remains a
        // valid insertion/deletion and is intentionally preserved).
        return None;
    }
    if args.edits.len() == 1 {
        let edit = &args.edits[0];
        return Some(MappedClaudeTool {
            tool_use_id,
            name: "Edit".into(),
            input: serde_json::json!({
                "file_path": args.path,
                "old_string": edit.old_text,
                "new_string": edit.new_text,
            }),
        });
    }
    let edits: Vec<serde_json::Value> = args
        .edits
        .iter()
        .map(|edit| {
            serde_json::json!({
                "old_string": edit.old_text,
                "new_string": edit.new_text,
            })
        })
        .collect();
    Some(MappedClaudeTool {
        tool_use_id,
        name: "MultiEdit".into(),
        input: serde_json::json!({
            "file_path": args.path,
            "edits": edits,
        }),
    })
}

fn map_fetch_args(args: &FetchArgs, call_id: String) -> Option<MappedClaudeTool> {
    Some(MappedClaudeTool {
        tool_use_id: if args.tool_call_id.is_empty() {
            call_id
        } else {
            args.tool_call_id.clone()
        },
        name: "WebFetch".into(),
        input: serde_json::json!({ "url": args.url }),
    })
}

/// Fill empty/partial MCP tool input from `partial_tool_call.args_text_delta`.
///
/// Cursor documents that field as aggregated JSON text so far. Incomplete JSON
/// is ignored so we never invent keys.
pub fn merge_partial_args_json(mapped: &mut MappedClaudeTool, args_text: &str) -> bool {
    let text = args_text.trim();
    if text.is_empty() {
        return false;
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return false;
    };
    let empty = mapped.input.is_null()
        || mapped
            .input
            .as_object()
            .is_some_and(serde_json::Map::is_empty);
    match (&mut mapped.input, value) {
        (serde_json::Value::Object(dst), serde_json::Value::Object(src)) => {
            if dst.is_empty() {
                *dst = src;
                return true;
            }
            let mut changed = false;
            for (key, val) in src {
                dst.entry(key).or_insert_with(|| {
                    changed = true;
                    val
                });
            }
            changed
        }
        (dst, src) if empty => {
            *dst = src;
            true
        }
        _ => false,
    }
}

/// Snapshot-or-append `args_text_delta` chunks. JSON objects/arrays replace when
/// they are at least as long as the buffer (aggregated snapshots); fragments append.
pub fn accumulate_partial_args_text(dst: &mut String, incoming: &str) {
    if incoming.is_empty() {
        return;
    }
    let trimmed = incoming.trim_start();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        if incoming.len() >= dst.len() || serde_json::from_str::<serde_json::Value>(dst).is_err() {
            *dst = incoming.to_string();
        }
        return;
    }
    dst.push_str(incoming);
}

fn decode_mcp_arg_value(raw: &[u8]) -> serde_json::Value {
    if raw.is_empty() {
        return serde_json::Value::Null;
    }
    if let Ok(s) = std::str::from_utf8(raw)
        && let Ok(v) = serde_json::from_str::<serde_json::Value>(s)
    {
        return v;
    }
    if let Some(v) = decode_protobuf_value_bytes(raw) {
        return v;
    }
    if let Ok(s) = std::str::from_utf8(raw) {
        return serde_json::Value::String(s.to_string());
    }
    serde_json::Value::String(format!("base64:{}", base64_std(raw)))
}

/// Live Cursor 2026-08 encodes each `McpArgs` map value as
/// `google.protobuf.Value` (`string_value=3` → `0x1a…`, `bool_value=4` →
/// `0x20 0x01`). Treating those bytes as UTF-8 leaves the tag prefix in
/// grok-build input (`\x1a\x10SPAWN smoke test`) and the child never starts.
fn decode_protobuf_value_bytes(raw: &[u8]) -> Option<serde_json::Value> {
    use prost::Message;
    let first = *raw.first()?;
    match first {
        0x08 | 0x11 | 0x1a | 0x2a | 0x32 => {}
        0x20 if raw.len() == 2 && matches!(raw[1], 0x00 | 0x01) => {}
        _ => return None,
    }
    let value = prost_types::Value::decode(raw).ok()?;
    value.kind.as_ref()?;
    prost_value_to_json(&value)
}

fn json_number_from_f64(n: f64) -> Option<serde_json::Value> {
    if !n.is_finite() {
        return None;
    }
    if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
        return Some(serde_json::Value::Number(serde_json::Number::from(
            n as i64,
        )));
    }
    Some(serde_json::Value::Number(serde_json::Number::from_f64(n)?))
}

fn prost_value_to_json(value: &prost_types::Value) -> Option<serde_json::Value> {
    use prost_types::value::Kind;
    Some(match value.kind.as_ref()? {
        Kind::NullValue(_) => serde_json::Value::Null,
        Kind::NumberValue(n) => json_number_from_f64(*n)?,
        Kind::StringValue(s) => serde_json::Value::String(s.clone()),
        Kind::BoolValue(b) => serde_json::Value::Bool(*b),
        Kind::StructValue(s) => {
            let mut map = serde_json::Map::new();
            for (k, v) in &s.fields {
                map.insert(k.clone(), prost_value_to_json(v)?);
            }
            serde_json::Value::Object(map)
        }
        Kind::ListValue(l) => serde_json::Value::Array(
            l.values
                .iter()
                .map(prost_value_to_json)
                .collect::<Option<Vec<_>>>()?,
        ),
    })
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
        // Cursor CLI `ShellArgs.timeout` is milliseconds (same as Claude Bash).
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

/// Quote one shell argument without allowing its contents to terminate the
/// surrounding single-quoted string.  Cursor sends cwd/path values through
/// the Claude Bash tool, so this must also handle apostrophes in paths.
pub(crate) fn shell_single_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Cursor's Delete exec is represented as a synthetic POSIX `rm` command when
/// it must be exposed through Claude's shell-shaped tool. PowerShell does not
/// understand that command, so translate only this exact proxy-generated
/// prefix and preserve paths containing apostrophes.
fn rewrite_cursor_delete_for_powershell(command: &str) -> Option<String> {
    const PREFIX: &str = "rm -f -- ";
    let quoted = command.strip_prefix(PREFIX)?.trim();
    let path = unquote_posix_single(quoted)?;
    Some(format!(
        "Remove-Item -Force -LiteralPath {}",
        powershell_single_quote(&path)
    ))
}

fn unquote_posix_single(value: &str) -> Option<String> {
    if value.len() < 2 || !value.starts_with('\'') || !value.ends_with('\'') {
        return None;
    }
    let inner = &value[1..value.len() - 1];
    let mut out = String::with_capacity(inner.len());
    let mut rest = inner;
    while let Some(pos) = rest.find('\'') {
        out.push_str(&rest[..pos]);
        rest = &rest[pos + 1..];
        let after_escape = rest.strip_prefix("\\''")?;
        out.push('\'');
        rest = after_escape;
    }
    out.push_str(rest);
    Some(out)
}

fn powershell_single_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Read an integer supplied by protobuf/JSON adapters.  `google.protobuf.Value`
/// numbers arrive as f64 and some clients serialize timeout values as strings;
/// accepting whole-number representations keeps the Claude Bash contract
/// stable while still rejecting fractions and negative values.
pub(crate) fn json_u64(value: &serde_json::Value) -> Option<u64> {
    coerce_whole_integer(value).and_then(|value| value.as_u64())
}

/// Advertised-name aliases for Cursor native tools, including grok-build
/// wire names (`read_file`, `run_terminal_command`, `list_dir`, `spawn_subagent`).
///
/// Order is preference: exact Claude/Cursor name first, then grok-build
/// `client_name`s from `xai-grok-tools` `claude_alias.rs`.
pub fn advertised_name_fallbacks(mapped_name: &str) -> &'static [&'static str] {
    match mapped_name {
        "Bash" => &[
            "Bash",
            "PowerShell",
            "Shell",
            "bash",
            "run_terminal_command",
            "run_terminal_cmd",
        ],
        "Read" | "read" | "read_file" | "ReadFile" => &["Read", "read", "read_file", "ReadFile"],
        "Write" | "write" | "write_file" | "WriteFile" => {
            &["Write", "write", "write_file", "WriteFile"]
        }
        // PiEdit's canonical mapping is Claude `Edit`; when the client only
        // advertises Anthropic's schema-less editor, resolve to that exact
        // name instead of dropping the call as "not advertised".
        "Edit"
        | "edit"
        | "StrReplace"
        | "StrReplaceTool"
        | "str_replace_based_edit_tool"
        | "str_replace_editor" => &[
            "Edit",
            "str_replace_based_edit_tool",
            "StrReplace",
            "StrReplaceTool",
            "str_replace_editor",
        ],
        // `MultiEdit` has a distinct array payload.  Do not alias it to the
        // single-operation schema-less editor here: doing so would discard
        // the `edits` array (or expose only a fabricated `view` call).  Native
        // PiEdit multi-replacements are expanded explicitly in the live path;
        // an XML MultiEdit call requires the legacy handler to be advertised.
        "MultiEdit" => &["MultiEdit"],
        "Grep" => &["Grep", "grep", "Search"],
        "Glob" => &["Glob", "glob", "Find", "list_dir"],
        "LS" | "Ls" => &[
            "LS",
            "Ls",
            "list_dir",
            "Bash",
            "run_terminal_command",
            "Glob",
        ],
        "WebSearch" => &["WebSearch", "web_search"],
        "WebFetch" => &["WebFetch", "web_fetch", "Fetch"],
        "TodoWrite" => &["TodoWrite", "todo_write"],
        "TodoRead" => &["TodoRead"],
        "AskUserQuestion" => &["AskUserQuestion", "AskQuestion", "ask_user_question"],
        // Cursor CreatePlan already carries the completed plan body and asks
        // the client to present it. That is an exit/submit operation, not an
        // EnterPlanMode transition (whose input is deliberately empty).
        // Mapping it to EnterPlanMode discarded the plan and could send the
        // model around the planning loop again.
        "CreatePlan" => &["CreatePlan", "Plan", "ExitPlanMode", "exit_plan_mode"],
        "Task" => &["Task", "spawn_subagent", "Agent", "task"],
        "TaskOutput"
        | "BashOutput"
        | "BashOutputTool"
        | "AgentOutputTool"
        | "AgentOutput"
        | "get_command_or_subagent_output"
        | "get_terminal_command_output"
        | "wait_commands_or_subagents" => &[
            // Claude Code 2.1.193 exposes TaskOutput and keeps the four
            // historical aliases on the same implementation. Grok-build's
            // lifecycle names are included in both directions so a native
            // Cursor event can resolve against whichever spelling the client
            // advertised.
            "TaskOutput",
            "BashOutput",
            "BashOutputTool",
            "AgentOutputTool",
            "AgentOutput",
            "get_command_or_subagent_output",
            "get_terminal_command_output",
            "wait_commands_or_subagents",
        ],
        "TaskStop"
        | "KillShell"
        | "KillBash"
        | "kill_command_or_subagent"
        | "kill_terminal_command" => &[
            "TaskStop",
            "KillShell",
            "KillBash",
            "kill_command_or_subagent",
            "kill_terminal_command",
        ],
        // Claude-local tools can be surfaced through both MCP and XML paths.
        // Cursor occasionally returns a runtime alias rather than the exact
        // spelling from Anthropic's catalog; share Claude Code's explicit
        // alias families with every advertised-name resolver.
        _ => crate::providers::cursor::request::claude_tool_aliases(mapped_name),
    }
}

/// Rewrite Cursor/Claude input keys to the schema the downstream client
/// advertised. grok-build validates `read_file.target_file` and rejects
/// `file_path`-only payloads. MCP `google.protobuf.Value` numbers arrive as
/// f64; grok-build `timeout_ms: Option<u64>` rejects those floats.
pub fn adapt_tool_input_for_client(
    advertised_name: &str,
    mut input: serde_json::Value,
) -> serde_json::Value {
    // Older Claude clients keep the `mcp_claude-local_` provider prefix in
    // tool_result history. Apply the same schema adapter to that spelling as
    // to the current bare tool name; foreign MCP providers remain untouched.
    // A provider-qualified foreign MCP name is opaque to this adapter.  Its
    // leaf may happen to match a Claude/Cursor built-in (for example
    // `other/read_file` or `plugin/Agent`), but applying the built-in schema
    // would silently rewrite an unrelated provider's payload.  Only the
    // explicitly recognized `claude-local` spellings are unwrapped here.
    if is_foreign_qualified_mcp_name(advertised_name) {
        return input;
    }
    let schema_name = crate::providers::cursor::request::strip_mcp_provider_prefix(advertised_name);
    if schema_name == "spawn_subagent" {
        return adapt_native_task_to_spawn_subagent(input);
    }
    if matches!(schema_name, "Agent" | "Task") {
        return adapt_claude_agent_input(input);
    }
    let Some(obj) = input.as_object_mut() else {
        return input;
    };
    match schema_name {
        "Read" => {
            coerce_integer_fields(obj, &["offset", "limit"]);
            normalize_read_range(obj, "offset", "limit");
        }
        "Edit" => {
            // Claude Code's Edit schema is strict. Cursor/MCP wrappers may
            // attach transport metadata (or use `path`/`content` aliases),
            // but none of that belongs in the Claude tool input.
            copy_alias_if_missing(obj, "file_path", &["path", "target_file"]);
            copy_alias_if_missing(obj, "old_string", &["oldText", "old_text"]);
            copy_alias_if_missing(obj, "new_string", &["newText", "new_text", "content"]);
            copy_alias_if_missing(obj, "replace_all", &["replaceAll"]);
            retain_object_keys(
                obj,
                &["file_path", "old_string", "new_string", "replace_all"],
            );
        }
        "MultiEdit" => {
            copy_alias_if_missing(obj, "file_path", &["path", "target_file"]);
            copy_alias_if_missing(obj, "replace_all", &["replaceAll"]);
            retain_object_keys(obj, &["file_path", "edits", "replace_all"]);
            if let Some(serde_json::Value::Array(edits)) = obj.get_mut("edits") {
                for edit in edits {
                    if let Some(edit) = edit.as_object_mut() {
                        copy_alias_if_missing(edit, "old_string", &["oldText", "old_text"]);
                        copy_alias_if_missing(edit, "new_string", &["newText", "new_text"]);
                        copy_alias_if_missing(edit, "replace_all", &["replaceAll"]);
                        retain_object_keys(edit, &["old_string", "new_string", "replace_all"]);
                    }
                }
            }
        }
        "NotebookEdit" => {
            normalize_notebook_edit(obj);
        }
        "read_file" | "ReadFile" => {
            coerce_integer_fields(obj, &["offset", "limit"]);
            normalize_read_range(obj, "offset", "limit");
            if !has_nonempty_str(obj, "target_file") {
                if let Some(path) = obj.get("file_path").or_else(|| obj.get("path")).cloned() {
                    obj.insert("target_file".into(), path);
                }
            }
            obj.remove("file_path");
            obj.remove("path");
        }
        "list_dir" => {
            if !has_nonempty_str(obj, "target_directory") {
                if let Some(path) = obj.get("path").or_else(|| obj.get("target_file")).cloned() {
                    obj.insert("target_directory".into(), path);
                }
            }
            obj.remove("path");
            obj.remove("pattern");
        }
        "Glob" | "glob" => {
            if !has_nonempty_str(obj, "pattern") {
                obj.insert("pattern".into(), serde_json::json!("*"));
            }
        }
        "run_terminal_command" | "run_terminal_cmd" => {
            coerce_integer_fields(obj, &["timeout"]);
            adapt_shell_like(obj, true, false);
        }
        "Bash" | "PowerShell" | "Shell" | "bash" => {
            coerce_integer_fields(obj, &["timeout"]);
            if schema_name == "PowerShell"
                && let Some(command) = obj.get("command").and_then(|value| value.as_str())
                && let Some(command) = rewrite_cursor_delete_for_powershell(command)
            {
                obj.insert("command".into(), serde_json::Value::String(command));
            }
            // Cursor ShellArgs has no description. Claude Code uses it as the
            // Bash widget title; without it the TUI dumps the whole command
            // (including python3 -c bodies) into the header.
            adapt_shell_like(obj, true, true);
        }
        "write" | "write_file" | "WriteFile" => {
            if !has_nonempty_str(obj, "file_path") {
                if let Some(path) = obj.get("path").or_else(|| obj.get("target_file")).cloned() {
                    obj.insert("file_path".into(), path);
                }
            }
            if !has_nonempty_str(obj, "content") {
                if let Some(content) = obj
                    .get("file_text")
                    .or_else(|| obj.get("contents"))
                    .cloned()
                {
                    obj.insert("content".into(), content);
                }
            }
            obj.remove("path");
            obj.remove("file_text");
            obj.remove("contents");
        }
        name if is_text_editor_tool_name(name) => {
            normalize_text_editor_input(obj);
        }
        "grep" => {
            coerce_integer_fields(obj, &["head_limit", "-B", "-A", "-C"]);
            if let Some(flag) = obj.remove("case_insensitive") {
                obj.insert("-i".into(), flag);
            }
        }
        "Grep" => {
            coerce_integer_fields(obj, &["head_limit", "-B", "-A", "-C"]);
            if let Some(flag) = obj.remove("case_insensitive") {
                obj.insert("-i".into(), flag);
            }
        }
        "web_search" => {
            if !has_nonempty_str(obj, "query") {
                if let Some(query) = obj.get("search_term").cloned() {
                    obj.insert("query".into(), query);
                }
            }
            obj.remove("search_term");
        }
        "WebFetch" | "Fetch" => {
            if !has_nonempty_str(obj, "prompt") {
                obj.insert(
                    "prompt".into(),
                    serde_json::json!("Extract the main content from this URL."),
                );
            }
        }
        "TodoWrite" => {
            if let Some(serde_json::Value::Array(todos)) = obj.get_mut("todos") {
                for todo in todos {
                    let Some(item) = todo.as_object_mut() else {
                        continue;
                    };
                    // Cursor includes an internal id and may send an empty or
                    // unknown status. Claude Code 2.1.193's strict TodoWrite
                    // item contract only accepts content/status/activeForm.
                    item.remove("id");
                    let content = item
                        .get("content")
                        .and_then(|value| value.as_str())
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .unwrap_or("todo")
                        .to_string();
                    item.insert("content".into(), serde_json::json!(content));
                    let status = item
                        .get("status")
                        .and_then(|value| value.as_str())
                        .filter(|status| matches!(*status, "pending" | "in_progress" | "completed"))
                        .unwrap_or("pending")
                        .to_string();
                    item.insert("status".into(), serde_json::json!(status));
                    if !has_nonempty_str(item, "activeForm") {
                        item.insert(
                            "activeForm".into(),
                            serde_json::json!(format!("Working on {content}")),
                        );
                    }
                }
            }
            obj.remove("merge");
        }
        "get_command_or_subagent_output"
        | "get_terminal_command_output"
        | "wait_commands_or_subagents" => {
            coerce_integer_fields(obj, &["timeout_ms"]);
            adapt_task_ids_list(obj);
        }
        "TaskOutput" | "BashOutput" | "BashOutputTool" | "AgentOutputTool" | "AgentOutput" => {
            normalize_task_output(obj);
        }
        "kill_command_or_subagent" | "kill_terminal_command" => {
            normalize_grok_task_stop(obj);
        }
        "TaskStop" | "KillShell" | "KillBash" => {
            normalize_claude_task_stop(obj);
        }
        "enter_plan_mode" | "EnterPlanMode" | "exit_plan_mode" => {
            obj.clear();
        }
        "ExitPlanMode" => {
            // Claude Code 2.1.x accepts an injected `plan` field even though
            // the public model-facing schema normally asks it to write the
            // plan file first. Cursor's CreatePlan is inline, so preserve only
            // that body (plus Claude's optional permission hints) and drop
            // Cursor UI metadata such as name/overview/todos.
            retain_object_keys(obj, &["plan", "allowedPrompts"]);
        }
        _ => {}
    }
    input
}

/// Single entry for ClientOnly / XML / MCP expose: spawn uses the allowlist
/// rebuild; everything else uses advertised-name schema adaptation.
pub fn adapt_client_tool_input(
    advertised_name: &str,
    input: serde_json::Value,
) -> serde_json::Value {
    if advertised_name == "spawn_subagent" {
        adapt_native_task_to_spawn_subagent(input)
    } else {
        adapt_tool_input_for_client(advertised_name, input)
    }
}

/// Normalize an Anthropic text-editor invocation for Claude Code's local
/// handler. The 20250728 contract is schema-less at the API boundary but uses
/// a fixed command/field vocabulary. Cursor/Grok may send Claude Edit aliases;
/// canonicalize those aliases and drop transport metadata before exposing the
/// client-only tool.
fn normalize_text_editor_input(obj: &mut serde_json::Map<String, serde_json::Value>) {
    copy_alias_if_missing(obj, "path", &["file_path", "target_file"]);
    copy_alias_if_missing(obj, "old_str", &["old_string", "oldText", "old_text"]);
    copy_alias_if_missing(
        obj,
        "new_str",
        &["new_string", "newText", "new_text", "content"],
    );
    copy_alias_if_missing(obj, "file_text", &["contents", "file_content"]);
    copy_alias_if_missing(obj, "insert_text", &["text"]);
    copy_alias_if_missing(obj, "insert_line", &["line", "line_number"]);
    copy_alias_if_missing(obj, "view_range", &["range"]);
    copy_alias_if_missing(obj, "max_characters", &["maxChars", "max_chars"]);

    // Normalize common command spellings. PiEdit → text-editor fallback has
    // no command field, so infer it from the operation-specific arguments.
    if let Some(command) = obj.get("command").and_then(|value| value.as_str()) {
        let normalized = match command {
            "strReplace" | "replace" | "replace_string" => Some("str_replace"),
            "new" | "write" | "overwrite" => Some("create"),
            _ => None,
        };
        if let Some(normalized) = normalized {
            obj.insert(
                "command".into(),
                serde_json::Value::String(normalized.into()),
            );
        }
    }
    if !obj.contains_key("command") {
        // Infer a command only when an operation-specific field is present.
        // A path-only (or path + unknown `new_path`) payload is incomplete;
        // defaulting it to `view` would turn an unsupported rename/malformed
        // call into a seemingly valid editor invocation. The XML bridge's
        // schema validator will then discard the incomplete call instead of
        // handing Claude Code a misleading tool_use.
        let command = if obj.contains_key("old_str") || obj.contains_key("new_str") {
            Some("str_replace")
        } else if obj.contains_key("file_text") {
            Some("create")
        } else if obj.contains_key("insert_text") || obj.contains_key("insert_line") {
            Some("insert")
        } else {
            None
        };
        if let Some(command) = command {
            obj.insert("command".into(), serde_json::Value::String(command.into()));
        }
    }

    retain_object_keys(
        obj,
        &[
            "command",
            "path",
            "old_str",
            "new_str",
            "file_text",
            "insert_line",
            "insert_text",
            "view_range",
            "max_characters",
        ],
    );
}

pub fn glob_pattern_is_directory_listing(input: &serde_json::Value) -> bool {
    matches!(
        input
            .get("pattern")
            .and_then(|value| value.as_str())
            .map(str::trim),
        None | Some("") | Some("*") | Some(".") | Some("**") | Some("**/*") | Some("*/*")
    )
}

pub fn resolve_glob_client_name(
    input: &serde_json::Value,
    resolved_glob: Option<String>,
    resolved_shell: Option<String>,
) -> Option<String> {
    let name = resolved_glob?;
    if name == "list_dir" && !glob_pattern_is_directory_listing(input) {
        return resolved_shell;
    }
    Some(name)
}

fn has_nonempty_str(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> bool {
    obj.get(key)
        .and_then(|value| value.as_str())
        .is_some_and(|value| !value.is_empty())
}

fn is_foreign_qualified_mcp_name(name: &str) -> bool {
    if is_claude_local_mcp_spelling(name) {
        return false;
    }
    if let Some(rest) = name.strip_prefix("mcp__") {
        return rest
            .split_once("__")
            .is_some_and(|(provider, tool)| !provider.is_empty() && !tool.is_empty());
    }
    name.split_once('/')
        .or_else(|| name.split_once(':'))
        .is_some_and(|(provider, tool)| !provider.is_empty() && !tool.is_empty())
}

fn coerce_integer_fields(obj: &mut serde_json::Map<String, serde_json::Value>, keys: &[&str]) {
    for key in keys {
        let Some(value) = obj.get(*key) else {
            continue;
        };
        match coerce_whole_integer(value) {
            Some(integer) => {
                obj.insert((*key).into(), integer);
            }
            None => {
                obj.remove(*key);
            }
        }
    }
}

/// Enforce Claude Code Read's range constraints after integer coercion.
/// Invalid optional fields are omitted so Claude applies its normal defaults;
/// clamping would silently change a caller's requested range.
fn normalize_read_range(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    offset_key: &str,
    limit_key: &str,
) {
    if obj
        .get(offset_key)
        .is_some_and(|value| value.as_i64().is_some_and(|offset| offset < 0))
    {
        obj.remove(offset_key);
    }
    if obj
        .get(limit_key)
        .is_some_and(|value| value.as_i64().is_some_and(|limit| limit <= 0))
    {
        obj.remove(limit_key);
    }
}

/// Keep only fields that belong to a strict Claude tool schema.  The
/// downstream Claude Code validator uses `strictObject` for several built-ins;
/// forwarding Cursor transport metadata causes the whole tool call to be
/// rejected even when its required fields are valid.
fn retain_object_keys(obj: &mut serde_json::Map<String, serde_json::Value>, allowed: &[&str]) {
    obj.retain(|key, _| allowed.contains(&key.as_str()));
}

fn copy_alias_if_missing(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    canonical: &str,
    aliases: &[&str],
) {
    if obj.contains_key(canonical) {
        return;
    }
    if let Some(value) = aliases.iter().find_map(|alias| obj.get(*alias).cloned()) {
        obj.insert(canonical.into(), value);
    }
}

fn normalize_notebook_edit(obj: &mut serde_json::Map<String, serde_json::Value>) {
    // A few older clients used the generic `path`/`source` spellings. Preserve
    // a canonical value before removing those aliases from the strict schema.
    if !has_nonempty_str(obj, "notebook_path")
        && let Some(path) = obj.get("path").cloned()
    {
        obj.insert("notebook_path".into(), path);
    }
    if !obj.contains_key("new_source")
        && let Some(source) = obj.get("source").cloned()
    {
        obj.insert("new_source".into(), source);
    }

    // Claude Code treats `cell-N` as an index for compatibility with old
    // notebooks. Convert the former numeric `cell_number` field into that
    // string selector before applying the strict key allow-list. Numeric
    // `cell_id` values are handled the same way.
    let cell_id_is_canonical = obj
        .get("cell_id")
        .and_then(|value| value.as_str())
        .is_some_and(|id| !id.trim().is_empty() && !is_numeric_cell_id(id));
    if !cell_id_is_canonical {
        let legacy_index = obj
            .get("cell_number")
            .and_then(legacy_cell_index)
            .or_else(|| obj.get("cell_id").and_then(legacy_cell_index));
        if let Some(index) = legacy_index {
            obj.insert("cell_id".into(), serde_json::json!(format!("cell-{index}")));
        }
    }
    retain_object_keys(
        obj,
        &[
            "notebook_path",
            "cell_id",
            "new_source",
            "cell_type",
            "edit_mode",
        ],
    );
}

fn is_numeric_cell_id(value: &str) -> bool {
    value.trim().parse::<i64>().is_ok()
}

fn legacy_cell_index(value: &serde_json::Value) -> Option<i64> {
    if let Some(index) = value.as_i64() {
        return (index >= 0).then_some(index);
    }
    if let Some(index) = value.as_u64() {
        return i64::try_from(index).ok();
    }
    if let Some(index) = value.as_f64()
        && index.is_finite()
        && index.fract() == 0.0
        && index >= 0.0
        && index <= i64::MAX as f64
    {
        return Some(index as i64);
    }
    value
        .as_str()
        .and_then(|text| text.trim().parse::<i64>().ok())
        .filter(|index| *index >= 0)
}

fn normalize_task_output(obj: &mut serde_json::Map<String, serde_json::Value>) {
    if let Some(task_id) = obj.get("task_id").and_then(task_id_string) {
        obj.insert("task_id".into(), serde_json::json!(task_id));
    }
    if !has_nonempty_str(obj, "task_id") {
        let candidate = obj
            .get("task_ids")
            .and_then(|value| value.as_array())
            .and_then(|ids| ids.iter().find_map(task_id_string))
            .or_else(|| obj.get("shell_id").and_then(task_id_string));
        if let Some(task_id) = candidate {
            obj.insert("task_id".into(), serde_json::json!(task_id));
        }
    }
    if !obj.get("block").is_some_and(serde_json::Value::is_boolean) {
        let block = obj
            .get("wait")
            .and_then(bool_value)
            .or_else(|| obj.get("blocking").and_then(bool_value));
        if let Some(block) = block {
            obj.insert("block".into(), serde_json::json!(block));
        }
    }
    if !obj.contains_key("timeout")
        && let Some(timeout) = obj.get("timeout_ms").and_then(coerce_timeout_value)
    {
        obj.insert("timeout".into(), timeout);
    }
    if let Some(timeout) = obj.get("timeout").cloned() {
        let valid = timeout.as_u64().is_some_and(|timeout| timeout <= 600_000)
            || timeout
                .as_i64()
                .is_some_and(|timeout| (0..=600_000).contains(&timeout))
            || timeout
                .as_f64()
                .is_some_and(|timeout| timeout.is_finite() && (0.0..=600_000.0).contains(&timeout));
        if !valid {
            obj.remove("timeout");
        }
    }
    retain_object_keys(obj, &["task_id", "block", "timeout"]);
}

fn normalize_claude_task_stop(obj: &mut serde_json::Map<String, serde_json::Value>) {
    if let Some(task_id) = obj.get("task_id").and_then(task_id_string) {
        obj.insert("task_id".into(), serde_json::json!(task_id));
    }
    if let Some(shell_id) = obj.get("shell_id").and_then(task_id_string) {
        obj.insert("shell_id".into(), serde_json::json!(shell_id));
    }
    if !has_nonempty_str(obj, "task_id") {
        let candidate = obj.get("shell_id").and_then(task_id_string).or_else(|| {
            obj.get("task_ids")
                .and_then(|value| value.as_array())
                .and_then(|ids| ids.iter().find_map(task_id_string))
        });
        if let Some(task_id) = candidate {
            obj.insert("task_id".into(), serde_json::json!(task_id));
        }
    }
    // `shell_id` is an official deprecated Claude alias and remains accepted
    // by TaskStop/KillShell/KillBash. Keep it, while dropping Cursor-only
    // arrays and transport metadata.
    retain_object_keys(obj, &["task_id", "shell_id"]);
}

fn normalize_grok_task_stop(obj: &mut serde_json::Map<String, serde_json::Value>) {
    if !has_nonempty_str(obj, "task_id") {
        let candidate = obj.get("shell_id").and_then(task_id_string).or_else(|| {
            obj.get("task_ids")
                .and_then(|value| value.as_array())
                .and_then(|ids| ids.iter().find_map(task_id_string))
        });
        if let Some(task_id) = candidate {
            obj.insert("task_id".into(), serde_json::json!(task_id));
        }
    }
    // Keep both spellings: current grok-build accepts the singular ID while
    // older Cursor/grok clients still require the `task_ids` array.
    retain_object_keys(obj, &["task_id", "task_ids"]);
}

fn task_id_string(value: &serde_json::Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .or_else(|| value.as_u64().map(|id| id.to_string()))
        .or_else(|| {
            value
                .as_i64()
                .filter(|id| *id >= 0)
                .map(|id| id.to_string())
        })
}

fn bool_value(value: &serde_json::Value) -> Option<bool> {
    value.as_bool().or_else(|| {
        value.as_str().and_then(|text| match text.trim() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        })
    })
}

fn coerce_timeout_value(value: &serde_json::Value) -> Option<serde_json::Value> {
    coerce_whole_integer(value).and_then(|value| {
        value
            .as_i64()
            .filter(|timeout| *timeout >= 0)
            .map(|timeout| serde_json::json!(timeout))
            .or_else(|| value.as_u64().map(|timeout| serde_json::json!(timeout)))
    })
}

fn coerce_whole_integer(value: &serde_json::Value) -> Option<serde_json::Value> {
    if let Some(u) = value.as_u64() {
        return Some(serde_json::json!(u));
    }
    if let Some(i) = value.as_i64() {
        return Some(serde_json::json!(i));
    }
    if let Some(f) = value.as_f64() {
        if f.is_finite() && f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
            return Some(serde_json::json!(f as i64));
        }
        return None;
    }
    if let Some(text) = value.as_str() {
        return text
            .trim()
            .parse::<i64>()
            .ok()
            .map(|i| serde_json::json!(i));
    }
    None
}

fn adapt_shell_like(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    require_description: bool,
    claude_schema: bool,
) {
    if !has_nonempty_str(obj, "command") {
        if let Some(pattern) = nonempty_string(obj.get("pattern")) {
            let path = nonempty_string(obj.get("path")).unwrap_or_else(|| ".".into());
            obj.insert(
                "command".into(),
                serde_json::json!(format!(
                    "rg --files -g {} -- {}",
                    shell_single_quote(&pattern),
                    shell_single_quote(&path)
                )),
            );
            obj.remove("pattern");
            obj.remove("path");
        } else if let Some(path) = nonempty_string(obj.get("path")) {
            obj.insert(
                "command".into(),
                serde_json::json!(format!("ls -la -- {}", shell_single_quote(&path))),
            );
            obj.remove("path");
        }
    }
    if require_description && !has_nonempty_str(obj, "description") {
        if let Some(command) = obj.get("command").and_then(|value| value.as_str()) {
            obj.insert(
                "description".into(),
                serde_json::json!(shell_description(command)),
            );
        }
    }
    if claude_schema {
        // Claude Code's Bash/PowerShell schemas call this field
        // `run_in_background`; Cursor/Grok use `background` or
        // `is_background`. Prefer an already-canonical boolean and then
        // translate the aliases, dropping all fields unknown to the strict
        // PowerShell schema.
        let run_in_background = obj
            .get("run_in_background")
            .filter(|value| value.is_boolean())
            .cloned()
            .or_else(|| {
                obj.get("background")
                    .filter(|value| value.is_boolean())
                    .cloned()
            })
            .or_else(|| {
                obj.get("is_background")
                    .filter(|value| value.is_boolean())
                    .cloned()
            });
        obj.remove("background");
        obj.remove("is_background");
        obj.remove("run_in_background");
        for key in ["working_directory", "cwd", "shell"] {
            obj.remove(key);
        }
        if obj.get("dangerouslyDisableSandbox").is_none()
            && let Some(value) = obj.remove("dangerously_disable_sandbox")
            && value.is_boolean()
        {
            obj.insert("dangerouslyDisableSandbox".into(), value);
        }
        if let Some(flag) = run_in_background {
            obj.insert("run_in_background".into(), flag);
        }
    } else if obj.get("background").is_none() {
        if let Some(flag) = obj.remove("is_background") {
            obj.insert("background".into(), flag);
        }
    } else {
        obj.remove("is_background");
    }
}

fn shell_description(command: &str) -> String {
    let one_line = command.split_whitespace().collect::<Vec<_>>().join(" ");
    if one_line.is_empty() {
        return "run command".into();
    }
    const MAX: usize = 80;
    if one_line.chars().count() <= MAX {
        return one_line;
    }
    let mut out: String = one_line.chars().take(MAX.saturating_sub(1)).collect();
    out.push('…');
    out
}

fn adapt_task_ids_list(obj: &mut serde_json::Map<String, serde_json::Value>) {
    let has_list = obj
        .get("task_ids")
        .and_then(|value| value.as_array())
        .is_some_and(|arr| !arr.is_empty());
    if !has_list {
        if let Some(id) = nonempty_string(obj.get("task_id")) {
            obj.insert("task_ids".into(), serde_json::json!([id]));
        } else if let Some(id) = obj.get("task_id").and_then(|value| value.as_u64()) {
            obj.insert("task_ids".into(), serde_json::json!([id.to_string()]));
        }
    }
    obj.remove("task_id");
}

fn nonempty_string(value: Option<&serde_json::Value>) -> Option<String> {
    let value = value?;
    if let Some(text) = value.as_str() {
        return sanitize_spawn_string(text);
    }
    None
}

fn sanitize_spawn_string(raw: &str) -> Option<String> {
    if let Some(decoded) = decode_protobuf_value_bytes(raw.as_bytes()) {
        return match decoded {
            serde_json::Value::String(text) => sanitize_spawn_string(&text),
            _ => None,
        };
    }
    if raw.as_bytes().first() == Some(&0x1a) {
        return None;
    }
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}

fn spawn_background_flag(value: Option<&serde_json::Value>) -> Option<bool> {
    let value = value?;
    if let Some(flag) = value.as_bool() {
        return Some(flag);
    }
    let text = value.as_str()?;
    if let Some(decoded) = decode_protobuf_value_bytes(text.as_bytes()) {
        return decoded.as_bool();
    }
    None
}

fn remap_model_slug_subagent_type(mut input: serde_json::Value) -> serde_json::Value {
    let Some(obj) = input.as_object_mut() else {
        return input;
    };
    if let Some(raw) = nonempty_string(obj.get("subagent_type"))
        && spawn_type_looks_like_model_id(&raw)
    {
        obj.insert("subagent_type".into(), serde_json::json!("general-purpose"));
    }
    input
}

/// Keep Claude Code's native Agent/legacy Task payload within the exact
/// contract exposed by Claude Code 2.1.x. Cursor may attach its own model slug
/// (for example `cursor-grok4.6` or `claude-fable-5`) to a native Task call;
/// forwarding that value makes Claude reject the whole tool_use because its
/// `model` field is an enum of `sonnet`, `opus`, `haiku`, and `fable`.
///
/// Grok's `spawn_subagent` path has a separate allow-list adapter below. This
/// helper is intentionally limited to the Claude-facing aliases so unknown
/// model IDs are omitted and Claude falls back to the parent/agent default.
fn adapt_claude_agent_input(input: serde_json::Value) -> serde_json::Value {
    let mut input = remap_model_slug_subagent_type(input);
    let Some(obj) = input.as_object_mut() else {
        return input;
    };
    // Cursor's native Task carries resume state for its own child registry.
    // Claude Code 2.1.193 Agent/Task has no resume field; forwarding it can
    // make strict clients reject an otherwise valid tool_use.
    for key in ["resume", "resume_from"] {
        obj.remove(key);
    }
    let valid = obj
        .get("model")
        .and_then(|value| value.as_str())
        .is_some_and(|model| matches!(model, "sonnet" | "opus" | "haiku" | "fable"));
    if !valid {
        obj.remove("model");
    }
    input
}

fn normalize_spawn_subagent_type(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if spawn_type_looks_like_model_id(trimmed) {
        return Some("general-purpose".into());
    }
    let compact = trimmed
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-')
        .collect::<String>()
        .to_ascii_lowercase();
    Some(match compact.as_str() {
        "generalpurpose" => "general-purpose".into(),
        "explore" => "explore".into(),
        "plan" => "plan".into(),
        _ => trimmed.to_string(),
    })
}

fn spawn_type_looks_like_model_id(raw: &str) -> bool {
    let lower = raw.to_ascii_lowercase();
    if looks_like_named_agent_type(&lower) {
        return false;
    }
    lower.starts_with("cursor-")
        || lower.starts_with("claude-")
        || lower.starts_with("gpt-")
        || lower.starts_with("grok-")
        || lower.starts_with("gemini-")
        || lower.starts_with("composer-")
        || lower.starts_with("kimi-")
        || lower.starts_with("glm-")
        || lower.contains("claude-fable")
        || lower.starts_with("fable-")
}

fn looks_like_named_agent_type(lower: &str) -> bool {
    matches!(
        lower,
        "claude"
            | "claude-code-guide"
            | "general-purpose"
            | "explore"
            | "plan"
            | "statusline-setup"
    ) || lower.ends_with("-checker")
        || lower.ends_with("-guide")
        || lower.ends_with("-setup")
        || lower.ends_with("-researcher")
        || lower.ends_with("-builder")
}

/// Rebuild Cursor native Task args into grok-build `spawn_subagent` input.
///
/// Only allowlisted keys survive. Cursor `readonly` and any smuggled MCP
/// fields are dropped; `resume` and `run_in_background` are renamed. A native
/// model override is retained when it is a downstream Grok model ID; Cursor
/// slugs are omitted because Grok's spawn endpoint cannot route them.
pub fn adapt_native_task_to_spawn_subagent(input: serde_json::Value) -> serde_json::Value {
    let Some(obj) = input.as_object() else {
        return serde_json::json!({});
    };
    let mut out = serde_json::Map::new();
    if let Some(description) = nonempty_string(obj.get("description")) {
        out.insert("description".into(), serde_json::json!(description));
    }
    if let Some(prompt) = nonempty_string(obj.get("prompt")) {
        out.insert("prompt".into(), serde_json::json!(prompt));
    }
    if let Some(subagent_type) = nonempty_string(obj.get("subagent_type"))
        .and_then(|raw| normalize_spawn_subagent_type(&raw))
    {
        out.insert("subagent_type".into(), serde_json::json!(subagent_type));
    }
    if let Some(resume) =
        nonempty_string(obj.get("resume_from")).or_else(|| nonempty_string(obj.get("resume")))
    {
        out.insert("resume_from".into(), serde_json::json!(resume));
    }
    if let Some(background) = spawn_background_flag(
        obj.get("background")
            .or_else(|| obj.get("run_in_background")),
    ) {
        out.insert("background".into(), serde_json::json!(background));
    }
    if let Some(mode) =
        nonempty_string(obj.get("capability_mode")).and_then(|raw| normalize_capability_mode(&raw))
    {
        out.insert("capability_mode".into(), serde_json::json!(mode));
    }
    if let Some(isolation) =
        nonempty_string(obj.get("isolation")).and_then(|raw| normalize_isolation(&raw))
    {
        out.insert("isolation".into(), serde_json::json!(isolation));
    }
    if let Some(cwd) = nonempty_string(obj.get("cwd")) {
        out.insert("cwd".into(), serde_json::json!(cwd));
    }
    if let Some(model) = nonempty_string(obj.get("model")).and_then(|raw| spawn_client_model(&raw))
    {
        out.insert("model".into(), serde_json::json!(model));
    }
    serde_json::Value::Object(out)
}

fn normalize_capability_mode(raw: &str) -> Option<&'static str> {
    let compact = raw
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-')
        .collect::<String>()
        .to_ascii_lowercase();
    match compact.as_str() {
        "readonly" => Some("read-only"),
        "readwrite" => Some("read-write"),
        "execute" => Some("execute"),
        "all" => Some("all"),
        _ => None,
    }
}

fn normalize_isolation(raw: &str) -> Option<&'static str> {
    let compact = raw
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-')
        .collect::<String>()
        .to_ascii_lowercase();
    match compact.as_str() {
        "none" => Some("none"),
        "worktree" => Some("worktree"),
        _ => None,
    }
}

fn spawn_client_model(raw: &str) -> Option<String> {
    let lower = raw.to_ascii_lowercase();
    if lower.starts_with("cursor-") || lower.starts_with("claude-fable") {
        return None;
    }
    Some(raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cursor::proto::{
        AskQuestionItem, AskQuestionOption, AskQuestionToolCall, ExecServerMessage, ReadToolArgs,
        ReadToolCall, ShellToolCall, ToolCall, ToolCallStarted,
    };

    #[test]
    fn maps_shell_to_bash() {
        let started = ToolCallStarted {
            call_id: "c1".into(),
            tool_call: Some(ToolCall {
                shell_tool_call: Some(ShellToolCall {
                    args: Some(ShellArgs {
                        command: "ls -la".into(),
                        working_directory: "/tmp".into(),
                        timeout: 30_000,
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&started).unwrap();
        assert_eq!(m.name, "Bash");
        assert_eq!(m.input["command"], "cd '/tmp' && ls -la");
        assert_eq!(m.input["timeout"], 30_000);
    }

    #[test]
    fn maps_shell_timeout_milliseconds_pass_through() {
        let started = ToolCallStarted {
            call_id: "c-timeout".into(),
            tool_call: Some(ToolCall {
                shell_tool_call: Some(ShellToolCall {
                    args: Some(ShellArgs {
                        command: "sleep 1".into(),
                        working_directory: String::new(),
                        timeout: 30_000,
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&started).unwrap();
        assert_eq!(m.input["timeout"], 30_000);
        assert!(m.input.get("command").is_some());
    }

    #[test]
    fn maps_shell_omits_timeout_when_zero() {
        let started = ToolCallStarted {
            call_id: "c0".into(),
            tool_call: Some(ToolCall {
                shell_tool_call: Some(ShellToolCall {
                    args: Some(ShellArgs {
                        command: "true".into(),
                        working_directory: String::new(),
                        timeout: 0,
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&started).unwrap();
        assert!(m.input.get("timeout").is_none());
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
    fn maps_read_drops_ranges_that_violate_claude_contract() {
        let started = ToolCallStarted {
            call_id: "r-invalid".into(),
            tool_call: Some(ToolCall {
                read_tool_call: Some(ReadToolCall {
                    args: Some(ReadToolArgs {
                        path: "/tmp/a.rs".into(),
                        offset: Some(-1),
                        limit: Some(0),
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let mapped = map_tool_call_started(&started).expect("Read maps");
        assert!(mapped.input.get("offset").is_none());
        assert!(mapped.input.get("limit").is_none());

        let exec = ExecServerMessage {
            id: 1,
            exec_id: Some("exec-invalid".into()),
            read_args: Some(crate::providers::cursor::proto::ExecReadArgs {
                path: "/tmp/a.rs".into(),
                tool_call_id: "read-invalid".into(),
                offset: Some(-4),
                limit: Some(0),
            }),
            ..Default::default()
        };
        let mapped = map_exec_server_message(&exec).expect("exec Read maps");
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
    fn maps_ask_question_to_claude_schema_with_options_and_multiselect() {
        let started = ToolCallStarted {
            call_id: "ask-full".into(),
            tool_call: Some(ToolCall {
                ask_question_tool_call: Some(AskQuestionToolCall {
                    args: Some(crate::providers::cursor::proto::AskQuestionArgs {
                        title: "Choose implementation".into(),
                        questions: vec![AskQuestionItem {
                            id: "approach".into(),
                            prompt: "Which approach should we use".into(),
                            options: vec![
                                AskQuestionOption {
                                    id: "a".into(),
                                    label: "Approach A".into(),
                                },
                                AskQuestionOption {
                                    id: "b".into(),
                                    label: "Approach B".into(),
                                },
                            ],
                            allow_multiple: true,
                        }],
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };

        let mapped = map_tool_call_started(&started).expect("AskQuestion maps");
        assert_eq!(mapped.name, "AskUserQuestion");
        assert!(mapped.input.get("title").is_none());
        let question = &mapped.input["questions"][0];
        assert_eq!(question["question"], "Which approach should we use?");
        assert_eq!(question["header"], "Choose imple");
        assert_eq!(question["multiSelect"], true);
        assert_eq!(question["options"][0]["label"], "Approach A");
        assert_eq!(question["options"][1]["label"], "Approach B");
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
        assert!(
            m.input.get("provider_identifier").is_none(),
            "provider_identifier is a Cursor wire field, not Anthropic tool input"
        );
    }

    fn proto_value_bytes(kind: prost_types::value::Kind) -> Vec<u8> {
        use prost::Message;
        prost_types::Value { kind: Some(kind) }.encode_to_vec()
    }

    #[test]
    fn maps_mcp_tool_args_as_protobuf_value() {
        use prost_types::value::Kind;
        let description = proto_value_bytes(Kind::StringValue("SPAWN smoke test".into()));
        let background = proto_value_bytes(Kind::BoolValue(true));
        assert_eq!(description[0], 0x1a, "live Cursor string_value tag");
        assert_eq!(background, [0x20, 0x01], "live Cursor bool_value true");

        let mut args_map = std::collections::HashMap::new();
        args_map.insert("description".into(), description);
        args_map.insert(
            "prompt".into(),
            proto_value_bytes(Kind::StringValue(
                "Reply with exactly one line: SPAWN_OK".into(),
            )),
        );
        args_map.insert(
            "subagent_type".into(),
            proto_value_bytes(Kind::StringValue("general-purpose".into())),
        );
        args_map.insert("background".into(), background);
        let started = ToolCallStarted {
            call_id: "m-proto".into(),
            tool_call: Some(ToolCall {
                mcp_tool_call: Some(crate::providers::cursor::proto::McpToolCall {
                    args: Some(crate::providers::cursor::proto::McpArgs {
                        name: "mcp_claude-local_spawn_subagent".into(),
                        args: args_map,
                        tool_call_id: "m-proto".into(),
                        provider_identifier: "claude-local".into(),
                        tool_name: "mcp_claude-local_spawn_subagent".into(),
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&started).unwrap();
        assert_eq!(m.input["description"], "SPAWN smoke test");
        assert_eq!(m.input["prompt"], "Reply with exactly one line: SPAWN_OK");
        assert_eq!(m.input["subagent_type"], "general-purpose");
        assert_eq!(m.input["background"], true);
        for key in ["description", "prompt", "subagent_type"] {
            let text = m.input[key].as_str().unwrap();
            assert!(
                !text.as_bytes().contains(&0x1a),
                "{key} must not keep protobuf string_value prefix: {text:?}"
            );
        }
    }

    #[test]
    fn mcp_arg_space_prefixed_text_is_not_bool_value() {
        let mut args_map = std::collections::HashMap::new();
        args_map.insert("label".into(), b" A".to_vec());
        let started = ToolCallStarted {
            call_id: "m-space".into(),
            tool_call: Some(ToolCall {
                mcp_tool_call: Some(crate::providers::cursor::proto::McpToolCall {
                    args: Some(crate::providers::cursor::proto::McpArgs {
                        name: "search".into(),
                        args: args_map,
                        tool_call_id: "m-space".into(),
                        provider_identifier: "plugin".into(),
                        tool_name: "search".into(),
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&started).unwrap();
        assert_eq!(m.input["label"], " A");
    }

    #[test]
    fn maps_workflow_mcp_without_provider_identifier_in_input() {
        let mut args_map = std::collections::HashMap::new();
        args_map.insert("name".into(), br#""deep-research""#.to_vec());
        let started = ToolCallStarted {
            call_id: "wf1".into(),
            tool_call: Some(ToolCall {
                mcp_tool_call: Some(crate::providers::cursor::proto::McpToolCall {
                    args: Some(crate::providers::cursor::proto::McpArgs {
                        name: "Workflow".into(),
                        args: args_map,
                        tool_call_id: "wf1".into(),
                        provider_identifier: "claude-local".into(),
                        tool_name: "Workflow".into(),
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&started).unwrap();
        assert_eq!(m.name, "Workflow");
        assert_eq!(m.input["name"], "deep-research");
        assert_eq!(m.input.as_object().map(|o| o.len()), Some(1));
        assert!(m.input.get("provider_identifier").is_none());
        assert!(m.input.get("args").is_none());
    }

    #[test]
    fn maps_cursor_task_tool_call_tag_19() {
        let started = ToolCallStarted {
            call_id: "task-1".into(),
            tool_call: Some(ToolCall {
                task_tool_call: Some(crate::providers::cursor::proto::TaskToolCall {
                    args: Some(crate::providers::cursor::proto::TaskToolCallArgsProto {
                        description: "explore live".into(),
                        prompt: "find TaskToolCall".into(),
                        model: Some("fable".into()),
                        subagent_type: "explore".into(),
                        resume: Some("sa-1".into()),
                        run_in_background: Some(true),
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&started).unwrap();
        assert_eq!(m.name, "Task");
        assert_eq!(m.input["description"], "explore live");
        assert_eq!(m.input["prompt"], "find TaskToolCall");
        assert_eq!(m.input["model"], "fable");
        assert_eq!(m.input["subagent_type"], "explore");
        assert_eq!(m.input["resume"], "sa-1");
        assert_eq!(m.input["run_in_background"], true);
        let adapted = adapt_tool_input_for_client("spawn_subagent", m.input);
        assert_eq!(adapted["resume_from"], "sa-1");
        assert_eq!(adapted["background"], true);
    }

    #[test]
    fn maps_exec_write_args_to_claude_write_schema() {
        let exec = ExecServerMessage {
            id: 9,
            exec_id: Some("exec-w".into()),
            write_args: Some(crate::providers::cursor::proto::WriteArgs {
                path: "/tmp/out.md".into(),
                file_text: "# hello\n".into(),
            }),
            ..Default::default()
        };
        let m = map_exec_server_message(&exec).unwrap();
        assert_eq!(m.name, "Write");
        assert_eq!(m.input["file_path"], "/tmp/out.md");
        assert_eq!(m.input["content"], "# hello\n");
        assert!(m.input.get("path").is_none());
        assert!(m.input.get("file_text").is_none());
        assert!(m.input.get("contents").is_none());
    }

    #[test]
    fn maps_pi_write_exec_args_to_claude_write_schema() {
        let exec = ExecServerMessage {
            id: 10,
            exec_id: Some("pi-exec-w".into()),
            pi_write_args: Some(crate::providers::cursor::proto::PiWriteExecArgs {
                path: "/tmp/pi.md".into(),
                content: "created\n".into(),
            }),
            ..Default::default()
        };
        let mapped = map_exec_server_message(&exec).expect("Pi write must map");
        assert_eq!(mapped.name, "Write");
        assert_eq!(mapped.tool_use_id, "pi-exec-w");
        assert_eq!(mapped.input["file_path"], "/tmp/pi.md");
        assert_eq!(mapped.input["content"], "created\n");
    }

    #[test]
    fn maps_pi_write_tool_call_field_64() {
        let started = ToolCallStarted {
            call_id: "pi-tool-1".into(),
            tool_call: Some(ToolCall {
                pi_write_tool_call: Some(crate::providers::cursor::proto::PiWriteToolCall {
                    args: Some(crate::providers::cursor::proto::PiWriteToolArgs {
                        path: "/tmp/pi.txt".into(),
                        content: "hello".into(),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let mapped = map_tool_call_started(&started).expect("Pi tool must map");
        assert_eq!(mapped.name, "Write");
        assert_eq!(mapped.input["file_path"], "/tmp/pi.txt");
        assert_eq!(mapped.input["content"], "hello");
    }

    #[test]
    fn maps_pi_edit_exec_single_replacement_to_edit() {
        let exec = ExecServerMessage {
            id: 11,
            exec_id: Some("pi-exec-edit".into()),
            pi_edit_args: Some(crate::providers::cursor::proto::PiEditExecArgs {
                path: "/tmp/pi.rs".into(),
                edits: vec![crate::providers::cursor::proto::PiEditReplacement {
                    old_text: "before".into(),
                    new_text: "after".into(),
                }],
            }),
            ..Default::default()
        };
        let mapped = map_exec_server_message(&exec).expect("Pi edit must map");
        assert_eq!(mapped.name, "Edit");
        assert_eq!(mapped.tool_use_id, "pi-exec-edit");
        assert_eq!(mapped.input["file_path"], "/tmp/pi.rs");
        assert_eq!(mapped.input["old_string"], "before");
        assert_eq!(mapped.input["new_string"], "after");
        assert!(mapped.input.get("edits").is_none());
    }

    #[test]
    fn maps_pi_edit_exec_multiple_replacements_to_multi_edit() {
        let exec = ExecServerMessage {
            id: 12,
            exec_id: Some("pi-exec-multi".into()),
            pi_edit_args: Some(crate::providers::cursor::proto::PiEditExecArgs {
                path: "/tmp/pi.rs".into(),
                edits: vec![
                    crate::providers::cursor::proto::PiEditReplacement {
                        old_text: "one".into(),
                        new_text: "1".into(),
                    },
                    crate::providers::cursor::proto::PiEditReplacement {
                        old_text: "two".into(),
                        new_text: "2".into(),
                    },
                ],
            }),
            ..Default::default()
        };
        let mapped = map_exec_server_message(&exec).expect("Pi multi-edit must map");
        assert_eq!(mapped.name, "MultiEdit");
        assert_eq!(mapped.input["file_path"], "/tmp/pi.rs");
        assert_eq!(mapped.input["edits"][0]["old_string"], "one");
        assert_eq!(mapped.input["edits"][1]["new_string"], "2");
    }

    #[test]
    fn maps_pi_edit_tool_call_field_63() {
        let started = ToolCallStarted {
            call_id: "pi-tool-edit".into(),
            tool_call: Some(ToolCall {
                pi_edit_tool_call: Some(crate::providers::cursor::proto::PiEditToolCall {
                    args: Some(crate::providers::cursor::proto::PiEditToolArgs {
                        path: "/tmp/pi.txt".into(),
                        edits: vec![crate::providers::cursor::proto::PiEditReplacement {
                            old_text: "x".into(),
                            new_text: "y".into(),
                        }],
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let mapped = map_tool_call_started(&started).expect("Pi edit tool must map");
        assert_eq!(mapped.name, "Edit");
        assert_eq!(mapped.input["file_path"], "/tmp/pi.txt");
        assert_eq!(mapped.input["old_string"], "x");
    }

    #[test]
    fn empty_pi_edit_does_not_create_tool_call() {
        let exec = ExecServerMessage {
            id: 13,
            pi_edit_args: Some(crate::providers::cursor::proto::PiEditExecArgs {
                path: "/tmp/empty".into(),
                edits: vec![],
            }),
            ..Default::default()
        };
        assert!(map_exec_server_message(&exec).is_none());
    }

    #[test]
    fn maps_cursor_edit_with_content_to_write_not_edit() {
        let started = ToolCallStarted {
            call_id: "e1".into(),
            tool_call: Some(ToolCall {
                edit_tool_call: Some(crate::providers::cursor::proto::EditToolCall {
                    args: Some(crate::providers::cursor::proto::EditArgs {
                        path: "/tmp/a.rs".into(),
                        stream_content: Some("fn main() {}".into()),
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&started).unwrap();
        assert_eq!(m.name, "Write");
        assert_eq!(m.input["file_path"], "/tmp/a.rs");
        assert_eq!(m.input["content"], "fn main() {}");
        assert!(m.input.get("old_string").is_none());
        assert!(m.input.get("new_string").is_none());
    }

    #[test]
    fn incomplete_cursor_edit_is_not_mapped_to_read() {
        let started = ToolCallStarted {
            call_id: "e2".into(),
            tool_call: Some(ToolCall {
                edit_tool_call: Some(crate::providers::cursor::proto::EditToolCall {
                    args: Some(crate::providers::cursor::proto::EditArgs {
                        path: "/tmp/a.rs".into(),
                        stream_content: None,
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        assert!(map_tool_call_started(&started).is_none());
    }

    #[test]
    fn empty_cursor_edit_maps_to_write_that_truncates_file() {
        let started = ToolCallStarted {
            call_id: "e3".into(),
            tool_call: Some(ToolCall {
                edit_tool_call: Some(crate::providers::cursor::proto::EditToolCall {
                    args: Some(crate::providers::cursor::proto::EditArgs {
                        path: "/tmp/empty.txt".into(),
                        stream_content: Some(String::new()),
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let mapped = map_tool_call_started(&started).expect("explicit empty content is complete");
        assert_eq!(mapped.name, "Write");
        assert_eq!(mapped.input["file_path"], "/tmp/empty.txt");
        assert_eq!(mapped.input["content"], "");
    }

    #[test]
    fn maps_web_fetch_tool_call_tag_37_like_fetch() {
        let started = ToolCallStarted {
            call_id: "wf37".into(),
            tool_call: Some(ToolCall {
                web_fetch_tool_call: Some(crate::providers::cursor::proto::WebFetchToolCall {
                    args: Some(crate::providers::cursor::proto::FetchArgs {
                        url: "https://example.com/docs".into(),
                        tool_call_id: "wf37".into(),
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&started).unwrap();
        assert_eq!(m.name, "WebFetch");
        assert_eq!(m.input["url"], "https://example.com/docs");
        assert_eq!(m.tool_use_id, "wf37");
    }

    #[test]
    fn merge_partial_args_fills_empty_workflow_input() {
        let mut mapped = MappedClaudeTool {
            tool_use_id: "mcp-1".into(),
            name: "Workflow".into(),
            input: serde_json::json!({}),
        };
        assert!(merge_partial_args_json(
            &mut mapped,
            r#"{"name":"deep-research"}"#
        ));
        assert_eq!(mapped.input["name"], "deep-research");
        assert!(!merge_partial_args_json(&mut mapped, "{incomplete"));
    }

    #[test]
    fn accumulate_partial_args_prefers_json_snapshots() {
        let mut buf = String::new();
        accumulate_partial_args_text(&mut buf, r#"{"name":""#);
        accumulate_partial_args_text(&mut buf, r#"{"name":"deep-research"}"#);
        assert_eq!(buf, r#"{"name":"deep-research"}"#);
        let mut frag = String::new();
        accumulate_partial_args_text(&mut frag, r#"{"name":""#);
        accumulate_partial_args_text(&mut frag, r#"deep-research"}"#);
        assert_eq!(frag, r#"{"name":"deep-research"}"#);
    }

    #[test]
    fn adapt_read_file_renames_file_path_to_target_file() {
        let adapted = adapt_tool_input_for_client(
            "read_file",
            serde_json::json!({"file_path": "/tmp/a.rs", "offset": 1}),
        );
        assert_eq!(adapted["target_file"], "/tmp/a.rs");
        assert_eq!(adapted["offset"], 1);
        assert!(adapted.get("file_path").is_none());
        let already = adapt_tool_input_for_client(
            "read_file",
            serde_json::json!({"target_file": "/kept.rs", "file_path": "/ignored.rs"}),
        );
        assert_eq!(already["target_file"], "/kept.rs");
        let claude =
            adapt_tool_input_for_client("Read", serde_json::json!({"file_path": "/tmp/a.rs"}));
        assert_eq!(claude["file_path"], "/tmp/a.rs");
        assert!(claude.get("target_file").is_none());
    }

    #[test]
    fn task_fallbacks_include_grok_build_canonical_task() {
        assert!(advertised_name_fallbacks("Task").contains(&"task"));
        assert!(advertised_name_fallbacks("Task").contains(&"spawn_subagent"));
    }

    #[test]
    fn lifecycle_fallbacks_are_bidirectional_for_all_claude_aliases() {
        let output_aliases = [
            "TaskOutput",
            "BashOutput",
            "BashOutputTool",
            "AgentOutputTool",
            "AgentOutput",
        ];
        for alias in output_aliases {
            assert!(advertised_name_fallbacks(alias).contains(&"get_command_or_subagent_output"));
            assert!(advertised_name_fallbacks("get_command_or_subagent_output").contains(&alias));
        }
        for alias in ["TaskStop", "KillShell", "KillBash"] {
            assert!(advertised_name_fallbacks(alias).contains(&"kill_command_or_subagent"));
            assert!(advertised_name_fallbacks("kill_command_or_subagent").contains(&alias));
        }
    }

    #[test]
    fn claude_client_alias_fallbacks_are_bidirectional() {
        for (canonical, alias) in [
            ("Agent", "Task"),
            ("TaskStop", "KillShell"),
            ("TaskStop", "KillBash"),
            ("TaskOutput", "AgentOutputTool"),
            ("TaskOutput", "BashOutputTool"),
            ("TaskOutput", "AgentOutput"),
            ("TaskOutput", "BashOutput"),
            ("ListAgents", "ListPeers"),
            ("SendUserMessage", "Brief"),
            ("ListMcpResourcesTool", "ListMcpResources"),
            ("ReadMcpResourceTool", "ReadMcpResource"),
            ("ReadMcpResourceDirTool", "ReadMcpResourceDir"),
            ("Workflow", "RunWorkflow"),
        ] {
            assert!(advertised_name_fallbacks(canonical).contains(&alias));
            assert!(advertised_name_fallbacks(alias).contains(&canonical));
        }
        assert!(advertised_name_fallbacks("runworkflow").contains(&"Workflow"));
        assert!(advertised_name_fallbacks("brief").contains(&"SendUserMessage"));
    }

    #[test]
    fn adapt_native_task_to_spawn_rebuilds_allowlist() {
        let adapted = adapt_native_task_to_spawn_subagent(serde_json::json!({
            "description": "explore live",
            "prompt": "find TaskToolCall",
            "subagent_type": "generalPurpose",
            "resume": "sa-1",
            "run_in_background": false,
            "model": "cursor-grok4.6",
            "readonly": true,
            "provider_identifier": "evil",
            "args": {"nested": true}
        }));
        let obj = adapted.as_object().expect("object");
        assert_eq!(
            obj.get("description").and_then(|v| v.as_str()),
            Some("explore live")
        );
        assert_eq!(
            obj.get("prompt").and_then(|v| v.as_str()),
            Some("find TaskToolCall")
        );
        assert_eq!(
            obj.get("subagent_type").and_then(|v| v.as_str()),
            Some("general-purpose")
        );
        assert_eq!(
            obj.get("resume_from").and_then(|v| v.as_str()),
            Some("sa-1")
        );
        assert_eq!(obj.get("background"), Some(&serde_json::json!(false)));
        for leaked in [
            "model",
            "readonly",
            "resume",
            "run_in_background",
            "provider_identifier",
            "args",
            "capability_mode",
        ] {
            assert!(obj.get(leaked).is_none(), "{leaked} must not leak");
        }
        let unknown = adapt_native_task_to_spawn_subagent(serde_json::json!({
            "prompt": "p",
            "subagent_type": "my-custom-agent"
        }));
        assert_eq!(unknown["subagent_type"], "my-custom-agent");
        let model_slug = adapt_native_task_to_spawn_subagent(serde_json::json!({
            "prompt": "p",
            "description": "d",
            "subagent_type": "cursor-grok-4.5-high-fast"
        }));
        assert_eq!(
            model_slug["subagent_type"], "general-purpose",
            "Cursor Task often puts the model slug in subagent_type; grok-build only accepts general-purpose/explore/plan"
        );
        let gemini = adapt_native_task_to_spawn_subagent(serde_json::json!({
            "prompt": "p",
            "description": "d",
            "subagent_type": "gemini-3.6-flash-high"
        }));
        assert_eq!(
            gemini["subagent_type"], "general-purpose",
            "gemini-3.6-flash-high is a Cursor model id, not a grok-build agent type"
        );
        assert!(unknown.get("background").is_none());
        let already = adapt_native_task_to_spawn_subagent(serde_json::json!({
            "prompt": "p",
            "resume_from": "kept",
            "background": false,
            "resume": "ignored",
            "run_in_background": true
        }));
        assert_eq!(already["resume_from"], "kept");
        assert_eq!(already["background"], false);
    }

    #[test]
    fn adapt_agent_remaps_gemini_model_slug_only() {
        let adapted = adapt_client_tool_input(
            "Agent",
            serde_json::json!({
                "description": "CARVE INITIAL A1585-0026",
                "prompt": "do the carve",
                "subagent_type": "gemini-3.6-flash-high",
                "run_in_background": true
            }),
        );
        assert_eq!(
            adapted["subagent_type"], "general-purpose",
            "Claude Code Agent rejects Cursor model slugs as agent types"
        );
        assert_eq!(adapted["run_in_background"], true);
        assert_eq!(adapted["description"], "CARVE INITIAL A1585-0026");
        assert_eq!(adapted["prompt"], "do the carve");
        assert!(
            adapted.get("background").is_none(),
            "Agent must keep Claude Code run_in_background, not grok background"
        );
        assert!(adapted.get("resume_from").is_none());

        let explore = adapt_client_tool_input(
            "Agent",
            serde_json::json!({
                "prompt": "p",
                "subagent_type": "Explore"
            }),
        );
        assert_eq!(
            explore["subagent_type"], "Explore",
            "Claude Code agent catalog is case-sensitive"
        );

        let guide = adapt_client_tool_input(
            "Agent",
            serde_json::json!({
                "prompt": "p",
                "subagent_type": "claude-code-guide"
            }),
        );
        assert_eq!(guide["subagent_type"], "claude-code-guide");

        let custom = adapt_client_tool_input(
            "Agent",
            serde_json::json!({
                "prompt": "p",
                "subagent_type": "my-custom-agent"
            }),
        );
        assert_eq!(custom["subagent_type"], "my-custom-agent");
    }

    #[test]
    fn adapt_task_remaps_gemini_model_slug_only() {
        let adapted = adapt_tool_input_for_client(
            "Task",
            serde_json::json!({
                "prompt": "p",
                "subagent_type": "gemini-3.6-flash-high",
                "run_in_background": false
            }),
        );
        assert_eq!(adapted["subagent_type"], "general-purpose");
        assert_eq!(adapted["run_in_background"], false);
        let plan = adapt_tool_input_for_client(
            "Task",
            serde_json::json!({"prompt": "p", "subagent_type": "Plan"}),
        );
        assert_eq!(plan["subagent_type"], "Plan");
    }

    #[test]
    fn adapt_claude_agent_keeps_supported_model_and_drops_provider_slugs() {
        let supported = adapt_client_tool_input(
            "Agent",
            serde_json::json!({
                "description": "inspect",
                "prompt": "p",
                "subagent_type": "Explore",
                "model": "fable"
            }),
        );
        assert_eq!(supported["model"], "fable");

        let resumed = adapt_client_tool_input(
            "Agent",
            serde_json::json!({
                "description": "resume",
                "prompt": "continue",
                "resume": "cursor-child-1",
                "resume_from": "cursor-child-2"
            }),
        );
        assert!(resumed.get("resume").is_none());
        assert!(resumed.get("resume_from").is_none());

        for model in [
            "cursor-grok4.6",
            "claude-fable-5",
            "grok-4.6",
            "gemini-3.1-pro",
            "unknown",
        ] {
            let adapted =
                adapt_client_tool_input("Task", serde_json::json!({"prompt": "p", "model": model}));
            assert!(
                adapted.get("model").is_none(),
                "provider model {model} must not reach Claude Task"
            );
        }
    }

    #[test]
    fn adapt_provider_qualified_claude_tools_uses_leaf_schema() {
        let agent = adapt_client_tool_input(
            "mcp_claude-local_Agent",
            serde_json::json!({"prompt": "p", "model": "cursor-grok4.6"}),
        );
        assert!(agent.get("model").is_none());

        let powershell = adapt_client_tool_input(
            "mcp__claude-local__PowerShell",
            serde_json::json!({
                "command": "Get-ChildItem",
                "background": true,
                "dangerously_disable_sandbox": true
            }),
        );
        assert_eq!(powershell["run_in_background"], true);
        assert_eq!(powershell["dangerouslyDisableSandbox"], true);
        assert!(powershell.get("background").is_none());
    }

    #[test]
    fn adapt_foreign_provider_tools_does_not_apply_builtin_leaf_schema() {
        for name in [
            "other/Edit",
            "plugin:Agent",
            "foreign/read_file",
            "mcp__other__Edit",
            "mcp__plugin__Agent",
            "mcp__foreign__read_file",
        ] {
            let input = serde_json::json!({
                "path": "/tmp/provider-owned",
                "content": "provider-owned-content",
                "model": "provider-model",
                "custom_transport_field": true
            });
            assert_eq!(
                adapt_client_tool_input(name, input.clone()),
                input,
                "foreign MCP input for {name} must remain opaque"
            );
        }
    }

    #[test]
    fn adapt_spawn_subagent_renames_resume_and_background() {
        let adapted = adapt_tool_input_for_client(
            "spawn_subagent",
            serde_json::json!({
                "description": "explore live",
                "prompt": "find TaskToolCall",
                "subagent_type": "explore",
                "resume": "sa-1",
                "run_in_background": true
            }),
        );
        assert_eq!(adapted["resume_from"], "sa-1");
        assert_eq!(adapted["background"], true);
        assert!(adapted.get("resume").is_none());
        assert!(adapted.get("run_in_background").is_none());
        let already = adapt_tool_input_for_client(
            "spawn_subagent",
            serde_json::json!({
                "prompt": "p",
                "description": "d",
                "resume_from": "kept",
                "background": false,
                "resume": "ignored",
                "run_in_background": true
            }),
        );
        assert_eq!(already["resume_from"], "kept");
        assert_eq!(already["background"], false);
    }

    #[test]
    fn adapt_native_task_unwraps_protobuf_prefixed_strings() {
        use prost_types::value::Kind;
        let description = String::from_utf8(proto_value_bytes(Kind::StringValue(
            "SPAWN smoke test".into(),
        )))
        .unwrap();
        let prompt = String::from_utf8(proto_value_bytes(Kind::StringValue(
            "Reply with exactly one line: SPAWN_OK".into(),
        )))
        .unwrap();
        let subagent_type = String::from_utf8(proto_value_bytes(Kind::StringValue(
            "general-purpose".into(),
        )))
        .unwrap();
        let adapted = adapt_native_task_to_spawn_subagent(serde_json::json!({
            "description": description,
            "prompt": prompt,
            "subagent_type": subagent_type,
            "background": " \u{0001}",
        }));
        assert_eq!(adapted["description"], "SPAWN smoke test");
        assert_eq!(adapted["prompt"], "Reply with exactly one line: SPAWN_OK");
        assert_eq!(adapted["subagent_type"], "general-purpose");
        assert_eq!(adapted["background"], true);
    }

    #[test]
    fn adapt_spawn_keeps_grok_isolation_and_drops_cursor_model() {
        let adapted = adapt_native_task_to_spawn_subagent(serde_json::json!({
            "description": "review",
            "prompt": "audit isolation",
            "capability_mode": "readOnly",
            "isolation": "work_tree",
            "cwd": "/tmp/carve",
            "model": "cursor-grok4.5",
            "readonly": true
        }));
        assert_eq!(adapted["capability_mode"], "read-only");
        assert_eq!(adapted["isolation"], "worktree");
        assert_eq!(adapted["cwd"], "/tmp/carve");
        assert!(adapted.get("model").is_none());
        assert!(adapted.get("readonly").is_none());
        let kept_model = adapt_native_task_to_spawn_subagent(serde_json::json!({
            "prompt": "p",
            "description": "d",
            "model": "grok-4.6"
        }));
        assert_eq!(kept_model["model"], "grok-4.6");
        let rejected = adapt_native_task_to_spawn_subagent(serde_json::json!({
            "prompt": "p",
            "description": "d",
            "capability_mode": "superuser",
            "isolation": "jail"
        }));
        assert!(rejected.get("capability_mode").is_none());
        assert!(rejected.get("isolation").is_none());
    }

    #[test]
    fn glob_list_dir_only_when_pattern_is_a_directory_listing() {
        assert!(glob_pattern_is_directory_listing(
            &serde_json::json!({"pattern": "*"})
        ));
        assert!(!glob_pattern_is_directory_listing(
            &serde_json::json!({"pattern": "**/*.rs"})
        ));
        let listing = adapt_tool_input_for_client(
            "list_dir",
            serde_json::json!({"pattern": "*", "path": "/tmp/carve"}),
        );
        assert_eq!(listing["target_directory"], "/tmp/carve");
        assert!(listing.get("pattern").is_none());
        let shelled = adapt_tool_input_for_client(
            "run_terminal_command",
            serde_json::json!({"pattern": "**/*.rs", "path": "src"}),
        );
        assert_eq!(
            shelled["command"].as_str(),
            Some("rg --files -g '**/*.rs' -- 'src'")
        );
        assert!(
            resolve_glob_client_name(
                &serde_json::json!({"pattern": "**/*.rs"}),
                Some("list_dir".into()),
                Some("run_terminal_command".into()),
            )
            .as_deref()
                == Some("run_terminal_command")
        );
        assert!(
            resolve_glob_client_name(
                &serde_json::json!({"pattern": "**/*.rs"}),
                Some("list_dir".into()),
                None,
            )
            .is_none()
        );
    }

    #[test]
    fn unknown_mcp_tool_keeps_fractional_timeout() {
        let adapted = adapt_tool_input_for_client(
            "custom_timer",
            serde_json::json!({"timeout": 1.5, "offset": 2.0}),
        );
        assert_eq!(adapted["timeout"], 1.5);
        assert_eq!(adapted["offset"], 2.0);
    }

    #[test]
    fn grok_build_fallbacks_cover_cursor_and_claude_names() {
        for (mapped, grok) in [
            ("Bash", "run_terminal_command"),
            ("Read", "read_file"),
            ("Write", "write"),
            ("Grep", "grep"),
            ("Glob", "list_dir"),
            ("LS", "list_dir"),
            ("WebSearch", "web_search"),
            ("WebFetch", "web_fetch"),
            ("TodoWrite", "todo_write"),
            ("AskUserQuestion", "ask_user_question"),
            ("Task", "spawn_subagent"),
            ("TaskOutput", "get_command_or_subagent_output"),
            ("BashOutput", "get_command_or_subagent_output"),
            ("KillShell", "kill_command_or_subagent"),
            ("CreatePlan", "exit_plan_mode"),
            ("CreatePlan", "ExitPlanMode"),
        ] {
            assert!(
                advertised_name_fallbacks(mapped).contains(&grok),
                "{mapped} must map to grok-build {grok}"
            );
        }
        assert!(
            advertised_name_fallbacks("Bash").contains(&"PowerShell"),
            "Cursor Shell must map to Claude Code's Windows-native PowerShell tool"
        );
    }

    #[test]
    fn create_plan_submits_instead_of_reentering_plan_mode() {
        let fallbacks = advertised_name_fallbacks("CreatePlan");
        assert!(fallbacks.contains(&"ExitPlanMode"));
        assert!(fallbacks.contains(&"exit_plan_mode"));
        assert!(!fallbacks.contains(&"EnterPlanMode"));
        assert!(!fallbacks.contains(&"enter_plan_mode"));

        let claude = adapt_client_tool_input(
            "ExitPlanMode",
            serde_json::json!({
                "name": "implementation",
                "overview": "ready",
                "plan": "1. inspect\n2. patch\n3. test",
                "todos": [{"content": "patch"}],
                "is_project": true
            }),
        );
        assert_eq!(
            claude,
            serde_json::json!({"plan": "1. inspect\n2. patch\n3. test"})
        );

        let grok = adapt_client_tool_input(
            "exit_plan_mode",
            serde_json::json!({"plan": "inline Cursor plan"}),
        );
        assert_eq!(grok, serde_json::json!({}));
    }

    #[test]
    fn powershell_native_shell_input_keeps_claude_contract() {
        let adapted = adapt_tool_input_for_client(
            "PowerShell",
            serde_json::json!({
                "command": "Get-ChildItem",
                "working_directory": "C:\\work",
                "timeout": 120000.0,
                "background": true,
                "is_background": false,
                "dangerously_disable_sandbox": true
            }),
        );
        assert_eq!(adapted["command"], "Get-ChildItem");
        assert_eq!(adapted["timeout"], 120000);
        assert!(adapted["description"].is_string());
        assert_eq!(adapted["run_in_background"], true);
        assert_eq!(adapted["dangerouslyDisableSandbox"], true);
        for key in [
            "background",
            "is_background",
            "working_directory",
            "cwd",
            "shell",
        ] {
            assert!(
                adapted.get(key).is_none(),
                "unexpected PowerShell key: {key}"
            );
        }
    }

    #[test]
    fn cursor_delete_fallback_uses_literal_path_on_powershell() {
        let path = r"C:\work\O'Brien.txt";
        let adapted = adapt_tool_input_for_client(
            "PowerShell",
            serde_json::json!({
                "command": format!("rm -f -- {}", shell_single_quote(path))
            }),
        );
        assert_eq!(
            adapted["command"],
            "Remove-Item -Force -LiteralPath 'C:\\work\\O''Brien.txt'"
        );

        let ordinary = adapt_tool_input_for_client(
            "PowerShell",
            serde_json::json!({"command": "rm -f -- $dynamicPath"}),
        );
        assert_eq!(ordinary["command"], "rm -f -- $dynamicPath");
    }

    #[test]
    fn bash_translates_cursor_background_aliases_to_run_in_background() {
        let adapted = adapt_tool_input_for_client(
            "Bash",
            serde_json::json!({
                "command": "echo ok",
                "background": false
            }),
        );
        assert_eq!(adapted["run_in_background"], false);
        assert!(adapted.get("background").is_none());
    }

    #[test]
    fn mcp_whole_number_args_decode_as_integers() {
        use prost_types::value::Kind;
        let mut args_map = std::collections::HashMap::new();
        args_map.insert(
            "timeout_ms".into(),
            proto_value_bytes(Kind::NumberValue(5000.0)),
        );
        args_map.insert(
            "timeout".into(),
            proto_value_bytes(Kind::NumberValue(120000.0)),
        );
        let started = ToolCallStarted {
            call_id: "m-num".into(),
            tool_call: Some(ToolCall {
                mcp_tool_call: Some(crate::providers::cursor::proto::McpToolCall {
                    args: Some(crate::providers::cursor::proto::McpArgs {
                        name: "mcp_claude-local_get_command_or_subagent_output".into(),
                        args: args_map,
                        tool_call_id: "m-num".into(),
                        provider_identifier: "claude-local".into(),
                        tool_name: "mcp_claude-local_get_command_or_subagent_output".into(),
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&started).unwrap();
        assert_eq!(
            m.input["timeout_ms"].as_u64(),
            Some(5000),
            "protobuf NumberValue whole numbers must be JSON integers; grok-build rejects f64 timeout_ms"
        );
        assert_eq!(m.input["timeout"].as_u64(), Some(120000));
    }

    #[test]
    fn adapt_run_terminal_command_adds_description_and_integer_timeout() {
        let adapted = adapt_tool_input_for_client(
            "run_terminal_command",
            serde_json::json!({
                "command": "ls -la",
                "timeout": 30000.0,
                "is_background": false
            }),
        );
        assert_eq!(adapted["timeout"].as_u64(), Some(30000));
        assert_eq!(adapted["background"], false);
        assert!(adapted.get("is_background").is_none());
        let description = adapted["description"].as_str().unwrap_or("");
        assert!(!description.is_empty(), "grok-build requires description");
        assert!(
            !description.contains('\0'),
            "description must be clean text"
        );
    }

    #[test]
    fn adapt_bash_adds_description_for_claude_code_tui() {
        let adapted = adapt_tool_input_for_client(
            "Bash",
            serde_json::json!({
                "command": "python3 -c \"\nimport sys\nprint(1)\n\"",
                "timeout": 30000
            }),
        );
        let description = adapted["description"].as_str().unwrap_or("");
        assert!(
            !description.is_empty(),
            "Claude Code Bash TUI uses description as the widget title; Cursor Shell has none"
        );
        assert!(
            !description.contains('\n'),
            "description must be a single-line preview, got {description:?}"
        );
        assert!(adapted["command"].as_str().unwrap().contains('\n'));
    }

    #[test]
    fn adapt_get_output_coerces_timeout_ms_and_task_id() {
        let adapted = adapt_tool_input_for_client(
            "get_command_or_subagent_output",
            serde_json::json!({
                "task_id": "sa-1",
                "timeout_ms": 8000.0
            }),
        );
        assert_eq!(adapted["task_ids"], serde_json::json!(["sa-1"]));
        assert_eq!(adapted["timeout_ms"].as_u64(), Some(8000));
        assert!(adapted.get("task_id").is_none());
        let dropped = adapt_tool_input_for_client(
            "get_command_or_subagent_output",
            serde_json::json!({
                "task_ids": ["sa-1"],
                "timeout_ms": 80.5
            }),
        );
        assert!(
            dropped.get("timeout_ms").is_none(),
            "fractional timeout_ms must be dropped so grok-build can poll"
        );
    }

    #[test]
    fn adapt_task_output_normalizes_grok_fields_to_claude_schema() {
        let adapted = adapt_tool_input_for_client(
            "TaskOutput",
            serde_json::json!({
                "task_ids": ["sa-1"],
                "timeout_ms": 8000.0,
                "wait": false,
                "provider_identifier": "claude-local"
            }),
        );
        assert_eq!(
            adapted,
            serde_json::json!({"task_id": "sa-1", "timeout": 8000, "block": false})
        );
    }

    #[test]
    fn adapt_task_stop_accepts_shell_alias_and_drops_transport_fields() {
        let adapted = adapt_tool_input_for_client(
            "KillShell",
            serde_json::json!({
                "shell_id": "shell-1",
                "task_ids": ["ignored"],
                "provider_identifier": "claude-local"
            }),
        );
        assert_eq!(
            adapted,
            serde_json::json!({"task_id": "shell-1", "shell_id": "shell-1"})
        );

        let grok = adapt_tool_input_for_client(
            "kill_command_or_subagent",
            serde_json::json!({"task_ids": ["sa-2"], "provider_identifier": "claude-local"}),
        );
        assert_eq!(grok["task_id"], "sa-2");
        assert_eq!(grok["task_ids"], serde_json::json!(["sa-2"]));
    }

    #[test]
    fn adapt_edit_family_removes_unknown_fields_and_converts_legacy_notebook_index() {
        let edit = adapt_tool_input_for_client(
            "Edit",
            serde_json::json!({
                "file_path": "/tmp/a.rs",
                "old_string": "one",
                "new_string": "two",
                "replace_all": false,
                "tool_use_id": "transport"
            }),
        );
        assert_eq!(
            edit,
            serde_json::json!({
                "file_path": "/tmp/a.rs",
                "old_string": "one",
                "new_string": "two",
                "replace_all": false
            })
        );

        let multi = adapt_tool_input_for_client(
            "MultiEdit",
            serde_json::json!({
                "file_path": "/tmp/a.rs",
                "edits": [{"old_string": "one", "new_string": "two", "id": 1}],
                "replace_all": true,
                "debug": true
            }),
        );
        assert_eq!(multi["edits"][0].get("id"), None);
        assert_eq!(multi.get("debug"), None);

        let notebook = adapt_tool_input_for_client(
            "NotebookEdit",
            serde_json::json!({
                "notebook_path": "/tmp/a.ipynb",
                "cell_number": 2,
                "new_source": "print(2)",
                "cell_type": "code",
                "edit_mode": "replace",
                "verbose": true
            }),
        );
        assert_eq!(notebook["cell_id"], "cell-2");
        assert!(notebook.get("cell_number").is_none());
        assert!(notebook.get("verbose").is_none());
    }

    #[test]
    fn adapt_ls_path_to_list_dir_or_shell() {
        let listed =
            adapt_tool_input_for_client("list_dir", serde_json::json!({"path": "/tmp/carve"}));
        assert_eq!(listed["target_directory"], "/tmp/carve");
        assert!(listed.get("path").is_none());
        let shelled = adapt_tool_input_for_client(
            "run_terminal_command",
            serde_json::json!({"path": "/tmp/carve"}),
        );
        assert_eq!(shelled["command"].as_str(), Some("ls -la -- '/tmp/carve'"));
        assert!(
            shelled["description"]
                .as_str()
                .is_some_and(|s| !s.is_empty())
        );
    }

    #[test]
    fn adapt_grep_renames_case_insensitive_for_grok() {
        let adapted = adapt_tool_input_for_client(
            "grep",
            serde_json::json!({
                "pattern": "TODO",
                "path": "src",
                "case_insensitive": true,
                "head_limit": 20.0
            }),
        );
        assert_eq!(adapted["-i"], true);
        assert_eq!(adapted["head_limit"].as_u64(), Some(20));
        assert!(adapted.get("case_insensitive").is_none());
        let claude = adapt_tool_input_for_client(
            "Grep",
            serde_json::json!({"pattern": "TODO", "case_insensitive": true}),
        );
        assert_eq!(claude["-i"], true);
        assert!(claude.get("case_insensitive").is_none());
    }

    #[test]
    fn adapt_todo_write_adds_active_form_and_drops_merge() {
        let adapted = adapt_tool_input_for_client(
            "TodoWrite",
            serde_json::json!({
                "merge": true,
                "todos": [{"id": "1", "content": "collect", "status": "in_progress"}]
            }),
        );
        assert!(adapted.get("merge").is_none());
        assert_eq!(adapted["todos"][0]["content"], "collect");
        assert_eq!(adapted["todos"][0]["activeForm"], "Working on collect");
        assert!(adapted["todos"][0].get("id").is_none());
        let grok = adapt_tool_input_for_client(
            "todo_write",
            serde_json::json!({
                "merge": true,
                "todos": [{"id": "1", "content": "collect", "status": "in_progress"}]
            }),
        );
        assert_eq!(grok["merge"], true);
        assert!(grok["todos"][0].get("activeForm").is_none());
    }

    #[test]
    fn adapt_todo_write_repairs_empty_items_to_strict_schema() {
        let adapted = adapt_tool_input_for_client(
            "TodoWrite",
            serde_json::json!({
                "todos": [{"id": "internal", "content": " ", "status": "bogus", "activeForm": ""}]
            }),
        );
        assert_eq!(
            adapted["todos"][0],
            serde_json::json!({
                "content": "todo",
                "status": "pending",
                "activeForm": "Working on todo"
            })
        );
    }

    #[test]
    fn adapt_webfetch_adds_prompt_for_claude_code() {
        let claude = adapt_tool_input_for_client(
            "WebFetch",
            serde_json::json!({"url": "https://example.com/doc"}),
        );
        assert_eq!(claude["url"], "https://example.com/doc");
        assert!(
            claude["prompt"].as_str().is_some_and(|p| !p.is_empty()),
            "Claude Code WebFetch requires prompt"
        );
        let grok = adapt_tool_input_for_client(
            "web_fetch",
            serde_json::json!({"url": "https://example.com/doc"}),
        );
        assert_eq!(grok["url"], "https://example.com/doc");
        assert!(grok.get("prompt").is_none());
    }

    #[test]
    fn maps_cursor_ls_to_canonical_ls_not_bash() {
        let started = ToolCallStarted {
            call_id: "ls1".into(),
            tool_call: Some(ToolCall {
                ls_tool_call: Some(crate::providers::cursor::proto::LsToolCall {
                    args: Some(crate::providers::cursor::proto::LsArgs {
                        path: "/tmp/carve".into(),
                    }),
                }),
                ..Default::default()
            }),
            model_call_id: String::new(),
        };
        let m = map_tool_call_started(&started).unwrap();
        assert_eq!(m.name, "LS");
        assert_eq!(m.input["path"], "/tmp/carve");
        assert!(m.input.get("command").is_none());
    }
}
