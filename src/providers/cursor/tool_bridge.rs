//! Cursor tool bridge: state machine, result builders, pending tool tracking,
//! stream re-entry, and SSE pause/resume.
//!
//! The bridge coordinates the pause-and-continue lifecycle when the Cursor
//! upstream emits a `<tool_use>` text block. The bridge pauses the SSE stream,
//! stores the pending tool, and waits for Claude's `tool_result` in the next
//! client request. On resume it builds Cursor protocol result messages and
//! continues producing SSE output from stored upstream events.

use std::collections::BTreeSet;
use std::sync::Mutex;

use once_cell::sync::Lazy;

use crate::anthropic::schema::MessagesRequest;
use crate::providers::cursor::native_tools::{
    adapt_tool_input_for_client, advertised_name_fallbacks, json_u64, shell_single_quote,
};
use crate::providers::cursor::request::{
    claude_tool_names_equivalent, cursor_mcp_wire_name, is_claude_local_mcp_spelling,
    is_model_visible_tool_definition, is_text_editor_tool_name, preferred_text_editor_name,
    strip_mcp_provider_prefix,
};
use crate::providers::cursor::response::CursorStreamEvent;
use crate::providers::cursor::sse::CursorSseFramer;
use crate::providers::cursor::tool_use_xml::{CursorToolUseXmlParser, RecoveredCursorEvent};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Execution context for a Cursor tool.
#[derive(Debug, Clone, PartialEq)]
pub struct CursorExec {
    pub id: Option<u64>,
    pub exec_id: Option<String>,
    pub args: serde_json::Value,
}

/// A tool result produced by Claude.
#[derive(Debug, Clone, PartialEq)]
pub struct CursorNativeToolResult {
    pub content: String,
    pub is_error: bool,
}

/// A pending Cursor tool that Claude must fulfill.
#[derive(Debug, Clone)]
pub enum PendingCursorTool {
    Read {
        tool_use_id: String,
        path: String,
    },
    Write {
        tool_use_id: String,
        path: String,
        content: String,
    },
    Bash {
        tool_use_id: String,
        command: String,
        working_directory: String,
        timeout_ms: u64,
    },
    Delete {
        tool_use_id: String,
        path: String,
    },
    Grep {
        tool_use_id: String,
        pattern: String,
        path: String,
    },
    Ls {
        tool_use_id: String,
        path: String,
    },
    /// Any Claude tool name (Glob, Grep, …) from native Cursor mapping.
    Generic {
        tool_use_id: String,
        name: String,
        input: serde_json::Value,
    },
}

impl PendingCursorTool {
    pub fn name(&self) -> &str {
        match self {
            Self::Read { .. } => "Read",
            Self::Write { .. } => "Write",
            Self::Bash { .. } => "Bash",
            Self::Delete { .. } => "Delete",
            Self::Grep { .. } => "Grep",
            Self::Ls { .. } => "LS",
            Self::Generic { name, .. } => name.as_str(),
        }
    }

    pub fn tool_use_id(&self) -> &str {
        match self {
            Self::Read { tool_use_id, .. }
            | Self::Write { tool_use_id, .. }
            | Self::Bash { tool_use_id, .. }
            | Self::Delete { tool_use_id, .. }
            | Self::Grep { tool_use_id, .. }
            | Self::Ls { tool_use_id, .. }
            | Self::Generic { tool_use_id, .. } => tool_use_id,
        }
    }

    /// Build the JSON input that matches the Claude tool_use block.
    pub fn input_json(&self) -> serde_json::Value {
        match self {
            Self::Generic { input, .. } => input.clone(),
            Self::Read { path, .. } => {
                serde_json::json!({ "file_path": path })
            }
            Self::Write { path, content, .. } => {
                serde_json::json!({ "file_path": path, "content": content })
            }
            Self::Bash {
                command,
                working_directory,
                timeout_ms,
                ..
            } => {
                let cmd = if working_directory.is_empty() {
                    command.clone()
                } else {
                    format!("cd {} && {command}", shell_single_quote(working_directory))
                };
                serde_json::json!({
                    "command": cmd,
                    "timeout": timeout_ms,
                    "description": "Run Cursor-requested shell command",
                    "run_in_background": false,
                    "dangerouslyDisableSandbox": false
                })
            }
            Self::Delete { path, .. } => serde_json::json!({ "path": path }),
            Self::Grep { pattern, path, .. } => {
                let mut input = serde_json::Map::new();
                input.insert("pattern".into(), serde_json::Value::String(pattern.clone()));
                if !path.is_empty() {
                    input.insert("path".into(), serde_json::Value::String(path.clone()));
                }
                serde_json::Value::Object(input)
            }
            Self::Ls { path, .. } => serde_json::json!({ "path": path }),
        }
    }
}

/// Bridge state stored per live-run key.
///
/// The parent Claude session id is not sufficient for nested Claude Code
/// agents: nested Workflow requests intentionally reuse that header and are
/// distinguished by `x-claude-code-agent-id`. Callers pass the same
/// length-prefixed key used by the live registry (`p:...` / `a:...`) so a
/// pending tool from one agent can never be consumed by another agent in the
/// same Claude session.
#[derive(Debug)]
pub struct CursorBridgeState {
    pub session_id: String,
    pub message_id: String,
    pub model: String,
    pub pending_tool: Option<PendingCursorTool>,
    pub remaining_events: Vec<CursorStreamEvent>,
    pub event_cursor: usize,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub allowed_tool_names: Option<BTreeSet<String>>,
    pub xml_parser: CursorToolUseXmlParser,
}

impl CursorBridgeState {
    fn new(
        session_id: String,
        message_id: String,
        model: String,
        allowed_tool_names: Option<BTreeSet<String>>,
        id_factory: Box<dyn FnMut() -> String + Send>,
    ) -> Self {
        Self {
            session_id,
            message_id,
            model,
            pending_tool: None,
            remaining_events: Vec::new(),
            event_cursor: 0,
            input_tokens: 0,
            output_tokens: 0,
            allowed_tool_names: allowed_tool_names.clone(),
            xml_parser: CursorToolUseXmlParser::new_with_id_factory(allowed_tool_names, id_factory),
        }
    }
}

// ---------------------------------------------------------------------------
// Global bridge registry
// ---------------------------------------------------------------------------

static BRIDGE_REGISTRY: Lazy<Mutex<BridgeRegistryInner>> =
    Lazy::new(|| Mutex::new(BridgeRegistryInner::new()));

struct BridgeRegistryInner {
    sessions: Vec<CursorBridgeState>,
}

impl BridgeRegistryInner {
    fn new() -> Self {
        Self {
            sessions: Vec::new(),
        }
    }
}

/// Global registry of active bridge sessions.
pub struct BridgeRegistry;

impl BridgeRegistry {
    /// Insert a new bridge state for a live-run key.
    ///
    /// There is at most one paused bridge per key. Replacing an older entry is
    /// important after a client retry: retaining both would make `find()` pick
    /// a stale pending tool and resume the wrong generation.
    pub fn insert(state: CursorBridgeState) {
        let mut reg = BRIDGE_REGISTRY.lock().unwrap();
        if let Some(pos) = reg
            .sessions
            .iter()
            .position(|existing| existing.session_id == state.session_id)
        {
            reg.sessions[pos] = state;
        } else {
            reg.sessions.push(state);
        }
    }

    /// Get the bridge state index for a live-run key.
    pub fn get(session_id: &str) -> Option<usize> {
        let reg = BRIDGE_REGISTRY.lock().unwrap();
        reg.sessions.iter().position(|s| s.session_id == session_id)
    }

    /// Get the pending tool for a live-run key (if any).
    pub fn pending_tool(session_id: &str) -> Option<PendingCursorTool> {
        let reg = BRIDGE_REGISTRY.lock().unwrap();
        reg.sessions
            .iter()
            .find(|s| s.session_id == session_id)
            .and_then(|s| s.pending_tool.clone())
    }

    /// Take the bridge state for a live-run key (removes it).
    pub fn take(session_id: &str) -> Option<CursorBridgeState> {
        let mut reg = BRIDGE_REGISTRY.lock().unwrap();
        let pos = reg
            .sessions
            .iter()
            .position(|s| s.session_id == session_id)?;
        Some(reg.sessions.swap_remove(pos))
    }

    /// Remove a bridge state for a live-run key.
    pub fn remove(session_id: &str) {
        let mut reg = BRIDGE_REGISTRY.lock().unwrap();
        reg.sessions.retain(|s| s.session_id != session_id);
    }

    /// Insert or update the pending tool for a live-run key.
    pub fn set_pending_tool(session_id: &str, tool: PendingCursorTool) {
        let mut reg = BRIDGE_REGISTRY.lock().unwrap();
        if let Some(state) = reg.sessions.iter_mut().find(|s| s.session_id == session_id) {
            state.pending_tool = Some(tool);
        }
    }

    /// Update usage for a live-run key.
    pub fn record_usage(session_id: &str, input_tokens: u64, output_tokens: u64) {
        let mut reg = BRIDGE_REGISTRY.lock().unwrap();
        if let Some(state) = reg.sessions.iter_mut().find(|s| s.session_id == session_id) {
            state.input_tokens = input_tokens.max(state.input_tokens);
            state.output_tokens = output_tokens.max(state.output_tokens);
        }
    }

    /// Clear all bridge state.
    pub fn clear() {
        let mut reg = BRIDGE_REGISTRY.lock().unwrap();
        reg.sessions.clear();
    }

    /// Number of active sessions.
    pub fn active_count() -> usize {
        let reg = BRIDGE_REGISTRY.lock().unwrap();
        reg.sessions.len()
    }
}

/// Serialize tests that share the process-wide bridge registry.
pub fn lock_bridge_registry_for_test() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------------------
// Tool detection helpers
// ---------------------------------------------------------------------------

/// Extract advertised tool names from a MessagesRequest.
pub fn advertised_tool_names(body: &MessagesRequest) -> Option<BTreeSet<String>> {
    let tools = body.extra.get("tools")?.as_array()?;
    let names: BTreeSet<String> = tools
        .iter()
        .filter(|tool| is_model_visible_tool_definition(tool))
        .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
        .map(|n| n.to_string())
        .collect();
    // `tools` absent and `tools: []` have different meanings at the protocol
    // boundary. An explicit empty catalog must suppress native Cursor tool
    // emission; returning `None` here makes the live path interpret it as an
    // unfiltered legacy request and can inject synthetic tool_use blocks into
    // a plain chat response.
    Some(names)
}

/// Whether the request can use the Cursor native / XML tool bridge.
///
/// Full Claude Code agent mode: streaming + session id + any advertised tools.
pub fn can_bridge_cursor_native_tools(body: &MessagesRequest, session_id: Option<&str>) -> bool {
    let _sid = match session_id {
        Some(id) if !id.is_empty() => id,
        _ => return false,
    };
    if !body.stream {
        return false;
    }
    advertised_tool_names(body).is_some_and(|n| !n.is_empty())
}

/// Incremental text-to-event adapter used by Sand's tool-catalog fallback.
///
/// Some Sand provider/model combinations accept a request only when the
/// `tools` field is empty, while still emitting the XML tool protocol in their
/// text stream.  The normal Cursor bridge is entered only when a native tool
/// catalog is present, so callers on that fallback path need a small adapter
/// that feeds text through the same parser and exposes recovered calls as
/// structured `NativeTool` events.  Keeping this adapter here makes the
/// allow-list and XML semantics identical to the existing Agent bridge.
#[derive(Debug)]
pub struct CursorXmlEventBridge {
    parser: CursorToolUseXmlParser,
    allowed_tool_names: Option<BTreeSet<String>>,
}

impl CursorXmlEventBridge {
    /// Create an adapter.  `Some(set)` restricts recovered calls to the tool
    /// names advertised by Claude Code; `None` is reserved for protocol
    /// fixtures where the caller intentionally accepts every XML name.
    pub fn new(allowed_tool_names: Option<BTreeSet<String>>) -> Self {
        Self {
            parser: CursorToolUseXmlParser::new(allowed_tool_names.clone()),
            allowed_tool_names,
        }
    }

    /// Decode one possibly fragmented text delta into Cursor events.
    pub fn push(&mut self, text: &str) -> Vec<CursorStreamEvent> {
        self.parser
            .push(text)
            .into_iter()
            .map(recovered_event_to_cursor_event)
            .collect()
    }

    /// Flush a stream at EOF, converting any complete buffered XML call and
    /// preserving ordinary trailing text.
    pub fn flush(&mut self) -> Vec<CursorStreamEvent> {
        self.parser
            .flush()
            .into_iter()
            .map(recovered_event_to_cursor_event)
            .collect()
    }

    /// Whether at least one XML tool call has been recovered.
    pub fn saw_tool_use(&self) -> bool {
        self.parser.saw_tool_use()
    }

    /// Discard buffered text and recovered-call state before replaying a
    /// failed upstream attempt.  The allow-list remains unchanged, while the
    /// new parser receives fresh IDs and cannot join a partial XML tag from
    /// the abandoned stream with the replacement response.
    pub fn reset(&mut self) {
        self.parser = CursorToolUseXmlParser::new(self.allowed_tool_names.clone());
    }
}

fn recovered_event_to_cursor_event(event: RecoveredCursorEvent) -> CursorStreamEvent {
    match event {
        RecoveredCursorEvent::Text(text) => CursorStreamEvent::TextDelta { text },
        RecoveredCursorEvent::ToolUse(tool_use) => CursorStreamEvent::NativeTool {
            tool_use_id: tool_use.id,
            name: tool_use.name,
            input: serde_json::Value::Object(tool_use.input),
        },
    }
}

// ---------------------------------------------------------------------------
// Result helpers
// ---------------------------------------------------------------------------

/// Find the last `tool_result` block matching `tool_use_id` in the request.
pub fn find_tool_result<'a>(
    body: &'a MessagesRequest,
    tool_use_id: &str,
) -> Option<&'a serde_json::Value> {
    for message in body.messages.iter().rev() {
        if message.role != "user" {
            continue;
        }
        let blocks = match &message.content {
            serde_json::Value::Array(arr) => arr,
            _ => continue,
        };
        for block in blocks.iter().rev() {
            if block.get("type").and_then(|t| t.as_str()) == Some("tool_result")
                && block.get("tool_use_id").and_then(|t| t.as_str()) == Some(tool_use_id)
            {
                return Some(block);
            }
        }
    }
    None
}

/// Render the content of a `tool_result` block into a string.
pub fn render_tool_result_content(result: &serde_json::Value) -> String {
    let Some(content) = result.get("content") else {
        // Structured tool implementations occasionally put their payload in
        // `structured_output`/`data` instead of Anthropic's content field.
        // Preserve it for Cursor rather than silently acknowledging an empty
        // result, which makes the model repeat the same tool call.
        return result
            .get("structured_output")
            .or_else(|| result.get("data"))
            .and_then(|value| serde_json::to_string(value).ok())
            .unwrap_or_default();
    };
    match content {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Array(blocks) => blocks
            .iter()
            .map(render_tool_result_block)
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        serde_json::Value::Object(_) => render_tool_result_block(content),
        serde_json::Value::Null => String::new(),
        scalar => serde_json::to_string(scalar).unwrap_or_default(),
    }
}

fn render_tool_result_block(block: &serde_json::Value) -> String {
    if let serde_json::Value::String(text) = block {
        return text.clone();
    }
    match block.get("type").and_then(|value| value.as_str()) {
        Some("text") => block
            .get("text")
            .and_then(|text| text.as_str())
            .unwrap_or("")
            .to_string(),
        Some("image") | Some("input_image") => "[image result omitted]".to_string(),
        Some("thinking") => block
            .get("thinking")
            .and_then(|text| text.as_str())
            .unwrap_or("")
            .to_string(),
        _ => serde_json::to_string(block).unwrap_or_default(),
    }
}

/// Whether a `tool_result` block indicates an error.
pub fn tool_result_is_error(result: &serde_json::Value) -> bool {
    result
        .get("is_error")
        .and_then(|e| e.as_bool())
        .unwrap_or(false)
}

/// Build the partial JSON string for a pending tool's input (for the
/// input_json_delta in the tool_use content block).
pub fn build_tool_use_input_json(tool: &PendingCursorTool) -> String {
    serde_json::to_string(&tool.input_json()).unwrap_or_else(|_| "{}".to_string())
}

// ---------------------------------------------------------------------------
// Cursor protocol message builders
// ---------------------------------------------------------------------------

/// Inject `id` and `execId` fields into a JSON payload.
pub fn with_exec_ids(
    exec: &CursorExec,
    mut payload: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    if let Some(id) = exec.id {
        payload.insert("id".into(), id.into());
    }
    if let Some(ref exec_id) = exec.exec_id {
        payload.insert("execId".into(), exec_id.clone().into());
    }
    serde_json::Value::Object(payload)
}

/// Build the Cursor `readResult` message from a Claude `tool_result`.
pub fn build_read_result_from_native(
    exec: &CursorExec,
    result: &CursorNativeToolResult,
) -> serde_json::Value {
    let path = exec
        .args
        .get("file_path")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let content = &result.content;
    let lines = if content.is_empty() {
        0
    } else {
        content.lines().count()
    };
    let file_size = content.len().to_string();

    let read_result = if result.is_error {
        serde_json::json!({
            "error": {
                "path": path,
                "error": content
            }
        })
    } else {
        serde_json::json!({
            "success": {
                "path": path,
                "content": content,
                "totalLines": lines,
                "fileSize": file_size
            }
        })
    };

    let mut map = serde_json::Map::new();
    map.insert("readResult".into(), read_result);
    with_exec_ids(exec, map)
}

/// Build the Cursor `writeResult` message from a Claude `tool_result`.
pub fn build_write_result_from_native(
    exec: &CursorExec,
    result: &CursorNativeToolResult,
) -> serde_json::Value {
    let path = exec
        .args
        .get("file_path")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let write_result = if result.is_error {
        serde_json::json!({
            "error": {
                "path": path,
                "error": result.content
            }
        })
    } else {
        // Prefer written file bytes from exec args — tool_result text is a
        // status string and would skew linesCreated/fileSize (prost path in
        // exec_results.rs already uses file content).
        let file = exec
            .args
            .get("content")
            .or_else(|| exec.args.get("file_text"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let lines = if file.is_empty() {
            0
        } else {
            file.lines().count()
        };
        serde_json::json!({
            "success": {
                "path": path,
                "linesCreated": lines,
                "fileSize": file.len()
            }
        })
    };

    let mut map = serde_json::Map::new();
    map.insert("writeResult".into(), write_result);
    with_exec_ids(exec, map)
}

/// Build the Cursor `deleteResult` message from a Claude `tool_result`.
pub fn build_delete_result_from_native(
    exec: &CursorExec,
    result: &CursorNativeToolResult,
) -> serde_json::Value {
    let path = exec
        .args
        .get("path")
        .or_else(|| exec.args.get("file_path"))
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let delete_result = if result.is_error {
        serde_json::json!({
            "error": {
                "path": path,
                "error": result.content
            }
        })
    } else {
        serde_json::json!({
            "success": {
                "path": path,
                "deletedFile": path,
                "fileSize": 0,
                "prevContent": ""
            }
        })
    };
    let mut map = serde_json::Map::new();
    map.insert("deleteResult".into(), delete_result);
    with_exec_ids(exec, map)
}

/// Build the Cursor `grepResult` message from a Claude `tool_result`.
pub fn build_grep_result_from_native(
    exec: &CursorExec,
    result: &CursorNativeToolResult,
) -> serde_json::Value {
    let pattern = exec
        .args
        .get("pattern")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let path = exec
        .args
        .get("path")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let grep_result = if result.is_error {
        serde_json::json!({ "error": { "error": result.content } })
    } else {
        let matches: Vec<serde_json::Value> = result
            .content
            .lines()
            .enumerate()
            .map(|(index, line)| {
                serde_json::json!({
                    "lineNumber": index.saturating_add(1),
                    "content": line,
                    "contentTruncated": false,
                    "isContextLine": false
                })
            })
            .collect();
        let line_count = matches.len();
        serde_json::json!({
            "success": {
                "pattern": pattern,
                "path": path,
                "outputMode": "content",
                "workspaceResults": {},
                "activeEditorResult": {
                    "content": {
                        "matches": [{
                            "file": path,
                            "matches": matches
                        }],
                        "totalLines": line_count,
                        "totalMatchedLines": line_count,
                        "clientTruncated": false,
                        "ripgrepTruncated": false
                    }
                }
            }
        })
    };
    let mut map = serde_json::Map::new();
    map.insert("grepResult".into(), grep_result);
    with_exec_ids(exec, map)
}

/// Build the Cursor `lsResult` message from a Claude `tool_result`.
pub fn build_ls_result_from_native(
    exec: &CursorExec,
    result: &CursorNativeToolResult,
) -> serde_json::Value {
    let path = exec
        .args
        .get("path")
        .or_else(|| exec.args.get("target_directory"))
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let ls_result = if result.is_error {
        serde_json::json!({
            "error": {
                "path": path,
                "error": result.content
            }
        })
    } else {
        let children_files: Vec<serde_json::Value> = result
            .content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::json!({ "name": line }))
            .collect();
        serde_json::json!({
            "success": {
                "directoryTreeRoot": {
                    "absPath": path,
                    "childrenDirs": [],
                    "numFiles": children_files.len(),
                    "childrenFiles": children_files,
                    "childrenWereProcessed": true,
                    "fullSubtreeExtensionCounts": {}
                }
            }
        })
    };
    let mut map = serde_json::Map::new();
    map.insert("lsResult".into(), ls_result);
    with_exec_ids(exec, map)
}

/// Build the collection of Cursor `shellStream` messages from a Claude
/// `tool_result`.
///
/// Returns: start, stdout/stderr, exit, streamClose.
pub fn build_shell_stream_result(
    exec: &CursorExec,
    result: &CursorNativeToolResult,
    local_execution_time: std::time::Duration,
    cwd: &str,
) -> Vec<serde_json::Value> {
    let mut messages: Vec<serde_json::Value> = Vec::new();

    // Start
    let start_msg = with_exec_ids(
        exec,
        serde_json::json!({ "shellStream": { "start": {} } })
            .as_object()
            .cloned()
            .unwrap_or_default(),
    );
    messages.push(start_msg);

    // Content (stdout or stderr)
    if !result.content.is_empty() {
        let stream_key = if result.is_error { "stderr" } else { "stdout" };
        let content_msg = with_exec_ids(
            exec,
            serde_json::json!({ "shellStream": { stream_key: { "data": result.content } } })
                .as_object()
                .cloned()
                .unwrap_or_default(),
        );
        messages.push(content_msg);
    }

    // Exit
    let exit_code: u32 = if result.is_error { 1 } else { 0 };
    let exit_msg = with_exec_ids(
        exec,
        serde_json::json!({
            "shellStream": {
                "exit": {
                    "code": exit_code,
                    "cwd": cwd,
                    "localExecutionTimeMs": local_execution_time.as_millis() as u64,
                }
            }
        })
        .as_object()
        .cloned()
        .unwrap_or_default(),
    );
    messages.push(exit_msg);

    // Stream close
    if let Some(id) = exec.id {
        let close_msg = serde_json::json!({
            "execClientControlMessage": {
                "streamClose": {
                    "id": id
                }
            }
        });
        messages.push(close_msg);
    } else {
        let close_msg = serde_json::json!({
            "execClientControlMessage": {
                "streamClose": {}
            }
        });
        messages.push(close_msg);
    }

    messages
}

// ---------------------------------------------------------------------------
// Bridge start and resume
// ---------------------------------------------------------------------------

/// Start a new tool bridge session.
///
/// Processes upstream events through XML recovery. When a `<tool_use>` is
/// recovered, emits the SSE pause (tool_use content block + message_stop with
/// stop_reason="tool_use") and stores the bridge state for resume.
///
/// Returns the SSE bytes and whether a tool_use pause was emitted.
pub fn start_cursor_tool_bridge(
    message_id: &str,
    model: &str,
    session_id: &str,
    events: &[CursorStreamEvent],
    allowed_tool_names: Option<BTreeSet<String>>,
    id_factory: Box<dyn FnMut() -> String + Send>,
) -> (Vec<u8>, bool) {
    let mut sse = Vec::new();
    let mut framer = CursorSseFramer::new(&mut sse, message_id, model);

    let mut state = CursorBridgeState::new(
        session_id.to_string(),
        message_id.to_string(),
        model.to_string(),
        allowed_tool_names,
        id_factory,
    );

    let mut paused = false;

    for event in events {
        if paused {
            state.remaining_events.push(event.clone());
            continue;
        }

        match event {
            CursorStreamEvent::ThinkingDelta { text } => {
                framer.emit_thinking_delta(text);
            }
            CursorStreamEvent::NativeTool {
                tool_use_id,
                name,
                input,
            } => {
                if paused {
                    state.remaining_events.push(event.clone());
                    continue;
                }
                let Some(emit_name) =
                    resolve_advertised_name(name, state.allowed_tool_names.as_ref())
                else {
                    // Tool not in Claude Code's advertised set — skip.
                    continue;
                };
                let adapted = adapt_tool_input_for_client(&emit_name, input.clone());
                let input_json =
                    serde_json::to_string(&adapted).unwrap_or_else(|_| "{}".to_string());
                framer.emit_tool_pause(tool_use_id, &emit_name, &input_json);
                state.pending_tool =
                    Some(pending_from_native_tool(tool_use_id, &emit_name, &adapted));
                paused = true;
            }
            CursorStreamEvent::TextDelta { text } => {
                let recovered = state.xml_parser.push(text);
                for recovered_event in &recovered {
                    if paused {
                        match recovered_event {
                            RecoveredCursorEvent::Text(t) => {
                                state
                                    .remaining_events
                                    .push(CursorStreamEvent::TextDelta { text: t.clone() });
                            }
                            RecoveredCursorEvent::ToolUse(tool_use) => {
                                // Preserve a second tool recovered from the
                                // same upstream delta for the next resume.
                                state.remaining_events.push(CursorStreamEvent::NativeTool {
                                    tool_use_id: tool_use.id.clone(),
                                    name: tool_use.name.clone(),
                                    input: serde_json::Value::Object(tool_use.input.clone()),
                                });
                            }
                        }
                        continue;
                    }
                    match recovered_event {
                        RecoveredCursorEvent::Text(t) => {
                            framer.emit_text_delta(t);
                        }
                        RecoveredCursorEvent::ToolUse(tool_use) => {
                            let input = serde_json::Value::Object(tool_use.input.clone());
                            let Some(emit_name) = resolve_advertised_name(
                                &tool_use.name,
                                state.allowed_tool_names.as_ref(),
                            ) else {
                                // Never expose a recovered XML call that was
                                // not part of Claude Code's advertised tool
                                // set. Native execs use the same gate below.
                                continue;
                            };
                            let adapted = adapt_tool_input_for_client(&emit_name, input);
                            let input_json = serde_json::to_string(&adapted)
                                .unwrap_or_else(|_| "{}".to_string());
                            framer.emit_tool_pause(&tool_use.id, &emit_name, &input_json);

                            state.pending_tool =
                                Some(pending_from_native_tool(&tool_use.id, &emit_name, &adapted));

                            paused = true;
                        }
                    }
                }
            }
            CursorStreamEvent::Usage {
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
            } => {
                framer.record_usage(
                    *input_tokens,
                    *output_tokens,
                    *cache_read_tokens,
                    *cache_write_tokens,
                );
                state.input_tokens = *input_tokens;
                state.output_tokens = *output_tokens;
            }
            CursorStreamEvent::OutputTokenDelta { tokens } => {
                framer.add_output_tokens(*tokens);
                state.output_tokens = state.output_tokens.saturating_add(*tokens);
            }
            CursorStreamEvent::Session { .. } => {
                // Session info is not mapped to SSE events
            }
            CursorStreamEvent::End => {
                // If we haven't paused, finalize normally
                if !paused {
                    // Process any remaining XML before finalizing
                    let flushed = state.xml_parser.flush();
                    for evt in flushed {
                        match evt {
                            RecoveredCursorEvent::Text(text) => framer.emit_text_delta(&text),
                            RecoveredCursorEvent::ToolUse(tool_use) => {
                                let input = serde_json::Value::Object(tool_use.input.clone());
                                let Some(emit_name) = resolve_advertised_name(
                                    &tool_use.name,
                                    state.allowed_tool_names.as_ref(),
                                ) else {
                                    continue;
                                };
                                let adapted = adapt_tool_input_for_client(&emit_name, input);
                                let input_json = serde_json::to_string(&adapted)
                                    .unwrap_or_else(|_| "{}".to_string());
                                framer.emit_tool_pause(&tool_use.id, &emit_name, &input_json);
                                state.pending_tool = Some(pending_from_native_tool(
                                    &tool_use.id,
                                    &emit_name,
                                    &adapted,
                                ));
                                paused = true;
                            }
                        }
                    }
                    if !paused {
                        framer.finalize();
                    }
                }
            }
        }
    }

    if !paused {
        // Flush any remaining text from XML parser
        let flushed = state.xml_parser.flush();
        for evt in flushed {
            match evt {
                RecoveredCursorEvent::Text(text) => framer.emit_text_delta(&text),
                RecoveredCursorEvent::ToolUse(tool_use) => {
                    let input = serde_json::Value::Object(tool_use.input.clone());
                    let Some(emit_name) =
                        resolve_advertised_name(&tool_use.name, state.allowed_tool_names.as_ref())
                    else {
                        continue;
                    };
                    let adapted = adapt_tool_input_for_client(&emit_name, input);
                    let input_json =
                        serde_json::to_string(&adapted).unwrap_or_else(|_| "{}".to_string());
                    framer.emit_tool_pause(&tool_use.id, &emit_name, &input_json);
                    state.pending_tool =
                        Some(pending_from_native_tool(&tool_use.id, &emit_name, &adapted));
                    paused = true;
                }
            }
        }
        if !paused {
            framer.finalize();
        }
    }

    if paused {
        let remaining = state.remaining_events.clone();
        let mut stored_state = CursorBridgeState::new(
            session_id.to_string(),
            message_id.to_string(),
            model.to_string(),
            state.allowed_tool_names.clone(),
            Box::new(|| {
                format!(
                    "call_cursor_{}",
                    uuid::Uuid::new_v4().to_string().replace('-', "")
                )
            }),
        );
        stored_state.pending_tool = state.pending_tool.clone();
        stored_state.remaining_events = remaining;
        stored_state.event_cursor = 0;
        stored_state.input_tokens = state.input_tokens;
        stored_state.output_tokens = state.output_tokens;
        // Preserve the incremental parser across the pause. A second tool can
        // begin in a delta before the first tool pause and finish only after
        // Claude sends its result; rebuilding a fresh parser would lose that
        // partial XML and could also forget the advertised-tool allow-list.
        stored_state.xml_parser = state.xml_parser;
        BridgeRegistry::insert(stored_state);
    }

    (sse, paused)
}

/// Convert a collected Cursor event segment into an Anthropic SSE response.
///
/// Sand's inference stream is often collected before the HTTP response is
/// committed (for example while classifying a tool-capability probe).  This
/// entry point keeps that path on the same XML recovery, tool allow-list, and
/// pause/registry lifecycle as the normal buffered Cursor bridge.  A paused
/// segment is retained in [`BridgeRegistry`] and can be continued with
/// [`resume_cursor_tool_bridge`].
///
/// The returned boolean is `true` when a client-visible `tool_use` pause was
/// emitted.  The event slice is borrowed so callers can safely retain their
/// replay buffer; the bridge only stores cloned continuation events.
pub fn bridge_cursor_events_to_sse(
    message_id: &str,
    model: &str,
    session_id: &str,
    events: &[CursorStreamEvent],
    allowed_tool_names: Option<BTreeSet<String>>,
) -> (Vec<u8>, bool) {
    start_cursor_tool_bridge(
        message_id,
        model,
        session_id,
        events,
        allowed_tool_names,
        Box::new(|| {
            format!(
                "call_cursor_{}",
                uuid::Uuid::new_v4().to_string().replace('-', "")
            )
        }),
    )
}

/// Convert a complete event segment for a stateless transport such as Sand.
///
/// Unlike AgentService, Sand sends the full Anthropic history on every turn;
/// its next request carries the `tool_result` directly and never enters the
/// Cursor live-run continuation path.  Reusing [`bridge_cursor_events_to_sse`]
/// would therefore leave a pending entry in the process-wide registry after
/// every XML tool call.  This variant keeps the same output but removes that
/// transient state before returning.
pub fn bridge_cursor_events_to_sse_stateless(
    message_id: &str,
    model: &str,
    session_id: &str,
    events: &[CursorStreamEvent],
    allowed_tool_names: Option<BTreeSet<String>>,
) -> (Vec<u8>, bool) {
    let result =
        bridge_cursor_events_to_sse(message_id, model, session_id, events, allowed_tool_names);
    BridgeRegistry::remove(session_id);
    result
}

/// Resume a paused tool bridge session.
///
/// Finds the stored state by live-run key, resolves the pending tool with
/// Claude's `tool_result`, and continues producing SSE from remaining events.
pub fn resume_cursor_tool_bridge(
    session_id: &str,
    new_message_id: &str,
    new_model: &str,
    result: &serde_json::Value,
    pending_tool: &PendingCursorTool,
) -> (Vec<serde_json::Value>, Vec<u8>) {
    let native_result = CursorNativeToolResult {
        content: render_tool_result_content(result),
        is_error: tool_result_is_error(result),
    };

    // Build Cursor protocol messages for the resolved tool
    let exec = CursorExec {
        id: None,
        exec_id: None,
        args: pending_tool.input_json(),
    };
    let result_messages = match pending_tool {
        PendingCursorTool::Read { .. } => {
            let msg = build_read_result_from_native(&exec, &native_result);
            vec![msg]
        }
        PendingCursorTool::Write { .. } => {
            let msg = build_write_result_from_native(&exec, &native_result);
            vec![msg]
        }
        PendingCursorTool::Bash {
            working_directory, ..
        } => build_shell_stream_result(
            &exec,
            &native_result,
            std::time::Duration::from_millis(0),
            working_directory,
        ),
        PendingCursorTool::Delete { .. } => {
            vec![build_delete_result_from_native(&exec, &native_result)]
        }
        PendingCursorTool::Grep { .. } => {
            vec![build_grep_result_from_native(&exec, &native_result)]
        }
        PendingCursorTool::Ls { .. } => vec![build_ls_result_from_native(&exec, &native_result)],
        PendingCursorTool::Generic { .. } => {
            // Generic tools are fulfilled by Claude Code; no Cursor protocol result needed.
            vec![]
        }
    };

    // Generate SSE continuation from remaining events
    let mut sse = Vec::new();
    let mut framer = CursorSseFramer::new(&mut sse, new_message_id, new_model);

    // Retrieve the complete paused state. The old two-step pending_tool/take
    // lookup discarded the parser and allowed-tool set, which made a second
    // tool continuation diverge from the initial request.
    let (remaining, allowed_tool_names, mut xml_parser, input_tokens, output_tokens) =
        match BridgeRegistry::take(session_id) {
            Some(state) => (
                state.remaining_events,
                state.allowed_tool_names,
                state.xml_parser,
                state.input_tokens,
                state.output_tokens,
            ),
            None => (Vec::new(), None, CursorToolUseXmlParser::new(None), 0, 0),
        };

    if remaining.is_empty() {
        // No remaining events: just finalize
        framer.finalize();
    } else {
        let mut paused_again = false;
        let mut next_pending_tool: Option<PendingCursorTool> = None;
        let mut next_remaining_events: Vec<CursorStreamEvent> = Vec::new();

        for event in &remaining {
            if paused_again {
                next_remaining_events.push(event.clone());
                continue;
            }

            match event {
                CursorStreamEvent::ThinkingDelta { text } => {
                    framer.emit_thinking_delta(text);
                }
                CursorStreamEvent::TextDelta { text } => {
                    let recovered = xml_parser.push(text);
                    for evt in recovered {
                        if paused_again {
                            match evt {
                                RecoveredCursorEvent::Text(text) => {
                                    next_remaining_events
                                        .push(CursorStreamEvent::TextDelta { text });
                                }
                                RecoveredCursorEvent::ToolUse(tool_use) => {
                                    next_remaining_events.push(CursorStreamEvent::NativeTool {
                                        tool_use_id: tool_use.id,
                                        name: tool_use.name,
                                        input: serde_json::Value::Object(tool_use.input),
                                    });
                                }
                            }
                            continue;
                        }
                        match evt {
                            RecoveredCursorEvent::Text(text) => {
                                framer.emit_text_delta(&text);
                            }
                            RecoveredCursorEvent::ToolUse(tool_use) => {
                                let input = serde_json::Value::Object(tool_use.input.clone());
                                let Some(emit_name) = resolve_advertised_name(
                                    &tool_use.name,
                                    allowed_tool_names.as_ref(),
                                ) else {
                                    continue;
                                };
                                let adapted = adapt_tool_input_for_client(&emit_name, input);
                                let input_json = serde_json::to_string(&adapted)
                                    .unwrap_or_else(|_| "{}".to_string());
                                framer.emit_tool_pause(&tool_use.id, &emit_name, &input_json);
                                next_pending_tool = Some(pending_from_native_tool(
                                    &tool_use.id,
                                    &emit_name,
                                    &adapted,
                                ));
                                paused_again = true;
                            }
                        }
                    }
                }
                CursorStreamEvent::Usage {
                    input_tokens,
                    output_tokens,
                    cache_read_tokens,
                    cache_write_tokens,
                } => {
                    framer.record_usage(
                        *input_tokens,
                        *output_tokens,
                        *cache_read_tokens,
                        *cache_write_tokens,
                    );
                }
                CursorStreamEvent::OutputTokenDelta { tokens } => {
                    framer.add_output_tokens(*tokens);
                }
                CursorStreamEvent::Session { .. } => {}
                CursorStreamEvent::NativeTool {
                    tool_use_id,
                    name,
                    input,
                } => {
                    let Some(emit_name) =
                        resolve_advertised_name(name, allowed_tool_names.as_ref())
                    else {
                        continue;
                    };
                    let adapted = adapt_tool_input_for_client(&emit_name, input.clone());
                    let input_json =
                        serde_json::to_string(&adapted).unwrap_or_else(|_| "{}".to_string());
                    framer.emit_tool_pause(tool_use_id, &emit_name, &input_json);
                    next_pending_tool =
                        Some(pending_from_native_tool(tool_use_id, &emit_name, &adapted));
                    paused_again = true;
                }
                CursorStreamEvent::End => {
                    // Flush before finalizing. `flush` can yield trailing text
                    // as well as a complete tool_use.
                    let flushed = xml_parser.flush();
                    for evt in flushed {
                        if paused_again {
                            match evt {
                                RecoveredCursorEvent::Text(text) => {
                                    next_remaining_events
                                        .push(CursorStreamEvent::TextDelta { text });
                                }
                                RecoveredCursorEvent::ToolUse(tool_use) => {
                                    next_remaining_events.push(CursorStreamEvent::NativeTool {
                                        tool_use_id: tool_use.id,
                                        name: tool_use.name,
                                        input: serde_json::Value::Object(tool_use.input),
                                    });
                                }
                            }
                            continue;
                        }
                        match evt {
                            RecoveredCursorEvent::Text(text) => framer.emit_text_delta(&text),
                            RecoveredCursorEvent::ToolUse(tool_use) => {
                                let input = serde_json::Value::Object(tool_use.input.clone());
                                let Some(emit_name) = resolve_advertised_name(
                                    &tool_use.name,
                                    allowed_tool_names.as_ref(),
                                ) else {
                                    continue;
                                };
                                let adapted = adapt_tool_input_for_client(&emit_name, input);
                                let input_json = serde_json::to_string(&adapted)
                                    .unwrap_or_else(|_| "{}".to_string());
                                framer.emit_tool_pause(&tool_use.id, &emit_name, &input_json);
                                next_pending_tool = Some(pending_from_native_tool(
                                    &tool_use.id,
                                    &emit_name,
                                    &adapted,
                                ));
                                paused_again = true;
                            }
                        }
                    }
                    if !paused_again {
                        framer.finalize();
                    }
                }
            }
        }

        if !paused_again {
            let flushed = xml_parser.flush();
            for evt in flushed {
                match evt {
                    RecoveredCursorEvent::Text(text) => framer.emit_text_delta(&text),
                    RecoveredCursorEvent::ToolUse(tool_use) => {
                        let input = serde_json::Value::Object(tool_use.input.clone());
                        let Some(emit_name) =
                            resolve_advertised_name(&tool_use.name, allowed_tool_names.as_ref())
                        else {
                            continue;
                        };
                        let adapted = adapt_tool_input_for_client(&emit_name, input);
                        let input_json =
                            serde_json::to_string(&adapted).unwrap_or_else(|_| "{}".to_string());
                        framer.emit_tool_pause(&tool_use.id, &emit_name, &input_json);
                        next_pending_tool =
                            Some(pending_from_native_tool(&tool_use.id, &emit_name, &adapted));
                        paused_again = true;
                    }
                }
            }
            if !paused_again {
                framer.finalize();
            }
        }

        if paused_again {
            let state = CursorBridgeState::new(
                session_id.to_string(),
                new_message_id.to_string(),
                new_model.to_string(),
                allowed_tool_names.clone(),
                Box::new(|| {
                    format!(
                        "call_cursor_{}",
                        uuid::Uuid::new_v4().to_string().replace('-', "")
                    )
                }),
            );
            let mut state = state;
            state.pending_tool = next_pending_tool;
            state.remaining_events = next_remaining_events;
            state.input_tokens = input_tokens;
            state.output_tokens = output_tokens;
            state.xml_parser = xml_parser;
            BridgeRegistry::insert(state);
        }
    }

    (result_messages, sse)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Keep native tool events on their Cursor result path. The legacy buffered
/// bridge can receive `NativeTool` events when the live BiDi path is skipped;
/// storing a native Write as Generic would make the Claude result disappear
/// instead of sending Cursor's `writeResult`, so the upstream repeats it.
fn pending_from_native_tool(
    tool_use_id: &str,
    name: &str,
    input: &serde_json::Value,
) -> PendingCursorTool {
    let object = input.as_object();
    match name {
        "Read" | "read" | "read_file" | "ReadFile" => PendingCursorTool::Read {
            tool_use_id: tool_use_id.to_string(),
            path: object.map(claude_file_path).unwrap_or_default(),
        },
        "Write" | "write" | "write_file" | "WriteFile" => PendingCursorTool::Write {
            tool_use_id: tool_use_id.to_string(),
            path: object.map(claude_file_path).unwrap_or_default(),
            content: object.map(claude_write_content).unwrap_or_default(),
        },
        "Bash" | "PowerShell" | "Shell" | "bash" | "run_terminal_command" | "run_terminal_cmd" => {
            let command = object
                .and_then(|obj| obj.get("command"))
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string();
            let working_directory = object
                .and_then(|obj| obj.get("working_directory").or_else(|| obj.get("cwd")))
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string();
            let timeout_ms = object
                .and_then(|obj| obj.get("timeout").or_else(|| obj.get("timeout_ms")))
                .and_then(json_u64)
                .unwrap_or(30_000);
            PendingCursorTool::Bash {
                tool_use_id: tool_use_id.to_string(),
                command,
                working_directory,
                timeout_ms,
            }
        }
        "Delete" | "delete" | "DeleteFile" => PendingCursorTool::Delete {
            tool_use_id: tool_use_id.to_string(),
            path: object.map(claude_file_path).unwrap_or_default(),
        },
        "Grep" | "grep" | "Search" => PendingCursorTool::Grep {
            tool_use_id: tool_use_id.to_string(),
            pattern: object
                .and_then(|obj| obj.get("pattern"))
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string(),
            path: object
                .and_then(|obj| obj.get("path"))
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string(),
        },
        "LS" | "Ls" | "ls" | "list_dir" => PendingCursorTool::Ls {
            tool_use_id: tool_use_id.to_string(),
            path: object
                .and_then(|obj| obj.get("path").or_else(|| obj.get("target_directory")))
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string(),
        },
        _ => PendingCursorTool::Generic {
            tool_use_id: tool_use_id.to_string(),
            name: name.to_string(),
            input: input.clone(),
        },
    }
}

pub(crate) fn resolve_advertised_name(
    mapped_name: &str,
    allowed: Option<&BTreeSet<String>>,
) -> Option<String> {
    let Some(allowed) = allowed else {
        return Some(mapped_name.to_string());
    };
    // A Cursor PiEdit event is mapped to the legacy `Edit` label for
    // compatibility, but Claude Code 2.1.193's advertised handler is the
    // schema-less `str_replace_based_edit_tool`.  Choose the canonical modern
    // spelling before the exact legacy hit whenever both are advertised.
    // Cursor's full-file overwrite path maps to `Write`, so it is unaffected.
    if (mapped_name.eq_ignore_ascii_case("Edit") || is_text_editor_tool_name(mapped_name))
        && let Some(name) = preferred_text_editor_name(allowed)
    {
        return Some(name);
    }
    if allowed.contains(mapped_name) {
        return Some(mapped_name.to_string());
    }
    // Never fall back to Edit: Claude Edit requires old_string/new_string,
    // while Cursor Write/Edit overwrite maps to {file_path, content}.
    let fallbacks = advertised_name_fallbacks(mapped_name);
    for cand in fallbacks {
        if allowed.contains(*cand) {
            return Some((*cand).to_string());
        }
    }
    // Claude Code's bundled runtime exposes a small, explicit alias table
    // (for example `Brief`/`SendUserMessage`, `Workflow`/`RunWorkflow`).
    // Resolve only those names against the client's allow-list and preserve
    // the exact spelling the client advertised. Do not case-fold arbitrary
    // MCP names: two unrelated providers may legitimately differ only by
    // case.
    if let Some(hit) = allowed.iter().find(|candidate| {
        candidate.eq_ignore_ascii_case(mapped_name)
            && claude_tool_names_equivalent(mapped_name, candidate)
    }) {
        return Some(hit.clone());
    }
    if let Some(hit) = allowed
        .iter()
        .find(|candidate| claude_tool_names_equivalent(mapped_name, candidate))
    {
        return Some(hit.clone());
    }
    // Older Claude clients can retain a `claude-local` provider prefix in
    // tool history. Compare aliases using the bare leaf, but only for the
    // explicitly recognized provider-qualified forms.
    if is_claude_local_mcp_spelling(mapped_name)
        || allowed
            .iter()
            .any(|candidate| is_claude_local_mcp_spelling(candidate))
    {
        let leaf = strip_mcp_provider_prefix(mapped_name);
        if let Some(hit) = allowed.iter().find(|candidate| {
            is_claude_local_mcp_spelling(candidate)
                && strip_mcp_provider_prefix(candidate).eq_ignore_ascii_case(leaf)
                && claude_tool_names_equivalent(leaf, strip_mcp_provider_prefix(candidate))
        }) {
            return Some(hit.clone());
        }
        if let Some(hit) = allowed.iter().find(|candidate| {
            is_claude_local_mcp_spelling(candidate)
                && claude_tool_names_equivalent(leaf, strip_mcp_provider_prefix(candidate))
        }) {
            return Some(hit.clone());
        }
        // The mapped event may be qualified while the client advertised the
        // bare name (or vice versa). Compare both leaves in that case.
        if is_claude_local_mcp_spelling(mapped_name)
            && let Some(hit) = allowed.iter().find(|candidate| {
                candidate.eq_ignore_ascii_case(leaf)
                    && claude_tool_names_equivalent(leaf, candidate)
            })
        {
            return Some(hit.clone());
        }
        if is_claude_local_mcp_spelling(mapped_name)
            && let Some(hit) = allowed
                .iter()
                .find(|candidate| claude_tool_names_equivalent(leaf, candidate))
        {
            return Some(hit.clone());
        }
    }
    if mapped_name.contains("__") {
        if let Some(hit) = allowed
            .iter()
            .find(|candidate| cursor_mcp_wire_name(candidate) == mapped_name)
        {
            return Some(hit.clone());
        }
        if let Some(hit) = allowed.iter().find(|n| n.as_str() == mapped_name) {
            return Some(hit.clone());
        }
    }
    // Last resort: if Bash is allowed, shell-ify unknown tools were already Bash.
    if mapped_name == "Bash" && allowed.iter().any(|n| n.eq_ignore_ascii_case("bash")) {
        return allowed
            .iter()
            .find(|n| n.eq_ignore_ascii_case("bash"))
            .cloned();
    }
    None
}

/// Claude Code Read/Write use `file_path` / `content`; tolerate Cursor-ish aliases
/// that sometimes appear in XML tool_use JSON.
fn claude_file_path(input: &serde_json::Map<String, serde_json::Value>) -> String {
    input
        .get("file_path")
        .or_else(|| input.get("target_file"))
        .or_else(|| input.get("path"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn claude_write_content(input: &serde_json::Map<String, serde_json::Value>) -> String {
    input
        .get("content")
        .or_else(|| input.get("contents"))
        .or_else(|| input.get("file_text"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

/// Create a `PendingCursorTool` from a recovered XML tool_use event.
///
/// Claude-local tools (`Workflow`, `Skill`, `mcp__*`, …) map to
/// [`PendingCursorTool::Generic`] so Claude Code can fulfill them and the
/// bridge can resume without inventing a Cursor exec protocol result.
#[allow(dead_code)]
fn pending_from_recovered_tool(
    tool_use: &crate::providers::cursor::tool_use_xml::RecoveredCursorToolUse,
) -> Option<PendingCursorTool> {
    match tool_use.name.as_str() {
        "Read" | "read" | "read_file" | "ReadFile" => {
            let file_path = claude_file_path(&tool_use.input);
            Some(PendingCursorTool::Read {
                tool_use_id: tool_use.id.clone(),
                path: file_path,
            })
        }
        // Cursor/grok clients have shipped all of these spellings. Keep them
        // on the native Write result path instead of treating an alias as a
        // generic client-only tool (which leaves Cursor waiting forever for
        // writeResult and makes Claude retry the same Write repeatedly).
        "Write" | "write" | "write_file" | "WriteFile" => {
            let file_path = claude_file_path(&tool_use.input);
            let content = claude_write_content(&tool_use.input);
            Some(PendingCursorTool::Write {
                tool_use_id: tool_use.id.clone(),
                path: file_path,
                content,
            })
        }
        "Bash" | "PowerShell" | "Shell" | "bash" | "run_terminal_command" | "run_terminal_cmd" => {
            let command = tool_use
                .input
                .get("command")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let working_directory = tool_use
                .input
                .get("working_directory")
                .or_else(|| tool_use.input.get("cwd"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let timeout_ms = tool_use
                .input
                .get("timeout")
                .or_else(|| tool_use.input.get("timeout_ms"))
                .and_then(json_u64)
                .unwrap_or(30_000);
            Some(PendingCursorTool::Bash {
                tool_use_id: tool_use.id.clone(),
                command,
                working_directory,
                timeout_ms,
            })
        }
        "Delete" | "delete" | "DeleteFile" => Some(PendingCursorTool::Delete {
            tool_use_id: tool_use.id.clone(),
            path: claude_file_path(&tool_use.input),
        }),
        "Grep" | "grep" | "Search" => Some(PendingCursorTool::Grep {
            tool_use_id: tool_use.id.clone(),
            pattern: tool_use
                .input
                .get("pattern")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string(),
            path: tool_use
                .input
                .get("path")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string(),
        }),
        "LS" | "Ls" | "ls" | "list_dir" => Some(PendingCursorTool::Ls {
            tool_use_id: tool_use.id.clone(),
            path: tool_use
                .input
                .get("path")
                .or_else(|| tool_use.input.get("target_directory"))
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string(),
        }),
        name if !name.is_empty() => Some(PendingCursorTool::Generic {
            tool_use_id: tool_use.id.clone(),
            name: tool_use.name.clone(),
            input: serde_json::Value::Object(tool_use.input.clone()),
        }),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anthropic::schema::MessagesRequest;
    use crate::providers::cursor::tool_use_xml::RecoveredCursorToolUse;

    // -----------------------------------------------------------------------
    // PendingCursorTool tests
    // -----------------------------------------------------------------------

    #[test]
    fn xml_event_bridge_recovers_tool_use_without_upstream_catalog() {
        let mut bridge = CursorXmlEventBridge::new(Some(BTreeSet::from(["Read".to_string()])));

        // The fallback request carries no upstream `tools` field.  Claude
        // Code's original allow-list still gates the text protocol locally.
        let first = bridge.push("prefix <tool_");
        assert_eq!(first.len(), 1);
        assert!(matches!(
            &first[0],
            CursorStreamEvent::TextDelta { text } if text == "prefix "
        ));
        let second = bridge.push(r#"use name="Read">{"file_path":"/tmp/a"}</tool_use> tail"#);
        assert_eq!(second.len(), 2);
        assert!(matches!(
            &second[0],
            CursorStreamEvent::NativeTool { tool_use_id, name, input }
                if tool_use_id.starts_with("call_cursor_")
                    && name == "Read"
                    && input == &serde_json::json!({"file_path": "/tmp/a"})
        ));
        assert!(matches!(
            &second[1],
            CursorStreamEvent::TextDelta { text } if text == " tail"
        ));
        assert!(bridge.saw_tool_use());
    }

    #[test]
    fn xml_event_bridge_filters_unadvertised_tool_names() {
        let mut bridge = CursorXmlEventBridge::new(Some(BTreeSet::from(["Read".to_string()])));
        let events =
            bridge.push(r#"before <tool_use name="Bash">{"command":"pwd"}</tool_use> after"#);

        let visible = events
            .iter()
            .filter_map(|event| match event {
                CursorStreamEvent::TextDelta { text } => Some(text.as_str()),
                CursorStreamEvent::NativeTool { .. } => None,
                _ => None,
            })
            .collect::<String>();
        assert_eq!(visible, "before  after");
        assert!(
            events
                .iter()
                .all(|event| !matches!(event, CursorStreamEvent::NativeTool { .. }))
        );
        assert!(!bridge.saw_tool_use());
    }

    #[test]
    fn bridge_cursor_events_to_sse_recovers_xml_and_retains_pause_state() {
        let _lock = lock_bridge_registry_for_test();
        BridgeRegistry::clear();

        let events = vec![
            CursorStreamEvent::TextDelta {
                text: "before <tool_".into(),
            },
            CursorStreamEvent::TextDelta {
                text: r#"use name="Read">{"file_path":"/tmp/a"}</tool_use> after"#.into(),
            },
            CursorStreamEvent::End,
        ];
        let (sse, paused) = bridge_cursor_events_to_sse(
            "msg-sand-xml",
            "claude-fable-5",
            "session-sand-xml",
            &events,
            Some(BTreeSet::from(["Read".to_string()])),
        );

        assert!(paused);
        let body = String::from_utf8(sse).expect("Anthropic SSE must be UTF-8");
        assert!(body.contains("event: message_start"));
        assert!(body.contains("\"type\":\"tool_use\""));
        assert!(body.contains("\"name\":\"Read\""));
        assert!(body.contains("\"stop_reason\":\"tool_use\""));
        // Text after a recovered tool call belongs to the continuation and
        // must remain in the registry rather than being emitted twice.
        assert!(!body.contains(" after"));
        let pending = BridgeRegistry::pending_tool("session-sand-xml")
            .expect("XML tool pause should be retained for Sand resume");
        assert_eq!(pending.name(), "Read");
        assert_eq!(pending.input_json()["file_path"], "/tmp/a");
        BridgeRegistry::remove("session-sand-xml");
    }

    #[test]
    fn bridge_cursor_events_to_sse_finalizes_plain_text_without_registry_entry() {
        let _lock = lock_bridge_registry_for_test();
        BridgeRegistry::clear();

        let events = vec![
            CursorStreamEvent::TextDelta {
                text: "plain Sand answer".into(),
            },
            CursorStreamEvent::Usage {
                input_tokens: 11,
                output_tokens: 3,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
            },
            CursorStreamEvent::End,
        ];
        let (sse, paused) = bridge_cursor_events_to_sse(
            "msg-sand-text",
            "claude-fable-5",
            "session-sand-text",
            &events,
            Some(BTreeSet::new()),
        );

        assert!(!paused);
        let body = String::from_utf8(sse).expect("Anthropic SSE must be UTF-8");
        assert!(body.contains("plain Sand answer"));
        assert!(body.contains("\"stop_reason\":\"end_turn\""));
        assert!(BridgeRegistry::get("session-sand-text").is_none());
    }

    #[test]
    fn stateless_bridge_does_not_retain_sand_tool_state() {
        let _lock = lock_bridge_registry_for_test();
        BridgeRegistry::clear();
        let events = vec![CursorStreamEvent::TextDelta {
            text: r#"<tool_use name="Read">{"file_path":"/tmp/a"}</tool_use>"#.into(),
        }];
        let (sse, paused) = bridge_cursor_events_to_sse_stateless(
            "msg-sand-stateless",
            "claude-fable-5",
            "session-sand-stateless",
            &events,
            Some(BTreeSet::from(["Read".to_string()])),
        );
        assert!(paused);
        assert!(
            sse.windows(b"tool_use".len())
                .any(|window| window == b"tool_use")
        );
        assert!(BridgeRegistry::get("session-sand-stateless").is_none());
    }

    #[test]
    fn pending_read_input_matches_claude_read_tool() {
        let tool = PendingCursorTool::Read {
            tool_use_id: "call_cursor_1".into(),
            path: "/tmp/a".into(),
        };
        let json = tool.input_json();
        assert_eq!(json["file_path"], "/tmp/a");
        assert_eq!(tool.name(), "Read");
        assert_eq!(tool.tool_use_id(), "call_cursor_1");
    }

    #[test]
    fn pending_write_input_matches_claude_write_tool() {
        let tool = PendingCursorTool::Write {
            tool_use_id: "call_cursor_2".into(),
            path: "/tmp/b".into(),
            content: "hello".into(),
        };
        let json = tool.input_json();
        assert_eq!(json["file_path"], "/tmp/b");
        assert_eq!(json["content"], "hello");
        assert_eq!(tool.name(), "Write");
    }

    #[test]
    fn write_aliases_use_native_write_result_path() {
        for name in ["write", "write_file", "WriteFile"] {
            let tool = RecoveredCursorToolUse {
                id: format!("call_{name}"),
                original_id: None,
                name: name.into(),
                input: serde_json::json!({
                    "file_path": "/tmp/alias.txt",
                    "content": "alias content"
                })
                .as_object()
                .cloned()
                .unwrap(),
            };
            let pending = pending_from_recovered_tool(&tool).expect("write alias pending");
            assert!(matches!(pending, PendingCursorTool::Write { .. }));
            assert_eq!(pending.name(), "Write");
            assert_eq!(pending.input_json()["file_path"], "/tmp/alias.txt");
        }
    }

    #[test]
    fn native_write_event_is_not_downgraded_to_generic() {
        let pending = pending_from_native_tool(
            "native-write-1",
            "Write",
            &serde_json::json!({
                "file_path": "/tmp/native.txt",
                "content": "native content"
            }),
        );
        assert!(matches!(pending, PendingCursorTool::Write { .. }));
        assert_eq!(pending.tool_use_id(), "native-write-1");
        assert_eq!(pending.input_json()["content"], "native content");
    }

    #[test]
    fn native_write_bridge_resume_builds_write_result() {
        let _lock = lock_bridge_registry_for_test();
        BridgeRegistry::clear();
        let events = vec![
            CursorStreamEvent::NativeTool {
                tool_use_id: "native-write-bridge".into(),
                name: "Write".into(),
                input: serde_json::json!({
                    "file_path": "/tmp/native-bridge.txt",
                    "content": "hello"
                }),
            },
            CursorStreamEvent::End,
        ];
        let allowed = BTreeSet::from(["Write".to_string()]);
        let (_, paused) = start_cursor_tool_bridge(
            "msg-native-write",
            "cursor-test",
            "session-native-write",
            &events,
            Some(allowed),
            Box::new(|| "unused".into()),
        );
        assert!(paused);
        let pending = BridgeRegistry::pending_tool("session-native-write")
            .expect("native write should be pending");
        assert!(matches!(pending, PendingCursorTool::Write { .. }));
        let (result_messages, _) = resume_cursor_tool_bridge(
            "session-native-write",
            "msg-native-write-resume",
            "cursor-test",
            &serde_json::json!({
                "type": "tool_result",
                "tool_use_id": "native-write-bridge",
                "content": "ok"
            }),
            &pending,
        );
        assert_eq!(result_messages.len(), 1);
        assert_eq!(
            result_messages[0]["writeResult"]["success"]["path"],
            "/tmp/native-bridge.txt"
        );
        BridgeRegistry::remove("session-native-write");
    }

    #[test]
    fn native_delete_grep_ls_events_use_matching_result_envelopes() {
        let _lock = lock_bridge_registry_for_test();
        for (index, (name, input, expected_result)) in [
            (
                "Delete",
                serde_json::json!({"path": "/tmp/delete-me"}),
                "deleteResult",
            ),
            (
                "Grep",
                serde_json::json!({"pattern": "needle", "path": "/tmp/file"}),
                "grepResult",
            ),
            ("LS", serde_json::json!({"path": "/tmp"}), "lsResult"),
        ]
        .into_iter()
        .enumerate()
        {
            let session = format!("session-native-{name}");
            let tool_id = format!("native-{name}");
            BridgeRegistry::clear();
            let events = vec![CursorStreamEvent::NativeTool {
                tool_use_id: tool_id.clone(),
                name: name.into(),
                input,
            }];
            let allowed = BTreeSet::from([name.to_string()]);
            let (_, paused) = start_cursor_tool_bridge(
                &format!("msg-native-{index}"),
                "cursor-test",
                &session,
                &events,
                Some(allowed),
                Box::new(|| "unused".into()),
            );
            assert!(paused);
            let pending = BridgeRegistry::pending_tool(&session).expect("native tool pending");
            let (messages, _) = resume_cursor_tool_bridge(
                &session,
                &format!("msg-native-resume-{index}"),
                "cursor-test",
                &serde_json::json!({
                    "type": "tool_result",
                    "tool_use_id": tool_id,
                    "content": "line one\nline two"
                }),
                &pending,
            );
            assert_eq!(messages.len(), 1);
            assert!(messages[0].get(expected_result).is_some(), "{messages:?}");
        }
        BridgeRegistry::clear();
    }

    #[test]
    fn continuation_preserves_allow_list_and_pending_tool_for_second_tool() {
        let _lock = lock_bridge_registry_for_test();
        BridgeRegistry::clear();

        let events = vec![
            CursorStreamEvent::NativeTool {
                tool_use_id: "first-read".into(),
                name: "Read".into(),
                input: serde_json::json!({"file_path": "/tmp/first"}),
            },
            CursorStreamEvent::TextDelta {
                text: concat!(
                    "before-second ",
                    r#"<tool_use name="Write">{"file_path":"/tmp/second","content":"body"}</tool_use>"#,
                    " after-second"
                )
                .into(),
            },
            CursorStreamEvent::End,
        ];
        let allowed = BTreeSet::from(["Read".to_string(), "Write".to_string()]);
        let mut next_id = 0u8;
        let (_, paused) = start_cursor_tool_bridge(
            "msg-first-tool",
            "cursor-test",
            "session-second-tool",
            &events,
            Some(allowed),
            Box::new(move || {
                next_id += 1;
                format!("xml-tool-{next_id}")
            }),
        );
        assert!(paused);

        let first_pending =
            BridgeRegistry::pending_tool("session-second-tool").expect("first pending tool");
        assert_eq!(first_pending.name(), "Read");
        let (_, second_sse) = resume_cursor_tool_bridge(
            "session-second-tool",
            "msg-second-tool",
            "cursor-test",
            &serde_json::json!({"type": "tool_result", "content": "read ok"}),
            &first_pending,
        );

        let second_pending = BridgeRegistry::pending_tool("session-second-tool")
            .expect("second pending tool must survive continuation");
        assert_eq!(second_pending.name(), "Write");
        assert_eq!(second_pending.input_json()["file_path"], "/tmp/second");
        let second_sse_text = String::from_utf8_lossy(&second_sse);
        assert!(
            second_sse_text.contains("tool_use"),
            "continuation should pause on the second tool"
        );
        assert!(
            !second_sse_text.contains("after-second"),
            "text after the second pause must wait for its result"
        );

        let (_, final_sse) = resume_cursor_tool_bridge(
            "session-second-tool",
            "msg-final-tool",
            "cursor-test",
            &serde_json::json!({"type": "tool_result", "content": "write ok"}),
            &second_pending,
        );
        assert!(
            String::from_utf8_lossy(&final_sse).contains("after-second"),
            "text after the second tool should be emitted after its result"
        );
        BridgeRegistry::remove("session-second-tool");
    }

    #[test]
    fn same_delta_second_xml_tool_is_queued_for_next_resume() {
        let _lock = lock_bridge_registry_for_test();
        BridgeRegistry::clear();
        let events = vec![CursorStreamEvent::TextDelta {
            text: concat!(
                r#"<tool_use name="Read">{"file_path":"/tmp/first"}</tool_use>"#,
                r#"<tool_use name="Write">{"file_path":"/tmp/second","content":"body"}</tool_use>"#,
                " tail"
            )
            .into(),
        }];
        let allowed = BTreeSet::from(["Read".to_string(), "Write".to_string()]);
        let (_, paused) = start_cursor_tool_bridge(
            "msg-same-delta",
            "cursor-test",
            "session-same-delta",
            &events,
            Some(allowed),
            Box::new(|| "xml-first".into()),
        );
        assert!(paused);
        let first_pending =
            BridgeRegistry::pending_tool("session-same-delta").expect("first XML tool");
        assert_eq!(first_pending.name(), "Read");

        let (_, second_sse) = resume_cursor_tool_bridge(
            "session-same-delta",
            "msg-same-delta-resume",
            "cursor-test",
            &serde_json::json!({"type": "tool_result", "content": "read ok"}),
            &first_pending,
        );
        let second_pending = BridgeRegistry::pending_tool("session-same-delta")
            .expect("second XML tool from same delta");
        assert_eq!(second_pending.name(), "Write");
        assert!(!String::from_utf8_lossy(&second_sse).contains(" tail"));
        BridgeRegistry::remove("session-same-delta");
    }

    #[test]
    fn partial_second_xml_survives_pause_and_resume() {
        let _lock = lock_bridge_registry_for_test();
        BridgeRegistry::clear();
        let events = vec![
            CursorStreamEvent::TextDelta {
                text: concat!(
                    r#"<tool_use name="Read">{"file_path":"/tmp/first"}</tool_use>"#,
                    "<tool_use name=\"Write\">{\"file_path\":\"/tmp/second\",\"content\":\""
                )
                .into(),
            },
            CursorStreamEvent::TextDelta {
                text: " body\"}</tool_use> tail".into(),
            },
        ];
        let allowed = BTreeSet::from(["Read".to_string(), "Write".to_string()]);
        let (_, paused) = start_cursor_tool_bridge(
            "msg-partial-second",
            "cursor-test",
            "session-partial-second",
            &events,
            Some(allowed),
            Box::new(|| "xml-partial-first".into()),
        );
        assert!(paused);
        let first_pending =
            BridgeRegistry::pending_tool("session-partial-second").expect("first XML tool");
        assert_eq!(first_pending.name(), "Read");

        let (_, second_sse) = resume_cursor_tool_bridge(
            "session-partial-second",
            "msg-partial-second-resume",
            "cursor-test",
            &serde_json::json!({"type": "tool_result", "content": "read ok"}),
            &first_pending,
        );
        let second_pending = BridgeRegistry::pending_tool("session-partial-second")
            .expect("partial second XML tool");
        assert_eq!(second_pending.name(), "Write");
        assert_eq!(second_pending.input_json()["content"], " body");
        assert!(!String::from_utf8_lossy(&second_sse).contains(" tail"));
        BridgeRegistry::remove("session-partial-second");
    }

    #[test]
    fn pending_bash_input_matches_claude_bash_tool() {
        let tool = PendingCursorTool::Bash {
            tool_use_id: "call_cursor_3".into(),
            command: "pwd".into(),
            working_directory: "/tmp".into(),
            timeout_ms: 30_000,
        };
        let json = tool.input_json();
        assert_eq!(json["command"], "cd '/tmp' && pwd");
        assert_eq!(json["timeout"], 30_000);
        assert_eq!(json["description"], "Run Cursor-requested shell command");
        assert_eq!(tool.name(), "Bash");
    }

    #[test]
    fn pending_bash_no_working_directory() {
        let tool = PendingCursorTool::Bash {
            tool_use_id: "call_cursor_4".into(),
            command: "ls".into(),
            working_directory: "".into(),
            timeout_ms: 10_000,
        };
        let json = tool.input_json();
        // Without a working directory, command is passed as-is
        assert_eq!(json["command"], "ls");
    }

    #[test]
    fn pending_bash_escapes_apostrophe_in_working_directory() {
        let tool = PendingCursorTool::Bash {
            tool_use_id: "call_cursor_quote".into(),
            command: "pwd".into(),
            working_directory: "/tmp/it's-here".into(),
            timeout_ms: 5_000,
        };
        assert_eq!(
            tool.input_json()["command"],
            "cd '/tmp/it'\\''s-here' && pwd"
        );
    }

    #[test]
    fn recovered_bash_accepts_string_and_float_timeout() {
        for timeout in [serde_json::json!("5000"), serde_json::json!(5000.0)] {
            let tool = RecoveredCursorToolUse {
                id: "call_bash_timeout".into(),
                original_id: None,
                name: "Bash".into(),
                input: serde_json::json!({
                    "command": "pwd",
                    "timeout": timeout,
                    "cwd": "/tmp/it's-here"
                })
                .as_object()
                .cloned()
                .unwrap(),
            };
            let pending = pending_from_recovered_tool(&tool).expect("Bash pending");
            assert_eq!(pending.input_json()["timeout"], 5_000);
            assert_eq!(
                pending.input_json()["command"],
                "cd '/tmp/it'\\''s-here' && pwd"
            );
        }
    }

    #[test]
    fn powershell_alias_uses_native_shell_result_path() {
        let tool = RecoveredCursorToolUse {
            id: "call_powershell".into(),
            original_id: None,
            name: "PowerShell".into(),
            input: serde_json::json!({
                "command": "Get-ChildItem",
                "timeout": 5000,
                "cwd": "C:\\work"
            })
            .as_object()
            .cloned()
            .unwrap(),
        };
        let pending = pending_from_recovered_tool(&tool).expect("PowerShell pending");
        assert!(matches!(pending, PendingCursorTool::Bash { .. }));
        assert_eq!(pending.name(), "Bash");
        assert_eq!(pending.input_json()["timeout"], 5000);
    }

    #[test]
    fn native_powershell_alias_uses_native_shell_result_path() {
        let pending = pending_from_native_tool(
            "native-powershell",
            "PowerShell",
            &serde_json::json!({
                "command": "Get-ChildItem",
                "timeout": 5000,
                "cwd": "C:\\work"
            }),
        );
        assert!(matches!(pending, PendingCursorTool::Bash { .. }));
        assert_eq!(pending.tool_use_id(), "native-powershell");
    }

    // -----------------------------------------------------------------------
    // Result builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn with_exec_ids_adds_id_and_exec_id() {
        let exec = CursorExec {
            id: Some(7),
            exec_id: Some("exec-1".into()),
            args: serde_json::json!({}),
        };
        let mut payload = serde_json::Map::new();
        payload.insert("test".into(), serde_json::json!("value"));
        let result = with_exec_ids(&exec, payload);
        assert_eq!(result["id"], 7);
        assert_eq!(result["execId"], "exec-1");
        assert_eq!(result["test"], "value");
    }

    #[test]
    fn with_exec_ids_omits_missing_fields() {
        let exec = CursorExec {
            id: None,
            exec_id: None,
            args: serde_json::json!({}),
        };
        let mut payload = serde_json::Map::new();
        payload.insert("test".into(), serde_json::json!("v"));
        let result = with_exec_ids(&exec, payload);
        assert!(result.get("id").is_none());
        assert!(result.get("execId").is_none());
        assert_eq!(result["test"], "v");
    }

    #[test]
    fn read_result_from_successful_result() {
        let exec = CursorExec {
            id: Some(1),
            exec_id: None,
            args: serde_json::json!({"file_path": "/tmp/a"}),
        };
        let result = CursorNativeToolResult {
            content: "file content".into(),
            is_error: false,
        };
        let msg = build_read_result_from_native(&exec, &result);
        assert_eq!(msg["id"], 1);
        assert_eq!(msg["readResult"]["success"]["path"], "/tmp/a");
        assert_eq!(msg["readResult"]["success"]["content"], "file content");
        assert_eq!(msg["readResult"]["success"]["totalLines"], 1);
    }

    #[test]
    fn read_result_from_error_uses_the_error_variant() {
        let exec = CursorExec {
            id: Some(1),
            exec_id: None,
            args: serde_json::json!({"file_path": "/tmp/missing"}),
        };
        let result = CursorNativeToolResult {
            content: "file not found".into(),
            is_error: true,
        };
        let msg = build_read_result_from_native(&exec, &result);
        assert!(msg["readResult"].get("success").is_none());
        assert_eq!(msg["readResult"]["error"]["path"], "/tmp/missing");
        assert_eq!(msg["readResult"]["error"]["error"], "file not found");
    }

    #[test]
    fn write_result_from_successful_result() {
        let exec = CursorExec {
            id: Some(2),
            exec_id: None,
            args: serde_json::json!({"file_path": "/tmp/b", "content": "hi\nthere\n"}),
        };
        let result = CursorNativeToolResult {
            // Status text must not drive linesCreated/fileSize.
            content: "Wrote contents to /tmp/b".into(),
            is_error: false,
        };
        let msg = build_write_result_from_native(&exec, &result);
        assert_eq!(msg["id"], 2);
        assert_eq!(msg["writeResult"]["success"]["path"], "/tmp/b");
        assert_eq!(msg["writeResult"]["success"]["linesCreated"], 2);
        assert_eq!(msg["writeResult"]["success"]["fileSize"], 9);
    }

    #[test]
    fn write_result_from_error_result() {
        let exec = CursorExec {
            id: Some(3),
            exec_id: None,
            args: serde_json::json!({"file_path": "/tmp/c"}),
        };
        let result = CursorNativeToolResult {
            content: "permission denied".into(),
            is_error: true,
        };
        let msg = build_write_result_from_native(&exec, &result);
        assert_eq!(msg["writeResult"]["error"]["path"], "/tmp/c");
        assert_eq!(msg["writeResult"]["error"]["error"], "permission denied");
    }

    #[test]
    fn shell_stream_result_emits_start_output_exit_and_close() {
        let exec = CursorExec {
            id: Some(7),
            exec_id: Some("e".into()),
            args: serde_json::json!({}),
        };
        let messages = build_shell_stream_result(
            &exec,
            &CursorNativeToolResult {
                content: "hi".into(),
                is_error: false,
            },
            std::time::Duration::from_millis(3),
            "/tmp",
        );
        assert_eq!(messages.len(), 4);
        // Start
        assert!(
            messages[0]
                .get("shellStream")
                .and_then(|s| s.get("start"))
                .is_some()
        );
        // Stdout content
        assert_eq!(messages[1]["shellStream"]["stdout"]["data"], "hi");
        // Exit
        assert_eq!(messages[2]["shellStream"]["exit"]["code"], 0);
        assert_eq!(messages[2]["shellStream"]["exit"]["cwd"], "/tmp");
        // Stream close
        assert_eq!(
            messages[3]["execClientControlMessage"]["streamClose"]["id"],
            7
        );
    }

    #[test]
    fn shell_stream_handles_error_result() {
        let exec = CursorExec {
            id: Some(8),
            exec_id: None,
            args: serde_json::json!({}),
        };
        let messages = build_shell_stream_result(
            &exec,
            &CursorNativeToolResult {
                content: "error msg".into(),
                is_error: true,
            },
            std::time::Duration::from_millis(5),
            "/tmp",
        );
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[1]["shellStream"]["stderr"]["data"], "error msg");
        assert_eq!(messages[2]["shellStream"]["exit"]["code"], 1);
    }

    // -----------------------------------------------------------------------
    // find_tool_result tests
    // -----------------------------------------------------------------------

    #[test]
    fn finds_tool_result_in_request() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [
                {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "result text"}]}
            ]
        }))
        .unwrap();
        let result = find_tool_result(&body, "call_1");
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().get("content").and_then(|c| c.as_str()),
            Some("result text")
        );
    }

    #[test]
    fn find_tool_result_returns_none_when_not_found() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": "hello"}]}
            ]
        }))
        .unwrap();
        assert!(find_tool_result(&body, "call_1").is_none());
    }

    #[test]
    fn find_tool_result_scans_newest_first() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [
                {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "old"}]},
                {"role": "assistant", "content": "ok"},
                {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "new"}]}
            ]
        }))
        .unwrap();
        let result = find_tool_result(&body, "call_1");
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().get("content").and_then(|c| c.as_str()),
            Some("new")
        );
    }

    // -----------------------------------------------------------------------
    // advertised_tool_names tests
    // -----------------------------------------------------------------------

    #[test]
    fn advertised_tool_names_extracts_read_write_bash() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [
                {"name": "Read", "description": "read", "input_schema": {}},
                {"name": "Write", "description": "write", "input_schema": {}},
                {"name": "Bash", "description": "bash", "input_schema": {}}
            ]
        }))
        .unwrap();
        let names = advertised_tool_names(&body).unwrap();
        assert!(names.contains("Read"));
        assert!(names.contains("Write"));
        assert!(names.contains("Bash"));
    }

    #[test]
    fn advertised_tool_names_no_tools_returns_none() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .unwrap();
        assert!(advertised_tool_names(&body).is_none());
    }

    #[test]
    fn advertised_tool_names_preserves_explicit_empty_catalog() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{"role": "user", "content": "hi"}],
            "tools": []
        }))
        .unwrap();
        let names = advertised_tool_names(&body).expect("explicit empty catalog");
        assert!(names.is_empty());
        assert!(!can_bridge_cursor_native_tools(&body, Some("session-1")));
    }

    #[test]
    fn advertised_tool_names_filters_internal_and_deprecated_tools() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [
                {"name": "Read", "description": "read", "input_schema": {}},
                {"name": "TaskOutput", "description": "deprecated", "input_schema": {}},
                {"name": "mcp__plugin_lobster-channel_lobster-channel__notify_messa00a7caa", "description": "internal", "input_schema": {}},
                {"name": "mcp__plugin_lobster-channel_lobster-channel__lobster_reply", "description": "public", "input_schema": {}}
            ]
        }))
        .unwrap();
        let names = advertised_tool_names(&body).expect("Read and public tool remain");
        assert!(names.contains("Read"));
        assert!(names.contains("mcp__plugin_lobster-channel_lobster-channel__lobster_reply"));
        assert!(
            !names.contains("TaskOutput"),
            "deprecated TaskOutput must not be exposed to the model"
        );
        assert!(
            !names.contains("mcp__plugin_lobster-channel_lobster-channel__notify_messa00a7caa")
        );
    }

    #[test]
    fn can_bridge_returns_true_for_stream_with_read_tool() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "stream": true,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [{"name": "Read", "description": "read", "input_schema": {}}]
        }))
        .unwrap();
        assert!(can_bridge_cursor_native_tools(&body, Some("session-1")));
    }

    #[test]
    fn can_bridge_returns_false_for_non_streaming() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "stream": false,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [{"name": "Read", "description": "read", "input_schema": {}}]
        }))
        .unwrap();
        assert!(!can_bridge_cursor_native_tools(&body, Some("session-1")));
    }

    #[test]
    fn can_bridge_returns_false_without_session_id() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "stream": true,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [{"name": "Read", "description": "read", "input_schema": {}}]
        }))
        .unwrap();
        assert!(!can_bridge_cursor_native_tools(&body, None));
        assert!(!can_bridge_cursor_native_tools(&body, Some("")));
    }

    // -----------------------------------------------------------------------
    // BridgeRegistry tests
    // -----------------------------------------------------------------------

    #[test]
    fn bridge_registry_manages_sessions() {
        let _lock = lock_bridge_registry_for_test();
        BridgeRegistry::clear();
        assert_eq!(BridgeRegistry::active_count(), 0);

        let state = CursorBridgeState::new(
            "session-test".into(),
            "msg-1".into(),
            "cursor-test".into(),
            None,
            Box::new(|| "id".into()),
        );
        BridgeRegistry::insert(state);
        assert_eq!(BridgeRegistry::active_count(), 1);
        assert!(BridgeRegistry::get("session-test").is_some());

        let state = BridgeRegistry::take("session-test");
        assert!(state.is_some());
        assert_eq!(BridgeRegistry::active_count(), 0);
    }

    #[test]
    fn bridge_registry_set_and_get_pending_tool() {
        let _lock = lock_bridge_registry_for_test();
        BridgeRegistry::clear();
        let state = CursorBridgeState::new(
            "session-pt".into(),
            "msg-1".into(),
            "cursor-test".into(),
            None,
            Box::new(|| "id".into()),
        );
        BridgeRegistry::insert(state);

        let tool = PendingCursorTool::Read {
            tool_use_id: "call_1".into(),
            path: "/tmp/a".into(),
        };
        BridgeRegistry::set_pending_tool("session-pt", tool);

        let retrieved = BridgeRegistry::pending_tool("session-pt");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name(), "Read");

        BridgeRegistry::clear();
    }

    #[test]
    fn bridge_registry_isolates_nested_keys_and_replaces_stale_duplicates() {
        let _lock = lock_bridge_registry_for_test();
        BridgeRegistry::clear();

        let parent = CursorBridgeState::new(
            "p:13:shared-session".into(),
            "msg-parent".into(),
            "cursor-test".into(),
            None,
            Box::new(|| "parent-id".into()),
        );
        BridgeRegistry::insert(parent);
        BridgeRegistry::set_pending_tool(
            "p:13:shared-session",
            PendingCursorTool::Read {
                tool_use_id: "parent-tool".into(),
                path: "/tmp/parent".into(),
            },
        );

        let child = CursorBridgeState::new(
            "a:13:shared-session9:child".into(),
            "msg-child".into(),
            "cursor-test".into(),
            None,
            Box::new(|| "child-id".into()),
        );
        BridgeRegistry::insert(child);
        BridgeRegistry::set_pending_tool(
            "a:13:shared-session9:child",
            PendingCursorTool::Write {
                tool_use_id: "child-tool".into(),
                path: "/tmp/child".into(),
                content: "child".into(),
            },
        );

        assert_eq!(BridgeRegistry::active_count(), 2);
        assert_eq!(
            BridgeRegistry::pending_tool("p:13:shared-session")
                .as_ref()
                .map(PendingCursorTool::tool_use_id),
            Some("parent-tool")
        );
        assert_eq!(
            BridgeRegistry::pending_tool("a:13:shared-session9:child")
                .as_ref()
                .map(PendingCursorTool::tool_use_id),
            Some("child-tool")
        );

        // A retry for the same key must replace the stale paused generation,
        // rather than leaving two entries where `find()` could pick the old one.
        let replacement = CursorBridgeState::new(
            "a:13:shared-session9:child".into(),
            "msg-child-retry".into(),
            "cursor-test".into(),
            None,
            Box::new(|| "child-retry-id".into()),
        );
        BridgeRegistry::insert(replacement);
        BridgeRegistry::set_pending_tool(
            "a:13:shared-session9:child",
            PendingCursorTool::Generic {
                tool_use_id: "child-tool-retry".into(),
                name: "Workflow".into(),
                input: serde_json::json!({"name":"deep-research"}),
            },
        );
        assert_eq!(BridgeRegistry::active_count(), 2);
        assert_eq!(
            BridgeRegistry::pending_tool("a:13:shared-session9:child")
                .as_ref()
                .map(PendingCursorTool::tool_use_id),
            Some("child-tool-retry")
        );

        BridgeRegistry::clear();
    }

    // -----------------------------------------------------------------------
    // render_tool_result_content tests
    // -----------------------------------------------------------------------

    #[test]
    fn renders_string_content() {
        let result = serde_json::json!({
            "type": "tool_result",
            "content": "plain string"
        });
        assert_eq!(render_tool_result_content(&result), "plain string");
    }

    #[test]
    fn renders_array_content() {
        let result = serde_json::json!({
            "type": "tool_result",
            "content": [
                {"type": "text", "text": "part one"},
                {"type": "text", "text": "part two"}
            ]
        });
        let rendered = render_tool_result_content(&result);
        assert!(rendered.contains("part one"));
        assert!(rendered.contains("part two"));
    }

    #[test]
    fn renders_mixed_content_types() {
        let result = serde_json::json!({
            "type": "tool_result",
            "content": [
                {"type": "text", "text": "text result"},
                {"type": "image", "source": {"type": "base64", "data": "AAAA"}}
            ]
        });
        let rendered = render_tool_result_content(&result);
        assert!(rendered.contains("text result"));
        assert!(rendered.contains("[image result omitted]"));
    }

    #[test]
    fn renders_structured_and_scalar_tool_result_content() {
        let object = serde_json::json!({
            "type": "tool_result",
            "content": {"status": "ok", "items": [1, 2]}
        });
        let rendered = render_tool_result_content(&object);
        assert!(rendered.contains("\"status\":\"ok\""));
        assert!(rendered.contains("\"items\":[1,2]"));

        let structured = serde_json::json!({
            "type": "tool_result",
            "structured_output": {"answer": 42}
        });
        assert_eq!(render_tool_result_content(&structured), r#"{"answer":42}"#);
    }

    #[test]
    fn renders_string_items_in_tool_result_arrays_without_json_quotes() {
        let result = serde_json::json!({
            "type": "tool_result",
            "content": ["first", "second"]
        });
        assert_eq!(render_tool_result_content(&result), "first\nsecond");
    }

    #[test]
    fn render_empty_tool_result() {
        let result = serde_json::json!({"type": "tool_result"});
        assert_eq!(render_tool_result_content(&result), "");
    }

    #[test]
    fn detects_error_from_tool_result() {
        let result = serde_json::json!({"type": "tool_result", "is_error": true});
        assert!(tool_result_is_error(&result));

        let result = serde_json::json!({"type": "tool_result", "is_error": false});
        assert!(!tool_result_is_error(&result));

        let result = serde_json::json!({"type": "tool_result"});
        assert!(!tool_result_is_error(&result));
    }

    #[test]
    fn write_is_not_remapped_to_edit_when_only_edit_is_advertised() {
        let allowed: BTreeSet<String> = ["Edit".into()].into_iter().collect();
        assert!(
            resolve_advertised_name("Write", Some(&allowed)).is_none(),
            "Write→Edit remaps Claude Edit schema (old_string/new_string) incorrectly"
        );
    }

    #[test]
    fn write_prefers_write_over_edit_aliases() {
        let allowed: BTreeSet<String> = ["Edit".into(), "Write".into()].into_iter().collect();
        assert_eq!(
            resolve_advertised_name("Write", Some(&allowed)).as_deref(),
            Some("Write")
        );
    }

    #[test]
    fn write_alias_resolution_uses_the_advertised_spelling() {
        for alias in ["write", "write_file", "WriteFile"] {
            let allowed: BTreeSet<String> = [alias.to_string()].into_iter().collect();
            assert_eq!(
                resolve_advertised_name("Write", Some(&allowed)).as_deref(),
                Some(alias),
                "native Write should follow the client's registered alias"
            );
            assert_eq!(
                resolve_advertised_name(alias, Some(&allowed)).as_deref(),
                Some(alias),
                "recovered XML aliases should remain callable"
            );
        }
    }

    #[test]
    fn pi_edit_resolves_to_modern_text_editor_when_only_modern_name_is_advertised() {
        let allowed: BTreeSet<String> = ["str_replace_based_edit_tool".to_string()]
            .into_iter()
            .collect();
        assert_eq!(
            resolve_advertised_name("Edit", Some(&allowed)).as_deref(),
            Some("str_replace_based_edit_tool"),
            "Pi Edit must follow Claude Code 2.1.193's exact text-editor name"
        );
        // All historical spellings should resolve in the same direction when
        // an upstream Cursor frame says StrReplace/Edit interchangeably.
        for mapped in ["Edit", "StrReplace", "StrReplaceTool", "str_replace_editor"] {
            assert_eq!(
                resolve_advertised_name(mapped, Some(&allowed)).as_deref(),
                Some("str_replace_based_edit_tool"),
                "{mapped} should resolve to the modern advertised editor"
            );
        }
    }

    #[test]
    fn claude_alias_resolution_uses_the_advertised_spelling() {
        let cases = [
            ("RunWorkflow", "Workflow"),
            ("Brief", "SendUserMessage"),
            ("ListMcpResources", "ListMcpResourcesTool"),
            ("ReadMcpResource", "ReadMcpResourceTool"),
            ("ReadMcpResourceDir", "ReadMcpResourceDirTool"),
            ("ListPeers", "ListAgents"),
            ("KillBash", "TaskStop"),
            ("AgentOutputTool", "TaskOutput"),
        ];
        for (mapped, advertised) in cases {
            let allowed: BTreeSet<String> = [advertised.to_string()].into_iter().collect();
            assert_eq!(
                resolve_advertised_name(mapped, Some(&allowed)).as_deref(),
                Some(advertised),
                "{mapped} should resolve to the exact advertised spelling"
            );
        }
    }

    #[test]
    fn claude_alias_resolution_handles_qualified_local_names() {
        let allowed: BTreeSet<String> = ["Workflow".into()].into_iter().collect();
        assert_eq!(
            resolve_advertised_name("mcp_claude-local_RunWorkflow", Some(&allowed)).as_deref(),
            Some("Workflow")
        );

        let qualified: BTreeSet<String> =
            ["mcp_claude-local_Workflow".into()].into_iter().collect();
        assert_eq!(
            resolve_advertised_name("RunWorkflow", Some(&qualified)).as_deref(),
            Some("mcp_claude-local_Workflow")
        );
    }

    #[test]
    fn claude_alias_resolution_prefers_same_spelling_before_alias() {
        let allowed: BTreeSet<String> = ["RunWorkflow".into(), "Workflow".into()]
            .into_iter()
            .collect();
        assert_eq!(
            resolve_advertised_name("workflow", Some(&allowed)).as_deref(),
            Some("Workflow")
        );
    }

    #[test]
    fn arbitrary_mcp_names_remain_exact_only() {
        let allowed: BTreeSet<String> = ["mcp__plugin__CustomTool".into()].into_iter().collect();
        assert!(resolve_advertised_name("mcp__plugin__customtool", Some(&allowed)).is_none());
        assert!(resolve_advertised_name("CustomTool", Some(&allowed)).is_none());

        let same_leaf: BTreeSet<String> = ["mcp__provider_b__search".into()].into_iter().collect();
        assert!(
            resolve_advertised_name("mcp__provider_a__search", Some(&same_leaf)).is_none(),
            "MCP tools with the same leaf must remain isolated by provider"
        );
    }

    #[test]
    fn pending_workflow_xml_maps_to_generic_for_claude_local_fulfillment() {
        let tool = RecoveredCursorToolUse {
            id: "call_wf_1".into(),
            original_id: Some("wf1".into()),
            name: "Workflow".into(),
            input: serde_json::json!({
                "name": "deep-research",
                "args": "what is rust async"
            })
            .as_object()
            .cloned()
            .unwrap(),
        };
        let pending = pending_from_recovered_tool(&tool).unwrap();
        assert_eq!(pending.name(), "Workflow");
        assert_eq!(pending.tool_use_id(), "call_wf_1");
        let json = pending.input_json();
        assert_eq!(json["name"], "deep-research");
        assert_eq!(json["args"], "what is rust async");
        assert!(matches!(pending, PendingCursorTool::Generic { .. }));
    }
}
