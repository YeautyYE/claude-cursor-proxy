use crate::anthropic::schema::MessagesRequest;
use crate::providers::cursor::client::{
    CursorUpstreamResponse, decode_frame_payload, decode_upstream_frames,
};
use crate::providers::cursor::connect::{ConnectEndError, FLAG_END, parse_connect_error};
use crate::providers::cursor::proto::AgentServerMessage;

/// A decoded event from the Cursor upstream response stream.
#[derive(Debug, Clone)]
pub enum CursorStreamEvent {
    Session {
        session_id: String,
    },
    ThinkingDelta {
        text: String,
    },
    TextDelta {
        text: String,
    },
    /// Native Cursor tool call (InteractionUpdate.tool_call_started or Exec* args).
    /// Mapped to Claude Code Anthropic tool names/inputs.
    NativeTool {
        tool_use_id: String,
        name: String,
        input: serde_json::Value,
    },
    Usage {
        input_tokens: u64,
        output_tokens: u64,
        cache_read_tokens: u64,
        cache_write_tokens: u64,
    },
    /// Incremental output/thinking tokens from Cursor `token_delta`.
    /// Must not wipe input/cache counters the way a full `Usage` snapshot would.
    OutputTokenDelta {
        tokens: u64,
    },
    End,
}

#[derive(Debug, Clone)]
pub enum CursorDecodeError {
    ConnectEnd(ConnectEndError),
    Decode(String),
}

impl CursorDecodeError {
    pub fn status(&self) -> Option<u16> {
        match self {
            CursorDecodeError::ConnectEnd(err) => Some(err.status),
            CursorDecodeError::Decode(_) => None,
        }
    }
}

impl std::fmt::Display for CursorDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CursorDecodeError::ConnectEnd(err) => write!(f, "{err}"),
            CursorDecodeError::Decode(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for CursorDecodeError {}

/// Decode upstream response bytes into a sequence of CursorStreamEvents.
///
/// Returns both the events and the final usage for the response, since the
/// upstream may send multiple update frames.
pub fn decode_upstream_response(body: &[u8]) -> Result<Vec<CursorStreamEvent>, CursorDecodeError> {
    let frames =
        decode_upstream_frames(body).map_err(|e| CursorDecodeError::Decode(e.to_string()))?;
    let mut events = Vec::new();

    for frame in &frames {
        if frame.flags & FLAG_END != 0 {
            // Check for Connect error in end frame
            if !frame.payload.is_empty()
                && let Some(err) = parse_connect_error(&frame.payload)
            {
                return Err(CursorDecodeError::ConnectEnd(err));
            }
            events.push(CursorStreamEvent::End);
            continue;
        }

        let msg = match decode_frame_payload(frame) {
            Ok(m) => m,
            Err(_) => continue,
        };

        events_from_message(&msg, &mut events);
    }

    Ok(events)
}

/// Build an accumulated Anthropic response JSON from upstream bytes for
/// non-streaming mode.
pub fn decode_cursor_upstream(
    upstream: &CursorUpstreamResponse,
    message_id: &str,
    model: &str,
) -> Result<serde_json::Value, CursorDecodeError> {
    let events = decode_upstream_response(&upstream.body)?;

    let mut text_content = String::new();
    let mut final_input_tokens: u64 = 0;
    let mut final_output_tokens: u64 = 0;
    let mut final_cache_read: u64 = 0;
    let mut final_cache_write: u64 = 0;

    for event in &events {
        match event {
            CursorStreamEvent::TextDelta { text } => text_content.push_str(text),
            CursorStreamEvent::Usage {
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
            } => {
                final_input_tokens = *input_tokens;
                final_output_tokens = *output_tokens;
                final_cache_read = *cache_read_tokens;
                final_cache_write = *cache_write_tokens;
            }
            CursorStreamEvent::OutputTokenDelta { tokens } => {
                final_output_tokens = final_output_tokens.saturating_add(*tokens);
            }
            CursorStreamEvent::End => break,
            _ => {}
        }
    }

    let (input_tokens, output_tokens, cache_read_tokens, cache_write_tokens) =
        crate::providers::cursor::sse::normalize_cursor_usage_for_anthropic(
            final_input_tokens.max(estimate_input_tokens(&text_content)),
            final_output_tokens,
            final_cache_read,
            final_cache_write,
        );

    Ok(serde_json::json!({
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "content": [
            {"type": "text", "text": text_content}
        ],
        "model": model,
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cache_creation_input_tokens": cache_write_tokens,
            "cache_read_input_tokens": cache_read_tokens
        }
    }))
}

fn estimate_input_tokens(_content: &str) -> u64 {
    // Rough upper bound: 4 chars per token for input estimation
    (_content.len() / 4) as u64
}

fn events_from_message(msg: &AgentServerMessage, events: &mut Vec<CursorStreamEvent>) {
    if let Some(ref exec) = msg.exec_server_message {
        if let Some(ref sid) = exec.exec_id
            && !sid.is_empty()
        {
            events.push(CursorStreamEvent::Session {
                session_id: sid.clone(),
            });
        }
        // BiDi exec tool requests (not request_context) → Claude tool_use.
        if exec.request_context_args.is_none()
            && let Some(mapped) = super::native_tools::map_exec_server_message(exec)
        {
            events.push(CursorStreamEvent::NativeTool {
                tool_use_id: mapped.tool_use_id,
                name: mapped.name,
                input: mapped.input,
            });
        }
    }

    if let Some(ref update) = msg.interaction_update {
        push_interaction_stream_events(update, events, 0);
    }
}

fn push_interaction_stream_events(
    update: &super::proto::InteractionUpdate,
    events: &mut Vec<CursorStreamEvent>,
    nest_depth: u8,
) {
    if let Some(ref td) = update.thinking_delta
        && !td.text.is_empty()
    {
        events.push(CursorStreamEvent::ThinkingDelta {
            text: td.text.clone(),
        });
    }

    if let Some(ref td) = update.text_delta
        && !td.text.is_empty()
    {
        events.push(CursorStreamEvent::TextDelta {
            text: td.text.clone(),
        });
    }

    // tool_call_started/completed belong to Cursor's UI transcript. Local
    // execution is requested separately by ExecServerMessage and only that
    // message carries the ids needed to return a native result.
    // Nested Task deltas (tag 2) carry another InteractionUpdate — flatten
    // one level so subagent text is not dropped. MCP arg accumulation for
    // ClientOnly lives on the live BiDi path.
    if nest_depth < super::proto::MAX_TASK_DELTA_NEST
        && let Some(nested) = update
            .tool_call_delta
            .as_ref()
            .and_then(super::proto::ToolCallDeltaUpdate::nested_task_update)
    {
        push_interaction_stream_events(nested, events, nest_depth + 1);
    }

    if nest_depth > 0 {
        // Nested turn_ended must not end the parent Task stream.
        return;
    }

    // Token delta is an incremental output/thinking signal — never a full
    // usage snapshot. Mapping it to Usage{input:0,..} previously wiped the
    // status bar down to In:1 Out:N.
    if let Some(ref td) = update.token_delta
        && td.tokens > 0
    {
        events.push(CursorStreamEvent::OutputTokenDelta {
            tokens: td.tokens as u64,
        });
    }

    // Turn ended (usage + end) — fields are optional on wire
    if let Some(ref te) = update.turn_ended {
        events.push(CursorStreamEvent::Usage {
            input_tokens: te.input_tokens.unwrap_or(0),
            output_tokens: te
                .output_tokens
                .unwrap_or(0)
                .saturating_add(te.reasoning_tokens.unwrap_or(0)),
            cache_read_tokens: te.cache_read_tokens.unwrap_or(0),
            cache_write_tokens: te.cache_write_tokens.unwrap_or(0),
        });
        events.push(CursorStreamEvent::End);
    }
}

/// Cheap input-token estimate for `message_start` seeding.
///
/// Avoids re-running [`super::request::render_cursor_prompt`] (full history +
/// tools JSON), which can cost tens–hundreds of ms on large Claude Code turns
/// and delays TTFT. `turn_ended` usage replaces this seed when available.
pub fn estimate_request_input_tokens(req: &MessagesRequest) -> u64 {
    let mut chars = 0usize;
    for message in &req.messages {
        chars += match &message.content {
            serde_json::Value::String(s) => s.len(),
            serde_json::Value::Array(blocks) => blocks
                .iter()
                .map(|block| match block.get("type").and_then(|t| t.as_str()) {
                    Some("text") => block
                        .get("text")
                        .and_then(|t| t.as_str())
                        .map_or(0, str::len),
                    Some("thinking") => block
                        .get("thinking")
                        .and_then(|t| t.as_str())
                        .map_or(0, str::len),
                    Some("tool_result") => match block.get("content") {
                        Some(serde_json::Value::String(s)) => s.len(),
                        Some(serde_json::Value::Array(parts)) => parts
                            .iter()
                            .map(|p| p.get("text").and_then(|t| t.as_str()).map_or(0, str::len))
                            .sum(),
                        _ => 64,
                    },
                    _ => 64,
                })
                .sum(),
            _ => 0,
        };
    }
    if let Some(tools) = req.extra.get("tools") {
        // Schema dump dominates Ctx. Walk the Value tree for a size estimate —
        // `tools.to_string()` re-serializes the full tools array (often 100KB+)
        // on every request and only delayed TTFT / burned CPU before first byte.
        chars = chars.saturating_add(json_size_estimate(tools));
    }
    (chars / 4).max(1) as u64
}

/// Approximate serialized JSON size without allocating the full string.
fn json_size_estimate(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null => 4,
        serde_json::Value::Bool(true) => 4,
        serde_json::Value::Bool(false) => 5,
        serde_json::Value::Number(n) => n.to_string().len(),
        serde_json::Value::String(s) => s.len().saturating_add(2),
        serde_json::Value::Array(items) => {
            let inner: usize = items.iter().map(json_size_estimate).sum();
            // brackets + commas
            inner
                .saturating_add(2)
                .saturating_add(items.len().saturating_sub(1))
        }
        serde_json::Value::Object(map) => {
            let inner: usize = map
                .iter()
                .map(|(k, v)| {
                    k.len()
                        .saturating_add(3)
                        .saturating_add(json_size_estimate(v))
                })
                .sum();
            inner
                .saturating_add(2)
                .saturating_add(map.len().saturating_sub(1))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cursor::connect::encode_connect_frame;
    use crate::providers::cursor::proto::*;
    use crate::providers::cursor::test_frames;
    use prost::Message as ProstMessage;

    #[test]
    fn decodes_text_and_usage_events() {
        let mut body = Vec::new();
        body.extend_from_slice(&test_frames::text_frame("Hello"));
        body.extend_from_slice(&test_frames::text_frame(" world"));
        body.extend_from_slice(&test_frames::usage_frame(10, 5));
        body.extend_from_slice(&test_frames::end_frame());

        let events = decode_upstream_response(&body).unwrap();
        assert_eq!(events.len(), 5);
        assert!(matches!(events[0], CursorStreamEvent::TextDelta { .. }));
        assert!(matches!(events[1], CursorStreamEvent::TextDelta { .. }));
        assert!(matches!(events[2], CursorStreamEvent::Usage { .. }));
        assert!(matches!(events[3], CursorStreamEvent::End));
        assert!(matches!(events[4], CursorStreamEvent::End));
    }

    #[test]
    fn decodes_thinking_delta() {
        let body = test_frames::thinking_frame("thinking...");

        let events = decode_upstream_response(&body).unwrap();
        assert_eq!(events.len(), 1);
        if let CursorStreamEvent::ThinkingDelta { text } = &events[0] {
            assert_eq!(text, "thinking...");
        } else {
            panic!("expected ThinkingDelta");
        }
    }

    #[test]
    fn decodes_session_event() {
        let msg = AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: None,
            kv_server_message: None,
            interaction_query: None,
            exec_server_message: Some(ExecServerMessage {
                id: 0,
                exec_id: Some("session-123".to_string()),
                shell_args: None,
                write_args: None,
                delete_args: None,
                grep_args: None,
                read_args: None,
                ls_args: None,
                request_context_args: None,
                shell_stream_args: None,
            }),
        };
        let mut payload = Vec::new();
        msg.encode(&mut payload).unwrap();
        let body = encode_connect_frame(&payload, 0).to_vec();

        let events = decode_upstream_response(&body).unwrap();
        assert_eq!(events.len(), 1);
        if let CursorStreamEvent::Session { session_id } = &events[0] {
            assert_eq!(session_id, "session-123");
        } else {
            panic!("expected Session");
        }
    }

    #[test]
    fn accumulate_response_produces_anthropic_json() {
        let mut body = Vec::new();
        body.extend_from_slice(&test_frames::text_frame("Hello world"));
        body.extend_from_slice(&test_frames::usage_frame(15, 3));
        body.extend_from_slice(&test_frames::end_frame());

        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };

        let json = decode_cursor_upstream(&upstream, "msg_test", "cursor-test").unwrap();
        assert_eq!(json["id"], "msg_test");
        assert_eq!(json["content"][0]["text"], "Hello world");
        assert_eq!(json["usage"]["input_tokens"].as_u64(), Some(15));
        assert_eq!(json["usage"]["output_tokens"].as_u64(), Some(3));
        assert_eq!(
            json["usage"]["cache_creation_input_tokens"].as_u64(),
            Some(0)
        );
        assert_eq!(json["usage"]["cache_read_input_tokens"].as_u64(), Some(0));
        assert_eq!(json["stop_reason"], "end_turn");
    }

    #[test]
    fn empty_upstream_produces_empty_response() {
        let upstream = CursorUpstreamResponse {
            status: 200,
            body: Vec::new(),
            error_detail: None,
        };
        let json = decode_cursor_upstream(&upstream, "msg_empty", "cursor-test").unwrap();
        assert_eq!(json["content"][0]["text"], "");
    }

    #[test]
    fn connect_end_frame_with_error_is_rejected() {
        let json_err = serde_json::json!({
            "error": {"code": "resource_exhausted", "message": "quota exceeded"}
        });
        let payload = serde_json::to_vec(&json_err).unwrap();
        let frame = encode_connect_frame(&payload, FLAG_END);
        let result = decode_upstream_response(&frame);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), Some(429));
        assert!(err.to_string().contains("quota exceeded"));
    }

    #[test]
    fn multiple_text_deltas_accumulate() {
        let mut body = Vec::new();
        body.extend_from_slice(&test_frames::text_frame("Hello "));
        body.extend_from_slice(&test_frames::text_frame("world"));
        body.extend_from_slice(&test_frames::usage_frame(10, 2));
        body.extend_from_slice(&test_frames::end_frame());

        let events = decode_upstream_response(&body).unwrap();
        let text: String = events
            .iter()
            .filter_map(|e| {
                if let CursorStreamEvent::TextDelta { text } = e {
                    Some(text.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(text, "Hello world");
    }

    #[test]
    fn decodes_partial_tool_call_fixture_frame() {
        let msg = AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                partial_tool_call: Some(PartialToolCall {
                    call_id: "mcp-1".into(),
                    model_call_id: "model-1".into(),
                    args_text_delta: r#"{"name":"deep-research"}"#.into(),
                    tool_call: Some(ToolCall {
                        mcp_tool_call: Some(McpToolCall {
                            args: Some(McpArgs {
                                name: "Workflow".into(),
                                tool_name: "Workflow".into(),
                                tool_call_id: "mcp-1".into(),
                                provider_identifier: "claude-local".into(),
                                args: Default::default(),
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            kv_server_message: None,
            interaction_query: None,
            exec_server_message: None,
        };
        let mut payload = Vec::new();
        msg.encode(&mut payload).unwrap();
        assert_eq!(
            payload[0], 0x0a,
            "AgentServerMessage.interaction_update is tag 1"
        );
        let body = encode_connect_frame(&payload, 0).to_vec();
        let events = decode_upstream_response(&body).unwrap();
        assert!(
            events.is_empty(),
            "partial_tool_call is transcript/arg stream, not a buffered NativeTool: {events:?}"
        );
        let decoded = AgentServerMessage::decode(payload.as_slice()).unwrap();
        let partial = decoded
            .interaction_update
            .unwrap()
            .partial_tool_call
            .unwrap();
        assert_eq!(partial.call_id, "mcp-1");
        assert_eq!(partial.args_text_delta, r#"{"name":"deep-research"}"#);
    }

    #[test]
    fn decodes_tool_call_delta_fixture_frame() {
        let msg = AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                tool_call_delta: Some(ToolCallDeltaUpdate {
                    call_id: "edit-1".into(),
                    model_call_id: "model-1".into(),
                    tool_call_delta: Some(ToolCallDelta {
                        edit_tool_call_delta: Some(EditToolCallDelta {
                            stream_content_delta: "// code".into(),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            kv_server_message: None,
            interaction_query: None,
            exec_server_message: None,
        };
        let mut payload = Vec::new();
        msg.encode(&mut payload).unwrap();
        assert!(
            payload.contains(&0x7a),
            "tool_call_delta tag 15 missing in {payload:?}"
        );
        let body = encode_connect_frame(&payload, 0).to_vec();
        let events = decode_upstream_response(&body).unwrap();
        assert!(events.is_empty(), "tool_call_delta is not a NativeTool");
        let decoded = AgentServerMessage::decode(payload.as_slice()).unwrap();
        assert_eq!(
            decoded
                .interaction_update
                .unwrap()
                .tool_call_delta
                .unwrap()
                .call_id,
            "edit-1"
        );
    }

    #[test]
    fn nested_task_delta_text_surfaces_without_ending_parent() {
        let msg = AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                tool_call_delta: Some(ToolCallDeltaUpdate {
                    call_id: "task-1".into(),
                    model_call_id: "model-1".into(),
                    tool_call_delta: Some(ToolCallDelta {
                        task_tool_call_delta: Some(TaskToolCallDelta {
                            interaction_update: Some(Box::new(InteractionUpdate {
                                text_delta: Some(TextDelta {
                                    text: "from subagent".into(),
                                }),
                                turn_ended: Some(TurnEnded {
                                    output_tokens: Some(3),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            kv_server_message: None,
            interaction_query: None,
            exec_server_message: None,
        };
        let mut payload = Vec::new();
        msg.encode(&mut payload).unwrap();
        let body = encode_connect_frame(&payload, 0).to_vec();
        let events = decode_upstream_response(&body).unwrap();
        let text: Vec<&str> = events
            .iter()
            .filter_map(|e| match e {
                CursorStreamEvent::TextDelta { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(text, ["from subagent"]);
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, CursorStreamEvent::End | CursorStreamEvent::Usage { .. })),
            "nested turn_ended must not end the parent: {events:?}"
        );
    }

    #[test]
    fn nested_task_delta_second_level_text_is_not_flattened() {
        let inner = InteractionUpdate {
            text_delta: Some(TextDelta {
                text: "secret".into(),
            }),
            ..Default::default()
        };
        let mid = InteractionUpdate {
            text_delta: Some(TextDelta {
                text: "visible".into(),
            }),
            tool_call_delta: Some(ToolCallDeltaUpdate {
                call_id: "task-inner".into(),
                model_call_id: "model-1".into(),
                tool_call_delta: Some(ToolCallDelta {
                    task_tool_call_delta: Some(TaskToolCallDelta {
                        interaction_update: Some(Box::new(inner)),
                    }),
                    ..Default::default()
                }),
            }),
            ..Default::default()
        };
        let msg = AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                tool_call_delta: Some(ToolCallDeltaUpdate {
                    call_id: "task-outer".into(),
                    model_call_id: "model-1".into(),
                    tool_call_delta: Some(ToolCallDelta {
                        task_tool_call_delta: Some(TaskToolCallDelta {
                            interaction_update: Some(Box::new(mid)),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            kv_server_message: None,
            interaction_query: None,
            exec_server_message: None,
        };
        let mut payload = Vec::new();
        msg.encode(&mut payload).unwrap();
        let body = encode_connect_frame(&payload, 0).to_vec();
        let events = decode_upstream_response(&body).unwrap();
        let text: Vec<&str> = events
            .iter()
            .filter_map(|e| match e {
                CursorStreamEvent::TextDelta { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(text, ["visible"]);
        assert!(
            !text.iter().any(|t| t.contains("secret")),
            "second nested Task level must stay capped: {events:?}"
        );
    }

    #[test]
    fn estimate_request_input_tokens_avoids_tools_tostring_blowup() {
        // Large tools schema must not require a full JSON re-serialize of the
        // tools array (previously `tools.to_string().len()` on every request).
        let tools = serde_json::json!([
            {
                "name": "Read",
                "description": "x".repeat(8_000),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string", "description": "y".repeat(8_000) }
                    }
                }
            }
        ]);
        let mut extra = serde_json::Map::new();
        extra.insert("tools".into(), tools);
        let req = MessagesRequest {
            model: Some("claude-fable-5".into()),
            messages: vec![crate::anthropic::schema::Message {
                role: "user".into(),
                content: serde_json::json!("hi"),
            }],
            max_tokens: Some(16),
            stream: true,
            extra,
        };
        let started = std::time::Instant::now();
        let tokens = estimate_request_input_tokens(&req);
        let elapsed = started.elapsed();
        assert!(
            tokens > 1_000,
            "expected large tools to dominate estimate, got {tokens}"
        );
        assert!(
            elapsed.as_millis() < 50,
            "tools size estimate too slow: {elapsed:?}"
        );
    }
}
