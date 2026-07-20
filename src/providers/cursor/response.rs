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
        // Thinking delta
        if let Some(ref td) = update.thinking_delta
            && !td.text.is_empty()
        {
            events.push(CursorStreamEvent::ThinkingDelta {
                text: td.text.clone(),
            });
        }

        // Text delta
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
}

/// Extract an estimate of input tokens from a MessagesRequest for usage
/// reporting. This is a rough heuristic based on JSON string length.
pub fn estimate_request_input_tokens(req: &MessagesRequest) -> u64 {
    let prompt = super::request::render_cursor_prompt(req);
    (prompt.len() / 4).max(1) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cursor::connect::encode_connect_frame;
    use crate::providers::cursor::proto::*;
    use crate::providers::cursor::test_frames;
    use prost::Message;

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
}
