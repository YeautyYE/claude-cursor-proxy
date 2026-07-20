use crate::providers::cursor::client::CursorUpstreamResponse;
use crate::providers::cursor::response::{CursorStreamEvent, decode_upstream_response};

/// SSE event name constants.
pub const EVENT_MESSAGE_START: &str = "message_start";
pub const EVENT_CONTENT_BLOCK_START: &str = "content_block_start";
pub const EVENT_CONTENT_BLOCK_DELTA: &str = "content_block_delta";
pub const EVENT_CONTENT_BLOCK_STOP: &str = "content_block_stop";
pub const EVENT_MESSAGE_DELTA: &str = "message_delta";
pub const EVENT_MESSAGE_STOP: &str = "message_stop";
pub const EVENT_PING: &str = "ping";
pub const EVENT_ERROR: &str = "error";

/// Frame upstream Cursor response bytes into Anthropic SSE event bytes.
///
/// Produces the standard message lifecycle:
/// 1. message_start (with initial usage)
/// 2. content_block_start (text)
/// 3. content_block_delta (text deltas) / content_block_delta (thinking deltas)
/// 4. content_block_stop
/// 5. message_delta (with final usage and stop_reason)
/// 6. message_stop
pub fn frame_cursor_stream(
    upstream: &CursorUpstreamResponse,
    message_id: &str,
    model: &str,
) -> Vec<u8> {
    let events = match decode_upstream_response(&upstream.body) {
        Ok(e) => e,
        Err(e) => {
            return format_sse_error(&e.to_string());
        }
    };

    let mut sse = Vec::new();
    let mut framer = CursorSseFramer::new(&mut sse, message_id, model);

    for event in &events {
        match event {
            CursorStreamEvent::ThinkingDelta { text } => {
                framer.emit_thinking_delta(text);
            }
            CursorStreamEvent::TextDelta { text } => {
                framer.emit_text_delta(text);
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
            CursorStreamEvent::End => {
                framer.emit_final_message("end_turn");
            }
            CursorStreamEvent::Session { .. } => {
                // Session events are informational, not mapped to SSE
            }
            CursorStreamEvent::NativeTool {
                tool_use_id,
                name,
                input,
            } => {
                let input_json = serde_json::to_string(input).unwrap_or_else(|_| "{}".to_string());
                framer.emit_tool_pause(tool_use_id, name, &input_json);
            }
        }
    }

    framer.finalize();
    sse
}

/// Format an SSE error event.
fn format_sse_error(error: &str) -> Vec<u8> {
    let data = serde_json::json!({
        "type": "error",
        "error": {
            "type": "api_error",
            "message": error
        }
    });
    format_sse_event_bytes("error", &data)
}

/// Write one SSE event into `out` without an intermediate `String`/`Vec`.
pub(crate) fn write_sse_event(out: &mut Vec<u8>, event: &str, data: &serde_json::Value) {
    out.reserve(32 + event.len());
    out.extend_from_slice(b"event: ");
    out.extend_from_slice(event.as_bytes());
    out.extend_from_slice(b"\ndata: ");
    if serde_json::to_writer(&mut *out, data).is_err() {
        out.extend_from_slice(b"{}");
    }
    out.extend_from_slice(b"\n\n");
}

/// Hot-path content_block_delta writer (text / thinking) — no `json!` Value tree.
fn write_content_delta(out: &mut Vec<u8>, index: i32, delta_type: &str, field: &str, value: &str) {
    use std::io::Write;
    out.reserve(96 + value.len());
    let _ = write!(
        out,
        "event: {EVENT_CONTENT_BLOCK_DELTA}\ndata: {{\"type\":\"content_block_delta\",\"index\":{index},\"delta\":{{\"type\":\"{delta_type}\",\"{field}\":"
    );
    if serde_json::to_writer(&mut *out, value).is_err() {
        out.extend_from_slice(b"\"\"");
    }
    out.extend_from_slice(b"}}\n\n");
}

/// Format a single SSE event into bytes.
pub(crate) fn format_sse_event_bytes(event: &str, data: &serde_json::Value) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + event.len());
    write_sse_event(&mut out, event, data);
    out
}

// ---------------------------------------------------------------------------
// SSE Framer
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------

/// Map Cursor `TurnEnded` usage onto Anthropic Messages usage without
/// double-counting.
///
/// Cursor often sets `input_tokens` to the **full** prompt size while also
/// returning `cache_read` / `cache_write` that already partition that total
/// (observed: input ≈ read + write). Anthropic clients such as Claude Code
/// then treat usage as `input + cache_read + cache_creation`, which inflates
/// the context meter to ~2× and can show **100% context used**.
pub(crate) fn normalize_cursor_usage_for_anthropic(
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_write_tokens: u64,
) -> (u64, u64, u64, u64) {
    let cache_parts = cache_read_tokens.saturating_add(cache_write_tokens);
    if input_tokens > 0 && cache_parts > 0 && input_tokens >= cache_parts {
        // input already includes the cache breakdown → uncached remainder only.
        (
            input_tokens - cache_parts,
            output_tokens,
            cache_read_tokens,
            cache_write_tokens,
        )
    } else if input_tokens > 0 && cache_read_tokens == input_tokens && cache_write_tokens == 0 {
        // Duplicate totals (input == cache_read) → keep a single copy.
        (input_tokens, output_tokens, 0, 0)
    } else {
        (
            input_tokens,
            output_tokens,
            cache_read_tokens,
            cache_write_tokens,
        )
    }
}

/// SSE framer that tracks state to produce well-formed Anthropic SSE events.
pub struct CursorSseFramer<'a> {
    output: &'a mut Vec<u8>,
    message_id: &'a str,
    model: &'a str,
    state: CursorSseState,
}

/// Mutable lifecycle state shared by the borrowed framer and the owned,
/// incremental encoder below.
#[derive(Debug, Default)]
struct CursorSseState {
    started: bool,
    thinking_open: bool,
    text_open: bool,
    next_index: i32,
    thinking_index: i32,
    text_index: i32,
    usage_input_tokens: u64,
    usage_output_tokens: u64,
    /// Char/4 floor from thinking+text deltas; finalized as max with usage_output_tokens.
    usage_output_estimate: u64,
    usage_cache_read_tokens: u64,
    usage_cache_write_tokens: u64,
    finalized: bool,
}

impl<'a> CursorSseFramer<'a> {
    pub fn new(output: &'a mut Vec<u8>, message_id: &'a str, model: &'a str) -> Self {
        Self {
            output,
            message_id,
            model,
            state: CursorSseState {
                thinking_index: -1,
                text_index: -1,
                ..CursorSseState::default()
            },
        }
    }

    pub fn ensure_start(&mut self) {
        if self.state.started || self.state.finalized {
            return;
        }
        self.state.started = true;

        let data = serde_json::json!({
            "type": "message_start",
            "message": {
                "id": self.message_id,
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": self.model,
                "stop_reason": null,
                "stop_sequence": null,
                "usage": {
                    "input_tokens": self.state.usage_input_tokens.max(1),
                    "output_tokens": 0,
                    "cache_creation_input_tokens": self.state.usage_cache_write_tokens,
                    "cache_read_input_tokens": self.state.usage_cache_read_tokens
                }
            }
        });
        write_sse_event(self.output, EVENT_MESSAGE_START, &data);
    }

    fn open_thinking(&mut self) -> bool {
        if self.state.finalized {
            return false;
        }
        if self.state.thinking_open {
            return true;
        }
        // Anthropic content blocks are emitted serially. This is mostly a
        // defensive path for an unusual upstream text -> thinking transition.
        if self.state.text_open {
            self.close_text();
        }
        self.ensure_start();
        self.state.thinking_open = true;
        self.state.thinking_index = self.state.next_index;
        self.state.next_index += 1;

        let data = serde_json::json!({
            "type": "content_block_start",
            "index": self.state.thinking_index,
            "content_block": {
                "type": "thinking",
                "thinking": "",
                "signature": ""
            }
        });
        write_sse_event(self.output, EVENT_CONTENT_BLOCK_START, &data);
        true
    }

    fn open_text(&mut self) -> bool {
        if self.state.finalized {
            return false;
        }
        if self.state.text_open {
            return true;
        }
        if self.state.thinking_open {
            self.close_thinking();
        }
        self.ensure_start();
        self.state.text_open = true;
        self.state.text_index = self.state.next_index;
        self.state.next_index += 1;

        let data = serde_json::json!({
            "type": "content_block_start",
            "index": self.state.text_index,
            "content_block": {
                "type": "text",
                "text": ""
            }
        });
        write_sse_event(self.output, EVENT_CONTENT_BLOCK_START, &data);
        true
    }

    /// Thinking blocks require a signature delta immediately before their
    /// content_block_stop. Cursor does not expose an Anthropic signature, so a
    /// stable proxy signature keeps the event lifecycle well formed.
    fn close_thinking(&mut self) {
        if !self.state.thinking_open {
            return;
        }

        let data = serde_json::json!({
            "type": "content_block_delta",
            "index": self.state.thinking_index,
            "delta": {
                "type": "signature_delta",
                "signature": "cursor-proxy"
            }
        });
        write_sse_event(self.output, EVENT_CONTENT_BLOCK_DELTA, &data);

        let data = serde_json::json!({
            "type": "content_block_stop",
            "index": self.state.thinking_index
        });
        write_sse_event(self.output, EVENT_CONTENT_BLOCK_STOP, &data);
        self.state.thinking_open = false;
    }

    fn close_text(&mut self) {
        if !self.state.text_open {
            return;
        }
        let data = serde_json::json!({
            "type": "content_block_stop",
            "index": self.state.text_index
        });
        write_sse_event(self.output, EVENT_CONTENT_BLOCK_STOP, &data);
        self.state.text_open = false;
    }

    pub fn close_open_blocks(&mut self) {
        self.close_thinking();
        self.close_text();
    }

    pub fn emit_thinking_delta(&mut self, text: &str) {
        if !self.open_thinking() {
            return;
        }
        self.note_generated_text(text);
        // Hot path: avoid `json!` + intermediate String for every thinking chunk.
        write_content_delta(
            self.output,
            self.state.thinking_index,
            "thinking_delta",
            "thinking",
            text,
        );
    }

    pub fn emit_text_delta(&mut self, text: &str) {
        if !self.open_text() {
            return;
        }
        self.note_generated_text(text);
        // Hot path: avoid `json!` + intermediate String for every text chunk.
        write_content_delta(
            self.output,
            self.state.text_index,
            "text_delta",
            "text",
            text,
        );
    }

    /// Accumulate a rough Out floor from streamed text/thinking. Merged with
    /// token_delta / turn_ended via [`Self::resolved_output_tokens`] so the two
    /// signals do not double-count.
    fn note_generated_text(&mut self, text: &str) {
        if self.state.finalized || text.is_empty() {
            return;
        }
        let approx = (text.len() / 4) as u64;
        self.state.usage_output_estimate = self.state.usage_output_estimate.saturating_add(approx);
    }

    fn resolved_output_tokens(&self) -> u64 {
        self.state
            .usage_output_tokens
            .max(self.state.usage_output_estimate)
    }

    pub fn record_usage(
        &mut self,
        input_tokens: u64,
        output_tokens: u64,
        cache_read_tokens: u64,
        cache_write_tokens: u64,
    ) {
        if self.state.finalized {
            return;
        }
        // Legacy path: some callers historically mapped token_delta → Usage with
        // input/cache zeroed. Treat that as an output-only bump so we never wipe
        // a prior input/cache snapshot (status bar In:1 Out:N).
        if input_tokens == 0
            && cache_read_tokens == 0
            && cache_write_tokens == 0
            && output_tokens > 0
        {
            self.add_output_tokens(output_tokens);
            return;
        }
        let (input_tokens, output_tokens, cache_read_tokens, cache_write_tokens) =
            normalize_cursor_usage_for_anthropic(
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
            );
        self.state.usage_input_tokens = input_tokens;
        self.state.usage_output_tokens = output_tokens;
        self.state.usage_cache_read_tokens = cache_read_tokens;
        self.state.usage_cache_write_tokens = cache_write_tokens;
    }

    /// Accumulate incremental output/thinking tokens without clearing input/cache.
    pub fn add_output_tokens(&mut self, tokens: u64) {
        if self.state.finalized || tokens == 0 {
            return;
        }
        self.state.usage_output_tokens = self.state.usage_output_tokens.saturating_add(tokens);
    }

    /// Seed a provisional input estimate (e.g. from prompt length) until Cursor
    /// `turn_ended` supplies authoritative usage. Does not overwrite a real snapshot.
    pub fn seed_estimated_input_tokens(&mut self, tokens: u64) {
        if self.state.finalized || tokens == 0 {
            return;
        }
        if self.state.usage_input_tokens == 0
            && self.state.usage_cache_read_tokens == 0
            && self.state.usage_cache_write_tokens == 0
        {
            self.state.usage_input_tokens = tokens;
        }
    }

    pub fn next_content_block_index(&mut self) -> i32 {
        let index = self.state.next_index;
        self.state.next_index += 1;
        index
    }

    /// Emit one complete `tool_use` content block without ending the message.
    ///
    /// Cursor can request several native execs in one model turn. Anthropic
    /// represents that as several sibling `tool_use` blocks followed by one
    /// `message_delta(stop_reason="tool_use")`, so finalization is deliberately
    /// kept separate from this helper.
    pub fn emit_tool_use_block(&mut self, tool_use_id: &str, tool_name: &str, partial_json: &str) {
        if self.state.finalized {
            return;
        }
        self.close_open_blocks();
        self.ensure_start();
        let index = self.next_content_block_index();

        let data = serde_json::json!({
            "type": "content_block_start",
            "index": index,
            "content_block": {
                "type": "tool_use",
                "id": tool_use_id,
                "name": tool_name,
                "input": {}
            }
        });
        write_sse_event(self.output, EVENT_CONTENT_BLOCK_START, &data);

        let data = serde_json::json!({
            "type": "content_block_delta",
            "index": index,
            "delta": {
                "type": "input_json_delta",
                "partial_json": partial_json
            }
        });
        write_sse_event(self.output, EVENT_CONTENT_BLOCK_DELTA, &data);

        let data = serde_json::json!({
            "type": "content_block_stop",
            "index": index
        });
        write_sse_event(self.output, EVENT_CONTENT_BLOCK_STOP, &data);
    }

    /// Emit a single-tool pause. This preserves the historical wire sequence
    /// while [`Self::emit_tool_use_block`] also permits batched tool calls.
    pub fn emit_tool_pause(&mut self, tool_use_id: &str, tool_name: &str, partial_json: &str) {
        self.emit_tool_use_block(tool_use_id, tool_name, partial_json);
        self.emit_final_message("tool_use");
    }

    pub fn emit_final_message(&mut self, stop_reason: &str) {
        if self.state.finalized {
            return;
        }
        self.ensure_start();
        self.close_open_blocks();

        // message_delta
        let data = serde_json::json!({
            "type": "message_delta",
            "delta": {
                "stop_reason": stop_reason,
                "stop_sequence": null
            },
            "usage": {
                "input_tokens": self.state.usage_input_tokens.max(1),
                "output_tokens": self.resolved_output_tokens(),
                "cache_creation_input_tokens": self.state.usage_cache_write_tokens,
                "cache_read_input_tokens": self.state.usage_cache_read_tokens
            }
        });
        write_sse_event(self.output, EVENT_MESSAGE_DELTA, &data);

        // message_stop
        let data = serde_json::json!({
            "type": "message_stop"
        });
        write_sse_event(self.output, EVENT_MESSAGE_STOP, &data);

        self.state.finalized = true;
    }

    pub fn finalize(&mut self) {
        if !self.state.finalized {
            self.emit_final_message("end_turn");
        }
    }

    pub fn is_finalized(&self) -> bool {
        self.state.finalized
    }
}

// ---------------------------------------------------------------------------
// Incremental SSE encoder
// ---------------------------------------------------------------------------

/// Owned incremental Anthropic SSE encoder.
///
/// Unlike [`CursorSseFramer`], this type owns its byte buffer and lifecycle
/// state. Call [`Self::push_event`] as decoded Cursor events arrive, then call
/// [`Self::take_bytes`] to drain only the newly generated SSE bytes.
pub struct CursorSseEncoder {
    output: Vec<u8>,
    message_id: String,
    model: String,
    state: CursorSseState,
}

impl CursorSseEncoder {
    pub fn new(message_id: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            output: Vec::new(),
            message_id: message_id.into(),
            model: model.into(),
            state: CursorSseState {
                thinking_index: -1,
                text_index: -1,
                ..CursorSseState::default()
            },
        }
    }

    /// Emit `message_start` eagerly. Repeated calls are idempotent.
    pub fn begin(&mut self) {
        self.with_framer(|framer| framer.ensure_start());
    }

    /// Encode one decoded upstream event. Session events are informational and
    /// intentionally produce no Anthropic SSE bytes.
    pub fn push_event(&mut self, event: &CursorStreamEvent) {
        if self.state.finalized {
            return;
        }

        self.with_framer(|framer| match event {
            CursorStreamEvent::ThinkingDelta { text } => framer.emit_thinking_delta(text),
            CursorStreamEvent::TextDelta { text } => framer.emit_text_delta(text),
            CursorStreamEvent::Usage {
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
            } => framer.record_usage(
                *input_tokens,
                *output_tokens,
                *cache_read_tokens,
                *cache_write_tokens,
            ),
            CursorStreamEvent::OutputTokenDelta { tokens } => framer.add_output_tokens(*tokens),
            CursorStreamEvent::NativeTool {
                tool_use_id,
                name,
                input,
            } => {
                let input_json = serde_json::to_string(input).unwrap_or_else(|_| "{}".to_string());
                framer.emit_tool_pause(tool_use_id, name, &input_json);
            }
            CursorStreamEvent::End => framer.emit_final_message("end_turn"),
            CursorStreamEvent::Session { .. } => {}
        });
    }

    /// Alias useful at call sites that refer to encoding rather than pushing.
    pub fn encode_event(&mut self, event: &CursorStreamEvent) {
        self.push_event(event);
    }

    pub fn emit_thinking_delta(&mut self, text: &str) {
        self.with_framer(|framer| framer.emit_thinking_delta(text));
    }

    pub fn emit_text_delta(&mut self, text: &str) {
        self.with_framer(|framer| framer.emit_text_delta(text));
    }

    pub fn record_usage(
        &mut self,
        input_tokens: u64,
        output_tokens: u64,
        cache_read_tokens: u64,
        cache_write_tokens: u64,
    ) {
        self.with_framer(|framer| {
            framer.record_usage(
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
            )
        });
    }

    pub fn add_output_tokens(&mut self, tokens: u64) {
        self.with_framer(|framer| framer.add_output_tokens(tokens));
    }

    pub fn seed_estimated_input_tokens(&mut self, tokens: u64) {
        self.with_framer(|framer| framer.seed_estimated_input_tokens(tokens));
    }

    /// Snapshot of the best-known Anthropic usage for TUI/monitor updates.
    pub fn current_usage(&self) -> (u64, u64) {
        (
            self.state.usage_input_tokens,
            self.state
                .usage_output_tokens
                .max(self.state.usage_output_estimate),
        )
    }

    pub fn emit_tool_pause(&mut self, tool_use_id: &str, tool_name: &str, partial_json: &str) {
        self.with_framer(|framer| framer.emit_tool_pause(tool_use_id, tool_name, partial_json));
    }

    /// Emit all native execs requested by one Cursor turn as sibling Anthropic
    /// `tool_use` blocks and finalize the downstream segment exactly once.
    pub fn emit_tool_batch<'a, I>(&mut self, tools: I)
    where
        I: IntoIterator<Item = (&'a str, &'a str, &'a str)>,
    {
        self.with_framer(|framer| {
            for (tool_use_id, tool_name, partial_json) in tools {
                framer.emit_tool_use_block(tool_use_id, tool_name, partial_json);
            }
            framer.emit_final_message("tool_use");
        });
    }

    pub fn finalize(&mut self) {
        self.with_framer(|framer| framer.finalize());
    }

    pub fn is_finalized(&self) -> bool {
        self.state.finalized
    }

    /// Drain the bytes generated since the previous call while retaining all
    /// lifecycle state for the next upstream event.
    pub fn take_bytes(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.output)
    }

    fn with_framer(&mut self, emit: impl FnOnce(&mut CursorSseFramer<'_>)) {
        let state = std::mem::take(&mut self.state);
        let mut framer = CursorSseFramer {
            output: &mut self.output,
            message_id: &self.message_id,
            model: &self.model,
            state,
        };
        emit(&mut framer);
        self.state = framer.state;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cursor::client::CursorUpstreamResponse;
    use crate::providers::cursor::response::CursorStreamEvent;
    use crate::providers::cursor::test_frames;

    #[test]
    fn normalize_splits_cursor_total_into_anthropic_parts() {
        // Observed live shape: input ≈ cache_read + cache_write.
        assert_eq!(
            normalize_cursor_usage_for_anthropic(53_037, 573, 25_980, 27_053),
            (4, 573, 25_980, 27_053)
        );
    }

    #[test]
    fn output_token_delta_does_not_wipe_input_or_cache() {
        let mut sse = Vec::new();
        let mut framer = CursorSseFramer::new(&mut sse, "msg_usage", "cursor-test");
        framer.seed_estimated_input_tokens(12_000);
        framer.add_output_tokens(2);
        framer.add_output_tokens(3);
        // Legacy wipe shape must also preserve seeded input.
        framer.record_usage(0, 7, 0, 0);
        framer.emit_final_message("end_turn");

        let events = parse_sse_events(&String::from_utf8_lossy(&sse));
        let delta = events
            .iter()
            .find(|(name, _)| *name == "message_delta")
            .map(|(_, data)| data)
            .expect("message_delta");
        assert_eq!(delta["usage"]["input_tokens"].as_u64(), Some(12_000));
        assert_eq!(delta["usage"]["output_tokens"].as_u64(), Some(12)); // 2+3+7
    }

    #[test]
    fn turn_ended_usage_replaces_seed_and_deltas() {
        let mut encoder = CursorSseEncoder::new("msg_usage2", "cursor-test");
        encoder.seed_estimated_input_tokens(99);
        encoder.add_output_tokens(5);
        encoder.push_event(&CursorStreamEvent::Usage {
            input_tokens: 53_037,
            output_tokens: 573,
            cache_read_tokens: 25_980,
            cache_write_tokens: 27_053,
        });
        encoder.push_event(&CursorStreamEvent::End);
        let events = parse_sse_events(&String::from_utf8_lossy(&encoder.take_bytes()));
        let delta = events
            .iter()
            .find(|(name, _)| *name == "message_delta")
            .map(|(_, data)| data)
            .expect("message_delta");
        assert_eq!(delta["usage"]["input_tokens"].as_u64(), Some(4));
        assert_eq!(delta["usage"]["output_tokens"].as_u64(), Some(573));
        assert_eq!(
            delta["usage"]["cache_read_input_tokens"].as_u64(),
            Some(25_980)
        );
        assert_eq!(
            delta["usage"]["cache_creation_input_tokens"].as_u64(),
            Some(27_053)
        );
    }

    #[test]
    fn normalize_drops_duplicate_cache_read_equal_to_input() {
        // input == cache_read (and no write) → treat input as already including
        // the cache portion, leaving uncached=0 + cache_read=total.
        assert_eq!(
            normalize_cursor_usage_for_anthropic(1_200_000, 1400, 1_200_000, 0),
            (0, 1400, 1_200_000, 0)
        );
    }

    #[test]
    fn normalize_leaves_plain_usage_alone() {
        assert_eq!(
            normalize_cursor_usage_for_anthropic(100, 10, 0, 0),
            (100, 10, 0, 0)
        );
    }

    #[test]
    fn sse_produces_message_start_and_stop() {
        let mut body = Vec::new();
        body.extend_from_slice(&test_frames::text_frame("hello"));
        body.extend_from_slice(&test_frames::usage_frame(10, 5));
        body.extend_from_slice(&test_frames::end_frame());

        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };

        let sse = frame_cursor_stream(&upstream, "msg_1", "cursor-test");
        let sse_str = String::from_utf8_lossy(&sse);

        // Verify event structure with explicit parsing
        let events = parse_sse_events(&sse_str);
        let event_names: Vec<&str> = events.iter().map(|e| e.0.as_str()).collect();

        assert_eq!(
            event_names,
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop"
            ]
        );
    }

    #[test]
    fn message_start_echoes_fable_wire_model() {
        let mut body = Vec::new();
        body.extend_from_slice(&test_frames::text_frame("hi"));
        body.extend_from_slice(&test_frames::end_frame());

        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };

        let wire = "claude-fable-5[1m]";
        let sse = frame_cursor_stream(&upstream, "msg_fable", wire);
        let events = parse_sse_events(&String::from_utf8_lossy(&sse));
        let start = events
            .iter()
            .find(|(name, _)| name == "message_start")
            .expect("message_start");
        assert_eq!(start.1["message"]["model"], wire);
    }

    #[test]
    fn sse_includes_text_delta_content() {
        let mut body = Vec::new();
        body.extend_from_slice(&test_frames::text_frame("Hello world"));
        body.extend_from_slice(&test_frames::usage_frame(10, 2));
        body.extend_from_slice(&test_frames::end_frame());

        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };

        let sse = frame_cursor_stream(&upstream, "msg_1", "cursor-test");
        let sse_str = String::from_utf8_lossy(&sse);
        let events = parse_sse_events(&sse_str);

        // Find text_delta event
        let text_delta = events
            .iter()
            .find(|(name, _)| *name == "content_block_delta")
            .map(|(_, data)| data["delta"]["text"].as_str().unwrap_or(""));
        assert_eq!(text_delta, Some("Hello world"));
    }

    #[test]
    fn sse_includes_usage_in_message_delta() {
        let mut body = Vec::new();
        body.extend_from_slice(&test_frames::text_frame("hi"));
        body.extend_from_slice(&test_frames::usage_frame(25, 7));
        body.extend_from_slice(&test_frames::end_frame());

        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };

        let sse = frame_cursor_stream(&upstream, "msg_1", "cursor-test");
        let sse_str = String::from_utf8_lossy(&sse);
        let events = parse_sse_events(&sse_str);

        let msg_delta = events
            .iter()
            .find(|(name, _)| *name == "message_delta")
            .map(|(_, data)| data.clone());
        assert!(msg_delta.is_some());
        let delta = msg_delta.unwrap();
        assert_eq!(delta["usage"]["input_tokens"].as_u64(), Some(25));
        assert_eq!(delta["usage"]["output_tokens"].as_u64(), Some(7));
        assert_eq!(
            delta["usage"]["cache_creation_input_tokens"].as_u64(),
            Some(0)
        );
        assert_eq!(delta["usage"]["cache_read_input_tokens"].as_u64(), Some(0));
    }

    #[test]
    fn sse_handles_empty_upstream() {
        let upstream = CursorUpstreamResponse {
            status: 200,
            body: Vec::new(),
            error_detail: None,
        };

        let sse = frame_cursor_stream(&upstream, "msg_1", "cursor-test");
        let sse_str = String::from_utf8_lossy(&sse);

        // Should still produce events even with empty body
        let events = parse_sse_events(&sse_str);
        let event_names: Vec<&str> = events.iter().map(|e| e.0.as_str()).collect();
        assert!(event_names.contains(&"message_start"));
        assert!(event_names.contains(&"message_stop"));
    }

    #[test]
    fn sse_emits_thinking_before_text() {
        let mut body = test_frames::thinking_frame("thinking...");

        body.extend_from_slice(&test_frames::text_frame("result"));
        body.extend_from_slice(&test_frames::usage_frame(10, 5));
        body.extend_from_slice(&test_frames::end_frame());

        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };

        let sse = frame_cursor_stream(&upstream, "msg_1", "cursor-test");
        let sse_str = String::from_utf8_lossy(&sse);
        let events = parse_sse_events(&sse_str);
        assert!(events.iter().any(|(_, data)| {
            data.get("content_block")
                .and_then(|c| c.get("type"))
                .and_then(|t| t.as_str())
                == Some("thinking")
        }));

        // Should have text content block
        assert!(events.iter().any(|(_, data)| {
            data.get("content_block")
                .and_then(|c| c.get("type"))
                .and_then(|t| t.as_str())
                == Some("text")
        }));
    }

    #[test]
    fn sse_error_response() {
        let sse = format_sse_error("something broke");
        let sse_str = String::from_utf8_lossy(&sse);
        let events = parse_sse_events(&sse_str);

        let (name, data) = &events[0];
        assert_eq!(name, "error");
        assert_eq!(data["error"]["type"], "api_error");
        assert_eq!(data["error"]["message"], "something broke");
    }

    #[test]
    fn incremental_encoder_emits_strict_tool_sequence_and_preserves_usage() {
        let mut encoder = CursorSseEncoder::new("msg_incremental", "cursor-test");
        let mut bytes = Vec::new();

        // Usage may arrive before the first content event and must be retained,
        // including the two cache counters.
        encoder.push_event(&CursorStreamEvent::Usage {
            input_tokens: 31,
            output_tokens: 9,
            cache_read_tokens: 7,
            cache_write_tokens: 5,
        });
        assert!(encoder.take_bytes().is_empty());

        encoder.begin();
        bytes.extend_from_slice(&encoder.take_bytes());
        encoder.begin();
        assert!(encoder.take_bytes().is_empty());

        encoder.push_event(&CursorStreamEvent::ThinkingDelta {
            text: "consider".to_string(),
        });
        bytes.extend_from_slice(&encoder.take_bytes());

        encoder.push_event(&CursorStreamEvent::TextDelta {
            text: "answer".to_string(),
        });
        bytes.extend_from_slice(&encoder.take_bytes());

        encoder.push_event(&CursorStreamEvent::NativeTool {
            tool_use_id: "tool_1".to_string(),
            name: "Read".to_string(),
            input: serde_json::json!({"file_path": "/tmp/example"}),
        });
        bytes.extend_from_slice(&encoder.take_bytes());

        assert!(encoder.is_finalized());
        let rendered = String::from_utf8(bytes).unwrap();
        let events = parse_sse_events(&rendered);
        let event_names: Vec<&str> = events.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(
            event_names,
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_delta",
                "content_block_stop",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );

        assert_eq!(events[1].1["index"], 0);
        assert_eq!(events[1].1["content_block"]["type"], "thinking");
        assert_eq!(events[2].1["delta"]["type"], "thinking_delta");
        assert_eq!(events[3].1["delta"]["type"], "signature_delta");
        assert_eq!(events[3].1["delta"]["signature"], "cursor-proxy");
        assert_eq!(events[4].1["index"], 0);

        assert_eq!(events[5].1["index"], 1);
        assert_eq!(events[5].1["content_block"]["type"], "text");
        assert_eq!(events[6].1["delta"]["type"], "text_delta");
        assert_eq!(events[7].1["index"], 1);

        assert_eq!(events[8].1["index"], 2);
        assert_eq!(events[8].1["content_block"]["type"], "tool_use");
        assert_eq!(events[9].1["delta"]["type"], "input_json_delta");
        assert_eq!(events[10].1["index"], 2);
        assert_eq!(events[11].1["delta"]["stop_reason"], "tool_use");
        // Cursor totals are split: uncached = input - cache_read - cache_write.
        assert_eq!(events[11].1["usage"]["input_tokens"], 19);
        assert_eq!(events[11].1["usage"]["output_tokens"], 9);
        assert_eq!(events[11].1["usage"]["cache_creation_input_tokens"], 5);
        assert_eq!(events[11].1["usage"]["cache_read_input_tokens"], 7);

        assert_eq!(
            events
                .iter()
                .filter(|(name, _)| name == EVENT_MESSAGE_START)
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|(name, _)| name == EVENT_MESSAGE_DELTA)
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|(name, _)| name == EVENT_MESSAGE_STOP)
                .count(),
            1
        );
    }

    #[test]
    fn incremental_encoder_emits_multiple_tool_blocks_before_one_pause() {
        let mut encoder = CursorSseEncoder::new("msg_batch", "cursor-test");
        encoder.begin();
        let mut bytes = encoder.take_bytes();
        encoder.emit_tool_batch([
            ("tool_1", "Read", r#"{"file_path":"/one"}"#),
            ("tool_2", "Read", r#"{"file_path":"/two"}"#),
        ]);
        bytes.extend_from_slice(&encoder.take_bytes());

        let events = parse_sse_events(&String::from_utf8(bytes).unwrap());
        let tool_starts: Vec<&serde_json::Value> = events
            .iter()
            .filter_map(|(name, data)| {
                (name == EVENT_CONTENT_BLOCK_START && data["content_block"]["type"] == "tool_use")
                    .then_some(data)
            })
            .collect();
        assert_eq!(tool_starts.len(), 2);
        assert_eq!(tool_starts[0]["content_block"]["id"], "tool_1");
        assert_eq!(tool_starts[1]["content_block"]["id"], "tool_2");
        assert_eq!(tool_starts[0]["index"], 0);
        assert_eq!(tool_starts[1]["index"], 1);
        assert_eq!(
            events
                .iter()
                .filter(|(name, _)| name == EVENT_MESSAGE_DELTA)
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|(name, _)| name == EVENT_MESSAGE_STOP)
                .count(),
            1
        );
        assert!(encoder.is_finalized());
    }

    #[test]
    fn incremental_encoder_ignores_every_event_after_finalization() {
        let mut encoder = CursorSseEncoder::new("msg_final", "cursor-test");
        encoder.begin();
        encoder.take_bytes();
        encoder.push_event(&CursorStreamEvent::TextDelta {
            text: "done".to_string(),
        });
        encoder.take_bytes();
        encoder.push_event(&CursorStreamEvent::End);

        let final_events = parse_sse_events(&String::from_utf8(encoder.take_bytes()).unwrap());
        assert_eq!(
            final_events
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            vec!["content_block_stop", "message_delta", "message_stop"]
        );
        assert!(encoder.is_finalized());

        let late_events = [
            CursorStreamEvent::ThinkingDelta {
                text: "late thinking".to_string(),
            },
            CursorStreamEvent::TextDelta {
                text: "late text".to_string(),
            },
            CursorStreamEvent::Usage {
                input_tokens: 100,
                output_tokens: 100,
                cache_read_tokens: 100,
                cache_write_tokens: 100,
            },
            CursorStreamEvent::NativeTool {
                tool_use_id: "late_tool".to_string(),
                name: "Bash".to_string(),
                input: serde_json::json!({"command": "echo late"}),
            },
            CursorStreamEvent::End,
        ];
        for event in &late_events {
            encoder.push_event(event);
        }
        encoder.begin();
        encoder.finalize();
        encoder.emit_thinking_delta("late direct thinking");
        encoder.emit_text_delta("late direct text");
        encoder.record_usage(1, 2, 3, 4);
        encoder.emit_tool_pause("late_direct", "Read", "{}");
        assert!(encoder.take_bytes().is_empty());
    }

    // -----------------------------------------------------------------------
    // SSE parser helper for tests
    // -----------------------------------------------------------------------

    pub fn parse_sse_events(sse: &str) -> Vec<(String, serde_json::Value)> {
        let mut events = Vec::new();
        let mut current_event = String::new();

        for line in sse.lines() {
            if let Some(event) = line.strip_prefix("event: ") {
                current_event = event.to_string();
            } else if let Some(data_str) = line.strip_prefix("data: ") {
                if let Ok(data) = serde_json::from_str::<serde_json::Value>(data_str) {
                    events.push((current_event.clone(), data));
                }
            }
        }

        events
    }
}
