use std::time::{Duration, Instant};

use crate::providers::cursor::client::CursorUpstreamResponse;
use crate::providers::cursor::connect::anthropic_error_type_from_live_error;
use crate::providers::cursor::native_tools::adapt_tool_input_for_client;
use crate::providers::cursor::response::{
    CursorStreamEvent, decode_upstream_response, decode_upstream_response_with_allowed,
};
use crate::providers::cursor::tool_bridge::resolve_advertised_name;
use std::collections::BTreeSet;

/// SSE event name constants.
pub const EVENT_MESSAGE_START: &str = "message_start";
pub const EVENT_CONTENT_BLOCK_START: &str = "content_block_start";
pub const EVENT_CONTENT_BLOCK_DELTA: &str = "content_block_delta";
pub const EVENT_CONTENT_BLOCK_STOP: &str = "content_block_stop";
pub const EVENT_MESSAGE_DELTA: &str = "message_delta";
pub const EVENT_MESSAGE_STOP: &str = "message_stop";
pub const EVENT_PING: &str = "ping";
pub const EVENT_ERROR: &str = "error";

/// Claude Code statusline In/Out/Cached/Ctx only advance from Anthropic usage
/// fields on `message_start` / `message_delta` — not from thinking/text deltas.
/// Throttle mid-stream `message_delta` so Out updates live without flooding SSE.
///
/// Do **not** emit those progress deltas while a thinking block is open (or
/// before the first `text_delta`). Claude Code 2.1.193 `pEo`/`s8a` treats ANY
/// `message_delta` with `usage.output_tokens` as `{type:"end"}`, which freezes
/// the thinking OTPS meter. Live thinking progress comes from `thinking_delta`
/// (`ceil(len/4)` while `outputTokens` is still null).
const USAGE_PROGRESS_MIN_INTERVAL: Duration = Duration::from_millis(200);
const USAGE_PROGRESS_MIN_OUTPUT_DELTA: u64 = 8;

// Some Fable runs serialize one leading private-reasoning block through the
// text field as the exact protocol wrapper `<thinking>...</thinking>`.  Treat
// only that narrow shape as protocol.  Searching for tags throughout an answer
// corrupts ordinary XML/code examples, and accepting aliases, attributes, or
// case variants creates the same ambiguity.  Non-Fable models bypass this
// classifier entirely.
const FABLE_THINKING_OPEN: &str = "<thinking>";
const FABLE_THINKING_CLOSE: &str = "</thinking>";
const FABLE_THINKING_LEADING_WS_MAX_BYTES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeadingThinkingArtifactState {
    /// Model/operation is not eligible for protocol-artifact recognition.
    Disabled,
    /// No visible output has been committed; inspect only the leading bytes.
    Candidate,
    /// The exact leading opening wrapper was confirmed.
    Body,
    /// The response was classified as ordinary text, or the one artifact ended.
    Passthrough,
}

fn fable_protocol_artifact_candidate(model: &str) -> bool {
    model.to_ascii_lowercase().contains("fable")
}

/// Bytes at the end of `value` that may be the beginning of `marker`.
/// `marker` is ASCII, so a positive result is always a UTF-8 boundary.
fn trailing_marker_prefix_len(value: &str, marker: &str) -> usize {
    let value = value.as_bytes();
    let marker = marker.as_bytes();
    let max = value.len().min(marker.len().saturating_sub(1));
    (1..=max)
        .rev()
        .find(|&len| value.ends_with(&marker[..len]))
        .unwrap_or(0)
}

/// Frame upstream Cursor response bytes into Anthropic SSE event bytes.
///
/// Produces the standard message lifecycle:
/// 1. message_start (with initial/estimated input + cache usage)
/// 2. content_block_start (text)
/// 3. content_block_delta (text deltas) / content_block_delta (thinking deltas)
/// 4. mid-stream message_delta after first text_delta (stop_reason null)
/// 5. content_block_stop
/// 6. message_delta (final usage and stop_reason)
/// 7. message_stop
pub fn frame_cursor_stream(
    upstream: &CursorUpstreamResponse,
    message_id: &str,
    model: &str,
) -> Vec<u8> {
    frame_cursor_stream_with_allowed(upstream, message_id, model, None)
}

/// Frame a Cursor response as Anthropic SSE while filtering native tool calls
/// against the downstream request's advertised tool set.
///
/// `None` preserves the historical unfiltered behavior of
/// [`frame_cursor_stream`]. Request handlers should pass `Some(&allowed)`;
/// passing an empty set suppresses every native tool call.
pub fn frame_cursor_stream_with_allowed(
    upstream: &CursorUpstreamResponse,
    message_id: &str,
    model: &str,
    allowed_tool_names: Option<&BTreeSet<String>>,
) -> Vec<u8> {
    frame_cursor_stream_with_allowed_mode(upstream, message_id, model, allowed_tool_names, false)
}

/// Frame a Cursor compaction response as Anthropic SSE.
///
/// Grok Build's Responses compaction collector only accepts output text. Real
/// `text_delta` content is authoritative; reasoning is promoted only when the
/// compaction stream ends without any text summary.
pub fn frame_cursor_stream_compaction(
    upstream: &CursorUpstreamResponse,
    message_id: &str,
    model: &str,
) -> Vec<u8> {
    let empty = BTreeSet::new();
    frame_cursor_stream_with_allowed_mode(upstream, message_id, model, Some(&empty), true)
}

fn frame_cursor_stream_with_allowed_mode(
    upstream: &CursorUpstreamResponse,
    message_id: &str,
    model: &str,
    allowed_tool_names: Option<&BTreeSet<String>>,
    compaction_mode: bool,
) -> Vec<u8> {
    let decoded = match (allowed_tool_names, compaction_mode) {
        (Some(allowed), false) => {
            decode_upstream_response_with_allowed(&upstream.body, Some(allowed))
        }
        _ => decode_upstream_response(&upstream.body),
    };
    let events = match decoded {
        Ok(e) => e,
        Err(e) => {
            return format_sse_error(&e.to_string());
        }
    };

    let mut sse = Vec::new();
    let mut framer = if compaction_mode {
        CursorSseFramer::new_compaction(&mut sse, message_id, model)
    } else {
        CursorSseFramer::new(&mut sse, message_id, model)
    };

    // A buffered Cursor response can contain several native execs in one
    // turn.  Anthropic represents those as sibling `tool_use` blocks followed
    // by a single `message_delta(stop_reason="tool_use")`.  The framer marks
    // itself finalized after `emit_tool_pause`, so queue the translated native
    // calls and emit them as one batch after all preceding text/usage events
    // have been handled.  Without this, only the first PiEdit replacement
    // survives the response and later replacements silently disappear.
    let mut pending_tools: Vec<(String, String, String)> = Vec::new();
    let mut saw_tool = false;

    for event in &events {
        // Once a native tool has appeared, visible text after it belongs to a
        // later Cursor segment and must not be appended after Anthropic's
        // tool-use stop.  Continue scanning only to collect sibling native
        // calls from the same buffered turn.
        if saw_tool
            && !matches!(
                event,
                CursorStreamEvent::NativeTool { .. } | CursorStreamEvent::Usage { .. }
            )
        {
            continue;
        }
        match event {
            CursorStreamEvent::ThinkingDelta { text } => framer.emit_thinking_delta(text),
            CursorStreamEvent::ThinkingSignature { signature } => {
                framer.emit_thinking_signature(signature)
            }
            CursorStreamEvent::ThinkingCompleted => framer.complete_thinking(),
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
                let (name, input) = match allowed_tool_names {
                    Some(allowed) => {
                        let Some(name) = resolve_advertised_name(name, Some(allowed)) else {
                            continue;
                        };
                        let input = adapt_tool_input_for_client(&name, input.clone());
                        (name, input)
                    }
                    None => (name.clone(), input.clone()),
                };
                let input_json = serde_json::to_string(&input).unwrap_or_else(|_| "{}".to_string());
                pending_tools.push((tool_use_id.clone(), name, input_json));
                saw_tool = true;
            }
        }
    }

    if pending_tools.is_empty() {
        framer.finalize();
    } else {
        framer.emit_tool_batch(
            pending_tools
                .iter()
                .map(|(id, name, input)| (id.as_str(), name.as_str(), input.as_str())),
        );
    }
    sse
}

/// Format an SSE error event.
fn format_sse_error(error: &str) -> Vec<u8> {
    let data = serde_json::json!({
        "type": "error",
        "error": {
            "type": anthropic_error_type_from_live_error(error),
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

/// Parse complete Anthropic SSE events from a buffered response. Comments and
/// unknown lines are skipped; each JSON data line is returned with the most
/// recent event name.
pub(crate) fn parse_sse_events(sse: &str) -> Vec<(String, serde_json::Value)> {
    let mut events = Vec::new();
    let mut current_event = String::new();
    for line in sse.lines() {
        if let Some(event) = line.strip_prefix("event: ") {
            current_event = event.to_string();
        } else if let Some(data) = line.strip_prefix("data: ")
            && let Ok(value) = serde_json::from_str::<serde_json::Value>(data)
        {
            events.push((current_event.clone(), value));
        }
    }
    events
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
#[derive(Debug)]
struct CursorSseState {
    started: bool,
    compaction_mode: bool,
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
    /// Last `message_delta` usage progress snapshot (Claude Code Out meter).
    last_progress_input: u64,
    last_progress_output: u64,
    last_progress_cache_read: u64,
    last_progress_cache_write: u64,
    last_progress_at: Option<Instant>,
    /// Mid-stream usage `message_delta` is withheld until the first text delta
    /// so Claude Code's thinking OTPS meter can keep counting `thinking_delta`.
    seen_text_delta: bool,
    /// Classifier for Fable's one known leading text-channel protocol artifact.
    leading_thinking_artifact: LeadingThinkingArtifactState,
    /// Small split-marker buffer, or the currently unflushed artifact body.
    leading_thinking_buffer: String,
    /// Compaction may put its summary in reasoning, but a later real text delta
    /// is authoritative. Hold reasoning until end so mixed streams never expose
    /// private reasoning concatenated with the actual summary.
    compaction_thinking_fallback: String,
    compaction_seen_text: bool,
    finalized: bool,
}

impl Default for CursorSseState {
    fn default() -> Self {
        Self {
            started: false,
            compaction_mode: false,
            thinking_open: false,
            text_open: false,
            next_index: 0,
            thinking_index: -1,
            text_index: -1,
            usage_input_tokens: 0,
            usage_output_tokens: 0,
            usage_output_estimate: 0,
            usage_cache_read_tokens: 0,
            usage_cache_write_tokens: 0,
            last_progress_input: 0,
            last_progress_output: 0,
            last_progress_cache_read: 0,
            last_progress_cache_write: 0,
            last_progress_at: None,
            seen_text_delta: false,
            leading_thinking_artifact: LeadingThinkingArtifactState::Disabled,
            leading_thinking_buffer: String::new(),
            compaction_thinking_fallback: String::new(),
            compaction_seen_text: false,
            finalized: false,
        }
    }
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
                leading_thinking_artifact: if fable_protocol_artifact_candidate(model) {
                    LeadingThinkingArtifactState::Candidate
                } else {
                    LeadingThinkingArtifactState::Disabled
                },
                ..CursorSseState::default()
            },
        }
    }

    fn new_compaction(output: &'a mut Vec<u8>, message_id: &'a str, model: &'a str) -> Self {
        Self {
            output,
            message_id,
            model,
            state: CursorSseState {
                compaction_mode: true,
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

        let input = self.state.usage_input_tokens.max(1);
        let cache_write = self.state.usage_cache_write_tokens;
        let cache_read = self.state.usage_cache_read_tokens;
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
                    "input_tokens": input,
                    "output_tokens": 0,
                    "cache_creation_input_tokens": cache_write,
                    "cache_read_input_tokens": cache_read
                }
            }
        });
        write_sse_event(self.output, EVENT_MESSAGE_START, &data);
        // Seed progress watermark so the first mid-stream message_delta only
        // fires once Out (or input/cache) actually moves.
        self.state.last_progress_input = input;
        self.state.last_progress_output = 0;
        self.state.last_progress_cache_read = cache_read;
        self.state.last_progress_cache_write = cache_write;
        self.state.last_progress_at = Some(Instant::now());
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

    /// Only upstream signatures may be replayed in a later model request.
    /// Unsigned CLI reasoning remains unsigned rather than receiving a
    /// fabricated signature that the next upstream cannot validate.
    fn close_thinking(&mut self) {
        if !self.state.thinking_open {
            return;
        }

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
        self.flush_deferred_text();
        self.close_thinking();
        self.close_text();
    }

    pub fn emit_thinking_delta(&mut self, text: &str) {
        if self.state.compaction_mode {
            if !self.state.compaction_seen_text && !self.state.finalized {
                self.state.compaction_thinking_fallback.push_str(text);
            }
            return;
        }
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
        // No mid-stream message_delta: Claude Code already maps thinking text →
        // ceil(len/4) while outputTokens is null. Progress resumes on text_delta.
    }

    pub fn emit_thinking_signature(&mut self, signature: &str) {
        if self.state.compaction_mode || signature.is_empty() || !self.open_thinking() {
            return;
        }
        write_content_delta(
            self.output,
            self.state.thinking_index,
            "signature_delta",
            "signature",
            signature,
        );
    }

    pub fn complete_thinking(&mut self) {
        if !self.state.compaction_mode {
            self.close_thinking();
        }
    }

    pub fn emit_text_delta(&mut self, text: &str) {
        if self.state.compaction_mode {
            if text.is_empty() || self.state.finalized {
                return;
            }
            if !self.state.compaction_seen_text {
                self.state.compaction_seen_text = true;
                self.state.compaction_thinking_fallback.clear();
            }
            self.emit_text_delta_raw(text);
            return;
        }
        if text.is_empty() || self.state.finalized {
            return;
        }
        match self.state.leading_thinking_artifact {
            LeadingThinkingArtifactState::Candidate | LeadingThinkingArtifactState::Body => {
                self.state.leading_thinking_buffer.push_str(text);
                self.drain_leading_thinking_artifact(false);
            }
            LeadingThinkingArtifactState::Disabled | LeadingThinkingArtifactState::Passthrough => {
                self.emit_text_delta_raw(text)
            }
        }
    }

    /// Emit text after the literal-thinking protocol filter has classified the
    /// chunk as ordinary visible output.
    fn emit_text_delta_raw(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        if !self.open_text() {
            return;
        }
        self.state.seen_text_delta = true;
        self.note_generated_text(text);
        // Hot path: avoid `json!` + intermediate String for every text chunk.
        write_content_delta(
            self.output,
            self.state.text_index,
            "text_delta",
            "text",
            text,
        );
        self.maybe_emit_usage_progress(false);
    }

    /// Flush deferred protocol classification or the compaction reasoning
    /// fallback before the content blocks close.
    fn flush_deferred_text(&mut self) {
        if self.state.compaction_mode {
            if !self.state.compaction_seen_text
                && !self.state.compaction_thinking_fallback.is_empty()
            {
                let fallback = std::mem::take(&mut self.state.compaction_thinking_fallback);
                self.emit_text_delta_raw(&fallback);
            } else {
                self.state.compaction_thinking_fallback.clear();
            }
            return;
        }
        self.drain_leading_thinking_artifact(true);
    }

    /// Recognize only an exact, leading Fable `<thinking>...</thinking>` block.
    /// Once ordinary text is observed (or the one block closes), all remaining
    /// bytes are passed through verbatim, including later XML examples.
    fn drain_leading_thinking_artifact(&mut self, flush: bool) {
        loop {
            match self.state.leading_thinking_artifact {
                LeadingThinkingArtifactState::Disabled
                | LeadingThinkingArtifactState::Passthrough => {
                    if !self.state.leading_thinking_buffer.is_empty() {
                        let visible = std::mem::take(&mut self.state.leading_thinking_buffer);
                        self.emit_text_delta_raw(&visible);
                    }
                    return;
                }
                LeadingThinkingArtifactState::Candidate => {
                    if self.state.leading_thinking_buffer.is_empty() {
                        return;
                    }
                    let whitespace_len = self
                        .state
                        .leading_thinking_buffer
                        .char_indices()
                        .find_map(|(at, ch)| (!ch.is_ascii_whitespace()).then_some(at))
                        .unwrap_or(self.state.leading_thinking_buffer.len());
                    if whitespace_len > FABLE_THINKING_LEADING_WS_MAX_BYTES {
                        self.state.leading_thinking_artifact =
                            LeadingThinkingArtifactState::Passthrough;
                        continue;
                    }
                    let candidate = &self.state.leading_thinking_buffer[whitespace_len..];
                    if candidate.starts_with(FABLE_THINKING_OPEN) {
                        self.state
                            .leading_thinking_buffer
                            .drain(..whitespace_len + FABLE_THINKING_OPEN.len());
                        self.state.leading_thinking_artifact = LeadingThinkingArtifactState::Body;
                        continue;
                    }
                    if !flush && FABLE_THINKING_OPEN.starts_with(candidate) {
                        return;
                    }
                    self.state.leading_thinking_artifact =
                        LeadingThinkingArtifactState::Passthrough;
                }
                LeadingThinkingArtifactState::Body => {
                    if let Some(close_at) = self
                        .state
                        .leading_thinking_buffer
                        .find(FABLE_THINKING_CLOSE)
                    {
                        if close_at > 0 {
                            let reasoning =
                                self.state.leading_thinking_buffer[..close_at].to_owned();
                            self.emit_thinking_delta(&reasoning);
                        }
                        self.state
                            .leading_thinking_buffer
                            .drain(..close_at + FABLE_THINKING_CLOSE.len());
                        self.state.leading_thinking_artifact =
                            LeadingThinkingArtifactState::Passthrough;
                        continue;
                    }
                    if flush {
                        let reasoning = std::mem::take(&mut self.state.leading_thinking_buffer);
                        self.emit_thinking_delta(&reasoning);
                        return;
                    }
                    let keep = trailing_marker_prefix_len(
                        &self.state.leading_thinking_buffer,
                        FABLE_THINKING_CLOSE,
                    );
                    let emit_len = self.state.leading_thinking_buffer.len() - keep;
                    if emit_len > 0 {
                        let reasoning = self.state.leading_thinking_buffer[..emit_len].to_owned();
                        self.state.leading_thinking_buffer.drain(..emit_len);
                        self.emit_thinking_delta(&reasoning);
                    }
                    return;
                }
            }
        }
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

    fn usage_snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.state.usage_input_tokens.max(1),
            self.resolved_output_tokens(),
            self.state.usage_cache_read_tokens,
            self.state.usage_cache_write_tokens,
        )
    }

    /// Emit a non-final `message_delta` with current usage.
    ///
    /// Claude Code merges `message_delta.usage` into the live message (Out from
    /// `output_tokens`; In/Cached when those fields are > 0). `stop_reason` stays
    /// null so the stream remains open. Content deltas alone never move the
    /// statusline meters. Withheld during thinking: `pEo` would treat
    /// `usage.output_tokens` as thinking-meter end.
    fn maybe_emit_usage_progress(&mut self, force: bool) {
        if self.state.finalized || !self.state.started {
            return;
        }
        if self.state.thinking_open || !self.state.seen_text_delta {
            return;
        }
        let (input, output, cache_read, cache_write) = self.usage_snapshot();
        let input_changed = input != self.state.last_progress_input;
        let cache_changed = cache_read != self.state.last_progress_cache_read
            || cache_write != self.state.last_progress_cache_write;
        let output_delta = output.saturating_sub(self.state.last_progress_output);
        let output_changed = output_delta > 0;
        if !input_changed && !cache_changed && !output_changed {
            return;
        }
        if !force {
            let first_output = output_changed && self.state.last_progress_output == 0;
            let significant = input_changed
                || cache_changed
                || output_delta >= USAGE_PROGRESS_MIN_OUTPUT_DELTA
                || first_output;
            if !significant {
                return;
            }
            // Never delay the first Out>0 update after text has started —
            // Claude Code statusline stays at Out:0 until it sees
            // message_delta.usage.output_tokens.
            if !first_output
                && let Some(last) = self.state.last_progress_at
                && last.elapsed() < USAGE_PROGRESS_MIN_INTERVAL
                && !input_changed
                && !cache_changed
            {
                return;
            }
        }

        let data = serde_json::json!({
            "type": "message_delta",
            "delta": {
                "stop_reason": null,
                "stop_sequence": null
            },
            "usage": {
                "input_tokens": input,
                "output_tokens": output,
                "cache_creation_input_tokens": cache_write,
                "cache_read_input_tokens": cache_read
            }
        });
        write_sse_event(self.output, EVENT_MESSAGE_DELTA, &data);
        self.state.last_progress_input = input;
        self.state.last_progress_output = output;
        self.state.last_progress_cache_read = cache_read;
        self.state.last_progress_cache_write = cache_write;
        self.state.last_progress_at = Some(Instant::now());
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
        // Authoritative Cursor snapshot → push into Anthropic SSE immediately
        // once message_start has been sent (statusline In/Cached/Out).
        self.maybe_emit_usage_progress(true);
    }

    /// Accumulate incremental output/thinking tokens without clearing input/cache.
    pub fn add_output_tokens(&mut self, tokens: u64) {
        if self.state.finalized || tokens == 0 {
            return;
        }
        self.state.usage_output_tokens = self.state.usage_output_tokens.saturating_add(tokens);
        self.maybe_emit_usage_progress(false);
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

    /// Emit several sibling tool-use blocks and terminate the message once.
    /// Buffered responses can carry multiple native execs in one turn; using
    /// this helper keeps every block instead of finalizing after the first.
    pub fn emit_tool_batch<'b, I>(&mut self, tools: I)
    where
        I: IntoIterator<Item = (&'b str, &'b str, &'b str)>,
    {
        if self.state.finalized {
            return;
        }
        let mut emitted = false;
        for (tool_use_id, tool_name, partial_json) in tools {
            self.emit_tool_use_block(tool_use_id, tool_name, partial_json);
            emitted = true;
        }
        if emitted {
            self.emit_final_message("tool_use");
        }
    }

    pub fn emit_final_message(&mut self, stop_reason: &str) {
        if self.state.finalized {
            return;
        }
        self.ensure_start();
        self.close_open_blocks();

        let (input, output, cache_read, cache_write) = self.usage_snapshot();
        // message_delta
        let data = serde_json::json!({
            "type": "message_delta",
            "delta": {
                "stop_reason": stop_reason,
                "stop_sequence": null
            },
            "usage": {
                "input_tokens": input,
                "output_tokens": output,
                "cache_creation_input_tokens": cache_write,
                "cache_read_input_tokens": cache_read
            }
        });
        write_sse_event(self.output, EVENT_MESSAGE_DELTA, &data);
        self.state.last_progress_input = input;
        self.state.last_progress_output = output;
        self.state.last_progress_cache_read = cache_read;
        self.state.last_progress_cache_write = cache_write;
        self.state.last_progress_at = Some(Instant::now());

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
        let model = model.into();
        Self {
            output: Vec::new(),
            message_id: message_id.into(),
            model: model.clone(),
            state: CursorSseState {
                thinking_index: -1,
                text_index: -1,
                leading_thinking_artifact: if fable_protocol_artifact_candidate(&model) {
                    LeadingThinkingArtifactState::Candidate
                } else {
                    LeadingThinkingArtifactState::Disabled
                },
                ..CursorSseState::default()
            },
        }
    }

    /// Construct an encoder for a Responses context-compaction operation.
    /// Actual text wins; a reasoning-only stream is surfaced as text at end.
    pub fn new_compaction(message_id: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            output: Vec::new(),
            message_id: message_id.into(),
            model: model.into(),
            state: CursorSseState {
                compaction_mode: true,
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
            CursorStreamEvent::ThinkingSignature { signature } => {
                framer.emit_thinking_signature(signature)
            }
            CursorStreamEvent::ThinkingCompleted => framer.complete_thinking(),
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

    pub fn emit_thinking_signature(&mut self, signature: &str) {
        self.with_framer(|framer| framer.emit_thinking_signature(signature));
    }

    pub fn complete_thinking(&mut self) {
        self.with_framer(|framer| framer.complete_thinking());
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

        assert_eq!(event_names.first().copied(), Some("message_start"));
        assert!(event_names.contains(&"content_block_start"));
        assert!(event_names.contains(&"content_block_delta"));
        assert!(event_names.contains(&"content_block_stop"));
        assert!(event_names.contains(&"message_delta"));
        assert_eq!(event_names.last().copied(), Some("message_stop"));
        // Mid-stream progress may insert extra message_delta before the final one.
        assert!(
            event_names
                .iter()
                .filter(|n| **n == "message_delta")
                .count()
                >= 1
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
    fn thinking_signature_preserves_full_upstream_value_and_block_boundaries() {
        let signature = "upstream-signed-data/+=\"".repeat(4096);
        let mut encoder = CursorSseEncoder::new("msg_signed", "claude-fable-5-1-thinking-max");
        let mut bytes = Vec::new();
        for event in [
            CursorStreamEvent::ThinkingDelta {
                text: "first".into(),
            },
            CursorStreamEvent::ThinkingSignature {
                signature: signature.clone(),
            },
            CursorStreamEvent::ThinkingCompleted,
            CursorStreamEvent::ThinkingSignature {
                signature: "second-signature".into(),
            },
            CursorStreamEvent::ThinkingCompleted,
            CursorStreamEvent::TextDelta {
                text: "answer".into(),
            },
            CursorStreamEvent::End,
        ] {
            encoder.push_event(&event);
            bytes.extend_from_slice(&encoder.take_bytes());
        }
        let events = parse_sse_events(&String::from_utf8(bytes).unwrap());
        let signatures: Vec<_> = events
            .iter()
            .filter_map(|(_, data)| {
                (data["delta"]["type"] == "signature_delta")
                    .then_some((data["index"].clone(), data["delta"]["signature"].clone()))
            })
            .collect();
        assert_eq!(
            signatures,
            vec![
                (serde_json::json!(0), serde_json::json!(signature)),
                (serde_json::json!(1), serde_json::json!("second-signature")),
            ]
        );
        let stops: Vec<_> = events
            .iter()
            .filter_map(|(name, data)| {
                (name == EVENT_CONTENT_BLOCK_STOP).then_some(data["index"].clone())
            })
            .collect();
        assert_eq!(
            stops,
            vec![
                serde_json::json!(0),
                serde_json::json!(1),
                serde_json::json!(2)
            ]
        );
        for (at, (_, data)) in events.iter().enumerate() {
            if data["delta"]["type"] == "signature_delta" {
                assert_eq!(events[at + 1].0, EVENT_CONTENT_BLOCK_STOP);
                assert_eq!(events[at + 1].1["index"], data["index"]);
            }
        }
        assert_eq!(events.last().unwrap().0, EVENT_MESSAGE_STOP);
    }

    #[test]
    fn thinking_signature_is_not_fabricated_for_unsigned_reasoning() {
        let mut encoder = CursorSseEncoder::new("msg_unsigned", "cursor-test");
        encoder.emit_thinking_delta("unsigned thought");
        encoder.complete_thinking();
        encoder.emit_text_delta("answer");
        encoder.finalize();
        let events = parse_sse_events(&String::from_utf8(encoder.take_bytes()).unwrap());
        assert!(
            events
                .iter()
                .all(|(_, data)| data["delta"]["type"] != "signature_delta")
        );
        assert!(
            events
                .iter()
                .any(|(name, data)| name == EVENT_CONTENT_BLOCK_STOP && data["index"] == 0)
        );
    }

    #[test]
    fn thinking_signature_does_not_change_compaction_output() {
        let mut encoder = CursorSseEncoder::new_compaction("msg_compact_signed", "cursor-test");
        encoder.emit_thinking_signature("opaque-signature");
        encoder.complete_thinking();
        encoder.emit_text_delta("summary");
        encoder.finalize();
        let bytes = encoder.take_bytes();
        assert_eq!(rendered_channels(&bytes), ("summary".into(), String::new()));
        assert!(
            !String::from_utf8(bytes)
                .unwrap()
                .contains("opaque-signature")
        );
    }

    #[test]
    fn compaction_sse_promotes_thinking_to_output_text() {
        let mut body = test_frames::thinking_frame("summary from reasoning");
        body.extend_from_slice(&test_frames::end_frame());
        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };

        let sse = frame_cursor_stream_compaction(&upstream, "msg_compact", "claude-fable-5");
        let events = parse_sse_events(&String::from_utf8_lossy(&sse));
        assert!(
            events.iter().any(|(_, data)| {
                data.get("delta")
                    .and_then(|delta| delta.get("type"))
                    .and_then(|value| value.as_str())
                    == Some("text_delta")
                    && data["delta"]["text"] == "summary from reasoning"
            }),
            "compaction summaries must be visible as text deltas: {events:?}"
        );
        assert!(
            events.iter().all(|(_, data)| {
                data.get("delta").and_then(|delta| delta.get("type"))
                    != Some(&serde_json::json!("thinking_delta"))
            }),
            "compaction output must not be exposed on the thinking channel: {events:?}"
        );
    }

    #[test]
    fn buffered_compaction_sse_prefers_real_text_over_reasoning() {
        let mut body = test_frames::thinking_frame("private reasoning");
        body.extend_from_slice(&test_frames::text_frame("actual summary"));
        body.extend_from_slice(&test_frames::thinking_frame("later private reasoning"));
        body.extend_from_slice(&test_frames::end_frame());
        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };

        let sse = frame_cursor_stream_compaction(&upstream, "msg_compact_mixed", "grok-build");
        let wire = String::from_utf8_lossy(&sse);
        assert_eq!(rendered_channels(&sse).0, "actual summary");
        assert!(!wire.contains("private reasoning"), "{wire}");
    }

    fn rendered_channels(bytes: &[u8]) -> (String, String) {
        let wire = String::from_utf8_lossy(bytes);
        let events = parse_sse_events(&wire);
        let visible = events
            .iter()
            .filter_map(|(_, data)| {
                (data["delta"]["type"] == "text_delta")
                    .then(|| data["delta"]["text"].as_str())
                    .flatten()
            })
            .collect::<String>();
        let reasoning = events
            .iter()
            .filter_map(|(_, data)| {
                (data["delta"]["type"] == "thinking_delta")
                    .then(|| data["delta"]["thinking"].as_str())
                    .flatten()
            })
            .collect::<String>();
        (visible, reasoning)
    }

    #[test]
    fn compaction_encoder_uses_thinking_only_as_end_of_stream_fallback() {
        let mut encoder = CursorSseEncoder::new_compaction("msg_compact_encoder", "claude-fable-5");
        encoder.begin();
        let _ = encoder.take_bytes();
        encoder.push_event(&CursorStreamEvent::ThinkingDelta {
            text: "summary from reasoning".into(),
        });
        assert!(
            encoder.take_bytes().is_empty(),
            "reasoning must remain retractable until real text or stream end"
        );
        encoder.push_event(&CursorStreamEvent::End);
        let bytes = encoder.take_bytes();
        assert_eq!(
            rendered_channels(&bytes),
            ("summary from reasoning".into(), "".into())
        );
    }

    #[test]
    fn compaction_real_text_replaces_buffered_reasoning_in_mixed_stream() {
        let mut encoder = CursorSseEncoder::new_compaction("msg_compact_mixed", "claude-fable-5");
        encoder.push_event(&CursorStreamEvent::ThinkingDelta {
            text: "private chain of thought".into(),
        });
        assert!(encoder.take_bytes().is_empty());
        encoder.push_event(&CursorStreamEvent::TextDelta {
            text: "actual ".into(),
        });
        encoder.push_event(&CursorStreamEvent::ThinkingDelta {
            text: "later private reasoning".into(),
        });
        encoder.push_event(&CursorStreamEvent::TextDelta {
            text: "summary".into(),
        });
        encoder.push_event(&CursorStreamEvent::End);

        let bytes = encoder.take_bytes();
        let wire = String::from_utf8_lossy(&bytes);
        assert!(!wire.contains("private chain of thought"), "{wire}");
        assert!(!wire.contains("later private reasoning"), "{wire}");
        assert_eq!(
            rendered_channels(&bytes),
            ("actual summary".into(), "".into())
        );
    }

    #[test]
    fn compaction_real_text_preserves_literal_xml() {
        let mut encoder = CursorSseEncoder::new_compaction("msg_compact_xml", "claude-fable-5");
        encoder.push_event(&CursorStreamEvent::ThinkingDelta {
            text: "discard me".into(),
        });
        encoder.push_event(&CursorStreamEvent::TextDelta {
            text: "<thinking>quoted markup</thinking>".into(),
        });
        encoder.push_event(&CursorStreamEvent::End);
        assert_eq!(
            rendered_channels(&encoder.take_bytes()).0,
            "<thinking>quoted markup</thinking>"
        );
    }

    #[test]
    fn fable_filters_only_one_exact_leading_split_protocol_block() {
        let mut encoder = CursorSseEncoder::new("msg_leading_artifact", "claude-fable-5");
        for text in [
            " \n<thin",
            "king>private ",
            "reasoning</think",
            "ing>\nanswer <thinking>visible example</thinking>",
        ] {
            encoder.push_event(&CursorStreamEvent::TextDelta { text: text.into() });
        }
        encoder.push_event(&CursorStreamEvent::End);

        assert_eq!(
            rendered_channels(&encoder.take_bytes()),
            (
                "\nanswer <thinking>visible example</thinking>".into(),
                "private reasoning".into(),
            )
        );
    }

    #[test]
    fn fable_reasoning_only_protocol_block_does_not_leak_xml() {
        let mut encoder = CursorSseEncoder::new("msg_artifact_only", "claude-fable-5");
        encoder.push_event(&CursorStreamEvent::TextDelta {
            text: "<thinking>private only</thinking>".into(),
        });
        encoder.push_event(&CursorStreamEvent::End);
        assert_eq!(
            rendered_channels(&encoder.take_bytes()),
            ("".into(), "private only".into())
        );
    }

    #[test]
    fn fable_preserves_embedded_and_code_fenced_xml_examples() {
        for (id, text) in [
            ("embedded", "visible <thinking>quoted</thinking> tail"),
            ("fenced", "```xml\n<thinking>quoted</thinking>\n```"),
        ] {
            let mut encoder = CursorSseEncoder::new(id, "claude-fable-5");
            encoder.push_event(&CursorStreamEvent::TextDelta { text: text.into() });
            encoder.push_event(&CursorStreamEvent::End);
            assert_eq!(rendered_channels(&encoder.take_bytes()).0, text, "{id}");
        }
    }

    #[test]
    fn non_fable_preserves_even_exact_leading_thinking_xml() {
        let text = "<thinking>ordinary Gemini XML</thinking>";
        let mut encoder = CursorSseEncoder::new("msg_gemini_xml", "gemini-3.6-flash");
        encoder.push_event(&CursorStreamEvent::TextDelta { text: text.into() });
        encoder.push_event(&CursorStreamEvent::End);
        assert_eq!(rendered_channels(&encoder.take_bytes()).0, text);
    }

    #[test]
    fn fable_preserves_lookalike_or_incomplete_thinking_markup() {
        for (id, text) in [
            ("case", "<THINKING>quoted</THINKING>"),
            ("alias", "<think>quoted</think>"),
            ("attribute", "<thinking kind=\"example\">quoted</thinking>"),
            ("partial", "<thi"),
        ] {
            let mut encoder = CursorSseEncoder::new(id, "claude-fable-5");
            encoder.push_event(&CursorStreamEvent::TextDelta { text: text.into() });
            encoder.push_event(&CursorStreamEvent::End);
            assert_eq!(rendered_channels(&encoder.take_bytes()).0, text, "{id}");
        }
    }

    #[test]
    fn confirmed_fable_protocol_body_streams_with_bounded_marker_tail() {
        let mut encoder = CursorSseEncoder::new("msg_artifact_bounded", "claude-fable-5");
        encoder.push_event(&CursorStreamEvent::TextDelta {
            text: "<thinking>".into(),
        });
        encoder.push_event(&CursorStreamEvent::TextDelta {
            text: "private".repeat(20_000),
        });
        assert!(encoder.state.leading_thinking_buffer.len() < FABLE_THINKING_CLOSE.len());
        let partial = encoder.take_bytes();
        assert!(rendered_channels(&partial).1.contains("private"));

        encoder.push_event(&CursorStreamEvent::TextDelta {
            text: "</thinking>done".into(),
        });
        encoder.push_event(&CursorStreamEvent::End);
        assert_eq!(rendered_channels(&encoder.take_bytes()).0, "done");
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
    fn sse_error_preserves_rate_limit_type() {
        let sse = format_sse_error("Connect error 429: quota [resource_exhausted]");
        let sse_str = String::from_utf8_lossy(&sse);
        let events = parse_sse_events(&sse_str);
        let (_, data) = &events[0];
        assert_eq!(data["error"]["type"], "rate_limit_error");
        assert_eq!(
            data["error"]["message"],
            "Connect error 429: quota [resource_exhausted]"
        );
    }

    #[test]
    fn filtered_sse_does_not_emit_unadvertised_native_tool() {
        use crate::providers::cursor::proto::{
            AgentServerMessage, ExecReadArgs, ExecServerMessage,
        };
        use crate::providers::cursor::test_frames;
        use prost::Message;
        use std::collections::BTreeSet;

        let msg = AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: None,
            kv_server_message: None,
            interaction_query: None,
            exec_server_control_message: None,
            exec_server_message: Some(ExecServerMessage {
                id: 8,
                exec_id: Some("read-8".into()),
                shell_args: None,
                write_args: None,
                delete_args: None,
                grep_args: None,
                read_args: Some(ExecReadArgs {
                    path: "/tmp/example.txt".into(),
                    tool_call_id: "read-8".into(),
                    offset: None,
                    limit: None,
                }),
                ls_args: None,
                request_context_args: None,
                shell_stream_args: None,
                pi_write_args: None,
                pi_edit_args: None,
            }),

            ..Default::default()
        };
        let mut payload = Vec::new();
        msg.encode(&mut payload).unwrap();
        let mut body =
            crate::providers::cursor::connect::encode_connect_frame(&payload, 0).to_vec();
        body.extend_from_slice(&test_frames::end_frame());
        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };

        let empty = BTreeSet::new();
        let sse = frame_cursor_stream_with_allowed(
            &upstream,
            "msg_no_tools",
            "cursor-test",
            Some(&empty),
        );
        let events = parse_sse_events(&String::from_utf8_lossy(&sse));
        assert!(
            events
                .iter()
                .all(|(_, data)| data["type"] != "content_block_start"
                    || data["content_block"]["type"] != "tool_use")
        );
    }

    #[test]
    fn buffered_pi_edit_sse_emits_modern_single_replacement_blocks() {
        use crate::providers::cursor::proto::{
            AgentServerMessage, ExecServerMessage, PiEditExecArgs, PiEditReplacement,
        };
        use crate::providers::cursor::test_frames;
        use prost::Message;
        use std::collections::BTreeSet;

        let msg = AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: None,
            kv_server_message: None,
            interaction_query: None,
            exec_server_control_message: None,
            exec_server_message: Some(ExecServerMessage {
                id: 18,
                exec_id: Some("pi-sse-18".into()),
                shell_args: None,
                write_args: None,
                delete_args: None,
                grep_args: None,
                read_args: None,
                ls_args: None,
                request_context_args: None,
                shell_stream_args: None,
                pi_write_args: None,
                pi_edit_args: Some(PiEditExecArgs {
                    path: "/tmp/example.rs".into(),
                    edits: vec![
                        PiEditReplacement {
                            old_text: "a".into(),
                            new_text: "b".into(),
                        },
                        PiEditReplacement {
                            old_text: "c".into(),
                            new_text: "d".into(),
                        },
                    ],
                }),
            }),

            ..Default::default()
        };
        let mut payload = Vec::new();
        msg.encode(&mut payload).unwrap();
        let mut body =
            crate::providers::cursor::connect::encode_connect_frame(&payload, 0).to_vec();
        body.extend_from_slice(&test_frames::end_frame());
        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };
        let allowed = BTreeSet::from(["str_replace_based_edit_tool".to_string()]);
        let rendered = frame_cursor_stream_with_allowed(
            &upstream,
            "msg_pi_sse",
            "claude-fable-5",
            Some(&allowed),
        );
        let events = parse_sse_events(&String::from_utf8(rendered).unwrap());
        let tools: Vec<_> = events
            .iter()
            .filter(|(_, data)| {
                data["type"] == "content_block_start" && data["content_block"]["type"] == "tool_use"
            })
            .collect();
        assert_eq!(tools.len(), 2);
        assert_eq!(
            tools[0].1["content_block"]["name"],
            "str_replace_based_edit_tool"
        );
        assert_eq!(tools[1].1["content_block"]["id"], "pi-sse-18__part_2");
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
        assert_eq!(event_names.first().copied(), Some("message_start"));
        assert_eq!(event_names.last().copied(), Some("message_stop"));
        assert!(event_names.contains(&"content_block_start"));
        assert!(event_names.contains(&"content_block_delta"));
        assert!(event_names.contains(&"content_block_stop"));
        assert!(event_names.contains(&"message_delta"));

        let thinking_start = events
            .iter()
            .find(|(_, data)| {
                data.get("content_block").and_then(|c| c.get("type"))
                    == Some(&serde_json::json!("thinking"))
            })
            .expect("thinking block");
        assert_eq!(thinking_start.1["index"], 0);

        let text_start = events
            .iter()
            .find(|(_, data)| {
                data.get("content_block").and_then(|c| c.get("type"))
                    == Some(&serde_json::json!("text"))
            })
            .expect("text block");
        assert_eq!(text_start.1["index"], 1);

        let tool_start = events
            .iter()
            .find(|(_, data)| {
                data.get("content_block").and_then(|c| c.get("type"))
                    == Some(&serde_json::json!("tool_use"))
            })
            .expect("tool_use block");
        assert_eq!(tool_start.1["index"], 2);
        assert_eq!(tool_start.1["content_block"]["id"], "tool_1");

        let final_delta = events
            .iter()
            .rev()
            .find(|(name, data)| {
                name == EVENT_MESSAGE_DELTA && data["delta"]["stop_reason"] == "tool_use"
            })
            .map(|(_, data)| data)
            .expect("final tool_use message_delta");
        // Cursor totals are split: uncached = input - cache_read - cache_write.
        assert_eq!(final_delta["usage"]["input_tokens"], 19);
        assert_eq!(final_delta["usage"]["output_tokens"], 9);
        assert_eq!(final_delta["usage"]["cache_creation_input_tokens"], 5);
        assert_eq!(final_delta["usage"]["cache_read_input_tokens"], 7);

        assert_eq!(
            events
                .iter()
                .filter(|(name, _)| name == EVENT_MESSAGE_START)
                .count(),
            1
        );
        assert!(
            events
                .iter()
                .filter(|(name, _)| name == EVENT_MESSAGE_DELTA)
                .count()
                >= 1
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
        assert!(final_events.iter().any(|(n, _)| n == "message_delta"));
        assert_eq!(
            final_events.last().map(|(n, _)| n.as_str()),
            Some("message_stop")
        );
        assert!(
            final_events
                .iter()
                .any(|(n, d)| n == "message_delta" && d["delta"]["stop_reason"] == "end_turn")
        );
        assert!(encoder.is_finalized());

        let late_events = [
            CursorStreamEvent::ThinkingDelta {
                text: "late thinking".to_string(),
            },
            CursorStreamEvent::ThinkingSignature {
                signature: "late-signature".into(),
            },
            CursorStreamEvent::ThinkingCompleted,
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

    #[test]
    fn message_start_carries_seeded_input_usage() {
        let mut encoder = CursorSseEncoder::new("msg_seed", "cursor-test");
        encoder.seed_estimated_input_tokens(71_700);
        encoder.begin();
        let events = parse_sse_events(&String::from_utf8_lossy(&encoder.take_bytes()));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "message_start");
        assert_eq!(events[0].1["message"]["usage"]["input_tokens"], 71_700);
        assert_eq!(events[0].1["message"]["usage"]["output_tokens"], 0);
    }

    fn is_progress_message_delta(name: &str, data: &serde_json::Value) -> bool {
        name == EVENT_MESSAGE_DELTA && data["delta"]["stop_reason"].is_null()
    }

    #[test]
    fn no_progress_message_delta_during_thinking() {
        let mut encoder = CursorSseEncoder::new("msg_think", "cursor-test");
        encoder.seed_estimated_input_tokens(12_000);
        encoder.begin();
        let _ = encoder.take_bytes();

        // 16 chars → 4 tok estimate. Claude Code 2.1.193 `pEo`/`s8a` treats ANY
        // message_delta with usage.output_tokens as thinking-meter `{type:"end"}`.
        encoder.emit_thinking_delta("abcdefghijklmnop");
        encoder.add_output_tokens(32);
        encoder.record_usage(12_000, 40, 0, 0);
        std::thread::sleep(USAGE_PROGRESS_MIN_INTERVAL + Duration::from_millis(20));
        encoder.emit_thinking_delta("more thinking text here!!");

        let events = parse_sse_events(&String::from_utf8_lossy(&encoder.take_bytes()));
        assert!(
            events
                .iter()
                .any(|(n, d)| n == "content_block_delta" && d["delta"]["type"] == "thinking_delta"),
            "thinking_delta remains the live Claude Code signal"
        );
        assert!(
            events.iter().all(|(n, d)| !is_progress_message_delta(n, d)),
            "mid-stream message_delta during thinking poisons Claude Code OTPS; got {events:?}"
        );
        let (input, output) = encoder.current_usage();
        assert_eq!(input, 12_000);
        assert!(
            output >= 40,
            "proxy TUI Out must keep tracking encoder.current_usage(), got {output}"
        );
    }

    #[test]
    fn thinking_only_turn_still_emits_final_usage() {
        let mut encoder = CursorSseEncoder::new("msg_think_end", "cursor-test");
        encoder.seed_estimated_input_tokens(12_000);
        encoder.begin();
        encoder.emit_thinking_delta("abcdefghijklmnop");
        encoder.push_event(&CursorStreamEvent::End);

        let events = parse_sse_events(&String::from_utf8_lossy(&encoder.take_bytes()));
        assert!(
            events.iter().all(|(n, d)| !is_progress_message_delta(n, d)),
            "thinking-only turn must not emit progress message_delta"
        );
        let final_delta = events
            .iter()
            .rev()
            .find(|(n, d)| n == EVENT_MESSAGE_DELTA && d["delta"]["stop_reason"] == "end_turn")
            .map(|(_, d)| d)
            .expect("final message_delta");
        assert_eq!(final_delta["usage"]["input_tokens"], 12_000);
        assert!(final_delta["usage"]["output_tokens"].as_u64().unwrap_or(0) >= 4);
    }

    #[test]
    fn first_text_delta_may_emit_mid_stream_usage_progress() {
        let mut encoder = CursorSseEncoder::new("msg_text_progress", "cursor-test");
        encoder.seed_estimated_input_tokens(12_000);
        encoder.begin();
        let _ = encoder.take_bytes();

        encoder.emit_thinking_delta("abcdefghijklmnop");
        let think_events = parse_sse_events(&String::from_utf8_lossy(&encoder.take_bytes()));
        assert!(
            think_events
                .iter()
                .all(|(n, d)| !is_progress_message_delta(n, d))
        );

        encoder.emit_text_delta("abcdefghijklmnop"); // 16 chars → first Out after thinking
        let events = parse_sse_events(&String::from_utf8_lossy(&encoder.take_bytes()));
        assert!(
            events
                .iter()
                .any(|(n, d)| n == "content_block_delta" && d["delta"]["type"] == "text_delta")
        );
        let progress = events
            .iter()
            .find(|(n, d)| is_progress_message_delta(n, d))
            .map(|(_, d)| d)
            .expect("first text_delta may start mid-stream usage message_delta");
        assert_eq!(progress["usage"]["input_tokens"], 12_000);
        assert!(progress["usage"]["output_tokens"].as_u64().unwrap_or(0) >= 4);

        std::thread::sleep(USAGE_PROGRESS_MIN_INTERVAL + Duration::from_millis(20));
        encoder.add_output_tokens(32);
        let events = parse_sse_events(&String::from_utf8_lossy(&encoder.take_bytes()));
        let progress = events
            .iter()
            .find(|(n, d)| is_progress_message_delta(n, d))
            .map(|(_, d)| d)
            .expect("token_delta message_delta after text has started");
        assert!(progress["usage"]["output_tokens"].as_u64().unwrap_or(0) >= 32);

        encoder.push_event(&CursorStreamEvent::End);
        let events = parse_sse_events(&String::from_utf8_lossy(&encoder.take_bytes()));
        let final_delta = events
            .iter()
            .rev()
            .find(|(n, d)| n == EVENT_MESSAGE_DELTA && d["delta"]["stop_reason"] == "end_turn")
            .map(|(_, d)| d)
            .expect("final message_delta");
        assert_eq!(final_delta["usage"]["input_tokens"], 12_000);
        assert!(final_delta["usage"]["output_tokens"].as_u64().unwrap_or(0) >= 32);
    }

    #[test]
    fn content_delta_encode_is_fast_enough_for_streaming() {
        // Mental budget: ~11 tok/s would be ~90ms/token. Encoding a short
        // Anthropic content_block_delta must be orders of magnitude cheaper.
        let mut encoder = CursorSseEncoder::new("msg_bench", "cursor-test");
        encoder.begin();
        let _ = encoder.take_bytes();
        let started = std::time::Instant::now();
        for i in 0..2_000 {
            encoder.emit_text_delta(&format!("t{i}"));
            let _ = encoder.take_bytes();
        }
        let elapsed = started.elapsed();
        let per = elapsed / 2_000;
        assert!(
            per.as_micros() < 500,
            "SSE text delta encode too slow: {per:?}/delta ({elapsed:?} for 2000)"
        );
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
