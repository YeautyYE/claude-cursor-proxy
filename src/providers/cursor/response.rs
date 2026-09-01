use crate::anthropic::schema::MessagesRequest;
use crate::providers::cursor::client::{
    CursorUpstreamResponse, decode_frame_payload, decode_upstream_frames,
};
use crate::providers::cursor::connect::{ConnectEndError, FLAG_END, parse_connect_error};
use crate::providers::cursor::native_tools::adapt_tool_input_for_client;
use crate::providers::cursor::proto::AgentServerMessage;
use crate::providers::cursor::request::preferred_text_editor_name;
use crate::providers::cursor::tool_bridge::resolve_advertised_name;
use std::collections::BTreeSet;

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

/// Text markers emitted by a few Cursor model/tokenizer combinations when an
/// end-of-sequence token is decoded as ordinary text.  They are transport
/// sentinels, not user-visible model output. Keep this list deliberately
/// narrow: stripping arbitrary XML-ish text would corrupt valid answers.
const CURSOR_EOS_SENTINELS: &[&str] = &[
    "◁eos▷",
    "<|eos|>",
    "<|endoftext|>",
    "<|end_of_text|>",
    "<|end_of_sequence|>",
    "<|eot_id|>",
    "<|end|>",
    "</s>",
];

/// Result of normalizing one Cursor text delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NormalizedCursorText<'a> {
    /// The visible portion of the delta. `None` means the delta consisted
    /// entirely of EOS marker(s) and whitespace.
    pub text: Option<&'a str>,
    /// Whether one or more EOS marker(s) were consumed.
    pub eos: bool,
}

/// Remove a decoded EOS sentinel without touching ordinary answer text.
///
/// Cursor may send the marker alone, repeatedly (`◁eos▷ ◁eos▷`), or appended
/// to the final answer. We only consume complete markers at the end of a
/// delta (or when the complete delta is marker-only); a marker in the middle
/// of prose remains visible so a model discussing token syntax is preserved.
pub(crate) fn normalize_cursor_text_delta(raw: &str) -> NormalizedCursorText<'_> {
    if raw.is_empty() {
        return NormalizedCursorText {
            text: None,
            eos: false,
        };
    }

    // Work from the right edge so repeated markers with arbitrary whitespace
    // between them are consumed in one pass while preserving the caller's
    // original slice and allocation-free ordinary deltas.
    let mut content = raw;
    let mut eos = false;
    loop {
        let trimmed = content.trim_end();
        let marker = CURSOR_EOS_SENTINELS
            .iter()
            .copied()
            .find(|candidate| trimmed.ends_with(candidate));
        let Some(marker) = marker else {
            break;
        };

        let marker_start = trimmed.len().saturating_sub(marker.len());
        content = &trimmed[..marker_start];
        eos = true;
    }

    if !eos {
        return NormalizedCursorText {
            text: Some(raw),
            eos: false,
        };
    }

    // A marker-only delta (including repeated markers and whitespace) should
    // not become a blank Anthropic text chunk.
    let content = content.trim_end();
    let text = (!content.trim().is_empty()).then_some(content);
    NormalizedCursorText { text, eos: true }
}

/// Stateful EOS filter for a stream of text deltas.
///
/// Protobuf message boundaries do not necessarily line up with tokenizer
/// markers. Keep only a possible marker prefix between calls; ordinary text is
/// emitted immediately, while a complete marker at the end of the stream is
/// consumed and marks the stream finished. This prevents a split `◁` +
/// `eos▷` from leaking into the client or the XML tool parser.
#[derive(Debug, Default)]
pub(crate) struct CursorEosFilter {
    pending: String,
    finished: bool,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct FilteredCursorText {
    pub text: Option<String>,
    pub eos: bool,
}

impl CursorEosFilter {
    pub(crate) fn push(&mut self, raw: &str) -> FilteredCursorText {
        if self.finished || raw.is_empty() {
            return FilteredCursorText::default();
        }
        self.pending.push_str(raw);
        self.drain(false)
    }

    /// Flush a non-terminal stream. An incomplete marker is ordinary text when
    /// the upstream closes without ever completing it.
    pub(crate) fn finish(&mut self) -> FilteredCursorText {
        if self.finished {
            return FilteredCursorText::default();
        }
        self.drain(true)
    }

    #[allow(dead_code)]
    pub(crate) fn finished(&self) -> bool {
        self.finished
    }

    fn drain(&mut self, flush: bool) -> FilteredCursorText {
        let mut visible = String::new();
        if let Some((start, _end)) = terminal_marker_span(&self.pending) {
            visible.push_str(&self.pending[..start]);
            self.pending.clear();
            self.finished = true;
            let text = visible.trim_end();
            return FilteredCursorText {
                text: (!text.trim().is_empty()).then(|| text.to_string()),
                eos: true,
            };
        }

        if flush {
            visible.push_str(&self.pending);
            self.pending.clear();
        } else {
            // Keep trailing whitespace together with a possible marker prefix.
            // A protobuf delta often ends in `"answer "`, followed by a
            // separate delta containing `"◁eos▷"`; retaining that whitespace
            // lets us remove the complete control suffix without leaking a
            // blank before the terminal event. If the next delta is ordinary
            // text, the retained bytes are emitted unchanged.
            let trimmed_len = self.pending.trim_end().len();
            let marker_prefix = longest_marker_prefix_suffix(&self.pending[..trimmed_len]);
            let mut keep_start = if marker_prefix > 0 {
                trimmed_len.saturating_sub(marker_prefix)
            } else {
                trailing_whitespace_start(&self.pending)
            };
            if marker_prefix > 0 {
                while keep_start > 0 {
                    let Some((at, ch)) = self.pending[..keep_start].char_indices().next_back()
                    else {
                        break;
                    };
                    if !ch.is_whitespace() {
                        break;
                    }
                    keep_start = at;
                }
            }
            let emit_len = keep_start;
            if emit_len > 0 {
                visible.push_str(&self.pending[..emit_len]);
                self.pending.drain(..emit_len);
            }
        }

        FilteredCursorText {
            text: (!visible.is_empty()).then_some(visible),
            eos: false,
        }
    }
}

/// Return a complete marker that terminates `value`, ignoring whitespace and
/// repeated/incomplete control markers after it. A marker in the middle of
/// ordinary prose is deliberately not considered terminal.
fn terminal_marker_span(value: &str) -> Option<(usize, usize)> {
    for marker in CURSOR_EOS_SENTINELS {
        let mut search_from = 0;
        while let Some(relative) = value[search_from..].find(marker) {
            let start = search_from + relative;
            let end = start + marker.len();
            if control_tail_only(&value[end..]) {
                return Some((start, end));
            }
            search_from = end;
        }
    }
    None
}

fn control_tail_only(mut tail: &str) -> bool {
    loop {
        tail = tail.trim_start();
        if tail.is_empty() {
            return true;
        }
        if let Some(marker) = CURSOR_EOS_SENTINELS
            .iter()
            .copied()
            .find(|marker| tail.starts_with(marker))
        {
            tail = &tail[marker.len()..];
            continue;
        }
        // A duplicate marker may itself be split across deltas. Holding this
        // suffix until the next call is preferable to leaking a control glyph.
        return CURSOR_EOS_SENTINELS
            .iter()
            .any(|marker| marker.starts_with(tail));
    }
}

fn longest_marker_prefix_suffix(value: &str) -> usize {
    CURSOR_EOS_SENTINELS
        .iter()
        .map(|marker| trailing_marker_prefix_len(value, marker))
        .max()
        .unwrap_or(0)
}

fn trailing_marker_prefix_len(value: &str, marker: &str) -> usize {
    marker
        .char_indices()
        .map(|(at, ch)| at + ch.len_utf8())
        .filter(|&len| len < marker.len() && value.ends_with(&marker[..len]))
        .max()
        .unwrap_or(0)
}

fn trailing_whitespace_start(value: &str) -> usize {
    let mut start = value.len();
    while start > 0 {
        let Some((at, ch)) = value[..start].char_indices().next_back() else {
            break;
        };
        if !ch.is_whitespace() {
            break;
        }
        start = at;
    }
    start
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
    decode_upstream_response_with_allowed_inner(body, None)
}

/// Decode a buffered Cursor response while retaining the downstream tool
/// catalog.  The modern Claude text editor accepts one `str_replace` per
/// tool_use, whereas Cursor's PiEdit exec can contain an array of
/// replacements.  Knowing the allow-list at decode time lets us expand that
/// *native* PiEdit event without weakening the generic/XML `MultiEdit` gate.
pub(crate) fn decode_upstream_response_with_allowed(
    body: &[u8],
    allowed_tool_names: Option<&BTreeSet<String>>,
) -> Result<Vec<CursorStreamEvent>, CursorDecodeError> {
    decode_upstream_response_with_allowed_inner(body, allowed_tool_names)
}

fn decode_upstream_response_with_allowed_inner(
    body: &[u8],
    allowed_tool_names: Option<&BTreeSet<String>>,
) -> Result<Vec<CursorStreamEvent>, CursorDecodeError> {
    let frames =
        decode_upstream_frames(body).map_err(|e| CursorDecodeError::Decode(e.to_string()))?;
    let mut events = Vec::new();
    let mut eos_filters = (0..=super::proto::MAX_TASK_DELTA_NEST)
        .map(|_| CursorEosFilter::default())
        .collect::<Vec<_>>();
    // A buffered response can carry both an InteractionUpdate terminal and a
    // Connect FLAG_END frame.  Keep one response-scoped bit so synthetic EOS
    // completion and transport completion cannot append duplicate End events.
    let mut terminal_emitted = false;

    for frame in &frames {
        if frame.flags & FLAG_END != 0 {
            flush_eos_filters(&mut eos_filters, &mut events, &mut terminal_emitted);
            // Check for Connect error in end frame
            if !frame.payload.is_empty()
                && let Some(err) = parse_connect_error(&frame.payload)
            {
                return Err(CursorDecodeError::ConnectEnd(err));
            }
            push_end_once(&mut events, &mut terminal_emitted);
            continue;
        }

        let msg = match decode_frame_payload(frame) {
            Ok(m) => m,
            Err(_) => continue,
        };

        events_from_message_with_allowed(
            &msg,
            &mut events,
            allowed_tool_names,
            &mut eos_filters,
            &mut terminal_emitted,
        );
    }

    // Buffered bodies may omit a Connect END frame. Flush any ordinary text
    // held while checking for a split marker before returning the events.
    flush_eos_filters(&mut eos_filters, &mut events, &mut terminal_emitted);

    Ok(events)
}

/// Fold live/buffered Cursor events into one Anthropic Messages JSON body.
///
/// Claude Code's non-streaming fallback (`stream=false`) still needs this shape
/// after we drive the live BiDi path (SSE would fail JSON parse).
#[derive(Debug)]
pub struct AnthropicJsonAcc {
    text: String,
    compaction_mode: bool,
    /// Reasoning is only a compaction fallback. Keep it retractable until we
    /// know whether Cursor emits an authoritative text summary later.
    compaction_thinking_fallback: String,
    compaction_seen_text: bool,
    tools: Vec<(String, String, serde_json::Value)>,
    input_tokens: u64,
    output_tokens: u64,
    cache_read: u64,
    cache_write: u64,
    estimated_input: u64,
}

impl AnthropicJsonAcc {
    pub fn new(estimated_input: u64) -> Self {
        Self::new_mode(estimated_input, false)
    }

    /// Create an accumulator for a context compaction turn. Actual text is
    /// authoritative; reasoning becomes assistant text only as a final fallback.
    pub fn new_mode(estimated_input: u64, compaction_mode: bool) -> Self {
        Self {
            text: String::new(),
            compaction_mode,
            compaction_thinking_fallback: String::new(),
            compaction_seen_text: false,
            tools: Vec::new(),
            input_tokens: 0,
            output_tokens: 0,
            cache_read: 0,
            cache_write: 0,
            estimated_input,
        }
    }

    pub fn push(&mut self, event: &CursorStreamEvent) {
        match event {
            CursorStreamEvent::TextDelta { text } => {
                if self.compaction_mode && !self.compaction_seen_text {
                    self.compaction_seen_text = true;
                    self.compaction_thinking_fallback.clear();
                }
                self.text.push_str(text);
            }
            CursorStreamEvent::Usage {
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
            } => {
                self.input_tokens = *input_tokens;
                self.output_tokens = *output_tokens;
                self.cache_read = *cache_read_tokens;
                self.cache_write = *cache_write_tokens;
            }
            CursorStreamEvent::OutputTokenDelta { tokens } => {
                self.output_tokens = self.output_tokens.saturating_add(*tokens);
            }
            CursorStreamEvent::NativeTool {
                tool_use_id,
                name,
                input,
            } => self.push_native_tool(tool_use_id.clone(), name.clone(), input.clone()),
            CursorStreamEvent::ThinkingDelta { text } if self.compaction_mode => {
                if !self.compaction_seen_text {
                    self.compaction_thinking_fallback.push_str(text);
                }
            }
            CursorStreamEvent::ThinkingDelta { .. }
            | CursorStreamEvent::Session { .. }
            | CursorStreamEvent::End => {}
        }
    }

    pub fn push_native_tool(&mut self, id: String, name: String, input: serde_json::Value) {
        self.tools.push((id, name, input));
    }

    pub fn has_useful(&self) -> bool {
        !self.text.is_empty()
            || (self.compaction_mode && !self.compaction_thinking_fallback.is_empty())
            || !self.tools.is_empty()
    }

    pub fn usage_pair(&self) -> (u64, u64) {
        let (input, output, _, _) = self.normalized_usage();
        (input, output)
    }

    fn normalized_usage(&self) -> (u64, u64, u64, u64) {
        crate::providers::cursor::sse::normalize_cursor_usage_for_anthropic(
            self.input_tokens.max(self.estimated_input.max(1)),
            self.output_tokens,
            self.cache_read,
            self.cache_write,
        )
    }

    pub fn into_message_json(mut self, message_id: &str, model: &str) -> serde_json::Value {
        let text = if self.compaction_mode && self.text.is_empty() {
            std::mem::take(&mut self.compaction_thinking_fallback)
        } else {
            std::mem::take(&mut self.text)
        };
        let mut content = Vec::new();
        if !text.is_empty() || self.tools.is_empty() {
            content.push(serde_json::json!({
                "type": "text",
                "text": text,
            }));
        }
        for (id, name, input) in &self.tools {
            content.push(serde_json::json!({
                "type": "tool_use",
                "id": id,
                "name": name,
                "input": input,
            }));
        }
        let stop_reason = if self.tools.is_empty() {
            "end_turn"
        } else {
            "tool_use"
        };
        let (input_tokens, output_tokens, cache_read_tokens, cache_write_tokens) =
            self.normalized_usage();
        serde_json::json!({
            "id": message_id,
            "type": "message",
            "role": "assistant",
            "content": content,
            "model": model,
            "stop_reason": stop_reason,
            "stop_sequence": null,
            "usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_creation_input_tokens": cache_write_tokens,
                "cache_read_input_tokens": cache_read_tokens
            }
        })
    }
}

/// Build an accumulated Anthropic response JSON from upstream bytes for
/// non-streaming mode.
pub fn decode_cursor_upstream(
    upstream: &CursorUpstreamResponse,
    message_id: &str,
    model: &str,
) -> Result<serde_json::Value, CursorDecodeError> {
    decode_cursor_upstream_with_allowed(upstream, message_id, model, None)
}

/// Build a non-streaming Anthropic response while filtering native tool calls
/// against the tools advertised by the downstream request.
///
/// `None` preserves the historical unfiltered behavior of
/// [`decode_cursor_upstream`]. Callers handling an actual Claude request should
/// pass `Some(&allowed)`; an empty set then suppresses every native tool call.
pub fn decode_cursor_upstream_with_allowed(
    upstream: &CursorUpstreamResponse,
    message_id: &str,
    model: &str,
    allowed_tool_names: Option<&BTreeSet<String>>,
) -> Result<serde_json::Value, CursorDecodeError> {
    decode_cursor_upstream_with_allowed_mode(upstream, message_id, model, allowed_tool_names, false)
}

/// Build a non-streaming Anthropic response for a context-compaction turn.
///
/// The Responses compaction collector only consumes output text. This variant
/// prefers real `text_delta` content and falls back to reasoning only when no
/// text summary arrives, while leaving the normal response path unchanged.
pub fn decode_cursor_upstream_compaction(
    upstream: &CursorUpstreamResponse,
    message_id: &str,
    model: &str,
) -> Result<serde_json::Value, CursorDecodeError> {
    decode_cursor_upstream_with_allowed_mode(
        upstream,
        message_id,
        model,
        Some(&BTreeSet::new()),
        true,
    )
}

fn decode_cursor_upstream_with_allowed_mode(
    upstream: &CursorUpstreamResponse,
    message_id: &str,
    model: &str,
    allowed_tool_names: Option<&BTreeSet<String>>,
    compaction_mode: bool,
) -> Result<serde_json::Value, CursorDecodeError> {
    let events = match allowed_tool_names {
        Some(allowed) => decode_upstream_response_with_allowed(&upstream.body, Some(allowed))?,
        None => decode_upstream_response(&upstream.body)?,
    };

    let mut text_content = String::new();
    let mut compaction_thinking_fallback = String::new();
    let mut compaction_seen_text = false;
    let mut tool_content: Vec<serde_json::Value> = Vec::new();
    let mut final_input_tokens: u64 = 0;
    let mut final_output_tokens: u64 = 0;
    let mut final_cache_read: u64 = 0;
    let mut final_cache_write: u64 = 0;

    for event in &events {
        match event {
            CursorStreamEvent::TextDelta { text } => {
                if compaction_mode && !compaction_seen_text {
                    compaction_seen_text = true;
                    compaction_thinking_fallback.clear();
                }
                text_content.push_str(text);
            }
            CursorStreamEvent::ThinkingDelta { text } if compaction_mode => {
                if !compaction_seen_text {
                    compaction_thinking_fallback.push_str(text);
                }
            }
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
                tool_content.push(serde_json::json!({
                    "type": "tool_use",
                    "id": tool_use_id,
                    "name": name,
                    "input": input,
                }));
            }
            CursorStreamEvent::End => break,
            _ => {}
        }
    }

    if compaction_mode && text_content.is_empty() {
        text_content = compaction_thinking_fallback;
    }

    let (input_tokens, output_tokens, cache_read_tokens, cache_write_tokens) =
        crate::providers::cursor::sse::normalize_cursor_usage_for_anthropic(
            final_input_tokens.max(estimate_input_tokens(&text_content)),
            final_output_tokens,
            final_cache_read,
            final_cache_write,
        );

    let mut content = Vec::new();
    if !text_content.is_empty() || tool_content.is_empty() {
        content.push(serde_json::json!({"type": "text", "text": text_content}));
    }
    content.extend(tool_content);
    let stop_reason = if content
        .iter()
        .any(|block| block.get("type").and_then(|value| value.as_str()) == Some("tool_use"))
    {
        "tool_use"
    } else {
        "end_turn"
    };

    Ok(serde_json::json!({
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": model,
        "stop_reason": stop_reason,
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

fn events_from_message_with_allowed(
    msg: &AgentServerMessage,
    events: &mut Vec<CursorStreamEvent>,
    allowed_tool_names: Option<&BTreeSet<String>>,
    eos_filters: &mut [CursorEosFilter],
    terminal_emitted: &mut bool,
) {
    if let Some(ref exec) = msg.exec_server_message {
        if let Some(ref sid) = exec.exec_id
            && !sid.is_empty()
        {
            events.push(CursorStreamEvent::Session {
                session_id: sid.clone(),
            });
        }
        // BiDi exec tool requests (not request_context) → Claude tool_use.
        if exec.request_context_args.is_none() {
            // Cursor PiEdit is the one native operation whose payload shape
            // cannot be represented by Claude Code 2.1+'s modern editor in a
            // single tool_use: `edits` is an array upstream, while the
            // `str_replace` command carries exactly one pair. Expand only
            // field-47 PiEdit here; generic/XML MultiEdit remains governed by
            // the normal exact allow-list resolver.
            if let (Some(args), Some(modern)) = (
                exec.pi_edit_args.as_ref(),
                allowed_tool_names.and_then(preferred_text_editor_name),
            ) {
                let base_id = exec
                    .exec_id
                    .clone()
                    .filter(|id| !id.is_empty())
                    .unwrap_or_else(|| format!("exec_{}", exec.id));
                for (index, edit) in args.edits.iter().enumerate() {
                    let tool_use_id = if index == 0 {
                        base_id.clone()
                    } else {
                        format!("{base_id}__part_{}", index + 1)
                    };
                    events.push(CursorStreamEvent::NativeTool {
                        tool_use_id,
                        name: modern.clone(),
                        input: serde_json::json!({
                            "command": "str_replace",
                            "path": args.path,
                            "old_str": edit.old_text,
                            "new_str": edit.new_text,
                        }),
                    });
                }
            } else if let Some(mapped) = super::native_tools::map_exec_server_message(exec) {
                events.push(CursorStreamEvent::NativeTool {
                    tool_use_id: mapped.tool_use_id,
                    name: mapped.name,
                    input: mapped.input,
                });
            }
        }
    }

    if let Some(ref update) = msg.interaction_update {
        push_interaction_stream_events(update, events, 0, eos_filters, terminal_emitted);
    }
}

fn push_interaction_stream_events(
    update: &super::proto::InteractionUpdate,
    events: &mut Vec<CursorStreamEvent>,
    nest_depth: u8,
    eos_filters: &mut [CursorEosFilter],
    terminal_emitted: &mut bool,
) {
    let mut eos_seen = false;
    if let Some(ref td) = update.thinking_delta
        && !td.text.is_empty()
    {
        events.push(CursorStreamEvent::ThinkingDelta {
            text: td.text.clone(),
        });
    }

    if let Some(ref td) = update.text_delta {
        let filtered = eos_filters
            .get_mut(nest_depth as usize)
            .map(|filter| filter.push(&td.text))
            .unwrap_or_default();
        if let Some(text) = filtered.text
            && !text.is_empty()
        {
            events.push(CursorStreamEvent::TextDelta { text });
        }
        eos_seen = filtered.eos;
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
        push_interaction_stream_events(
            nested,
            events,
            nest_depth + 1,
            eos_filters,
            terminal_emitted,
        );
    }

    if nest_depth > 0 {
        // Nested turn_ended must not end the parent Task stream.
        if update.turn_ended.is_some() {
            flush_eos_filter_at_depth(eos_filters, events, nest_depth);
        }
        // A nested EOS/turn boundary closes only this child. Reset its filter
        // so a later sibling Task can emit text instead of being suppressed
        // by the previous child's finished state.
        if update.turn_ended.is_some() || eos_seen {
            if let Some(filter) = eos_filters.get_mut(nest_depth as usize) {
                *filter = CursorEosFilter::default();
            }
        }
        return;
    }

    // `turn_ended` is terminal even when the final text delta arrived in a
    // previous protobuf frame. Flush whitespace/partial-marker state before
    // emitting Usage and End so no visible text is ordered after termination.
    if update.turn_ended.is_some() || eos_seen {
        // Flush nested state first as nested Task text belongs to this parent
        // turn and must never be appended after its terminal event.
        flush_nested_eos_filters(eos_filters, events);
        let flushed = flush_eos_filter_at_depth(eos_filters, events, nest_depth);
        eos_seen |= flushed;
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
        push_end_once(events, terminal_emitted);
    } else if eos_seen && nest_depth == 0 {
        // A decoded EOS is authoritative for a buffered response when Cursor
        // omitted its usual turn_ended update. The live path applies the same
        // rule after it has flushed pending XML/native tools.
        push_end_once(events, terminal_emitted);
    }
}

fn flush_eos_filter_at_depth(
    eos_filters: &mut [CursorEosFilter],
    events: &mut Vec<CursorStreamEvent>,
    nest_depth: u8,
) -> bool {
    let Some(filter) = eos_filters.get_mut(nest_depth as usize) else {
        return false;
    };
    let filtered = filter.finish();
    if let Some(text) = filtered.text
        && !text.is_empty()
    {
        events.push(CursorStreamEvent::TextDelta { text });
    }
    filtered.eos
}

fn flush_nested_eos_filters(
    eos_filters: &mut [CursorEosFilter],
    events: &mut Vec<CursorStreamEvent>,
) {
    for depth in 1..eos_filters.len() {
        flush_eos_filter_at_depth(eos_filters, events, depth as u8);
    }
}

fn flush_eos_filters(
    eos_filters: &mut [CursorEosFilter],
    events: &mut Vec<CursorStreamEvent>,
    terminal_emitted: &mut bool,
) {
    flush_nested_eos_filters(eos_filters, events);
    if flush_eos_filter_at_depth(eos_filters, events, 0) {
        push_end_once(events, terminal_emitted);
    }
}

fn push_end_once(events: &mut Vec<CursorStreamEvent>, terminal_emitted: &mut bool) {
    if *terminal_emitted {
        // A synthetic EOS can precede a late `turn_ended` usage snapshot. Keep
        // the terminal event at the end so consumers that stop at End still
        // observe the authoritative Usage event first.
        if let Some(index) = events
            .iter()
            .position(|event| matches!(event, CursorStreamEvent::End))
            && index + 1 < events.len()
        {
            let end = events.remove(index);
            events.push(end);
        }
        return;
    }
    events.push(CursorStreamEvent::End);
    *terminal_emitted = true;
}

/// Input-token estimate for `message_start` seeding.
///
/// Keep this estimate tied to the exact prompt renderer used by the live
/// request. In particular, MCP schemas are registered in Cursor's catalog and
/// are omitted (or compacted) from the text prompt; counting the raw Anthropic
/// `tools` JSON here can inflate `Ctx` by hundreds of thousands of tokens when
/// the upstream later fails before sending authoritative `turn_ended` usage.
/// `turn_ended` usage replaces this provisional seed when available.
pub fn estimate_request_input_tokens(req: &MessagesRequest) -> u64 {
    let parts = super::request::render_cursor_prompt_parts(req);
    estimate_rendered_prompt_tokens(&parts)
}

/// Estimate the text actually handed to Cursor for a request.
///
/// This is public so the request handler can reuse the already-rendered
/// `CursorPromptParts` when it has applied checkpoint/delta options, avoiding a
/// second full history render on the hot path.
pub fn estimate_rendered_prompt_tokens(parts: &super::request::CursorPromptParts) -> u64 {
    let system_chars = parts.custom_system_prompt.as_ref().map_or(0, String::len);
    (system_chars
        .saturating_add(parts.user_text.len())
        .saturating_add(3)
        / 4)
    .max(1) as u64
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
        assert_eq!(events.len(), 4);
        assert!(matches!(events[0], CursorStreamEvent::TextDelta { .. }));
        assert!(matches!(events[1], CursorStreamEvent::TextDelta { .. }));
        assert!(matches!(events[2], CursorStreamEvent::Usage { .. }));
        assert!(matches!(events[3], CursorStreamEvent::End));
    }

    #[test]
    fn strips_cursor_eos_sentinel_from_text_deltas() {
        let trailing = normalize_cursor_text_delta("answer ◁eos▷ ◁eos▷");
        assert_eq!(trailing.text, Some("answer"));
        assert!(trailing.eos);

        let marker_only = normalize_cursor_text_delta(" ◁eos▷  ◁eos▷ ");
        assert_eq!(marker_only.text, None);
        assert!(marker_only.eos);

        let ordinary = normalize_cursor_text_delta("the token ◁eos▷ is discussed here");
        assert_eq!(ordinary.text, Some("the token ◁eos▷ is discussed here"));
        assert!(!ordinary.eos);
    }

    #[test]
    fn stateful_eos_filter_handles_split_marker_without_leaking_prefix() {
        let mut filter = CursorEosFilter::default();
        let first = filter.push("answer ◁");
        assert_eq!(first.text.as_deref(), Some("answer"));
        assert!(!first.eos);

        let second = filter.push("eos▷");
        assert_eq!(second.text, None);
        assert!(second.eos);
        assert!(filter.finished());

        // Once a terminal marker has been consumed, later upstream text is
        // stale and must not re-open the response.
        assert_eq!(filter.push("stale text"), FilteredCursorText::default());
    }

    #[test]
    fn stateful_eos_filter_preserves_prose_and_flushes_incomplete_marker() {
        let mut prose = CursorEosFilter::default();
        let visible = prose.push("the token ◁eos▷ is discussed here");
        assert_eq!(
            visible.text.as_deref(),
            Some("the token ◁eos▷ is discussed here")
        );
        assert!(!visible.eos);

        let mut incomplete = CursorEosFilter::default();
        assert_eq!(
            incomplete.push("literal ◁eo").text.as_deref(),
            Some("literal")
        );
        let flushed = incomplete.finish();
        assert_eq!(flushed.text.as_deref(), Some(" ◁eo"));
        assert!(!flushed.eos);
    }

    #[test]
    fn stateful_eos_filter_ignores_repeated_markers_and_marker_only_deltas() {
        let mut filter = CursorEosFilter::default();
        let first = filter.push(" ◁eos▷  ");
        assert_eq!(first.text, None);
        assert!(first.eos);
        assert_eq!(filter.push("◁eos▷"), FilteredCursorText::default());

        let mut split = CursorEosFilter::default();
        assert_eq!(split.push("◁").text, None);
        let done = split.push("eos▷ ◁eos▷");
        assert_eq!(done.text, None);
        assert!(done.eos);
    }

    #[test]
    fn buffered_decode_filters_eos_split_across_protobuf_frames() {
        let mut body = test_frames::text_frame("answer ◁");
        body.extend_from_slice(&test_frames::text_frame("eos▷"));

        let events = decode_upstream_response(&body).unwrap();
        let text: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                CursorStreamEvent::TextDelta { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(text, ["answer"]);
        assert!(
            events
                .iter()
                .any(|event| matches!(event, CursorStreamEvent::End))
        );
        assert!(!text.iter().any(|value| value.contains("eos")));
    }

    #[test]
    fn eos_marker_completes_buffered_response_without_turn_ended() {
        let mut body = test_frames::text_frame("answer ◁eos▷ ◁eos▷");
        // No explicit usage/turn_ended frame: this is the shape observed on
        // the Grok/Fable route when the tokenizer sentinel leaks into text.
        let events = decode_upstream_response(&body).unwrap();
        assert_eq!(
            events
                .iter()
                .filter_map(|event| match event {
                    CursorStreamEvent::TextDelta { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec!["answer"]
        );
        assert!(matches!(events.last(), Some(CursorStreamEvent::End)));

        body.extend_from_slice(&test_frames::end_frame());
        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };
        let json = decode_cursor_upstream(&upstream, "msg_eos", "claude-fable-5").unwrap();
        assert_eq!(json["content"][0]["text"], "answer");
        assert_eq!(json["stop_reason"], "end_turn");
    }

    #[test]
    fn buffered_eos_emits_one_end_across_turn_and_connect_boundaries() {
        let mut body = test_frames::text_frame("answer ◁eos▷");
        // Some routes report the tokenizer EOS in text and still send the
        // usual usage/turn_ended update before the Connect FLAG_END frame.
        body.extend_from_slice(&test_frames::usage_frame(10, 2));
        body.extend_from_slice(&test_frames::end_frame());

        let events = decode_upstream_response(&body).unwrap();
        let end_count = events
            .iter()
            .filter(|event| matches!(event, CursorStreamEvent::End))
            .count();
        assert_eq!(end_count, 1);
        assert!(matches!(events.last(), Some(CursorStreamEvent::End)));
        assert_eq!(
            events
                .iter()
                .filter_map(|event| match event {
                    CursorStreamEvent::TextDelta { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec!["answer"]
        );
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
            exec_server_control_message: None,
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
                pi_write_args: None,
                pi_edit_args: None,
            }),

            ..Default::default()
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
    fn compaction_json_promotes_thinking_summary_to_text() {
        let mut body = test_frames::thinking_frame("summary from reasoning");
        body.extend_from_slice(&test_frames::end_frame());
        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };

        let json =
            decode_cursor_upstream_compaction(&upstream, "msg_compact_json", "claude-fable-5")
                .expect("compaction response should decode");
        assert_eq!(json["content"][0]["type"], "text");
        assert_eq!(json["content"][0]["text"], "summary from reasoning");
        assert_eq!(json["stop_reason"], "end_turn");
    }

    #[test]
    fn compaction_json_prefers_text_over_mixed_reasoning() {
        let mut body = test_frames::thinking_frame("private reasoning");
        body.extend_from_slice(&test_frames::text_frame("actual summary"));
        body.extend_from_slice(&test_frames::thinking_frame("later private reasoning"));
        body.extend_from_slice(&test_frames::end_frame());
        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };

        let json =
            decode_cursor_upstream_compaction(&upstream, "msg_compact_mixed", "claude-fable-5")
                .expect("mixed compaction response should decode");
        assert_eq!(json["content"][0]["text"], "actual summary");
    }

    #[test]
    fn json_acc_collects_text_usage_and_end_turn() {
        let mut acc = AnthropicJsonAcc::new(9);
        acc.push(&CursorStreamEvent::TextDelta {
            text: "Hello".into(),
        });
        acc.push(&CursorStreamEvent::Usage {
            input_tokens: 12,
            output_tokens: 2,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        });
        acc.push(&CursorStreamEvent::End);
        let json = acc.into_message_json("msg_json", "claude-fable-5");
        assert_eq!(json["id"], "msg_json");
        assert_eq!(json["content"][0]["type"], "text");
        assert_eq!(json["content"][0]["text"], "Hello");
        assert_eq!(json["stop_reason"], "end_turn");
        assert_eq!(json["usage"]["input_tokens"].as_u64(), Some(12));
        assert_eq!(json["usage"]["output_tokens"].as_u64(), Some(2));
    }

    #[test]
    fn compaction_json_acc_prefers_text_and_uses_reasoning_only_as_fallback() {
        let mut mixed = AnthropicJsonAcc::new_mode(9, true);
        mixed.push(&CursorStreamEvent::ThinkingDelta {
            text: "private reasoning".into(),
        });
        assert!(mixed.has_useful());
        mixed.push(&CursorStreamEvent::TextDelta {
            text: "actual summary".into(),
        });
        mixed.push(&CursorStreamEvent::ThinkingDelta {
            text: "later private reasoning".into(),
        });
        assert_eq!(
            mixed.into_message_json("msg_mixed", "claude-fable-5")["content"][0]["text"],
            "actual summary"
        );

        let mut fallback = AnthropicJsonAcc::new_mode(9, true);
        fallback.push(&CursorStreamEvent::ThinkingDelta {
            text: "summary from reasoning".into(),
        });
        assert_eq!(
            fallback.into_message_json("msg_fallback", "claude-fable-5")["content"][0]["text"],
            "summary from reasoning"
        );
    }

    #[test]
    fn json_acc_tool_batch_is_tool_use_stop() {
        let mut acc = AnthropicJsonAcc::new(4);
        acc.push_native_tool(
            "toolu_1".into(),
            "Read".into(),
            serde_json::json!({"file_path": "/tmp/a"}),
        );
        let json = acc.into_message_json("msg_tools", "claude-fable-5");
        assert_eq!(json["stop_reason"], "tool_use");
        assert_eq!(json["content"][0]["type"], "tool_use");
        assert_eq!(json["content"][0]["id"], "toolu_1");
        assert_eq!(json["content"][0]["name"], "Read");
        assert_eq!(json["content"][0]["input"]["file_path"], "/tmp/a");
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
    fn non_streaming_native_tools_require_an_advertised_name() {
        let msg = AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: None,
            kv_server_message: None,
            interaction_query: None,
            exec_server_control_message: None,
            exec_server_message: Some(ExecServerMessage {
                id: 7,
                exec_id: Some("read-7".into()),
                shell_args: None,
                write_args: None,
                delete_args: None,
                grep_args: None,
                read_args: Some(ExecReadArgs {
                    path: "/tmp/example.txt".into(),
                    tool_call_id: "read-7".into(),
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
        let mut body = encode_connect_frame(&payload, 0).to_vec();
        body.extend_from_slice(&test_frames::end_frame());
        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };

        let empty = BTreeSet::new();
        let plain = decode_cursor_upstream_with_allowed(
            &upstream,
            "msg_no_tools",
            "cursor-test",
            Some(&empty),
        )
        .unwrap();
        assert_eq!(plain["stop_reason"], "end_turn");
        assert_eq!(plain["content"].as_array().unwrap().len(), 1);
        assert_eq!(plain["content"][0]["type"], "text");

        let allowed = BTreeSet::from(["Read".to_string()]);
        let with_tool = decode_cursor_upstream_with_allowed(
            &upstream,
            "msg_read",
            "cursor-test",
            Some(&allowed),
        )
        .unwrap();
        assert_eq!(with_tool["stop_reason"], "tool_use");
        assert_eq!(with_tool["content"][0]["type"], "tool_use");
        assert_eq!(with_tool["content"][0]["name"], "Read");
        assert_eq!(
            with_tool["content"][0]["input"]["file_path"],
            "/tmp/example.txt"
        );
    }

    #[test]
    fn buffered_pi_edit_expands_each_replacement_for_modern_editor() {
        let msg = AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: None,
            kv_server_message: None,
            interaction_query: None,
            exec_server_control_message: None,
            exec_server_message: Some(ExecServerMessage {
                id: 17,
                exec_id: Some("pi-buffered-17".into()),
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
                            old_text: "one".into(),
                            new_text: "1".into(),
                        },
                        PiEditReplacement {
                            old_text: "two".into(),
                            new_text: "2".into(),
                        },
                    ],
                }),
            }),

            ..Default::default()
        };
        let mut payload = Vec::new();
        msg.encode(&mut payload).unwrap();
        let body = encode_connect_frame(&payload, 0).to_vec();
        let allowed = BTreeSet::from(["str_replace_based_edit_tool".to_string()]);
        let upstream = CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        };
        let json = decode_cursor_upstream_with_allowed(
            &upstream,
            "msg_pi_buffered",
            "claude-fable-5",
            Some(&allowed),
        )
        .unwrap();
        assert_eq!(json["stop_reason"], "tool_use");
        let content = json["content"].as_array().expect("content array");
        assert_eq!(content.len(), 2);
        assert_eq!(content[0]["name"], "str_replace_based_edit_tool");
        assert_eq!(content[0]["id"], "pi-buffered-17");
        assert_eq!(content[0]["input"]["command"], "str_replace");
        assert_eq!(content[0]["input"]["old_str"], "one");
        assert_eq!(content[1]["id"], "pi-buffered-17__part_2");
        assert_eq!(content[1]["input"]["new_str"], "2");
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
            exec_server_control_message: None,
            exec_server_message: None,
            ..Default::default()
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
            exec_server_control_message: None,
            exec_server_message: None,

            ..Default::default()
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
            exec_server_control_message: None,
            exec_server_message: None,

            ..Default::default()
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
    fn nested_eos_does_not_suppress_a_later_sibling_task() {
        fn nested_frame(text: &str) -> Vec<u8> {
            let msg = AgentServerMessage {
                interaction_update: Some(InteractionUpdate {
                    tool_call_delta: Some(ToolCallDeltaUpdate {
                        call_id: "task-parent".into(),
                        model_call_id: "model-parent".into(),
                        tool_call_delta: Some(ToolCallDelta {
                            task_tool_call_delta: Some(TaskToolCallDelta {
                                interaction_update: Some(Box::new(InteractionUpdate {
                                    text_delta: Some(TextDelta { text: text.into() }),
                                    ..Default::default()
                                })),
                            }),
                            ..Default::default()
                        }),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let mut payload = Vec::new();
            msg.encode(&mut payload).unwrap();
            encode_connect_frame(&payload, 0).to_vec()
        }

        let mut body = nested_frame("child one ◁eos▷");
        body.extend_from_slice(&nested_frame("child two"));
        let events = decode_upstream_response(&body).unwrap();
        let text: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                CursorStreamEvent::TextDelta { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(text, ["child one", "child two"]);
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, CursorStreamEvent::End))
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
            exec_server_control_message: None,
            exec_server_message: None,

            ..Default::default()
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
        // tools array just to seed the provisional context counter.
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

    #[test]
    fn estimate_request_input_tokens_uses_compact_mcp_prompt() {
        let tools = serde_json::json!([
            {
                "name": "mcp__workspace__search",
                "description": "search",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "z".repeat(80_000)
                        }
                    }
                }
            }
        ]);
        let raw_chars = serde_json::to_string(&tools).unwrap().len();
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
        let tokens = estimate_request_input_tokens(&req);
        assert!(
            tokens < (raw_chars / 4) as u64 / 10,
            "MCP schema should not be counted as raw prompt text: raw={raw_chars}, estimate={tokens}"
        );
    }
}
