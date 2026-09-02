//! Cursor Desktop Sand inference transport.
//!
//! Sand stopped being accepted by `agent.v1.AgentService/Run` in the current
//! Cursor service.  The patched desktop runtime sends a Connect-JSON request
//! to `aiserver.v1.InferenceService/Stream` instead.  This module deliberately
//! keeps that wire format separate from the protobuf Agent transport used by
//! the CLI/IDE paths.
//!
//! The Connect framing is shared with the other Cursor transports:
//! `flags (u8)`, `payload length (u32, big endian)`, then the payload.  Unlike
//! AgentService frames, payloads here are UTF-8 JSON objects.

use base64::Engine as _;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::anthropic::schema::MessagesRequest;
use crate::providers::cursor::client::{CursorError, CursorHttpClient, CursorUpstreamResponse};
use crate::providers::cursor::connect::{
    ConnectFrame, ConnectFrameDecoder, FLAG_END, FLAG_GZIP, connect_error_status,
    decode_gzip_frame, encode_connect_frame, is_non_retryable_provider_error_message,
    parse_connect_error,
};
use crate::providers::cursor::request::{
    image_candidate, is_model_visible_tool_definition, message_blocks, normalize_image_data,
};
use crate::providers::cursor::response::CursorStreamEvent;

/// Current Sand inference endpoint.
pub const SAND_INFERENCE_STREAM_PATH: &str = "/aiserver.v1.InferenceService/Stream";

/// Maximum JSON payload accepted by the Sand decoder.  A normal token frame is
/// tiny; the generous ceiling leaves room for tool arguments while preventing
/// a corrupt length prefix from retaining unbounded memory.
pub const MAX_SAND_FRAME_PAYLOAD: usize = 64 * 1024 * 1024;

/// Wire role values used by `InferenceMessageRole`.
pub const ROLE_USER: u32 = 1;
pub const ROLE_ASSISTANT: u32 = 2;
pub const ROLE_TOOL: u32 = 3;
pub const ROLE_SYSTEM: u32 = 4;

/// Connect response control/trailer bit emitted by current Cursor Sand
/// gateways.  These frames carry transport metadata (and are not JSON
/// `InferenceStreamResponse` values), so they must be consumed without being
/// surfaced as model output or parsed as an error.
pub const FLAG_CONTROL: u8 = 0x80;

/// Return whether an error from an already-open Sand stream is safe to retry
/// before any model-visible text/tool output has been committed.  The ordinary
/// Cursor retry classifier treats "no useful progress" as an ambiguous live
/// acceptance (which is correct for AgentService's resumable runs), but Sand
/// requests are full-history, UUID-scoped InferenceService calls.  A bounded
/// replay is therefore preferable for connect resets, idle stalls and gateway
/// overloads while the downstream client is still waiting for its first token.
pub fn stream_error_is_retryable(error: &CursorError) -> bool {
    let message = error.client_message();
    let lower = message.to_ascii_lowercase();

    // Account/entitlement and model validation errors are deterministic.  Do
    // not turn these into a retry storm, even when a gateway labels them 502.
    if is_non_retryable_provider_error_message(&message)
        || crate::retry::is_billing_block(&message)
        || crate::retry::is_policy_rate_limit(&message)
        || crate::retry::is_capacity_shed(&message)
        || lower.contains("sand traffic is not supported")
        || lower.contains("bad model name")
        || lower.contains("outdated client")
    {
        return false;
    }

    // Sand's stream can report a stale invocation as 409/"already active";
    // each replay gets fresh UUIDs, so this is transient rather than a live
    // AgentService ownership conflict.
    if lower.contains("already active")
        || lower.contains("connection reset")
        || lower.contains("connection closed")
        || lower.contains("stream idle")
        || lower.contains("idle timeout")
        || lower.contains("no useful progress")
        || lower.contains("no chunks received")
        || lower.contains("connect failed")
        || lower.contains("timed out")
    {
        return true;
    }

    matches!(error.status, 408 | 425 | 429 | 500 | 502 | 503 | 504)
}

/// Maximum number of stream-level Sand replays after an accepted response.
/// Keep this lower than open retries: a response body may already have reached
/// the upstream, so unbounded retries would multiply model invocations.
pub fn stream_retry_limit() -> u32 {
    std::env::var("CCP_CURSOR_SAND_STREAM_RETRIES")
        .ok()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .unwrap_or(2)
        .min(5)
}

/// One message in an InferenceService request.
///
/// The protobuf JSON representation uses a `oneof` for message content.  We
/// keep the small wire-shaped fields here instead of serializing Anthropic's
/// blocks directly: text-only messages use `text`, multimodal messages use
/// `parts.parts[]`, assistant tool calls use `toolCalls[]`, and tool results
/// use `toolContent.parts[]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandInferenceMessage {
    pub role: u32,
    pub text: Option<String>,
    pub parts: Vec<Value>,
    pub tool_calls: Vec<Value>,
    pub tool_content: Option<Value>,
}

impl SandInferenceMessage {
    pub fn user(text: impl Into<String>) -> Self {
        Self {
            role: ROLE_USER,
            text: Some(text.into()),
            parts: Vec::new(),
            tool_calls: Vec::new(),
            tool_content: None,
        }
    }

    pub fn assistant(text: impl Into<String>) -> Self {
        Self {
            role: ROLE_ASSISTANT,
            text: Some(text.into()),
            parts: Vec::new(),
            tool_calls: Vec::new(),
            tool_content: None,
        }
    }

    pub fn system(text: impl Into<String>) -> Self {
        Self {
            role: ROLE_SYSTEM,
            text: Some(text.into()),
            parts: Vec::new(),
            tool_calls: Vec::new(),
            tool_content: None,
        }
    }

    pub fn tool(parts: Vec<Value>) -> Self {
        Self {
            role: ROLE_TOOL,
            text: None,
            parts: Vec::new(),
            tool_calls: Vec::new(),
            tool_content: Some(json!({ "parts": parts })),
        }
    }

    pub fn with_parts(mut self, parts: Vec<Value>) -> Self {
        self.text = None;
        self.parts = parts;
        self
    }

    pub fn with_tool_calls(mut self, tool_calls: Vec<Value>) -> Self {
        self.tool_calls = tool_calls;
        self
    }

    fn to_json_value(&self) -> Value {
        let mut object = Map::new();
        object.insert("role".into(), json!(self.role));
        if let Some(tool_content) = &self.tool_content {
            object.insert("toolContent".into(), tool_content.clone());
        } else if !self.parts.is_empty() {
            object.insert("parts".into(), json!({ "parts": self.parts }));
        } else if let Some(text) = &self.text {
            object.insert("text".into(), json!(text));
        }
        if !self.tool_calls.is_empty() {
            object.insert("toolCalls".into(), Value::Array(self.tool_calls.clone()));
        }
        Value::Object(object)
    }
}

/// Convert an Anthropic Messages request to the `InferenceCoreMessage` JSON
/// shape used by the current Sand endpoint.  This intentionally preserves
/// message boundaries and roles; flattening the entire history into one XML
/// user message loses tool-call/result semantics and makes follow-up turns
/// impossible for the inference service to reconcile.
pub fn messages_from_anthropic(
    request: &MessagesRequest,
    compaction_mode: bool,
) -> Vec<SandInferenceMessage> {
    let tool_names = assistant_tool_names(request);
    let mut output = Vec::new();
    for message in &request.messages {
        let role = match message.role.trim().to_ascii_lowercase().as_str() {
            "system" => ROLE_SYSTEM,
            "assistant" => ROLE_ASSISTANT,
            "tool" => ROLE_TOOL,
            _ => ROLE_USER,
        };
        let blocks = message_blocks(message);
        let mut text = String::new();
        let mut parts = Vec::new();
        let mut tool_calls = Vec::new();
        let mut tool_results = Vec::new();

        for block in blocks {
            let block_type = block.get("type").and_then(Value::as_str).unwrap_or("text");
            match block_type {
                "text" => {
                    if let Some(value) = block.get("text").and_then(Value::as_str) {
                        append_text(&mut text, value);
                    }
                }
                // Historical reasoning is represented by a separate response
                // channel. Replaying signatures/markup as user text causes
                // Sand models to echo or treat it as an instruction.
                "thinking" if !compaction_mode => {}
                "thinking" => {}
                "compaction" => {
                    if let Some(value) = block.get("content").and_then(Value::as_str) {
                        append_text(&mut text, value);
                    }
                }
                "image" | "input_image" | "image_url" => {
                    if let Some(part) = image_part(&block) {
                        parts.push(part);
                    }
                }
                "document" | "file" => {
                    if let Some(part) = file_part(&block) {
                        parts.push(part);
                    } else if let Some(value) = document_text(&block) {
                        append_text(&mut text, &value);
                    }
                }
                "tool_use" => {
                    if role == ROLE_ASSISTANT {
                        let id = block.get("id").and_then(Value::as_str).unwrap_or_default();
                        let name = block
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        let args = block.get("input").cloned().unwrap_or_else(|| json!({}));
                        if !id.is_empty() || !name.is_empty() {
                            tool_calls.push(json!({
                                "toolCallId": id,
                                "toolName": name,
                                "args": args,
                            }));
                        }
                    }
                }
                "tool_result" => {
                    let id = block
                        .get("tool_use_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if !id.is_empty() {
                        let name = block
                            .get("name")
                            .and_then(Value::as_str)
                            .or_else(|| tool_names.get(id).map(String::as_str))
                            .unwrap_or("unknown_tool");
                        let result = block.get("content").cloned().unwrap_or(Value::Null);
                        let is_error = block
                            .get("is_error")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        tool_results.push(json!({
                            "toolCallId": id,
                            "toolName": name,
                            "result": result,
                            "isError": is_error,
                        }));
                    }
                }
                // Keep server-side tool/search results as ordinary structured
                // content. They are not callable Sand tool results.
                "server_tool_use" | "web_search_tool_result" => {
                    if let Ok(value) = serde_json::to_string(&block) {
                        append_text(&mut text, &value);
                    }
                }
                _ => {
                    if let Some(value) = block.get("text").and_then(Value::as_str) {
                        append_text(&mut text, value);
                    }
                }
            }
        }

        // A user message may contain only tool_result blocks. The Inference
        // schema requires those to be a role=TOOL message with toolContent.
        if !tool_results.is_empty() {
            if !text.trim().is_empty() || !parts.is_empty() {
                output.push(content_message(role, text, parts, Vec::new()));
            }
            output.push(SandInferenceMessage::tool(tool_results));
            continue;
        }
        if !text.trim().is_empty() || !parts.is_empty() || !tool_calls.is_empty() {
            output.push(content_message(role, text, parts, tool_calls));
        }
    }
    output
}

/// Map Anthropic's tool catalog to `InferenceAgentTool` protobuf-JSON. Sand
/// expects the schema under `parameters` (a google.protobuf.Struct), rather
/// than Anthropic's `input_schema` field name.
pub fn tools_from_anthropic(request: &MessagesRequest, omit_tools: bool) -> Vec<Value> {
    if omit_tools {
        return Vec::new();
    }
    request
        .extra
        .get("tools")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        // Keep the Sand catalog identical to the model-facing Cursor catalog.
        // Claude-local hook/deprecated definitions are implementation details;
        // forwarding them here lets Sand call tools that the downstream client
        // will later discard and can leave the turn waiting forever.
        .filter(|tool| is_model_visible_tool_definition(tool))
        .filter_map(|tool| {
            let name = tool.get("name").and_then(Value::as_str)?.trim();
            if name.is_empty() {
                return None;
            }
            let description = tool
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("");
            let parameters = tool
                .get("input_schema")
                .or_else(|| tool.get("parameters"))
                .cloned()
                .unwrap_or_else(|| json!({}));
            Some(json!({
                "name": name,
                "description": description,
                "parameters": parameters,
            }))
        })
        .collect()
}

fn content_message(
    role: u32,
    text: String,
    mut parts: Vec<Value>,
    tool_calls: Vec<Value>,
) -> SandInferenceMessage {
    let mut message = SandInferenceMessage {
        role,
        text: None,
        parts: Vec::new(),
        tool_calls,
        tool_content: None,
    };
    if !text.trim().is_empty() {
        if parts.is_empty() {
            message.text = Some(text);
        } else {
            parts.insert(0, json!({ "text": { "text": text } }));
        }
    }
    if !parts.is_empty() {
        message.parts = parts;
    }
    message
}

fn append_text(target: &mut String, value: &str) {
    if value.is_empty() {
        return;
    }
    if !target.is_empty() {
        target.push('\n');
    }
    target.push_str(value);
}

fn image_part(block: &Value) -> Option<Value> {
    let (raw, hinted_mime) = image_candidate(block)?;
    let (data, mime_type) = normalize_image_data(raw, hinted_mime)?;
    Some(json!({
        "image": {
            // InferenceImagePart.data is a provider-ready string.  Cursor's
            // desktop runtime preserves a data URI here (the legacy Agent
            // protobuf path is the one that receives bare base64 bytes).
            "data": format!("data:{mime_type};base64,{data}"),
            "mimeType": mime_type,
        }
    }))
}

/// Convert Anthropic/OpenAI file-like content blocks to Cursor's native
/// `InferenceFilePart`.  The current endpoint accepts inline data URIs and
/// does not resolve remote URLs, so URL-only documents are represented as
/// text below instead of triggering a hidden network fetch.
fn file_part(block: &Value) -> Option<Value> {
    let block_type = block.get("type").and_then(Value::as_str).unwrap_or("");
    let source = block.get("source").and_then(Value::as_object);
    let file_object = block.get("file").and_then(Value::as_object);
    if source
        .and_then(|source| source.get("type"))
        .and_then(Value::as_str)
        .is_some_and(|kind| kind.eq_ignore_ascii_case("text"))
    {
        return None;
    }

    let raw = source
        .and_then(|source| source.get("data").or_else(|| source.get("file_data")))
        .or_else(|| file_object.and_then(|file| file.get("file_data").or_else(|| file.get("data"))))
        .or_else(|| block.get("data"))
        .and_then(Value::as_str)?
        .trim();
    if raw.is_empty() || raw.starts_with("http://") || raw.starts_with("https://") {
        return None;
    }

    let hinted_mime = source
        .and_then(|source| source.get("media_type").or_else(|| source.get("mime_type")))
        .or_else(|| {
            file_object.and_then(|file| file.get("media_type").or_else(|| file.get("mime_type")))
        })
        .or_else(|| block.get("media_type").or_else(|| block.get("mime_type")))
        .and_then(Value::as_str)
        .filter(|mime| !mime.trim().is_empty())
        .unwrap_or("application/octet-stream");
    let filename = source
        .and_then(|source| {
            source
                .get("filename")
                .or_else(|| source.get("name"))
                .or_else(|| block.get("title"))
        })
        .or_else(|| file_object.and_then(|file| file.get("filename").or_else(|| file.get("name"))))
        .or_else(|| {
            block
                .get("filename")
                .or_else(|| block.get("name"))
                .or_else(|| block.get("title"))
        })
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| {
            if block_type.eq_ignore_ascii_case("document") {
                "document"
            } else {
                "file"
            }
        });

    // normalize_image_data validates flexible base64 and canonicalizes the
    // alphabet/padding.  Its MIME fallback is image-specific, so use a small
    // data-URI decoder here and retain the declared document media type.
    let (uri_mime, encoded) = if let Some(rest) = raw.strip_prefix("data:") {
        let (metadata, encoded) = rest.split_once(',')?;
        if !metadata
            .split(';')
            .any(|part| part.eq_ignore_ascii_case("base64"))
        {
            return None;
        }
        (
            metadata.split(';').next().filter(|mime| !mime.is_empty()),
            encoded,
        )
    } else {
        (None, raw)
    };
    let compact: String = encoded
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace())
        .collect();
    let bytes = decode_base64_flexible_local(&compact)?;
    let mime_type = uri_mime.unwrap_or(hinted_mime).trim();
    let mime_type = if mime_type.is_empty() {
        "application/octet-stream"
    } else {
        mime_type
    };
    let canonical = base64::engine::general_purpose::STANDARD.encode(bytes);
    Some(json!({
        "file": {
            "data": format!("data:{mime_type};base64,{canonical}"),
            "mediaType": mime_type,
            "filename": filename,
        }
    }))
}

fn decode_base64_flexible_local(value: &str) -> Option<Vec<u8>> {
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
    STANDARD
        .decode(value)
        .or_else(|_| STANDARD_NO_PAD.decode(value))
        .or_else(|_| URL_SAFE.decode(value))
        .or_else(|_| URL_SAFE_NO_PAD.decode(value))
        .ok()
}

fn document_text(block: &Value) -> Option<String> {
    let source = block.get("source").and_then(Value::as_object);
    let text = source
        .and_then(|source| source.get("text"))
        .or_else(|| block.get("text"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())?;
    Some(text.to_string())
}

/// Serialize the model parameter list using the protobuf JSON field names.
/// Keeping this conversion local avoids exposing the Agent protobuf module in
/// the public Sand request API while still forwarding effort/context settings
/// selected by the TUI or model catalog.
fn requested_model_parameters_json(model_id: &str) -> Vec<Value> {
    crate::providers::cursor::model::requested_model_parameters(model_id)
        .into_iter()
        .map(|parameter| json!({ "id": parameter.id, "value": parameter.value }))
        .collect()
}

fn assistant_tool_names(request: &MessagesRequest) -> HashMap<String, String> {
    let mut names = HashMap::new();
    for message in &request.messages {
        if !message.role.eq_ignore_ascii_case("assistant") {
            continue;
        }
        for block in message_blocks(message) {
            if block.get("type").and_then(Value::as_str) != Some("tool_use") {
                continue;
            }
            let (Some(id), Some(name)) = (
                block.get("id").and_then(Value::as_str),
                block.get("name").and_then(Value::as_str),
            ) else {
                continue;
            };
            names.insert(id.to_string(), name.to_string());
        }
    }
    names
}

/// JSON request accepted by the current Sand stream endpoint.
#[derive(Debug, Clone, PartialEq)]
pub struct SandInferenceRequest {
    pub messages: Vec<SandInferenceMessage>,
    pub model_id: String,
    /// Optional CLI/catalog id used only to derive Desktop model parameters.
    /// The Sand wire `model_id` remains the canonical family id, while effort
    /// and thinking settings can still be inherited from the request's
    /// resolved CLI variant (for example Fable's `thinking-max`).
    pub parameter_model_id: Option<String>,
    pub conversation_id: String,
    pub invocation_id: String,
    pub max_mode: bool,
    pub max_tokens: Option<u64>,
    pub tools: Vec<Value>,
    /// Forward-compatible fields supplied by a newer desktop build.  Keeping
    /// these as JSON avoids baking unstable protobuf-generated fields into the
    /// proxy and lets callers pass tool/config metadata when available.
    pub extra: Map<String, Value>,
}

impl SandInferenceRequest {
    pub fn new(
        model_id: impl Into<String>,
        conversation_id: impl Into<String>,
        invocation_id: impl Into<String>,
        messages: Vec<SandInferenceMessage>,
    ) -> Self {
        Self {
            messages,
            model_id: model_id.into(),
            parameter_model_id: None,
            conversation_id: conversation_id.into(),
            invocation_id: invocation_id.into(),
            max_mode: false,
            max_tokens: None,
            tools: Vec::new(),
            extra: Map::new(),
        }
    }

    pub fn with_max_mode(mut self, enabled: bool) -> Self {
        self.max_mode = enabled;
        self
    }

    /// Derive `requestedModel.parameters` from a catalog/CLI variant while
    /// keeping `requestedModel.modelId` on the Sand family namespace.
    pub fn with_parameter_model_id(mut self, model_id: impl Into<String>) -> Self {
        let model_id = model_id.into();
        if !model_id.trim().is_empty() {
            self.parameter_model_id = Some(model_id);
        }
        self
    }

    pub fn with_max_tokens(mut self, max_tokens: Option<u64>) -> Self {
        self.max_tokens = max_tokens.filter(|value| *value > 0);
        self
    }

    pub fn with_extra(mut self, key: impl Into<String>, value: Value) -> Self {
        self.extra.insert(key.into(), value);
        self
    }

    pub fn with_tools(mut self, tools: Vec<Value>) -> Self {
        self.tools = tools;
        self
    }

    /// Clone a full-history request with fresh InferenceService lifecycle
    /// identifiers. Sand does not use the AgentService session registry, and a
    /// retry after a half-open socket must not collide with the abandoned
    /// invocation (which otherwise surfaces as repeated 503 "already active").
    pub fn with_fresh_ids(mut self) -> Self {
        self.conversation_id = uuid::Uuid::new_v4().to_string();
        self.invocation_id = uuid::Uuid::new_v4().to_string();
        self
    }

    /// Build the unframed JSON object.  This is public for protocol tests and
    /// for callers that need to inspect/log a redacted request before sending.
    pub fn to_json_value(&self) -> Value {
        let mut object = self.extra.clone();
        object.insert(
            "messages".into(),
            Value::Array(
                self.messages
                    .iter()
                    .map(SandInferenceMessage::to_json_value)
                    .collect(),
            ),
        );
        let parameter_model_id = self
            .parameter_model_id
            .as_deref()
            .unwrap_or(self.model_id.as_str());
        object.insert(
            "requestedModel".into(),
            json!({
                "modelId": self.model_id,
                "builtInModel": true,
                "maxMode": self.max_mode,
                // `parameters` is a repeated protobuf field.  Cursor's
                // managed-local runtime reads it with `.map(...)` while
                // constructing the provider attempt, so keep the array
                // explicit even when a model has no effort parameters.
                "parameters": requested_model_parameters_json(parameter_model_id),
                "isVariantStringRepresentation": false,
            }),
        );
        // InferenceService validates the top-level model id as well as the
        // requestedModel envelope.  Desktop sends both fields; omitting the
        // duplicate makes the endpoint classify the request as an older
        // AgentService payload.
        object.insert("modelId".into(), json!(self.model_id));
        object.insert("conversationId".into(), json!(self.conversation_id));
        object.insert("invocationId".into(), json!(self.invocation_id));
        // These are repeated fields in InferenceStreamRequest. Proto3 JSON
        // defaults omitted arrays correctly, but emitting them keeps the wire
        // contract explicit while staying within the current schema.
        object.insert("tools".into(), Value::Array(self.tools.clone()));
        object.insert("providerDefinedTools".into(), Value::Array(Vec::new()));
        if let Some(max_tokens) = self.max_tokens {
            object.insert("modelConfig".into(), json!({ "maxTokens": max_tokens }));
        }
        Value::Object(object)
    }

    pub fn to_json_bytes(&self) -> Result<Vec<u8>, CursorError> {
        serde_json::to_vec(&self.to_json_value())
            .map_err(|error| CursorError::internal(format!("Sand request JSON encode: {error}")))
    }

    pub fn encode_frame(&self) -> Result<Bytes, CursorError> {
        Ok(encode_connect_frame(self.to_json_bytes()?, 0))
    }
}

/// Encode a raw JSON value as one Connect-JSON request frame.
pub fn encode_json_frame(value: &Value) -> Result<Bytes, CursorError> {
    let payload = serde_json::to_vec(value)
        .map_err(|error| CursorError::internal(format!("Sand JSON encode: {error}")))?;
    Ok(encode_connect_frame(payload, 0))
}

/// A decoded response stream from InferenceService/Stream.
pub struct SandInferenceStream {
    bytes: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    decoder: ConnectFrameDecoder,
    pending: VecDeque<Result<CursorStreamEvent, CursorError>>,
    timeout_secs: u64,
    ended: bool,
    saw_end: bool,
    /// Set once a terminal event (or terminal error) has been queued.  The
    /// Connect endpoint may repeat FLAG_END or append a final JSON marker;
    /// downstream Anthropic encoders must observe exactly one End event.
    terminal_emitted: bool,
    tool_buffers: HashMap<String, SandToolBuffer>,
    completed_tool_ids: HashSet<String>,
}

#[derive(Debug, Default)]
struct SandToolBuffer {
    name: String,
    /// JSON argument fragments are accumulated by tool-call id.  Cursor may
    /// split a large call over several `toolCallPart` frames.
    args_text: String,
    args_value: Option<Value>,
    /// `InferenceToolCallStreamPart.isComplete` is the authoritative commit
    /// signal for string fragments.  A syntactically complete prefix is not
    /// enough: the next frame may still append another property.
    complete: bool,
}

impl SandInferenceStream {
    fn new(response: reqwest::Response, timeout_secs: u64) -> Self {
        Self {
            bytes: Box::pin(response.bytes_stream()),
            decoder: ConnectFrameDecoder::new(),
            pending: VecDeque::new(),
            timeout_secs,
            ended: false,
            saw_end: false,
            terminal_emitted: false,
            tool_buffers: HashMap::new(),
            completed_tool_ids: HashSet::new(),
        }
    }

    /// Queue one and only one terminal event.  Marking the stream ended here
    /// also drops any frames coalesced after FLAG_END in the same HTTP chunk.
    fn emit_end_once(&mut self) {
        if self.terminal_emitted {
            return;
        }
        self.flush_tool_buffers();
        self.terminal_emitted = true;
        self.saw_end = true;
        self.ended = true;
        self.pending.push_back(Ok(CursorStreamEvent::End));
    }

    fn queue_terminal_error(&mut self, error: CursorError) {
        if self.terminal_emitted {
            return;
        }
        self.terminal_emitted = true;
        self.ended = true;
        self.pending.push_back(Err(error));
    }

    /// Decode an already-buffered HTTP response.  Useful for non-streaming
    /// callers and deterministic tests; the returned body retains Connect
    /// framing so existing response accounting can inspect it when needed.
    pub async fn collect_response(mut self) -> Result<CursorUpstreamResponse, CursorError> {
        let mut body = Vec::new();
        while let Some(item) = self.next().await {
            let event = item?;
            // Re-encode event data is lossy, so this helper is intentionally
            // only a transport success marker. Callers needing events should
            // consume the stream directly. Keeping an empty body here avoids
            // pretending JSON frames are Agent protobuf frames.
            let _ = event;
        }
        body.shrink_to_fit();
        Ok(CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        })
    }

    fn queue_frame(&mut self, frame: ConnectFrame) {
        if self.ended || self.terminal_emitted {
            return;
        }
        // Current Sand builds append a control/trailer frame with bit 7 set.
        // Its payload is often binary or an implementation-specific trailer;
        // attempting JSON decoding here turns an otherwise successful answer
        // into a spurious 502.  Check this before FLAG_END because a gateway
        // may combine the trailer and end bits.
        if frame.flags & FLAG_CONTROL != 0 {
            if frame.flags & FLAG_END != 0 {
                // A combined control+END frame carries no model payload, but
                // it is still the authoritative stream terminator.  Ignoring
                // the END bit here leaves `saw_end` set only implicitly at
                // EOF and can suppress the Anthropic message_end event.
                self.saw_end = true;
                self.emit_end_once();
            }
            return;
        }
        if frame.flags & FLAG_END != 0 {
            self.saw_end = true;
            if frame.payload.is_empty() {
                self.emit_end_once();
                return;
            }
            let payload = match frame_payload(&frame) {
                Ok(payload) => payload,
                Err(error) => {
                    self.queue_terminal_error(error);
                    return;
                }
            };
            if let Some(error) = parse_connect_error(&payload) {
                self.queue_terminal_error(CursorError::new(
                    error.status,
                    error.message,
                    Some(error.detail),
                ));
            } else {
                // Some gateways put a normal final JSON object on the END
                // frame. Decode it before emitting End so a final text delta
                // is not lost.
                match serde_json::from_slice::<Value>(&payload) {
                    Ok(value) => self.queue_json_value(&value),
                    Err(_) if !payload.is_empty() => self.queue_terminal_error(CursorError::new(
                        502,
                        "Sand inference END frame is not valid JSON",
                        Some(String::from_utf8_lossy(&payload).into_owned()),
                    )),
                    Err(_) => {}
                }
                self.emit_end_once();
            }
            return;
        }

        let payload = match frame_payload(&frame) {
            Ok(payload) => payload,
            Err(error) => {
                self.queue_terminal_error(error);
                return;
            }
        };
        if payload.is_empty() {
            return;
        }
        match serde_json::from_slice::<Value>(&payload) {
            Ok(value) => self.queue_json_value(&value),
            Err(error) => self.queue_terminal_error(CursorError::new(
                502,
                "Sand inference response frame is not valid JSON",
                Some(format!("{error}: {}", String::from_utf8_lossy(&payload))),
            )),
        }
    }

    fn queue_json_value(&mut self, value: &Value) {
        if self.ended || self.terminal_emitted {
            return;
        }
        if let Some(error) = json_error(value) {
            self.queue_terminal_error(error);
            return;
        }
        let value = value.get("result").unwrap_or(value);
        for event in
            events_from_json_with_state(value, &mut self.tool_buffers, &mut self.completed_tool_ids)
        {
            self.pending.push_back(Ok(event));
        }
        // A few desktop builds omit FLAG_END and mark the final object.  Honor
        // that marker, but only after queuing its text/usage/tool events.
        if json_is_terminal(value) {
            self.emit_end_once();
        }
    }

    fn flush_tool_buffers(&mut self) {
        let buffers = std::mem::take(&mut self.tool_buffers);
        for (id, buffer) in buffers {
            // Never expose an unterminated fragment as a JSON string.  Claude
            // Code treats that as a malformed tool invocation and can enter a
            // StrReplace/XML fallback loop.  A complete empty argument list is
            // represented by an empty object, matching the desktop client.
            let Some(input) = complete_tool_input(&buffer) else {
                continue;
            };
            if !buffer.name.is_empty() && self.completed_tool_ids.insert(id.clone()) {
                self.pending.push_back(Ok(CursorStreamEvent::NativeTool {
                    tool_use_id: id,
                    name: buffer.name,
                    input,
                }));
            }
        }
    }

    fn finish_at_eof(&mut self) {
        if self.ended {
            return;
        }
        self.ended = true;
        self.flush_tool_buffers();
        if self.decoder.buffered() != 0 {
            self.pending.push_back(Err(CursorError::new(
                502,
                "Sand inference stream ended with an incomplete Connect frame",
                Some(format!("{} trailing bytes", self.decoder.buffered())),
            )));
        } else if !self.terminal_emitted {
            // The endpoint normally sends FLAG_END. Treat a clean HTTP close
            // as terminal for compatibility with proxies that strip the end
            // marker; no useful frame is silently left hanging. Check the
            // actual terminal state rather than `saw_end`: a control/trailer
            // parser can observe the END bit before its terminal event is
            // queued, and that state must still be closed for downstream SSE.
            self.emit_end_once();
        }
    }
}

impl Stream for SandInferenceStream {
    type Item = Result<CursorStreamEvent, CursorError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(item) = this.pending.pop_front() {
                return Poll::Ready(Some(item));
            }
            if this.ended {
                return Poll::Ready(None);
            }
            match this.bytes.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(chunk))) => {
                    let frames = match this.decoder.push_with_limit(&chunk, MAX_SAND_FRAME_PAYLOAD)
                    {
                        Ok(frames) => frames,
                        Err(error) => {
                            this.ended = true;
                            return Poll::Ready(Some(Err(CursorError::new(
                                502,
                                "Sand inference Connect frame decode failed",
                                Some(error.to_string()),
                            ))));
                        }
                    };
                    for frame in frames {
                        this.queue_frame(frame);
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    this.ended = true;
                    return Poll::Ready(Some(Err(CursorError::from_reqwest(
                        error,
                        this.timeout_secs,
                    ))));
                }
                Poll::Ready(None) => {
                    this.finish_at_eof();
                }
            }
        }
    }
}

/// Thin HTTP client for the Sand endpoint.  It can be built from the shared
/// Cursor client so proxy/base-url/timeout settings remain identical.
#[derive(Clone)]
pub struct SandInferenceClient {
    client: reqwest::Client,
    base_url: String,
    timeout_secs: u64,
}

impl SandInferenceClient {
    pub fn new() -> Self {
        // Sand has its own endpoint override, but should inherit the same
        // timeout and proxy settings as the normal Cursor client.  Construct
        // the source with the resolved Sand URL rather than silently using
        // `CCP_CURSOR_BASE_URL`/the public default.
        let source = CursorHttpClient::with_base_url_timeout_and_prefer_http1(
            crate::config::cursor_sand_base_url(),
            crate::config::cursor_request_timeout_secs(),
            false,
        );
        Self::from_cursor_client(&source)
    }

    pub(crate) fn from_cursor_client(source: &CursorHttpClient) -> Self {
        // Sand must never inherit a process-wide HTTP/1 pin.  The shared
        // constructor selects strict H2 (or prior-knowledge H2 for fixtures).
        let sand = source.with_sand_transport_mode();
        Self {
            client: sand.client.clone(),
            base_url: sand.base_url.clone(),
            timeout_secs: sand.timeout_secs,
        }
    }

    pub fn with_base_url_timeout(
        base_url: impl Into<String>,
        timeout_secs: u64,
    ) -> Result<Self, CursorError> {
        let base_url = base_url.into();
        let source = CursorHttpClient::with_base_url_timeout_and_prefer_http1(
            base_url,
            timeout_secs.max(1),
            false,
        );
        Ok(Self::from_cursor_client(&source))
    }

    pub fn endpoint(&self) -> String {
        format!(
            "{}{}",
            self.base_url.trim_end_matches('/'),
            SAND_INFERENCE_STREAM_PATH
        )
    }

    /// Open one Sand stream.  No replay occurs after a response is accepted;
    /// callers can safely retry only a returned connect/open error.
    pub async fn open(
        &self,
        token: &str,
        request: &SandInferenceRequest,
    ) -> Result<SandInferenceStream, CursorError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let body = request.encode_frame()?;
        let client_type = "sand";
        let mut builder = self
            .client
            .post(self.endpoint())
            .bearer_auth(token)
            .header("content-type", "application/connect+json")
            .header("connect-protocol-version", "1")
            .header("connect-accept-encoding", "gzip")
            .header("user-agent", "connect-es/1.6.1")
            // The Desktop InferenceService checks the product version in the
            // legacy `x-cursor-version` header in addition to the newer
            // client-version identity header.
            .header(
                "x-cursor-version",
                crate::config::cursor_client_version_for_type("sand"),
            )
            .header("x-request-id", &request_id)
            .header("x-original-request-id", &request_id);
        builder = crate::providers::cursor::client::apply_cursor_identity_headers_for_client_type(
            builder,
            token,
            Some(client_type),
        );

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(self.timeout_secs.max(1)),
            builder.body(body).send(),
        )
        .await
        .map_err(|_| {
            CursorError::new(
                504,
                format!("Sand inference open timed out after {}s", self.timeout_secs),
                None,
            )
        })?
        .map_err(|error| CursorError::from_reqwest(error, self.timeout_secs))?;

        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            let detail = response.text().await.ok().filter(|text| !text.is_empty());
            return Err(CursorError::new(
                status,
                format!("Sand inference upstream HTTP {status}"),
                detail,
            )
            .with_retry_after(retry_after));
        }
        Ok(SandInferenceStream::new(response, self.timeout_secs))
    }

    /// Convenience wrapper for callers that need a stream of events and do
    /// not need to retain the HTTP response object.
    pub async fn stream_events(
        &self,
        token: &str,
        request: &SandInferenceRequest,
    ) -> Result<SandInferenceStream, CursorError> {
        self.open(token, request).await
    }
}

impl Default for SandInferenceClient {
    fn default() -> Self {
        Self::new()
    }
}

fn frame_payload(frame: &ConnectFrame) -> Result<Vec<u8>, CursorError> {
    if frame.flags & FLAG_GZIP != 0 {
        decode_gzip_frame(&frame.payload).map_err(|error| {
            CursorError::new(
                502,
                "Sand inference gzip frame decode failed",
                Some(error.to_string()),
            )
        })
    } else {
        Ok(frame.payload.to_vec())
    }
}

/// Extract an `InferenceStreamError` from a response object.  Connect
/// adapters in the wild wrap the protobuf-JSON response in one or more
/// transport envelopes (`result`, `response`, `data`, or `payload`), so look
/// through those known keys as well as the direct response shape.  Deliberate
/// key allow-listing keeps an `error` field inside tool arguments/metadata
/// from being interpreted as a stream failure.
fn json_error(value: &Value) -> Option<CursorError> {
    json_error_with_depth(value, 0)
}

fn json_error_with_depth(value: &Value, depth: u8) -> Option<CursorError> {
    // A malformed/proxy-generated envelope should not be able to force an
    // unbounded recursive walk.  Normal responses are at most one or two
    // wrappers deep; the extra headroom covers nested Connect adapters.
    const MAX_ERROR_ENVELOPE_DEPTH: u8 = 8;

    if let Some(error) = json_error_direct(value) {
        return Some(error);
    }
    if depth >= MAX_ERROR_ENVELOPE_DEPTH {
        return None;
    }
    let Value::Object(object) = value else {
        return None;
    };
    for key in ["result", "response", "data", "payload"] {
        if let Some(child) = object.get(key)
            && let Some(error) = json_error_with_depth(child, depth + 1)
        {
            return Some(error);
        }
    }
    None
}

fn json_error_direct(value: &Value) -> Option<CursorError> {
    let error = value.get("error")?;
    if error.is_null() {
        return None;
    }
    let error_object = error.as_object();
    let code_value = error_object.and_then(|object| object.get("code"));
    let error_type_value = error_object
        .and_then(|object| object.get("errorType"))
        .or_else(|| error_object.and_then(|object| object.get("error_type")));
    let error_type = error_type_value
        .and_then(value_as_string)
        .filter(|value| !value.trim().is_empty());
    let raw_code = code_value
        .and_then(value_as_string)
        .filter(|value| !value.trim().is_empty());
    let code = raw_code
        .clone()
        .or_else(|| error_type.clone())
        .unwrap_or_else(|| "upstream_error".into());
    let mut message = error_object
        .and_then(|object| object.get("message"))
        .and_then(value_as_string)
        .filter(|value| !value.trim().is_empty())
        .or_else(|| value_as_string(error))
        .unwrap_or_else(|| "Sand inference upstream error".into());
    if let Some(error_type) = error_type.as_deref()
        && !message
            .to_ascii_lowercase()
            .contains(&error_type.to_ascii_lowercase())
    {
        message.push_str(" [errorType=");
        message.push_str(error_type);
        message.push(']');
    }
    let status = if error_object.is_some_and(|object| {
        object
            .get("isInputTokenLimitError")
            .or_else(|| object.get("is_input_token_limit_error"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || object
                .get("isOutputTokenLimitError")
                .or_else(|| object.get("is_output_token_limit_error"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
    }) {
        400
    } else {
        connect_error_status(code_value, error_type_value, &message)
    };
    // Preserve the old concise `code` detail and add `errorType` when it
    // carries independent classification. CursorError::client_message then
    // exposes both without dumping an arbitrarily large error envelope.
    let mut detail = match (raw_code, error_type) {
        (Some(code), Some(error_type)) => format!("code={code}; errorType={error_type}"),
        (Some(code), None) => code,
        (None, Some(error_type)) => format!("errorType={error_type}"),
        (None, None) => code,
    };
    // Current Cursor Sand responses can wrap an account-specific provider
    // 4xx in an outer `resource_exhausted` envelope. Keep the small inner
    // diagnostic fields in the client message so the request router can
    // rotate accounts even when the error arrived after HTTP 200.
    if let Some(provider) = sand_provider_error_metadata(value) {
        detail.push_str("; ");
        detail.push_str(&provider);
    }
    Some(CursorError::new(status, message, Some(detail)))
}

fn sand_provider_error_metadata(value: &Value) -> Option<String> {
    let bytes = serde_json::to_vec(value).ok()?;
    let parsed = parse_connect_error(&bytes)?;
    let mut fields = Vec::new();
    if let Some(code) = parsed.provider_error_code {
        fields.push(format!("providerErrorCode={code}"));
    }
    if let Some(status) = parsed.provider_status_code {
        fields.push(format!("providerStatusCode={status}"));
    }
    if let Some(retryable) = parsed.provider_is_retryable {
        fields.push(format!("isRetryable={retryable}"));
    }
    (!fields.is_empty()).then(|| fields.join(" "))
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(boolean) => Some(boolean.to_string()),
        _ => None,
    }
}

fn json_is_terminal(value: &Value) -> bool {
    let object = match value {
        Value::Object(object) => object,
        _ => return false,
    };
    if [
        "done",
        "finished",
        "isFinished",
        "is_finished",
        "endOfStream",
        "end_of_stream",
    ]
    .iter()
    .any(|key| object.get(*key).and_then(Value::as_bool).unwrap_or(false))
        || object
            .get("finishReason")
            .or_else(|| object.get("finish_reason"))
            .is_some_and(|reason| !reason.is_null() && reason.as_str() != Some(""))
    {
        return true;
    }

    // A few Connect gateways wrap the protobuf JSON in `result`/`response`.
    // Recurse only through those known envelopes so an `isFinal` flag nested
    // inside tool arguments or provider metadata cannot close the stream.
    ["result", "response", "data", "payload"]
        .iter()
        .filter_map(|key| object.get(*key))
        .any(json_is_terminal)
}

#[cfg(test)]
fn events_from_json(value: &Value) -> Vec<CursorStreamEvent> {
    // Standalone callers expect one JSON object to be self-contained.  The
    // streaming decoder keeps the map on `SandInferenceStream` instead so
    // argument fragments can span frames.
    let mut buffers = HashMap::new();
    let mut completed = HashSet::new();
    let mut events = events_from_json_with_state(value, &mut buffers, &mut completed);
    events.extend(flush_tool_buffers_to_events(&mut buffers));
    events
}

fn events_from_json_with_state(
    value: &Value,
    buffers: &mut HashMap<String, SandToolBuffer>,
    completed: &mut HashSet<String>,
) -> Vec<CursorStreamEvent> {
    let mut events = Vec::new();
    let mut text = Vec::new();
    let mut thinking = Vec::new();
    collect_text_parts(value, &mut text, &mut thinking);
    // Preserve wire order where possible: InferenceService normally emits one
    // part per frame, so this order is deterministic and avoids combining a
    // reasoning delta with visible output in one event.
    for part in thinking {
        if !part.is_empty() {
            events.push(CursorStreamEvent::ThinkingDelta { text: part });
        }
    }
    for part in text {
        if !part.is_empty() {
            events.push(CursorStreamEvent::TextDelta { text: part });
        }
    }
    if let Some(usage) = extract_usage(value) {
        events.push(CursorStreamEvent::Usage {
            input_tokens: usage.0,
            output_tokens: usage.1,
            cache_read_tokens: usage.2,
            cache_write_tokens: usage.3,
        });
    }
    let exact_parts = extract_tool_call_parts(value);
    if exact_parts.is_empty() {
        // Older gateways used `toolCall`/`functionCall`; retain that fallback
        // while avoiding duplicate events when the modern `toolCallPart`
        // envelope is present.
        for (id, name, input) in extract_tool_calls(value) {
            events.push(CursorStreamEvent::NativeTool {
                tool_use_id: id,
                name,
                input,
            });
        }
    } else {
        for part in exact_parts {
            if let Some(event) = ingest_tool_call_part(part, buffers, completed) {
                events.push(event);
            }
        }
    }
    if let Some(session_id) = extract_string(value, &["sessionId", "session_id", "conversationId"])
    {
        // A conversation id is useful to callers that need to persist the
        // binding, but avoid emitting it when it is merely echoed on every
        // frame and no event otherwise exists.
        if !session_id.is_empty() && events.is_empty() {
            events.push(CursorStreamEvent::Session { session_id });
        }
    }
    events
}

#[derive(Debug)]
struct SandToolPart {
    id: String,
    name: String,
    args: Option<Value>,
    done: bool,
    index: Option<i32>,
}

/// Collect the current InferenceService schema exactly.  The endpoint emits
/// `toolCallPart` (and, in some builds, `tool_call_part`) rather than the
/// AgentService `toolCall` envelope.  Parsing this key explicitly prevents
/// metadata nested inside a tool argument from being mistaken for another
/// call.
fn extract_tool_call_parts(value: &Value) -> Vec<SandToolPart> {
    let mut out = Vec::new();
    collect_tool_call_parts(value, &mut out);
    out
}

fn collect_tool_call_parts(value: &Value, out: &mut Vec<SandToolPart>) {
    let Value::Object(object) = value else {
        return;
    };
    for (key, child) in object {
        let lower = key.to_ascii_lowercase();
        if matches!(lower.as_str(), "toolcallpart" | "tool_call_part") {
            collect_tool_part_value(child, out);
            continue;
        }
        if !matches!(lower.as_str(), "input" | "arguments" | "args") {
            collect_tool_call_parts(child, out);
        }
    }
}

fn collect_tool_part_value(value: &Value, out: &mut Vec<SandToolPart>) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_tool_part_value(item, out);
            }
        }
        Value::Object(object) => {
            let id = ["toolCallId", "tool_call_id", "id", "callId", "call_id"]
                .iter()
                .find_map(|key| object.get(*key).and_then(value_as_string))
                .unwrap_or_default();
            let name = ["toolName", "tool_name", "name"]
                .iter()
                .find_map(|key| object.get(*key).and_then(value_as_string))
                .or_else(|| {
                    object
                        .get("function")
                        .and_then(|function| function.get("name"))
                        .and_then(value_as_string)
                })
                .unwrap_or_default();
            let args = [
                "args",
                "arguments",
                "input",
                "toolCallArgs",
                "tool_call_args",
                "argsText",
                "args_text",
            ]
            .iter()
            .find_map(|key| object.get(*key).cloned())
            .or_else(|| {
                // A few revisions call the incremental argument field
                // `delta`; only use it when this is a tool-part object.
                object.get("delta").cloned()
            });
            let done = [
                "done",
                "isDone",
                "is_done",
                "finished",
                "isFinished",
                "is_finished",
                "complete",
                "completed",
                "isComplete",
                "is_complete",
                "final",
                "isFinal",
                "is_final",
            ]
            .iter()
            .any(|key| object.get(*key).and_then(Value::as_bool).unwrap_or(false));
            let index = ["toolIndex", "tool_index", "index"]
                .iter()
                .find_map(|key| object.get(*key).and_then(Value::as_i64))
                .and_then(|value| i32::try_from(value).ok());
            if !id.is_empty() || !name.is_empty() || args.is_some() {
                out.push(SandToolPart {
                    id,
                    name,
                    args,
                    done,
                    index,
                });
            }
        }
        _ => {}
    }
}

fn ingest_tool_call_part(
    part: SandToolPart,
    buffers: &mut HashMap<String, SandToolBuffer>,
    completed: &mut HashSet<String>,
) -> Option<CursorStreamEvent> {
    // The protocol normally supplies toolCallId.  A tool index is a stable
    // fallback for builds that omit the id on continuation frames; anonymous
    // one-shot calls are still supported when the part is explicitly marked
    // complete.
    let id = if !part.id.is_empty() {
        part.id
    } else if let Some(index) = part.index {
        format!("sand_tool_index_{index}")
    } else if part.done {
        format!("sand_tool_anon_{}", buffers.len() + 1)
    } else {
        // There is no safe key with which to join an anonymous fragment.
        return None;
    };
    if completed.contains(&id) {
        return None;
    }
    let buffer = buffers.entry(id.clone()).or_default();
    if !part.name.is_empty() {
        buffer.name = part.name;
    }
    let structured_args = part.args.as_ref().is_some_and(|args| !args.is_string());
    if let Some(args) = part.args {
        match args {
            Value::String(fragment) => merge_tool_args_text(&mut buffer.args_text, &fragment),
            value => {
                // The current schema declares `args` as a string.  Older
                // gateways occasionally sent a structured value directly;
                // preserve it as-is (an argument named `text` is valid) rather
                // than mistaking that field for an incremental delta.
                buffer.args_value = Some(value);
            }
        }
    }
    buffer.complete |= part.done || structured_args;

    // `isComplete` is authoritative for string fragments.  Do not emit as
    // soon as a prefix happens to parse: Cursor may append more fields in a
    // later frame, and doing so creates duplicate/partial tool calls.
    if !buffer.complete {
        return None;
    }

    let buffer = buffers.remove(&id)?;
    let input = complete_tool_input(&buffer)?;
    if buffer.name.is_empty() {
        // A continuation may carry the final arguments before the initial
        // frame's tool name arrives. Keep the completed buffer around so a
        // later frame can supply that name instead of permanently dropping
        // an otherwise valid call.
        buffers.insert(id, buffer);
        return None;
    }
    completed.insert(id.clone());
    Some(CursorStreamEvent::NativeTool {
        tool_use_id: id,
        name: buffer.name,
        input,
    })
}

/// Merge an argument update from `InferenceToolCallStreamPart`.
///
/// Cursor revisions use both representations on the wire: some emit a true
/// delta per frame, while others repeat the complete argument prefix (and the
/// `isComplete` frame commonly repeats the final JSON object once more).  A
/// blind concatenation turns the latter into invalid JSON (`{}{}")` and makes
/// Claude Code discard an otherwise valid tool call.  Prefer the longer
/// cumulative value when one update contains the other, ignore exact
/// duplicates, and append only when the update is a genuine fragment.
fn merge_tool_args_text(existing: &mut String, update: &str) {
    if update.is_empty() {
        return;
    }
    if existing.is_empty() {
        existing.push_str(update);
        return;
    }
    if update == existing {
        return;
    }
    if update.starts_with(existing.as_str()) {
        // Cumulative update (for example `{"command":"pwd"}` after the
        // prefix `{"command":"p`).
        *existing = update.to_string();
        return;
    }
    if existing.starts_with(update) {
        // A stale/shorter cumulative update; retaining the longer value is
        // safer than truncating an argument assembled from prior frames.
        return;
    }
    // A repeated final object can arrive after an incremental sequence.  When
    // both values are independently valid JSON, the newer value is a
    // cumulative snapshot (or a repeated final snapshot), not another delta.
    // Prefer it instead of producing concatenated objects such as `{}{}'.
    if serde_json::from_str::<Value>(existing).is_ok()
        && serde_json::from_str::<Value>(update).is_ok()
    {
        *existing = update.to_string();
        return;
    }
    existing.push_str(update);
}

#[cfg(test)]
fn flush_tool_buffers_to_events(
    buffers: &mut HashMap<String, SandToolBuffer>,
) -> Vec<CursorStreamEvent> {
    let drained = std::mem::take(buffers);
    drained
        .into_iter()
        .filter_map(|(id, buffer)| {
            if buffer.name.is_empty() || !buffer.complete {
                return None;
            }
            let input = complete_tool_input(&buffer)?;
            Some(CursorStreamEvent::NativeTool {
                tool_use_id: id,
                name: buffer.name,
                input,
            })
        })
        .collect()
}

/// Parse only a completed tool buffer.  Returning `None` for malformed or
/// incomplete JSON is intentional: forwarding the raw fragment as a string
/// causes Anthropic clients to execute a malformed call and retry forever.
fn complete_tool_input(buffer: &SandToolBuffer) -> Option<Value> {
    if !buffer.complete {
        return None;
    }
    if let Some(value) = &buffer.args_value {
        return Some(value.clone());
    }
    if buffer.args_text.trim().is_empty() {
        return Some(Value::Object(Map::new()));
    }
    serde_json::from_str::<Value>(&buffer.args_text).ok()
}

fn collect_text_parts(value: &Value, text: &mut Vec<String>, thinking: &mut Vec<String>) {
    let Value::Object(object) = value else {
        return;
    };
    for (key, child) in object {
        let normalized = key.to_ascii_lowercase();
        let is_thinking = normalized.contains("thinking")
            || normalized.contains("reasoning")
            || normalized.contains("thought");
        let is_text_part = normalized == "text"
            || normalized == "textpart"
            || normalized == "text_part"
            || normalized == "contentpart"
            || normalized == "content_part"
            || normalized == "textdelta"
            || normalized == "text_delta";
        if is_thinking || is_text_part {
            if let Some(value) = text_from_value(child) {
                if is_thinking {
                    thinking.push(value);
                } else {
                    text.push(value);
                }
                continue;
            }
        }
        // These are response oneof branches whose nested strings are
        // metadata, tool arguments, or diagnostics rather than assistant
        // output.  Restricting recursion here prevents e.g. a tool argument
        // `{\"text\": ...}` or provider metadata from leaking into the
        // Anthropic text stream.
        if matches!(
            normalized.as_str(),
            "toolcallpart"
                | "tool_call_part"
                | "toolcall"
                | "tool_call"
                | "tooluse"
                | "tool_use"
                | "functioncall"
                | "function_call"
                | "responseinfo"
                | "response_info"
                | "providermetadata"
                | "provider_metadata"
                | "usage"
                | "extendedusage"
                | "extended_usage"
                | "error"
                | "invocationid"
                | "invocation_id"
        ) {
            continue;
        }
        // Recurse through `result`, `response`, and wrapper objects. Do not
        // recurse into arbitrary `input` maps, where a tool argument named
        // `text` must not become assistant output.
        if !matches!(normalized.as_str(), "input" | "arguments" | "args") {
            collect_text_parts(child, text, thinking);
        }
    }
}

fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Object(object) => ["text", "value", "delta", "content"]
            .iter()
            .find_map(|key| object.get(*key).and_then(text_from_value)),
        Value::Array(items) => {
            let joined = items.iter().filter_map(text_from_value).collect::<String>();
            (!joined.is_empty()).then_some(joined)
        }
        _ => None,
    }
}

fn extract_usage(value: &Value) -> Option<(u64, u64, u64, u64)> {
    let Value::Object(object) = value else {
        return None;
    };
    for key in [
        "usage",
        "extendedUsage",
        "extended_usage",
        "tokenUsage",
        "token_usage",
    ] {
        if let Some(candidate) = object.get(key)
            && let Some(usage) = usage_object(candidate)
        {
            return Some(usage);
        }
    }
    object.iter().find_map(|(key, child)| {
        let normalized = key.to_ascii_lowercase();
        if matches!(
            normalized.as_str(),
            "input"
                | "arguments"
                | "args"
                | "toolcallpart"
                | "tool_call_part"
                | "toolcall"
                | "tool_call"
                | "tooluse"
                | "tool_use"
                | "functioncall"
                | "function_call"
                | "responseinfo"
                | "response_info"
                | "providermetadata"
                | "provider_metadata"
                | "error"
        ) {
            return None;
        }
        extract_usage(child)
    })
}

fn usage_object(value: &Value) -> Option<(u64, u64, u64, u64)> {
    let Value::Object(object) = value else {
        return None;
    };
    let input = number_for_keys(
        object,
        &[
            "inputTokens",
            "input_tokens",
            "promptTokens",
            "prompt_tokens",
            "prompt",
        ],
    );
    let output = number_for_keys(
        object,
        &[
            "outputTokens",
            "output_tokens",
            "completionTokens",
            "completion_tokens",
            "completion",
        ],
    );
    let cache_read = number_for_keys(
        object,
        &[
            "cacheReadTokens",
            "cache_read_tokens",
            "cacheRead",
            "cache_read",
        ],
    );
    let cache_write = number_for_keys(
        object,
        &[
            "cacheWriteTokens",
            "cache_write_tokens",
            "cacheCreationInputTokens",
            "cache_creation_input_tokens",
            "cacheWrite",
            "cache_write",
        ],
    );
    (input > 0 || output > 0 || cache_read > 0 || cache_write > 0).then_some((
        input,
        output,
        cache_read,
        cache_write,
    ))
}

fn number_for_keys(object: &Map<String, Value>, keys: &[&str]) -> u64 {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(number_value))
        .unwrap_or(0)
}

fn number_value(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|number| u64::try_from(number).ok()))
        .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
}

fn extract_string(value: &Value, keys: &[&str]) -> Option<String> {
    let Value::Object(object) = value else {
        return None;
    };
    keys.iter()
        .find_map(|key| object.get(*key).and_then(value_as_string))
}

fn extract_tool_calls(value: &Value) -> Vec<(String, String, Value)> {
    let mut out = Vec::new();
    collect_tool_calls(value, &mut out);
    out
}

fn collect_tool_calls(value: &Value, out: &mut Vec<(String, String, Value)>) {
    let Value::Object(object) = value else {
        return;
    };
    for key in [
        "toolCall",
        "tool_call",
        "toolUse",
        "tool_use",
        "functionCall",
        "function_call",
    ] {
        if let Some(candidate) = object.get(key) {
            collect_tool_value(candidate, out);
        }
    }
    for (key, child) in object {
        let lower = key.to_ascii_lowercase();
        if !matches!(lower.as_str(), "input" | "arguments" | "args") {
            collect_tool_calls(child, out);
        }
    }
}

fn collect_tool_value(value: &Value, out: &mut Vec<(String, String, Value)>) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_tool_value(item, out);
            }
        }
        Value::Object(object) => {
            let id = ["id", "toolUseId", "tool_use_id", "callId", "call_id"]
                .iter()
                .find_map(|key| object.get(*key).and_then(value_as_string))
                .unwrap_or_else(|| format!("sand_tool_{}", out.len() + 1));
            let name = object
                .get("name")
                .or_else(|| object.get("toolName"))
                .or_else(|| object.get("tool_name"))
                .or_else(|| object.get("function").and_then(|f| f.get("name")))
                .and_then(value_as_string)
                .unwrap_or_else(|| "unknown_tool".into());
            let input = object
                .get("input")
                .or_else(|| object.get("arguments"))
                .or_else(|| object.get("args"))
                .or_else(|| object.get("function").and_then(|f| f.get("arguments")))
                .cloned()
                .unwrap_or_else(|| Value::Object(object.clone()));
            out.push((id, name, input));
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    #[test]
    fn request_uses_current_sand_json_shape() {
        let request = SandInferenceRequest::new(
            "claude-fable-5",
            "conv-1",
            "invoke-1",
            vec![
                SandInferenceMessage::system("system"),
                SandInferenceMessage::user("hello"),
            ],
        )
        .with_max_tokens(Some(1234));
        let value = request.to_json_value();
        assert_eq!(value["messages"][0]["role"], ROLE_SYSTEM);
        assert_eq!(value["messages"][1]["role"], ROLE_USER);
        assert_eq!(value["requestedModel"]["modelId"], "claude-fable-5");
        assert_eq!(
            value["requestedModel"]["parameters"],
            json!([{ "id": "context", "value": "1m" }])
        );
        assert_eq!(
            value["requestedModel"]["isVariantStringRepresentation"],
            false
        );
        assert_eq!(value["conversationId"], "conv-1");
        assert_eq!(value["invocationId"], "invoke-1");
        assert_eq!(value["tools"], json!([]));
        assert_eq!(value["providerDefinedTools"], json!([]));
        assert!(value.get("acceptedUnadvertisedToolNames").is_none());
        assert_eq!(value["modelConfig"]["maxTokens"], 1234);
    }

    #[test]
    fn request_forwards_catalog_effort_parameters() {
        let request = SandInferenceRequest::new(
            "claude-fable-5-thinking-max",
            "conv-1",
            "invoke-1",
            vec![SandInferenceMessage::user("hello")],
        );
        let value = request.to_json_value();
        let params = value["requestedModel"]["parameters"]
            .as_array()
            .expect("parameters array");
        assert!(
            params
                .iter()
                .any(|value| { value["id"] == "thinking" && value["value"] == "true" })
        );
        assert!(
            params
                .iter()
                .any(|value| { value["id"] == "effort" && value["value"] == "max" })
        );
    }

    #[test]
    fn request_can_keep_canonical_sand_id_and_catalog_parameters_separate() {
        let request = SandInferenceRequest::new(
            "claude-fable-5",
            "conv-1",
            "invoke-1",
            vec![SandInferenceMessage::user("hello")],
        )
        .with_parameter_model_id("claude-fable-5-thinking-max");
        let value = request.to_json_value();
        assert_eq!(value["modelId"], "claude-fable-5");
        assert_eq!(value["requestedModel"]["modelId"], "claude-fable-5");
        let params = value["requestedModel"]["parameters"]
            .as_array()
            .expect("parameters array");
        assert!(
            params
                .iter()
                .any(|value| { value["id"] == "thinking" && value["value"] == "true" })
        );
        assert!(
            params
                .iter()
                .any(|value| { value["id"] == "effort" && value["value"] == "max" })
        );
    }

    #[test]
    fn request_frame_has_five_byte_connect_header() {
        let request =
            SandInferenceRequest::new("grok-4.6", "c", "i", vec![SandInferenceMessage::user("x")]);
        let frame = request.encode_frame().unwrap();
        assert_eq!(frame[0], 0);
        let length = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
        assert_eq!(length, frame.len() - 5);
        assert_eq!(
            serde_json::from_slice::<Value>(&frame[5..]).unwrap()["requestedModel"]["modelId"],
            "grok-4.6"
        );
    }

    #[test]
    fn anthropic_images_use_data_uri_on_sand_wire() {
        let request: MessagesRequest = serde_json::from_value(json!({
            "model": "claude-fable-5",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "inspect"},
                    {"type": "image", "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": "aGVsbG8="
                    }}
                ]
            }]
        }))
        .unwrap();
        let messages = messages_from_anthropic(&request, false);
        let parts = &messages[0].parts;
        assert_eq!(parts[1]["image"]["data"], "data:image/png;base64,aGVsbG8=");
        assert_eq!(parts[1]["image"]["mimeType"], "image/png");
    }

    #[test]
    fn document_and_openai_file_blocks_use_native_file_part() {
        let request: MessagesRequest = serde_json::from_value(json!({
            "model": "claude-fable-5",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "document", "title": "report.pdf", "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": "aGVsbG8="
                    }},
                    {"type": "file", "file": {
                        "filename": "notes.txt",
                        "file_data": "data:text/plain;base64,aGVsbG8="
                    }}
                ]
            }]
        }))
        .unwrap();
        let messages = messages_from_anthropic(&request, false);
        let parts = &messages[0].parts;
        assert_eq!(parts.len(), 2);
        assert_eq!(
            parts[0]["file"]["data"],
            "data:application/pdf;base64,aGVsbG8="
        );
        assert_eq!(parts[0]["file"]["mediaType"], "application/pdf");
        // `title` is the Anthropic document filename fallback.
        assert_eq!(parts[0]["file"]["filename"], "report.pdf");
        assert_eq!(parts[1]["file"]["data"], "data:text/plain;base64,aGVsbG8=");
        assert_eq!(parts[1]["file"]["mediaType"], "text/plain");
        assert_eq!(parts[1]["file"]["filename"], "notes.txt");
    }

    #[test]
    fn text_documents_fall_back_to_text_without_base64_decoding() {
        let request: MessagesRequest = serde_json::from_value(json!({
            "model": "claude-fable-5",
            "messages": [{
                "role": "user",
                "content": [{"type": "document", "source": {
                    "type": "text", "media_type": "text/plain", "text": "plain document"
                }}]
            }]
        }))
        .unwrap();
        let messages = messages_from_anthropic(&request, false);
        assert_eq!(messages[0].text.as_deref(), Some("plain document"));
        assert!(messages[0].parts.is_empty());
    }

    #[test]
    fn response_json_maps_text_thinking_usage_and_tool() {
        let value = json!({
            "textPart": {"text": "answer"},
            "thinkingPart": {"text": "thought"},
            "usage": {"promptTokens": 11, "completionTokens": 7, "cacheReadTokens": 2},
            "toolCall": {"id": "call-1", "name": "Bash", "arguments": {"command": "pwd"}}
        });
        let events = events_from_json(&value);
        assert!(events.iter().any(
            |event| matches!(event, CursorStreamEvent::TextDelta { text } if text == "answer")
        ));
        assert!(events.iter().any(
            |event| matches!(event, CursorStreamEvent::ThinkingDelta { text } if text == "thought")
        ));
        assert!(events.iter().any(|event| matches!(
            event,
            CursorStreamEvent::Usage {
                input_tokens: 11,
                output_tokens: 7,
                cache_read_tokens: 2,
                ..
            }
        )));
        assert!(events.iter().any(|event| matches!(event, CursorStreamEvent::NativeTool { tool_use_id, name, .. } if tool_use_id == "call-1" && name == "Bash")));
    }

    #[test]
    fn tool_call_fragments_wait_for_is_complete_and_emit_structured_input_once() {
        let first = json!({
            "toolCallPart": {
                "toolCallId": "call-fragment",
                "toolName": "Bash",
                "args": "{\"command\":\"pw",
                "isComplete": false
            }
        });
        let second = json!({
            "toolCallPart": {
                "toolCallId": "call-fragment",
                "args": "d\"}",
                "isComplete": true
            }
        });
        let duplicate = json!({
            "toolCallPart": {
                "toolCallId": "call-fragment",
                "toolName": "Bash",
                "args": "{\"command\":\"pwd\"}",
                "isComplete": true
            }
        });

        let mut buffers = HashMap::new();
        let mut completed = HashSet::new();
        assert!(events_from_json_with_state(&first, &mut buffers, &mut completed).is_empty());
        let events = events_from_json_with_state(&second, &mut buffers, &mut completed);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            CursorStreamEvent::NativeTool {
                tool_use_id,
                name,
                input,
            } if tool_use_id == "call-fragment"
                && name == "Bash"
                && input == &json!({"command": "pwd"})
        ));
        assert!(events_from_json_with_state(&duplicate, &mut buffers, &mut completed).is_empty());
    }

    #[test]
    fn cumulative_tool_args_and_repeated_final_frame_emit_once() {
        // Current Cursor commonly sends an args-only prefix followed by a
        // complete frame that repeats the whole object.  The repeated value
        // must not be concatenated into invalid JSON.
        let name = json!({
            "toolCallPart": {
                "toolCallId": "call-cumulative",
                "toolName": "Read"
            }
        });
        let prefix = json!({
            "toolCallPart": {
                "toolCallId": "call-cumulative",
                "args": "{\"path\":\"/tmp/"
            }
        });
        let cumulative = json!({
            "toolCallPart": {
                "toolCallId": "call-cumulative",
                "args": "{\"path\":\"/tmp/file.txt\"}"
            }
        });
        let final_frame = json!({
            "toolCallPart": {
                "toolCallId": "call-cumulative",
                "toolName": "Read",
                "args": "{\"path\":\"/tmp/file.txt\"}",
                "isComplete": true
            }
        });
        let mut buffers = HashMap::new();
        let mut completed = HashSet::new();
        assert!(events_from_json_with_state(&name, &mut buffers, &mut completed).is_empty());
        assert!(events_from_json_with_state(&prefix, &mut buffers, &mut completed).is_empty());
        assert!(events_from_json_with_state(&cumulative, &mut buffers, &mut completed).is_empty());
        let events = events_from_json_with_state(&final_frame, &mut buffers, &mut completed);
        assert!(matches!(
            events.as_slice(),
            [CursorStreamEvent::NativeTool {
                tool_use_id,
                name,
                input,
            }] if tool_use_id == "call-cumulative"
                && name == "Read"
                && input == &json!({"path": "/tmp/file.txt"})
        ));
    }

    #[test]
    fn incomplete_tool_fragments_are_not_forwarded_as_string_arguments() {
        let value = json!({
            "toolCallPart": {
                "toolCallId": "call-incomplete",
                "toolName": "Bash",
                "args": "{\"command\":\"pwd",
                "isComplete": false
            }
        });
        let mut buffers = HashMap::new();
        let mut completed = HashSet::new();
        assert!(events_from_json_with_state(&value, &mut buffers, &mut completed).is_empty());
        assert!(flush_tool_buffers_to_events(&mut buffers).is_empty());
    }

    #[test]
    fn text_and_thinking_is_final_markers_are_not_stream_terminal() {
        // `isFinal` belongs to the individual text/thinking oneof part.  A
        // response may still carry usage, tool, or finish metadata in later
        // frames, so only stream-level markers may close the Connect stream.
        assert!(!json_is_terminal(&json!({
            "textPart": {"text": "done", "isFinal": true}
        })));
        assert!(!json_is_terminal(&json!({
            "thinkingPart": {"text": "done", "is_final": true}
        })));
        assert!(!json_is_terminal(&json!({
            "toolCallPart": {"args": "{\"isFinal\":true}", "isComplete": false}
        })));
    }

    #[test]
    fn text_part_is_final_does_not_drop_later_usage_or_end() {
        // Exercise the frame queue directly.  Constructing a reqwest response
        // from an `http::Response<Bytes>` does not provide a reliably closing
        // body stream on every reqwest version, while queue_frame is the exact
        // boundary where terminal state is decided.
        let mut stream = test_stream();
        stream.queue_json_value(&json!({
            "textPart": {"text": "done", "isFinal": true}
        }));
        assert!(!stream.ended);
        stream.queue_json_value(&json!({
            "usage": {"promptTokens": 11, "completionTokens": 7}
        }));
        stream.queue_frame(ConnectFrame {
            flags: FLAG_END,
            payload: Bytes::new(),
        });

        let events: Vec<_> = stream.pending.drain(..).filter_map(Result::ok).collect();
        assert!(
            events.iter().any(
                |event| matches!(event, CursorStreamEvent::TextDelta { text } if text == "done")
            )
        );
        assert!(events.iter().any(|event| matches!(
            event,
            CursorStreamEvent::Usage {
                input_tokens: 11,
                output_tokens: 7,
                ..
            }
        )));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, CursorStreamEvent::End))
                .count(),
            1
        );
    }

    #[test]
    fn clean_eof_after_text_emits_one_terminal_event() {
        let mut stream = test_stream();
        stream.queue_json_value(&json!({
            "textPart": {"text": "answer before EOF"}
        }));

        // A reverse proxy may strip the Connect END frame.  The stream must
        // still provide the terminal marker required by the Anthropic SSE
        // encoder after the final text delta has been queued.
        stream.finish_at_eof();
        let events: Vec<_> = stream.pending.drain(..).filter_map(Result::ok).collect();
        assert!(events.iter().any(|event| matches!(
            event,
            CursorStreamEvent::TextDelta { text } if text == "answer before EOF"
        )));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, CursorStreamEvent::End))
                .count(),
            1
        );

        // `finish_at_eof` can only be reached once from the HTTP body, but
        // keeping the call idempotent protects against wrapper streams that
        // report EOF more than once.
        stream.finish_at_eof();
        assert!(stream.pending.is_empty());
    }

    #[test]
    fn clean_eof_repairs_end_bit_seen_without_queued_terminal() {
        let mut stream = test_stream();
        stream.queue_json_value(&json!({
            "textPart": {"text": "answer"}
        }));

        // Model the narrow parser state in which a trailer has set `saw_end`
        // but no terminal event made it into the queue yet.  Checking
        // `terminal_emitted` in `finish_at_eof` closes this gap; checking only
        // `saw_end` would make the downstream encoder report a missing
        // `turn_ended` event.
        stream.saw_end = true;
        stream.finish_at_eof();
        assert_eq!(
            stream
                .pending
                .iter()
                .filter(|event| matches!(event, Ok(CursorStreamEvent::End)))
                .count(),
            1
        );
    }

    fn test_stream() -> SandInferenceStream {
        SandInferenceStream {
            bytes: Box::pin(futures_util::stream::empty()),
            decoder: ConnectFrameDecoder::new(),
            pending: VecDeque::new(),
            timeout_secs: 5,
            ended: false,
            saw_end: false,
            terminal_emitted: false,
            tool_buffers: HashMap::new(),
            completed_tool_ids: HashSet::new(),
        }
    }

    #[test]
    fn sand_tool_catalog_hides_internal_definitions() {
        let request: MessagesRequest = serde_json::from_value(json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "go"}],
            "tools": [
                {
                    "name": "Read",
                    "description": "Read a file",
                    "input_schema": {"type": "object"}
                },
                {
                    "name": "mcp__plugin__notify_post",
                    "description": "INTERNAL hook",
                    "input_schema": {"type": "object"}
                },
                {
                    "name": "TaskOutput",
                    "description": "DEPRECATED",
                    "input_schema": {"type": "object"}
                }
            ]
        }))
        .unwrap();
        let tools = tools_from_anthropic(&request, false);
        let names: Vec<&str> = tools
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .collect();
        assert_eq!(names, vec!["Read"]);
    }

    #[test]
    fn response_metadata_and_tool_arguments_do_not_become_output_or_usage() {
        let value = json!({
            "responseInfo": {"messages": [{"content": "metadata text"}]},
            "providerMetadata": {"metadata": {"text": "provider text"}},
            "toolCallPart": {
                "toolCallId": "call-1",
                "toolName": "Bash",
                "args": "{\"text\":\"argument text\",\"usage\":{\"inputTokens\":999}}",
                "isComplete": false
            }
        });
        let events = events_from_json(&value);
        assert!(!events.iter().any(|event| matches!(
            event,
            CursorStreamEvent::TextDelta { .. } | CursorStreamEvent::ThinkingDelta { .. }
        )));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, CursorStreamEvent::Usage { .. }))
        );
    }

    #[test]
    fn repeated_end_frames_emit_one_end_event() {
        let frame = encode_connect_frame(
            serde_json::to_vec(&json!({"textPart": {"text": "done"}})).unwrap(),
            0,
        );
        let end = encode_connect_frame([], FLAG_END);
        let end_with_json = encode_connect_frame(
            serde_json::to_vec(&json!({"finished": true})).unwrap(),
            FLAG_END,
        );
        let mut body = Vec::new();
        body.extend_from_slice(&frame);
        body.extend_from_slice(&end);
        body.extend_from_slice(&end_with_json);
        let mut stream = test_stream();
        let mut decoder = ConnectFrameDecoder::new();
        let frames = decoder.push(&body).unwrap();
        for frame in frames {
            stream.queue_frame(frame);
        }
        let mut ends = 0;
        let mut text = String::new();
        while let Some(item) = stream.pending.pop_front() {
            match item.unwrap() {
                CursorStreamEvent::End => ends += 1,
                CursorStreamEvent::TextDelta { text: part } => text.push_str(&part),
                _ => {}
            }
        }
        assert_eq!(text, "done");
        assert_eq!(ends, 1);
    }

    #[test]
    fn control_frames_are_ignored_before_json_decoding() {
        let mut stream = test_stream();
        stream.queue_frame(ConnectFrame {
            // Deliberately malformed payload: control frames are not model
            // JSON and must never become a stream error.
            flags: FLAG_CONTROL,
            payload: Bytes::from_static(b"not-json"),
        });
        stream.queue_frame(ConnectFrame {
            flags: 0,
            payload: Bytes::from_static(br#"{"textPart":{"text":"ok"}}"#),
        });
        stream.queue_frame(ConnectFrame {
            flags: FLAG_END,
            payload: Bytes::new(),
        });

        let events: Vec<_> = stream.pending.drain(..).collect();
        assert!(events.iter().all(Result::is_ok));
        assert!(events.iter().any(|event| matches!(
            event,
            Ok(CursorStreamEvent::TextDelta { text }) if text == "ok"
        )));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Ok(CursorStreamEvent::End)))
                .count(),
            1
        );
    }

    #[test]
    fn control_end_frame_still_emits_terminal_event() {
        let mut stream = test_stream();
        stream.queue_frame(ConnectFrame {
            // Desktop gateways may combine the binary trailer/control bit
            // with FLAG_END. The payload is intentionally not JSON.
            flags: FLAG_CONTROL | FLAG_END,
            payload: Bytes::from_static(b"trailer"),
        });

        let events: Vec<_> = stream.pending.drain(..).collect();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Ok(CursorStreamEvent::End)))
                .count(),
            1
        );
        assert!(stream.ended);
        assert!(stream.saw_end);
    }

    #[test]
    fn json_result_wrapper_and_terminal_marker_are_supported() {
        let value = json!({"result": {"textPart": {"text": "done"}, "finished": true}});
        let inner = value.get("result").unwrap();
        let mut events = events_from_json(inner);
        assert!(
            events.iter().any(
                |event| matches!(event, CursorStreamEvent::TextDelta { text } if text == "done")
            )
        );
        assert!(json_is_terminal(inner));
        events.push(CursorStreamEvent::End);
        assert!(matches!(events.last(), Some(CursorStreamEvent::End)));
    }

    #[test]
    fn decoder_accepts_split_frames_and_end() {
        let first = encode_connect_frame(br#"{"textPart":{"text":"hi"}}"#, 0);
        let end = encode_connect_frame([], FLAG_END);
        let mut decoder = ConnectFrameDecoder::new();
        let mut frames = Vec::new();
        frames.extend(decoder.push(&first[..3]).unwrap());
        frames.extend(decoder.push(&first[3..]).unwrap());
        frames.extend(decoder.push(&end).unwrap());
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].flags, 0);
        assert_eq!(frames[1].flags, FLAG_END);
    }

    #[test]
    fn connect_error_maps_to_status() {
        let payload = br#"{"error":{"code":"resource_exhausted","message":"busy"}}"#;
        let error = parse_connect_error(payload).unwrap();
        assert_eq!(error.status, 429);
    }

    #[test]
    fn sand_json_error_maps_numeric_error_type() {
        let cases = [
            (2, 400),
            (3, 400),
            (4, 429),
            (5, 401),
            (6, 403),
            (7, 503),
            (8, 400),
        ];
        for (error_type, status) in cases {
            let value = json!({
                "error": {
                    "errorType": error_type,
                    "message": "stream failed"
                }
            });
            let error = json_error(&value).unwrap();
            assert_eq!(error.status, status, "errorType={error_type}");
            assert!(error.message.contains("errorType"));
            assert!(
                error
                    .detail
                    .as_deref()
                    .unwrap_or_default()
                    .contains("errorType")
            );
        }
    }

    #[test]
    fn sand_json_error_maps_string_error_type_over_generic_code() {
        let value = json!({
            "error": {
                "code": "internal",
                "error_type": "ERROR_OVERLOADED",
                "message": "busy"
            }
        });
        let error = json_error(&value).unwrap();
        assert_eq!(error.status, 503);
        assert!(error.message.contains("ERROR_OVERLOADED"));
        assert!(error.client_message().contains("ERROR_OVERLOADED"));
    }

    #[test]
    fn sand_json_error_keeps_provider_metadata_for_account_failover() {
        let value = json!({
            "error": {
                "code": "resource_exhausted",
                "message": "provider rejected request",
                "details": [{
                    "debug": {
                        "error": "ERROR_PROVIDER_ERROR",
                        "details": {
                            "additionalInfo": {"providerStatusCode": 400},
                            "isRetryable": false
                        }
                    }
                }]
            }
        });
        let error = json_error(&value).expect("provider error");
        assert_eq!(error.status, 429, "outer quota envelope remains visible");
        let message = error.client_message();
        assert!(message.contains("ERROR_PROVIDER_ERROR"), "{message}");
        assert!(message.contains("providerStatusCode=400"), "{message}");
        assert!(message.contains("isRetryable=false"), "{message}");
        assert!(is_non_retryable_provider_error_message(&message));
    }

    #[test]
    fn sand_json_error_follows_known_response_envelopes_only() {
        let nested = json!({
            "result": {
                "response": {
                    "data": {
                        "payload": {
                            "error": {
                                "code": "resource_exhausted",
                                "message": "busy"
                            }
                        }
                    }
                }
            }
        });
        let error = json_error(&nested).expect("nested Sand error");
        assert_eq!(error.status, 429);
        assert!(error.client_message().contains("busy"));

        // Do not walk arbitrary response branches such as tool arguments:
        // model-provided JSON may legitimately contain an `error` key.
        let tool_argument = json!({
            "toolCallPart": {
                "toolCallId": "call-1",
                "args": {"error": {"code": "internal", "message": "argument"}}
            }
        });
        assert!(json_error(&tool_argument).is_none());
    }

    #[test]
    fn nested_sand_error_terminates_stream_before_unwrapping_result() {
        let mut stream = test_stream();
        stream.queue_json_value(&json!({
            "result": {
                "error": {"code": "overloaded", "message": "worker busy"}
            }
        }));
        assert!(stream.ended);
        assert!(stream.terminal_emitted);
        let item = stream.pending.pop_front().expect("queued error");
        let error = item.expect_err("nested error must be surfaced");
        assert_eq!(error.status, 503);
        assert!(error.client_message().contains("worker busy"));
    }

    #[test]
    fn sand_stream_retry_classifier_covers_transport_stalls_but_not_policy_errors() {
        let idle = CursorError::new(
            504,
            "Sand stream idle timeout after 45s with no useful progress",
            None,
        );
        assert!(stream_error_is_retryable(&idle));

        let active = CursorError::new(
            503,
            "A Cursor live run is already active for this session; retry after it advances",
            None,
        );
        assert!(stream_error_is_retryable(&active));

        let quota = CursorError::new(429, "ERROR_PRO_USER_RATE_LIMIT_EXCEEDED", None);
        assert!(!stream_error_is_retryable(&quota));

        let invalid = CursorError::new(400, "Sand traffic is not supported on this endpoint", None);
        assert!(!stream_error_is_retryable(&invalid));
    }

    #[tokio::test]
    async fn stream_type_is_send_and_can_be_polled_from_fixture() {
        // This test exercises the JSON mapper without opening a network
        // socket; compile-time use of StreamExt also guards the public API.
        let mut queue: VecDeque<Result<CursorStreamEvent, CursorError>> = VecDeque::new();
        queue.push_back(Ok(CursorStreamEvent::TextDelta { text: "x".into() }));
        assert!(matches!(
            queue.pop_front().unwrap(),
            Ok(CursorStreamEvent::TextDelta { .. })
        ));
        let _ = futures_util::stream::iter(Vec::<Result<CursorStreamEvent, CursorError>>::new())
            .next()
            .await;
    }
}
