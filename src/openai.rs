use crate::anthropic::schema::{Message, MessagesRequest};
use crate::anthropic::sse::{SseEvent, encode_sse_event, parse_sse_events};
use serde_json::{Map, Value, json};

pub fn catalog_model(id: &str, owned_by: &str) -> Value {
    // grok-build's official Grok path is OpenAI Responses (`/v1/responses`).
    // Custom `[model.*]` blocks default to Chat Completions if this field is
    // omitted; advertising `messages` forced Anthropic onto grok-build.
    let api_backend = "responses";
    let context_window = if id.contains("[1m]") || id.contains("fable") {
        1_000_000
    } else if owned_by == "grok" {
        500_000
    } else {
        256_000
    };
    let supports_reasoning_effort = match owned_by {
        "grok" | "codex" | "kimi" => true,
        "cursor" if cursor_honors_effort(id) => true,
        _ => false,
    };
    let (reasoning_effort, reasoning_efforts, supports_backend_search) = match (owned_by, id) {
        ("grok", "grok-4.6") => ("high", grok46_reasoning_efforts(), true),
        ("grok", "grok-4.5") => ("high", grok45_reasoning_efforts(), false),
        ("grok", _) => ("high", generic_reasoning_efforts(), false),
        ("cursor", _) if supports_reasoning_effort => ("high", cursor_reasoning_efforts(), false),
        _ if supports_reasoning_effort => ("high", generic_reasoning_efforts(), false),
        _ => ("high", json!([]), false),
    };
    let mut model = json!({
        "id": id,
        "model": id,
        "object": "model",
        "created": 0,
        "owned_by": owned_by,
        "api_backend": api_backend,
        "context_window": context_window,
        "supports_reasoning_effort": supports_reasoning_effort,
        "supports_backend_search": supports_backend_search,
    });
    if supports_reasoning_effort {
        model["reasoning_effort"] = json!(reasoning_effort);
        model["reasoning_efforts"] = reasoning_efforts;
    }
    model
}

fn grok46_reasoning_efforts() -> Value {
    json!([
        {
            "value": "xhigh",
            "label": "Extra High Effort",
            "description": "Highest effort and reasoning level"
        },
        {
            "value": "high",
            "label": "High Effort",
            "description": "Higher implementation quality with extensive reasoning",
            "default": true
        },
        {
            "value": "medium",
            "label": "Medium Effort",
            "description": "Balanced effort with standard implementation and testing"
        },
        {
            "value": "low",
            "label": "Low Effort",
            "description": "Quick, fast implementations"
        }
    ])
}

fn grok45_reasoning_efforts() -> Value {
    json!([
        {
            "value": "high",
            "label": "High Effort",
            "description": "Highest implementation quality with extensive reasoning",
            "default": true
        },
        {
            "value": "medium",
            "label": "Medium Effort",
            "description": "Balanced effort with standard implementation and testing"
        },
        {
            "value": "low",
            "label": "Low Effort",
            "description": "Quick, fast implementations"
        }
    ])
}

fn cursor_reasoning_efforts() -> Value {
    json!([
        {
            "value": "max",
            "label": "Max Effort",
            "description": "Highest implementation quality"
        },
        {
            "value": "high",
            "label": "High Effort",
            "description": "Higher implementation quality",
            "default": true
        },
        {
            "value": "medium",
            "label": "Medium Effort",
            "description": "Balanced effort"
        },
        {
            "value": "low",
            "label": "Low Effort",
            "description": "Quick, fast implementations"
        }
    ])
}

fn cursor_honors_effort(id: &str) -> bool {
    let lower = id.to_ascii_lowercase();
    if lower.contains("grok") || lower == "auto" {
        return false;
    }
    lower.contains("fable")
        || lower.contains("composer")
        || lower == "cursor"
        || lower == "cursor-agent"
        || lower == "cursor-plan"
        || lower == "cursor-ask"
        || lower.starts_with("cursor-composer")
}

fn generic_reasoning_efforts() -> Value {
    json!([
        {
            "value": "high",
            "label": "High Effort",
            "default": true
        },
        { "value": "medium", "label": "Medium Effort" },
        {
            "value": "low",
            "label": "Low Effort",
            "description": "Quick, fast implementations"
        }
    ])
}

pub fn responses_model(body: &Value) -> Option<String> {
    body.get("model")
        .and_then(Value::as_str)
        .filter(|model| !model.is_empty())
        .map(str::to_string)
}

pub fn responses_to_messages(body: &Value) -> anyhow::Result<MessagesRequest> {
    let model = responses_model(body);
    let max_tokens = body
        .get("max_output_tokens")
        .or_else(|| body.get("max_tokens"))
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok());
    let stream = body.get("stream").and_then(Value::as_bool).unwrap_or(true);
    let mut extra = Map::new();
    if let Some(instructions) = body.get("instructions") {
        extra.insert("system".into(), instructions.clone());
    }
    if let Some(tools) = body.get("tools") {
        extra.insert("tools".into(), convert_tools(tools)?);
    }
    if let Some(effort) = body.pointer("/reasoning/effort") {
        if let Some(value) = effort.as_str() {
            crate::providers::translate_shared::parse_effort_str(value)?;
        } else if !effort.is_null() {
            anyhow::bail!("Invalid reasoning.effort: must be a string");
        }
        extra.insert("output_config".into(), json!({ "effort": effort }));
    }
    if let Some(thinking) = body.get("thinking") {
        extra.insert("thinking".into(), thinking.clone());
    }
    if let Some(tool_choice) = body.get("tool_choice") {
        extra.insert("tool_choice".into(), convert_tool_choice(tool_choice));
    }
    if let Some(temperature) = body.get("temperature") {
        extra.insert("temperature".into(), temperature.clone());
    }
    if let Some(top_p) = body.get("top_p") {
        extra.insert("top_p".into(), top_p.clone());
    }
    if let Some(previous) = body.get("previous_response_id") {
        extra.insert("previous_response_id".into(), previous.clone());
    }
    let (messages, system_parts) = convert_input(body.get("input"))?;
    if !system_parts.is_empty() {
        let existing = extra
            .get("system")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let mut parts = Vec::new();
        if !existing.is_empty() {
            parts.push(existing);
        }
        parts.extend(system_parts);
        extra.insert("system".into(), json!(parts.join("\n")));
    }
    Ok(MessagesRequest {
        model,
        max_tokens,
        messages,
        stream,
        extra,
    })
}

fn convert_tools(tools: &Value) -> anyhow::Result<Value> {
    let Some(items) = tools.as_array() else {
        anyhow::bail!("tools must be an array");
    };
    let mut out = Vec::new();
    for tool in items {
        let kind = tool
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("function");
        match kind {
            "web_search" | "web_search_preview" => out.push(json!({
                "name": "WebSearch",
                "description": "Search the web",
                "input_schema": {"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}
            })),
            "x_search" => out.push(json!({
                "name": "XSearch",
                "description": "Search X",
                "input_schema": {"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}
            })),
            _ => {
                let name = tool
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("function tool lacks name"))?;
                let schema = tool
                    .get("parameters")
                    .or_else(|| tool.get("input_schema"))
                    .cloned()
                    .unwrap_or_else(|| json!({"type":"object"}));
                out.push(json!({
                    "name": name,
                    "description": tool.get("description").cloned().unwrap_or(json!("")),
                    "input_schema": schema
                }));
            }
        }
    }
    Ok(Value::Array(out))
}

fn convert_tool_choice(value: &Value) -> Value {
    match value {
        Value::String(choice) => match choice.as_str() {
            "required" => json!({"type": "any"}),
            "none" => json!({"type": "none"}),
            _ => json!({"type": "auto"}),
        },
        Value::Object(object) if object.get("type").and_then(Value::as_str) == Some("function") => {
            json!({
                "type": "tool",
                "name": object.get("name").cloned().unwrap_or(json!(""))
            })
        }
        other => other.clone(),
    }
}

fn convert_input(input: Option<&Value>) -> anyhow::Result<(Vec<Message>, Vec<String>)> {
    let Some(input) = input else {
        return Ok((Vec::new(), Vec::new()));
    };
    if let Some(text) = input.as_str() {
        return Ok((
            vec![Message {
                role: "user".into(),
                content: json!(text),
            }],
            Vec::new(),
        ));
    }
    let Some(items) = input.as_array() else {
        anyhow::bail!("input must be a string or array");
    };
    let mut messages = Vec::new();
    let mut system_parts = Vec::new();
    for item in items {
        let kind = item
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("message");
        match kind {
            "message" => {
                let role = item
                    .get("role")
                    .and_then(Value::as_str)
                    .unwrap_or("user")
                    .to_string();
                if role == "system" {
                    let text = flatten_text_content(item.get("content"));
                    if !text.is_empty() {
                        system_parts.push(text);
                    }
                    continue;
                }
                messages.push(Message {
                    role,
                    content: convert_message_content(item.get("content"))?,
                });
            }
            "reasoning" => {
                let text = reasoning_text(item);
                if !text.is_empty() {
                    messages.push(Message {
                        role: "assistant".into(),
                        content: json!([{ "type": "thinking", "thinking": text }]),
                    });
                }
            }
            "function_call" => {
                let id = item
                    .get("call_id")
                    .or_else(|| item.get("id"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("function_call lacks call_id"))?;
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("function_call lacks name"))?;
                let arguments = item.get("arguments").cloned().unwrap_or(json!("{}"));
                let input = match arguments {
                    Value::String(raw) => serde_json::from_str(&raw).unwrap_or(json!({})),
                    other => other,
                };
                messages.push(Message {
                    role: "assistant".into(),
                    content: json!([{
                        "type": "tool_use",
                        "id": id,
                        "name": name,
                        "input": input
                    }]),
                });
            }
            "function_call_output" => {
                let id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("function_call_output lacks call_id"))?;
                let output = item.get("output").cloned().unwrap_or(json!(""));
                let content = match output {
                    Value::String(text) => json!(text),
                    other => json!(other.to_string()),
                };
                messages.push(Message {
                    role: "user".into(),
                    content: json!([{
                        "type": "tool_result",
                        "tool_use_id": id,
                        "content": content
                    }]),
                });
            }
            _ => {}
        }
    }
    Ok((messages, system_parts))
}

fn flatten_text_content(content: Option<&Value>) -> String {
    match content {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| {
                part.get("text")
                    .and_then(Value::as_str)
                    .or_else(|| part.as_str())
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn reasoning_text(item: &Value) -> String {
    for key in ["content", "summary"] {
        if let Some(parts) = item.get(key).and_then(Value::as_array) {
            let text = parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("");
            if !text.is_empty() {
                return text;
            }
        }
    }
    item.get("text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn convert_message_content(content: Option<&Value>) -> anyhow::Result<Value> {
    let Some(content) = content else {
        return Ok(json!(""));
    };
    if let Some(text) = content.as_str() {
        return Ok(json!(text));
    }
    let Some(parts) = content.as_array() else {
        anyhow::bail!("message content must be text or parts");
    };
    let mut blocks = Vec::new();
    for part in parts {
        let kind = part.get("type").and_then(Value::as_str).unwrap_or("text");
        match kind {
            "input_text" | "output_text" | "text" => {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    blocks.push(json!({"type":"text","text": text}));
                }
            }
            "input_image" | "image" => {
                if let Some(url) = part.get("image_url").and_then(|value| {
                    value
                        .as_str()
                        .or_else(|| value.get("url").and_then(Value::as_str))
                }) {
                    if let Some(image) = data_uri_image(url)? {
                        blocks.push(image);
                    } else {
                        blocks.push(json!({
                            "type": "image",
                            "source": { "type": "url", "url": url }
                        }));
                    }
                } else if let Some(source) = part.get("source") {
                    blocks.push(json!({"type":"image","source": source}));
                }
            }
            _ => {}
        }
    }
    Ok(Value::Array(blocks))
}

const MAX_DATA_URI_CHARS: usize = 15 * 1024 * 1024;

fn data_uri_image(url: &str) -> anyhow::Result<Option<Value>> {
    let Some(rest) = url.strip_prefix("data:") else {
        return Ok(None);
    };
    let Some((meta, data)) = rest.split_once(',') else {
        anyhow::bail!("invalid data-URI image");
    };
    if data.len() > MAX_DATA_URI_CHARS {
        anyhow::bail!("data-URI image exceeds the size limit");
    }
    if !meta.contains("base64") {
        anyhow::bail!("data-URI image must be base64");
    }
    let media_type = meta.split(';').next().unwrap_or("image/png");
    if !media_type.starts_with("image/") {
        anyhow::bail!("data-URI must be an image");
    }
    Ok(Some(json!({
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": media_type,
            "data": data
        }
    })))
}

#[derive(Default)]
pub struct AnthropicToResponses {
    buffer: Vec<u8>,
    id: String,
    model: String,
    started: bool,
    finished: bool,
    failed: bool,
    sequence: u64,
    input_tokens: u64,
    output_tokens: u64,
    cached_tokens: u64,
    tool_index: usize,
    next_output_index: usize,
    text_output_index: Option<usize>,
    tool_output_indexes: Vec<usize>,
    text_item_id: Option<String>,
    text: String,
    reasoning_text: String,
    tool_item_ids: Vec<String>,
    output_items: Vec<Value>,
    stop_reason: Option<String>,
}

impl AnthropicToResponses {
    pub fn new(id: String, model: String) -> Self {
        Self {
            id,
            model,
            ..Self::default()
        }
    }

    pub fn push(&mut self, chunk: &[u8]) -> Vec<u8> {
        self.buffer.extend_from_slice(chunk);
        let mut out = Vec::new();
        if let Some(split) = self
            .buffer
            .windows(2)
            .rposition(|window| window == b"\n\n")
            .map(|index| index + 2)
        {
            let ready: Vec<u8> = self.buffer.drain(..split).collect();
            for event in parse_sse_events(&ready) {
                out.extend(self.render(&event));
            }
        }
        out
    }

    pub fn finish(&mut self) -> Vec<u8> {
        let rest = std::mem::take(&mut self.buffer);
        let mut out = Vec::new();
        for event in parse_sse_events(&rest) {
            out.extend(self.render(&event));
        }
        if self.started && !self.finished && !self.failed {
            if self.has_deliverable_output() {
                // grok-build retries a failed Responses stream. A text (or
                // tool_use) turn that lost `message_stop` must complete.
                out.extend(self.completed());
            } else {
                out.extend(self.fail("upstream stream ended without completion"));
            }
        }
        out
    }

    pub fn fail(&mut self, message: &str) -> Vec<u8> {
        if self.finished || self.failed {
            return Vec::new();
        }
        let mut out = Vec::new();
        if !self.started {
            self.started = true;
            out.extend(self.emit(json!({
                "type": "response.created",
                "response": self.base_response("in_progress", json!([]))
            })));
        }
        out.extend(self.failed_event(message));
        out
    }

    fn render(&mut self, event: &SseEvent) -> Vec<u8> {
        if self.finished || self.failed {
            return Vec::new();
        }
        let Ok(value) = serde_json::from_str::<Value>(&event.data) else {
            return Vec::new();
        };
        let typ = value.get("type").and_then(Value::as_str).unwrap_or("");
        match typ {
            "message_start" => {
                if let Some(model) = value.pointer("/message/model").and_then(Value::as_str) {
                    self.model = model.to_string();
                }
                if let Some(usage) = value.pointer("/message/usage") {
                    self.ingest_usage(usage);
                }
                self.started = true;
                self.emit(json!({
                    "type": "response.created",
                    "response": self.base_response("in_progress", json!([]))
                }))
            }
            "content_block_delta" => {
                let delta = value.get("delta").cloned().unwrap_or(Value::Null);
                match delta.get("type").and_then(Value::as_str) {
                    Some("thinking_delta") => {
                        if let Some(text) = delta.get("thinking").and_then(Value::as_str) {
                            self.reasoning_text.push_str(text);
                            self.emit(json!({
                                "type": "response.reasoning_text.delta",
                                "item_id": format!("rs_{}", self.id),
                                "output_index": 0,
                                "content_index": 0,
                                "delta": text
                            }))
                        } else {
                            Vec::new()
                        }
                    }
                    Some("text_delta") => {
                        self.text_delta(delta.get("text").and_then(Value::as_str))
                    }
                    Some("input_json_delta") => {
                        if let Some(partial) = delta.get("partial_json").and_then(Value::as_str) {
                            let index = self.tool_index.saturating_sub(1);
                            if let Some(item) = self.output_items.get_mut(index) {
                                let current = item
                                    .get("arguments")
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_string();
                                item["arguments"] = json!(format!("{current}{partial}"));
                            }
                            let item_id = self
                                .tool_item_ids
                                .get(index)
                                .cloned()
                                .unwrap_or_else(|| format!("fc_{index}"));
                            self.emit(json!({
                                "type": "response.function_call_arguments.delta",
                                "item_id": item_id,
                                "output_index": self.tool_output_index(index),
                                "delta": partial
                            }))
                        } else {
                            Vec::new()
                        }
                    }
                    _ => Vec::new(),
                }
            }
            "content_block_start" => {
                let block = value.get("content_block").cloned().unwrap_or(Value::Null);
                if block.get("type").and_then(Value::as_str) == Some("tool_use") {
                    self.tool_index += 1;
                    let call_id = block
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    let name = block.get("name").cloned().unwrap_or(json!(""));
                    let item_id = format!("fc_{call_id}");
                    self.tool_item_ids.push(item_id.clone());
                    let output_index = self.allocate_output_index();
                    self.tool_output_indexes.push(output_index);
                    let item = json!({
                        "type": "function_call",
                        "id": item_id,
                        "call_id": call_id,
                        "name": name,
                        "arguments": "",
                        "status": "in_progress"
                    });
                    self.output_items.push(item.clone());
                    self.emit(json!({
                        "type": "response.output_item.added",
                        "output_index": output_index,
                        "item": item
                    }))
                } else {
                    Vec::new()
                }
            }
            "message_delta" => {
                if let Some(reason) = value
                    .pointer("/delta/stop_reason")
                    .and_then(Value::as_str)
                    .filter(|reason| !reason.is_empty())
                {
                    self.stop_reason = Some(reason.to_string());
                }
                if let Some(usage) = value.get("usage") {
                    self.ingest_usage(usage);
                }
                Vec::new()
            }
            "message_stop" => self.completed(),
            "error" => {
                let message = value
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("upstream error");
                self.fail(message)
            }
            "ping" => {
                // grok-build /v1/responses idle-watchdogs on a silent body.
                // Anthropic `ping` must become Responses bytes, not drop.
                let mut out = Vec::new();
                if !self.started {
                    self.started = true;
                    out.extend(self.emit(json!({
                        "type": "response.created",
                        "response": self.base_response("in_progress", json!([]))
                    })));
                }
                out.extend(self.emit(json!({
                    "type": "response.in_progress",
                    "response": self.base_response(
                        "in_progress",
                        json!(self.output_items.clone())
                    )
                })));
                out
            }
            _ => Vec::new(),
        }
    }

    fn text_delta(&mut self, text: Option<&str>) -> Vec<u8> {
        let Some(text) = text else {
            return Vec::new();
        };
        self.text.push_str(text);
        let mut out = Vec::new();
        if self.text_item_id.is_none() {
            let item_id = format!("msg_{}", self.id);
            let output_index = self.allocate_output_index();
            self.text_item_id = Some(item_id.clone());
            self.text_output_index = Some(output_index);
            out.extend(self.emit(json!({
                "type": "response.output_item.added",
                "output_index": output_index,
                "item": {
                    "type": "message",
                    "id": item_id,
                    "role": "assistant",
                    "status": "in_progress",
                    "content": []
                }
            })));
            out.extend(self.emit(json!({
                "type": "response.content_part.added",
                "item_id": item_id,
                "output_index": output_index,
                "content_index": 0,
                "part": { "type": "output_text", "text": "", "annotations": [], "logprobs": [] }
            })));
        }
        let item_id = self.text_item_id.clone().unwrap_or_default();
        let output_index = self.text_output_index.unwrap_or(0);
        out.extend(self.emit(json!({
            "type": "response.output_text.delta",
            "item_id": item_id,
            "output_index": output_index,
            "content_index": 0,
            "delta": text,
            "logprobs": []
        })));
        out
    }

    fn has_deliverable_output(&self) -> bool {
        !self.text.is_empty() || !self.output_items.is_empty() || !self.reasoning_text.is_empty()
    }

    fn completed(&mut self) -> Vec<u8> {
        if self.finished || self.failed {
            return Vec::new();
        }
        if self.stop_reason.as_deref() == Some("max_tokens") {
            return self.incomplete();
        }
        self.finished = true;
        let mut out = self.close_open_items();
        out.extend(self.emit(json!({
            "type": "response.completed",
            "response": self.base_response("completed", self.final_output())
        })));
        out.extend(emit_data(Value::String("[DONE]".into())));
        out
    }

    fn incomplete(&mut self) -> Vec<u8> {
        self.finished = true;
        let mut out = self.close_open_items();
        let mut response = self.base_response("incomplete", self.final_output());
        response["incomplete_details"] = json!({ "reason": "max_output_tokens" });
        out.extend(self.emit(json!({
            "type": "response.incomplete",
            "response": response
        })));
        out.extend(emit_data(Value::String("[DONE]".into())));
        out
    }

    fn close_open_items(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        if let Some(item_id) = self.text_item_id.clone() {
            let output_index = self.text_output_index.unwrap_or(0);
            out.extend(self.emit(json!({
                "type": "response.output_text.done",
                "item_id": item_id,
                "output_index": output_index,
                "content_index": 0,
                "text": self.text,
                "logprobs": []
            })));
            out.extend(self.emit(json!({
                "type": "response.content_part.done",
                "item_id": item_id,
                "output_index": output_index,
                "content_index": 0,
                "part": {
                    "type": "output_text",
                    "text": self.text,
                    "annotations": [],
                    "logprobs": []
                }
            })));
            out.extend(self.emit(json!({
                "type": "response.output_item.done",
                "output_index": output_index,
                "item": {
                    "type": "message",
                    "id": item_id,
                    "role": "assistant",
                    "status": "completed",
                    "content": [{
                        "type": "output_text",
                        "text": self.text,
                        "annotations": [],
                        "logprobs": []
                    }]
                }
            })));
        }
        for (index, mut item) in self.output_items.clone().into_iter().enumerate() {
            item["status"] = json!("completed");
            out.extend(self.emit(json!({
                "type": "response.function_call_arguments.done",
                "item_id": item.get("id").cloned().unwrap_or(json!("")),
                "output_index": self.tool_output_index(index),
                "arguments": item.get("arguments").cloned().unwrap_or(json!(""))
            })));
            out.extend(self.emit(json!({
                "type": "response.output_item.done",
                "output_index": self.tool_output_index(index),
                "item": item
            })));
        }
        out
    }

    fn failed_event(&mut self, message: &str) -> Vec<u8> {
        self.failed = true;
        self.finished = true;
        let mut response = self.base_response("failed", self.final_output());
        response["error"] = json!({
            "code": "server_error",
            "message": message
        });
        let mut out = self.emit(json!({
            "type": "response.failed",
            "response": response
        }));
        out.extend(emit_data(Value::String("[DONE]".into())));
        out
    }

    fn final_output(&self) -> Value {
        let mut output = Vec::new();
        if !self.reasoning_text.is_empty() {
            output.push(json!({
                "type": "reasoning",
                "id": format!("rs_{}", self.id),
                "summary": [{
                    "type": "summary_text",
                    "text": self.reasoning_text
                }]
            }));
        }
        if let Some(item_id) = &self.text_item_id {
            output.push(json!({
                "type": "message",
                "id": item_id,
                "role": "assistant",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "text": self.text,
                    "annotations": [],
                    "logprobs": []
                }]
            }));
        }
        output.extend(self.output_items.iter().cloned().map(|mut item| {
            item["status"] = json!("completed");
            item
        }));
        Value::Array(output)
    }

    fn base_response(&self, status: &str, output: Value) -> Value {
        json!({
            "id": self.id,
            "object": "response",
            "created_at": 0,
            "model": self.model,
            "status": status,
            "output": output,
            "usage": self.usage_object()
        })
    }

    fn usage_object(&self) -> Value {
        json!({
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self
                .input_tokens
                .saturating_add(self.output_tokens)
                .saturating_add(self.cached_tokens),
            "input_tokens_details": { "cached_tokens": self.cached_tokens },
            "output_tokens_details": { "reasoning_tokens": 0 }
        })
    }

    fn ingest_usage(&mut self, usage: &Value) {
        if let Some(input) = usage.get("input_tokens").and_then(Value::as_u64) {
            self.input_tokens = input;
        }
        if let Some(output) = usage.get("output_tokens").and_then(Value::as_u64) {
            self.output_tokens = output;
        }
        if let Some(cached) = usage.get("cache_read_input_tokens").and_then(Value::as_u64) {
            self.cached_tokens = cached;
        }
    }

    fn allocate_output_index(&mut self) -> usize {
        let index = self.next_output_index;
        self.next_output_index = self.next_output_index.saturating_add(1);
        index
    }

    fn tool_output_index(&self, tool_index: usize) -> usize {
        self.tool_output_indexes
            .get(tool_index)
            .copied()
            .unwrap_or(tool_index)
    }

    fn emit(&mut self, mut value: Value) -> Vec<u8> {
        if let Some(object) = value.as_object_mut() {
            object
                .entry("sequence_number")
                .or_insert_with(|| json!(self.next_seq()));
        }
        emit_data(value)
    }

    fn next_seq(&mut self) -> u64 {
        let sequence = self.sequence;
        self.sequence = self.sequence.saturating_add(1);
        sequence
    }
}

pub fn messages_json_to_responses(value: &Value, id: &str, model: &str) -> Value {
    let mut translator = AnthropicToResponses::new(id.to_string(), model.to_string());
    let sse = anthropic_message_to_sse(value, model);
    let mut bytes = translator.push(&sse);
    bytes.extend(translator.finish());
    terminal_response(&bytes).unwrap_or_else(|| {
        let mut translator = AnthropicToResponses::new(id.to_string(), model.to_string());
        let failed = translator.fail("upstream response is not a Messages object");
        terminal_response(&failed).unwrap_or_else(|| {
            json!({
                "id": id,
                "object": "response",
                "created_at": 0,
                "model": model,
                "status": "failed",
                "output": [],
                "usage": {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_tokens": 0,
                    "input_tokens_details": { "cached_tokens": 0 },
                    "output_tokens_details": { "reasoning_tokens": 0 }
                },
                "error": { "code": "server_error", "message": "upstream response is not a Messages object" }
            })
        })
    })
}

fn anthropic_message_to_sse(value: &Value, model: &str) -> Vec<u8> {
    let resolved_model = value.get("model").and_then(Value::as_str).unwrap_or(model);
    let mut events = encode_sse_event(
        Some("message_start"),
        &json!({
            "type": "message_start",
            "message": { "model": resolved_model }
        })
        .to_string(),
    );
    let content = value
        .get("content")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for (index, block) in content.iter().enumerate() {
        match block.get("type").and_then(Value::as_str).unwrap_or("") {
            "text" => {
                let text = block.get("text").and_then(Value::as_str).unwrap_or("");
                events.extend(encode_sse_event(
                    Some("content_block_delta"),
                    &json!({
                        "type": "content_block_delta",
                        "index": index,
                        "delta": { "type": "text_delta", "text": text }
                    })
                    .to_string(),
                ));
            }
            "tool_use" => {
                events.extend(encode_sse_event(
                    Some("content_block_start"),
                    &json!({
                        "type": "content_block_start",
                        "index": index,
                        "content_block": {
                            "type": "tool_use",
                            "id": block.get("id").cloned().unwrap_or(json!("")),
                            "name": block.get("name").cloned().unwrap_or(json!(""))
                        }
                    })
                    .to_string(),
                ));
                let input = block.get("input").cloned().unwrap_or(json!({}));
                let partial = match input {
                    Value::String(raw) => raw,
                    other => other.to_string(),
                };
                events.extend(encode_sse_event(
                    Some("content_block_delta"),
                    &json!({
                        "type": "content_block_delta",
                        "index": index,
                        "delta": { "type": "input_json_delta", "partial_json": partial }
                    })
                    .to_string(),
                ));
            }
            "thinking" => {
                let text = block.get("thinking").and_then(Value::as_str).unwrap_or("");
                events.extend(encode_sse_event(
                    Some("content_block_delta"),
                    &json!({
                        "type": "content_block_delta",
                        "index": index,
                        "delta": { "type": "thinking_delta", "thinking": text }
                    })
                    .to_string(),
                ));
            }
            _ => {}
        }
    }
    events.extend(encode_sse_event(
        Some("message_delta"),
        &json!({
            "type": "message_delta",
            "delta": { "stop_reason": value.get("stop_reason").cloned().unwrap_or(Value::Null) },
            "usage": value.get("usage").cloned().unwrap_or(json!({}))
        })
        .to_string(),
    ));
    events.extend(encode_sse_event(
        Some("message_stop"),
        r#"{"type":"message_stop"}"#,
    ));
    events
}

fn terminal_response(bytes: &[u8]) -> Option<Value> {
    let text = String::from_utf8_lossy(bytes);
    let mut last = None;
    for block in text.split("\n\n") {
        let Some(data) = block.lines().find_map(|line| line.strip_prefix("data: ")) else {
            continue;
        };
        if data == "[DONE]" {
            continue;
        }
        let Ok(event) = serde_json::from_str::<Value>(data) else {
            continue;
        };
        if matches!(
            event.get("type").and_then(Value::as_str),
            Some("response.completed" | "response.incomplete" | "response.failed")
        ) {
            last = event.get("response").cloned();
        }
    }
    last
}

fn emit_data(value: Value) -> Vec<u8> {
    match value {
        Value::String(data) if data == "[DONE]" => encode_sse_event(None, "[DONE]"),
        other => encode_sse_event(None, &other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_marks_cursor_as_responses() {
        let model = catalog_model("cursor-grok-4.6-xhigh-fast", "cursor");
        assert_eq!(
            model["api_backend"], "responses",
            "grok-build official wire is /v1/responses, not Anthropic Messages"
        );
        let fable = catalog_model("claude-fable-5[1m]", "cursor");
        assert_eq!(fable["api_backend"], "responses");
    }

    #[test]
    fn catalog_marks_grok_as_responses() {
        let model = catalog_model("grok-4.6", "grok");
        assert_eq!(model["api_backend"], "responses");
        assert_eq!(model["context_window"], 500_000);
        assert_eq!(model["model"], "grok-4.6");
        assert_eq!(model["supports_reasoning_effort"], true);
        assert!(
            model["reasoning_efforts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["value"] == "low")
        );
    }

    #[test]
    fn responses_to_messages_maps_function_round_trip() {
        let body = json!({
            "model": "claude-fable-5[1m]",
            "instructions": "rules",
            "input": [
                {"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]},
                {"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"q\":1}"},
                {"type":"function_call_output","call_id":"call_1","output":"ok"}
            ],
            "tools": [{"type":"function","name":"lookup","parameters":{"type":"object"}}],
            "reasoning": {"effort": "high"},
            "stream": true
        });
        let request = responses_to_messages(&body).unwrap();
        assert_eq!(request.model.as_deref(), Some("claude-fable-5[1m]"));
        assert_eq!(request.messages.len(), 3);
        assert_eq!(request.extra["output_config"]["effort"], "high");
        assert_eq!(request.extra["tools"][0]["name"], "lookup");
    }

    #[test]
    fn anthropic_to_responses_finish_emits_completed_once() {
        let mut translator =
            AnthropicToResponses::new("resp_1".into(), "claude-fable-5[1m]".into());
        let first = translator.push(
            br#"event: message_start
data: {"type":"message_start","message":{"model":"claude-fable-5[1m]"}}

event: content_block_delta
data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}

event: message_stop
data: {"type":"message_stop"}

"#,
        );
        let first_text = String::from_utf8(first).unwrap();
        assert!(first_text.contains("response.created"));
        assert!(first_text.contains("response.output_text.delta"));
        assert_eq!(first_text.matches("response.completed").count(), 1);
        let rest = translator.finish();
        assert!(
            !String::from_utf8(rest)
                .unwrap()
                .contains("response.completed")
        );
    }

    #[test]
    fn anthropic_to_responses_finish_completes_when_text_arrived_without_stop() {
        let mut translator =
            AnthropicToResponses::new("resp_idle".into(), "cursor-grok-4.5-high-fast".into());
        translator.push(
            br#"event: message_start
data: {"type":"message_start","message":{"model":"cursor-grok-4.5-high-fast"}}

event: content_block_delta
data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"tools restored"}}

"#,
        );
        let rest = String::from_utf8(translator.finish()).unwrap();
        assert!(
            rest.contains("response.completed"),
            "text-only Anthropic cutoff must complete so grok-build does not retry: {rest}"
        );
        assert!(
            !rest.contains("server_error"),
            "a delivered text turn must not become response.failed: {rest}"
        );
    }

    #[test]
    fn anthropic_to_responses_finish_fails_when_started_with_no_output() {
        let mut translator =
            AnthropicToResponses::new("resp_empty".into(), "cursor-grok-4.5-high-fast".into());
        translator.push(
            br#"event: message_start
data: {"type":"message_start","message":{"model":"cursor-grok-4.5-high-fast"}}

"#,
        );
        let rest = String::from_utf8(translator.finish()).unwrap();
        assert!(
            rest.contains("upstream stream ended without completion"),
            "empty cutoff must still fail closed: {rest}"
        );
    }

    #[test]
    fn anthropic_ping_emits_responses_in_progress() {
        let mut translator =
            AnthropicToResponses::new("resp_ping".into(), "cursor-grok-4.6".into());
        let started = translator.push(
            br#"event: message_start
data: {"type":"message_start","message":{"model":"cursor-grok-4.6"}}

"#,
        );
        assert!(
            String::from_utf8(started)
                .unwrap()
                .contains("response.created")
        );
        let ping = translator.push(
            br#"event: ping
data: {"type":"ping"}

"#,
        );
        let ping_text = String::from_utf8(ping).unwrap();
        assert!(
            ping_text.contains("response.in_progress"),
            "Anthropic ping must become Responses bytes, got {ping_text:?}"
        );
        assert!(
            !ping_text.contains("response.completed"),
            "a keepalive must not complete the Responses stream"
        );
    }
}
