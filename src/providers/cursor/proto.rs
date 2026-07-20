use prost::Message;

// ---------------------------------------------------------------------------
// Agent client message (request)
// Field tags aligned with Cursor 3.12.x agent.v1 schema (cursor-agent-exec).
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Message)]
pub struct AgentClientMessage {
    /// oneof message — only one of these set per frame on the wire.
    #[prost(message, optional, tag = "1")]
    pub run_request: Option<RunRequest>,
    #[prost(message, optional, tag = "2")]
    pub exec_client_message: Option<ExecClientMessage>,
    /// Per-run blob storage replies used by Cursor to checkpoint the model
    /// transcript around native tool calls.
    #[prost(message, optional, tag = "3")]
    pub kv_client_message: Option<KvClientMessage>,
    /// Control plane for a pending exec (heartbeat / close / throw).
    #[prost(message, optional, tag = "5")]
    pub exec_client_control_message: Option<ExecClientControlMessage>,
    /// Answers InteractionQuery approvals (web/plan/MCP auth / ask).
    #[prost(message, optional, tag = "6")]
    pub interaction_response: Option<InteractionResponse>,
    /// CLI heartbeats use tag 7 (not 2).
    #[prost(message, optional, tag = "7")]
    pub client_heartbeat: Option<ClientHeartbeat>,
}

/// Correlates RunSSE ↔ BidiAppend (also used as RunSSE request body).
#[derive(Clone, PartialEq, Message)]
pub struct BidiRequestId {
    #[prost(string, tag = "1")]
    pub request_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct RunRequest {
    /// Opaque ConversationState / ConversationStateStructure bytes.
    /// Wire-identical to a nested message field; empty vec = fresh turn.
    /// Subsequent turns replay the latest `conversation_checkpoint_update`.
    #[prost(bytes = "vec", optional, tag = "1")]
    pub conversation_state: Option<Vec<u8>>,
    #[prost(message, optional, tag = "2")]
    pub action: Option<Action>,
    /// Optional; prefer `requested_model` (tag 9) on modern Cursor.
    #[prost(message, optional, tag = "3")]
    pub model_details: Option<ModelDetails>,
    #[prost(message, optional, tag = "4")]
    pub mcp_tools: Option<McpTools>,
    #[prost(string, optional, tag = "5")]
    pub conversation_id: Option<String>,
    #[prost(string, optional, tag = "8")]
    pub custom_system_prompt: Option<String>,
    #[prost(message, optional, tag = "9")]
    pub requested_model: Option<CursorModel>,
    #[prost(bool, optional, tag = "12")]
    pub exclude_workspace_context: Option<bool>,
    #[prost(string, optional, tag = "13")]
    pub harness: Option<String>,
    #[prost(message, repeated, tag = "14")]
    pub selected_subagent_models: Vec<CursorModel>,
    #[prost(string, optional, tag = "16")]
    pub conversation_group_id: Option<String>,
    /// Prefetch KV blobs so the server does not round-trip get_blob for
    /// checkpoint-referenced ids on the opening frame.
    #[prost(message, repeated, tag = "17")]
    pub pre_fetched_blobs: Vec<PreFetchedBlob>,
    #[prost(bool, optional, tag = "19")]
    pub client_supports_inline_images: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PreFetchedBlob {
    #[prost(bytes = "vec", tag = "1")]
    pub id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub value: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Action {
    #[prost(message, optional, tag = "1")]
    pub user_message_action: Option<UserMessageAction>,
    /// Reconnect mid-turn only; normal follow-ups use user_message_action.
    #[prost(message, optional, tag = "2")]
    pub resume_action: Option<ResumeAction>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ResumeAction {
    #[prost(message, optional, tag = "2")]
    pub request_context: Option<RequestContext>,
}

#[derive(Clone, PartialEq, Message)]
pub struct UserMessageAction {
    #[prost(message, optional, tag = "1")]
    pub user_message: Option<UserMessage>,
}

#[derive(Clone, PartialEq, Message)]
pub struct UserMessage {
    #[prost(string, tag = "1")]
    pub text: String,
    #[prost(string, tag = "2")]
    pub message_id: String,
    #[prost(message, optional, tag = "3")]
    pub selected_context: Option<SelectedContext>,
    /// agent.v1.AgentMode enum (not a string):
    /// 0=UNSPECIFIED, 1=AGENT, 2=ASK, 3=PLAN, …
    #[prost(int32, tag = "4")]
    pub mode: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct SelectedContext {
    #[prost(message, repeated, tag = "1")]
    pub selected_images: Vec<SelectedImage>,
}

#[derive(Clone, PartialEq, Message)]
pub struct McpTools {
    /// Official CLI field name is `mcp_tools`; tag 1 repeated Definition.
    #[prost(message, repeated, tag = "1")]
    pub tools: Vec<McpTool>,
}

/// Maps to `agent.v1.McpToolDefinition` (Cursor CLI 2026.07).
///
/// Tag 3 is a `google.protobuf.Struct` (JSON object), not a JSON string.
/// Tags 4/5 identify the MCP provider + tool name for routing.
#[derive(Clone, PartialEq, Message)]
pub struct McpTool {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub description: String,
    #[prost(message, optional, tag = "3")]
    pub input_schema: Option<prost_types::Struct>,
    #[prost(string, tag = "4")]
    pub provider_identifier: String,
    #[prost(string, tag = "5")]
    pub tool_name: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ClientHeartbeat {}

/// Maps to agent.v1.RequestedModel in current Cursor builds.
#[derive(Clone, PartialEq, Message)]
pub struct CursorModel {
    #[prost(string, tag = "1")]
    pub model_id: String,
    #[prost(bool, optional, tag = "2")]
    pub max_mode: Option<bool>,
    #[prost(message, repeated, tag = "3")]
    pub parameters: Vec<ModelParameter>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ModelDetails {
    #[prost(string, optional, tag = "1")]
    pub model_id: Option<String>,
    #[prost(string, optional, tag = "3")]
    pub display_model_id: Option<String>,
    #[prost(string, optional, tag = "4")]
    pub display_name: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ModelParameter {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct SelectedImage {
    #[prost(string, tag = "1")]
    pub data: String,
    #[prost(string, tag = "2")]
    pub uuid: String,
    #[prost(string, tag = "3")]
    pub path: String,
    #[prost(string, tag = "4")]
    pub mime_type: String,
}

// ---------------------------------------------------------------------------
// Agent server message (response)
// Tags from Cursor CLI 2026.07.16 agent.v1 (index.js typeName fields).
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Message)]
pub struct AgentServerMessage {
    /// oneof message { … } — tag numbers are unique across the oneof.
    #[prost(message, optional, tag = "1")]
    pub interaction_update: Option<InteractionUpdate>,
    /// Was incorrectly tag 3; official CLI uses tag 2.
    #[prost(message, optional, tag = "2")]
    pub exec_server_message: Option<ExecServerMessage>,
    /// ConversationStateStructure bytes (checkpoint). Persist and replay on
    /// the next RunRequest.conversation_state. Opaque to avoid schema drift.
    #[prost(bytes = "vec", optional, tag = "3")]
    pub conversation_checkpoint_update: Option<Vec<u8>>,
    /// Per-run blob storage request. Cursor waits for these acknowledgements
    /// before starting the next model call after a native tool result.
    #[prost(message, optional, tag = "4")]
    pub kv_server_message: Option<KvServerMessage>,
    /// Approval / interactive prompts (web search, plan, ask, MCP auth).
    #[prost(message, optional, tag = "7")]
    pub interaction_query: Option<InteractionQuery>,
}

/// Server → client approval / interactive query (AgentServerMessage tag 7).
#[derive(Clone, PartialEq, Message)]
pub struct InteractionQuery {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(message, optional, tag = "2")]
    pub web_search_request_query: Option<WebSearchRequestQuery>,
    #[prost(message, optional, tag = "3")]
    pub ask_question_interaction_query: Option<AskQuestionInteractionQuery>,
    #[prost(message, optional, tag = "4")]
    pub switch_mode_request_query: Option<SwitchModeRequestQuery>,
    #[prost(message, optional, tag = "7")]
    pub create_plan_request_query: Option<CreatePlanRequestQuery>,
    #[prost(message, optional, tag = "9")]
    pub web_fetch_request_query: Option<WebFetchRequestQuery>,
    #[prost(message, optional, tag = "11")]
    pub mcp_auth_request_query: Option<McpAuthRequestQuery>,
}

#[derive(Clone, PartialEq, Message)]
pub struct InteractionResponse {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(message, optional, tag = "2")]
    pub web_search_request_response: Option<WebSearchRequestResponse>,
    #[prost(message, optional, tag = "3")]
    pub ask_question_interaction_response: Option<AskQuestionInteractionResponse>,
    #[prost(message, optional, tag = "4")]
    pub switch_mode_request_response: Option<SwitchModeRequestResponse>,
    #[prost(message, optional, tag = "7")]
    pub create_plan_request_response: Option<CreatePlanRequestResponse>,
    #[prost(message, optional, tag = "9")]
    pub web_fetch_request_response: Option<WebFetchRequestResponse>,
    #[prost(message, optional, tag = "11")]
    pub mcp_auth_request_response: Option<McpAuthRequestResponse>,
}

#[derive(Clone, PartialEq, Message)]
pub struct InteractionApproved {}

#[derive(Clone, PartialEq, Message)]
pub struct InteractionRejected {
    #[prost(string, tag = "1")]
    pub reason: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct WebSearchRequestQuery {
    #[prost(message, optional, tag = "1")]
    pub args: Option<WebSearchArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WebSearchRequestResponse {
    #[prost(message, optional, tag = "1")]
    pub approved: Option<InteractionApproved>,
    #[prost(message, optional, tag = "2")]
    pub rejected: Option<InteractionRejected>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WebFetchRequestQuery {
    #[prost(message, optional, tag = "1")]
    pub args: Option<FetchArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WebFetchRequestResponse {
    #[prost(message, optional, tag = "1")]
    pub approved: Option<InteractionApproved>,
    #[prost(message, optional, tag = "2")]
    pub rejected: Option<InteractionRejected>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SwitchModeRequestQuery {}

#[derive(Clone, PartialEq, Message)]
pub struct SwitchModeRequestResponse {
    #[prost(message, optional, tag = "1")]
    pub approved: Option<InteractionApproved>,
    #[prost(message, optional, tag = "2")]
    pub rejected: Option<InteractionRejected>,
}

#[derive(Clone, PartialEq, Message)]
pub struct McpAuthRequestQuery {}

#[derive(Clone, PartialEq, Message)]
pub struct McpAuthRequestResponse {
    #[prost(message, optional, tag = "1")]
    pub approved: Option<InteractionApproved>,
    #[prost(message, optional, tag = "2")]
    pub rejected: Option<InteractionRejected>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreatePlanRequestQuery {
    #[prost(message, optional, tag = "1")]
    pub args: Option<CreatePlanArgs>,
    #[prost(string, tag = "2")]
    pub tool_call_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreatePlanRequestResponse {
    #[prost(message, optional, tag = "1")]
    pub result: Option<CreatePlanResult>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreatePlanResult {
    #[prost(message, optional, tag = "1")]
    pub success: Option<CreatePlanSuccess>,
    #[prost(string, tag = "3")]
    pub plan_uri: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreatePlanSuccess {}

#[derive(Clone, PartialEq, Message)]
pub struct AskQuestionInteractionQuery {
    #[prost(message, optional, tag = "1")]
    pub args: Option<AskQuestionArgs>,
    #[prost(string, tag = "2")]
    pub tool_call_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct AskQuestionInteractionResponse {
    #[prost(message, optional, tag = "1")]
    pub result: Option<AskQuestionResult>,
}

#[derive(Clone, PartialEq, Message)]
pub struct AskQuestionResult {
    #[prost(message, optional, tag = "3")]
    pub rejected: Option<AskQuestionRejected>,
}

#[derive(Clone, PartialEq, Message)]
pub struct AskQuestionRejected {
    #[prost(string, tag = "1")]
    pub reason: String,
}

// ---------------------------------------------------------------------------
// Cursor per-run KV protocol (AgentClientMessage tag 3 / server tag 4)
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Message)]
pub struct KvServerMessage {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(message, optional, tag = "2")]
    pub get_blob_args: Option<GetBlobArgs>,
    #[prost(message, optional, tag = "3")]
    pub set_blob_args: Option<SetBlobArgs>,
    #[prost(message, optional, tag = "4")]
    pub span_context: Option<SpanContext>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetBlobArgs {
    #[prost(bytes = "vec", tag = "1")]
    pub blob_id: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SetBlobArgs {
    #[prost(bytes = "vec", tag = "1")]
    pub blob_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub blob_data: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SpanContext {
    #[prost(string, tag = "1")]
    pub trace_id: String,
    #[prost(string, tag = "2")]
    pub span_id: String,
    #[prost(bool, tag = "3")]
    pub sampled: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct KvClientMessage {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(message, optional, tag = "2")]
    pub get_blob_result: Option<GetBlobResult>,
    #[prost(message, optional, tag = "3")]
    pub set_blob_result: Option<SetBlobResult>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetBlobResult {
    #[prost(bytes = "vec", optional, tag = "1")]
    pub blob_data: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SetBlobResult {
    #[prost(message, optional, tag = "1")]
    pub error: Option<KvError>,
}

#[derive(Clone, PartialEq, Message)]
pub struct KvError {
    #[prost(string, tag = "1")]
    pub message: String,
}

/// InteractionUpdate oneof fields (CLI 2026.07):
/// 1=text_delta, 2=tool_call_started, 3=tool_call_completed, 4=thinking_delta,
/// 5=thinking_completed, 8=token_delta, 13=heartbeat, 14=turn_ended, …
#[derive(Clone, PartialEq, Message)]
pub struct InteractionUpdate {
    #[prost(message, optional, tag = "1")]
    pub text_delta: Option<TextDelta>,
    #[prost(message, optional, tag = "2")]
    pub tool_call_started: Option<ToolCallStarted>,
    #[prost(message, optional, tag = "3")]
    pub tool_call_completed: Option<ToolCallCompleted>,
    #[prost(message, optional, tag = "4")]
    pub thinking_delta: Option<ThinkingDelta>,
    /// Empty marker that reasoning finished (CLI tag 5).
    #[prost(message, optional, tag = "5")]
    pub thinking_completed: Option<ThinkingCompleted>,
    #[prost(message, optional, tag = "8")]
    pub token_delta: Option<TokenDelta>,
    /// Server keep-alive during long thinking (CLI tag 13). Must refresh our
    /// idle timers — otherwise quiet Fable thinking looks stalled.
    #[prost(message, optional, tag = "13")]
    pub heartbeat: Option<InteractionHeartbeat>,
    #[prost(message, optional, tag = "14")]
    pub turn_ended: Option<TurnEnded>,
}

#[derive(Clone, PartialEq, Message)]
pub struct InteractionHeartbeat {}

#[derive(Clone, PartialEq, Message)]
pub struct ToolCallStarted {
    #[prost(string, tag = "1")]
    pub call_id: String,
    #[prost(message, optional, tag = "2")]
    pub tool_call: Option<ToolCall>,
    #[prost(string, tag = "3")]
    pub model_call_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ToolCallCompleted {
    #[prost(string, tag = "1")]
    pub call_id: String,
    #[prost(message, optional, tag = "2")]
    pub tool_call: Option<ToolCall>,
    #[prost(string, tag = "3")]
    pub model_call_id: String,
}

/// ToolCall oneof (CLI 2026.07) — tools we map to Claude Code.
///
/// Unmapped CLI siblings (intentionally omitted): `PiWriteToolCall`,
/// `PiEditToolCall`, `ApplyAgentDiffToolCall`. Interaction updates are
/// transcript-only in live BiDi; FS mutations use [`ExecServerMessage`].
#[derive(Clone, PartialEq, Message)]
pub struct ToolCall {
    #[prost(message, optional, tag = "1")]
    pub shell_tool_call: Option<ShellToolCall>,
    #[prost(message, optional, tag = "3")]
    pub delete_tool_call: Option<DeleteToolCall>,
    #[prost(message, optional, tag = "4")]
    pub glob_tool_call: Option<GlobToolCall>,
    #[prost(message, optional, tag = "5")]
    pub grep_tool_call: Option<GrepToolCall>,
    #[prost(message, optional, tag = "8")]
    pub read_tool_call: Option<ReadToolCall>,
    #[prost(message, optional, tag = "9")]
    pub update_todos_tool_call: Option<UpdateTodosToolCall>,
    #[prost(message, optional, tag = "10")]
    pub read_todos_tool_call: Option<ReadTodosToolCall>,
    #[prost(message, optional, tag = "12")]
    pub edit_tool_call: Option<EditToolCall>,
    #[prost(message, optional, tag = "13")]
    pub ls_tool_call: Option<LsToolCall>,
    #[prost(message, optional, tag = "15")]
    pub mcp_tool_call: Option<McpToolCall>,
    #[prost(message, optional, tag = "17")]
    pub create_plan_tool_call: Option<CreatePlanToolCall>,
    #[prost(message, optional, tag = "18")]
    pub web_search_tool_call: Option<WebSearchToolCall>,
    #[prost(message, optional, tag = "23")]
    pub ask_question_tool_call: Option<AskQuestionToolCall>,
    #[prost(message, optional, tag = "24")]
    pub fetch_tool_call: Option<FetchToolCall>,
}

#[derive(Clone, PartialEq, Message)]
pub struct McpToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<McpArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct McpArgs {
    #[prost(string, tag = "1")]
    pub name: String,
    /// Values are typically UTF-8 JSON fragments.
    #[prost(map = "string, bytes", tag = "2")]
    pub args: std::collections::HashMap<String, Vec<u8>>,
    #[prost(string, tag = "3")]
    pub tool_call_id: String,
    #[prost(string, tag = "4")]
    pub provider_identifier: String,
    #[prost(string, tag = "5")]
    pub tool_name: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct UpdateTodosToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<UpdateTodosArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct UpdateTodosArgs {
    #[prost(message, repeated, tag = "1")]
    pub todos: Vec<TodoItem>,
    #[prost(bool, tag = "2")]
    pub merge: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct ReadTodosToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<ReadTodosArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ReadTodosArgs {
    #[prost(string, repeated, tag = "2")]
    pub id_filter: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TodoItem {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub content: String,
    /// 0=pending, 1=in_progress, 2=completed (Cursor TodoStatus).
    #[prost(int32, tag = "3")]
    pub status: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreatePlanToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<CreatePlanArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreatePlanArgs {
    #[prost(string, tag = "1")]
    pub plan: String,
    #[prost(message, repeated, tag = "2")]
    pub todos: Vec<TodoItem>,
    #[prost(string, tag = "3")]
    pub overview: String,
    #[prost(string, tag = "4")]
    pub name: String,
    #[prost(bool, tag = "5")]
    pub is_project: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct WebSearchToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<WebSearchArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WebSearchArgs {
    #[prost(string, tag = "1")]
    pub search_term: String,
    #[prost(string, tag = "2")]
    pub tool_call_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct FetchToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<FetchArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct FetchArgs {
    #[prost(string, tag = "1")]
    pub url: String,
    #[prost(string, tag = "2")]
    pub tool_call_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct AskQuestionToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<AskQuestionArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct AskQuestionArgs {
    #[prost(string, tag = "1")]
    pub title: String,
    #[prost(message, repeated, tag = "2")]
    pub questions: Vec<AskQuestionItem>,
}

#[derive(Clone, PartialEq, Message)]
pub struct AskQuestionItem {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub prompt: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShellToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<ShellArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShellArgs {
    #[prost(string, tag = "1")]
    pub command: String,
    #[prost(string, tag = "2")]
    pub working_directory: String,
    /// Seconds (Cursor).
    #[prost(int32, tag = "3")]
    pub timeout: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct ReadToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<ReadToolArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ReadToolArgs {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(int32, optional, tag = "2")]
    pub offset: Option<i32>,
    #[prost(int32, optional, tag = "3")]
    pub limit: Option<i32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct EditToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<EditArgs>,
}

/// Cursor Edit interaction args (full-file overwrite via streamed content).
///
/// Only `path` + `stream_content` (tag 6) are modeled. CLI may also emit
/// `EditToolCallDelta` / intermediate tags 2–5; until `stream_content` is set
/// we treat the edit as incomplete (`map_tool_call` returns `None`). Live FS
/// writes go through [`ExecServerMessage::write_args`], not this path.
#[derive(Clone, PartialEq, Message)]
pub struct EditArgs {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, optional, tag = "6")]
    pub stream_content: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GrepToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<GrepArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GrepArgs {
    #[prost(string, tag = "1")]
    pub pattern: String,
    #[prost(string, optional, tag = "2")]
    pub path: Option<String>,
    #[prost(string, optional, tag = "3")]
    pub glob: Option<String>,
    #[prost(bool, optional, tag = "8")]
    pub case_insensitive: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GlobToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<GlobToolArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GlobToolArgs {
    #[prost(string, optional, tag = "1")]
    pub target_directory: Option<String>,
    #[prost(string, tag = "2")]
    pub glob_pattern: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct LsToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<LsArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct LsArgs {
    #[prost(string, tag = "1")]
    pub path: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<DeleteArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteArgs {
    #[prost(string, tag = "1")]
    pub path: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct WriteArgs {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, tag = "2")]
    pub file_text: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ThinkingCompleted {}

#[derive(Clone, PartialEq, Message)]
pub struct ThinkingDelta {
    #[prost(string, tag = "1")]
    pub text: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct TextDelta {
    #[prost(string, tag = "1")]
    pub text: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct TokenDelta {
    #[prost(int32, tag = "1")]
    pub tokens: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct TurnEnded {
    #[prost(uint64, optional, tag = "1")]
    pub input_tokens: Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    pub output_tokens: Option<u64>,
    #[prost(uint64, optional, tag = "3")]
    pub cache_read_tokens: Option<u64>,
    #[prost(uint64, optional, tag = "4")]
    pub cache_write_tokens: Option<u64>,
    #[prost(uint64, optional, tag = "5")]
    pub reasoning_tokens: Option<u64>,
}

/// Server → client tool/exec request (AgentServerMessage tag 2).
///
/// Decoded payloads: shell / write / delete / grep / read / ls / request_context /
/// shell_stream. CLI also defines `PiWriteExecArgs` and `ApplyAgentDiff*` (and
/// matching ToolCall oneofs); those are intentionally unmapped — unknown exec
/// soft-fails via control throw rather than inventing a Claude Write.
#[derive(Clone, PartialEq, Message)]
pub struct ExecServerMessage {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(string, optional, tag = "15")]
    pub exec_id: Option<String>,
    #[prost(message, optional, tag = "2")]
    pub shell_args: Option<ShellArgs>,
    #[prost(message, optional, tag = "3")]
    pub write_args: Option<WriteArgs>,
    #[prost(message, optional, tag = "4")]
    pub delete_args: Option<DeleteArgs>,
    #[prost(message, optional, tag = "5")]
    pub grep_args: Option<GrepArgs>,
    /// Exec-path ReadArgs (tags differ from InteractionUpdate ReadToolArgs).
    #[prost(message, optional, tag = "7")]
    pub read_args: Option<ExecReadArgs>,
    #[prost(message, optional, tag = "8")]
    pub ls_args: Option<LsArgs>,
    /// Present (often empty) when Cursor asks the client for workspace request context.
    #[prost(message, optional, tag = "10")]
    pub request_context_args: Option<RequestContextArgs>,
    #[prost(message, optional, tag = "14")]
    pub shell_stream_args: Option<ShellArgs>,
}

/// ExecServerMessage.read_args (tag 7) — NOT the same layout as ReadToolArgs.
#[derive(Clone, PartialEq, Message)]
pub struct ExecReadArgs {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, tag = "2")]
    pub tool_call_id: String,
    #[prost(int32, optional, tag = "4")]
    pub offset: Option<i32>,
    #[prost(uint32, optional, tag = "5")]
    pub limit: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct RequestContextArgs {
    #[prost(string, optional, tag = "2")]
    pub notes_session_id: Option<String>,
    #[prost(string, optional, tag = "3")]
    pub workspace_id: Option<String>,
}

/// Client → server tool/exec result (AgentClientMessage tag 2).
#[derive(Clone, PartialEq, Message)]
pub struct ExecClientMessage {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(string, optional, tag = "15")]
    pub exec_id: Option<String>,
    #[prost(int32, optional, tag = "39")]
    pub local_execution_time_ms: Option<i32>,
    #[prost(message, optional, tag = "2")]
    pub shell_result: Option<ShellResult>,
    #[prost(message, optional, tag = "3")]
    pub write_result: Option<WriteResult>,
    #[prost(message, optional, tag = "4")]
    pub delete_result: Option<DeleteResult>,
    #[prost(message, optional, tag = "5")]
    pub grep_result: Option<GrepResult>,
    #[prost(message, optional, tag = "7")]
    pub read_result: Option<ReadResult>,
    #[prost(message, optional, tag = "8")]
    pub ls_result: Option<LsResult>,
    #[prost(message, optional, tag = "10")]
    pub request_context_result: Option<RequestContextResult>,
    #[prost(message, optional, tag = "14")]
    pub shell_stream: Option<ShellStream>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ExecClientControlMessage {
    #[prost(message, optional, tag = "1")]
    pub stream_close: Option<ExecClientStreamClose>,
    #[prost(message, optional, tag = "2")]
    pub throw: Option<ExecClientThrow>,
    #[prost(message, optional, tag = "3")]
    pub heartbeat: Option<ExecClientHeartbeat>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ExecClientStreamClose {
    #[prost(uint32, tag = "1")]
    pub id: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct ExecClientThrow {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(string, tag = "2")]
    pub error: String,
    #[prost(string, optional, tag = "3")]
    pub stack_trace: Option<String>,
    #[prost(string, optional, tag = "4")]
    pub error_code: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ExecClientHeartbeat {
    #[prost(uint32, tag = "1")]
    pub id: u32,
}

// ---------------------------------------------------------------------------
// Native exec results (client -> Cursor server)
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Message)]
pub struct ReadResult {
    #[prost(message, optional, tag = "1")]
    pub success: Option<ReadSuccess>,
    #[prost(message, optional, tag = "2")]
    pub error: Option<ReadError>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ReadSuccess {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, optional, tag = "2")]
    pub content: Option<String>,
    #[prost(int32, tag = "3")]
    pub total_lines: i32,
    #[prost(int64, tag = "4")]
    pub file_size: i64,
    #[prost(bool, tag = "6")]
    pub truncated: bool,
    #[prost(bool, tag = "8")]
    pub range_applied: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct ReadError {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, tag = "2")]
    pub error: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct WriteResult {
    #[prost(message, optional, tag = "1")]
    pub success: Option<WriteSuccess>,
    #[prost(message, optional, tag = "5")]
    pub error: Option<WriteError>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WriteSuccess {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(int32, tag = "2")]
    pub lines_created: i32,
    #[prost(int32, tag = "3")]
    pub file_size: i32,
    #[prost(string, optional, tag = "4")]
    pub file_content_after_write: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WriteError {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, tag = "2")]
    pub error: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteResult {
    #[prost(message, optional, tag = "1")]
    pub success: Option<DeleteSuccess>,
    #[prost(message, optional, tag = "7")]
    pub error: Option<DeleteError>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteSuccess {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, tag = "2")]
    pub deleted_file: String,
    #[prost(int64, tag = "3")]
    pub file_size: i64,
    #[prost(string, tag = "4")]
    pub prev_content: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteError {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, tag = "2")]
    pub error: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct GrepResult {
    #[prost(message, optional, tag = "1")]
    pub success: Option<GrepSuccess>,
    #[prost(message, optional, tag = "2")]
    pub error: Option<GrepError>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GrepError {
    #[prost(string, tag = "1")]
    pub error: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct GrepSuccess {
    #[prost(string, tag = "1")]
    pub pattern: String,
    #[prost(string, tag = "2")]
    pub path: String,
    #[prost(string, tag = "3")]
    pub output_mode: String,
    #[prost(map = "string, message", tag = "4")]
    pub workspace_results: std::collections::HashMap<String, GrepUnionResult>,
    #[prost(message, optional, tag = "5")]
    pub active_editor_result: Option<GrepUnionResult>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GrepUnionResult {
    #[prost(message, optional, tag = "3")]
    pub content: Option<GrepContentResult>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GrepContentResult {
    #[prost(message, repeated, tag = "1")]
    pub matches: Vec<GrepFileMatch>,
    #[prost(int32, tag = "2")]
    pub total_lines: i32,
    #[prost(int32, tag = "3")]
    pub total_matched_lines: i32,
    #[prost(bool, tag = "4")]
    pub client_truncated: bool,
    #[prost(bool, tag = "5")]
    pub ripgrep_truncated: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct GrepFileMatch {
    #[prost(string, tag = "1")]
    pub file: String,
    #[prost(message, repeated, tag = "2")]
    pub matches: Vec<GrepContentMatch>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GrepContentMatch {
    #[prost(int32, tag = "1")]
    pub line_number: i32,
    #[prost(string, tag = "2")]
    pub content: String,
    #[prost(bool, tag = "3")]
    pub content_truncated: bool,
    #[prost(bool, tag = "4")]
    pub is_context_line: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct LsResult {
    #[prost(message, optional, tag = "1")]
    pub success: Option<LsSuccess>,
    #[prost(message, optional, tag = "2")]
    pub error: Option<LsError>,
}

#[derive(Clone, PartialEq, Message)]
pub struct LsSuccess {
    #[prost(message, optional, tag = "1")]
    pub directory_tree_root: Option<LsDirectoryTreeNode>,
}

#[derive(Clone, PartialEq, Message)]
pub struct LsDirectoryTreeNode {
    #[prost(string, tag = "1")]
    pub abs_path: String,
    #[prost(message, repeated, tag = "2")]
    pub children_dirs: Vec<LsDirectoryTreeNode>,
    #[prost(message, repeated, tag = "3")]
    pub children_files: Vec<LsFile>,
    #[prost(bool, tag = "4")]
    pub children_were_processed: bool,
    #[prost(map = "string, int32", tag = "5")]
    pub full_subtree_extension_counts: std::collections::HashMap<String, i32>,
    #[prost(int32, tag = "6")]
    pub num_files: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct LsFile {
    #[prost(string, tag = "1")]
    pub name: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct LsError {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, tag = "2")]
    pub error: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShellResult {
    #[prost(message, optional, tag = "1")]
    pub success: Option<ShellSuccess>,
    #[prost(message, optional, tag = "2")]
    pub failure: Option<ShellFailure>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShellSuccess {
    #[prost(string, tag = "1")]
    pub command: String,
    #[prost(string, tag = "2")]
    pub working_directory: String,
    #[prost(int32, tag = "3")]
    pub exit_code: i32,
    #[prost(string, tag = "4")]
    pub signal: String,
    #[prost(string, tag = "5")]
    pub stdout: String,
    #[prost(string, tag = "6")]
    pub stderr: String,
    #[prost(int32, tag = "7")]
    pub execution_time: i32,
    #[prost(int32, optional, tag = "13")]
    pub local_execution_time_ms: Option<i32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShellFailure {
    #[prost(string, tag = "1")]
    pub command: String,
    #[prost(string, tag = "2")]
    pub working_directory: String,
    #[prost(int32, tag = "3")]
    pub exit_code: i32,
    #[prost(string, tag = "4")]
    pub signal: String,
    #[prost(string, tag = "5")]
    pub stdout: String,
    #[prost(string, tag = "6")]
    pub stderr: String,
    #[prost(int32, tag = "7")]
    pub execution_time: i32,
    #[prost(bool, tag = "11")]
    pub aborted: bool,
    #[prost(int32, optional, tag = "12")]
    pub local_execution_time_ms: Option<i32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShellStream {
    #[prost(message, optional, tag = "1")]
    pub stdout: Option<ShellStreamStdout>,
    #[prost(message, optional, tag = "2")]
    pub stderr: Option<ShellStreamStderr>,
    #[prost(message, optional, tag = "3")]
    pub exit: Option<ShellStreamExit>,
    #[prost(message, optional, tag = "4")]
    pub start: Option<ShellStreamStart>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShellStreamStdout {
    #[prost(string, tag = "1")]
    pub data: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShellStreamStderr {
    #[prost(string, tag = "1")]
    pub data: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShellStreamExit {
    #[prost(uint32, tag = "1")]
    pub code: u32,
    #[prost(string, tag = "2")]
    pub cwd: String,
    #[prost(bool, tag = "4")]
    pub aborted: bool,
    #[prost(int32, optional, tag = "6")]
    pub local_execution_time_ms: Option<i32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShellStreamStart {}

#[derive(Clone, PartialEq, Message)]
pub struct RequestContextResult {
    #[prost(message, optional, tag = "1")]
    pub success: Option<RequestContextSuccess>,
    #[prost(message, optional, tag = "2")]
    pub error: Option<RequestContextErrorMsg>,
}

#[derive(Clone, PartialEq, Message)]
pub struct RequestContextSuccess {
    #[prost(message, optional, tag = "1")]
    pub request_context: Option<RequestContext>,
    #[prost(bool, optional, tag = "2")]
    pub served_from_disk_cache: Option<bool>,
}

/// Minimal empty request context — enough for the agent to continue without a full IDE workspace.
#[derive(Clone, PartialEq, Message)]
pub struct RequestContext {}

#[derive(Clone, PartialEq, Message)]
pub struct RequestContextErrorMsg {
    #[prost(string, tag = "1")]
    pub error: String,
}

// ---------------------------------------------------------------------------
// GetUsableModels (unary catalog)
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Message)]
pub struct GetUsableModelsRequest {
    #[prost(string, repeated, tag = "1")]
    pub custom_model_ids: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetUsableModelsResponse {
    #[prost(message, repeated, tag = "1")]
    pub models: Vec<ModelDetails>,
}
