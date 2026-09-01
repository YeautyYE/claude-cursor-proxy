use bytes::{Buf, BufMut};
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
    /// Modern Agent Host control action.  Cursor keeps this in the same
    /// oneof as `run_request`; older CLI builds simply ignore the field.
    #[prost(message, optional, tag = "4")]
    pub conversation_action: Option<ConversationAction>,
    /// Control plane for a pending exec (heartbeat / close / throw).
    #[prost(message, optional, tag = "5")]
    pub exec_client_control_message: Option<ExecClientControlMessage>,
    /// Answers InteractionQuery approvals (web/plan/MCP auth / ask).
    #[prost(message, optional, tag = "6")]
    pub interaction_response: Option<InteractionResponse>,
    /// CLI heartbeats use tag 7 (not 2).
    #[prost(message, optional, tag = "7")]
    pub client_heartbeat: Option<ClientHeartbeat>,
    /// Agent Host prewarm request (used before the first user turn and when a
    /// model/runtime is switched).  This is deliberately represented on the
    /// wire even though the proxy does not currently issue proactive warms.
    #[prost(message, optional, tag = "8")]
    pub prewarm_request: Option<PrewarmRequest>,
}

/// Cursor `agent.v1.PrewarmRequest` (AgentClientMessage tag 8).
///
/// The desktop runtime sends this message to hydrate a local Agent Host
/// before dispatching a user action.  Keeping all current fields here makes
/// the proxy forward-compatible with Sand's managed-local route while empty
/// optional fields remain wire-compatible with older servers.
#[derive(Clone, PartialEq, Message)]
pub struct PrewarmRequest {
    #[prost(message, optional, tag = "1")]
    pub model_details: Option<ModelDetails>,
    #[prost(string, optional, tag = "2")]
    pub conversation_id: Option<String>,
    #[prost(bytes = "vec", optional, tag = "3")]
    pub conversation_state: Option<Vec<u8>>,
    #[prost(message, optional, tag = "4")]
    pub mcp_tools: Option<McpTools>,
    #[prost(message, optional, tag = "5")]
    pub mcp_file_system_options: Option<McpFileSystemOptions>,
    #[prost(string, optional, tag = "6")]
    pub best_of_n_group_id: Option<String>,
    #[prost(bool, optional, tag = "7")]
    pub try_use_best_of_n_promotion: Option<bool>,
    #[prost(string, optional, tag = "8")]
    pub custom_system_prompt: Option<String>,
    #[prost(message, optional, tag = "9")]
    pub requested_model: Option<CursorModel>,
    #[prost(bool, optional, tag = "10")]
    pub suggest_next_prompt: Option<bool>,
    #[prost(string, optional, tag = "11")]
    pub subagent_type_name: Option<String>,
    #[prost(bool, optional, tag = "12")]
    pub exclude_workspace_context: Option<bool>,
    #[prost(string, optional, tag = "13")]
    pub harness: Option<String>,
    #[prost(message, repeated, tag = "14")]
    pub selected_subagent_models: Vec<CursorModel>,
    #[prost(message, repeated, tag = "15")]
    pub selected_subagent_model_details: Vec<ModelDetails>,
    #[prost(string, optional, tag = "16")]
    pub conversation_group_id: Option<String>,
    #[prost(message, repeated, tag = "17")]
    pub pre_fetched_blobs: Vec<PreFetchedBlob>,
    #[prost(bool, optional, tag = "18")]
    pub client_supports_inline_images: Option<bool>,
    #[prost(message, repeated, tag = "19")]
    pub subagent_model_overrides: Vec<SubagentModelOverride>,
    #[prost(bool, optional, tag = "20")]
    pub can_create_cloud_subagents: Option<bool>,
    #[prost(bool, optional, tag = "21")]
    pub suppress_subagent_progress_update_tool: Option<bool>,
    #[prost(bool, optional, tag = "22")]
    pub client_supports_send_to_user: Option<bool>,
    /// Cursor Desktop 3.17.19: computer-use coordinate mode. The proxy does
    /// not currently advertise a local computer-use executor, so callers
    /// leave this unset; decoding it keeps a Prewarm frame wire-complete.
    #[prost(string, optional, tag = "23")]
    pub computer_use_coordinate_mode: Option<String>,
    /// Stable Agent Host worker identity for a prewarmed runtime. This is
    /// intentionally distinct from the Cursor conversation id.
    #[prost(string, optional, tag = "24")]
    pub agent_session_id: Option<String>,
    #[prost(bool, optional, tag = "25")]
    pub client_supports_prompt_context_usage_rpc: Option<bool>,
    #[prost(bool, optional, tag = "26")]
    pub client_supports_routed_model_update: Option<bool>,
    /// Gateway credentials are supplied only by a real Desktop runtime. The
    /// proxy preserves the schema for decoding but never synthesizes them.
    #[prost(message, optional, tag = "27")]
    pub client_llm_gateway_credential: Option<ClientLlmGatewayCredential>,
    #[prost(bool, optional, tag = "28")]
    pub client_supports_preview_card: Option<bool>,
    #[prost(bool, optional, tag = "29")]
    pub started_as_new_project: Option<bool>,
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
    /// Must be `Some` even when empty — omitting the field makes Cursor
    /// return `Conversation state is required [invalid_argument]`.
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
    // Tag 6 `mcp_file_system_options` and tag 7 `skill_options` exist on
    // `agent.v1.AgentRunRequest`. Official CLI live construction omits empty
    // `skill_options`; skills go on RequestContext.agent_skills (tag 29).
    // Do not send empty SkillOptions.
    //
    // Anthropic Messages `thinking` / `max_tokens` / `tool_choice` have **no**
    // AgentRunRequest fields. Verified against Cursor Desktop Agent Host:
    // - tags 1–32 include the managed-local lifecycle and capability fields
    //   (conversation_state, action, model_details, mcp_tools, conversation_id,
    //   mcp_file_system_options, skill_options, custom_system_prompt,
    //   requested_model, suggest_next_prompt, subagent_type_name,
    //   exclude_workspace_context, harness, selected_subagent_models,
    //   selected_subagent_model_details, conversation_group_id,
    //   pre_fetched_blobs, dev_raw_model_slug, client_supports_inline_images,
    //   subagent_model_overrides, can_create_cloud_subagents,
    //   suppress_subagent_progress_update_tool, client_supports_send_to_user).
    // - 0xlane `docs/proto/agent_v1.proto`: tags 1–18, same names, no extras.
    // - claudedocs/cursor-cli-reverse-2026-07.md §4.3 (CLI 2026.07 extract).
    // `tool_choice` does not appear as a proto field name in cursor-agent-exec.
    // `max_tokens` exists only on ConversationTokenDetails (usage window), not
    // this message. Nested RequestedModel.parameters (`thinking`/`effort`/
    // `context`) are catalog-id derived in model.rs — do not overlay Anthropic
    // generation controls onto them.
    // RequestContext is NOT a RunRequest field; the server asks via
    // ExecServerMessage.request_context_args (tag 10) and the client replies
    // ExecClientMessage.request_context_result (tag 10).
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
    /// Optional filesystem bridge metadata used by Cursor's managed-local
    /// agent host.  Older servers ignore this field; keeping the exact wire
    /// tag lets a Sand-capable sidecar forward the descriptor without having
    /// to maintain a second protobuf definition.
    #[prost(message, optional, tag = "6")]
    pub mcp_file_system_options: Option<McpFileSystemOptions>,
    /// Skill descriptors collected by the local runtime.
    #[prost(message, optional, tag = "7")]
    pub skill_options: Option<SkillOptions>,
    #[prost(bool, optional, tag = "10")]
    pub suggest_next_prompt: Option<bool>,
    #[prost(string, optional, tag = "11")]
    pub subagent_type_name: Option<String>,
    #[prost(message, repeated, tag = "15")]
    pub selected_subagent_model_details: Vec<ModelDetails>,
    #[prost(string, optional, tag = "18")]
    pub dev_raw_model_slug: Option<String>,
    #[prost(message, repeated, tag = "20")]
    pub subagent_model_overrides: Vec<SubagentModelOverride>,
    #[prost(bool, optional, tag = "21")]
    pub can_create_cloud_subagents: Option<bool>,
    #[prost(bool, optional, tag = "22")]
    pub suppress_subagent_progress_update_tool: Option<bool>,
    #[prost(bool, optional, tag = "23")]
    pub client_supports_send_to_user: Option<bool>,
    /// Coordinate system advertised by a real computer-use client.  The
    /// proxy leaves this unset until it can execute that tool locally.
    #[prost(string, optional, tag = "24")]
    pub computer_use_coordinate_mode: Option<String>,
    /// Stable logical turn identity.  It must survive a ResumeAction retry;
    /// callers should not substitute a transport-attempt request id here.
    #[prost(string, optional, tag = "25")]
    pub run_id: Option<String>,
    /// Stable Agent Host worker identity.  This intentionally remains
    /// independent from a Cursor conversation binding, which can rotate when
    /// the server compacts its KV state.
    #[prost(string, optional, tag = "26")]
    pub agent_session_id: Option<String>,
    #[prost(bool, optional, tag = "27")]
    pub client_supports_prompt_context_usage_rpc: Option<bool>,
    #[prost(bool, optional, tag = "28")]
    pub client_supports_routed_model_update: Option<bool>,
    #[prost(message, optional, tag = "29")]
    pub system_prompt_spec: Option<SystemPromptSpec>,
    /// Real desktop gateway credentials only.  The proxy never synthesizes
    /// this value from its normal Cursor account token.
    #[prost(message, optional, tag = "30")]
    pub client_llm_gateway_credential: Option<ClientLlmGatewayCredential>,
    #[prost(bool, optional, tag = "31")]
    pub client_supports_preview_card: Option<bool>,
    #[prost(bool, optional, tag = "32")]
    pub started_as_new_project: Option<bool>,
}

/// Metadata sent by the local MCP filesystem bridge (agent.v1).
#[derive(Clone, PartialEq, Message)]
pub struct McpFileSystemOptions {
    #[prost(bool, tag = "1")]
    pub enabled: bool,
    #[prost(string, tag = "2")]
    pub workspace_project_dir: String,
    #[prost(message, repeated, tag = "3")]
    pub mcp_descriptors: Vec<McpDescriptor>,
}

#[derive(Clone, PartialEq, Message)]
pub struct McpDescriptor {
    #[prost(string, tag = "1")]
    pub server_name: String,
    #[prost(string, tag = "2")]
    pub server_identifier: String,
    #[prost(string, optional, tag = "3")]
    pub folder_path: Option<String>,
    #[prost(string, optional, tag = "4")]
    pub server_use_instructions: Option<String>,
    #[prost(message, repeated, tag = "5")]
    pub tools: Vec<McpToolDescriptor>,
    #[prost(string, optional, tag = "7")]
    pub plugin: Option<String>,
    #[prost(string, optional, tag = "8")]
    pub marketplace: Option<String>,
    #[prost(string, optional, tag = "9")]
    pub plugin_db_id: Option<String>,
    #[prost(string, optional, tag = "10")]
    pub marketplace_id: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct McpToolDescriptor {
    #[prost(string, tag = "1")]
    pub tool_name: String,
    #[prost(string, optional, tag = "2")]
    pub definition_path: Option<String>,
    #[prost(string, optional, tag = "3")]
    pub description: Option<String>,
    #[prost(message, optional, tag = "4")]
    pub input_schema: Option<prost_types::Value>,
    #[prost(string, optional, tag = "5")]
    pub input_schema_json: Option<String>,
    #[prost(string, optional, tag = "6")]
    pub annotations_json: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SkillOptions {
    #[prost(message, repeated, tag = "1")]
    pub skill_descriptors: Vec<SkillDescriptor>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SkillDescriptor {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub description: String,
    #[prost(string, tag = "3")]
    pub folder_path: String,
    #[prost(bool, tag = "4")]
    pub enabled: bool,
    #[prost(string, optional, tag = "5")]
    pub parse_error: Option<String>,
    #[prost(string, tag = "6")]
    pub readme_file_path: String,
    /// Enum values are intentionally represented as i32 so unknown package
    /// kinds from newer Cursor builds round-trip without a generated enum.
    #[prost(int32, tag = "7")]
    pub package_type: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct SubagentModelOverride {
    #[prost(string, tag = "1")]
    pub subagent_type: String,
    /// The generated Cursor schema uses a oneof (`model`/`inherit`/`disabled`).
    /// Optional fields preserve the same wire tags while keeping this module's
    /// intentionally lightweight protobuf style (which avoids oneof enums).
    #[prost(message, optional, tag = "2")]
    pub model: Option<CursorModel>,
    #[prost(bool, optional, tag = "3")]
    pub inherit: Option<bool>,
    #[prost(bool, optional, tag = "4")]
    pub disabled: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SystemPromptSpec {
    #[prost(string, optional, tag = "1")]
    pub replace: Option<String>,
    #[prost(string, optional, tag = "2")]
    pub append: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ClientLlmGatewayCredential {
    #[prost(string, tag = "1")]
    pub bearer_token: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct PreFetchedBlob {
    #[prost(bytes = "vec", tag = "1")]
    pub id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub value: Vec<u8>,
}

/// Modern `agent.v1.ConversationAction` envelope.  The generated Cursor
/// schema models these fields as a protobuf `oneof`; this hand-written schema
/// keeps optional arms flat so older callers can continue using the `Action`
/// alias while preserving the exact wire tags.
#[derive(Clone, PartialEq, Message)]
pub struct ConversationAction {
    #[prost(message, optional, tag = "1")]
    pub user_message_action: Option<UserMessageAction>,
    #[prost(message, optional, tag = "2")]
    pub resume_action: Option<ResumeAction>,
    #[prost(message, optional, tag = "3")]
    pub cancel_action: Option<CancelAction>,
    #[prost(message, optional, tag = "4")]
    pub summarize_action: Option<SummarizeAction>,
    #[prost(message, optional, tag = "5")]
    pub shell_command_action: Option<ShellCommandAction>,
    #[prost(message, optional, tag = "6")]
    pub start_plan_action: Option<StartPlanAction>,
    #[prost(message, optional, tag = "7")]
    pub execute_plan_action: Option<ExecutePlanAction>,
    #[prost(message, optional, tag = "8")]
    pub async_ask_question_completion_action: Option<AsyncAskQuestionCompletionAction>,
    #[prost(message, optional, tag = "10")]
    pub cancel_subagent_action: Option<CancelSubagentAction>,
    #[prost(string, optional, tag = "11")]
    pub triggering_auth_id: Option<String>,
    #[prost(message, optional, tag = "12")]
    pub background_task_completion_action: Option<BackgroundTaskCompletionAction>,
    #[prost(message, optional, tag = "13")]
    pub background_shell_action: Option<BackgroundShellAction>,
    #[prost(message, optional, tag = "14")]
    pub background_subagent_action: Option<BackgroundSubagentAction>,
    #[prost(message, optional, tag = "15")]
    pub triggering_user_info: Option<TriggeringUserInfo>,
}

/// Backwards-compatible name used by the original proxy code.  Cursor's
/// `AgentRunRequest.action` has always been this ConversationAction envelope;
/// the alias exposes the newly added control arms without a second protobuf
/// message definition.
pub type Action = ConversationAction;

#[derive(Clone, PartialEq, Message)]
pub struct CancelAction {
    #[prost(string, tag = "1")]
    pub reason: String,
    #[prost(message, optional, tag = "3")]
    pub interrupted_pending_tool_call_resolutions: Option<InterruptedPendingToolCallResolutions>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SummarizeAction {}

#[derive(Clone, PartialEq, Message)]
pub struct ShellCommandAction {
    #[prost(message, optional, tag = "1")]
    pub shell_command: Option<ShellCommand>,
    #[prost(string, optional, tag = "2")]
    pub exec_id: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShellCommand {
    #[prost(string, tag = "1")]
    pub command: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct StartPlanAction {
    #[prost(message, optional, tag = "1")]
    pub user_message: Option<UserMessage>,
    #[prost(message, optional, tag = "2")]
    pub request_context: Option<RequestContext>,
    #[prost(bool, optional, tag = "3")]
    pub is_spec: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ExecutePlanAction {
    #[prost(message, optional, tag = "1")]
    pub request_context: Option<RequestContext>,
    #[prost(message, optional, tag = "2")]
    pub plan: Option<ConversationPlan>,
    #[prost(string, optional, tag = "3")]
    pub plan_file_uri: Option<String>,
    #[prost(string, optional, tag = "4")]
    pub plan_file_content: Option<String>,
    #[prost(int32, optional, tag = "5")]
    pub execution_mode: Option<i32>,
    #[prost(string, optional, tag = "6")]
    pub kickoff_message_id: Option<String>,
    #[prost(string, optional, tag = "7")]
    pub plan_id: Option<String>,
    #[prost(string, optional, tag = "8")]
    pub plan_file_path: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConversationPlan {
    #[prost(string, tag = "1")]
    pub plan: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct AsyncAskQuestionCompletionAction {
    #[prost(string, tag = "1")]
    pub original_tool_call_id: String,
    /// Opaque encoded ask-question args/result are retained for forward
    /// compatibility; the normal Claude bridge handles these at the HTTP
    /// layer and does not synthesize this action.
    #[prost(bytes = "vec", optional, tag = "2")]
    pub original_args: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "3")]
    pub result: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CancelSubagentAction {
    #[prost(string, tag = "1")]
    pub subagent_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct BackgroundTaskCompletionAction {
    #[prost(message, repeated, tag = "1")]
    pub completions: Vec<BackgroundTaskCompletion>,
}

#[derive(Clone, PartialEq, Message)]
pub struct BackgroundTaskCompletion {
    #[prost(string, tag = "1")]
    pub task_id: String,
    #[prost(int32, tag = "2")]
    pub kind: i32,
    #[prost(int32, tag = "3")]
    pub status: i32,
    #[prost(string, tag = "4")]
    pub title: String,
    #[prost(string, optional, tag = "5")]
    pub detail: Option<String>,
    #[prost(string, optional, tag = "6")]
    pub output_path: Option<String>,
    #[prost(string, optional, tag = "7")]
    pub thread_id: Option<String>,
    #[prost(int32, optional, tag = "8")]
    pub reason: Option<i32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct BackgroundShellAction {
    #[prost(string, tag = "1")]
    pub tool_call_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct BackgroundSubagentAction {
    #[prost(string, tag = "1")]
    pub tool_call_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct TriggeringUserInfo {
    #[prost(string, optional, tag = "1")]
    pub auth_id: Option<String>,
    #[prost(int32, optional, tag = "2")]
    pub user_id: Option<i32>,
}

/// Additional context emitted by a Claude/Cursor hook.  Cursor models this
/// as a small message repeated on `UserMessage`; keeping it explicit avoids
/// dropping hook output when a native tool result is resumed.
#[derive(Clone, PartialEq, Message)]
pub struct HookAdditionalContext {
    #[prost(string, tag = "1")]
    pub hook_event_name: String,
    #[prost(string, tag = "2")]
    pub content: String,
}

/// Metadata attached to plan-execution and other synthetic user messages.
/// The enum-valued fields intentionally use `i32`: protobuf enum values are
/// represented by the same wire type and this preserves unknown future
/// values without rejecting a frame.
#[derive(Clone, PartialEq, Message)]
pub struct SimulatedMessageMetadata {
    #[prost(string, optional, tag = "1")]
    pub title: Option<String>,
    #[prost(string, optional, tag = "2")]
    pub task_id: Option<String>,
    #[prost(string, optional, tag = "3")]
    pub fsd_finding_action: Option<String>,
    #[prost(string, optional, tag = "4")]
    pub url: Option<String>,
    #[prost(int32, optional, tag = "5")]
    pub subscription_source: Option<i32>,
}

/// Plan descriptor carried by `UserMessage.execute_plan_info`.
#[derive(Clone, PartialEq, Message)]
pub struct ExecutePlanInfo {
    #[prost(string, tag = "1")]
    pub plan_id: String,
    #[prost(string, tag = "2")]
    pub plan_title: String,
}

/// Submitted custom mode descriptor used by `CustomModeIntent.enter`.
#[derive(Clone, PartialEq, Message)]
pub struct SubmittedCustomMode {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub label: String,
    #[prost(int32, tag = "5")]
    pub source: i32,
    #[prost(string, optional, tag = "6")]
    pub source_path: Option<String>,
    #[prost(string, optional, tag = "7")]
    pub source_hash: Option<String>,
    #[prost(string, optional, tag = "10")]
    pub managed_skill_id: Option<String>,
    #[prost(string, optional, tag = "11")]
    pub plugin_id: Option<String>,
    #[prost(string, optional, tag = "12")]
    pub plugin_snapshot_token: Option<String>,
}

/// Descriptor for the mode being left by `CustomModeIntent.exit`.
#[derive(Clone, PartialEq, Message)]
pub struct SubmittedExitedCustomMode {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub label: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct CustomModeExitIntent {
    #[prost(int32, tag = "1")]
    pub next_mode: i32,
    #[prost(message, optional, tag = "2")]
    pub exited_mode: Option<SubmittedExitedCustomMode>,
}

/// Cursor's generated schema uses a oneof (`enter`/`exit`).  Flat optional
/// arms retain the exact field numbers while matching the lightweight proto
/// style used throughout this proxy.
#[derive(Clone, PartialEq, Message)]
pub struct CustomModeIntent {
    #[prost(message, optional, tag = "1")]
    pub enter: Option<SubmittedCustomMode>,
    #[prost(message, optional, tag = "2")]
    pub exit: Option<CustomModeExitIntent>,
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
    /// Request context can be attached directly to a user action by the
    /// desktop Agent Host.  The exec request/reply path remains supported for
    /// older CLI builds.
    #[prost(message, optional, tag = "2")]
    pub request_context: Option<RequestContext>,
    #[prost(bool, optional, tag = "3")]
    pub send_to_interaction_listener: Option<bool>,
    #[prost(message, repeated, tag = "4")]
    pub prepend_user_messages: Vec<UserMessage>,
    #[prost(message, optional, tag = "6")]
    pub interrupted_pending_tool_call_resolutions: Option<InterruptedPendingToolCallResolutions>,
    #[prost(message, optional, tag = "7")]
    pub conversation_history: Option<ConversationHistory>,
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
    #[prost(bool, optional, tag = "5")]
    pub is_simulated_msg: Option<bool>,
    #[prost(string, optional, tag = "6")]
    pub best_of_n_group_id: Option<String>,
    #[prost(bool, optional, tag = "7")]
    pub try_use_best_of_n_promotion: Option<bool>,
    #[prost(string, optional, tag = "8")]
    pub rich_text: Option<String>,
    /// Enum `agent.v1.SimulatedMsgReason` (wire-compatible i32).
    #[prost(int32, optional, tag = "9")]
    pub simulated_msg_reason: Option<i32>,
    #[prost(bytes = "vec", tag = "10")]
    pub conversation_state_blob_id: Vec<u8>,
    #[prost(string, optional, tag = "11")]
    pub subagent_system_reminder: Option<String>,
    #[prost(message, optional, tag = "13")]
    pub triggering_user_info: Option<TriggeringUserInfo>,
    #[prost(message, optional, tag = "14")]
    pub execute_plan_info: Option<ExecutePlanInfo>,
    #[prost(message, optional, tag = "15")]
    pub simulated_message_metadata: Option<SimulatedMessageMetadata>,
    #[prost(string, optional, tag = "16")]
    pub prompt_reference_id: Option<String>,
    #[prost(string, optional, tag = "17")]
    pub thread_id: Option<String>,
    #[prost(bytes = "vec", optional, tag = "18")]
    pub text_blob_id: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "19")]
    pub rich_text_blob_id: Option<Vec<u8>>,
    #[prost(message, repeated, tag = "21")]
    pub hook_additional_contexts: Vec<HookAdditionalContext>,
    #[prost(message, optional, tag = "22")]
    pub custom_mode_intent: Option<CustomModeIntent>,
}

/// A compact representation of Cursor's conversation history action payload.
/// Content arms are kept as optional fields rather than Rust enums so callers
/// can decode newer oneof variants without losing the surrounding message.
#[derive(Clone, PartialEq, Message)]
pub struct ConversationHistory {
    #[prost(message, repeated, tag = "1")]
    pub messages: Vec<ConversationHistoryMessage>,
    #[prost(bool, optional, tag = "2")]
    pub replace_user_info: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConversationHistoryMessage {
    #[prost(message, optional, tag = "1")]
    pub user: Option<ConversationHistoryUserMessage>,
    #[prost(message, optional, tag = "2")]
    pub assistant: Option<ConversationHistoryAssistantMessage>,
    #[prost(message, optional, tag = "3")]
    pub tool: Option<ConversationHistoryToolMessage>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConversationHistoryUserMessage {
    #[prost(message, repeated, tag = "1")]
    pub content: Vec<ConversationHistoryUserContent>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConversationHistoryUserContent {
    #[prost(message, optional, tag = "1")]
    pub text: Option<ConversationHistoryTextContent>,
    #[prost(message, optional, tag = "2")]
    pub image: Option<ConversationHistoryImageContent>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConversationHistoryTextContent {
    #[prost(string, tag = "1")]
    pub text: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConversationHistoryImageContent {
    #[prost(string, tag = "1")]
    pub data: String,
    #[prost(string, optional, tag = "2")]
    pub mime_type: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConversationHistoryAssistantMessage {
    #[prost(message, repeated, tag = "1")]
    pub content: Vec<ConversationHistoryAssistantContent>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConversationHistoryAssistantContent {
    #[prost(message, optional, tag = "1")]
    pub text: Option<ConversationHistoryTextContent>,
    #[prost(message, optional, tag = "2")]
    pub reasoning: Option<ConversationHistoryReasoningContent>,
    #[prost(message, optional, tag = "3")]
    pub redacted_reasoning: Option<ConversationHistoryRedactedReasoningContent>,
    #[prost(message, optional, tag = "4")]
    pub tool_call: Option<ConversationHistoryToolCall>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConversationHistoryReasoningContent {
    #[prost(string, tag = "1")]
    pub text: String,
    #[prost(string, optional, tag = "2")]
    pub signature: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConversationHistoryRedactedReasoningContent {
    #[prost(string, tag = "1")]
    pub data: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConversationHistoryToolCall {
    #[prost(string, tag = "1")]
    pub tool_call_id: String,
    #[prost(string, tag = "2")]
    pub tool_name: String,
    #[prost(string, tag = "3")]
    pub args_json: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConversationHistoryToolMessage {
    #[prost(string, tag = "1")]
    pub tool_call_id: String,
    #[prost(string, tag = "2")]
    pub tool_name: String,
    #[prost(message, repeated, tag = "3")]
    pub content: Vec<ConversationHistoryToolResultContent>,
    #[prost(bool, optional, tag = "4")]
    pub is_error: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConversationHistoryToolResultContent {
    #[prost(message, optional, tag = "1")]
    pub text: Option<ConversationHistoryTextContent>,
    #[prost(message, optional, tag = "2")]
    pub image: Option<ConversationHistoryImageContent>,
}

#[derive(Clone, PartialEq, Message)]
pub struct InterruptedPendingToolCallResolutions {
    #[prost(message, repeated, tag = "1")]
    pub resolutions: Vec<InterruptedPendingToolCallResolution>,
}

#[derive(Clone, PartialEq, Message)]
pub struct InterruptedPendingToolCallResolution {
    #[prost(string, tag = "1")]
    pub tool_call_id: String,
    #[prost(message, optional, tag = "2")]
    pub shell_result: Option<ShellResult>,
    #[prost(message, optional, tag = "3")]
    pub task_result: Option<TaskResult>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TaskResult {
    #[prost(message, optional, tag = "1")]
    pub success: Option<TaskSuccess>,
    #[prost(message, optional, tag = "2")]
    pub error: Option<TaskError>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TaskSuccess {
    #[prost(message, repeated, tag = "1")]
    pub conversation_steps: Vec<ConversationStep>,
    #[prost(string, optional, tag = "2")]
    pub agent_id: Option<String>,
    #[prost(bool, tag = "3")]
    pub is_background: bool,
    #[prost(uint64, optional, tag = "4")]
    pub duration_ms: Option<u64>,
    #[prost(string, optional, tag = "5")]
    pub result_suffix: Option<String>,
    #[prost(int32, tag = "6")]
    pub background_reason: i32,
    #[prost(string, optional, tag = "7")]
    pub transcript_path: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TaskError {
    #[prost(string, tag = "1")]
    pub error: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConversationStep {
    #[prost(message, optional, tag = "1")]
    pub assistant_message: Option<ConversationHistoryAssistantMessage>,
    #[prost(message, optional, tag = "2")]
    pub tool_call: Option<ToolCall>,
    #[prost(message, optional, tag = "3")]
    pub thinking_message: Option<ConversationHistoryReasoningContent>,
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
/// Tag 3 is `google.protobuf.Value` wrapping a Struct (`struct_value`).
/// Encoding a raw Struct here makes Cursor see tag 1 as a length-delimited
/// map entry where Value's tag 1 is `null_value` (varint) — live symptom
/// `parse binary: invalid end group tag`. Tags 4/5 are MCP routing.
#[derive(Clone, PartialEq, Message)]
pub struct McpTool {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub description: String,
    #[prost(message, optional, tag = "3")]
    pub input_schema: Option<prost_types::Value>,
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
    /// Optional BYOK credentials accepted by the current RequestedModel
    /// schema.  The upstream schema declares these three fields as a oneof;
    /// this module keeps the same lightweight optional-field convention used
    /// by the rest of the hand-written proto definitions.  Callers should set
    /// at most one arm for a request.
    #[prost(message, optional, tag = "4")]
    pub api_key_credentials: Option<ApiKeyCredentials>,
    #[prost(message, optional, tag = "5")]
    pub azure_credentials: Option<AzureCredentials>,
    #[prost(message, optional, tag = "6")]
    pub bedrock_credentials: Option<BedrockCredentials>,
    /// Cursor's built-in catalog marker.  The desktop AgentHost sets this to
    /// true for a normal catalog model; Sand's local PromptSession uses the
    /// marker when selecting its direct executor.
    #[prost(bool, optional, tag = "7")]
    pub built_in_model: Option<bool>,
    /// False for ordinary catalog ids; true only when the model id is a
    /// variant-string representation.  Explicitly carrying false mirrors the
    /// desktop client object and avoids an undefined-vs-false branch in older
    /// managed-local runtimes.
    #[prost(bool, optional, tag = "8")]
    pub is_variant_string_representation: Option<bool>,
}

/// `agent.v1.ApiKeyCredentials` (RequestedModel credentials oneof arm).
#[derive(Clone, PartialEq, Message)]
pub struct ApiKeyCredentials {
    #[prost(string, tag = "1")]
    pub api_key: String,
    #[prost(string, optional, tag = "2")]
    pub base_url: Option<String>,
}

/// `agent.v1.AzureCredentials` (RequestedModel credentials oneof arm).
#[derive(Clone, PartialEq, Message)]
pub struct AzureCredentials {
    #[prost(string, tag = "1")]
    pub api_key: String,
    #[prost(string, tag = "2")]
    pub base_url: String,
    #[prost(string, tag = "3")]
    pub deployment: String,
}

/// `agent.v1.BedrockCredentials` (RequestedModel credentials oneof arm).
#[derive(Clone, PartialEq, Message)]
pub struct BedrockCredentials {
    #[prost(string, tag = "1")]
    pub access_key: String,
    #[prost(string, tag = "2")]
    pub secret_key: String,
    #[prost(string, tag = "3")]
    pub region: String,
    #[prost(string, optional, tag = "4")]
    pub session_token: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ModelDetails {
    #[prost(string, optional, tag = "1")]
    pub model_id: Option<String>,
    /// Optional marker emitted by current Cursor catalog responses.  The
    /// upstream type is an empty message; retaining the exact tag/type avoids
    /// mis-decoding the following display metadata on newer Sand builds.
    #[prost(message, optional, tag = "2")]
    pub thinking_details: Option<ThinkingDetails>,
    #[prost(string, optional, tag = "3")]
    pub display_model_id: Option<String>,
    #[prost(string, optional, tag = "4")]
    pub display_name: Option<String>,
    #[prost(string, optional, tag = "5")]
    pub display_name_short: Option<String>,
    #[prost(string, repeated, tag = "6")]
    pub aliases: Vec<String>,
    #[prost(bool, optional, tag = "7")]
    pub max_mode: Option<bool>,
    #[prost(message, optional, tag = "8")]
    pub api_key_credentials: Option<ApiKeyCredentials>,
    #[prost(message, optional, tag = "9")]
    pub azure_credentials: Option<AzureCredentials>,
    #[prost(message, optional, tag = "10")]
    pub bedrock_credentials: Option<BedrockCredentials>,
}

/// `agent.v1.ThinkingDetails` is currently an empty marker message.  Keep a
/// named type rather than representing field 2 as opaque bytes so protobuf
/// decoders remain aligned with Cursor's ModelDetails schema.
#[derive(Clone, PartialEq, Message)]
pub struct ThinkingDetails {}

#[derive(Clone, PartialEq, Message)]
pub struct ModelParameter {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct SelectedImage {
    // Cursor CLI uses a `data_or_blob_id` oneof. Field 1 is a blob id and
    // field 8 is the inline byte payload. Encoding image bytes in field 1
    // makes Cursor treat them as an asset id and return `Image not found`.
    #[prost(bytes = "vec", tag = "1")]
    pub blob_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "8")]
    pub data: Vec<u8>,
    /// Optional blob id plus inline bytes. This is part of the current Cursor
    /// wire schema; inline Anthropic images use `data` above because there is
    /// no server blob id available to the proxy.
    #[prost(message, optional, tag = "9")]
    pub blob_id_with_data: Option<SelectedImageBlobIdWithData>,
    #[prost(string, tag = "2")]
    pub uuid: String,
    #[prost(string, tag = "3")]
    pub path: String,
    #[prost(message, optional, tag = "4")]
    pub dimension: Option<SelectedImageDimension>,
    #[prost(string, tag = "7")]
    pub mime_type: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct SelectedImageBlobIdWithData {
    #[prost(bytes = "vec", tag = "1")]
    pub blob_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub data: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SelectedImageDimension {
    #[prost(int32, tag = "1")]
    pub width: i32,
    #[prost(int32, tag = "2")]
    pub height: i32,
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
    /// Server-side control for a pending exec (currently the abort arm).
    ///
    /// Cursor's `agent.v1` schema reserves tag 5 for this message.  It is
    /// emitted when the Agent Host cancels an in-flight native tool call; the
    /// abort id corresponds to `ExecServerMessage.id`.  Keeping this arm in
    /// the envelope lets clients observe cancellation instead of treating the
    /// frame as an unknown/empty update.
    #[prost(message, optional, tag = "5")]
    pub exec_server_control_message: Option<ExecServerControlMessage>,
    /// Approval / interactive prompts (web search, plan, ask, MCP auth).
    #[prost(message, optional, tag = "7")]
    pub interaction_query: Option<InteractionQuery>,
    /// Server-side time-to-first-token accounting. This is metadata only, but
    /// modern Agent Host streams emit it before the first visible update.
    #[prost(message, optional, tag = "8")]
    pub ttft_breakdown: Option<TtftBreakdown>,
}

/// Cursor `agent.v1.TtftBreakdown` (`AgentServerMessage` tag 8).
///
/// The fields are doubles in the Desktop schema. Keeping them typed avoids a
/// decoder silently dropping a non-empty Agent Host frame before Sand emits
/// its first interaction update.
#[derive(Clone, PartialEq, Message)]
pub struct TtftBreakdown {
    #[prost(double, tag = "1")]
    pub server_first_token_ms: f64,
    #[prost(double, tag = "2")]
    pub pre_stream_setup_ms: f64,
    #[prost(double, tag = "3")]
    pub wait_for_first_event_ms: f64,
    #[prost(double, optional, tag = "4")]
    pub provider_ttft_ms: Option<f64>,
    #[prost(double, tag = "5")]
    pub slow_pool_wait_ms: f64,
}

/// Server → client control message for an execution stream
/// (`AgentServerMessage.exec_server_control_message`, tag 5).
#[derive(Clone, PartialEq, Message)]
pub struct ExecServerControlMessage {
    #[prost(message, optional, tag = "1")]
    pub abort: Option<ExecServerAbort>,
}

/// Identifies the execution that the server aborted.
#[derive(Clone, PartialEq, Message)]
pub struct ExecServerAbort {
    #[prost(uint32, tag = "1")]
    pub id: u32,
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
    /// Successful answers from the Cursor interaction UI.  Claude-local
    /// AskUserQuestion calls are normally handed back to Claude Code instead
    /// of being answered here, but keeping the complete wire shape is
    /// important for decoding/rejecting native queries and future UI paths.
    #[prost(message, optional, tag = "1")]
    pub success: Option<AskQuestionSuccess>,
    #[prost(message, optional, tag = "2")]
    pub error: Option<AskQuestionError>,
    #[prost(message, optional, tag = "3")]
    pub rejected: Option<AskQuestionRejected>,
    #[prost(message, optional, tag = "4")]
    pub r#async: Option<AskQuestionAsync>,
}

#[derive(Clone, PartialEq, Message)]
pub struct AskQuestionSuccess {
    #[prost(message, repeated, tag = "1")]
    pub answers: Vec<AskQuestionAnswer>,
}

#[derive(Clone, PartialEq, Message)]
pub struct AskQuestionAnswer {
    #[prost(string, tag = "1")]
    pub question_id: String,
    #[prost(string, repeated, tag = "2")]
    pub selected_option_ids: Vec<String>,
    #[prost(string, optional, tag = "3")]
    pub freeform_text: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct AskQuestionError {
    #[prost(string, tag = "1")]
    pub error_message: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct AskQuestionAsync {}

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

/// InteractionUpdate oneof fields (CLI 2026.07 / Cursor Desktop 3.17.19
/// `agent.v1`):
/// 1=text_delta, 2=tool_call_started, 3=tool_call_completed, 4=thinking_delta,
/// 5=thinking_completed, 6=user_message_appended, 7=partial_tool_call,
/// 8=token_delta, 9-12=summary/shell updates, 13=heartbeat, 14=turn_ended,
/// 15=tool_call_delta, 16-24=Agent Host lifecycle/metadata updates.
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
    #[prost(message, optional, tag = "6")]
    pub user_message_appended: Option<UserMessageAppendedUpdate>,
    /// Streaming tool args before `tool_call_started` (CLI `PartialToolCallUpdate`).
    #[prost(message, optional, tag = "7")]
    pub partial_tool_call: Option<PartialToolCall>,
    #[prost(message, optional, tag = "8")]
    pub token_delta: Option<TokenDelta>,
    #[prost(message, optional, tag = "9")]
    pub summary: Option<SummaryUpdate>,
    #[prost(message, optional, tag = "10")]
    pub summary_started: Option<SummaryStartedUpdate>,
    #[prost(message, optional, tag = "11")]
    pub summary_completed: Option<SummaryCompletedUpdate>,
    #[prost(message, optional, tag = "12")]
    pub shell_output_delta: Option<ShellStream>,
    /// Server keep-alive during long thinking (CLI tag 13). Must refresh our
    /// idle timers — otherwise quiet Fable thinking looks stalled.
    #[prost(message, optional, tag = "13")]
    pub heartbeat: Option<InteractionHeartbeat>,
    #[prost(message, optional, tag = "14")]
    pub turn_ended: Option<TurnEnded>,
    /// Shell/edit/task stream deltas (CLI `ToolCallDeltaUpdate`). MCP/Workflow
    /// args usually stream on `partial_tool_call`; a Task delta (tag 2) may
    /// nest another InteractionUpdate that itself carries `partial_tool_call`.
    #[prost(message, optional, tag = "15")]
    pub tool_call_delta: Option<ToolCallDeltaUpdate>,
    #[prost(message, optional, tag = "16")]
    pub step_started: Option<StepStartedUpdate>,
    #[prost(message, optional, tag = "17")]
    pub step_completed: Option<StepCompletedUpdate>,
    #[prost(message, optional, tag = "18")]
    pub prompt_suggestion: Option<PromptSuggestionUpdate>,
    #[prost(message, optional, tag = "19")]
    pub post_request_prompt: Option<PostRequestPromptUpdate>,
    #[prost(message, optional, tag = "20")]
    pub active_branch_change: Option<ActiveBranchChange>,
    #[prost(message, optional, tag = "21")]
    pub feedback_request: Option<FeedbackRequestUpdate>,
    #[prost(message, optional, tag = "22")]
    pub response_comparison: Option<ResponseComparisonUpdate>,
    #[prost(message, optional, tag = "23")]
    pub context_injection_state: Option<ContextInjectionStateUpdate>,
    #[prost(message, optional, tag = "24")]
    pub routed_model: Option<RoutedModelUpdate>,
}

impl AgentServerMessage {
    /// Whether this frame proves the Agent Host advanced without carrying an
    /// Anthropic-visible text or tool event. These lifecycle frames are valid
    /// progress for retry/idle bookkeeping, but callers must not translate
    /// them into model output.
    pub(crate) fn has_agent_host_metadata_progress(&self) -> bool {
        self.ttft_breakdown.is_some()
            || self
                .interaction_update
                .as_ref()
                .is_some_and(InteractionUpdate::has_agent_host_metadata_progress)
    }
}

impl InteractionUpdate {
    /// Desktop Agent Host lifecycle/UI metadata that can legitimately arrive
    /// before the first text delta. Keep this separate from `heartbeat`: a
    /// heartbeat proves only that the transport remains open, whereas these
    /// fields prove the server actually processed the run.
    pub(crate) fn has_agent_host_metadata_progress(&self) -> bool {
        self.has_agent_host_metadata_progress_at_depth(0)
    }

    fn has_agent_host_metadata_progress_at_depth(&self, depth: u8) -> bool {
        let direct = self.user_message_appended.is_some()
            || self.summary.is_some()
            || self.summary_started.is_some()
            || self.summary_completed.is_some()
            || self.shell_output_delta.is_some()
            || self.step_started.is_some()
            || self.step_completed.is_some()
            || self.prompt_suggestion.is_some()
            || self.post_request_prompt.is_some()
            || self.active_branch_change.is_some()
            || self.feedback_request.is_some()
            || self.response_comparison.is_some()
            || self.context_injection_state.is_some()
            || self.routed_model.is_some();
        if direct || depth >= MAX_TASK_DELTA_NEST {
            return direct;
        }
        self.tool_call_delta
            .as_ref()
            .and_then(ToolCallDeltaUpdate::nested_task_update)
            .is_some_and(|nested| nested.has_agent_host_metadata_progress_at_depth(depth + 1))
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct UserMessageAppendedUpdate {
    #[prost(message, optional, tag = "1")]
    pub user_message: Option<UserMessage>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SummaryUpdate {
    #[prost(string, tag = "1")]
    pub summary: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct SummaryStartedUpdate {}

#[derive(Clone, PartialEq, Message)]
pub struct SummaryCompletedUpdate {
    #[prost(string, optional, tag = "1")]
    pub hook_message: Option<String>,
    #[prost(bool, optional, tag = "2")]
    pub failed: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
pub struct StepStartedUpdate {
    #[prost(uint64, tag = "1")]
    pub step_id: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct StepCompletedUpdate {
    #[prost(uint64, tag = "1")]
    pub step_id: u64,
    #[prost(int64, tag = "2")]
    pub step_duration_ms: i64,
}

#[derive(Clone, PartialEq, Message)]
pub struct PromptSuggestionUpdate {
    #[prost(string, tag = "1")]
    pub suggestion: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct PostRequestPromptUpdate {
    #[prost(string, tag = "1")]
    pub title: String,
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(string, tag = "3")]
    pub button_label: String,
    #[prost(string, tag = "4")]
    pub button_url: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ActiveBranchChange {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, tag = "2")]
    pub branch_name: String,
}

/// Feedback metadata is intentionally decoded only through stable scalar
/// fields. The nested category schema is UI-only and can evolve without
/// affecting live-run recovery.
#[derive(Clone, PartialEq, Message)]
pub struct FeedbackRequestUpdate {
    #[prost(string, tag = "1")]
    pub request_id: String,
    #[prost(string, optional, tag = "2")]
    pub canonical_model_name: Option<String>,
}

/// Response-comparison events are UI metadata. Retaining the id lets the
/// proxy recognize the frame as progress while safely skipping newer arms.
#[derive(Clone, PartialEq, Message)]
pub struct ResponseComparisonUpdate {
    #[prost(string, tag = "1")]
    pub comparison_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ContextInjectionStateUpdate {
    #[prost(string, tag = "1")]
    pub injection_id: String,
    #[prost(message, optional, tag = "2")]
    pub state: Option<ContextInjectionState>,
}

/// The state is a oneof whose payloads are all UI bookkeeping. An empty
/// typed message safely consumes all known/future state arms while preserving
/// the outer progress signal.
#[derive(Clone, PartialEq, Message)]
pub struct ContextInjectionState {}

#[derive(Clone, PartialEq, Message)]
pub struct RoutedModelUpdate {
    #[prost(string, tag = "1")]
    pub display_name: String,
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

/// CLI `PartialToolCallUpdate` (InteractionUpdate tag 7).
/// `args_text_delta` is aggregated JSON text so far (may be incomplete).
#[derive(Clone, PartialEq, Message)]
pub struct PartialToolCall {
    #[prost(string, tag = "1")]
    pub call_id: String,
    #[prost(message, optional, tag = "2")]
    pub tool_call: Option<ToolCall>,
    #[prost(string, tag = "3")]
    pub args_text_delta: String,
    #[prost(string, tag = "4")]
    pub model_call_id: String,
}

/// CLI `ToolCallDeltaUpdate` (InteractionUpdate tag 15).
#[derive(Clone, PartialEq, Message)]
pub struct ToolCallDeltaUpdate {
    #[prost(string, tag = "1")]
    pub call_id: String,
    #[prost(message, optional, tag = "2")]
    pub tool_call_delta: Option<ToolCallDelta>,
    #[prost(string, tag = "3")]
    pub model_call_id: String,
}

/// Inner oneof for [`ToolCallDeltaUpdate`].
///
/// Cursor.app + 0xlane `agent_v1.proto`:
/// `shell=1`, `task=2` (`TaskToolCallDelta` → nested `InteractionUpdate`),
/// `edit=3`. Cursor.app also has `replace_env=4` (not decoded here).
/// Nested `InteractionUpdate` is boxed so the type is finite; live/buffered
/// processing applies [`MAX_TASK_DELTA_NEST`] extra level(s).
#[derive(Clone, PartialEq, Message)]
pub struct ToolCallDelta {
    #[prost(message, optional, tag = "1")]
    pub shell_tool_call_delta: Option<ShellToolCallDelta>,
    #[prost(message, optional, tag = "2")]
    pub task_tool_call_delta: Option<TaskToolCallDelta>,
    #[prost(message, optional, tag = "3")]
    pub edit_tool_call_delta: Option<EditToolCallDelta>,
}

/// `agent.v1.TaskToolCallDelta` — `interaction_update = 1`.
#[derive(Clone, PartialEq, Message)]
pub struct TaskToolCallDelta {
    #[prost(message, optional, boxed, tag = "1")]
    pub interaction_update: Option<Box<InteractionUpdate>>,
}

/// Nested Task deltas recurse (`ToolCallDelta` → `TaskToolCallDelta` →
/// `InteractionUpdate` → `ToolCallDelta`). Process one extra level only.
pub const MAX_TASK_DELTA_NEST: u8 = 1;

impl ToolCallDeltaUpdate {
    pub fn into_nested_task_update(self) -> Option<Box<InteractionUpdate>> {
        self.tool_call_delta
            .and_then(|d| d.task_tool_call_delta)
            .and_then(|t| t.interaction_update)
    }

    pub fn nested_task_update(&self) -> Option<&InteractionUpdate> {
        self.tool_call_delta
            .as_ref()
            .and_then(|d| d.task_tool_call_delta.as_ref())
            .and_then(|t| t.interaction_update.as_deref())
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct ShellToolCallDelta {
    #[prost(message, optional, tag = "1")]
    pub stdout: Option<ShellStreamTextDelta>,
    #[prost(message, optional, tag = "2")]
    pub stderr: Option<ShellStreamTextDelta>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ShellStreamTextDelta {
    #[prost(string, tag = "1")]
    pub content: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct EditToolCallDelta {
    #[prost(string, tag = "1")]
    pub stream_content_delta: String,
}

/// ToolCall oneof (CLI 2026.07) — tools we map to Claude Code.
///
/// Pi tool siblings are decoded for transcript correlation. Filesystem
/// mutations still arrive on the matching `ExecServerMessage` exec path.
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
    /// 0xlane `agent_v1.proto` + Cursor.app: `task_tool_call = 19`.
    /// Native Cursor Task / subagent. Mapped to grok-build `spawn_subagent`.
    #[prost(message, optional, tag = "19")]
    pub task_tool_call: Option<TaskToolCall>,
    #[prost(message, optional, tag = "23")]
    pub ask_question_tool_call: Option<AskQuestionToolCall>,
    #[prost(message, optional, tag = "24")]
    pub fetch_tool_call: Option<FetchToolCall>,
    /// Distinct from `fetch_tool_call` (24). Cursor.app + 0xlane `agent_v1.proto`.
    #[prost(message, optional, tag = "37")]
    pub web_fetch_tool_call: Option<WebFetchToolCall>,
    /// Cursor 3.12+ Pi write tool (field 64). The matching filesystem exec is
    /// `ExecServerMessage.pi_write_args` (field 48).
    #[prost(message, optional, tag = "64")]
    pub pi_write_tool_call: Option<PiWriteToolCall>,
    /// Cursor 3.12+ Pi string-replacement edit tool (field 63).  The matching
    /// filesystem exec is `ExecServerMessage.pi_edit_args` (field 47).
    #[prost(message, optional, tag = "63")]
    pub pi_edit_tool_call: Option<PiEditToolCall>,
}

/// Cursor Pi write tool call used in `InteractionUpdate.tool_call_started`.
#[derive(Clone, PartialEq, Message)]
pub struct PiWriteToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<PiWriteToolArgs>,
    #[prost(message, optional, tag = "2")]
    pub result: Option<PiWriteToolResult>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiWriteToolArgs {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, tag = "2")]
    pub content: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiWriteToolResult {
    #[prost(message, optional, tag = "1")]
    pub success: Option<PiWriteToolSuccess>,
    #[prost(message, optional, tag = "2")]
    pub error: Option<PiWriteToolError>,
    #[prost(message, optional, tag = "3")]
    pub rejected: Option<PiWriteToolRejected>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiWriteToolSuccess {
    #[prost(string, tag = "1")]
    pub output: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiWriteToolError {
    #[prost(string, tag = "1")]
    pub error: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiWriteToolRejected {
    #[prost(string, tag = "1")]
    pub reason: String,
}

/// Cursor Pi edit tool call used in `InteractionUpdate.tool_call_started`.
///
/// Pi edit is distinct from the legacy Cursor `EditToolCall`: it carries one
/// or more exact string replacements rather than a complete file body.
#[derive(Clone, PartialEq, Message)]
pub struct PiEditToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<PiEditToolArgs>,
    #[prost(message, optional, tag = "2")]
    pub result: Option<PiEditToolResult>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiEditToolArgs {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(message, repeated, tag = "2")]
    pub edits: Vec<PiEditReplacement>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiEditReplacement {
    #[prost(string, tag = "1")]
    pub old_text: String,
    #[prost(string, tag = "2")]
    pub new_text: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiEditToolResult {
    #[prost(message, optional, tag = "1")]
    pub success: Option<PiEditToolSuccess>,
    #[prost(message, optional, tag = "2")]
    pub error: Option<PiEditToolError>,
    #[prost(message, optional, tag = "3")]
    pub rejected: Option<PiEditToolRejected>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiEditToolSuccess {
    #[prost(string, tag = "1")]
    pub output: String,
    #[prost(string, tag = "2")]
    pub diff: String,
    #[prost(string, tag = "3")]
    pub patch: String,
    #[prost(uint32, optional, tag = "4")]
    pub first_changed_line: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiEditToolError {
    #[prost(string, tag = "1")]
    pub error: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiEditToolRejected {
    #[prost(string, tag = "1")]
    pub reason: String,
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
    /// Values are UTF-8 JSON fragments or `google.protobuf.Value` bytes
    /// (`string_value=3`, `bool_value=4`). Decode in `decode_mcp_arg_value`.
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

/// ToolCall tag 37. Args match `WebFetchArgs` (url=1, tool_call_id=2).
#[derive(Clone, PartialEq, Message)]
pub struct WebFetchToolCall {
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

/// `agent.v1.TaskToolCall` — Cursor native Task / subagent (ToolCall tag 19).
#[derive(Clone, PartialEq, Message)]
pub struct TaskToolCall {
    #[prost(message, optional, tag = "1")]
    pub args: Option<TaskToolCallArgsProto>,
}

/// Wire args for model-initiated [`TaskToolCall`] (ToolCall tag 19).
///
/// Layout matches 0xlane `TaskToolCallArgsProto` (string `subagent_type`).
/// Cursor has shipped both the documented bool-varint representation and a
/// length-delimited representation for the optional background flag (and has
/// likewise varied the unused readonly field at tag 6). A generated `bool`
/// field rejects the former representation and drops the entire
/// `InteractionUpdate`, so this small decoder accepts both wire forms.
/// This is not `ExecServerMessage.subagent_args` / `TaskArgs` (tag 28), whose
/// `subagent_type` is a nested message.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TaskToolCallArgsProto {
    pub description: String,
    pub prompt: String,
    pub model: Option<String>,
    pub subagent_type: String,
    pub resume: Option<String>,
    pub run_in_background: Option<bool>,
}

impl Message for TaskToolCallArgsProto {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        if !self.description.is_empty() {
            prost::encoding::string::encode(1, &self.description, buf);
        }
        if !self.prompt.is_empty() {
            prost::encoding::string::encode(2, &self.prompt, buf);
        }
        if let Some(model) = &self.model {
            prost::encoding::string::encode(3, model, buf);
        }
        if !self.subagent_type.is_empty() {
            prost::encoding::string::encode(4, &self.subagent_type, buf);
        }
        if let Some(resume) = &self.resume {
            prost::encoding::string::encode(5, resume, buf);
        }
        if let Some(background) = self.run_in_background {
            prost::encoding::bool::encode(7, &background, buf);
        }
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        use prost::encoding::{WireType, bool, bytes, skip_field, string};

        match tag {
            1 => string::merge(wire_type, &mut self.description, buf, ctx),
            2 => string::merge(wire_type, &mut self.prompt, buf, ctx),
            3 => string::merge(
                wire_type,
                self.model.get_or_insert_with(String::new),
                buf,
                ctx,
            ),
            4 => string::merge(wire_type, &mut self.subagent_type, buf, ctx),
            5 => string::merge(
                wire_type,
                self.resume.get_or_insert_with(String::new),
                buf,
                ctx,
            ),
            6 => {
                // `readonly` is not used by the bridge. Keep consuming it
                // regardless of the wire type Cursor chooses.
                skip_field(wire_type, tag, buf, ctx)
            }
            7 => match wire_type {
                WireType::Varint => bool::merge(
                    wire_type,
                    self.run_in_background.get_or_insert(false),
                    buf,
                    ctx,
                ),
                WireType::LengthDelimited => {
                    let mut raw = Vec::new();
                    bytes::merge(wire_type, &mut raw, buf, ctx.clone())?;
                    self.run_in_background = parse_flexible_background(&raw);
                    Ok(())
                }
                _ => skip_field(wire_type, tag, buf, ctx),
            },
            _ => skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        let mut len = 0;
        if !self.description.is_empty() {
            len += prost::encoding::string::encoded_len(1, &self.description);
        }
        if !self.prompt.is_empty() {
            len += prost::encoding::string::encoded_len(2, &self.prompt);
        }
        if let Some(model) = &self.model {
            len += prost::encoding::string::encoded_len(3, model);
        }
        if !self.subagent_type.is_empty() {
            len += prost::encoding::string::encoded_len(4, &self.subagent_type);
        }
        if let Some(resume) = &self.resume {
            len += prost::encoding::string::encoded_len(5, resume);
        }
        if let Some(background) = self.run_in_background {
            len += prost::encoding::bool::encoded_len(7, &background);
        }
        len
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

fn parse_flexible_background(raw: &[u8]) -> Option<bool> {
    let text = std::str::from_utf8(raw).ok()?.trim();
    if text.eq_ignore_ascii_case("true") || text == "1" {
        return Some(true);
    }
    if text.eq_ignore_ascii_case("false") || text == "0" {
        return Some(false);
    }

    // Some protobuf wrappers encode a BoolValue as field 1 = varint.
    let mut nested = raw;
    let (tag, wire_type) = prost::encoding::decode_key(&mut nested).ok()?;
    if tag != 1 || wire_type != prost::encoding::WireType::Varint {
        return None;
    }
    let mut value = false;
    prost::encoding::bool::merge(
        wire_type,
        &mut value,
        &mut nested,
        prost::encoding::DecodeContext::default(),
    )
    .ok()?;
    Some(value)
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
    #[prost(bool, tag = "5")]
    pub run_async: bool,
    #[prost(string, tag = "6")]
    pub async_original_tool_call_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct AskQuestionItem {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub prompt: String,
    #[prost(message, repeated, tag = "3")]
    pub options: Vec<AskQuestionOption>,
    #[prost(bool, tag = "4")]
    pub allow_multiple: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct AskQuestionOption {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub label: String,
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
    /// Milliseconds (Cursor CLI `agent.v1.ShellArgs.timeout`; Claude Bash too).
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
/// Decoded payloads include both the legacy `write_args` and the modern Pi
/// `pi_write_args` shape. Keeping both is required because Cursor has shipped
/// both exec variants in otherwise identical agent runs.
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
    /// Cursor 3.12+ Pi write exec (field 48).
    #[prost(message, optional, tag = "48")]
    pub pi_write_args: Option<PiWriteExecArgs>,
    /// Cursor 3.12+ Pi string-replacement edit exec (field 47).
    #[prost(message, optional, tag = "47")]
    pub pi_edit_args: Option<PiEditExecArgs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiWriteExecArgs {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, tag = "2")]
    pub content: String,
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
    /// Result for `ExecServerMessage.pi_write_args` (field 49).
    #[prost(message, optional, tag = "49")]
    pub pi_write_result: Option<PiWriteExecResult>,
    /// Result for `ExecServerMessage.pi_edit_args` (field 48).
    #[prost(message, optional, tag = "48")]
    pub pi_edit_result: Option<PiEditExecResult>,
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

#[derive(Clone, PartialEq, Message)]
pub struct PiWriteExecResult {
    #[prost(message, optional, tag = "1")]
    pub success: Option<PiWriteExecSuccess>,
    #[prost(message, optional, tag = "2")]
    pub error: Option<PiWriteExecError>,
    #[prost(message, optional, tag = "3")]
    pub rejected: Option<PiWriteExecRejected>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiWriteExecSuccess {
    #[prost(string, tag = "1")]
    pub output: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiWriteExecError {
    #[prost(string, tag = "1")]
    pub error: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiWriteExecRejected {
    #[prost(string, tag = "1")]
    pub reason: String,
}

/// Cursor Pi edit exec args.  The shape intentionally mirrors
/// [`PiEditToolArgs`], but it is a separate wire message in agent.v1.
#[derive(Clone, PartialEq, Message)]
pub struct PiEditExecArgs {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(message, repeated, tag = "2")]
    pub edits: Vec<PiEditReplacement>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiEditExecResult {
    #[prost(message, optional, tag = "1")]
    pub success: Option<PiEditExecSuccess>,
    #[prost(message, optional, tag = "2")]
    pub error: Option<PiEditExecError>,
    #[prost(message, optional, tag = "3")]
    pub rejected: Option<PiEditExecRejected>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiEditExecSuccess {
    #[prost(string, tag = "1")]
    pub output: String,
    #[prost(string, tag = "2")]
    pub diff: String,
    #[prost(string, tag = "3")]
    pub patch: String,
    #[prost(uint32, optional, tag = "4")]
    pub first_changed_line: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiEditExecError {
    #[prost(string, tag = "1")]
    pub error: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct PiEditExecRejected {
    #[prost(string, tag = "1")]
    pub reason: String,
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

/// `agent.v1.CursorRule` (RequestContext.rules tag 2). Minimal encodable shape.
#[derive(Clone, PartialEq, Message)]
pub struct CursorRule {
    #[prost(string, tag = "1")]
    pub full_path: String,
    #[prost(string, tag = "2")]
    pub content: String,
}

/// `agent.v1.AgentSkill` (RequestContext.agent_skills tag 29).
#[derive(Clone, PartialEq, Message)]
pub struct AgentSkill {
    #[prost(string, tag = "1")]
    pub full_path: String,
    #[prost(string, tag = "2")]
    pub content: String,
    #[prost(string, tag = "3")]
    pub description: String,
    #[prost(string, repeated, tag = "13")]
    pub globs: Vec<String>,
}

/// `agent.v1.RequestContextEnv` (CLI). `process_working_directory` is tag 21
/// on current CLI (not present in older open-cursor 0.1.0 extracts).
#[derive(Clone, PartialEq, Message)]
pub struct RequestContextEnv {
    #[prost(string, tag = "1")]
    pub os_version: String,
    #[prost(string, repeated, tag = "2")]
    pub workspace_paths: Vec<String>,
    #[prost(string, tag = "3")]
    pub shell: String,
    #[prost(bool, tag = "5")]
    pub sandbox_enabled: bool,
    #[prost(string, tag = "10")]
    pub time_zone: String,
    #[prost(string, tag = "11")]
    pub project_folder: String,
    /// Absolute cwd the agent should treat as process working directory.
    #[prost(string, tag = "21")]
    pub process_working_directory: String,
}

/// `agent.v1.GitRepoInfo`.
#[derive(Clone, PartialEq, Message)]
pub struct GitRepoInfo {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, tag = "2")]
    pub status: String,
    #[prost(string, tag = "3")]
    pub branch_name: String,
    #[prost(string, optional, tag = "4")]
    pub remote_url: Option<String>,
}

/// CLI `agent.v1.RequestContext`.
///
/// Filled when the server sends `ExecServerMessage.request_context_args` (tag 10)
/// and the client replies `ExecClientMessage.request_context_result` (tag 10).
/// Not a field on `AgentRunRequest`. live.rs owns the reply; this type must
/// encode the CLI tags so that reply can be populated.
///
/// Prost `Message` supplies `Default` (empty encode == previous empty message).
/// Call sites that used `RequestContext {}` should use `RequestContext::default()`
/// — prost does not accept Rust field-default syntax.
#[derive(Clone, PartialEq, Message)]
pub struct RequestContext {
    #[prost(message, repeated, tag = "2")]
    pub rules: Vec<CursorRule>,
    #[prost(message, optional, tag = "4")]
    pub env: Option<RequestContextEnv>,
    #[prost(message, repeated, tag = "11")]
    pub git_repos: Vec<GitRepoInfo>,
    #[prost(message, repeated, tag = "29")]
    pub agent_skills: Vec<AgentSkill>,
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_server_exec_control_abort_round_trips_tag_five() {
        let message = AgentServerMessage {
            exec_server_control_message: Some(ExecServerControlMessage {
                abort: Some(ExecServerAbort { id: 42 }),
            }),
            ..Default::default()
        };
        let bytes = message.encode_to_vec();
        // AgentServerMessage tag 5 is length-delimited (0x2a). The nested
        // control arm (tag 1) wraps the abort id (field 1, varint 42).
        assert_eq!(bytes, vec![0x2a, 0x04, 0x0a, 0x02, 0x08, 0x2a]);
        let decoded = AgentServerMessage::decode(bytes.as_slice()).expect("decode control");
        assert_eq!(decoded, message);
        assert_eq!(
            decoded
                .exec_server_control_message
                .and_then(|control| control.abort)
                .map(|abort| abort.id),
            Some(42)
        );
    }

    #[test]
    fn run_request_extended_agent_host_fields_round_trip() {
        // These fields were added to Cursor's AgentRunRequest after the
        // original CLI schema.  Keep a fixture in the proxy so a schema
        // refresh cannot silently drop managed-local/Sand metadata.
        let request = RunRequest {
            conversation_state: Some(vec![]),
            action: None,
            model_details: None,
            mcp_tools: None,
            conversation_id: Some("conv".into()),
            custom_system_prompt: None,
            requested_model: Some(CursorModel {
                model_id: "claude-fable-5-thinking-max".into(),
                max_mode: Some(true),
                parameters: vec![ModelParameter {
                    id: "effort".into(),
                    value: "max".into(),
                }],
                api_key_credentials: None,
                azure_credentials: None,
                bedrock_credentials: None,
                built_in_model: Some(true),
                is_variant_string_representation: Some(false),
            }),
            exclude_workspace_context: Some(false),
            harness: Some("sand-client".into()),
            selected_subagent_models: vec![],
            conversation_group_id: None,
            pre_fetched_blobs: vec![],
            client_supports_inline_images: Some(true),
            mcp_file_system_options: Some(McpFileSystemOptions {
                enabled: true,
                workspace_project_dir: "/workspace".into(),
                mcp_descriptors: vec![McpDescriptor {
                    server_name: "local".into(),
                    server_identifier: "local-1".into(),
                    folder_path: Some("/workspace/.mcp".into()),
                    server_use_instructions: None,
                    tools: vec![McpToolDescriptor {
                        tool_name: "read".into(),
                        definition_path: None,
                        description: Some("read file".into()),
                        input_schema: None,
                        input_schema_json: Some("{}".into()),
                        annotations_json: None,
                    }],
                    plugin: None,
                    marketplace: None,
                    plugin_db_id: None,
                    marketplace_id: None,
                }],
            }),
            skill_options: Some(SkillOptions {
                skill_descriptors: vec![SkillDescriptor {
                    name: "shell".into(),
                    description: "run shell commands".into(),
                    folder_path: "/workspace/.cursor/skills".into(),
                    enabled: true,
                    parse_error: None,
                    readme_file_path: "/workspace/.cursor/skills/README.md".into(),
                    package_type: 1,
                }],
            }),
            suggest_next_prompt: Some(true),
            subagent_type_name: Some("worker".into()),
            selected_subagent_model_details: vec![ModelDetails {
                model_id: Some("composer-2.5".into()),
                thinking_details: None,
                display_name_short: Some("Composer".into()),
                display_model_id: Some("composer-2.5".into()),
                display_name: Some("Composer".into()),
                aliases: vec!["composer".into()],
                max_mode: None,
                api_key_credentials: None,
                azure_credentials: None,
                bedrock_credentials: None,
            }],
            dev_raw_model_slug: Some("fable".into()),
            subagent_model_overrides: vec![SubagentModelOverride {
                subagent_type: "worker".into(),
                model: Some(CursorModel {
                    model_id: "composer-2.5".into(),
                    max_mode: None,
                    parameters: vec![],
                    api_key_credentials: None,
                    azure_credentials: None,
                    bedrock_credentials: None,
                    built_in_model: None,
                    is_variant_string_representation: None,
                }),
                inherit: None,
                disabled: None,
            }],
            can_create_cloud_subagents: Some(false),
            suppress_subagent_progress_update_tool: Some(true),
            client_supports_send_to_user: Some(true),
            computer_use_coordinate_mode: Some("screen-pixels".into()),
            run_id: Some("run-1".into()),
            agent_session_id: Some("session-1".into()),
            client_supports_prompt_context_usage_rpc: Some(false),
            client_supports_routed_model_update: Some(false),
            system_prompt_spec: Some(SystemPromptSpec {
                replace: Some("fixture system prompt".into()),
                append: None,
            }),
            client_llm_gateway_credential: Some(ClientLlmGatewayCredential {
                bearer_token: "fixture-only-token".into(),
            }),
            client_supports_preview_card: Some(false),
            started_as_new_project: Some(true),
        };

        let mut encoded = Vec::new();
        request
            .encode(&mut encoded)
            .expect("encode extended request");
        let decoded = RunRequest::decode(encoded.as_slice()).expect("decode extended request");
        assert_eq!(decoded, request);

        // Tags 24–31 use two-byte protobuf keys; tag 32 switches to a
        // three-byte key.  Keep each one visible so a schema refresh cannot
        // silently truncate new desktop metadata.
        for key in [
            [0xc2, 0x01], // computer_use_coordinate_mode
            [0xca, 0x01], // run_id
            [0xd2, 0x01], // agent_session_id
            [0xd8, 0x01], // prompt-context capability
            [0xe0, 0x01], // routed-model capability
            [0xea, 0x01], // system_prompt_spec
            [0xf2, 0x01], // client_llm_gateway_credential
            [0xf8, 0x01], // preview-card capability
            [0x80, 0x02], // started_as_new_project
        ] {
            assert!(
                encoded.windows(2).any(|window| window == key),
                "missing AgentRunRequest extension key {key:?} in {encoded:?}"
            );
        }
    }

    #[test]
    fn requested_model_current_schema_fields_round_trip() {
        // Cursor 3.18's RequestedModel has credential arms (tags 4–6) and
        // explicit built-in/variant markers (tags 7–8).  Keep a focused
        // fixture so a future proto edit does not silently truncate these
        // fields while forwarding a Sand request.
        let model = CursorModel {
            model_id: "claude-fable-5".into(),
            max_mode: Some(false),
            parameters: vec![ModelParameter {
                id: "effort".into(),
                value: "high".into(),
            }],
            api_key_credentials: Some(ApiKeyCredentials {
                api_key: "KEY".into(),
                base_url: Some("https://gateway.invalid".into()),
            }),
            azure_credentials: None,
            bedrock_credentials: None,
            built_in_model: Some(true),
            is_variant_string_representation: Some(false),
        };
        let mut encoded = Vec::new();
        model.encode(&mut encoded).expect("encode RequestedModel");
        let decoded = CursorModel::decode(encoded.as_slice()).expect("decode RequestedModel");
        assert_eq!(decoded, model);

        // Verify the other credential message shapes independently.  They
        // are oneof arms upstream, so callers should never populate more than
        // one in a single model object.
        for credential in [
            CursorModel {
                model_id: "azure-model".into(),
                max_mode: None,
                parameters: vec![],
                api_key_credentials: None,
                azure_credentials: Some(AzureCredentials {
                    api_key: "AZURE_KEY".into(),
                    base_url: "https://azure.invalid".into(),
                    deployment: "deployment".into(),
                }),
                bedrock_credentials: None,
                built_in_model: None,
                is_variant_string_representation: None,
            },
            CursorModel {
                model_id: "bedrock-model".into(),
                max_mode: None,
                parameters: vec![],
                api_key_credentials: None,
                azure_credentials: None,
                bedrock_credentials: Some(BedrockCredentials {
                    access_key: "ACCESS".into(),
                    secret_key: "SECRET".into(),
                    region: "us-east-1".into(),
                    session_token: Some("SESSION".into()),
                }),
                built_in_model: None,
                is_variant_string_representation: None,
            },
        ] {
            let mut bytes = Vec::new();
            credential.encode(&mut bytes).unwrap();
            assert_eq!(CursorModel::decode(bytes.as_slice()).unwrap(), credential);
        }
    }

    #[test]
    fn model_details_current_schema_field_numbers_round_trip() {
        // Cursor 3.18 places display_name_short/aliases at tags 5/6 and
        // reserves tag 2 for the empty ThinkingDetails marker.  A previous
        // hand-written schema put these at 2/5, which made a desktop response
        // decode as an invalid string/message pair.
        let details = ModelDetails {
            model_id: Some("claude-fable-5".into()),
            thinking_details: Some(ThinkingDetails {}),
            display_model_id: Some("fable".into()),
            display_name: Some("Fable".into()),
            display_name_short: Some("Fable".into()),
            aliases: vec!["fable".into(), "claude-fable-5".into()],
            max_mode: Some(true),
            api_key_credentials: None,
            azure_credentials: None,
            bedrock_credentials: None,
        };
        let mut bytes = Vec::new();
        details.encode(&mut bytes).expect("encode ModelDetails");
        let decoded = ModelDetails::decode(bytes.as_slice()).expect("decode ModelDetails");
        assert_eq!(decoded, details);
        // Empty marker is encoded as key 0x12 + zero-length payload, while
        // display_name_short/aliases use keys 0x2a/0x32 respectively.
        assert!(bytes.windows(2).any(|window| window == [0x12, 0x00]));
        assert!(bytes.contains(&0x2a));
        assert!(bytes.contains(&0x32));
    }
    use prost::Message;

    #[test]
    fn empty_request_context_encodes_to_zero_bytes() {
        let ctx = RequestContext::default();
        let mut buf = Vec::new();
        ctx.encode(&mut buf).unwrap();
        assert!(buf.is_empty(), "empty RequestContext must stay wire-empty");
    }

    #[test]
    fn empty_run_request_encodes_to_zero_bytes() {
        // Cursor.app AgentRunRequest tags 1–23 have no thinking / max_tokens /
        // tool_choice. Default must not invent those (or any) tags on the wire.
        let req = RunRequest::default();
        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        assert!(buf.is_empty(), "empty RunRequest must stay wire-empty");
    }

    #[test]
    fn modern_agent_client_control_actions_round_trip() {
        // Cursor Desktop uses AgentClientMessage tags 4/8 for actions that
        // happen outside a model turn.  Keep a fixture covering both arms so
        // a future schema cleanup does not silently renumber them.
        let action = ConversationAction {
            summarize_action: Some(SummarizeAction {}),
            ..Default::default()
        };
        let prewarm = PrewarmRequest {
            model_details: Some(ModelDetails {
                model_id: Some("claude-fable-5".into()),
                thinking_details: None,
                display_model_id: Some("claude-fable-5".into()),
                display_name: Some("Claude Fable 5".into()),
                display_name_short: Some("Fable 5".into()),
                aliases: vec!["fable5".into()],
                max_mode: Some(true),
                api_key_credentials: None,
                azure_credentials: None,
                bedrock_credentials: None,
            }),
            conversation_id: Some("conversation-1".into()),
            conversation_state: Some(vec![1, 2, 3]),
            requested_model: Some(CursorModel {
                model_id: "claude-fable-5".into(),
                max_mode: Some(true),
                parameters: vec![],
                api_key_credentials: None,
                azure_credentials: None,
                bedrock_credentials: None,
                built_in_model: Some(true),
                is_variant_string_representation: Some(false),
            }),
            client_supports_inline_images: Some(true),
            ..Default::default()
        };
        let message = AgentClientMessage {
            conversation_action: Some(action),
            prewarm_request: Some(prewarm),
            ..Default::default()
        };
        let mut bytes = Vec::new();
        message.encode(&mut bytes).unwrap();
        assert!(bytes.contains(&0x22), "conversation_action tag 4 missing");
        assert!(bytes.contains(&0x42), "prewarm_request tag 8 missing");
        let decoded = AgentClientMessage::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.conversation_action, message.conversation_action);
        assert_eq!(decoded.prewarm_request, message.prewarm_request);
    }

    #[test]
    fn prewarm_request_current_desktop_extension_fields_round_trip() {
        // Cursor Desktop 3.17.19 declares PrewarmRequest tags 23–29 with a
        // different layout from AgentRunRequest's tags 24–32. In particular,
        // Prewarm tag 27 is the gateway credential and tag 29 is the boolean
        // new-project marker; neither must be decoded as AgentRunRequest's
        // system-prompt shape.
        let request = PrewarmRequest {
            computer_use_coordinate_mode: Some("screen-pixels".into()),
            agent_session_id: Some("prewarm-session-1".into()),
            client_supports_prompt_context_usage_rpc: Some(false),
            client_supports_routed_model_update: Some(true),
            client_llm_gateway_credential: Some(ClientLlmGatewayCredential {
                bearer_token: "fixture-gateway-token".into(),
            }),
            client_supports_preview_card: Some(false),
            started_as_new_project: Some(true),
            ..Default::default()
        };

        let encoded = request.encode_to_vec();
        let decoded = PrewarmRequest::decode(encoded.as_slice()).expect("decode PrewarmRequest");
        assert_eq!(decoded, request);

        // Field 23 is the first two-byte field key in this message. Keep all
        // keys explicit so a future schema refresh cannot silently shift the
        // Sand prewarm metadata into AgentRunRequest's adjacent field range.
        for key in [
            [0xba, 0x01], // computer_use_coordinate_mode (23, length-delimited)
            [0xc2, 0x01], // agent_session_id (24, length-delimited)
            [0xc8, 0x01], // prompt-context capability (25, varint)
            [0xd0, 0x01], // routed-model capability (26, varint)
            [0xda, 0x01], // client_llm_gateway_credential (27, length-delimited)
            [0xe0, 0x01], // preview-card capability (28, varint)
            [0xe8, 0x01], // started_as_new_project (29, varint)
        ] {
            assert!(
                encoded.windows(2).any(|window| window == key),
                "missing PrewarmRequest extension key {key:?} in {encoded:?}"
            );
        }
    }

    #[test]
    fn user_action_history_and_image_fields_round_trip() {
        let action = UserMessageAction {
            user_message: Some(UserMessage {
                text: "with image".into(),
                message_id: "m1".into(),
                selected_context: None,
                mode: 1,
                is_simulated_msg: Some(false),
                best_of_n_group_id: Some("group".into()),
                try_use_best_of_n_promotion: Some(false),
                rich_text: Some("**with image**".into()),
                simulated_msg_reason: None,
                conversation_state_blob_id: vec![9, 8],
                subagent_system_reminder: Some("reminder".into()),
                triggering_user_info: None,
                execute_plan_info: None,
                simulated_message_metadata: None,
                prompt_reference_id: None,
                thread_id: None,
                text_blob_id: None,
                rich_text_blob_id: None,
                hook_additional_contexts: vec![],
                custom_mode_intent: None,
            }),
            request_context: None,
            send_to_interaction_listener: Some(true),
            prepend_user_messages: vec![UserMessage {
                text: "preface".into(),
                message_id: "m0".into(),
                selected_context: None,
                mode: 1,
                ..Default::default()
            }],
            interrupted_pending_tool_call_resolutions: None,
            conversation_history: Some(ConversationHistory {
                messages: vec![ConversationHistoryMessage {
                    user: Some(ConversationHistoryUserMessage {
                        content: vec![ConversationHistoryUserContent {
                            text: Some(ConversationHistoryTextContent {
                                text: "old turn".into(),
                            }),
                            image: Some(ConversationHistoryImageContent {
                                data: "BASE64".into(),
                                mime_type: Some("image/png".into()),
                            }),
                        }],
                    }),
                    assistant: None,
                    tool: None,
                }],
                replace_user_info: Some(true),
            }),
        };
        let mut bytes = Vec::new();
        action.encode(&mut bytes).unwrap();
        let decoded = UserMessageAction::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded, action);
    }

    #[test]
    fn user_message_official_extension_fields_round_trip() {
        // Cursor's current agent.v1.UserMessage leaves tags 12 and 20
        // reserved and adds synthetic-message/hook metadata at 9, 13–19,
        // 21 and 22. Exercise every extension so a schema refresh cannot
        // accidentally collapse bytes into the reserved slots.
        let message = UserMessage {
            text: "synthetic".into(),
            message_id: "m-ext".into(),
            selected_context: None,
            mode: 1,
            is_simulated_msg: Some(true),
            best_of_n_group_id: None,
            try_use_best_of_n_promotion: None,
            rich_text: Some("rich".into()),
            simulated_msg_reason: Some(17),
            conversation_state_blob_id: vec![1, 2],
            subagent_system_reminder: Some("reminder".into()),
            triggering_user_info: Some(TriggeringUserInfo {
                auth_id: Some("auth".into()),
                user_id: Some(42),
            }),
            execute_plan_info: Some(ExecutePlanInfo {
                plan_id: "plan-1".into(),
                plan_title: "Plan".into(),
            }),
            simulated_message_metadata: Some(SimulatedMessageMetadata {
                title: Some("Task".into()),
                task_id: Some("task-1".into()),
                fsd_finding_action: Some("apply".into()),
                url: Some("https://example.invalid".into()),
                subscription_source: Some(2),
            }),
            prompt_reference_id: Some("prompt-1".into()),
            thread_id: Some("thread-1".into()),
            text_blob_id: Some(vec![3, 4]),
            rich_text_blob_id: Some(vec![5, 6]),
            hook_additional_contexts: vec![HookAdditionalContext {
                hook_event_name: "after_tool".into(),
                content: "context".into(),
            }],
            custom_mode_intent: Some(CustomModeIntent {
                enter: Some(SubmittedCustomMode {
                    id: "mode-1".into(),
                    label: "Mode".into(),
                    source: 1,
                    source_path: None,
                    source_hash: None,
                    managed_skill_id: None,
                    plugin_id: None,
                    plugin_snapshot_token: None,
                }),
                exit: None,
            }),
        };
        let mut bytes = Vec::new();
        message.encode(&mut bytes).expect("encode UserMessage");
        let decoded = UserMessage::decode(bytes.as_slice()).expect("decode UserMessage");
        assert_eq!(decoded, message);
        // Reserved tags 12 (0x62) and 20 (0xa2 0x01) must not be emitted by
        // this message; all extension tags are length-delimited except the
        // enum varint at tag 9.
        assert!(!bytes.contains(&0x62), "reserved tag 12 unexpectedly set");
        assert!(!bytes.windows(2).any(|w| w == [0xa2, 0x01]));
        assert!(bytes.contains(&0x48), "simulated_msg_reason tag 9 missing");
        assert!(bytes.contains(&0x6a), "triggering_user_info tag 13 missing");
        assert!(
            bytes.contains(&0xaa),
            "hook_additional_contexts tag 21 missing"
        );
        assert!(
            bytes.windows(2).any(|w| w == [0xb2, 0x01]),
            "custom_mode_intent tag 22 missing"
        );
    }

    #[test]
    fn populated_request_context_uses_cli_env_git_and_skill_tags() {
        let ctx = RequestContext {
            env: Some(RequestContextEnv {
                os_version: "macos".into(),
                workspace_paths: vec!["/tmp/proj".into()],
                shell: "/bin/zsh".into(),
                sandbox_enabled: false,
                time_zone: "UTC".into(),
                project_folder: "/tmp/proj".into(),
                process_working_directory: "/tmp/proj".into(),
            }),
            git_repos: vec![GitRepoInfo {
                path: "/tmp/proj".into(),
                status: String::new(),
                branch_name: "main".into(),
                remote_url: None,
            }],
            rules: vec![CursorRule {
                full_path: "/tmp/proj/.cursor/rules/x.mdc".into(),
                content: "use tabs".into(),
            }],
            agent_skills: vec![AgentSkill {
                full_path: "/tmp/proj/.cursor/skills/demo/SKILL.md".into(),
                content: "# Demo".into(),
                description: "demo skill".into(),
                globs: vec!["**/*.rs".into()],
            }],
        };
        let mut buf = Vec::new();
        ctx.encode(&mut buf).unwrap();
        assert!(!buf.is_empty());
        // Field 2 rules → 0x12; field 4 env → 0x22; field 11 git_repos → 0x5a;
        // field 29 agent_skills → 0xea.
        assert!(buf.contains(&0x12), "rules tag 2 missing in {buf:?}");
        assert!(buf.contains(&0x22), "env tag 4 missing in {buf:?}");
        assert!(buf.contains(&0x5a), "git_repos tag 11 missing in {buf:?}");
        assert!(
            buf.contains(&0xea),
            "agent_skills tag 29 missing in {buf:?}"
        );
        let decoded = RequestContext::decode(&buf[..]).unwrap();
        let env = decoded.env.as_ref().unwrap();
        assert_eq!(env.workspace_paths, vec!["/tmp/proj"]);
        assert_eq!(env.project_folder, "/tmp/proj");
        assert_eq!(env.process_working_directory, "/tmp/proj");
        assert_eq!(decoded.git_repos[0].branch_name, "main");
        assert_eq!(decoded.rules[0].full_path, "/tmp/proj/.cursor/rules/x.mdc");
        assert_eq!(decoded.agent_skills[0].description, "demo skill");
        assert_eq!(decoded.agent_skills[0].globs, vec!["**/*.rs"]);
    }

    #[test]
    fn empty_conversation_state_still_encodes_tag_1() {
        let req = RunRequest {
            conversation_state: Some(Vec::new()),
            ..Default::default()
        };
        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        // Field 1, wire type 2, length 0 → 0x0a 0x00. `None` omits the field
        // and Cursor answers "Conversation state is required".
        assert_eq!(buf, vec![0x0a, 0x00]);
        let omitted = RunRequest::default();
        let mut empty = Vec::new();
        omitted.encode(&mut empty).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn request_context_env_encodes_process_working_directory_tag_21() {
        let env = RequestContextEnv {
            process_working_directory: "/tmp/cwd".into(),
            ..Default::default()
        };
        let mut buf = Vec::new();
        env.encode(&mut buf).unwrap();
        // Field 21 wire type 2 → tag 0xaa.
        assert!(
            buf.contains(&0xaa),
            "process_working_directory tag 21 missing in {buf:?}"
        );
        let decoded = RequestContextEnv::decode(&buf[..]).unwrap();
        assert_eq!(decoded.process_working_directory, "/tmp/cwd");
    }

    #[test]
    fn interaction_update_partial_tool_call_uses_tag_7() {
        let update = InteractionUpdate {
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
        };
        let mut buf = Vec::new();
        update.encode(&mut buf).unwrap();
        // Field 7, wire type 2 → tag 0x3a.
        assert_eq!(buf[0], 0x3a, "partial_tool_call tag 7 missing in {buf:?}");
        let decoded = InteractionUpdate::decode(&buf[..]).unwrap();
        let partial = decoded.partial_tool_call.expect("partial_tool_call");
        assert_eq!(partial.call_id, "mcp-1");
        assert_eq!(partial.args_text_delta, r#"{"name":"deep-research"}"#);
        assert_eq!(
            partial
                .tool_call
                .unwrap()
                .mcp_tool_call
                .unwrap()
                .args
                .unwrap()
                .tool_name,
            "Workflow"
        );
    }

    #[test]
    fn agent_host_metadata_progress_distinguishes_lifecycle_from_heartbeat() {
        let ttft = AgentServerMessage {
            ttft_breakdown: Some(TtftBreakdown::default()),
            ..Default::default()
        };
        assert!(ttft.has_agent_host_metadata_progress());

        let updates = [
            InteractionUpdate {
                user_message_appended: Some(UserMessageAppendedUpdate::default()),
                ..Default::default()
            },
            InteractionUpdate {
                summary: Some(SummaryUpdate::default()),
                ..Default::default()
            },
            InteractionUpdate {
                summary_started: Some(SummaryStartedUpdate::default()),
                ..Default::default()
            },
            InteractionUpdate {
                summary_completed: Some(SummaryCompletedUpdate::default()),
                ..Default::default()
            },
            InteractionUpdate {
                shell_output_delta: Some(ShellStream::default()),
                ..Default::default()
            },
            InteractionUpdate {
                step_started: Some(StepStartedUpdate::default()),
                ..Default::default()
            },
            InteractionUpdate {
                step_completed: Some(StepCompletedUpdate::default()),
                ..Default::default()
            },
            InteractionUpdate {
                prompt_suggestion: Some(PromptSuggestionUpdate::default()),
                ..Default::default()
            },
            InteractionUpdate {
                post_request_prompt: Some(PostRequestPromptUpdate::default()),
                ..Default::default()
            },
            InteractionUpdate {
                active_branch_change: Some(ActiveBranchChange::default()),
                ..Default::default()
            },
            InteractionUpdate {
                feedback_request: Some(FeedbackRequestUpdate::default()),
                ..Default::default()
            },
            InteractionUpdate {
                response_comparison: Some(ResponseComparisonUpdate::default()),
                ..Default::default()
            },
            InteractionUpdate {
                context_injection_state: Some(ContextInjectionStateUpdate::default()),
                ..Default::default()
            },
            InteractionUpdate {
                routed_model: Some(RoutedModelUpdate::default()),
                ..Default::default()
            },
        ];
        for update in updates {
            assert!(update.has_agent_host_metadata_progress());
            assert!(
                AgentServerMessage {
                    interaction_update: Some(update),
                    ..Default::default()
                }
                .has_agent_host_metadata_progress()
            );
        }

        let nested = InteractionUpdate {
            routed_model: Some(RoutedModelUpdate::default()),
            ..Default::default()
        };
        let parent = InteractionUpdate {
            tool_call_delta: Some(ToolCallDeltaUpdate {
                tool_call_delta: Some(ToolCallDelta {
                    task_tool_call_delta: Some(TaskToolCallDelta {
                        interaction_update: Some(Box::new(nested)),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(parent.has_agent_host_metadata_progress());
        assert!(
            !InteractionUpdate {
                heartbeat: Some(InteractionHeartbeat::default()),
                ..Default::default()
            }
            .has_agent_host_metadata_progress()
        );
    }

    #[test]
    fn interaction_update_tool_call_delta_uses_tag_15() {
        let update = InteractionUpdate {
            tool_call_delta: Some(ToolCallDeltaUpdate {
                call_id: "edit-1".into(),
                model_call_id: "model-1".into(),
                tool_call_delta: Some(ToolCallDelta {
                    edit_tool_call_delta: Some(EditToolCallDelta {
                        stream_content_delta: "fn main() {}".into(),
                    }),
                    ..Default::default()
                }),
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        update.encode(&mut buf).unwrap();
        // Field 15, wire type 2 → tag 0x7a.
        assert_eq!(buf[0], 0x7a, "tool_call_delta tag 15 missing in {buf:?}");
        let decoded = InteractionUpdate::decode(&buf[..]).unwrap();
        let delta = decoded.tool_call_delta.expect("tool_call_delta");
        assert_eq!(delta.call_id, "edit-1");
        assert_eq!(
            delta
                .tool_call_delta
                .unwrap()
                .edit_tool_call_delta
                .unwrap()
                .stream_content_delta,
            "fn main() {}"
        );
    }

    #[test]
    fn tool_call_web_fetch_uses_tag_37() {
        let call = ToolCall {
            web_fetch_tool_call: Some(WebFetchToolCall {
                args: Some(FetchArgs {
                    url: "https://example.com".into(),
                    tool_call_id: "wf-1".into(),
                }),
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        call.encode(&mut buf).unwrap();
        // Field 37, wire type 2 → tag 0xaa 0x02.
        assert!(
            buf.windows(2).any(|w| w == [0xaa, 0x02]),
            "web_fetch_tool_call tag 37 missing in {buf:?}"
        );
        let decoded = ToolCall::decode(&buf[..]).unwrap();
        let args = decoded.web_fetch_tool_call.unwrap().args.unwrap();
        assert_eq!(args.url, "https://example.com");
        assert_eq!(args.tool_call_id, "wf-1");
        assert!(decoded.fetch_tool_call.is_none());
    }

    #[test]
    fn tool_call_task_uses_tag_19() {
        let call = ToolCall {
            task_tool_call: Some(TaskToolCall {
                args: Some(TaskToolCallArgsProto {
                    description: "explore".into(),
                    prompt: "look around".into(),
                    model: None,
                    subagent_type: "explore".into(),
                    resume: None,
                    run_in_background: Some(true),
                }),
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        call.encode(&mut buf).unwrap();
        // Field 19, wire type 2 → tag 0x9a.
        assert!(
            buf.contains(&0x9a),
            "task_tool_call tag 19 missing in {buf:?}"
        );
        let decoded = ToolCall::decode(&buf[..]).unwrap();
        let args = decoded.task_tool_call.unwrap().args.unwrap();
        assert_eq!(args.prompt, "look around");
        assert_eq!(args.subagent_type, "explore");
        assert_eq!(args.run_in_background, Some(true));
        assert!(decoded.web_search_tool_call.is_none());
    }

    #[test]
    fn pi_write_exec_uses_modern_cursor_tags() {
        let exec = ExecServerMessage {
            id: 44,
            exec_id: Some("pi-44".into()),
            pi_write_args: Some(PiWriteExecArgs {
                path: "new.txt".into(),
                content: "hello".into(),
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        exec.encode(&mut buf).unwrap();
        // field 48, length-delimited -> 0x82 0x03.
        assert!(
            buf.windows(2).any(|w| w == [0x82, 0x03]),
            "pi_write_args field 48 missing from {buf:?}"
        );
        let decoded = ExecServerMessage::decode(&buf[..]).unwrap();
        let args = decoded.pi_write_args.unwrap();
        assert_eq!(args.path, "new.txt");
        assert_eq!(args.content, "hello");
    }

    #[test]
    fn pi_write_tool_uses_field_64() {
        let call = ToolCall {
            pi_write_tool_call: Some(PiWriteToolCall {
                args: Some(PiWriteToolArgs {
                    path: "new.txt".into(),
                    content: "hello".into(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        call.encode(&mut buf).unwrap();
        // field 64, length-delimited -> 0x82 0x04.
        assert!(
            buf.windows(2).any(|w| w == [0x82, 0x04]),
            "pi_write_tool_call field 64 missing from {buf:?}"
        );
        let decoded = ToolCall::decode(&buf[..]).unwrap();
        assert_eq!(
            decoded.pi_write_tool_call.unwrap().args.unwrap().path,
            "new.txt"
        );
    }

    #[test]
    fn pi_edit_exec_uses_modern_cursor_field_47() {
        let exec = ExecServerMessage {
            id: 45,
            exec_id: Some("pi-edit-45".into()),
            pi_edit_args: Some(PiEditExecArgs {
                path: "src/lib.rs".into(),
                edits: vec![PiEditReplacement {
                    old_text: "old".into(),
                    new_text: "new".into(),
                }],
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        exec.encode(&mut buf).unwrap();
        // field 47, length-delimited -> key 0xfa 0x02.
        assert!(
            buf.windows(2).any(|w| w == [0xfa, 0x02]),
            "pi_edit_args field 47 missing from {buf:?}"
        );
        let decoded = ExecServerMessage::decode(&buf[..]).unwrap();
        let args = decoded.pi_edit_args.unwrap();
        assert_eq!(args.path, "src/lib.rs");
        assert_eq!(args.edits[0].old_text, "old");
        assert_eq!(args.edits[0].new_text, "new");
    }

    #[test]
    fn pi_edit_tool_uses_field_63() {
        let call = ToolCall {
            pi_edit_tool_call: Some(PiEditToolCall {
                args: Some(PiEditToolArgs {
                    path: "src/lib.rs".into(),
                    edits: vec![PiEditReplacement {
                        old_text: "one".into(),
                        new_text: "two".into(),
                    }],
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        call.encode(&mut buf).unwrap();
        // field 63, length-delimited -> key 0xfa 0x03.
        assert!(
            buf.windows(2).any(|w| w == [0xfa, 0x03]),
            "pi_edit_tool_call field 63 missing from {buf:?}"
        );
        let decoded = ToolCall::decode(&buf[..]).unwrap();
        let args = decoded.pi_edit_tool_call.unwrap().args.unwrap();
        assert_eq!(args.path, "src/lib.rs");
        assert_eq!(args.edits[0].old_text, "one");
    }

    #[test]
    fn pi_edit_result_uses_exec_client_field_48() {
        let result = ExecClientMessage {
            id: 46,
            pi_edit_result: Some(PiEditExecResult {
                success: Some(PiEditExecSuccess {
                    output: "ok".into(),
                    diff: "-old +new".into(),
                    patch: String::new(),
                    first_changed_line: Some(3),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        result.encode(&mut buf).unwrap();
        assert!(
            buf.windows(2).any(|w| w == [0x82, 0x03]),
            "pi_edit_result field 48 missing from {buf:?}"
        );
        let decoded = ExecClientMessage::decode(&buf[..]).unwrap();
        let success = decoded.pi_edit_result.unwrap().success.unwrap();
        assert_eq!(success.output, "ok");
        assert_eq!(success.first_changed_line, Some(3));
    }

    fn proto_varint(mut value: u32) -> Vec<u8> {
        let mut out = Vec::new();
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
        out
    }

    fn proto_len_delim_field(field: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = proto_varint((field << 3) | 2);
        out.extend(proto_varint(payload.len() as u32));
        out.extend_from_slice(payload);
        out
    }

    fn proto_string(field: u32, value: &str) -> Vec<u8> {
        proto_len_delim_field(field, value.as_bytes())
    }

    #[test]
    fn tool_call_task_decodes_handcrafted_tag_19_bytes() {
        // 0xlane TaskToolCallArgsProto: description=1, prompt=2, model=3,
        // subagent_type=4, resume=5, readonly=6, run_in_background=7.
        let mut args = Vec::new();
        args.extend(proto_string(1, "explore live"));
        args.extend(proto_string(2, "look around"));
        args.extend(proto_string(3, "cursor-grok4.6"));
        args.extend(proto_string(4, "explore"));
        args.extend(proto_string(5, "sa-1"));
        args.extend_from_slice(&[0x30, 0x01]); // tag 6 bool — skipped on live wire
        args.extend_from_slice(&[0x38, 0x00]); // run_in_background = false
        let task_tool_call = proto_len_delim_field(1, &args);
        let buf = proto_len_delim_field(19, &task_tool_call);
        assert!(
            buf.windows(2).any(|w| w == [0x9a, 0x01]),
            "field 19 tag must be varint 0x9a 0x01, got {buf:?}"
        );

        let decoded = ToolCall::decode(&buf[..]).unwrap();
        let args = decoded.task_tool_call.expect("tag 19 must decode as Task");
        let args = args.args.expect("TaskToolCall.args");
        assert_eq!(args.description, "explore live");
        assert_eq!(args.prompt, "look around");
        assert_eq!(args.model.as_deref(), Some("cursor-grok4.6"));
        assert_eq!(args.subagent_type, "explore");
        assert_eq!(args.resume.as_deref(), Some("sa-1"));
        assert_eq!(args.run_in_background, Some(false));
        assert!(decoded.web_search_tool_call.is_none());

        // ExecServerMessage.subagent_args is tag 28. A ToolCall that only
        // carries tag 28 must not populate task_tool_call.
        let tag28 = proto_len_delim_field(28, b"not-a-task");
        let decoded28 = ToolCall::decode(&tag28[..]).unwrap();
        assert!(
            decoded28.task_tool_call.is_none(),
            "tag 28 must not be mistaken for native Task tag 19"
        );
    }

    #[test]
    fn tool_call_task_survives_length_delimited_tag_6() {
        // Live Cursor 2026-08-16 sends TaskToolCallArgsProto tag 6 as
        // LengthDelimited (`invalid wire type: LengthDelimited (expected
        // Varint)` on `readonly`). A type mismatch must not drop the Task
        // frame — otherwise grok-build never sees spawn_subagent and loops
        // on the same plan text.
        let mut args = Vec::new();
        args.extend(proto_string(1, "explore live"));
        args.extend(proto_string(2, "look around"));
        args.extend(proto_string(4, "explore"));
        args.extend(proto_string(6, "not-a-bool"));
        args.extend_from_slice(&[0x38, 0x01]); // run_in_background = true
        let task_tool_call = proto_len_delim_field(1, &args);
        let buf = proto_len_delim_field(19, &task_tool_call);
        let decoded =
            ToolCall::decode(&buf[..]).expect("length-delimited tag 6 must not fail Task decode");
        let args = decoded
            .task_tool_call
            .expect("tag 19 must decode as Task")
            .args
            .expect("TaskToolCall.args");
        assert_eq!(args.description, "explore live");
        assert_eq!(args.prompt, "look around");
        assert_eq!(args.subagent_type, "explore");
        assert_eq!(args.run_in_background, Some(true));
    }

    #[test]
    fn tool_call_task_accepts_length_delimited_background_flag() {
        // A Cursor build observed in the wild encoded the optional bool as a
        // length-delimited value. The flexible decoder must consume it without
        // dropping the enclosing InteractionUpdate.
        let mut args = Vec::new();
        args.extend(proto_string(1, "explore live"));
        args.extend(proto_string(2, "look around"));
        args.extend(proto_string(4, "explore"));
        args.extend(proto_string(7, "true"));
        let task_tool_call = proto_len_delim_field(1, &args);
        let buf = proto_len_delim_field(19, &task_tool_call);

        let decoded = ToolCall::decode(&buf[..])
            .expect("length-delimited run_in_background must not fail TaskToolCall decoding");
        let args = decoded
            .task_tool_call
            .expect("tag 19 must decode as Task")
            .args
            .expect("TaskToolCall.args");
        assert_eq!(args.run_in_background, Some(true));
    }

    #[test]
    fn tool_call_delta_task_nests_interaction_update_tag_2() {
        let nested = InteractionUpdate {
            partial_tool_call: Some(PartialToolCall {
                call_id: "mcp-nested".into(),
                model_call_id: "model-1".into(),
                args_text_delta: r#"{"name":"deep-research"}"#.into(),
                tool_call: None,
            }),
            text_delta: Some(TextDelta {
                text: "subagent".into(),
            }),
            ..Default::default()
        };
        let delta = ToolCallDelta {
            task_tool_call_delta: Some(TaskToolCallDelta {
                interaction_update: Some(Box::new(nested)),
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        delta.encode(&mut buf).unwrap();
        // Field 2, wire type 2 → tag 0x12.
        assert_eq!(
            buf[0], 0x12,
            "task_tool_call_delta tag 2 missing in {buf:?}"
        );
        let decoded = ToolCallDelta::decode(&buf[..]).unwrap();
        assert!(decoded.shell_tool_call_delta.is_none());
        assert!(decoded.edit_tool_call_delta.is_none());
        let nested = decoded
            .task_tool_call_delta
            .expect("task_tool_call_delta")
            .interaction_update
            .expect("nested InteractionUpdate");
        assert_eq!(
            nested.partial_tool_call.unwrap().args_text_delta,
            r#"{"name":"deep-research"}"#
        );
        assert_eq!(nested.text_delta.unwrap().text, "subagent");
    }

    #[test]
    fn interaction_update_tool_call_delta_task_round_trips_nested_partial() {
        let update = InteractionUpdate {
            tool_call_delta: Some(ToolCallDeltaUpdate {
                call_id: "task-1".into(),
                model_call_id: "model-1".into(),
                tool_call_delta: Some(ToolCallDelta {
                    task_tool_call_delta: Some(TaskToolCallDelta {
                        interaction_update: Some(Box::new(InteractionUpdate {
                            partial_tool_call: Some(PartialToolCall {
                                call_id: "mcp-nested".into(),
                                model_call_id: "model-1".into(),
                                args_text_delta: r#"{"name":"deep-research"}"#.into(),
                                tool_call: None,
                            }),
                            ..Default::default()
                        })),
                    }),
                    ..Default::default()
                }),
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        update.encode(&mut buf).unwrap();
        assert_eq!(buf[0], 0x7a, "tool_call_delta tag 15 missing in {buf:?}");
        let decoded = InteractionUpdate::decode(&buf[..]).unwrap();
        let nested = decoded
            .tool_call_delta
            .as_ref()
            .and_then(ToolCallDeltaUpdate::nested_task_update)
            .expect("nested task InteractionUpdate");
        assert_eq!(
            nested.partial_tool_call.as_ref().unwrap().call_id,
            "mcp-nested"
        );
        // A second nested Task delta still decodes (boxed) so the type is
        // finite; live processing caps at MAX_TASK_DELTA_NEST.
        let twice = InteractionUpdate {
            tool_call_delta: Some(ToolCallDeltaUpdate {
                call_id: "task-outer".into(),
                model_call_id: "model-1".into(),
                tool_call_delta: Some(ToolCallDelta {
                    task_tool_call_delta: Some(TaskToolCallDelta {
                        interaction_update: Some(Box::new(decoded)),
                    }),
                    ..Default::default()
                }),
            }),
            ..Default::default()
        };
        let mut buf2 = Vec::new();
        twice.encode(&mut buf2).unwrap();
        let round = InteractionUpdate::decode(&buf2[..]).unwrap();
        let inner = round
            .tool_call_delta
            .as_ref()
            .and_then(ToolCallDeltaUpdate::nested_task_update)
            .and_then(|u| u.tool_call_delta.as_ref())
            .and_then(ToolCallDeltaUpdate::nested_task_update)
            .expect("second nested level present on wire");
        assert_eq!(
            inner.partial_tool_call.as_ref().unwrap().args_text_delta,
            r#"{"name":"deep-research"}"#
        );
        assert_eq!(MAX_TASK_DELTA_NEST, 1);
    }
}
