use crate::anthropic::schema::MessagesRequest;
use crate::monitor::MonitorHandle;
use crate::traffic::TrafficCapture;
use anyhow::Result;
use async_trait::async_trait;
use axum::response::Response;
use clap::Subcommand;
use std::sync::Arc;

#[derive(Debug, Clone, Subcommand)]
pub enum AuthCommand {
    /// Sign in using browser-based authentication
    Login,
    /// Sign in using a device code
    Device,
    /// Show the current authentication status
    Status,
    /// Delete stored authentication credentials
    Logout,
}

#[async_trait]
pub trait Provider: Send + Sync {
    fn name(&self) -> &'static str;
    fn supported_models(&self) -> Vec<String>;
    fn cli(&self) -> &'static dyn CliHandlers;
    async fn handle_messages(&self, body: MessagesRequest, ctx: RequestContext) -> Response;
    async fn handle_count_tokens(&self, body: MessagesRequest, ctx: RequestContext) -> Response;
}

pub trait CliHandlers: Send + Sync {
    fn login(&self) -> Result<()>;
    fn device(&self) -> Result<()>;
    fn status(&self) -> Result<()>;
    fn logout(&self) -> Result<()>;
}

#[derive(Debug, Clone, Default)]
pub struct ClaudeCodeAgentHeaders {
    /// `x-claude-code-agent-id` (URL-encoded). Nested agents only.
    pub agent_id: Option<String>,
    /// `x-claude-code-parent-agent-id` (URL-encoded). Nested agents only.
    pub parent_agent_id: Option<String>,
    /// `x-app` (`cli` / `cli-bg`). Logging only, not used for routing.
    pub app: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RequestContext {
    pub req_id: String,
    /// Stable client operation id, currently `x-grok-req-id`.
    /// Unlike `req_id`, this survives HTTP retries of the same logical turn.
    pub client_request_id: Option<String>,
    /// Anthropic SDK helper marker (`x-stainless-helper`).  Claude Code uses
    /// the `compaction` value for its ToolRunner/server-side compaction path;
    /// preserve it alongside the parsed request so providers can keep that
    /// operation on an isolated summary-only lane.
    pub stainless_helper: Option<String>,
    pub session_id: Option<String>,
    pub session_seq: Option<u64>,
    pub provider: String,
    pub traffic: Option<Arc<TrafficCapture>>,
    pub monitor: Option<MonitorHandle>,
    /// Nested-agent headers from Claude Code. Same session id as the parent.
    pub claude_code: ClaudeCodeAgentHeaders,
    /// `/v1/responses` must not commit Anthropic SSE before live open, or
    /// grok-build maps a later `response.failed` to HTTP 500 and retries.
    pub hold_http_until_live_open: bool,
}
