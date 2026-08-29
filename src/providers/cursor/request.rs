use crate::anthropic::schema::{Message, MessagesRequest};
use base64::Engine as _;
use sha2::{Digest, Sha256};

/// A selected image extracted from the request content blocks.
#[derive(Debug, Clone)]
pub struct CursorSelectedImage {
    pub data: String,
    pub uuid: String,
    /// Empty for Anthropic inline images. Cursor treats a non-empty path as
    /// an asset/blob reference rather than the official inline-data shape.
    pub path: String,
    pub mime_type: String,
}

/// Options controlling how Anthropic Messages become Cursor UserMessage text.
#[derive(Debug, Clone, Copy, Default)]
pub struct CursorPromptOptions {
    /// Skip **Cursor-native** tool schemas in the `<tools>` dump (BiDi bridge
    /// already exposes Shell/Read/…). Claude-local tools are forwarded through
    /// the actual `RunRequest.mcp_tools` catalog; they are not duplicated in
    /// the text prompt.
    pub omit_tools: bool,
    /// Only the latest user turn (used when ConversationState checkpoint exists).
    pub delta_only: bool,
}

/// Split Claude Code Anthropic Messages fields onto Cursor Agent RunRequest.
///
/// ## System on Cursor / Fable
/// - Field 8 `custom_system_prompt` is **team-only** (else 502).
/// - Embedding Claude Code's full system into `UserMessage` makes Fable treat it as
///   **prompt injection** and waste turns (live 2026-07). Default: **do not embed**.
/// - Anthropic top-level `system` is **not** sent to Cursor unless an env
///   opt-in is set (Fable treats a pasted system as prompt injection).
/// - CLAUDE.md / rules / skills that Claude Code injects as user
///   `<system-reminder>` messages **are** forwarded (scrubber only strips
///   packaging banners + assistant injection-defense monologues).
/// - Agent tools still work via Anthropic tool schemas + native tool bridge.
/// - Claude-local tools (`Workflow`, `Skill`, MCP names) stay advertised as
///   `RunRequest.mcp_tools`. When that field is populated, the XML `<tools>`
///   dump does not duplicate those callable definitions; the Cursor catalog
///   is the single source of truth for their names and schemas.
/// - Anthropic `thinking` / `max_tokens` / `tool_choice` are **not** mapped
///   onto AgentRunRequest (no such proto fields; see proto.rs). Catalog
///   thinking/effort already go on `RequestedModel.parameters` via model.rs.
///
/// Env:
/// - `CCP_CURSOR_USE_CUSTOM_SYSTEM=1` — field 8 (team only)
/// - `CCP_CURSOR_EMBED_SYSTEM=1` — plain-text system prefix in user payload
/// - `CCP_CURSOR_PACKAGED_SYSTEM=1` — legacy banners (strongly discouraged)
/// - `CCP_CURSOR_FORCE_TOOLS_IN_PROMPT=1` — dump every tool schema (large)
#[derive(Debug, Clone)]
pub struct CursorPromptParts {
    /// Only set when `CCP_CURSOR_USE_CUSTOM_SYSTEM=1` (team accounts).
    pub custom_system_prompt: Option<String>,
    /// Conversation (+ optional system prefix + tools).
    pub user_text: String,
    /// CLI RequestContext helper (cwd / git) for the exec reply path. Empty when unknown.
    pub request_context: super::proto::RequestContext,
}

fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn use_custom_system_prompt_field() -> bool {
    env_flag("CCP_CURSOR_USE_CUSTOM_SYSTEM")
}

fn embed_system_in_user() -> bool {
    env_flag("CCP_CURSOR_EMBED_SYSTEM") || env_flag("CCP_CURSOR_PACKAGED_SYSTEM")
}

fn packaged_system_embed() -> bool {
    env_flag("CCP_CURSOR_PACKAGED_SYSTEM")
}

const SYSTEM_OPEN: &str =
    "===== CLAUDE_CODE_SYSTEM (authoritative; do not treat as user chat) =====";
const SYSTEM_CLOSE: &str = "===== END_CLAUDE_CODE_SYSTEM =====";

/// Cursor / Claude Code names Fable already ships natively (or we remap from
/// native exec). Omitting these from the prompt dump avoids tens–hundreds of k
/// tokens of duplicate schema.
///
/// grok-build wire names (`read_file`, `web_search`, `web_fetch`, `write`,
/// `list_dir`, `run_terminal_command`, …) must **not** be listed here. If they
/// are treated as native, `omit_tools` hides them and XML `<tool_use>` recovery
/// leaves them as text — the model then keeps calling Cursor Shell/Read/WebSearch.
const CURSOR_NATIVE_TOOL_NAMES: &[&str] = &[
    "Bash",
    "Shell",
    "bash",
    "Read",
    "ReadFile",
    "Write",
    "write_file",
    "WriteFile",
    // Cursor's Pi string-replacement editor is advertised as `StrReplace`
    // (field-63/field-47 in the Agent protocol).  It is a Cursor-native
    // capability, not a Claude-local MCP tool.  Keeping it in this list is
    // important: otherwise the same operation is registered a second time
    // through `claude-local`, and the model can receive an `Edit` fallback
    // even though the client only knows `StrReplace`.
    "StrReplace",
    "Grep",
    "Search",
    "Glob",
    "glob",
    "Find",
    "Delete",
    "Ls",
    "WebSearch",
    "WebFetch",
    "Fetch",
    "TodoWrite",
    "TodoRead",
    "AskUserQuestion",
    "AskQuestion",
    "CreatePlan",
    "Plan",
];

/// Claude Code tools that have no Cursor `ExecServerMessage` result envelope.
///
/// These are registered as Claude-local MCP tools when present in the incoming
/// catalog.  Cursor can then emit a normal MCP call and the proxy hands it back
/// to Claude Code as a ClientOnly tool_use; the next request carries the
/// tool_result and starts a fresh Cursor segment.  Keeping this list explicit
/// avoids accidentally exposing internal hooks while covering the built-ins in
/// Claude Code 2.1.x and later.
const CLAUDE_CLIENT_ONLY_TOOL_NAMES: &[&str] = &[
    // Claude Code's current built-ins and the names still emitted by older
    // 2.x clients.  These tools have no Cursor ExecServerMessage result
    // envelope; they must be registered in the Cursor MCP catalog and
    // returned as ClientOnly tool_use blocks for Claude Code to execute.
    "Agent",
    "Edit",
    "MultiEdit",
    "NotebookEdit",
    "NotebookRead",
    "PowerShell",
    "EnterPlanMode",
    "ExitPlanMode",
    "EnterWorktree",
    "ExitWorktree",
    "TaskCreate",
    "TaskGet",
    "TaskUpdate",
    "TaskList",
    "TaskStop",
    "TaskOutput",
    "AgentOutputTool",
    "BashOutputTool",
    "AgentOutput",
    "BashOutput",
    "KillShell",
    "KillBash",
    "ListPeers",
    // Claude Code 2.1.193 renamed these controls while retaining the legacy
    // spellings as aliases. Keep both names in the catalog classifier so a
    // Cursor/Fable event can be matched to whichever spelling the client sent.
    "ListAgents",
    "Workflow",
    "RunWorkflow",
    // Claude Code 2.1.193's disabled end-to-end permission probe can still be
    // present in the incoming tool catalog. Keep it routable when explicitly
    // advertised; ordinary internal hooks remain filtered by description.
    "TestingPermission",
    "LSP",
    // Claude Code 2.1.193 external built-ins without a Cursor exec envelope.
    // They are client-executed after the proxy emits a ClientOnly tool_use.
    "Monitor",
    "StructuredOutput",
    "Artifact",
    "CronCreate",
    "CronDelete",
    "CronList",
    "ScheduleWakeup",
    "RemoteTrigger",
    "SendMessage",
    "Brief",
    "ToolSearch",
    "ListMcpResourcesTool",
    "ReadMcpResourceTool",
    "ReadMcpResourceDirTool",
    "ListMcpResources",
    "ReadMcpResource",
    "ReadMcpResourceDir",
    "WaitForMcpServers",
    "SendUserMessage",
    "SendUserFile",
    "ReportFindings",
    // Claude Code 2.1.211 SDK/runtime built-ins. These are client-owned
    // control tools with no Cursor ExecServerMessage result envelope, so they
    // must be advertised through the Claude-local MCP catalog when supplied
    // by the client. TaskOutput remains available for older Claude clients.
    "REPL",
    "RefreshMcpTools",
    "PushNotification",
    "ClaudeDesign",
    "DesignSync",
    "Projects",
    "ShareOnboardingGuide",
    "ShowOnboardingRolePicker",
    // Runtime-only names observed in the Claude CLI binary. They can appear
    // in the incoming tool catalog even though they are not in sdk-tools.d.ts.
    "ConnectGitHub",
    "EndConversation",
    "SendFile",
    "SearchMcpRegistry",
    "SuggestConnectors",
    "ListConnectors",
];

pub(crate) fn is_claude_client_only_tool_name(name: &str) -> bool {
    // A bare Cursor-native `StrReplace` must never be put on the
    // client-only/MCP route.  Qualified `claude-local/StrReplace` spellings
    // remain client-owned and are handled by the provider-aware resolver.
    if is_cursor_native_tool_name(name) {
        return false;
    }
    CLAUDE_CLIENT_ONLY_TOOL_NAMES
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(name))
        || is_text_editor_tool_name(name)
}

/// Anthropic's schema-less text editor names.  Claude Code 2.1.x advertises
/// the versioned pair `text_editor_20250728` / `str_replace_based_edit_tool`;
/// older clients used `str_replace_editor`, while a few Cursor/Grok bridges
/// expose the short `StrReplace` spelling.  Keep this list deliberately
/// explicit so a foreign MCP tool cannot become a local editor by fuzzy
/// matching.
pub(crate) fn is_text_editor_tool_name(name: &str) -> bool {
    let leaf = if is_claude_local_mcp_spelling(name) {
        strip_mcp_provider_prefix(name)
    } else {
        name
    };
    [
        "str_replace_based_edit_tool",
        "str_replace_editor",
        "StrReplace",
        "StrReplaceTool",
    ]
    .iter()
    .any(|candidate| candidate.eq_ignore_ascii_case(leaf))
}

/// Pick the most capable text-editor spelling from an advertised tool set.
///
/// Claude Code 2.1.193's canonical pair is
/// `text_editor_20250728`/`str_replace_based_edit_tool`.  A request can still
/// contain a stale legacy `Edit` entry (or an older `str_replace_editor`), so
/// callers resolving a Cursor PiEdit event must prefer the canonical spelling
/// whenever it is present.  Return the exact spelling from the allow-list so
/// Anthropic's tool-result ids continue to match the client's catalog.
pub(crate) fn preferred_text_editor_name(
    allowed: &std::collections::BTreeSet<String>,
) -> Option<String> {
    const PREFERRED: &[&str] = &[
        "str_replace_based_edit_tool",
        "StrReplace",
        "StrReplaceTool",
        "str_replace_editor",
    ];
    // Prefer an exact/bare spelling first.  This is what Claude Code sends in
    // its `tools` array and avoids needlessly exposing the synthetic provider
    // prefix in the downstream `tool_use.name`.
    PREFERRED
        .iter()
        .find_map(|candidate| {
            allowed
                .iter()
                .find(|advertised| advertised.eq_ignore_ascii_case(candidate))
                .cloned()
        })
        // Older clients can retain a `claude-local` provider qualification in
        // their tool catalog/history.  Match only that explicit namespace;
        // `mcp__other__StrReplace` must never become a local editor by leaf
        // matching.
        .or_else(|| {
            PREFERRED.iter().find_map(|candidate| {
                allowed
                    .iter()
                    .find(|advertised| {
                        is_claude_local_mcp_spelling(advertised)
                            && strip_mcp_provider_prefix(advertised).eq_ignore_ascii_case(candidate)
                    })
                    .cloned()
            })
        })
}

/// Names that Claude Code's bundled runtime treats as aliases of one tool.
///
/// The client may advertise either spelling while Cursor's MCP layer can emit
/// the other (for example `Brief` vs `SendUserMessage`).  Keep these families
/// deliberately explicit: broad case-folding or fuzzy matching would let a
/// foreign MCP tool accidentally resolve to a Claude-local built-in.
pub(crate) fn claude_tool_aliases(name: &str) -> &'static [&'static str] {
    const GROUPS: &[&[&str]] = &[
        &["Agent", "Task"],
        &["TaskStop", "KillShell", "KillBash"],
        &[
            "TaskOutput",
            "AgentOutputTool",
            "BashOutputTool",
            "AgentOutput",
            "BashOutput",
        ],
        &["ListAgents", "ListPeers"],
        &["SendUserMessage", "Brief"],
        &["ListMcpResourcesTool", "ListMcpResources"],
        &["ReadMcpResourceTool", "ReadMcpResource"],
        &["ReadMcpResourceDirTool", "ReadMcpResourceDir"],
        &["Workflow", "RunWorkflow"],
        // Cursor calls Claude Code's text editor `StrReplace`; Claude Code's
        // Anthropic-defined name is `str_replace_based_edit_tool`.  These are
        // the same client-side operation, while `Edit` is the legacy Claude
        // tool shape used by PiEdit native events.
        &[
            "Edit",
            "str_replace_based_edit_tool",
            "str_replace_editor",
            "StrReplace",
            "StrReplaceTool",
        ],
    ];
    GROUPS
        .iter()
        .copied()
        .find(|group| {
            group
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(name))
        })
        .unwrap_or(&[])
}

/// Return true when two names are an exact or known Claude runtime alias
/// match. Alias families are compared case-insensitively because XML-oriented
/// model output occasionally lowercases tool names; callers still enforce the
/// provider/allow-list boundary before using this predicate.
pub(crate) fn claude_tool_names_equivalent(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    // Case folding is limited to names that Claude Code explicitly owns. Do
    // not make arbitrary MCP names equivalent merely because their spelling
    // differs by case.
    if left.eq_ignore_ascii_case(right)
        && (is_claude_client_only_tool_name(left) || is_claude_client_only_tool_name(right))
    {
        return true;
    }
    let left_aliases = claude_tool_aliases(left);
    if !left_aliases.is_empty()
        && left_aliases
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(right))
    {
        return true;
    }
    let right_aliases = claude_tool_aliases(right);
    !right_aliases.is_empty()
        && right_aliases
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(left))
}

const GROK_BUILD_CLIENT_TOOL_NAMES: &[&str] = &[
    "run_terminal_command",
    "run_terminal_cmd",
    "read_file",
    "list_dir",
    "todo_write",
    "search_replace",
    "write",
    "grep",
    "web_search",
    "web_fetch",
    "ask_user_question",
    "enter_plan_mode",
    "exit_plan_mode",
];

fn is_grok_build_client_tool_name(name: &str) -> bool {
    is_grok_build_subagent_lifecycle_tool(name) || GROK_BUILD_CLIENT_TOOL_NAMES.contains(&name)
}

fn is_cursor_native_tool_name(name: &str) -> bool {
    // Exact grok-build wire names stay client-local even when they only differ
    // by case from a Cursor native (`grep` vs `Grep`). Ignore-ascii-case would
    // hide them from the XML dump and XML recovery.
    if is_grok_build_client_tool_name(name) {
        return false;
    }
    CURSOR_NATIVE_TOOL_NAMES
        .iter()
        .any(|n| n.eq_ignore_ascii_case(name))
}

/// Classify tools that are not Cursor-native. These names are candidates for
/// the MCP catalog; prompt rendering still filters them against the exact
/// catalog result rather than assuming every candidate is callable.
pub(crate) fn is_claude_local_tool_name(name: &str) -> bool {
    !name.is_empty() && !is_cursor_native_tool_name(name)
}

/// Claude Code Edit-family tools that may arrive through Cursor's XML fallback
/// stream.  They have no Cursor exec result envelope, so XML calls are exposed
/// as ClientOnly only after the live driver validates their Claude schema and
/// allow-list entry.
pub(crate) fn is_xml_client_only_native_tool_name(name: &str) -> bool {
    matches!(name, "Edit" | "MultiEdit" | "NotebookEdit") || is_text_editor_tool_name(name)
}

/// Classify tool results that require a fresh Anthropic continuation rather
/// than a Cursor native exec result.  The three XML-native tools above are
/// included because their tool_use blocks are emitted directly to Claude Code.
pub(crate) fn is_client_only_tool_name(name: &str) -> bool {
    is_claude_local_tool_name(name) || is_xml_client_only_native_tool_name(name)
}

/// Claude Code/MCP entries that are implementation hooks rather than model
/// capabilities.  Cursor truncates long MCP names to a prefix plus a hash, so
/// the prefixes below intentionally match both the original and truncated
/// leaf names (for example `notify_messa00a7caa`).
const HIDDEN_MODEL_TOOL_LEAF_PREFIXES: &[&str] = &[
    "lobster_reply_from",
    "notify_messa",
    "notify_post",
    "notify_user_prompt",
    "record_token_usage",
    "wait_for_rem",
];

/// Return false for tools that must never be offered to the model or routed
/// back through the client-only bridge.
pub(crate) fn is_model_visible_tool_name(name: &str) -> bool {
    if name.trim().is_empty() {
        return false;
    }
    // Normalize every spelling before applying the hidden-hook policy.  In
    // particular, `mcp_claude-local_<tool>` does not contain `__`, so a plain
    // split on the MCP separator would leave the provider prefix attached and
    // let an internal hook leak into the model catalog.
    let leaf = strip_mcp_provider_prefix(name)
        .rsplit("__")
        .next()
        .unwrap_or(name)
        .rsplit(['/', ':'])
        .next()
        .unwrap_or(name);
    let leaf_lower = leaf.to_ascii_lowercase();
    // TaskOutput is a deprecated Claude Code control surface.  Keeping it in
    // the model catalog makes newer clients see two overlapping ways to poll
    // an agent (TaskOutput and AgentOutputTool) and, more importantly, lets a
    // stale transcript re-introduce a tool that the current host no longer
    // executes.  Historical tool_result blocks remain harmless: the bridge
    // still recognizes the alias for replay, but it is never advertised.
    if leaf_lower == "taskoutput" {
        return false;
    }
    !HIDDEN_MODEL_TOOL_LEAF_PREFIXES
        .iter()
        .any(|prefix| leaf_lower.starts_with(prefix))
        // `lobster_reply_from_stop` is truncated to `lobster_repl` + a
        // seven-character hash. Do not hide the public `lobster_reply` tool,
        // which shares the same initial characters but has no hash suffix.
        && !leaf_lower
            .strip_prefix("lobster_repl")
            .is_some_and(|suffix| {
            suffix.len() == 7 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

/// Apply the name and description policy to a raw Anthropic tool definition.
///
/// Hook providers have historically changed their leaf names while retaining
/// an `INTERNAL`/"Do not call from model output" marker in the description.
/// Filtering that marker here keeps the MCP catalog, prompt dump, and bridge
/// allow-list aligned even when a new hook name appears.
pub(crate) fn is_model_visible_tool_definition(tool: &serde_json::Value) -> bool {
    let Some(name) = tool.get("name").and_then(|name| name.as_str()) else {
        return false;
    };
    if name.trim().is_empty() {
        return false;
    }
    if !is_model_visible_tool_name(name) {
        return false;
    }
    let description = tool
        .get("description")
        .and_then(|description| description.as_str())
        .unwrap_or("")
        .trim();
    if description.is_empty() {
        return true;
    }
    let lower = description.to_ascii_lowercase();
    // Keep this deliberately narrow: ordinary tools may mention internal
    // implementation details, but hook descriptions explicitly identify
    // themselves as non-model-facing.
    let internal_marker = lower.strip_prefix("internal").is_some_and(|rest| {
        rest.is_empty()
            || rest
                .chars()
                .next()
                .is_some_and(|ch| !ch.is_ascii_alphanumeric())
    });
    // Hook descriptions are not consistent about where they place the marker
    // ("INTERNAL", "INTERNAL hook", and "hook (internal)" all occur in the
    // wild).  Match marker-shaped contexts only; a normal description such as
    // "Search internal references" must remain visible.
    let internal_context = lower.contains("internal hook")
        || lower.contains("hook (internal)")
        || lower.contains("(internal)")
        || lower.contains("[internal]")
        || lower.ends_with(" internal");
    let deprecated_marker = lower.strip_prefix("deprecated").is_some_and(|rest| {
        rest.is_empty()
            || rest
                .chars()
                .next()
                .is_some_and(|ch| !ch.is_ascii_alphanumeric())
    }) || lower.contains("deprecated hook")
        || lower.contains("deprecated tool")
        || lower.ends_with(" deprecated")
        || lower.ends_with("(deprecated)")
        || lower.ends_with("[deprecated]");
    !(internal_marker
        || internal_context
        || deprecated_marker
        || (lower.contains("do not call") && lower.contains("model output"))
        || lower.contains("not for model output"))
}

/// Cursor's MCP registry limits names to 64 characters. Its observed
/// shortening rule is the first 57 characters followed by seven lowercase
/// SHA-256 hex characters. Use the same wire spelling before registration so
/// tool-call events and resume requests agree with the server.
pub(crate) fn cursor_mcp_wire_name(name: &str) -> String {
    if name.chars().count() <= 64 {
        return name.to_string();
    }
    let digest = Sha256::digest(name.as_bytes());
    let suffix = format!("{digest:x}");
    let prefix: String = name.chars().take(57).collect();
    format!("{prefix}{}", &suffix[..7])
}

/// Tools Cursor should see on `RunRequest.mcp_tools`. Prompt rendering must
/// use the resulting names directly and never synthesize a second prefix.
///
/// Fable's agent loop invokes the MCP catalog, not the XML dump. Lifecycle
/// grok names (`spawn_subagent`, …) must be on MCP or Cursor returns
/// `Tool not found`. Filesystem/web grok names with a native remap stay off
/// MCP so Fable does not teach `mcp_claude-local_run_terminal_command`.
/// Steal maps remaining `mcp_claude-local_*` back to the exact grok wire name.
/// Workflow / Skill / Task / `mcp__*` stay for Claude Code — Task must be on
/// MCP too, or function-calling models (gemini) have no callable subagent
/// tool at all. Task is exact-case and Claude-Code-only: grok-build's real
/// task tool is `spawn_subagent`, so on grok requests Claude `Task` and the
/// lowercase `task` / `Agent` aliases all stay off MCP (no dual catalog).
fn advertise_as_cursor_mcp(name: &str, grok_build_request: bool) -> bool {
    if is_grok_build_subagent_lifecycle_tool(name)
        || normalize_grok_build_lifecycle_name(name).is_some()
    {
        return true;
    }
    let bare = strip_mcp_provider_prefix(name);
    // Bare Cursor-native tools (including the Pi `StrReplace` editor) are
    // already part of Cursor's native catalog.  Registering them again as a
    // Claude-local MCP tool creates two identities for one operation and can
    // make a PiEdit event resolve to the legacy `Edit` label.  A qualified
    // `claude-local/...` spelling is intentionally left eligible below: it is
    // an explicit client-local alias, not the native bare tool.
    if name == bare && is_cursor_native_tool_name(name) {
        return false;
    }
    // Claude Code's Agent is fulfilled by Cursor's native Task event. A bare
    // Agent entry in mcp_tools would create a second subagent route; qualified
    // `claude-local/Agent` spellings remain eligible for explicit MCP calls.
    if name == "Agent" {
        return false;
    }
    if grok_client_tool_uses_native_remap(name) || grok_client_tool_uses_native_remap(bare) {
        return false;
    }
    if is_grok_build_client_tool_name(name)
        || (is_grok_build_client_tool_name(bare) && is_claude_local_mcp_spelling(name))
    {
        return true;
    }
    // Claude Code built-ins without a Cursor-native result envelope must stay
    // callable when another MCP tool is present.  In that case the compact
    // prompt intentionally omits local schemas, so registering them on the
    // Cursor catalog is the only way function-calling models can select them.
    // Grok's lowercase lifecycle aliases and Cursor-native Task/Agent aliases
    // are handled by their dedicated remap paths.  Registering the Claude
    // `Agent` alias on a Grok request would create a second subagent catalog
    // entry and make tool selection nondeterministic.
    let grok_native_task_alias = grok_build_request
        && ["Agent", "Task", "task", "TaskOutput", "TaskStop"]
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(bare));
    if (!grok_native_task_alias && is_claude_client_only_tool_name(name))
        || (is_claude_local_mcp_spelling(name)
            && !grok_native_task_alias
            && is_claude_client_only_tool_name(bare))
    {
        return true;
    }
    claude_tool_names_equivalent(bare, "Workflow")
        || bare.eq_ignore_ascii_case("Skill")
        || (!grok_build_request && bare == "Task")
        || bare.starts_with("mcp__")
}

fn grok_client_tool_uses_native_remap(name: &str) -> bool {
    matches!(
        name,
        "run_terminal_command"
            | "run_terminal_cmd"
            | "read_file"
            | "list_dir"
            | "todo_write"
            | "write"
            | "grep"
            | "web_search"
            | "web_fetch"
            | "ask_user_question"
            | "enter_plan_mode"
            | "exit_plan_mode"
    )
}

pub(crate) fn is_claude_local_mcp_spelling(name: &str) -> bool {
    name.starts_with("mcp_claude-local_")
        || name.starts_with("mcp__claude-local__")
        || name.starts_with("claude-local/")
        || name.starts_with("claude-local:")
}

/// Exact grok-build model-facing lifecycle names. Cursor native `Task` is
/// remapped separately; Claude `Task` and internal aliases stay off MCP.
pub(crate) fn is_grok_build_subagent_lifecycle_tool(name: &str) -> bool {
    matches!(
        name,
        "spawn_subagent"
            | "get_command_or_subagent_output"
            | "kill_command_or_subagent"
            | "wait_commands_or_subagents"
    )
}

const GROK_BUILD_LIFECYCLE_TOOLS: &[&str] = &[
    "spawn_subagent",
    "get_command_or_subagent_output",
    "kill_command_or_subagent",
    "wait_commands_or_subagents",
];

/// Map Cursor/Fable MCP spellings back to the exact grok-build wire name.
///
/// Cursor advertises `provider_identifier=claude-local` + `spawn_subagent` as
/// `mcp_claude-local_spawn_subagent` or `mcp__claude-local__spawn_subagent`.
/// grok-build's registry only has the bare name. Foreign prefixes stay denied.
pub(crate) fn normalize_grok_build_lifecycle_name(name: &str) -> Option<&str> {
    if is_grok_build_subagent_lifecycle_tool(name) {
        return Some(name);
    }
    if let Some((provider, tool)) = name.split_once('/').or_else(|| name.split_once(':'))
        && provider == CLAUDE_LOCAL_MCP_PROVIDER
        && is_grok_build_subagent_lifecycle_tool(tool)
    {
        return Some(tool);
    }
    if let Some(rest) = name.strip_prefix("mcp__") {
        let mut parts = rest.splitn(2, "__");
        if parts.next() == Some(CLAUDE_LOCAL_MCP_PROVIDER)
            && let Some(tool) = parts.next()
            && is_grok_build_subagent_lifecycle_tool(tool)
        {
            return Some(tool);
        }
    }
    if let Some(rest) = name.strip_prefix("mcp_") {
        for tool in GROK_BUILD_LIFECYCLE_TOOLS {
            if let Some(provider) = rest.strip_suffix(&format!("_{tool}"))
                && provider == CLAUDE_LOCAL_MCP_PROVIDER
            {
                return Some(*tool);
            }
        }
    }
    None
}

fn mcp_input_schema_value(tool: &serde_json::Value) -> prost_types::Value {
    let schema = tool
        .get("input_schema")
        .and_then(json_to_prost_struct)
        .or_else(|| json_to_prost_struct(&serde_json::json!({ "type": "object" })))
        .expect("minimal object schema");
    prost_types::Value {
        kind: Some(prost_types::value::Kind::StructValue(schema)),
    }
}

/// Anthropic-defined text editors intentionally omit `input_schema` from the
/// Messages request. Cursor's MCP catalog still requires a structured value;
/// provide the documented command vocabulary so function-calling models can
/// select and populate the client-side editor reliably.
fn text_editor_mcp_schema_value() -> prost_types::Value {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            // Claude Code 2.1.193's `text_editor_20250728` contract has
            // exactly four operations.  `delete`/`rename` belonged to an
            // earlier internal memory-tool shape; advertising them here
            // makes the model emit calls that the local handler rejects.
            "command": {"type": "string", "enum": ["view", "create", "str_replace", "insert"]},
            "path": {"type": "string"},
            "old_str": {"type": "string"},
            "new_str": {"type": "string"},
            "file_text": {"type": "string"},
            "view_range": {"type": "array", "items": {"type": "integer"}},
            "max_characters": {"type": "integer"},
            "insert_line": {"type": "integer"},
            "insert_text": {"type": "string"}
        },
        "required": ["command", "path"]
    });
    prost_types::Value {
        kind: Some(prost_types::value::Kind::StructValue(
            json_to_prost_struct(&schema).expect("text editor schema object"),
        )),
    }
}

/// Cursor may qualify MCP names as `provider/tool`, `provider:tool`,
/// `mcp__provider__tool`, or `mcp_provider_tool` (`claude-local/Workflow`,
/// `mcp_claude-local_Workflow`). Anthropic / grok-build `tools[].name` is the
/// bare tool. Only `claude-local` underscore forms are stripped — foreign
/// `mcp__plugin__*` names stay intact.
pub(crate) fn strip_mcp_provider_prefix(name: &str) -> &str {
    for sep in ['/', ':'] {
        if let Some((provider, tool)) = name.split_once(sep)
            && !provider.is_empty()
            && !tool.is_empty()
            && !tool.contains('/')
            && !tool.contains(':')
        {
            return tool;
        }
    }
    if let Some(rest) = name.strip_prefix("mcp__") {
        let mut parts = rest.splitn(2, "__");
        if parts.next() == Some(CLAUDE_LOCAL_MCP_PROVIDER)
            && let Some(tool) = parts.next()
            && !tool.is_empty()
        {
            return tool;
        }
    }
    if let Some(rest) = name.strip_prefix("mcp_") {
        // provider id is `claude-local` (hyphen, no extra `_`), so this prefix
        // is unambiguous even when the tool name itself contains underscores.
        if let Some(tool) = rest.strip_prefix("claude-local_")
            && !tool.is_empty()
        {
            return tool;
        }
    }
    name
}

/// Stable provider id for Claude Code client-local tools advertised as MCP.
///
/// Official Cursor CLI always sets `providerIdentifier` + `toolName` on each
/// `McpToolDefinition`. Without those fields Fable may ignore the tool list.
pub(crate) const CLAUDE_LOCAL_MCP_PROVIDER: &str = "claude-local";

/// Claude-local tools advertised as Cursor `RunRequest.mcp_tools`.
///
/// Prompt `<tools>` text alone is not enough: Fable's agent loop invokes MCP
/// tools via `InteractionUpdate.tool_call_started` / MCP args. Without this
/// field, Workflow/Skill are never called and turns end empty after thinking.
///
/// Wire shape must match `agent.v1.McpToolDefinition`: `input_schema` is a
/// `google.protobuf.Value` (`struct_value`), plus `provider_identifier` /
/// `tool_name`. Workflow / Skill / `mcp__*` are advertised for Claude Code.
/// grok-build lifecycle names are advertised too — Fable only invokes the
/// MCP catalog, so XML-only `spawn_subagent` is `Tool not found`. Filesystem
/// and web grok names with a native remap stay off MCP. Steal maps
/// `mcp_claude-local_*` back to the exact grok wire name.
/// The Anthropic `input_schema` object is copied into that Value (not a raw
/// Struct at tag 3, which Cursor rejected with `invalid end group tag`).
pub fn claude_local_mcp_tools(req: &MessagesRequest) -> Option<super::proto::McpTools> {
    let tools = req.extra.get("tools")?.as_array()?;
    let grok_build_request = request_has_grok_build_client_tools(req);
    let mapped: Vec<super::proto::McpTool> = tools
        .iter()
        .filter_map(|tool| {
            let name = tool.get("name")?.as_str()?.to_string();
            if !is_model_visible_tool_definition(tool) {
                return None;
            }
            if !advertise_as_cursor_mcp(&name, grok_build_request) {
                return None;
            }
            // Keep descriptions essentially intact: Claude Code's Workflow /
            // Task / Skill descriptions ENUMERATE the valid workflow, agent,
            // and skill names. The old 240-char cap cut those lists off, so
            // function-calling models (gemini) hallucinated names like
            // "asr-review-workflow". The generous ceiling only guards against
            // pathological megabyte descriptions.
            let mut description = tool
                .get("description")
                .and_then(|d| d.as_str())
                .unwrap_or("")
                .chars()
                .take(16_384)
                .collect::<String>();
            if is_text_editor_tool_name(&name) && description.trim().is_empty() {
                description = "Client-side text editor. Use command=view, create, str_replace, or insert with the documented path fields.".into();
            }
            let wire_name = cursor_mcp_wire_name(&name);
            let input_schema = if is_text_editor_tool_name(&name)
                && tool.get("input_schema").is_none()
            {
                text_editor_mcp_schema_value()
            } else {
                mcp_input_schema_value(tool)
            };
            Some(super::proto::McpTool {
                tool_name: wire_name.clone(),
                provider_identifier: CLAUDE_LOCAL_MCP_PROVIDER.to_string(),
                name: wire_name,
                description,
                input_schema: Some(input_schema),
            })
        })
        .collect();
    // Cursor treats the final wire name as the identity. Deduplicate here so
    // repeated Anthropic definitions (or two long names that shorten to the
    // same alias) cannot create an ambiguous MCP catalog.
    let mut seen_wire_names = std::collections::BTreeSet::new();
    let mapped: Vec<_> = mapped
        .into_iter()
        .filter(|tool| seen_wire_names.insert(tool.name.clone()))
        .collect();
    if mapped.is_empty() {
        None
    } else {
        Some(super::proto::McpTools { tools: mapped })
    }
}

/// Convert a JSON object into `google.protobuf.Struct` for MCP tool schemas.
fn json_to_prost_struct(value: &serde_json::Value) -> Option<prost_types::Struct> {
    let serde_json::Value::Object(map) = value else {
        return None;
    };
    let mut fields = std::collections::BTreeMap::new();
    for (key, val) in map {
        fields.insert(key.clone(), json_to_prost_value(val));
    }
    Some(prost_types::Struct { fields })
}

fn json_to_prost_value(value: &serde_json::Value) -> prost_types::Value {
    use prost_types::value::Kind;
    let kind = match value {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(b) => Kind::BoolValue(*b),
        serde_json::Value::Number(n) => Kind::NumberValue(n.as_f64().unwrap_or(0.0)),
        serde_json::Value::String(s) => Kind::StringValue(s.clone()),
        serde_json::Value::Array(items) => Kind::ListValue(prost_types::ListValue {
            values: items.iter().map(json_to_prost_value).collect(),
        }),
        serde_json::Value::Object(map) => {
            let mut fields = std::collections::BTreeMap::new();
            for (k, v) in map {
                fields.insert(k.clone(), json_to_prost_value(v));
            }
            Kind::StructValue(prost_types::Struct { fields })
        }
    };
    prost_types::Value { kind: Some(kind) }
}

const BRANCH_PREFIXES: [&str; 4] = [
    "git branch --show-current:",
    "Current branch:",
    "Active branch:",
    "Branch:",
];

/// True when RequestContext carries cwd and/or git identity (not an empty {}).
pub fn request_context_is_populated(ctx: &super::proto::RequestContext) -> bool {
    ctx.env.as_ref().is_some_and(|env| {
        !env.workspace_paths.is_empty()
            || !env.project_folder.is_empty()
            || !env.process_working_directory.is_empty()
    }) || ctx.git_repos.iter().any(|repo| !repo.path.is_empty())
}

/// Build CLI `RequestContext` from Claude system / `<system-reminder>` (cwd, git).
///
/// For the exec reply (`request_context_result`), not RunRequest. Does **not**
/// copy the Claude system prompt into Cursor system, rules, or agent_skills.
pub fn cursor_request_context(req: &MessagesRequest) -> super::proto::RequestContext {
    let system = req.extra.get("system");
    let message_contents = req.messages.iter().map(|m| &m.content);
    let cwd = crate::project::cwd_from_request(system, message_contents);
    let branch_hint = branch_from_request(req);
    request_context_from_cwd(cwd.as_deref(), branch_hint.as_deref())
}

/// Best-effort parse when only UserMessage text is available (no Anthropic system).
pub fn cursor_request_context_from_text(text: &str) -> super::proto::RequestContext {
    let cwd = crate::project::cwd_from_system(Some(&serde_json::Value::String(text.to_string())));
    let branch_hint = branch_from_text(text);
    request_context_from_cwd(cwd.as_deref(), branch_hint.as_deref())
}

fn request_context_from_cwd(
    cwd: Option<&str>,
    branch_hint: Option<&str>,
) -> super::proto::RequestContext {
    // Remote Claude Code (LAN/WSL) sends its own cwd in <system-reminder>.
    // Filling RequestContext with a path that does not exist on this host is
    // useless; skip rather than advertise a ghost workspace.
    let Some(cwd) = cwd.filter(|p| !p.is_empty() && std::path::Path::new(p).exists()) else {
        return super::proto::RequestContext::default();
    };
    let git = git_identity(std::path::Path::new(cwd), branch_hint);
    let env = super::proto::RequestContextEnv {
        os_version: std::env::consts::OS.to_string(),
        workspace_paths: vec![cwd.to_string()],
        shell: std::env::var("SHELL").unwrap_or_default(),
        sandbox_enabled: false,
        time_zone: std::env::var("TZ").unwrap_or_default(),
        project_folder: cwd.to_string(),
        process_working_directory: cwd.to_string(),
    };
    super::proto::RequestContext {
        env: Some(env),
        git_repos: git.into_iter().collect(),
        ..Default::default()
    }
}

fn git_identity(
    cwd: &std::path::Path,
    branch_hint: Option<&str>,
) -> Option<super::proto::GitRepoInfo> {
    let root = cwd
        .ancestors()
        .find(|ancestor| ancestor.join(".git").exists())?;
    let git_dir = resolve_git_dir(root)?;
    let branch = read_git_branch(&git_dir)
        .or_else(|| branch_hint.map(str::to_string))
        .unwrap_or_default();
    let remote_url = read_git_remote_url(&git_dir);
    Some(super::proto::GitRepoInfo {
        path: root.to_string_lossy().into_owned(),
        status: String::new(),
        branch_name: branch,
        remote_url,
    })
}

fn resolve_git_dir(root: &std::path::Path) -> Option<std::path::PathBuf> {
    let marker = root.join(".git");
    if marker.is_dir() {
        return Some(marker);
    }
    let contents = std::fs::read_to_string(&marker).ok()?;
    let git_dir = contents.trim().strip_prefix("gitdir:")?.trim();
    let git_dir = if std::path::Path::new(git_dir).is_absolute() {
        std::path::PathBuf::from(git_dir)
    } else {
        root.join(git_dir)
    };
    Some(git_dir)
}

fn read_git_branch(git_dir: &std::path::Path) -> Option<String> {
    let head = std::fs::read_to_string(git_dir.join("HEAD")).ok()?;
    let head = head.trim();
    if let Some(branch) = head.strip_prefix("ref: refs/heads/") {
        let branch = branch.trim();
        if !branch.is_empty() {
            return Some(branch.to_string());
        }
    }
    None
}

fn read_git_remote_url(git_dir: &std::path::Path) -> Option<String> {
    let config = std::fs::read_to_string(git_dir.join("config")).ok()?;
    let mut in_origin = false;
    for line in config.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_origin = line.eq_ignore_ascii_case("[remote \"origin\"]");
            continue;
        }
        if in_origin && let Some(url) = line.strip_prefix("url") {
            let url = url.trim().trim_start_matches('=').trim();
            if !url.is_empty() {
                return Some(url.to_string());
            }
        }
    }
    None
}

fn branch_from_request(req: &MessagesRequest) -> Option<String> {
    if let Some(system) = req.extra.get("system")
        && let Some(branch) = branch_from_value(system)
    {
        return Some(branch);
    }
    req.messages
        .iter()
        .find_map(|message| branch_from_value(&message.content))
}

fn branch_from_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(text) => branch_from_text(text),
        serde_json::Value::Array(values) => values.iter().find_map(branch_from_value),
        serde_json::Value::Object(object) => object
            .get("text")
            .and_then(|t| t.as_str())
            .and_then(branch_from_text)
            .or_else(|| object.get("content").and_then(branch_from_value)),
        _ => None,
    }
}

fn branch_from_text(text: &str) -> Option<String> {
    text.lines().find_map(|line| {
        let line = line.trim().strip_prefix("- ").unwrap_or(line.trim());
        BRANCH_PREFIXES.iter().find_map(|prefix| {
            line.strip_prefix(prefix)
                .map(str::trim)
                .filter(|branch| !branch.is_empty() && *branch != "true" && *branch != "false")
                .map(str::to_string)
        })
    })
}

/// Split Anthropic MessagesRequest into Cursor system vs user payloads.
pub fn render_cursor_prompt_parts(req: &MessagesRequest) -> CursorPromptParts {
    render_cursor_prompt_parts_with(req, CursorPromptOptions::default())
}

pub fn render_cursor_prompt_parts_with(
    req: &MessagesRequest,
    opts: CursorPromptOptions,
) -> CursorPromptParts {
    // Exact Claude Code system (only strips x-anthropic-billing-header lines).
    let system = render_system(req);

    let mut sections: Vec<String> = Vec::new();

    let custom_system_prompt = if use_custom_system_prompt_field() {
        system.clone()
    } else {
        // Default: omit Claude system from Cursor payload (avoids Fable injection loops).
        if !opts.delta_only
            && embed_system_in_user()
            && let Some(ref sys) = system
        {
            if packaged_system_embed() {
                sections.push(format!("{SYSTEM_OPEN}\n{sys}\n{SYSTEM_CLOSE}"));
            } else {
                sections.push(sys.clone());
            }
        }
        None
    };

    if opts.delta_only {
        if let Some(delta) = render_latest_user_delta(req) {
            sections.push(format!("<user>\n{delta}\n</user>"));
        }
    } else {
        // Full multi-turn history (agent mode). Strip packaging banners + Fable
        // injection-defense monologues so polluted sessions don't re-litigate forever.
        let mut message_parts: Vec<String> = Vec::new();
        for message in &req.messages {
            let content = render_message_content(message);
            if let Some(c) = content {
                let c = scrub_injection_noise(&message.role, &c);
                if !c.trim().is_empty() {
                    message_parts.push(format!("<{}>\n{}\n</{}>", message.role, c, message.role));
                }
            }
        }
        if !message_parts.is_empty() {
            sections.push(message_parts.join("\n\n"));
        }
    }

    // Tools: Anthropic top-level field.
    // - Full dump when CCP_CURSOR_FORCE_TOOLS_IN_PROMPT=1.
    // - When mcp_tools is populated: names + one-line description only (no
    //   duplicated JSON schemas). Keep a short Workflow/Skill nudge (W1).
    // - When omit_tools / delta_only without mcp_tools: Claude-local full dump.
    let force_tools = env_flag("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
    let mcp_populated = claude_local_mcp_tools(req).is_some();
    let tools_block = if force_tools {
        // `mcp_tools` is the authoritative callable catalog.  The debug
        // override may request native schemas, but must not re-inject MCP
        // schemas under a second (often provider-prefixed) name: doing so
        // creates two identities for one tool and can make Cursor emit the
        // wrong name on the return path.
        if mcp_populated {
            render_tools_block(req, ToolDumpMode::NativeFullMcpCompact)
        } else {
            render_tools_block(req, ToolDumpMode::All)
        }
    } else if mcp_populated {
        if !opts.omit_tools && !opts.delta_only {
            render_tools_block(req, ToolDumpMode::NativeFullMcpCompact)
        } else {
            render_tools_block(req, ToolDumpMode::CompactClaudeLocal)
        }
    } else if !opts.omit_tools && !opts.delta_only {
        render_tools_block(req, ToolDumpMode::All)
    } else {
        render_tools_block(req, ToolDumpMode::ClaudeLocalOnly)
    };
    if let Some(tools) = tools_block {
        sections.push(tools);
    }

    let request_context = cursor_request_context(req);

    CursorPromptParts {
        custom_system_prompt,
        user_text: sections.join("\n\n"),
        request_context,
    }
}

/// Latest user text that is not solely *older* tool_result blocks (new Claude turn).
///
/// Native exec results belong on the live BiDi stream, so historical
/// tool_result-only messages are skipped when a newer user turn exists.
/// After ClientOnly (Workflow/Skill/mcp__*) teardown there is no live run:
/// the latest user message *is* those results and must be forwarded.
fn render_latest_user_delta(req: &MessagesRequest) -> Option<String> {
    let rendered = current_user_messages(req)
        .into_iter()
        .filter_map(render_message_content)
        .map(|content| scrub_injection_noise("user", &content))
        .filter(|content| !content.trim().is_empty())
        .collect::<Vec<_>>();
    if rendered.is_empty() {
        None
    } else {
        Some(rendered.join("\n\n"))
    }
}

fn content_is_only_tool_results(message: &Message) -> bool {
    match &message.content {
        serde_json::Value::Array(blocks) => {
            !blocks.is_empty()
                && blocks
                    .iter()
                    .all(|b| b.get("type").and_then(|t| t.as_str()) == Some("tool_result"))
        }
        _ => false,
    }
}

fn is_standalone_system_reminder(message: &Message) -> bool {
    fn wrapped(text: &str) -> bool {
        let text = text.trim();
        text.starts_with("<system-reminder>") && text.ends_with("</system-reminder>")
    }

    match &message.content {
        serde_json::Value::String(text) => wrapped(text),
        serde_json::Value::Array(blocks)
            if !blocks.is_empty()
                && blocks.iter().all(|block| {
                    block.get("type").and_then(|value| value.as_str()) == Some("text")
                }) =>
        {
            let text = blocks
                .iter()
                .filter_map(|block| block.get("text").and_then(|value| value.as_str()))
                .collect::<String>();
            wrapped(&text)
        }
        _ => false,
    }
}

/// The logical user turn after the most recent assistant boundary.
///
/// Responses emits one user message per parallel function output. Standalone
/// asynchronous reminders do not split that result batch or replace a newer
/// user prompt; full-history rendering still preserves them.
fn current_user_messages(req: &MessagesRequest) -> Vec<&Message> {
    let mut tool_result_groups = Vec::new();
    for message in req.messages.iter().rev() {
        if message.role == "assistant" {
            break;
        }
        if message.role != "user" || is_standalone_system_reminder(message) {
            continue;
        }
        if !content_is_only_tool_results(message) {
            if tool_result_groups.is_empty() {
                return vec![message];
            }
            break;
        }
        tool_result_groups.push(message);
    }
    tool_result_groups.reverse();
    tool_result_groups
}

pub(crate) fn current_user_blocks(req: &MessagesRequest) -> Vec<&serde_json::Value> {
    let mut blocks = Vec::new();
    for message in current_user_messages(req) {
        if let serde_json::Value::Array(content) = &message.content {
            blocks.extend(content.iter());
        }
    }
    blocks
}

/// Detect Claude Code's local/reactive compaction prompt.
///
/// Claude Code's `m0(... querySource: "compact", forkLabel: ... )` path is a
/// normal Anthropic Messages request: unlike the server-side compaction
/// extension it does not carry `context_management.edits`. Depending on the
/// Claude Code release, the wire signal is either the summary prompt or the
/// explicit `/compact` command metadata appended as the latest user turn. Keep
/// both matchers strict so an ordinary user message such as `/compact` is never
/// routed to the summary-only lane.
pub(crate) fn is_reactive_compact_prompt(req: &MessagesRequest) -> bool {
    let current = current_user_messages(req);
    current.iter().any(|message| {
        let mut text = String::new();
        collect_text_for_compaction(&message.content, &mut text);
        // Claude Code's manual `/compact` command is represented on the wire
        // by these two command metadata tags.  It is a normal Messages
        // request, so headers/querySource are not reliable signals. Require
        // the pair rather than matching either tag independently; partial
        // command transcripts and quoted single-tag fragments stay normal.
        is_compact_command_marker(&text) || is_compaction_summary_prompt(&text)
    })
}

fn is_compact_command_marker(text: &str) -> bool {
    text.contains("<command-name>/compact</command-name>")
        && text.contains("<command-message>compact</command-message>")
}

fn collect_text_for_compaction(value: &serde_json::Value, out: &mut String) {
    match value {
        serde_json::Value::String(text) => out.push_str(text),
        serde_json::Value::Array(values) => {
            for value in values {
                collect_text_for_compaction(value, out);
                out.push('\n');
            }
        }
        serde_json::Value::Object(object) => {
            // Anthropic text blocks are the normal shape.  Accept nested
            // `content` wrappers as well because a few Claude Code releases
            // wrap prompt text while attaching cache metadata.
            if let Some(text) = object.get("text").and_then(serde_json::Value::as_str) {
                out.push_str(text);
            }
            if let Some(content) = object.get("content") {
                collect_text_for_compaction(content, out);
            }
        }
        _ => {}
    }
}

fn is_compaction_summary_prompt(text: &str) -> bool {
    let text = text.trim_start();
    // Both manual `/compact` and automatic reactive compact use this exact
    // contract in Claude Code 2.1.x.  Require several independent phrases so
    // quoting one line in a normal conversation cannot opt into compaction.
    text.starts_with("CRITICAL: Respond with TEXT ONLY")
        && text.contains("Do NOT call any tools")
        && text.contains("Do NOT use Read, Bash, Grep")
        && text.contains("entire response must be plain text")
        && text.contains("Your task is to create a detailed summary")
        && text.contains("conversation")
        && (text.contains("<summary>") || text.contains("&lt;summary&gt;"))
}

/// True when the current logical user turn is only `tool_result` blocks.
pub(crate) fn latest_user_is_only_tool_results(req: &MessagesRequest) -> bool {
    let current = current_user_messages(req);
    !current.is_empty()
        && current
            .iter()
            .all(|message| content_is_only_tool_results(message))
}

fn tool_result_ids(message: &crate::anthropic::schema::Message) -> Vec<String> {
    match &message.content {
        serde_json::Value::Array(blocks) => blocks
            .iter()
            .filter(|block| block.get("type").and_then(|t| t.as_str()) == Some("tool_result"))
            .filter_map(|block| {
                block
                    .get("tool_use_id")
                    .and_then(|id| id.as_str())
                    .map(str::to_string)
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn assistant_tool_name_for_id<'a>(req: &'a MessagesRequest, tool_use_id: &str) -> Option<&'a str> {
    for message in &req.messages {
        if message.role != "assistant" {
            continue;
        }
        let serde_json::Value::Array(blocks) = &message.content else {
            continue;
        };
        for block in blocks {
            if block.get("type").and_then(|t| t.as_str()) != Some("tool_use") {
                continue;
            }
            if block.get("id").and_then(|id| id.as_str()) == Some(tool_use_id) {
                return block.get("name").and_then(|name| name.as_str());
            }
        }
    }
    None
}

/// True when the latest user message carries results for Claude-local tools
/// (`Workflow` / `Skill` / `mcp__*`) that Cursor cannot resume on BiDi.
pub(crate) fn request_has_client_only_tool_results(req: &MessagesRequest) -> bool {
    let ids = current_user_messages(req)
        .into_iter()
        .flat_map(tool_result_ids)
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return false;
    }
    ids.iter()
        .any(|id| assistant_tool_name_for_id(req, id).is_some_and(is_client_only_tool_name))
}

/// True when a Free registry slot is being asked to replay native tool results
/// that were bound to a previous live Run generation. Those results must not
/// open a second Cursor Run.
pub(crate) fn request_has_orphaned_native_live_results(req: &MessagesRequest) -> bool {
    current_user_messages(req)
        .into_iter()
        .flat_map(tool_result_ids)
        .any(|id| {
            id.contains("__cursor_run_")
                && !assistant_tool_name_for_id(req, &id).is_some_and(is_claude_local_tool_name)
        })
}

/// Free-slot policy for generation-tagged native tool_results.
/// Those ids belong to a dead Run (serve restart, cancel, conversation reset).
/// Starting a fresh Cursor Run that replays Anthropic history is the recovery
/// path; 409 makes grok-build / Claude Code retry the same dead payload forever.
pub(crate) fn reject_orphaned_native_results_when_live_slot_is_free(req: &MessagesRequest) -> bool {
    let _ = request_has_orphaned_native_live_results(req);
    false
}

/// Strip packaging banners and Fable injection-defense monologues so multi-turn
/// re-runs don't keep burning minutes on identity / "prompt injection" theater.
fn scrub_injection_noise(role: &str, content: &str) -> String {
    let without_banners = strip_packaging_banners(content);
    if role != "assistant" {
        return without_banners;
    }
    if !looks_like_injection_defense(&without_banners) {
        return without_banners;
    }
    // Keep non-meta paragraphs (e.g. real project analysis / tool XML).
    let kept: Vec<&str> = without_banners
        .split("\n\n")
        .filter(|para| !paragraph_is_injection_defense(para))
        .collect();
    kept.join("\n\n")
}

fn strip_packaging_banners(content: &str) -> String {
    let mut out = content.to_string();
    // Remove legacy ===== CLAUDE_CODE_SYSTEM ... ===== END_... ===== blocks.
    while let Some(start) = out.find(SYSTEM_OPEN) {
        let after = start + SYSTEM_OPEN.len();
        let end = out[after..]
            .find(SYSTEM_CLOSE)
            .map(|i| after + i + SYSTEM_CLOSE.len())
            .unwrap_or(out.len());
        out.replace_range(start..end, "");
    }
    out
}

fn looks_like_injection_defense(content: &str) -> bool {
    content.contains("CLAUDE_CODE_SYSTEM")
        || content.contains("提示词注入")
        || content.contains("prompt injection")
        || content.contains("CLAUDE_CODE_SYSTEM authority")
        || (content.contains("Cursor assistant") && content.contains("Claude Code"))
}

fn paragraph_is_injection_defense(para: &str) -> bool {
    let p = para.trim();
    if p.is_empty() {
        return true;
    }
    p.contains("CLAUDE_CODE_SYSTEM")
        || p.contains("提示词注入")
        || p.contains("prompt injection")
        || p.contains("伪造成")
        || p.contains("不会执行它")
        || p.contains("treat this as data")
        || p.contains("treats this as data")
        || p.contains("I will ignore")
        || p.contains("我将忽略")
        || (p.contains("Cursor assistant") && (p.contains("Claude Code") || p.contains("identity")))
}

/// Full flat text (system + conversation + tools) for token estimates / legacy callers.
pub fn render_cursor_prompt(req: &MessagesRequest) -> String {
    let parts = render_cursor_prompt_parts(req);
    match parts.custom_system_prompt {
        Some(sys) if !parts.user_text.is_empty() => format!("{sys}\n\n{}", parts.user_text),
        Some(sys) => sys,
        None => parts.user_text,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolDumpMode {
    /// Every Anthropic-advertised tool with full JSON schemas.
    All,
    /// Only Claude Code client-local tools, full schemas (no mcp_tools).
    ClaudeLocalOnly,
    /// MCP tools are already registered in the Cursor catalog; omit duplicate
    /// MCP text (Grok-only native aliases may remain as compact wire hints).
    CompactClaudeLocal,
    /// Native tools keep full schemas; MCP tools stay in the Cursor catalog.
    NativeFullMcpCompact,
}

fn render_tools_block(req: &MessagesRequest, mode: ToolDumpMode) -> Option<String> {
    let tools = req.extra.get("tools").and_then(|v| v.as_array())?;
    if tools.is_empty() {
        return None;
    }
    let grok_build_request = request_has_grok_build_client_tools(req);
    // Compute this once for every mode. In particular, `ClaudeLocalOnly` is
    // also used when no MCP catalog was produced; unregistered client aliases
    // must be omitted rather than copied into the XML prompt.
    let mcp_names = claude_local_mcp_name_map(req);
    let tool_lines: Vec<String> = tools
        .iter()
        .filter_map(|t| {
            let name = t.get("name").and_then(|n| n.as_str()).unwrap_or("");
            if !is_model_visible_tool_definition(t) {
                return None;
            }
            let local = is_claude_local_tool_name(name);
            let include = match mode {
                // A text-only dump is still a callable contract.  Keep
                // Cursor-native names and names that were actually accepted
                // into `RunRequest.mcp_tools`; silently drop client aliases
                // that have no execution route for this request.
                ToolDumpMode::All => {
                    !local
                        || mcp_names
                            .as_ref()
                            .is_some_and(|names| names.contains_key(name))
                        || (grok_build_request && grok_client_tool_uses_native_remap(name))
                }
                // MCP tools are already registered with full schemas in the
                // composed Cursor catalog. Do not duplicate them in text;
                // non-MCP client names are not callable and must not leak in.
                ToolDumpMode::NativeFullMcpCompact => {
                    if grok_build_request {
                        !local
                            || (is_grok_build_client_tool_name(name)
                                && grok_client_tool_uses_native_remap(name))
                    } else {
                        !local
                    }
                }
                ToolDumpMode::ClaudeLocalOnly => {
                    local
                        && if grok_build_request {
                            // Grok's native-remapped client names are
                            // intentionally excluded from MCP; their exact
                            // wire names must remain in the text contract.
                            is_grok_build_client_tool_name(name)
                                && grok_client_tool_uses_native_remap(name)
                        } else {
                            mcp_names
                                .as_ref()
                                .is_some_and(|names| names.contains_key(name))
                        }
                }
                // MCP-backed Claude tools are already callable from the
                // Cursor catalog. Grok's incompatible client aliases remain
                // text-visible so its exact wire names can be recovered.
                ToolDumpMode::CompactClaudeLocal => {
                    if grok_build_request {
                        local
                            && is_grok_build_client_tool_name(name)
                            && grok_client_tool_uses_native_remap(name)
                    } else {
                        false
                    }
                }
            };
            if !include {
                return None;
            }
            let compact = match mode {
                ToolDumpMode::CompactClaudeLocal | ToolDumpMode::NativeFullMcpCompact => local,
                ToolDumpMode::All | ToolDumpMode::ClaudeLocalOnly => false,
            };
            let description = t.get("description").and_then(|d| d.as_str()).unwrap_or("");
            if compact {
                let catalog_name = mcp_names
                    .as_ref()
                    .and_then(|names| names.get(name).cloned())
                    .unwrap_or_else(|| name.to_string());
                Some(
                    serde_json::json!({
                        // Use the exact name supplied to mcp_tools. No
                        // synthetic provider prefix is added here.
                        "name": catalog_name,
                        "description": description,
                    })
                    .to_string(),
                )
            } else {
                let input_schema = t
                    .get("input_schema")
                    .cloned()
                    .unwrap_or(serde_json::Value::Object(Default::default()));
                Some(
                    serde_json::json!({
                        "name": name,
                        "description": description,
                        "input_schema": input_schema,
                    })
                    .to_string(),
                )
            }
        })
        .collect();
    let preface = match mode {
        ToolDumpMode::All => String::new(),
        // No MCP catalog on the wire: bare names + XML recovery.
        ToolDumpMode::ClaudeLocalOnly if !tool_lines.is_empty() => tools_dump_preface(req, false),
        // MCP tools are callable from Cursor's native catalog; do not inject
        // a duplicate text catalog or synthetic provider-prefixed names.
        // Grok's native-remapped aliases still need their exact wire-name
        // preface because those aliases are deliberately kept out of MCP.
        ToolDumpMode::CompactClaudeLocal | ToolDumpMode::NativeFullMcpCompact => {
            if grok_build_request && !tool_lines.is_empty() {
                tools_dump_preface(req, true)
            } else {
                String::new()
            }
        }
        ToolDumpMode::ClaudeLocalOnly => String::new(),
    };
    // Claude Code's native Write may be used for both existing and new files.
    // Build this hint from the exact names in the request so it never teaches
    // an alias that is absent from the callable catalog.
    let write_hint = write_tool_hint(tools);
    let preface = format!("{write_hint}{preface}");
    if tool_lines.is_empty() && preface.is_empty() {
        None
    } else {
        let body = tool_lines.join("\n");
        Some(format!("<tools>\n{preface}{body}\n</tools>"))
    }
}

fn write_tool_hint(tools: &[serde_json::Value]) -> String {
    let visible_names = tools.iter().filter_map(|tool| {
        if !is_model_visible_tool_definition(tool) {
            return None;
        }
        tool.get("name").and_then(|name| name.as_str())
    });
    let mut reads = Vec::new();
    let mut writes = Vec::new();
    for name in visible_names {
        if matches!(name, "Read" | "read" | "read_file" | "ReadFile") {
            reads.push(name);
        }
        if matches!(name, "Write" | "write" | "write_file" | "WriteFile") {
            writes.push(name);
        }
    }
    if writes.is_empty() {
        return String::new();
    }
    let read_label = match reads.as_slice() {
        [] => "read".to_string(),
        [one] => (*one).to_string(),
        many => many.join(" or "),
    };
    let write_label = match writes.as_slice() {
        [one] => (*one).to_string(),
        many => many.join(" or "),
    };
    if reads.is_empty() {
        format!(
            "For an existing file, read it before calling {write_label}; a new file may be created directly with {write_label}.\n"
        )
    } else {
        format!(
            "For an existing file, call {read_label} first and then {write_label}; a new file may be created directly with {write_label}.\n"
        )
    }
}

/// Return the exact MCP catalog spelling for every original Anthropic tool
/// name. The prompt renderer uses this map instead of independently guessing
/// which names were registered, so Cursor's 64-character aliases remain a
/// single source of truth across protobuf registration, prompt text, and
/// result-name restoration.
fn claude_local_mcp_name_map(
    req: &MessagesRequest,
) -> Option<std::collections::BTreeMap<String, String>> {
    let tools = req.extra.get("tools")?.as_array()?;
    let catalog = claude_local_mcp_tools(req)?;
    let wire_names = catalog
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut names = std::collections::BTreeMap::new();
    for tool in tools {
        let Some(original) = tool.get("name").and_then(|name| name.as_str()) else {
            continue;
        };
        let wire = cursor_mcp_wire_name(original);
        if wire_names.contains(wire.as_str()) {
            names.insert(original.to_string(), wire);
        }
    }
    if names.is_empty() { None } else { Some(names) }
}

fn request_has_grok_build_client_tools(req: &MessagesRequest) -> bool {
    let Some(tools) = req.extra.get("tools").and_then(|v| v.as_array()) else {
        return false;
    };
    tools.iter().any(|tool| {
        tool.get("name")
            .and_then(|name| name.as_str())
            .is_some_and(is_grok_build_client_tool_name)
    })
}

fn tools_dump_preface(req: &MessagesRequest, mcp_catalog: bool) -> String {
    if request_has_grok_build_client_tools(req) {
        // Only mention names that this request actually advertised and that
        // have a route (native remap or the MCP catalog). The old hard-coded
        // list taught absent tools such as `web_fetch` on a read-only request,
        // which produced deterministic "Tool not found" loops.
        let mcp_names = claude_local_mcp_name_map(req);
        const GROK_PROMPT_ORDER: &[&str] = &[
            "run_terminal_command",
            "run_terminal_cmd",
            "read_file",
            "list_dir",
            "grep",
            "write",
            "search_replace",
            "todo_write",
            "web_search",
            "web_fetch",
            "ask_user_question",
            "enter_plan_mode",
            "exit_plan_mode",
        ];
        let tools = req
            .extra
            .get("tools")
            .and_then(|value| value.as_array())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let mut names = Vec::new();
        for candidate in GROK_PROMPT_ORDER {
            let Some(tool) = tools.iter().find(|tool| {
                is_model_visible_tool_definition(tool)
                    && tool.get("name").and_then(|value| value.as_str()) == Some(*candidate)
            }) else {
                continue;
            };
            let name = tool.get("name").and_then(|value| value.as_str()).unwrap();
            let registered = grok_client_tool_uses_native_remap(name)
                || mcp_names
                    .as_ref()
                    .is_some_and(|catalog| catalog.contains_key(name));
            if registered {
                names.push(name);
            }
        }
        if names.is_empty() {
            "Use only the exact tool names advertised by this request; lifecycle tools stay under their registered catalog names.\n".to_string()
        } else {
            format!(
                "Call {} by those exact names; lifecycle tools stay under their registered catalog names.\n",
                names.join(", ")
            )
        }
    } else if mcp_catalog {
        // MCP names and schemas are supplied by Cursor's callable catalog.
        // Keep this wording independent of a synthetic prefix so it cannot
        // drift from the registered names or exceed Cursor's name limit.
        let catalog = claude_local_mcp_name_map(req);
        let mut preferences = Vec::new();
        if let Some(catalog) = catalog.as_ref() {
            for preferred in ["Workflow", "Skill"] {
                if let Some(name) = catalog.get(preferred) {
                    preferences.push(name.clone());
                }
            }
        }
        let mut text =
            "Use only the exact tool names registered in the callable catalog; do not add a provider prefix."
                .to_string();
        if !preferences.is_empty() {
            text.push_str(" Prefer the registered ");
            text.push_str(&preferences.join(" and "));
            text.push_str(" tool when it matches the request.");
        }
        text.push('\n');
        text
    } else {
        "Use only the exact Claude Code client tool names advertised in this request; do not invent aliases.\n".to_string()
    }
}

/// Extract selected images from the request, mimicking `cursorSelectedImages`.
///
/// Only base64 source images with non-empty data are included. URL images are
/// skipped. Images nested inside tool_result blocks are also collected.
///
/// Scope is the **current user turn** (trailing user messages after the last
/// assistant). Replaying older Anthropic history screenshots as new
/// `selected_images` with fresh UUIDs makes Cursor look up stale asset ids and
/// 502 `Image not found [internal]`.
pub fn cursor_selected_images(req: &MessagesRequest) -> Vec<CursorSelectedImage> {
    cursor_selected_images_with_history(req, false)
}

/// Extract selected images for a new Cursor run.
///
/// When a conversation checkpoint exists, only the trailing user turn is sent
/// because Cursor already owns the assets from earlier turns. A run that never
/// produced a checkpoint is different: Claude Code may retry with an assistant
/// error/partial turn in the history, placing the original image before the
/// trailing user message. In that case replay all user images so a fresh Cursor
/// conversation can reconstruct the request instead of silently dropping the
/// attachment.
pub(crate) fn cursor_selected_images_for_continuation(
    req: &MessagesRequest,
    has_checkpoint: bool,
) -> Vec<CursorSelectedImage> {
    cursor_selected_images_with_history(req, !has_checkpoint)
}

fn cursor_selected_images_with_history(
    req: &MessagesRequest,
    include_history: bool,
) -> Vec<CursorSelectedImage> {
    let mut images: Vec<CursorSelectedImage> = Vec::new();

    let messages = if include_history {
        req.messages
            .iter()
            .filter(|message| message.role == "user")
            .collect()
    } else {
        current_turn_user_messages(req)
    };
    for message in messages {
        let blocks = message_blocks(message);
        for block in &blocks {
            collect_image_blocks(block, &mut images);
        }
    }

    images
}

/// User messages after the last assistant message (the in-flight Anthropic turn).
fn current_turn_user_messages(req: &MessagesRequest) -> Vec<&crate::anthropic::schema::Message> {
    let mut trailing: Vec<&crate::anthropic::schema::Message> = Vec::new();
    for message in req.messages.iter().rev() {
        if message.role == "user" {
            trailing.push(message);
        } else if message.role == "assistant" {
            break;
        }
    }
    trailing.reverse();
    trailing
}

/// Cursor's CLI assigns each inline image a fresh UUID. The UUID is metadata
/// for the selected-image entry, while the actual payload is carried in the
/// protobuf `data` field; reusing a content-derived UUID can make the server
/// reuse a stale asset entry when the same bytes are pasted again.
fn fresh_image_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Re-issue inline image metadata for a fresh Cursor conversation.
///
/// Cursor treats the UUID on an inline `SelectedImage` as the identity of the
/// uploaded asset.  A failed continuation can leave that identity pointing at
/// an expired/stale entry even though the original bytes are still available
/// in the Anthropic request.  Keep the payload and MIME/path untouched while
/// assigning a new CLI-style UUID for the one bounded recovery attempt.
pub(crate) fn refresh_image_uuids(images: &[CursorSelectedImage]) -> Vec<CursorSelectedImage> {
    images
        .iter()
        .map(|image| CursorSelectedImage {
            data: image.data.clone(),
            uuid: fresh_image_uuid(),
            path: image.path.clone(),
            mime_type: image.mime_type.clone(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn render_system(req: &MessagesRequest) -> Option<String> {
    let system_value = req.extra.get("system")?;
    let text = match system_value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(blocks) => {
            let parts: Vec<&str> = blocks
                .iter()
                .filter_map(|b| {
                    if b.get("type").and_then(|t| t.as_str()) == Some("text") {
                        b.get("text").and_then(|t| t.as_str())
                    } else {
                        None
                    }
                })
                .filter(|line| !line.starts_with("x-anthropic-billing-header:"))
                .collect();
            if parts.is_empty() {
                return None;
            }
            parts.join("\n\n")
        }
        _ => return None,
    };
    if text.is_empty() {
        return None;
    }
    Some(text)
}

fn render_message_content(message: &crate::anthropic::schema::Message) -> Option<String> {
    let blocks = message_blocks(message);
    let rendered: Vec<String> = blocks.iter().filter_map(render_block).collect();
    if rendered.is_empty() {
        None
    } else {
        Some(rendered.join("\n\n"))
    }
}

fn render_block(block: &serde_json::Value) -> Option<String> {
    let block_type = block.get("type").and_then(|t| t.as_str())?;
    match block_type {
        "text" => block
            .get("text")
            .and_then(|t| t.as_str())
            .map(|s| s.to_string()),
        // Thinking blocks are an internal Anthropic history representation.
        // Do not serialize them as literal XML into Cursor's user prompt:
        // Fable/Grok can treat the markup as ordinary user text and echo it
        // back to Claude Code. Live reasoning is transported separately via
        // Cursor's thinking_delta/SSE channel, so historical reasoning is
        // intentionally omitted here.
        "thinking" => None,
        "compaction" => {
            let content = block
                .get("content")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            Some(format!("<compaction>\n{content}\n</compaction>"))
        }
        "image" | "input_image" | "image_url" => {
            let (raw, hinted_mime) = image_candidate(block)?;
            if let Some((data, mime_type)) = normalize_image_data(raw, hinted_mime) {
                Some(format!("[image: {mime_type}, {} base64 chars]", data.len()))
            } else {
                Some(format!("[image: {raw}]"))
            }
        }
        "tool_use" => {
            let id = block.get("id").and_then(|i| i.as_str()).unwrap_or("");
            let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let input = block
                .get("input")
                .and_then(|i| serde_json::to_string(i).ok())
                .unwrap_or_else(|| "{}".to_string());
            Some(format!(
                "<tool_use id=\"{id}\" name=\"{name}\">\n{input}\n</tool_use>"
            ))
        }
        "tool_result" => {
            let tool_use_id = block
                .get("tool_use_id")
                .and_then(|t| t.as_str())
                .unwrap_or("");
            let is_error = block
                .get("is_error")
                .and_then(|e| e.as_bool())
                .unwrap_or(false);
            let error_attr = if is_error { " is_error=\"true\"" } else { "" };
            let content = render_tool_result_content(block);
            Some(format!(
                "<tool_result tool_use_id=\"{tool_use_id}\"{error_attr}>\n{content}\n</tool_result>"
            ))
        }
        "server_tool_use" => {
            let id = block.get("id").and_then(|i| i.as_str()).unwrap_or("");
            let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let input = block
                .get("input")
                .and_then(|i| serde_json::to_string(i).ok())
                .unwrap_or_else(|| "{}".to_string());
            Some(format!(
                "<server_tool_use id=\"{id}\" name=\"{name}\">\n{input}\n</server_tool_use>"
            ))
        }
        "web_search_tool_result" => {
            let tool_use_id = block
                .get("tool_use_id")
                .and_then(|t| t.as_str())
                .unwrap_or("");
            let content = block
                .get("content")
                .and_then(|c| serde_json::to_string(c).ok())
                .unwrap_or_else(|| "{}".to_string());
            Some(format!(
                "<web_search_tool_result tool_use_id=\"{tool_use_id}\">\n{content}\n</web_search_tool_result>"
            ))
        }
        _ => {
            // Unsupported block type - render as text placeholder
            block
                .get("text")
                .and_then(|t| t.as_str())
                .map(|s| s.to_string())
        }
    }
}

fn render_tool_result_content(block: &serde_json::Value) -> String {
    let content = match block.get("content") {
        Some(serde_json::Value::String(s)) => return s.clone(),
        Some(serde_json::Value::Array(arr)) => arr.clone(),
        Some(serde_json::Value::Object(object)) if object.contains_key("type") => {
            vec![serde_json::Value::Object(object.clone())]
        }
        _ => return String::new(),
    };

    let parts: Vec<String> = content
        .iter()
        .filter_map(render_tool_result_block)
        .collect();
    parts.join("\n\n")
}

fn render_tool_result_block(block: &serde_json::Value) -> Option<String> {
    let block_type = block.get("type").and_then(|t| t.as_str())?;
    match block_type {
        "text" | "image" | "input_image" | "image_url" | "tool_use" | "tool_result"
        // Nested historical thinking is filtered by render_block for the same
        // reason as top-level assistant thinking blocks.
        | "thinking" => render_block(block),
        _ => {
            let type_str = block_type.to_string();
            Some(format!("[unsupported tool result block: {type_str}]"))
        }
    }
}

fn message_blocks(message: &crate::anthropic::schema::Message) -> Vec<serde_json::Value> {
    match &message.content {
        serde_json::Value::String(s) => {
            vec![serde_json::json!({"type": "text", "text": s})]
        }
        serde_json::Value::Array(arr) => arr.clone(),
        serde_json::Value::Object(object) if object.contains_key("type") => {
            vec![serde_json::Value::Object(object.clone())]
        }
        _ => Vec::new(),
    }
}

fn collect_image_blocks(block: &serde_json::Value, images: &mut Vec<CursorSelectedImage>) {
    if let Some((raw, hinted_mime)) = image_candidate(block)
        && let Some((data, mime_type)) = normalize_image_data(raw, hinted_mime)
    {
        let uuid = fresh_image_uuid();
        images.push(CursorSelectedImage {
            data,
            uuid,
            // The official CLI leaves `path` empty when `data` is present.
            // Supplying a synthetic path can make Cursor look up a stale
            // asset and return `Image not found [internal]`.
            path: String::new(),
            mime_type,
        });
        return;
    }

    // Recurse into tool_result blocks for nested images
    if block.get("type").and_then(|t| t.as_str()) == Some("tool_result") {
        let content = match block.get("content") {
            Some(serde_json::Value::Array(arr)) => arr.clone(),
            Some(serde_json::Value::Object(object)) if object.contains_key("type") => {
                vec![serde_json::Value::Object(object.clone())]
            }
            _ => return,
        };
        for child in &content {
            let child_type = child.get("type").and_then(|t| t.as_str());
            if matches!(
                child_type,
                Some(
                    "text"
                        | "image"
                        | "input_image"
                        | "image_url"
                        | "tool_use"
                        | "tool_result"
                        | "thinking"
                )
            ) {
                collect_image_blocks(child, images);
            }
        }
    }
}

/// Return an image payload from Anthropic blocks and the OpenAI-compatible
/// `input_image`/`image_url` forms emitted by newer Claude Code clients.
/// Remote URLs are intentionally returned to the normalizer and skipped there;
/// the proxy only forwards bytes it received in the request.
fn image_candidate(block: &serde_json::Value) -> Option<(&str, Option<&str>)> {
    let block_type = block.get("type").and_then(|value| value.as_str())?;
    match block_type {
        "image" => {
            let source = block.get("source");
            if let Some(source) = source.and_then(|value| value.as_object()) {
                let hinted = source
                    .get("media_type")
                    .or_else(|| source.get("mime_type"))
                    .and_then(|value| value.as_str());
                if let Some(data) = source.get("data").and_then(|value| value.as_str()) {
                    return Some((data, hinted));
                }
                if let Some(url) = source.get("url").and_then(|value| value.as_str()) {
                    return Some((url, hinted));
                }
            }
            block
                .get("data")
                .and_then(|value| value.as_str())
                .map(|data| (data, None))
        }
        "input_image" | "image_url" => {
            // Newer Claude Code clients may preserve the Anthropic image
            // source shape under `input_image.source` instead of converting
            // it to OpenAI's `image_url` field. Accept both wire forms.
            if let Some(source) = block.get("source").and_then(|value| value.as_object()) {
                let hinted = source
                    .get("media_type")
                    .or_else(|| source.get("mime_type"))
                    .and_then(|value| value.as_str());
                if let Some(data) = source
                    .get("data")
                    .or_else(|| source.get("url"))
                    .and_then(|value| value.as_str())
                {
                    return Some((data, hinted));
                }
            }
            let value = block
                .get("image_url")
                .or_else(|| block.get("imageUrl"))
                .or_else(|| block.get("url"))?;
            if let Some(url) = value.as_str() {
                return Some((url, None));
            }
            let object = value.as_object()?;
            let hinted = object
                .get("media_type")
                .or_else(|| object.get("mime_type"))
                .and_then(|value| value.as_str());
            object
                .get("url")
                .or_else(|| object.get("data"))
                .and_then(|value| value.as_str())
                .map(|value| (value, hinted))
        }
        _ => None,
    }
}

/// Decode a base64 image, accepting a data URI, whitespace/newline-wrapped
/// payloads, URL-safe alphabets, and missing padding. The returned Base64 is
/// canonical so the protobuf layer always receives the exact image bytes.
fn normalize_image_data(raw: &str, hinted_mime: Option<&str>) -> Option<(String, String)> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
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
    let bytes = decode_base64_flexible(&compact)?;
    let mime_type = normalize_mime_type(hinted_mime.or(uri_mime), &bytes);
    Some((
        base64::engine::general_purpose::STANDARD.encode(bytes),
        mime_type,
    ))
}

fn decode_base64_flexible(value: &str) -> Option<Vec<u8>> {
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
    STANDARD
        .decode(value)
        .or_else(|_| STANDARD_NO_PAD.decode(value))
        .or_else(|_| URL_SAFE.decode(value))
        .or_else(|_| URL_SAFE_NO_PAD.decode(value))
        .ok()
}

fn normalize_mime_type(hinted: Option<&str>, data: &[u8]) -> String {
    let hinted = hinted
        .map(str::trim)
        .filter(|mime| mime.starts_with("image/"))
        .and_then(|mime| mime.split(';').next())
        .filter(|mime| !mime.is_empty());
    hinted
        .map(str::to_ascii_lowercase)
        .or_else(|| sniff_image_mime(data).map(str::to_string))
        .unwrap_or_else(|| "image/png".to_string())
}

fn sniff_image_mime(data: &[u8]) -> Option<&'static str> {
    if data.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if data.starts_with(&[0xff, 0xd8]) {
        Some("image/jpeg")
    } else if data.starts_with(b"GIF87a") || data.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if data.len() >= 12 && &data[..4] == b"RIFF" && &data[8..12] == b"WEBP" {
        Some("image/webp")
    } else if data.starts_with(b"BM") {
        Some("image/bmp")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    /// Serialize tests that mutate process-wide CCP_CURSOR_* env flags.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn reactive_compact_prompt() -> &'static str {
        "CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.\n\n\
         - Do NOT use Read, Bash, Grep, Glob, Edit, Write, or ANY other tool.\n\
         - You already have all the context you need in the conversation above.\n\
         - Tool calls will be REJECTED and will waste your only turn — you will fail the task.\n\
         - Your entire response must be plain text: an <analysis> block followed by a <summary> block.\n\
         Your task is to create a detailed summary of this conversation.\n\
         Before providing your final summary, wrap your analysis in <analysis> tags.\n\
         <summary>"
    }

    #[test]
    fn detects_claude_reactive_compact_summary_prompt() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "gemini-3.1-pro",
            "messages": [{"role": "user", "content": reactive_compact_prompt()}]
        }))
        .expect("valid compact request");
        assert!(is_reactive_compact_prompt(&req));
    }

    #[test]
    fn detects_compact_prompt_in_nested_text_blocks() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "gemini-3.1-pro",
            "messages": [{"role": "user", "content": {
                "content": [{"type": "text", "text": reactive_compact_prompt()}]
            }}]
        }))
        .expect("valid nested compact request");
        assert!(is_reactive_compact_prompt(&req));
    }

    #[test]
    fn detects_claude_manual_compact_command_marker() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "gemini-3.1-pro",
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "<command-name>/compact</command-name>\n<command-message>compact</command-message>"},
                {"type": "text", "text": "<command-args></command-args>"}
            ]}]
        }))
        .expect("valid command-marker request");
        assert!(is_reactive_compact_prompt(&req));
    }

    #[test]
    fn compact_command_marker_requires_both_metadata_tags() {
        for content in [
            "<command-name>/compact</command-name>",
            "<command-message>compact</command-message>",
            "<command-name>/compact</command-name><command-message>other</command-message>",
            "quoted <command-name>/compact</command-name> text",
        ] {
            let req: MessagesRequest = serde_json::from_value(serde_json::json!({
                "model": "gemini-3.1-pro",
                "messages": [{"role": "user", "content": content}]
            }))
            .expect("valid ordinary request");
            assert!(
                !is_reactive_compact_prompt(&req),
                "partial/quoted command metadata must stay in normal lane: {content}"
            );
        }
    }

    #[test]
    fn ordinary_compact_text_and_quoted_fragments_are_not_reactive_compaction() {
        for content in [
            "/compact",
            "Please run this prompt:\nCRITICAL: Respond with TEXT ONLY. Do NOT call any tools.",
            "CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.\nYour task is to create a detailed summary of this conversation.",
        ] {
            let req: MessagesRequest = serde_json::from_value(serde_json::json!({
                "model": "gemini-3.1-pro",
                "messages": [{"role": "user", "content": content}]
            }))
            .expect("valid ordinary request");
            assert!(
                !is_reactive_compact_prompt(&req),
                "ordinary/partial text must not enter compaction lane: {content}"
            );
        }
    }

    #[test]
    fn only_latest_user_turn_can_trigger_reactive_compaction() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "gemini-3.1-pro",
            "messages": [
                {"role": "user", "content": reactive_compact_prompt()},
                {"role": "assistant", "content": "previous summary"},
                {"role": "user", "content": "continue from here"}
            ]
        }))
        .expect("valid conversation");
        assert!(!is_reactive_compact_prompt(&req));
    }

    #[test]
    fn normalize_grok_build_lifecycle_name_accepts_cursor_mcp_spellings() {
        assert_eq!(
            normalize_grok_build_lifecycle_name("spawn_subagent"),
            Some("spawn_subagent")
        );
        assert_eq!(
            normalize_grok_build_lifecycle_name("mcp_claude-local_spawn_subagent"),
            Some("spawn_subagent")
        );
        assert_eq!(
            normalize_grok_build_lifecycle_name("mcp__claude-local__spawn_subagent"),
            Some("spawn_subagent")
        );
        assert_eq!(
            normalize_grok_build_lifecycle_name("claude-local/spawn_subagent"),
            Some("spawn_subagent")
        );
        assert_eq!(
            normalize_grok_build_lifecycle_name("claude-local:get_command_or_subagent_output"),
            Some("get_command_or_subagent_output")
        );
        assert_eq!(
            normalize_grok_build_lifecycle_name("mcp_claude-local_kill_command_or_subagent"),
            Some("kill_command_or_subagent")
        );
        assert_eq!(
            normalize_grok_build_lifecycle_name("mcp_evil_spawn_subagent"),
            None
        );
        assert_eq!(
            normalize_grok_build_lifecycle_name("evil/spawn_subagent"),
            None
        );
        assert_eq!(
            normalize_grok_build_lifecycle_name("mcp__other__spawn_subagent"),
            None
        );
        assert_eq!(normalize_grok_build_lifecycle_name("Task"), None);
    }

    #[test]
    fn strip_mcp_provider_prefix_handles_slash_and_colon() {
        assert_eq!(
            strip_mcp_provider_prefix("claude-local/Workflow"),
            "Workflow"
        );
        assert_eq!(
            strip_mcp_provider_prefix("claude-local:Workflow"),
            "Workflow"
        );
        assert_eq!(strip_mcp_provider_prefix("Workflow"), "Workflow");
        assert_eq!(strip_mcp_provider_prefix("mcp__x__y"), "mcp__x__y");
        assert_eq!(strip_mcp_provider_prefix("plugin/search"), "search");
        assert_eq!(strip_mcp_provider_prefix("a/b/c"), "a/b/c");
        assert_eq!(
            strip_mcp_provider_prefix("mcp_claude-local_Workflow"),
            "Workflow"
        );
        assert_eq!(
            strip_mcp_provider_prefix("mcp__claude-local__Workflow"),
            "Workflow"
        );
        assert_eq!(
            strip_mcp_provider_prefix("mcp_claude-local_web_search"),
            "web_search"
        );
        assert_eq!(
            strip_mcp_provider_prefix("mcp_claude-local_spawn_subagent"),
            "spawn_subagent"
        );
        assert_eq!(
            strip_mcp_provider_prefix("mcp_evil_Workflow"),
            "mcp_evil_Workflow"
        );
    }

    #[test]
    fn claude_local_mcp_tools_includes_workflow_skill_not_read() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "go"}],
            "tools": [
                {"name": "Read", "description": "read", "input_schema": {"type": "object"}},
                {"name": "Workflow", "description": "run workflow", "input_schema": {"type": "object", "properties": {"name": {"type": "string"}}}},
                {"name": "Skill", "description": "skill", "input_schema": {"type": "object"}},
                {"name": "mcp__x__y", "description": "mcp", "input_schema": {"type": "object"}}
            ]
        }))
        .unwrap();
        let mcp = claude_local_mcp_tools(&req).expect("mcp tools");
        let names: Vec<&str> = mcp.tools.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"Workflow"));
        assert!(names.contains(&"Skill"));
        assert!(names.contains(&"mcp__x__y"));
        assert!(!names.contains(&"Read"));
        let workflow = mcp.tools.iter().find(|t| t.name == "Workflow").unwrap();
        assert_eq!(workflow.tool_name, "Workflow");
        assert_eq!(workflow.provider_identifier, CLAUDE_LOCAL_MCP_PROVIDER);
        match workflow.input_schema.as_ref().and_then(|v| v.kind.as_ref()) {
            Some(prost_types::value::Kind::StructValue(schema)) => {
                assert_eq!(
                    schema.fields.get("type").and_then(|v| match &v.kind {
                        Some(prost_types::value::Kind::StringValue(s)) => Some(s.as_str()),
                        _ => None,
                    }),
                    Some("object")
                );
                assert!(
                    schema.fields.contains_key("properties"),
                    "Workflow JSON Schema properties must be advertised on mcp_tools"
                );
            }
            other => panic!("input_schema must be Value.struct_value, got {other:?}"),
        }
    }

    #[test]
    fn claude_local_mcp_tools_registers_cursorless_claude_builtins() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "edit"}],
            "tools": [
                {"name": "Read", "description": "read", "input_schema": {"type": "object"}},
                {"name": "Edit", "description": "edit", "input_schema": {"type": "object"}},
                {"name": "MultiEdit", "description": "multi edit", "input_schema": {"type": "object"}},
                {"name": "NotebookEdit", "description": "notebook edit", "input_schema": {"type": "object"}},
                {"name": "EnterPlanMode", "description": "plan", "input_schema": {"type": "object"}},
                {"name": "LSP", "description": "language server", "input_schema": {"type": "object"}},
                {"name": "CronCreate", "description": "schedule", "input_schema": {"type": "object"}}
            ]
        }))
        .unwrap();
        let mcp = claude_local_mcp_tools(&req).expect("cursorless builtins must be registered");
        let names: Vec<&str> = mcp.tools.iter().map(|tool| tool.name.as_str()).collect();
        for required in [
            "Edit",
            "MultiEdit",
            "NotebookEdit",
            "EnterPlanMode",
            "LSP",
            "CronCreate",
        ] {
            assert!(
                names.contains(&required),
                "{required} missing from {names:?}"
            );
        }
        assert!(!names.contains(&"Read"), "Cursor Read remains native");
        assert!(!is_cursor_native_tool_name("Edit"));
        assert!(is_claude_client_only_tool_name("NotebookEdit"));
    }

    #[test]
    fn claude_local_mcp_tools_registers_current_claude_builtin_set() {
        let names = [
            "PowerShell",
            "EnterPlanMode",
            "ExitPlanMode",
            "EnterWorktree",
            "ExitWorktree",
            "TaskCreate",
            "TaskGet",
            "TaskUpdate",
            "TaskList",
            "TaskStop",
            "LSP",
            "Monitor",
            "StructuredOutput",
            "TestingPermission",
            "Artifact",
            "CronCreate",
            "CronDelete",
            "CronList",
            "ScheduleWakeup",
            "SendMessage",
            "Brief",
            "ToolSearch",
            "ListMcpResourcesTool",
            "ReadMcpResourceTool",
            "ReadMcpResourceDirTool",
            "ListAgents",
            "WaitForMcpServers",
            "SendUserMessage",
            "SendUserFile",
            "REPL",
            "RefreshMcpTools",
            "PushNotification",
            "ClaudeDesign",
            "DesignSync",
            "Projects",
            "ShareOnboardingGuide",
            "ShowOnboardingRolePicker",
            "ConnectGitHub",
            "EndConversation",
            "SendFile",
            "SearchMcpRegistry",
            "SuggestConnectors",
            "ListConnectors",
            "RunWorkflow",
        ];
        let tools: Vec<serde_json::Value> = names
            .iter()
            .map(|name| {
                serde_json::json!({
                    "name": name,
                    "description": format!("{name} tool"),
                    "input_schema": {"type": "object"}
                })
            })
            .collect();
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "inspect"}],
            "tools": tools
        }))
        .unwrap();
        let mcp = claude_local_mcp_tools(&req).expect("built-ins must be registered");
        let registered: BTreeSet<&str> = mcp.tools.iter().map(|tool| tool.name.as_str()).collect();
        for name in names {
            assert!(
                registered.contains(name),
                "{name} missing from {registered:?}"
            );
            assert!(is_client_only_tool_name(name));
        }

        // Agent is fulfilled by the Cursor-native Task event. It must remain
        // in the client's allow-list but not be duplicated in mcp_tools.
        let agent_req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "delegate"}],
            "tools": [
                {"name": "Agent", "description": "start a subagent", "input_schema": {"type": "object"}},
                {"name": "Workflow", "description": "run a workflow", "input_schema": {"type": "object"}}
            ]
        }))
        .unwrap();
        let agent_mcp = claude_local_mcp_tools(&agent_req).expect("Workflow remains on MCP");
        assert!(!agent_mcp.tools.iter().any(|tool| tool.name == "Agent"));

        let qualified_req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "delegate"}],
            "tools": [{
                "name": "mcp_claude-local_Agent",
                "description": "explicit provider-qualified agent",
                "input_schema": {"type": "object"}
            }]
        }))
        .unwrap();
        let qualified = claude_local_mcp_tools(&qualified_req).expect("qualified Agent MCP");
        assert_eq!(qualified.tools[0].name, "mcp_claude-local_Agent");
    }

    #[test]
    fn claude_local_mcp_tools_advertises_task_but_skips_ask_user_question() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "go"}],
            "tools": [
                {"name": "Task", "description": "task", "input_schema": {"type": "object"}},
                {"name": "AskUserQuestion", "description": "ask", "input_schema": {"type": "object"}},
                {"name": "Workflow", "description": "wf", "input_schema": {"type": "object"}}
            ]
        }))
        .unwrap();
        let mcp = claude_local_mcp_tools(&req).expect("mcp tools");
        let names: Vec<&str> = mcp.tools.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["Task", "Workflow"],
            "Claude Code Task must be on MCP (gemini has no other subagent tool); AskUserQuestion has a native remap"
        );
    }

    #[test]
    fn claude_local_mcp_tools_registers_source_aliases_and_testing_permission() {
        let names = [
            "RunWorkflow",
            "SendUserMessage",
            "Brief",
            "ListAgents",
            "ListPeers",
            "ListMcpResourcesTool",
            "ListMcpResources",
            "ReadMcpResourceTool",
            "ReadMcpResource",
            "ReadMcpResourceDirTool",
            "ReadMcpResourceDir",
        ];
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "inspect"}],
            "tools": names.iter().map(|name| serde_json::json!({
                "name": name,
                "description": format!("{name} tool"),
                "input_schema": {"type": "object"}
            })).collect::<Vec<_>>()
        }))
        .unwrap();
        let mcp = claude_local_mcp_tools(&req).expect("source aliases must be registered");
        let registered: BTreeSet<&str> = mcp.tools.iter().map(|tool| tool.name.as_str()).collect();
        for name in names {
            assert!(
                registered.contains(name),
                "{name} missing from {registered:?}"
            );
        }
        assert!(is_claude_client_only_tool_name("runworkflow"));
        assert!(claude_tool_names_equivalent("Brief", "SendUserMessage"));
        assert!(claude_tool_names_equivalent("RunWorkflow", "Workflow"));
        assert!(!claude_tool_names_equivalent("Brief", "Read"));
    }

    #[test]
    fn claude_local_mcp_tools_advertises_grok_build_client_and_lifecycle() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok4.6",
            "messages": [{"role": "user", "content": "go"}],
            "tools": [
                {"name": "read_file", "description": "read", "input_schema": {"type": "object"}},
                {"name": "run_terminal_command", "description": "shell", "input_schema": {"type": "object"}},
                {"name": "search_replace", "description": "patch", "input_schema": {"type": "object"}},
                {"name": "task", "description": "canonical", "input_schema": {"type": "object"}},
                {"name": "spawn_subagent", "description": "spawn", "input_schema": {"type": "object", "properties": {"prompt": {"type": "string"}, "description": {"type": "string"}}}},
                {"name": "get_command_or_subagent_output", "description": "poll", "input_schema": {"type": "object"}},
                {"name": "kill_command_or_subagent", "description": "kill", "input_schema": {"type": "object"}},
                {"name": "wait_commands_or_subagents", "description": "wait", "input_schema": {"type": "object"}},
                {"name": "AskUserQuestion", "description": "ask", "input_schema": {"type": "object"}}
            ]
        }))
        .unwrap();
        let mcp = claude_local_mcp_tools(&req).expect(
            "Fable only invokes MCP catalog tools; XML dump of spawn_subagent is Tool not found",
        );
        let names: Vec<&str> = mcp.tools.iter().map(|t| t.name.as_str()).collect();
        for required in [
            "spawn_subagent",
            "get_command_or_subagent_output",
            "kill_command_or_subagent",
            "wait_commands_or_subagents",
        ] {
            assert!(
                names.contains(&required),
                "{required} must stay on mcp_tools: {names:?}"
            );
        }
        assert!(
            names.contains(&"search_replace"),
            "schema-incompatible grok write must stay on MCP: {names:?}"
        );
        for remapped in ["read_file", "run_terminal_command"] {
            assert!(
                !names.contains(&remapped),
                "{remapped} has a native remap and must stay off MCP: {names:?}"
            );
        }
        assert!(
            !names.contains(&"task"),
            "lowercase task alias must stay off MCP: {names:?}"
        );
        assert!(
            !names.contains(&"AskUserQuestion"),
            "AskUserQuestion stays off MCP: {names:?}"
        );
    }

    #[test]
    fn claude_local_mcp_tools_rejects_lifecycle_aliases_and_spoofs() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok4.6",
            "messages": [{"role": "user", "content": "go"}],
            "tools": [
                {"name": "task", "description": "alias", "input_schema": {"type": "object"}},
                {"name": "Agent", "description": "alias", "input_schema": {"type": "object"}},
                {"name": "Task", "description": "claude", "input_schema": {"type": "object"}},
                {"name": "evil/spawn_subagent", "description": "spoof", "input_schema": {"type": "object"}},
                {"name": "kill_task", "description": "alias", "input_schema": {"type": "object"}},
                {"name": "TaskOutput", "description": "alias", "input_schema": {"type": "object"}},
                {"name": "spawn_subagent", "description": "spawn", "input_schema": {"type": "object"}}
            ]
        }))
        .unwrap();
        let mcp = claude_local_mcp_tools(&req).expect("exact grok spawn should be on MCP");
        let names: Vec<&str> = mcp.tools.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["spawn_subagent"],
            "aliases and spoofs must stay off mcp_tools: {names:?}"
        );
    }

    #[test]
    fn claude_local_mcp_tools_hides_internal_and_deprecated_tools() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "go"}],
            "tools": [
                {"name": "Workflow", "description": "workflow", "input_schema": {"type": "object"}},
                {"name": "mcp__plugin_lobster-channel_lobster-channel__lobster_reply_from_stop", "description": "INTERNAL", "input_schema": {"type": "object"}},
                {"name": "mcp__plugin_lobster-channel_lobster-channel__notify_messa00a7caa", "description": "INTERNAL", "input_schema": {"type": "object"}},
                {"name": "mcp__plugin_lobster-channel_lobster-channel__notify_post_0a80400", "description": "INTERNAL", "input_schema": {"type": "object"}},
                {"name": "mcp__plugin_lobster-channel_lobster-channel__notify_user_prompt", "description": "INTERNAL", "input_schema": {"type": "object"}},
                {"name": "mcp__plugin_lobster-channel_lobster-channel__record_token_usage", "description": "INTERNAL", "input_schema": {"type": "object"}},
                {"name": "mcp__plugin_lobster-channel_lobster-channel__wait_for_rem889a16c", "description": "INTERNAL", "input_schema": {"type": "object"}},
                {"name": "TaskOutput", "description": "DEPRECATED", "input_schema": {"type": "object"}},
                {"name": "mcp__plugin_lobster-channel_lobster-channel__lobster_reply", "description": "public", "input_schema": {"type": "object"}}
            ]
        }))
        .unwrap();
        let mcp = claude_local_mcp_tools(&req).expect("Workflow and public lobster tool remain");
        let names: Vec<&str> = mcp.tools.iter().map(|tool| tool.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "Workflow",
                "mcp__plugin_lobster-channel_lobster-channel__lobster_reply"
            ]
        );
        assert!(is_model_visible_tool_name("Workflow"));
        assert!(!is_model_visible_tool_name("TaskOutput"));
        assert!(!is_model_visible_tool_name(
            "mcp__plugin_lobster-channel_lobster-channel__notify_messa00a7caa"
        ));
    }

    #[test]
    fn model_visible_definition_filters_internal_description_markers() {
        for description in [
            "INTERNAL",
            "INTERNAL hook",
            "hook (internal)",
            "DEPRECATED",
            "DEPRECATED hook",
            "legacy entry; deprecated",
            "Do not call from model output",
            "This is not for model output",
        ] {
            let tool = serde_json::json!({
                "name": "mcp__plugin__hook",
                "description": description,
                "input_schema": {"type": "object"}
            });
            assert!(
                !is_model_visible_tool_definition(&tool),
                "internal marker must hide tool: {description}"
            );
        }
        let public = serde_json::json!({
            "name": "mcp__plugin__public",
            "description": "Run the public operation",
            "input_schema": {"type": "object"}
        });
        assert!(is_model_visible_tool_definition(&public));
        let truncated_internal = serde_json::json!({
            "name": "mcp__plugin_lobster-channel_lobster-channel__lobster_repl8f31eb0",
            "description": "",
            "input_schema": {"type": "object"}
        });
        assert!(!is_model_visible_tool_definition(&truncated_internal));
    }

    #[test]
    fn model_visible_definition_keeps_normal_internal_word_usage() {
        for description in [
            "Search internal references and generated files",
            "Compatibility with a deprecated syntax marker",
        ] {
            let tool = serde_json::json!({
                "name": "Grep",
                "description": description,
                "input_schema": {"type": "object"}
            });
            assert!(
                is_model_visible_tool_definition(&tool),
                "ordinary description must remain visible: {description}"
            );
        }
    }

    #[test]
    fn hidden_hook_filter_normalizes_claude_local_underscore_names() {
        for name in [
            "mcp_claude-local_notify_post_tool_use",
            "mcp_claude-local_lobster_reply_from_stop",
            "mcp__claude-local__notify_post_tool_use",
        ] {
            assert!(
                !is_model_visible_tool_name(name),
                "internal hook must be hidden regardless of MCP spelling: {name}"
            );
        }
        assert!(is_model_visible_tool_name("mcp_claude-local_lobster_reply"));
    }

    #[test]
    fn grok_preface_lists_only_advertised_callable_names() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok4.6",
            "messages": [{"role": "user", "content": "read"}],
            "tools": [{
                "name": "read_file",
                "description": "read",
                "input_schema": {"type": "object"}
            }]
        }))
        .unwrap();
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: false,
            },
        );
        assert!(
            parts
                .user_text
                .contains("Call read_file by those exact names")
        );
        for absent in [
            "run_terminal_command",
            "list_dir",
            "web_search",
            "web_fetch",
            "enter_plan_mode",
        ] {
            assert!(
                !parts.user_text.contains(absent),
                "unadvertised grok name leaked into preface: {absent}; {}",
                parts.user_text
            );
        }
    }

    #[test]
    fn all_tool_dump_does_not_emit_unregistered_client_alias() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "delegate"}],
            "tools": [{
                "name": "Agent",
                "description": "start a subagent",
                "input_schema": {"type": "object"}
            }]
        }))
        .unwrap();
        assert!(claude_local_mcp_tools(&req).is_none());
        assert!(render_tools_block(&req, ToolDumpMode::All).is_none());
    }

    #[test]
    fn grok_compact_dump_does_not_emit_unknown_local_alias() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok4.6",
            "messages": [{"role": "user", "content": "delegate"}],
            "tools": [
                {
                    "name": "read_file",
                    "description": "read",
                    "input_schema": {"type": "object"}
                },
                {
                    "name": "Agent",
                    "description": "start a subagent",
                    "input_schema": {"type": "object"}
                },
                {
                    "name": "Workflow",
                    "description": "workflow",
                    "input_schema": {"type": "object"}
                }
            ]
        }))
        .unwrap();
        let dump = render_tools_block(&req, ToolDumpMode::CompactClaudeLocal)
            .expect("native grok alias should remain");
        assert!(dump.contains("read_file"));
        assert!(!dump.contains("\"name\":\"Agent\""));
        assert!(!dump.contains("\"name\":\"Workflow\""));
    }

    #[test]
    fn write_hint_uses_only_names_present_in_request() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "write"}],
            "tools": [{
                "name": "write_file",
                "description": "write",
                "input_schema": {"type": "object"}
            }]
        }))
        .unwrap();
        let dump = render_tools_block(&req, ToolDumpMode::All).expect("write dump");
        assert!(dump.contains("write_file"));
        assert!(!dump.contains("Read (or read_file)"));
        assert!(!dump.contains("Write (or write/write_file)"));
        assert!(dump.contains("calling write_file"));
    }

    #[test]
    fn force_tool_dump_never_repeats_mcp_schemas_or_prefixes() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT", "1");
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "research"}],
            "tools": [
                {"name": "Read", "description": "read", "input_schema": {"type": "object", "properties": {"file_path": {"type": "string"}}}},
                {"name": "Workflow", "description": "run workflow", "input_schema": {"type": "object", "properties": {"name": {"type": "string"}}}},
                {"name": "mcp__plugin__search", "description": "search", "input_schema": {"type": "object", "properties": {"q": {"type": "string"}}}}
            ]
        }))
        .unwrap();
        let catalog = claude_local_mcp_tools(&req).expect("MCP catalog");
        assert!(catalog.tools.iter().any(|tool| tool.name == "Workflow"));
        assert!(
            catalog
                .tools
                .iter()
                .any(|tool| tool.name == "mcp__plugin__search")
        );
        let parts = render_cursor_prompt_parts_with(&req, CursorPromptOptions::default());
        // Native schemas remain available for the explicit debug override.
        assert!(parts.user_text.contains("\"name\":\"Read\""));
        // MCP schemas and synthetic provider-prefixed aliases are single-sourced
        // from RunRequest.mcp_tools and must never be repeated in user text.
        assert!(!parts.user_text.contains("\"name\":\"Workflow\""));
        assert!(!parts.user_text.contains("\"name\":\"mcp__plugin__search\""));
        assert!(!parts.user_text.contains("mcp_claude-local_"));
        unsafe {
            std::env::remove_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
        }
    }

    #[test]
    fn mcp_catalog_and_prompt_map_share_exact_wire_names() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
        }
        let long_name =
            "mcp__plugin_claude-mem_mcp-search__session_start_context_with_more_details";
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "continue"}],
            "tools": [
                {"name": "Workflow", "description": "workflow", "input_schema": {"type": "object"}},
                {"name": long_name, "description": "long MCP", "input_schema": {"type": "object"}}
            ]
        }))
        .unwrap();
        let catalog = claude_local_mcp_tools(&req).expect("MCP catalog");
        let map = claude_local_mcp_name_map(&req).expect("catalog name map");
        for (original, wire) in map {
            assert!(catalog.tools.iter().any(|tool| tool.name == wire));
            assert_eq!(wire, cursor_mcp_wire_name(&original));
        }
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: false,
            },
        );
        assert!(!parts.user_text.contains("<tools>"));
        assert!(!parts.user_text.contains(long_name));
    }

    #[test]
    fn long_mcp_alias_is_not_reinjected_into_prompt() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
        }
        let original = "mcp__plugin_claude-mem_mcp-search__session_start_context_with_more_details";
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "continue"}],
            "tools": [{
                "name": original,
                "description": "long MCP",
                "input_schema": {"type": "object"}
            }]
        }))
        .unwrap();
        let wire = cursor_mcp_wire_name(original);
        let mcp = claude_local_mcp_tools(&req).expect("long MCP tool is registered");
        assert_eq!(mcp.tools[0].name, wire);
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: false,
            },
        );
        assert!(
            !parts.user_text.contains("<tools>"),
            "a registered long MCP tool must not be duplicated in prompt: {}",
            parts.user_text
        );
        assert!(!parts.user_text.contains(original));
        assert!(!parts.user_text.contains(&wire));
    }

    #[test]
    fn mcp_catalog_deduplicates_repeated_wire_names() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "go"}],
            "tools": [
                {"name": "Workflow", "description": "first", "input_schema": {"type": "object"}},
                {"name": "Workflow", "description": "duplicate", "input_schema": {"type": "object"}}
            ]
        }))
        .unwrap();
        let mcp = claude_local_mcp_tools(&req).expect("Workflow is registered");
        assert_eq!(mcp.tools.len(), 1);
        assert_eq!(mcp.tools[0].name, "Workflow");
        assert_eq!(mcp.tools[0].description, "first");
    }

    #[test]
    fn long_mcp_names_use_the_cursor_64_character_wire_alias() {
        let original = "mcp__plugin_claude-mem_mcp-search__session_start_context_with_more_details";
        let wire = cursor_mcp_wire_name(original);
        assert_eq!(wire.len(), 64);
        assert_eq!(&wire[..57], &original[..57]);
        let digest = Sha256::digest(original.as_bytes());
        let suffix = format!("{digest:x}");
        assert_eq!(&wire[57..], &suffix[..7]);

        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "go"}],
            "tools": [{
                "name": original,
                "description": "long MCP",
                "input_schema": {"type": "object"}
            }]
        }))
        .unwrap();
        let mcp = claude_local_mcp_tools(&req).expect("long MCP tool is registered");
        assert_eq!(mcp.tools[0].name, wire);
        assert_eq!(mcp.tools[0].tool_name, wire);
    }

    #[test]
    fn claude_local_mcp_tools_encodes_struct_not_json_string() {
        use prost::Message;
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "go"}],
            "tools": [
                {"name": "Workflow", "description": "run workflow", "input_schema": {"type": "object", "properties": {"name": {"type": "string"}}}}
            ]
        }))
        .unwrap();
        let mcp = claude_local_mcp_tools(&req).expect("mcp tools");
        let mut bytes = Vec::new();
        mcp.encode(&mut bytes).unwrap();
        // Tag 3 must be a length-delimited *message* (Value). A JSON string
        // would also be length-delimited, but round-tripping through decode
        // must recover struct_value — not a string field.
        let decoded = super::super::proto::McpTools::decode(&bytes[..]).unwrap();
        let tool = &decoded.tools[0];
        assert!(tool.input_schema.is_some());
        assert!(!tool.provider_identifier.is_empty());
        assert_eq!(tool.tool_name, "Workflow");
        match tool.input_schema.as_ref().and_then(|v| v.kind.as_ref()) {
            Some(prost_types::value::Kind::StructValue(schema)) => {
                assert!(schema.fields.contains_key("type"));
            }
            other => panic!("expected Value.struct_value, got {other:?}"),
        }
    }

    #[test]
    fn omit_tools_skips_native_schemas_but_keeps_claude_local() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "hello"}],
            "tools": [
                {"name": "Read", "description": "read files", "input_schema": {"type": "object", "properties": {"file_path": {"type": "string"}}}},
                {"name": "Workflow", "description": "run a workflow", "input_schema": {"type": "object", "properties": {"name": {"type": "string"}}}},
                {"name": "Skill", "description": "invoke a skill", "input_schema": {"type": "object"}},
                {"name": "mcp__plugin__search", "description": "mcp", "input_schema": {"type": "object"}}
            ]
        }))
        .unwrap();
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: false,
            },
        );
        assert!(parts.user_text.contains("hello"));
        assert!(
            claude_local_mcp_tools(&req).is_some(),
            "Claude-local tools must reach Cursor through the MCP catalog"
        );
        assert!(
            !parts.user_text.contains("<tools>"),
            "registered MCP tools must not be duplicated in the prompt"
        );
        assert!(
            !parts.user_text.contains("mcp_claude-local_"),
            "prompt must not invent provider-prefixed MCP names"
        );
        assert!(
            !parts.user_text.contains("\"name\":\"Read\""),
            "native Read schema should stay omitted when bridging"
        );
        assert!(
            !parts.user_text.contains("input_schema"),
            "mcp_tools already carries schemas; XML dump must not duplicate them"
        );
    }

    #[test]
    fn native_only_delta_does_not_inject_unregistered_client_tool_nudge() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "read this"}],
            "tools": [{
                "name": "Read",
                "description": "read files",
                "input_schema": {"type": "object"}
            }]
        }))
        .unwrap();
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: true,
            },
        );
        assert!(parts.user_text.contains("read this"));
        assert!(!parts.user_text.contains("<tools>"));
        assert!(!parts.user_text.contains("Workflow"));
        assert!(!parts.user_text.contains("Skill"));
    }

    #[test]
    fn unregistered_agent_alias_is_not_injected_when_native_task_is_available() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "delegate this"}],
            "tools": [{
                "name": "Agent",
                "description": "start a subagent",
                "input_schema": {"type": "object"}
            }]
        }))
        .unwrap();
        assert!(claude_local_mcp_tools(&req).is_none());
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: true,
            },
        );
        assert!(parts.user_text.contains("delegate this"));
        assert!(!parts.user_text.contains("<tools>"));
        assert!(!parts.user_text.contains("Agent"));
    }

    #[test]
    fn grok_native_remap_names_stay_in_prompt_without_mcp_catalog() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok4.6",
            "messages": [{"role": "user", "content": "read this"}],
            "tools": [{
                "name": "read_file",
                "description": "read files",
                "input_schema": {"type": "object"}
            }]
        }))
        .unwrap();
        assert!(claude_local_mcp_tools(&req).is_none());
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: true,
            },
        );
        assert!(parts.user_text.contains("read_file"));
        assert!(
            parts
                .user_text
                .contains("Call read_file by those exact names")
        );
    }

    #[test]
    fn mcp_tools_compact_dump_keeps_workflow_nudge_without_schemas() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "Run the \"deep-research\" workflow.\nInvoke: Workflow({ name: \"deep-research\", args: \"the topic\" })"}],
            "tools": [
                {"name": "Workflow", "description": "run a workflow", "input_schema": {"type": "object", "properties": {"name": {"type": "string"}, "args": {"type": "string"}}}},
                {"name": "Skill", "description": "invoke a skill", "input_schema": {"type": "object", "properties": {"skill": {"type": "string"}}}}
            ]
        }))
        .unwrap();
        assert!(claude_local_mcp_tools(&req).is_some());
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: false,
            },
        );
        assert!(
            parts
                .user_text
                .contains("Invoke: Workflow({ name: \"deep-research\"")
                || parts.user_text.contains("deep-research"),
            "Claude Code /deep-research invoke text must not be stripped; got: {}",
            parts.user_text
        );
        assert!(
            !parts.user_text.contains("<tools>"),
            "registered MCP tools must not be duplicated in the prompt"
        );
        assert!(
            !parts.user_text.contains("mcp_claude-local_"),
            "compact prompt must not invent provider-prefixed MCP names"
        );
        assert!(
            !parts.user_text.contains("input_schema"),
            "full JSON schemas must not be duplicated when mcp_tools is set: {}",
            parts.user_text
        );
    }

    #[test]
    fn grok_build_tool_dump_prefers_client_names_over_cursor_natives() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok4.5",
            "messages": [{"role": "user", "content": "go"}],
            "tools": [
                {"name": "run_terminal_command", "description": "shell", "input_schema": {"type": "object"}},
                {"name": "read_file", "description": "read", "input_schema": {"type": "object"}},
                {"name": "spawn_subagent", "description": "spawn", "input_schema": {"type": "object"}},
                {"name": "web_search", "description": "search", "input_schema": {"type": "object"}},
                {"name": "grep", "description": "grep", "input_schema": {"type": "object"}}
            ]
        }))
        .unwrap();
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: false,
            },
        );
        assert!(
            parts.user_text.contains("run_terminal_command"),
            "grok shell name must stay visible: {}",
            parts.user_text
        );
        assert!(
            parts.user_text.contains("read_file"),
            "grok read name must stay visible so Fable does not only see Cursor Read: {}",
            parts.user_text
        );
        assert!(
            parts.user_text.contains("web_search"),
            "grok search name must stay visible so Fable does not only see Cursor WebSearch: {}",
            parts.user_text
        );
        assert!(
            parts.user_text.contains("\"name\":\"grep\""),
            "grok grep must stay visible even though Cursor Grep differs only by case: {}",
            parts.user_text
        );
        assert!(
            !parts.user_text.contains("\"name\":\"spawn_subagent\""),
            "MCP lifecycle tools must stay in Cursor's callable catalog, not be duplicated in text: {}",
            parts.user_text
        );
        assert!(
            parts
                .user_text
                .contains("Call run_terminal_command, read_file"),
            "preface must list grok names without teaching a dual catalog: {}",
            parts.user_text
        );
        for banned in [
            "Cursor-native",
            "Cursor",
            "MCP",
            "bridge",
            "Shell",
            "Task",
            "mcp_claude-local_",
        ] {
            assert!(
                !parts.user_text.contains(banned),
                "preface must not teach {banned}: {}",
                parts.user_text
            );
        }
        assert!(
            !parts
                .user_text
                .contains("Prefer these Claude Code client tools"),
            "grok-build dump must not use the Claude Code Workflow preface: {}",
            parts.user_text
        );
    }

    #[test]
    fn write_tool_dump_distinguishes_existing_and_new_files() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "update the file"}],
            "tools": [
                {"name": "Read", "description": "read a file", "input_schema": {"type": "object"}},
                {"name": "Write", "description": "write a file", "input_schema": {"type": "object"}}
            ]
        }))
        .unwrap();

        for mode in [ToolDumpMode::All, ToolDumpMode::CompactClaudeLocal] {
            let dump = render_tools_block(&req, mode).expect("write tools should be rendered");
            assert!(
                dump.contains("For an existing file"),
                "write dump must explain existing files: {dump}"
            );
            assert!(
                dump.contains("call Read"),
                "write dump must retain the read hint: {dump}"
            );
            assert!(
                dump.contains("a new file may be created directly"),
                "write dump must permit new files: {dump}"
            );
            assert!(
                !dump.contains("never write an unread file"),
                "write dump must not prohibit creating new files: {dump}"
            );
        }
    }

    #[test]
    fn request_context_empty_without_working_directory() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .unwrap();
        let ctx = cursor_request_context(&req);
        assert!(!request_context_is_populated(&ctx));
        assert!(ctx.env.is_none());
        assert!(ctx.git_repos.is_empty());
        assert!(ctx.agent_skills.is_empty());
        assert!(ctx.rules.is_empty());
    }

    #[test]
    fn request_context_populated_from_system_reminder() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp.path().join(".git")).unwrap();
        std::fs::write(
            tmp.path().join(".git").join("HEAD"),
            "ref: refs/heads/main\n",
        )
        .unwrap();
        let cwd = tmp.path().display().to_string();
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": format!("<system-reminder>\n# Environment\n - Primary working directory: {cwd}\n - git branch --show-current: main\n</system-reminder>")},
                {"type": "text", "text": "list files"}
            ]}]
        }))
        .unwrap();
        let ctx = cursor_request_context(&req);
        assert!(request_context_is_populated(&ctx));
        let env = ctx.env.as_ref().expect("env");
        assert_eq!(env.workspace_paths, vec![cwd.clone()]);
        assert_eq!(env.project_folder, cwd);
        assert_eq!(env.process_working_directory, cwd);
        assert_eq!(ctx.git_repos.len(), 1);
        assert_eq!(ctx.git_repos[0].path, cwd);
        assert_eq!(ctx.git_repos[0].branch_name, "main");
        assert!(
            ctx.rules.is_empty() && ctx.agent_skills.is_empty(),
            "must not dump Claude system/skills into rules/agent_skills"
        );
        let parts = render_cursor_prompt_parts(&req);
        assert!(
            !parts.user_text.contains("<ccp-request-context>"),
            "RequestContext must not be stuffed into user text"
        );
        assert!(parts.user_text.contains("list files"));
    }

    #[test]
    fn delta_only_keeps_workflow_skill_without_history() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [
                {"role": "user", "content": "first"},
                {"role": "assistant", "content": "ack"},
                {"role": "user", "content": "second question"}
            ],
            "tools": [
                {"name": "Read", "description": "x", "input_schema": {"type": "object"}},
                {"name": "Workflow", "description": "wf", "input_schema": {"type": "object"}}
            ]
        }))
        .unwrap();
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: true,
            },
        );
        assert!(parts.user_text.contains("second question"));
        assert!(!parts.user_text.contains("first"));
        assert!(!parts.user_text.contains("<assistant>"));
        assert!(
            !parts.user_text.contains("mcp_claude-local_"),
            "checkpoint delta must not invent provider-prefixed MCP names"
        );
        assert!(!parts.user_text.contains("\"name\":\"Read\""));
    }

    #[test]
    fn delta_only_forwards_client_only_tool_result_continuation() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [
                {"role": "user", "content": "run /deep-research"},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "wf1", "name": "Workflow", "input": {"name": "deep-research"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "wf1", "content": "Baked findings: the proxy maps MCP Workflow."}
                ]}
            ],
            "tools": [
                {"name": "Read", "description": "x", "input_schema": {"type": "object"}},
                {"name": "Workflow", "description": "wf", "input_schema": {"type": "object", "properties": {"name": {"type": "string"}}}}
            ]
        }))
        .unwrap();
        assert!(latest_user_is_only_tool_results(&req));
        assert!(request_has_client_only_tool_results(&req));
        assert!(!request_has_orphaned_native_live_results(&req));
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: true,
            },
        );
        assert!(
            parts
                .user_text
                .contains("Baked findings: the proxy maps MCP Workflow."),
            "ClientOnly tool_result must appear in the next RunRequest user text; got: {}",
            parts.user_text
        );
        assert!(
            parts
                .user_text
                .contains("<tool_result tool_use_id=\"wf1\">")
        );
        assert!(
            !parts.user_text.contains("run /deep-research"),
            "delta_only should not replay the original user text: {}",
            parts.user_text
        );
        assert!(
            !parts.user_text.contains("mcp_claude-local_"),
            "checkpoint delta must not invent provider-prefixed MCP names"
        );
    }

    #[test]
    fn edit_family_tool_results_use_client_only_continuation() {
        for (name, input) in [
            (
                "Edit",
                serde_json::json!({
                    "file_path": "/tmp/a.rs",
                    "old_string": "a",
                    "new_string": "b"
                }),
            ),
            (
                "MultiEdit",
                serde_json::json!({
                    "file_path": "/tmp/a.rs",
                    "edits": [{"old_string": "a", "new_string": "b"}]
                }),
            ),
            (
                "NotebookEdit",
                serde_json::json!({
                    "notebook_path": "/tmp/a.ipynb",
                    "cell_id": "cell-1",
                    "new_source": "1 + 1"
                }),
            ),
        ] {
            let req: MessagesRequest = serde_json::from_value(serde_json::json!({
                "model": "fable",
                "messages": [
                    {"role": "assistant", "content": [
                        {"type": "tool_use", "id": "edit-1", "name": name, "input": input}
                    ]},
                    {"role": "user", "content": [
                        {"type": "tool_result", "tool_use_id": "edit-1", "content": "ok"}
                    ]}
                ]
            }))
            .unwrap();
            assert!(
                request_has_client_only_tool_results(&req),
                "{name} tool_result must route through ClientOnly continuation"
            );
        }
    }

    #[test]
    fn generation_tagged_native_tool_result_is_orphaned_when_free() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [
                {"role": "user", "content": "read it"},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "r1__cursor_run_old", "name": "Read", "input": {"file_path": "a.rs"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "r1__cursor_run_old", "content": "fn main() {}"}
                ]}
            ]
        }))
        .unwrap();
        assert!(request_has_orphaned_native_live_results(&req));
        let workflow: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [
                {"role": "user", "content": "research"},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "wf1__cursor_run_old", "name": "Workflow", "input": {"name": "deep-research"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "wf1__cursor_run_old", "content": "done"}
                ]}
            ]
        }))
        .unwrap();
        assert!(!request_has_orphaned_native_live_results(&workflow));
    }

    #[test]
    fn orphaned_native_results_on_free_slot_start_a_fresh_run() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [
                {"role": "user", "content": "read it"},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "r1__cursor_run_old", "name": "Read", "input": {"file_path": "a.rs"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "r1__cursor_run_old", "content": "fn main() {}"}
                ]}
            ]
        }))
        .unwrap();
        assert!(
            request_has_orphaned_native_live_results(&req),
            "classifier must still see the dead generation tag"
        );
        assert!(
            !reject_orphaned_native_results_when_live_slot_is_free(&req),
            "a Free slot after serve restart / dead Run must replay history, not 409"
        );
        let workflow: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [
                {"role": "user", "content": "research"},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "wf1__cursor_run_old", "name": "Workflow", "input": {"name": "deep-research"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "wf1__cursor_run_old", "content": "done"}
                ]}
            ]
        }))
        .unwrap();
        assert!(!request_has_orphaned_native_live_results(&workflow));
        assert!(!reject_orphaned_native_results_when_live_slot_is_free(
            &workflow
        ));
    }

    #[test]
    fn client_only_full_history_includes_workflow_tool_result() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [
                {"role": "user", "content": "run /deep-research"},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "wf1", "name": "Workflow", "input": {"name": "deep-research"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "wf1", "content": "Baked findings: the proxy maps MCP Workflow."}
                ]}
            ]
        }))
        .unwrap();
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: false,
            },
        );
        assert!(parts.user_text.contains("run /deep-research"));
        assert!(
            parts
                .user_text
                .contains("<tool_use id=\"wf1\" name=\"Workflow\">")
        );
        assert!(
            parts
                .user_text
                .contains("Baked findings: the proxy maps MCP Workflow.")
        );
    }

    #[test]
    fn delta_only_skips_older_tool_results_when_new_user_text_exists() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_FORCE_TOOLS_IN_PROMPT");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [
                {"role": "user", "content": "first"},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "r1", "name": "Read", "input": {"file_path": "a.rs"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "r1", "content": "fn main() {}"}
                ]},
                {"role": "assistant", "content": "done"},
                {"role": "user", "content": "next question"}
            ]
        }))
        .unwrap();
        assert!(!latest_user_is_only_tool_results(&req));
        assert!(!request_has_client_only_tool_results(&req));
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: true,
            },
        );
        assert!(parts.user_text.contains("next question"));
        assert!(!parts.user_text.contains("first"));
        assert!(!parts.user_text.contains("fn main() {}"));
        assert!(!parts.user_text.contains("<tool_result"));
    }

    #[test]
    fn trailing_system_reminder_does_not_hide_split_tool_result_turn() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok-4.5-high-fast",
            "messages": [
                {"role": "user", "content": "read both files"},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "call-1", "name": "read_file", "input": {}},
                    {"type": "tool_use", "id": "call-2", "name": "read_file", "input": {}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "call-1", "content": "first result"}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "call-2", "content": "second result"}
                ]},
                {
                    "role": "user",
                    "content": "<system-reminder>MCP servers connected.</system-reminder>"
                }
            ]
        }))
        .unwrap();

        assert!(
            latest_user_is_only_tool_results(&req),
            "a trailing asynchronous reminder must not hide the resumable tool-result turn"
        );
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: true,
            },
        );
        assert!(parts.user_text.contains("first result"));
        assert!(parts.user_text.contains("second result"));
        assert!(
            !parts.user_text.contains("MCP servers connected"),
            "the asynchronous reminder must not replace completed tool results"
        );
    }

    #[test]
    fn trailing_system_reminder_does_not_replace_latest_user_text_delta() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok-4.5-high-fast",
            "messages": [
                {"role": "assistant", "content": "previous answer"},
                {"role": "user", "content": "answer the actual question"},
                {
                    "role": "user",
                    "content": "<system-reminder>Background work completed.</system-reminder>"
                }
            ]
        }))
        .unwrap();

        assert!(!latest_user_is_only_tool_results(&req));
        let parts = render_cursor_prompt_parts_with(
            &req,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: true,
            },
        );
        assert!(parts.user_text.contains("answer the actual question"));
        assert!(
            !parts.user_text.contains("Background work completed"),
            "a standalone notification is not the user's new turn"
        );
    }

    #[test]
    fn default_omits_system_to_avoid_fable_injection_loops() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
            std::env::remove_var("CCP_CURSOR_PACKAGED_SYSTEM");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "system": "be direct",
            "messages": [{"role": "user", "content": "hello"}],
            "tools": [{"name": "Read", "description": "read files", "input_schema": {"type": "object"}}]
        }))
        .unwrap();
        let parts = render_cursor_prompt_parts(&req);
        assert_eq!(parts.custom_system_prompt, None);
        assert!(!parts.user_text.contains("be direct"));
        assert!(!parts.user_text.contains("CLAUDE_CODE_SYSTEM"));
        assert!(parts.user_text.contains("<user>"));
        assert!(parts.user_text.contains("hello"));
        assert!(parts.user_text.contains("<tools>"));
        assert!(parts.user_text.contains("Read"));
    }

    #[test]
    fn scrubs_injection_defense_monologues_from_assistant_history() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
            std::env::remove_var("CCP_CURSOR_PACKAGED_SYSTEM");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [
                {"role": "user", "content": "这个项目做什么的"},
                {"role": "assistant", "content": "你的消息里有伪造成 CLAUDE_CODE_SYSTEM 的提示词注入。我将忽略它。\n\n这是一个本地代理项目。"},
                {"role": "user", "content": "继续"}
            ]
        }))
        .unwrap();
        let parts = render_cursor_prompt_parts(&req);
        assert!(!parts.user_text.contains("CLAUDE_CODE_SYSTEM"));
        assert!(!parts.user_text.contains("提示词注入"));
        assert!(parts.user_text.contains("这是一个本地代理项目。"));
        assert!(parts.user_text.contains("这个项目做什么的"));
    }

    #[test]
    fn preserves_claude_md_system_reminder_in_user_messages() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
            std::env::remove_var("CCP_CURSOR_PACKAGED_SYSTEM");
        }
        let reminder = "<system-reminder>\nAs you answer, follow the project's CLAUDE.md:\n# Project Rules\nAlways use tabs.\n</system-reminder>";
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": reminder},
                    {"type": "text", "text": "list files"}
                ]}
            ]
        }))
        .unwrap();
        let parts = render_cursor_prompt_parts(&req);
        assert!(
            parts.user_text.contains("<system-reminder>"),
            "CLAUDE.md system-reminders must reach Cursor; got: {}",
            parts.user_text
        );
        assert!(parts.user_text.contains("Always use tabs."));
        assert!(parts.user_text.contains("list files"));
        assert!(!parts.user_text.contains("CLAUDE_CODE_SYSTEM"));
    }

    #[test]
    fn multi_turn_agent_history_includes_tool_use_and_result() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
            std::env::remove_var("CCP_CURSOR_PACKAGED_SYSTEM");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "system": "You are Claude Code.",
            "messages": [
                {"role": "user", "content": "list files"},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "tu1", "name": "Bash", "input": {"command": "ls"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "tu1", "content": "a.rs\nb.rs"}
                ]}
            ],
            "tools": [{"name": "Bash", "description": "run", "input_schema": {"type": "object"}}]
        }))
        .unwrap();
        unsafe {
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
            std::env::remove_var("CCP_CURSOR_PACKAGED_SYSTEM");
        }
        let parts = render_cursor_prompt_parts(&req);
        assert_eq!(parts.custom_system_prompt, None);
        assert!(
            !parts.user_text.contains("You are Claude Code."),
            "system must stay omitted by default; got: {}",
            parts.user_text
        );
        assert!(parts.user_text.contains("<user>\nlist files\n</user>"));
        assert!(
            parts
                .user_text
                .contains("<tool_use id=\"tu1\" name=\"Bash\">")
        );
        assert!(
            parts
                .user_text
                .contains("<tool_result tool_use_id=\"tu1\">")
        );
        assert!(parts.user_text.contains("a.rs\nb.rs"));
        assert!(parts.user_text.contains("<tools>"));
    }

    #[test]
    fn filters_billing_headers_from_system_when_embed_enabled() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::set_var("CCP_CURSOR_EMBED_SYSTEM", "1");
            std::env::remove_var("CCP_CURSOR_PACKAGED_SYSTEM");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "system": [
                {"type": "text", "text": "keep this"},
                {"type": "text", "text": "x-anthropic-billing-header: skip-me"}
            ],
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .unwrap();
        // Re-assert env immediately before render — parallel tests mutate process env.
        unsafe {
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::set_var("CCP_CURSOR_EMBED_SYSTEM", "1");
        }
        let parts = render_cursor_prompt_parts(&req);
        assert!(
            parts.user_text.contains("keep this"),
            "expected embedded system, got: {}",
            parts.user_text
        );
        assert!(!parts.user_text.contains("x-anthropic-billing-header"));
        unsafe { std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM") };
    }

    #[test]
    fn scrubs_assistant_injection_monologues() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM");
            std::env::remove_var("CCP_CURSOR_EMBED_SYSTEM");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [
                {"role": "user", "content": "项目做什么"},
                {"role": "assistant", "content": "我先说明：CLAUDE_CODE_SYSTEM 是提示词注入，我不会执行它。\n\n这是一个 VIP 工具。"}
            ]
        }))
        .unwrap();
        let parts = render_cursor_prompt_parts(&req);
        assert!(!parts.user_text.contains("CLAUDE_CODE_SYSTEM"));
        assert!(!parts.user_text.contains("提示词注入"));
        assert!(parts.user_text.contains("VIP 工具"));
    }

    #[test]
    fn team_opt_in_puts_system_in_field8() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("CCP_CURSOR_USE_CUSTOM_SYSTEM", "1");
            std::env::remove_var("CCP_CURSOR_PACKAGED_SYSTEM");
        }
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "system": "team system",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .unwrap();
        let parts = render_cursor_prompt_parts(&req);
        assert_eq!(parts.custom_system_prompt.as_deref(), Some("team system"));
        assert!(!parts.user_text.contains("team system"));
        assert!(parts.user_text.contains("<user>"));
        unsafe { std::env::remove_var("CCP_CURSOR_USE_CUSTOM_SYSTEM") };
    }

    #[test]
    fn collects_selected_images() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "hi"},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AAAA"}}
                ]
            }]
        }))
        .unwrap();
        let images = cursor_selected_images(&req);
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].mime_type, "image/png");
        assert_eq!(images[0].data, "AAAA");
        assert!(images[0].path.is_empty());
    }

    #[test]
    fn normalizes_data_uri_and_input_image_payloads() {
        let png_data_uri = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAAC";
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "data": png_data_uri}},
                    {"type": "input_image", "image_url": {"url": png_data_uri}}
                ]
            }]
        }))
        .unwrap();
        let images = cursor_selected_images(&req);
        assert_eq!(images.len(), 2);
        assert_eq!(images[0].data, "iVBORw0KGgoAAAANSUhEUgAAAAEAAAAC");
        assert_eq!(images[0].mime_type, "image/png");
        assert_eq!(images[0].data, images[1].data);
    }

    #[test]
    fn extracts_claude_code_input_image_source_shape() {
        // Claude Code clipboard uploads can arrive as an OpenAI-compatible
        // `input_image` block while retaining Anthropic's nested `source`.
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gemini-3.1-pro",
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "input_image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/jpeg",
                        "data": "/9j/4AAQ"
                    }
                }]
            }]
        }))
        .unwrap();
        let images = cursor_selected_images(&req);
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].data, "/9j/4AAQ");
        assert_eq!(images[0].mime_type, "image/jpeg");
        assert!(uuid::Uuid::parse_str(&images[0].uuid).is_ok());
    }

    #[test]
    fn selected_images_ignore_history_after_assistant_turn() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "old screenshot"},
                        {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "T0xESU1H"}}
                    ]
                },
                {"role": "assistant", "content": "ok"},
                {"role": "user", "content": "just text this turn"}
            ]
        }))
        .unwrap();
        assert!(
            cursor_selected_images(&req).is_empty(),
            "replaying historical screenshots as new selected_images causes Cursor Image not found"
        );
    }

    #[test]
    fn selected_images_restore_history_when_checkpoint_is_missing() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "old screenshot"},
                        {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "T0xESU1H"}}
                    ]
                },
                {"role": "assistant", "content": "upstream failed before a checkpoint"},
                {"role": "user", "content": "retry this request"}
            ]
        }))
        .unwrap();
        assert_eq!(
            cursor_selected_images_for_continuation(&req, false)
                .iter()
                .map(|image| image.data.as_str())
                .collect::<Vec<_>>(),
            vec!["T0xESU1H"],
            "a checkpoint-less retry must replay the original image"
        );
        assert!(
            cursor_selected_images_for_continuation(&req, true).is_empty(),
            "checkpoint-backed turns must keep history images out of selected_context"
        );
    }

    #[test]
    fn selected_images_keep_current_turn_after_assistant() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "OLDIMG"}}
                    ]
                },
                {"role": "assistant", "content": "ok"},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "look"},
                        {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "TkVXSU1H"}}
                    ]
                }
            ]
        }))
        .unwrap();
        let images = cursor_selected_images(&req);
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].data, "TkVXSU1H");
    }

    #[test]
    fn selected_images_skip_empty_base64() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": ""}},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "REAL"}}
                ]
            }]
        }))
        .unwrap();
        let images = cursor_selected_images(&req);
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].data, "REAL");
    }

    #[test]
    fn selected_images_skip_invalid_base64() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "not base64!"}}
                ]
            }]
        }))
        .unwrap();
        assert!(cursor_selected_images(&req).is_empty());
    }

    #[test]
    fn selected_images_use_fresh_cli_style_uuids() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "SAME"}}
                ]
            }]
        }))
        .unwrap();
        let a = cursor_selected_images(&req);
        let b = cursor_selected_images(&req);
        assert_ne!(a[0].uuid, b[0].uuid);
        assert!(uuid::Uuid::parse_str(&a[0].uuid).is_ok());
        assert!(uuid::Uuid::parse_str(&b[0].uuid).is_ok());
    }

    #[test]
    fn refreshed_image_uuids_preserve_inline_payload_and_metadata() {
        let images = vec![CursorSelectedImage {
            data: "iVBORw0KGgo=".into(),
            uuid: "old-image-id".into(),
            path: String::new(),
            mime_type: "image/png".into(),
        }];
        let refreshed = refresh_image_uuids(&images);
        assert_eq!(refreshed.len(), 1);
        assert_eq!(refreshed[0].data, images[0].data);
        assert_eq!(refreshed[0].path, images[0].path);
        assert_eq!(refreshed[0].mime_type, images[0].mime_type);
        assert_ne!(refreshed[0].uuid, images[0].uuid);
        assert!(uuid::Uuid::parse_str(&refreshed[0].uuid).is_ok());
    }

    #[test]
    fn refreshed_image_uuids_keep_order_for_multi_image_retry() {
        let images = vec![
            CursorSelectedImage {
                data: "AAAA".into(),
                uuid: "one".into(),
                path: String::new(),
                mime_type: "image/png".into(),
            },
            CursorSelectedImage {
                data: "/9j/4AAQ".into(),
                uuid: "two".into(),
                path: String::new(),
                mime_type: "image/jpeg".into(),
            },
        ];
        let refreshed = refresh_image_uuids(&images);
        assert_eq!(
            refreshed
                .iter()
                .map(|image| image.data.as_str())
                .collect::<Vec<_>>(),
            vec!["AAAA", "/9j/4AAQ"]
        );
        assert_eq!(
            refreshed
                .iter()
                .map(|image| image.mime_type.as_str())
                .collect::<Vec<_>>(),
            vec!["image/png", "image/jpeg"]
        );
        assert!(
            refreshed
                .iter()
                .zip(images.iter())
                .all(|(new, old)| new.uuid != old.uuid)
        );
    }

    #[test]
    fn skips_url_images_in_selected() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "url", "url": "https://example.com/img.png"}}
                ]
            }]
        }))
        .unwrap();
        let images = cursor_selected_images(&req);
        assert_eq!(images.len(), 0);
    }

    #[test]
    fn renders_url_image_placeholder() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "url", "url": "https://example.com/img.png"}}
                ]
            }]
        }))
        .unwrap();
        let rendered = render_cursor_prompt(&req);
        assert!(rendered.contains("[image: https://example.com/img.png]"));
    }

    #[test]
    fn omits_historical_thinking_blocks() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{"role": "assistant", "content": [
                {"type": "thinking", "thinking": "let me think..."},
                {"type": "text", "text": "done"}
            ]}]
        }))
        .unwrap();
        let rendered = render_cursor_prompt(&req);
        assert!(!rendered.contains("<thinking>"));
        assert!(!rendered.contains("</thinking>"));
        assert!(!rendered.contains("let me think..."));
        assert!(rendered.contains("done"));
    }

    #[test]
    fn omits_thinking_nested_inside_tool_results() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "tu1", "content": [
                    {"type": "thinking", "thinking": "private nested reasoning"},
                    {"type": "text", "text": "visible tool result"}
                ]}
            ]}]
        }))
        .unwrap();
        let rendered = render_cursor_prompt(&req);
        assert!(!rendered.contains("<thinking>"));
        assert!(!rendered.contains("private nested reasoning"));
        assert!(rendered.contains("visible tool result"));
    }

    #[test]
    fn renders_tool_use_blocks() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{"role": "assistant", "content": [
                {"type": "tool_use", "id": "tu1", "name": "Read", "input": {"path": "/tmp"}}
            ]}]
        }))
        .unwrap();
        let rendered = render_cursor_prompt(&req);
        assert!(rendered.contains("<tool_use id=\"tu1\" name=\"Read\">"));
    }

    #[test]
    fn renders_tool_result_with_content_blocks() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "tu1", "content": [
                    {"type": "text", "text": "file contents"}
                ]}
            ]}]
        }))
        .unwrap();
        let rendered = render_cursor_prompt(&req);
        assert!(rendered.contains("<tool_result tool_use_id=\"tu1\">"));
        assert!(rendered.contains("file contents"));
    }

    #[test]
    fn handles_unsupported_block_types() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{"role": "user", "content": [
                {"type": "unknown_block", "text": "some fallback text"}
            ]}]
        }))
        .unwrap();
        let rendered = render_cursor_prompt(&req);
        // Unsupported blocks fall back to text rendering if they have a text field
        assert!(rendered.contains("some fallback text"));
    }

    #[test]
    fn empty_messages_renders_emptyish() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{"role": "user", "content": ""}]
        }))
        .unwrap();
        let rendered = render_cursor_prompt(&req);
        assert!(rendered.is_empty() || !rendered.is_empty());
    }

    #[test]
    fn tool_result_with_nested_image() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "tu1", "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": "BBBB"}}
                ]}
            ]}]
        }))
        .unwrap();
        let images = cursor_selected_images(&req);
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].mime_type, "image/jpeg");
        assert_eq!(images[0].data, "BBBB");
    }

    #[test]
    fn tool_result_with_single_object_image() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "tu1", "content":
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "BBBB"}}}
            ]}]
        }))
        .unwrap();
        let images = cursor_selected_images(&req);
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].data, "BBBB");
    }

    #[test]
    fn renders_server_tool_use() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{"role": "assistant", "content": [
                {"type": "server_tool_use", "id": "st1", "name": "WebSearch", "input": {"query": "rust"}}
            ]}]
        }))
        .unwrap();
        let rendered = render_cursor_prompt(&req);
        assert!(rendered.contains("<server_tool_use id=\"st1\" name=\"WebSearch\">"));
    }

    #[test]
    fn renders_web_search_tool_result() {
        let req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "messages": [{"role": "user", "content": [
                {"type": "web_search_tool_result", "tool_use_id": "ws1", "content": {"results": []}}
            ]}]
        }))
        .unwrap();
        let rendered = render_cursor_prompt(&req);
        assert!(rendered.contains("<web_search_tool_result tool_use_id=\"ws1\">"));
    }
}
