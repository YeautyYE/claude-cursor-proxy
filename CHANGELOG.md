# Changelog

Project renamed to **claude-cursor-proxy** — public repo [YeautyYE/claude-cursor-proxy](https://github.com/YeautyYE/claude-cursor-proxy).
Adapted from [raine/claude-code-proxy](https://github.com/raine/claude-code-proxy). Earlier entries below retain upstream history (including Homebrew notes that do **not** apply here).

## v0.1.62 (2026-08-21)

- Reserve protected admission capacity for interactive models: a 32-wide `cursor-grok-*` fan-out can no longer starve Gemini/Claude subagent starts into `admission queue timed out`. Starts are classed as bulk (`cursor-grok-*`, capped by `CCP_CURSOR_LIVE_CONCURRENCY`) or interactive (everything else, which may borrow idle bulk slots and falls back to `CCP_CURSOR_LIVE_INTERACTIVE_RESERVE`, default 8); resumes keep their own reserve that no start class can consume.
- Admit against local capacity before claiming the per-session live slot. A start waiting in the local admission queue no longer makes overlapping retries fail with `A Cursor live run is already active for this session`, and an admission timeout leaves the session slot free. Admission timeouts and slow admissions now log their class, limits, and queue time.
- Add an opt-in durable operation ledger (`CCP_CURSOR_OPERATION_LEDGER=1`; SHA-256 request fingerprints, owner-fenced CAS transitions, crash-safe atomic writes, cross-process `flock`): ambiguous or completed live operations survive proxy restarts, duplicate replays are refused with 409, and a stale owner can never clear a newer owner's marker. It ships disabled: completion is not yet gated on downstream delivery, so a response dropped mid-stream could otherwise refuse the client's legitimate retry, and an expired unresolved stage now expires to retryable instead of a permanent synthetic tombstone.
- Restore vacant-slot adoption in live-run publication: a Run accepted by Cursor while its reservation was cancelled is adopted instead of dropped, so a retry cannot open a second Run for the same turn; ambiguous tombstones remain non-overwritable.
- Roll back in-memory conversation state when a conditional checkpoint/blob/clear write fails to reach disk. Memory can no longer run ahead of durable state, so a later turn cannot silently continue from a checkpoint that never survived a restart.
- Treat downstream tool execution as a paused live-run phase: start its TTL only when the batch reaches Grok, default that TTL to the configured live-generation timeout, pause the active generation budget while tools run, and grant each accepted tool-result continuation a fresh generation budget. A tool-result POST already in flight wins the expiry boundary.
- Classify a heartbeat-live Run whose completion remains unresolved as HTTP 409 on its first response. The registry and HTTP adapters now agree that retrying could duplicate execution, eliminating the misleading 502 → automatic retry → 409 cascade while preserving retryable pre-connect 502s.
- Add deterministic 100-way admission turnover coverage proving bounded starts, reserved resume progress, eventual completion, and full semaphore recovery.

## v0.1.61 (2026-08-21)

- Keep legitimate heartbeat-backed Fable/Grok thinking past the old 240-second cutoff, but bound heartbeat-only runs to 10 minutes without model progress instead of the 30-minute hard timeout. Recover the process-wide H2 circuit after a 30-second cooldown through one side-effect-free `GetUsableModels` probe, avoiding both permanent HTTP/1 stalls and ambiguous user-Run probes.

## v0.1.60 (2026-08-21)

- Bound Grok fan-out without reviving the old 429 failure mode: admit 32 new generations fairly, reserve four additional slots for paused tool-result resumes, release capacity between tool segments, and return HTTP 503 with jittered `Retry-After` after a 15-second overflow queue.
- Isolate shared transport failures across four stable H2 client pools keyed by full conversation/agent identity, cap simultaneous ResumeAction replacement opens at four, and retain full jitter before retryable reconnect opens. A hollow HTTP 200 remains fail-closed because replaying ResumeAction could duplicate execution.
- Continue after a hollow post-tool ResumeAction only when a newer checkpoint arrives after the tool results are submitted. Already-queued pre-submit checkpoints are absorbed into the baseline first so they cannot later look like post-tool proof. The confirmed checkpoint is retained without replaying tools; an old checkpoint, stale tool-result generation, partial output, or unresolved acceptance remains fail-closed as 409.
- Release a live-slot reservation when its HTTP future is cancelled in the local admission queue; start sealing the slot only when the first upstream open is about to begin.
- Show the random UUID suffix in eight-character TUI session columns so nearby UUIDv7 subagents no longer appear to be the same session.

## v0.1.59 (2026-08-20)

- Wait 90s for the first HTTP/2 live open (`CCP_CURSOR_LIVE_H2_OPEN_SECS`, clamp 10–180) so a proxy-local 20s deadline does not cut Cursor off under Grok fan-out.
- Later unsent POSTs against a Starting, Ambiguous, or otherwise busy live slot return HTTP 503 with jittered `Retry-After` instead of a fatal Grok 409. The ambiguous open still fail-closes the same send as 409 and still tombstones the slot for 90s, so a retry cannot start a second Cursor Run.

## v0.1.58 (2026-08-20)

- Remove the proxy's local live-generation and live-open admission gates. Cursor upstream is the only concurrency owner; a Grok fan-out no longer dies with `rate_limit_error: Cursor live generation concurrency saturated`. Per-conversation live slots remain so the same Grok conversation cannot start two Runs.

## v0.1.57 (2026-08-20)

- Align Grok identity with its actual protocol: `x-grok-conv-id` is the Cursor conversation boundary, `x-grok-req-id` is the idempotent operation key, and the install-wide `x-grok-agent-id` no longer collapses unrelated subagents into one live slot.
- Treat local admission pressure as HTTP 503 with server-jittered `Retry-After` instead of 429, raise the default active-generation limit to Grok's 32-way fan-out, and keep tool-result resumes ahead of new starts.
- Preserve one `x-original-request-id` across transport fallback and reconnect while generating a fresh `x-request-id` for every Run/BidiAppend attempt. Definite pre-connect and explicit upstream 5xx failures remain retryable and no longer poison the slot as 409; ambiguous sends still fail closed.
- Recover from the one-way transport circuit seen in runtime logs: when circuit-selected HTTP/1 cannot connect its initial BidiAppend, safely half-open HTTP/2 unless the operator explicitly forced HTTP/1.

## v0.1.56 (2026-08-20)

- Treat thinking-only or checkpoint-less hollow ResumeAction as a retryable conversation reset instead of a non-retryable 409. Grok fan-out no longer dies after Fable thinking when Cursor produces no text or tools. Cancellation without client-visible output is 502 so the next POST can continue.

## v0.1.55 (2026-08-20)

- Fail-fast when the 16 live-generation slots are full: overflow waits 8 seconds, then returns retryable 429 with `Retry-After: 2` instead of holding grok HTTP for 4 minutes. Lone Fable turns still get a 240-second heartbeat-only thinking window; a saturated gate cuts that to 30 seconds so hung holders cannot convoy the rest of a fan-out.

## v0.1.54 (2026-08-20)

- Separate tool-result generation admission from the 20-second live-driver acknowledgement budget, keep resume requests ahead of new starts, and allow a bounded 240-second local queue while retaining the 16-generation upstream cap. Cancelled or completed drivers now leave that queue immediately.
- Treat duplicate active requests and fresh-turn replacement races as retryable 429 contention instead of invalid-request 409. Request fingerprints still keep stale tool results and ambiguous upstream acceptance fail-closed, preventing retries from attaching to a different generation or duplicating execution.

## v0.1.53 (2026-08-20)

- Resume every parallel OpenAI Responses function output as one logical Cursor tool-result batch, even when outputs are split across user messages or followed by a standalone `<system-reminder>`. This prevents missing-result failures, repeated tool calls, and high-fanout subagent retry loops.
- Bound empty-turn recovery to one internal retry and one five-minute episode. Stale hollow checkpoints are rotated, while checkpoints that already consumed completed tool results are preserved and continued without replaying those tools. Reasoning-only Responses streams now still emit a terminal event at EOF.
- Limit active Cursor generations to 16 by default, release capacity between tool segments, and prioritize tool-result resumptions over unrelated new starts. `CCP_CURSOR_LIVE_CONCURRENCY` and `CCP_CURSOR_LIVE_QUEUE_SECS` remain configurable.

## v0.1.52 (2026-08-19)

- Quarantine malformed or unadvertised Cursor control XML instead of leaking or reconstructing it in grok transcripts. Preserve each client's exact advertised `workflow`/`Workflow` and `skill`/`Skill` names so the bridge neither intercepts nor invents tools.
- Treat no-output Cursor turns as retryable upstream failures instead of successful assistant notes, so tool-result continuations such as `spawn_subagent` cannot silently end a grok session. Live resume dispatch now waits up to 20 seconds and returns retryable 429 on contention.

## v0.1.51 (2026-08-18)

- Recover grok Anthropic-style `<tool_use><parameter>` XML as structured tools instead of leaking it into the transcript. XML `spawn_subagent` waits for `turn_ended` so a 64-way stream is not torn after the first chunk. Sibling MCP lifecycle spawns still share one batch.
- Raise `RLIMIT_NOFILE` at startup (macOS often starts at 256) so Surge CONNECT fan-out does not fail as `Cursor auth failed: /usr/bin/security: Too many open files`. Unreadable `auth.json` fails closed instead of impersonating Keychain. Retry initial HTTP/1 `BidiAppend` timeouts inside the POST before 409.

## v0.1.50 (2026-08-18)

- Retry Cursor live-open misses inside the proxy before returning 409: H2 20s timeouts and pre-connect `BidiAppend` failures try HTTP/1 on the same request; only exhausted attempts fail closed as 409 so grok-build does not 5xx-retry. High Load 429s are not retried into the same shed.
- Remap Cursor `Agent`/`Task` `subagent_type` model slugs (`gemini-*`, `composer-*`, …) to `general-purpose` so Claude Code does not reject `Agent type 'gemini-3.6-flash-high' not found`. The H2 circuit now pins HTTP/1 until an H2 open succeeds.

## v0.1.49 (2026-08-18)

- Live-open concurrency soft-starts at 4 and doubles on success up to 128 (grok-cli fan-out). `CCP_CURSOR_LIVE_OPEN_CONCURRENCY` is an optional cap, not a required knob. A full gate waits, then returns 429 instead of failing immediately.
- ResumeAction open uses the same budget as a first open (H2 20s, HTTP/1 90s, still bounded by the 45s recovery window). A flat 10s cap no longer kills HTTP/1 resumes after H2 `INTERNAL_ERROR` (the Claude Code `gemini-3.6-flash-high` 409).

## v0.1.48 (2026-08-18)

- Stabilize grok-build `/v1/responses` against Cursor live-open races: hold HTTP until the Run is accepted, map ambiguous open/resume timeouts to HTTP 409 (no same-request 5xx retry), and return 429 when the live-open semaphore is saturated so grok can back off.
- Hash a fallback session from user + cwd + first user text (and Responses `metadata.user_id`) so grok-build without Claude session headers does not share one Cursor slot. Recover allowlisted grok `<tool_call>` XML into structured tools.
- Keep Claude Code on immediate `/v1/messages` SSE. Synthesize a short Bash `description` from Cursor Shell (which has none) so the TUI title is not the entire `python3 -c` body; prefer advertised `Bash` when both Claude and grok names are listed.
- H2 keepalive is 60s/20s without idle PINGs; reconnect uses full jitter plus an open semaphore; the H2 circuit half-opens after cooldown instead of sticking on HTTP/1.

## v0.1.47 (2026-08-17)

- Pass Cursor billing and geo/policy failures through as HTTP 429/403 instead of grok-build's generic 500 "our side". Unpaid-invoice text stays on 429, unsupported country/region text stays on 403, and `/v1/responses` holds the stream peek until a pre-output 4xx can be returned as JSON.

## v0.1.46 (2026-08-17)

- grok-build / Grok CLI is a first-class client over OpenAI Responses (`/v1/responses`).
- Cursor native Shell/Task/Read/WebSearch are remapped onto exact grok-build client names (`run_terminal_command`, `spawn_subagent`, …).
- Live session recovery so grok-build does not loop on `mcp_claude-local_*` / 409 / 502; dead Cursor runs recover in-request.
- Advertise those grok-build names on the MCP catalog (Fable only invokes MCP), steal `mcp_claude-local_*` back, reject foreign spoofs, and map `subagent_type` model slugs to general-purpose.

## v0.1.45 (2026-08-14)

- Fix the 400 `Missing tool_result blocks for pending tools` after a Claude Code tool turn is interrupted or abandoned (often while background shells are still running). A fresh request with no current-turn `tool_result` now briefly queues, then supersedes the stale live run instead of demanding an impossible result. Supersession and downstream tool ids are bound to the observed Run generation; replacement atomically reserves the slot and waits for the old transport/pump to tear down, so a delayed waiter cannot cancel a newer Run, overlap two Runs, or inject an old result into its replacement. Historical `tool_result` blocks no longer misclassify a new user turn as a resume; partial, mismatched, extra, duplicate, and mixed result-plus-user-content batches remain a strict 400.
- Recover the same Claude Code session after Cursor reports `Conversation data missing` / missing blobs. The unrecoverable Cursor conversation id, checkpoint, and blob cache are discarded without being re-persisted during driver teardown; after the explicit failed turn, the first retry starts one fresh Run with full Anthropic history instead of replaying the 502 or requiring a new Claude chat. The same reset applies to HTTP open, ResumeAction, and BidiAppend errors that carry the missing-conversation detail, not only Connect END. Tool-result requests no longer sit in a 30-second pre-response wait (where no SSE ping is possible); the default is 5 seconds, preventing Claude Code's `Stream idle timeout - no chunks received` during stale-run classification.
- Fail closed on live delivery ambiguity: HTTP/1 `BidiAppend` is attempted once; a Starting open that times out or is dropped occupies the session for 90s; a successful turn leaves a same-prompt fingerprint tombstone so a retried POST cannot start a second Cursor Run; cancel/disconnect after an accepted but unconfirmed resume is ambiguous instead of success.

## v0.1.44 (2026-08-14)

- First-turn H2 `broken pipe` no longer dies with `reconnect skipped: no checkpoint`. ResumeAction can reattach with the session `conversation_id` and empty conversation state (not a second user message). `broken pipe` forces HTTP/1 on that reconnect, same as H2 `INTERNAL_ERROR`.

## v0.1.43 (2026-08-14)

- Fable `high` thinking no longer dies at 45s with `Cursor stream produced no useful progress`. `setup_idle` only fires when the Cursor stream has gone silent (no frames, including heartbeats). Heartbeat-only thinking waits 2× stream idle (240s default), same budget as a thinking-only turn that already advertised tools.

## v0.1.42 (2026-08-13)

- Stop the 0.1.41 20s-open → buffered-duplicate cascade. H2 first-open is 20s (`CCP_CURSOR_LIVE_H2_OPEN_SECS`); HTTP/1 is 90s (`CCP_CURSOR_LIVE_OPEN_SECS`) and only after an explicit 421/464/`HTTP_1_1_REQUIRED` — a timed-out H2 `.send()` does not start a second Run. Live start errors do not fall through to buffered `/Run`.
- After visible text, heartbeat-only silence at 2× stream idle (240s default) ResumeAction's or errors instead of waiting until the 1800s hard timeout. Hard timeout is `CCP_CURSOR_LIVE_TIMEOUT_SECS` (default 1800) and is no longer the buffered 90s `CCP_CURSOR_TIMEOUT_SECS`.
- Reconnect budget ignores handshake `request_context`, KV, checkpoint, and interaction query (including gzip-wrapped native-exec frames). Deadline checks run at the top of the driver loop so heartbeats cannot starve them. H1 fallback leftover is shared wall-clock. Reconnect never auto-pins HTTP/1 from the H2 breaker, a 504 open timeout, a hollow HTTP 200 resume, or delayed hollow EOF during probation — those are fail-closed (ambiguous accept). 464-rejected HTTP/1 does not pin remaining H2 opens. BidiAppend / control_close 429 is terminal (not ResumeAction) and maps to `rate_limit_error`. Dropped SSE / clean buffered EOF / complete-idle-after-text without `turn_ended` is an error. An in-flight Starting open is not superseded or Conflict-cancelled; session claim is one lock (`try_claim_run` / `conflict_action`). A timed-out open leaves a 90s tombstone (pre-connect `Cursor upstream connect failed` does not). An accepted Run whose reservation was stolen is cancelled and 409s — it does not `start_live` again. Live-eligible turns that cannot take the slot 409 instead of falling through to buffered `/Run`. Buffered `/Run` does not retry setup-idle over HTTP/1.
- Clash TUN / origin 429 / origin hang can still fail a turn. Pin `CCP_CURSOR_HTTP1=1` or DIRECT `*.cursor.sh` when H2 blackholes.

## v0.1.41 (2026-08-13)

- Live reconnect is a 45s wall-clock recovery episode (at most four ResumeAction opens, 10s per open). Delayed hollow resumes (HTTP 200 then RST after the 1ms peek) stay hollow instead of restarting exponential backoff. Backoff is capped at 8s and never more than half the remaining window — that was the ~5-minute 502. Initial live open is capped at 20s. HTTP/1 fallback is skipped once 464/421 rejected it. H2/H1 circuit breakers open after three consecutive failures. Tool-advertised thinking-only turns stall at 2× stream idle (240s default) instead of the 1800s hard timeout; turns that already emitted text still wait for `turn_ended`. Non-streaming collect no longer treats a dropped channel with partial tokens as success. SSE sets `X-Accel-Buffering: no` and `Cache-Control: no-cache, no-transform`.

## v0.1.40 (2026-08-13)

- Stop replaying historical Anthropic screenshots as new Cursor `selected_images`. Claude Code sends the full `messages` array; those old base64 blocks were given fresh UUIDs while `conversation_state` still held earlier image ids, so Cursor 502'd `Image not found [internal]` on text-only turns. Only the current user turn is attached, empty base64 is skipped, and image UUIDs are stable for the same bytes. A missing-image Connect END clears the poisoned checkpoint so the next retry is not doomed.

## v0.1.39 (2026-08-13)

- Claude Code `stream=false` (non-streaming fallback after a 502) now uses the same live BiDi path as streaming, then collects events into one Anthropic JSON body. That path was the 45s `idle timeout` / `0 response bytes` 502: buffered H2 `/Run` never saw request_context. Live continuation no longer 400s when the retry is non-streaming.
- If live is skipped, log `live_skipped` with `{stream, hasSession, bidiEnabled, reason}`. Buffered `/Run` pins HTTP/1 from the client (not a second env read), retries setup-idle once over HTTP/1, and includes Connect frame count in the empty-body 502.

## v0.1.38 (2026-08-13)

- Live reconnect no longer false-aborts quiet Fable resumes (3s first-byte gate) or replenishes the retry budget on heartbeats / partial bytes. Retired pumps are fenced so a stale EOF cannot kill a healthy HTTP/1 replacement. H2 hollow + Clash 464 fail fast instead of oscillating; 464 flip-back rebuilds a real H2 client even when `CCP_CURSOR_HTTP1=1`. Failed `control_close` keeps collecting natives. `/healthz` reports the running version and start time.

## v0.1.37 (2026-08-13)

- Root-cause fix for `unexpected internal error` + `reconnect budget exhausted`: HTTP 200 ResumeAction was treated as success before any body bytes, and the "HTTP/1" fallback still used an HTTP/2 reqwest client for `/RunSSE`. Wait for the first body byte, pin `http1_only()` when falling back, and skip exponential backoff on zero-byte RSTs (that backoff was the 5-minute 502).

## v0.1.36 (2026-08-13)

- Do not spend the ResumeAction budget on HTTP/2 `INTERNAL_ERROR` after a 200: drop the poisoned connection pool, and if a reconnect returns zero body bytes, switch that run to HTTP/1 `RunSSE` (Clash 464 falls back to a fresh H2 client).

## v0.1.35 (2026-08-13)

- Live stream decode failures include the reqwest/hyper/h2 source chain (so TUI shows `connection reset` / `unexpected EOF`, not just `error decoding response body`).
- ResumeAction reconnect after a dropped Cursor stream: first attempt is immediate; retryable open failures loop up to `CCP_CURSOR_RECONNECT_MAX` and can flip to HTTP/1. Unexposed collecting tools are `control_close`d; Claude-owed `tool_result`s stay queued so a mid-tool disconnect can resume. Skip/fail reasons are logged and appended to the Anthropic `event: error`.
- Conversation checkpoints are persisted as soon as Cursor sends `conversation_checkpoint_update`, not only when the BiDi driver exits.
- Tool-result resume retries ResumeAction once if the request stream was already closed.
- `CCP_CURSOR_NO_PROXY=1` skips HTTP(S)_PROXY for Cursor API (Clash/Surge TUN still needs a DIRECT rule for `*.cursor.sh`).

## v0.1.34 (2026-08-13)

- Route live Cursor catalog ids (`claude-fable-5-thinking-high[1m]`, …) through `/v1/messages`; overlapping `gpt-5.5` without a `cursor:` prefix still goes to Codex.
- Mixed Workflow/Skill + native Read/Bash batches end the BiDi run so the next POST includes ClientOnly `tool_result` history (in-flight native execs are `control_close`d).
- Abrupt live EOF without `turn_ended` is an Anthropic `event: error`, not a successful `message_stop`. Failed KV/context/interaction sends are terminal errors too.
- Server heartbeats no longer reset setup/stream idle clocks (heartbeat-only stalls recover at 45–120s instead of the 1800s hard timeout).
- Hosted `web_fetch`: reject unresolvable / trailing-dot localhost hosts, cap the body while streaming, and require a stored Cursor login. Gzip Connect envelopes are size-capped; advertise `gzip` only (not Brotli).
- Do not H2→H1-fallback or buffered-retry 429/400/401/403. Reconnect can use the opening checkpoint. `count_tokens` uses the current request body. Conversation files are `0700`/`0600` and abandoned disk TTLs are scanned. HTTP/1 `BidiAppend` has a 30s timeout. Live Connect errors map to `rate_limit_error` / `authentication_error` / `permission_error`.

## v0.1.33 (2026-08-13)

- Nested Claude Code agents (`x-claude-code-agent-id`) compact prompts against `{session}::agent::{id}`, not the parent session checkpoint.
- Restore Workflow / Skill / `mcp__*` JSON Schema on `RunRequest.mcp_tools` as `Value.struct_value` (the 0.1.31 `{type:object}` strip was leftover conservatism).
- Expose Cursor Glob `tool_call_started` as ClientOnly — official `ExecServerMessage` has no `glob_args`.
- Unmatched `InteractionQuery` errors the BiDi turn instead of answering on the AskQuestion oneof.
- Alias `haiku` resolves to `claude-haiku-4-5`.
- TUI keeps a stream `event: error` as HTTP 502 instead of overwriting with the SSE envelope 200.
- Cursor conversation checkpoints persist under the state dir (or `CCP_CURSOR_CONV_DIR`) so a proxy restart does not drop `conversation_state`.

## v0.1.32 (2026-08-13)

- Always send `RunRequest.conversation_state` (empty bytes for a fresh turn). 0.1.31 omitted the field when empty; Cursor then returned `Conversation state is required [invalid_argument]`.

## v0.1.31 (2026-08-13)

- Fix Cursor `Connect error 502: parse binary: invalid end group tag` on the first `/v1/messages` turn. `McpToolDefinition.input_schema` is now `google.protobuf.Value` (`struct_value`) with a minimal `{type:object}` schema; only Workflow / Skill / `mcp__*` are advertised. Encoding every Claude-local tool's full JSON Schema as a raw Struct made Cursor reject the Run. Empty `conversation_state` is omitted. RequestContext cwd is skipped when the path does not exist on this host (LAN/WSL). Connect END errors are logged and stored as terminal failures (TUI 200 was the SSE envelope; the real error is in the stream).

## v0.1.30 (2026-08-13)

- Decode `ToolCallDelta.task` (tag 2): nested `TaskToolCallDelta.interaction_update` is boxed and processed one extra level so subagent `partial_tool_call` still fills ClientOnly MCP/Workflow args and nested text/`tool_use` is not dropped. Nested `turn_ended` does not end the parent Task.

## v0.1.29 (2026-08-13)

- Decode Cursor `InteractionUpdate.partial_tool_call` (tag 7) and `tool_call_delta` (tag 15). Streamed MCP/Workflow JSON in `args_text_delta` is merged into ClientOnly `tool_use.input` so Fable cannot expose Workflow with an empty object.
- Decode `ToolCall.web_fetch_tool_call` (tag 37, distinct from Fetch tag 24). Cursor-native WebFetch stays transcript/exec; nested Anthropic hosted `web_fetch` is still the emulator on `/v1/messages`.

## v0.1.28 (2026-08-13)

- Nested live runs are keyed by `(session_id, agent_id)` using plumbed `x-claude-code-agent-id` / `x-claude-code-parent-agent-id` (no synthetic session UUID). Nested Workflow POSTs share `X-Claude-Code-Session-Id` without superseding the parent BiDi.
- Fill CLI `RequestContext` on the exec reply (`request_context_result`) with cwd/git from Claude system / `<system-reminder>` — not as a `RunRequest` field.
- When `RunRequest.mcp_tools` is set, the prompt XML `<tools>` dump is names + one-line descriptions only (no duplicated JSON schemas), plus a Workflow/Skill nudge.
- Empty turns synthesize a real Workflow `tool_use` from `Invoke: Workflow({ name: "deep-research", … })` (or the `"deep-research"` workflow line) instead of a text-only recovery note.
- Mixed ClientOnly + native batches keep BiDi open so in-flight Read/Bash are not dropped when Workflow/Skill is exposed.
- SSE delta coalesce only under channel backpressure (tokens otherwise stream at Cursor cadence).
- Map Cursor `AskQuestion` to Claude Code `AskUserQuestion` and expose it as ClientOnly.
- Session header fallback: when `X-Claude-Code-Session-Id` is missing, derive a stable `ccp-fb-` id from `metadata.user_id` + project cwd so live BiDi still starts (`session_id_fallback`). Nested agents keep that session id. `x-app` is logged (`cli` / `cli-bg`), not used for routing.
- `/v1/messages/count_tokens` seeds from the session's last Cursor `turn_ended.input_tokens` when available; otherwise char/4 of the rendered Cursor prompt.
- Cursor CLI `ShellArgs.timeout` is milliseconds (same as Claude Bash); pass through without converting to seconds.
- Anthropic listen sockets and Cursor upstream HTTP set `TCP_NODELAY` so Nagle does not delay SSE chunks.

## v0.1.27 (2026-08-13)

- Fix ClientOnly (Workflow/Skill/mcp__*) continuation: after BiDi teardown, a tool_result-only POST is forwarded into the next Cursor Run instead of being skipped as a native exec resume. Clear the in-flight MCP checkpoint so the next turn is not a zombie resume.
- Treat Cursor qualified MCP names (`claude-local/Workflow`, `claude-local:Workflow`) as ClientOnly and emit Anthropic `tool_use.name` `Workflow` so `/deep-research` reaches Claude Code.
- Stop stuffing Cursor `provider_identifier` into Anthropic Workflow/MCP tool input (`additionalProperties: false`).
- Mixed native+ClientOnly batches expose Workflow/Skill without flushing still-collecting Read/Bash into the same Anthropic pause. Starting live-run reservations count as occupied; ResumeAction reconnect keeps `mcp_tools`.
- Stop mid-stream Anthropic `message_delta` while a thinking block is open so Claude Code's thinking OTPS meter keeps counting.
- Emulate nested Anthropic hosted `web_fetch` for `/deep-research` Workflow agents.

## v0.1.26 (2026-07-21)

- Fix `RunRequest.mcp_tools` wire shape to match Cursor CLI `McpToolDefinition`: `input_schema` as `google.protobuf.Struct` (not JSON string), plus `provider_identifier` / `tool_name` (`claude-local`). v0.1.25 advertised tools with the wrong encoding so Fable could still ignore Workflow.
- Emit the empty-turn recovery note on Connect `FLAG_END` and exhausted EOF (not only `turn_ended`), so silent Anthropic Out:0 after ~1m heartbeat-only runs cannot happen.
- Log `empty_turn_note` to `proxy.log` always; richer `CCP_CURSOR_DEBUG` start_live_agent mcp_tools listing.

## v0.1.25 (2026-07-21)

- Advertise Claude-local tools (`Workflow`, `Skill`, `mcp__*`) via Cursor `RunRequest.mcp_tools` so Fable can actually invoke them (prompt `<tools>` text alone was ignored → empty Out=0 turns after ~1m heartbeat-only thinking).
- Expose MCP `tool_call_started` for Claude-local names as Anthropic `tool_use` and end the BiDi segment (same ClientOnly path as XML recovery).
- Clear stalled UI-only `tool_call_started` using a heartbeat-immune timer; surface a short note instead of contentless `end_turn` when Cursor finishes with no text/tools.

## v0.1.24 (2026-07-20)

- Fix Workflow/`turn_ended` race: expose Claude-local `<tool_use>` XML (Workflow/Skill) immediately — including when Cursor ends the turn in the same chunk — so Anthropic gets `Workflow(name: "deep-research")` instead of empty Out=0 completions.
- Eliminate remaining 409 `already active` races: retry supersede+start on Starting→Running steal instead of failing the client.
- Prefer Claude-local tools in BiDi `<tools>` dumps so Fable does not reinvent `/deep-research` with Bash.

## v0.1.23 (2026-07-20)

- Fix Claude Code `Stream idle timeout` → 409 cascade on long BiDi turns: detect Anthropic SSE disconnect immediately (even under Cursor heartbeat flood), cancel/supersede zombie live runs instead of waiting then 409, keep Anthropic `ping` spacing with `MissedTickBehavior::Delay`.
- Live BiDi: recover Claude-local `<tool_use>` XML (`Workflow`/`Skill`/…) into Anthropic tool_use, end the Cursor run so Claude Code can fulfill locally, then continue on the next turn with `tool_result` history.
- Buffered tool bridge: map unknown XML tools (including `Workflow`) to `Generic` so resume does not drop pending state.
- Confirm scrubber keeps user `<system-reminder>` (CLAUDE.md / rules); document `ENABLE_TOOL_SEARCH` / `enableWorkflows` / optional embed-system env.
- Forward mid-stream Anthropic `message_delta.usage` (and seed `message_start.usage`) so Claude Code statusline In/Out/Cached/Ctx update live during thinking — not only via the proxy TUI monitor path.
- When BiDi-bridging, still forward Claude-local tools (`Workflow`, `Skill`, `mcp__*`, …) into Cursor `<tools>`; only omit Cursor-native schemas (Read/Bash/…). Fixes silent drop that made `/deep-research`/skills look like plain Bash agenting. CLAUDE.md/rules in Anthropic `system` remain omitted by default (Fable injection loops).
- Cursor live streaming latency (CLI parity): never drop thinking/text deltas under SSE backpressure (old 5ms `try_send` timeout discarded tokens); resume fan-out matches start capacity (512); prefer draining InteractionUpdates before heartbeat ticks; non-blocking exec/client heartbeats (HTTP/1 BidiAppend no longer stalls the read loop); larger upstream pump; disable SSE try_recv coalesce so tokens stream at Cursor cadence; early tool expose when quiet window already elapsed; `CCP_CURSOR_TOOL_BATCH_MS=0` honored (default remains 25ms and does not gate thinking).
- Cheaper TTFT seeding: zero-copy Connect decode for uncompressed frames; estimate tools JSON size without re-serializing the full schema dump on every request.
- SSE hot path: throttle + `try_lock` monitor progress publishes so TUI snapshot cloning cannot stall token emission; Bytes-clone classify path (no per-frame `to_vec`).
- Emulate Anthropic hosted `web_search_20250305` (DuckDuckGo HTML → `server_tool_use` / `web_search_tool_result` SSE) so Claude Code `WebSearch` and `/deep-research` nested searches work through the proxy.

## v0.1.22 (2026-07-20)

- Rename public identity to **claude-cursor-proxy** (one-way proxy: Claude Code → proxy → Cursor).
- GitHub repo renamed in place (`YeautyYE/claude-cursor-proxy`); old URL redirects.
- Config/state dirs move to `claude-cursor-proxy`; auth still falls back to `claude-cursor-bridge` and `claude-code-proxy`.
- Installer accepts prior env aliases and creates optional symlinks for old binary names.

## v0.1.21 (2026-07-15)

- The monitor shows session token activity trends at common terminal widths,
  making throughput history visible without an extra-wide window.

## v0.1.20 (2026-07-15)

- The monitor reliably shows project names for Claude Code sessions and keeps
  them visible as requests are sequenced.
- Keyboard navigation scrolls session and recent-request tables to keep the
  selected row visible.
- Pressing `q` asks for confirmation before gracefully shutting down the proxy.
- Compact monitor layouts show more project, provider, model, effort, and token
  details without requiring a wider terminal.

## v0.1.19 (2026-07-15)

- The monitor shows project and session context at more terminal widths while
  preserving key request details in narrower layouts.

## v0.1.18 (2026-07-15)

- Codex preserves encrypted reasoning across turns, improving continuity when
  conversation history is replayed. ([#52](https://github.com/raine/claude-code-proxy/pull/52))
- The new `demo` command opens the interactive monitor with simulated traffic,
  without starting a proxy server or requiring provider credentials.
- Session rows show project names and output-token activity over time, making
  concurrent sessions and usage bursts easier to identify.
- Monitor tables adapt more consistently across terminal sizes and keep important
  request details readable in compact layouts.
- The monitor stays visible during graceful shutdown and shows progress until the
  proxy finishes draining connections.

## v0.1.17 (2026-07-14)

- The proxy can listen on a configurable IP address through `CCP_BIND_ADDRESS`
  or `bindAddress`, enabling protected access from containers and remote hosts.
  ([#48](https://github.com/raine/claude-code-proxy/pull/48))
- Model names with context-window hints such as `[1m]` route correctly across
  providers. ([#50](https://github.com/raine/claude-code-proxy/pull/50))
- The monitor reports more accurate output rates by measuring generation time
  and excluding requests without complete usage and timing data.

## v0.1.16 (2026-07-13)

- GPT-5.6 Luna requests work without a custom User-Agent instead of failing with
  a model unavailable error.
  ([#45](https://github.com/raine/claude-code-proxy/issues/45))
- Canceled or replaced Codex prompts cannot interrupt later turns with stale
  continuation state.
- GPT-5.6 setup examples use a 272K compaction window to stay within the current
  ChatGPT context limit.
- Homebrew installations can run the proxy at login as a background service with
  `brew services start claude-code-proxy`.
  ([#44](https://github.com/raine/claude-code-proxy/pull/44))

## v0.1.15 (2026-07-12)

- Codex function tools preserve optional parameters, preventing unintended tool
  arguments and incorrect agent isolation choices.
  ([#43](https://github.com/raine/claude-code-proxy/issues/43))
- Forced Codex web searches return live results while preserving allowed and
  blocked domain filters.
  ([#26](https://github.com/raine/claude-code-proxy/issues/26))
- Codex credentials are stored and refreshed independently from the native Codex
  CLI, preventing either application from invalidating the other's login. Users
  who relied on the native Codex login must sign in to the proxy once after
  upgrading.
- [Expanded guidance](https://github.com/raine/claude-code-proxy/#switching-models-and-backends)
  explains how to switch models within the proxy and how to switch between the
  proxy and direct Anthropic.

## v0.1.14 (2026-07-12)

- Codex hosted web searches work when Claude Code routes them through the Luna
  small model. ([#26](https://github.com/raine/claude-code-proxy/issues/26),
  [#35](https://github.com/raine/claude-code-proxy/pull/35))
- Codex context-window errors trigger Claude Code's compaction flow instead of
  ending the request. ([#29](https://github.com/raine/claude-code-proxy/pull/29))
- Codex requests fall back to HTTP after WebSocket handshake failures while
  preserving live streaming for established connections.
  ([#39](https://github.com/raine/claude-code-proxy/pull/39))
- Codex HTTP and WebSocket failures retain upstream status codes and error
  details, making failures clearer and more actionable.
  ([#40](https://github.com/raine/claude-code-proxy/pull/40))

## v0.1.13 (2026-07-12)

- Grok users can sign in on headless hosts with `grok auth device`.
  ([#38](https://github.com/raine/claude-code-proxy/pull/38))
- Grok tool calls accept Claude Code's prompt-cache markers, preventing errors
  when switching to Grok during a tool-using session.
  ([#37](https://github.com/raine/claude-code-proxy/pull/37))
- Codex hosted web searches return their result links and citations to Claude
  Code instead of appearing to produce zero results.
  ([#10](https://github.com/raine/claude-code-proxy/issues/10))
- Codex authentication refresh is coordinated across concurrent requests and
  automatically recovers live WebSocket requests after credentials expire.
- Codex requests recover more reliably from temporary upstream failures,
  connection resets, overloads, and long-running responses.

## v0.1.12 (2026-07-12)

- Codex hosted web searches work with GPT-5.6 models instead of failing with an
  unsupported tool error. ([#26](https://github.com/raine/claude-code-proxy/issues/26),
  [#35](https://github.com/raine/claude-code-proxy/pull/35))
- Codex WebSocket connection timeouts are retried automatically, reducing
  interrupted requests.

## v0.1.11 (2026-07-11)

- Grok subscriptions can power Claude Code through browser login, with support for
  Grok 4.5 and Composer 2.5 Fast, streaming, thinking, tools, and token counts.
- Codex WebSocket requests recover from handshake failures and stay marked active
  until the full response body finishes streaming.
- The monitor shows local timestamps, clearer request status and detail indicators,
  more compact columns, arrow-key pane navigation, and an uncluttered display.
- Forward Claude Code's `max` effort as Codex `reasoning.effort: "max"` so
  GPT-5.6 can use its highest supported reasoning level instead of silently
  receiving `xhigh`. ([#28](https://github.com/raine/claude-code-proxy/pull/28))

## v0.1.10 (2026-07-10)

- Claude Code requests using Opus 4.8, Sonnet 5, and Fable 5 model names can
  route through Codex

## v0.1.9 (2026-07-10)

- Claude model aliases use the matching GPT-5.6 tier through Codex: Haiku uses
  Luna, Sonnet uses Terra, and Opus uses Sol.
- GPT-5.6 Codex requests preserve reasoning context and support system guidance
  and tools through the Responses Lite API.
- The dashboard shows requested effort and resolved upstream models, making
  routing decisions easier to inspect.

## v0.1.8 (2026-07-09)

- Codex requests can use `gpt-5.6-sol`, `gpt-5.6-terra`, and `gpt-5.6-luna`,
  including `-fast` variants.
- The default Codex setup uses `gpt-5.6-sol` with `gpt-5.6-luna` as the small
  fast model and a 372K compaction window.

## v0.1.7 (2026-07-06)

- Codex `Read` tool calls get clearer offset guidance and recover from clearly
  invalid large offsets, reducing stalled sessions caused by mistaken
  line-number reads.
- The monitor keeps request lists accurate when a client disconnects or abandons
  a request.

## v0.1.5 (2026-07-03)

- Claude Code's `xhigh` and `max` effort settings now work with Codex and Kimi
  requests instead of being rejected or downgraded unexpectedly.
  ([#20](https://github.com/raine/claude-code-proxy/pull/20))
- Codex receives clearer `Read` tool guidance for line offsets, reducing
  incorrect follow-up reads on large files.
  ([#22](https://github.com/raine/claude-code-proxy/pull/22))

## v0.1.4 (2026-07-01)

- Codex WebSocket streams recover when a pooled continuation connection closes
  before the final response, retrying the turn with full context instead of
  failing the session.

## v0.1.3 (2026-07-01)

- Codex WebSocket streams deliver live text and reasoning progress while reusing
  pooled session continuations to reduce repeated upstream input.
- Codex stream recovery handles retryable startup failures, context-window
  errors, stale continuations, completed tool-call disconnects, stalled `Read`
  arguments, quiet upstream turns, and completed-turn stop reasons.
- Codex gateway requests and tool result translation use accepted payload shapes
  and preserve omitted-block markers for malformed text and image result
  content.

## v0.1.2 (2026-06-30)

- Codex WebSocket continuations recover from streams that only deliver rate
  limit or control events, preventing Claude Code sessions from waiting
  indefinitely on a stalled upstream response.

## v0.1.1 (2026-06-30)

- Codex reasoning summaries are now surfaced as thinking blocks in the response
  stream, so you can see the model's reasoning in your Claude Code session
  when reasoning effort is enabled. Set `codex.reasoningSummary` or
  `CCP_CODEX_REASONING_SUMMARY` to `off` or `none` to suppress summary display
  while keeping reasoning effort active. (Thanks @samot-gc!)
- Codex transport errors (WebSocket connection failures, etc.) now show the
  actual error message instead of a generic "Upstream error", making
  connection issues easier to diagnose.

## v0.1.0 (2026-06-30)

- Ships the native Rust implementation as the release binary.
- Adds the default monitor TUI for `serve`.
- Improves diagnostics with failed-response captures and clearer monitor
  request details.

## v0.0.22 (2026-06-24)

- Codex requests now retry more transient stream and overload failures, making temporary upstream errors less likely to interrupt Claude Code sessions. ([#15](https://github.com/raine/claude-code-proxy/issues/15))
- Codex can now recover stalled `Read` tool calls that previously left Claude Code waiting on incomplete streamed arguments.
- Cursor tool calls are recovered more reliably when Cursor returns XML-style tool use, improving compatibility with Claude Code tools.
- Cursor auth can now be isolated with `CCP_CONFIG_DIR`, so separate proxy configs can keep separate Cursor logins.
- Cursor `composer-2.5` requests now stay in non-fast mode unless fast mode is explicitly requested. ([#17](https://github.com/raine/claude-code-proxy/issues/17), [#18](https://github.com/raine/claude-code-proxy/pull/18))

## v0.0.21 (2026-06-15)

- Forced Codex web search requests now use hosted web search correctly, fixing repeated upstream `Tool choice 'function' not found in 'tools' parameter.` errors. ([#10](https://github.com/raine/claude-code-proxy/issues/10))

## v0.0.20 (2026-06-15)

- Cursor's generic `cursor`, `cursor-agent`, `cursor-plan`, and `cursor-ask` aliases now use Cursor default model selection instead of forcing Composer 2.5 fast mode.

## v0.0.19 (2026-06-14)

- Codex now supports Claude Code hosted web search through Codex's native web search, including domain filters and search usage accounting. ([#10](https://github.com/raine/claude-code-proxy/issues/10))

## v0.0.18 (2026-06-09)

- Cursor sessions now stop heartbeat traffic after streams close, reducing stray connection errors.
- Codex now treats runtime system messages as developer guidance instead of assistant output, preventing Claude Code reminders from being repeated.

## v0.0.17 (2026-06-08)

- Added Cursor Agent as a provider, including login, model selection, ask mode, plan mode, and session continuation.
- Cursor users can select models from the Cursor catalog with `cursor:<model-id>`, `cursor-plan:<model-id>`, and `cursor-ask:<model-id>` aliases.

## v0.0.16 (2026-06-02)

- Codex now uses WebSocket transport by default
- Codex sessions can opt in to append-only continuation with `previous_response_id`, reducing repeated upload size on compatible turns.
- `CCP_TRAFFIC_LOG=1` writes redacted per-request traffic captures to help debug sessions.
- Codex request logging now includes size summaries and image warnings to make compaction and large requests easier to diagnose.
- README guidance for Codex context limits and `[1m]` model suffixes is clearer.

## v0.0.15 (2026-05-30)

- Anthropic requests that omit `stream` now receive JSON responses, fixing Claude Code `/model` validation through the proxy.

## v0.0.14 (2026-05-30)

- Codex streaming now stays responsive during long `Read` tool calls by sending keepalive pings while tool arguments are buffered.
- Truncated Codex streams now return a clear error instead of appearing to finish successfully with incomplete tool calls.
- Stalled Codex requests now time out and retry when response headers never arrive, with clearer diagnostics for slow upstream responses.

## v0.0.13 (2026-05-14)

- Windows users can now download prebuilt `windows-amd64` and `windows-arm64` release archives.

## v0.0.12 (2026-05-12)

- Codex requests can now use `gpt-5.3-codex-spark` as a supported model. ([#14](https://github.com/raine/claude-code-proxy/pull/14))

## v0.0.11 (2026-05-12)

- Claude-style aliases such as `haiku`, `sonnet`, and `opus` now default to Codex while still following the provider already active in the current Claude Code session.
- Mixed Codex and Kimi sessions now keep background alias and token-count requests on the right provider instead of unexpectedly switching providers.
- Tool results with images, errors, or unsupported blocks are handled more safely, reducing malformed upstream requests.

## v0.0.10 (2026-05-06)

- Codex requests can now use `codex.serviceTier` or `CCP_CODEX_SERVICE_TIER` to request a service tier; `fast` is sent upstream as `priority`.
- Codex model names can now include `-fast`, such as `gpt-5.4-fast[1m]`, to request fast mode per request without restarting the proxy.
- Codex's upstream endpoint can now be overridden with `codex.baseUrl` or `CCP_CODEX_BASE_URL`.

## v0.0.9 (2026-05-03)

- Kimi debugging overrides now use `CCP_KIMI_OAUTH_HOST` and `CCP_KIMI_BASE_URL`, matching the proxy's `CCP_` environment variable naming.

## v0.0.8 (2026-04-30)

- Added exponential backoff retry on upstream 429 errors, respecting
  `Retry-After` headers when present
- Added `config.json` as an alternative to environment variables (read from
  `~/.config/claude-code-proxy/config.json` on macOS, XDG-compliant on Linux)
- Made the `originator` and `User-Agent` headers configurable via new env vars
  (`CCP_CODEX_ORIGINATOR`, `CCP_CODEX_USER_AGENT`, `CCP_KIMI_USER_AGENT`,
  `CCP_ORIGINATOR`, `CCP_USER_AGENT`) and the config file
- Codex now sends a default `User-Agent: claude-code-proxy/<version>` header

## v0.0.7 (2026-04-25)

- Some security hardening inspired by [#5](https://github.com/raine/claude-code-proxy/pull/5)

## v0.0.6 (2026-04-25)

- Added support for `gpt-5.5`, and `opus`/`claude-opus-4-7` aliases now map to
  `gpt-5.5` instead of `gpt-5.4`
- Model names with a `[1m]` context suffix (e.g. `gpt-5.4[1m]`) are now
  accepted and stripped before routing, so Claude Code's larger-context model
  variants work without errors
- Documented how to switch between the proxy and direct Anthropic in the README

## v0.0.5 (2026-04-22)

- Added `CCP_CODEX_MODEL` and `CCP_CODEX_EFFORT` environment variables to
  override the model and reasoning effort for Codex requests
  ([#2](https://github.com/raine/claude-code-proxy/pull/2))
- Added `claude-sonnet-4-6` and additional model aliases so more Claude-style
  model names resolve correctly
- Improved request logging with usage summaries, time-to-first-byte metrics, and
  stream completion details for easier debugging
- Client disconnections during streaming are now handled gracefully

## v0.0.4 (2026-04-20)

- Kimi: reasoning content is now preserved across turns as Anthropic thinking
  blocks, so Claude Code sees the model's thinking and multi-turn reasoning
  stays coherent
- Kimi: thinking is always enabled

## v0.0.3 (2026-04-20)

- Renamed to `claude-code-proxy` to reflect multi-provider support
- Added Kimi (kimi.com) as a provider, with device-code login via the install
  script and support for Kimi's chat models
- Requests are now routed to providers based on the requested model, so a single
  proxy can serve both Codex and Kimi models simultaneously
- Improved token counting accuracy and fixed cached token usage reporting
- Added MIT license

## v0.0.2 (2026-04-19)

- Accept Claude-style model aliases (`haiku`, `sonnet`, `opus`, and `claude-*`
  names), resolving them to the appropriate upstream model so portable configs
  and subagents work without edits
- Fix malformed streamed Read tool arguments that Claude Code would reject when
  upstream emitted an empty `pages` field

## v0.0.1 (2026-04-19)

Initial release.
