# Cursor Agent CLI Reverse Engineering — 2026.07.16-899851b

**Date:** 2026-07-20  
**Local install confirmed:** `/Users/yeauty/.local/bin/agent` → `cursor-agent` →  
`/Users/yeauty/.local/share/cursor-agent/versions/2026.07.16-899851b/`  
**Package:** `@anysphere/agent-cli-runtime` (private)  
**Ignore:** `/Users/yeauty/.grok/bin/agent` (Grok CLI, not Cursor)

**Evidence sources**

| Source | Role |
|--------|------|
| Local install tree + wrapper scripts | Paths, entry, runtime layout |
| `~/.cursor/cli-config.json`, `agent-cli-state.json` | Live config (HTTP version, ghost mode, selected model) |
| IDE `storage.json` / `machineid` | Machine id candidates (IDE telemetry path) |
| Webpack chunks under install (`*.index.js`, `index.js`) | Module graph: `@connectrpc/connect@1.6.1`, `aiserver.v1.*`, auth/login |
| [0xlane/reverse-cursor-agent](https://github.com/0xlane/reverse-cursor-agent) docs + extracted `agent_v1.proto` (CLI **2026.04.16/17**) | Full Connect/proto surface (same product line; slightly older than this install) |
| This repo `src/providers/cursor/**` | Unofficial proxy baseline for gap list |

Bundles are single-line webpack outputs; string extraction is lossy without offline `rg -o`. Cross-checks below prefer install layout + config + published CLI proto extract.

---

## 1. Install map

### 1.1 Entry points

| Path | Notes |
|------|-------|
| `/Users/yeauty/.local/bin/agent` | Bash wrapper; sets `CURSOR_INVOKED_AS` |
| `/Users/yeauty/.local/bin/cursor-agent` | Same wrapper content |
| Realdir | `~/.local/share/cursor-agent/versions/2026.07.16-899851b/` |

Wrapper behavior (`agent` / `cursor-agent`):

1. Resolve `SCRIPT_DIR` (symlink-safe).
2. Use bundled `SCRIPT_DIR/node`.
3. Enable Node compile cache:
   - macOS: `$HOME/Library/Caches/cursor-compile-cache`
   - else: `$XDG_CACHE_HOME` / `~/.cache/cursor-compile-cache`
4. Prefer `node --use-system-ca index.js` unless `AGENT_CLI_CREDENTIAL_STORE=file`.
5. `exec -a "$0" "$NODE_BIN" … "$SCRIPT_DIR/index.js" "$@"`

### 1.2 Version tree layout

```
~/.local/share/cursor-agent/
  versions/
    2026.07.16-899851b/
      package.json              # {"name":"@anysphere/agent-cli-runtime","private":true}
      node                      # bundled Node runtime
      index.js                  # main webpack bundle (very large)
      index.js.LICENSE.txt
      cursor-agent              # same bash launcher as ~/.local/bin
      cursor-agent-svc / .js    # background service bundle
      cursor-askpass / .js
      NNNN.index.js             # lazy chunks (auth, bridge, MCP, telemetry, …)
      workers: diff-worker.js, diff-patch-worker.js, unified-diff-worker.js, pdf-worker.js
      native:
        file_service.darwin-arm64.node
        merkle-tree-napi.darwin-arm64.node
        node_sqlite3.node
        pty.node
        better-sqlite3 (node_modules)
      tools: rg, crepectl, cursorsandbox, spawn-helper
```

Chunk path strings show build root similar to:

`…/agent-cli-mac-macos-arm64-<uuid>/pnpm-virtual-store/@connectrpc+connect@1.6.1_…`

So: **Connect-ES 1.6.1** + **@bufbuild/protobuf ~1.10.x** + Node HTTP/2 transport (`@connectrpc/connect-node`).

### 1.3 Config / state / cache (user machine)

| Path | Purpose |
|------|---------|
| `~/.cursor/cli-config.json` | Primary CLI config (model, privacy, network, authInfo summary) |
| `~/.cursor/agent-cli-state.json` | Small CLI state (`version`, migration flags) |
| `~/.cursor/chats/**` | ACP/chat session sqlite stores (`store.db`, `meta.json`) |
| `~/.cursor/ai-tracking/ai-code-tracking.db` | AI attribution / blame tracking |
| `~/Library/Caches/cursor-compile-cache` | Node compile cache |
| `~/Library/Application Support/Cursor/…` | **IDE** storage (not the CLI package itself) |
| IDE `User/globalStorage/storage.json` | `telemetry.machineId` / `macMachineId` / `devDeviceId` |
| IDE `machineid` | Device UUID fallback |

**Live `cli-config.json` observations (this host, 2026-07-20):**

```json
{
  "network": { "useHttp1ForAgent": false },
  "privacyCache": { "ghostMode": true, "privacyMode": 1 },
  "model": { "modelId": "claude-fable-5", "maxMode": false },
  "selectedModel": {
    "modelId": "claude-fable-5",
    "parameters": [
      { "id": "thinking", "value": "true" },
      { "id": "context", "value": "300k" },
      { "id": "effort", "value": "high" }
    ]
  }
}
```

Implications:

- Default agent transport is **HTTP/2 BiDi** (`useHttp1ForAgent: false`).
- Ghost mode is **on** for this install.
- Models use **parameterized** selection (`thinking` / `context` / `effort`), not only bare `model_id`.

### 1.4 Auth storage (official CLI)

macOS (`KeychainCredentialManager`, product name `"cursor"`):

| Item | Value |
|------|--------|
| Account | `cursor-user` |
| Service `cursor-access-token` | JWT session bearer |
| Service `cursor-refresh-token` | refresh token |
| Service `cursor-api-key` | optional API key |
| Probe | `cursor-keychain-probe` / same account (unlock check) |

Non-macOS file store:

| OS | Path |
|----|------|
| Linux | `$XDG_CONFIG_HOME/cursor/auth.json` or `~/.config/cursor/auth.json` |
| Windows | `%APPDATA%/Cursor/auth.json` |

Shape:

```json
{
  "accessToken": "eyJ…",
  "refreshToken": "eyJ…",
  "apiKey": "…",
  "bedrockCredentials": { "accessKey": "…", "secretKey": "…", "sessionToken": "…" }
}
```

Env / CLI:

- `CURSOR_API_KEY` / `--api-key` → `POST {endpoint}/auth/exchange_user_api_key` → tokens.
- Browser login: `https://cursor.com/loginDeepControl?challenge=…&uuid=…&mode=login&redirectTarget=cli` then poll `GET {base}/auth/poll?uuid=…&verifier=…`.
- Refresh: `POST {base}/auth/refresh` with bearer refresh token.
- Override store: `AGENT_CLI_CREDENTIAL_STORE=file` (wrapper also skips `--use-system-ca` in that mode).

JWT claims (session tokens): `iss=https://authentication.cursor.sh`, `aud=https://cursor.com`, `type=session`, ~60d `exp`.

**Proxy note:** `claude-code-proxy` stores Cursor tokens under its own service `claude-code-proxy.cursor`, and when that store is empty falls back to CLI Keychain (`cursor-access-token` / `cursor-user`) or `~/.config/cursor/auth.json` (`CCP_CURSOR_CLI_KEYCHAIN_FALLBACK`, default on).

---

## 2. Network identity & headers

### 2.1 Endpoints

| Role | Default |
|------|---------|
| Primary API | `https://api2.cursor.sh` (`--endpoint`) |
| Agent override | optional; may be filled from `ServerConfigService` → `AgentUrlConfig` into `cli-config.json` cache |
| Repo / indexing | `https://repo42.cursor.sh` (`--repo-endpoint`) |

Connect path form:

```text
POST {baseUrl}/{package.ServiceName}/{MethodName}
```

### 2.2 AgentService methods (CLI)

Service: **`agent.v1.AgentService`**

| Method | Kind | Purpose |
|--------|------|---------|
| `Run` | BiDi streaming | Primary agent loop |
| `RunSSE` | Server streaming | HTTP/1.1 fallback |
| `RunPoll` | Server streaming | Long-running / poll path |
| `NameAgent` | Unary | Session title |
| `CreateTranscriptOverview` | Unary | Transcript summary |
| **`GetUsableModels`** | Unary | **CLI model catalog** |
| `GetDefaultModelForCli` | Unary | Default CLI model |
| `GetAllowedModelIntents` | Unary | Internal; not usable with normal Bearer |
| `UploadConversationBlobs` / `NotifyConversationClone` / nudge RPCs | Unary | Session extras |

Related:

| Service | Use |
|---------|-----|
| `aiserver.v1.BidiService` / `BidiAppend` | Client→server appends when using RunSSE |
| `aiserver.v1.AnalyticsService` | Telemetry / Statsig bootstrap |
| `aiserver.v1.ServerConfigService` | Dynamic agent URL |
| `aiserver.v1.MetricsService` | CLI metrics buffer (present in local chunks) |
| `aiserver.v1.DashboardService` / `BackgroundComposerService` | Cloud agents / worker-debug paths in chunks |

### 2.3 Headers — official CLI interceptor (primary)

From CLI reverse docs (build series `cli-2026.04.x`; same interceptor pattern applies to `cli-2026.07.16-899851b`):

| Header | CLI value / rule |
|--------|------------------|
| `authorization` | `Bearer <accessToken>` |
| `content-type` | `application/connect+proto` (streaming) or `application/json` (some unary Connect-JSON) |
| `connect-protocol-version` / `Connect-Protocol-Version` | `1` |
| `user-agent` | `connect-es/1.6.1` |
| `x-cursor-client-type` | **`cli`** (also `extension` / `acp` for other surfaces) |
| `x-cursor-client-version` | **`cli-{version}`** e.g. `cli-2026.07.16-899851b` (+ channel suffix if not prod) |
| `x-ghost-mode` | `"true"` / `"false"` from `privacyCache.ghostMode` (**default true** in docs; true on this machine) |
| `x-request-id` | new UUID per request |
| `x-original-request-id` | set once; stable across retries |
| `x-cursor-streaming` | **`true` only on HTTP/1.1 RunSSE path** |
| Subagent lineage | `x-parent-request-id`, `x-root-parent-request-id`, `x-parent-agent-tool-call-id`, `x-direct-meta-parent-child-subagent` |

**Not listed on CLI interceptor (important):**

- `x-cursor-checksum`
- `x-cursor-client-device-type` / `os` / `arch`
- `x-cursor-client-commit`
- `x-cursor-timezone`
- `x-new-onboarding-completed`
- `x-amzn-trace-id`
- `x-client-key` / `x-session-id`

Those appear in **IDE / free-vip style** fingerprints and in this proxy’s `client.rs`, not the lean CLI interceptor list.

### 2.4 Client version string

| Surface | Format | This install |
|---------|--------|--------------|
| CLI Agent | `cli-YYYY.MM.DD-<git>` | **`cli-2026.07.16-899851b`** |
| Lab channel | `cli-…-lab` | if channel ∉ {`prod`,`prod-stable-internal`} |
| VS Code extension | `extension-{vscode}` | separate |
| Proxy default today | product-style `3.12.17` + type `ide` | **skew vs CLI** |

### 2.5 Transport

| Mode | When | Wire |
|------|------|------|
| **HTTP/2 BiDi** | default (`useHttp1ForAgent: false`, `--http-version 2`) | Connect streaming `application/connect+proto` on `AgentService/Run` |
| **HTTP/1.1 SSE** | `network.useHttp1ForAgent: true` | MethodMapper rewrites `Run` → **`RunSSE`**; sets `x-cursor-streaming: true`; later client messages via **`aiserver.v1.BidiService/BidiAppend`** (hex-encoded `AgentClientMessage`, seqno, max 16 in-flight) |

Connect frame: `flags(1) + length_be(4) + payload` (gzip bit / end bit as usual).

Library: patched **`@connectrpc/connect@1.6.1`** + **`@connectrpc/connect-node`**.

### 2.6 Checksum / machineId

**CLI headers docs do not include `x-cursor-checksum`.**

IDE / unofficial reimplementations (also in this proxy `identity.rs`) use:

```text
E = floor(Date.now() / 1e6)
raw = big-endian 6 bytes of E
A = acf(raw)   // rolling XOR/add seed 165
I = base64(A)
checksum = I + machineId + "/" + macMachineId
```

Machine ids:

| Style | Derivation |
|-------|------------|
| Token-derived (free-vip) | `sha256(token + "machineId")` hex / `…+"macMachineId"` |
| IDE storage | `telemetry.machineId` / `telemetry.macMachineId` in `storage.json` |
| Device file | `Application Support/Cursor/machineid` UUID |

CLI may still compute host machine ids for analytics / sandbox (`host-machine-id` modules appear in bridge chunks) without putting them on every AgentService header.

---

## 3. Models

### 3.1 How the CLI lists models

Primary CLI RPC:

```http
POST /agent.v1.AgentService/GetUsableModels
Content-Type: application/json
Connect-Protocol-Version: 1
Authorization: Bearer <token>
x-cursor-client-version: cli-2026.07.16-899851b
x-cursor-client-type: cli
x-ghost-mode: false|true

{}
```

Optional request field: `custom_model_ids[]`.

Response: `GetUsableModelsResponse { repeated ModelDetails models = 1 }`.

Also:

- `GetDefaultModelForCli` → empty body; historically default like `composer-2-fast` (version-dependent).
- IDE-rich catalog: `AvailableModels` (more fields: context limits, supportsAgent/Thinking/MaxMode). Not the CLI list path.

### 3.2 Selection on wire

`RequestedModel` (agent.v1):

```protobuf
message RequestedModel {
  string model_id = 1;
  bool max_mode = 2;
  repeated ModelParameterValue parameters = 3;
  // credentials oneofs …
  bool built_in_model = 7;
  bool is_variant_string_representation = 8;
}
```

Local CLI config already sends **parameters**:

| id | example |
|----|---------|
| `thinking` | `true` |
| `context` | `300k` |
| `effort` | `high` |

Effort is often also baked into catalog ids (`-low` / `-medium` / `-high` / `-xhigh` / `-max` / `-fast` / `-thinking`).

### 3.3 Proxy model path today

- Static legacy names + aliases (`fable` → `claude-fable-5`, etc.).
- Sends `requested_model.model_id` with **empty `parameters`** and `max_mode=None`.
- Does **not** call `GetUsableModels`.
- README claim “catalog from `cursor-agent --list-models`” is not implemented in code.

---

## 4. RunRequest / stream protocol (agent.v1)

### 4.1 AgentClientMessage (client → server)

```protobuf
message AgentClientMessage {
  oneof message {
    AgentRunRequest          run_request                 = 1;
    ExecClientMessage        exec_client_message         = 2;
    KvClientMessage          kv_client_message           = 3;
    ConversationAction       conversation_action         = 4;
    ExecClientControlMessage exec_client_control_message = 5;
    InteractionResponse      interaction_response        = 6;
    ClientHeartbeat          client_heartbeat            = 7;  // empty; ~every 5s
    PrewarmRequest           prewarm_request             = 8;
  }
}
```

### 4.2 AgentServerMessage (server → client)

```protobuf
message AgentServerMessage {
  oneof message {
    InteractionUpdate          interaction_update             = 1;
    ExecServerMessage          exec_server_message            = 2;
    ConversationStateStructure conversation_checkpoint_update = 3;
    KvServerMessage            kv_server_message              = 4;
    ExecServerControlMessage   exec_server_control_message    = 5;
    InteractionQuery           interaction_query              = 7;
  }
}
```

### 4.3 AgentRunRequest (opening payload)

Extracted from CLI bundle (2026.04 series; 2026.07 may add trailing fields):

```protobuf
message AgentRunRequest {
  ConversationState conversation_state = 1;
  Action action = 2;
  ModelDetails model_details = 3;
  McpTools mcp_tools = 4;
  optional string conversation_id = 5;
  optional McpFileSystemOptions mcp_file_system_options = 6;
  optional SkillOptions skill_options = 7;
  optional string custom_system_prompt = 8;
  optional RequestedModel requested_model = 9;
  optional bool suggest_next_prompt = 10;
  optional string subagent_type_name = 11;
  optional bool exclude_workspace_context = 12;
  optional string harness = 13;
  repeated … selected_subagent_models = 14;
  … selected_subagent_model_details = 15;
  optional string conversation_group_id = 16;
  repeated BlobEntry pre_fetched_blobs = 17;
  optional string dev_raw_model_slug = 18;
  // 2026.07 may include additional optional fields (e.g. image flags) — verify against live captures
}
```

`ConversationState` is a **rich** structure (turns, todos, file_states, summary, plans, …), not a void message.

`custom_system_prompt` (field 8) is durable vs summarization (turns get compressed; system field less so).

### 4.4 InteractionUpdate tags (critical)

Official CLI extract:

| Tag | Field |
|-----|-------|
| 1 | `text_delta` |
| 2 | `tool_call_started` |
| 3 | `tool_call_completed` |
| 4 | `thinking_delta` |
| 5 | `thinking_completed` |
| … | … |
| 13 | `heartbeat` |
| **14** | **`turn_ended`** |
| 15+ | tool_call_delta, step_*, prompts, … |

### 4.5 Heartbeats & stall

| Signal | Interval / rule |
|--------|-----------------|
| `client_heartbeat` (AgentClientMessage tag 7) | ~**5s** while stream open |
| Exec control heartbeat during tool exec | ~**3s** |
| Server `InteractionUpdate.heartbeat` | keep-alive |
| Client stall detect | ~**30s** silence → reconnect / resume |
| Tool loop free within same stream | multiple model iterations until `turn_ended` |

### 4.6 Tools (native)

Server drives tools via **`exec_server_message`** (not text XML). Client returns **`exec_client_message`** with matching `id` / `exec_id`.

ToolCall oneof includes ~40 tools: Shell, Read, Edit, Grep, Glob, Delete, Ls, MCP, WebSearch/Fetch, Task/subagent, AskQuestion, ComputerUse, etc.

KV blob get/set is a parallel sub-protocol on the same BiDi stream.

Approvals / web / plan / MCP auth use **`interaction_query`** / **`interaction_response`**.

### 4.7 Typical message flow

```
Client                              Server
  │── run_request (state+action) ──→ │
  │←── interaction_update (text/thinking/tools)
  │←── exec_server_message (e.g. shell/read)
  │── exec_client_message (result) ─→ │
  │── exec_client_control (hb/close)
  │←── conversation_checkpoint_update
  │── client_heartbeat (periodic)
  │←── interaction_update.turn_ended
  stream close
```

Optional: `prewarm_request` then later `conversation_action` on same stream.

Resume after disconnect: latest checkpoint + `ResumeAction`.

---

## 5. What the unofficial proxy does today (baseline)

| Area | Proxy (`src/providers/cursor`) |
|------|--------------------------------|
| Endpoint | `POST …/agent.v1.AgentService/Run` only |
| HTTP | **Default HTTP/1.1** (`CCP_CURSOR_HTTP1` default true) |
| Body | **Fully buffered** then framed → Anthropic SSE |
| Timeout | **45s** hard |
| Client type/version | default **`ide` / `3.12.17`** |
| Headers | IDE-heavy set + token checksum |
| Proto | Hand-written prost subset |
| Heartbeat | field exists, **never sent** |
| BiDi / BidiAppend | **absent** |
| Tools | XML-in-text pause; result messages **not** re-sent upstream |
| conversation_id | always `None` |
| Models | local aliases; no `GetUsableModels` |
| Auth | proxy keychain `claude-code-proxy.cursor` + browser poll |

### 5.1 Proto mismatches vs CLI extract

| Item | CLI extract | Proxy prost |
|------|-------------|-------------|
| `InteractionUpdate.text_delta` | **1** | **2** |
| `InteractionUpdate.thinking_delta` | **4** | **1** |
| `InteractionUpdate.turn_ended` | **14** | **3** |
| `AgentServerMessage.exec_server_message` | **2** | **3** (tag 3 is checkpoint in CLI) |
| `AgentClientMessage` tools/KV/actions | full oneof 1–8 | only run_request + heartbeat |
| `ConversationState` | rich | empty message |
| `RequestedModel.parameters` | used by real CLI config | always `[]` |

If live 2026.07 still matches the 2026.04 extract, **response decoding is systematically wrong** for thinking/text/turn_ended and tool frames. Tests only prove mock↔proxy consistency.

---

## 6. Structured gap list vs minimal “match official CLI” client

Ordered for an unofficial proxy that wants **AgentService/Run parity**, not full IDE feature clone.

### P0 — must for real agent turns

1. **HTTP/2 BiDi `AgentService/Run` as primary**  
   Match CLI default (`useHttp1ForAgent: false`). Keep HTTP/1 `RunSSE`+`BidiAppend` as fallback.

2. **True streaming of Connect frames**  
   Decode frames as they arrive; emit Anthropic SSE incrementally. Stop full-body buffer-before-first-token.

3. **Timeouts: connect vs idle**  
   Drop 45s whole-request kill. Use long-lived stream + idle stall (~30s) + heartbeats.

4. **Client heartbeats**  
   Send `AgentClientMessage.client_heartbeat` (~5s). During tools, exec control heartbeats (~3s).

5. **Correct InteractionUpdate / AgentServerMessage field tags**  
   Align with CLI `agent_v1` extract (text=1, thinking=4, turn_ended=14, exec=2, …). Re-verify on 2026.07 live capture.

6. **CLI fingerprint defaults**  
   `x-cursor-client-type=cli`, `x-cursor-client-version=cli-2026.07.16-899851b` (or discover from install path). Ghost mode from config/default **true** unless user opts out. Prefer lean CLI header set over IDE kitchen-sink.

7. **Duplex tool re-entry**  
   Decode `exec_server_message`; execute or bridge; send `exec_client_message` on **same** stream. Do not discard result builders.

8. **`conversation_id` + conversation state**  
   Map Claude session → Cursor conversation id; persist checkpoints / session ids; stop always-empty state if multi-turn fidelity matters.

### P1 — multi-turn / catalog / fidelity

9. **`GetUsableModels` (+ optional `GetDefaultModelForCli`)** for `/v1/models` and validation.

10. **`RequestedModel.parameters` + `max_mode`**  
    Map Claude effort / thinking / context onto CLI parameter ids (`thinking`, `context`, `effort`) or catalog suffixes.

11. **Native tool surface**  
    Minimum Read/Write/Shell (+ Grep/LS) exec protos; MCP tools field when advertising tools.

12. **KV + checkpoint handling**  
    Respond to get/set blob; store conversation checkpoints for resume.

13. **Auth import path**  
    Optional: read CLI Keychain (`cursor-user` / `cursor-access-token`) or `auth.json`, in addition to proxy-owned login.

14. **HTTP/1 fallback stack**  
    `RunSSE` + `BidiAppend` with hex payload + seqno when forced by proxy/Clash.

### P2 — polish / edge

15. **Prewarm**, `interaction_query` approvals, subagent lineage headers, analytics/metrics optional.

16. **Workspace / request_context**  
    Real CLI fills env, repo info, rules, project layouts via exec `request_context` — empty context is a quality gap.

17. **Checksum policy**  
    Confirm whether CLI path accepts/requires `x-cursor-checksum`; avoid assuming free-vip header set is mandatory for CLI type.

18. **Proto generation from local bundle**  
    Automate extract from `2026.07.16-899851b` (like reverse-cursor-agent scripts) so tags stay current.

---

## 7. Top 15 gaps (concise) — unofficial proxy vs official CLI

| # | Gap | Official CLI | Proxy now | Impact |
|---|-----|--------------|-----------|--------|
| 1 | Transport | HTTP/2 BiDi `Run` default | HTTP/1 buffered `Run` | Broken long runs; no duplex |
| 2 | Streaming | Frame-by-frame | Buffer entire body | “Hang” until finish / timeout |
| 3 | Heartbeat | client 5s + exec 3s | None | Edge/proxy idle drops |
| 4 | Timeout model | Stall ~30s, multi-min turns | 45s hard | False failures |
| 5 | Client type | `cli` | `ide` | Fingerprint skew / rate quirks |
| 6 | Client version | `cli-2026.07.16-899851b` | `3.12.17` | Same |
| 7 | Header set | Lean CLI interceptor | IDE + checksum + extra | Mismatch risk |
| 8 | Ghost mode default | true (docs + this config) | false | Privacy/rate behavior |
| 9 | Response proto tags | text=1, think=4, end=14, exec=2 | wrong tags | Mis-decode / silent drops |
| 10 | Tool protocol | Native exec BiDi | XML text + no upstream results | Not a real agent loop |
| 11 | conversation_id / state | Full + checkpoints | always empty / None | No resume / multi-turn |
| 12 | Model list | `GetUsableModels` | Static aliases | Catalog drift |
| 13 | Model parameters | thinking/context/effort | bare model_id | Wrong effort/context |
| 14 | Auth storage | Keychain `cursor-user` / auth.json | separate proxy store | Double login |
| 15 | H1 fallback | RunSSE + BidiAppend | not implemented | Proxy environments |

---

## 8. Recommended parity profile (config sketch)

```json
{
  "cursor": {
    "baseUrl": "https://api2.cursor.sh",
    "clientType": "cli",
    "clientVersion": "cli-2026.07.16-899851b",
    "ghostMode": true,
    "httpVersion": 2,
    "useHttp1ForAgent": false
  }
}
```

Discover version from:

```text
basename "$(dirname "$(realpath ~/.local/bin/agent)")"
# → 2026.07.16-899851b  →  prefix with cli-
```

---

## 9. Local artifact checklist (repro)

```bash
# Entry
ls -la ~/.local/bin/agent ~/.local/bin/cursor-agent
realpath ~/.local/bin/agent

# Install
ls ~/.local/share/cursor-agent/versions/2026.07.16-899851b | head

# Config
jq '{network,privacyCache,model,selectedModel}' ~/.cursor/cli-config.json

# Keychain (do not paste secrets into logs)
security find-generic-password -a "cursor-user" -s "cursor-access-token" -g 2>&1 | head

# Optional: extract strings from bundles
python3 claudedocs/_extract_cursor_cli.py
```

Helper script (optional): `claudedocs/_extract_cursor_cli.py` scans local `*.js` for protocol strings when run offline.

---

## 10. References

- Local: `~/.local/share/cursor-agent/versions/2026.07.16-899851b/`
- Local config: `~/.cursor/cli-config.json`
- Proxy: `src/providers/cursor/{client,auth,identity,proto,model,mod}.rs`
- Prior audit: `claudedocs/cursor-proxy-gap-audit-2026-07.md`
- External reverse (2026.04 CLI): [0xlane/reverse-cursor-agent](https://github.com/0xlane/reverse-cursor-agent) docs `00–12`, `docs/proto/agent_v1.proto`

---

*End of reverse report. Implementation intentionally out of scope for this document.*
