# claude-cursor-proxy

**[中文](README.zh-CN.md) | English**

[![CI](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/ci.yml)
[![Release](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/release.yml/badge.svg)](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/release.yml)
[![GitHub Release](https://img.shields.io/github/v/release/YeautyYE/claude-cursor-proxy?display_name=tag)](https://github.com/YeautyYE/claude-cursor-proxy/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Platforms](https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey)](https://github.com/YeautyYE/claude-cursor-proxy/releases)

Adapted from [raine/claude-code-proxy](https://github.com/raine/claude-code-proxy). This project is a **Cursor-first** local **proxy** for [Claude Code](https://docs.anthropic.com/en/docs/claude-code) and [Grok Build](https://x.ai/cli) (`grok` / grok-build).

**Run Cursor models (Fable 5) from Claude Code or grok-build — stably.**

```
Claude Code ──Anthropic /v1/messages──► claude-cursor-proxy (:18765)
grok-build  ──Responses / Messages ──►        │
                                              ├── Cursor (Fable 5)   ← primary
                                              ├── Codex             ← additional
                                              ├── Kimi
                                              └── Grok
```

[Quick start](#quick-start) · [Models](#models) · [Sand mode](#sand-mode) · [Features](#features) · [Config](#configuration) · [MCP troubleshooting](#mcp-troubleshooting) · [Limitations](#limitations)

---

## What it does

Claude Code speaks Anthropic (`/v1/messages`). grok-build speaks OpenAI Responses (`/v1/responses`). Cursor uses its own Agent protocol. They do not talk to each other directly.

This tool runs a local one-way proxy (default `127.0.0.1:18765`):

1. Claude Code or grok-build send their usual requests to the proxy
2. The proxy translates them for Cursor and forwards upstream
3. It streams the matching SSE back — Anthropic `ping` keep-alive for Claude Code, Responses events for grok-build

Primary upstream: **Cursor (Fable 5)**. Additional backends in the same process: Codex, Kimi, Grok.

> Not affiliated with Anthropic, Cursor, OpenAI, Moonshot, or xAI.

---

## Why

| | |
| --- | --- |
| **Stable sessions** | HTTP/2 BiDi upstream + Anthropic `ping` SSE keep-alive downstream |
| **Fable 5** | Set `ANTHROPIC_MODEL=claude-fable-5[1m]` (and the same for `ANTHROPIC_SMALL_FAST_MODEL`) |
| **Usage / ctx** | Cursor turn usage mapped onto Anthropic `usage` for status lines and compaction |
| **Tools** | Cursor exec / native tools remapped into Claude Code and grok-build tool loops (best-effort) |
| **Simple install** | Checksummed binaries; macOS ad-hoc codesign; config under `~/.config/claude-cursor-proxy` |

Honest scope: best-effort compatibility — **not** a full Cursor IDE mirror. See [Limitations](#limitations).

---

## Quick start

### Install

```bash
curl -fsSL https://raw.githubusercontent.com/YeautyYE/claude-cursor-proxy/main/install.sh | bash
```

macOS / Linux. Windows: download the `.zip` from [Releases](https://github.com/YeautyYE/claude-cursor-proxy/releases) (or use WSL).

<details>
<summary>Other install options</summary>

| Method | Command |
| --- | --- |
| Pin version | `CLAUDE_CURSOR_PROXY_VERSION=v0.1.86 curl -fsSL …/install.sh \| bash` |
| Custom dir | `CLAUDE_CURSOR_PROXY_INSTALL_DIR=/opt/bin bash install.sh` |
| From source | `cargo install --git https://github.com/YeautyYE/claude-cursor-proxy --locked` |
| Fork / mirror | `GITHUB_REPO=owner/repo curl -fsSL https://raw.githubusercontent.com/owner/repo/main/install.sh \| bash` |

</details>

### Log in + serve

```bash
claude-cursor-proxy cursor auth login
claude-cursor-proxy serve                 # 127.0.0.1:18765 + monitor TUI
claude-cursor-proxy serve --no-monitor    # logs only
claude-cursor-proxy serve --port 11435    # custom port
```

#### Hot account switch (no restart)

Run `claude-cursor-proxy cursor auth login` in another terminal while `serve`
is running. Credentials are re-read from the store on every request, so:

- new requests use the new account immediately;
- in-flight runs keep the token they captured at start and finish on the
  previous login — nothing is interrupted;
- existing sessions start a fresh Cursor conversation on their next turn
  (the client resends its history automatically).

`cursor auth status` shows the active account. Note: if
`CCP_CURSOR_AUTH_TOKEN`/`CURSOR_AUTH_TOKEN` is set in the `serve` process's
environment, the env token shadows the store and a login hot swap will not
take effect until you unset it.

#### Multiple Cursor accounts

Use `login` when you want the newly authenticated account to become active.
Use `add` when you want to keep the current account active and append another
login to the local account pool:

```bash
claude-cursor-proxy cursor auth add --label work
claude-cursor-proxy cursor auth list
claude-cursor-proxy cursor auth use ACCOUNT_ID
claude-cursor-proxy cursor auth usage                 # every saved account
claude-cursor-proxy cursor auth usage ACCOUNT_ID      # one account
claude-cursor-proxy cursor auth usage --json          # machine-readable
```

`ACCOUNT_ID` may be the id printed by `list`, an unambiguous email, or a label.
The pool is stored in `cursor/accounts.json`; the selected credential remains
mirrored to the existing `cursor/auth.json` so older installations continue to
work. In the monitor TUI, press `a` to open the account panel, `Enter` to
switch, `u` to fetch the selected account, and `U` to fetch every account in
parallel. Adding or switching accounts does not require restarting `serve`.

### Point Claude Code at the proxy (Fable 5)

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:18765
export ANTHROPIC_AUTH_TOKEN=unused
export ANTHROPIC_MODEL=claude-fable-5[1m]
export ANTHROPIC_SMALL_FAST_MODEL=claude-fable-5[1m]
export CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1
export CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1
claude
```

Same keys work under `"env"` in `~/.claude/settings.json`.

Always set `ANTHROPIC_SMALL_FAST_MODEL` to a full model id (same as `ANTHROPIC_MODEL` is fine). Otherwise Claude Code’s background small-model calls return HTTP 400.

<details>
<summary>Codex / Kimi / Grok</summary>

```bash
claude-cursor-proxy codex auth login
ANTHROPIC_BASE_URL=http://127.0.0.1:18765 ANTHROPIC_AUTH_TOKEN=unused \
  ANTHROPIC_MODEL=gpt-5.6-sol[1m] ANTHROPIC_SMALL_FAST_MODEL=gpt-5.6-luna[1m] \
  CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1 CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1 \
  claude

claude-cursor-proxy kimi auth login   # or: grok auth login
```

</details>

### Point grok-build at the proxy

Do **not** set `GROK_CLI_CHAT_PROXY_BASE_URL` to this process. That env is for xAI’s official chat-proxy. grok-build talks to this proxy as a normal custom `base_url`.

**1. Log in (once) and start the proxy**

```bash
claude-cursor-proxy grok auth login      # required for grok-4.5 / grok-4.6 chat
claude-cursor-proxy cursor auth login    # only if you also route Fable / Composer
claude-cursor-proxy serve                # 127.0.0.1:18765
```

**2. Edit `~/.grok/config.toml`** — override the official ids so Fast + effort menus stay enabled. Fast is `reasoning.effort = "low"`.

```toml
# ~/.grok/config.toml

[model.grok-4.6]
base_url = "http://127.0.0.1:18765/v1"
api_key = "unused"

[model.grok-4.5]
base_url = "http://127.0.0.1:18765/v1"
api_key = "unused"

# Cursor catalog via grok-build (official OpenAI Responses, not Claude Messages)
[model.cursor-grok]
model = "cursor-grok-4.6-xhigh-fast"
base_url = "http://127.0.0.1:18765/v1"
api_backend = "responses"
api_key = "unused"

# Optional: Claude Code-style Anthropic Messages (not the grok-build default)
[model.via-ccp]
model = "claude-fable-5[1m]"
base_url = "http://127.0.0.1:18765/v1"
api_backend = "messages"
context_window = 1000000
api_key = "unused"
supports_reasoning_effort = true
reasoning_effort = "high"

# Optional: image / video tools (global URL, not the model base_url)
[endpoints]
xai_api_base_url = "http://127.0.0.1:18765/v1"
```

**3. Run grok-build**

```bash
grok --model grok-4.6
# or: grok --model grok-4.5
# or: grok --model cursor-grok
# or: grok --model via-ccp
```

Inbound `api_key` is accepted (`Authorization: Bearer …` or `x-api-key`; `unused`, other placeholders, and JWT-looking session tokens are treated as empty) and is **not** used as a user/tenant id. Grok `/v1/responses` passthrough forwards conversation, compaction (`x-compaction-at`, `x-compactions-remaining`), doom-loop, and a charset-limited `x-grok-model-override` — never `Authorization`, `Cookie`, or `x-grok-user-id`.

`GET /v1/models` advertises `model`, `context_window`, `api_backend=responses` (grok-build's official OpenAI Responses backend), `supports_reasoning_effort`, and `reasoning_efforts` (grok-4.6 includes `xhigh` / `high` / `medium` / `low`). A custom `[model.*]` block that omits `api_backend` defaults to Chat Completions in grok-build; set `api_backend = "responses"`. This proxy does not implement `/v1/chat/completions`. `/v1/messages` remains for Claude Code.

Media routes (`/v1/images/*`, `/v1/videos/*`) proxy to `https://api.x.ai/v1` (override with `CCP_GROK_MEDIA_BASE_URL`). A real client key is forwarded; placeholders and grok-build session JWTs fall back to the stored Grok OAuth token.

---

## Models

Set `ANTHROPIC_MODEL` and `ANTHROPIC_SMALL_FAST_MODEL` to **full model ids**. Recommended Cursor default: `claude-fable-5[1m]`.

Other backends use their own full ids (for example `gpt-5.6-sol[1m]`, `kimi-for-coding`, `grok-composer-2.5-fast`). Unknown ids return **400**.

### How to list supported models

```bash
# Built-in registry
claude-cursor-proxy models
claude-cursor-proxy models --full

# While serve is running — Anthropic-compatible list
# (merges Cursor GetUsableModels when logged in + registry)
curl -s http://127.0.0.1:18765/v1/models | jq '.data[].id'
```

## Sand mode

Sand is a separate Cursor request surface selected **per model**. For a model
that matches the Sand policy, the proxy sends Cursor's
`x-cursor-client-type: sand`; other Cursor models keep the normal `cli` (or
your configured `CCP_CURSOR_CLIENT_TYPE`) identity. Mixed routing happens in
the same `claude-cursor-proxy serve` process; a second Sand binary is not
needed.
The policy applies only to requests resolved to the Cursor provider; Codex,
Kimi, and Grok routes are unchanged.

> **Recommended: configure Sand from the monitor TUI.** Keep one `serve`
> process running and use the shortcuts below; this avoids hand-editing
> configuration files and makes the active request type visible immediately.

| Key | Action |
| --- | --- |
| `s` | Open **Sand Models** and select the model list |
| `j` / `k` | Move through models |
| `Space` / `Enter` | Toggle the selected model between `[sand]` and `[cli]` |
| `a` | Add a model id manually (for example `claude-fable-5`) |
| `u` | Open the account-usage view |
| `a` (main view) | Open the Cursor account panel |
| `Esc` / `s` | Close the Sand editor |

### Fast setup

```bash
claude-cursor-proxy cursor auth login
claude-cursor-proxy serve              # keep the monitor TUI open
```

In the monitor TUI, press `s` to open **Sand Models**. Use `j`/`k` to select a
model, `Space` or `Enter` to toggle it, and `a` to enter an exact Cursor
catalog id such as `claude-fable-5`. From the model list, press `u` to inspect
account usage. The list is marked `[sand]` or `[cli]`; changes apply to new
requests and are written atomically to `config.json`. The TUI requires a
terminal; `serve --no-monitor` keeps the proxy running without it.

Cursor model cells in the Sessions, Active requests, Recent requests, and
Events panes carry the same `[sand]`/`[cli]` badge, so the selected request
surface is visible without opening the editor. Fable aliases are resolved
before matching: a rule for `claude-fable-5-thinking-max` also covers the
usual `claude-fable-5[1m]`, `fable[1m]`, and `cursor:` forms.

This TUI flow is the recommended way to manage Sand routing. You do not need
to edit a file or launch another binary; the running `serve` process picks up
the saved policy for the next request. Environment variables and
`config.json` are fallback interfaces for automation, not the normal setup
path.

### Use the selected model

After enabling a model with `s`, point Claude Code at that model:

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:18765
export ANTHROPIC_AUTH_TOKEN=unused
export ANTHROPIC_MODEL="claude-fable-5"
export ANTHROPIC_SMALL_FAST_MODEL="claude-fable-5"
claude
```

For a temporary shell/session or automation, `CCP_CURSOR_SAND_MODELS` can
override the TUI policy:

```bash
export CCP_CURSOR_SAND_MODELS="claude-fable-5"
```

To add an exact Cursor catalog id that is not in the current list, press `a`
inside **Sand Models** and enter it directly; for example,
`claude-fable-5`.

`CCP_CURSOR_SAND_MODELS` is a comma-separated list and supports `*` and `?`.
Model matching is case-insensitive and normalizes `[1m]` plus
`cursor:`/`cursor-agent:`/`cursor-plan:`/`cursor-ask:` prefixes, so a rule for
`claude-fable-5` also covers `cursor:claude-fable-5[1m]`. An environment value
always overrides `cursor.sandModels` in `config.json`; unset it to edit the
file from the TUI. Leave `CCP_CURSOR_CLIENT_TYPE` at its default `cli` when you
want mixed routing; setting it to `sand` makes unmatched models use Sand too.
An explicitly empty `CCP_CURSOR_SAND_MODELS=` disables all Sand matches until
the variable is unset.

### Model discovery

The built-in Cursor catalog is only an offline/startup fallback. With a Cursor
login, the proxy fetches `GetUsableModels` at startup and refreshes it when
`GET /v1/models` is requested. The returned account catalog is merged into the
TUI and the model list. You can still add an exact id with `a` or set it in the
environment, but the signed-in Cursor account must expose that model upstream.
The catalog is invalidated on a hot account switch/logout, and an in-flight
response from the previous account is discarded.

### Account usage

The monitor polls Cursor's read-only dashboard endpoints and shows the signed-in
account, plan, Auto/API percentages, on-demand dollars, dashboard cost/event
totals, and the Sand/Grok Bot period meter when the account provides it. Press
`u` for the multi-line usage view, including the Sand period and recent usage
events. `cursor auth status` shows the active login. On macOS, the monitor can
fall back to Cursor Desktop's read-only `state.vscdb`; missing dashboard fields
are omitted rather than invented. In headless `serve --no-monitor`, a lightweight
poller requests only the Sand meter once per minute so an exhausted Sand turn
can still be reported as HTTP 429; usage display remains a TUI feature.

```bash
claude-cursor-proxy cursor auth status
```

---

## Features

- Anthropic surface: `POST /v1/messages`, `count_tokens`, `/healthz`, `/v1/models`
- grok-build surface: `POST /v1/responses`, `POST /v1/images/generations`, `POST /v1/images/edits`, `POST /v1/videos/generations`, `GET /v1/videos/{id}`
- Cursor Agent Connect (BiDi `Run`); optional HTTP/1 via `CCP_CURSOR_HTTP1=1`
- SSE keep-alive (`ping`) so quiet thinking does not look stalled
- Model routing by `ANTHROPIC_MODEL`
- Auth stored by the proxy; Cursor can fall back to Cursor Agent Keychain / `auth.json`
- Monitor TUI when stdout is a TTY (`demo` for a simulated UI)

---

## Configuration

For interactive Sand routing and usage inspection, prefer the monitor TUI:
`s` selects Sand models, `a` adds an exact model id, and `u` opens account
usage. The file and environment settings below are primarily for headless or
automated deployments.

Precedence: **env > `config.json` > defaults**.

| Platform | Path |
| --- | --- |
| macOS / Linux | `~/.config/claude-cursor-proxy/config.json` |
| Windows | `%APPDATA%\claude-cursor-proxy\config.json` |

Override with `CCP_CONFIG_DIR`. Env prefix stays **`CCP_*`** (unchanged from earlier builds). Provider auth files under previous paths (`~/.config/claude-cursor-bridge/`, `~/.config/claude-code-proxy/`) are still read as a migration fallback.

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `18765` | Listen port |
| `CCP_BIND_ADDRESS` | `127.0.0.1` | Bind address |
| `CCP_ADVERTISED_MODELS` | unset | Optional comma-separated allowlist for `GET /v1/models` (useful for managed desktop model pickers) |
| `CCP_CURSOR_AUTH_TOKEN` | unset | Cursor bearer override |
| `CCP_CURSOR_BASE_URL` | `https://api2.cursor.sh` | Cursor API base |
| `CCP_CURSOR_CLIENT_TYPE` | `cli` | Default `x-cursor-client-type` value |
| `CCP_CURSOR_SAND_MODELS` | unset | Comma-separated model selectors routed with `x-cursor-client-type: sand`; supports `*` and `?` |
| `CCP_CURSOR_STATE_DB` | Cursor Desktop state path on macOS | Optional read-only state.vscdb path used by the TUI usage fallback |
| `CCP_CURSOR_HAIKU_MODEL` | `claude-haiku-4-5` | Cursor catalog id used for Anthropic `haiku` aliases and desktop small-model probes |
| `CCP_CURSOR_CLI_KEYCHAIN_FALLBACK` | on | Disable with `0` / `false` |
| `CCP_CURSOR_EMBED_SYSTEM` | off | Forward Anthropic `system` into Cursor user text (can trigger Fable injection loops) |
| `CCP_CURSOR_FORCE_TOOLS_IN_PROMPT` | off | Dump **all** tool schemas (large); BiDi already keeps Claude-local tools (`Workflow`/`Skill`/…) |
| `CCP_CURSOR_LIVE_CONCURRENCY` | `1024` | Fair cap for bulk (`cursor-grok-*`) generation starts (1–8192) |
| `CCP_CURSOR_LIVE_RUNS` | `4096` | Process-wide cap for live requests holding a Run slot (1–16384); generation-start capacity is controlled separately by `CCP_CURSOR_LIVE_CONCURRENCY` |
| `CCP_CURSOR_LIVE_INTERACTIVE_RESERVE` | `128` | Protected start capacity for non-Grok models (Gemini/Claude/… subagents); interactive starts may also borrow idle bulk slots, but bulk never borrows the reserve (0–1024) |
| `CCP_CURSOR_LIVE_QUEUE_SECS` | `30` | Maximum local admission wait before retryable HTTP 503 (1–300s) |
| `CCP_CURSOR_LIVE_ATTACH_WAIT_MS` | `15000` | Same-operation attach handoff wait before local busy is returned (500–60000ms) |
| `CCP_CURSOR_LIVE_RESUME_ATTACH_WAIT_MS` | `4000` | Pre-response same-operation attach wait (500–5000ms); kept below the Claude Code stream watchdog |
| `CCP_CURSOR_LIVE_CONFLICT_WAIT_MS` | `180000` | Wait for a different operation to observe the current session Run advance (500–600000ms) |
| `CCP_CURSOR_LIVE_RESUME_WAIT_MS` | `5000` | Pre-response tool-result handoff wait; kept below the client stream watchdog (500–5000ms) |
| `CCP_CURSOR_LIVE_NESTED_WAIT_MS` | `1500` | Pre-response nested-agent handoff wait (500–5000ms) |
| `CCP_CURSOR_RESOURCE_RETRIES` | `6` | Same-request retries for transient Cursor `ERROR_RESOURCE_EXHAUSTED` responses (1–12); billing/quota/capacity policy 429s are never hidden-retried |
| `CCP_CURSOR_POLICY_429_COOLDOWN_SECS` | `30` | Local cooldown after an account/model/Sand-or-CLI policy 429; fresh requests on that exact route fail fast with HTTP 429 + `Retry-After` (5–600s) |
| `CCP_CURSOR_POLICY_429_PROBE_WINDOW_MS` | `30000` | Cold account/model/route single-flight window: useful output releases the wave immediately; a quiet expiry admits only one additional probe rather than fanning out all retries (25–120000ms) |
| `CCP_CURSOR_STEP_FAILURE_RETRIES` | `4` | Same-request retries for pre-output Cursor `Failed to run step, exceeded max retries` failures (1–8); post-output failures are forwarded |
| `CCP_CURSOR_LIVE_RESUME_RESERVE` | `64` | Additional capacity reserved for paused Runs that need to submit tool results (0–512) |
| `CCP_CURSOR_OPERATION_LEDGER` | off | Opt-in durable operation ledger (crash-safe replay refusal). Stays off by default until completion is gated on downstream delivery, so dropped responses cannot permanently refuse client retries |
| `CCP_CURSOR_LIVE_TIMEOUT_SECS` | `1800` | Active model-generation budget for each live segment (max 3600s; paused while downstream tools run) |
| `CCP_CURSOR_TOOL_TTL_SECS` | same as live timeout | Maximum wait after a tool batch reaches the downstream client; an admitted result is allowed to finish dispatch |
| `CCP_CURSOR_HEARTBEAT_PROGRESS_SECS` | `1200` | Maximum heartbeat-only thinking period without model progress |
| `CCP_CURSOR_GEMINI_FLASH_PROGRESS_SECS` | `180` | Gemini Flash heartbeat-only progress deadline; a hollow pre-output run is rotated and retried inside the same client request instead of appearing stuck |
| `CCP_CURSOR_H2_SHARDS` | `16` | Stable H2 client pools used to isolate concurrent conversations (1–64) |
| `CCP_CURSOR_LIVE_RECOVERY_OPENS` | `16` | Process-wide cap for simultaneous ResumeAction replacement opens (1–128) |
| `CCP_ANTHROPIC_SSE_PING_SECS` | `5` | SSE heartbeat interval (message_delta + ping; keep below Claude Code's 10s stream watchdog) |
| `CCP_CURSOR_NO_PROXY` | off | Skip HTTP(S)_PROXY for Cursor API (`1` / `true`) |
| `CCP_LOG_STDERR` / `CCP_LOG_VERBOSE` / `CCP_TRAFFIC_LOG` | unset | Debug |

### Claude Code (client) env / settings

These are Claude Code knobs (not proxy config). Useful when `/deep-research` or ToolSearch misbehaves through a custom `ANTHROPIC_BASE_URL`:

| Variable / setting | Purpose |
| --- | --- |
| `enableWorkflows: true` (settings) | Force Workflows on if your plan defaults them off |
| `ENABLE_TOOL_SEARCH=true` | Re-enable ToolSearch when BASE_URL is not `api.anthropic.com` |
| `_CLAUDE_CODE_ASSUME_FIRST_PARTY_BASE_URL=1` | Treat proxy BASE_URL as first-party for some gates (use only if you know you need it) |

**Rules / skills:** Claude Code injects `CLAUDE.md` and skill text into `/v1/messages` locally (often as user `<system-reminder>`). The proxy forwards those messages; it does **not** strip them. Top-level Anthropic `system` stays omitted by default (opt in with `CCP_CURSOR_EMBED_SYSTEM=1`).

**Verify `/deep-research`:** transcript should show a `Workflow` tool_use (`name: deep-research`), not only Bash `curl`/`mkdir`.

```json
{
  "bindAddress": "127.0.0.1",
  "port": 18765,
  "cursor": {
    "sandModels": ["claude-fable-5"]
  },
  "log": { "stderr": false, "verbose": false }
}
```

This is the shape written by the TUI; manual editing is mainly useful for
automation. See [Sand mode](#sand-mode) for the complete routing, TUI, model
discovery, and usage guide.

---

## MCP troubleshooting

If Claude Code repeatedly prints
`MCP server 'plugin:lobster-channel:lobster-channel' not connected`, the
message is produced by Claude Code's local hook dispatcher when its Lobster
client is missing or stale. It happens before an HTTP request reaches this
proxy, so proxy stream retries do not repair the local MCP registry.

One confirmed cause is Lobster `1.23.0`'s local lifecycle: Claude Code can
start one plugin process per session while the processes share a bridge
binding. When a newer process takes over, the bridge closes the older one with
code `4405` (`session superseded`), and that runtime calls `process.exit(1)`.
The older Claude session then keeps a disconnected MCP registry entry and can
print the error on every hook. Competing pairing processes can similarly cause
`4407` handshake takeovers. This is a local Lobster/session-lifecycle problem,
not a Cursor inference-stream retry failure.

Use this order:

1. Keep the existing `serve` process. Use the monitor TUI first: press `s` to
   inspect or toggle Sand models and `u` to inspect account usage. A second
   Sand binary is not needed.
2. Inspect the selected project:

   ```bash
   claude-cursor-proxy mcp-doctor --cwd "$PWD"
   claude-cursor-proxy mcp-doctor --cwd "$PWD" --json
   ```

   The doctor scans installed `dist/server.js` files for the exit-prone 4405
   branch, reports explicit 4405/4407 log events, and warns when multiple
   Lobster processes can compete for the shared binding. If it reports an
   exit-prone runtime, update Lobster to a build that keeps a superseded
   process dormant instead of terminating the stdio MCP child.

3. If the report lists Lobster under `disabledMcpServers`, run
   `claude-cursor-proxy mcp-doctor --cwd "$PWD" --repair`. The repair makes a
   timestamped backup and removes only Lobster entries. Start a new Claude Code
   session after the repair.
4. For an existing session, try
   `/mcp reconnect plugin:lobster-channel:lobster-channel`. If the session
   still reports `not connected` after a 4405 exit, start a new session after
   updating Lobster; reconnecting the registry cannot revive a child process
   that already exited. This does not require restarting the proxy.
5. Batch commands such as `claude --bare --tools "" -p ...` should use a
   dedicated Claude config directory, so global Lobster hooks are not loaded
   into a process with no matching MCP client:

   ```bash
   BATCH_CONFIG="$(mktemp -d)"
   CLAUDE_CONFIG_DIR="$BATCH_CONFIG" claude --bare --tools "" -p "$PROMPT"
   ```

   Put only the settings needed by the batch in that directory; do not copy
   the global plugin/hooks tree. The doctor follows Claude Code's path rules:
   default `~/.claude.json`, or
   `$CLAUDE_CONFIG_DIR/.claude.json` when the variable is non-empty. A legacy
   `.config.json` is used only when the canonical file is absent.

---

## Limitations

- **Not official.** Provider ToS and account risk are yours.
- **No client auth on the proxy.** Loopback by default; non-loopback only behind a firewall or authenticating reverse proxy.
- **Rate limits** follow the upstream account.
- **Parity is best-effort.** Text, tools, thinking, and streaming work for supported paths; some edge cases are approximated or omitted.
- **Not a full Cursor IDE.** Workspace/tool callbacks beyond Claude Code / grok-build tool loops are incomplete.
- **Linux prebuilts are glibc.** Alpine/musl: build from source.

| Symptom | Fix |
| --- | --- |
| macOS `Killed: 9` | `codesign --force -s - "$(command -v claude-cursor-proxy)"` |
| Auth / 401 | `claude-cursor-proxy cursor auth login` |
| Background 400 | Set `ANTHROPIC_SMALL_FAST_MODEL` to a known full model id |
| Duplicated tools | `CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1` |
| Claude Code prints `Edit` unavailable / switches to `StrReplace`, or an edit call loops | Update to ≥0.1.83 and restart `serve`. Claude Code 2.1.193's `text_editor_20250728` / `str_replace_based_edit_tool` pair is preserved end-to-end; Cursor PiEdit replacements are normalized and returned with the matching native result. |
| `/deep-research` uses Bash/curl only | Update proxy (≥ Workflow passthrough); confirm `Workflow` in transcript; set `enableWorkflows: true` if needed |
| Hung SSE | Check `~/.local/state/claude-cursor-proxy/proxy.log`; try `CCP_LOG_STDERR=1 CCP_TRAFFIC_LOG=1 serve --no-monitor` |
| Image attachment immediately fails with 502 `Image not found [internal]` | Update to ≥0.1.83 and restart `serve`. The proxy keeps the original inline bytes, rotates the selected-image id once, and retries on a fresh Cursor conversation; a persistent upstream error is then surfaced instead of opening an unbounded retry loop. |
| grok-build returns 413 `Cursor KV blob store limit exceeded` (`blobs=4097` / about 64 MiB) | Update to ≥0.1.84 and restart `serve`. The proxy rotates a near-limit Cursor conversation before the next turn; an upstream 413 receives one bounded fresh-conversation retry with the complete Anthropic history and refreshed image ids. No manual `/compact` or new chat is needed. |
| Gemini/Fable Sand returns `ERROR_PRO_USER_RATE_LIMIT_EXCEEDED` while the same model works as CLI | Sand and CLI are separate Cursor request identities and quota buckets. In the TUI press `s`, select the model, and toggle it to `[cli]`; the proxy keeps Sand 429s visible and does not silently spend the CLI/API allowance. |
| grok-build `Server error (500) - Something went wrong on our side` on unpaid invoice or unsupported country/region | Update to ≥0.1.47 and restart serve. Cursor billing is HTTP 429 with the invoice text; geo/policy blocks are HTTP 403 with the country/region text. |
| grok-build `Server error (500)` after `Cursor live open timed out` / duplicate Cursor runs | Update to ≥0.1.57 and restart serve. Response-less live opens fail closed as HTTP 409; local open-slot saturation is jittered HTTP 503. |
| Claude Code `unexpected internal error` then `live open timed out after 10s` (often `gemini-3.6-flash-high`) | Update to ≥0.1.58 and restart serve. HTTP/1 ResumeAction uses the first-open budget, not a flat 10s. |
| grok-build `Conflict (409) - error sending request` / `live open timed out after 20s`, or Claude Code `Agent type 'gemini-3.6-flash-high' not found` | Update to ≥0.1.57 and restart serve. Proven pre-connect misses may switch transport; response-less sends are never replayed. Agent/Task model slugs remap to `general-purpose`. |
| grok-build dumps raw `<tool_use>` / `<parameter>` XML, or `Cursor auth failed: /usr/bin/security: Too many open files` | Update to ≥0.1.51 and restart serve. Named-parameter XML is recovered as tools; XML `spawn_subagent` waits for turn end; serve raises the macOS 256-file limit. |
| grok-build ends with `Cursor finished this turn without text or tool calls`, or reports that `workflow` was intercepted/renamed | Update to the current release and restart serve. Live heartbeats no longer abort valid thinking at 240s, while a heartbeat-only run with no model progress is bounded to 20 minutes by default. A truly empty Cursor turn still retries instead of becoming successful assistant text; malformed control XML is quarantined; exact workflow/skill casing is preserved. |
| grok-build/Grok 4.6 fan-out shows many failed subagents, repeats completed tools, stalls without tokens, or reports `rate_limit_error: Cursor live generation concurrency saturated` | Update to the current release and restart serve. Current defaults admit 1024 bulk starts, protect another 128 interactive starts and 64 tool-result resumes, queue overflow fairly for up to 30 seconds, spread conversations across 16 H2 pools, and bound replacement opens to 16. Start a fresh Grok session after upgrading. |
| grok-build `Conflict (409) - Cursor live open timed out after 20s` then many `A Cursor live run is already active`, or requests stay streaming at 0 B/s | Update to ≥0.1.65 and restart serve. H2 first-open waits 90s; after one timeout the H2 circuit uses HTTP/1 for 30s, then half-opens with one read-only model-catalog probe. A still-connected duplicate is HTTP 503 + `Retry-After`; an identical retry whose original consumer is gone attaches to the in-flight run and replays the segment, and a retry of an already-completed turn receives the retained original response. Genuinely ambiguous acceptance remains fail-closed as 409. Start a fresh Grok session after upgrading. |
| `Cursor produced an empty turn after tool results without a newer checkpoint`, or grok-build `Conflict (409) - Cursor resume produced no progress` after tool results | Update to ≥0.1.82 and restart serve. A newer post-result checkpoint is continued directly. If Cursor omits it and no text/new tool reached the client, the proxy clears the stale Cursor state and internally retries the same downstream request from the complete Anthropic history, which already contains the finished `tool_result`. Partial result dispatch or client-visible partial output still remains ambiguity-fenced. |
| grok-build reports `Cursor tool result wait expired`, or a heartbeat stall first appears as 502 and then 409 | Update to ≥0.1.62 and restart serve. Tool time starts when Grok receives the batch and no longer consumes the next model segment's budget; an already-admitted tool result wins the TTL boundary. Unresolved heartbeat completion is reported as 409 immediately because replay could duplicate the Run. |
| Claude Code Bash widget titles a giant `python3 -c` script | Update to ≥0.1.48 and restart serve. Cursor Shell has no description; the proxy now fills a short one-line title. |
| 45s 502 `idle timeout` / `0 response bytes` | Update to ≥0.1.39 and restart serve. Still set `CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1`. Clash/Surge TUN: DIRECT `*.cursor.sh`. Optional: `CCP_CURSOR_HTTP1=1` |
| `Stream idle timeout - no chunks received`, especially on the first turn or before a background tool result resumes | Update to the current release and restart serve. `/v1/messages` commits the Anthropic SSE lifecycle before Cursor live open, so the client receives bytes immediately and emits a watchdog-safe `message_delta` + `ping` heartbeat every 5s by default. Pre-output Cursor open/step failures stay inside the bounded retry loop; `/v1/responses` intentionally keeps held-HTTP mapping for `response.failed`. |
| Gemini/Fable returns `ERROR_PRO_USER_RATE_LIMIT_EXCEEDED` repeatedly, or Sand says `finished this turn without text or tool calls` on every resend | Update to ≥0.1.82 and restart serve. Explicit policy errors and Sand's 100%-meter empty-END sentinel become HTTP 429 with `Retry-After`; a short cold-key gate stops the first retry wave before it opens many identical Runs. Cooldowns are isolated by stable account, resolved model, and Sand/CLI route, while native tool-result continuations and accepted attaches keep their resume path. |
| grok-build context compaction reports `idle timeout after 45s with no useful progress` / `0 response bytes`, or the response parser rejects compaction events | Update to ≥0.1.82 and restart serve. `xai-compact-*` and `compact_20260112` requests use a stable isolated Cursor live lane, and summaries are emitted as standard Responses assistant/output-text events accepted by Grok Build. |
| Claude Code shows `Cursor live run cancelled`, or Grok 4.6 repeatedly reports `A Cursor live run is already active` after `/compact` | Update to ≥0.1.77 and restart serve. Replacement reservations keep both the old operation fingerprint and its durable Run owner through cancellation teardown; a dropped handoff now seals the existing ledger marker as a scoped ambiguous operation instead of leaving `Dispatched` state that blocks every later turn. Completed runs are replayed after an attach race and resolved pre-output cancellation is retried inside the same SSE request. |
| 502 `Image not found [internal]` on a text-only turn | Update to ≥0.1.40 and restart serve, then retry the same message once (the poisoned conversation checkpoint is cleared on that error). A new Claude Code session also works. |
| 502 `Conversation data missing` / `missing blobs` and the session cannot recover | Update to ≥0.1.45 and restart serve, then retry the same message. The failed turn now resets the unrecoverable Cursor conversation binding; the first retry replays full history in a fresh Cursor conversation without requiring a new Claude Code chat. |
| 400 `Missing tool_result blocks for pending tools` after an interrupted turn / with background shells | Update to ≥0.1.45 and restart serve. A new request without current-turn tool results now supersedes the abandoned live turn; partial tool-result batches are still rejected. |
| 25s 502 `broken pipe (reconnect skipped: no checkpoint)` on the first message | Update to ≥0.1.44 and restart serve. First-turn ResumeAction uses `conversation_id` even before a checkpoint, and broken-pipe H2 flips to HTTP/1. Clash/Surge TUN: DIRECT `*.cursor.sh`. Optional: `CCP_CURSOR_HTTP1=1` |
| 46s 502 `Cursor stream produced no useful progress` on Fable high | Update to ≥0.1.43 and restart serve. Heartbeat-only thinking waits 240s; a fully silent stream still fails at 45s. Clash/Surge TUN: DIRECT `*.cursor.sh`. Optional: `CCP_CURSOR_HTTP1=1` |
| 502 `Cursor live open timed out after 20s` / follow-on 90s buffered 502 | Update to ≥0.1.42 and restart serve. H2 first-open is 20s; HTTP/1 is 90s and only after 464/421. Clash/Surge TUN: DIRECT `*.cursor.sh`. Optional: `CCP_CURSOR_HTTP1=1` |
| ~8 min 502 `error decoding response body` | Update to ≥0.1.41 and restart serve. Reconnect is bounded to 45s. Clash/Surge TUN: DIRECT `*.cursor.sh`. HTTP proxy mode: `CCP_CURSOR_NO_PROXY=1`. Optional: `CCP_CURSOR_HTTP1=1` |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Before a PR: `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test --all`.

Security: [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE) — includes copyright from the upstream project and this fork’s maintainers.
