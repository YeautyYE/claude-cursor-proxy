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

[Quick start](#quick-start) · [Models](#models) · [Sand mode](#sand-mode) · [Features](#features) · [Config](#configuration) · [Limitations](#limitations)

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
| Pin version | `CLAUDE_CURSOR_PROXY_VERSION=v0.1.74 curl -fsSL …/install.sh \| bash` |
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

### Fast setup

```bash
claude-cursor-proxy cursor auth login
claude-cursor-proxy serve              # keep the monitor TUI open
```

In the monitor TUI, press `s` to open **Sand Models**. Use `j`/`k` to select a
model, `Space` or `Enter` to toggle it, and `a` to enter an exact Cursor
catalog id. The list is marked `[sand]` or `[cli]`; changes apply to new
requests and are written atomically to `config.json`. The TUI requires a
terminal; `serve --no-monitor` keeps the proxy running without it.

Cursor model cells in the Sessions, Active requests, Recent requests, and
Events panes carry the same `[sand]`/`[cli]` badge, so the selected request
surface is visible without opening the editor. Fable aliases are resolved
before matching: a rule for `claude-fable-5-thinking-max` also covers the
usual `claude-fable-5[1m]`, `fable[1m]`, and `cursor:` forms.

This TUI flow is the recommended way to manage Sand routing. You do not need
to edit a file or launch another binary; the running `serve` process picks up
the saved policy for the next request.

### Use the selected model

After enabling a model with `s`, point Claude Code at that model:

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:18765
export ANTHROPIC_AUTH_TOKEN=unused
export ANTHROPIC_MODEL="cursor:claude-fable-5[1m]"
export ANTHROPIC_SMALL_FAST_MODEL="cursor:claude-fable-5[1m]"
claude
```

For a temporary shell/session or automation, `CCP_CURSOR_SAND_MODELS` can
override the TUI policy:

```bash
export CCP_CURSOR_SAND_MODELS="claude-fable-5"
```

For another account-enabled Cursor catalog id, press `a` in the TUI and enter
it directly; for example, `gemini-3.1-pro`.

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

### Account usage

The monitor polls Cursor's read-only dashboard endpoints and shows the signed-in
account, plan, Auto/API percentages, on-demand dollars, dashboard cost/event
totals, and the Sand/Grok Bot period meter when the account provides it. Press
`u` for the multi-line usage view, including the Sand period and recent usage
events. `cursor auth status` shows the active login. On macOS, the monitor can
fall back to Cursor Desktop's read-only `state.vscdb`; missing dashboard fields
are omitted rather than invented.

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
| `CCP_CURSOR_LIVE_CONCURRENCY` | `32` | Fair cap for bulk (`cursor-grok-*`) generation starts (1–128) |
| `CCP_CURSOR_LIVE_INTERACTIVE_RESERVE` | `8` | Protected start capacity for non-Grok models (Gemini/Claude/… subagents); interactive starts may also borrow idle bulk slots, but bulk never borrows the reserve (0–32) |
| `CCP_CURSOR_LIVE_QUEUE_SECS` | `15` | Maximum local admission wait before retryable HTTP 503 (1–300s) |
| `CCP_CURSOR_LIVE_ATTACH_WAIT_MS` | `15000` | Same-operation attach handoff wait before local busy is returned (500–60000ms) |
| `CCP_CURSOR_LIVE_CONFLICT_WAIT_MS` | `30000` | Wait for a different operation to observe the current session Run advance (500–120000ms) |
| `CCP_CURSOR_LIVE_RESUME_WAIT_MS` | `5000` | Pre-response tool-result handoff wait; kept below the client stream watchdog (500–15000ms) |
| `CCP_CURSOR_LIVE_NESTED_WAIT_MS` | `1500` | Pre-response nested-agent handoff wait (500–15000ms) |
| `CCP_CURSOR_RESOURCE_RETRIES` | `6` | Same-request retries for transient Cursor `ERROR_RESOURCE_EXHAUSTED` responses (1–12); billing/quota/capacity policy 429s are never hidden-retried |
| `CCP_CURSOR_STEP_FAILURE_RETRIES` | `4` | Same-request retries for pre-output Cursor `Failed to run step, exceeded max retries` failures (1–8); post-output failures are forwarded |
| `CCP_CURSOR_LIVE_RESUME_RESERVE` | `4` | Additional capacity reserved for paused Runs that need to submit tool results (0–16) |
| `CCP_CURSOR_OPERATION_LEDGER` | off | Opt-in durable operation ledger (crash-safe replay refusal). Stays off by default until completion is gated on downstream delivery, so dropped responses cannot permanently refuse client retries |
| `CCP_CURSOR_LIVE_TIMEOUT_SECS` | `1800` | Active model-generation budget for each live segment (max 3600s; paused while downstream tools run) |
| `CCP_CURSOR_TOOL_TTL_SECS` | same as live timeout | Maximum wait after a tool batch reaches the downstream client; an admitted result is allowed to finish dispatch |
| `CCP_CURSOR_HEARTBEAT_PROGRESS_SECS` | `600` | Maximum heartbeat-only thinking period without model progress |
| `CCP_CURSOR_H2_SHARDS` | `4` | Stable H2 client pools used to isolate concurrent conversations (1–16) |
| `CCP_CURSOR_LIVE_RECOVERY_OPENS` | `4` | Process-wide cap for simultaneous ResumeAction replacement opens (1–16) |
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
    "sandModels": ["claude-fable-5", "cursor:gpt-5.5"]
  },
  "log": { "stderr": false, "verbose": false }
}
```

This is the shape written by the TUI; manual editing is mainly useful for
automation. See [Sand mode](#sand-mode) for the complete routing, TUI, model
discovery, and usage guide.

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
| `/deep-research` uses Bash/curl only | Update proxy (≥ Workflow passthrough); confirm `Workflow` in transcript; set `enableWorkflows: true` if needed |
| Hung SSE | Check `~/.local/state/claude-cursor-proxy/proxy.log`; try `CCP_LOG_STDERR=1 CCP_TRAFFIC_LOG=1 serve --no-monitor` |
| Image attachment immediately fails with 502 `Image not found [internal]` | Update to a build that encodes inline `SelectedImage.data` as protobuf field 8 bytes. Field 1 is a Cursor blob id in current CLI builds. |
| grok-build `Server error (500) - Something went wrong on our side` on unpaid invoice or unsupported country/region | Update to ≥0.1.47 and restart serve. Cursor billing is HTTP 429 with the invoice text; geo/policy blocks are HTTP 403 with the country/region text. |
| grok-build `Server error (500)` after `Cursor live open timed out` / duplicate Cursor runs | Update to ≥0.1.57 and restart serve. Response-less live opens fail closed as HTTP 409; local open-slot saturation is jittered HTTP 503. |
| Claude Code `unexpected internal error` then `live open timed out after 10s` (often `gemini-3.6-flash-high`) | Update to ≥0.1.58 and restart serve. HTTP/1 ResumeAction uses the first-open budget, not a flat 10s. |
| grok-build `Conflict (409) - error sending request` / `live open timed out after 20s`, or Claude Code `Agent type 'gemini-3.6-flash-high' not found` | Update to ≥0.1.57 and restart serve. Proven pre-connect misses may switch transport; response-less sends are never replayed. Agent/Task model slugs remap to `general-purpose`. |
| grok-build dumps raw `<tool_use>` / `<parameter>` XML, or `Cursor auth failed: /usr/bin/security: Too many open files` | Update to ≥0.1.51 and restart serve. Named-parameter XML is recovered as tools; XML `spawn_subagent` waits for turn end; serve raises the macOS 256-file limit. |
| grok-build ends with `Cursor finished this turn without text or tool calls`, or reports that `workflow` was intercepted/renamed | Update to ≥0.1.61 and restart serve. Live heartbeats no longer abort valid thinking at 240s, while a heartbeat-only run with no model progress is bounded to 10 minutes. A truly empty Cursor turn still retries instead of becoming successful assistant text; malformed control XML is quarantined; exact workflow/skill casing is preserved. |
| grok-build/Grok 4.6 fan-out shows many failed subagents, repeats completed tools, stalls without tokens, or reports `rate_limit_error: Cursor live generation concurrency saturated` | Update to ≥0.1.60 and restart serve. Normal 32-way fan-out is admitted, four extra slots are reserved for tool-result resumes, overflow queues fairly then returns retryable 503, conversations span four H2 pools, and replacement opens are bounded. Start a fresh Grok session after upgrading. |
| grok-build `Conflict (409) - Cursor live open timed out after 20s` then many `A Cursor live run is already active`, or requests stay streaming at 0 B/s | Update to ≥0.1.65 and restart serve. H2 first-open waits 90s; after one timeout the H2 circuit uses HTTP/1 for 30s, then half-opens with one read-only model-catalog probe. A still-connected duplicate is HTTP 503 + `Retry-After`; an identical retry whose original consumer is gone attaches to the in-flight run and replays the segment, and a retry of an already-completed turn receives the retained original response. Genuinely ambiguous acceptance remains fail-closed as 409. Start a fresh Grok session after upgrading. |
| grok-build `Conflict (409) - Cursor resume produced no progress before the recovery deadline` after tool results | Update to ≥0.1.60 and restart serve. The proxy retries without replaying tools only if Cursor emitted a newer checkpoint after receiving those results and no new text/tools reached the client. Without that proof, 409 is intentional because automatic replay could duplicate execution. |
| grok-build reports `Cursor tool result wait expired`, or a heartbeat stall first appears as 502 and then 409 | Update to ≥0.1.62 and restart serve. Tool time starts when Grok receives the batch and no longer consumes the next model segment's budget; an already-admitted tool result wins the TTL boundary. Unresolved heartbeat completion is reported as 409 immediately because replay could duplicate the Run. |
| Claude Code Bash widget titles a giant `python3 -c` script | Update to ≥0.1.48 and restart serve. Cursor Shell has no description; the proxy now fills a short one-line title. |
| 45s 502 `idle timeout` / `0 response bytes` | Update to ≥0.1.39 and restart serve. Still set `CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1`. Clash/Surge TUN: DIRECT `*.cursor.sh`. Optional: `CCP_CURSOR_HTTP1=1` |
| `Stream idle timeout - no chunks received`, especially on the first turn or before a background tool result resumes | Update to the current release and restart serve. `/v1/messages` commits the Anthropic SSE lifecycle before Cursor live open, so the client receives bytes immediately and emits a watchdog-safe `message_delta` + `ping` heartbeat every 5s by default. Pre-output Cursor open/step failures stay inside the bounded retry loop; `/v1/responses` intentionally keeps held-HTTP mapping for `response.failed`. |
| Claude Code shows `Cursor live run cancelled` and the turn stops | Update to ≥0.1.74 and restart serve. A resolved cancellation before the first text/tool event is retried inside the same SSE request; cancellation reports that include unresolved/ambiguous acceptance are kept fail-closed to prevent duplicate tool execution. |
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
