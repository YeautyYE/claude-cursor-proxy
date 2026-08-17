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

[Quick start](#quick-start) · [Models](#models) · [Features](#features) · [Config](#configuration) · [Limitations](#limitations)

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
| Pin version | `CLAUDE_CURSOR_PROXY_VERSION=v0.1.26 curl -fsSL …/install.sh \| bash` |
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
| `CCP_CURSOR_AUTH_TOKEN` | unset | Cursor bearer override |
| `CCP_CURSOR_BASE_URL` | `https://api2.cursor.sh` | Cursor API base |
| `CCP_CURSOR_CLI_KEYCHAIN_FALLBACK` | on | Disable with `0` / `false` |
| `CCP_CURSOR_EMBED_SYSTEM` | off | Forward Anthropic `system` into Cursor user text (can trigger Fable injection loops) |
| `CCP_CURSOR_FORCE_TOOLS_IN_PROMPT` | off | Dump **all** tool schemas (large); BiDi already keeps Claude-local tools (`Workflow`/`Skill`/…) |
| `CCP_ANTHROPIC_SSE_PING_SECS` | `15` | SSE keep-alive interval |
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
  "log": { "stderr": false, "verbose": false }
}
```

```bash
claude-cursor-proxy cursor auth status
```

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
| grok-build `Server error (500) - Something went wrong on our side` on unpaid invoice or unsupported country/region | Update to ≥0.1.47 and restart serve. Cursor billing is HTTP 429 with the invoice text; geo/policy blocks are HTTP 403 with the country/region text. |
| 45s 502 `idle timeout` / `0 response bytes` | Update to ≥0.1.39 and restart serve. Still set `CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1`. Clash/Surge TUN: DIRECT `*.cursor.sh`. Optional: `CCP_CURSOR_HTTP1=1` |
| `Stream idle timeout - no chunks received` before a background tool result resumes | Update to ≥0.1.45 and restart serve. The pre-response live-result classification wait is capped at 5s instead of 30s; once an SSE response exists, 15s Anthropic pings still protect quiet model thinking. |
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
