# claude-cursor-proxy

**[中文](README.zh-CN.md) | English**

[![CI](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/ci.yml)
[![Release](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/release.yml/badge.svg)](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/release.yml)
[![GitHub Release](https://img.shields.io/github/v/release/YeautyYE/claude-cursor-proxy?display_name=tag)](https://github.com/YeautyYE/claude-cursor-proxy/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Platforms](https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey)](https://github.com/YeautyYE/claude-cursor-proxy/releases)

Adapted from [raine/claude-code-proxy](https://github.com/raine/claude-code-proxy). This project is a **Cursor-first** Anthropic-compatible **proxy** for [Claude Code](https://docs.anthropic.com/en/docs/claude-code).

**Run Cursor models (Fable 5) from Claude Code — stably.**

```
Claude Code ──Anthropic /v1/messages──► claude-cursor-proxy (:18765)
                                              │
                                              ├── Cursor (Fable 5)   ← primary
                                              ├── Codex             ← additional
                                              ├── Kimi
                                              └── Grok
```

[Quick start](#quick-start) · [Models](#models) · [Features](#features) · [Config](#configuration) · [Limitations](#limitations)

---

## What it does

Claude Code only speaks Anthropic’s API (`/v1/messages`, etc.).  
Cursor uses its own Agent protocol. They do not talk to each other directly.

This tool runs a local one-way proxy (default `127.0.0.1:18765`):

1. Claude Code sends normal Anthropic requests to the proxy
2. The proxy translates them for Cursor and forwards upstream
3. It streams Anthropic-shaped SSE back — with keep-alive so long thinking turns are not killed by Claude Code’s idle watchdog

Primary upstream: **Cursor (Fable 5)** via `ANTHROPIC_MODEL=claude-fable-5[1m]`. Additional backends in the same process: Codex, Kimi, Grok.

> Not affiliated with Anthropic, Cursor, OpenAI, Moonshot, or xAI.

---

## Why

| | |
| --- | --- |
| **Stable sessions** | HTTP/2 BiDi upstream + Anthropic `ping` SSE keep-alive downstream |
| **Fable 5** | Set `ANTHROPIC_MODEL=claude-fable-5[1m]` (and the same for `ANTHROPIC_SMALL_FAST_MODEL`) |
| **Usage / ctx** | Cursor turn usage mapped onto Anthropic `usage` for status lines and compaction |
| **Tools** | Cursor exec / native tools proxied into Claude Code’s tool loop (best-effort) |
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
| Pin version | `CLAUDE_CURSOR_PROXY_VERSION=v0.1.22 curl -fsSL …/install.sh \| bash` |
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
| `CCP_ANTHROPIC_SSE_PING_SECS` | `15` | SSE keep-alive interval |
| `CCP_LOG_STDERR` / `CCP_LOG_VERBOSE` / `CCP_TRAFFIC_LOG` | unset | Debug |

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
- **Not a full Cursor IDE.** Workspace/tool callbacks beyond Claude Code’s tool loop are incomplete.
- **Linux prebuilts are glibc.** Alpine/musl: build from source.

| Symptom | Fix |
| --- | --- |
| macOS `Killed: 9` | `codesign --force -s - "$(command -v claude-cursor-proxy)"` |
| Auth / 401 | `claude-cursor-proxy cursor auth login` |
| Background 400 | Set `ANTHROPIC_SMALL_FAST_MODEL` to a known full model id |
| Duplicated tools | `CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1` |
| Hung SSE | Check `~/.local/state/claude-cursor-proxy/proxy.log`; try `CCP_LOG_STDERR=1 CCP_TRAFFIC_LOG=1 serve --no-monitor` |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Before a PR: `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test --all`.

Security: [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE) — includes copyright from the upstream project and this fork’s maintainers.
