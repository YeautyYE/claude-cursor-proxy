# claude-code-proxy vs Cursor CLI — gap audit

CLI reference: `2026.07.16-899851b` (`~/.local/share/cursor-agent`)

## P0 done this session

| Item | Before | After |
|------|--------|--------|
| client-type | default `ide` | default **`cli`** |
| client-version | `3.12.17` | **`cli-<detected>`** or `cli-2026.07.16-899851b` |
| ghost-mode | default false | default **true** (CLI privacy default) |
| checksum | always on | **off for CLI profile**; `CCP_CURSOR_CHECKSUM_MODE` |
| IDE headers | always sent | only if `CCP_CURSOR_CLIENT_PROFILE=ide` |
| fable | `claude-fable-5` (invalid bare) | **`claude-fable-5-thinking-max`** |
| bare `cursor` | sent as model id → BAD_MODEL_NAME | **`composer-2.5`** |
| composer mode | Plan/Ask (wrong for coding) | **Agent + model id** |
| timeout | 45s | **300s** (`CCP_CURSOR_TIMEOUT_SECS`) |
| model_details | omitted | filled alongside requested_model |
| loopback proxy | could hit Clash → mock fail | cleartext **no_proxy** |
| BiDi live Run | unary POST hang | **`live.rs`**: heartbeat + exec resume + tool bridge |
| native tools | XML-only / silent end_turn | **tool_call_started / Exec → Claude tool_use** |
| system embed | CLAUDE_CODE_SYSTEM banners | **default omit** (Fable injection loops) |

## P1 remaining (not full parity)

1. ~~`RunSSE` / `RunPoll` fallbacks~~ — **DONE** RunSSE+BidiAppend when `CCP_CURSOR_HTTP1=1`; RunPoll **BLOCKED** (not in CLI agent path)
2. ~~`GetUsableModels` / `GetDefaultModelForCli`~~ — **DONE**
3. ~~Native tool loop completeness~~ — **DONE** for MCP/Plan/Todo/WebSearch/Fetch/AskQuestion mapping; rarer IDE-only tools still out of scope
4. ~~ConversationState persistence~~ — **DONE**
5. ~~Reuse official CLI token store~~ — **DONE** (Keychain + auth.json)

**2026-07-20 refresh:** full matrix + **396.6k Ctx verdict** → [`usage-ctx-and-parity-audit-2026-07-20.md`](./usage-ctx-and-parity-audit-2026-07-20.md). Reverse doc §5 is stale (BiDi/heartbeats/cli tags are in).  

## Hang notes (2026-07-20)

- Polluted sessions that already contain Fable “CLAUDE_CODE_SYSTEM / 提示词注入” monologues must be **`/new`**’d; scrubbing strips them from Cursor payloads but Claude Code UI history remains.
- After `tool_result` resume, proxy keeps a **120s grace** (`CCP_CURSOR_RESUME_GRACE_SECS`) so quiet thinking is not killed as `no useful progress`.
- **Long quiet thinking (≥5–6m):** Claude Code stream idle watchdog is **≥300s with no SSE bytes**. Proxy now emits Anthropic `ping` every 15s (`CCP_ANTHROPIC_SSE_PING_SECS`) and no longer `complete_idle`-ends agent runs. **Restart `claude-code-proxy serve`** after install.
- After installing a new binary, re-`codesign --force -s -` on macOS (`cp` breaks signature).

## Stability refresh (2026-07-20 evening)

CLI `2026.07.16-899851b` stall/retry extract applied:

| CLI behavior | Proxy |
|--------------|--------|
| stall fail **30s** / heartbeat-only **90s** | server `InteractionUpdate.heartbeat` refreshes idle; Anthropic `ping` 15s; live always waits `turn_ended` |
| transport retries **10** + 1s→60s +20% jitter | `CCP_CURSOR_RECONNECT_MAX` (default 10) + `ResumeAction` mid-stream when checkpoint exists |
| `useHttp1ForAgent` default false; FORCE_BIDI → H1 | H2 BiDi first; auto **RunSSE+BidiAppend** on 464/502/503/… (`CCP_CURSOR_HTTP1=1` still forces H1) |
| BidiAppend in-flight **16** | same + transient append retry |
| client hb **5s** / exec hb **3s** | in live driver select (non-blocking try_send) |

Still **BLOCKED**: full IDE ComputerUse/sandbox, RunPoll, MCP OAuth / AskQuestion UI.

## Env knobs

| Env | Default | Meaning |
|-----|---------|---------|
| `CCP_CURSOR_CLIENT_TYPE` | `cli` | `cli` / `ide` |
| `CCP_CURSOR_CLIENT_VERSION` | auto `cli-…` | override version header |
| `CCP_CURSOR_CLIENT_PROFILE` | `cli` | `cli` or `ide` (extra headers) |
| `CCP_CURSOR_GHOST_MODE` | true | privacy header |
| `CCP_CURSOR_CHECKSUM_MODE` | `none` (cli) / `token` (ide) | `none`/`token`/`storage` |
| `CCP_CURSOR_HTTP1` | false | force HTTP/1.1 + **RunSSE/BidiAppend** |
| `CCP_CURSOR_CLI_KEYCHAIN_FALLBACK` | true | reuse CLI Keychain / auth.json when proxy store empty |
| `CCP_CURSOR_TIMEOUT_SECS` | 1800 (live hard) | upstream hard timeout |
| `CCP_CURSOR_RECONNECT_MAX` | 10 | mid-stream ResumeAction retries (CLI transport limit) |
| `CCP_ANTHROPIC_SSE_PING_SECS` | 15 | Anthropic SSE keepalive during quiet thinking |
| `CCP_CURSOR_HARNESS` | unset | optional harness override |
| `CCP_CURSOR_DEBUG` | unset | stderr request diagnostics |
| `CCP_CURSOR_FORCE_TOOLS_IN_PROMPT` | unset | force `<tools>` dump even when bridging |

## Verify

```bash
# Official CLI baseline (absolute path — avoid Grok `agent`)
~/.local/bin/agent -p --trust --model claude-fable-5-thinking-max "ok"

# Proxy (same proxy env as successful CLI)
claude-code-proxy serve
ANTHROPIC_BASE_URL=http://127.0.0.1:18765 \
ANTHROPIC_AUTH_TOKEN=unused \
ANTHROPIC_MODEL=fable \
  claude -p "reply with just: ok"
```
EOF
