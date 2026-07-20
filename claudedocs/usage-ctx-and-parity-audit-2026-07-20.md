# Usage / Ctx 裁决 + Cursor 完整度审计

**Date:** 2026-07-20  
**Trigger:** 用户一问（项目做什么）+ agent 若干 search/read 后状态栏：
`In: 396.6k | Out: 1.9k | Cached: 793.1k / Total: 1.1M | Ctx: 396.6k | Ctx Used: 40.0%`

Sources: proxy `src/providers/cursor/**`, ccstatusline `~/.claude/ccstatusline`, Claude Code `~/Temp/claude-2.1.193-src`, Cursor CLI reverse docs, live code re-verify.

---

## 1. 裁决：396.6k Ctx 是代理算错，还是 Cursor 真账单？

### Verdict

**不是「In+Cached 双重累加」导致的 Ctx 虚高。**  
**396.6k 是最近一次 Anthropic `message` 写入 transcript 的 `input + cache_read + cache_creation`，在当前 normalize 路径下 ≈ Cursor `turn_ended` 报出的该次 prompt 总量（或未带 cache 字段的 `input_tokens`）。**

`Ctx Used: 40.0%` 与 **1M 窗口**一致：`396.6k / 1_000_000 ≈ 39.7%`（先前 bare `fable` 按 200k 会显示 ~65%/100%；`anthropic_wire_model` → `claude-fable-5[1m]` 已修正分母）。

用户直觉「一句话不该 40 万」在**计量公式**上不成立；更可能是 **真实上游 prompt 很大**（见 §3），而非 statusbar 把 Cached 又加进 Ctx 一次。

### 数字如何对得上

| 栏位 | 来源 | 用户截图 |
|------|------|----------|
| **Ctx** | 末次 assistant：`input + cache_read + cache_creation`（Claude `EAn` / ccstatusline `contextLength`） | 396.6k |
| **Ctx Used %** | `EAn`：`round(Ctx / PE(model) * 100)`；`PE`：`[1m]` / fable·`gB` / 1m-beta → **1M**，否则 **200k** | 40.0% |
| **In** | 若走 Claude 注入的 `context_window.total_input_tokens` → **与 Ctx 同式（末轮）**；若 ccstatusline 只读 transcript → 会话 Σ`input_tokens` | 396.6k（与 Ctx 同 → 符合末轮快照路径） |
| **Cached / Total** | ccstatusline **transcript 会话累加**：Σ(cache_*) / Σ(input+output+cache) | 793.1k / ~1.1M |

Claude Code 2.1.193 在 bundle 内用 `Wre` 合并 `message_start`+`message_delta` usage，再经 `fat`/`ene` 取**最后一条 assistant**；`statusLine.command`（常为独立包 **ccstatusline**，非打进 cli）读 stdin JSON。  
Anthropic 语义（bundle 自述）：全量 prompt = `input_tokens + cache_creation + cache_read`（`input` = uncached 分区）。

因此：**末轮 Ctx=In=396.6k 且会话 Cached=793.1k** = 末轮几乎无 cache 字段活动，历史轮次的 cache 只进 Cached——与双数据源设计一致，**不是**单条 usage 上 Cached>Ctx 的矛盾。

代理 `normalize_cursor_usage_for_anthropic` 把 Cursor「全量 input + 分区 cache」拆成 Anthropic 三字段后，客户端再相加 → **Ctx ≈ Cursor 原始 input 全量**。
### normalize 在做什么（防双计，不是造 396k）

[`sse.rs` `normalize_cursor_usage_for_anthropic`](../src/providers/cursor/sse.rs)：

- 若 `input >= cache_read + cache_write` → `input' = input - cache_*`，保留 cache 字段；  
  则 ccstatusline Ctx = `input' + cache_*` = **原始 Cursor input**。
- 若 `input == cache_read` 且无 write → 清零重复 cache。
- 否则原样透传。

**结论：** 在「Cursor 报全量 input + 分区 cache」的观测形态下，normalize **刻意让 Ctx ≈ Cursor 全量**，不是把 Cached 再叠一次。  
用户看到的 396.6k **应视为 Cursor 对该次 Run 的 prompt 记账**，不是 `396.6+793`。

### seed / token_delta / reasoning

| 机制 | 会不会把 Ctx 吹到 396k？ |
|------|-------------------------|
| `seed_estimated_input_tokens`（`len/4`） | **否（有 turn_ended 时）**：`record_usage` 整表覆盖。仅在无 usage 时当占位。 |
| `token_delta` → `OutputTokenDelta` | **否**：只加 output，不再 `Usage{input:0}` 清零。 |
| `reasoning_tokens` 并入 output | **只影响 Out**，不影响 Ctx。 |
| `.max(1)` on input | 只防 0，可忽略。 |

### 边界残留（次要）

若 Cursor **已按 Anthropic 互斥语义** 发 `input=uncached` 且 `cache_read` 很大，且碰巧 `input >= cache_parts`，normalize 会 **少报** input（Ctx 偏小），与本次「虚高」方向相反。  
若 `input < cache_parts` 且两边都是「互斥大数」却实际重叠，才会 **透传双计** → Ctx≈In+Cached；本次末条 Ctx≈In，**不符合**该故障形态。

其他不对称（不影响本次截图主因）：

- **非流式 JSON 路径** `decode_cursor_upstream`：未走 `normalize_*`，且 cache 字段恒写 0（`response.rs`）。
- **`reasoning_tokens` 并入 output**：若服务端将来同时填满 `output_tokens` 与 `reasoning_tokens`，Out 可能偏高（非 Ctx）。
- 第三方对照：entireio stop hook 对 Cursor 用量做同样的 `total − cache_*` 拆分；naive「四字段全加」才会 ~2×。

---

## 2. 为何「一问」Cursor 仍可能报到 ~400k？

代理每次把 Claude Code 的 `/v1/messages` 打成 Cursor `AgentService/Run`，且：

1. **空 `ConversationState` + `conversation_id: None`**（`client.rs`）→ 无法走官方多轮 state/checkpoint；历史靠 **把整段 Anthropic messages 扁成一大段 user text**。
2. **默认把完整 `tools` JSON schema 打进 `<tools>…</tools>`**（`request.rs`）→ Claude Code 工具面（含大量 MCP）可轻易到数万～十余万 tokens。
3. **Agent 多轮 tool_use**：每一轮 Claude 会把 Grep/Read 的 **全文 tool_result** 再塞进下一次请求 → 读几个大文件后末轮轻松到数十万。
4. **`exclude_workspace_context` 默认不关** → Cursor 仍可注入 workspace/索引侧上下文；`request_context` 回复目前是 **空 `RequestContext {}`**。官方 CLI 会填 rules/skills/env/repo（reverse 文档里 rules+skills 可占 payload 大头）；空回复不代表服务端不注入。
5. 默认 **不** 嵌入 Claude system（防 Fable 注入循环），故 system 不是主因；**tools + tool 历史 + Cursor workspace** 才是。
6. 本地 CLI 配置常见 `context: 300k` 量级参数 → 六位数 prompt 在产品设计范围内。

「用户只说了一句话」≠「末次 API 只含那一句话」。状态栏 Ctx 看的是 **最后一次** Messages 调用的用量。`turn_ended.input_tokens` 计量的是模型见到的全量 prompt，不是用户短句。

---

## 3. 建议的验证与修复（用量相关）

### 立刻可证伪 / 证实

1. 打开 `CCP_CURSOR_DEBUG=1`，抓一条 `turn_ended` 原始 `input/cache_read/cache_write/reasoning`。  
2. 对照 transcript 最后一条 `message.usage`（与 statusbar Ctx 同源）。  
3. 同项目用官方 `~/.local/bin/agent -p --model claude-fable-5-thinking-max "项目做什么"` 对比是否也出现同量级（CLI 工具面更小，通常会低很多）。

### 若目标是「降真实账单」而非改显示

| 优先级 | 改动 |
|--------|------|
| P0 | 持久化 `conversation_id` + 有意义的 `ConversationState`/checkpoint，避免每轮全量重放 |
| P0 | 停止把完整 Anthropic tools 目录塞进 user text；改 `mcp_tools` / 仅声明已桥接工具，或压缩 schema |
| P1 | 充实 / 裁剪 `request_context`；评估 `CCP_CURSOR_EXCLUDE_WORKSPACE` |
| P1 | debug 日志：raw turn_ended vs normalize 后 usage |
| P2 | `RequestedModel.parameters`（thinking/context/effort）与 CLI 对齐 |

### 若目标是「显示更贴近用户直觉」

- 可选：status 用「末轮 uncached」或「估算用户可见 tokens」——会与 Anthropic/Claude Code 语义偏离，**不推荐**除非单独做「proxy 解释层」。
- 修正 ccstatusline 硬编码 200k%（用户若仍用旧 widget）——与代理无关；Ctx Used 40% 说明分母已是 1M。

---

## 4. 完整度矩阵（相对 Cursor CLI + Claude Code）

> Reverse doc §5 已过时。下表以 **2026-07-20 代码**为准。

### P0（能跑真 agent）

| 项 | 状态 |
|----|------|
| H2 BiDi `Run` + 心跳 | **DONE**（HTTPS）；`CCP_CURSOR_HTTP1=1` 时走 RunSSE |
| 增量 SSE（live/tool 路径） | **DONE**；无工具路径仍可能 buffer |
| InteractionUpdate 字段标签 | **DONE**（text=1, thinking=4, heartbeat=13, turn_ended=14, exec=2） |
| CLI fingerprint / ghost / checksum | **DONE** |
| Native exec 工具环（Shell/Read/Edit/…） | **DONE**（核心 + MCP/Todo/Plan/WebSearch/Fetch/AskQuestion 映射） |
| conversation_id / ConversationState | **DONE**（opaque Structure + KV blobs 跨回合；delta prompt） |
| 长 thinking 不中断（≥5–6m） | **DONE**（见 §6） |
| Usage → Anthropic SSE + normalize | **DONE**（见上） |

### P1

| 项 | 状态 |
|----|------|
| `GetUsableModels` / 真 `/v1/models` | **DONE**（live catalog + cache；失败回退静态） |
| `RequestedModel.parameters` | **DONE**（thinking/effort/context 从 catalog id 推导） |
| `RunSSE` + `BidiAppend` | **DONE**（`CCP_CURSOR_HTTP1=1`：RunSSE 读 + BidiAppend 写） |
| CLI Keychain 复用 | **DONE**（macOS `cursor-access-token`；Linux/Win `~/.config/cursor/auth.json`；`CCP_CURSOR_CLI_KEYCHAIN_FALLBACK`） |
| KV 跨回合 checkpoint | **DONE**（conversation store + pre_fetched_blobs） |
| count_tokens | **PARTIAL**（char/4） |
| Thinking SSE | **DONE** |
| InteractionQuery | **DONE**（自动 approve web/plan/switch；AskQuestion/MCP auth 明确 reject） |
| `RunPoll` | **BLOCKED**（CLI extract 无此方法；仅后台自动化场景，代理不需要） |

### 对 Claude Code 的契约

| API | 状态 |
|-----|------|
| `POST /v1/messages` stream + tools | **PARTIAL**（live 完整；其余弱） |
| thinking blocks | **DONE** |
| count_tokens | **PARTIAL** |
| `/v1/models` | **DONE** |

### 仍 BLOCKED / 不做

| 项 | 原因 |
|----|------|
| 全量 ~40 native tools（ComputerUse、Task subagent、Grind…） | 需 IDE/沙箱宿主；Claude Code 侧无对等 tool |
| MCP OAuth / AskQuestion 真交互 UI | 代理无浏览器审批面；已 reject 并说明 |
| 充实 `request_context`（rules/skills/repo） | 质量差距，非协议阻塞；空 context 仍可跑 |
| `RunPoll` | 产品路径不走；CLI 2026.07 extract 亦未见 |

---

## 6. Bugfix：长 thinking → `API Error: … mid-response`

**Symptom (2026-07-20):** Claude Code via proxy after ~6m+ quiet Fable thinking:
`API Error: … mid-response. The response above may be incomplete.`
Cursor IDE on similar work does **not** fail.

**Evidence (Claude Code 2.1.193 `cli.js`):**
- Stream idle: `f4r() = Math.max(CLAUDE_STREAM_IDLE_TIMEOUT_MS\|\|0, 300000)` → **≥5 minutes** with **no SSE bytes**.
- Watchdog → `Response stalled mid-stream…`; abrupt close → `Connection closed mid-response…`.
- Proxy kept Cursor BiDi alive (`client_heartbeat` ~5s) but **emitted zero Anthropic SSE** during quiet thinking → Claude’s watchdog fires. IDE never leaves the Anthropic SSE layer idle that long.

**Also fixed (proxy self-kill):**
- Agent `complete_idle` was **180s** after any text → invented `End` during long post-plan thinking. Now **disabled** when native tools are advertised; wait for `turn_ended` / hard timeout (default **1800s**).
- Decode `InteractionUpdate.heartbeat` (tag 13) and refresh idle timers.

**Fix shipped:**
1. `live_sse_response` emits Anthropic `event: ping` every **15s** (`CCP_ANTHROPIC_SSE_PING_SECS`) while the live run is open.
2. Agent runs no longer `complete_idle`-end; hard timeout backstop raised.
3. Server heartbeat updates `last_progress`.

**Verify:** restart proxy, run a long-thinking Fable turn (>6m quiet). Status bar should keep updating; no mid-response abort. Optional: `CCP_CURSOR_DEBUG=1` + watch for continued BiDi frames.

---

## 5. 一句话给用户

**396.6k Ctx 不是把 Cached 再加一遍的显示 bug；它反映 Cursor 对「该次（通常已含多轮工具结果 + 工具 schema + 可能的 workspace）prompt」的记账。**  
要变小，需要减真实上行体积（会话状态、工具打包、workspace），而不是再改 normalize 去「藏」数字。  
**长 thinking 中断**则是另一问题：代理没把 keepalive 打进 Anthropic SSE，Claude Code ≥5m 空闲看门狗会杀流——已用 ping + 取消 agent complete_idle 修复。