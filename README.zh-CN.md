# claude-cursor-proxy

**[English](README.md) | 中文**

[![CI](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/ci.yml)
[![Release](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/release.yml/badge.svg)](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/release.yml)
[![GitHub Release](https://img.shields.io/github/v/release/YeautyYE/claude-cursor-proxy?display_name=tag)](https://github.com/YeautyYE/claude-cursor-proxy/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Platforms](https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey)](https://github.com/YeautyYE/claude-cursor-proxy/releases)

基于 [raine/claude-code-proxy](https://github.com/raine/claude-code-proxy) 改进。本地 **单向代理**：Claude Code 与 [Grok Build](https://x.ai/cli)（`grok` / grok-build）→ 本代理 → Cursor。命令行工具名与仓库名均为 **`claude-cursor-proxy`**。

**让 Claude Code 和 grok-build 稳定调用 Cursor 上的模型（推荐 Fable 5）。**

```
Claude Code ──Anthropic /v1/messages──► claude-cursor-proxy (:18765)
grok-build  ──Responses / Messages ──►        │
                                              ├── Cursor (Fable 5)   ← 主路径
                                              ├── Codex             ← 额外后端
                                              ├── Kimi
                                              └── Grok
```

[快速开始](#快速开始) · [模型](#模型) · [Sand 模式](#sand-模式) · [功能](#功能) · [配置](#配置) · [常见问题](#常见问题) · [限制](#限制)

---

## 这是什么

Claude Code 走 Anthropic（`/v1/messages`），grok-build 走 OpenAI Responses（`/v1/responses`）。Cursor 用自己的 Agent 协议，两边直接连不上。

本工具在本机跑一个单向代理（默认 `127.0.0.1:18765`）：

1. Claude Code 或 grok-build 照常把请求发给本代理
2. 代理转成 Cursor 能懂的请求，再发给 Cursor
3. 按客户端把流式回复转回去：Claude Code 用 Anthropic SSE（带 `ping` keep-alive），grok-build 用 Responses 事件

**主路径是 Cursor（Fable 5）**。同一进程里还可选 Codex / Kimi / Grok 等额外后端。

> 本项目与 Anthropic、Cursor、OpenAI、Moonshot、xAI 均无官方关联。

---

## 为什么用它

| | |
| --- | --- |
| **会话更稳** | 上游连 Cursor 长连接；下游给 Claude Code 定期 `ping`，长思考不易被掐断 |
| **Fable 5** | 设 `ANTHROPIC_MODEL=claude-fable-5[1m]`（`ANTHROPIC_SMALL_FAST_MODEL` 写同样的即可） |
| **用量 / 上下文** | 把 Cursor 的用量信息转成 Anthropic 的 `usage`，状态栏和上下文压缩更正常 |
| **工具调用** | 尽量把 Cursor 侧工具接到 Claude Code / grok-build 的工具循环里（尽力而为） |
| **安装简单** | 预编译包带校验；macOS 会做 ad-hoc 签名；配置在 `~/.config/claude-cursor-proxy` |

说明：这是兼容层，**不是**完整 Cursor IDE。边界见 [限制](#限制)。

---

## 快速开始

### 1. 安装

```bash
curl -fsSL https://raw.githubusercontent.com/YeautyYE/claude-cursor-proxy/main/install.sh | bash
```

支持 macOS / Linux。Windows 请从 [Releases](https://github.com/YeautyYE/claude-cursor-proxy/releases) 下载 `.zip`，或用 WSL。

<details>
<summary>其他安装方式</summary>

| 方式 | 命令 |
| --- | --- |
| 固定版本 | `CLAUDE_CURSOR_PROXY_VERSION=v0.1.81 curl -fsSL …/install.sh \| bash` |
| 安装到指定目录 | `CLAUDE_CURSOR_PROXY_INSTALL_DIR=/opt/bin bash install.sh` |
| 从源码安装 | `cargo install --git https://github.com/YeautyYE/claude-cursor-proxy --locked` |
| Fork / 镜像 | `GITHUB_REPO=owner/repo curl -fsSL https://raw.githubusercontent.com/owner/repo/main/install.sh \| bash` |

</details>

### 2. 登录并启动服务

```bash
claude-cursor-proxy cursor auth login
claude-cursor-proxy serve                 # 默认 127.0.0.1:18765，带监控界面
claude-cursor-proxy serve --no-monitor    # 只要日志，不要监控界面
claude-cursor-proxy serve --port 11435    # 换端口
```

#### 运行中热切换账号（无需重启）

`serve` 运行时，直接在另一个终端执行 `claude-cursor-proxy cursor auth login`
即可换号。代理每个请求都会重新读取凭据，所以：

- 新请求立即使用新账号；
- 正在进行的运行继续用启动时拿到的旧 token 跑完，不会被打断；
- 已有会话的下一轮会在新账号下重开 Cursor 对话（客户端自动重发完整历史，上下文不丢）。

`cursor auth status` 可查看当前生效的账号。注意：如果 `serve` 进程的环境里设了
`CCP_CURSOR_AUTH_TOKEN`/`CURSOR_AUTH_TOKEN`，env token 会遮蔽存储的登录态，
热切换不会生效，需先取消该环境变量。

### 3. 让 Claude Code 走本机代理（Fable 5）

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:18765
export ANTHROPIC_AUTH_TOKEN=unused
export ANTHROPIC_MODEL=claude-fable-5[1m]
export ANTHROPIC_SMALL_FAST_MODEL=claude-fable-5[1m]
export CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1
export CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1
claude
```

也可以写进 `~/.claude/settings.json` 的 `"env"` 字段，效果一样。

**务必**同时设置 `ANTHROPIC_SMALL_FAST_MODEL`（写成和 `ANTHROPIC_MODEL` 一样的完整模型 id 即可）。  
否则 Claude Code 后台的小模型请求会报 HTTP 400。

<details>
<summary>改用 Codex / Kimi / Grok（额外后端）</summary>

```bash
claude-cursor-proxy codex auth login
ANTHROPIC_BASE_URL=http://127.0.0.1:18765 ANTHROPIC_AUTH_TOKEN=unused \
  ANTHROPIC_MODEL=gpt-5.6-sol[1m] ANTHROPIC_SMALL_FAST_MODEL=gpt-5.6-luna[1m] \
  CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1 CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1 \
  claude

claude-cursor-proxy kimi auth login   # 或：grok auth login
```

</details>

### 让 grok-build 走本代理

**不要**把 `GROK_CLI_CHAT_PROXY_BASE_URL` 设成本代理。那个环境变量是给 xAI 官方 chat-proxy 用的。grok-build 把本代理当成普通自定义 `base_url` 即可。

**1. 登录（一次）并启动代理**

```bash
claude-cursor-proxy grok auth login      # grok-4.5 / grok-4.6 聊天必需
claude-cursor-proxy cursor auth login    # 只有还要走 Fable / Composer 时才需要
claude-cursor-proxy serve                # 127.0.0.1:18765
```

**2. 改 `~/.grok/config.toml`** — 覆盖官方 id，Fast 和 effort 菜单还在。Fast 就是 `reasoning.effort = "low"`。

```toml
# ~/.grok/config.toml

[model.grok-4.6]
base_url = "http://127.0.0.1:18765/v1"
api_key = "unused"

[model.grok-4.5]
base_url = "http://127.0.0.1:18765/v1"
api_key = "unused"

# 用 grok-build 跑 Cursor 目录（官方 OpenAI Responses，不是 Claude Messages）
[model.cursor-grok]
model = "cursor-grok-4.6-xhigh-fast"
base_url = "http://127.0.0.1:18765/v1"
api_backend = "responses"
api_key = "unused"

# 可选：Claude Code 风格的 Anthropic Messages（不是 grok-build 默认）
[model.via-ccp]
model = "claude-fable-5[1m]"
base_url = "http://127.0.0.1:18765/v1"
api_backend = "messages"
context_window = 1000000
api_key = "unused"
supports_reasoning_effort = true
reasoning_effort = "high"

# 可选：图/视频工具（全局 URL，不是模型 base_url）
[endpoints]
xai_api_base_url = "http://127.0.0.1:18765/v1"
```

**3. 启动 grok-build**

```bash
grok --model grok-4.6
# 或：grok --model grok-4.5
# 或：grok --model cursor-grok
# 或：grok --model via-ccp
```

入站 `api_key` 会收下（`Authorization: Bearer …` 或 `x-api-key`；`unused`、其他占位值，以及看起来像 JWT 的 session token 视为空），但**不会**当成用户/租户身份。Grok `/v1/responses` 透传会转发会话、compaction（`x-compaction-at`、`x-compactions-remaining`）、doom-loop，以及字符集受限的 `x-grok-model-override`，不会转发 `Authorization`、`Cookie` 或 `x-grok-user-id`。

`GET /v1/models` 会带上 `model`、`context_window`、`api_backend=responses`（grok-build 官方的 OpenAI Responses）、`supports_reasoning_effort` 和 `reasoning_efforts`（grok-4.6 含 `xhigh` / `high` / `medium` / `low`）。自定义 `[model.*]` 若省略 `api_backend`，grok-build 会默认走 Chat Completions；请写 `api_backend = "responses"`。本代理不实现 `/v1/chat/completions`。`/v1/messages` 仍留给 Claude Code。

媒体路由（`/v1/images/*`、`/v1/videos/*`）转发到 `https://api.x.ai/v1`（可用 `CCP_GROK_MEDIA_BASE_URL` 覆盖）。有真实客户端 key 就转发；占位 key 和 grok-build session JWT 回退到本机已登录的 Grok OAuth。

---

## 模型

请把 `ANTHROPIC_MODEL` 和 `ANTHROPIC_SMALL_FAST_MODEL` 设为**完整模型 id**。Cursor 推荐默认：`claude-fable-5[1m]`。

其他后端使用各自的完整 id（例如 `gpt-5.6-sol[1m]`、`kimi-for-coding`、`grok-composer-2.5-fast`）。不认识的 id 会返回 **400**。

### 怎么查看支持的模型

```bash
# 看内置模型列表
claude-cursor-proxy models
claude-cursor-proxy models --full

# 服务已启动时：按 Anthropic 兼容接口列模型
#（已登录 Cursor 时，会合并 Cursor 可用模型列表）
curl -s http://127.0.0.1:18765/v1/models | jq '.data[].id'
```

## Sand 模式

Sand 是 Cursor 的独立请求面，按**模型**逐个选择。命中 Sand 规则的请求会
发送 `x-cursor-client-type: sand`；其他 Cursor 请求继续使用普通的 `cli`
（或你设置的 `CCP_CURSOR_CLIENT_TYPE`）身份。混合路由由同一个
`claude-cursor-proxy serve` 进程完成，不需要再启动第二个 Sand 二进制。
这套规则只作用于最终路由到 Cursor 的请求；Codex、Kimi、Grok 路由不受影响。

### 最快配置

```bash
claude-cursor-proxy cursor auth login
claude-cursor-proxy serve              # 保持监控 TUI 打开
```

在监控 TUI 中按 `s` 打开 **Sand Models**。用 `j`/`k` 选择模型，按空格或
回车切换，按 `a` 输入一个精确的 Cursor catalog ID。列表会标记
`[sand]` 或 `[cli]`；修改只影响新请求，并以原子方式写入 `config.json`。
TUI 需要终端；`serve --no-monitor` 仍可运行代理，但不会显示设置面板。

Sessions、Active requests、Recent requests 和 Events 面板的 Cursor 模型列
也会显示同样的 `[sand]`/`[cli]` 标记，不用打开设置就能确认请求面。Fable
会先解析别名再匹配规则：`claude-fable-5-thinking-max` 规则也覆盖常用的
`claude-fable-5[1m]`、`fable[1m]` 和 `cursor:` 写法。

推荐优先使用这套 TUI 流程管理 Sand，不需要手动编辑配置文件，也不需要再
启动第二个二进制。正在运行的 `serve` 会在下一条请求时使用保存后的规则。

### 使用已选择的模型

在 TUI 中启用模型后，再让 Claude Code 使用这个模型：

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:18765
export ANTHROPIC_AUTH_TOKEN=unused
export ANTHROPIC_MODEL="cursor:claude-fable-5"
export ANTHROPIC_SMALL_FAST_MODEL="cursor:claude-fable-5"
claude
```

如果是临时 shell 或自动化任务，可以用 `CCP_CURSOR_SAND_MODELS` 覆盖 TUI
策略：

```bash
export CCP_CURSOR_SAND_MODELS="claude-fable-5"
```

如果要使用账号提供的其他 Cursor catalog ID，可在 TUI 中按 `a` 直接填写，
例如 `gemini-3.1-pro`。

`CCP_CURSOR_SAND_MODELS` 是逗号分隔的规则，支持 `*` 和 `?`。匹配不区分
大小写，并会自动归一化 `[1m]` 以及 `cursor:`/`cursor-agent:`/
`cursor-plan:`/`cursor-ask:` 前缀。因此，`claude-fable-5` 也会匹配
`cursor:claude-fable-5[1m]`。环境变量优先于 `config.json` 的
`cursor.sandModels`；想从 TUI 编辑文件时先取消这个环境变量。需要混合
路由时请保留 `CCP_CURSOR_CLIENT_TYPE` 默认值 `cli`；把它设为 `sand` 会让
未命中规则的模型也使用 Sand。
显式设置为空值 `CCP_CURSOR_SAND_MODELS=` 会关闭全部 Sand 匹配；取消该变量
后才会重新读取 `config.json`。

### 模型目录从哪里来

代码内置的 Cursor 列表只是启动和离线时的兜底目录。已登录 Cursor 时，
代理启动会调用 `GetUsableModels`，请求 `GET /v1/models` 时也会刷新；账号
返回的实时 catalog 会合并到 TUI 和模型列表。你仍可以按 `a` 填写任意精确
ID，或直接写入环境变量，但当前 Cursor 账号必须在上游目录中提供该模型。
热切换账号或登出会立即清掉旧目录；切换前账号的并发目录请求完成后也不会回写。

### 账号用量

监控器会轮询 Cursor 只读 Dashboard 接口，显示当前账号、套餐、Auto/API
百分比、按量余额、Dashboard 费用/事件统计，以及账号提供时的 Sand/Grok
Bot 周期用量。按 `u` 打开多行用量详情，其中包含 Sand 周期和最近用量事件。
`cursor auth status` 可查看当前登录账号；macOS 没有代理/Agent 登录态时，
监控器会只读回退到 Cursor Desktop 的 `state.vscdb`。Dashboard 没提供的字段
会留空，不会伪造数据。

```bash
claude-cursor-proxy cursor auth status
```

---

## 功能

- 提供 Anthropic 兼容接口：`POST /v1/messages`、`count_tokens`、`/healthz`、`/v1/models`
- 提供 grok-build 兼容接口：`POST /v1/responses`、`POST /v1/images/generations`、`POST /v1/images/edits`、`POST /v1/videos/generations`、`GET /v1/videos/{id}`
- 主上游走 Cursor Agent 长连接；需要时可用 `CCP_CURSOR_HTTP1=1` 改走 HTTP/1
- 流式回复带 keep-alive（`ping`），长时间安静思考时 Claude Code 不易误判卡住
- 按 `ANTHROPIC_MODEL` 选后端
- 登录态由本工具保存；Cursor 也可回退到本机 Cursor Agent Keychain / `auth.json`
- 在终端里跑时有监控界面（`demo` 可模拟界面，方便截图）

---

## 配置

优先级：**环境变量 > `config.json` > 内置默认值**。

| 平台 | 配置文件路径 |
| --- | --- |
| macOS / Linux | `~/.config/claude-cursor-proxy/config.json` |
| Windows | `%APPDATA%\claude-cursor-proxy\config.json` |

可用 `CCP_CONFIG_DIR` 改配置目录。环境变量前缀仍是 **`CCP_*`**。  
若你以前用过旧项目名，`~/.config/claude-cursor-bridge/` 与 `~/.config/claude-code-proxy/` 下的登录文件仍会作为迁移回退读取。

| 变量 | 默认 | 作用 |
| --- | --- |
| `PORT` | `18765` | 监听端口 |
| `CCP_BIND_ADDRESS` | `127.0.0.1` | 监听地址（默认只本机） |
| `CCP_ADVERTISED_MODELS` | 未设置 | `GET /v1/models` 的可选逗号白名单，适合托管桌面端模型选择器 |
| `CCP_CURSOR_AUTH_TOKEN` | 未设置 | 手动覆盖 Cursor 登录令牌 |
| `CCP_CURSOR_BASE_URL` | `https://api2.cursor.sh` | Cursor API 地址 |
| `CCP_CURSOR_CLIENT_TYPE` | `cli` | 默认的 `x-cursor-client-type` 请求头 |
| `CCP_CURSOR_SAND_MODELS` | 未设置 | 逗号分隔的 Sand 模型匹配规则，支持 `*` 和 `?` |
| `CCP_CURSOR_STATE_DB` | macOS 默认使用 Cursor Desktop 状态路径 | TUI 用量回退读取的 `state.vscdb` 路径 |
| `CCP_CURSOR_HAIKU_MODEL` | `claude-haiku-4-5` | Anthropic `haiku` 别名和桌面端小模型探针实际使用的 Cursor 模型 id |
| `CCP_CURSOR_CLI_KEYCHAIN_FALLBACK` | 开 | 设 `0` / `false` 可关闭 Keychain 回退 |
| `CCP_CURSOR_EMBED_SYSTEM` | 关 | 把 Anthropic `system` 塞进 Cursor（可能触发 Fable 注入防御） |
| `CCP_CURSOR_FORCE_TOOLS_IN_PROMPT` | 关 | 强制倾倒全部 tools schema；BiDi 已默认保留 `Workflow`/`Skill` 等 |
| `CCP_CURSOR_LIVE_CONCURRENCY` | `32` | 批量类（`cursor-grok-*`）generation start 的公平并发上限（1–128） |
| `CCP_CURSOR_LIVE_INTERACTIVE_RESERVE` | `8` | 为非 Grok 模型（Gemini/Claude 等子代理）保留的受保护 start 容量；交互类可借用空闲批量槽，批量类永不可占用保留槽（0–32） |
| `CCP_CURSOR_LIVE_QUEUE_SECS` | `15` | 本地准入最多等待多久后返回可重试 HTTP 503（1–300 秒） |
| `CCP_CURSOR_LIVE_ATTACH_WAIT_MS` | `15000` | 同一操作等待 attach 交接的时间，超时后才返回本地 busy（500–60000 毫秒） |
| `CCP_CURSOR_LIVE_RESUME_ATTACH_WAIT_MS` | `4000` | 响应提交前同一操作的 attach 等待（500–5000 毫秒），保持低于 Claude Code 的 stream watchdog |
| `CCP_CURSOR_LIVE_CONFLICT_WAIT_MS` | `180000` | 等待不同操作观察当前 session Run 前进（500–600000 毫秒） |
| `CCP_CURSOR_LIVE_RESUME_WAIT_MS` | `5000` | 响应提交前等待工具结果交接；保持低于客户端 stream watchdog（500–5000 毫秒） |
| `CCP_CURSOR_LIVE_NESTED_WAIT_MS` | `1500` | 响应提交前等待嵌套 agent 交接（500–5000 毫秒） |
| `CCP_CURSOR_RESOURCE_RETRIES` | `6` | Cursor 瞬时 `ERROR_RESOURCE_EXHAUSTED` 的同请求自动重试次数（1–12）；账单、额度和 High Load 等策略型 429 不会隐藏重试 |
| `CCP_CURSOR_STEP_FAILURE_RETRIES` | `4` | 输出产生前 Cursor `Failed to run step, exceeded max retries` 的同请求自动重试次数（1–8）；输出产生后直接转发错误 |
| `CCP_CURSOR_LIVE_RESUME_RESERVE` | `4` | 为暂停后需要提交工具结果的 Run 额外保留的容量（0–16） |
| `CCP_CURSOR_OPERATION_LEDGER` | 关 | 可选的持久化操作账本（跨重启拒绝重放）。在“完成标记以下游送达为准”落地前默认关闭，避免响应丢失后客户端重试被永久拒绝 |
| `CCP_CURSOR_LIVE_TIMEOUT_SECS` | `1800` | 每段活跃模型生成的预算（最多 3600 秒；下游工具执行期间暂停） |
| `CCP_CURSOR_TOOL_TTL_SECS` | 与 live timeout 相同 | 工具批次送达下游后的最长等待时间；已准入的结果允许完成派发 |
| `CCP_CURSOR_HEARTBEAT_PROGRESS_SECS` | `600` | 只有心跳而没有模型进展时的最长思考时间 |
| `CCP_CURSOR_H2_SHARDS` | `16` | 隔离并发 conversation 的稳定 H2 客户端池数量（1–64） |
| `CCP_CURSOR_LIVE_RECOVERY_OPENS` | `4` | 进程内同时打开 ResumeAction 替代连接的上限（1–16） |
| `CCP_ANTHROPIC_SSE_PING_SECS` | `5` | 下游 SSE 心跳间隔（秒，包含 message_delta + ping；低于 Claude Code 的 10 秒 watchdog） |
| `CCP_CURSOR_NO_PROXY` | 关 | 对 Cursor API 跳过 HTTP(S)_PROXY（`1` / `true`） |
| `CCP_LOG_STDERR` / `CCP_LOG_VERBOSE` / `CCP_TRAFFIC_LOG` | 未设置 | 调试日志 |

### Claude Code 侧（非代理配置）

| 变量 / 设置 | 作用 |
| --- | --- |
| `enableWorkflows: true` | 若套餐默认关 Workflows，强制打开 |
| `ENABLE_TOOL_SEARCH=true` | 自定义 `ANTHROPIC_BASE_URL` 时重新打开 ToolSearch |
| `_CLAUDE_CODE_ASSUME_FIRST_PARTY_BASE_URL=1` | 仅在确实需要时，把代理 BASE_URL 当作 first-party |

**规则 / skills：** Claude Code 会在本地把 `CLAUDE.md` 等注入 `/v1/messages`（常为 user `<system-reminder>`）；代理会原样转发，不会 scrub 掉。顶层 `system` 默认仍不发给 Cursor（可用 `CCP_CURSOR_EMBED_SYSTEM=1`）。

**验证 `/deep-research`：** transcript 里应出现 `Workflow`（`name: deep-research`），而不是只有 Bash `curl`/`mkdir`。

示例 `config.json`：

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

上面就是 TUI 写入的配置形状；手动编辑主要用于自动化场景。完整的路由、
TUI、模型发现和用量说明见 [Sand 模式](#sand-模式)。

---

## 常见问题

| 现象 | 怎么处理 |
| --- | --- |
| macOS 报 `Killed: 9` | `codesign --force -s - "$(command -v claude-cursor-proxy)"` |
| 鉴权失败 / 401 | 重新执行 `claude-cursor-proxy cursor auth login` |
| 后台小请求 400 | 把 `ANTHROPIC_SMALL_FAST_MODEL` 设成已知的完整模型 id（可与主模型相同） |
| 工具调用重复 | 加上 `CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1` |
| `/deep-research` 只用 Bash/curl | 升级代理；transcript 应有 `Workflow`；必要时 `enableWorkflows: true` |
| 流式一直卡住 | 看日志 `~/.local/state/claude-cursor-proxy/proxy.log`；可试 `CCP_LOG_STDERR=1 CCP_TRAFFIC_LOG=1 serve --no-monitor` |
| 附带图片立即报 502 `Image not found [internal]` | 升级到把内联 `SelectedImage.data` 编码为 Protobuf 字段 8 bytes 的版本；当前 Cursor CLI 中字段 1 是 blob id。 |
| grok-build 在未付款账单或不支持的国家/区域时报 `Server error (500) - Something went wrong on our side` | 升级到 ≥0.1.47 并重启 serve。未付款是 HTTP 429 并带发票原文；地区限制是 HTTP 403 并带国家/区域原文。 |
| grok-build 在 `Cursor live open timed out` 后报 `Server error (500)` / 重复开 Cursor Run | 升级到 ≥0.1.57 并重启 serve。没有响应、接受状态不明的 live open 会 fail-closed 为 HTTP 409；本地打开槽饱和改为带抖动的 HTTP 503。 |
| Claude Code 报 `unexpected internal error` 随后 `live open timed out after 10s`（常见于 `gemini-3.6-flash-high`） | 升级到 ≥0.1.58 并重启 serve。H2 RST 后的 HTTP/1 ResumeAction 使用首次打开的预算，不再卡死在 10 秒。 |
| grok-build 报 `Conflict (409) - error sending request` / `live open timed out after 20s`，或 Claude Code 报 `Agent type 'gemini-3.6-flash-high' not found` | 升级到 ≥0.1.57 并重启 serve。只有可证明尚未连接的失败才会切换传输；没有响应的 send 不再重放。Agent/Task 的模型 slug 会改写成 `general-purpose`。 |
| grok-build 把 `<tool_use>` / `<parameter>` XML 打到正文，或报 `Cursor auth failed: /usr/bin/security: Too many open files` | 升级到 ≥0.1.51 并重启 serve。带 named parameter 的 XML 会收成工具；XML `spawn_subagent` 等到 turn 结束再一批发出；serve 会抬高 macOS 256 文件上限。 |
| grok-build 以 `Cursor finished this turn without text or tool calls` 莫名结束，或提示 `workflow` 被桥接拦截/改名 | 升级到 ≥0.1.61 并重启 serve。有心跳的有效思考不再在 240s 后被掐死；只有心跳、没有任何模型进展的 Run 最多等待 10 分钟。真正的空回合仍会重试，不再伪装成成功文本；畸形控制 XML 会被隔离；workflow/skill 保留客户端声明的精确大小写。 |
| grok-build/Grok 4.6 扇出时大量子代理失败、重复执行已完成工具、卡住不返回 token，或报 `rate_limit_error: Cursor live generation concurrency saturated` | 升级到 ≥0.1.60 并重启 serve。正常 32 路扇出直接准入，另为工具结果恢复保留 4 个槽位；溢出公平排队后返回可重试 503；conversation 分布到四个 H2 池，替代连接打开也有界。升级后请新开 Grok session。 |
| grok-build 先报 `Conflict (409) - Cursor live open timed out after 20s`，随后大量 `A Cursor live run is already active`，或请求长期停在 streaming 0 B/s | 升级到 ≥0.1.65 并重启 serve。H2 首次打开等待 90 秒；一次超时后仅临时切 HTTP/1 30 秒，再用单个只读模型目录请求半开探测 H2。仍连着的重复请求返回 HTTP 503 + `Retry-After`；原消费者已断开的相同重试会挂到进行中的 Run 并回放该段；对已完成回合的相同重试直接取回保留的原响应。真正无法判断是否已接受的 open 仍保留 409。升级后请新开 Grok session。 |
| grok-build 在工具结果后报 `Conflict (409) - Cursor resume produced no progress before the recovery deadline` | 升级到 ≥0.1.60 并重启 serve。只有 Cursor 在收到这些工具结果后发出了更新的 checkpoint，且没有新文本/工具暴露给客户端，代理才会在不重放工具的前提下重试；没有这项证明时保留 409，因为自动重放可能重复执行。 |
| grok-build 报 `Cursor tool result wait expired`，或心跳卡顿先报 502、重试后才报 409 | 升级到 ≥0.1.62 并重启 serve。工具计时从 Grok 收到批次时开始，不再消耗下一段模型生成预算；已准入的工具结果会越过 TTL 边界完成派发。无法确认结束状态的心跳 Run 会立即返回 409，因为重放可能造成重复执行。 |
| Claude Code 的 Bash 标题是整段 `python3 -c` 脚本 | 升级到 ≥0.1.48 并重启 serve。Cursor Shell 没有 description，代理会补一行短标题。 |
| 约 45 秒 502 `idle timeout` / `0 response bytes` | 升级到 ≥0.1.39 并重启 serve。仍建议 `CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1`。Clash/Surge TUN 把 `*.cursor.sh` 设为 DIRECT；仍断可试 `CCP_CURSOR_HTTP1=1` |
| 出现 `Stream idle timeout - no chunks received`，尤其是首轮会话或后台工具结果恢复前 | 升级到当前版本并重启 serve。`/v1/messages` 会先提交 Anthropic SSE 生命周期，再等待 Cursor live 建连；客户端立即收到字节，默认每 5 秒发送可刷新 watchdog 的 `message_delta` + `ping` 心跳。首个客户端可见输出前的 Cursor 建连/步骤瞬时错误会在代理内部按上限重试；`/v1/responses` 为正确映射 `response.failed` 仍保留 held-HTTP。 |
| grok-build context compaction 报 `idle timeout after 45s with no useful progress` / `0 response bytes` | 升级到 ≥0.1.81 并重启 serve。`xai-compact-*` 与 `compact_20260112` 请求会进入稳定且隔离的 Cursor live lane，Connect 心跳和重连保持有效，不再落入 buffered 的 45 秒 setup watchdog。 |
| Claude Code 显示 `Cursor live run cancelled`，或 Grok 4.6 在 `/compact` 后反复出现 `A Cursor live run is already active` | 升级到 ≥0.1.77 并重启 serve。替换 reservation 会在取消未决时同时绑定旧操作指纹和旧 Run 的持久 owner；交接 future 被中断时会把已有账本标记封存为 scoped ambiguous，而不是遗留 `Dispatched` 状态阻塞后续所有回合。首个文本/工具事件提交前的已确定取消会在同一个 SSE 请求内部重试，attach 竞态会优先重放已完成回合。 |
| 纯文本回合却 502 `Image not found [internal]` | 升级到 ≥0.1.40 并重启 serve，然后把同一条消息再发一次（该错误会清掉带过期图片 id 的 conversation checkpoint）。新开一个 Claude Code session 也可以。 |
| 502 `Conversation data missing` / `missing blobs`，且当前 session 无法恢复 | 升级到 ≥0.1.45 并重启 serve，然后重试同一条消息。失败回合会清除不可恢复的 Cursor conversation id、checkpoint 和 blob 缓存；第一次重试会在新的 Cursor conversation 中重放完整历史，无需新开 Claude Code chat。 |
| 中断回合后或仍有后台 shell 时 400 `Missing tool_result blocks for pending tools` | 升级到 ≥0.1.45 并重启 serve。当前回合没有工具结果的新请求会接管已放弃的 live 回合；真正缺项的工具结果批次仍会被拒绝。 |
| 约 25 秒 502 `broken pipe (reconnect skipped: no checkpoint)`（第一条消息） | 升级到 ≥0.1.44 并重启 serve。首轮在收到 checkpoint 前也可用 `conversation_id` 做 ResumeAction，H2 broken pipe 会切 HTTP/1。Clash/Surge TUN 把 `*.cursor.sh` 设为 DIRECT；仍断可试 `CCP_CURSOR_HTTP1=1` |
| 约 46 秒 502 `Cursor stream produced no useful progress`（Fable high） | 升级到 ≥0.1.43 并重启 serve。仅心跳的思考会等到 240 秒；完全无帧仍 45 秒失败。Clash/Surge TUN 把 `*.cursor.sh` 设为 DIRECT；仍断可试 `CCP_CURSOR_HTTP1=1` |
| 502 `Cursor live open timed out after 20s`，随后又 90 秒 buffered 502 | 升级到 ≥0.1.42 并重启 serve。H2 首次打开 20 秒；HTTP/1 90 秒且仅在 464/421 后尝试。Clash/Surge TUN 把 `*.cursor.sh` 设为 DIRECT；H2 黑洞可设 `CCP_CURSOR_HTTP1=1` |
| 约 8 分钟后 502 `error decoding response body` | 升级到 ≥0.1.41 并重启 serve。重连上限约 45 秒。Clash/Surge TUN 把 `*.cursor.sh` 设为 DIRECT；HTTP 代理模式可设 `CCP_CURSOR_NO_PROXY=1`；仍断可试 `CCP_CURSOR_HTTP1=1` |

---

## 限制

- **非官方。** 各平台服务条款与账号风险自负。
- **代理本身不做访问控制。** 默认只监听本机；若绑到公网，务必放在防火墙或带鉴权的反向代理后面。
- **限流** 跟你的上游账号走。
- **兼容是尽力而为。** 文本、工具、思考、流式在支持路径上可用；部分边界情况会近似或省略。
- **不是完整 Cursor IDE。** 超出 Claude Code / grok-build 工具循环的 workspace / 回调能力不完整。
- **Linux 预编译依赖 glibc。** Alpine / musl 请自行从源码编译。

---

## 贡献

见 [CONTRIBUTING.md](CONTRIBUTING.md)。提 PR 前请跑：`cargo fmt`、`cargo clippy -- -D warnings`、`cargo test --all`。

安全披露见 [SECURITY.md](SECURITY.md)。

## 许可

[MIT](LICENSE) — 含上游项目与本仓库维护者的版权声明。
