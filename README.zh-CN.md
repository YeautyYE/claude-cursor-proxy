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

[快速开始](#快速开始) · [模型](#模型) · [功能](#功能) · [配置](#配置) · [常见问题](#常见问题) · [限制](#限制)

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
| 固定版本 | `CLAUDE_CURSOR_PROXY_VERSION=v0.1.26 curl -fsSL …/install.sh \| bash` |
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
| `CCP_CURSOR_AUTH_TOKEN` | 未设置 | 手动覆盖 Cursor 登录令牌 |
| `CCP_CURSOR_BASE_URL` | `https://api2.cursor.sh` | Cursor API 地址 |
| `CCP_CURSOR_CLI_KEYCHAIN_FALLBACK` | 开 | 设 `0` / `false` 可关闭 Keychain 回退 |
| `CCP_CURSOR_EMBED_SYSTEM` | 关 | 把 Anthropic `system` 塞进 Cursor（可能触发 Fable 注入防御） |
| `CCP_CURSOR_FORCE_TOOLS_IN_PROMPT` | 关 | 强制倾倒全部 tools schema；BiDi 已默认保留 `Workflow`/`Skill` 等 |
| `CCP_ANTHROPIC_SSE_PING_SECS` | `15` | 下游 keep-alive 间隔（秒） |
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
  "log": { "stderr": false, "verbose": false }
}
```

检查 Cursor 登录状态：

```bash
claude-cursor-proxy cursor auth status
```

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
| grok-build 在未付款账单或不支持的国家/区域时报 `Server error (500) - Something went wrong on our side` | 升级到 ≥0.1.47 并重启 serve。未付款是 HTTP 429 并带发票原文；地区限制是 HTTP 403 并带国家/区域原文。 |
| grok-build 在 `Cursor live open timed out` 后报 `Server error (500)` / 重复开 Cursor Run | 升级到 ≥0.1.57 并重启 serve。没有响应、接受状态不明的 live open 会 fail-closed 为 HTTP 409；本地打开槽饱和改为带抖动的 HTTP 503。 |
| Claude Code 报 `unexpected internal error` 随后 `live open timed out after 10s`（常见于 `gemini-3.6-flash-high`） | 升级到 ≥0.1.58 并重启 serve。H2 RST 后的 HTTP/1 ResumeAction 使用首次打开的预算，不再卡死在 10 秒。代理不再本地限制 live open 并发。 |
| grok-build 报 `Conflict (409) - error sending request` / `live open timed out after 20s`，或 Claude Code 报 `Agent type 'gemini-3.6-flash-high' not found` | 升级到 ≥0.1.57 并重启 serve。只有可证明尚未连接的失败才会切换传输；没有响应的 send 不再重放。Agent/Task 的模型 slug 会改写成 `general-purpose`。 |
| grok-build 把 `<tool_use>` / `<parameter>` XML 打到正文，或报 `Cursor auth failed: /usr/bin/security: Too many open files` | 升级到 ≥0.1.51 并重启 serve。带 named parameter 的 XML 会收成工具；XML `spawn_subagent` 等到 turn 结束再一批发出；serve 会抬高 macOS 256 文件上限。 |
| grok-build 以 `Cursor finished this turn without text or tool calls` 莫名结束，或提示 `workflow` 被桥接拦截/改名 | 升级到 ≥0.1.52 并重启 serve。Cursor 空回合会重试，不再伪装成成功文本；畸形控制 XML 会被隔离；workflow/skill 保留客户端声明的精确大小写。 |
| grok-build/Grok 4.6 扇出时大量子代理失败、重复执行已完成工具、卡住不返回 token，或报 `rate_limit_error: Cursor live generation concurrency saturated` | 升级到 ≥0.1.58 并重启 serve。代理不再自设 generation/open 闸门，并发完全交给 Cursor 上游。升级后请新开 Grok session。 |
| Claude Code 的 Bash 标题是整段 `python3 -c` 脚本 | 升级到 ≥0.1.48 并重启 serve。Cursor Shell 没有 description，代理会补一行短标题。 |
| 约 45 秒 502 `idle timeout` / `0 response bytes` | 升级到 ≥0.1.39 并重启 serve。仍建议 `CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1`。Clash/Surge TUN 把 `*.cursor.sh` 设为 DIRECT；仍断可试 `CCP_CURSOR_HTTP1=1` |
| 后台工具结果恢复前出现 `Stream idle timeout - no chunks received` | 升级到 ≥0.1.45 并重启 serve。live 工具结果在 HTTP 响应前的分类等待由 30 秒缩短到最多 5 秒；HTTP SSE 建立后仍会每 15 秒发送 Anthropic ping，保护长时间静默思考。 |
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
