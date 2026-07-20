# claude-cursor-proxy

**[English](README.md) | 中文**

[![CI](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/ci.yml)
[![Release](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/release.yml/badge.svg)](https://github.com/YeautyYE/claude-cursor-proxy/actions/workflows/release.yml)
[![GitHub Release](https://img.shields.io/github/v/release/YeautyYE/claude-cursor-proxy?display_name=tag)](https://github.com/YeautyYE/claude-cursor-proxy/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Platforms](https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey)](https://github.com/YeautyYE/claude-cursor-proxy/releases)

基于 [raine/claude-code-proxy](https://github.com/raine/claude-code-proxy) 改进。本地 **单向代理**（Claude Code → 本代理 → Cursor）。命令行工具名与仓库名均为 **`claude-cursor-proxy`**。

**让 Claude Code 稳定调用 Cursor 上的模型（推荐 Fable 5）。**

```
Claude Code ──Anthropic /v1/messages──► claude-cursor-proxy (:18765)
                                              │
                                              ├── Cursor (Fable 5)   ← 主路径
                                              ├── Codex             ← 额外后端
                                              ├── Kimi
                                              └── Grok
```

[快速开始](#快速开始) · [模型](#模型) · [功能](#功能) · [配置](#配置) · [常见问题](#常见问题) · [限制](#限制)

---

## 这是什么

Claude Code 只认 Anthropic 的接口（`/v1/messages` 等）。  
Cursor 用的是自己的 Agent 协议，两边直接连不上。

本工具在本机跑一个单向代理（默认 `127.0.0.1:18765`）：

1. Claude Code 照常发 Anthropic 请求
2. 代理转成 Cursor 能懂的请求，再发给 Cursor
3. 把 Cursor 的流式回复转回 Anthropic 格式给 Claude Code  
   （会定期发 keep-alive，避免长时间思考被 Claude Code 当成卡住而断开）

**主路径是 Cursor（Fable 5）**：设 `ANTHROPIC_MODEL=claude-fable-5[1m]` 即可。  
同一进程里还可选 Codex / Kimi / Grok 等额外后端。

> 本项目与 Anthropic、Cursor、OpenAI、Moonshot、xAI 均无官方关联。

---

## 为什么用它

| | |
| --- | --- |
| **会话更稳** | 上游连 Cursor 长连接；下游给 Claude Code 定期 `ping`，长思考不易被掐断 |
| **Fable 5** | 设 `ANTHROPIC_MODEL=claude-fable-5[1m]`（`ANTHROPIC_SMALL_FAST_MODEL` 写同样的即可） |
| **用量 / 上下文** | 把 Cursor 的用量信息转成 Anthropic 的 `usage`，状态栏和上下文压缩更正常 |
| **工具调用** | 尽量把 Cursor 侧工具接到 Claude Code 的工具循环里（尽力而为） |
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
| 固定版本 | `CLAUDE_CURSOR_PROXY_VERSION=v0.1.24 curl -fsSL …/install.sh \| bash` |
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

---

## 限制

- **非官方。** 各平台服务条款与账号风险自负。
- **代理本身不做访问控制。** 默认只监听本机；若绑到公网，务必放在防火墙或带鉴权的反向代理后面。
- **限流** 跟你的上游账号走。
- **兼容是尽力而为。** 文本、工具、思考、流式在支持路径上可用；部分边界情况会近似或省略。
- **不是完整 Cursor IDE。** 超出 Claude Code 工具循环的 workspace / 回调能力不完整。
- **Linux 预编译依赖 glibc。** Alpine / musl 请自行从源码编译。

---

## 贡献

见 [CONTRIBUTING.md](CONTRIBUTING.md)。提 PR 前请跑：`cargo fmt`、`cargo clippy -- -D warnings`、`cargo test --all`。

安全披露见 [SECURITY.md](SECURITY.md)。

## 许可

[MIT](LICENSE) — 含上游项目与本仓库维护者的版权声明。
