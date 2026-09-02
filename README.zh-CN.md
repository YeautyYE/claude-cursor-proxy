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

[快速开始](#快速开始) · [模型](#模型) · [Sand 模式](#sand-模式) · [功能](#功能) · [配置](#配置) · [MCP 故障排查](#mcp-故障排查) · [常见问题](#常见问题) · [限制](#限制)

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
| 固定版本 | `CLAUDE_CURSOR_PROXY_VERSION=v0.1.101 curl -fsSL …/install.sh \| bash` |
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

#### 多账号管理

需要让新登录账号立即生效时使用 `login`；只想把账号加入账号池、保持当前账号继续
工作时使用 `add`：

```bash
claude-cursor-proxy cursor auth add --label work
claude-cursor-proxy cursor auth list
claude-cursor-proxy cursor auth use ACCOUNT_ID
claude-cursor-proxy cursor auth usage                 # 拉取全部账号
claude-cursor-proxy cursor auth usage ACCOUNT_ID      # 只拉取一个账号
claude-cursor-proxy cursor auth usage --json          # JSON 输出
```

`ACCOUNT_ID` 可以是 `list` 输出的 ID，也可以是唯一的邮箱或标签。账号池保存在
`cursor/accounts.json`，当前账号仍会镜像到原有的 `cursor/auth.json`，旧版本也能继续
读取。监控 TUI 中按 `a` 打开账号面板，按 `Enter` 切换，按 `u` 拉取选中账号用量，按
`U` 并行拉取全部账号。每个账号都有独立的刷新 worker，切换到下一行不会取消刚才
发起的账号刷新。成功快照会保存到 state 目录，下次打开 TUI 会先显示缓存；宽终端的
`Updated` 列（窄终端的选中行详情）显示最近一次 Dashboard 拉取时间。按 `r` 可刷新账号列表和全部用量。选中账号后按 `d`，再按 `y`/回车确认删除（`n`/`Esc` 取消）；删除当前账号后会立即切换到剩余账号。添加、切换或删除账号都不需要重启 `serve`。

需要把指定模型固定到某个已保存账号时，在 TUI 主界面或账号面板按 `m`。用
`j`/`k` 选择模型，按回车/空格选择某个账号或 `automatic`；按 `x` 清除
绑定，按 `a` 输入列表里暂时没有的 catalog id。修改对新请求立即生效，不会改变
当前活动账号。按模型绑定的 Live Run、conversation checkpoint、KV 状态和工具续传
会按账号隔离，因此同一个 `serve` 进程可以并发使用不同账号。TUI 会保存稳定的账号
ID；无界面配置也可以用唯一标签或邮箱作为选择器。

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
这套规则只作用于最终路由到 Cursor 的请求；原生 Codex、Kimi 和 Grok 默认
仍走各自的后端。Grok 有一个明确的例外：为 `grok-*` 名称配置
`cursor.modelAccounts` 后，该名称会进入 Cursor 的账号路由。这样同一个公开
名称可以选择原生 Grok 或 Cursor 账号，但两条路径的登录态和额度桶不同。

Sand 选择和账号选择相互独立。例如，可以先按 `s` 把 `gemini-3.1-pro` 设为
`[sand]`，再按 `m` 指定它使用某个 Cursor 账号，两项设置会同时应用到请求。

### Cursor 额度通道（Grok 返回 429 时先看这里）

Cursor 提供两个相互独立的额度通道。`modelAccounts` 只决定使用**哪个账号**，
不会把请求从一个通道切换到另一个通道：

| TUI 标记 | Cursor 请求面 | Dashboard 指标 | 含义 |
| --- | --- | --- | --- |
| `[cli]` | AgentService / `x-cursor-client-type: cli` | CLI/API（`apiPercentUsed`，以及 Auto/Total） | 普通 Cursor CLI 请求 |
| `[sand]` | Desktop InferenceService / `x-cursor-client-type: sand` | Sand/Grok Bot（`usagePercent`） | Sand 请求，包括 Cursor Grok |

百分比表示**已使用比例**：`100%` 就是该通道已耗尽。Sand/Bot 较低不会补回
已耗尽的 CLI/API；CLI/API 满额也不代表 Sand 不可用。账号面板和 `m` 模型-账号
编辑器会同时显示两个指标，并把当前模型对应的通道放在前面。按 `u` 刷新一个
账号，按 `U` 并行刷新全部账号；排查前先看 `Updated` 时间，避免把旧缓存当成
当前额度。

代理不会静默切换额度通道。遇到账号级 Sand 策略/额度错误时，会在同一通道内
依次尝试其他未冷却的已保存账号，每个请求最多切换 16 次；如果模型显式绑定账号，
故障转移仍限定在该绑定范围内。需要切换通道或固定账号时，请在 TUI 调整模型的
`[cli]`/`[sand]` 选择或账号绑定。

#### Grok 4.6：原生路由与 Cursor Sand

`grok-4.6` 是两个后端都使用的名称：

* 没有 Cursor 模型-账号绑定时，`grok-4.6` 走原生 **Grok** 后端，使用
  `grok auth login` 的凭据，不消耗 Cursor CLI/Sand 额度。
* 为 `grok-4.6` 配置 `cursor.modelAccounts`（或直接使用
  `cursor-grok-4.6-*` catalog id）后，才会走 Cursor，并使用绑定的
  `cursor auth login` / `cursor auth add` 账号。
* 要使用 Sand/Grok Bot 通道，必须在 TUI 中把精确 Cursor 模型标记为
  `[sand]`。`high` 和 `xhigh` 是独立的 catalog 行，可以绑定不同账号；账号
  余额不同的时候不要只写一个宽泛通配规则。

使用 Cursor Grok 4.6 的推荐 TUI 流程：

1. 按 `s`，必要时按 `a` 添加/选择 `cursor-grok-4.6-xhigh-fast`，再切换为
   `[sand]`。
2. 按 `m` 回到同一模型行，选择 Sand/Bot 余额充足的账号并按回车保存。
3. 让 grok-build（或 Claude Code）使用这个精确 id。grok-build 配置示例：

   ```toml
   [model.cursor-grok-sand]
   model = "cursor-grok-4.6-xhigh-fast"
   base_url = "http://127.0.0.1:18765/v1"
   api_backend = "responses"
   api_key = "unused"
   ```

   ```bash
   grok --model cursor-grok-sand
   ```

如果 Events/Requests 面板显示 `cursor-grok-4.6-xhigh-fast [cli]`，说明请求
正在消耗 CLI/API，即使绑定账号仍有 Sand/Bot 余额；请在 TUI 把同一行切换为
`[sand]`。如果已经显示 `[sand]` 且 Sand 指标有余额，再核对选中的账号名称、
邮箱和 `Updated` 时间后重试。原生 `grok --model grok-4.6` 配置仍会走 Grok
后端，除非明确为它设置 Cursor 账号绑定。

需要确认映射时，直接查看代理的结构化日志并发送一条请求。若要启动诊断实例，
请在启动 `serve` 前设置日志变量并使用空闲端口；已有 `serve` 进程保持不动：

```bash
CCP_LOG_STDERR=1 CCP_LOG_VERBOSE=1 claude-cursor-proxy serve --no-monitor --port 18766
```

让诊断客户端指向 `http://127.0.0.1:18766`；继续使用原端口时，直接查看它的
`proxy.log` 即可。

`cursor_account_selected` 记录包含 `accountBinding`；这两类记录都会包含截断后的
`accountId`、解析后的 `model`、`clientType`、`quotaLane`，以及缓存的
`apiPercent`/`botPercent`。不会记录 bearer token。把这些字段与请求徽标、账号行的
`Updated` 时间对照：字段不一致说明路由/配置有问题；字段一致可确认实际尝试的
账号和通道，但 Cursor 仍可能因策略或容量判定返回 429。

`SandClientMode` 和 `SandStreamToolkit` 是绑定特定 Cursor Desktop 版本的
bundle 补丁工具。本代理不会安装、修改或依赖打过补丁的 `Cursor.app`，而是让
每条选中的请求走自身的 Sand H2 路径。`sand-status` 中可选的 Desktop bundle
检查仅供查看，不是启动条件。

> **推荐优先使用监控 TUI 配置 Sand。** 保持一个 `serve` 进程运行，使用
> 下面的快捷键即可，不需要手动编辑配置文件，而且请求当前走 `[sand]` 还是
> `[cli]` 会直接显示出来。

| 按键 | 操作 |
| --- | --- |
| `s` | 打开 **Sand Models** 模型列表 |
| `j` / `k` | 上下选择模型 |
| `空格` / `Enter` | 在 `[sand]` 与 `[cli]` 之间切换 |
| `a` | 手动添加精确模型 id（例如 `claude-fable-5`） |
| `u` | 打开账号用量详情 |
| `a`（主界面） | 打开 Cursor 账号面板 |
| `m`（主界面） | 为 Cursor 模型指定已保存账号 |
| `Esc` / `s` | 关闭 Sand 编辑器 |

### 最快配置

```bash
claude-cursor-proxy cursor auth login
claude-cursor-proxy serve              # 保持监控 TUI 打开
```

在监控 TUI 中按 `s` 打开 **Sand Models**。用 `j`/`k` 选择模型，按空格或
回车切换，按 `a` 输入一个精确的 Cursor catalog ID（例如
`claude-fable-5`）；在模型列表中按 `u` 查看账号用量。列表会标记 `[sand]`
或 `[cli]`；修改只影响新请求，并以原子方式写入 `config.json`。TUI 需要
终端；`serve --no-monitor` 仍可运行代理，但不会显示设置面板。

Fable 是内建的 Sand/Bot 路由。全新配置或保存了任意非空 Sand 策略时，
`claude-fable-5[1m]` 会标记为 `[sand]` 并使用 Cursor Bot 通道，当前选择的
账号都可使用。只有显式空策略（`cursor.sandModels: []` 或
`CCP_CURSOR_SAND_MODELS=`）会关闭这条内建路由，让所有模型保持配置的默认身份。

需要在终端只读检查 Sand 是否完整时，可运行：

```bash
claude-cursor-proxy cursor sand-status
claude-cursor-proxy cursor sand-status --json
```

该命令会显示当前模型策略、Sand client 版本、H2 传输、代理路由标记、账号名称
和用量缓存时间。Desktop bundle 检查会明确显示为可选，且
`requiredForProxy: false`；命令不会发起 Cursor 请求，也不会消耗模型额度。
JSON 输出不包含 access/refresh token。

Sessions、Active requests、Recent requests 和 Events 面板的 Cursor 模型列
也会显示同样的 `[sand]`/`[cli]` 标记，不用打开设置就能确认请求面。Fable
会先解析别名再匹配规则：`claude-fable-5-thinking-max` 规则也覆盖常用的
`claude-fable-5[1m]`、`fable[1m]` 和 `cursor:` 写法。

推荐优先使用这套 TUI 流程管理 Sand，不需要手动编辑配置文件，也不需要再
启动第二个二进制。正在运行的 `serve` 会在下一条请求时使用保存后的规则。
环境变量和 `config.json` 是无界面自动化场景的备用入口。

### 使用已选择的模型

在 TUI 中启用模型后，再让 Claude Code 使用这个模型：

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:18765
export ANTHROPIC_AUTH_TOKEN=unused
export ANTHROPIC_MODEL="claude-fable-5"
export ANTHROPIC_SMALL_FAST_MODEL="claude-fable-5"
claude
```

如果是临时 shell 或自动化任务，可以用 `CCP_CURSOR_SAND_MODELS` 覆盖 TUI
策略：

```bash
export CCP_CURSOR_SAND_MODELS="claude-fable-5"
```

如果当前列表没有目标 Cursor catalog ID，可在 **Sand Models** 中按 `a` 直接
填写精确 id；示例仍使用 `claude-fable-5`。

`CCP_CURSOR_SAND_MODELS` 是逗号分隔的规则，支持 `*` 和 `?`。匹配不区分
大小写，并会自动归一化 `[1m]` 以及 `cursor:`/`cursor-agent:`/
`cursor-plan:`/`cursor-ask:` 前缀。因此，`claude-fable-5` 也会匹配
`cursor:claude-fable-5[1m]`。环境变量优先于 `config.json` 的
`cursor.sandModels`；想从 TUI 编辑文件时先取消这个环境变量。需要混合
路由时请保留 `CCP_CURSOR_CLIENT_TYPE` 默认值 `cli`；把它设为 `sand` 会让
未命中规则的模型也使用 Sand。
只要 Sand 策略非空，Fable 就会作为内建 Sand/Bot 路由保留，即使 TUI 中还选择了
其他模型；因此所有已登录 Cursor 账号都可以使用 `claude-fable-5[1m]`，账号选择和
额度故障切换彼此隔离。只有显式空数组 `cursor.sandModels: []` 或空值
`CCP_CURSOR_SAND_MODELS=` 才会关闭内建路由并让所有模型保持配置的默认身份。

### 模型目录从哪里来

代码内置的 Cursor 列表只是启动和离线时的兜底目录。已登录 Cursor 时，
代理启动会调用 `GetUsableModels`，请求 `GET /v1/models` 时也会刷新；在可用时
还会探测 `aiserver.v1.AiService/AvailableModels`，该目录提供规范 family id、
别名和 effort 变体，用于把 `gemini-3.6-flash-high` 之类的 CLI slug 映射到
Sand 所需的 `gemini-3.6-flash`。目录快照按账号**和请求身份**（`cli` / `sand`）
隔离，并在短 TTL 后过期，因此一个账号或通道的模型权限不会泄漏到另一个账号。
实时 catalog 会合并到 TUI 和模型列表。你仍可以按 `a` 填写任意精确 ID，或直接
写入环境变量，但当前 Cursor 账号必须在上游目录中提供该模型。热切换账号或登出
会立即清掉旧目录；切换前账号的并发目录请求完成后也不会回写。

### 账号用量

监控器会轮询 Cursor 只读 Dashboard 接口，显示当前账号、套餐、Auto/API
百分比、按量余额、Dashboard 费用/事件统计，以及账号提供时的 Sand/Grok
Bot 周期用量。按 `u` 打开多行用量详情，其中包含 Sand 周期和最近用量事件。
`cursor auth status` 可查看当前登录账号；macOS 没有代理/Agent 登录态时，
监控器会只读回退到 Cursor Desktop 的 `state.vscdb`。Dashboard 没提供的字段
会留空，不会伪造数据。无界面的 `serve --no-monitor` 会每分钟只轻量请求一次
Sand 用量，用于把额度耗尽后的空回合识别为 HTTP 429；用量展示仍以 TUI 为准。
每个账号成功拉取的 Dashboard 快照会缓存到
`~/.local/state/claude-cursor-proxy/cursor/account-usage.json`（Windows 使用
平台对应的 state 目录）。缓存只保存用量和拉取时间，不保存 access/refresh token；
文件损坏或超限时会按缓存未命中处理；已有旧快照会先显示，成功刷新后再替换。
如果 worker 异常退出或超过 watchdog，旧快照仍会保留，界面不会一直卡在刷新中；旧请求
退出后可再次按 `u` 刷新。迟到结果只有在凭据仍属于该账号时才会写回。

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

交互式配置路由和查看用量时，推荐使用监控 TUI：按 `s` 选择 Sand 模型，按
`m` 为模型指定 Cursor 账号，按 `a` 管理账号，按 `u` 打开账号用量。下面的
文件和环境变量主要用于无界面或自动化部署。

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
| `CCP_CURSOR_SAND_BASE_URL` | 跟随 `CCP_CURSOR_BASE_URL` | 可选的 Sand `InferenceService/Stream` 专用地址 |
| `CCP_CURSOR_CLIENT_TYPE` | `cli` | 默认的 `x-cursor-client-type` 请求头 |
| `CCP_CURSOR_SAND_MODELS` | 未设置 | 逗号分隔的 Sand 模型匹配规则，支持 `*` 和 `?` |
| `CCP_CURSOR_MODEL_ACCOUNTS` | 未设置 | JSON 对象或 `模型=账号` 列表；把 Cursor 模型规则绑定到账号 ID、唯一标签或邮箱，支持 `*` 和 `?` |
| `CCP_CURSOR_STATE_DB` | macOS 默认使用 Cursor Desktop 状态路径 | TUI 用量回退读取的 `state.vscdb` 路径 |
| `CCP_CURSOR_HAIKU_MODEL` | `claude-haiku-4-5` | Anthropic `haiku` 别名和桌面端小模型探针实际使用的 Cursor 模型 id |
| `CCP_CURSOR_CLI_KEYCHAIN_FALLBACK` | 开 | 设 `0` / `false` 可关闭 Keychain 回退 |
| `CCP_CURSOR_EMBED_SYSTEM` | 关 | 把 Anthropic `system` 塞进 Cursor（可能触发 Fable 注入防御） |
| `CCP_CURSOR_FORCE_TOOLS_IN_PROMPT` | 关 | 强制倾倒全部 tools schema；BiDi 已默认保留 `Workflow`/`Skill` 等 |
| `CCP_CURSOR_LIVE_CONCURRENCY` | `1024` | 批量类（`cursor-grok-*`）generation start 的公平并发上限（1–8192） |
| `CCP_CURSOR_LIVE_RUNS` | `4096` | 进程内持有 Run 槽位的 live 请求总上限（1–16384）；generation start 容量由 `CCP_CURSOR_LIVE_CONCURRENCY` 单独控制 |
| `CCP_CURSOR_LIVE_INTERACTIVE_RESERVE` | `128` | 为非 Grok 模型（Gemini/Claude 等子代理）保留的受保护 start 容量；交互类可借用空闲批量槽，批量类永不可占用保留槽（0–1024） |
| `CCP_CURSOR_LIVE_QUEUE_SECS` | `30` | 本地准入最多等待多久后返回可重试 HTTP 503（1–300 秒） |
| `CCP_CURSOR_LIVE_ATTACH_WAIT_MS` | `15000` | 同一操作等待 attach 交接的时间，超时后才返回本地 busy（500–60000 毫秒） |
| `CCP_CURSOR_LIVE_RESUME_ATTACH_WAIT_MS` | `4000` | 响应提交前同一操作的 attach 等待（500–5000 毫秒），保持低于 Claude Code 的 stream watchdog |
| `CCP_CURSOR_LIVE_CONFLICT_WAIT_MS` | `180000` | 等待不同操作观察当前 session Run 前进（500–600000 毫秒） |
| `CCP_CURSOR_LIVE_RESUME_WAIT_MS` | `5000` | 响应提交前等待工具结果交接；保持低于客户端 stream watchdog（500–5000 毫秒） |
| `CCP_CURSOR_LIVE_NESTED_WAIT_MS` | `1500` | 响应提交前等待嵌套 agent 交接（500–5000 毫秒） |
| `CCP_CURSOR_RESOURCE_RETRIES` | `6` | Cursor 瞬时 `ERROR_RESOURCE_EXHAUSTED` 的同请求自动重试次数（1–12）；账单、额度和 High Load 等策略型 429 不会隐藏重试 |
| `CCP_CURSOR_POLICY_429_COOLDOWN_SECS` | `30` | 账号/模型/Sand-or-CLI 路由策略型 429 后的本地冷却时间；该精确路由的新请求快速返回 HTTP 429 + `Retry-After`（5–600 秒） |
| `CCP_CURSOR_POLICY_429_PROBE_WINDOW_MS` | `30000` | 冷账号/模型/路由的 single-flight 窗口：有效输出会立即放行；安静超时时每个窗口只增放一个探测，不会将全部重试一次性放行（25–120000 毫秒） |
| `CCP_CURSOR_STEP_FAILURE_RETRIES` | `4` | 输出产生前 Cursor `Failed to run step, exceeded max retries` 的同请求自动重试次数（1–8）；输出产生后直接转发错误 |
| `CCP_CURSOR_LIVE_RESUME_RESERVE` | `64` | 为暂停后需要提交工具结果的 Run 额外保留的容量（0–512） |
| `CCP_CURSOR_OPERATION_LEDGER` | 关 | 可选的持久化操作账本（跨重启拒绝重放）。在“完成标记以下游送达为准”落地前默认关闭，避免响应丢失后客户端重试被永久拒绝 |
| `CCP_CURSOR_LIVE_TIMEOUT_SECS` | `1800` | 每段活跃模型生成的预算（最多 3600 秒；下游工具执行期间暂停） |
| `CCP_CURSOR_TOOL_TTL_SECS` | 与 live timeout 相同 | 工具批次送达下游后的最长等待时间；已准入的结果允许完成派发 |
| `CCP_CURSOR_HEARTBEAT_PROGRESS_SECS` | `1200` | 只有心跳而没有模型进展时的最长思考时间 |
| `CCP_CURSOR_GEMINI_FLASH_PROGRESS_SECS` | `180` | Gemini Flash 只有心跳、没有进展时的期限；尚未输出文本/工具的空跑会在同一个客户端请求内轮换并重试，避免看起来一直卡住 |
| `CCP_CURSOR_H2_SHARDS` | `16` | 隔离并发 conversation 的稳定 H2 客户端池数量（1–64） |
| `CCP_CURSOR_LIVE_RECOVERY_OPENS` | `16` | 进程内同时打开 ResumeAction 替代连接的上限（1–128） |
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
    "sandModels": ["claude-fable-5"],
    "modelAccounts": {
      "claude-fable-5": "work",
      "gemini-3.1-pro": "ACCOUNT_ID"
    }
  },
  "log": { "stderr": false, "verbose": false }
}
```

上面就是 TUI 写入的配置形状；手动编辑主要用于自动化场景。账号值可以是
`cursor auth list` 输出的 ID、唯一标签或唯一邮箱。模型规则不区分大小写，支持
`*` 和 `?`，精确模型规则优先于通配规则。纯环境变量部署可写为：

```bash
export CCP_CURSOR_MODEL_ACCOUNTS='{"claude-fable-5":"work","gemini-*":"ACCOUNT_ID"}'
```

完整的路由、TUI、模型发现和用量说明见 [Sand 模式](#sand-模式)。

---

## MCP 故障排查

如果 Claude Code 反复显示
`MCP server 'plugin:lobster-channel:lobster-channel' not connected`，这条
消息来自 Claude Code 本地的 hook dispatcher：Lobster MCP client 没有连接
或已过期。它发生在 HTTP 请求到达本代理之前，因此代理内部的流式重试不会
刷新本地 MCP registry。

已经确认的一个根因是 Lobster `1.23.0` 的本地生命周期：Claude Code 可为
每个会话启动一个插件进程，但这些进程共用同一 bridge binding。新进程接管后，
bridge 会用 `4405`（`session superseded`）关闭旧进程，而该版本会执行
`process.exit(1)`。旧 Claude 会话因此保留一个已断开的 MCP registry 条目，
随后每次 hook 都可能重复报错。多个配对进程竞争时还会出现 `4407` handshake
takeover。这属于本地 Lobster/会话生命周期问题，不是 Cursor 推理流重试失败。

按下面顺序处理：

1. 保持现有的 `serve` 进程，先使用监控 TUI：按 `s` 查看或切换 Sand
   模型，按 `u` 查看账号用量。不需要再启动第二个 Sand 二进制。
2. 检查当前项目：

   ```bash
   claude-cursor-proxy mcp-doctor --cwd "$PWD"
   claude-cursor-proxy mcp-doctor --cwd "$PWD" --json
   ```

   doctor 会扫描已安装的 `dist/server.js` 是否包含会退出进程的 4405 分支，
   统计日志中明确的 4405/4407 事件，并在多个 Lobster 进程可能竞争共享 binding
   时告警。如果报告 exit-prone runtime，请升级到 4405 后进入 dormant 状态、
   而不是结束 stdio MCP 子进程的 Lobster 版本。

3. 如果报告在 `disabledMcpServers` 中列出 Lobster，执行
   `claude-cursor-proxy mcp-doctor --cwd "$PWD" --repair`。修复前会创建带
   时间戳的备份，只移除 Lobster 条目；完成后新开 Claude Code 会话。
4. 已有会话可以先执行
   `/mcp reconnect plugin:lobster-channel:lobster-channel`。仍显示
   `not connected` 且日志已有 4405 退出时，应先更新 Lobster 再新开会话；
   registry reconnect 不能复活已经退出的子进程，也不需要重启代理。
5. 使用 `claude --bare --tools "" -p ...` 的批处理脚本应使用独立的 Claude
   配置目录，避免全局 Lobster hooks 被加载到没有对应 MCP client 的进程：

   ```bash
   BATCH_CONFIG="$(mktemp -d)"
   CLAUDE_CONFIG_DIR="$BATCH_CONFIG" claude --bare --tools "" -p "$PROMPT"
   ```

   只把批处理需要的设置放进该目录，不要复制全局 plugin/hooks 目录。
   doctor 遵循 Claude Code 的路径规则：默认读取 `~/.claude.json`；环境变量
   非空时读取 `$CLAUDE_CONFIG_DIR/.claude.json`。仅在规范文件不存在时，
   才兼容旧的 `.config.json`。

---

## 常见问题

| 现象 | 怎么处理 |
| --- | --- |
| macOS 报 `Killed: 9` | `codesign --force -s - "$(command -v claude-cursor-proxy)"` |
| 鉴权失败 / 401 | 重新执行 `claude-cursor-proxy cursor auth login` |
| 后台小请求 400 | 把 `ANTHROPIC_SMALL_FAST_MODEL` 设成已知的完整模型 id（可与主模型相同） |
| 工具调用重复 | 加上 `CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1` |
| Claude Code 提示 `Edit` 不可用并切到 `StrReplace`，或编辑调用反复重试 | 升级到 ≥0.1.83 并重启 `serve`。Claude Code 2.1.193 的 `text_editor_20250728` / `str_replace_based_edit_tool` 成对名称会端到端保留；Cursor PiEdit 替换会规范化，并回传匹配的原生结果。 |
| `/deep-research` 只用 Bash/curl | 升级代理；transcript 应有 `Workflow`；必要时 `enableWorkflows: true` |
| 流式一直卡住 | 看日志 `~/.local/state/claude-cursor-proxy/proxy.log`；可试 `CCP_LOG_STDERR=1 CCP_TRAFFIC_LOG=1 serve --no-monitor` |
| 附带图片立即报 502 `Image not found [internal]` | 升级到 ≥0.1.83 并重启 `serve`。代理会保留原始内联图片字节，只轮换一次图片 id，并在新的 Cursor conversation 中重试；上游持续报错时会直接返回，不再形成无限重试波次。 |
| grok-build 返回 413 `Cursor KV blob store limit exceeded`（`blobs=4097` / 约 64 MiB） | 升级到 ≥0.1.84 并重启 `serve`。代理会在下一回合前轮换接近上限的 Cursor conversation；如果上游先返回 413，会在全量 Anthropic 历史和新图片 id 上进行一次有界的新会话重试，不需手动 `/compact` 或新建聊天。 |
| Gemini/Fable 在 Sand 下返回 `ERROR_PRO_USER_RATE_LIMIT_EXCEEDED`，但切 CLI 正常 | Sand 与 CLI 是 Cursor 的两个请求身份和额度桶。请在 TUI 按 `s`，选中模型并切换为 `[cli]`；代理会保留清晰的 Sand 429，不会静默消耗 CLI/API 额度。 |
| `grok-build`/Claude Code 对 `grok-4.6` 或 `cursor-grok-*` 返回 HTTP 429 `You're out of usage`，但绑定账号仍有 Sand/Bot 余额 | 先看请求徽标：`[cli]` 消耗账号的 CLI/API 指标，`[sand]` 消耗 Sand/Grok Bot。模型-账号绑定只选择凭据，不会切换通道。要让 Cursor Grok 使用 Bot 额度，请在 **Sand Models** 添加精确的 `cursor-grok-4.6-*`，切换为 `[sand]`，再用 `m` 给同一行绑定账号；用 `u`/`U` 刷新并核对 `Updated` 时间。裸 `grok-4.6` 默认仍走原生 Grok，除非明确为它配置 Cursor 账号绑定。 |
| grok-build 在未付款账单或不支持的国家/区域时报 `Server error (500) - Something went wrong on our side` | 升级到 ≥0.1.47 并重启 serve。未付款是 HTTP 429 并带发票原文；地区限制是 HTTP 403 并带国家/区域原文。 |
| grok-build 在 `Cursor live open timed out` 后报 `Server error (500)` / 重复开 Cursor Run | 升级到 ≥0.1.57 并重启 serve。没有响应、接受状态不明的 live open 会 fail-closed 为 HTTP 409；本地打开槽饱和改为带抖动的 HTTP 503。 |
| Claude Code 报 `unexpected internal error` 随后 `live open timed out after 10s`（常见于 `gemini-3.6-flash-high`） | 升级到 ≥0.1.58 并重启 serve。H2 RST 后的 HTTP/1 ResumeAction 使用首次打开的预算，不再卡死在 10 秒。 |
| grok-build 报 `Conflict (409) - error sending request` / `live open timed out after 20s`，或 Claude Code 报 `Agent type 'gemini-3.6-flash-high' not found` | 升级到 ≥0.1.57 并重启 serve。只有可证明尚未连接的失败才会切换传输；没有响应的 send 不再重放。Agent/Task 的模型 slug 会改写成 `general-purpose`。 |
| grok-build 把 `<tool_use>` / `<parameter>` XML 打到正文，或报 `Cursor auth failed: /usr/bin/security: Too many open files` | 升级到 ≥0.1.51 并重启 serve。带 named parameter 的 XML 会收成工具；XML `spawn_subagent` 等到 turn 结束再一批发出；serve 会抬高 macOS 256 文件上限。 |
| grok-build 以 `Cursor finished this turn without text or tool calls` 莫名结束，或提示 `workflow` 被桥接拦截/改名 | 升级到当前版本并重启 serve。有心跳的有效思考不再在 240s 后被掐死；只有心跳、没有任何模型进展的 Run 默认最多等待 20 分钟。真正的空回合仍会重试，不再伪装成成功文本；畸形控制 XML 会被隔离；workflow/skill 保留客户端声明的精确大小写。 |
| grok-build/Grok 4.6 扇出时大量子代理失败、重复执行已完成工具、卡住不返回 token，或报 `rate_limit_error: Cursor live generation concurrency saturated` | 升级到当前版本并重启 serve。当前默认可准入 1024 个批量 start，另保护 128 个交互 start 和 64 个工具结果恢复槽位；溢出最多公平排队 30 秒；conversation 分布到 16 个 H2 池，替代连接同时打开上限为 16。升级后请新开 Grok session。 |
| grok-build 先报 `Conflict (409) - Cursor live open timed out after 20s`，随后大量 `A Cursor live run is already active`，或请求长期停在 streaming 0 B/s | 升级到 ≥0.1.65 并重启 serve。H2 首次打开等待 90 秒；一次超时后仅临时切 HTTP/1 30 秒，再用单个只读模型目录请求半开探测 H2。仍连着的重复请求返回 HTTP 503 + `Retry-After`；原消费者已断开的相同重试会挂到进行中的 Run 并回放该段；对已完成回合的相同重试直接取回保留的原响应。真正无法判断是否已接受的 open 仍保留 409。升级后请新开 Grok session。 |
| 工具结果后出现 `Cursor produced an empty turn after tool results without a newer checkpoint`，或 grok-build 报 `Conflict (409) - Cursor resume produced no progress` | 升级到 ≥0.1.82 并重启 serve。若有更新的 post-result checkpoint，代理直接续接；若 Cursor 没发 checkpoint，且没有文本或新工具暴露给客户端，代理会清除旧 Cursor 状态，并在同一个下游请求内用已包含完成 `tool_result` 的完整 Anthropic 历史重试。工具结果只发出一部分或已有客户端可见的部分输出时，仍保留歧义隔离。 |
| grok-build 报 `Cursor tool result wait expired`，或心跳卡顿先报 502、重试后才报 409 | 升级到 ≥0.1.62 并重启 serve。工具计时从 Grok 收到批次时开始，不再消耗下一段模型生成预算；已准入的工具结果会越过 TTL 边界完成派发。无法确认结束状态的心跳 Run 会立即返回 409，因为重放可能造成重复执行。 |
| Claude Code 的 Bash 标题是整段 `python3 -c` 脚本 | 升级到 ≥0.1.48 并重启 serve。Cursor Shell 没有 description，代理会补一行短标题。 |
| 约 45 秒 502 `idle timeout` / `0 response bytes` | 升级到 ≥0.1.39 并重启 serve。仍建议 `CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1`。Clash/Surge TUN 把 `*.cursor.sh` 设为 DIRECT；仍断可试 `CCP_CURSOR_HTTP1=1` |
| 出现 `Stream idle timeout - no chunks received`，尤其是首轮会话或后台工具结果恢复前 | 升级到当前版本并重启 serve。`/v1/messages` 会先提交 Anthropic SSE 生命周期，再等待 Cursor live 建连；客户端立即收到字节，默认每 5 秒发送可刷新 watchdog 的 `message_delta` + `ping` 心跳。首个客户端可见输出前的 Cursor 建连/步骤瞬时错误会在代理内部按上限重试；`/v1/responses` 为正确映射 `response.failed` 仍保留 held-HTTP。 |
| Gemini/Fable 反复返回 `ERROR_PRO_USER_RATE_LIMIT_EXCEEDED`，或 Sand 每次重发都提示 `finished this turn without text or tool calls` | 升级到 ≥0.1.82 并重启 serve。显式策略错误以及 Sand 在 100% 用量时返回的空 END 都会映射为 HTTP 429 与 `Retry-After`；短暂的冷 key 合并窗口会在大量相同 Run 打开前拦住首波重试。冷却按稳定账号、解析后的模型和 Sand/CLI 路由隔离；已接受的原生工具结果续接和 attach 仍走恢复路径。 |
| grok-build context compaction 报 `idle timeout after 45s with no useful progress` / `0 response bytes`，或响应解析器拒绝 compaction 事件 | 升级到 ≥0.1.82 并重启 serve。`xai-compact-*` 与 `compact_20260112` 请求会进入稳定且隔离的 Cursor live lane；摘要使用 Grok Build 可解析的标准 Responses assistant/output-text 事件。 |
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
