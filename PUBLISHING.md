# Publishing notes (local)

Maintainer checklist for GitHub releases.

## Naming decision (2026-07-20)

**Chosen public identity: `claude-cursor-proxy`**

| Candidate | Verdict |
| --- | --- |
| `claude-cursor-proxy` | **Chosen** — one-way proxy (Claude Code → proxy → Cursor); searchable; matches binary & repo slug |
| `claude-cursor-bridge` | Previous name (v0.1.21); GitHub rename redirects; install keeps binary symlinks |
| `cursor-claude-proxy` | Clear, but “proxy for Claude” can sound official / Cursor-owned |
| `claude-code-proxy` | Upstream / old fork name; no longer matches Cursor-first product truth |

Binary, crate, and default GitHub slug are all **`claude-cursor-proxy`**. Env prefix stays **`CCP_*`** for continuity.

## Release

Tag a release to trigger `.github/workflows/release.yml` (example: `v0.1.22`), then push the tag.

Install one-liner:

```bash
curl -fsSL https://raw.githubusercontent.com/YeautyYE/claude-cursor-proxy/main/install.sh | bash
```

## Identity map

| Item | Value |
| --- | --- |
| GitHub | `YeautyYE/claude-cursor-proxy` |
| Binary / crate | `claude-cursor-proxy` |
| Config dir | `~/.config/claude-cursor-proxy` (override: `CCP_CONFIG_DIR`) |
| State / logs | `~/.local/state/claude-cursor-proxy` |
| Env prefix | `CCP_*` (kept) |
| Install pin | `CLAUDE_CURSOR_PROXY_VERSION` (legacy: `CLAUDE_CURSOR_BRIDGE_*`, `CLAUDE_CODE_PROXY_*`) |
| Legacy config | `~/.config/claude-cursor-bridge` and `~/.config/claude-code-proxy` still used as auth read fallback |

## Docs

- Primary: `README.md` (English)
- Chinese companion: `README.zh-CN.md` (full parity)
- First screen: attribution → tagline → ASCII architecture → Fable 5 quick start

## Intentionally not done

- No Homebrew tap
- No musl static Linux builds (gnu targets only)
- No Apple Developer ID / notarization (install.sh ad-hoc codesign covers Gatekeeper `Killed: 9`)
- Compatibility symlinks: `claude-cursor-bridge` / `claude-code-proxy` → `claude-cursor-proxy` (install.sh)
