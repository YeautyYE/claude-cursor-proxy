# Publishing notes (local)

Maintainer checklist for the **new** public GitHub repo. Safe to delete after the first successful release.

## Naming decision (2026-07-20)

**Chosen public identity: `claude-cursor-bridge`**

| Candidate | Verdict |
| --- | --- |
| `claude-cursor-bridge` | **Chosen** — Claude Code + Cursor + bridge; searchable; not trademark-adjacent; matches binary & repo slug |
| `cursor-claude-proxy` | Clear, but “proxy for Claude” can sound official / Cursor-owned |
| `claude-code-proxy` | Upstream / old fork name; no longer matches Cursor-first product truth |
| `ccp-cursor` | Opaque acronym; weak brand |
| `fable-bridge` | Dates when the hero model changes |
| `cursor-for-claude` | Trademark-adjacent; sounds official |

Binary, crate, and default GitHub slug are all **`claude-cursor-bridge`**. Env prefix stays **`CCP_*`** for continuity.

## Create the empty GitHub repo

Do **not** push until the local tree is ready. Suggested steps:

```bash
# 1. Create empty public repo under YeautyYE (no README/license — local tree already has them)
gh repo create YeautyYE/claude-cursor-bridge --public --description "Stable Cursor reverse-proxy bridge for Claude Code (Fable 5)"

# 2. Point this checkout at the new remote (example)
git remote rename origin old-origin   # if needed
git remote add origin git@github.com:YeautyYE/claude-cursor-bridge.git

# 3. Push main (or your release branch)
git push -u origin HEAD

# 4. Tag a release to trigger .github/workflows/release.yml
git tag v0.1.21   # or next version
git push origin v0.1.21
```

Install one-liner after `main` + release exist:

```bash
curl -fsSL https://raw.githubusercontent.com/YeautyYE/claude-cursor-bridge/main/install.sh | bash
```

## Identity map

| Item | Value |
| --- | --- |
| GitHub | `YeautyYE/claude-cursor-bridge` |
| Binary / crate | `claude-cursor-bridge` |
| Config dir | `~/.config/claude-cursor-bridge` (override: `CCP_CONFIG_DIR`) |
| State / logs | `~/.local/state/claude-cursor-bridge` |
| Env prefix | `CCP_*` (kept) |
| Install pin | `CLAUDE_CURSOR_BRIDGE_VERSION` (legacy alias: `CLAUDE_CODE_PROXY_VERSION`) |
| Legacy config | `~/.config/claude-code-proxy` still used as auth/config read fallback |

## Docs

- Primary: `README.md` (English)
- Chinese companion: `README.zh-CN.md` (full parity)
- First screen: attribution → tagline → ASCII architecture → Fable 5 quick start

## Intentionally not done

- No git commit / push / GitHub repo creation from this checklist alone
- No Homebrew tap
- No musl static Linux builds (gnu targets only)
- No Apple Developer ID / notarization (install.sh ad-hoc codesign covers Gatekeeper `Killed: 9`)
- No second binary alias named `claude-code-proxy` (document migration; one public binary)
