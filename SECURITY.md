# Security Policy

## Supported versions

Security fixes are applied to the latest release on the default branch. Older tags are not generally backported.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security-sensitive reports.

Prefer one of:

1. **GitHub Security Advisories** — [Report a vulnerability](https://github.com/YeautyYE/claude-cursor-proxy/security/advisories/new) on this repository (once the repo is public and advisories are enabled).
2. **Private contact** — open a draft advisory or contact the maintainer via GitHub (@YeautyYE) with a short description and reproduction steps.

Please include:

- Affected version / commit
- Impact (e.g. credential leak, remote request forgery, DoS)
- Minimal reproduction if possible

We will acknowledge receipt when we can and coordinate a fix before public disclosure.

## Scope notes

This project is a **local** Anthropic-compatible proxy. By default it binds to `127.0.0.1`. Binding to `0.0.0.0` does **not** add client authentication — only do that behind a firewall or authenticating reverse proxy.
