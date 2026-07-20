# Contributing

Thanks for contributing to `claude-cursor-proxy`.

## Development setup

```bash
cargo build
cargo test --all
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

CI runs the same checks on every pull request.

## Pull requests

1. Keep changes focused — one concern per PR when practical.
2. Do not invent “100% parity” claims in docs; keep limitations honest.
3. If you change user-facing behavior (CLI flags, env vars, install), update **both** `README.md` and `README.zh-CN.md`.
4. Keep the binary/crate name `claude-cursor-proxy`.
5. Do not commit secrets, local auth files, or reverse-engineering dumps under `claudedocs/`.

## Releases

Maintainers publish by tagging `v*` (see `.github/workflows/release.yml`). Packaging notes live in `PUBLISHING.md`.

## Code of conduct

Be respectful. Harassment or abuse is not welcome.
