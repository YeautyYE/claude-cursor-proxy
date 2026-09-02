# `sand(3).py` reference audit (2026-09-03)

## Finding

`sand(3).py` is an older **Sand Client Mode installer v1.0.1**. It is not a
stream client and does not implement the current Bot/Sand wire protocol.

The script only edits unpacked Cursor JavaScript bundles:

- `SAND_CLIENT_MODE_V1` / `SAND_CLIENT_EXISTING_V1`: replace selected
  `ide`/`sand` client-type literals and `x-cursor-client-type` header defaults.
- `SAND_ELIGIBILITY_MODE_V1`: inject an early `return!1` into eligibility checks.
- Four extension/runtime targets are scanned; there are no direct stream or
  agent-exec bridge markers.
- It contains no HTTP request implementation, Connect framing, model payload,
  account selection, usage query, or `InferenceService/Stream` path.

## Current protocol represented by this proxy

The current implementation uses the newer Desktop/Bot route:

- `POST /aiserver.v1.InferenceService/Stream`
- HTTP/2 + Connect-JSON five-byte frames
- `x-cursor-client-type: sand`, desktop identity headers,
  `local-client-mode: true`, checksum and product-version headers
- `requestedModel.modelId: claude-fable-5` with `thinking`, `effort`, and
  `context=1m` parameters for `claude-fable-5[1m]`
- bounded fresh-UUID replay, stream idle recovery, and account-pool failover

Do not port the v1.0.1 bundle substitutions into the proxy; doing so would
reintroduce the obsolete client-only route and can produce the endpoint error
`Sand traffic is not supported on this endpoint`.

## Local checks

```text
python3 -m py_compile sand(3).py       PASS
sand(3).py --version                   PASS (1.0.1)
sand(3).py install                     no file changes; current Cursor 3.18.25
                                        has no matching v1.0.1 patch markers
```

The newer `sand_stream_installer.py` 1.2.5 adds direct-stream V2, managed-local
runtime, and agent-host bridge markers. It still has an exact install gate for
Cursor 3.18.9, so it reports 3.18.25 as unsupported. The proxy's direct Rust
transport does not require installing either bundle patch.
