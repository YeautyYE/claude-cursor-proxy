# Sand / Claude Code 2.1.211 Compatibility

## Scope

Investigated repeated `Sand stream ended without useful progress` responses
using the selected `claude-fable-5-1-thinking-max` Sand route. No user process,
installed binary, account selection, or model route was changed.

## Verified Sources

The installed Claude executable was inspected using
`~/Temp/claude-decompile-kit`. Extraction:
`~/Temp/claude-current-decompiled-1788602558/bundled/src/entrypoints/cli.js`.

- Claude version: 2.1.211.
- Binary SHA256: `16c44461c0d13817322a73903e7c9f9d62af4b2f6362e080505b6480032e5790`.
- Extracted JavaScript SHA256: `6a7bb59be3ae6de6eb7d2854c5c1f3a029432814e56f052c5a0b9cb0d6272ce4`.
- Both hashes match the extraction metadata.
- Current Cursor Desktop protobuf JSON definitions were checked in
  `/Applications/Cursor.app/Contents/Resources/app/out/vs/workbench/workbench.desktop.main.js`.

Relevant Claude source byte offsets:

| Offset | Behavior |
| --- | --- |
| 10295362 | CLI consumes `beta.messages.create(stream: true).withResponse()` directly |
| 10305550 | `signature_delta` replaces the current thinking block signature |
| 10306135 | Completed content blocks are emitted as assistant history |
| 10312379 | EOF guard requires message start and either completed blocks or a stop reason |
| 12323672 | Tool results are ordered before other user content |
| 12373469 | Signed reasoning is stripped for model switches, not ordinary same-model turns |

The SDK's `finalText()` text-only requirement does not describe the CLI's
streaming path. A completed thinking block is not itself a CLI protocol error.

## Confirmed Gaps And Fixes

1. Sand `thinkingPart.signature` was discarded and SSE emitted the fabricated
   `cursor-proxy` signature. The real signature now survives parsing, SSE,
   buffered JSON, and tool bridging. `thinkingPart.isFinal` closes only the
   thinking block. Signatures count toward replay memory limits.
2. Historical thinking was dropped despite the request schema supporting
   `reasoningParts`. Signed and redacted assistant reasoning now uses that
   field. Old placeholder signatures and unsigned reasoning are not replayed
   as authenticated history; compaction behavior is unchanged.
3. Mixed user text and tool results were reordered to USER then TOOL. Tool
   results now remain adjacent to the previous assistant call, before user
   follow-up text.
4. Text was moved ahead of all images/files. Interleaved multimodal content
   now retains its order.
5. Schema-defined `responseInfo.errorMessage` was ignored. It now enters the
   ordinary upstream error path, including known response envelopes.

No model-family substitution was added. Production still separates the Sand
family ID `claude-fable-5-1` from the selected variant's thinking/max parameters.
This is the existing namespace conversion, not a downgrade to Fable 5.

## Diagnostics

`sand_hollow_stream` records request ID, model, filtered tool count, and bounded
numeric counters for received frames, decoded text/thinking, tool parts,
unfinished tools, response snapshots, and unknown frames. It contains no
prompts, response bodies, tool arguments, credentials, or signatures.

The optional `examples/sand_multiturn_probe.rs` issues only synthetic requests.
It uses the same model normalization and parameter derivation as production.
`CCP_CURSOR_SAND_PROBE_CASE=signed_text_continuation` selects the first-turn and
real-signature replay checks. It does not alter persistent configuration.

## Evidence Limits

Production logs show five bounded retries for each reported hollow request.
An HTTP 200 envelope was not evidence of completed inference. No raw Sand
response from those original failing requests was captured.

Live short-request checks returned useful text for first-turn, text
continuation, native tool-result history without a catalog, and text-bridge
history. The native-catalog control returned an upstream error. These checks
validate the observed protocol and request conversion, but do not reproduce
the original long conversation. No new response-field aliases or snapshot
fallbacks were introduced without wire evidence. Empty ordinary turns still
fail after bounded retries; reasoning is not relabeled as a final answer.

A further live round trip used a nontrivial arithmetic prompt. Sand returned
thinking text and an opaque signature; the next request replayed exactly one
signed `reasoningParts` entry and completed with useful text. No signature
value was logged or written to the report.

Verification: 1789 library tests, all integration suites (including the
512-concurrent-stream fixture), formatting, and strict Clippy checks passed.
These checks preceded the v0.1.115 release build.
