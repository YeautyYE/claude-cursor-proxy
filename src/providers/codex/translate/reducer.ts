import { createSseStreamStats, parseSseStream, type SseStreamStats } from "../../../sse.ts";
import type { Logger } from "../../../log.ts";
import { logVerbose } from "../../../config.ts";

export class UpstreamStreamError extends Error {
  constructor(
    public kind: "rate_limit" | "failed",
    message: string,
    public retryAfterSeconds?: number,
  ) {
    super(message);
    this.name = "UpstreamStreamError";
  }
}

export interface CodexUsage {
  input_tokens?: number;
  output_tokens?: number;
  input_tokens_details?: { cached_tokens?: number };
  output_tokens_details?: { reasoning_tokens?: number };
}

export type StopReason = "end_turn" | "tool_use" | "max_tokens";

export type ReducerEvent =
  | { kind: "text-start"; index: number }
  | { kind: "text-delta"; index: number; text: string }
  | { kind: "text-stop"; index: number }
  | { kind: "tool-start"; index: number; id: string; name: string }
  | { kind: "tool-delta"; index: number; partialJson: string }
  | { kind: "tool-progress"; index: number }
  | { kind: "progress" }
  | { kind: "tool-stop"; index: number }
  | { kind: "finish"; stopReason: StopReason; usage: CodexUsage | undefined };

interface TextState {
  kind: "text";
  index: number;
  textAccum: string;
}
interface ToolState {
  kind: "tool";
  index: number;
  outputIndex: number;
  callId: string;
  name: string;
  argsAccum: string;
  deltaCount: number;
  startedAt: number;
  lastProgressLogAt: number;
  largeArgsLogged: boolean;
  hadDelta: boolean;
  bufferUntilDone: boolean;
  emittedArgs: boolean;
}
type BlockState = TextState | ToolState;

const BUFFERED_TOOL_PROGRESS_LOG_INTERVAL_MS = 30_000;
const BUFFERED_TOOL_LARGE_ARGS_BYTES = 1_000_000;
const BUFFERED_TOOL_MAX_ARGS_BYTES = 5_000_000;
const BUFFERED_TOOL_MAX_DURATION_MS = 120_000;

function shouldBufferToolArgs(name: string): boolean {
  return name === "Read";
}

function sanitizeToolArgs(name: string, args: string): string {
  if (name !== "Read" || !args) return args;
  try {
    const parsed = JSON.parse(args);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return args;
    if (!("pages" in parsed) || parsed.pages !== "") return args;
    const sanitized = { ...parsed };
    delete sanitized.pages;
    return JSON.stringify(sanitized);
  } catch {
    return args;
  }
}

function toolArgSummary(args: string): Record<string, unknown> {
  const trimmed = args.trimEnd();
  return {
    length: args.length,
    trimmedLength: trimmed.length,
    trailingWhitespace: args.length - trimmed.length,
    prefix: args.slice(0, 120),
    suffix: args.slice(-120),
  };
}

function toolArgJsonState(args: string): Record<string, unknown> {
  const trimmed = args.trimEnd();
  try {
    const parsed = JSON.parse(trimmed);
    return {
      parseOk: true,
      parsedKeys: parsed && typeof parsed === "object" && !Array.isArray(parsed) ? Object.keys(parsed) : undefined,
      trimmedLength: trimmed.length,
      trailingWhitespace: args.length - trimmed.length,
    };
  } catch (err) {
    return {
      parseOk: false,
      parseError: err instanceof Error ? err.message : String(err),
      trimmedLength: trimmed.length,
      trailingWhitespace: args.length - trimmed.length,
    };
  }
}

function logBufferedToolProgress(log: Logger, state: ToolState, force = false): void {
  if (!state.bufferUntilDone) return;
  const now = Date.now();
  const large = state.argsAccum.length >= BUFFERED_TOOL_LARGE_ARGS_BYTES && !state.largeArgsLogged;
  const stale = now - state.lastProgressLogAt >= BUFFERED_TOOL_PROGRESS_LOG_INTERVAL_MS;
  if (!force && !large && !stale) return;
  if (large) state.largeArgsLogged = true;
  state.lastProgressLogAt = now;
  log.info("buffered tool arguments progress", {
    outputIndex: state.outputIndex,
    index: state.index,
    callId: state.callId,
    name: state.name,
    deltaCount: state.deltaCount,
    elapsedMs: now - state.startedAt,
    args: toolArgSummary(state.argsAccum),
  });
}

function throwIfBufferedToolExceeded(log: Logger, state: ToolState): void {
  if (!state.bufferUntilDone) return;
  const elapsedMs = Date.now() - state.startedAt;
  if (state.argsAccum.length <= BUFFERED_TOOL_MAX_ARGS_BYTES && elapsedMs <= BUFFERED_TOOL_MAX_DURATION_MS) return;
  log.warn("buffered tool arguments exceeded safe limits", {
    outputIndex: state.outputIndex,
    index: state.index,
    callId: state.callId,
    name: state.name,
    deltaCount: state.deltaCount,
    elapsedMs,
    args: toolArgSummary(state.argsAccum),
    json: toolArgJsonState(state.argsAccum),
  });
  throw new UpstreamStreamError(
    "failed",
    `Buffered ${state.name} tool arguments exceeded safe limits`,
  );
}

function describeOpenBlock(outputIndex: number, state: BlockState): Record<string, unknown> {
  if (state.kind === "text") {
    return {
      outputIndex,
      index: state.index,
      kind: state.kind,
      textLength: state.textAccum.length,
    };
  }
  return {
    outputIndex,
    index: state.index,
    kind: state.kind,
    callId: state.callId,
    name: state.name,
    deltaCount: state.deltaCount,
    elapsedMs: Date.now() - state.startedAt,
    bufferUntilDone: state.bufferUntilDone,
    emittedArgs: state.emittedArgs,
    args: toolArgSummary(state.argsAccum),
    json: toolArgJsonState(state.argsAccum),
  };
}

/**
 * Single source of truth for translating Codex Responses SSE into a
 * stream of typed, downstream-agnostic ReducerEvents. Both the streaming
 * and non-streaming frontends consume this generator.
 *
 * Throws UpstreamStreamError on codex.rate_limits.limit_reached or
 * response.failed/response.error. Any usage that arrived before the
 * failure is discarded.
 */
export interface UpstreamStreamDiagnostics {
  stats: SseStreamStats;
  lastEventType?: string;
  sawTerminalEvent: boolean;
}

export function createUpstreamStreamDiagnostics(): UpstreamStreamDiagnostics {
  return {
    stats: createSseStreamStats(),
    sawTerminalEvent: false,
  };
}

export async function* reduceUpstream(
  upstream: ReadableStream<Uint8Array>,
  log: Logger,
  diagnostics = createUpstreamStreamDiagnostics(),
): AsyncGenerator<ReducerEvent> {
  const blocksByOutputIndex = new Map<number, BlockState>();
  const itemIdToOutputIndex = new Map<string, number>();
  let anthropicIndex = 0;
  let sawToolUse = false;
  let finalUsage: CodexUsage | undefined;
  let incomplete = false;

  for await (const evt of parseSseStream(upstream, diagnostics.stats)) {
    if (!evt.data) continue;
    let p: any;
    try {
      p = JSON.parse(evt.data);
    } catch (err) {
      log.warn("upstream sse: invalid json", { err: String(err), preview: evt.data.slice(0, 200) });
      continue;
    }
    const t: string = p.type || evt.event || "";
    diagnostics.lastEventType = t;

    if (logVerbose())
      log.debug("upstream event", { type: t, output_index: p.output_index, item_id: p.item_id });

    if (t === "codex.rate_limits") {
      if (p.rate_limits?.limit_reached) {
        throw new UpstreamStreamError(
          "rate_limit",
          "rate limit reached",
          p.rate_limits?.primary?.reset_after_seconds,
        );
      }
      yield { kind: "progress" };
      continue;
    }
    if (t === "keepalive") {
      yield { kind: "progress" };
      continue;
    }
    if (t === "response.failed" || t === "response.error" || t === "error") {
      const message = p?.response?.error?.message || p?.error?.message || "Upstream error";
      throw new UpstreamStreamError("failed", message);
    }

    if (t === "response.output_item.added") {
      const item = p.item;
      const outputIndex: number = p.output_index;
      if (!item) continue;
      if (item.type === "reasoning") continue;
      if (item.type === "message") {
        const idx = anthropicIndex++;
        blocksByOutputIndex.set(outputIndex, { kind: "text", index: idx, textAccum: "" });
        if (item.id) itemIdToOutputIndex.set(item.id, outputIndex);
        yield { kind: "text-start", index: idx };
        continue;
      }
      if (item.type === "function_call") {
        sawToolUse = true;
        const idx = anthropicIndex++;
        const bufferUntilDone = shouldBufferToolArgs(item.name);
        blocksByOutputIndex.set(outputIndex, {
          kind: "tool",
          index: idx,
          outputIndex,
          callId: item.call_id,
          name: item.name,
          argsAccum: "",
          deltaCount: 0,
          startedAt: Date.now(),
          lastProgressLogAt: Date.now(),
          largeArgsLogged: false,
          hadDelta: false,
          bufferUntilDone,
          emittedArgs: false,
        });
        log.info("tool block started", {
          outputIndex,
          index: idx,
          callId: item.call_id,
          name: item.name,
          bufferUntilDone,
        });
        yield { kind: "tool-start", index: idx, id: item.call_id, name: item.name };
        continue;
      }

      continue;
    }

    if (t === "response.output_text.delta") {
      const outputIndex: number | undefined = p.output_index;
      const itemId: string | undefined = p.item_id;
      let state: BlockState | undefined;
      if (typeof outputIndex === "number") state = blocksByOutputIndex.get(outputIndex);
      if (!state && itemId) {
        const mapped = itemIdToOutputIndex.get(itemId);
        if (mapped !== undefined) state = blocksByOutputIndex.get(mapped);
      }
      if (!state || state.kind !== "text") continue;
      const delta: string = p.delta ?? "";
      if (!delta) continue;
      state.textAccum += delta;
      yield { kind: "text-delta", index: state.index, text: delta };
      continue;
    }

    if (t === "response.function_call_arguments.delta") {
      const state = blocksByOutputIndex.get(p.output_index);
      if (!state || state.kind !== "tool") continue;
      const delta: string = p.delta ?? "";
      if (!delta) continue;
      state.argsAccum += delta;
      state.deltaCount += 1;
      state.hadDelta = true;
      logBufferedToolProgress(log, state);
      throwIfBufferedToolExceeded(log, state);
      if (!state.bufferUntilDone) {
        state.emittedArgs = true;
        yield { kind: "tool-delta", index: state.index, partialJson: delta };
      } else {
        yield { kind: "tool-progress", index: state.index };
      }
      continue;
    }

    if (t === "response.function_call_arguments.done") {
      const state = blocksByOutputIndex.get(p.output_index);
      if (!state || state.kind !== "tool") continue;
      if (typeof p.arguments === "string" && !state.argsAccum) {
        state.argsAccum = p.arguments;
      }
      log.info("tool arguments done", {
        outputIndex: p.output_index,
        index: state.index,
        callId: state.callId,
        name: state.name,
        deltaCount: state.deltaCount,
        elapsedMs: Date.now() - state.startedAt,
        args: toolArgSummary(state.argsAccum),
      });
      continue;
    }

    if (t === "response.output_item.done") {
      const item = p.item;
      const state = blocksByOutputIndex.get(p.output_index);
      if (!state) continue;
      if (!item) {
        log.warn("output item done without item", {
          outputIndex: p.output_index,
          stateKind: state.kind,
        });
        if (state.kind === "text") yield { kind: "text-stop", index: state.index };
        else yield { kind: "tool-stop", index: state.index };
        blocksByOutputIndex.delete(p.output_index);
        continue;
      }
      if (item.type === "reasoning") continue;
      if (state.kind === "tool") {
        const finalArgs =
          (typeof item.arguments === "string" && item.arguments.length
            ? item.arguments
            : state.argsAccum) || "";
        log.info("tool output item done", {
          outputIndex: p.output_index,
          index: state.index,
          callId: state.callId,
          name: state.name,
          itemType: item.type,
          deltaCount: state.deltaCount,
          elapsedMs: Date.now() - state.startedAt,
          finalArgs: toolArgSummary(finalArgs),
        });
        if (finalArgs.length) {
          state.argsAccum = sanitizeToolArgs(state.name, finalArgs);
          if (state.bufferUntilDone || !state.emittedArgs) {
            state.emittedArgs = true;
            yield { kind: "tool-delta", index: state.index, partialJson: state.argsAccum };
          }
        }
      }
      if (state.kind === "text") {
        log.debug("text block complete", { index: state.index, text: state.textAccum });
        yield { kind: "text-stop", index: state.index };
      } else {
        log.debug("tool block complete", {
          index: state.index,
          callId: state.callId,
          name: state.name,
          args: toolArgSummary(state.argsAccum),
        });
        yield { kind: "tool-stop", index: state.index };
      }
      blocksByOutputIndex.delete(p.output_index);
      continue;
    }

    if (t === "response.completed" || t === "response.incomplete") {
      diagnostics.sawTerminalEvent = true;
      finalUsage = p.response?.usage;
      const reason = p.response?.incomplete_details?.reason;
      if (
        t === "response.incomplete" ||
        reason === "max_output_tokens" ||
        p.response?.status === "incomplete"
      ) {
        incomplete = true;
      }
      continue;
    }
  }

  const openBlocks = Array.from(blocksByOutputIndex, ([outputIndex, state]) =>
    describeOpenBlock(outputIndex, state),
  );
  if (!diagnostics.sawTerminalEvent || openBlocks.length) {
    log.warn("upstream stream ended without complete response", {
      sawTerminalEvent: diagnostics.sawTerminalEvent,
      lastEventType: diagnostics.lastEventType,
      openBlocks,
      stats: diagnostics.stats,
    });
    throw new UpstreamStreamError(
      "failed",
      diagnostics.sawTerminalEvent
        ? "Upstream stream ended with open output blocks"
        : "Upstream stream ended without a terminal response event",
    );
  }

  const stopReason: StopReason = incomplete ? "max_tokens" : sawToolUse ? "tool_use" : "end_turn";
  yield { kind: "finish", stopReason, usage: finalUsage };
}

export function mapUsageToAnthropic(u: CodexUsage | undefined): {
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens: number;
  cache_read_input_tokens: number;
} {
  const cachedTokens = u?.input_tokens_details?.cached_tokens ?? 0;
  const totalInputTokens = u?.input_tokens ?? 0;
  return {
    // OpenAI-style usage reports cached prompt tokens inside input_tokens.
    // Anthropic-style usage reports cache reads separately, and Claude Code
    // sums input_tokens + cache_read_input_tokens when deciding context size.
    // Subtract cached reads here so the downstream total matches the real
    // prompt window instead of double-counting cached context.
    input_tokens: Math.max(0, totalInputTokens - cachedTokens),
    output_tokens: u?.output_tokens ?? 0,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: cachedTokens,
  };
}
