import { describe, expect, it } from "bun:test";
import { translateStream } from "./stream.ts";
import {
  abortingUpstream,
  collect,
  sse,
  silentLog,
  upstreamFromChunks,
  upstreamThatErrorsAfterChunks,
} from "./test-helpers.ts";

let now = 0;
const realNow = Date.now;

describe("translateStream", () => {
  it("emits keepalive pings while Read arguments are buffered", async () => {
    Date.now = () => now;
    try {
      const chunks = [
        sse("response.output_item.added", {
          output_index: 0,
          item: { type: "function_call", call_id: "call_read", name: "Read" },
        }),
        sse("response.function_call_arguments.delta", { output_index: 0, delta: "{\"file_path\"" }),
        sse("response.function_call_arguments.delta", { output_index: 0, delta: ":\"/tmp/a\"}" }),
        sse("response.output_item.done", {
          output_index: 0,
          item: { type: "function_call", arguments: "{\"file_path\":\"/tmp/a\"}" },
        }),
        sse("response.completed", { response: { usage: {} } }),
      ];

      const output = await collect(
        translateStream(upstreamFromChunks(chunks, () => {
          now += 16_000;
        }), {
          messageId: "msg_1",
          model: "gpt-5.5",
          log: silentLog,
        }),
      );

      expect(output).toContain("event: content_block_start");
      expect(output).toContain("event: content_block_delta");
      expect(output.match(/event: ping/g)?.length).toBeGreaterThanOrEqual(2);
      expect(output).toContain("event: message_stop");
    } finally {
      Date.now = realNow;
      now = 0;
    }
  });

  it("emits keepalive pings for upstream keepalive events", async () => {
    Date.now = () => now;
    try {
      const chunks = [
        sse("response.output_item.added", {
          output_index: 0,
          item: { type: "message", id: "msg_upstream" },
        }),
        sse("keepalive", {}),
        sse("keepalive", {}),
        sse("response.output_item.done", {
          output_index: 0,
          item: { type: "message", id: "msg_upstream" },
        }),
        sse("response.completed", { response: { usage: {} } }),
      ];

      const output = await collect(
        translateStream(upstreamFromChunks(chunks, () => {
          now += 16_000;
        }), {
          messageId: "msg_1",
          model: "gpt-5.5",
          log: silentLog,
        }),
      );

      expect(output.match(/event: ping/g)?.length).toBeGreaterThanOrEqual(2);
      expect(output).toContain("event: message_stop");
    } finally {
      Date.now = realNow;
      now = 0;
    }
  });

  it("emits an error instead of message_stop when upstream ends with an open Read block", async () => {
    const chunks = [
      sse("response.output_item.added", {
        output_index: 0,
        item: { type: "function_call", call_id: "call_read", name: "Read" },
      }),
      sse("response.function_call_arguments.delta", { output_index: 0, delta: "{\"file_path\"" }),
      sse("response.function_call_arguments.delta", { output_index: 0, delta: ":\"/tmp/a\"" }),
    ];

    const output = await collect(
      translateStream(upstreamFromChunks(chunks), {
        messageId: "msg_1",
        model: "gpt-5.5",
        log: silentLog,
      }),
    );

    expect(output).toContain("event: content_block_start");
    expect(output).toContain("event: content_block_stop");
    expect(output).toContain("event: error");
    expect(output).toContain("Upstream stream ended without a terminal response event");
    expect(output).not.toContain("input_json_delta");
    expect(output).not.toContain("event: message_stop");
  });

  it("reports upstream read failures as upstream stream errors", async () => {
    const chunks = [
      sse("response.output_item.added", {
        output_index: 0,
        item: { type: "function_call", call_id: "call_read", name: "Read" },
      }),
      sse("response.function_call_arguments.delta", { output_index: 0, delta: "{\"file_path\"" }),
    ];
    const err = new DOMException("The connection was closed.", "AbortError");
    const logs: Array<{ msg: string; fields: unknown }> = [];
    const log = {
      ...silentLog,
      warn: (msg: string, fields: unknown) => logs.push({ msg, fields }),
    };

    const output = await collect(
      translateStream(upstreamThatErrorsAfterChunks(chunks, err), {
        messageId: "msg_1",
        model: "gpt-5.5",
        log,
      }),
    );

    expect(output.indexOf("event: content_block_stop")).toBeLessThan(output.indexOf("event: error"));
    expect(output).toContain("event: error");
    expect(output).toContain("Upstream stream read failed: The connection was closed.");
    expect(output).not.toContain("input_json_delta");
    expect(logs.some((entry) => entry.msg === "upstream stream error")).toBe(true);
  });

  it("fails buffered Read arguments that exceed the safe duration", async () => {
    Date.now = () => now;
    try {
      const chunks = [
        sse("response.output_item.added", {
          output_index: 0,
          item: { type: "function_call", call_id: "call_read", name: "Read" },
        }),
        sse("response.function_call_arguments.delta", { output_index: 0, delta: "{\"file_path\"" }),
        sse("response.function_call_arguments.delta", { output_index: 0, delta: ":\"/tmp/a\"" }),
      ];

      const output = await collect(
        translateStream(upstreamFromChunks(chunks, () => {
          now += 121_000;
        }), {
          messageId: "msg_1",
          model: "gpt-5.5",
          log: silentLog,
        }),
      );

      expect(output).toContain("Buffered Read tool arguments exceeded safe limits");
      expect(output).toContain("event: content_block_stop");
      expect(output).toContain("event: error");
      expect(output).not.toContain("input_json_delta");
      expect(output).not.toContain("event: message_stop");
    } finally {
      Date.now = realNow;
      now = 0;
    }
  });

  it("treats aborted upstream reads as cancellation", async () => {
    const abort = new AbortController();
    abort.abort();
    const err = new DOMException("The connection was closed.", "AbortError");

    const output = await collect(
      translateStream(abortingUpstream(err), {
        messageId: "msg_1",
        model: "gpt-5.5",
        log: silentLog,
        signal: abort.signal,
      }),
    );

    expect(output).not.toContain("event: error");
  });
});
