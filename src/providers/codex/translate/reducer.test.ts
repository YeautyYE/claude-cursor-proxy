import { describe, expect, it } from "bun:test";
import { reduceUpstream } from "./reducer.ts";
import { sse, silentLog, upstreamFromChunks } from "./test-helpers.ts";

async function events(chunks: string[]) {
  const out = [];
  for await (const event of reduceUpstream(upstreamFromChunks(chunks), silentLog)) out.push(event);
  return out;
}

describe("reduceUpstream finish metadata", () => {
  it("captures completed response id and assistant text output items", async () => {
    const out = await events([
      sse("response.output_item.added", {
        output_index: 0,
        item: { type: "message", id: "msg_upstream" },
      }),
      sse("response.output_text.delta", { output_index: 0, delta: "hello" }),
      sse("response.output_item.done", {
        output_index: 0,
        item: { type: "message", id: "msg_upstream" },
      }),
      sse("response.completed", { response: { id: "resp_1", usage: { input_tokens: 3 } } }),
    ]);

    expect(out.at(-1)).toEqual({
      kind: "finish",
      stopReason: "end_turn",
      terminalType: "response.completed",
      continuationEligible: true,
      usage: { input_tokens: 3 },
      responseId: "resp_1",
      outputItems: [
        { type: "message", role: "assistant", content: [{ type: "output_text", text: "hello" }] },
      ],
    });
  });

  it("captures sanitized Read function call arguments", async () => {
    const out = await events([
      sse("response.output_item.added", {
        output_index: 0,
        item: { type: "function_call", call_id: "call_1", name: "Read" },
      }),
      sse("response.function_call_arguments.done", {
        output_index: 0,
        arguments: '{"file_path":"/tmp/a","pages":""}',
      }),
      sse("response.output_item.done", {
        output_index: 0,
        item: {
          type: "function_call",
          call_id: "call_1",
          name: "Read",
          arguments: '{"file_path":"/tmp/a","pages":""}',
        },
      }),
      sse("response.completed", { response: { id: "resp_1", usage: {} } }),
    ]);

    expect(out.at(-1)).toMatchObject({
      kind: "finish",
      stopReason: "tool_use",
      terminalType: "response.completed",
      continuationEligible: true,
      responseId: "resp_1",
      outputItems: [
        {
          type: "function_call",
          call_id: "call_1",
          name: "Read",
          arguments: '{"file_path":"/tmp/a"}',
        },
      ],
    });
  });

  it("marks response.done as continuation eligible when complete", async () => {
    const out = await events([
      sse("response.done", {
        response: {
          id: "resp_1",
          usage: {},
        },
      }),
    ]);

    expect(out.at(-1)).toMatchObject({
      kind: "finish",
      stopReason: "end_turn",
      terminalType: "response.done",
      continuationEligible: true,
      responseId: "resp_1",
      outputItems: [],
    });
  });

  it("marks incomplete terminals as max tokens and preserves terminal type", async () => {
    const out = await events([
      sse("response.incomplete", {
        response: {
          id: "resp_1",
          status: "incomplete",
          incomplete_details: { reason: "max_output_tokens" },
          usage: {},
        },
      }),
    ]);

    expect(out.at(-1)).toMatchObject({
      kind: "finish",
      stopReason: "max_tokens",
      terminalType: "response.incomplete",
      continuationEligible: false,
      responseId: "resp_1",
      outputItems: [],
    });
  });
});
