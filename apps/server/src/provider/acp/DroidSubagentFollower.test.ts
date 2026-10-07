// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { followDroidSubagent } from "./DroidSessionTranscript.ts";

const row = (value: object) => `${JSON.stringify(value)}\n`;
const userAt = (timestamp: string, text: string) =>
  row({ type: "message", timestamp, message: { role: "user", content: [{ type: "text", text }] } });
const toolCall = (id: string) =>
  row({
    type: "message",
    message: {
      role: "assistant",
      content: [{ type: "tool_use", id, name: "Execute", input: { command: "sleep 5" } }],
    },
  });
const toolResult = (id: string, text: string, timestamp: string) =>
  row({
    type: "message",
    timestamp,
    message: { role: "user", content: [{ type: "tool_result", tool_use_id: id, content: text }] },
  });
const reply = (text: string) =>
  row({ type: "message", message: { role: "assistant", content: [{ type: "text", text }] } });
const outcome = row({ type: "agent_turn_outcome", reason: "completed" });

const T0 = "2026-10-07T20:00:00.000Z";
const T1 = "2026-10-07T20:00:05.000Z";
const startedAtMillis = Date.parse(T0) - 1000;

const withFile = <A>(use: (file: string) => Effect.Effect<A>) =>
  Effect.promise(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "droid-follow-"))).pipe(
    Effect.flatMap((directory) =>
      use(NodePath.join(directory, "worker.jsonl")).pipe(
        Effect.ensuring(Effect.promise(() => NodeFSP.rm(directory, { recursive: true }))),
      ),
    ),
  );
const append = (file: string, text: string) => Effect.promise(() => NodeFSP.appendFile(file, text));

describe("followDroidSubagent", () => {
  it.effect("shows a tool as running until its result is written, then completed", () =>
    withFile((file) =>
      Effect.gen(function* () {
        const follow = followDroidSubagent({ file, startedAtMillis });
        yield* append(file, userAt(T0, "go") + toolCall("call-1"));
        const running = yield* follow;
        assert.deepEqual(
          running.work.steps.map((step) => (step.kind === "tool" ? step.output : step.text)),
          [null],
        );
        assert.isUndefined(running.outcome);

        yield* append(file, toolResult("call-1", "done", T1) + reply("ALL_DONE") + outcome);
        const ended = yield* follow;
        assert.deepEqual(
          ended.work.steps.map((step) => (step.kind === "tool" ? step.output : step.text)),
          ["done", "ALL_DONE"],
        );
        assert.equal(ended.outcome, "completed");
        assert.deepEqual(ended.assistantMessages, ["ALL_DONE"]);
      }),
    ),
  );

  it.effect("does not read a row Droid is still writing until its line is complete", () =>
    withFile((file) =>
      Effect.gen(function* () {
        const follow = followDroidSubagent({ file, startedAtMillis });
        const whole = reply("HALF_WRITTEN");
        yield* append(file, userAt(T0, "go") + whole.slice(0, 20));
        assert.equal((yield* follow).work.steps.length, 0);
        yield* append(file, whole.slice(20));
        assert.deepEqual(
          (yield* follow).work.steps.map((step) => (step.kind === "message" ? step.text : "")),
          ["HALF_WRITTEN"],
        );
      }),
    ),
  );

  it.effect("returns the same work object when nothing was appended", () =>
    withFile((file) =>
      Effect.gen(function* () {
        const follow = followDroidSubagent({ file, startedAtMillis });
        yield* append(file, userAt(T0, "go") + toolCall("call-1"));
        const first = yield* follow;
        const second = yield* follow;
        assert.strictEqual(second.work, first.work);
        yield* append(file, reply("more"));
        assert.notStrictEqual((yield* follow).work, first.work);
      }),
    ),
  );

  it.effect("ignores an earlier turn's outcome in a resumed worker's file", () =>
    withFile((file) =>
      Effect.gen(function* () {
        yield* append(file, userAt("2026-10-07T10:00:00.000Z", "old") + reply("OLD") + outcome);
        const ended = yield* followDroidSubagent({ file, startedAtMillis });
        assert.isUndefined(ended.outcome);
        assert.deepEqual(ended.assistantMessages, []);
      }),
    ),
  );

  it.effect("reports nothing for a file that does not exist yet", () =>
    withFile((file) =>
      Effect.gen(function* () {
        const follow = followDroidSubagent({ file, startedAtMillis });
        const empty = yield* follow;
        assert.deepEqual(empty.work.steps, []);
        assert.isUndefined(empty.outcome);
        yield* append(file, userAt(T0, "go") + reply("LATE"));
        assert.equal((yield* follow).work.steps.length, 1);
      }),
    ),
  );
});
