// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";

import { followDroidSubagent, watchDroidSessionDirectory } from "./DroidSessionTranscript.ts";

const row = (value: object) => `${JSON.stringify(value)}\n`;
const userAt = (timestamp: string, text: string) =>
  row({ type: "message", timestamp, message: { role: "user", content: [{ type: "text", text }] } });
const toolCall = (id: string) =>
  row({
    type: "message",
    id: `row:${id}`,
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
  row({
    type: "message",
    id: text,
    message: { role: "assistant", content: [{ type: "text", text }] },
  });
const outcome = row({ type: "agent_turn_outcome", reason: "completed" });

const T0 = "2026-10-07T20:00:00.000Z";
const T1 = "2026-10-07T20:00:05.000Z";
const startedAtMillis = Date.parse(T0) - 1000;

const withFile = <A, E>(use: (file: string) => Effect.Effect<A, E>) =>
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

  it.effect("emits only changed steps when nothing or another row was appended", () =>
    withFile((file) =>
      Effect.gen(function* () {
        const follow = followDroidSubagent({ file, startedAtMillis });
        yield* append(file, userAt(T0, "go") + toolCall("call-1"));
        const first = yield* follow;
        const second = yield* follow;
        assert.equal(first.work.steps.length, 1);
        assert.deepEqual(second.work.steps, []);
        yield* append(file, reply("more"));
        assert.deepEqual(
          (yield* follow).work.steps.map((step) => step.id),
          ["more:0"],
        );
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

  it.effect("counts undecodable complete rows without consuming an unfinished last row", () =>
    withFile((file) =>
      Effect.gen(function* () {
        const follow = followDroidSubagent({ file, startedAtMillis });
        const pending = reply("later");
        yield* append(
          file,
          "not-json\n" +
            row({ type: "message", message: { role: "invalid" } }) +
            pending.slice(0, 12),
        );
        const first = yield* follow;
        assert.equal(first.undecodableRows, 2);
        assert.deepEqual(first.work.steps, []);
        yield* append(file, pending.slice(12));
        const next = yield* follow;
        assert.equal(next.undecodableRows, 2);
        assert.deepEqual(
          next.work.steps.map((step) => step.id),
          ["later:0"],
        );
      }),
    ),
  );

  it.effect("restarts incremental parsing after truncation without retaining old tool joins", () =>
    withFile((file) =>
      Effect.gen(function* () {
        const follow = followDroidSubagent({ file, startedAtMillis });
        yield* append(file, toolCall("old") + reply("a sufficiently long old reply"));
        yield* follow;
        yield* Effect.promise(() => NodeFSP.writeFile(file, reply("new")));
        assert.deepEqual(
          (yield* follow).work.steps.map((step) => step.id),
          ["new:0"],
        );
        assert.deepEqual((yield* follow).work.steps, []);
      }),
    ),
  );

  it.effect("replays recorded child tools, results, and reasoning with stable identities", () =>
    withFile((file) =>
      Effect.gen(function* () {
        const content = yield* Effect.promise(() =>
          NodeFSP.readFile(new URL("./fixtures/droid-child.jsonl", import.meta.url), "utf8"),
        );
        const follow = followDroidSubagent({ file, startedAtMillis });
        yield* append(file, content);
        const state = yield* follow;
        assert.isTrue(
          state.work.steps.some(
            (step) =>
              step.kind === "message" &&
              step.reasoning === true &&
              step.text === "Recorded reasoning fixture",
          ),
        );
        assert.isTrue(
          state.work.steps.some((step) => step.kind === "tool" && step.output === "fixture output"),
        );
        assert.deepEqual((yield* follow).work.steps, []);
      }),
    ),
  );

  it.effect("observes file creation after announcement without listing the session directory", () =>
    withFile((file) =>
      Effect.scoped(
        Effect.gen(function* () {
          const watcher = yield* watchDroidSessionDirectory(NodePath.dirname(file));
          const dirty = yield* Queue.sliding<void>(1);
          const remove = watcher.listen(NodePath.basename(file), () =>
            Queue.offerUnsafe(dirty, undefined),
          );
          const follow = followDroidSubagent({ file, startedAtMillis });
          assert.deepEqual((yield* follow).work.steps, []);
          yield* append(file, reply("created"));
          yield* Queue.take(dirty);
          assert.deepEqual(
            (yield* follow).work.steps.map((step) => step.id),
            ["created:0"],
          );
          remove();
        }),
      ),
    ),
  );
});
