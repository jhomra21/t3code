// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { readDroidWakeReply } from "./DroidSessionTranscript.ts";

const SESSION_ID = "11111111-1111-4111-8111-111111111111";
const WORKER_ID = "22222222-2222-4222-8222-222222222222";
const OTHER_WORKER_ID = "33333333-3333-4333-8333-333333333333";

const message = (role: "user" | "assistant", text: string) => ({
  type: "message",
  message: { role, content: [{ type: "text", text }] },
});
const outcome = { type: "agent_turn_outcome", reason: "completed" };
const wake = (workerId: string) =>
  message("user", `Background task completed.\ntask_id: ${workerId}\ntype: worker-luna`);

const readReply = (rows: ReadonlyArray<object>, workerId = WORKER_ID) =>
  Effect.promise(async () => {
    const home = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "droid-wake-"));
    const directory = NodePath.join(home, ".factory", "sessions", "-project");
    await NodeFSP.mkdir(directory, { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(directory, `${SESSION_ID}.jsonl`),
      `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`,
    );
    return { home };
  }).pipe(
    Effect.flatMap(({ home }) =>
      readDroidWakeReply({
        sessionId: SESSION_ID,
        childSessionId: workerId,
        environment: { FACTORY_HOME_OVERRIDE: home },
      }).pipe(Effect.ensuring(Effect.promise(() => NodeFSP.rm(home, { recursive: true })))),
    ),
  );

describe("readDroidWakeReply", () => {
  it.effect("returns the reply of an idle wake turn once its outcome is written", () =>
    Effect.gen(function* () {
      const reply = yield* readReply([
        message("user", "launch it"),
        message("assistant", "launched"),
        outcome,
        wake(WORKER_ID),
        message("assistant", "The worker finished with WORKER_OK."),
        outcome,
      ]);
      assert.equal(reply, "The worker finished with WORKER_OK.");
    }),
  );

  it.effect("is undefined until the wake message is in the file", () =>
    Effect.gen(function* () {
      assert.isUndefined(yield* readReply([message("assistant", "launched"), outcome]));
    }),
  );

  it.effect("is undefined while the wake turn has no outcome yet", () =>
    Effect.gen(function* () {
      assert.isUndefined(
        yield* readReply([
          message("assistant", "launched"),
          outcome,
          wake(WORKER_ID),
          message("assistant", "Looking at it."),
        ]),
      );
    }),
  );

  it.effect("ignores a wake for a different worker", () =>
    Effect.gen(function* () {
      assert.isUndefined(
        yield* readReply([
          message("assistant", "launched"),
          outcome,
          wake(OTHER_WORKER_ID),
          message("assistant", "Other worker."),
          outcome,
        ]),
      );
    }),
  );

  it.effect("has nothing to mirror when the wake landed inside a turn ACP already carried", () =>
    Effect.gen(function* () {
      assert.isNull(
        yield* readReply([
          message("user", "do two things"),
          message("assistant", "working"),
          wake(WORKER_ID),
          message("assistant", "Saw the worker finish."),
          outcome,
        ]),
      );
    }),
  );

  it.effect("has nothing to mirror when the wake turn has no reply text", () =>
    Effect.gen(function* () {
      assert.isNull(
        yield* readReply([message("assistant", "launched"), outcome, wake(WORKER_ID), outcome]),
      );
    }),
  );

  it.effect("skips an injected reminder between the turn outcome and the wake", () =>
    Effect.gen(function* () {
      const reply = yield* readReply([
        message("assistant", "launched"),
        outcome,
        message("user", "<system-reminder>MCP status changed</system-reminder>"),
        wake(WORKER_ID),
        message("assistant", "Done."),
        outcome,
      ]);
      assert.equal(reply, "Done.");
    }),
  );
});
