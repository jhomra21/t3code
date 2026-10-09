import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";

import { extractDroidSubagentUpdate, makeDroidSubagentSupport } from "./DroidAcp.ts";

const extract = extractDroidSubagentUpdate({
  environment: { FACTORY_HOME_OVERRIDE: "/nonexistent" },
});

const task = (rawInput: Record<string, unknown>) =>
  extract({
    toolCallId: "call_1",
    status: "inProgress",
    data: {
      rawInput: { subagent_type: "worker-luna", description: "Do it", prompt: "p", ...rawInput },
    },
  });

describe("extractDroidSubagentUpdate effort", () => {
  it("shows Task complexity as the effort a reader expects", () => {
    assert.strictEqual(task({ complexity: "light" })?.effort, "low");
    assert.strictEqual(task({ complexity: "medium" })?.effort, "medium");
    assert.strictEqual(task({ complexity: "heavy" })?.effort, "high");
  });

  it("reports no effort when complexity is missing or not one Task accepts", () => {
    assert.isUndefined(task({})?.effort);
    assert.isUndefined(task({ complexity: "high" })?.effort);
    assert.isUndefined(task({ complexity: 3 })?.effort);
    assert.isUndefined(task({ complexity: "constructor" })?.effort);
  });
});

describe("Droid blocking subagent discovery", () => {
  it.effect("discovers only a matching new child header and streams its work before its end", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const home = yield* fs.makeTempDirectoryScoped();
      const rootSessionId = "33333333-3333-4333-8333-333333333333";
      const childSessionId = "fc2a2f6c-76df-4eaf-89a7-014f8712f83f";
      const directory = `${home}/.factory/sessions/project`;
      yield* fs.makeDirectory(directory, { recursive: true });
      yield* fs.writeFileString(`${directory}/${rootSessionId}.jsonl`, "{}\n");
      const support = makeDroidSubagentSupport({ environment: { HOME: home } });
      const discover = yield* support.prepareSubagentSessionDiscovery({ rootSessionId });
      const found = yield* discover("task-call").pipe(Effect.forkChild);
      const line = (value: object) => `${JSON.stringify(value)}\n`;
      const child = `${directory}/${childSessionId}.jsonl`;
      yield* fs.writeFileString(
        child,
        line({
          type: "session_start",
          id: childSessionId,
          callingSessionId: rootSessionId,
          callingToolUseId: "task-call",
        }),
      );
      assert.equal(yield* Fiber.join(found), childSessionId);
      assert.equal(
        yield* discover("task-call"),
        childSessionId,
        "a cached match must not depend on another append",
      );
      const toolSeen = yield* Deferred.make<void>();
      const watching = yield* support
        .awaitSubagentEnd({
          childSessionId,
          rootSessionId,
          startedAtMillis: 0,
          onWork: (work) =>
            Effect.gen(function* () {
              if (work.steps.some((step) => step.kind === "tool" && step.output === null))
                yield* Deferred.succeed(toolSeen, undefined);
            }),
        })
        .pipe(Effect.forkChild);
      yield* fs.writeFileString(
        child,
        (yield* fs.readFileString(child)) +
          line({
            type: "message",
            id: "call-row",
            timestamp: "2099-01-01T00:00:00Z",
            message: { role: "user", content: [] },
          }) +
          line({
            type: "message",
            id: "assistant-row",
            message: {
              role: "assistant",
              content: [
                {
                  type: "tool_use",
                  id: "call",
                  name: "Execute",
                  input: { command: "echo fixture" },
                },
              ],
            },
          }),
      );
      yield* Deferred.await(toolSeen);
      const end = line({ type: "agent_turn_outcome", reason: "completed" });
      yield* fs.writeFileString(child, (yield* fs.readFileString(child)) + end);
      assert.equal((yield* Fiber.join(watching))?.status, "completed");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
