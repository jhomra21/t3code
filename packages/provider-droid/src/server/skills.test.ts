import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { discoverDroidSkills, mapDroidSkills, readDroidSkills } from "./skills.ts";
import { DroidRpcError } from "./rpc.ts";

const makeRpcSpawner = (
  results: Readonly<Record<string, unknown>>,
  requestedMethods: Array<string> = [],
) =>
  ChildProcessSpawner.make(() =>
    Effect.gen(function* () {
      const output = yield* Queue.unbounded<Uint8Array>();
      let inputBuffer = "";
      const stdout = Stream.fromQueue(output);
      const stdin = Sink.forEach((chunk: Uint8Array) =>
        Effect.gen(function* () {
          inputBuffer += new TextDecoder().decode(chunk);
          let newline: number;
          while ((newline = inputBuffer.indexOf("\n")) >= 0) {
            const frame = JSON.parse(inputBuffer.slice(0, newline));
            inputBuffer = inputBuffer.slice(newline + 1);
            requestedMethods.push(frame.method);
            const response = Object.hasOwn(results, frame.method)
              ? { result: results[frame.method] }
              : { error: { code: -32601, message: "Method not found" } };
            yield* Queue.offer(
              output,
              new TextEncoder().encode(
                `${JSON.stringify({
                  jsonrpc: "2.0",
                  type: "response",
                  id: frame.id,
                  ...response,
                })}\n`,
              ),
            );
          }
        }),
      );
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: Effect.never,
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin,
        stdout,
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );

describe("Droid skills", () => {
  it("maps skills and gives project skills precedence over same-name user skills", () => {
    expect(
      mapDroidSkills({
        skills: [
          {
            name: "review",
            description: "Project version",
            location: "project",
            filePath: "/repo/.factory/skills/review/SKILL.md",
            enabled: true,
            userInvocable: true,
          },
          {
            name: "disabled",
            location: "user",
            filePath: "/home/.factory/skills/disabled/SKILL.md",
            enabled: false,
            userInvocable: true,
          },
          {
            name: "review",
            description: "User version",
            location: "user",
            filePath: "/home/.factory/skills/review/SKILL.md",
            enabled: true,
            userInvocable: true,
          },
          { name: "missing-path", location: "user", enabled: true },
          {
            name: "agent-only",
            location: "builtin",
            filePath: "builtin:agent-only",
            enabled: true,
            userInvocable: false,
          },
        ],
      }),
    ).toEqual([
      {
        name: "agent-only",
        path: "builtin:agent-only",
        scope: "builtin",
        enabled: true,
        userInvocable: false,
      },
      {
        name: "disabled",
        path: "/home/.factory/skills/disabled/SKILL.md",
        scope: "user",
        enabled: false,
        userInvocable: true,
      },
      {
        name: "review",
        description: "Project version",
        path: "/repo/.factory/skills/review/SKILL.md",
        scope: "project",
        enabled: true,
        userInvocable: true,
      },
    ]);
  });

  it("lets a later project skill replace an earlier same-name user skill", () => {
    expect(
      mapDroidSkills({
        skills: [
          { name: "review", location: "user", filePath: "/home/.factory/skills/review/SKILL.md" },
          {
            name: "review",
            location: "project",
            filePath: "/repo/.factory/skills/review/SKILL.md",
          },
        ],
      }),
    ).toEqual([
      {
        name: "review",
        path: "/repo/.factory/skills/review/SKILL.md",
        scope: "project",
        enabled: true,
      },
    ]);
  });

  it("does not return names or entries without paths", () => {
    expect(
      mapDroidSkills({ skills: [{ name: "no-path" }, { filePath: "/tmp/SKILL.md" }] }),
    ).toEqual([]);
  });

  it.effect("treats an unsupported skills RPC as an empty catalog without a model turn", () =>
    Effect.gen(function* () {
      const requestedMethods: Array<string> = [];
      const skills = yield* discoverDroidSkills({ binaryPath: "droid" }, {}, "/repo").pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          makeRpcSpawner({}, requestedMethods),
        ),
      );
      expect(skills).toEqual([]);
      expect(requestedMethods).toEqual(["droid.list_skills"]);
    }),
  );

  it.effect("keeps skills RPC failures typed instead of returning a cacheable empty list", () =>
    Effect.gen(function* () {
      const result = yield* readDroidSkills({
        request: () =>
          Effect.fail(new DroidRpcError({ operation: "droid.list_skills", code: -32000 })),
        notifications: Stream.empty,
        close: Effect.void,
      }).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
    }),
  );

  it("retains a large valid skill catalog", () => {
    const skills = mapDroidSkills({
      skills: Array.from({ length: 2_000 }, (_, index) => ({
        name: `skill-${index}`,
        location: "user",
        filePath: `/home/.factory/skills/skill-${index}/SKILL.md`,
        enabled: true,
      })),
    });
    expect(skills).toHaveLength(2_000);
  });
});
