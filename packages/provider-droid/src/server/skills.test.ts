import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import {
  discoverDroidProjectSkills,
  discoverDroidSkills,
  mapDroidSkills,
  mergeDroidSkills,
  readDroidSkills,
} from "./skills.ts";
import { DroidRpcError } from "./rpc.ts";

const makeRpcSpawner = (
  results: Readonly<Record<string, unknown>>,
  requestedMethods: Array<string> = [],
  failingMethods: ReadonlySet<string> = new Set(),
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
            const response = failingMethods.has(frame.method)
              ? { error: { code: -32000, message: "RPC failed" } }
              : Object.hasOwn(results, frame.method)
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

  it.effect("preserves personal and plugin skills when the native skills RPC fails", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const home = yield* fileSystem.makeTempDirectoryScoped();
      const personalDirectory = path.join(home, ".factory", "skills", "personal");
      const pluginPath = path.join(home, ".factory", "plugins", "cache", "example");
      const pluginSkillDirectory = path.join(pluginPath, "skills", "plugin");
      yield* fileSystem.makeDirectory(personalDirectory, { recursive: true });
      yield* fileSystem.makeDirectory(pluginSkillDirectory, { recursive: true });
      yield* fileSystem.writeFileString(
        path.join(personalDirectory, "SKILL.md"),
        ["---", "description: Personal skill", "---", ""].join("\n"),
      );
      yield* fileSystem.writeFileString(
        path.join(pluginSkillDirectory, "SKILL.md"),
        ["---", "name: plugin-skill", "description: Installed plugin skill", "---", ""].join("\n"),
      );
      yield* fileSystem.writeFileString(
        path.join(home, ".factory", "plugins", "installed_plugins.json"),
        JSON.stringify({
          plugins: {
            "example@marketplace": [{ installPath: pluginPath }],
          },
        }),
      );
      const requestedMethods: Array<string> = [];
      const skills = yield* discoverDroidSkills({ binaryPath: "droid" }, {}, home).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          makeRpcSpawner({}, requestedMethods, new Set(["droid.list_skills"])),
        ),
      );
      expect(skills).toEqual([
        {
          name: "personal",
          path: path.join(personalDirectory, "SKILL.md"),
          scope: "personal",
          enabled: true,
          description: "Personal skill",
        },
        {
          name: "plugin-skill",
          path: path.join(pluginSkillDirectory, "SKILL.md"),
          scope: "personal",
          enabled: true,
          description: "Installed plugin skill",
        },
      ]);
      expect(requestedMethods).toEqual(["droid.list_skills"]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("discovers project skills from SKILL.md without starting Droid", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const cwd = yield* fileSystem.makeTempDirectoryScoped();
      const skillDirectory = path.join(cwd, ".factory", "skills", "workspace-review");
      yield* fileSystem.makeDirectory(skillDirectory, { recursive: true });
      yield* fileSystem.writeFileString(
        path.join(skillDirectory, "SKILL.md"),
        ["---", "name: project-review", "description: Project skill description", "---", ""].join(
          "\n",
        ),
      );
      const agentsSkillDirectory = path.join(cwd, ".agents", "skills", "agents-skill");
      yield* fileSystem.makeDirectory(agentsSkillDirectory, { recursive: true });
      yield* fileSystem.writeFileString(
        path.join(agentsSkillDirectory, "SKILL.md"),
        ["---", "name: agents-project-skill", "description: Shared project skill", "---", ""].join(
          "\n",
        ),
      );
      const skills = yield* discoverDroidProjectSkills(cwd);
      expect(skills).toEqual([
        {
          name: "agents-project-skill",
          description: "Shared project skill",
          path: path.join(agentsSkillDirectory, "SKILL.md"),
          scope: "project",
          enabled: true,
        },
        {
          name: "project-review",
          description: "Project skill description",
          path: path.join(skillDirectory, "SKILL.md"),
          scope: "project",
          enabled: true,
        },
      ]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it("merges machine skills with project skills taking precedence", () => {
    expect(
      mergeDroidSkills(
        [
          {
            name: "review",
            path: "/home/.factory/skills/review/SKILL.md",
            scope: "personal",
            enabled: true,
          },
          {
            name: "builtin",
            path: "builtin:builtin",
            scope: "builtin",
            enabled: true,
          },
          {
            name: "personal-only",
            path: "/home/.factory/skills/personal-only/SKILL.md",
            scope: "personal",
            enabled: true,
          },
        ],
        [
          {
            name: "review",
            path: "/repo/.factory/skills/review/SKILL.md",
            scope: "project",
            enabled: true,
          },
        ],
      ),
    ).toEqual([
      {
        name: "builtin",
        path: "builtin:builtin",
        scope: "builtin",
        enabled: true,
      },
      {
        name: "personal-only",
        path: "/home/.factory/skills/personal-only/SKILL.md",
        scope: "personal",
        enabled: true,
      },
      {
        name: "review",
        path: "/repo/.factory/skills/review/SKILL.md",
        scope: "project",
        enabled: true,
      },
    ]);
  });

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
