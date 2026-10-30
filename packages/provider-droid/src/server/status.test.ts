import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as NodeOS from "node:os";
import { vi } from "vite-plus/test";
import * as HostProcess from "@t3tools/shared/HostProcess";

import { DroidSettings } from "../settings.ts";
import {
  makeDroidEnvironment,
  makeDroidInstanceEnvironment,
  makePendingDroidProvider,
  checkDroidProviderStatus,
} from "./status.ts";

vi.mock("node:os", { spy: true });

const settings = Schema.decodeSync(DroidSettings)({ enabled: false });
const missingBinarySettings = Schema.decodeSync(DroidSettings)({
  enabled: true,
  binaryPath: "/definitely-missing/droid",
});

const userCustomModels = [
  {
    model: "claude-haiku-5-5",
    id: "custom:Claude Haiku 5.5-0",
    displayName: "Claude Haiku 5.5",
    provider: "anthropic",
    baseUrl: "https://models.example.test/anthropic",
    apiKey: "sk-user-secret",
    extraHeaders: { "x-tenant-token": "header-user-secret" },
  },
  {
    model: "gpt-6-luna",
    id: "custom:GPT-6-Luna-1",
    provider: "generic-chat-completion-api",
    baseUrl: "https://models.example.test/v1",
    apiKeyHelper: "/usr/local/bin/print-user-secret",
    apiKeyHelperTtlMs: 60000,
  },
];

const writeFakeDroid = Effect.fn("writeFakeDroid")(function* (directory: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const isWindows = (yield* HostProcess.Platform) === "win32";
  const binary = path.join(directory, isWindows ? "droid.cmd" : "droid");
  const calls = path.join(directory, "calls.jsonl");
  const script = `const fs = require("node:fs");
const path = require("node:path");
const args = process.argv.slice(2);
const sandboxSettingsPath = process.env.FACTORY_HOME_OVERRIDE && path.join(process.env.FACTORY_HOME_OVERRIDE, ".factory", "settings.json");
const sandboxSettings = sandboxSettingsPath && fs.existsSync(sandboxSettingsPath) ? fs.readFileSync(sandboxSettingsPath, "utf8") : null;
fs.appendFileSync(process.env.DROID_STATUS_CALLS, JSON.stringify({
  args,
  cwd: process.cwd(),
  home: process.env.HOME,
  factoryHome: process.env.FACTORY_HOME_OVERRIDE,
  runtimeSettingsPath: process.env.FACTORY_RUNTIME_SETTINGS_PATH,
  sandboxSettings
}) + "\\n");
if (args[0] === "--version") {
  console.log("0.236.0");
} else if (args[0] === "doctor") {
  console.log(JSON.stringify({ results: [{ id: "auth.verify", status: "pass" }] }));
} else {
  let input = "";
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (chunk) => {
    input += chunk;
    while (input.includes("\\n")) {
      const index = input.indexOf("\\n");
      const request = JSON.parse(input.slice(0, index));
      input = input.slice(index + 1);
      if (request.method !== "droid.close_session") {
        fs.appendFileSync(process.env.DROID_STATUS_CALLS, JSON.stringify({ method: request.method }) + "\\n");
      }
      const result = request.method === "droid.list_models"
        ? { models: [{
            id: "test-model",
            displayName: "Test model",
            supportedReasoningEfforts: [],
            defaultReasoningEffort: ""
          }] }
        : { skills: [] };
      process.stdout.write(JSON.stringify({
        jsonrpc: "2.0",
        type: "response",
        id: request.id,
        result
      }) + "\\n");
      if (request.method === "droid.close_session") process.exit(0);
    }
  });
}
`;
  if (isWindows) {
    yield* fileSystem.writeFileString(path.join(directory, "droid.cjs"), script);
    yield* fileSystem.writeFileString(
      binary,
      `@echo off\r\n"${process.execPath}" "%~dp0droid.cjs" %*\r\n`,
    );
  } else {
    yield* fileSystem.writeFileString(binary, `#!${process.execPath}\n${script}`);
    yield* fileSystem.chmod(binary, 0o755);
  }
  return { binary, calls };
});

const writeUserSettings = Effect.fn("writeUserSettings")(function* (
  home: string,
  contents: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fileSystem.makeDirectory(path.join(home, ".factory"), { recursive: true });
  yield* fileSystem.writeFileString(path.join(home, ".factory", "settings.json"), contents);
});

const readDiscoveryInvocations = Effect.fn("readDiscoveryInvocations")(function* (calls: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  const invocations = (yield* fileSystem.readFileString(calls))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line));
  return invocations.filter(
    (invocation) => Array.isArray(invocation.args) && invocation.args[0] === "exec",
  );
});

describe("Droid status", () => {
  it.effect("does not probe a disabled provider", () =>
    Effect.gen(function* () {
      const snapshot = yield* makePendingDroidProvider(settings);
      assert.isFalse(snapshot.enabled);
      assert.equal(snapshot.auth.status, "unknown");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("lets clients offer conversation rollback", () =>
    Effect.gen(function* () {
      const snapshot = yield* makePendingDroidProvider(missingBinarySettings);
      assert.notStrictEqual(snapshot.supportsConversationRollback, false);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("offers manual compaction only while enabled", () =>
    Effect.gen(function* () {
      const disabled = yield* makePendingDroidProvider(settings);
      const enabled = yield* makePendingDroidProvider(missingBinarySettings);
      assert.deepEqual(disabled.slashCommands, []);
      assert.deepEqual(
        enabled.slashCommands.map((command) => command.name),
        ["compact"],
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reports a missing CLI as not installed", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkDroidProviderStatus(missingBinarySettings, {}, process.cwd());
      assert.isFalse(snapshot.installed);
      assert.include(snapshot.message ?? "", "not found");
      assert.deepEqual(snapshot.skills, []);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("isolates model discovery from project hooks and MCP configuration", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fileSystem.makeTempDirectoryScoped({
        directory: NodeOS.tmpdir(),
        prefix: "droid-status-probe-",
      });
      const home = path.join(directory, "home");
      const projectCwd = home;
      const personalSkillDirectory = path.join(home, ".factory", "skills", "project-skill");
      yield* fileSystem.makeDirectory(personalSkillDirectory, { recursive: true });
      yield* fileSystem.writeFileString(
        path.join(personalSkillDirectory, "SKILL.md"),
        ["---", "name: project-skill", "description: Personal skill", "---", ""].join("\n"),
      );
      const { binary, calls } = yield* writeFakeDroid(directory);
      const statusEnvironment = {
        DROID_STATUS_CALLS: calls,
        FACTORY_RUNTIME_SETTINGS_PATH: "/project/settings.json",
      };
      const snapshot = yield* checkDroidProviderStatus(
        { ...missingBinarySettings, binaryPath: binary },
        statusEnvironment,
        projectCwd,
        home,
      );
      const invocations = (yield* fileSystem.readFileString(calls))
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line));
      const discovery = invocations.filter(
        (invocation) => Array.isArray(invocation.args) && invocation.args[0] === "exec",
      );
      const requests = invocations
        .filter((invocation) => typeof invocation.method === "string")
        .map((invocation) => invocation.method);
      assert.isTrue(snapshot.installed);
      assert.equal(snapshot.auth.status, "authenticated");
      assert.equal(snapshot.status, "ready");
      assert.isUndefined(snapshot.message);
      assert.isTrue(snapshot.models.some((model) => model.slug === "test-model"));
      assert.deepEqual(requests, ["droid.list_models", "droid.list_skills"]);
      assert.lengthOf(discovery, 2);
      for (const invocation of discovery) {
        assert.isString(invocation.cwd);
        assert.notEqual(invocation.cwd, projectCwd);
        assert.isString(invocation.factoryHome);
        assert.notEqual(invocation.factoryHome, process.env.HOME);
        assert.isString(invocation.home);
        assert.notEqual(invocation.home, process.env.HOME);
        assert.isUndefined(invocation.runtimeSettingsPath);
      }
      assert.deepEqual(
        snapshot.skills.map((skill) => [skill.name, skill.scope]),
        [["project-skill", "personal"]],
      );
      assert.isFalse(snapshot.skills.some((skill) => skill.scope === "project"));

      const failedHome = path.join(directory, "failed-home");
      yield* fileSystem.makeDirectory(path.join(failedHome, ".factory"), { recursive: true });
      yield* fileSystem.writeFileString(
        path.join(failedHome, ".factory", "skills"),
        "not a directory",
      );
      const failedSkillsSnapshot = yield* checkDroidProviderStatus(
        { ...missingBinarySettings, binaryPath: binary },
        statusEnvironment,
        projectCwd,
        failedHome,
      );
      assert.include(failedSkillsSnapshot.message ?? "", "skill discovery failed");
      assert.include(failedSkillsSnapshot.message ?? "", "Refresh to retry");
      assert.equal(failedSkillsSnapshot.status, "warning");
      assert.isTrue(failedSkillsSnapshot.models.some((model) => model.slug === "test-model"));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("gives model discovery the user's custom models without credentials", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fileSystem.makeTempDirectoryScoped({
        directory: NodeOS.tmpdir(),
        prefix: "droid-status-custom-models-",
      });
      const home = path.join(directory, "home");
      yield* writeUserSettings(home, JSON.stringify({ customModels: userCustomModels }));
      const { binary, calls } = yield* writeFakeDroid(directory);
      const snapshot = yield* checkDroidProviderStatus(
        { ...missingBinarySettings, binaryPath: binary },
        { DROID_STATUS_CALLS: calls, HOME: home },
        directory,
        home,
      );
      const discovery = yield* readDiscoveryInvocations(calls);
      // Model listing runs first; skill discovery uses its own sandbox.
      assert.lengthOf(discovery, 2);
      assert.deepStrictEqual(JSON.parse(discovery[0].sandboxSettings), {
        customModels: [
          {
            model: "claude-haiku-5-5",
            id: "custom:Claude Haiku 5.5-0",
            displayName: "Claude Haiku 5.5",
            provider: "anthropic",
            baseUrl: "https://models.example.test/anthropic",
          },
          {
            model: "gpt-6-luna",
            id: "custom:GPT-6-Luna-1",
            provider: "generic-chat-completion-api",
            baseUrl: "https://models.example.test/v1",
          },
        ],
      });
      assert.isTrue(snapshot.models.some((model) => model.slug === "test-model"));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reads user custom models from FACTORY_HOME_OVERRIDE before HOME", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fileSystem.makeTempDirectoryScoped({
        directory: NodeOS.tmpdir(),
        prefix: "droid-status-custom-models-",
      });
      const home = path.join(directory, "home");
      const overrideHome = path.join(directory, "override-home");
      yield* writeUserSettings(
        home,
        JSON.stringify({
          customModels: [
            { model: "home-model", id: "custom:home", provider: "anthropic", baseUrl: "https://h" },
          ],
        }),
      );
      yield* writeUserSettings(
        overrideHome,
        JSON.stringify({
          customModels: [
            {
              model: "override-model",
              id: "custom:override",
              provider: "anthropic",
              baseUrl: "https://o",
            },
          ],
        }),
      );
      const { binary, calls } = yield* writeFakeDroid(directory);
      yield* checkDroidProviderStatus(
        { ...missingBinarySettings, binaryPath: binary },
        { DROID_STATUS_CALLS: calls, HOME: home, FACTORY_HOME_OVERRIDE: overrideHome },
        directory,
        home,
      );
      const discovery = yield* readDiscoveryInvocations(calls);
      assert.deepStrictEqual(JSON.parse(discovery[0].sandboxSettings), {
        customModels: [
          {
            model: "override-model",
            id: "custom:override",
            provider: "anthropic",
            baseUrl: "https://o",
          },
        ],
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "keeps model discovery working without sandbox settings when user settings are missing, malformed, or empty",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fileSystem.makeTempDirectoryScoped({
          directory: NodeOS.tmpdir(),
          prefix: "droid-status-custom-models-",
        });
        const cases: ReadonlyArray<readonly [string, string | undefined]> = [
          ["missing", undefined],
          ["malformed", "{ not json"],
          ["invalid-entry", JSON.stringify({ customModels: ["not-a-model"] })],
          ["empty", JSON.stringify({ customModels: [] })],
        ];
        for (const [name, contents] of cases) {
          const caseDirectory = path.join(directory, name);
          const home = path.join(caseDirectory, "home");
          yield* fileSystem.makeDirectory(home, { recursive: true });
          if (contents !== undefined) yield* writeUserSettings(home, contents);
          const { binary, calls } = yield* writeFakeDroid(caseDirectory);
          const snapshot = yield* checkDroidProviderStatus(
            { ...missingBinarySettings, binaryPath: binary },
            { DROID_STATUS_CALLS: calls, HOME: home },
            caseDirectory,
            home,
          );
          const discovery = yield* readDiscoveryInvocations(calls);
          assert.equal(snapshot.status, "ready", name);
          assert.isTrue(
            snapshot.models.some((model) => model.slug === "test-model"),
            name,
          );
          assert.lengthOf(discovery, 2, name);
          for (const invocation of discovery) {
            assert.isNull(invocation.sandboxSettings, name);
          }
        }
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("falls back to the OS home directory when HOME is unset", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fileSystem.makeTempDirectoryScoped({
        directory: NodeOS.tmpdir(),
        prefix: "droid-status-os-home-",
      });
      const home = path.join(directory, "home");
      yield* writeUserSettings(
        home,
        JSON.stringify({
          customModels: [
            { model: "home-model", id: "custom:home", provider: "anthropic", baseUrl: "https://h" },
          ],
        }),
      );
      const skillDirectory = path.join(home, ".factory", "skills", "home-skill");
      yield* fileSystem.makeDirectory(skillDirectory, { recursive: true });
      yield* fileSystem.writeFileString(
        path.join(skillDirectory, "SKILL.md"),
        ["---", "name: home-skill", "description: Personal skill", "---", ""].join("\n"),
      );
      const { binary, calls } = yield* writeFakeDroid(directory);
      const homedir = vi.spyOn(NodeOS, "homedir").mockReturnValue(home);
      const snapshot = yield* checkDroidProviderStatus(
        { ...missingBinarySettings, binaryPath: binary },
        { DROID_STATUS_CALLS: calls },
        directory,
      ).pipe(Effect.ensuring(Effect.sync(() => homedir.mockRestore())));
      const discovery = yield* readDiscoveryInvocations(calls);
      assert.deepStrictEqual(JSON.parse(discovery[0].sandboxSettings), {
        customModels: [
          { model: "home-model", id: "custom:home", provider: "anthropic", baseUrl: "https://h" },
        ],
      });
      assert.deepEqual(
        snapshot.skills.map((skill) => skill.name),
        ["home-skill"],
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it("adds non-interactive settings without mutating the given environment", () => {
    const environment = makeDroidEnvironment({
      FACTORY_API_KEY: "inherited-factory-key",
      DROID_API_KEY: "inherited-droid-key",
      PATH: "/usr/bin",
    });
    assert.equal(environment.FACTORY_API_KEY, "inherited-factory-key");
    assert.equal(environment.DROID_API_KEY, "inherited-droid-key");
    assert.equal(environment.PATH, "/usr/bin");
    assert.equal(environment.FACTORY_DISABLE_SETTINGS_PERSISTENCE, "1");
    assert.equal(environment.DROID_DISABLE_AUTO_UPDATE, "1");
  });

  it.effect("isolates instance credentials from the host environment", () =>
    Effect.gen(function* () {
      const environment = yield* makeDroidInstanceEnvironment(
        [{ name: "FACTORY_API_KEY", value: "instance-factory-key", sensitive: true }],
        {
          FACTORY_API_KEY: "host-factory-key",
          DROID_API_KEY: "host-droid-key",
          PATH: "/usr/bin",
        },
      );
      assert.equal(environment.FACTORY_API_KEY, "instance-factory-key");
      assert.isUndefined(environment.DROID_API_KEY);
      assert.equal(environment.PATH, "/usr/bin");
    }),
  );
});
