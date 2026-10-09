import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  DroidSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/process";
import * as AcpRegistrySupport from "./acp/AcpRegistrySupport.ts";
import { checkDroidProviderStatus, findDroidBinary } from "./DroidProvider.ts";
import { makeDroidDiscoverySpawner } from "./DroidProvider.testkit.ts";

// Failures: missing install accidentally spawns/installs; disabled instance
// probes; wrong executable or ACP args; truncated custom ids; invented effort
// options; empty discovery treated as sign-in; credentials/skill content leak.
const settings = Schema.decodeSync(DroidSettings)({});
const decodeSnapshot = Schema.decodeUnknownEffect(ServerProvider);
const catalogLayer = Layer.mock(AcpRegistrySupport.AcpRegistryCatalog)({
  resolve: (config, cwd, env) =>
    Effect.succeed({
      distribution: "local",
      spawn: {
        command: config.commandPath,
        args: [],
        cwd,
        env: env ?? {},
        shell: false,
      },
    }),
});

describe("Droid provider discovery", () => {
  it.effect("finds the existing pinned install without installing anything", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const home = yield* fs.makeTempDirectoryScoped();
      const binary = path.join(home, ".t3/tools/factory-droid/0.234.0/npm/bin/droid");
      yield* fs.makeDirectory(path.dirname(binary), { recursive: true });
      yield* fs.writeFileString(binary, "#!/bin/sh\n");
      assert.equal(yield* findDroidBinary(settings, { HOME: home }, "/unused"), binary);
      assert.equal(
        yield* findDroidBinary({ ...settings, binaryPath: "/missing" }, { HOME: home }, "/unused"),
        undefined,
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.effect("discovers exact BYOK models and commands without initializing a session", () =>
    Effect.gen(function* () {
      const { spawner, methods, launches } = yield* makeDroidDiscoverySpawner();
      const snapshot = yield* checkDroidProviderStatus(
        { ...settings, binaryPath: "/native-droid" },
        {},
        "/project",
        "/unused",
      ).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provide(catalogLayer),
        Effect.provideService(FileSystem.FileSystem, {
          ...(yield* FileSystem.FileSystem),
          exists: () => Effect.succeed(true),
        }),
      );
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.auth.status, "authenticated");
      assert.equal(snapshot.version, "0.234.0");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["custom:GPT-6 Luna-12", "custom:Claude Sonnet 5.5-15"],
      );
      assert.equal(snapshot.models[0]?.name, "GPT-6 Luna");
      const effort = snapshot.models[1]?.capabilities?.optionDescriptors?.find(
        (option) => option.id === "reasoning_effort",
      );
      assert.equal(effort?.type, "select");
      if (effort?.type === "select")
        assert.deepEqual(
          effort.options.map((choice) => choice.id),
          ["off", "high", "max"],
        );
      assert.equal(snapshot.skills[0]?.scope, "project");
      assert.isFalse("content" in (snapshot.skills[0] ?? {}));
      assert.equal(snapshot.slashCommands[0]?.input?.hint, "<files>");
      assert.isFalse(methods.includes("droid.initialize_session"));
      assert.isTrue(launches.every((command) => command.command === "/native-droid"));
      assert.isTrue(launches.every((command) => !command.args.includes("acp")));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.effect("never spawns or installs for disabled or missing binaries", () =>
    Effect.gen(function* () {
      const { spawner, launches } = yield* makeDroidDiscoverySpawner();
      for (const config of [
        { ...settings, enabled: false },
        { ...settings, binaryPath: "/missing" },
      ]) {
        const snapshot = yield* checkDroidProviderStatus(config, {}, "/tmp", "/missing-tools").pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provide(catalogLayer),
        );
        assert.isFalse(snapshot.enabled);
        assert.isFalse(snapshot.installed);
      }
      assert.isEmpty(launches);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.effect("does not claim authentication from an empty model inventory", () =>
    Effect.gen(function* () {
      const { spawner } = yield* makeDroidDiscoverySpawner({ models: [], authStatus: null });
      const snapshot = yield* checkDroidProviderStatus(
        { ...settings, binaryPath: "/native-droid" },
        {},
        "/tmp",
        "/unused",
      ).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(FileSystem.FileSystem, {
          ...(yield* FileSystem.FileSystem),
          exists: () => Effect.succeed(true),
        }),
        Effect.provide(catalogLayer),
      );
      assert.equal(snapshot.auth.status, "unknown");
      assert.equal(snapshot.status, "warning");
      assert.isEmpty(snapshot.models);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.effect("rejects incompatible discovery protocols without initializing a session", () =>
    Effect.gen(function* () {
      const { spawner, methods } = yield* makeDroidDiscoverySpawner({ protocol: "future" });
      const snapshot = yield* checkDroidProviderStatus(
        { ...settings, binaryPath: "/native-droid" },
        {},
        "/tmp",
        "/unused",
      ).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(FileSystem.FileSystem, {
          ...(yield* FileSystem.FileSystem),
          exists: () => Effect.succeed(true),
        }),
        Effect.provide(catalogLayer),
      );
      assert.equal(snapshot.status, "warning");
      assert.isEmpty(snapshot.models);
      assert.deepEqual(methods, ["droid.list_models", "droid.close_session"]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.effect.skipIf(!process.env.T3_TEST_REAL_DROID_BINARY)(
    "reads the real native catalog without hooks or session creation",
    () =>
      checkDroidProviderStatus(
        { ...settings, binaryPath: process.env.T3_TEST_REAL_DROID_BINARY ?? "" },
        process.env,
        "/tmp",
        "/unused",
      ).pipe(
        Effect.flatMap((snapshot) =>
          decodeSnapshot({
            ...snapshot,
            instanceId: ProviderInstanceId.make("droid"),
            driver: ProviderDriverKind.make("droid"),
          }),
        ),
        Effect.map((snapshot) => {
          assert.equal(snapshot.auth.status, "authenticated");
          assert.equal(snapshot.version, "0.234.0");
          assert.equal(snapshot.status, "ready");
          assert.isTrue(snapshot.models.some((model) => model.slug === "custom:GPT-6 Luna-12"));
          assert.isTrue(
            snapshot.models.some((model) => model.slug === "custom:Claude Sonnet 5.5-15"),
          );
          assert.isAtLeast(snapshot.skills.length, 87);
          assert.isFalse(snapshot.skills.some((skill) => "content" in skill));
        }),
        Effect.provide(Layer.merge(catalogLayer, NodeServices.layer)),
      ),
  );
});
