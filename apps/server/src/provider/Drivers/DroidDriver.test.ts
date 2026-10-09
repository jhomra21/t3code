import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ServerSettings,
  ModelSelection,
} from "@t3tools/contracts";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import { ProviderHost } from "@t3tools/provider-core/server/ProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";
import * as ServerConfig from "../../config.ts";
import * as AcpRegistrySupport from "../acp/AcpRegistrySupport.ts";
import { deriveProviderInstanceConfigMap } from "../ProviderInstanceRegistryHydration.ts";
import { makeDroidDiscoverySpawner } from "../DroidProvider.testkit.ts";
import { DroidDriver } from "./DroidDriver.ts";

const decodeServerSettings = Schema.decodeSync(ServerSettings);
const decodeSelection = Schema.decodeSync(ModelSelection);
const encodeSelection = Schema.encodeSync(ModelSelection);
const serverSettings = decodeServerSettings({});
const layer = Layer.mergeAll(
  NodeServices.layer,
  IdAllocator.layer,
  ServerConfig.layerTest("/tmp", { prefix: "t3-droid-driver-" }).pipe(
    Layer.provide(NodeServices.layer),
  ),
  Layer.succeed(ProviderHost, {
    paths: {
      cwd: "/tmp",
      baseDir: "/t3-sandbox",
      stateDir: "/tmp",
      providerStatusCacheDir: "/tmp",
    },
    settings: {
      get: Effect.succeed(serverSettings),
      changes: Stream.never,
      subscribe: Effect.succeed(Stream.never),
    },
    shouldRunBackgroundWork: () => Effect.succeed(false),
    resolveAttachmentPath: () => null,
  }),
  Layer.mock(AcpRegistrySupport.AcpRegistryCatalog)({
    resolve: (settings, cwd, env) =>
      Effect.succeed({
        distribution: "local",
        spawn: {
          command: settings.commandPath,
          args: [],
          cwd,
          env: env ?? {},
          shell: false,
        },
      }),
  }),
);
describe("native Droid driver", () => {
  it("adds its default instance without migrating ACP configuration or model selections", () => {
    const fallback = {
      driver: ProviderDriverKind.make("acpRegistry"),
      config: { agentId: "factory-droid" },
    };
    const settings = decodeServerSettings({ providerInstances: { fallback } });
    const configs = deriveProviderInstanceConfigMap(settings);
    assert.equal(configs[ProviderInstanceId.make("droid")]?.driver, "droid");
    assert.deepEqual(configs[ProviderInstanceId.make("fallback")], fallback);
    assert.equal(DroidDriver.metadata.displayName, "Factory Droid (native)");
    const selection = decodeSelection({
      instanceId: ProviderInstanceId.make("droid"),
      model: "custom:GPT-6 Luna-12",
      options: [
        { id: "reasoning_effort", value: "high" },
        { id: "autonomy_level", value: "low" },
      ],
    });
    assert.deepEqual(encodeSelection(selection), selection);
  });
  it.effect("is automatically enabled only when installed, respecting explicit disable", () =>
    Effect.gen(function* () {
      const originalFs = yield* FileSystem.FileSystem;
      for (const { installed, enabled } of [
        { installed: false, enabled: true },
        { installed: true, enabled: false },
        { installed: true, enabled: true },
      ]) {
        const { spawner, launches } = yield* makeDroidDiscoverySpawner();
        const instance = yield* DroidDriver.create({
          instanceId: ProviderInstanceId.make("droid"),
          displayName: undefined,
          environment: [],
          enabled,
          config: DroidDriver.defaultConfig(),
        }).pipe(
          Effect.provideService(FileSystem.FileSystem, {
            ...originalFs,
            exists: () => Effect.succeed(installed),
          }),
          Effect.provideService(HostProcessEnvironment, {
            HOME: "/test-home",
            FACTORY_API_KEY: "do-not-use",
          }),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        );
        assert.equal(instance.enabled, installed && enabled);
        assert.equal(instance.orchestrationAdapter.driver, "droid");
        const snapshot = yield* instance.snapshot.refresh;
        assert.equal(snapshot.instanceId, "droid");
        assert.equal(snapshot.enabled, installed && enabled);
        if (instance.enabled) {
          assert.equal(snapshot.auth.status, "authenticated");
          assert.equal(snapshot.models[0]?.slug, "custom:GPT-6 Luna-12");
          assert.isFalse(
            launches.some((command) => command.options.env?.FACTORY_API_KEY !== undefined),
          );
          if (!instance.snapshotForCwd) return yield* Effect.die("Missing workspace discovery");
          const workspace = yield* instance.snapshotForCwd("/project");
          assert.equal(workspace.skills[0]?.scope, "project");
        } else {
          assert.isEmpty(launches);
        }
        const text = yield* instance.textGeneration
          .generateThreadTitle({
            cwd: "/tmp",
            message: "A title",
            modelSelection: { instanceId: instance.instanceId, model: "auto" },
          })
          .pipe(Effect.result);
        assert.equal(text._tag, "Failure");
        assert.isFalse(snapshot.supportsTextGeneration);
      }
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );
});
