import { DroidSettings } from "@t3tools/contracts";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/process";
import { ProviderHost } from "@t3tools/provider-core/server/ProviderHost";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/continuationRequests";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { makeManualOnlyProviderMaintenanceCapabilities } from "@t3tools/provider-core/server/maintenanceResolver";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "@t3tools/provider-core/server/snapshotSettings";
import * as AcpRegistrySupport from "../acp/AcpRegistrySupport.ts";
import * as ServerConfig from "../../config.ts";
import {
  DROID_PROVIDER,
  makeDroidAdapterV2,
} from "../../orchestration-v2/Adapters/DroidAdapterV2.ts";
import {
  checkDroidProviderStatus,
  discoverDroidCatalog,
  findDroidBinary,
  makeDroidEnvironment,
  makePendingDroidProvider,
  resolveDroidBinary,
} from "../DroidProvider.ts";
import { makeDroidTextGeneration } from "../../textGeneration/DroidTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";

const decodeSettings = Schema.decodeSync(DroidSettings);

export type DroidDriverEnv =
  | ProviderHost
  | ServerConfig.ServerConfig
  | IdAllocator.IdAllocatorV2
  | FileSystem.FileSystem
  | Path.Path
  | ChildProcessSpawner.ChildProcessSpawner
  | AcpRegistrySupport.AcpRegistryCatalog;

export const DroidDriver: ProviderDriver<DroidSettings, DroidDriverEnv> = {
  driverKind: DROID_PROVIDER,
  metadata: { displayName: "Factory Droid (native)", supportsMultipleInstances: true },
  configSchema: DroidSettings,
  defaultConfig: () => decodeSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const host = yield* ProviderHost;
      const serverConfig = yield* ServerConfig.ServerConfig;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const catalog = yield* AcpRegistrySupport.AcpRegistryCatalog;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const continuations = yield* ProviderContinuationRequests.ProviderContinuationRequests;
      const hostEnvironment = yield* HostProcessEnvironment;
      const processEnvironment = makeDroidEnvironment(
        mergeProviderInstanceEnvironment(environment, hostEnvironment),
      );
      const toolsDir = path.join(host.paths.baseDir, "tools");
      const installedBinary = yield* findDroidBinary(config, processEnvironment, toolsDir);
      const effectiveConfig = { ...config, enabled: enabled && installedBinary !== undefined };
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DROID_PROVIDER,
        instanceId,
      });
      const stamp = withInstanceIdentity({
        instanceId,
        driverKind: DROID_PROVIDER,
        displayName: displayName ?? "Factory Droid (native)",
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, host.settings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<DroidSettings>>({
        resolveMaintenance: () =>
          Effect.succeed(
            makeManualOnlyProviderMaintenanceCapabilities({
              provider: DROID_PROVIDER,
              packageName: null,
            }),
          ),
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          makePendingDroidProvider(settings.provider).pipe(Effect.map(stamp)),
        checkProvider: checkDroidProviderStatus(
          effectiveConfig,
          processEnvironment,
          host.paths.cwd,
          toolsDir,
        ).pipe(
          Effect.map(stamp),
          Effect.provideService(AcpRegistrySupport.AcpRegistryCatalog, catalog),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.provideService(Path.Path, path),
        ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DROID_PROVIDER,
              instanceId,
              detail: "Failed to build native Droid snapshot.",
              cause,
            }),
        ),
      );
      const orchestrationAdapter = makeDroidAdapterV2({
        instanceId,
        environment: processEnvironment,
        attachmentsDir: serverConfig.attachmentsDir,
        fileSystem: fs,
        path,
        idAllocator: ids,
        childProcessSpawner: spawner,
        continuationRequests: continuations,
        resolver: {
          resolve: (_settings, cwd, env) =>
            resolveDroidBinary(effectiveConfig, env ?? processEnvironment, cwd, toolsDir).pipe(
              Effect.provideService(AcpRegistrySupport.AcpRegistryCatalog, catalog),
              Effect.provideService(FileSystem.FileSystem, fs),
              Effect.provideService(Path.Path, path),
            ),
        },
      });
      return {
        instanceId,
        driverKind: DROID_PROVIDER,
        continuationIdentity,
        displayName,
        accentColor,
        enabled: effectiveConfig.enabled,
        snapshot,
        orchestrationAdapter,
        textGeneration: makeDroidTextGeneration(),
        snapshotForCwd: (cwd) =>
          !effectiveConfig.enabled || installedBinary === undefined
            ? snapshot.getSnapshot
            : Effect.all([
                snapshot.getSnapshot,
                discoverDroidCatalog(installedBinary, processEnvironment, cwd).pipe(
                  Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
                  Effect.mapError(
                    (cause) =>
                      new ProviderDriverError({
                        driver: DROID_PROVIDER,
                        instanceId,
                        detail: "Failed to discover Droid workspace commands.",
                        cause,
                      }),
                  ),
                ),
              ]).pipe(
                Effect.map(([snapshot, catalog]) => ({
                  ...snapshot,
                  slashCommands: catalog.slashCommands,
                  skills: catalog.skills,
                })),
              ),
      } satisfies ProviderInstance;
    }),
};
