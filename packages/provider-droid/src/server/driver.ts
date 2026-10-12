import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as HttpClient from "effect/http/HttpClient";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
} from "@t3tools/provider-core/server/snapshotSettings";
import { withInstanceIdentity } from "@t3tools/provider-core/server/instanceIdentity";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";

import { DroidSettings } from "../settings.ts";
import { makeDroidTextGeneration } from "./textGeneration.ts";
import { DROID_PROVIDER, makeDroidAdapterV2 } from "./adapter.ts";
import {
  checkDroidProviderStatus,
  makeDroidInstanceEnvironment,
  makePendingDroidProvider,
} from "./status.ts";
import { droidMaintenance, enrichDroidSnapshot } from "./maintenance.ts";

const decodeDroidSettings = Schema.decodeSync(DroidSettings);

export type DroidDriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | IdAllocator.IdAllocatorV2
  | McpProviderSessions.McpProviderSessions
  | Path.Path
  | ProviderLatestVersions.ProviderLatestVersions
  | ProviderHost.ProviderHost;

export const DroidDriver: ProviderDriver<DroidSettings, DroidDriverEnv> = {
  driverKind: DROID_PROVIDER,
  metadata: { displayName: "Factory Droid", supportsMultipleInstances: true },
  configSchema: DroidSettings,
  defaultConfig: () => decodeDroidSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const host = yield* ProviderHost.ProviderHost;
      const continuationRequests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
      const latestVersions = yield* ProviderLatestVersions.ProviderLatestVersions;
      const httpClient = yield* HttpClient.HttpClient;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const hostEnvironment = yield* HostProcess.Environment;
      const processEnvironment = yield* makeDroidInstanceEnvironment(environment, hostEnvironment);
      const effectiveConfig = {
        ...config,
        enabled,
        binaryPath: expandHomePath(config.binaryPath, yield* HostProcess.HomeDirectory),
      };
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DROID_PROVIDER,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DROID_PROVIDER,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const snapshotSettings = yield* makeProviderSnapshotSettingsSource(effectiveConfig);
      const snapshot = yield* makeManagedServerProvider({
        resolveMaintenance: () => Effect.succeed(droidMaintenance),
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          makePendingDroidProvider(settings.provider).pipe(Effect.map(stampIdentity)),
        enrichSnapshot: ({ settings, snapshot: currentSnapshot, publishSnapshot }) =>
          enrichDroidSnapshot({
            snapshot: currentSnapshot,
            maintenanceCapabilities: droidMaintenance,
            enableProviderUpdateChecks: settings.enableProviderUpdateChecks,
          }).pipe(
            Effect.provideService(HttpClient.HttpClient, httpClient),
            Effect.provideService(ProviderLatestVersions.ProviderLatestVersions, latestVersions),
            Effect.map(stampIdentity),
            Effect.flatMap(publishSnapshot),
          ),
        checkProvider: checkDroidProviderStatus(
          effectiveConfig,
          processEnvironment,
          host.paths.cwd,
        ).pipe(
          Effect.map(stampIdentity),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, path),
        ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DROID_PROVIDER,
              instanceId,
              detail: "Failed to build Factory Droid snapshot.",
              cause,
            }),
        ),
      );
      const orchestrationAdapter = yield* makeDroidAdapterV2({
        instanceId,
        environment: processEnvironment,
        binaryPath: effectiveConfig.binaryPath.trim() || "droid",
        modelCatalog: snapshot.getSnapshot.pipe(Effect.map((current) => current.models)),
        continuationRequests,
      });
      return {
        instanceId,
        driverKind: DROID_PROVIDER,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        orchestrationAdapter,
        textGeneration: makeDroidTextGeneration(),
      } satisfies ProviderInstance;
    }),
};
