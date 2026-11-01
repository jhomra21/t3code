import { ProviderDriverKind, type ServerProvider } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  makePackageManagedProviderMaintenanceResolver,
  normalizeCommandPath,
  type ProviderMaintenanceCapabilities,
} from "@t3tools/provider-core/server/maintenanceResolver";
import { parseGenericCliVersion } from "@t3tools/provider-core/server/snapshotProbe";

const DRIVER = ProviderDriverKind.make("droid");
const PACKAGE_URL = "https://registry.npmjs.org/droid/latest";
const LatestPackage = Schema.fromJsonString(Schema.Struct({ version: Schema.String }));

export const parseDroidVersion = parseGenericCliVersion;

const isPackageManagedPath = (commandPath: string) => {
  const normalized = normalizeCommandPath(commandPath);
  return (
    normalized.includes("/node_modules/") ||
    normalized.includes("/.bun/bin/") ||
    /\/yarn\/(?:data\/)?global\/node_modules\//.test(normalized) ||
    /\/(?:\.local\/share\/pnpm|library\/pnpm|local\/share\/pnpm|pnpm\/global)\//.test(normalized) ||
    /\/(?:\.vite-plus|\.local\/share\/vite-plus|appdata\/local\/vite-plus)\/bin\//.test(
      normalized,
    ) ||
    normalized.includes("/.volta/tools/image/packages/")
  );
};

const packageManagedResolver = makePackageManagedProviderMaintenanceResolver({
  provider: DRIVER,
  npmPackageName: "droid",
  nativeUpdate: {
    args: ["update"],
    isCommandPath: (commandPath) =>
      /(?:\/\.local\/bin\/droid(?:\.exe)?$|^[a-z]:\/users\/[^/]+\/bin\/droid\.exe$)/.test(
        normalizeCommandPath(commandPath),
      ) && !isPackageManagedPath(commandPath),
  },
});

export const droidMaintenanceResolver = {
  resolve: Effect.fn("droidMaintenanceResolver.resolve")(function* (
    context: Parameters<typeof packageManagedResolver.resolve>[0],
  ) {
    const capabilities = yield* packageManagedResolver.resolve(
      context !== null && isPackageManagedPath(context.realCommandPath)
        ? { ...context, resolvedCommandPath: context.realCommandPath }
        : context,
    );
    if (context?.platform !== "win32" || capabilities.update?.lockKey !== "droid-native") {
      return capabilities;
    }
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const backupPath = path.join(path.dirname(context.resolvedCommandPath), "droid.exe.old");
    return {
      ...capabilities,
      onUpdated: fileSystem
        .remove(backupPath)
        .pipe(Effect.catch(() => Effect.logDebug("Could not remove the previous Droid binary"))),
    };
  }),
};

const latestDroidVersion = Effect.fn("latestDroidVersion")(function* (options?: {
  readonly fresh?: boolean;
}) {
  const http = yield* HttpClient.HttpClient;
  const latestVersions = yield* ProviderLatestVersions.ProviderLatestVersions;
  return yield* latestVersions.cached(
    `droid:${PACKAGE_URL}`,
    http.execute(HttpClientRequest.get(PACKAGE_URL)).pipe(
      Effect.flatMap((response) =>
        response.status >= 200 && response.status < 300
          ? response.text.pipe(
              Effect.flatMap(Schema.decodeUnknownEffect(LatestPackage)),
              Effect.map((manifest) => parseDroidVersion(manifest.version)),
            )
          : Effect.succeed(null),
      ),
      Effect.timeout(4_000),
      Effect.orElseSucceed(() => null),
    ),
    options,
  );
});

export const enrichDroidSnapshot = Effect.fn("enrichDroidSnapshot")(function* (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks: boolean;
}) {
  const latestVersion =
    input.enableProviderUpdateChecks && input.snapshot.enabled && input.snapshot.installed
      ? yield* latestDroidVersion()
      : null;
  return yield* enrichProviderSnapshotWithVersionAdvisory(
    input.snapshot,
    { ...input.maintenanceCapabilities, latestVersion },
    { enableProviderUpdateChecks: input.enableProviderUpdateChecks },
  );
});
