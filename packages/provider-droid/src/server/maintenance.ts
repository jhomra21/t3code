import { ProviderDriverKind, type ServerProvider } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
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

export const droidMaintenanceResolver = makePackageManagedProviderMaintenanceResolver({
  provider: DRIVER,
  npmPackageName: "droid",
  nativeUpdate: {
    args: ["update"],
    isCommandPath: (commandPath) =>
      /\/\.local\/bin\/droid(?:\.exe)?$/.test(normalizeCommandPath(commandPath)),
  },
});

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
