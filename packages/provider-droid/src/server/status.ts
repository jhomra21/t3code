import {
  type ProviderInstanceEnvironment,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as NodeOS from "node:os";
import * as ChildProcess from "effect/process/ChildProcess";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import {
  buildServerProvider,
  COMPACT_SLASH_COMMAND,
  providerModelsFromSettings,
  isCommandMissingCause,
  spawnAndCollect,
} from "@t3tools/provider-core/server/snapshotProbe";
import type { DroidSettings } from "../settings.ts";
import { makeConnection } from "./rpc.ts";
import { discoverDroidSkills } from "./skills.ts";
import { parseDroidVersion } from "./maintenance.ts";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";

const PRESENTATION = {
  displayName: "Factory Droid",
  showInteractionModeToggle: false,
  supportedRuntimeModes: ["approval-required", "auto-accept-edits", "auto", "full-access"],
  reportsContextWindow: false,
  requiresNewThreadForModelChange: false,
} as const;

const DEFAULT_CAPABILITIES = createModelCapabilities({ optionDescriptors: [] });
const Models = Schema.Struct({
  models: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      displayName: Schema.String,
      supportedReasoningEfforts: Schema.Array(Schema.String),
      defaultReasoningEffort: Schema.String,
      disabled: Schema.optionalKey(Schema.Boolean),
    }),
  ),
});
const Doctor = Schema.fromJsonString(
  Schema.Struct({
    results: Schema.Array(Schema.Struct({ id: Schema.String, status: Schema.String })),
  }),
);
const UserSettings = Schema.fromJsonString(
  Schema.Struct({ customModels: Schema.Array(Schema.Record(Schema.String, Schema.Unknown)) }),
);
const decodeModels = Schema.decodeUnknownEffect(Models);
const decodeDoctor = Schema.decodeOption(Doctor);
const decodeUserSettings = Schema.decodeOption(UserSettings);
const encodeUserSettings = Schema.encodeSync(UserSettings);

const modelsFromCatalog = (catalog: typeof Models.Type): ReadonlyArray<ServerProviderModel> =>
  catalog.models
    .filter((model) => !model.disabled)
    .map((model) => ({
      slug: model.id,
      name: model.displayName,
      isCustom: false,
      ...(model.id === "auto" ? { isDefault: true } : {}),
      capabilities: createModelCapabilities({
        optionDescriptors:
          model.supportedReasoningEfforts.length === 0
            ? []
            : [
                {
                  type: "select",
                  id: "reasoning_effort",
                  label: "Reasoning",
                  currentValue: model.defaultReasoningEffort,
                  options: model.supportedReasoningEfforts.map((id) => ({
                    id,
                    label: id,
                    isDefault: id === model.defaultReasoningEffort,
                  })),
                },
              ],
      }),
    }));

export const makeDroidEnvironment = (environment: NodeJS.ProcessEnv): NodeJS.ProcessEnv => ({
  ...environment,
  FACTORY_DISABLE_SETTINGS_PERSISTENCE: "1",
  DROID_DISABLE_AUTO_UPDATE: "1",
});

export const makeDroidInstanceEnvironment = Effect.fn(function* (
  instanceEnvironment: ProviderInstanceEnvironment,
  hostEnvironment: NodeJS.ProcessEnv,
) {
  const {
    FACTORY_API_KEY: _factoryApiKey,
    DROID_API_KEY: _droidApiKey,
    ...safeHostEnvironment
  } = hostEnvironment;
  return makeDroidEnvironment(
    yield* mergeProviderInstanceEnvironment(instanceEnvironment, safeHostEnvironment),
  );
});

// Windows rarely sets HOME; os.homedir() honors HOME where it is set and uses USERPROFILE otherwise.
const droidUserHome = (environment: NodeJS.ProcessEnv) => environment.HOME || NodeOS.homedir();

const readUserCustomModels = Effect.fn("readUserCustomModels")(function* (
  environment: NodeJS.ProcessEnv,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const userFactoryHome = environment.FACTORY_HOME_OVERRIDE || droidUserHome(environment);
  const contents = yield* fileSystem
    .readFileString(path.join(userFactoryHome, ".factory", "settings.json"))
    .pipe(Effect.option);
  const settings = Option.flatMap(contents, decodeUserSettings);
  if (Option.isNone(settings)) return [];
  return settings.value.customModels.map(
    ({
      apiKey: _apiKey,
      apiKeyHelper: _apiKeyHelper,
      apiKeyHelperTtlMs: _apiKeyHelperTtlMs,
      extraHeaders: _extraHeaders,
      ...model
    }) => model,
  );
});

const discoverDroidModels = Effect.fn("discoverDroidModels")(function* (
  binary: string,
  environment: NodeJS.ProcessEnv,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-droid-status-" });
  const isolatedCwd = path.join(root, "cwd");
  const isolatedHome = path.join(root, "home");
  const factoryHome = path.join(root, "factory-home");
  yield* fileSystem.makeDirectory(isolatedCwd);
  yield* fileSystem.makeDirectory(isolatedHome);
  yield* fileSystem.makeDirectory(factoryHome);
  // The isolated home hides the user's customModels from model listing. Copy only
  // their definitions; credential fields stay on the host.
  const customModels = yield* readUserCustomModels(environment);
  if (customModels.length > 0) {
    yield* fileSystem.makeDirectory(path.join(factoryHome, ".factory"));
    yield* fileSystem.writeFileString(
      path.join(factoryHome, ".factory", "settings.json"),
      encodeUserSettings({ customModels }),
    );
  }
  const {
    FACTORY_RUNTIME_SETTINGS_PATH: _runtimeSettingsPath,
    FACTORY_HOME_OVERRIDE: _factoryHomeOverride,
    ...isolatedEnvironment
  } = environment;
  // Keep model discovery off the project path and user Factory home.
  const rpc = yield* makeConnection({
    command: binary,
    env: {
      ...isolatedEnvironment,
      HOME: isolatedHome,
      FACTORY_HOME_OVERRIDE: factoryHome,
    },
    cwd: isolatedCwd,
    closeSession: false,
  });
  yield* rpc.notifications.pipe(Stream.runDrain, Effect.ignore, Effect.forkScoped);
  const response = yield* rpc.request("droid.list_models", {});
  const catalog = yield* decodeModels(response.result);
  return modelsFromCatalog(catalog);
});

export const makePendingDroidProvider = Effect.fn("makePendingDroidProvider")(function* (
  settings: DroidSettings,
) {
  return {
    ...buildServerProvider({
      presentation: PRESENTATION,
      enabled: settings.enabled,
      checkedAt: DateTime.formatIso(yield* DateTime.now),
      models: providerModelsFromSettings([], settings.customModels, DEFAULT_CAPABILITIES),
      slashCommands: settings.enabled ? [COMPACT_SLASH_COMMAND] : [],
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: settings.enabled ? "Checking Factory Droid..." : "Factory Droid is disabled.",
      },
    }),
    supportsTextGeneration: false,
  };
});

export const checkDroidProviderStatus = Effect.fn("checkDroidProviderStatus")(function* (
  settings: DroidSettings,
  environment: NodeJS.ProcessEnv,
  cwd: string,
  home = droidUserHome(environment),
) {
  if (!settings.enabled) return yield* makePendingDroidProvider(settings);
  const binary = settings.binaryPath.trim() || "droid";
  const env = makeDroidEnvironment(environment);
  const versionResult = yield* Effect.gen(function* () {
    const spawnCommand = yield* resolveSpawnCommand(binary, ["--version"], { env });
    return yield* spawnAndCollect(
      binary,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        cwd,
        env,
        extendEnv: false,
        shell: spawnCommand.shell,
      }),
    );
  }).pipe(Effect.timeoutOption("6 seconds"), Effect.result);
  if (versionResult._tag === "Failure" && isCommandMissingCause(versionResult.failure)) {
    return {
      ...buildServerProvider({
        presentation: PRESENTATION,
        enabled: true,
        checkedAt: DateTime.formatIso(yield* DateTime.now),
        models: providerModelsFromSettings([], settings.customModels, DEFAULT_CAPABILITIES),
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message:
            "Factory Droid is not installed: the droid CLI was not found on this server host.",
        },
      }),
      supportsTextGeneration: false,
    };
  }
  const version =
    versionResult._tag === "Success" && Option.isSome(versionResult.success)
      ? parseDroidVersion(versionResult.success.value.stdout)
      : null;
  const authOutput = yield* Effect.gen(function* () {
    const spawnCommand = yield* resolveSpawnCommand(
      binary,
      ["doctor", "--auth", "--json", "--timeout", "3000"],
      { env },
    );
    return yield* spawnAndCollect(
      binary,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        cwd,
        env,
        extendEnv: false,
        shell: spawnCommand.shell,
      }),
    );
  }).pipe(
    Effect.timeoutOption("6 seconds"),
    Effect.orElseSucceed(() => Option.none()),
  );
  const doctor = Option.isSome(authOutput) ? decodeDoctor(authOutput.value.stdout) : Option.none();
  const authResult = Option.isSome(doctor)
    ? doctor.value.results.find((result) => result.id === "auth.verify")
    : undefined;
  const auth: ServerProvider["auth"]["status"] =
    authResult?.status === "pass"
      ? "authenticated"
      : authResult?.status === "fail"
        ? "unauthenticated"
        : "unknown";
  const models = yield* discoverDroidModels(binary, env).pipe(
    Effect.scoped,
    Effect.timeoutOption("10 seconds"),
    Effect.orElseSucceed(() => Option.none()),
  );
  const skills = yield* discoverDroidSkills({ binaryPath: binary }, env, home).pipe(
    Effect.scoped,
    Effect.timeoutOption("10 seconds"),
    Effect.orElseSucceed(() => Option.none()),
  );
  if (Option.isNone(skills)) {
    yield* Effect.logWarning("Droid skill discovery failed; refresh to retry.");
  }
  const modelList = providerModelsFromSettings(
    Option.isSome(models) ? models.value : [],
    settings.customModels,
    DEFAULT_CAPABILITIES,
  );
  const skillList = Option.isSome(skills) ? skills.value : [];
  const message =
    auth === "unauthenticated"
      ? "Sign in with Factory Droid on this environment."
      : Option.isNone(models)
        ? "Factory Droid model discovery failed. Refresh to retry."
        : Option.isNone(skills)
          ? "Factory Droid skill discovery failed. Refresh to retry."
          : auth === "unknown"
            ? "Factory Droid authentication could not be verified."
            : undefined;
  return {
    ...buildServerProvider({
      presentation: PRESENTATION,
      enabled: true,
      checkedAt: DateTime.formatIso(yield* DateTime.now),
      models: modelList,
      slashCommands: [COMPACT_SLASH_COMMAND],
      skills: skillList,
      probe: {
        installed: version !== null,
        version,
        auth: { status: auth },
        status:
          auth === "authenticated" && Option.isSome(models) && Option.isSome(skills)
            ? "ready"
            : "warning",
        ...(message === undefined ? {} : { message }),
      },
    }),
    supportsTextGeneration: false,
  };
});
