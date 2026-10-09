import {
  AcpRegistrySettings,
  type DroidSettings,
  officialAcpRegistryIconUrlForAgentId,
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
import { ChildProcess } from "effect/process";
import {
  buildServerProvider,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
} from "@t3tools/provider-core/server/snapshotProbe";
import * as AcpRegistrySupport from "./acp/AcpRegistrySupport.ts";
import * as DroidRpc from "../orchestration-v2/Adapters/DroidRpc.ts";

const PRESENTATION = {
  displayName: "Factory Droid (native)",
  supportsConversationRollback: false,
  showInteractionModeToggle: false,
  supportedRuntimeModes: ["approval-required", "auto-accept-edits", "auto", "full-access"],
  reportsContextWindow: false,
  requiresNewThreadForModelChange: false,
} as const;
const AUTONOMY = {
  type: "select",
  id: "autonomy_level",
  label: "Autonomy",
  // No default selection: absence leaves the adapter's runtime-policy mapping
  // in charge. Choosing an option explicitly overrides that mapping.
  options: [
    { id: "off", label: "Supervised" },
    { id: "low", label: "Low" },
    { id: "medium", label: "Medium" },
    { id: "high", label: "High" },
  ],
} as const;
const DEFAULT_CAPABILITIES = createModelCapabilities({ optionDescriptors: [AUTONOMY] });
const LOCAL_BINARY_SETTINGS = Schema.decodeSync(AcpRegistrySettings)({ source: "local" });
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
const Commands = Schema.Struct({
  commands: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      description: Schema.optionalKey(Schema.String),
      argumentHint: Schema.optionalKey(Schema.String),
    }),
  ),
});
const Skills = Schema.Struct({
  skills: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      description: Schema.optionalKey(Schema.String),
      location: Schema.String,
      filePath: Schema.String,
      enabled: Schema.optionalKey(Schema.Boolean),
      userInvocable: Schema.optionalKey(Schema.Boolean),
    }),
  ),
});
const Doctor = Schema.fromJsonString(
  Schema.Struct({
    results: Schema.Array(Schema.Struct({ id: Schema.String, status: Schema.String })),
  }),
);
const decodeModels = Schema.decodeUnknownEffect(Models);
const decodeCommands = Schema.decodeUnknownEffect(Commands);
const decodeSkills = Schema.decodeUnknownEffect(Skills);
const decodeDoctor = Schema.decodeUnknownOption(Doctor);

/** Use only an already installed, pinned binary; never install or update on a status read. */
export const findDroidBinary = Effect.fn("findDroidBinary")(function* (
  settings: DroidSettings,
  environment: NodeJS.ProcessEnv,
  toolsDir: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home = environment.HOME ?? environment.USERPROFILE;
  const override =
    settings.binaryPath.startsWith("~/") && home !== undefined
      ? path.join(home, settings.binaryPath.slice(2))
      : settings.binaryPath;
  const candidates = override
    ? [override]
    : [
        path.join(toolsDir, "factory-droid", "0.234.0", "npm", "bin", "droid"),
        ...(home === undefined
          ? []
          : [path.join(home, ".t3", "tools", "factory-droid", "0.234.0", "npm", "bin", "droid")]),
      ];
  for (const candidate of new Set(candidates)) {
    if (yield* fs.exists(candidate).pipe(Effect.orElseSucceed(() => false))) return candidate;
  }
  return undefined;
});

export const resolveDroidBinary = Effect.fn("resolveDroidBinary")(function* (
  settings: DroidSettings,
  environment: NodeJS.ProcessEnv,
  cwd: string,
  toolsDir: string,
) {
  const binary = yield* findDroidBinary(settings, environment, toolsDir);
  if (binary === undefined)
    return yield* new AcpRegistrySupport.AcpRegistryError({
      reason: "runner_unavailable",
      detail: "The pinned Factory Droid 0.234.0 executable is not installed.",
    });
  const catalog = yield* AcpRegistrySupport.AcpRegistryCatalog;
  return yield* catalog.resolve(
    { ...LOCAL_BINARY_SETTINGS, commandPath: binary },
    cwd,
    environment,
  );
});

export function makeDroidEnvironment(environment: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const {
    FACTORY_API_KEY: _apiKey,
    DROID_API_KEY: _droidApiKey,
    ...accountEnvironment
  } = environment;
  return {
    ...accountEnvironment,
    FACTORY_DISABLE_SETTINGS_PERSISTENCE: "1",
    DROID_DISABLE_AUTO_UPDATE: "1",
  };
}

const modelsFromCatalog = (catalog: typeof Models.Type): ReadonlyArray<ServerProviderModel> =>
  catalog.models
    .filter((model) => !model.disabled)
    .map((model) => ({
      slug: model.id,
      name: model.displayName,
      // CLI custom models are discovered, not T3-authored editable overrides.
      isCustom: false,
      ...(model.id === "auto" ? { isDefault: true } : {}),
      capabilities: createModelCapabilities({
        optionDescriptors: [
          AUTONOMY,
          ...(model.supportedReasoningEfforts.length === 0
            ? []
            : [
                {
                  type: "select" as const,
                  id: "reasoning_effort",
                  label: "Reasoning",
                  currentValue: model.defaultReasoningEffort,
                  options: model.supportedReasoningEfforts.map((id) => ({
                    id,
                    label: id,
                    isDefault: id === model.defaultReasoningEffort,
                  })),
                },
              ]),
        ],
      }),
    }));

/** These native reads work before initialize_session: no hooks, MCP, or persisted session. */
export const discoverDroidCatalog = Effect.fn("discoverDroidCatalog")(
  function* (binary: string, environment: NodeJS.ProcessEnv, cwd: string) {
    const rpc = yield* DroidRpc.makeConnection({
      command: binary,
      env: makeDroidEnvironment(environment),
      cwd,
    });
    yield* rpc.notifications.pipe(Stream.runDrain, Effect.ignore, Effect.forkScoped);
    const response = yield* rpc.request("droid.list_models", {});
    if (response.factoryProtocolVersion !== "1.246.0")
      return yield* new DroidRpc.DroidRpcError({ operation: "protocol-version" });
    const models = modelsFromCatalog(yield* decodeModels(response.result));
    const commands = yield* rpc
      .request("droid.list_commands", {})
      .pipe(Effect.flatMap((response) => decodeCommands(response.result)));
    const skills = yield* rpc
      .request("droid.list_skills", {})
      .pipe(Effect.flatMap((response) => decodeSkills(response.result)));
    return {
      models,
      slashCommands: commands.commands.map((command) => ({
        name: command.name,
        ...(command.description?.trim() ? { description: command.description } : {}),
        ...(command.argumentHint?.trim() ? { input: { hint: command.argumentHint } } : {}),
      })),
      skills: skills.skills.map((skill) => ({
        name: skill.name,
        path: skill.filePath,
        scope: skill.location,
        enabled: skill.enabled ?? true,
        ...(skill.description?.trim() ? { description: skill.description } : {}),
        ...(skill.userInvocable === undefined ? {} : { userInvocable: skill.userInvocable }),
      })),
    };
  },
  Effect.scoped,
  Effect.timeout("20 seconds"),
);

export const makePendingDroidProvider = Effect.fn("makePendingDroidProvider")(function* (
  settings: DroidSettings,
) {
  return {
    ...buildServerProvider({
      presentation: PRESENTATION,
      enabled: settings.enabled,
      checkedAt: DateTime.formatIso(yield* DateTime.now),
      models: providerModelsFromSettings([], settings.customModels, DEFAULT_CAPABILITIES),
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: settings.enabled
          ? "Checking Factory Droid..."
          : "Factory Droid is disabled or not installed.",
      },
    }),
    supportsTextGeneration: false,
  };
});

export const checkDroidProviderStatus = Effect.fn("checkDroidProviderStatus")(function* (
  settings: DroidSettings,
  environment: NodeJS.ProcessEnv,
  cwd: string,
  toolsDir: string,
) {
  if (!settings.enabled) return yield* makePendingDroidProvider(settings);
  const resolved = yield* resolveDroidBinary(settings, environment, cwd, toolsDir).pipe(
    Effect.option,
  );
  if (Option.isNone(resolved))
    return yield* makePendingDroidProvider({ ...settings, enabled: false });
  const binary = resolved.value.spawn.command;
  const env = makeDroidEnvironment(resolved.value.spawn.env ?? environment);
  const run = (args: ReadonlyArray<string>) =>
    spawnAndCollect(
      binary,
      ChildProcess.make(binary, args, {
        env,
        extendEnv: false,
        cwd,
        shell: false,
      }),
    ).pipe(
      Effect.timeoutOption("6 seconds"),
      Effect.orElseSucceed(() => Option.none()),
    );
  const versionOutput = yield* run(["--version"]);
  const version = Option.isSome(versionOutput)
    ? parseGenericCliVersion(versionOutput.value.stdout)
    : null;
  const authOutput = yield* run(["doctor", "--auth", "--json", "--timeout", "3000"]);
  const doctor = Option.flatMap(authOutput, (output) => decodeDoctor(output.stdout));
  const verified = Option.isSome(doctor)
    ? doctor.value.results.find((row) => row.id === "auth.verify")
    : undefined;
  let authStatus: ServerProvider["auth"]["status"] = "unknown";
  if (verified?.status === "pass") authStatus = "authenticated";
  else if (verified?.status === "fail") authStatus = "unauthenticated";
  const auth = { status: authStatus };
  const discovery = yield* discoverDroidCatalog(binary, env, cwd).pipe(Effect.option);
  let message: string | undefined;
  if (auth.status === "unauthenticated") message = "Sign in with Droid on this environment.";
  else if (Option.isNone(discovery))
    message = "Factory Droid catalog discovery failed. Refresh to retry.";
  else if (auth.status === "unknown")
    message = "Factory Droid authentication could not be verified.";
  const result = buildServerProvider({
    presentation: PRESENTATION,
    enabled: true,
    checkedAt: DateTime.formatIso(yield* DateTime.now),
    models: providerModelsFromSettings(
      Option.isSome(discovery) ? discovery.value.models : [],
      settings.customModels,
      DEFAULT_CAPABILITIES,
    ),
    slashCommands: Option.isSome(discovery) ? discovery.value.slashCommands : [],
    skills: Option.isSome(discovery) ? discovery.value.skills : [],
    probe: {
      installed: true,
      version,
      auth,
      status: auth.status !== "authenticated" || Option.isNone(discovery) ? "warning" : "ready",
      ...(message === undefined ? {} : { message }),
    },
  });
  const iconUrl = officialAcpRegistryIconUrlForAgentId("factory-droid");
  return { ...result, supportsTextGeneration: false, ...(iconUrl ? { iconUrl } : {}) };
});
