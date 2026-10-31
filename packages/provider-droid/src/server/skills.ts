import type { ServerProviderSkill } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { fromYaml } from "@t3tools/shared/schemaYaml";
import { DroidRpcError, makeConnection, type DroidRpcConnection } from "./rpc.ts";
const isDroidRpcError = Schema.is(DroidRpcError);

const Skill = Schema.Struct({
  name: Schema.String,
  description: Schema.optionalKey(Schema.String),
  location: Schema.optionalKey(Schema.String),
  filePath: Schema.optionalKey(Schema.String),
  enabled: Schema.optionalKey(Schema.Boolean),
  userInvocable: Schema.optionalKey(Schema.Boolean),
});
const SkillCatalog = Schema.Struct({ skills: Schema.Array(Skill) });
const decodeSkillCatalog = Schema.decodeUnknownOption(SkillCatalog);
const MAX_SKILL_CATALOG_SIZE = 10_000;
const ProjectSkillFrontmatter = Schema.Struct({
  name: Schema.optional(Schema.String),
  description: Schema.optional(Schema.String),
  "user-invocable": Schema.optional(Schema.Boolean),
});
const ProjectSkillYaml = fromYaml(Schema.Unknown);
const decodeProjectSkillYaml = Schema.decodeUnknownOption(ProjectSkillYaml);
const decodeProjectSkillFrontmatter = Schema.decodeUnknownOption(ProjectSkillFrontmatter);
const InstalledPlugins = Schema.Struct({
  plugins: Schema.Record(
    Schema.String,
    Schema.Array(Schema.Struct({ installPath: Schema.String })),
  ),
});
const decodeInstalledPluginsJson = Schema.decodeOption(Schema.fromJsonString(InstalledPlugins));
const FRONTMATTER_PATTERN = /^---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)/;

export const mergeDroidSkills = (
  machineSkills: ReadonlyArray<ServerProviderSkill>,
  projectSkills: ReadonlyArray<ServerProviderSkill>,
): ReadonlyArray<ServerProviderSkill> => {
  const skillsByName = new Map(machineSkills.map((skill) => [skill.name, skill]));
  for (const skill of projectSkills) skillsByName.set(skill.name, skill);
  return [...skillsByName.values()].sort((left, right) => left.name.localeCompare(right.name));
};

export const mapDroidSkills = (value: unknown): ReadonlyArray<ServerProviderSkill> => {
  const catalog = decodeSkillCatalog(value);
  if (catalog._tag === "None") return [];
  const skillsByName = new Map<string, ServerProviderSkill>();
  for (const entry of catalog.value.skills.slice(0, MAX_SKILL_CATALOG_SIZE)) {
    const name = entry.name.trim();
    const path = entry.filePath?.trim() ?? "";
    if (!name || !path) continue;
    const scope = entry.location?.trim() ?? "";
    const skill = {
      name,
      path,
      enabled: entry.enabled !== false,
      ...(scope ? { scope } : {}),
      ...(entry.description?.trim() ? { description: entry.description.trim() } : {}),
      ...(entry.userInvocable === undefined ? {} : { userInvocable: entry.userInvocable }),
    } satisfies ServerProviderSkill;
    const existing = skillsByName.get(name);
    if (existing === undefined || (skill.scope === "project" && existing.scope !== "project"))
      skillsByName.set(name, skill);
  }
  return [...skillsByName.values()].sort((left, right) => left.name.localeCompare(right.name));
};

export const readDroidSkills = Effect.fn("readDroidSkills")(function* (rpc: DroidRpcConnection) {
  const response = yield* rpc.request("droid.list_skills", {}, "4 seconds").pipe(
    Effect.catchIf(
      (error) => isDroidRpcError(error) && error.code === -32601,
      () => Effect.succeed({ result: { skills: [] } }),
    ),
  );
  return mapDroidSkills(response.result);
});

const readSkillsFromRoot = Effect.fn("readDroidSkillsFromRoot")(function* (
  directory: string,
  scope: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const entries = yield* fileSystem.readDirectory(directory).pipe(
    Effect.catchTags({
      PlatformError: (error) =>
        error.reason._tag === "NotFound" ? Effect.succeed([]) : Effect.fail(error),
    }),
  );
  const skills: Array<ServerProviderSkill> = [];
  for (const entry of entries.slice(0, MAX_SKILL_CATALOG_SIZE)) {
    if (path.basename(entry) !== entry) continue;
    const skillDirectory = path.join(directory, entry);
    const directoryInfo = yield* fileSystem.stat(skillDirectory).pipe(
      Effect.catchTags({
        PlatformError: (error) =>
          error.reason._tag === "NotFound" ? Effect.sync(() => undefined) : Effect.fail(error),
      }),
    );
    if (directoryInfo?.type !== "Directory") continue;
    const skillPath = path.join(skillDirectory, "SKILL.md");
    const info = yield* fileSystem.stat(skillPath).pipe(
      Effect.catchTags({
        PlatformError: (error) =>
          error.reason._tag === "NotFound" ? Effect.sync(() => undefined) : Effect.fail(error),
      }),
    );
    if (info?.type !== "File" || info.size > 1_000_000n) continue;
    const contents = yield* fileSystem.readFileString(skillPath);
    const match = FRONTMATTER_PATTERN.exec(contents);
    if (!match) continue;
    const metadata = decodeProjectSkillYaml(match[1] ?? "");
    if (metadata._tag === "None") continue;
    const frontmatter = decodeProjectSkillFrontmatter(metadata.value);
    if (frontmatter._tag === "None") continue;
    const name = frontmatter.value.name ?? entry;
    if (!name || name !== name.trim()) continue;
    const description = frontmatter.value.description?.trim();
    skills.push({
      name,
      path: skillPath,
      scope,
      enabled: true,
      ...(description ? { description } : {}),
      ...(frontmatter.value["user-invocable"] === undefined
        ? {}
        : { userInvocable: frontmatter.value["user-invocable"] }),
    });
  }
  return skills;
});

export const discoverDroidProjectSkills = Effect.fn("discoverDroidProjectSkills")(function* (
  cwd: string,
) {
  const path = yield* Path.Path;
  const roots = yield* Effect.forEach(
    [".factory/skills", ".agents/skills"],
    (root) => readSkillsFromRoot(path.join(cwd, root), "project"),
    { concurrency: 2 },
  );
  const skillsByName = new Map<string, ServerProviderSkill>();
  for (const skill of roots.flat()) {
    if (!skillsByName.has(skill.name)) skillsByName.set(skill.name, skill);
  }
  return [...skillsByName.values()].sort((left, right) => left.name.localeCompare(right.name));
});

const discoverDroidUserSkills = Effect.fn("discoverDroidUserSkills")(function* (
  home: string,
  pluginsContents: string | undefined,
) {
  const path = yield* Path.Path;
  const factoryDirectory = path.join(home, ".factory");
  const personalSkills = yield* readSkillsFromRoot(
    path.join(factoryDirectory, "skills"),
    "personal",
  );
  if (pluginsContents === undefined) return personalSkills;
  const plugins = decodeInstalledPluginsJson(pluginsContents);
  if (plugins._tag === "None") return personalSkills;
  const pluginSkills = yield* Effect.forEach(
    Object.values(plugins.value.plugins).flatMap((installs) => installs),
    (install) => readSkillsFromRoot(path.join(install.installPath, "skills"), "personal"),
    { concurrency: 8 },
  );
  return mergeDroidSkills(pluginSkills.flat(), personalSkills);
});

export const discoverDroidSkills = Effect.fn("discoverDroidSkills")(function* (
  settings: { readonly binaryPath: string },
  environment: NodeJS.ProcessEnv,
  home: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-droid-skills-" });
  const isolatedCwd = path.join(root, "cwd");
  const isolatedHome = path.join(root, "home");
  const factoryHome = path.join(root, "factory-home");
  yield* fileSystem.makeDirectory(isolatedCwd);
  yield* fileSystem.makeDirectory(isolatedHome);
  yield* fileSystem.makeDirectory(factoryHome);
  const {
    FACTORY_HOME_OVERRIDE: _factoryHomeOverride,
    FACTORY_RUNTIME_SETTINGS_PATH: _runtimeSettingsPath,
    ...environmentWithoutFactoryOverride
  } = environment;
  const isolatedEnvironment = {
    ...environmentWithoutFactoryOverride,
    HOME: isolatedHome,
    FACTORY_HOME_OVERRIDE: factoryHome,
    FACTORY_DISABLE_SETTINGS_PERSISTENCE: "1",
    DROID_DISABLE_AUTO_UPDATE: "1",
  };
  const rpc = yield* makeConnection({
    command: settings.binaryPath.trim() || "droid",
    env: isolatedEnvironment,
    cwd: isolatedCwd,
    closeSession: false,
  });
  yield* rpc.notifications.pipe(Stream.runDrain, Effect.ignore, Effect.forkScoped);
  const builtinSkills = yield* readDroidSkills(rpc).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("Droid builtin skill discovery failed.", {
        cause: Cause.pretty(cause),
      }).pipe(Effect.as([])),
    ),
  );
  const pluginsFile = path.join(home, ".factory", "plugins", "installed_plugins.json");
  const pluginsContents = yield* fileSystem.readFileString(pluginsFile).pipe(
    Effect.catchTags({
      PlatformError: (error) =>
        error.reason._tag === "NotFound" ? Effect.sync(() => undefined) : Effect.fail(error),
    }),
  );
  const personalSkills = yield* discoverDroidUserSkills(home, pluginsContents);
  return mergeDroidSkills(builtinSkills, personalSkills);
});
