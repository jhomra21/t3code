import type { ServerProviderSkill } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
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

export const discoverDroidSkills = Effect.fn("discoverDroidSkills")(function* (
  settings: { readonly binaryPath: string },
  environment: NodeJS.ProcessEnv,
  cwd: string,
) {
  const rpc = yield* makeConnection({
    command: settings.binaryPath.trim() || "droid",
    env: environment,
    cwd,
  });
  yield* rpc.notifications.pipe(Stream.runDrain, Effect.ignore, Effect.forkScoped);
  return yield* readDroidSkills(rpc);
});
