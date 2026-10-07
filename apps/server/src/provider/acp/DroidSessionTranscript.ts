// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const isSessionId = Schema.is(Schema.String.check(Schema.isUUID()));

const SessionRow = Schema.Struct({
  type: Schema.String,
  reason: Schema.optionalKey(Schema.String),
  timestamp: Schema.optionalKey(Schema.String),
  message: Schema.optionalKey(
    Schema.Struct({
      role: Schema.Literals(["user", "assistant"]),
      content: Schema.Array(
        Schema.Struct({ type: Schema.String, text: Schema.optionalKey(Schema.String) }),
      ),
    }),
  ),
});
const decodeRow = Schema.decodeUnknownOption(Schema.fromJsonString(SessionRow));

/** Droid keeps each session under a directory named for its project, so the id alone is searched. */
export const findDroidSessionFile = Effect.fn("findDroidSessionFile")(
  function* (input: { readonly sessionId: string; readonly environment: NodeJS.ProcessEnv }) {
    if (!isSessionId(input.sessionId)) return undefined;
    const home =
      input.environment.FACTORY_HOME_OVERRIDE ?? input.environment.HOME ?? NodeOS.homedir();
    const sessionsRoot = NodePath.join(home, ".factory", "sessions");
    return yield* Effect.tryPromise(async () => {
      for (const directory of await NodeFSP.readdir(sessionsRoot)) {
        const candidate = NodePath.join(sessionsRoot, directory, `${input.sessionId}.jsonl`);
        if (
          await NodeFSP.stat(candidate).then(
            (stat) => stat.isFile(),
            () => false,
          )
        ) {
          return candidate;
        }
      }
      return undefined;
    });
  },
  Effect.catch(() => Effect.succeed(undefined)),
);

const readSessionRows = Effect.fn("readDroidSessionRows")(
  function* (input: { readonly sessionId: string; readonly environment: NodeJS.ProcessEnv }) {
    const file = yield* findDroidSessionFile(input);
    if (file === undefined) return [];
    const content = yield* Effect.tryPromise(() => NodeFSP.readFile(file, "utf8"));
    return content.split("\n").flatMap((line) => {
      const row = decodeRow(line);
      return row._tag === "Some" ? [row.value] : [];
    });
  },
  Effect.catch(() => Effect.succeed([])),
);

function messageTexts(rows: ReadonlyArray<typeof SessionRow.Type>) {
  return rows.flatMap((row) => {
    if (row.type !== "message" || row.message === undefined) return [];
    const { role, content } = row.message;
    const text = content
      .flatMap((part) => (part.type === "text" && part.text !== undefined ? [part.text] : []))
      .join("\n\n");
    return text.trim().length === 0 ? [] : [{ role, text }];
  });
}

/**
 * Droid answers `session/load` with only the context it still holds after compaction, while its own
 * session file keeps every message. Tool calls and thinking are not text parts and are skipped.
 */
export const readDroidTranscript = Effect.fn("readDroidTranscript")(function* (input: {
  readonly sessionId: string;
  readonly environment: NodeJS.ProcessEnv;
}) {
  return messageTexts(yield* readSessionRows(input));
});

/**
 * A worker's own session, which Droid never streams over ACP. `outcome` is set only once the file
 * ends on a turn outcome (or session end) for a turn that began at or after `startedAtMillis`, so a
 * running worker, or a resumed one whose file still ends on an earlier turn, reports none.
 */
export const readDroidSubagentSession = Effect.fn("readDroidSubagentSession")(function* (input: {
  readonly sessionId: string;
  readonly environment: NodeJS.ProcessEnv;
  readonly startedAtMillis: number;
}) {
  const rows = yield* readSessionRows(input);
  const last = rows.at(-1);
  const lastUserAt = rows.findLast((row) => row.message?.role === "user")?.timestamp;
  const currentTurn = lastUserAt !== undefined && Date.parse(lastUserAt) >= input.startedAtMillis;
  const outcome = !currentTurn
    ? undefined
    : last?.type === "session_end"
      ? ("completed" as const)
      : last?.type === "agent_turn_outcome"
        ? last.reason === "completed"
          ? ("completed" as const)
          : last.reason === "cancelled"
            ? ("cancelled" as const)
            : ("failed" as const)
        : undefined;
  return {
    outcome,
    assistantMessages: messageTexts(rows).flatMap((message) =>
      message.role === "assistant" ? [message.text] : [],
    ),
  };
});
