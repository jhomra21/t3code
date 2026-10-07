// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const isSessionId = Schema.is(Schema.String.check(Schema.isUUID()));

const TranscriptRow = Schema.Struct({
  type: Schema.String,
  message: Schema.optionalKey(
    Schema.Struct({
      role: Schema.Literals(["user", "assistant"]),
      content: Schema.Array(
        Schema.Struct({ type: Schema.String, text: Schema.optionalKey(Schema.String) }),
      ),
    }),
  ),
});
const decodeRow = Schema.decodeUnknownOption(Schema.fromJsonString(TranscriptRow));

/**
 * Droid answers `session/load` with only the context it still holds after compaction, while its own
 * session file keeps every message. Tool calls and thinking are not text parts and are skipped.
 */
export const readDroidTranscript = Effect.fn("readDroidTranscript")(
  function* (input: { readonly sessionId: string; readonly environment: NodeJS.ProcessEnv }) {
    if (!isSessionId(input.sessionId)) return [];
    const home =
      input.environment.FACTORY_HOME_OVERRIDE ?? input.environment.HOME ?? NodeOS.homedir();
    const sessionsRoot = NodePath.join(home, ".factory", "sessions");
    const file = yield* Effect.tryPromise(async () => {
      for (const directory of await NodeFS.readdir(sessionsRoot)) {
        const candidate = NodePath.join(sessionsRoot, directory, `${input.sessionId}.jsonl`);
        if (
          await NodeFS.stat(candidate).then(
            (stat) => stat.isFile(),
            () => false,
          )
        ) {
          return NodeFS.readFile(candidate, "utf8");
        }
      }
      return "";
    });
    return file.split("\n").flatMap((line) => {
      const row = decodeRow(line);
      if (row._tag === "None" || row.value.type !== "message" || row.value.message === undefined) {
        return [];
      }
      const { role, content } = row.value.message;
      const text = content
        .flatMap((part) => (part.type === "text" && part.text !== undefined ? [part.text] : []))
        .join("\n\n");
      return text.trim().length === 0 ? [] : [{ role, text }];
    });
  },
  Effect.catch(() => Effect.succeed([])),
);
