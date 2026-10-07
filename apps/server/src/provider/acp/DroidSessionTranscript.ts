// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
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
        Schema.Struct({
          type: Schema.String,
          text: Schema.optionalKey(Schema.String),
          id: Schema.optionalKey(Schema.String),
          name: Schema.optionalKey(Schema.String),
          input: Schema.optionalKey(Schema.Unknown),
          tool_use_id: Schema.optionalKey(Schema.String),
          content: Schema.optionalKey(Schema.Unknown),
          is_error: Schema.optionalKey(Schema.Boolean),
        }),
      ),
    }),
  ),
});
const decodeRow = Schema.decodeUnknownOption(Schema.fromJsonString(SessionRow));
const decodeSettings = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.Struct({ model: Schema.optional(Schema.String) })),
);

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

const decodeRows = (content: string) =>
  content.split("\n").flatMap((line) => {
    const row = decodeRow(line);
    return row._tag === "Some" ? [row.value] : [];
  });

const readSessionRows = Effect.fn("readDroidSessionRows")(
  function* (file: string) {
    return decodeRows(yield* Effect.tryPromise(() => NodeFSP.readFile(file, "utf8")));
  },
  Effect.catch(() => Effect.succeed([])),
);

/** Only the end of a file, so a check that needs the last rows does not read all of a long session. */
const readSessionTailRows = Effect.fn("readDroidSessionTailRows")(
  function* (file: string, tailBytes: number) {
    const tail = yield* Effect.tryPromise(async () => {
      const handle = await NodeFSP.open(file, "r");
      try {
        const { size } = await handle.stat();
        const start = Math.max(0, size - tailBytes);
        const buffer = Buffer.alloc(size - start);
        await handle.read(buffer, 0, buffer.length, start);
        const text = buffer.toString("utf8");
        // Reading from mid-file starts inside a line, which cannot decode.
        return start === 0 ? text : text.slice(text.indexOf("\n") + 1);
      } finally {
        await handle.close();
      }
    });
    return decodeRows(tail);
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
  const file = yield* findDroidSessionFile(input);
  return file === undefined ? [] : messageTexts(yield* readSessionRows(file));
});

export type DroidSubagentStep =
  | { readonly kind: "message"; readonly text: string }
  | {
      readonly kind: "tool";
      readonly id: string;
      readonly name: string;
      readonly input: unknown;
      /** `null` while the call has no result in the file yet. */
      readonly output: string | null;
      readonly isError: boolean;
    };

// Tool output travels to every client, and one diff or file read can be hundreds of KB.
const MAX_TOOL_OUTPUT_CHARS = 20_000;

/** A tool result is a string, or a list of parts of which only text is kept (images are base64). */
function toolResultText(content: unknown): string {
  const text =
    typeof content === "string"
      ? content
      : !Array.isArray(content)
        ? ""
        : content
            .flatMap((part) =>
              typeof part === "object" &&
              part !== null &&
              "text" in part &&
              typeof part.text === "string"
                ? [part.text]
                : [],
            )
            .join("\n");
  return text.length <= MAX_TOOL_OUTPUT_CHARS
    ? text
    : `${text.slice(0, MAX_TOOL_OUTPUT_CHARS)}\n… truncated ${text.length - MAX_TOOL_OUTPUT_CHARS} characters`;
}

/**
 * The worker's replies and tool calls in order. User text is skipped: it is the injected context and
 * the Task prompt, which the parent already holds. Thinking is skipped.
 */
function subagentSteps(
  rows: ReadonlyArray<typeof SessionRow.Type>,
): ReadonlyArray<DroidSubagentStep> {
  const parts = rows.flatMap((row) => (row.message === undefined ? [] : row.message.content));
  const results = new Map(
    parts.flatMap((part) =>
      part.type === "tool_result" && part.tool_use_id !== undefined
        ? [[part.tool_use_id, part] as const]
        : [],
    ),
  );
  return rows.flatMap((row) => {
    if (row.message?.role !== "assistant") return [];
    return row.message.content.flatMap((part): ReadonlyArray<DroidSubagentStep> => {
      if (part.type === "text" && part.text !== undefined && part.text.trim().length > 0) {
        return [{ kind: "message", text: part.text }];
      }
      if (part.type !== "tool_use" || part.id === undefined) return [];
      const result = results.get(part.id);
      return [
        {
          kind: "tool",
          id: part.id,
          name: part.name ?? "Tool",
          input: part.input ?? null,
          output: result === undefined ? null : toolResultText(result.content),
          isError: result?.is_error === true,
        },
      ];
    });
  });
}

// A parent session can be tens of MB, and the wake turn is always at its end.
const WAKE_TAIL_BYTES = 1_048_576;

const isInjectedReminder = (row: typeof SessionRow.Type) =>
  row.message?.role === "user" &&
  row.message.content.every((part) => part.text?.startsWith("<system-reminder>") === true);

/**
 * When a background worker ends, Droid appends a "Background task completed" message to the parent's
 * own session and runs a turn on it. Over ACP that turn is only visible if a prompt was still open;
 * after the prompt returned, nothing is sent. This reads that reply from the file:
 * - `undefined`: the message or its turn outcome is not in the file yet.
 * - `null`: nothing to mirror, either the message landed inside a turn ACP already carried, or the
 *   turn has no reply text. Tool calls in the turn are not included.
 * - otherwise the reply text of a turn that began right after an earlier turn ended.
 */
export const readDroidWakeReply = Effect.fn("readDroidWakeReply")(function* (input: {
  readonly file: string;
  readonly childSessionId: string;
}) {
  const rows = yield* readSessionTailRows(input.file, WAKE_TAIL_BYTES);
  const wakeIndex = rows.findLastIndex(
    (row) =>
      row.message?.role === "user" &&
      row.message.content.some(
        (part) =>
          part.text?.startsWith("Background task") === true &&
          part.text.includes(`task_id: ${input.childSessionId}`),
      ),
  );
  if (wakeIndex === -1) return undefined;
  const turn = rows.slice(wakeIndex + 1);
  const outcomeIndex = turn.findIndex((row) => row.type === "agent_turn_outcome");
  if (outcomeIndex === -1) return undefined;
  const before = rows.slice(0, wakeIndex).findLast((row) => !isInjectedReminder(row));
  if (before?.type !== "agent_turn_outcome") return null;
  const reply = messageTexts(turn.slice(0, outcomeIndex))
    .flatMap((message) => (message.role === "assistant" ? [message.text] : []))
    .join("\n\n");
  return reply.trim().length === 0 ? null : reply;
});

/**
 * The model a worker ran on, from the settings file beside its session file. Droid names a custom
 * model `custom:<display name>-<index>`, and only the display name is worth showing.
 */
const readSubagentModel = (file: string) =>
  Effect.tryPromise(() =>
    NodeFSP.readFile(file.replace(/\.jsonl$/u, ".settings.json"), "utf8"),
  ).pipe(
    Effect.map((content) => {
      const model = Option.getOrUndefined(decodeSettings(content))?.model?.trim();
      return model === undefined || model.length === 0
        ? undefined
        : model.replace(/^custom:/u, "").replace(/-\d+$/u, "");
    }),
    Effect.catch(() => Effect.succeed(undefined)),
  );

export interface DroidSubagentWork {
  readonly model: string | undefined;
  readonly steps: ReadonlyArray<DroidSubagentStep>;
}

/** `DroidSubagentWork` for a worker whose ending was not observed, which reads its file itself. */
export const readDroidSubagentWork = Effect.fn("readDroidSubagentWork")(function* (input: {
  readonly sessionId: string;
  readonly environment: NodeJS.ProcessEnv;
}) {
  const file = yield* findDroidSessionFile(input);
  if (file === undefined) return { model: undefined, steps: [] };
  const [rows, model] = yield* Effect.all([readSessionRows(file), readSubagentModel(file)], {
    concurrency: 2,
  });
  return { model, steps: subagentSteps(rows) };
});

/** Appended bytes up to the last complete line, so a row Droid is still writing is read next time. */
const readAppended = (file: string, offset: number) =>
  Effect.tryPromise(async () => {
    const handle = await NodeFSP.open(file, "r");
    try {
      const { size } = await handle.stat();
      if (size < offset) return { restarted: true, text: "", consumed: 0 };
      const buffer = Buffer.alloc(size - offset);
      await handle.read(buffer, 0, buffer.length, offset);
      const complete = buffer.lastIndexOf(0x0a) + 1;
      return {
        restarted: false,
        text: buffer.subarray(0, complete).toString("utf8"),
        consumed: complete,
      };
    } finally {
      await handle.close();
    }
  }).pipe(Effect.catch(() => Effect.succeed({ restarted: false, text: "", consumed: 0 })));

/**
 * Follows a worker's own session, which Droid never streams over ACP. Each call to the returned
 * effect reads only what was appended since the last one, so the file is never re-read from the
 * start however long the worker runs.
 *
 * `outcome` is set only once the file ends on a turn outcome (or session end) for a turn that began
 * at or after `startedAtMillis`, so a running worker, or a resumed one whose file still ends on an
 * earlier turn, reports none. `work` is what the worker has done so far, and is only recomputed
 * when rows were appended.
 */
export const followDroidSubagent = (input: {
  readonly file: string;
  readonly startedAtMillis: number;
}) => {
  const rows: Array<typeof SessionRow.Type> = [];
  let offset = 0;
  let model: string | undefined;
  let work: DroidSubagentWork = { model: undefined, steps: [] };
  return Effect.gen(function* () {
    const appended = yield* readAppended(input.file, offset);
    if (appended.restarted) {
      rows.length = 0;
      offset = 0;
    }
    const added = decodeRows(appended.text);
    offset += appended.consumed;
    rows.push(...added);
    if (model === undefined && rows.length > 0) model = yield* readSubagentModel(input.file);
    if (added.length > 0 || appended.restarted) work = { model, steps: subagentSteps(rows) };
    else if (work.model !== model) work = { model, steps: work.steps };
    const last = rows.at(-1);
    const lastUserAt = rows.findLast((row) => row.message?.role === "user")?.timestamp;
    const currentTurn = lastUserAt !== undefined && Date.parse(lastUserAt) >= input.startedAtMillis;
    const outcome =
      !currentTurn || (last?.type !== "session_end" && last?.type !== "agent_turn_outcome")
        ? undefined
        : last.type === "session_end" || last.reason === "completed"
          ? ("completed" as const)
          : last.reason === "cancelled"
            ? ("cancelled" as const)
            : ("failed" as const);
    return {
      outcome,
      work,
      assistantMessages:
        outcome === undefined
          ? []
          : messageTexts(rows).flatMap((message) =>
              message.role === "assistant" ? [message.text] : [],
            ),
    };
  });
};
