// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";

import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

const isSessionId = Schema.is(Schema.String.check(Schema.isUUID()));

const SessionRow = Schema.Struct({
  type: Schema.String,
  id: Schema.optionalKey(Schema.String),
  reason: Schema.optionalKey(Schema.String),
  timestamp: Schema.optionalKey(Schema.String),
  message: Schema.optionalKey(
    Schema.Struct({
      role: Schema.Literals(["user", "assistant"]),
      content: Schema.Array(
        Schema.Struct({
          type: Schema.String,
          text: Schema.optionalKey(Schema.String),
          thinking: Schema.optionalKey(Schema.String),
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

const decodeRows = Effect.fn("decodeDroidSessionRows")(function* (content: string) {
  const rows: Array<typeof SessionRow.Type> = [];
  let undecodableRows = 0;
  for (const line of content.split("\n")) {
    if (line.trim().length === 0) continue;
    const row = decodeRow(line);
    if (row._tag === "Some") rows.push(row.value);
    else undecodableRows++;
  }
  if (undecodableRows > 0)
    yield* Effect.logWarning("Droid transcript contains undecodable rows", { undecodableRows });
  return { rows, undecodableRows };
});

const readSessionRows = Effect.fn("readDroidSessionRows")(
  function* (file: string) {
    return (yield* decodeRows(yield* Effect.tryPromise(() => NodeFSP.readFile(file, "utf8")))).rows;
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
    return (yield* decodeRows(tail)).rows;
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
  | {
      readonly kind: "message";
      readonly id: string;
      readonly text: string;
      readonly reasoning?: boolean;
    }
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

/** Keep published identities across resumes without retaining every tool's output. */
export const droidSubagentStepSignature = (step: DroidSubagentStep) =>
  NodeCrypto.hash("sha256", JSON.stringify(step));

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
const displayDroidModel = (model: string | undefined) => {
  const trimmed = model?.trim();
  return trimmed === undefined || trimmed.length === 0
    ? undefined
    : trimmed.replace(/^custom:/u, "").replace(/-\d+$/u, "");
};

const readSubagentModel = (file: string) =>
  Effect.tryPromise(() =>
    NodeFSP.readFile(file.replace(/\.jsonl$/u, ".settings.json"), "utf8"),
  ).pipe(
    Effect.map((content) =>
      displayDroidModel(Option.getOrUndefined(decodeSettings(content))?.model),
    ),
    Effect.catch(() => Effect.succeed(undefined)),
  );

/**
 * The model a custom droid is pinned to, from `model:` in its definition's frontmatter. Known the
 * moment the worker launches, long before its own session file shows a model. Undefined for a
 * droid with no pinned model or no personal definition.
 */
export const readDroidDefinitionModel = (input: {
  readonly environment: NodeJS.ProcessEnv;
  readonly droidName: string;
}): string | undefined => {
  if (!/^[\w.-]+$/u.test(input.droidName)) return undefined;
  const home =
    input.environment.FACTORY_HOME_OVERRIDE ?? input.environment.HOME ?? NodeOS.homedir();
  try {
    const source = NodeFS.readFileSync(
      NodePath.join(home, ".factory", "droids", `${input.droidName}.md`),
      "utf8",
    );
    const frontmatter = /^---\r?\n([\s\S]*?)\r?\n---/u.exec(source)?.[1] ?? "";
    const line = /^model:\s*(.+)$/mu.exec(frontmatter)?.[1];
    return displayDroidModel(line?.trim().replace(/^(["'])(.*)\1$/u, "$2"));
  } catch {
    return undefined;
  }
};

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
  return (yield* followDroidSubagent({ file, startedAtMillis: 0 })).work;
});

/** Appended bytes up to the last complete line, so a row Droid is still writing is read next time. */
const readAppended = (file: string, offset: number) =>
  Effect.tryPromise(async () => {
    const handle = await NodeFSP.open(file, "r");
    try {
      const { size, ino } = await handle.stat();
      if (size < offset) return { restarted: true, text: "", consumed: 0, inode: ino };
      const buffer = Buffer.alloc(size - offset);
      const { bytesRead } = await handle.read(buffer, 0, buffer.length, offset);
      const complete = buffer.subarray(0, bytesRead).lastIndexOf(0x0a) + 1;
      return {
        restarted: false,
        text: buffer.subarray(0, complete).toString("utf8"),
        consumed: complete,
        inode: ino,
      };
    } finally {
      await handle.close();
    }
  }).pipe(
    Effect.catch(() =>
      Effect.succeed({ restarted: false, text: "", consumed: 0, inode: undefined }),
    ),
  );

/**
 * Follows a worker's own session, which Droid never streams over ACP. Each call to the returned
 * effect reads only what was appended since the last one, so the file is never re-read from the
 * start however long the worker runs.
 *
 * `outcome` is set only once the file ends on a turn outcome (or session end) for a turn that began
 * at or after `startedAtMillis`, so a running worker, or a resumed one whose file still ends on an
 * earlier turn, reports none. `work` contains only new or changed steps. Completed rows are not
 * retained: only open tool calls are kept to join their eventual results.
 */
export const followDroidSubagent = (input: {
  readonly file: string;
  readonly startedAtMillis: number;
}) => {
  const tools = new Map<string, Extract<DroidSubagentStep, { readonly kind: "tool" }>>();
  let offset = 0;
  let inode: number | undefined;
  let model: string | undefined;
  let lastUserAt: string | undefined;
  let outcome: "completed" | "cancelled" | "failed" | undefined;
  let lastAssistant: string | undefined;
  let undecodableRows = 0;
  return Effect.gen(function* () {
    let appended = yield* readAppended(input.file, offset);
    if (
      appended.restarted ||
      (inode !== undefined && appended.inode !== undefined && inode !== appended.inode)
    ) {
      tools.clear();
      offset = 0;
      outcome = undefined;
      lastUserAt = undefined;
      lastAssistant = undefined;
      appended = yield* readAppended(input.file, 0);
    }
    inode = appended.inode ?? inode;
    const steps = new Map<string, DroidSubagentStep>();
    const completeOffset = offset + appended.consumed;
    for (const line of appended.text.split("\n").slice(0, -1)) {
      const rowOffset = offset;
      offset += Buffer.byteLength(line) + 1;
      if (line.trim().length === 0) continue;
      const decoded = yield* decodeRows(line);
      undecodableRows += decoded.undecodableRows;
      outcome = undefined;
      const row = decoded.rows[0];
      if (row === undefined) continue;
      if (row.message?.role === "user" && row.timestamp !== undefined) lastUserAt = row.timestamp;
      if (row.type === "session_end" || row.type === "agent_turn_outcome")
        outcome =
          row.type === "session_end" || row.reason === "completed"
            ? "completed"
            : row.reason === "cancelled"
              ? "cancelled"
              : "failed";
      for (const [index, part] of (row.message?.content ?? []).entries()) {
        const id = `${row.id ?? `byte:${rowOffset}`}:${index}`;
        if (row.message?.role === "assistant") {
          if (
            (part.type === "text" && part.text?.trim()) ||
            (part.type === "thinking" && part.thinking?.trim())
          ) {
            const text = part.type === "thinking" ? part.thinking : part.text;
            if (text === undefined) continue;
            steps.set(id, {
              kind: "message",
              id,
              text,
              ...(part.type === "thinking" ? { reasoning: true } : {}),
            });
            if (part.type === "text") lastAssistant = text;
          } else if (part.type === "tool_use" && part.id !== undefined) {
            const step: Extract<DroidSubagentStep, { readonly kind: "tool" }> = {
              kind: "tool",
              id: part.id,
              name: part.name ?? "Tool",
              input: part.input ?? null,
              output: null,
              isError: false,
            };
            tools.set(part.id, step);
            steps.set(part.id, step);
          }
        }
        if (part.type === "tool_result" && part.tool_use_id !== undefined) {
          const tool = tools.get(part.tool_use_id);
          if (tool === undefined) continue;
          steps.set(tool.id, {
            ...tool,
            output: toolResultText(part.content),
            isError: part.is_error === true,
          });
          tools.delete(tool.id);
        }
      }
    }
    // Empty complete lines consume bytes too; the incomplete final line never does.
    offset = completeOffset;
    if (model === undefined && appended.consumed > 0) model = yield* readSubagentModel(input.file);
    const currentTurn = lastUserAt !== undefined && Date.parse(lastUserAt) >= input.startedAtMillis;
    return {
      outcome: currentTurn ? outcome : undefined,
      work: { model, steps: Array.from(steps.values()) },
      assistantMessages:
        currentTurn && outcome !== undefined && lastAssistant !== undefined ? [lastAssistant] : [],
      undecodableRows,
    };
  });
};

/** One scoped directory watcher; child files need not exist when listeners are registered. */
export const watchDroidSessionDirectory = Effect.fn("watchDroidSessionDirectory")(function* (
  directory: string,
) {
  const listeners = new Map<string, Set<() => void>>();
  const errors = yield* Queue.sliding<Error>(1);
  yield* Stream.fromQueue(errors).pipe(
    Stream.runForEach((cause) => Effect.logWarning("Droid session watcher failed", { cause })),
    Effect.forkScoped,
  );
  yield* Effect.acquireRelease(
    Effect.try(() =>
      NodeFS.watch(directory, (_event, name) => {
        if (name === null) return;
        for (const listener of listeners.get(String(name)) ?? []) listener();
      }).on("error", (error) => {
        Queue.offerUnsafe(errors, error);
        // Wake registered readers once; their final read and parent lifecycle still settle the child.
        for (const callbacks of listeners.values()) for (const listener of callbacks) listener();
      }),
    ),
    (watcher) =>
      Effect.sync(() => watcher.close()).pipe(
        Effect.andThen(Effect.logDebug("Droid session watchers", { watchersOpen: 0 })),
      ),
  ).pipe(Effect.tap(() => Effect.logDebug("Droid session watchers", { watchersOpen: 1 })));
  return {
    listen: (name: string, listener: () => void) => {
      const current = listeners.get(name) ?? new Set();
      current.add(listener);
      listeners.set(name, current);
      return () => {
        current.delete(listener);
        if (current.size === 0) listeners.delete(name);
      };
    },
  };
});
