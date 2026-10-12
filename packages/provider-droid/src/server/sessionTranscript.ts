// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

const isSessionId = Schema.is(Schema.String.check(Schema.isUUID()));
const partSchema = Schema.Struct({
  type: Schema.String,
  text: Schema.optionalKey(Schema.String),
  thinking: Schema.optionalKey(Schema.String),
  id: Schema.optionalKey(Schema.String),
  name: Schema.optionalKey(Schema.String),
  input: Schema.optionalKey(Schema.Unknown),
  tool_use_id: Schema.optionalKey(Schema.String),
  content: Schema.optionalKey(Schema.Unknown),
  is_error: Schema.optionalKey(Schema.Boolean),
});
const rowSchema = Schema.Struct({
  type: Schema.String,
  id: Schema.optionalKey(Schema.String),
  reason: Schema.optionalKey(Schema.String),
  timestamp: Schema.optionalKey(Schema.String),
  message: Schema.optionalKey(
    Schema.Struct({
      role: Schema.Literals(["user", "assistant"]),
      content: Schema.Array(partSchema),
    }),
  ),
});
const decodeRow = Schema.decodeUnknownOption(Schema.fromJsonString(rowSchema));
const decodeSettings = Schema.decodeOption(
  Schema.fromJsonString(Schema.Struct({ model: Schema.optional(Schema.String) })),
);

export type DroidSubagentStep =
  | {
      readonly kind: "message";
      readonly id: string;
      readonly text: string;
      readonly reasoning?: true;
    }
  | {
      readonly kind: "tool";
      readonly id: string;
      readonly name: string;
      readonly input: unknown;
      readonly output: string | null;
      readonly isError: boolean;
    };

export type DroidSubagentWork = {
  readonly model?: string;
  readonly steps: ReadonlyArray<DroidSubagentStep>;
  readonly outcome?: "completed" | "cancelled" | "failed";
  readonly result?: string;
};

export const droidSubagentStepSignature = (step: DroidSubagentStep) =>
  NodeCrypto.hash("sha256", JSON.stringify(step));

export const readDroidSessionAppend = (file: string, offset: number, inode?: number) =>
  Effect.tryPromise(async () => {
    const handle = await NodeFSP.open(file, "r");
    try {
      const stat = await handle.stat();
      const restarted = stat.size < offset || (inode !== undefined && inode !== stat.ino);
      const start = restarted ? 0 : offset;
      const buffer = Buffer.alloc(stat.size - start);
      const { bytesRead } = await handle.read(buffer, 0, buffer.length, start);
      const complete = buffer.subarray(0, bytesRead).lastIndexOf(0x0a) + 1;
      return {
        text: buffer.subarray(0, complete).toString("utf8"),
        consumed: complete,
        restarted,
        inode: stat.ino,
      };
    } finally {
      await handle.close();
    }
  }).pipe(
    Effect.orElseSucceed(() => ({
      text: "",
      consumed: 0,
      restarted: false,
      inode,
    })),
  );

const displayModel = (value: string | undefined) => {
  const model = value?.trim();
  return model === undefined || model.length === 0
    ? undefined
    : model.replace(/^custom:/u, "").replace(/-\d+$/u, "");
};

const readSessionModel = (file: string) =>
  Effect.tryPromise(() =>
    NodeFSP.readFile(file.replace(/\.jsonl$/u, ".settings.json"), "utf8"),
  ).pipe(
    Effect.map((text) => {
      const decoded = decodeSettings(text);
      return decoded._tag === "Some" ? displayModel(decoded.value.model) : undefined;
    }),
    Effect.orElseSucceed(() => undefined),
  );

export const readDroidDefinitionModel = (
  environment: NodeJS.ProcessEnv,
  droidName: string | undefined,
) => {
  if (
    droidName === undefined ||
    droidName === "." ||
    droidName === ".." ||
    !/^[\w.-]+$/u.test(droidName)
  )
    return undefined;
  const home = environment.FACTORY_HOME_OVERRIDE ?? environment.HOME ?? NodeOS.homedir();
  try {
    const source = NodeFS.readFileSync(
      NodePath.join(home, ".factory", "droids", `${droidName}.md`),
      "utf8",
    );
    const frontmatter = /^---\r?\n([\s\S]*?)\r?\n---/u.exec(source)?.[1] ?? "";
    return displayModel(
      /^model:\s*(.+)$/mu
        .exec(frontmatter)?.[1]
        ?.trim()
        .replace(/^(["'])(.*)\1$/u, "$2"),
    );
  } catch {
    return undefined;
  }
};

export const findDroidSessionDirectory = (input: {
  readonly sessionId: string;
  readonly environment: NodeJS.ProcessEnv;
}) =>
  Effect.tryPromise(async () => {
    if (!isSessionId(input.sessionId)) return undefined;
    const home =
      input.environment.FACTORY_HOME_OVERRIDE ?? input.environment.HOME ?? NodeOS.homedir();
    const root = NodePath.join(home, ".factory", "sessions");
    const realRoot = await NodeFSP.realpath(root);
    for (const project of await NodeFSP.readdir(root)) {
      const directory = NodePath.join(root, project);
      const candidate = NodePath.join(directory, `${input.sessionId}.jsonl`);
      if (
        !(await NodeFSP.stat(candidate).then(
          (stat) => stat.isFile(),
          () => false,
        ))
      )
        continue;
      const realDirectory = await NodeFSP.realpath(directory);
      if (realDirectory === realRoot || realDirectory.startsWith(`${realRoot}${NodePath.sep}`))
        return realDirectory;
    }
    return undefined;
  }).pipe(Effect.orElseSucceed(() => undefined));

const maxToolOutputChars = 20_000;
const toolOutput = (value: unknown) => {
  const text =
    typeof value === "string"
      ? value
      : Array.isArray(value)
        ? value
            .flatMap((part) =>
              typeof part === "object" &&
              part !== null &&
              "text" in part &&
              typeof part.text === "string"
                ? [part.text]
                : [],
            )
            .join("\n")
        : "";
  return text.length <= maxToolOutputChars
    ? text
    : `${text.slice(0, maxToolOutputChars)}\n… truncated ${text.length - maxToolOutputChars} characters`;
};

export const followDroidSubagent = (input: {
  readonly directory: string;
  readonly sessionId: string;
  readonly startedAtMillis: number;
  readonly onWork: (work: DroidSubagentWork) => Effect.Effect<void>;
}) =>
  Effect.gen(function* () {
    if (!isSessionId(input.sessionId)) return;
    const expectedFile = NodePath.join(input.directory, `${input.sessionId}.jsonl`);

    const dirty = yield* Queue.sliding<void>(1);
    yield* Effect.acquireRelease(
      Effect.try(() =>
        NodeFS.watch(input.directory, (_event, name) => {
          if (
            name !== null &&
            [
              NodePath.basename(expectedFile),
              NodePath.basename(expectedFile.replace(/\.jsonl$/u, ".settings.json")),
            ].includes(String(name))
          )
            Queue.offerUnsafe(dirty, undefined);
        }),
      ),
      (resource) => Effect.sync(() => resource.close()),
    ).pipe(Effect.asVoid);
    const tools = new Map<string, Extract<DroidSubagentStep, { readonly kind: "tool" }>>();
    let offset = 0;
    let inode: number | undefined;
    let model: string | undefined;
    let lastUserAt: string | undefined;
    let outcome: DroidSubagentWork["outcome"];
    let lastAssistant: string | undefined;
    yield* Queue.offer(dirty, undefined);
    yield* Stream.fromQueue(dirty).pipe(
      Stream.mapEffect(() =>
        Effect.gen(function* () {
          const appended = yield* readDroidSessionAppend(expectedFile, offset, inode);
          if (appended.restarted) {
            offset = 0;
            tools.clear();
            lastUserAt = undefined;
            lastAssistant = undefined;
            outcome = undefined;
          }
          inode = appended.inode;
          if (model === undefined) model = yield* readSessionModel(expectedFile);
          const steps = new Map<string, DroidSubagentStep>();
          let rowOffset = offset;
          for (const line of appended.text.split("\n").slice(0, -1)) {
            const currentOffset = rowOffset;
            rowOffset += Buffer.byteLength(line) + 1;
            outcome = undefined;
            if (line.trim().length === 0) continue;
            const decoded = decodeRow(line);
            if (decoded._tag === "None") continue;
            const row = decoded.value;
            if (row.message?.role === "user" && row.timestamp !== undefined)
              lastUserAt = row.timestamp;
            if (row.type === "session_end" || row.type === "agent_turn_outcome")
              outcome =
                row.type === "session_end" || row.reason === "completed"
                  ? "completed"
                  : row.reason === "cancelled"
                    ? "cancelled"
                    : "failed";
            for (const [index, part] of (row.message?.content ?? []).entries()) {
              const id = `${row.id ?? `offset:${currentOffset}`}:${index}`;
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
                    ...(part.type === "thinking" ? { reasoning: true as const } : {}),
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
                  output: toolOutput(part.content),
                  isError: part.is_error === true,
                });
                tools.delete(tool.id);
              }
            }
          }
          offset += appended.consumed;
          const currentTurn =
            lastUserAt !== undefined && Date.parse(lastUserAt) >= input.startedAtMillis;
          const currentOutcome = currentTurn ? outcome : undefined;
          const work: DroidSubagentWork = {
            ...(model === undefined ? {} : { model }),
            steps: Array.from(steps.values()),
            ...(currentOutcome === undefined ? {} : { outcome: currentOutcome }),
            ...(currentOutcome === undefined || lastAssistant === undefined
              ? {}
              : { result: lastAssistant }),
          };
          yield* input.onWork(work);
          return currentOutcome !== undefined;
        }),
      ),
      Stream.takeUntil((done) => done),
      Stream.runDrain,
    );
  });
