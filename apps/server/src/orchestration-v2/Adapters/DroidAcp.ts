import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
// @effect-diagnostics nodeBuiltinImport:off -- Adapter-local transcript paths use Node filesystem semantics.
import * as NodePath from "node:path";
import * as Stream from "effect/Stream";

import type { AcpToolCallState } from "../../provider/acp/AcpRuntimeModel.ts";
import {
  findDroidSessionFile,
  followDroidSubagent,
  watchDroidSessionDirectory,
  readDroidDefinitionModel,
  readDroidSubagentWork,
  readDroidWakeReply,
} from "../../provider/acp/DroidSessionTranscript.ts";
import type { AcpAdapterV2Flavor, AcpAdapterV2SubagentUpdate } from "./AcpAdapterV2.ts";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

// Task's `complexity` is Droid's name for the worker's reasoning effort.
const EFFORT_BY_COMPLEXITY = new Map([
  ["light", "low"],
  ["medium", "medium"],
  ["heavy", "high"],
]);

const BACKGROUND_LAUNCH = /^Task launched in background\./;
const SESSION_ID = /^session_id: (\S+)/m;
// A blocking Task returns the worker's reply behind this fixed two-line header.
const REPORT_HEADER = /^session_id: \S+\nUse this report as the source of record\.[^\n]*\n?/;

/**
 * Factory Droid reports subagent launches over ACP as an ordinary `Task` tool
 * call (`rawInput.subagent_type`, `description`, `prompt`). The worker's own
 * session id appears only in the tool output, never as a child session.
 *
 * A background launch (`await: false`) completes the tool call immediately with
 * a launch note, so that is the worker starting, not finishing. Droid sends no
 * ACP update when it later ends.
 */
export const extractDroidSubagentUpdate = (input: {
  readonly environment: NodeJS.ProcessEnv;
}): NonNullable<AcpAdapterV2Flavor["extractSubagentUpdate"]> => {
  // Updates repeat per tool call, and a definition read per update is wasted work.
  const modelByDroid = new Map<string, string | undefined>();
  const modelOf = (droidName: string) => {
    if (!modelByDroid.has(droidName)) {
      modelByDroid.set(
        droidName,
        readDroidDefinitionModel({ environment: input.environment, droidName }),
      );
    }
    return modelByDroid.get(droidName) ?? null;
  };
  return (tool) => extractDroidSubagentUpdateFrom(tool, modelOf);
};

function extractDroidSubagentUpdateFrom(
  tool: AcpToolCallState,
  modelOf: (droidName: string) => string | null,
): AcpAdapterV2SubagentUpdate | undefined {
  // Not the title: a later update without one is rewritten to a generic "Tool".
  const input = tool.data.rawInput;
  if (!isRecord(input) || typeof input.subagent_type !== "string") return undefined;
  const effort =
    typeof input.complexity === "string" ? EFFORT_BY_COMPLEXITY.get(input.complexity) : undefined;
  const output = isRecord(tool.data.rawOutput) ? (text(tool.data.rawOutput.text) ?? "") : "";
  const launchedInBackground = BACKGROUND_LAUNCH.test(output);
  const status =
    tool.status === "failed"
      ? "failed"
      : tool.status === "completed" && !launchedInBackground
        ? "completed"
        : "running";
  return {
    nativeTaskId: tool.toolCallId,
    childSessionId: SESSION_ID.exec(output)?.[1] ?? null,
    prompt: text(input.prompt) ?? text(input.description) ?? "",
    title: text(input.description) ?? text(input.subagent_type),
    model: modelOf(input.subagent_type),
    ...(effort === undefined ? {} : { effort }),
    status,
    result: status === "running" ? null : text(output.replace(REPORT_HEADER, "")),
  };
}

// Droid's own wake turn is a model turn, so it can run tools first. Past this, the wake is dropped
// rather than left waiting on a parent session that may never answer.
const WAKE_REPLY_TIMEOUT = Duration.minutes(5);

/**
 * Resolves with Droid's reply to its own "Background task completed" turn in the parent session, or
 * `undefined` when there is none (empty reply, unreadable file, or the timeout above). The watcher
 * starts before the first read so a reply landing between the two is still seen.
 */
export const awaitDroidWakeReply =
  (input: {
    readonly fileSystem: FileSystem.FileSystem;
    readonly environment: NodeJS.ProcessEnv;
  }): NonNullable<AcpAdapterV2Flavor["awaitParentWakeReply"]> =>
  ({ rootSessionId, childSessionId }) =>
    Effect.gen(function* () {
      const file = yield* findDroidSessionFile({
        sessionId: rootSessionId,
        environment: input.environment,
      });
      if (file === undefined) return undefined;
      const reply = yield* Stream.merge(
        input.fileSystem.watch(file).pipe(Stream.debounce(Duration.millis(200))),
        Stream.make(undefined),
      ).pipe(
        Stream.mapEffect(() => readDroidWakeReply({ file, childSessionId })),
        Stream.filter((text) => text !== undefined),
        Stream.take(1),
        Stream.runHead,
        Effect.timeoutOption(WAKE_REPLY_TIMEOUT),
      );
      if (reply._tag === "None" || reply.value._tag === "None") return undefined;
      return reply.value.value ?? undefined;
    }).pipe(Effect.catch(() => Effect.succeed(undefined)));

export const readDroidSubagentWorkFor =
  (input: {
    readonly environment: NodeJS.ProcessEnv;
  }): NonNullable<AcpAdapterV2Flavor["readSubagentWork"]> =>
  ({ childSessionId }) =>
    readDroidSubagentWork({ sessionId: childSessionId, environment: input.environment });

/** Session-owned watchers are prepared before a prompt can create its blocking Task children. */
export const makeDroidSubagentSupport = (input: { readonly environment: NodeJS.ProcessEnv }) => {
  const sessions = new WeakMap<
    Scope.Scope,
    Map<string, Effect.Success<ReturnType<typeof watchDroidSessionDirectory>>>
  >();
  const parentDirectory = Effect.fn("DroidAcp.parentDirectory")(function* (rootSessionId: string) {
    const file = yield* findDroidSessionFile({
      sessionId: rootSessionId,
      environment: input.environment,
    });
    if (file === undefined) return undefined;
    return NodePath.dirname(file);
  });
  const prepareSubagentSessionDiscovery: NonNullable<
    AcpAdapterV2Flavor["prepareSubagentSessionDiscovery"]
  > = ({ rootSessionId }) =>
    Effect.gen(function* () {
      const scope = yield* Effect.scope;
      let watchers = sessions.get(scope);
      if (watchers === undefined) {
        watchers = new Map();
        sessions.set(scope, watchers);
      }
      const directory = yield* parentDirectory(rootSessionId);
      if (directory === undefined) return () => Effect.succeed(undefined);
      let watcher = watchers.get(directory);
      if (watcher === undefined) {
        watcher = yield* watchDroidSessionDirectory(directory).pipe(Effect.orDie);
        watchers.set(directory, watcher);
      }
      return watcher.discoverFor(rootSessionId);
    });
  const awaitSubagentEnd: NonNullable<AcpAdapterV2Flavor["awaitSubagentEnd"]> = ({
    rootSessionId,
    childSessionId,
    startedAtMillis,
    onWork,
  }) =>
    Effect.gen(function* () {
      const scope = yield* Effect.scope;
      const watchers = sessions.get(scope);
      if (watchers === undefined) return undefined;
      const directory = yield* parentDirectory(rootSessionId);
      if (directory === undefined) return undefined;
      const watcher = watchers.get(directory);
      if (watcher === undefined) return undefined;
      const dirty = yield* Queue.sliding<void>(1);
      const remove = watcher.listen(`${childSessionId}.jsonl`, () =>
        Queue.offerUnsafe(dirty, undefined),
      );
      const follow = followDroidSubagent({
        file: NodePath.join(directory, `${childSessionId}.jsonl`),
        startedAtMillis,
      });
      yield* Queue.offer(dirty, undefined);
      const end = yield* Stream.fromQueue(dirty).pipe(
        Stream.mapEffect(() => follow.pipe(Effect.tap((state) => onWork(state.work)))),
        Stream.filter((state) => state.outcome !== undefined),
        Stream.take(1),
        Stream.runHead,
        Effect.ensuring(Effect.sync(remove)),
      );
      if (end._tag === "None" || end.value.outcome === undefined) return undefined;
      return {
        status: end.value.outcome,
        result: end.value.assistantMessages.at(-1) ?? null,
        work: end.value.work,
      };
    });
  return { prepareSubagentSessionDiscovery, awaitSubagentEnd };
};
