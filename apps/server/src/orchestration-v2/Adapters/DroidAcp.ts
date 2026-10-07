import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";
import * as Stream from "effect/Stream";

import type { AcpToolCallState } from "../../provider/acp/AcpRuntimeModel.ts";
import {
  findDroidSessionFile,
  readDroidSubagentSession,
  readDroidSubagentSteps,
  readDroidWakeReply,
} from "../../provider/acp/DroidSessionTranscript.ts";
import type { AcpAdapterV2Flavor, AcpAdapterV2SubagentUpdate } from "./AcpAdapterV2.ts";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

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
export function extractDroidSubagentUpdate(
  tool: AcpToolCallState,
): AcpAdapterV2SubagentUpdate | undefined {
  // Not the title: a later update without one is rewritten to a generic "Tool".
  const input = tool.data.rawInput;
  if (!isRecord(input) || typeof input.subagent_type !== "string") return undefined;
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
    model: null,
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
        Stream.mapEffect(() =>
          readDroidWakeReply({
            sessionId: rootSessionId,
            childSessionId,
            environment: input.environment,
          }),
        ),
        Stream.filter((text) => text !== undefined),
        Stream.take(1),
        Stream.runHead,
        Effect.timeoutOption(WAKE_REPLY_TIMEOUT),
      );
      if (reply._tag === "None" || reply.value._tag === "None") return undefined;
      return reply.value.value ?? undefined;
    }).pipe(Effect.catch(() => Effect.succeed(undefined)));

export const readDroidSubagentStepsFor =
  (input: {
    readonly environment: NodeJS.ProcessEnv;
  }): NonNullable<AcpAdapterV2Flavor["readSubagentSteps"]> =>
  ({ childSessionId }) =>
    readDroidSubagentSteps({ sessionId: childSessionId, environment: input.environment });

/**
 * Resolves when a background worker's own session file ends its turn. The
 * watcher starts before the first read, so an end that lands between the two is
 * still seen. A worker whose file cannot be found yields `undefined`, which
 * leaves the subagent as launched rather than guessing an outcome.
 */
export const awaitDroidSubagentEnd =
  (input: {
    readonly fileSystem: FileSystem.FileSystem;
    readonly environment: NodeJS.ProcessEnv;
  }): NonNullable<AcpAdapterV2Flavor["awaitSubagentEnd"]> =>
  ({ childSessionId, startedAtMillis }) =>
    Effect.gen(function* () {
      const file = yield* findDroidSessionFile({
        sessionId: childSessionId,
        environment: input.environment,
      });
      if (file === undefined) return undefined;
      const end = yield* Stream.merge(
        input.fileSystem.watch(file).pipe(Stream.debounce(Duration.millis(200))),
        Stream.make(undefined),
      ).pipe(
        Stream.mapEffect(() =>
          readDroidSubagentSession({
            sessionId: childSessionId,
            environment: input.environment,
            startedAtMillis,
          }),
        ),
        Stream.filter((session) => session.outcome !== undefined),
        Stream.take(1),
        Stream.runHead,
      );
      if (end._tag === "None" || end.value.outcome === undefined) return undefined;
      return { status: end.value.outcome, result: end.value.assistantMessages.at(-1) ?? null };
    }).pipe(Effect.catch(() => Effect.succeed(undefined)));
