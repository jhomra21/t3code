/**
 * Droid stream-JSON-RPC replay. The real adapter and rpc.ts framing run
 * unchanged; only ChildProcessSpawner's stdin/stdout transport is replaced.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderInstanceId,
  ProviderReplayEntry,
  type ProviderReplayEntry as ProviderReplayEntryType,
  type ProviderReplayTranscript,
} from "@t3tools/contracts";
import {
  DROID_PROVIDER,
  makeDroidAdapterV2,
  type DroidAdapterV2Options,
} from "@t3tools/provider-droid/testing";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";

import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import type { OrchestratorV2ProviderReplayHarness } from "../testkit/ProviderReplayHarness.ts";

export const DROID_STREAM_JSONRPC_PROTOCOL = "droid.stream-jsonrpc";
export const DROID_REPLAY_ANY = "<any>";

const DroidReplayTranscript = Schema.Struct({
  provider: Schema.Literal(DROID_PROVIDER),
  protocol: Schema.Literal(DROID_STREAM_JSONRPC_PROTOCOL),
  version: Schema.String,
  scenario: Schema.String,
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  entries: Schema.Array(ProviderReplayEntry),
});
type DroidReplayTranscript = typeof DroidReplayTranscript.Type;
const decodeDroidReplayTranscript = Schema.decodeUnknownEffect(DroidReplayTranscript);

class DroidReplayTranscriptDecodeError extends Schema.TaggedError<DroidReplayTranscriptDecodeError>()(
  "DroidReplayTranscriptDecodeError",
  { scenario: Schema.optional(Schema.String), cause: Schema.Defect() },
) {
  override get message(): string {
    return `Failed to decode Droid replay transcript for ${this.scenario ?? "<unknown>"}.`;
  }
}

class DroidReplayMismatchError extends Schema.TaggedError<DroidReplayMismatchError>()(
  "DroidReplayMismatchError",
  {
    scenario: Schema.String,
    cursor: Schema.Number,
    host: Schema.Number,
    expected: Schema.Unknown,
    actual: Schema.Unknown,
  },
) {
  override get message(): string {
    return `Droid replay frame mismatch at cursor ${this.cursor} on host ${this.host} in ${this.scenario}.`;
  }
}

class DroidReplayIncompleteError extends Schema.TaggedError<DroidReplayIncompleteError>()(
  "DroidReplayIncompleteError",
  {
    scenario: Schema.String,
    cursor: Schema.Number,
    remaining: Schema.Number,
  },
) {
  override get message(): string {
    return `Droid replay ended with ${this.remaining} unconsumed entries in ${this.scenario}.`;
  }
}

const DroidReplayHarnessError = Schema.Union([DroidReplayTranscriptDecodeError]);
type DroidReplayHarnessError = typeof DroidReplayHarnessError.Type;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function replayValueMatches(expected: unknown, actual: unknown): boolean {
  if (expected === DROID_REPLAY_ANY) return true;
  if (Array.isArray(expected)) {
    return (
      Array.isArray(actual) &&
      expected.length === actual.length &&
      expected.every((entry, index) => replayValueMatches(entry, actual[index]))
    );
  }
  if (isRecord(expected)) {
    if (!isRecord(actual)) return false;
    const keys = new Set([...Object.keys(expected), ...Object.keys(actual)]);
    return [...keys].every((key) => replayValueMatches(expected[key], actual[key]));
  }
  return Object.is(expected, actual);
}

function startsTurn(entry: ProviderReplayEntryType): boolean {
  if (entry.type !== "expect_outbound" || !isRecord(entry.frame)) return false;
  return entry.frame.method === "droid.add_user_message";
}

function entryHost(entry: ProviderReplayEntryType): number {
  const label = entry.type === "runtime_exit" ? undefined : entry.label;
  const match = label === undefined ? null : /@h(\d+)$/u.exec(label);
  return match === null ? 1 : Number(match[1]);
}

function recordLabel(frame: unknown, direction: "expect_outbound" | "emit_inbound", host: number) {
  if (!isRecord(frame)) return undefined;
  const method = typeof frame.method === "string" ? frame.method : undefined;
  const base =
    method === undefined
      ? typeof frame.type === "string"
        ? frame.type
        : undefined
      : direction === "emit_inbound" && frame.type === "request"
        ? `request:${method}`
        : method;
  return base === undefined || host === 1 ? base : `${base}@h${host}`;
}

function makeLineSplitter() {
  let buffer = "";
  const decoder = new TextDecoder();
  return (chunk: Uint8Array): ReadonlyArray<string> => {
    buffer += decoder.decode(chunk, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    return lines.map((line) => line.replace(/\r$/u, "")).filter((line) => line.length > 0);
  };
}

const decodeJsonLine = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

type DroidReplayHost = {
  readonly ordinal: number;
  readonly stdout: Queue.Queue<Uint8Array, Cause.Done>;
  readonly ids: Map<string | number, string | number>;
  open: boolean;
};

class DroidReplayController {
  private cursor = 0;
  private failure: unknown = null;
  private readonly consumed = new Set<number>();
  private readonly held: Array<{ readonly host: DroidReplayHost; readonly actual: unknown }> = [];
  private readonly hosts: Array<DroidReplayHost> = [];
  private readonly transcript: DroidReplayTranscript;

  constructor(transcript: DroidReplayTranscript) {
    this.transcript = transcript;
  }

  spawn(stdout: Queue.Queue<Uint8Array, Cause.Done>): DroidReplayHost {
    const host = {
      ordinal: this.hosts.length + 1,
      stdout,
      ids: new Map<string | number, string | number>(),
      open: true,
    };
    this.hosts.push(host);
    return host;
  }

  receive(host: DroidReplayHost, actual: unknown): void {
    if (this.failure !== null) return;
    const nextEntry = this.transcript.entries[this.cursor];
    if (
      isRecord(actual) &&
      actual.type === "request" &&
      actual.method === "droid.get_context_breakdown" &&
      (typeof actual.id === "string" || typeof actual.id === "number") &&
      !(
        nextEntry?.type === "expect_outbound" &&
        isRecord(nextEntry.frame) &&
        nextEntry.frame.method === "droid.get_context_breakdown"
      )
    ) {
      Queue.offerUnsafe(
        host.stdout,
        new TextEncoder().encode(
          `${encodeJsonLine({
            jsonrpc: "2.0",
            type: "response",
            id: actual.id,
            // Recorded sessions predate the query; they answer as a session that cannot report.
            result: {},
          })}\n`,
        ),
      );
      return;
    }
    let possible = false;
    for (let index = this.cursor; index < this.transcript.entries.length && !possible; index += 1) {
      const entry = this.transcript.entries[index]!;
      possible =
        entry.type === "expect_outbound" &&
        !this.consumed.has(index) &&
        entryHost(entry) === host.ordinal &&
        replayValueMatches(this.rebindExpected(entry.frame, actual), actual);
      if (!possible && entry.type === "expect_outbound" && startsTurn(entry)) break;
    }
    if (!possible) {
      this.fail(
        new DroidReplayMismatchError({
          scenario: this.transcript.scenario,
          cursor: this.cursor,
          host: host.ordinal,
          expected: this.transcript.entries[this.cursor] ?? null,
          actual,
        }),
      );
      return;
    }
    this.held.push({ host, actual });
    this.advance();
  }

  close(host: DroidReplayHost): void {
    host.open = false;
    Queue.endUnsafe(host.stdout);
  }

  assertComplete(): void {
    if (this.failure !== null) throw this.failure;
    if (this.cursor !== this.transcript.entries.length || this.held.length > 0) {
      throw new DroidReplayIncompleteError({
        scenario: this.transcript.scenario,
        cursor: this.cursor,
        remaining: this.transcript.entries.length - this.cursor,
      });
    }
  }

  private advance(): void {
    let progressed = true;
    while (progressed && this.failure === null) {
      progressed = this.drainInbound();
      for (const [index, write] of this.held.entries()) {
        if (this.consumePending(write.host, write.actual)) {
          this.held.splice(index, 1);
          progressed = true;
          break;
        }
      }
    }
  }

  private consumePending(host: DroidReplayHost, actual: unknown): boolean {
    for (let index = this.cursor; index < this.transcript.entries.length; index += 1) {
      const entry = this.transcript.entries[index]!;
      if (entry.type !== "expect_outbound") return false;
      if (this.consumed.has(index) || entryHost(entry) !== host.ordinal) continue;
      if (!replayValueMatches(this.rebindExpected(entry.frame, actual), actual)) continue;
      const recordedId = isRecord(entry.frame) ? entry.frame.id : undefined;
      const actualId = isRecord(actual) ? actual.id : undefined;
      if (
        (typeof recordedId === "string" || typeof recordedId === "number") &&
        (typeof actualId === "string" || typeof actualId === "number") &&
        isRecord(entry.frame) &&
        entry.frame.type !== "response"
      )
        host.ids.set(recordedId, actualId);
      this.consumed.add(index);
      while (this.consumed.has(this.cursor)) this.cursor += 1;
      return true;
    }
    return false;
  }

  private drainInbound(): boolean {
    let emitted = false;
    while (this.failure === null) {
      const entry = this.transcript.entries[this.cursor];
      if (entry?.type === "runtime_exit") {
        this.cursor += 1;
        emitted = true;
        const host = this.hosts.at(-1);
        if (host !== undefined) this.close(host);
        continue;
      }
      if (entry?.type !== "emit_inbound") return emitted;
      const host = this.hosts[entryHost(entry) - 1];
      if (host === undefined) return emitted;
      this.cursor += 1;
      emitted = true;
      if (!host.open) continue;
      const frame = entry.frame;
      const recordedId = isRecord(frame) ? frame.id : undefined;
      const reboundId =
        (typeof recordedId === "string" || typeof recordedId === "number") &&
        isRecord(frame) &&
        frame.type === "response"
          ? host.ids.get(recordedId)
          : undefined;
      const rebound = this.rebindFrame(
        isRecord(frame) && reboundId !== undefined ? { ...frame, id: reboundId } : frame,
        host.ids,
      );
      Queue.offerUnsafe(host.stdout, new TextEncoder().encode(`${encodeJsonLine(rebound)}\n`));
    }
    return emitted;
  }

  private rebindExpected(expected: unknown, actual: unknown): unknown {
    if (!isRecord(expected) || !isRecord(actual)) return expected;
    const expectedId = expected.id;
    const actualId = actual.id;
    return (typeof expectedId === "string" || typeof expectedId === "number") &&
      (typeof actualId === "string" || typeof actualId === "number") &&
      expected.type === actual.type &&
      expected.method === actual.method
      ? { ...expected, id: actualId }
      : expected;
  }

  private rebindFrame(frame: unknown, ids: ReadonlyMap<string | number, string | number>): unknown {
    if (typeof frame === "string") return ids.get(frame) ?? frame;
    if (Array.isArray(frame)) return frame.map((value) => this.rebindFrame(value, ids));
    if (!isRecord(frame)) return frame;
    return Object.fromEntries(
      Object.entries(frame).map(([key, value]) => [key, this.rebindFrame(value, ids)]),
    );
  }

  private fail(cause: unknown): void {
    if (this.failure !== null) return;
    this.failure = cause;
    for (const host of this.hosts) Queue.endUnsafe(host.stdout);
  }
}

function makeReplaySpawner(controller: DroidReplayController) {
  return ChildProcessSpawner.make(() =>
    Effect.gen(function* () {
      const host = controller.spawn(yield* Queue.unbounded<Uint8Array, Cause.Done>());
      yield* Effect.addFinalizer(() => Effect.sync(() => controller.close(host)));
      const split = makeLineSplitter();
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(999_999_999),
        exitCode: Effect.never,
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((chunk: Uint8Array) =>
          Effect.sync(() => {
            for (const line of split(chunk)) controller.receive(host, decodeJsonLine(line));
          }),
        ),
        stdout: Stream.fromQueue(host.stdout),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );
}

export function layer(input: {
  readonly spawner: Layer.Layer<ChildProcessSpawner.ChildProcessSpawner>;
  readonly binaryPath: string;
  readonly environment?: NodeJS.ProcessEnv;
}) {
  return ProviderAdapterRegistry.layerFromAdaptersEffect(
    Effect.gen(function* () {
      const continuationRequests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
      return [
        yield* makeDroidAdapterV2({
          instanceId: ProviderInstanceId.make("droid"),
          environment: input.environment ?? {},
          binaryPath: input.binaryPath,
          continuationRequests,
        } satisfies DroidAdapterV2Options),
      ];
    }),
  ).pipe(
    Layer.provide(input.spawner),
    Layer.provide(
      Layer.mergeAll(
        TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
        NodeServices.layer,
        IdAllocator.layer,
      ),
    ),
  );
}

export const DroidOrchestratorReplayHarness: OrchestratorV2ProviderReplayHarness<
  DroidReplayTranscript,
  DroidReplayHarnessError
> = {
  driver: DROID_PROVIDER,
  decodeTranscript: (transcript: ProviderReplayTranscript) =>
    decodeDroidReplayTranscript(transcript).pipe(
      Effect.mapError(
        (cause) => new DroidReplayTranscriptDecodeError({ scenario: transcript.scenario, cause }),
      ),
    ),
  makeProviderAdapterRegistryLayer: (transcript) =>
    Layer.unwrap(
      Effect.gen(function* () {
        const controller = new DroidReplayController(transcript);
        yield* Effect.addFinalizer(() => Effect.sync(() => controller.assertComplete()));
        return layer({
          spawner: Layer.succeed(
            ChildProcessSpawner.ChildProcessSpawner,
            makeReplaySpawner(controller),
          ),
          binaryPath: "droid",
        });
      }),
    ),
};

export function makeDroidRecordingSpawner(
  live: ChildProcessSpawner.ChildProcessSpawner["Service"],
  entries: Array<ProviderReplayEntryType>,
): ChildProcessSpawner.ChildProcessSpawner["Service"] {
  let spawned = 0;
  return ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      const handle = yield* live.spawn(command);
      const host = ++spawned;
      const record = (type: "expect_outbound" | "emit_inbound", frame: unknown) => {
        const label = recordLabel(frame, type, host);
        entries.push({ type, ...(label === undefined ? {} : { label }), frame });
      };
      const splitIn = makeLineSplitter();
      const splitOut = makeLineSplitter();
      return ChildProcessSpawner.makeHandle({
        ...handle,
        stdin: handle.stdin.pipe(
          Sink.mapInput((chunk: Uint8Array) => {
            for (const line of splitIn(chunk)) record("expect_outbound", decodeJsonLine(line));
            return chunk;
          }),
        ),
        stdout: handle.stdout.pipe(
          Stream.tap((chunk) =>
            Effect.sync(() => {
              for (const line of splitOut(chunk)) record("emit_inbound", decodeJsonLine(line));
            }),
          ),
        ),
      });
    }),
  );
}
