import {
  ProviderReplayTranscript,
  ThreadId,
  ProviderInstanceId,
  type OrchestrationV2ProviderTurn,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as NodeURL from "node:url";

import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DroidRpc from "./DroidRpc.ts";
import * as DroidAdapter from "./DroidAdapterV2.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/continuationRequests";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import * as ServerConfig from "../../config.ts";
import {
  makeReplayServerConfig,
  type OrchestratorV2ProviderReplayHarness,
} from "../testkit/ProviderReplayHarness.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";

const Frame = Schema.Struct({
  method: Schema.optionalKey(Schema.String),
  type: Schema.optionalKey(Schema.String),
  id: Schema.optionalKey(Schema.String),
  params: Schema.optionalKey(Schema.Unknown),
});
const Params = Schema.Struct({
  sessionId: Schema.optionalKey(Schema.String),
  notification: Schema.Record(Schema.String, Schema.Unknown),
});
const decodeFrame = Schema.decodeUnknownEffect(Frame);
const decodeParams = Schema.decodeUnknownEffect(Params);
const decodeLoadParams = Schema.decodeUnknownSync(Schema.Struct({ sessionId: Schema.String }));
const decodeMessageParams = Schema.decodeUnknownSync(Schema.Struct({ messageId: Schema.String }));
const decodeTranscript = Schema.decodeUnknownEffect(
  Schema.fromJsonString(ProviderReplayTranscript),
);

export const fixture = Effect.fnUntraced(function* (name: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = NodeURL.fileURLToPath(
    new URL(`../testkit/fixtures/droid_${name}.json`, import.meta.url),
  );
  const text = yield* fs.readFileString(path);
  return yield* decodeTranscript(text);
});

/**
 * Replay the protocol boundary with causal Deferred barriers, never timed
 * sleeps. Live transport framing is exercised separately in DroidRpc.test.
 */
export const makeReplayConnection = Effect.fnUntraced(function* (
  transcript?: ProviderReplayTranscript,
  autoRelease = false,
  responses: {
    readonly protocolVersion?: string;
    readonly initializedSessionId?: string;
    readonly compactedSessionId?: string;
  } = {},
) {
  const incoming = yield* Queue.unbounded<DroidRpc.DroidRpcNotification, DroidRpc.DroidRpcError>();
  const exit = yield* Deferred.make<number, DroidRpc.DroidRpcError>();
  const scope = yield* Effect.scope;
  const methods: string[] = [];
  const requests: { method: string; params: unknown }[] = [];
  const replies: { id: string | number; reply: DroidRpc.DroidRpcReply | undefined }[] = [];
  const answered = yield* Queue.unbounded<void>();
  let closed = false;
  let released = false;
  let nativeId = responses.initializedSessionId ?? "native-session";
  let messageId: string | undefined;
  let handler: Parameters<typeof DroidRpc.makeConnection>[0]["onRequest"];

  const emit = (notification: Record<string, unknown>) =>
    Queue.offer(incoming, { notification }).pipe(Effect.asVoid);
  const drain = Effect.gen(function* () {
    const ack = yield* Deferred.make<void>();
    yield* emit({ type: "t3_barrier", ack });
    yield* Deferred.await(ack);
  });
  const ask = Effect.fnUntraced(function* (request: DroidRpc.DroidRpcRequest) {
    const reply = handler === undefined ? undefined : yield* handler(request);
    replies.push({ id: request.id, reply });
    yield* Queue.offer(answered, undefined);
    return reply;
  });
  const replayFrames = Effect.fnUntraced(function* (recording: ProviderReplayTranscript) {
    for (const entry of recording.entries) {
      if (entry.type !== "emit_inbound") continue;
      const frame = yield* decodeFrame(entry.frame);
      if (frame.type === "request") {
        if (frame.id === undefined || frame.method === undefined) continue;
        yield* ask({ id: frame.id, method: frame.method, params: frame.params }).pipe(
          Effect.forkIn(scope),
        );
      } else if (frame.method === "droid.session_notification") {
        const params = yield* decodeParams(frame.params);
        // Replay the captured native id on this test connection.
        const notification = params.notification;
        const turnId = notification["turnId"];
        yield* Queue.offer(incoming, {
          ...params,
          sessionId: nativeId,
          notification:
            notification["type"] === "agent_turn_completed" &&
            typeof turnId === "string" &&
            !turnId.startsWith("task-completion:")
              ? { ...notification, turnId: messageId }
              : notification,
        });
      }
    }
  });
  const release = Effect.gen(function* () {
    if (released) return;
    released = true;
    if (transcript !== undefined) yield* replayFrames(transcript);
  });
  const connection: DroidRpc.DroidRpcConnection = {
    request: (method, params) =>
      Effect.gen(function* () {
        if (closed) throw new Error("Request after captured replay process closed");
        methods.push(method);
        requests.push({ method, params });
        if (method === "droid.add_user_message") messageId = decodeMessageParams(params).messageId;
        if (method === "droid.load_session") {
          const id = decodeLoadParams(params);
          nativeId = id.sessionId;
        }
        if (method === "droid.add_user_message" && autoRelease)
          yield* release.pipe(Effect.forkIn(scope));
        return {
          factoryProtocolVersion: responses.protocolVersion ?? "1.246.0",
          result:
            method === "droid.initialize_session"
              ? { sessionId: nativeId, session: { messages: [] } }
              : method === "droid.load_session"
                ? { session: { messages: [] } }
                : method === "droid.compact_session"
                  ? { newSessionId: responses.compactedSessionId ?? nativeId }
                  : {},
        };
      }),
    notifications: Stream.fromEffectRepeat(Queue.take(incoming)).pipe(
      Stream.mapEffect((notice) =>
        Effect.gen(function* () {
          const ack = notice.notification["ack"];
          if (notice.notification["type"] !== "t3_barrier" || !Deferred.isDeferred(ack))
            return notice;
          yield* Deferred.succeed(ack, undefined);
          return undefined;
        }),
      ),
      Stream.filter((notice): notice is DroidRpc.DroidRpcNotification => notice !== undefined),
    ),
    exited: Deferred.await(exit),
    close: Effect.gen(function* () {
      closed = true;
      yield* Deferred.succeed(exit, 0);
      yield* Queue.fail(incoming, new DroidRpc.DroidRpcError({ operation: "exit" }));
    }),
  };
  return {
    open: (options: Parameters<typeof DroidRpc.makeConnection>[0]) =>
      Effect.sync(() => {
        handler = options.onRequest;
        return connection;
      }),
    emit,
    drain,
    release,
    replayFrames,
    crash: Queue.fail(incoming, new DroidRpc.DroidRpcError({ operation: "exit" })),
    ask,
    methods,
    requests,
    replies,
    awaitReply: Queue.take(answered),
    closed: () => closed,
    assertComplete: Effect.sync(() => {
      if (transcript !== undefined && !released) throw new Error("Droid fixture never released");
    }),
    activeTurn: (runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime) =>
      Effect.gen(function* () {
        const thread = yield* runtime.ensureThread({
          threadId: ThreadId.make("droid-test-thread"),
          modelSelection: { instanceId: runtime.instanceId, model: "test" },
          runtimePolicy: {
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd: "/workspace",
          },
        });
        const snapshot = yield* runtime.readThreadSnapshot({ providerThread: thread });
        const active: OrchestrationV2ProviderTurn | undefined = snapshot.providerTurns.find(
          (turn) => turn.status === "running",
        );
        if (active === undefined)
          return yield* new DroidRpc.DroidRpcError({ operation: "active_turn" });
        return active;
      }),
  };
});

export const DroidOrchestratorReplayHarness: OrchestratorV2ProviderReplayHarness<ProviderReplayTranscript> =
  {
    driver: DroidAdapter.DROID_PROVIDER,
    decodeTranscript: (transcript) => Effect.succeed(transcript),
    makeProviderAdapterRegistryLayer: (transcript) => {
      const config = Layer.effect(
        ServerConfig.ServerConfig,
        makeReplayServerConfig(transcript.scenario).pipe(Effect.orDie),
      ).pipe(Layer.provide(NodeServices.layer));
      return Layer.unwrap(
        Effect.gen(function* () {
          const replay = yield* makeReplayConnection(transcript, true);
          return ProviderAdapterRegistry.layerFromAdapters([
            DroidAdapter.makeDroidAdapterV2({
              instanceId: ProviderInstanceId.make("droid-native-test"),
              environment: {},
              attachmentsDir: (yield* ServerConfig.ServerConfig).attachmentsDir,
              fileSystem: yield* FileSystem.FileSystem,
              path: yield* Path.Path,
              idAllocator: yield* IdAllocator.IdAllocatorV2,
              continuationRequests:
                yield* ProviderContinuationRequests.ProviderContinuationRequests,
              makeConnection: replay.open,
            }),
          ]);
        }),
      ).pipe(Layer.provide(Layer.mergeAll(NodeServices.layer, IdAllocator.layer, config)));
    },
  };
