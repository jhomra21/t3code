// @effect-diagnostics nodeBuiltinImport:off
import { assert, describe, it } from "@effect/vitest";
import * as NodeFSP from "node:fs/promises";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeOS from "node:os";
import * as NodeEvents from "node:events";
import {
  EnvironmentId,
  ChatAttachmentId,
  CheckpointId,
  MessageId,
  NodeId,
  OrchestrationV2AppThread,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { ChildProcessSpawner } from "effect/process";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import type { McpProviderSessionConfig } from "@t3tools/provider-core/server/mcpSession";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import { DROID_PROVIDER, droidSystemPromptAppend, makeDroidAdapterV2 } from "./adapter.ts";
import {
  DroidSessionWatch,
  DroidSessionRead,
  readDroidSessionAppend,
} from "./sessionTranscript.ts";

const instanceId = ProviderInstanceId.make("droid-adapter-test");
const selection = { instanceId, model: "custom:GPT-6 Luna-12" };
const policy = {
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: "/workspace",
} satisfies ProviderAdapter.ProviderAdapterV2RuntimePolicy;
const threadId = ThreadId.make("droid-adapter-test-thread");
const decodeAppThread = Schema.decodeUnknownEffect(OrchestrationV2AppThread);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

type RpcFrame = {
  readonly type?: string;
  readonly id?: string | number;
  readonly method?: string;
  readonly params?: unknown;
  readonly result?: unknown;
};

const makeFakeDroid = (
  options: {
    readonly protocolVersion?: string;
    readonly compactSessionResult?: {
      readonly newSessionId: string;
      readonly removedCount: number;
    };
    readonly contextBreakdown?: {
      readonly contextBudget: number;
      readonly usedTokens: number;
      readonly lastCallCompactionTokens?: number;
    };
    readonly contextBreakdownSequence?: ReadonlyArray<{
      readonly contextBudget: number;
      readonly usedTokens: number;
      readonly lastCallCompactionTokens?: number;
    }>;
    readonly holdAddUserMessageResponse?: boolean;
    readonly nativeSessionId?: string;
    readonly rewindSessionId?: string;
    readonly failInterruptSession?: boolean;
    readonly holdInterruptSessionResponse?: boolean;
  } = {},
) =>
  Effect.gen(function* () {
    const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
    const requests = yield* Queue.unbounded<RpcFrame>();
    const replies = yield* Queue.unbounded<RpcFrame>();
    const requestedMethods: string[] = [];
    let buffer = "";
    let closed = false;
    let heldAddUserMessage: RpcFrame | undefined;
    let heldAddUserMessageOnce = false;
    let heldInterruptSession: RpcFrame | undefined;
    let heldInterruptSessionOnce = false;
    let contextBreakdownIndex = 0;
    const emit = (frame: unknown) =>
      Queue.offer(stdout, new TextEncoder().encode(`${encodeJson(frame)}\n`)).pipe(Effect.asVoid);
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(
        ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(999_999_999),
          exitCode: Effect.never,
          isRunning: Effect.succeed(true),
          kill: () =>
            Effect.sync(() => {
              closed = true;
            }),
          unref: Effect.succeed(Effect.void),
          stdin: Sink.forEach((chunk: Uint8Array) =>
            Effect.gen(function* () {
              buffer += new TextDecoder().decode(chunk);
              while (true) {
                const newline = buffer.indexOf("\n");
                if (newline < 0) return;
                const frame = decodeFrame(buffer.slice(0, newline));
                buffer = buffer.slice(newline + 1);
                if (frame.type === "request" && frame.id !== undefined) {
                  if (frame.method !== undefined) requestedMethods.push(frame.method);
                  yield* Queue.offer(requests, frame);
                  const contextBreakdown =
                    frame.method === "droid.get_context_breakdown"
                      ? options.contextBreakdownSequence?.[
                          Math.min(
                            contextBreakdownIndex++,
                            (options.contextBreakdownSequence?.length ?? 1) - 1,
                          )
                        ]
                      : undefined;
                  const responseOptions =
                    contextBreakdown === undefined ? options : { ...options, contextBreakdown };
                  if (
                    options.holdAddUserMessageResponse &&
                    !heldAddUserMessageOnce &&
                    frame.method === "droid.add_user_message"
                  ) {
                    heldAddUserMessage = frame;
                    heldAddUserMessageOnce = true;
                  } else if (
                    options.holdInterruptSessionResponse &&
                    !heldInterruptSessionOnce &&
                    frame.method === "droid.interrupt_session"
                  ) {
                    heldInterruptSession = frame;
                    heldInterruptSessionOnce = true;
                  } else {
                    yield* emit(responseFor(frame, responseOptions));
                  }
                } else if (frame.type === "response") {
                  yield* Queue.offer(replies, frame);
                }
              }
            }),
          ),
          stdout: Stream.fromQueue(stdout),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        }),
      ),
    );
    return {
      spawner,
      emitNotification: (
        notification: Record<string, unknown>,
        sessionId = options.nativeSessionId ?? "native-session",
      ) =>
        emit({
          type: "notification",
          jsonrpc: "2.0",
          method: "droid.session_notification",
          params: { sessionId, notification },
        }),
      emitRequest: (id: string, method: string, params: unknown) =>
        emit({ type: "request", jsonrpc: "2.0", id, method, params }),
      takeRequest: (method: string) =>
        Effect.gen(function* () {
          while (true) {
            const frame = yield* Queue.take(requests);
            if (frame.method === method) return frame;
          }
        }),
      takeReply: Queue.take(replies),
      requestedMethods,
      answerAddUserMessage: Effect.gen(function* () {
        if (heldAddUserMessage === undefined)
          return yield* Effect.die(new Error("No held droid.add_user_message request"));
        yield* emit(responseFor(heldAddUserMessage, options));
        heldAddUserMessage = undefined;
      }),
      answerInterruptSession: Effect.gen(function* () {
        if (heldInterruptSession === undefined)
          return yield* Effect.die(new Error("No held droid.interrupt_session request"));
        yield* emit(responseFor(heldInterruptSession, options));
        heldInterruptSession = undefined;
      }),
      closed: () => closed,
      closeStdout: Queue.end(stdout),
    };
  });

const responseFor = (
  frame: RpcFrame,
  options: {
    readonly protocolVersion?: string;
    readonly compactSessionResult?: {
      readonly newSessionId: string;
      readonly removedCount: number;
    };
    readonly contextBreakdown?: {
      readonly contextBudget: number;
      readonly usedTokens: number;
      readonly lastCallCompactionTokens?: number;
    };
    readonly nativeSessionId?: string;
    readonly rewindSessionId?: string;
    readonly failInterruptSession?: boolean;
    readonly holdInterruptSessionResponse?: boolean;
  },
) => ({
  type: "response",
  jsonrpc: "2.0",
  id: frame.id,
  factoryProtocolVersion: options.protocolVersion ?? "1.257.0",
  ...(frame.method === "droid.interrupt_session" && options.failInterruptSession
    ? { error: { code: -32000, message: "Synthetic interrupt failure" } }
    : {
        result:
          frame.method === "droid.initialize_session"
            ? {
                sessionId: options.nativeSessionId ?? "native-session",
                session: { messages: [] },
              }
            : frame.method === "droid.load_session"
              ? { session: { messages: [] } }
              : frame.method === "droid.get_context_breakdown"
                ? (options.contextBreakdown ?? {})
                : frame.method === "droid.compact_session"
                  ? (options.compactSessionResult ?? {})
                  : frame.method === "droid.execute_rewind"
                    ? { newSessionId: options.rewindSessionId ?? "rewound-session" }
                    : {},
      }),
});

const RpcFrameSchema = Schema.Struct({
  type: Schema.optionalKey(Schema.String),
  id: Schema.optionalKey(Schema.Union([Schema.String, Schema.Number])),
  method: Schema.optionalKey(Schema.String),
  params: Schema.optionalKey(Schema.Unknown),
  result: Schema.optionalKey(Schema.Unknown),
});
const decodeFrame = Schema.decodeSync(Schema.fromJsonString(RpcFrameSchema));

const open = Effect.fnUntraced(function* (
  fake: Effect.Success<ReturnType<typeof makeFakeDroid>>,
  continuationRequests?: Parameters<typeof makeDroidAdapterV2>[0]["continuationRequests"],
  cwd = policy.cwd,
  mcpSession?: McpProviderSessionConfig,
  resolveAttachmentPath?: (
    attachment: Parameters<ProviderHost.ProviderHost["Service"]["resolveAttachmentPath"]>[0],
  ) => string | null,
  existingProviderThread?: ProviderAdapter.ProviderAdapterV2EnsureThreadInput["existingProviderThread"],
  skipEnsureThread = false,
  environment: NodeJS.ProcessEnv = {},
) {
  const providerHost = yield* ProviderHost.ProviderHost;
  if (mcpSession !== undefined)
    yield* McpProviderSessions.McpProviderSessions.pipe(
      Effect.flatMap((sessions) => sessions.set(mcpSession)),
    );
  const adapter = yield* makeDroidAdapterV2({
    instanceId,
    environment,
    binaryPath: "droid",
    ...(continuationRequests === undefined ? {} : { continuationRequests }),
  }).pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
    Effect.provideService(
      ProviderHost.ProviderHost,
      resolveAttachmentPath === undefined
        ? providerHost
        : { ...providerHost, resolveAttachmentPath },
    ),
  );
  const runtime = yield* adapter.openSession({
    threadId,
    providerSessionId: ProviderSessionId.make("droid-adapter-test-session"),
    modelSelection: selection,
    runtimePolicy: { ...policy, cwd },
  });
  const providerThread = skipEnsureThread
    ? existingProviderThread
    : yield* runtime.ensureThread({
        threadId,
        modelSelection: selection,
        runtimePolicy: { ...policy, cwd },
        ...(existingProviderThread === undefined ? {} : { existingProviderThread }),
      });
  if (providerThread === undefined)
    return yield* Effect.die(new Error("Fresh Droid session requires a persisted provider thread"));
  const now = yield* DateTime.now;
  const appThread = yield* decodeAppThread({
    id: threadId,
    projectId: ProjectId.make("droid-adapter-test-project"),
    title: "Droid adapter test",
    createdBy: "user",
    creationSource: "web",
    providerInstanceId: instanceId,
    modelSelection: selection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThread.id,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  });
  const input: ProviderAdapter.ProviderAdapterV2TurnInput = {
    appThread,
    threadId,
    providerThread,
    runId: RunId.make("droid-adapter-test-run"),
    runOrdinal: 1,
    providerTurnOrdinal: 1,
    attemptId: RunAttemptId.make("droid-adapter-test-attempt"),
    rootNodeId: NodeId.make("droid-adapter-test-root"),
    message: {
      messageId: MessageId.make("droid-adapter-test-message"),
      text: "Hello",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: selection,
    runtimePolicy: policy,
  };
  return { runtime, providerThread, input };
});

const provide = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.scoped,
    Effect.provide(
      Layer.mergeAll(
        NodeServices.layer,
        IdAllocator.layer,
        McpProviderSessions.layer,
        TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
      ),
    ),
  );

class SyntheticWatcher extends NodeEvents.EventEmitter {
  closed = false;
  close() {
    this.closed = true;
  }
  ref() {
    return this;
  }
  unref() {
    return this;
  }
}

const lifecycleFixture = Effect.fnUntraced(function* (
  watchOpenFails = false,
  initiallyEnded = false,
  failInitialWatch = false,
  failInterruptSession = false,
  holdInterruptSessionResponse = false,
  registerInitialChild = true,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home = yield* fs.makeTempDirectoryScoped({
    directory: NodeOS.tmpdir(),
    prefix: "droid-lifecycle-",
  });
  const directory = path.join(home, ".factory", "sessions", "workspace");
  yield* fs.makeDirectory(directory, { recursive: true });
  const parentId = "229d7d4f-9420-4fc5-8ad4-e4f48dcc0401";
  const childId = "43a7fb2d-79a1-4c49-a8d1-3b089cf7d202";
  const fileFor = (id: string) => path.join(directory, `${id}.jsonl`);
  const file = fileFor(childId);
  const row = (value: object) => `${JSON.stringify(value)}\n`;
  const user = (timestamp: string) =>
    row({ type: "message", timestamp, message: { role: "user", content: [] } });
  const text = (id: string, value: string) =>
    row({
      type: "message",
      id,
      message: { role: "assistant", content: [{ type: "text", text: value }] },
    });
  yield* Effect.promise(() =>
    NodeFSP.writeFile(path.join(directory, `${parentId}.jsonl`), row({ type: "session_start" })),
  );
  const now = yield* DateTime.now;
  const initialRows =
    user(DateTime.formatIso(now)) +
    text("ready", "ready") +
    (initiallyEnded ? row({ type: "agent_turn_outcome", reason: "completed" }) : "");
  yield* Effect.promise(() => NodeFSP.writeFile(file, initialRows));
  yield* Effect.promise(() =>
    NodeFSP.writeFile(file.replace(/\.jsonl$/u, ".settings.json"), '{"model":"pinned"}'),
  );
  const watchers: SyntheticWatcher[] = [];
  const listeners: Array<(event: string, name: string | Buffer | null) => void> = [];
  const watchClosed = yield* Deferred.make<void>();
  let firstRead = true;
  const fake = yield* makeFakeDroid({
    nativeSessionId: parentId,
    failInterruptSession,
    holdInterruptSessionResponse,
  });
  const sessionScope = yield* Scope.fork(yield* Effect.scope, "sequential");
  const opened = yield* open(fake, undefined, undefined, undefined, undefined, undefined, false, {
    FACTORY_HOME_OVERRIDE: home,
  }).pipe(
    Effect.provideService(Scope.Scope, sessionScope),
    Effect.provideService(DroidSessionRead, (readFile, offset, inode) =>
      (initiallyEnded || failInitialWatch) && firstRead
        ? Effect.sync(() => {
            if (failInitialWatch && firstRead)
              watchers[0]!.emit("error", new Error("synchronous watcher failure"));
            firstRead = false;
            return {
              text: initialRows.slice(offset),
              consumed: Buffer.byteLength(initialRows.slice(offset)),
              restarted: false,
              inode: undefined,
            };
          })
        : readDroidSessionAppend(readFile, offset, inode),
    ),
    Effect.provideService(DroidSessionWatch, (_directory, listener) => {
      if (watchOpenFails) throw new Error("synthetic open failure");
      const watcher = new SyntheticWatcher();
      watcher.close = () => {
        watcher.closed = true;
        Deferred.doneUnsafe(watchClosed, Effect.void);
      };
      watchers.push(watcher);
      listeners.push(listener);
      return watcher;
    }),
  );
  const events: ProviderAdapter.ProviderAdapterV2Event[] = [];
  const ready = yield* Deferred.make<void>();
  const finished = yield* Deferred.make<void>();
  const childModelReady = yield* Deferred.make<void>();
  const terminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
  let notificationBarrier: Deferred.Deferred<void> | undefined;
  let terminalBarrier: Deferred.Deferred<ProviderAdapter.ProviderAdapterV2Event> | undefined;
  let barrierNumber = 0;
  const childSettled = new Map<string, Deferred.Deferred<void>>();
  yield* opened.runtime.events.pipe(
    Stream.runForEach((event) => {
      events.push(event);
      if (event.type === "message.updated" && event.message.text.startsWith("lifecycle barrier"))
        return notificationBarrier === undefined
          ? Effect.void
          : Deferred.succeed(notificationBarrier, undefined);
      if (event.type === "subagent.updated" && event.subagent.status !== "running")
        return Effect.all([
          Deferred.succeed(finished, undefined),
          ...(childSettled.has(event.subagent.nativeTaskRef?.nativeId ?? "")
            ? [
                Deferred.succeed(
                  childSettled.get(event.subagent.nativeTaskRef?.nativeId ?? "")!,
                  undefined,
                ),
              ]
            : []),
        ]);
      if (
        event.type === "subagent.updated" &&
        event.subagent.nativeTaskRef?.nativeId === childId &&
        event.subagent.model !== null
      )
        return Deferred.succeed(childModelReady, undefined);
      if (event.type === "turn.terminal")
        return Effect.all([
          Deferred.succeed(terminal, event),
          terminalBarrier === undefined ? Effect.void : Deferred.succeed(terminalBarrier, event),
        ]);
      return event.type === "message.updated" && event.message.text === "ready"
        ? Deferred.succeed(ready, undefined)
        : Effect.void;
    }),
    Effect.forkChild,
  );
  yield* opened.runtime.startTurn(opened.input);
  yield* fake.takeRequest("droid.add_user_message");
  const barrier = Effect.gen(function* () {
    notificationBarrier = yield* Deferred.make<void>();
    barrierNumber++;
    yield* fake.emitNotification({
      type: "create_message",
      message: {
        id: `barrier-${barrierNumber}`,
        role: "assistant",
        content: [{ type: "text", text: `lifecycle barrier ${barrierNumber}` }],
      },
    });
    yield* Deferred.await(notificationBarrier);
  });
  const register = (toolUseId = "lifecycle-task", id = childId) =>
    fake
      .emitNotification({
        type: "child_session_available",
        childSessionId: id,
        toolUseId,
        description: "Lifecycle child",
      })
      .pipe(Effect.andThen(barrier));
  if (registerInitialChild) yield* register();
  if (!watchOpenFails && registerInitialChild) yield* Deferred.await(ready);
  const complete = (reason = "completed", id = childId, waitForBarrier = true) => {
    const notification = fake.emitNotification({
      type: "create_message",
      message: {
        id: `task-completion:${id}`,
        role: "system",
        content: [
          {
            type: "text",
            text: `task_id: ${id}\nreason: ${reason}\noutput: parent result\n\nBackground task finished`,
          },
        ],
      },
    });
    return waitForBarrier ? notification.pipe(Effect.andThen(barrier)) : notification;
  };
  const append = (value: string, id = childId) =>
    Effect.promise(() => NodeFSP.appendFile(fileFor(id), value));
  const writeChild = (id: string, value: string) =>
    Effect.promise(() => NodeFSP.writeFile(fileFor(id), value));
  const status = (id = childId) => {
    for (let index = events.length - 1; index >= 0; index--) {
      const event = events[index];
      if (event?.type === "subagent.updated" && event.subagent.nativeTaskRef?.nativeId === id)
        return event.subagent;
    }
    return undefined;
  };
  return {
    ...opened,
    fake,
    file,
    fileFor,
    watchers,
    listeners,
    events,
    row,
    user,
    text,
    append,
    writeChild,
    complete,
    register,
    barrier,
    status,
    finished,
    childModelReady,
    terminal,
    waitForTerminal: (deferred: Deferred.Deferred<ProviderAdapter.ProviderAdapterV2Event>) => {
      terminalBarrier = deferred;
    },
    trackChildSettled: (id: string, deferred: Deferred.Deferred<void>) =>
      childSettled.set(id, deferred),
    watchClosed,
    close: Scope.close(sessionScope, Exit.void),
  };
});

describe("Droid adapter", () => {
  it.effect("projects reported native child effort without inheriting parent options", () =>
    provide(
      Effect.gen(function* () {
        const f = yield* lifecycleFixture(false, false, false, false, false, false);
        yield* Effect.promise(() =>
          NodeFSP.writeFile(f.file.replace(/\.jsonl$/u, ".settings.json"), "{}"),
        );
        yield* f.fake.emitNotification({
          type: "create_message",
          message: {
            id: "effort-parent",
            role: "assistant",
            content: [
              {
                type: "tool_use",
                id: "lifecycle-task",
                name: "Task",
                input: { description: "Lifecycle child", prompt: "Do work", complexity: "medium" },
              },
            ],
          },
        });
        yield* f.barrier;
        yield* f.register();
        assert.equal(f.status()?.model, null);
        assert.equal(f.status()?.modelSelection?.model, f.input.modelSelection.model);
        assert.deepEqual(f.status()?.modelSelection?.options, [
          { id: "reasoning_effort", value: "medium" },
        ]);
        yield* Effect.promise(() =>
          NodeFSP.writeFile(
            f.file.replace(/\.jsonl$/u, ".settings.json"),
            '{"model":"custom:GPT-6 Luna-12"}',
          ),
        );
        f.listeners[0]!(
          "change",
          f.file
            .replace(/\.jsonl$/u, ".settings.json")
            .split("/")
            .at(-1)!,
        );
        yield* Deferred.await(f.childModelReady);

        const task = f.status();
        assert.isDefined(task);
        assert.equal(task.model, "GPT-6 Luna");
        assert.equal(task.effort, "medium");
        assert.equal(task.modelSelection?.model, "GPT-6 Luna");
        assert.deepEqual(task.modelSelection?.options, [
          { id: "reasoning_effort", value: "medium" },
        ]);
      }),
    ),
  );

  it.effect.each(["completed", "failed"])(
    "drains final child disk rows on a %s Task result when the watch callback is suppressed",
    (status) =>
      provide(
        Effect.gen(function* () {
          const f = yield* lifecycleFixture();
          yield* f.append(
            f.text("final", "final child text") +
              f.row({
                type: "message",
                id: "tool",
                message: {
                  role: "assistant",
                  content: [
                    {
                      type: "tool_use",
                      id: "final-tool",
                      name: "Execute",
                      input: { command: "synthetic" },
                    },
                  ],
                },
              }) +
              f.row({
                type: "message",
                message: {
                  role: "user",
                  content: [
                    { type: "tool_result", tool_use_id: "final-tool", content: "final output" },
                  ],
                },
              }) +
              f.row({ type: "agent_turn_outcome", reason: "completed" }),
          );
          yield* f.fake.emitNotification({
            type: "tool_result",
            toolUseId: "lifecycle-task",
            content: "parent result",
            isError: status === "failed",
          });
          yield* f.barrier;
          assert.isTrue(
            f.events.some(
              (event) =>
                event.type === "message.updated" && event.message.text === "final child text",
            ),
          );
          assert.equal(f.status()?.status, status);
          assert.equal(f.status()?.result, "parent result");
          assert.isTrue(
            f.events.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.output === "final output",
            ),
          );
          assert.isTrue(f.watchers[0]?.closed);
        }),
      ),
  );

  it.effect("parks parent completion until the later disk outcome and releases the watcher", () =>
    provide(
      Effect.gen(function* () {
        const f = yield* lifecycleFixture();
        yield* f.complete();
        assert.equal(f.status()?.status, "running");
        assert.isFalse(f.watchers[0]!.closed);
        yield* f.append(
          f.text("late", "late text") + f.row({ type: "agent_turn_outcome", reason: "completed" }),
        );
        f.listeners[0]!("change", f.file.split("/").at(-1)!);
        yield* Deferred.await(f.finished);
        assert.equal(f.status()?.result, "parent result");
        // A parent barrier waits for settlement's roster publication and cleanup.
        yield* f.barrier;
        assert.isTrue(f.watchers[0]!.closed);
        assert.equal(f.watchers[0]!.listenerCount("error"), 0);
      }),
    ),
  );

  it.effect.each(["completed", "failed"])(
    "applies parked %s after asynchronous watcher failure",
    (reason) =>
      provide(
        Effect.gen(function* () {
          const f = yield* lifecycleFixture();
          yield* f.complete(reason);
          yield* f.append(
            f.text("failure-drain", "disk result") +
              f.row({ type: "agent_turn_outcome", reason: "completed" }),
          );
          f.watchers[0]!.emit("error", new Error("synthetic failure"));
          yield* Deferred.await(f.finished);
          assert.equal(f.status()?.status, reason);
          assert.equal(f.status()?.result, "parent result");
          assert.isTrue(f.watchers[0]!.closed);
          assert.equal(f.watchers[0]!.listenerCount("error"), 0);
        }),
      ),
  );

  it.effect("clears the parent roster before a transcript-only background child completion", () =>
    provide(
      Effect.gen(function* () {
        const childId = "43a7fb2d-79a1-4c49-a8d1-3b089cf7d202";
        const f = yield* lifecycleFixture();
        yield* f.fake.emitNotification({
          type: "tool_result",
          toolUseId: "lifecycle-task",
          content: "Task launched in background.\nTask ID: lifecycle-task",
        });
        yield* f.barrier;
        yield* f.fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: f.input.message.messageId,
        });
        // No parent notification: only the child's transcript outcome settles it.
        yield* f.append(
          f.text("late", "late text") + f.row({ type: "agent_turn_outcome", reason: "completed" }),
        );
        f.listeners[0]!("change", f.file.split("/").at(-1)!);
        yield* Deferred.await(f.finished);
        yield* Deferred.await(f.terminal);
        const rosterAdded = f.events.findIndex(
          (event) =>
            event.type === "provider_thread.updated" &&
            (event.providerThread.pendingBackgroundTasks ?? []).some(
              (task) => task.taskId === childId,
            ),
        );
        const rosterCleared = f.events.findIndex(
          (event, index) =>
            index > rosterAdded &&
            event.type === "provider_thread.updated" &&
            event.providerThread.pendingBackgroundTasks?.length === 0,
        );
        const childCompleted = f.events.findIndex(
          (event) =>
            event.type === "subagent.updated" &&
            event.subagent.nativeTaskRef?.nativeId === childId &&
            event.subagent.status === "completed",
        );
        assert.isAbove(rosterAdded, -1);
        assert.isAbove(childCompleted, -1);
        // The consumer releases ingestion on the last settled child event, so the
        // roster clear must already be queued before that event.
        assert.isAbove(rosterCleared, rosterAdded);
        assert.isBelow(rosterCleared, childCompleted);
      }),
    ),
  );

  it.effect("preserves the drained activity offset when watcher failure settles parked work", () =>
    provide(
      Effect.gen(function* () {
        const f = yield* lifecycleFixture();
        yield* f.complete();
        assert.equal(f.status()?.status, "running");
        yield* f.append(
          f.text("parked-final", "parked transcript result") +
            f.row({ type: "agent_turn_outcome", reason: "failed" }),
        );
        f.watchers[0]!.emit("error", new Error("synthetic failure"));
        yield* Deferred.await(f.finished);
        assert.equal(f.status()?.status, "completed");

        yield* f.register("resumed-task");
        yield* f.barrier;
        assert.equal(f.status()?.status, "running");
        const resumedAt = yield* DateTime.now;
        yield* f.append(
          f.user(DateTime.formatIso(resumedAt)) +
            f.text("fresh-resumed-final", "fresh resumed result") +
            f.row({ type: "agent_turn_outcome", reason: "completed" }),
        );
        f.listeners[1]!("change", f.file.split("/").at(-1)!);
        yield* f.complete();
        assert.equal(f.status()?.status, "completed");
        assert.equal(f.status()?.result, "parent result");
        assert.isTrue(
          f.events.some(
            (event) =>
              event.type === "message.updated" && event.message.text === "fresh resumed result",
          ),
        );
      }),
    ),
  );

  it.effect("settles parked child work and clears its roster on breaker connection failure", () =>
    provide(
      Effect.gen(function* () {
        const f = yield* lifecycleFixture(false, false, false, true);
        yield* f.register();
        yield* f.fake.emitNotification({
          type: "tool_result",
          toolUseId: "lifecycle-task",
          content: "Task launched in background.\nTask ID: lifecycle-child",
        });
        yield* f.fake.emitRequest("background-child-barrier", "droid.unknown_barrier", {});
        yield* f.fake.takeReply;
        yield* f.append(f.text("forced-close-final", "drained before forced close"));
        yield* f.complete();
        assert.equal(f.status()?.status, "running");
        for (const id of ["parked-breaker-one", "parked-breaker-two"]) {
          yield* f.fake.emitNotification({
            type: "create_message",
            message: {
              id: `${id}-message`,
              role: "assistant",
              content: [{ type: "tool_use", id, name: "ToolSearch", input: {} }],
            },
          });
          yield* f.fake.emitNotification({
            type: "tool_result",
            toolUseId: id,
            content: "Error: Invalid ToolSearch input: query: Required",
            isError: true,
          });
        }
        yield* f.fake.takeRequest("droid.interrupt_session");
        const result = yield* Deferred.await(f.terminal);
        assert.equal(
          result.type === "turn.terminal" ? result.threadDisposition : undefined,
          "broken",
        );
        assert.equal(f.status()?.status, "completed");
        assert.equal(f.status()?.result, "parent result");
        assert.equal(yield* f.runtime.hasPendingBackgroundWorkForThread!(f.providerThread), false);
        assert.isTrue(
          f.events.some(
            (event) =>
              event.type === "message.updated" &&
              event.message.text === "drained before forced close",
          ),
        );
      }),
    ),
  );

  it.effect("settles a prior-turn parked child without closing a healthy follower", () =>
    provide(
      Effect.gen(function* () {
        const f = yield* lifecycleFixture(false, false, false, true, false, false);
        const priorChild = "16ea4f8c-6ad1-4cce-bd1c-b6134301fe11";
        const healthyChild = "16ea4f8c-6ad1-4cce-bd1c-b6134301fe12";
        const priorFile = f.fileFor(priorChild);
        const healthyFile = f.fileFor(healthyChild);
        yield* f.writeChild(
          priorChild,
          f.row({ type: "session_start" }) +
            f.user(DateTime.formatIso(yield* DateTime.now)) +
            f.text("prior-ready", "prior ready"),
        );
        yield* f.writeChild(
          healthyChild,
          f.row({ type: "session_start" }) +
            f.user(DateTime.formatIso(yield* DateTime.now)) +
            f.text("healthy-ready", "healthy ready"),
        );
        const settledPrior = yield* Deferred.make<void>();
        const settledHealthy = yield* Deferred.make<void>();
        f.trackChildSettled(priorChild, settledPrior);
        f.trackChildSettled(healthyChild, settledHealthy);
        const events = f.events;

        yield* f.register("prior-tool", priorChild);
        yield* f.fake.emitNotification({
          type: "tool_result",
          toolUseId: "prior-tool",
          content: `Task launched in background.\nTask ID: ${priorChild}`,
        });
        yield* f.barrier;
        yield* f.fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: f.input.message.messageId,
        });
        yield* Deferred.await(f.terminal);
        assert.equal(f.status(priorChild)?.status, "running");

        const nextInput = {
          ...f.input,
          runId: RunId.make("droid-two-child-run"),
          attemptId: RunAttemptId.make("droid-two-child-attempt"),
          message: { ...f.input.message, messageId: MessageId.make("droid-two-child-message") },
        };
        yield* f.runtime.startTurn(nextInput);
        yield* f.fake.takeRequest("droid.add_user_message");
        yield* f.register("healthy-tool", healthyChild);
        yield* f.fake.emitNotification({
          type: "tool_result",
          toolUseId: "healthy-tool",
          content: `Task launched in background.\nTask ID: ${healthyChild}`,
        });
        yield* f.barrier;
        yield* f.complete("completed", priorChild);
        assert.equal(f.status(priorChild)?.status, "running");
        yield* f.append(
          f.text("prior-final", "prior final work") +
            f.row({ type: "agent_turn_outcome", reason: "failed" }),
          priorChild,
        );
        const breakerTerminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        f.waitForTerminal(breakerTerminal);
        for (const id of ["mixed-breaker-one", "mixed-breaker-two"]) {
          yield* f.fake.emitNotification({
            type: "create_message",
            message: {
              id: `${id}-message`,
              role: "assistant",
              content: [{ type: "tool_use", id, name: "ToolSearch", input: {} }],
            },
          });
          yield* f.fake.emitNotification({
            type: "tool_result",
            toolUseId: id,
            content: "Error: Invalid ToolSearch input: query: Required",
            isError: true,
          });
        }
        yield* f.fake.takeRequest("droid.interrupt_session");
        const broken = yield* Deferred.await(breakerTerminal);
        assert.equal(
          broken.type === "turn.terminal" ? broken.threadDisposition : undefined,
          "broken",
        );
        assert.isTrue(yield* Deferred.isDone(settledPrior));
        assert.equal(f.status(priorChild)?.status, "completed");
        assert.equal(f.status(priorChild)?.result, "parent result");
        assert.isTrue(
          events.some(
            (event) =>
              event.type === "message.updated" && event.message.text === "prior final work",
          ),
        );
        assert.isTrue(f.watchers[0]!.closed);
        assert.isFalse(f.watchers[1]!.closed);
        assert.equal(f.status(healthyChild)?.status, "running");
        assert.isTrue(yield* f.runtime.hasPendingBackgroundWorkForThread!(f.providerThread));
        const latestRoster = f.events
          .flatMap((event) =>
            event.type === "provider_thread.updated" ? [event.providerThread] : [],
          )
          .at(-1)?.pendingBackgroundTasks;
        assert.isTrue(latestRoster?.some((task) => task.taskId === healthyChild));
        assert.isFalse(latestRoster?.some((task) => task.taskId === priorChild));

        yield* f.append(
          f.user(DateTime.formatIso(yield* DateTime.now)) +
            f.text("healthy-final", "healthy fresh result") +
            f.row({ type: "agent_turn_outcome", reason: "completed" }),
          healthyChild,
        );
        f.listeners[1]!("change", healthyFile.split("/").at(-1)!);
        yield* Deferred.await(settledHealthy);
        assert.equal(f.status(healthyChild)?.status, "completed");
        assert.equal(f.status(healthyChild)?.result, "healthy fresh result");
        assert.isTrue(f.watchers[1]!.closed);
        assert.equal(yield* f.runtime.hasPendingBackgroundWorkForThread!(f.providerThread), false);

        const resumedInput = {
          ...f.input,
          runId: RunId.make("droid-resume-prior-child-run"),
          attemptId: RunAttemptId.make("droid-resume-prior-child-attempt"),
          message: {
            ...f.input.message,
            messageId: MessageId.make("droid-resume-prior-child-message"),
          },
        };
        yield* f.runtime.startTurn(resumedInput);
        yield* f.fake.takeRequest("droid.add_user_message");
        yield* f.fake.emitNotification({
          type: "create_message",
          message: {
            id: "resume-prior-child",
            role: "assistant",
            content: [
              {
                type: "tool_use",
                id: "resume-prior-child-tool",
                name: "Task",
                input: { resume: priorChild, prompt: "continue previous task" },
              },
            ],
          },
        });
        yield* f.fake.emitNotification({
          type: "tool_result",
          toolUseId: "resume-prior-child-tool",
          content: `Task launched in background.\nTask ID: ${priorChild}`,
        });
        yield* f.barrier;
        assert.equal(f.status(priorChild)?.status, "running");
        assert.isTrue(yield* f.runtime.hasPendingBackgroundWorkForThread!(f.providerThread));
        const resumedSettled = yield* Deferred.make<void>();
        f.trackChildSettled(priorChild, resumedSettled);
        const resumedAt = yield* DateTime.now;
        yield* f.append(
          f.user(DateTime.formatIso(resumedAt)) +
            f.text("prior-resumed-final", "prior resumed fresh result") +
            f.row({ type: "agent_turn_outcome", reason: "completed" }),
          priorChild,
        );
        f.listeners[2]!("change", priorFile.split("/").at(-1)!);
        yield* Deferred.await(resumedSettled);
        assert.equal(f.status(priorChild)?.status, "completed");
        assert.equal(f.status(priorChild)?.result, "prior resumed fresh result");
        assert.equal(yield* f.runtime.hasPendingBackgroundWorkForThread!(f.providerThread), false);

        // Keep the file reference live so the per-child fixture paths are verified as distinct.
        assert.notEqual(priorFile, healthyFile);
      }),
    ),
  );

  it.effect.each(["completed", "failed"])(
    "applies actual parent %s after watch open failure",
    (status) =>
      provide(
        Effect.gen(function* () {
          const f = yield* lifecycleFixture(true);
          assert.equal(f.status()?.status, "running");
          yield* f.complete(status);
          assert.equal(f.status()?.status, status);
          assert.equal(f.status()?.result, "parent result");
        }),
      ),
  );

  it.effect("does not resurrect a follower that completes during its initial read", () =>
    provide(
      Effect.gen(function* () {
        const f = yield* lifecycleFixture(false, true);
        yield* Deferred.await(f.finished);
        yield* f.complete();
        assert.equal(f.status()?.status, "completed");
        assert.isTrue(f.watchers[0]!.closed);
        assert.equal(f.watchers[0]!.listenerCount("error"), 0);
      }),
    ),
  );

  it.effect("does not resurrect follower ownership after synchronous startup failure", () =>
    provide(
      Effect.gen(function* () {
        const f = yield* lifecycleFixture(false, false, true);
        yield* Deferred.await(f.watchClosed);
        yield* f.complete();
        assert.equal(f.status()?.status, "completed");
        assert.equal(f.status()?.result, "parent result");
        assert.equal(f.watchers[0]!.listenerCount("error"), 0);
      }),
    ),
  );

  it.effect("resume ignores old outcome and stale watcher callbacks", () =>
    provide(
      Effect.gen(function* () {
        const f = yield* lifecycleFixture(false, true);
        yield* Deferred.await(f.finished);
        const startedAt = f.status()?.startedAt;
        yield* TestClock.adjust("1 second");
        yield* f.register("resumed-task");
        assert.equal(f.status()?.startedAt, startedAt);
        yield* f.complete();
        assert.equal(f.status()?.status, "running");
        assert.isTrue(f.watchers[0]!.closed);
        assert.equal(f.watchers[0]!.listenerCount("error"), 0);
        f.listeners[0]!("change", f.file.split("/").at(-1)!);
        yield* f.barrier;
        assert.equal(f.status()?.status, "running");
        const resumedAt = yield* DateTime.now;
        yield* f.append(
          f.user(DateTime.formatIso(resumedAt)) +
            f.text("resumed-final", "resumed result") +
            f.row({ type: "agent_turn_outcome", reason: "completed" }),
        );
        yield* f.complete();
        assert.equal(f.status()?.status, "completed");
        assert.isTrue(
          f.events.some(
            (event) => event.type === "message.updated" && event.message.text === "resumed result",
          ),
        );
        assert.isTrue(f.watchers[1]!.closed);
      }),
    ),
  );

  it.effect("accepts resumed transcript activity written before its notification", () =>
    provide(
      Effect.gen(function* () {
        const f = yield* lifecycleFixture(false, true);
        yield* Deferred.await(f.finished);
        const startedAt = f.status()?.startedAt;
        const resumedAt = yield* DateTime.now;
        yield* f.append(
          f.user(DateTime.formatIso(resumedAt)) +
            f.text("resumed-before-register", "resumed before register") +
            f.row({ type: "agent_turn_outcome", reason: "completed" }),
        );
        yield* TestClock.adjust("1 second");
        yield* f.register("resumed-task");
        yield* f.complete();
        assert.equal(f.status()?.status, "completed");
        assert.equal(f.status()?.result, "parent result");
        assert.equal(f.status()?.startedAt, startedAt);
        assert.isTrue(
          f.events.some(
            (event) =>
              event.type === "message.updated" && event.message.text === "resumed before register",
          ),
        );
        assert.isTrue(f.watchers[1]!.closed);
      }),
    ),
  );

  it.effect(
    "settles a valid disk terminal found during watcher failure without parent completion",
    () =>
      provide(
        Effect.gen(function* () {
          const f = yield* lifecycleFixture();
          yield* f.append(
            f.text("watch-failure-final", "disk terminal result") +
              f.row({ type: "agent_turn_outcome", reason: "completed" }),
          );
          f.watchers[0]!.emit("error", new Error("synthetic failure"));
          yield* Deferred.await(f.finished);
          assert.equal(f.status()?.status, "completed");
          assert.equal(f.status()?.result, "disk terminal result");
          assert.isTrue(f.watchers[0]!.closed);
        }),
      ),
  );

  it.effect(
    "replacing an active follower keeps its activity cutoff and closes the old watcher",
    () =>
      provide(
        Effect.gen(function* () {
          const f = yield* lifecycleFixture();
          yield* TestClock.adjust("1 second");
          yield* f.register("replacement-task");
          assert.isTrue(f.watchers[0]!.closed);
          yield* f.append(
            f.text("active-final", "active result") +
              f.row({ type: "agent_turn_outcome", reason: "completed" }),
          );
          f.listeners[0]!("change", f.file.split("/").at(-1)!);
          yield* f.complete();
          assert.equal(f.status()?.status, "completed");
          assert.isTrue(f.watchers[1]!.closed);
        }),
      ),
  );

  it.effect("watcher failure without a parent terminal does not fabricate completion", () =>
    provide(
      Effect.gen(function* () {
        const f = yield* lifecycleFixture();
        const closed = yield* Deferred.make<void>();
        const watcher = f.watchers[0]!;
        watcher.close = () => {
          watcher.closed = true;
          Deferred.doneUnsafe(closed, Effect.void);
        };
        watcher.emit("error", new Error("synthetic failure"));
        yield* Deferred.await(closed);
        assert.equal(f.status()?.status, "running");
        yield* f.complete("failed");
        assert.equal(f.status()?.status, "failed");
        assert.equal(watcher.listenerCount("error"), 0);
      }),
    ),
  );

  it.effect.each(["cancel", "close", "process exit"])(
    "drains and releases the child watcher on %s",
    (operation) =>
      provide(
        Effect.gen(function* () {
          const f = yield* lifecycleFixture();
          yield* f.append(f.text("cleanup-final", "cleanup text"));
          if (operation === "cancel") {
            yield* f.fake.emitNotification({
              type: "create_message",
              message: {
                id: "stop",
                role: "assistant",
                content: [
                  {
                    type: "tool_use",
                    id: "stop-tool",
                    name: "TaskStop",
                    input: { task_id: "43a7fb2d-79a1-4c49-a8d1-3b089cf7d202" },
                  },
                ],
              },
            });
            yield* f.fake.emitNotification({
              type: "tool_result",
              toolUseId: "stop-tool",
              content: "stopped",
            });
            yield* f.barrier;
          } else if (operation === "close") {
            yield* f.close;
            yield* Deferred.await(f.finished);
          } else {
            yield* f.fake.closeStdout;
            yield* Deferred.await(f.finished);
          }
          assert.equal(f.status()?.status, operation === "process exit" ? "failed" : "cancelled");
          assert.isTrue(
            f.events.some(
              (event) => event.type === "message.updated" && event.message.text === "cleanup text",
            ),
          );
          assert.isTrue(f.watchers[0]!.closed);
          assert.equal(f.watchers[0]!.listenerCount("error"), 0);
        }),
      ),
  );
  it.effect("interrupts before add_user_message is answered and keeps the runtime usable", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ holdAddUserMessageResponse: true });
        const { runtime, providerThread, input } = yield* open(fake);
        const terminals: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
        const firstTerminal = yield* Deferred.make<void>();
        const secondTerminal = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            if (event.type !== "turn.terminal") return Effect.void;
            terminals.push(event);
            return event.status === "completed"
              ? Deferred.succeed(secondTerminal, undefined)
              : Deferred.succeed(firstTerminal, undefined);
          }),
          Effect.forkChild,
        );
        const start = yield* runtime.startTurn(input).pipe(Effect.forkChild);
        yield* fake.takeRequest("droid.add_user_message");
        const turn = (yield* runtime.readThreadSnapshot({ providerThread })).providerTurns[0];
        assert.isDefined(turn);
        yield* runtime.interruptTurn({ providerThread, providerTurnId: turn.id });
        yield* fake.takeRequest("droid.interrupt_session");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "cancelled",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(firstTerminal);
        assert.equal(
          terminals[0]?.type === "turn.terminal" ? terminals[0].status : undefined,
          "interrupted",
        );
        yield* fake.answerAddUserMessage;
        yield* Fiber.join(start);

        yield* runtime.startTurn({
          ...input,
          runId: RunId.make("droid-adapter-following-run"),
          attemptId: RunAttemptId.make("droid-adapter-following-attempt"),
          message: {
            ...input.message,
            messageId: MessageId.make("droid-adapter-following-message"),
          },
        });
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: "droid-adapter-following-message",
        });
        yield* Deferred.await(secondTerminal);
      }),
    ),
  );

  it.effect("does not stop after the first malformed ToolSearch call", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const terminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        const secondTerminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        let terminalCount = 0;
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            if (event.type !== "turn.terminal") return Effect.void;
            terminalCount++;
            return terminalCount === 1
              ? Deferred.succeed(terminal, event)
              : Deferred.succeed(secondTerminal, event);
          }),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "tool-search-first",
            role: "assistant",
            content: [
              { type: "tool_use", id: "tool-search-first-id", name: "ToolSearch", input: {} },
            ],
          },
        });
        yield* fake.emitNotification({
          type: "tool_result",
          toolUseId: "tool-search-first-id",
          content: "Error: Invalid ToolSearch input: query: Required",
          isError: true,
        });
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        const result = yield* Deferred.await(terminal);
        assert.equal(result.type === "turn.terminal" ? result.status : undefined, "completed");
        assert.equal(
          fake.requestedMethods.filter((method) => method === "droid.interrupt_session").length,
          0,
        );
        yield* runtime.startTurn({
          ...input,
          runId: RunId.make("droid-adapter-second-tool-search-run"),
          attemptId: RunAttemptId.make("droid-adapter-second-tool-search-attempt"),
          message: {
            ...input.message,
            messageId: MessageId.make("droid-adapter-second-tool-search-message"),
          },
        });
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          turnId: input.message.messageId,
          type: "create_message",
          message: {
            id: "late-first-turn-message",
            role: "assistant",
            content: [
              { type: "tool_use", id: "late-first-turn-tool", name: "ToolSearch", input: {} },
            ],
          },
        });
        yield* fake.emitNotification({
          turnId: input.message.messageId,
          type: "tool_result",
          toolUseId: "late-first-turn-tool",
          content: "Error: Invalid ToolSearch input: query: Required",
          isError: true,
        });
        const secondTurnFailure = (id: string) =>
          Effect.gen(function* () {
            yield* fake.emitNotification({
              type: "create_message",
              message: {
                id: `${id}-message`,
                role: "assistant",
                content: [{ type: "tool_use", id, name: "ToolSearch", input: {} }],
              },
            });
            yield* fake.emitNotification({
              type: "tool_result",
              toolUseId: id,
              content: "Error: Invalid ToolSearch input: query: Required",
              isError: true,
            });
          });
        yield* secondTurnFailure("second-turn-error-one");
        assert.equal(
          fake.requestedMethods.filter((method) => method === "droid.interrupt_session").length,
          0,
        );
        yield* secondTurnFailure("second-turn-error-two");
        yield* fake.takeRequest("droid.interrupt_session");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "cancelled",
          turnId: MessageId.make("droid-adapter-second-tool-search-message"),
        });
        const secondResult = yield* Deferred.await(secondTerminal);
        assert.equal(
          secondResult.type === "turn.terminal" ? secondResult.status : undefined,
          "failed",
        );
      }),
    ),
  );

  it.effect("fails the active turn once after two malformed ToolSearch calls", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input, providerThread } = yield* open(fake);
        const terminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        const events: ProviderAdapter.ProviderAdapterV2Event[] = [];
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            events.push(event);
            return event.type === "turn.terminal" ? Deferred.succeed(terminal, event) : Effect.void;
          }),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        const malformedSearch = (id: string) =>
          Effect.gen(function* () {
            yield* fake.emitNotification({
              type: "create_message",
              message: {
                id: `${id}-message`,
                role: "assistant",
                content: [{ type: "tool_use", id, name: "ToolSearch", input: {} }],
              },
            });
            yield* fake.emitNotification({
              type: "tool_result",
              toolUseId: id,
              content: "Error: Invalid ToolSearch input: query: Required",
              isError: true,
            });
          });
        const toolResult = (id: string, name: string, input: unknown, content: string) =>
          Effect.gen(function* () {
            yield* fake.emitNotification({
              type: "create_message",
              message: {
                id: `${id}-message`,
                role: "assistant",
                content: [{ type: "tool_use", id, name, input }],
              },
            });
            yield* fake.emitNotification({
              type: "tool_result",
              toolUseId: id,
              content,
              isError: true,
            });
          });
        yield* fake.emitNotification({
          type: "create_message",
          message: { id: "malformed-message", role: "assistant", content: "not an array" },
        });
        yield* fake.emitNotification(
          {
            turnId: input.message.messageId,
            type: "create_message",
            message: {
              id: "stale-search-message",
              role: "assistant",
              content: [{ type: "tool_use", id: "stale-search", name: "ToolSearch", input: {} }],
            },
          },
          "child-native-session",
        );
        yield* fake.emitNotification(
          {
            turnId: input.message.messageId,
            type: "tool_result",
            toolUseId: "stale-search",
            content: "Error: Invalid ToolSearch input: query: Required",
            isError: true,
          },
          "child-native-session",
        );
        yield* toolResult(
          "valid-search",
          "ToolSearch",
          { query: "select:delegate_task" },
          "Already loaded.",
        );
        yield* toolResult("non-matching-search", "ToolSearch", {}, "Already loaded.");
        yield* toolResult(
          "unknown-tool",
          "UnknownTool",
          {},
          "Error: Invalid ToolSearch input: query: Required",
        );
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "malformed-tool-call-notice",
            role: "assistant",
            content: [
              { type: "tool_use", id: "malformed-tool-call", name: "ToolSearch", input: {} },
            ],
          },
        });
        yield* fake.emitNotification({
          type: "tool_result",
          toolUseId: "malformed-tool-call",
          content: "Error: Invalid ToolSearch input: query: Required",
          isError: false,
        });
        yield* malformedSearch("tool-search-one");
        yield* fake.emitNotification({
          type: "tool_result",
          toolUseId: "tool-search-one",
          content: "Error: Invalid ToolSearch input: query: Required",
          isError: true,
        });
        assert.equal(
          fake.requestedMethods.filter((method) => method === "droid.interrupt_session").length,
          0,
        );
        yield* malformedSearch("tool-search-two");
        const interrupt = yield* fake.takeRequest("droid.interrupt_session");
        assert.isDefined(interrupt);
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "cancelled",
          turnId: input.message.messageId,
        });
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "after-break-message",
            role: "assistant",
            content: [
              { type: "tool_use", id: "after-break-tool", name: "ShouldNotRun", input: {} },
            ],
          },
        });
        const result = yield* Deferred.await(terminal);
        assert.equal(result.type === "turn.terminal" ? result.status : undefined, "failed");
        assert.equal(
          result.type === "turn.terminal" ? result.failure?.retryable : undefined,
          false,
        );
        assert.equal(
          result.type === "turn.terminal" ? result.failure?.message : undefined,
          "Droid repeated a ToolSearch call without its required query. The turn was stopped.",
        );
        assert.equal(
          fake.requestedMethods.filter((method) => method === "droid.interrupt_session").length,
          1,
        );
        assert.equal(
          fake.requestedMethods.filter((method) => method === "droid.add_user_message").length,
          1,
        );
        assert.equal(
          events.filter(
            (event) =>
              event.type === "provider_turn.updated" &&
              event.providerTurn.status === "failed" &&
              event.providerTurn.providerThreadId === providerThread.id &&
              event.providerTurn.runAttemptId === input.attemptId,
          ).length,
          1,
        );
        assert.isFalse(
          events.some(
            (event) =>
              event.type === "turn_item.updated" && event.turnItem.title === "ShouldNotRun",
          ),
        );
        assert.equal(events.filter((event) => event.type === "turn.terminal").length, 1);
      }),
    ),
  );

  it.effect("waits for native completion after an interrupt acknowledgment", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ holdInterruptSessionResponse: true });
        const { runtime, input } = yield* open(fake);
        const terminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        const events: ProviderAdapter.ProviderAdapterV2Event[] = [];
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            events.push(event);
            return event.type === "turn.terminal" ? Deferred.succeed(terminal, event) : Effect.void;
          }),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        for (const id of ["completion-wait-one", "completion-wait-two"]) {
          yield* fake.emitNotification({
            type: "create_message",
            message: {
              id: `${id}-message`,
              role: "assistant",
              content: [{ type: "tool_use", id, name: "ToolSearch", input: {} }],
            },
          });
          yield* fake.emitNotification({
            type: "tool_result",
            toolUseId: id,
            content: "Error: Invalid ToolSearch input: query: Required",
            isError: true,
          });
        }
        yield* fake.takeRequest("droid.interrupt_session");
        assert.isFalse(yield* Deferred.isDone(terminal));
        yield* fake.answerInterruptSession;
        yield* Effect.yieldNow;
        yield* Effect.yieldNow;
        yield* TestClock.adjust("1 second");
        assert.isFalse(yield* Deferred.isDone(terminal));
        const blockedRestart = yield* runtime
          .startTurn({
            ...input,
            runId: RunId.make("droid-blocked-restart-run"),
            attemptId: RunAttemptId.make("droid-blocked-restart-attempt"),
            message: {
              ...input.message,
              messageId: MessageId.make("droid-blocked-restart-message"),
            },
          })
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(blockedRestart));
        assert.isFalse(yield* Deferred.isDone(terminal));
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "residual-after-interrupt-ack",
            role: "assistant",
            content: [{ type: "text", text: "must not be attributed" }],
          },
        });
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "cancelled",
          turnId: "mismatched-turn-id",
        });
        assert.isFalse(yield* Deferred.isDone(terminal));
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "cancelled",
          turnId: input.message.messageId,
        });
        const result = yield* Deferred.await(terminal);
        assert.equal(result.type === "turn.terminal" ? result.status : undefined, "failed");
        assert.equal(
          result.type === "turn.terminal" ? result.threadDisposition : undefined,
          "reusable",
        );
        assert.equal(
          result.type === "turn.terminal" ? result.failure?.message : undefined,
          "Droid repeated a ToolSearch call without its required query. The turn was stopped.",
        );
        assert.equal(
          result.type === "turn.terminal" ? result.failure?.retryable : undefined,
          false,
        );
        assert.equal(events.filter((event) => event.type === "turn.terminal").length, 1);
        assert.isFalse(
          events.some(
            (event) =>
              event.type === "message.updated" && event.message.text === "must not be attributed",
          ),
        );
      }),
    ),
  );

  it.effect("keeps the breaker as terminal owner when a restart is rejected", () =>
    provide(
      Effect.gen(function* () {
        const f = yield* lifecycleFixture();
        const childId = "43a7fb2d-79a1-4c49-a8d1-3b089cf7d202";
        yield* f.register();
        yield* f.fake.emitNotification({
          type: "tool_result",
          toolUseId: "lifecycle-task",
          content: "Task launched in background.\nTask ID: 43a7fb2d-79a1-4c49-a8d1-3b089cf7d202",
        });
        yield* f.barrier;
        for (const id of ["owner-restart-one", "owner-restart-two"]) {
          yield* f.fake.emitNotification({
            type: "create_message",
            message: {
              id: `${id}-message`,
              role: "assistant",
              content: [{ type: "tool_use", id, name: "ToolSearch", input: {} }],
            },
          });
          yield* f.fake.emitNotification({
            type: "tool_result",
            toolUseId: id,
            content: "Error: Invalid ToolSearch input: query: Required",
            isError: true,
          });
        }
        yield* f.fake.takeRequest("droid.interrupt_session");
        const restart = yield* f.runtime
          .startTurn({
            ...f.input,
            runId: RunId.make("droid-owner-restart-run"),
            attemptId: RunAttemptId.make("droid-owner-restart-attempt"),
            message: {
              ...f.input.message,
              messageId: MessageId.make("droid-owner-restart-message"),
            },
          })
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(restart));
        const compactThread = f.runtime.compactThread;
        assert.isDefined(compactThread);
        if (compactThread === undefined) return;
        const compact = yield* compactThread({
          ...f.input,
          runId: RunId.make("droid-owner-compact-run"),
          attemptId: RunAttemptId.make("droid-owner-compact-attempt"),
          message: {
            ...f.input.message,
            messageId: MessageId.make("droid-owner-compact-message"),
          },
        }).pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(compact));
        yield* f.append(f.row({ type: "agent_turn_outcome", reason: "completed" }));
        yield* f.complete("completed", childId, false);
        yield* Deferred.await(f.finished);
        assert.isFalse(yield* Deferred.isDone(f.terminal));
        yield* f.fake.emitNotification({
          type: "agent_turn_completed",
          reason: "cancelled",
          turnId: "wrong-turn-owner-restart",
        });
        assert.isFalse(yield* Deferred.isDone(f.terminal));
        const activeTurn = (yield* f.runtime.readThreadSnapshot({
          providerThread: f.providerThread,
        })).providerTurns.find((turn) => turn.runAttemptId === f.input.attemptId);
        assert.equal(activeTurn?.status, "running");
        yield* f.fake.emitNotification({
          type: "agent_turn_completed",
          reason: "cancelled",
          turnId: f.input.message.messageId,
        });
        const original = yield* Deferred.await(f.terminal);
        assert.equal(
          original.type === "turn.terminal" ? original.failure?.message : undefined,
          "Droid repeated a ToolSearch call without its required query. The turn was stopped.",
        );
        assert.equal(
          original.type === "turn.terminal" ? original.failure?.retryable : undefined,
          false,
        );
        assert.equal(
          original.type === "turn.terminal" ? original.threadDisposition : undefined,
          "reusable",
        );
        assert.equal(f.events.filter((event) => event.type === "turn.terminal").length, 1);
      }),
    ),
  );

  it.effect("does not send a competing interrupt while the breaker owns stopping", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ holdInterruptSessionResponse: true });
        const { runtime, input, providerThread } = yield* open(fake);
        const terminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal" ? Deferred.succeed(terminal, event) : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        for (const id of ["owner-interrupt-one", "owner-interrupt-two"]) {
          yield* fake.emitNotification({
            type: "create_message",
            message: {
              id: `${id}-message`,
              role: "assistant",
              content: [{ type: "tool_use", id, name: "ToolSearch", input: {} }],
            },
          });
          yield* fake.emitNotification({
            type: "tool_result",
            toolUseId: id,
            content: "Error: Invalid ToolSearch input: query: Required",
            isError: true,
          });
        }
        yield* fake.takeRequest("droid.interrupt_session");
        const activeTurn = (yield* runtime.readThreadSnapshot({ providerThread })).providerTurns.at(
          -1,
        );
        assert.isDefined(activeTurn);
        yield* runtime.interruptTurn({
          providerThread,
          providerTurnId: activeTurn.id,
        });
        const steer = yield* runtime
          .steerTurn({
            threadId,
            runId: input.runId,
            providerThread,
            providerTurnId: activeTurn.id,
            message: {
              ...input.message,
              messageId: MessageId.make("droid-stopped-steer-message"),
              text: "do not send this while stopping",
            },
          })
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(steer));
        assert.equal(
          fake.requestedMethods.filter((method) => method === "droid.interrupt_session").length,
          1,
        );
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "cancelled",
          turnId: input.message.messageId,
        });
        yield* fake.answerInterruptSession;
        const ended = yield* Deferred.await(terminal);
        assert.equal(
          ended.type === "turn.terminal" ? ended.failure?.message : undefined,
          "Droid repeated a ToolSearch call without its required query. The turn was stopped.",
        );
        assert.equal(
          fake.requestedMethods.filter((method) => method === "droid.interrupt_session").length,
          1,
        );
      }),
    ),
  );

  it.effect("keeps breaker failure identity when the notification stream fails", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const terminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal" ? Deferred.succeed(terminal, event) : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        for (const id of ["stream-owner-one", "stream-owner-two"]) {
          yield* fake.emitNotification({
            type: "create_message",
            message: {
              id: `${id}-message`,
              role: "assistant",
              content: [{ type: "tool_use", id, name: "ToolSearch", input: {} }],
            },
          });
          yield* fake.emitNotification({
            type: "tool_result",
            toolUseId: id,
            content: "Error: Invalid ToolSearch input: query: Required",
            isError: true,
          });
        }
        yield* fake.takeRequest("droid.interrupt_session");
        yield* fake.closeStdout;
        const result = yield* Deferred.await(terminal);
        assert.equal(result.type === "turn.terminal" ? result.status : undefined, "failed");
        assert.equal(
          result.type === "turn.terminal" ? result.threadDisposition : undefined,
          "broken",
        );
        assert.equal(
          result.type === "turn.terminal" ? result.failure?.message : undefined,
          "Droid repeated a ToolSearch call without its required query. The turn was stopped.",
        );
        assert.equal(
          result.type === "turn.terminal" ? result.failure?.retryable : undefined,
          false,
        );
        assert.equal(
          fake.requestedMethods.filter((method) => method === "droid.interrupt_session").length,
          1,
        );
      }),
    ),
  );

  it.effect("breaks the connection when native completion never follows interrupt", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ holdInterruptSessionResponse: true });
        const { runtime, input } = yield* open(fake);
        const terminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal" ? Deferred.succeed(terminal, event) : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        for (const id of ["missing-completion-one", "missing-completion-two"]) {
          yield* fake.emitNotification({
            type: "create_message",
            message: {
              id: `${id}-message`,
              role: "assistant",
              content: [{ type: "tool_use", id, name: "ToolSearch", input: {} }],
            },
          });
          yield* fake.emitNotification({
            type: "tool_result",
            toolUseId: id,
            content: "Error: Invalid ToolSearch input: query: Required",
            isError: true,
          });
        }
        yield* fake.takeRequest("droid.interrupt_session");
        yield* fake.answerInterruptSession;
        yield* TestClock.adjust("31 seconds");
        const result = yield* Deferred.await(terminal);
        assert.equal(result.type === "turn.terminal" ? result.status : undefined, "failed");
        assert.equal(
          result.type === "turn.terminal" ? result.threadDisposition : undefined,
          "broken",
        );
        assert.equal(
          result.type === "turn.terminal" ? result.failure?.message : undefined,
          "Droid repeated a ToolSearch call without its required query. The turn was stopped.",
        );
        assert.equal(
          result.type === "turn.terminal" ? result.failure?.retryable : undefined,
          false,
        );
        assert.isTrue(fake.closed());
      }),
    ),
  );

  it.effect("counts each malformed ToolSearch call ID once across replayed pairs", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const terminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal" ? Deferred.succeed(terminal, event) : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        const malformedPair = (id: string) =>
          Effect.gen(function* () {
            yield* fake.emitNotification({
              type: "create_message",
              message: {
                id: `${id}-message`,
                role: "assistant",
                content: [{ type: "tool_use", id, name: "ToolSearch", input: {} }],
              },
            });
            yield* fake.emitNotification({
              type: "tool_result",
              toolUseId: id,
              content: "Error: Invalid ToolSearch input: query: Required",
              isError: true,
            });
          });
        yield* malformedPair("replayed-failure");
        yield* malformedPair("replayed-failure");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        const replayResult = yield* Deferred.await(terminal);
        assert.equal(
          fake.requestedMethods.filter((method) => method === "droid.interrupt_session").length,
          0,
        );
        assert.equal(
          replayResult.type === "turn.terminal" ? replayResult.status : undefined,
          "completed",
        );
      }),
    ),
  );

  it.effect("does not enroll old-turn-tagged call creation in the current turn", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const firstTerminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        const secondTerminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        const staleBarrier = yield* Deferred.make<void>();
        const events: ProviderAdapter.ProviderAdapterV2Event[] = [];
        let terminalCount = 0;
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            events.push(event);
            if (
              event.type === "turn_item.updated" &&
              event.turnItem.type === "compaction" &&
              event.turnItem.title === "Context compacted"
            )
              return Deferred.succeed(staleBarrier, undefined);
            if (event.type !== "turn.terminal") return Effect.void;
            terminalCount++;
            return terminalCount === 1
              ? Deferred.succeed(firstTerminal, event)
              : Deferred.succeed(secondTerminal, event);
          }),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(firstTerminal);
        const nextInput = {
          ...input,
          runId: RunId.make("droid-stale-call-run"),
          attemptId: RunAttemptId.make("droid-stale-call-attempt"),
          message: { ...input.message, messageId: MessageId.make("droid-stale-call-message") },
        };
        yield* runtime.startTurn(nextInput);
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "create_message",
          turnId: input.message.messageId,
          message: {
            id: "old-tagged-call",
            role: "assistant",
            content: [{ type: "tool_use", id: "old-tagged-tool", name: "ToolSearch", input: {} }],
          },
        });
        yield* fake.emitNotification({
          type: "tool_result",
          toolUseId: "old-tagged-tool",
          content: "Error: Invalid ToolSearch input: query: Required",
          isError: true,
        });
        yield* fake.emitNotification({ type: "session_compacted" });
        yield* Deferred.await(staleBarrier);
        const staleTools = events.filter(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.runId === nextInput.runId &&
            event.turnItem.title === "ToolSearch",
        );
        assert.isEmpty(staleTools);
        const addMalformed = (id: string) =>
          Effect.gen(function* () {
            yield* fake.emitNotification({
              type: "create_message",
              message: {
                id: `${id}-message`,
                role: "assistant",
                content: [{ type: "tool_use", id, name: "ToolSearch", input: {} }],
              },
            });
            yield* fake.emitNotification({
              type: "tool_result",
              toolUseId: id,
              content: "Error: Invalid ToolSearch input: query: Required",
              isError: true,
            });
          });
        yield* addMalformed("current-failure-one");
        yield* fake.emitRequest("current-failure-barrier", "droid.unknown_barrier", {});
        yield* fake.takeReply;
        assert.equal(
          fake.requestedMethods.filter((method) => method === "droid.interrupt_session").length,
          0,
        );
        yield* addMalformed("current-failure-two");
        yield* fake.takeRequest("droid.interrupt_session");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "cancelled",
          turnId: nextInput.message.messageId,
        });
        const result = yield* Deferred.await(secondTerminal);
        assert.equal(result.type === "turn.terminal" ? result.status : undefined, "failed");
        assert.isTrue(
          events.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.runId === nextInput.runId &&
              event.turnItem.title === "ToolSearch",
          ),
        );
      }),
    ),
  );

  it.effect("fails closed and closes the captured connection when interrupt fails", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ failInterruptSession: true });
        const { runtime, input } = yield* open(fake);
        const terminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal" ? Deferred.succeed(terminal, event) : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        for (const id of ["search-error-one", "search-error-two"]) {
          yield* fake.emitNotification({
            type: "create_message",
            message: {
              id: `${id}-message`,
              role: "assistant",
              content: [{ type: "tool_use", id, name: "ToolSearch", input: {} }],
            },
          });
          yield* fake.emitNotification({
            type: "tool_result",
            toolUseId: id,
            content: "Error: Invalid ToolSearch input: query: Required",
            isError: true,
          });
        }
        yield* fake.takeRequest("droid.interrupt_session");
        const result = yield* Deferred.await(terminal);
        assert.equal(result.type === "turn.terminal" ? result.status : undefined, "failed");
        assert.equal(
          result.type === "turn.terminal" ? result.threadDisposition : undefined,
          "broken",
        );
        assert.equal(
          result.type === "turn.terminal" ? result.failure?.message : undefined,
          "Droid repeated a ToolSearch call without its required query. The turn was stopped.",
        );
        assert.equal(
          result.type === "turn.terminal" ? result.failure?.retryable : undefined,
          false,
        );
        assert.isTrue(fake.closed());
      }),
    ),
  );

  it.effect("applies the breaker to a native session loaded from an existing thread", () =>
    provide(
      Effect.gen(function* () {
        const source = yield* open(yield* makeFakeDroid());
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(
          fake,
          undefined,
          policy.cwd,
          undefined,
          undefined,
          source.providerThread,
        );
        const terminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal" ? Deferred.succeed(terminal, event) : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        assert.isTrue(fake.requestedMethods.includes("droid.load_session"));
        for (const id of ["loaded-search-one", "loaded-search-two"]) {
          yield* fake.emitNotification({
            type: "create_message",
            message: {
              id: `${id}-message`,
              role: "assistant",
              content: [{ type: "tool_use", id, name: "ToolSearch", input: {} }],
            },
          });
          yield* fake.emitNotification({
            type: "tool_result",
            toolUseId: id,
            content: "Error: Invalid ToolSearch input: query: Required",
            isError: true,
          });
        }
        yield* fake.takeRequest("droid.interrupt_session");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "cancelled",
          turnId: input.message.messageId,
        });
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "cancelled",
          turnId: input.message.messageId,
        });
        const result = yield* Deferred.await(terminal);
        assert.equal(result.type === "turn.terminal" ? result.status : undefined, "failed");
      }),
    ),
  );

  it.effect("passes the T3 MCP endpoint to Droid and completes a turn that never uses it", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const config: McpProviderSessionConfig = {
          environmentId: EnvironmentId.make("droid-adapter-test-environment"),
          threadId,
          providerSessionId: "droid-adapter-test-session",
          providerInstanceId: instanceId,
          endpoint: "http://127.0.0.1:1/mcp",
          authorizationHeader: "Bearer test",
          browserToolsAvailable: false,
        };
        const { runtime, input } = yield* open(fake, undefined, undefined, config);
        const terminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal" ? Deferred.succeed(terminal, event) : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        const initialize = yield* fake.takeRequest("droid.initialize_session");
        assert.deepEqual(
          typeof initialize.params === "object" && initialize.params !== null
            ? Reflect.get(initialize.params, "mcpServers")
            : undefined,
          [
            {
              type: "http",
              name: "t3-code",
              url: config.endpoint,
              headers: [{ name: "Authorization", value: config.authorizationHeader }],
            },
          ],
        );
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        const ended = yield* Deferred.await(terminal);
        assert.equal(ended.type === "turn.terminal" ? ended.status : undefined, "completed");
      }),
    ),
  );

  it.effect("canonicalizes a symlinked workspace before initializing the session", () =>
    provide(
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fileSystem.makeTempDirectoryScoped({
          directory: NodeOS.tmpdir(),
          prefix: "droid-adapter-cwd-",
        });
        const realDirectory = path.join(root, "real");
        const symlink = path.join(root, "workspace");
        yield* fileSystem.makeDirectory(realDirectory);
        yield* fileSystem.symlink(realDirectory, symlink);
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake, undefined, symlink);
        yield* runtime.startTurn(input);
        const initialize = yield* fake.takeRequest("droid.initialize_session");
        const canonicalPath = yield* fileSystem.realPath(symlink);
        assert.equal(runtime.providerSession.cwd, canonicalPath);
        assert.equal(
          typeof initialize.params === "object" && initialize.params !== null
            ? Reflect.get(initialize.params, "cwd")
            : undefined,
          canonicalPath,
        );
      }),
    ),
  );

  it("gives Droid one way to delegate and one way to reach T3 tools, only when the T3 MCP server is attached", () => {
    const withMcp = droidSystemPromptAppend(true);
    assert.include(withMcp, "T3 Code orchestration");
    assert.include(withMcp, "Start every subagent with `delegate_task`");
    assert.include(withMcp, "select:t3-code___delegate_task");
    assert.include(withMcp, "call it instead of searching again");
    assert.include(withMcp, "only when a call to that tool returned an error");
    // Other harnesses' alternatives must not appear next to Droid's single path.
    assert.notInclude(withMcp, "Prefer native subagent tools");
    assert.notInclude(withMcp, "T3_ACP_MCP_NODE");
    assert.notInclude(withMcp, "ACP fallback");
    assert.notInclude(withMcp, "bounded direct attempt");
    assert.notInclude(withMcp, "<t3_subagents>");
    assert.notInclude(withMcp, "replaces the guidance");
    const withoutMcp = droidSystemPromptAppend(false);
    assert.notInclude(withoutMcp, "T3 Code orchestration");
    assert.notInclude(withoutMcp, "delegate_task");
    assert.include(withoutMcp, "<sign_in_links>");
  });

  it.effect("shows a completed compaction item and switches to the compacted session", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          compactSessionResult: { newSessionId: "compacted-session", removedCount: 3 },
        });
        const { runtime, input } = yield* open(fake);
        const events: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
        const terminalSeen = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            events.push(event);
            return event.type === "turn.terminal"
              ? Deferred.succeed(terminalSeen, undefined)
              : Effect.void;
          }),
          Effect.forkChild,
        );
        assert.isDefined(runtime.compactThread);
        yield* runtime.compactThread?.(input) ?? Effect.void;
        yield* Deferred.await(terminalSeen);
        yield* fake.takeRequest("droid.compact_session");
        const loaded = yield* fake.takeRequest("droid.load_session");
        assert.deepEqual(
          typeof loaded.params === "object" && loaded.params !== null
            ? Reflect.get(loaded.params, "sessionId")
            : undefined,
          "compacted-session",
        );
        const compaction = events.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "compaction"
            ? [event.turnItem]
            : [],
        );
        assert.lengthOf(compaction, 1);
        assert.equal(compaction[0]?.status, "completed");
        assert.equal(compaction[0]?.title, "Context compacted");
        assert.isTrue(
          events.some(
            (event) =>
              event.type === "provider_thread.updated" &&
              event.providerThread.nativeThreadRef?.nativeId === "compacted-session",
          ),
        );
        const terminal = events.find((event) => event.type === "turn.terminal");
        assert.equal(terminal?.status, "completed");
      }),
    ),
  );

  it.effect("persists compacted transcript usage after a manual compaction", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          compactSessionResult: { newSessionId: "compacted-session", removedCount: 1_097 },
          contextBreakdown: {
            contextBudget: 300_000,
            usedTokens: 18_000,
            lastCallCompactionTokens: 225_204,
          },
        });
        const { runtime, input } = yield* open(fake);
        const terminalSeen = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal"
              ? Deferred.succeed(terminalSeen, undefined)
              : Effect.void,
          ),
          Effect.forkChild,
        );

        yield* runtime.compactThread?.(input) ?? Effect.void;
        yield* Deferred.await(terminalSeen);
        const snapshot = yield* runtime.readThreadSnapshot({
          providerThread: input.providerThread,
        });

        assert.equal(snapshot.providerTurns.at(-1)?.tokenUsage?.usedTokens, 18_000);
        assert.equal(snapshot.providerTurns.at(-1)?.tokenUsage?.maxTokens, 300_000);
      }),
    ),
  );

  it.effect("keeps ordinary-turn usage based on last-call tokens", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          contextBreakdown: {
            contextBudget: 300_000,
            usedTokens: 12_000,
            lastCallCompactionTokens: 40_000,
          },
        });
        const { runtime, input } = yield* open(fake);
        const terminalSeen = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal"
              ? Deferred.succeed(terminalSeen, undefined)
              : Effect.void,
          ),
          Effect.forkChild,
        );

        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(terminalSeen);
        const snapshot = yield* runtime.readThreadSnapshot({
          providerThread: input.providerThread,
        });

        assert.equal(snapshot.providerTurns.at(-1)?.tokenUsage?.usedTokens, 40_000);
        assert.equal(snapshot.providerTurns.at(-1)?.tokenUsage?.maxTokens, 300_000);
      }),
    ),
  );

  it.effect("uses fresh last-call usage on the turn after compaction", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          contextBreakdownSequence: [
            {
              contextBudget: 300_000,
              usedTokens: 18_000,
              lastCallCompactionTokens: 225_204,
            },
            {
              contextBudget: 300_000,
              usedTokens: 18_000,
              lastCallCompactionTokens: 225_204,
            },
            {
              contextBudget: 300_000,
              usedTokens: 18_000,
              lastCallCompactionTokens: 225_204,
            },
            {
              contextBudget: 300_000,
              usedTokens: 60_000,
              lastCallCompactionTokens: 41_000,
            },
          ],
        });
        const { runtime, input } = yield* open(fake);
        const firstTerminal = yield* Deferred.make<void>();
        const secondTerminal = yield* Deferred.make<void>();
        let terminalCount = 0;
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            if (event.type !== "turn.terminal") return Effect.void;
            terminalCount += 1;
            return terminalCount === 1
              ? Deferred.succeed(firstTerminal, undefined)
              : Deferred.succeed(secondTerminal, undefined);
          }),
          Effect.forkChild,
        );

        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "session_compacted",
          summaryId: "summary-1",
          removedCount: 1_097,
          visibleBoundaryMessageId: null,
        });
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(firstTerminal);

        const nextInput = {
          ...input,
          runId: RunId.make("droid-after-compaction-run"),
          attemptId: RunAttemptId.make("droid-after-compaction-attempt"),
          message: {
            ...input.message,
            messageId: MessageId.make("droid-after-compaction-message"),
          },
        };
        yield* runtime.startTurn(nextInput);
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: nextInput.message.messageId,
        });
        yield* Deferred.await(secondTerminal);
        const snapshot = yield* runtime.readThreadSnapshot({
          providerThread: input.providerThread,
        });

        assert.equal(snapshot.providerTurns.at(-1)?.tokenUsage?.usedTokens, 41_000);
        assert.equal(snapshot.providerTurns.at(-1)?.tokenUsage?.maxTokens, 300_000);
      }),
    ),
  );

  it.effect("folds Droid's repeated automatic compaction reports into one item", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          contextBreakdownSequence: [
            { contextBudget: 300_000, usedTokens: 678_000, lastCallCompactionTokens: 281_000 },
            { contextBudget: 300_000, usedTokens: 678_000, lastCallCompactionTokens: 281_000 },
            { contextBudget: 300_000, usedTokens: 42_000, lastCallCompactionTokens: 20_000 },
          ],
        });
        const { runtime, input } = yield* open(fake);
        const events: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
        const terminalSeen = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            events.push(event);
            return event.type === "turn.terminal"
              ? Deferred.succeed(terminalSeen, undefined)
              : Effect.void;
          }),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        // Droid reports one automatic compaction twice, without leaving the session.
        yield* fake.emitNotification({
          type: "session_compacted",
          summaryId: "summary-1",
          removedCount: 1,
          visibleBoundaryMessageId: null,
        });
        yield* fake.emitNotification({
          type: "session_compacted",
          summaryId: "summary-2",
          removedCount: 1,
          visibleBoundaryMessageId: "message-9",
        });
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(terminalSeen);
        const compaction = events.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "compaction"
            ? [event.turnItem]
            : [],
        );
        assert.isAbove(compaction.length, 0);
        assert.lengthOf(new Set(compaction.map((item) => item.id)), 1);
        assert.equal(compaction.at(-1)?.status, "completed");
        assert.equal(compaction.at(-1)?.title, "Context compacted");
        const snapshot = yield* runtime.readThreadSnapshot({
          providerThread: input.providerThread,
        });
        assert.equal(snapshot.providerTurns.at(-1)?.tokenUsage?.usedTokens, 42_000);
        assert.isFalse(
          events.some(
            (event) =>
              event.type === "provider_thread.updated" &&
              event.providerThread.nativeThreadRef?.nativeId !== "native-session",
          ),
        );
      }),
    ),
  );

  it.effect("persists current transcript usage after automatic mid-turn compaction", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          contextBreakdown: {
            contextBudget: 300_000,
            usedTokens: 18_000,
            lastCallCompactionTokens: 225_204,
          },
        });
        const { runtime, input } = yield* open(fake);
        const terminalSeen = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal"
              ? Deferred.succeed(terminalSeen, undefined)
              : Effect.void,
          ),
          Effect.forkChild,
        );

        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "session_compacted",
          summaryId: "summary-1",
          removedCount: 1_097,
          visibleBoundaryMessageId: null,
        });
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(terminalSeen);
        const snapshot = yield* runtime.readThreadSnapshot({
          providerThread: input.providerThread,
        });

        assert.equal(snapshot.providerTurns.at(-1)?.tokenUsage?.usedTokens, 18_000);
        assert.equal(snapshot.providerTurns.at(-1)?.tokenUsage?.maxTokens, 300_000);
      }),
    ),
  );

  it.effect.each([
    ["foreground", "Task completed.", false, "heavy", "high"],
    ["background", "Task launched in background.\nTask ID: child", true, "light", "low"],
    ["medium", "Task completed.", false, "medium", "medium"],
    ["missing", "Task completed.", false, undefined, undefined],
    ["unknown", "Task completed.", false, "unrecognized", undefined],
  ] as const)(
    "projects only a %s Task in the provider roster",
    ([, result, expected, complexity, expectedEffort]) =>
      provide(
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const { runtime, input } = yield* open(fake);
          const providerThreadEvents: ProviderAdapter.ProviderAdapterV2Event[] = [];
          const subagentEvents: ProviderAdapter.ProviderAdapterV2Event[] = [];
          const rosterReady = yield* Deferred.make<void>();
          const childReady = yield* Deferred.make<void>();
          const taskResultReady = yield* Deferred.make<void>();
          const turnSettled = yield* Deferred.make<void>();
          const childId = "child-session";
          yield* runtime.events.pipe(
            Stream.runForEach((event) => {
              if (event.type === "provider_thread.updated") {
                providerThreadEvents.push(event);
                if (
                  (event.providerThread.pendingBackgroundTasks ?? []).some(
                    (task) => task.taskId === childId,
                  )
                )
                  return Deferred.succeed(rosterReady, undefined);
              }
              if (
                event.type === "subagent.updated" &&
                event.subagent.nativeTaskRef?.nativeId === childId
              ) {
                subagentEvents.push(event);
                return Deferred.succeed(childReady, undefined);
              }
              if (
                event.type === "turn_item.updated" &&
                event.turnItem.type === "dynamic_tool" &&
                event.turnItem.title === "Task" &&
                event.turnItem.status === "completed"
              )
                return Deferred.succeed(taskResultReady, undefined);
              if (event.type === "turn.terminal") return Deferred.succeed(turnSettled, undefined);
              return Effect.void;
            }),
            Effect.forkChild,
          );
          yield* runtime.startTurn(input);
          yield* fake.takeRequest("droid.add_user_message");
          yield* fake.emitNotification({
            type: "create_message",
            message: {
              id: "parent",
              role: "assistant",
              content: [
                {
                  type: "tool_use",
                  id: "task-call",
                  name: "Task",
                  input: { description: "Background child", prompt: "Do work", complexity },
                },
              ],
            },
          });
          yield* fake.emitNotification({
            type: "child_session_available",
            childSessionId: childId,
            toolUseId: "task-call",
            description: "Background child",
          });
          yield* Deferred.await(childReady);
          yield* fake.emitNotification({
            type: "tool_result",
            toolUseId: "task-call",
            content: result,
          });
          yield* Deferred.await(taskResultReady);
          yield* fake.emitNotification({
            type: "agent_turn_completed",
            reason: "completed",
            turnId: input.message.messageId,
          });
          yield* Deferred.await(turnSettled);
          if (expected) yield* Deferred.await(rosterReady);
          assert.equal(
            providerThreadEvents.some(
              (event) =>
                event.type === "provider_thread.updated" &&
                event.providerThread.pendingBackgroundTasks?.some(
                  (task) => task.taskId === childId,
                ),
            ),
            expected,
          );
          assert.equal(
            yield* runtime.hasPendingBackgroundWorkForThread!(input.providerThread),
            expected,
          );
          const child = subagentEvents
            .flatMap((event) => (event.type === "subagent.updated" ? [event.subagent] : []))
            .find((subagent) => subagent.nativeTaskRef?.nativeId === childId);
          assert.equal(Reflect.get(child ?? {}, "effort"), expectedEffort);
        }),
      ),
  );
  it.effect("reconciles the persisted roster after an idle background completion", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const observed: ProviderAdapter.ProviderAdapterV2Event[] = [];
        const firstTerminal = yield* Deferred.make<void>();
        const secondTerminal = yield* Deferred.make<void>();
        const childFinished = yield* Deferred.make<void>();
        const idleRosterCleared = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            observed.push(event);
            if (
              event.type === "provider_thread.updated" &&
              event.providerThread.pendingBackgroundTasks?.length === 0
            )
              return Deferred.succeed(idleRosterCleared, undefined);
            if (
              event.type === "subagent.updated" &&
              event.subagent.nativeTaskRef?.nativeId === "roster-child" &&
              event.subagent.status === "completed"
            )
              return Deferred.succeed(childFinished, undefined);
            if (event.type === "turn.terminal") {
              return observed.filter((item) => item.type === "turn.terminal").length === 1
                ? Deferred.succeed(firstTerminal, undefined)
                : Deferred.succeed(secondTerminal, undefined);
            }
            return Effect.void;
          }),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "roster-parent",
            role: "assistant",
            content: [
              {
                type: "tool_use",
                id: "roster-task",
                name: "Task",
                input: { description: "Background child", prompt: "Do work" },
              },
            ],
          },
        });
        yield* fake.emitNotification({
          type: "child_session_available",
          childSessionId: "roster-child",
          toolUseId: "roster-task",
          description: "Background child",
        });
        yield* fake.emitNotification({
          type: "tool_result",
          toolUseId: "roster-task",
          content: "Task launched in background.\nTask ID: roster-child",
        });
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(firstTerminal);
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "task-completion:roster-child",
            role: "system",
            content: [
              {
                type: "text",
                text: "task_id: roster-child\nreason: completed\noutput: Done",
              },
            ],
          },
        });
        yield* Deferred.await(childFinished);
        yield* Deferred.await(idleRosterCleared);
        const staleProviderThread = {
          ...input.providerThread,
          pendingBackgroundTasks: [
            { taskId: "roster-child", kind: "subagent" as const, description: "Background child" },
          ],
        };
        const startIndex = observed.length;
        yield* runtime.startTurn({
          ...input,
          providerThread: staleProviderThread,
          runId: RunId.make("stale-roster-wake"),
          attemptId: RunAttemptId.make("stale-roster-attempt"),
          runOrdinal: 2,
          providerTurnOrdinal: 2,
          message: { ...input.message, createdBy: "agent", creationSource: "provider" },
        });
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: "task-completion:roster-child",
        });
        yield* Deferred.await(secondTerminal);
        const turnIndex = observed.findIndex(
          (event, index) =>
            index >= startIndex &&
            event.type === "provider_turn.updated" &&
            event.providerTurn.runAttemptId === RunAttemptId.make("stale-roster-attempt"),
        );
        const rosterIndex = observed.findIndex(
          (event, index) =>
            index >= startIndex &&
            event.type === "provider_thread.updated" &&
            event.providerThread.pendingBackgroundTasks?.length === 0,
        );
        assert.isAtLeast(turnIndex, startIndex);
        assert.isAbove(rosterIndex, turnIndex);
      }),
    ),
  );

  it.effect("streams a Droid child transcript and its pinned model into the child thread", () =>
    provide(
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = yield* fileSystem.makeTempDirectoryScoped({
          directory: NodeOS.tmpdir(),
          prefix: "droid-child-transcript-",
        });
        const sessions = path.join(home, ".factory", "sessions", "workspace");
        yield* fileSystem.makeDirectory(sessions, { recursive: true });
        const parentId = "229d7d4f-9420-4fc5-8ad4-e4f48dcc0401";
        const childId = "43a7fb2d-79a1-4c49-a8d1-3b089cf7d202";
        yield* Effect.promise(() =>
          NodeFSP.writeFile(
            path.join(sessions, `${parentId}.jsonl`),
            `${JSON.stringify({ type: "session_start", id: parentId })}\n`,
          ),
        );
        yield* fileSystem.makeDirectory(path.join(home, ".factory", "droids"), {
          recursive: true,
        });
        yield* Effect.promise(() =>
          NodeFSP.writeFile(
            path.join(home, ".factory", "droids", "worker.md"),
            '---\nname: worker\nmodel: "custom:GPT-6 Luna-12"\n---\n',
          ),
        );
        const fake = yield* makeFakeDroid({ nativeSessionId: parentId });
        const { runtime, input } = yield* open(
          fake,
          undefined,
          undefined,
          undefined,
          undefined,
          undefined,
          false,
          { FACTORY_HOME_OVERRIDE: home },
        );
        const childMessage = yield* Deferred.make<void>();
        const childModel = yield* Deferred.make<void>();
        const childToolStarted = yield* Deferred.make<void>();
        const childToolOutputSeen = yield* Deferred.make<void>();
        const events: ProviderAdapter.ProviderAdapterV2Event[] = [];
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            events.push(event);
            if (
              event.type === "message.updated" &&
              event.message.threadId !== threadId &&
              event.message.text === "Child is working"
            )
              return Deferred.succeed(childMessage, undefined);
            if (
              event.type === "subagent.updated" &&
              event.subagent.nativeTaskRef?.nativeId === childId &&
              event.subagent.model === "GPT-6 Luna"
            )
              return Deferred.succeed(childModel, undefined);
            if (
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              event.turnItem.status === "running" &&
              event.turnItem.threadId !== threadId
            )
              return Deferred.succeed(childToolStarted, undefined);
            if (
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              event.turnItem.output === "child output" &&
              event.turnItem.threadId !== threadId
            )
              return Deferred.succeed(childToolOutputSeen, undefined);
            return Effect.void;
          }),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "parent-with-task",
            role: "assistant",
            content: [
              {
                type: "tool_use",
                id: "transcript-task",
                name: "Task",
                input: { description: "Transcript child", prompt: "Do work" },
              },
            ],
          },
        });
        yield* fake.emitNotification({
          type: "child_session_available",
          childSessionId: childId,
          toolUseId: "transcript-task",
          subagentType: "worker",
          description: "Transcript child",
        });
        const childFile = path.join(sessions, `${childId}.jsonl`);
        const initialRows = `${JSON.stringify({
          type: "message",
          timestamp: "2026-10-12T10:00:00.000Z",
          message: { role: "user", content: [{ type: "text", text: "Do work" }] },
        })}\n`;
        const childReply = JSON.stringify({
          type: "message",
          id: "child-reply",
          message: {
            role: "assistant",
            content: [
              { type: "text", text: "Child is working" },
              {
                type: "tool_use",
                id: "child-exec",
                name: "Execute",
                input: { command: "echo child" },
              },
            ],
          },
        });
        const childToolResult = `${JSON.stringify({
          type: "message",
          id: "child-tool-result",
          message: {
            role: "user",
            content: [{ type: "tool_result", tool_use_id: "child-exec", content: "child output" }],
          },
        })}\n`;
        yield* Effect.promise(() =>
          NodeFSP.writeFile(childFile, `not-json\n${initialRows}${childReply.slice(0, 24)}`),
        );
        yield* Effect.promise(() => NodeFSP.appendFile(childFile, `${childReply.slice(24)}\n`));
        yield* Deferred.await(childMessage);
        yield* Deferred.await(childModel);
        yield* Deferred.await(childToolStarted);
        yield* Effect.promise(() => NodeFSP.appendFile(childFile, childToolResult));
        yield* Deferred.await(childToolOutputSeen);
        assert.isTrue(
          events.some(
            (event) =>
              event.type === "message.updated" &&
              event.message.text === "Do work" &&
              event.message.threadId !== threadId,
          ),
        );
        assert.isFalse(
          events.some(
            (event) =>
              event.type === "message.updated" &&
              event.message.threadId !== threadId &&
              event.message.text !== "Child is working" &&
              event.message.role === "assistant",
          ),
        );
        assert.isTrue(
          events.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              event.turnItem.output === "child output" &&
              event.turnItem.threadId !== threadId,
          ),
        );
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: `task-completion:${childId}`,
            role: "system",
            content: [
              { type: "text", text: `task_id: ${childId}\nreason: completed\noutput: done` },
            ],
          },
        });
        const extraAssistant = JSON.stringify({
          type: "message",
          id: "after-finish",
          message: { role: "assistant", content: [{ type: "text", text: "must not appear" }] },
        });
        yield* Effect.promise(() => NodeFSP.appendFile(childFile, `${extraAssistant}\n`));
      }),
    ),
  );

  it.effect("requires the exact wake turnId rather than any task-completion prefix", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const wakeBuffered = yield* Deferred.make<void>();
        const { runtime, input } = yield* open(fake, {
          offer: () => Deferred.succeed(wakeBuffered, undefined),
        });
        const terminal = yield* Deferred.make<void>();
        const wakeTerminal = yield* Deferred.make<void>();
        let terminalCount = 0;
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            if (event.type !== "turn.terminal") return Effect.void;
            terminalCount++;
            return terminalCount === 1
              ? Deferred.succeed(terminal, undefined)
              : Deferred.succeed(wakeTerminal, undefined);
          }),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(terminal);
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "task-completion:expected",
            role: "user",
            content: [],
          },
        });
        yield* Deferred.await(wakeBuffered);
        const continuation = {
          ...input,
          attemptId: RunAttemptId.make("exact-wake-attempt"),
          runOrdinal: 2,
          providerTurnOrdinal: 2,
          message: {
            ...input.message,
            createdBy: "agent" as const,
            creationSource: "provider" as const,
          },
        };
        yield* runtime.startTurn(continuation);
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: "task-completion:unrelated",
        });
        yield* fake.emitRequest("wrong-turn-barrier", "droid.unknown_barrier", {});
        yield* fake.takeReply;
        const snapshot = yield* runtime.readThreadSnapshot({
          providerThread: input.providerThread,
        });
        assert.equal(snapshot.providerTurns.at(-1)?.status, "running");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: "task-completion:expected",
        });
        yield* Deferred.await(wakeTerminal);
      }),
    ),
  );

  it.effect(
    "drains an undispatched background wake before a user turn and ignores its late completion",
    () =>
      provide(
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const wakeBuffered = yield* Deferred.make<void>();
          const { runtime, input } = yield* open(fake, {
            offer: () => Deferred.succeed(wakeBuffered, undefined),
          });
          const firstTerminal = yield* Deferred.make<void>();
          const nextTerminal = yield* Deferred.make<void>();
          let terminalCount = 0;
          yield* runtime.events.pipe(
            Stream.runForEach((event) => {
              if (event.type !== "turn.terminal") return Effect.void;
              terminalCount++;
              return terminalCount === 1
                ? Deferred.succeed(firstTerminal, undefined)
                : Deferred.succeed(nextTerminal, undefined);
            }),
            Effect.forkChild,
          );
          yield* runtime.startTurn(input);
          yield* fake.takeRequest("droid.add_user_message");
          yield* fake.emitNotification({
            type: "agent_turn_completed",
            reason: "completed",
            turnId: input.message.messageId,
          });
          yield* Deferred.await(firstTerminal);
          yield* fake.emitNotification({
            type: "create_message",
            message: {
              id: "task-completion:buffered-child",
              role: "user",
              content: [
                { type: "text", text: "task_id: buffered-child\nreason: completed\noutput: DONE" },
              ],
            },
          });
          yield* Deferred.await(wakeBuffered);
          yield* fake.emitNotification({
            type: "create_message",
            message: {
              id: "wake-answer",
              role: "assistant",
              content: [{ type: "text", text: "Wake result" }],
            },
          });
          yield* fake.emitNotification({
            type: "agent_turn_completed",
            reason: "completed",
            turnId: "task-completion:buffered-child",
          });
          yield* fake.emitRequest("idle-barrier", "droid.unknown_barrier", {});
          yield* fake.takeReply;
          const userTurn = {
            ...input,
            runId: RunId.make("user-after-wake"),
            attemptId: RunAttemptId.make("user-after-wake-attempt"),
            runOrdinal: 2,
            providerTurnOrdinal: 2,
            message: {
              ...input.message,
              messageId: MessageId.make("user-after-wake-message"),
            },
          };
          yield* runtime.startTurn(userTurn);
          yield* fake.takeRequest("droid.add_user_message");
          yield* fake.emitNotification({
            type: "agent_turn_completed",
            reason: "completed",
            turnId: "task-completion:buffered-child",
          });
          yield* fake.emitRequest("late-completion-barrier", "droid.unknown_barrier", {});
          yield* fake.takeReply;
          const snapshot = yield* runtime.readThreadSnapshot({
            providerThread: input.providerThread,
          });
          assert.equal(snapshot.providerTurns.at(-1)?.status, "running");
          yield* fake.emitNotification({
            type: "agent_turn_completed",
            reason: "completed",
            turnId: userTurn.message.messageId,
          });
          yield* Deferred.await(nextTerminal);
          assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
          assert.isTrue(snapshot.messages.some((message) => message.text === "Wake result"));
        }),
      ),
  );

  it.effect("sends image paths on steering messages", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(
          fake,
          undefined,
          undefined,
          undefined,
          (attachment) => `/attachments/${attachment.id}`,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        const turn = (yield* runtime.readThreadSnapshot({ providerThread: input.providerThread }))
          .providerTurns[0];
        assert.isDefined(turn);
        if (turn === undefined) return;
        const attachmentId = ChatAttachmentId.make("steer-image");
        yield* runtime.steerTurn({
          threadId,
          runId: input.runId,
          providerThread: input.providerThread,
          providerTurnId: turn.id,
          message: {
            ...input.message,
            messageId: MessageId.make("steer-image-message"),
            text: "Look again",
            attachments: [
              {
                type: "image",
                id: attachmentId,
                name: "image.png",
                mimeType: "image/png",
                sizeBytes: 1,
              },
            ],
          },
        });
        const request = yield* fake.takeRequest("droid.add_user_message");
        const imagePaths =
          typeof request.params === "object" && request.params !== null
            ? Reflect.get(request.params, "imagePaths")
            : undefined;
        assert.deepEqual(imagePaths, [`/attachments/${attachmentId}`]);
      }),
    ),
  );

  it.effect("sends a selected native slash command as unchanged user text", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        yield* runtime.startTurn({
          ...input,
          message: { ...input.message, text: "/review src/example.ts" },
        });
        const request = yield* fake.takeRequest("droid.add_user_message");
        assert.deepEqual(request.params, {
          messageId: input.message.messageId,
          text: "/review src/example.ts",
          imagePaths: [],
          userMessageSource: "sdk",
          queuePlacement: "end_of_turn",
        });
      }),
    ),
  );

  it.effect("rejects an unresolvable image instead of sending null image paths", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake, undefined, undefined, undefined, () => null);
        const result = yield* Effect.result(
          runtime.startTurn({
            ...input,
            message: {
              ...input.message,
              attachments: [
                {
                  type: "image",
                  id: ChatAttachmentId.make("missing-image"),
                  name: "pixel.png",
                  mimeType: "image/png",
                  sizeBytes: 68,
                },
              ],
            },
          }),
        );
        assert.equal(result._tag, "Failure");
        assert.isFalse(fake.requestedMethods.includes("droid.add_user_message"));
      }),
    ),
  );

  it.effect("cancels truly unroutable idle requests without creating UI artifacts", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const observed: ProviderAdapter.ProviderAdapterV2Event[] = [];
        const turnEnded = yield* Deferred.make<void>();
        const idleRequestArtifact = yield* Deferred.make<void>();
        const reader = yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              observed.push(event);
              if (event.type === "turn.terminal") yield* Deferred.succeed(turnEnded, undefined);
              if (event.type === "runtime_request.updated")
                yield* Deferred.succeed(idleRequestArtifact, undefined);
            }),
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(turnEnded);
        observed.length = 0;
        yield* fake.emitRequest("orphan-ask", "droid.ask_user", {
          toolCallId: "orphan",
          questions: [{ index: 1, topic: "Color", question: "Color?", options: ["Blue"] }],
        });
        const outcome = yield* Effect.raceFirst(
          fake.takeReply.pipe(Effect.map((reply) => ({ kind: "reply" as const, reply }))),
          Deferred.await(idleRequestArtifact).pipe(
            Effect.map(() => ({ kind: "artifact" as const })),
          ),
        );
        assert.equal(outcome.kind, "reply");
        if (outcome.kind === "reply") {
          assert.equal(outcome.reply.id, "orphan-ask");
          assert.deepEqual(outcome.reply.result, { cancelled: true, answers: [] });
        }
        assert.isFalse(observed.some((event) => event.type === "runtime_request.updated"));
        yield* Fiber.interrupt(reader);
      }),
    ),
  );

  it.effect("skips malformed native notification payloads without losing the live session", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const terminal = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal" ? Deferred.succeed(terminal, undefined) : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "create_message",
          message: { role: "assistant", content: [] },
        });
        yield* fake.emitNotification({
          type: "child_session_available",
          toolUseId: "missing-child-id",
        });
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "bad-tool",
            role: "assistant",
            content: [{ type: "tool_use", name: "Execute" }],
          },
        });
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "still-live",
            role: "assistant",
            content: [{ type: "text", text: "Still connected" }],
          },
        });
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(terminal);
        assert.isFalse(fake.closed());
        const snapshot = yield* runtime.readThreadSnapshot({
          providerThread: input.providerThread,
        });
        assert.equal(snapshot.providerTurns.at(-1)?.status, "completed");
        assert.equal(snapshot.messages.at(-1)?.text, "Still connected");
      }),
    ),
  );

  it.effect("reopens a completed child when Task resumes its session", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const childId = "resumable-child";
        const rows: Array<import("@t3tools/contracts").OrchestrationV2Subagent> = [];
        const completed = yield* Deferred.make<void>();
        const resumedTaskSettled = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            if (event.type === "subagent.updated") {
              rows.push(event.subagent);
              if (event.subagent.status === "completed")
                return Deferred.succeed(completed, undefined);
            }
            if (
              event.type === "turn_item.updated" &&
              event.turnItem.type === "dynamic_tool" &&
              event.turnItem.nativeItemRef?.nativeId === "resume-task-call" &&
              event.turnItem.status === "completed"
            )
              return Deferred.succeed(resumedTaskSettled, undefined);
            return Effect.void;
          }),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "task-parent",
            role: "assistant",
            content: [
              {
                type: "tool_use",
                id: "initial-task",
                name: "Task",
                input: { description: "Worker", prompt: "Do work" },
              },
            ],
          },
        });
        yield* fake.emitNotification({
          type: "child_session_available",
          childSessionId: childId,
          toolUseId: "initial-task",
          description: "Worker",
        });
        yield* fake.emitNotification({
          type: "tool_result",
          toolUseId: "initial-task",
          content: "Task launched in background.\nTask ID: resumable-child",
        });
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "task-completion:initial",
            role: "system",
            content: [
              { type: "text", text: "task_id: resumable-child\nreason: completed\noutput: FIRST" },
            ],
          },
        });
        yield* Deferred.await(completed);
        const originalId = rows.at(-1)?.id;
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            role: "assistant",
            id: "resume-task",
            content: [
              {
                type: "tool_use",
                id: "resume-task-call",
                name: "Task",
                input: { resume: childId, prompt: "Continue work" },
              },
            ],
          },
        });
        yield* fake.emitNotification({
          type: "tool_result",
          toolUseId: "resume-task-call",
          content: "Task launched in background.\nTask ID: resumable-child",
        });
        yield* Deferred.await(resumedTaskSettled);
        assert.equal(rows.at(-1)?.id, originalId);
        assert.equal(rows.at(-1)?.nativeTaskRef?.nativeId, childId);
        assert.equal(rows.at(-1)?.status, "running");
      }),
    ),
  );

  it.effect("does not emit a provider roster update when an empty roster already matches", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const events: ProviderAdapter.ProviderAdapterV2Event[] = [];
        const terminal = yield* Deferred.make<void>();
        const turnStarted = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            events.push(event);
            if (event.type === "provider_turn.updated")
              return Deferred.succeed(turnStarted, undefined);
            return event.type === "turn.terminal"
              ? Deferred.succeed(terminal, undefined)
              : Effect.void;
          }),
          Effect.forkChild,
        );
        yield* runtime.startTurn({
          ...input,
          runId: RunId.make("matching-roster"),
          attemptId: RunAttemptId.make("matching-roster-attempt"),
          runOrdinal: 2,
          providerTurnOrdinal: 2,
        });
        yield* Deferred.await(turnStarted);
        assert.isTrue(events.some((event) => event.type === "provider_turn.updated"));
        assert.isFalse(events.some((event) => event.type === "provider_thread.updated"));
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(terminal);
      }),
    ),
  );

  it.effect.each(["completed", "cancelled"] as const)(
    "offers a named wake when a resumed child is %s",
    (outcome) =>
      provide(
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          let offered: ProviderContinuationRequests.ProviderContinuationRequest | undefined;
          const { runtime, input } = yield* open(fake, {
            offer: (request) =>
              Effect.sync(() => {
                offered = request;
              }),
          });
          const childId = "fc2a2f6c-76df-4eaf-89a7-014f8712f83f";
          const updates: Array<import("@t3tools/contracts").OrchestrationV2Subagent> = [];
          const observed: ProviderAdapter.ProviderAdapterV2Event[] = [];
          const rosterAdded = yield* Deferred.make<void>();
          const rosterCleared = yield* Deferred.make<void>();
          const turnSettled = yield* Deferred.make<void>();
          const childFinished = yield* Deferred.make<void>();
          let rosterWasAdded = false;
          yield* runtime.events.pipe(
            Stream.runForEach((event) => {
              observed.push(event);
              if (event.type === "subagent.updated") {
                updates.push(event.subagent);
                if (event.subagent.status === outcome)
                  return Deferred.succeed(childFinished, undefined);
              }
              if (event.type === "provider_thread.updated") {
                const tasks = event.providerThread.pendingBackgroundTasks ?? [];
                if (tasks.some((task) => task.taskId === childId)) {
                  rosterWasAdded = true;
                  return Deferred.succeed(rosterAdded, undefined);
                }
                if (rosterWasAdded && tasks.every((task) => task.taskId !== childId))
                  return Deferred.succeed(rosterCleared, undefined);
              }
              if (event.type === "turn.terminal") return Deferred.succeed(turnSettled, undefined);
              return Effect.void;
            }),
            Effect.forkChild,
          );
          yield* runtime.startTurn(input);
          yield* fake.takeRequest("droid.add_user_message");
          yield* fake.emitNotification({
            type: "create_message",
            message: {
              id: "resume-parent",
              role: "assistant",
              content: [
                {
                  type: "tool_use",
                  id: "resume-task",
                  name: "Task",
                  input: {
                    resume: childId,
                    description: "Resumed child",
                    prompt: "Continue",
                  },
                },
              ],
            },
          });
          yield* fake.emitNotification({
            type: "tool_result",
            toolUseId: "resume-task",
            content: `Task launched in background.\nTask ID: ${childId}`,
          });
          yield* fake.emitNotification({
            type: "agent_turn_completed",
            reason: "completed",
            turnId: input.message.messageId,
          });
          yield* Deferred.await(turnSettled);
          assert.equal(updates.at(-1)?.status, "running");
          assert.equal(updates.at(-1)?.nativeTaskRef?.nativeId, childId);
          const childThreadId = updates.at(-1)?.childThreadId;
          assert.isDefined(childThreadId);
          const subagentIndex = observed.findIndex(
            (event) =>
              event.type === "subagent.updated" &&
              event.subagent.nativeTaskRef?.nativeId === childId,
          );
          const rootNodeIndex = observed.findIndex(
            (event) =>
              event.type === "node.updated" &&
              event.node.threadId === childThreadId &&
              event.node.kind === "root_turn",
          );
          const rootNode = rootNodeIndex < 0 ? undefined : observed[rootNodeIndex];
          assert.isAbove(rootNodeIndex, subagentIndex);
          assert.equal(
            rootNode?.type === "node.updated" ? rootNode.node.status : undefined,
            "running",
          );
          assert.isNull(rootNode?.type === "node.updated" ? rootNode.node.completedAt : undefined);
          assert.isTrue(yield* runtime.hasPendingBackgroundWorkForThread!(input.providerThread));
          yield* Deferred.await(rosterAdded);
          yield* fake.emitNotification({
            type: "create_message",
            message: {
              id: `task-completion:${childId}`,
              role: "system",
              content: [
                {
                  type: "text",
                  text: `task_id: ${childId}\nreason: ${outcome}\noutput: Done\n\nBackground task report`,
                },
              ],
            },
          });
          yield* Deferred.await(childFinished);
          yield* Deferred.await(rosterCleared);
          assert.equal(updates.at(-1)?.status, outcome);
          assert.equal(offered?.notification?.source.kind, "subagent");
          assert.equal(
            offered?.notification?.source.kind === "subagent"
              ? offered.notification.source.childThreadId
              : undefined,
            childThreadId,
          );
          assert.equal(offered?.notification?.outcome, outcome);
          assert.equal(
            offered?.notification?.summary,
            outcome === "cancelled"
              ? 'Subagent "Resumed child" was stopped'
              : 'Subagent "Resumed child" finished',
          );
        }),
      ),
  );

  it.effect("keeps the generic notification for an unknown completed child", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const requested = yield* Deferred.make<void>();
        let offered: ProviderContinuationRequests.ProviderContinuationRequest | undefined;
        const { runtime, input } = yield* open(fake, {
          offer: (request) =>
            Effect.sync(() => {
              offered = request;
            }).pipe(Effect.andThen(Deferred.succeed(requested, undefined)), Effect.asVoid),
        });
        const turnSettled = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal" ? Deferred.succeed(turnSettled, undefined) : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(turnSettled);
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "task-completion:unknown-child",
            role: "system",
            content: [
              { type: "text", text: "task_id: unknown-child\nreason: completed\noutput: Done" },
            ],
          },
        });
        yield* Deferred.await(requested);
        assert.isUndefined(offered?.notification);
      }),
    ),
  );

  it.effect.each(["TaskStop", "task completion"] as const)(
    "clears the provider roster and pending-work state after %s",
    (completion) =>
      provide(
        Effect.gen(function* () {
          const fake = yield* makeFakeDroid();
          const { runtime, input } = yield* open(fake);
          const rosterAdded = yield* Deferred.make<void>();
          const rosterCleared = yield* Deferred.make<void>();
          yield* runtime.events.pipe(
            Stream.runForEach((event) => {
              if (event.type !== "provider_thread.updated") return Effect.void;
              const tasks = event.providerThread.pendingBackgroundTasks ?? [];
              if (tasks.some((task) => task.taskId === "child-session"))
                return Deferred.succeed(rosterAdded, undefined);
              if (tasks.length === 0) return Deferred.succeed(rosterCleared, undefined);
              return Effect.void;
            }),
            Effect.forkChild,
          );
          yield* runtime.startTurn(input);
          yield* fake.takeRequest("droid.add_user_message");
          yield* fake.emitNotification({
            type: "create_message",
            message: {
              id: "parent",
              role: "assistant",
              content: [
                {
                  type: "tool_use",
                  id: "task-call",
                  name: "Task",
                  input: { description: "Background child", prompt: "Do work" },
                },
              ],
            },
          });
          yield* fake.emitNotification({
            type: "child_session_available",
            childSessionId: "child-session",
            toolUseId: "task-call",
            description: "Background child",
          });
          yield* fake.emitNotification({
            type: "tool_result",
            toolUseId: "task-call",
            content: "Task launched in background.\nTask ID: child-session",
          });
          yield* Deferred.await(rosterAdded);
          assert.isTrue(yield* runtime.hasPendingBackgroundWorkForThread!(input.providerThread));
          if (completion === "TaskStop") {
            yield* fake.emitNotification({
              type: "create_message",
              message: {
                id: "stop-parent",
                role: "assistant",
                content: [
                  {
                    type: "tool_use",
                    id: "stop-call",
                    name: "TaskStop",
                    input: { task_id: "child-session" },
                  },
                ],
              },
            });
            yield* fake.emitNotification({
              type: "tool_result",
              toolUseId: "stop-call",
              content: "Stopped",
            });
          } else {
            yield* fake.emitNotification({
              type: "create_message",
              message: {
                id: "task-completion:child-session",
                role: "system",
                content: [
                  {
                    type: "text",
                    text: "task_id: child-session\nreason: completed\noutput: Done\n\nBackground task report",
                  },
                ],
              },
            });
          }
          yield* Deferred.await(rosterCleared);
          assert.isFalse(yield* runtime.hasPendingBackgroundWorkForThread!(input.providerThread));
        }),
      ),
  );

  it.effect("stamps each item with the moment it appeared, not the turn start", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const items = new Map<string, { startedAt: number; completedAt: number | null }>();
        const toolDone = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) => {
            if (event.type !== "turn_item.updated") return Effect.void;
            items.set(event.turnItem.nativeItemRef?.nativeId ?? "", {
              startedAt: DateTime.toEpochMillis(
                event.turnItem.startedAt ?? event.turnItem.updatedAt,
              ),
              completedAt:
                event.turnItem.completedAt === null
                  ? null
                  : DateTime.toEpochMillis(event.turnItem.completedAt),
            });
            return event.turnItem.nativeItemRef?.nativeId === "tool-1" &&
              event.turnItem.status === "completed"
              ? Deferred.succeed(toolDone, undefined)
              : Effect.void;
          }),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.add_user_message");
        const turnStart = DateTime.toEpochMillis(yield* DateTime.now);
        yield* TestClock.adjust("5 seconds");
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "assistant-1",
            role: "assistant",
            content: [
              { type: "tool_use", id: "tool-1", name: "Execute", input: { command: "ls" } },
            ],
          },
        });
        yield* TestClock.adjust("5 seconds");
        yield* fake.emitNotification({
          type: "tool_result",
          toolUseId: "tool-1",
          content: "ok",
        });
        yield* Deferred.await(toolDone);
        const tool = items.get("tool-1");
        assert.equal(tool?.startedAt, turnStart + 5_000);
        assert.equal(tool?.completedAt, turnStart + 10_000);
      }),
    ),
  );

  it.effect("reports Droid's own window and context occupancy", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          contextBreakdown: {
            contextBudget: 300_000,
            usedTokens: 678_000,
            lastCallCompactionTokens: 281_000,
          },
        });
        const { runtime, input } = yield* open(fake);
        const terminalSeen = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal"
              ? Deferred.succeed(terminalSeen, undefined)
              : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        assert.equal(runtime.getModelContextWindow?.(selection), 300_000);
        assert.isUndefined(runtime.getModelContextWindow?.({ ...selection, model: "other" }));
        assert.isUndefined(
          runtime.getModelContextWindow?.({
            instanceId: ProviderInstanceId.make("someone-else"),
            model: selection.model,
          }),
        );
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(terminalSeen);
        const snapshot = yield* runtime.readThreadSnapshot({
          providerThread: input.providerThread,
        });
        assert.equal(snapshot.providerTurns.at(-1)?.status, "completed");
        assert.equal(snapshot.providerTurns.at(-1)?.tokenUsage?.usedTokens, 281_000);
        assert.equal(snapshot.providerTurns.at(-1)?.tokenUsage?.maxTokens, 300_000);
      }),
    ),
  );

  it.effect("completes a turn when the session cannot report context", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const terminalSeen = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal"
              ? Deferred.succeed(terminalSeen, undefined)
              : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(terminalSeen);
        const snapshot = yield* runtime.readThreadSnapshot({
          providerThread: input.providerThread,
        });
        assert.equal(snapshot.providerTurns.at(-1)?.status, "completed");
        assert.isUndefined(snapshot.providerTurns.at(-1)?.tokenUsage);
      }),
    ),
  );

  it.effect("uses transcript usage when last-call usage is absent", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({
          contextBreakdown: { contextBudget: 300_000, usedTokens: 18_000 },
        });
        const { runtime, input } = yield* open(fake);
        const terminalSeen = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal"
              ? Deferred.succeed(terminalSeen, undefined)
              : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(terminalSeen);
        const snapshot = yield* runtime.readThreadSnapshot({
          providerThread: input.providerThread,
        });

        assert.equal(snapshot.providerTurns.at(-1)?.tokenUsage?.usedTokens, 18_000);
        assert.equal(snapshot.providerTurns.at(-1)?.tokenUsage?.maxTokens, 300_000);
      }),
    ),
  );

  it.effect("classifies captured rate limits and retains retry attempts", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const observed: ProviderAdapter.ProviderAdapterV2Event[] = [];
        const terminalSeen = yield* Deferred.make<void>();
        const reader = yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              observed.push(event);
              if (event.type === "turn.terminal") yield* Deferred.succeed(terminalSeen, undefined);
            }),
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        for (const attempt of [1, 2, 6])
          yield* fake.emitNotification({ type: "llm_retry", attempt, reason: "rate_limited" });
        yield* fake.emitNotification({
          type: "error",
          message: "429 You exceeded your current quota",
        });
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "model_rate_limited",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(terminalSeen);
        const retries = observed.filter(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "error" &&
            event.turnItem.retry !== undefined,
        );
        assert.deepEqual(
          retries.map((event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "error"
              ? [event.turnItem.status, event.turnItem.retry?.attempt]
              : [],
          ),
          [
            ["running", 1],
            ["running", 2],
            ["running", 6],
            ["failed", 6],
          ],
        );
        const terminal = observed.find((event) => event.type === "turn.terminal");
        assert.equal(
          terminal?.type === "turn.terminal" ? terminal.failure?.class : undefined,
          "usage_limit",
        );
        assert.equal(
          terminal?.type === "turn.terminal" ? terminal.failure?.message : undefined,
          "429 You exceeded your current quota",
        );
        assert.equal(
          terminal?.type === "turn.terminal" ? terminal.failure?.code : undefined,
          "model_rate_limited",
        );
        yield* Fiber.interrupt(reader);
      }),
    ),
  );

  it.effect("classifies payment-required completion as a usage limit", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const terminalSeen = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal" ? Deferred.succeed(terminalSeen, event) : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({ type: "error", message: "402 Payment required" });
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "model_usage_exhausted",
          turnId: input.message.messageId,
        });
        const terminal = yield* Deferred.await(terminalSeen);
        assert.equal(
          terminal.type === "turn.terminal" ? terminal.failure?.class : undefined,
          "usage_limit",
        );
        assert.equal(
          terminal.type === "turn.terminal" ? terminal.failure?.message : undefined,
          "402 Payment required",
        );
      }),
    ),
  );

  it.effect("settles an indefinitely stalled retry when interrupted", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, providerThread, input } = yield* open(fake);
        const observed: ProviderAdapter.ProviderAdapterV2Event[] = [];
        const terminalSeen = yield* Deferred.make<void>();
        const reader = yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              observed.push(event);
              if (event.type === "turn.terminal") yield* Deferred.succeed(terminalSeen, undefined);
            }),
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({ type: "llm_retry", attempt: 1, reason: "rate_limited" });
        yield* runtime.interruptTurn({
          providerThread,
          providerTurnId: (yield* runtime.readThreadSnapshot({ providerThread })).providerTurns[0]!
            .id,
        });
        yield* fake.takeRequest("droid.interrupt_session");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "cancelled",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(terminalSeen);
        assert.deepEqual(
          observed
            .filter(
              (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
            )
            .map((event) => (event.type === "turn_item.updated" ? event.turnItem.status : "")),
          ["running", "interrupted"],
        );
        assert.isTrue(
          observed.some(
            (event) => event.type === "turn.terminal" && event.status === "interrupted",
          ),
        );
        yield* Fiber.interrupt(reader);
      }),
    ),
  );

  it.effect("shows captured approval details and preserves Droid response mapping", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const waiting = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        const pendingRequest = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        const reader = yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              if (
                event.type === "turn_item.updated" &&
                event.turnItem.type === "approval_request" &&
                event.turnItem.status === "waiting"
              )
                yield* Deferred.succeed(waiting, event);
              if (
                event.type === "runtime_request.updated" &&
                event.runtimeRequest.status === "pending"
              )
                yield* Deferred.succeed(pendingRequest, event);
            }),
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitRequest("approval-1", "droid.request_permission", {
          toolUses: [
            {
              toolUse: {
                id: "patch-1",
                name: "ApplyPatch",
                input: {},
              },
              confirmationType: "apply_patch",
              details: {
                type: "apply_patch",
                files: [{ filePath: "/workspace/new.txt", operation: "create" }],
              },
            },
          ],
          options: [
            { label: "Yes, allow", value: "proceed_once" },
            { label: "No, cancel", value: "cancel" },
          ],
        });
        const approval = yield* Deferred.await(waiting);
        assert.equal(
          approval.type === "turn_item.updated" && approval.turnItem.type === "approval_request"
            ? approval.turnItem.prompt
            : undefined,
          "create /workspace/new.txt",
        );
        if (approval.type !== "turn_item.updated" || approval.turnItem.type !== "approval_request")
          return;
        assert.deepEqual(approval.turnItem.options, [
          { decision: "accept", label: "Yes, allow" },
          { decision: "cancel", label: "No, cancel" },
        ]);
        const request = yield* Deferred.await(pendingRequest);
        assert.isTrue(request?.type === "runtime_request.updated");
        if (request?.type !== "runtime_request.updated") return;
        yield* runtime.respondToRuntimeRequest({
          requestId: request.runtimeRequest.id,
          decision: "accept",
        });
        const reply = yield* fake.takeReply;
        assert.deepEqual(reply, {
          type: "response",
          id: "approval-1",
          result: { selectedOption: "proceed_once" },
        });
        yield* Fiber.interrupt(reader);
      }),
    ),
  );

  it.effect("continues routing parent permissions by explicit session id", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const pendingRequest = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        const reader = yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending"
              ? Deferred.succeed(pendingRequest, event)
              : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitRequest("parent-permission", "droid.request_permission", {
          sessionId: "native-session",
          toolUses: [
            {
              toolUse: {
                id: "parent-execute",
                name: "Execute",
                input: { command: "echo parent" },
              },
              confirmationType: "exec",
              details: {},
            },
          ],
          options: [{ label: "Yes, allow", value: "proceed_once" }],
        });
        const event = yield* Deferred.await(pendingRequest);
        assert.isTrue(event.type === "runtime_request.updated");
        if (event.type !== "runtime_request.updated") return;
        yield* runtime.respondToRuntimeRequest({
          requestId: event.runtimeRequest.id,
          decision: "accept",
        });
        assert.equal((yield* fake.takeReply).id, "parent-permission");
        yield* Fiber.interrupt(reader);
      }),
    ),
  );

  it.effect("routes child approval requests to the announced child session", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const childId = "a9867e72-7406-41d4-b4bc-ed12db267ad4";
        const pendingRequest = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        const childSeen = yield* Deferred.make<void>();
        const observed: ProviderAdapter.ProviderAdapterV2Event[] = [];
        const reader = yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              observed.push(event);
              if (
                event.type === "runtime_request.updated" &&
                event.runtimeRequest.status === "pending"
              )
                yield* Deferred.succeed(pendingRequest, event);
              if (
                event.type === "subagent.updated" &&
                event.subagent.nativeTaskRef?.nativeId === childId
              )
                yield* Deferred.succeed(childSeen, undefined);
            }),
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "child_session_available",
          childSessionId: childId,
          toolUseId: "child-task",
          subagentType: "worker",
          description: "Do requested work",
        });
        yield* Deferred.await(childSeen);
        yield* fake.emitRequest("child-approval", "droid.request_permission", {
          toolUses: [
            {
              toolUse: {
                id: "child-execute",
                name: "Execute",
                input: { command: "touch marker" },
              },
              confirmationType: "exec",
              details: {},
            },
          ],
          options: [
            { label: "Yes, allow", value: "proceed_once" },
            { label: "No, cancel", value: "cancel" },
          ],
          associatedSessionIds: [childId, "native-session"],
        });
        const event = yield* Deferred.await(pendingRequest);
        assert.isTrue(event.type === "runtime_request.updated");
        if (event.type !== "runtime_request.updated") return;
        const child = observed.find(
          (entry) =>
            entry.type === "subagent.updated" && entry.subagent.nativeTaskRef?.nativeId === childId,
        );
        assert.isTrue(child?.type === "subagent.updated");
        if (child?.type !== "subagent.updated") return;
        assert.isTrue(
          observed.some(
            (entry) =>
              entry.type === "node.updated" &&
              entry.node.kind === "approval_request" &&
              entry.node.parentNodeId === child.subagent.id,
          ),
        );
        yield* runtime.respondToRuntimeRequest({
          requestId: event.runtimeRequest.id,
          decision: "cancel",
        });
        assert.equal((yield* fake.takeReply).id, "child-approval");
        yield* Fiber.interrupt(reader);
      }),
    ),
  );

  it.effect("rejects incompatible protocol versions and closes its own connection", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ protocolVersion: "unsupported" });
        const result = yield* open(fake).pipe(Effect.exit);
        assert.equal(result._tag, "Failure");
        assert.isTrue(fake.closed());
      }),
    ),
  );

  it.effect("settles a running turn when the captured process closes", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input, providerThread } = yield* open(fake);
        const terminal = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
        const childFailed = yield* Deferred.make<void>();
        const rosterCleared = yield* Deferred.make<void>();
        const rosterAdded = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              if (event.type === "turn.terminal") yield* Deferred.succeed(terminal, event);
              if (event.type === "subagent.updated" && event.subagent.status === "failed")
                yield* Deferred.succeed(childFailed, undefined);
              if (event.type === "provider_thread.updated") {
                const tasks = event.providerThread.pendingBackgroundTasks ?? [];
                if (tasks.some((task) => task.taskId === "child-before-crash"))
                  yield* Deferred.succeed(rosterAdded, undefined);
                if (tasks.length === 0) yield* Deferred.succeed(rosterCleared, undefined);
              }
            }),
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "child_session_available",
          childSessionId: "child-before-crash",
          toolUseId: "live-child",
          subagentType: "worker",
          description: "Still running",
        });
        yield* fake.emitNotification({
          type: "tool_result",
          toolUseId: "live-child",
          content: "Task launched in background.\nTask ID: child-before-crash",
        });
        yield* Deferred.await(rosterAdded);
        yield* fake.closeStdout;
        const event = yield* Deferred.await(terminal);
        yield* Deferred.await(childFailed);
        yield* Deferred.await(rosterCleared);
        assert.equal(event.type === "turn.terminal" ? event.status : undefined, "failed");
        const snapshot = yield* runtime.readThreadSnapshot({ providerThread });
        assert.equal(snapshot.providerTurns[0]?.status, "failed");
        assert.isFalse(yield* runtime.hasPendingBackgroundWorkForThread!(providerThread));
      }),
    ),
  );

  it.effect("rewinds after the target turn and discards the later in-memory snapshot", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid({ rewindSessionId: "rewound-native-session" });
        const { runtime, input, providerThread } = yield* open(fake);
        const terminals = yield* Queue.unbounded<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal" ? Queue.offer(terminals, undefined) : Effect.void,
          ),
          Effect.forkChild,
        );
        let initialized = false;
        const finishTurn = (turnInput: typeof input, assistantId: string) =>
          Effect.gen(function* () {
            yield* runtime.startTurn(turnInput);
            if (!initialized) {
              yield* fake.takeRequest("droid.initialize_session");
              initialized = true;
            }
            yield* fake.takeRequest("droid.get_context_breakdown");
            yield* fake.takeRequest("droid.get_context_breakdown");
            yield* fake.takeRequest("droid.add_user_message");
            yield* fake.emitNotification({
              type: "create_message",
              message: {
                id: assistantId,
                role: "assistant",
                content: [{ type: "text", text: assistantId }],
              },
            });
            yield* fake.emitNotification({
              type: "agent_turn_completed",
              reason: "completed",
              turnId: turnInput.message.messageId,
            });
            yield* Queue.take(terminals);
          });
        const firstInput = {
          ...input,
          attemptId: RunAttemptId.make("droid-rollback-first"),
          message: { ...input.message, messageId: MessageId.make("droid-rollback-first-message") },
        };
        yield* finishTurn(firstInput, "droid-rollback-first-assistant");
        const firstTurn = (yield* runtime.readThreadSnapshot({ providerThread })).providerTurns[0];
        assert.isDefined(firstTurn);
        const secondInput = {
          ...input,
          providerTurnOrdinal: 2,
          attemptId: RunAttemptId.make("droid-rollback-second"),
          message: { ...input.message, messageId: MessageId.make("droid-rollback-second-message") },
        };
        yield* finishTurn(secondInput, "droid-rollback-second-assistant");
        const secondTurn = (yield* runtime.readThreadSnapshot({ providerThread })).providerTurns[1];
        assert.isDefined(secondTurn);
        const capabilities = runtime.providerSession.capabilities;
        assert.isTrue(capabilities.threads.canRollbackThread);
        assert.isTrue(capabilities.checkpointing.providerCanRollbackConversation);
        assert.isTrue(capabilities.checkpointing.providerRollbackReturnsSnapshot);
        if (firstTurn === undefined || secondTurn === undefined) return;

        const result = yield* runtime.rollbackThread({
          providerThread,
          target: {
            type: "provider_turn",
            checkpointId: CheckpointId.make("droid-rollback-checkpoint"),
            appRunOrdinal: 1,
            providerTurn: firstTurn,
          },
          providerThreadTurns: [firstTurn, secondTurn],
        });
        const rewind = yield* fake.takeRequest("droid.execute_rewind");
        const params = rewind.params;
        assert.equal(
          typeof params === "object" && params !== null ? Reflect.get(params, "messageId") : null,
          "droid-rollback-second-message",
        );
        assert.deepEqual(
          typeof params === "object" && params !== null
            ? Reflect.get(params, "filesToRestore")
            : null,
          [],
        );
        assert.deepEqual(
          typeof params === "object" && params !== null
            ? Reflect.get(params, "filesToDelete")
            : null,
          [],
        );
        const load = yield* fake.takeRequest("droid.load_session");
        assert.deepEqual(load.params, {
          sessionId: "rewound-native-session",
          autoRejectPermissionRequests: false,
        });
        assert.equal(result.providerThread.id, providerThread.id);
        assert.equal(result.providerThread.nativeThreadRef?.nativeId, "rewound-native-session");
        assert.deepEqual(
          fake.requestedMethods.filter((method) => method === "droid.load_session"),
          ["droid.load_session"],
        );
        assert.equal(result.providerThread.status, "idle");
        assert.equal(result.providerThread.lastRunOrdinal, 1);
        assert.equal(
          (yield* runtime.readThreadSnapshot({ providerThread: result.providerThread }))
            .providerThread.nativeThreadRef?.nativeId,
          "rewound-native-session",
        );
        const snapshot = yield* runtime.readThreadSnapshot({
          providerThread: result.providerThread,
        });
        assert.deepEqual(
          snapshot.providerTurns.map((turn) => turn.id),
          [firstTurn.id],
        );
        assert.deepEqual(
          snapshot.messages.map((message) => message.text),
          ["droid-rollback-first-assistant"],
        );
      }),
    ),
  );

  it.effect("rewinds thread start to the first user message", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input, providerThread } = yield* open(fake);
        const terminal = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal" ? Deferred.succeed(terminal, undefined) : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(terminal);
        const turn = (yield* runtime.readThreadSnapshot({ providerThread })).providerTurns[0];
        assert.isDefined(turn);
        if (turn === undefined) return;

        yield* runtime.rollbackThread({
          providerThread,
          target: {
            type: "thread_start",
            checkpointId: CheckpointId.make("droid-rollback-start"),
            appRunOrdinal: 0,
          },
          providerThreadTurns: [turn],
        });
        const rewind = yield* fake.takeRequest("droid.execute_rewind");
        assert.equal(
          typeof rewind.params === "object" && rewind.params !== null
            ? Reflect.get(rewind.params, "messageId")
            : null,
          input.message.messageId,
        );
      }),
    ),
  );

  it.effect.each(["provider_turn", "thread_start"] as const)(
    "loads a persisted session before rewinding a %s target",
    (targetType) =>
      provide(
        Effect.gen(function* () {
          const sourceFake = yield* makeFakeDroid({ nativeSessionId: "old-session" });
          const source = yield* open(sourceFake);
          const terminal = yield* Deferred.make<void>();
          yield* source.runtime.events.pipe(
            Stream.runForEach((event) =>
              event.type === "turn.terminal" ? Deferred.succeed(terminal, undefined) : Effect.void,
            ),
            Effect.forkChild,
          );
          yield* source.runtime.startTurn(source.input);
          yield* sourceFake.takeRequest("droid.initialize_session");
          yield* sourceFake.takeRequest("droid.get_context_breakdown");
          yield* sourceFake.takeRequest("droid.get_context_breakdown");
          yield* sourceFake.takeRequest("droid.add_user_message");
          yield* sourceFake.emitNotification({
            type: "agent_turn_completed",
            reason: "completed",
            turnId: source.input.message.messageId,
          });
          yield* Deferred.await(terminal);
          const sourceTurn = (yield* source.runtime.readThreadSnapshot({
            providerThread: source.providerThread,
          })).providerTurns[0];
          assert.isDefined(sourceTurn);
          if (sourceTurn === undefined) return;

          const secondTurn = {
            ...sourceTurn,
            id: ProviderTurnId.make("droid-persisted-second-turn"),
            ordinal: sourceTurn.ordinal + 1,
            nativeTurnRef: {
              driver: DROID_PROVIDER,
              nativeId: "droid-next-user-message",
              strength: "strong",
            },
          } satisfies typeof sourceTurn;
          const persistedThread = {
            ...source.providerThread,
            nativeThreadRef: {
              driver: DROID_PROVIDER,
              nativeId: "old-session",
              strength: "strong",
            },
            lastRunOrdinal: 2,
          } satisfies typeof source.providerThread;
          const freshFake = yield* makeFakeDroid({ rewindSessionId: "rewound-session" });
          const fresh = yield* open(
            freshFake,
            undefined,
            undefined,
            undefined,
            undefined,
            persistedThread,
            true,
          );
          const target: ProviderAdapter.ProviderAdapterV2RollbackTarget =
            targetType === "thread_start"
              ? {
                  type: "thread_start" as const,
                  checkpointId: CheckpointId.make("droid-fresh-start-rollback"),
                  appRunOrdinal: 0,
                }
              : {
                  type: "provider_turn" as const,
                  checkpointId: CheckpointId.make("droid-fresh-turn-rollback"),
                  appRunOrdinal: 1,
                  providerTurn: sourceTurn,
                };
          const result = yield* fresh.runtime.rollbackThread({
            providerThread: persistedThread,
            target,
            providerThreadTurns:
              targetType === "thread_start" ? [sourceTurn] : [sourceTurn, secondTurn],
          });
          const sourceLoad = yield* freshFake.takeRequest("droid.load_session");
          assert.deepEqual(sourceLoad.params, {
            sessionId: "old-session",
            mcpServers: [],
            autoRejectPermissionRequests: false,
          });
          const rewind = yield* freshFake.takeRequest("droid.execute_rewind");
          assert.equal(
            typeof rewind.params === "object" && rewind.params !== null
              ? Reflect.get(rewind.params, "messageId")
              : null,
            targetType === "thread_start"
              ? source.input.message.messageId
              : "droid-next-user-message",
          );
          assert.deepEqual(
            typeof rewind.params === "object" && rewind.params !== null
              ? Reflect.get(rewind.params, "filesToRestore")
              : null,
            [],
          );
          assert.deepEqual(
            typeof rewind.params === "object" && rewind.params !== null
              ? Reflect.get(rewind.params, "filesToDelete")
              : null,
            [],
          );
          const rewoundLoad = yield* freshFake.takeRequest("droid.load_session");
          assert.deepEqual(rewoundLoad.params, {
            sessionId: "rewound-session",
            autoRejectPermissionRequests: false,
          });
          assert.equal(result.providerThread.id, persistedThread.id);
          assert.equal(result.providerThread.nativeThreadRef?.nativeId, "rewound-session");
          assert.equal(result.providerThread.status, "idle");
          assert.equal(result.providerThread.lastRunOrdinal, target.appRunOrdinal);
        }),
      ),
  );

  it.effect("refuses rewind during an active turn or pending wake work", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input, providerThread } = yield* open(fake);
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        const activeTurn = (yield* runtime.readThreadSnapshot({ providerThread })).providerTurns[0];
        assert.isDefined(activeTurn);
        if (activeTurn === undefined) return;
        const target = {
          type: "provider_turn",
          checkpointId: CheckpointId.make("droid-active-rollback"),
          appRunOrdinal: 1,
          providerTurn: activeTurn,
        } satisfies ProviderAdapter.ProviderAdapterV2RollbackTarget;
        const activeError = yield* Effect.flip(
          runtime.rollbackThread({ providerThread, target, providerThreadTurns: [activeTurn] }),
        );
        assert.instanceOf(activeError, ProviderAdapter.ProviderAdapterRollbackThreadError);
        assert.isEmpty(fake.requestedMethods.filter((method) => method === "droid.execute_rewind"));

        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* fake.emitNotification({
          type: "create_message",
          message: {
            id: "task-completion:rollback-child",
            role: "system",
            content: [{ type: "text", text: "task_id: rollback-child\nreason: completed" }],
          },
        });
        yield* fake.emitRequest("rollback-wake-barrier", "droid.unknown_barrier", {});
        yield* fake.takeReply;
        assert.isTrue(yield* runtime.hasPendingBackgroundWorkForThread!(providerThread));
        const wakeError = yield* Effect.flip(
          runtime.rollbackThread({ providerThread, target, providerThreadTurns: [activeTurn] }),
        );
        assert.instanceOf(wakeError, ProviderAdapter.ProviderAdapterRollbackThreadError);
        assert.isEmpty(fake.requestedMethods.filter((method) => method === "droid.execute_rewind"));
      }),
    ),
  );

  it.effect("refuses rewind when the next turn has no rewindable user message id", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input, providerThread } = yield* open(fake);
        const terminal = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal" ? Deferred.succeed(terminal, undefined) : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(terminal);
        const firstTurn = (yield* runtime.readThreadSnapshot({ providerThread })).providerTurns[0];
        assert.isDefined(firstTurn);
        if (firstTurn === undefined) return;
        const wakeTurn = {
          ...firstTurn,
          ordinal: firstTurn.ordinal + 1,
          nativeTurnRef: {
            driver: firstTurn.nativeTurnRef?.driver ?? DROID_PROVIDER,
            nativeId: "task-completion:rollback-child",
            strength: "strong",
          },
        } satisfies NonNullable<typeof firstTurn>;
        const error = yield* Effect.flip(
          runtime.rollbackThread({
            providerThread,
            target: {
              type: "provider_turn",
              checkpointId: CheckpointId.make("droid-unrewindable-next-turn"),
              appRunOrdinal: 1,
              providerTurn: firstTurn,
            },
            providerThreadTurns: [firstTurn, wakeTurn],
          }),
        );
        assert.instanceOf(error, ProviderAdapter.ProviderAdapterRollbackThreadError);
        assert.isEmpty(fake.requestedMethods.filter((method) => method === "droid.execute_rewind"));
      }),
    ),
  );

  it.effect("refuses rewind while a native background child is still running", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input, providerThread } = yield* open(fake);
        const terminal = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            event.type === "turn.terminal" ? Deferred.succeed(terminal, undefined) : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* fake.takeRequest("droid.initialize_session");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.get_context_breakdown");
        yield* fake.takeRequest("droid.add_user_message");
        yield* fake.emitNotification({
          type: "child_session_available",
          childSessionId: "rollback-pending-child",
          toolUseId: "rollback-pending-tool",
          description: "Still running",
        });
        yield* fake.emitNotification({
          type: "tool_result",
          toolUseId: "rollback-pending-tool",
          content: "Task launched in background.\nTask ID: rollback-pending-child",
        });
        yield* fake.emitRequest("rollback-child-barrier", "droid.unknown_barrier", {});
        yield* fake.takeReply;
        yield* fake.emitNotification({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* Deferred.await(terminal);
        const turn = (yield* runtime.readThreadSnapshot({ providerThread })).providerTurns[0];
        assert.isDefined(turn);
        assert.isTrue(yield* runtime.hasPendingBackgroundWorkForThread!(providerThread));
        if (turn === undefined) return;
        const error = yield* Effect.flip(
          runtime.rollbackThread({
            providerThread,
            target: {
              type: "thread_start",
              checkpointId: CheckpointId.make("droid-pending-child-rollback"),
              appRunOrdinal: 0,
            },
            providerThreadTurns: [turn],
          }),
        );
        assert.instanceOf(error, ProviderAdapter.ProviderAdapterRollbackThreadError);
        assert.isEmpty(fake.requestedMethods.filter((method) => method === "droid.execute_rewind"));
      }),
    ),
  );
});
