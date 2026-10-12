import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeOS from "node:os";
import {
  EnvironmentId,
  ChatAttachmentId,
  MessageId,
  NodeId,
  OrchestrationV2AppThread,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
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
import { droidSystemPromptAppend, makeDroidAdapterV2 } from "./adapter.ts";

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
      readonly lastCallCompactionTokens: number;
    };
    readonly holdAddUserMessageResponse?: boolean;
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
                  if (
                    options.holdAddUserMessageResponse &&
                    !heldAddUserMessageOnce &&
                    frame.method === "droid.add_user_message"
                  ) {
                    heldAddUserMessage = frame;
                    heldAddUserMessageOnce = true;
                  } else {
                    yield* emit(responseFor(frame, options));
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
      emitNotification: (notification: Record<string, unknown>) =>
        emit({
          type: "notification",
          jsonrpc: "2.0",
          method: "droid.session_notification",
          params: { sessionId: "native-session", notification },
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
      readonly lastCallCompactionTokens: number;
    };
  },
) => ({
  type: "response",
  jsonrpc: "2.0",
  id: frame.id,
  factoryProtocolVersion: options.protocolVersion ?? "1.257.0",
  result:
    frame.method === "droid.initialize_session"
      ? { sessionId: "native-session", session: { messages: [] } }
      : frame.method === "droid.get_context_breakdown"
        ? (options.contextBreakdown ?? {})
        : frame.method === "droid.compact_session"
          ? (options.compactSessionResult ?? {})
          : {},
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
) {
  const providerHost = yield* ProviderHost.ProviderHost;
  if (mcpSession !== undefined)
    yield* McpProviderSessions.McpProviderSessions.pipe(
      Effect.flatMap((sessions) => sessions.set(mcpSession)),
    );
  const adapter = yield* makeDroidAdapterV2({
    instanceId,
    environment: {},
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
  const providerThread = yield* runtime.ensureThread({
    threadId,
    modelSelection: selection,
    runtimePolicy: { ...policy, cwd },
  });
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

describe("Droid adapter", () => {
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

  it.effect("folds Droid's repeated automatic compaction reports into one item", () =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
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

  it.effect.each([
    ["foreground", "Task completed.", false],
    ["background", "Task launched in background.\nTask ID: child", true],
  ] as const)("projects only a %s Task in the provider roster", ([, result, expected]) =>
    provide(
      Effect.gen(function* () {
        const fake = yield* makeFakeDroid();
        const { runtime, input } = yield* open(fake);
        const providerThreadEvents: ProviderAdapter.ProviderAdapterV2Event[] = [];
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
            )
              return Deferred.succeed(childReady, undefined);
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
                input: { description: "Background child", prompt: "Do work" },
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
              event.providerThread.pendingBackgroundTasks?.some((task) => task.taskId === childId),
          ),
          expected,
        );
        assert.equal(
          yield* runtime.hasPendingBackgroundWorkForThread!(input.providerThread),
          expected,
        );
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
});
