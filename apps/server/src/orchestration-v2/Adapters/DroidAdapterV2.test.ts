import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  OrchestrationV2AppThread,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  CommandId,
  EventId,
  type OrchestrationV2Subagent,
  type ProviderReplayTranscript,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import { runOrchestratorV2ProviderReplayScenario } from "../testkit/ProviderReplayHarness.ts";
import { provideDeterministicTestRuntime } from "../testkit/DeterministicRuntime.ts";
import * as DroidAdapter from "./DroidAdapterV2.ts";
import * as DroidTestkit from "./DroidAdapterV2.testkit.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";

// Failure cases: wrong protocol/autonomy; duplicate resumed child rows; lost
// wakes or double prompts; unanswered AskUser/approval; foreground interrupt
// killing workers; idle Stop leaving them alive; stale running crash ledger;
// terminal turns leaving open tools; truncated load history presented as full.
// Image failures: nullable paths crossing the SDK boundary; steering silently
// dropping images; non-image attachments misclassified as images.
const instanceId = ProviderInstanceId.make("droid-native-test");
const selection = { instanceId, model: "custom:GPT-6 Luna-12" };
const policy = {
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: "/workspace",
} satisfies ProviderAdapter.ProviderAdapterV2RuntimePolicy;
const threadId = ThreadId.make("droid-test-thread");
const decodeAppThread = Schema.decodeUnknownEffect(OrchestrationV2AppThread);
const decodeEvent = Schema.decodeUnknownEffect(ProviderAdapter.ProviderAdapterV2Event);

const open = Effect.fnUntraced(function* (
  transcript?: ProviderReplayTranscript,
  continuationRequests?: DroidAdapter.DroidAdapterV2Options["continuationRequests"],
  setup: {
    readonly environment?: NodeJS.ProcessEnv;
    readonly responses?: Parameters<typeof DroidTestkit.makeReplayConnection>[2];
    readonly existingProviderThread?: ProviderAdapter.ProviderAdapterV2EnsureThreadInput["existingProviderThread"];
  } = {},
) {
  const replay = yield* DroidTestkit.makeReplayConnection(transcript, false, setup.responses);
  const adapter = DroidAdapter.makeDroidAdapterV2({
    instanceId,
    environment: setup.environment ?? {},
    attachmentsDir: "/tmp/droid-native-test-attachments",
    fileSystem: yield* FileSystem.FileSystem,
    path: yield* Path.Path,
    idAllocator: yield* IdAllocator.IdAllocatorV2,
    makeConnection: replay.open,
    ...(continuationRequests === undefined ? {} : { continuationRequests }),
  });
  const runtime = yield* adapter.openSession({
    threadId,
    providerSessionId: ProviderSessionId.make("droid-test-session"),
    modelSelection: selection,
    runtimePolicy: policy,
  });
  const providerThread = yield* runtime.ensureThread({
    threadId,
    modelSelection: selection,
    runtimePolicy: policy,
    ...(setup.existingProviderThread === undefined
      ? {}
      : { existingProviderThread: setup.existingProviderThread }),
  });
  const now = yield* DateTime.now;
  const appThread = yield* decodeAppThread({
    id: threadId,
    projectId: ProjectId.make("droid-test-project"),
    title: "Droid test",
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
    runId: RunId.make("droid-run"),
    runOrdinal: 1,
    providerTurnOrdinal: 1,
    attemptId: RunAttemptId.make("droid-attempt"),
    rootNodeId: NodeId.make("droid-root"),
    message: {
      messageId: MessageId.make("droid-message"),
      text: "Hello",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: selection,
    runtimePolicy: policy,
  };
  return { adapter, runtime, replay, input };
});
const provide = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer)));

describe("DroidAdapterV2", () => {
  it.effect("sends image paths on initial and steering messages using the SDK shape", () =>
    provide(
      Effect.gen(function* () {
        const { runtime, replay, input } = yield* open();
        const message: typeof input.message = {
          ...input.message,
          attachments: [
            {
              type: "image",
              id: "test-image",
              name: "pixel.png",
              mimeType: "image/png",
              sizeBytes: 68,
            },
            { type: "file", id: "notes", name: "notes.txt", mimeType: "text/plain", sizeBytes: 10 },
          ],
        };
        yield* runtime.startTurn({ ...input, message });
        const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
          driver: DroidAdapter.DROID_PROVIDER,
          nativeTurnId: `turn:${input.attemptId}`,
        });
        yield* runtime.steerTurn({
          threadId,
          runId: input.runId,
          providerThread: input.providerThread,
          providerTurnId,
          message: { ...message, messageId: MessageId.make("image-steer"), text: "Look again" },
        });
        const requests = replay.requests.filter(
          (request) => request.method === "droid.add_user_message",
        );
        assert.deepEqual(
          requests.map((request) => request.params),
          [
            {
              messageId: message.messageId,
              text: message.text,
              imagePaths: ["/tmp/droid-native-test-attachments/test-image.png"],
              userMessageSource: "sdk",
              queuePlacement: "end_of_turn",
            },
            {
              messageId: "image-steer",
              text: "Look again",
              imagePaths: ["/tmp/droid-native-test-attachments/test-image.png"],
              userMessageSource: "sdk",
              queuePlacement: "end_of_loop",
            },
          ],
        );
      }),
    ),
  );
  it.effect("rejects an unresolvable image instead of sending null image paths", () =>
    provide(
      Effect.gen(function* () {
        const { runtime, replay, input } = yield* open();
        const result = yield* Effect.result(
          runtime.startTurn({
            ...input,
            message: {
              ...input.message,
              attachments: [
                {
                  type: "image",
                  id: "../escape",
                  name: "pixel.png",
                  mimeType: "image/png",
                  sizeBytes: 68,
                },
              ],
            },
          }),
        );
        assert.equal(result._tag, "Failure");
        assert.isFalse(replay.methods.includes("droid.add_user_message"));
        yield* runtime.startTurn(input);
        assert.isTrue(replay.methods.includes("droid.add_user_message"));
      }),
    ),
  );
  it.effect("sends a selected native slash command as unchanged user text", () =>
    provide(
      Effect.gen(function* () {
        const { runtime, replay, input } = yield* open();
        yield* runtime.startTurn({
          ...input,
          message: { ...input.message, text: "/review src/example.ts" },
        });
        const request = replay.requests.find(
          (request) => request.method === "droid.add_user_message",
        );
        assert.deepEqual(request?.params, {
          messageId: input.message.messageId,
          text: "/review src/example.ts",
          imagePaths: [],
          userMessageSource: "sdk",
          queuePlacement: "end_of_turn",
        });
        assert.isFalse(replay.methods.some((method) => method.includes("execute_command")));
      }),
    ),
  );
  it.each([
    ["approval-required", "off"],
    ["auto-accept-edits", "low"],
    ["auto", "medium"],
    ["full-access", "high"],
  ])("maps %s to %s", (runtimeMode, expected) => {
    assert.equal(
      DroidAdapter.droidSessionSettings(selection, decodePolicy({ ...policy, runtimeMode }))
        .autonomyLevel,
      expected,
    );
  });

  it("preserves explicit autonomy and plan mode", () => {
    assert.deepEqual(
      DroidAdapter.droidSessionSettings(
        {
          ...selection,
          options: [
            { id: "autonomy_level", value: "normal" },
            { id: "reasoning_effort", value: "low" },
          ],
        },
        { ...policy, interactionMode: "plan" },
      ),
      {
        modelId: selection.model,
        autonomyLevel: "off",
        interactionMode: "spec",
        reasoningEffort: "low",
      },
    );
    assert.equal(
      DroidAdapter.droidSessionSettings(
        { ...selection, options: [{ id: "autonomy_level", value: "unknown" }] },
        decodePolicy({ ...policy, runtimeMode: "auto-accept-edits" }),
      ).autonomyLevel,
      "low",
    );
  });

  it.effect("streams a native turn into validated orchestration artifacts", () =>
    provide(
      Effect.gen(function* () {
        const { runtime, replay, input } = yield* open(yield* DroidTestkit.fixture("simple"));
        const collected = yield* runtime.events.pipe(
          Stream.takeUntil((event) => event.type === "turn.terminal"),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* replay.release;
        const events = yield* Fiber.join(collected);
        for (const event of events) yield* decodeEvent(event);
        assert.isTrue(
          events.some(
            (event) =>
              event.type === "message.updated" && event.message.text === "DROID_NATIVE_3A_OK",
          ),
        );
        assert.equal(events.at(-1)?.type, "turn.terminal");
        assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
        yield* replay.assertComplete;
      }),
    ),
  );

  it.effect.each(["ask_user", "approval"])(
    "answers the recorded %s request through the live UI contract",
    (name) =>
      provide(
        Effect.gen(function* () {
          const { runtime, replay, input } = yield* open(yield* DroidTestkit.fixture(name));
          yield* runtime.startTurn(input);
          const waiting =
            yield* Deferred.make<import("@t3tools/contracts").OrchestrationV2RuntimeRequest>();
          const collected = yield* runtime.events.pipe(
            Stream.tap((event) =>
              event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending"
                ? Deferred.succeed(waiting, event.runtimeRequest)
                : Effect.void,
            ),
            Stream.takeUntil(
              (event) =>
                event.type === "turn_item.updated" &&
                (event.turnItem.type === "user_input_request" ||
                  event.turnItem.type === "approval_request") &&
                event.turnItem.status === "completed",
            ),
            Stream.runCollect,
            Effect.forkChild,
          );
          yield* replay.release;
          const request = yield* Deferred.await(waiting);
          assert.deepEqual(request.responseCapability, {
            type: "live",
            providerSessionId: runtime.providerSessionId,
          });
          yield* runtime.respondToRuntimeRequest({
            requestId: request.id,
            ...(name === "ask_user" ? { answers: { "1": "Blue" } } : { decision: "accept" }),
          });
          yield* replay.awaitReply;
          const events = yield* Fiber.join(collected);
          for (const event of events) yield* decodeEvent(event);
          assert.equal(
            events.filter((event) => event.type === "runtime_request.updated").length,
            2,
          );
          assert.equal(events.filter((event) => event.type === "node.updated").length, 2);
          assert.equal(events.filter((event) => event.type === "turn_item.updated").length, 2);
          assert.deepEqual(replay.replies[0]?.reply, {
            result:
              name === "ask_user"
                ? {
                    answers: [{ index: 1, question: "Which color do you prefer?", answer: "Blue" }],
                    cancelled: false,
                  }
                : { selectedOption: "proceed_once" },
          });
        }),
      ),
  );

  it.effect("ingests a background wake as a continuation without prompting twice", () =>
    provide(
      Effect.gen(function* () {
        const requested = yield* Deferred.make<void>();
        const { runtime, replay, input } = yield* open(
          yield* DroidTestkit.fixture("background_wake"),
          {
            offer: () => Deferred.succeed(requested, undefined).pipe(Effect.asVoid),
          },
        );
        yield* runtime.startTurn(input);
        yield* replay.release;
        yield* Deferred.await(requested);
        yield* replay.drain;
        assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
        assert.isFalse(yield* runtime.hasPendingBackgroundWorkForThread!(input.providerThread));
        const completed = yield* runtime.events.pipe(
          Stream.filter((event) => event.type === "turn.terminal"),
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* runtime.startTurn({
          ...input,
          runId: RunId.make("wake-run"),
          attemptId: RunAttemptId.make("wake-attempt"),
          runOrdinal: 2,
          providerTurnOrdinal: 2,
          message: { ...input.message, createdBy: "agent", creationSource: "provider" },
        });
        const terminalEvents = yield* Fiber.join(completed);
        assert.equal(terminalEvents.length, 2);
        assert.equal(
          replay.methods.filter((method) => method === "droid.add_user_message").length,
          1,
        );
        assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
      }),
    ),
  );
  it.effect("drains an undispatched wake before a user turn and ignores its late completion", () =>
    provide(
      Effect.gen(function* () {
        const { runtime, replay, input } = yield* open();
        yield* runtime.startTurn(input);
        yield* replay.emit({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: input.message.messageId,
        });
        yield* replay.drain;
        yield* replay.emit({
          type: "create_message",
          message: {
            id: "task-completion:worker",
            role: "user",
            content: [
              {
                type: "text",
                text: "task_id: worker\nreason: completed\noutput: DONE\n\nBackground task finished.",
              },
            ],
          },
        });
        yield* replay.emit({
          type: "create_message",
          message: {
            id: "wake-answer",
            role: "assistant",
            content: [{ type: "text", text: "Wake result" }],
          },
        });
        yield* replay.emit({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: "task-completion:worker",
        });
        yield* replay.drain;
        const user = {
          ...input,
          runId: RunId.make("user-race"),
          attemptId: RunAttemptId.make("user-race"),
          message: { ...input.message, messageId: MessageId.make("user-race") },
        };
        yield* runtime.startTurn(user);
        assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
        yield* replay.emit({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: "task-completion:worker",
        });
        yield* replay.drain;
        const snapshot = yield* runtime.readThreadSnapshot({
          providerThread: input.providerThread,
        });
        assert.equal(snapshot.providerTurns.at(-1)?.status, "running");
        yield* replay.emit({
          type: "agent_turn_completed",
          reason: "completed",
          turnId: "user-race",
        });
        yield* replay.drain;
        assert.equal(
          (yield* runtime.readThreadSnapshot({
            providerThread: input.providerThread,
          })).providerTurns.at(-1)?.status,
          "completed",
        );
        assert.isTrue(
          (yield* runtime.readThreadSnapshot({
            providerThread: input.providerThread,
          })).messages.some((message) => message.text === "Wake result"),
        );
      }),
    ),
  );

  it.effect("reads every persisted message instead of the load_session window", () =>
    provide(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = yield* fs.makeTempDirectoryScoped();
        const directory = path.join(home, ".factory", "sessions", "workspace");
        yield* fs.makeDirectory(directory, { recursive: true });
        const sessionId = "12345678-1234-4234-8234-123456789012";
        yield* fs.writeFileString(
          path.join(directory, `${sessionId}.jsonl`),
          Array.from({ length: 120 }, (_, index) =>
            JSON.stringify({
              type: "message",
              id: `message-${index}`,
              timestamp: "2026-10-08T23:00:00Z",
              message: { role: "assistant", content: [{ type: "text", text: `Reply ${index}` }] },
            }),
          ).join("\n") + "\n",
        );
        const replay = yield* DroidTestkit.makeReplayConnection();
        const adapter = DroidAdapter.makeDroidAdapterV2({
          instanceId,
          environment: { HOME: home },
          attachmentsDir: directory,
          fileSystem: fs,
          path,
          idAllocator: yield* IdAllocator.IdAllocatorV2,
          makeConnection: replay.open,
        });
        const runtime = yield* adapter.openSession({
          threadId,
          providerSessionId: ProviderSessionId.make("snapshot-session"),
          modelSelection: selection,
          runtimePolicy: policy,
        });
        const thread = yield* runtime.ensureThread({
          threadId,
          modelSelection: selection,
          runtimePolicy: policy,
        });
        const snapshot = yield* runtime.readThreadSnapshot({
          providerThread: {
            ...thread,
            nativeThreadRef: {
              driver: DroidAdapter.DROID_PROVIDER,
              nativeId: sessionId,
              strength: "strong",
            },
          },
        });
        assert.equal(snapshot.messages.length, 120);
        assert.equal(snapshot.messages[0]?.text, "Reply 0");
      }),
    ),
  );

  it.effect.each(["simple", "background_wake"])(
    "persists the %s replay, run completion, and checkpoints",
    (name) =>
      Effect.gen(function* () {
        const transcript = yield* DroidTestkit.fixture(name).pipe(
          Effect.provide(NodeServices.layer),
        );
        const cwd = yield* checkpointWorkspace(`droid-native-${name}`);
        const secondRunId = (yield* Effect.service(IdAllocator.IdAllocatorV2).pipe(
          Effect.provide(IdAllocator.layer),
        )).derive.run({
          threadId,
          ordinal: 2,
        });
        const scenario = {
          name: "droid-native-simple",
          transcript,
          commands: [
            {
              type: "thread.create",
              commandId: CommandId.make("create-droid"),
              threadId,
              projectId: ProjectId.make("project-droid"),
              title: "Droid",
              createdBy: "user",
              creationSource: "web",
              modelSelection: selection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: cwd,
            },
            {
              type: "message.dispatch",
              commandId: CommandId.make("send-droid"),
              threadId,
              messageId: MessageId.make("message-droid"),
              text: "Hello",
              attachments: [],
              createdBy: "user",
              creationSource: "web",
              modelSelection: selection,
              dispatchMode: { type: "start_immediately" },
            },
          ],
        } satisfies Parameters<typeof runOrchestratorV2ProviderReplayScenario>[0];
        const result = yield* runOrchestratorV2ProviderReplayScenario(
          {
            ...scenario,
            steps: [
              ...scenario.commands.map(
                (command) =>
                  ({
                    type: "dispatch",
                    command,
                  }) satisfies import("../testkit/OrchestratorScenario.ts").OrchestratorV2ScenarioStep,
              ),
              { type: "await_thread_idle", threadId },
              ...(name === "background_wake"
                ? [
                    {
                      type: "await_run_status",
                      threadId,
                      runId: secondRunId,
                      status: "completed",
                    } satisfies import("../testkit/OrchestratorScenario.ts").OrchestratorV2ScenarioStep,
                  ]
                : []),
            ],
          },
          DroidTestkit.DroidOrchestratorReplayHarness,
          { runContinuationWorker: name === "background_wake" },
        ).pipe(provideDeterministicTestRuntime);
        const projection = result.projections.get(threadId);
        assert.equal(projection?.providerThreads.length, 1);
        assert.equal(projection?.providerThreads[0]?.nativeThreadRef?.nativeId, "native-session");
        assert.equal(projection?.runs.at(-1)?.status, "completed");
        assert.isTrue(
          projection?.messages.some((message) =>
            name === "simple"
              ? message.text === "DROID_NATIVE_3A_OK"
              : message.text.includes("DONE"),
          ),
        );
        assert.isAtLeast(projection?.checkpoints.length ?? 0, name === "simple" ? 1 : 2);
        if (name === "background_wake") {
          assert.equal(projection?.runs.length, 2);
          assert.equal(projection?.subagents.length, 1);
          assert.equal(projection?.subagents[0]?.status, "completed");
          assert.isTrue(projection?.runs.every((run) => run.status === "completed"));
        }
      }),
  );

  it.effect.each(["resume", "task_stop"])("replays %s with one stable child row", (name) =>
    provide(
      Effect.gen(function* () {
        const { runtime, replay, input } = yield* open(yield* DroidTestkit.fixture(name));
        const rows: OrchestrationV2Subagent[] = [];
        const settled = yield* Deferred.make<void>();
        const reader = yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              if (event.type !== "subagent.updated") return;
              rows.push(event.subagent);
              if (name === "resume" ? rows.length === 2 : event.subagent.status === "cancelled")
                yield* Deferred.succeed(settled, undefined);
            }),
          ),
          Effect.forkChild,
        );
        yield* runtime.startTurn(input);
        yield* replay.release;
        yield* Deferred.await(settled);
        assert.equal(new Set(rows.map((row) => row.id)).size, 1);
        assert.equal(rows.at(-1)?.nativeTaskRef?.nativeId, "fc2a2f6c-76df-4eaf-89a7-014f8712f83f");
        assert.equal(rows.at(-1)?.status, name === "resume" ? "running" : "cancelled");
        assert.equal(yield* runtime.hasPendingBackgroundWork!, name === "resume");
        yield* replay.assertComplete;
        yield* Fiber.interrupt(reader);
      }),
    ),
  );

  it.effect("preserves background work on interrupt and closes it on idle Stop", () =>
    provide(
      Effect.gen(function* () {
        const { runtime, replay, input } = yield* open(yield* DroidTestkit.fixture("interrupt"));
        yield* runtime.startTurn(input);
        yield* replay.emit({
          type: "child_session_available",
          childSessionId: "child",
          toolUseId: "task",
          subagentType: "worker-luna",
          description: "Worker",
        });
        yield* replay.drain;
        const turn = yield* replay.activeTurn(runtime);
        yield* runtime.interruptTurn({
          providerThread: input.providerThread,
          providerTurnId: turn.id,
        });
        assert.equal(replay.methods.at(-1), "droid.interrupt_session");
        assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
        yield* replay.release;
        yield* replay.drain;
        yield* runtime.interruptTurn({
          providerThread: input.providerThread,
          providerTurnId: turn.id,
          requestRuntimeRestart: true,
        });
        assert.isTrue(replay.closed());
        assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
        yield* replay.assertComplete;
      }),
    ),
  );
  it.effect.each(["running", "completed"])(
    "reconciles a %s child on fresh parent resume and reopens its original row",
    (status) =>
      provide(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const home = yield* fs.makeTempDirectoryScoped();
          const parentId = "12345678-1234-4234-8234-123456789012";
          const childId = "fc2a2f6c-76df-4eaf-89a7-014f8712f83f";
          const state = path.join(home, ".factory", "state");
          const sessions = path.join(home, ".factory", "sessions", "workspace");
          yield* fs.makeDirectory(state, { recursive: true });
          yield* fs.makeDirectory(sessions, { recursive: true });
          yield* fs.writeFileString(
            path.join(state, "task-invocations.json"),
            JSON.stringify({
              invocations: [
                {
                  parentSessionId: parentId,
                  childSessionId: childId,
                  parentToolUseId: "task-original",
                  status,
                  subagentType: "worker-luna",
                  description: "Worker",
                },
                {
                  parentSessionId: parentId,
                  childSessionId: "finished-child",
                  parentToolUseId: "finished-tool",
                  status: "completed",
                },
              ],
            }),
          );
          if (status === "completed")
            yield* fs.writeFileString(
              path.join(sessions, `${childId}.jsonl`),
              [
                JSON.stringify({
                  type: "message",
                  timestamp: "2026-10-08T23:00:00Z",
                  message: { role: "user", content: [{ type: "text", text: "Work" }] },
                }),
                JSON.stringify({
                  type: "message",
                  message: { role: "assistant", content: [{ type: "text", text: "DONE" }] },
                }),
                JSON.stringify({ type: "agent_turn_outcome", reason: "completed" }),
              ].join("\n") + "\n",
            );
          const recording = yield* DroidTestkit.fixture("resume");
          const original = yield* open(recording, undefined, {
            environment: { HOME: home },
            responses: { initializedSessionId: parentId },
          });
          yield* original.runtime.startTurn(original.input);
          yield* original.replay.release;
          yield* original.replay.drain;
          const before = yield* original.runtime.events.pipe(
            Stream.filter((event) => event.type === "subagent.updated"),
            Stream.take(2),
            Stream.runCollect,
          );
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const at = yield* DateTime.now;
          yield* projections.apply({
            id: EventId.make("resume-parent"),
            type: "thread.created",
            threadId,
            occurredAt: at,
            payload: original.input.appThread,
          });
          const previous = {
            ...before[0]!.subagent,
            runId: RunId.make("original-run"),
            prompt: "Original prompt, not ledger description",
          };
          const finished = {
            ...previous,
            id: NodeId.make("finished-child-node"),
            status: "completed" as const,
            nativeTaskRef: {
              driver: DroidAdapter.DROID_PROVIDER,
              nativeId: "finished-child",
              strength: "strong" as const,
            },
          };
          for (const [index, row] of [previous, finished].entries())
            yield* projections.apply({
              id: EventId.make(`resume-child-${index}`),
              type: "subagent.updated",
              threadId,
              occurredAt: at,
              payload: row,
            });
          yield* original.replay.crash;
          yield* original.runtime.events.pipe(
            Stream.filter(
              (event) => event.type === "subagent.updated" && event.subagent.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
          );
          const resumed = yield* open(recording, undefined, {
            environment: { HOME: home },
            existingProviderThread: original.input.providerThread,
          });
          const rows: OrchestrationV2Subagent[] = [];
          const childThreads: ThreadId[] = [];
          const reopened = yield* Deferred.make<void>();
          const reader = yield* resumed.runtime.events.pipe(
            Stream.runForEach((event) =>
              Effect.gen(function* () {
                yield* decodeEvent(event);
                if (event.type === "app_thread.created") childThreads.push(event.appThread.id);
                if (event.type !== "subagent.updated") return;
                rows.push(event.subagent);
                if (event.subagent.status === "cancelled")
                  yield* Deferred.succeed(reopened, undefined);
              }),
            ),
            Effect.forkChild,
          );
          yield* resumed.runtime.startTurn(resumed.input);
          yield* resumed.replay.drain;
          if (status === "running") yield* Deferred.await(reopened);
          if (status === "running") {
            const terminal = rows.find((row) => row.status === "cancelled");
            assert.isDefined(terminal);
            assert.equal(terminal.runId, previous.runId);
            assert.deepEqual(terminal.startedAt, previous.startedAt);
            assert.equal(terminal.id, previous.id);
            assert.equal(terminal.prompt, previous.prompt);
            assert.equal(terminal.childThreadId, previous.childThreadId);
            assert.equal(rows.length, 1);
          } else assert.deepEqual(rows, []);
          assert.isFalse(rows.some((row) => row.id === finished.id));
          assert.deepEqual(
            (yield* projections.getThreadRecords(threadId, ["subagents"])).subagents.find(
              (row) => row.id === finished.id,
            ),
            finished,
          );
          assert.deepEqual(childThreads, []);
          assert.equal(resumed.input.providerThread.id, original.input.providerThread.id);
          assert.equal(resumed.input.providerThread.nativeThreadRef?.nativeId, parentId);
          assert.isTrue(resumed.replay.methods.includes("droid.load_session"));
          yield* Fiber.interrupt(reader);
        }).pipe(Effect.provide(ProjectionStore.layer.pipe(Layer.provide(Sqlite.layerMemory)))),
      ),
  );

  it.effect.each(["native-session", "replacement-session"])(
    "compacts to %s without changing the T3 thread identity",
    (newSessionId) =>
      provide(
        Effect.gen(function* () {
          const { runtime, replay, input } = yield* open(undefined, undefined, {
            responses: { compactedSessionId: newSessionId },
          });
          const collected = yield* runtime.events.pipe(
            Stream.takeUntil((event) => event.type === "turn.terminal"),
            Stream.runCollect,
            Effect.forkChild,
          );
          yield* runtime.compactThread!(input);
          const events = yield* Fiber.join(collected);
          for (const event of events) yield* decodeEvent(event);
          const rebound = events.find((event) => event.type === "provider_thread.updated");
          assert.equal(rebound?.providerThread.id, input.providerThread.id);
          assert.equal(rebound?.providerThread.appThreadId, threadId);
          assert.equal(rebound?.providerThread.nativeThreadRef?.nativeId, newSessionId);
          assert.isFalse(replay.methods.includes("droid.add_user_message"));
          assert.deepEqual(
            replay.requests.find((request) => request.method === "droid.load_session")?.params,
            {
              sessionId: newSessionId,
              autoRejectPermissionRequests: false,
            },
          );
          assert.equal(events.at(-1)?.type, "turn.terminal");
        }),
      ),
  );

  it.effect("settles the root and live children when the captured process crashes", () =>
    provide(
      Effect.gen(function* () {
        const { runtime, replay, input } = yield* open(yield* DroidTestkit.fixture("resume"));
        yield* runtime.startTurn(input);
        yield* replay.release;
        yield* replay.drain;
        const finished = yield* runtime.events.pipe(
          Stream.filter(
            (event) => event.type === "subagent.updated" && event.subagent.status === "failed",
          ),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* replay.crash;
        const events = yield* Fiber.join(finished);
        const failed = events[0];
        assert.equal(failed?.type, "subagent.updated");
        if (failed?.type !== "subagent.updated") return;
        assert.equal(failed.subagent.status, "failed");
        const snapshot = yield* runtime.readThreadSnapshot({
          providerThread: input.providerThread,
        });
        assert.equal(snapshot.providerTurns[0]?.status, "failed");
        assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
        assert.isTrue(replay.closed());
      }),
    ),
  );

  it.effect("rejects incompatible protocol versions and closes only its connection", () =>
    provide(
      Effect.gen(function* () {
        const exit = yield* open(undefined, undefined, {
          responses: { protocolVersion: "unsupported" },
        }).pipe(Effect.exit);
        assert.equal(exit._tag, "Failure");
      }),
    ),
  );
});

const ProviderAdapterPolicy = Schema.Struct({
  runtimeMode: Schema.Literals(["approval-required", "auto-accept-edits", "auto", "full-access"]),
  interactionMode: Schema.Literals(["default", "plan"]),
  cwd: Schema.NullOr(Schema.String),
});
const decodePolicy = Schema.decodeUnknownSync(ProviderAdapterPolicy);
