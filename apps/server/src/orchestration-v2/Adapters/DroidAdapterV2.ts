import {
  AcpRegistrySettings,
  ProviderDriverKind,
  type ModelSelection,
  type NodeId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
  type ProviderInstanceId,
  type RuntimeRequestId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import type * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import type * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import type * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";

import type * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/continuationRequests";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import * as McpProviderSession from "@t3tools/provider-core/server/mcpSession";
import { T3_CODE_ORCHESTRATION_INSTRUCTIONS } from "@t3tools/provider-core/server/orchestrationInstructions";
import type * as AcpRegistrySupport from "../../provider/acp/AcpRegistrySupport.ts";
import {
  findDroidSessionFile,
  followDroidSubagent,
  type DroidSubagentWork,
} from "../../provider/acp/DroidSessionTranscript.ts";
import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { buildRuntimeInstructions } from "../../provider/RuntimeInstructions.ts";
import {
  makeSubagentChildThread,
  makeSubagentConversationArtifacts,
} from "../SubagentProjection.ts";
import * as DroidRpc from "./DroidRpc.ts";
import { makeProviderTextDeltaCoalescer } from "./ProviderTextDeltaCoalescer.ts";

export const DROID_PROVIDER = ProviderDriverKind.make("droid");
const SUPPORTED_PROTOCOL = "1.246.0";
const REGISTRY_SETTINGS = Schema.decodeSync(AcpRegistrySettings)({
  agentId: "factory-droid",
});

export const DroidProviderCapabilitiesV2 = {
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: true,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: true,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    activeSteeringInterruptsTools: false,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: false,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: true,
    supportsDynamicToolCallbacks: true,
  },
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: true,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: true,
    exposesSubagentThreadIds: true,
    emitsSubagentLifecycle: true,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: true,
    acceptsDeveloperContext: true,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: true,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: true,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
  runtimePolicy: { enforcement: "native" },
} satisfies OrchestrationV2ProviderCapabilities;

const AutonomyLevel = Schema.Literals(["off", "low", "medium", "high"]);
const decodeAutonomyLevel = Schema.decodeUnknownOption(AutonomyLevel);
const ToolUse = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  input: Schema.Unknown,
});
const ContentPart = Schema.Struct({
  type: Schema.String,
  text: Schema.optionalKey(Schema.String),
  thinking: Schema.optionalKey(Schema.String),
  id: Schema.optionalKey(Schema.String),
  name: Schema.optionalKey(Schema.String),
  input: Schema.optionalKey(Schema.Unknown),
});
const Message = Schema.Struct({
  id: Schema.String,
  role: Schema.String,
  content: Schema.Array(ContentPart),
  visibility: Schema.optionalKey(Schema.String),
});
const Child = Schema.Struct({
  childSessionId: Schema.String,
  subagentType: Schema.optionalKey(Schema.String),
  description: Schema.optionalKey(Schema.String),
  status: Schema.optionalKey(Schema.String),
});
const SessionResult = Schema.Struct({
  sessionId: Schema.optionalKey(Schema.String),
  session: Schema.Struct({ messages: Schema.Array(Message) }),
  hasOlderMessages: Schema.optionalKey(Schema.Boolean),
  subagentInvocations: Schema.optionalKey(Schema.Array(Child)),
});
const AskUser = Schema.Struct({
  toolCallId: Schema.String,
  questions: Schema.Array(
    Schema.Struct({
      index: Schema.Int,
      topic: Schema.String,
      question: Schema.String,
      options: Schema.Array(Schema.String),
    }),
  ),
});
const Permission = Schema.Struct({
  toolUses: Schema.Array(
    Schema.Struct({
      toolUse: ToolUse,
      confirmationType: Schema.String,
      details: Schema.Unknown,
    }),
  ),
  options: Schema.Array(Schema.Struct({ label: Schema.String, value: Schema.String })),
});
const Ledger = Schema.Struct({
  invocations: Schema.Array(
    Schema.Struct({
      parentSessionId: Schema.String,
      childSessionId: Schema.String,
      parentToolUseId: Schema.String,
      status: Schema.String,
      subagentType: Schema.optionalKey(Schema.String),
      description: Schema.optionalKey(Schema.String),
    }),
  ),
});
const TranscriptRow = Schema.Struct({
  type: Schema.String,
  id: Schema.optionalKey(Schema.String),
  timestamp: Schema.optionalKey(Schema.String),
  message: Schema.optionalKey(
    Schema.Struct({
      role: Schema.String,
      content: Schema.Array(ContentPart),
      visibility: Schema.optionalKey(Schema.String),
    }),
  ),
});
const decodeJsonRow = Schema.decodeUnknownOption(Schema.fromJsonString(TranscriptRow));
const fields = Schema.decodeUnknownOption(Schema.Record(Schema.String, Schema.Unknown));
const stringField = (value: unknown, key: string): string | undefined => {
  const record = fields(value);
  const entry = Option.isSome(record) ? record.value[key] : undefined;
  return typeof entry === "string" ? entry : undefined;
};
const protocolError = (detail: string, cause?: unknown) =>
  new ProviderAdapter.ProviderAdapterProtocolError({
    driver: DROID_PROVIDER,
    detail,
    ...(cause === undefined ? {} : { cause }),
  });
const decode = <A, I>(schema: Schema.Codec<A, I>, value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError((cause) => protocolError("Invalid native Droid payload", cause)),
  );
const nativeRef = (nativeId: string): OrchestrationV2ProviderRef => ({
  driver: DROID_PROVIDER,
  nativeId,
  strength: "strong",
});

export function droidSessionSettings(
  selection: ModelSelection,
  policy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
) {
  const autonomy = selection.options?.find((option) => option.id === "autonomy_level")?.value;
  const mapped = {
    "approval-required": "off",
    "auto-accept-edits": "low",
    auto: "medium",
    "full-access": "high",
  };
  const aliases = new Map([
    ["normal", "off"],
    ["auto-low", "low"],
    ["auto-medium", "medium"],
    ["auto-high", "high"],
  ]);
  const requestedAutonomy =
    autonomy === undefined
      ? mapped[policy.runtimeMode]
      : (aliases.get(String(autonomy)) ?? autonomy);
  const autonomyLevel = Option.getOrElse(
    decodeAutonomyLevel(requestedAutonomy),
    () => mapped[policy.runtimeMode],
  );
  const effort =
    selection.options?.find(
      (option) =>
        option.id === "reasoning_effort" ||
        option.id === "reasoningEffort" ||
        option.id === "effort",
    )?.value ?? policy.reasoningEffort;
  return {
    modelId: selection.model,
    autonomyLevel,
    interactionMode: policy.interactionMode === "plan" ? "spec" : "auto",
    ...(effort === undefined ? {} : { reasoningEffort: String(effort) }),
  };
}

export type DroidAdapterV2Options = {
  readonly instanceId: ProviderInstanceId;
  readonly environment: NodeJS.ProcessEnv;
  readonly attachmentsDir: string;
  readonly fileSystem: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly idAllocator: IdAllocator.IdAllocatorV2["Service"];
  readonly resolver?: Pick<AcpRegistrySupport.AcpRegistryCatalog["Service"], "resolve">;
  readonly childProcessSpawner?: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly continuationRequests?: Pick<
    Context.Service.Shape<typeof ProviderContinuationRequests.ProviderContinuationRequests>,
    "offer"
  >;
  readonly makeConnection?: (
    options: Parameters<typeof DroidRpc.makeConnection>[0],
  ) => Effect.Effect<DroidRpc.DroidRpcConnection, DroidRpc.DroidRpcError, Scope.Scope>;
};

type ActiveTurn = {
  readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly turn: OrchestrationV2ProviderTurn;
  readonly ordinals: Map<string, number>;
  readonly blocks: Map<string, { text: string; reasoning: boolean }>;
  readonly tools: Map<string, typeof ToolUse.Type>;
  interrupted: boolean;
};
type ChildState = {
  task: OrchestrationV2Subagent;
  readonly rootNodeId: NodeId;
  readonly parentRootNodeId: NodeId;
  readonly context: ActiveTurn;
  readonly ordinal: number;
  toolUseId: string;
  follower: Fiber.Fiber<void, never> | null;
  generation: number;
  readonly publishedSteps: Map<string, string | null>;
};
type PendingRequest = {
  readonly reply: Deferred.Deferred<DroidRpc.DroidRpcReply>;
  readonly request: OrchestrationV2RuntimeRequest;
  readonly node: OrchestrationV2ExecutionNode;
  readonly item: OrchestrationV2TurnItem;
  readonly questions?: (typeof AskUser.Type)["questions"];
  readonly options?: (typeof Permission.Type)["options"];
};

export function makeDroidAdapterV2(
  options: DroidAdapterV2Options,
): ProviderAdapter.ProviderAdapterV2Shape {
  const ids = options.idAllocator;
  return {
    instanceId: options.instanceId,
    driver: DROID_PROVIDER,
    getCapabilities: () => Effect.succeed(DroidProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (sessionInput) =>
      Effect.gen(function* () {
        const sessionScope = yield* Effect.scope;
        const now = yield* DateTime.now;
        const session = {
          id: sessionInput.providerSessionId,
          driver: DROID_PROVIDER,
          providerInstanceId: options.instanceId,
          cwd: sessionInput.runtimePolicy.cwd ?? process.cwd(),
          model: sessionInput.modelSelection.model,
          status: "ready",
          capabilities: DroidProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        } satisfies ProviderAdapter.ProviderAdapterV2SessionRuntime["providerSession"];
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const pending = new Map<RuntimeRequestId, PendingRequest>();
        const children = new Map<string, ChildState>();
        const childByToolUseId = new Map<string, ChildState>();
        const turns = new Map<string, OrchestrationV2ProviderTurn>();
        const messages = new Map<string, OrchestrationV2ConversationMessage>();
        let active: ActiveTurn | null = null;
        let lastContext: ActiveTurn | null = null;
        let providerThread: OrchestrationV2ProviderThread | null = null;
        let connection: DroidRpc.DroidRpcConnection | null = null;
        let nativeId: string | null = null;
        let connectionScope: Scope.Closeable | null = null;
        let protocolVersion: string | null = null;
        let wakePending = false;
        let wakeGeneration = 0;
        let wakeBuffer: DroidRpc.DroidRpcNotification[] = [];
        let importedFile: { file: string; size: number; modified: number } | null = null;
        let resumedChildren: typeof SessionResult.Type | null = null;

        const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
          Queue.offer(events, event).pipe(Effect.asVoid);
        const emitItem = (turnItem: OrchestrationV2TurnItem) =>
          emit({ type: "turn_item.updated", driver: DROID_PROVIDER, turnItem });
        const emitNode = (node: OrchestrationV2ExecutionNode) =>
          emit({ type: "node.updated", driver: DROID_PROVIDER, node });
        const ordinal = (context: ActiveTurn, key: string) => {
          const existing = context.ordinals.get(key);
          if (existing !== undefined) return existing;
          const value = context.input.providerTurnOrdinal * 100 + context.ordinals.size + 1;
          context.ordinals.set(key, value);
          return value;
        };
        const itemBase = (context: ActiveTurn, key: string, updatedAt: DateTime.Utc) =>
          ({
            id: ids.derive.turnItemFromProviderItem({ driver: DROID_PROVIDER, nativeItemId: key }),
            threadId: context.input.threadId,
            runId: context.input.runId,
            nodeId: context.input.rootNodeId,
            providerThreadId: context.input.providerThread.id,
            providerTurnId: context.turn.id,
            nativeItemRef: nativeRef(key),
            parentItemId: null,
            ordinal: ordinal(context, key),
            status: "running",
            title: null,
            startedAt: context.turn.startedAt,
            completedAt: null,
            updatedAt,
          }) satisfies Pick<
            OrchestrationV2TurnItem,
            | "id"
            | "threadId"
            | "runId"
            | "nodeId"
            | "providerThreadId"
            | "providerTurnId"
            | "nativeItemRef"
            | "parentItemId"
            | "ordinal"
            | "status"
            | "title"
            | "startedAt"
            | "completedAt"
            | "updatedAt"
          >;
        const emitText = Effect.fnUntraced(function* (
          context: ActiveTurn,
          key: string,
          text: string,
          reasoning: boolean,
          streaming: boolean,
        ) {
          const updatedAt = yield* DateTime.now;
          context.blocks.set(key, { text, reasoning });
          const base = itemBase(context, key, updatedAt);
          const status = streaming ? "running" : "completed";
          if (reasoning) {
            yield* emitItem({
              ...base,
              type: "reasoning",
              title: "Thinking",
              text,
              streaming,
              status,
              completedAt: streaming ? null : updatedAt,
            });
            return;
          }
          const id = ids.derive.messageFromProviderItem({
            driver: DROID_PROVIDER,
            nativeItemId: key,
          });
          const message: OrchestrationV2ConversationMessage = {
            id,
            threadId: context.input.threadId,
            runId: context.input.runId,
            nodeId: context.input.rootNodeId,
            role: "assistant",
            text,
            attachments: [],
            streaming,
            createdBy: "agent",
            creationSource: "provider",
            createdAt: messages.get(id)?.createdAt ?? updatedAt,
            updatedAt,
          };
          messages.set(id, message);
          yield* emit({ type: "message.updated", driver: DROID_PROVIDER, message });
          yield* emitItem({
            ...base,
            type: "assistant_message",
            messageId: id,
            text,
            streaming,
            status,
            completedAt: streaming ? null : updatedAt,
          });
        });
        const textDeltas = yield* makeProviderTextDeltaCoalescer({
          flushIntervalMs: 50,
          emit: (update) => {
            const context = active;
            return context === null || context.turn.id !== update.turnId
              ? Effect.void
              : emitText(
                  context,
                  update.itemId,
                  update.text,
                  update.itemId.endsWith(":thinking"),
                  !update.completed,
                );
          },
        });
        const emitTool = Effect.fnUntraced(function* (
          context: ActiveTurn,
          tool: typeof ToolUse.Type,
          status: OrchestrationV2TurnItem["status"],
          output?: string,
          child?: ChildState,
        ) {
          const updatedAt = yield* DateTime.now;
          const key =
            child === undefined ? tool.id : `${child.task.nativeTaskRef?.nativeId}:${tool.id}`;
          const nodeId = ids.derive.nodeFromProviderItem({
            driver: DROID_PROVIDER,
            nativeItemId: key,
          });
          const base = {
            ...itemBase(context, key, updatedAt),
            threadId: child?.task.childThreadId ?? context.input.threadId,
            runId: child === undefined ? context.input.runId : null,
            nodeId,
            status,
            title: tool.name,
            providerThreadId: child === undefined ? context.input.providerThread.id : null,
            providerTurnId: child === undefined ? context.turn.id : null,
            completedAt: status === "running" ? null : updatedAt,
          };
          yield* emitNode({
            id: nodeId,
            threadId: base.threadId,
            runId: base.runId,
            parentNodeId: child?.rootNodeId ?? context.input.rootNodeId,
            rootNodeId: child?.rootNodeId ?? context.input.rootNodeId,
            kind: "tool_call",
            status,
            countsForRun: false,
            providerThreadId: base.providerThreadId,
            providerTurnId: base.providerTurnId,
            nativeItemRef: nativeRef(key),
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: base.startedAt,
            completedAt: base.completedAt,
          });
          const boundedOutput = output?.slice(0, 20_000);
          if (tool.name === "Execute") {
            yield* emitItem({
              ...base,
              type: "command_execution",
              input: stringField(tool.input, "command") ?? "",
              ...(boundedOutput === undefined ? {} : { output: boundedOutput }),
            });
          } else if (["ApplyPatch", "Edit", "Create"].includes(tool.name)) {
            yield* emitItem({
              ...base,
              type: "file_change",
              fileName:
                stringField(tool.input, "file_path") ??
                stringField(tool.input, "filePath") ??
                tool.name,
              ...(boundedOutput === undefined ? {} : { diffStr: boundedOutput }),
            });
          } else {
            yield* emitItem({
              ...base,
              type: "dynamic_tool",
              toolName: tool.name,
              input: tool.input,
              ...(boundedOutput === undefined ? {} : { output: boundedOutput }),
            });
          }
        });

        const emitChild = Effect.fnUntraced(function* (child: ChildState) {
          const task = child.task;
          const context = child.context;
          const turnId = context.turn.id;
          const updatedAt = task.updatedAt;
          yield* emitNode({
            id: task.id,
            threadId: task.threadId,
            runId: task.runId,
            parentNodeId: task.parentNodeId,
            rootNodeId: child.parentRootNodeId,
            kind: "subagent",
            status: task.status,
            countsForRun: false,
            providerThreadId: task.providerThreadId,
            providerTurnId: turnId,
            nativeItemRef: task.nativeTaskRef,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: task.startedAt,
            completedAt: task.completedAt,
          });
          if (task.childThreadId !== null) {
            yield* emitNode({
              id: child.rootNodeId,
              threadId: task.childThreadId,
              runId: null,
              parentNodeId: null,
              rootNodeId: child.rootNodeId,
              kind: "root_turn",
              status: task.status,
              countsForRun: false,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: task.nativeTaskRef,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: task.startedAt,
              completedAt: task.completedAt,
            });
          }
          yield* emit({ type: "subagent.updated", driver: DROID_PROVIDER, subagent: task });
          yield* emitItem({
            id: ids.derive.turnItemFromProviderItem({
              driver: DROID_PROVIDER,
              nativeItemId: `child:${task.nativeTaskRef?.nativeId}`,
            }),
            threadId: task.threadId,
            runId: task.runId,
            nodeId: task.id,
            providerThreadId: task.providerThreadId,
            providerTurnId: turnId,
            nativeItemRef: task.nativeTaskRef,
            parentItemId: null,
            ordinal: child.ordinal,
            status: task.status,
            title: task.title,
            startedAt: task.startedAt,
            completedAt: task.completedAt,
            updatedAt,
            type: "subagent",
            subagentId: task.id,
            origin: task.origin,
            driver: DROID_PROVIDER,
            providerInstanceId: options.instanceId,
            childThreadId: task.childThreadId,
            prompt: task.prompt,
            result: task.result,
          });
        });
        const finishChild = Effect.fnUntraced(function* (
          child: ChildState,
          status: "completed" | "failed" | "cancelled",
          result: string | null,
        ) {
          child.generation++;
          if (child.follower !== null) {
            yield* Fiber.interrupt(child.follower);
            child.follower = null;
          }
          child.task = {
            ...child.task,
            status,
            result,
            completedAt: yield* DateTime.now,
            updatedAt: yield* DateTime.now,
          };
          if (!reconcile) yield* emitChild(child);
        });
        const publishChildWork = Effect.fnUntraced(function* (
          child: ChildState,
          work: DroidSubagentWork,
        ) {
          const childThreadId = child.task.childThreadId;
          if (childThreadId === null) return;
          const updatedAt = yield* DateTime.now;
          if (work.model !== undefined)
            child.task = { ...child.task, model: work.model, updatedAt };
          for (const [index, step] of work.steps.entries()) {
            const stepId = step.kind === "tool" ? step.id : `message:${index}`;
            const output = step.kind === "tool" ? step.output : step.text;
            if (child.publishedSteps.has(stepId) && child.publishedSteps.get(stepId) === output)
              continue;
            child.publishedSteps.set(stepId, output);
            if (step.kind === "tool") {
              yield* emitTool(
                child.context,
                { id: step.id, name: step.name, input: step.input },
                step.output === null ? "running" : step.isError ? "failed" : "completed",
                step.output ?? undefined,
                child,
              );
            } else {
              const key = `child:${child.task.nativeTaskRef?.nativeId}:message:${index}`;
              const artifacts = makeSubagentConversationArtifacts({
                messageId: ids.derive.messageFromProviderItem({
                  driver: DROID_PROVIDER,
                  nativeItemId: key,
                }),
                turnItemId: ids.derive.turnItemFromProviderItem({
                  driver: DROID_PROVIDER,
                  nativeItemId: key,
                }),
                threadId: childThreadId,
                rootNodeId: child.rootNodeId,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: nativeRef(key),
                role: "assistant",
                text: step.text,
                ordinal: index + 101,
                now: updatedAt,
              });
              yield* emit({
                type: "message.updated",
                driver: DROID_PROVIDER,
                message: artifacts.message,
              });
              yield* emitItem(artifacts.turnItem);
            }
          }
        });
        const startFollower = Effect.fnUntraced(function* (childId: string, child: ChildState) {
          const file = yield* findDroidSessionFile({
            sessionId: childId,
            environment: options.environment,
          });
          if (file === undefined) return;
          const generation = child.generation;
          const follow = followDroidSubagent({
            file,
            startedAtMillis: DateTime.toEpochMillis(child.task.startedAt ?? now),
          });
          let reported: DroidSubagentWork | null = null;
          child.follower = yield* Stream.merge(
            options.fileSystem.watch(file),
            Stream.make(undefined),
          ).pipe(
            Stream.mapEffect(() => follow),
            Stream.takeUntil((state) => state.outcome !== undefined),
            Stream.runForEach((state) =>
              Effect.gen(function* () {
                if (child.generation !== generation) return;
                if (state.work !== reported) {
                  reported = state.work;
                  yield* publishChildWork(child, state.work);
                }
                if (state.outcome !== undefined) {
                  // The follower cannot interrupt itself.
                  child.follower = null;
                  yield* finishChild(child, state.outcome, state.assistantMessages.at(-1) ?? null);
                }
              }),
            ),
            Effect.ignore,
            Effect.forkIn(sessionScope),
          );
        });
        const registerChild = Effect.fnUntraced(function* (
          notice: typeof Child.Type & { readonly toolUseId: string },
          context: ActiveTurn,
          reconcile = false,
        ) {
          const existing = children.get(notice.childSessionId);
          if (existing?.follower !== null && existing?.follower !== undefined)
            yield* Fiber.interrupt(existing.follower);
          const updatedAt = yield* DateTime.now;
          const key = `child:${notice.childSessionId}`;
          const nodeId = ids.derive.nodeFromProviderItem({
            driver: DROID_PROVIDER,
            nativeItemId: key,
          });
          const parentNodeId = context.input.rootNodeId;
          const tool = context.tools.get(notice.toolUseId);
          const childThreadId = ids.derive.threadFromProviderThread({
            driver: DROID_PROVIDER,
            providerInstanceId: options.instanceId,
            nativeThreadId: notice.childSessionId,
          });
          const task: OrchestrationV2Subagent = {
            ...(reconcile && existing !== undefined ? existing.task : {}),
            id: nodeId,
            threadId: existing?.task.threadId ?? context.input.threadId,
            runId: existing?.task.runId ?? context.input.runId,
            parentNodeId: existing?.task.parentNodeId ?? parentNodeId,
            origin: "provider_native",
            createdBy: "agent",
            driver: DROID_PROVIDER,
            providerInstanceId: options.instanceId,
            providerThreadId: context.input.providerThread.id,
            childThreadId,
            nativeTaskRef: nativeRef(notice.childSessionId),
            prompt:
              reconcile && existing !== undefined
                ? existing.task.prompt
                : (stringField(tool?.input, "prompt") ??
                  existing?.task.prompt ??
                  notice.description ??
                  ""),
            title: notice.description ?? notice.subagentType ?? null,
            model: existing?.task.model ?? null,
            status: "running",
            result: null,
            startedAt: reconcile && existing !== undefined ? existing.task.startedAt : updatedAt,
            completedAt: null,
            updatedAt,
          };
          const child: ChildState = {
            task,
            context,
            rootNodeId: ids.derive.nodeFromProviderItem({
              driver: DROID_PROVIDER,
              nativeItemId: `${key}:root`,
            }),
            parentRootNodeId: parentNodeId,
            ordinal: existing?.ordinal ?? ordinal(context, key),
            toolUseId: notice.toolUseId,
            follower: null,
            generation: (existing?.generation ?? 0) + 1,
            publishedSteps: existing?.publishedSteps ?? new Map(),
          };
          children.set(notice.childSessionId, child);
          if (existing !== undefined) childByToolUseId.delete(existing.toolUseId);
          childByToolUseId.set(notice.toolUseId, child);
          if (existing === undefined && !reconcile) {
            const appThread = makeSubagentChildThread({
              parentThread: context.input.appThread,
              childThreadId,
              parentNodeId: nodeId,
              activeProviderThreadId: null,
              providerInstanceId: options.instanceId,
              modelSelection: context.input.modelSelection,
              title: task.title ?? "Droid subagent",
              now: updatedAt,
              createdBy: "agent",
              creationSource: "provider",
            });
            yield* emit({ type: "app_thread.created", driver: DROID_PROVIDER, appThread });
          }
          yield* emitChild(child);
          if (!reconcile) {
            const promptKey = `${key}:prompt:${notice.toolUseId}`;
            const artifacts = makeSubagentConversationArtifacts({
              messageId: ids.derive.messageFromProviderItem({
                driver: DROID_PROVIDER,
                nativeItemId: promptKey,
              }),
              turnItemId: ids.derive.turnItemFromProviderItem({
                driver: DROID_PROVIDER,
                nativeItemId: promptKey,
              }),
              senderThreadId: context.input.threadId,
              threadId: childThreadId,
              rootNodeId: child.rootNodeId,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: nativeRef(promptKey),
              role: "user",
              text: task.prompt,
              ordinal: context.input.providerTurnOrdinal * 100,
              now: updatedAt,
            });
            yield* emit({
              type: "message.updated",
              driver: DROID_PROVIDER,
              message: artifacts.message,
            });
            yield* emitItem(artifacts.turnItem);
          }
          if (!reconcile) yield* startFollower(notice.childSessionId, child);
          return child;
        });

        const finishRequests = Effect.fnUntraced(function* () {
          for (const entry of pending.values()) {
            yield* Deferred.succeed(entry.reply, {
              result:
                entry.questions === undefined
                  ? { selectedOption: "cancel" }
                  : { cancelled: true, answers: [] },
            });
          }
        });
        const terminal = Effect.fnUntraced(function* (
          status: "completed" | "interrupted" | "cancelled" | "failed",
          broken = false,
          nativeTurnId?: string,
        ) {
          const context = active;
          if (context === null) return;
          yield* textDeltas.flushTurn(context.turn.id);
          active = null;
          const completedAt = yield* DateTime.now;
          for (const [key, block] of context.blocks) {
            yield* emitText(context, key, block.text, block.reasoning, false);
          }
          for (const tool of context.tools.values()) {
            yield* emitTool(context, tool, status === "completed" ? "interrupted" : status);
          }
          context.tools.clear();
          yield* finishRequests();
          const turn: OrchestrationV2ProviderTurn = {
            ...context.turn,
            status,
            completedAt,
            ...(nativeTurnId === undefined ? {} : { nativeTurnRef: nativeRef(nativeTurnId) }),
          };
          turns.set(turn.id, turn);
          if (importedFile !== null) {
            const stat = yield* options.fileSystem.stat(importedFile.file).pipe(Effect.option);
            if (Option.isSome(stat))
              importedFile = {
                file: importedFile.file,
                size: Number(stat.value.size),
                modified: Option.isSome(stat.value.mtime) ? stat.value.mtime.value.getTime() : 0,
              };
          }
          yield* emit({
            type: "provider_turn.updated",
            driver: DROID_PROVIDER,
            providerTurn: turn,
          });
          if (status === "failed") {
            yield* emit({
              type: "turn.terminal",
              driver: DROID_PROVIDER,
              providerThreadId: context.input.providerThread.id,
              providerTurnId: turn.id,
              runOrdinal: context.input.runOrdinal,
              failureItemOrdinal: ordinal(context, "failure"),
              status,
              failure: makeProviderFailure({
                class: broken ? "transport_error" : "provider_error",
                message: broken
                  ? "The native Droid process exited."
                  : "Droid did not complete the turn.",
                retryable: false,
              }),
              threadDisposition: broken ? "broken" : "reusable",
            });
          } else {
            yield* emit({
              type: "turn.terminal",
              driver: DROID_PROVIDER,
              providerThreadId: context.input.providerThread.id,
              providerTurnId: turn.id,
              runOrdinal: context.input.runOrdinal,
              status,
              failure: null,
              threadDisposition: broken ? "broken" : "reusable",
            });
          }
        });
        const handleRequest = Effect.fnUntraced(function* (request: DroidRpc.DroidRpcRequest) {
          const context = active;
          if (context === null) {
            // A wake or idle child has no owning run for a UI request; cancellation is safer than
            // attaching approval to a later, unrelated user turn.
            yield* Effect.logWarning("Cancelling native Droid request without an active run", {
              method: request.method,
              requestId: request.id,
            });
            return undefined;
          }
          if (request.method !== "droid.ask_user" && request.method !== "droid.request_permission")
            return undefined;
          const asking =
            request.method === "droid.ask_user"
              ? yield* decode(AskUser, request.params)
              : undefined;
          const permission =
            asking === undefined ? yield* decode(Permission, request.params) : undefined;
          const requestId = yield* ids.allocate.runtimeRequest({
            driver: DROID_PROVIDER,
            providerTurnId: context.turn.id,
            nativeRequestId: String(request.id),
          });
          const updatedAt = yield* DateTime.now;
          const nodeId = ids.derive.approvalNode({ requestId });
          const kind =
            asking !== undefined
              ? "user_input"
              : permission?.toolUses.some((entry) => entry.confirmationType === "exec")
                ? "command"
                : permission?.toolUses.some((entry) => entry.confirmationType === "apply_patch")
                  ? "file-change"
                  : "permission";
          const node: OrchestrationV2ExecutionNode = {
            id: nodeId,
            threadId: context.input.threadId,
            runId: context.input.runId,
            parentNodeId: context.input.rootNodeId,
            rootNodeId: context.input.rootNodeId,
            kind: asking === undefined ? "approval_request" : "user_input_request",
            status: "waiting",
            countsForRun: false,
            providerThreadId: context.input.providerThread.id,
            providerTurnId: context.turn.id,
            nativeItemRef: nativeRef(String(request.id)),
            runtimeRequestId: requestId,
            checkpointScopeId: null,
            startedAt: updatedAt,
            completedAt: null,
          };
          const runtimeRequest: OrchestrationV2RuntimeRequest = {
            id: requestId,
            nodeId,
            providerTurnId: context.turn.id,
            nativeRequestRef: nativeRef(String(request.id)),
            kind,
            status: "pending",
            responseCapability: { type: "live", providerSessionId: sessionInput.providerSessionId },
            createdAt: updatedAt,
            resolvedAt: null,
          };
          const base = {
            ...itemBase(context, `request:${request.id}`, updatedAt),
            id: ids.derive.approvalTurnItem({ requestId }),
            nodeId,
            status: "waiting",
          } satisfies Pick<
            OrchestrationV2TurnItem,
            | "id"
            | "threadId"
            | "runId"
            | "nodeId"
            | "providerThreadId"
            | "providerTurnId"
            | "nativeItemRef"
            | "parentItemId"
            | "ordinal"
            | "status"
            | "title"
            | "startedAt"
            | "completedAt"
            | "updatedAt"
          >;
          const item: OrchestrationV2TurnItem =
            asking === undefined
              ? {
                  ...base,
                  type: "approval_request",
                  requestId,
                  requestKind: kind === "command" || kind === "file-change" ? kind : "permission",
                  prompt: permission?.toolUses.map((entry) => entry.toolUse.name).join(", "),
                }
              : {
                  ...base,
                  type: "user_input_request",
                  requestId,
                  questions: asking.questions.map((question) => ({
                    id: String(question.index),
                    header: question.topic,
                    question: question.question,
                    options: question.options.map((label) => ({
                      label,
                      description: label,
                      value: label,
                    })),
                    allowCustomAnswer: true,
                    multiSelect: false,
                  })),
                };
          const reply = yield* Deferred.make<DroidRpc.DroidRpcReply>();
          pending.set(requestId, {
            reply,
            request: runtimeRequest,
            node,
            item,
            ...(asking === undefined ? {} : { questions: asking.questions }),
            ...(permission === undefined ? {} : { options: permission.options }),
          });
          yield* emitNode(node);
          yield* emit({ type: "runtime_request.updated", driver: DROID_PROVIDER, runtimeRequest });
          yield* emitItem(item);
          const answer = yield* Deferred.await(reply);
          pending.delete(requestId);
          const resolvedAt = yield* DateTime.now;
          yield* emit({
            type: "runtime_request.updated",
            driver: DROID_PROVIDER,
            runtimeRequest: { ...runtimeRequest, status: "resolved", resolvedAt },
          });
          yield* emitNode({ ...node, status: "completed", completedAt: resolvedAt });
          yield* emitItem({
            ...item,
            status: "completed",
            completedAt: resolvedAt,
            updatedAt: resolvedAt,
          });
          return answer;
        });

        const handleNotification = Effect.fnUntraced(function* (
          notice: DroidRpc.DroidRpcNotification,
        ) {
          const notification = notice.notification;
          const type = notification["type"];
          if (notice.sessionId !== undefined && nativeId !== null && notice.sessionId !== nativeId)
            return;
          if (type === "create_message") {
            const message = yield* decode(Message, notification["message"]);
            if (message.id.startsWith("task-completion:")) {
              const text = message.content.map((part) => part.text ?? "").join("");
              const childId = /^task_id: (\S+)$/mu.exec(text)?.[1];
              const reason = /^reason: (\S+)$/mu.exec(text)?.[1];
              const child = childId === undefined ? undefined : children.get(childId);
              if (child?.task.status === "running")
                yield* finishChild(
                  child,
                  reason === "completed"
                    ? "completed"
                    : reason === "cancelled"
                      ? "cancelled"
                      : "failed",
                  /^output: ([\s\S]*?)\n\nBackground task/u.exec(text)?.[1] ?? null,
                );
            }
            if (
              message.id.startsWith("task-completion:") &&
              active === null &&
              lastContext !== null
            ) {
              wakeBuffer.push(notice);
              if (!wakePending) {
                wakePending = true;
                const generation = ++wakeGeneration;
                yield* (
                  options.continuationRequests?.offer({
                    threadId: lastContext.input.threadId,
                    providerThreadId: lastContext.input.providerThread.id,
                    driver: DROID_PROVIDER,
                    detail: "Native Droid background task completed.",
                    delivery: "adapter_buffered",
                    dispatchIfCurrent: (effect) =>
                      Effect.suspend(() =>
                        wakePending && wakeGeneration === generation
                          ? effect.pipe(Effect.asSome)
                          : Effect.succeedNone,
                      ),
                    clearIfCurrent: () =>
                      Effect.sync(() => {
                        if (wakeGeneration === generation) {
                          wakePending = false;
                          wakeBuffer = [];
                        }
                      }),
                  }) ?? Effect.void
                );
              }
              return;
            }
          }
          if (active === null) {
            if (wakePending) wakeBuffer.push(notice);
            return;
          }
          const context = active;
          if (type === "child_session_available") {
            const child = yield* decode(
              Schema.Struct({ ...Child.fields, toolUseId: Schema.String }),
              notification,
            );
            yield* registerChild(child, context);
          } else if (type === "create_message") {
            const message = yield* decode(Message, notification["message"]);
            if (message.role !== "assistant") return;
            const text = message.content
              .filter((part) => part.type === "text")
              .map((part) => part.text ?? "")
              .join("\n\n");
            const thinking = message.content
              .filter((part) => part.type === "thinking")
              .map((part) => part.thinking ?? "")
              .join("\n\n");
            if (text.length > 0)
              yield* textDeltas.complete({
                turnId: context.turn.id,
                itemId: message.id,
                finalText: text,
              });
            if (thinking.length > 0)
              yield* textDeltas.complete({
                turnId: context.turn.id,
                itemId: `${message.id}:thinking`,
                finalText: thinking,
              });
            for (const part of message.content) {
              if (part.type !== "tool_use") continue;
              const tool = yield* decode(ToolUse, part);
              context.tools.set(tool.id, tool);
              yield* emitTool(context, tool, "running");
            }
          } else if (type === "assistant_text_delta" || type === "thinking_text_delta") {
            const messageId = stringField(notification, "messageId");
            const delta = stringField(notification, "textDelta");
            if (messageId === undefined || delta === undefined) return;
            const reasoning = type === "thinking_text_delta";
            const key = reasoning ? `${messageId}:thinking` : messageId;
            yield* textDeltas.append({ turnId: context.turn.id, itemId: key, delta });
          } else if (type === "assistant_text_complete" || type === "thinking_text_complete") {
            const messageId = stringField(notification, "messageId");
            if (messageId === undefined) return;
            const reasoning = type === "thinking_text_complete";
            const key = reasoning ? `${messageId}:thinking` : messageId;
            yield* textDeltas.complete({ turnId: context.turn.id, itemId: key, emitEmpty: false });
          } else if (type === "tool_result") {
            const toolUseId = stringField(notification, "toolUseId");
            if (toolUseId === undefined) return;
            const tool = context.tools.get(toolUseId);
            const output = stringField(notification, "content") ?? "";
            if (tool !== undefined) {
              yield* emitTool(
                context,
                tool,
                notification["isError"] === true ? "failed" : "completed",
                output,
              );
              if (tool.name === "TaskStop" && notification["isError"] !== true) {
                const childId = stringField(tool.input, "task_id");
                const child = childId === undefined ? undefined : children.get(childId);
                if (child !== undefined) yield* finishChild(child, "cancelled", output);
              }
              context.tools.delete(toolUseId);
            }
            if (!output.startsWith("Task launched in background.")) {
              const child = childByToolUseId.get(toolUseId);
              if (child !== undefined && child.task.status === "running") {
                yield* finishChild(
                  child,
                  notification["isError"] === true ? "failed" : "completed",
                  output,
                );
              }
            }
          } else if (type === "agent_turn_completed") {
            const reason = stringField(notification, "reason");
            yield* terminal(
              context.interrupted
                ? "interrupted"
                : reason === "completed" || reason === "spec_handoff"
                  ? "completed"
                  : reason === "cancelled" || reason === "permission_rejected"
                    ? "cancelled"
                    : "failed",
              false,
              stringField(notification, "turnId"),
            );
          }
        });

        const requireConnection = Effect.fnUntraced(function* () {
          if (connection === null) return yield* protocolError("No live native Droid session");
          return connection;
        });
        const closeConnection = Effect.fnUntraced(function* () {
          const owned = connection;
          connection = null;
          wakeGeneration++;
          wakePending = false;
          wakeBuffer = [];
          yield* finishRequests();
          if (owned !== null) yield* owned.close;
          for (const child of children.values()) {
            if (child.task.status === "running")
              yield* finishChild(child, "cancelled", "The parent Droid process stopped.");
          }
          if (connectionScope !== null) {
            yield* Scope.close(connectionScope, Exit.void);
            connectionScope = null;
          }
        });
        const reconcileChildren = Effect.fnUntraced(function* (
          result: typeof SessionResult.Type,
          context: ActiveTurn,
        ) {
          if (nativeId === null) return;
          const home = options.environment.FACTORY_HOME_OVERRIDE ?? options.environment.HOME;
          const ledger =
            home === undefined
              ? []
              : yield* options.fileSystem
                  .readFileString(
                    options.path.join(home, ".factory", "state", "task-invocations.json"),
                  )
                  .pipe(
                    Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(Ledger))),
                    Effect.map((data) =>
                      data.invocations.filter((row) => row.parentSessionId === nativeId),
                    ),
                    Effect.orElseSucceed(() => []),
                  );
          const byId = new Map(ledger.map((row) => [row.childSessionId, row]));
          for (const entry of result.subagentInvocations ?? []) {
            if (!byId.has(entry.childSessionId))
              byId.set(entry.childSessionId, {
                ...entry,
                parentSessionId: nativeId,
                parentToolUseId: `child:${entry.childSessionId}`,
                status: entry.status ?? "running",
              });
          }
          for (const entry of byId.values()) {
            if (entry.status !== "running") continue;
            const child = yield* registerChild(
              { ...entry, toolUseId: entry.parentToolUseId },
              context,
              true,
            );
            // A fresh parent has no in-process workers. The ledger's "running"
            // is not liveness evidence: Droid leaves it behind after SIGKILL.
            yield* finishChild(
              child,
              "cancelled",
              "The owning Droid process is no longer running.",
            );
          }
        });
        const openConnection = Effect.fnUntraced(
          function* (
            selection: ModelSelection,
            policy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
            resumeId?: string,
          ) {
            if (connection !== null && nativeId === resumeId) return;
            if (connection !== null) yield* closeConnection();
            const childScope = yield* Scope.fork(sessionScope, "sequential");
            connectionScope = childScope;
            const mcp = McpProviderSession.readMcpProviderSession(sessionInput.threadId);
            const env = McpProviderSession.withAgentDeviceEnvironment(
              {
                ...options.environment,
                FACTORY_DISABLE_SETTINGS_PERSISTENCE: "1",
              },
              mcp,
            );
            const cwd = policy.cwd ?? options.environment.PWD ?? process.cwd();
            const transportOptions = {
              cwd,
              env,
              onRequest: (request: DroidRpc.DroidRpcRequest) =>
                handleRequest(request).pipe(
                  Effect.mapError(
                    (cause) => new DroidRpc.DroidRpcError({ operation: "handler", cause }),
                  ),
                ),
            };
            let rpc: DroidRpc.DroidRpcConnection;
            if (options.makeConnection !== undefined) {
              rpc = yield* options
                .makeConnection({ ...transportOptions, command: "droid" })
                .pipe(Effect.provideService(Scope.Scope, childScope));
            } else {
              if (options.resolver === undefined || options.childProcessSpawner === undefined) {
                return yield* protocolError(
                  "Native Droid construction requires its managed binary resolver and process spawner",
                );
              }
              const resolved = yield* options.resolver.resolve(REGISTRY_SETTINGS, cwd, env);
              rpc = yield* DroidRpc.makeConnection({
                ...transportOptions,
                command: resolved.spawn.command,
                env: resolved.spawn.env ?? env,
              }).pipe(
                Effect.provideService(
                  ChildProcessSpawner.ChildProcessSpawner,
                  options.childProcessSpawner,
                ),
                Effect.provideService(Scope.Scope, childScope),
              );
            }
            connection = rpc;
            const mcpServers =
              mcp === undefined
                ? []
                : [
                    {
                      type: "http",
                      name: "t3-code",
                      url: mcp.endpoint,
                      headers: [{ name: "Authorization", value: mcp.authorizationHeader }],
                    },
                  ];
            const settings = droidSessionSettings(selection, policy);
            const response = yield* rpc.request(
              resumeId === undefined ? "droid.initialize_session" : "droid.load_session",
              resumeId === undefined
                ? {
                    machineId: "default",
                    cwd,
                    ...settings,
                    mcpServers,
                    autoRejectPermissionRequests: false,
                    systemPrompt: {
                      type: "preset",
                      preset: "droid",
                      append:
                        buildRuntimeInstructions({ harness: "Factory Droid" }) +
                        T3_CODE_ORCHESTRATION_INSTRUCTIONS,
                    },
                  }
                : { sessionId: resumeId, mcpServers, autoRejectPermissionRequests: false },
            );
            protocolVersion = response.factoryProtocolVersion ?? null;
            if (protocolVersion !== SUPPORTED_PROTOCOL) {
              yield* closeConnection();
              return yield* protocolError("The native Droid protocol version is not supported");
            }
            const result = yield* decode(SessionResult, response.result);
            nativeId = resumeId ?? result.sessionId ?? null;
            if (nativeId === null)
              return yield* protocolError("Droid initialization did not return a session id");
            if (resumeId !== undefined) {
              yield* rpc.request("droid.update_session_settings", settings);
              // Resume precedes the run subscription. Publish reconciliation
              // only once startTurn provides a routable run/turn owner.
              resumedChildren = result;
              const file = yield* findDroidSessionFile({
                sessionId: nativeId,
                environment: options.environment,
              });
              if (file !== undefined) {
                const stat = yield* options.fileSystem.stat(file);
                importedFile = {
                  file,
                  size: Number(stat.size),
                  modified: Option.isSome(stat.mtime) ? stat.mtime.value.getTime() : 0,
                };
              }
            }
            yield* rpc.notifications.pipe(
              Stream.runForEach((notice) => handleNotification(notice)),
              Effect.catchCause(() =>
                Effect.gen(function* () {
                  if (connection !== rpc) return;
                  connection = null;
                  yield* rpc.close;
                  wakePending = false;
                  wakeBuffer = [];
                  yield* terminal("failed", true);
                  for (const child of children.values()) {
                    if (child.task.status === "running")
                      yield* finishChild(child, "failed", "The parent Droid process exited.");
                  }
                }),
              ),
              Effect.forkIn(childScope),
            );
          },
          Effect.onError(() => closeConnection()),
        );
        const makeThread = Effect.fnUntraced(function* (
          input: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
        ) {
          if (input.existingProviderThread !== undefined) {
            providerThread = input.existingProviderThread;
            yield* openConnection(
              input.modelSelection,
              input.runtimePolicy,
              input.existingProviderThread.nativeThreadRef?.nativeId ?? undefined,
            );
            if (nativeId === null) return yield* protocolError("No native Droid session id");
            providerThread = {
              ...input.existingProviderThread,
              nativeThreadRef: nativeRef(nativeId),
              providerSessionId: session.id,
              status: "idle",
              updatedAt: yield* DateTime.now,
            } satisfies OrchestrationV2ProviderThread;
            return providerThread;
          }
          if (providerThread !== null) return providerThread;
          yield* openConnection(input.modelSelection, input.runtimePolicy);
          if (nativeId === null) return yield* protocolError("No native Droid session id");
          const createdAt = yield* DateTime.now;
          providerThread = {
            id: ids.derive.providerThread({
              driver: DROID_PROVIDER,
              providerInstanceId: options.instanceId,
              nativeThreadId: nativeId,
            }),
            driver: DROID_PROVIDER,
            providerInstanceId: options.instanceId,
            providerSessionId: session.id,
            appThreadId: input.threadId,
            ownerNodeId: null,
            nativeThreadRef: nativeRef(nativeId),
            nativeConversationHeadRef: null,
            status: "idle",
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            forkedFrom: null,
            createdAt,
            updatedAt: createdAt,
          };
          return providerThread;
        });
        // The native SDK accepts local imagePaths and reads/encodes the images
        // itself. Initial and queued steering messages use the same wire shape.
        const imagePathsFor = (message: ProviderAdapter.ProviderAdapterV2TurnInput["message"]) =>
          Effect.forEach(
            message.attachments.filter((attachment) => attachment.type === "image"),
            (attachment) => {
              const imagePath = resolveAttachmentPath({
                attachmentsDir: options.attachmentsDir,
                attachment,
              });
              return imagePath === null
                ? protocolError(`Cannot resolve Droid image attachment: ${attachment.name}`)
                : Effect.succeed(imagePath);
            },
          );
        const startTurn = Effect.fnUntraced(function* (
          input: ProviderAdapter.ProviderAdapterV2TurnInput,
          compact = false,
        ) {
          if (active !== null) return yield* protocolError("A native Droid turn is already active");
          const isContinuation =
            input.message.createdBy === "agent" && input.message.creationSource === "provider";
          const imagePaths = isContinuation || compact ? [] : yield* imagePathsFor(input.message);
          const resumedId = input.providerThread.nativeThreadRef?.nativeId;
          if (connection === null)
            yield* openConnection(
              input.modelSelection,
              input.runtimePolicy,
              resumedId ?? undefined,
            );
          if (importedFile !== null && !isContinuation) {
            const stat = yield* options.fileSystem.stat(importedFile.file);
            const modified = Option.isSome(stat.mtime) ? stat.mtime.value.getTime() : 0;
            if (Number(stat.size) !== importedFile.size || modified !== importedFile.modified) {
              // An external CLI can modify an imported transcript without a lock.
              yield* closeConnection();
              yield* openConnection(
                input.modelSelection,
                input.runtimePolicy,
                resumedId ?? undefined,
              );
            }
          }
          const rpc = yield* requireConnection();
          if (!isContinuation)
            yield* rpc.request(
              "droid.update_session_settings",
              droidSessionSettings(input.modelSelection, input.runtimePolicy),
            );
          const startedAt = yield* DateTime.now;
          const context: ActiveTurn = {
            input,
            turn: {
              id: ids.derive.providerTurn({
                driver: DROID_PROVIDER,
                nativeTurnId: `turn:${input.attemptId}`,
              }),
              providerThreadId: input.providerThread.id,
              nodeId: input.rootNodeId,
              runAttemptId: input.attemptId,
              nativeTurnRef: nativeRef(input.message.messageId),
              ordinal: input.providerTurnOrdinal,
              status: "running",
              startedAt,
              completedAt: null,
            },
            ordinals: new Map(),
            blocks: new Map(),
            tools: new Map(),
            interrupted: false,
          };
          active = context;
          lastContext = context;
          providerThread = input.providerThread;
          turns.set(context.turn.id, context.turn);
          yield* emit({
            type: "provider_turn.updated",
            driver: DROID_PROVIDER,
            providerTurn: context.turn,
          });
          if (resumedChildren !== null) {
            const result = resumedChildren;
            resumedChildren = null;
            yield* reconcileChildren(result, context);
          }
          if (compact) {
            if (wakePending || children.values().some((child) => child.task.status === "running")) {
              yield* terminal("failed");
              return yield* protocolError(
                "Stop background work before compacting the native Droid session",
              );
            }
            const response = yield* rpc.request("droid.compact_session", {}, "5 minutes");
            const result = yield* decode(
              Schema.Struct({ newSessionId: Schema.String }),
              response.result,
            );
            nativeId = result.newSessionId;
            providerThread = {
              ...input.providerThread,
              nativeThreadRef: nativeRef(nativeId),
              updatedAt: yield* DateTime.now,
            };
            yield* rpc.request("droid.load_session", {
              sessionId: nativeId,
              autoRejectPermissionRequests: false,
            });
            yield* emit({
              type: "provider_thread.updated",
              driver: DROID_PROVIDER,
              providerThread,
            });
            yield* terminal("completed");
            return;
          }
          if (isContinuation) {
            const buffer = wakeBuffer;
            wakeBuffer = [];
            wakePending = false;
            if (buffer.length === 0) return yield* terminal("completed");
            for (const notice of buffer) yield* handleNotification(notice);
            return;
          }
          yield* rpc.request("droid.add_user_message", {
            messageId: input.message.messageId,
            text: input.message.text,
            imagePaths,
            userMessageSource: "sdk",
            queuePlacement: "end_of_turn",
          });
        }).pipe(Effect.onError(() => terminal("failed")));
        const snapshot = Effect.fnUntraced(function* (
          input: ProviderAdapter.ProviderAdapterV2ReadThreadSnapshotInput,
        ) {
          const id = input.providerThread.nativeThreadRef?.nativeId;
          if (id === undefined || id === null)
            return yield* protocolError("Missing native Droid session id");
          const file = yield* findDroidSessionFile({
            sessionId: id,
            environment: options.environment,
          });
          const persisted: OrchestrationV2ConversationMessage[] = [];
          if (file !== undefined) {
            const content = yield* options.fileSystem.readFileString(file);
            for (const line of content.split("\n")) {
              const row = decodeJsonRow(line);
              if (
                Option.isNone(row) ||
                row.value.message === undefined ||
                row.value.id === undefined
              )
                continue;
              const message = row.value.message;
              if (
                message.visibility === "llm_only" ||
                (message.role !== "user" && message.role !== "assistant")
              )
                continue;
              const text = message.content
                .filter((part) => part.type === "text")
                .map((part) => part.text ?? "")
                .join("\n\n");
              if (text.length === 0) continue;
              persisted.push({
                id: ids.derive.messageFromProviderItem({
                  driver: DROID_PROVIDER,
                  nativeItemId: row.value.id,
                }),
                threadId: input.providerThread.appThreadId ?? sessionInput.threadId,
                runId: null,
                nodeId: null,
                role: message.role,
                text,
                attachments: [],
                streaming: false,
                createdBy: message.role === "user" ? "user" : "agent",
                creationSource: "provider",
                createdAt:
                  row.value.timestamp === undefined
                    ? now
                    : Option.getOrElse(DateTime.make(row.value.timestamp), () => now),
                updatedAt:
                  row.value.timestamp === undefined
                    ? now
                    : Option.getOrElse(DateTime.make(row.value.timestamp), () => now),
              });
            }
          }
          return {
            providerThread: input.providerThread,
            providerTurns: [...turns.values()],
            messages: file === undefined ? [...messages.values()] : persisted,
            runtimeRequests: [...pending.values()].map((entry) => entry.request),
            providerPayload: { factoryProtocolVersion: protocolVersion },
          };
        });
        yield* Effect.addFinalizer(() => closeConnection());
        return {
          instanceId: options.instanceId,
          driver: DROID_PROVIDER,
          providerSessionId: session.id,
          providerSession: session,
          events: Stream.fromEffectRepeat(Queue.take(events)),
          hasPendingBackgroundWork: Effect.sync(
            () => wakePending || children.values().some((child) => child.task.status === "running"),
          ),
          // Children already have run-owned lifecycle artifacts. Wake output
          // belongs to the next run, not to the finished root's subscription.
          hasPendingBackgroundWorkForThread: () => Effect.succeed(false),
          ensureThread: (input) =>
            makeThread(input).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterEnsureThreadError({
                    driver: DROID_PROVIDER,
                    threadId: input.threadId,
                    cause,
                  }),
              ),
            ),
          resumeThread: (input) =>
            makeThread({
              threadId: input.threadId ?? input.providerThread.appThreadId ?? sessionInput.threadId,
              existingProviderThread: input.providerThread,
              modelSelection: input.modelSelection ?? sessionInput.modelSelection,
              runtimePolicy: input.runtimePolicy ?? sessionInput.runtimePolicy,
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterResumeThreadError({
                    driver: DROID_PROVIDER,
                    providerSessionId: session.id,
                    providerThreadId: input.providerThread.id,
                    cause,
                  }),
              ),
            ),
          startTurn: (input) =>
            startTurn(input).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterTurnStartError({
                    driver: DROID_PROVIDER,
                    threadId: input.threadId,
                    providerThreadId: input.providerThread.id,
                    runId: input.runId,
                    cause,
                  }),
              ),
            ),
          steerTurn: (input) =>
            Effect.gen(function* () {
              if (active?.turn.id !== input.providerTurnId)
                return yield* protocolError("The requested Droid turn is not active");
              const rpc = yield* requireConnection();
              yield* rpc.request("droid.add_user_message", {
                messageId: input.message.messageId,
                text: input.message.text,
                imagePaths: yield* imagePathsFor(input.message),
                userMessageSource: "sdk",
                queuePlacement: "end_of_loop",
              });
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterSteerRunError({
                    driver: DROID_PROVIDER,
                    providerThreadId: input.providerThread.id,
                    providerTurnId: input.providerTurnId,
                    cause,
                  }),
              ),
            ),
          interruptTurn: (input) =>
            Effect.gen(function* () {
              if (active === null) {
                yield* closeConnection();
                return;
              }
              if (active.turn.id !== input.providerTurnId)
                return yield* protocolError("The requested Droid turn is not active");
              active.interrupted = true;
              const rpc = yield* requireConnection();
              yield* finishRequests();
              yield* rpc
                .request("droid.interrupt_session", {})
                .pipe(Effect.onError(() => terminal("interrupted")));
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterInterruptError({
                    driver: DROID_PROVIDER,
                    providerThreadId: input.providerThread.id,
                    providerTurnId: input.providerTurnId,
                    cause,
                  }),
              ),
            ),
          respondToRuntimeRequest: (input) =>
            Effect.gen(function* () {
              const entry = pending.get(input.requestId);
              if (entry === undefined)
                return yield* protocolError("No pending native Droid request");
              if (entry.questions !== undefined) {
                const answers = entry.questions.flatMap((question) => {
                  const answer = input.answers?.[String(question.index)];
                  return answer === undefined
                    ? []
                    : [
                        {
                          index: question.index,
                          question: question.question,
                          answer: Array.isArray(answer)
                            ? answer
                                .filter((value): value is string => typeof value === "string")
                                .join(", ")
                            : typeof answer === "string"
                              ? answer
                              : String(answer ?? ""),
                        },
                      ];
                });
                yield* Deferred.succeed(entry.reply, {
                  result: { answers, cancelled: answers.length === 0 },
                });
              } else {
                const requested =
                  input.decision === "accept"
                    ? "proceed_once"
                    : input.decision === "acceptForSession" || input.decision === "acceptAlways"
                      ? "proceed_always"
                      : "cancel";
                const selectedOption = entry.options?.some((option) => option.value === requested)
                  ? requested
                  : "cancel";
                yield* Deferred.succeed(entry.reply, { result: { selectedOption } });
              }
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                    driver: DROID_PROVIDER,
                    requestId: input.requestId,
                    cause,
                  }),
              ),
            ),
          readThreadSnapshot: (input) =>
            snapshot(input).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                    driver: DROID_PROVIDER,
                    providerThreadId: input.providerThread.id,
                    cause,
                  }),
              ),
            ),
          // Rewind is before a USER message and changes session identity. A T3
          // checkpoint is after a provider turn; that cursor mapping is not yet
          // implemented, so refuse before checkpoint rollback changes files.
          rollbackThread: (input) =>
            Effect.fail(
              new ProviderAdapter.ProviderAdapterRollbackThreadError({
                driver: DROID_PROVIDER,
                providerThreadId: input.providerThread.id,
                cause: protocolError(
                  "Droid rewind requires a verified after-turn checkpoint to before-user-message cursor mapping",
                ),
              }),
            ),
          forkThread: (input) =>
            Effect.fail(
              new ProviderAdapter.ProviderAdapterForkThreadError({
                driver: DROID_PROVIDER,
                providerThreadId: input.sourceProviderThread.id,
                cause: protocolError(
                  "Native Droid fork identity and history semantics have not been verified",
                ),
              }),
            ),
          compactThread: (input) =>
            startTurn(input, true).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterTurnStartError({
                    driver: DROID_PROVIDER,
                    threadId: input.threadId,
                    providerThreadId: input.providerThread.id,
                    runId: input.runId,
                    cause,
                  }),
              ),
            ),
        } satisfies ProviderAdapter.ProviderAdapterV2SessionRuntime;
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver: DROID_PROVIDER,
              providerSessionId: sessionInput.providerSessionId,
              cause,
            }),
        ),
      ),
  };
}
