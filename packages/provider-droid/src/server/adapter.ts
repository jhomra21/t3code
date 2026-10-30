import {
  DROID_DEFAULT_MODEL,
  ProviderDriverKind,
  type ModelSelection,
  type NodeId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderRetry,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2PlanArtifact,
  type OrchestrationV2PlanStep,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
  type ProviderInstanceId,
  type ServerProviderModel,
  type ProviderApprovalOption,
  type RuntimeRequestId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import type * as Context from "effect/Context";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import {
  makeProviderFailure,
  makeProviderRetryTurnItem,
} from "@t3tools/provider-core/server/failure";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";

import { t3CodeOrchestrationInstructions } from "@t3tools/provider-core/server/orchestrationInstructions";
import {
  isProviderNativeImageAttachment,
  providerMessageTextWithAttachmentPaths,
} from "@t3tools/provider-core/server/attachmentPrompt";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as McpProviderSession from "@t3tools/provider-core/server/mcpSession";
import { backgroundWorkNotification } from "@t3tools/provider-core/server/notification";

import { buildRuntimeInstructions } from "@t3tools/provider-core/server/runtimeInstructions";
import {
  makeSubagentChildThread,
  makeSubagentConversationArtifacts,
} from "@t3tools/provider-core/server/subagentProjection";
import * as DroidRpc from "./rpc.ts";
import { isAcceptedDroidProtocolVersion } from "./protocolVersion.ts";
import {
  findDroidSessionDirectory,
  makeDroidSubagentFollower,
  DroidSessionWatch,
  DroidSessionRead,
  readDroidDefinitionModel,
  droidSubagentStepSignature,
  type DroidSubagentWork,
} from "./sessionTranscript.ts";

export const DROID_PROVIDER = ProviderDriverKind.make("droid");
// These versions have been verified in replay; accept compatible 1.x versions from 1.246.0 onward.
const VERIFIED_PROTOCOLS = new Set(["1.246.0", "1.252.0", "1.257.0", "1.286.0"]);
const SIGN_IN_LINK_INSTRUCTIONS = `\n\n<sign_in_links>
When a tool result contains a sign-in or authorization link, such as one from ConnectApp, paste the full link in your reply to the user. Tool output is collapsed in T3 Code and the user may not see it.
</sign_in_links>`;
const DROID_EFFORT_BY_COMPLEXITY = new Map([
  ["light", "low"],
  ["medium", "medium"],
  ["heavy", "high"],
]);
const droidChildModelSelection = (
  instanceId: ProviderInstanceId,
  model: string,
  effort: string | undefined,
): ModelSelection => ({
  instanceId,
  model,
  ...(effort === undefined ? {} : { options: [{ id: "reasoning_effort", value: effort }] }),
});
// Droid's native Task children are provider-owned: T3 cannot stop or recover them, so
// the only subagent path Droid is given is T3's delegate_task. Droid also lists MCP tools
// as deferred (its MCP tool search), so they must be loaded once before the first call.
const DROID_TOOL_ACCESS = `The \`t3-code\` tools are named \`t3-code___<tool>\` and Droid lists them as deferred. Load one with \`ToolSearch\` using \`select:t3-code___delegate_task\`, then call it. When \`ToolSearch\` replies "Already loaded", the tool is callable now: call it instead of searching again. Treat a T3 tool as unavailable only when a call to that tool returned an error, and quote that error to the user.`;

export function droidSystemPromptAppend(hasT3Mcp: boolean): string {
  return (
    buildRuntimeInstructions({ harness: "Factory Droid" }) +
    (hasT3Mcp
      ? t3CodeOrchestrationInstructions({ delegateOnly: true, toolAccess: DROID_TOOL_ACCESS })
      : "") +
    SIGN_IN_LINK_INSTRUCTIONS
  );
}
const droidProviderCapabilitiesV2 = {
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
    canRollbackThread: true,
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
    streamsToolOutput: true,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: true,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: true,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: true,
  },
  planning: {
    emitsPlanUpdated: true,
    emitsTodoList: true,
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
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: true,
    providerCanRollbackConversation: true,
    providerRollbackReturnsSnapshot: true,
    providerCanReadConversationSnapshot: false,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
  runtimePolicy: { enforcement: "native" },
} satisfies OrchestrationV2ProviderCapabilities;

const ToolUse = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  input: Schema.Unknown,
});
const ApprovalFile = Schema.Struct({
  filePath: Schema.optionalKey(Schema.String),
  operation: Schema.optionalKey(Schema.String),
});
const ApprovalDetails = Schema.Struct({
  type: Schema.optionalKey(Schema.String),
  fullCommand: Schema.optionalKey(Schema.String),
  filePath: Schema.optionalKey(Schema.String),
  files: Schema.optionalKey(Schema.Array(ApprovalFile)),
});
const decodeApprovalDetails = Schema.decodeUnknownOption(ApprovalDetails);
const ApplyPatchResultFile = Schema.Struct({
  file_path: Schema.String,
  display_operation: Schema.Literals(["create", "update", "delete"]),
});
const ApplyPatchResult = Schema.Struct({
  files: Schema.Array(ApplyPatchResultFile),
});
const decodeApplyPatchResult = Schema.decodeUnknownOption(Schema.fromJsonString(ApplyPatchResult));
const MAX_PRESENTATION_LINES = 20;
const MAX_PRESENTATION_LINE_LENGTH = 2_000;
const MAX_TODO_INPUT_LENGTH = 20_000;
const MAX_TODO_LINES = 100;
const ToolProgressUpdate = Schema.Struct({
  toolUseId: Schema.String,
  toolName: Schema.Literal("Execute"),
  update: Schema.Struct({
    type: Schema.Literal("status"),
    timestamp: Schema.Finite,
    fullOutput: Schema.optionalKey(Schema.String),
  }),
});
const decodeToolProgressUpdate = Schema.decodeUnknownOption(ToolProgressUpdate);
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
const AssociatedSessions = Schema.Struct({
  associatedSessionIds: Schema.optionalKey(Schema.Array(Schema.String)),
});
const fields = Schema.decodeUnknownOption(Schema.Record(Schema.String, Schema.Unknown));
const decodeAssociatedSessions = Schema.decodeUnknownOption(AssociatedSessions);
const stringField = (value: unknown, key: string): string | undefined => {
  const record = fields(value);
  const entry = Option.isSome(record) ? record.value[key] : undefined;
  return typeof entry === "string" ? entry : undefined;
};
const boundedToolOutput = (output: string) => output.slice(0, 20_000);
const boundedLines = (lines: ReadonlyArray<string>) => {
  const visible = lines
    .slice(0, MAX_PRESENTATION_LINES)
    .map((line) => line.slice(0, MAX_PRESENTATION_LINE_LENGTH));
  const remaining = lines.length - visible.length;
  return remaining > 0 ? `${visible.join("\n")}\n+${remaining} more` : visible.join("\n");
};
const droidApprovalPrompt = (permission: typeof Permission.Type) => {
  const tools = permission.toolUses;
  const execTools = tools.filter((entry) => entry.confirmationType === "exec");
  if (execTools.length > 0) {
    const commands = execTools.map((entry) => decodeApprovalDetails(entry.details));
    if (
      commands.every((details) => Option.isSome(details) && details.value.fullCommand !== undefined)
    )
      return boundedLines(
        commands.flatMap((details) =>
          Option.isSome(details) && details.value.fullCommand !== undefined
            ? [details.value.fullCommand]
            : [],
        ),
      );
  }
  const patchTools = tools.filter((entry) => entry.confirmationType === "apply_patch");
  if (patchTools.length > 0) {
    const decoded = patchTools.map((entry) => decodeApprovalDetails(entry.details));
    if (decoded.every(Option.isSome)) {
      const changes = decoded.flatMap((details) => {
        if (Option.isNone(details)) return [];
        const files = details.value.files;
        if (files !== undefined && files.length > 0) {
          if (files.some((file) => file.filePath === undefined || file.operation === undefined))
            return [];
          return files.map((file) => `${file.operation} ${file.filePath}`);
        }
        return details.value.filePath === undefined ? [] : [details.value.filePath];
      });
      if (changes.length > 0) return boundedLines(changes);
    }
  }
  return tools.map((entry) => entry.toolUse.name).join(", ");
};
const droidTodoSteps = (input: unknown): ReadonlyArray<OrchestrationV2PlanStep> | undefined => {
  const todos = stringField(input, "todos");
  if (todos === undefined || todos.length > MAX_TODO_INPUT_LENGTH) return undefined;
  const lines = todos.split("\n").filter((line) => line.length > 0);
  if (lines.length === 0 || lines.length > MAX_TODO_LINES) return undefined;
  const steps: OrchestrationV2PlanStep[] = [];
  for (const line of lines) {
    if (line.length > MAX_PRESENTATION_LINE_LENGTH) return undefined;
    const match = /^(\d+)\. \[(in_progress|pending|completed)\] (.+)$/u.exec(line);
    if (match === null) return undefined;
    const [, number, status, text] = match;
    if (number === undefined || status === undefined || text === undefined) return undefined;
    steps.push({
      id: `todo-${number}`,
      text,
      status:
        status === "in_progress" ? "running" : status === "completed" ? "completed" : "pending",
    });
  }
  return steps;
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
const isRollbackThreadError = Schema.is(ProviderAdapter.ProviderAdapterRollbackThreadError);
/**
 * `contextBudget` is the window Droid compacts against. `lastCallCompactionTokens` is the size
 * of the last model call, which is what compaction measures, but it is stale after compaction
 * until the next model call. Until then, `usedTokens` estimates the whole transcript and counts
 * image base64 as text, so it can be more than twice as large.
 */
const decodeContextBreakdown = Schema.decodeUnknownEffect(
  Schema.Struct({
    contextBudget: Schema.Number,
    usedTokens: Schema.Number,
    lastCallCompactionTokens: Schema.optionalKey(Schema.Number),
  }),
);
const decodeNotice = <A, I>(schema: Schema.Codec<A, I>, value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.catch(() => Effect.logWarning("Skipping malformed native Droid notification payload")),
  );
const nativeRef = (nativeId: string): OrchestrationV2ProviderRef => ({
  driver: DROID_PROVIDER,
  nativeId,
  strength: "strong",
});

function droidSessionSettings(
  selection: ModelSelection,
  policy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
  defaultModel?: string,
  supportedEfforts: ReadonlyArray<string> = [],
) {
  const autonomyLevel = {
    "approval-required": "off",
    "auto-accept-edits": "low",
    auto: "medium",
    "full-access": "high",
  }[policy.runtimeMode];
  const effort = selection.options?.find((option) => option.id === "reasoning_effort")?.value;
  const reasoningEffort =
    effort !== undefined && supportedEfforts.includes(String(effort)) ? String(effort) : undefined;
  return {
    ...(selection.model === DROID_DEFAULT_MODEL
      ? defaultModel === undefined
        ? {}
        : { modelId: defaultModel }
      : { modelId: selection.model }),
    autonomyLevel,
    interactionMode: "auto",
    ...(reasoningEffort === undefined ? {} : { reasoningEffort }),
  };
}

export type DroidAdapterV2Options = {
  readonly instanceId: ProviderInstanceId;
  readonly environment: NodeJS.ProcessEnv;
  readonly binaryPath: string;
  readonly modelCatalog?: Effect.Effect<ReadonlyArray<ServerProviderModel>>;
  readonly continuationRequests?: Pick<
    Context.Service.Shape<typeof ProviderContinuationRequests.ProviderContinuationRequests>,
    "offer"
  >;
};

type ActiveTurn = {
  readonly isWake: boolean;
  readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly turn: OrchestrationV2ProviderTurn;
  readonly ordinals: Map<string, number>;
  readonly itemStartedAt: Map<string, DateTime.Utc>;
  readonly blocks: Map<string, { text: string; reasoning: boolean }>;
  readonly tools: Map<string, typeof ToolUse.Type>;
  readonly toolProgress: Map<string, { timestamp: number; output: string }>;
  readonly malformedToolSearchFailureIds: Set<string>;
  readonly toolSearchCompletion: Deferred.Deferred<void>;
  toolSearchStopped: boolean;
  toolSearchFailure: ReturnType<typeof makeProviderFailure> | undefined;
  lastError: string | undefined;
  providerRetry:
    | {
        readonly itemOrdinal: number;
        readonly failure: ReturnType<typeof makeProviderFailure>;
        readonly retry: OrchestrationV2ProviderRetry;
        readonly startedAt: DateTime.Utc;
      }
    | undefined;
  interrupted: boolean;
};
type ChildState = {
  task: OrchestrationV2Subagent;
  background: boolean;
  readonly rootNodeId: NodeId;
  readonly parentRootNodeId: NodeId;
  readonly context: ActiveTurn;
  readonly ordinal: number;
  toolUseId: string;
  activityOffset: number | undefined;
  follower: {
    readonly scope: Scope.Closeable;
    handle: Effect.Success<ReturnType<typeof makeDroidSubagentFollower>> | null;
    fiber: Fiber.Fiber<void, unknown> | null;
  } | null;
  readonly publishedSteps: Map<string, string>;
  pendingCompletion: {
    readonly status: "completed" | "failed" | "cancelled";
    readonly result: string | null;
  } | null;
};
type PendingRequest = {
  readonly reply: Deferred.Deferred<DroidRpc.DroidRpcReply>;
  readonly request: OrchestrationV2RuntimeRequest;
  readonly node: OrchestrationV2ExecutionNode;
  readonly item: OrchestrationV2TurnItem;
  readonly questions?: (typeof AskUser.Type)["questions"];
  readonly options?: (typeof Permission.Type)["options"];
  /** What T3 answered, recorded on the request once it resolves. */
  response?: Pick<OrchestrationV2RuntimeRequest, "decision" | "answers">;
};

export const makeDroidAdapterV2 = Effect.fn("makeDroidAdapterV2")(function* (
  options: DroidAdapterV2Options,
) {
  const ids = yield* IdAllocator.IdAllocatorV2;
  const providerHost = yield* ProviderHost.ProviderHost;
  const fileSystem = yield* FileSystem.FileSystem;
  const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const watchDirectory = yield* DroidSessionWatch;
  const readAppend = yield* DroidSessionRead;
  const resolveSettings = Effect.fnUntraced(function* (
    selection: ModelSelection,
    policy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
  ) {
    const catalog = options.modelCatalog === undefined ? [] : yield* options.modelCatalog;
    const defaultModel = catalog.find((model) => model.isDefault && !model.isCustom)?.slug;
    const selectedModel = selection.model === DROID_DEFAULT_MODEL ? defaultModel : selection.model;
    const descriptor = catalog
      .find((model) => model.slug === selectedModel)
      ?.capabilities?.optionDescriptors?.find((candidate) => candidate.id === "reasoning_effort");
    const efforts =
      descriptor?.type === "select" ? descriptor.options.map((option) => option.id) : [];
    return droidSessionSettings(selection, policy, defaultModel, efforts);
  });
  const canonicalWorkspacePath = (requestedPath: string) =>
    fileSystem.realPath(requestedPath).pipe(Effect.orElseSucceed(() => requestedPath));
  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: DROID_PROVIDER,
    getCapabilities: Effect.fn("DroidAdapter.getCapabilities")(() =>
      Effect.succeed(droidProviderCapabilitiesV2),
    ),
    planSelectionTransition: Effect.fn("DroidAdapter.planSelectionTransition")(() =>
      Effect.succeed(turnScopedSelectionTransition()),
    ),
    openSession: Effect.fn("DroidAdapter.openSession")(
      function* (sessionInput: ProviderAdapter.ProviderAdapterV2OpenSessionInput) {
        if (sessionInput.runtimePolicy.interactionMode === "plan")
          return yield* protocolError("Factory Droid does not expose T3's proposed-plan mode");
        const sessionScope = yield* Effect.scope;
        const now = yield* DateTime.now;
        const cwd = yield* canonicalWorkspacePath(
          sessionInput.runtimePolicy.cwd ?? providerHost.paths.cwd,
        );
        const session = {
          id: sessionInput.providerSessionId,
          driver: DROID_PROVIDER,
          providerInstanceId: options.instanceId,
          cwd,
          model: sessionInput.modelSelection.model,
          status: "ready",
          capabilities: droidProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        } satisfies ProviderAdapter.ProviderAdapterV2SessionRuntime["providerSession"];
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const pending = new Map<RuntimeRequestId, PendingRequest>();
        const children = new Map<string, ChildState>();
        const childByToolUseId = new Map<string, ChildState>();
        const reopenedChildren = new Set<string>();
        const contextWindows = new Map<string, number>();
        let contextUsageAfterCompaction = false;
        const turns = new Map<string, OrchestrationV2ProviderTurn>();
        const messages = new Map<string, OrchestrationV2ConversationMessage>();
        const messageTurns = new Map<string, OrchestrationV2ProviderTurn["id"]>();
        const todoPlanIds = new Map<string, OrchestrationV2PlanArtifact["id"]>();
        const todoStepSignatures = new Map<string, string>();
        let active: ActiveTurn | null = null;
        let lastContext: ActiveTurn | null = null;
        let providerThread: OrchestrationV2ProviderThread | null = null;
        let connection: DroidRpc.DroidRpcConnection | null = null;
        let nativeId: string | null = null;
        let connectionScope: Scope.Closeable | null = null;
        let wakePending = false;
        let wakeGeneration = 0;
        let wakeBuffer: DroidRpc.DroidRpcNotification[] = [];
        let wakeOwner: ActiveTurn | null = null;
        let nativeNotificationTurnId: string | null = null;
        let settingsModel: string | null = null;

        const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
          Queue.offer(events, event).pipe(Effect.asVoid);
        const emitItem = (turnItem: OrchestrationV2TurnItem) =>
          emit({ type: "turn_item.updated", driver: DROID_PROVIDER, turnItem });
        const syncChildRoster = Effect.fnUntraced(function* () {
          if (providerThread === null) return;
          const roster = Array.from(children.values())
            .filter((child) => child.task.status === "running" && child.background)
            .map((child) => ({
              taskId: child.task.nativeTaskRef?.nativeId ?? String(child.task.id),
              kind: "subagent" as const,
              ...(child.task.title?.trim() ? { description: child.task.title.trim() } : {}),
              ...(child.task.childThreadId === null
                ? {}
                : { childThreadId: child.task.childThreadId }),
            }));
          const current = providerThread.pendingBackgroundTasks ?? [];
          if (
            current.length === roster.length &&
            current.every(
              (task, index) =>
                task.taskId === roster[index]?.taskId &&
                task.kind === roster[index]?.kind &&
                task.description === roster[index]?.description &&
                task.childThreadId === roster[index]?.childThreadId,
            )
          )
            return;
          providerThread = {
            ...providerThread,
            pendingBackgroundTasks: roster,
            updatedAt: yield* DateTime.now,
          };
          yield* emit({ type: "provider_thread.updated", driver: DROID_PROVIDER, providerThread });
        });
        const emitTodoList = Effect.fnUntraced(function* (
          context: ActiveTurn,
          nativeItemId: string,
          steps: ReadonlyArray<OrchestrationV2PlanStep>,
        ) {
          const signature = steps
            .map(
              (step) =>
                `${step.id.length}:${step.id}${step.status}${step.text.length}:${step.text}`,
            )
            .join("");
          if (todoStepSignatures.get(nativeItemId) === signature) return;
          todoStepSignatures.set(nativeItemId, signature);
          const updatedAt = yield* DateTime.now;
          const nativeId = `todo_list:${nativeItemId}`;
          let planId = todoPlanIds.get(nativeId);
          if (planId === undefined) {
            planId = yield* ids.allocate.plan({
              threadId: context.input.threadId,
              runId: context.input.runId,
              driver: DROID_PROVIDER,
            });
            todoPlanIds.set(nativeId, planId);
          }
          const nodeId = ids.derive.nodeFromProviderItem({
            driver: DROID_PROVIDER,
            nativeItemId: nativeId,
          });
          const plan: OrchestrationV2PlanArtifact = {
            id: planId,
            threadId: context.input.threadId,
            runId: context.input.runId,
            nodeId,
            kind: "todo_list",
            status: steps.every((step) => step.status === "completed") ? "completed" : "active",
            steps,
          };
          const node: OrchestrationV2ExecutionNode = {
            id: nodeId,
            threadId: context.input.threadId,
            runId: context.input.runId,
            parentNodeId: context.input.rootNodeId,
            rootNodeId: context.input.rootNodeId,
            kind: "todo_list",
            status: "completed",
            countsForRun: false,
            providerThreadId: context.input.providerThread.id,
            providerTurnId: context.turn.id,
            nativeItemRef: nativeRef(nativeItemId),
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: updatedAt,
            completedAt: updatedAt,
          };
          yield* emitNode(node);
          yield* emit({ type: "plan.updated", driver: DROID_PROVIDER, plan });
          yield* emitItem({
            ...itemBase(context, nativeId, updatedAt),
            nodeId,
            type: "todo_list",
            planId,
            steps,
            status: "completed",
            completedAt: updatedAt,
          });
        });
        const emitNode = (node: OrchestrationV2ExecutionNode) =>
          emit({ type: "node.updated", driver: DROID_PROVIDER, node });
        const ordinal = (context: ActiveTurn, key: string) => {
          const existing = context.ordinals.get(key);
          if (existing !== undefined) return existing;
          const value = context.input.providerTurnOrdinal * 100 + context.ordinals.size + 1;
          context.ordinals.set(key, value);
          return value;
        };
        // The timeline shows an item's startedAt as its time, so each item keeps
        // the moment it first appeared instead of inheriting the turn's start.
        const itemStartedAt = (context: ActiveTurn, key: string, now: DateTime.Utc) => {
          const known = context.itemStartedAt.get(key);
          if (known !== undefined) return known;
          context.itemStartedAt.set(key, now);
          return now;
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
            startedAt: itemStartedAt(context, key, updatedAt),
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
          messageTurns.set(id, context.turn.id);
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
        const textBuffer = new Map<string, string>();
        const textDeltas = {
          append: Effect.fn("DroidAdapter.appendText")(
            ({
              turnId,
              itemId,
              delta,
            }: {
              readonly turnId: string;
              readonly itemId: string;
              readonly delta: string;
            }) => {
              const context = active;
              if (context === null || context.turn.id !== turnId) return Effect.void;
              const key = `${turnId}\0${itemId}`;
              const text = `${textBuffer.get(key) ?? ""}${delta}`;
              textBuffer.set(key, text);
              return emitText(context, itemId, text, itemId.endsWith(":thinking"), true);
            },
          ),
          complete: Effect.fn("DroidAdapter.completeText")(
            ({
              turnId,
              itemId,
              finalText,
              emitEmpty = true,
            }: {
              readonly turnId: string;
              readonly itemId: string;
              readonly finalText?: string;
              readonly emitEmpty?: boolean;
            }) => {
              const key = `${turnId}\0${itemId}`;
              const text = finalText ?? textBuffer.get(key) ?? "";
              textBuffer.delete(key);
              const context = active;
              if (
                context === null ||
                context.turn.id !== turnId ||
                (!emitEmpty && text.length === 0)
              )
                return Effect.succeed(text);
              return emitText(context, itemId, text, itemId.endsWith(":thinking"), false).pipe(
                Effect.as(text),
              );
            },
          ),
          flushTurn: (turnId: string) => {
            const context = active;
            if (context === null || context.turn.id !== turnId) return Effect.void;
            return Effect.forEach(
              [...textBuffer.entries()].filter(([key]) => key.startsWith(`${turnId}\0`)),
              ([key, text]) => {
                textBuffer.delete(key);
                return emitText(
                  context,
                  key.slice(turnId.length + 1),
                  text,
                  key.endsWith(":thinking"),
                  false,
                );
              },
              { discard: true },
            );
          },
        };
        const emitTool = Effect.fnUntraced(function* (
          context: ActiveTurn,
          tool: typeof ToolUse.Type,
          status: OrchestrationV2TurnItem["status"],
          output?: string,
          child?: ChildState,
          changes?: ReadonlyArray<{ operation: string; path: string }>,
        ) {
          const updatedAt = yield* DateTime.now;
          // Released clients recognize the Codex `server.tool` form, not Droid's native separator.
          const toolName = tool.name.replace("___", ".");
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
            title: toolName,
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
          const boundedOutput = output === undefined ? undefined : boundedToolOutput(output);
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
              ...(changes === undefined ? {} : { changes }),
              ...(boundedOutput === undefined ? {} : { diffStr: boundedOutput }),
            });
          } else {
            yield* emitItem({
              ...base,
              type: "dynamic_tool",
              toolName,
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
          yield* emit({ type: "subagent.updated", driver: DROID_PROVIDER, subagent: task });
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
        const publishChildWork = Effect.fnUntraced(function* (
          child: ChildState,
          work: DroidSubagentWork,
        ) {
          const childThreadId = child.task.childThreadId;
          if (childThreadId === null) return;
          const updatedAt = yield* DateTime.now;
          if (work.model !== undefined && work.model !== child.task.model) {
            child.task = {
              ...child.task,
              model: work.model,
              modelSelection: droidChildModelSelection(
                options.instanceId,
                work.model,
                child.task.effort,
              ),
              updatedAt,
            };
            yield* emitChild(child);
          }
          for (const step of work.steps) {
            const signature = droidSubagentStepSignature(step);
            if (child.publishedSteps.get(step.id) === signature) continue;
            child.publishedSteps.set(step.id, signature);
            if (step.kind === "tool") {
              yield* emitTool(
                child.context,
                { id: step.id, name: step.name, input: step.input },
                step.output === null ? "running" : step.isError ? "failed" : "completed",
                step.output ?? undefined,
                child,
              );
              continue;
            }
            const key = `child:${child.task.nativeTaskRef?.nativeId}:${step.id}`;
            const itemOrdinal = ordinal(child.context, key);
            if (step.reasoning === true) {
              yield* emitItem({
                ...itemBase(child.context, key, updatedAt),
                threadId: childThreadId,
                runId: null,
                nodeId: child.rootNodeId,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: nativeRef(key),
                type: "reasoning",
                title: "Thinking",
                text: step.text,
                streaming: false,
                status: "completed",
                completedAt: updatedAt,
              });
            } else {
              const artifacts = makeSubagentConversationArtifacts({
                messageId: ids.derive.messageFromProviderItem({
                  driver: DROID_PROVIDER,
                  nativeItemId: key,
                }),
                turnItemId: ids.derive.turnItemFromProviderItem({
                  driver: DROID_PROVIDER,
                  nativeItemId: key,
                }),
                senderThreadId: childThreadId,
                threadId: childThreadId,
                rootNodeId: child.rootNodeId,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: nativeRef(key),
                role: "assistant",
                text: step.text,
                ordinal: itemOrdinal,
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
        const startChildFollower = Effect.fnUntraced(function* (
          childId: string,
          child: ChildState,
        ) {
          if (nativeId === null) return;
          const directory = yield* findDroidSessionDirectory({
            sessionId: nativeId,
            environment: options.environment,
          });
          if (directory === undefined) return;
          // Install ownership before any read or fork can complete. A detached fiber cannot
          // put itself back on a completed or resumed child.
          const owner: NonNullable<ChildState["follower"]> = {
            scope: yield* Scope.fork(sessionScope, "sequential"),
            handle: null,
            fiber: null,
          };
          child.follower = owner;
          const failed = (cause: Cause.Cause<unknown>) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.void
              : Effect.gen(function* () {
                  yield* Effect.logWarning("Droid child follower failed", { cause });
                  yield* Scope.close(owner.scope, Exit.void);
                  if (child.follower !== owner) return;
                  const work = owner.handle === null ? undefined : yield* owner.handle.drain;
                  if (child.follower !== owner) return;
                  child.follower = null;
                  const pending = child.pendingCompletion;
                  if (child.task.status !== "running") return;
                  if (pending !== null) {
                    yield* settleChild(child, pending.status, pending.result, work?.activityOffset);
                  } else if (work?.outcome !== undefined) {
                    yield* settleChild(
                      child,
                      work.outcome,
                      work.result ?? child.task.result,
                      work.activityOffset,
                    );
                  }
                });
          yield* Effect.gen(function* () {
            const handle = yield* makeDroidSubagentFollower({
              directory,
              sessionId: childId,
              ...(child.activityOffset === undefined
                ? {}
                : { previousActivityOffset: child.activityOffset }),
              watchDirectory,
              readAppend,
              onWork: (work) =>
                child.follower === owner ? publishChildWork(child, work) : Effect.void,
            }).pipe(Effect.provideService(Scope.Scope, owner.scope));
            owner.handle = handle;
            const fiber = yield* Effect.forkIn(
              handle
                .run((work) =>
                  Effect.gen(function* () {
                    if (child.follower !== owner || work.outcome === undefined) return;
                    // Settlement can close this handle, but must not interrupt its own fiber.
                    owner.fiber = null;
                    yield* settleChild(
                      child,
                      child.pendingCompletion?.status ?? work.outcome,
                      child.pendingCompletion?.result ?? work.result ?? child.task.result,
                      work.activityOffset,
                    );
                  }),
                )
                .pipe(
                  Effect.ensuring(Scope.close(owner.scope, Exit.void)),
                  Effect.catchCause(failed),
                ),
              sessionScope,
              { startImmediately: true },
            );
            if (child.follower === owner) owner.fiber = fiber;
          }).pipe(Effect.catchCause(failed));
        });
        const stopChildFollower = Effect.fnUntraced(function* (child: ChildState) {
          const owner = child.follower;
          child.follower = null;
          if (owner === null) return;
          if (owner.handle !== null) yield* owner.handle.close;
          if (owner.fiber !== null) yield* Fiber.interrupt(owner.fiber);
          yield* Scope.close(owner.scope, Exit.void);
        });
        const settleChild = Effect.fnUntraced(function* (
          child: ChildState,
          status: "completed" | "failed" | "cancelled",
          result: string | null,
          activityOffset?: number,
        ) {
          child.pendingCompletion = null;
          if (activityOffset !== undefined) child.activityOffset = activityOffset;
          yield* stopChildFollower(child);
          child.task = {
            ...child.task,
            status,
            result,
            completedAt: yield* DateTime.now,
            updatedAt: yield* DateTime.now,
          };
          // The roster clear goes first: the consumer releases ingestion on the last settled
          // child event, so a roster update queued after it can be dropped.
          yield* syncChildRoster();
          yield* emitChild(child);
        });
        const finishChild = Effect.fnUntraced(function* (
          child: ChildState,
          status: "completed" | "failed" | "cancelled",
          result: string | null,
        ) {
          const owner = child.follower;
          child.pendingCompletion = { status, result };
          const work =
            owner?.handle === null || owner?.handle === undefined
              ? undefined
              : yield* owner.handle.drain;
          if (child.follower !== owner) return;
          // Parent notifications can overtake the final append. Only explicit cancellation or
          // a dead parent forces settlement without a disk outcome.
          if (
            owner !== null &&
            work?.outcome === undefined &&
            status !== "cancelled" &&
            connection !== null
          )
            return;
          yield* settleChild(child, status, result, work?.activityOffset);
        });
        const registerChild = Effect.fnUntraced(function* (
          notice: typeof Child.Type & { readonly toolUseId: string },
          context: ActiveTurn,
          reopenExisting = false,
        ) {
          const existing = children.get(notice.childSessionId);
          const updatedAt = yield* DateTime.now;
          const key = `child:${notice.childSessionId}`;
          const nodeId = ids.derive.nodeFromProviderItem({
            driver: DROID_PROVIDER,
            nativeItemId: key,
          });
          const parentNodeId = context.input.rootNodeId;
          const tool = context.tools.get(notice.toolUseId);
          const complexity = stringField(tool?.input, "complexity");
          const effort =
            (complexity === undefined ? undefined : DROID_EFFORT_BY_COMPLEXITY.get(complexity)) ??
            existing?.task.effort;
          const model =
            existing?.task.model ??
            readDroidDefinitionModel(options.environment, notice.subagentType);
          const childThreadId = ids.derive.threadFromProviderThread({
            driver: DROID_PROVIDER,
            providerInstanceId: options.instanceId,
            nativeThreadId: notice.childSessionId,
          });
          const task: OrchestrationV2Subagent = {
            ...(reopenExisting && existing !== undefined ? existing.task : {}),
            id: nodeId,
            threadId: existing?.task.threadId ?? context.input.threadId,
            runId:
              reopenExisting && existing?.task.status !== "running"
                ? context.input.runId
                : (existing?.task.runId ?? context.input.runId),
            parentNodeId: existing?.task.parentNodeId ?? parentNodeId,
            origin: "provider_native",
            createdBy: "agent",
            driver: DROID_PROVIDER,
            providerInstanceId: options.instanceId,
            providerThreadId: context.input.providerThread.id,
            childThreadId,
            nativeTaskRef: nativeRef(notice.childSessionId),
            prompt:
              reopenExisting && existing !== undefined
                ? existing.task.prompt
                : (stringField(tool?.input, "prompt") ??
                  existing?.task.prompt ??
                  notice.description ??
                  ""),
            title: notice.description ?? notice.subagentType ?? null,
            model: model ?? null,
            modelSelection: droidChildModelSelection(
              options.instanceId,
              model ?? context.input.modelSelection.model,
              effort,
            ),
            ...(effort === undefined ? {} : { effort }),
            status: "running",
            result: null,
            startedAt:
              reopenExisting && existing !== undefined ? existing.task.startedAt : updatedAt,
            completedAt: null,
            updatedAt,
          };
          const child: ChildState = {
            task,
            background: existing?.background ?? false,
            context,
            rootNodeId: ids.derive.nodeFromProviderItem({
              driver: DROID_PROVIDER,
              nativeItemId: `${key}:root`,
            }),
            parentRootNodeId: parentNodeId,
            ordinal: existing?.ordinal ?? ordinal(context, key),
            toolUseId: notice.toolUseId,
            activityOffset: existing?.activityOffset,
            follower: null,
            publishedSteps: existing?.publishedSteps ?? new Map(),
            pendingCompletion: null,
          };
          if (existing !== undefined) yield* stopChildFollower(existing);
          children.set(notice.childSessionId, child);
          if (existing !== undefined) childByToolUseId.delete(existing.toolUseId);
          childByToolUseId.set(notice.toolUseId, child);
          yield* syncChildRoster();
          if (existing === undefined && !reopenExisting) {
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
          if (!reopenExisting || existing?.task.status !== "running") yield* emitChild(child);
          if (!reopenExisting) {
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
          if (child.task.status === "running")
            yield* startChildFollower(notice.childSessionId, child);
          return child;
        });

        const finishRequests = Effect.fnUntraced(function* (providerTurnId?: string) {
          for (const entry of pending.values()) {
            if (providerTurnId !== undefined && entry.request.providerTurnId !== providerTurnId)
              continue;
            yield* Deferred.succeed(entry.reply, {
              result:
                entry.questions === undefined
                  ? { selectedOption: "cancel" }
                  : { cancelled: true, answers: [] },
            });
          }
        });
        const settleProviderRetry = Effect.fnUntraced(function* (
          context: ActiveTurn,
          status: "completed" | "failed" | "interrupted" | "cancelled",
          failure = context.providerRetry?.failure,
        ) {
          const providerRetry = context.providerRetry;
          if (providerRetry === undefined) return;
          context.providerRetry = undefined;
          yield* emit({
            type: "turn_item.updated",
            driver: DROID_PROVIDER,
            turnItem: makeProviderRetryTurnItem({
              driver: DROID_PROVIDER,
              threadId: context.input.threadId,
              runId: context.input.runId,
              nodeId: context.input.rootNodeId,
              providerThreadId: context.input.providerThread.id,
              providerTurnId: context.turn.id,
              itemOrdinal: providerRetry.itemOrdinal,
              failure: failure ?? providerRetry.failure,
              retry: providerRetry.retry,
              status,
              startedAt: providerRetry.startedAt,
              updatedAt: yield* DateTime.now,
            }),
          });
        });
        const completeProviderRetry = (context: ActiveTurn) =>
          settleProviderRetry(context, "completed");
        const terminal = Effect.fnUntraced(function* (
          status: "completed" | "interrupted" | "cancelled" | "failed",
          broken = false,
          nativeTurnId?: string,
          failure?: ReturnType<typeof makeProviderFailure>,
          expectedContext?: ActiveTurn,
        ) {
          const context = active;
          if (context === null || (expectedContext !== undefined && context !== expectedContext))
            return;
          if (context.toolSearchStopped && context.toolSearchFailure !== undefined) {
            status = "failed";
            failure = context.toolSearchFailure;
          }
          const terminalFailure =
            failure ??
            makeProviderFailure({
              class: broken ? "transport_error" : "provider_error",
              message: broken
                ? "The native Droid process exited."
                : "Droid did not complete the turn.",
              retryable: false,
            });
          yield* textDeltas.flushTurn(context.turn.id);
          yield* settleProviderRetry(
            context,
            status === "completed" ? "completed" : status,
            status === "failed" ? terminalFailure : undefined,
          );
          active = null;
          const completedAt = yield* DateTime.now;
          for (const [key, block] of context.blocks) {
            yield* emitText(context, key, block.text, block.reasoning, false);
          }
          for (const tool of context.tools.values()) {
            yield* emitTool(context, tool, status === "completed" ? "interrupted" : status);
          }
          context.tools.clear();
          context.toolProgress.clear();
          yield* finishRequests(context.turn.id);
          const usage = broken ? undefined : yield* readContextStats();
          const turn: OrchestrationV2ProviderTurn = {
            ...context.turn,
            status,
            completedAt,
            ...(nativeTurnId === undefined ? {} : { nativeTurnRef: nativeRef(nativeTurnId) }),
            ...(usage === undefined
              ? {}
              : { tokenUsage: { ...usage, updatedAt: DateTime.formatIso(completedAt) } }),
          };
          turns.set(turn.id, turn);
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
              failureItemOrdinal: ordinal(context, `terminal-failure:${context.turn.id}`),
              status,
              failure: terminalFailure,
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
        const closeStoppedConnection = Effect.fnUntraced(function* (
          context: ActiveTurn,
          rpc: DroidRpc.DroidRpcConnection,
        ) {
          if (connection !== rpc || active !== context) return;
          connection = null;
          wakeGeneration++;
          wakePending = false;
          wakeBuffer = [];
          yield* rpc.close;
          if (connectionScope !== null) {
            const failedScope = connectionScope;
            connectionScope = null;
            yield* Scope.close(failedScope, Exit.void);
          }
          for (const child of children.values()) {
            if (child.task.status === "running" && child.pendingCompletion !== null) {
              const pending = child.pendingCompletion;
              yield* finishChild(child, pending.status, pending.result);
            }
          }
        });
        const handleRequest = Effect.fnUntraced(function* (request: DroidRpc.DroidRpcRequest) {
          if (request.method !== "droid.ask_user" && request.method !== "droid.request_permission")
            return undefined;
          const requestSessionId = stringField(request.params, "sessionId");
          const associatedSessionIds = Option.match(decodeAssociatedSessions(request.params), {
            onNone: () => [],
            onSome: (decoded) => decoded.associatedSessionIds ?? [],
          });
          const child =
            (requestSessionId === undefined ? undefined : children.get(requestSessionId)) ??
            associatedSessionIds
              .map((sessionId) => children.get(sessionId))
              .find((candidate) => candidate !== undefined);
          const context =
            child?.context ??
            (requestSessionId !== undefined && requestSessionId !== nativeId
              ? null
              : nativeNotificationTurnId?.startsWith("task-completion:") &&
                  (active === null || active.isWake)
                ? wakeOwner
                : active);
          if (context === null) {
            yield* Effect.logWarning("Cancelling unroutable native Droid request", {
              method: request.method,
              requestId: request.id,
            });
            return undefined;
          }
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
            parentNodeId: child?.task.id ?? context.input.rootNodeId,
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
                  prompt: permission === undefined ? undefined : droidApprovalPrompt(permission),
                  options: permission?.options.flatMap(
                    (option): ReadonlyArray<ProviderApprovalOption> =>
                      option.value === "proceed_once"
                        ? [{ decision: "accept", label: option.label }]
                        : option.value === "proceed_always"
                          ? [{ decision: "acceptForSession", label: option.label }]
                          : option.value === "cancel"
                            ? [{ decision: "cancel", label: option.label }]
                            : [],
                  ),
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
          const entry: PendingRequest = {
            reply,
            request: runtimeRequest,
            node,
            item,
            ...(asking === undefined ? {} : { questions: asking.questions }),
            ...(permission === undefined ? {} : { options: permission.options }),
          };
          pending.set(requestId, entry);
          yield* emitNode(node);
          yield* emit({ type: "runtime_request.updated", driver: DROID_PROVIDER, runtimeRequest });
          yield* emitItem(item);
          const answer = yield* Deferred.await(reply);
          pending.delete(requestId);
          const resolvedAt = yield* DateTime.now;
          yield* emit({
            type: "runtime_request.updated",
            driver: DROID_PROVIDER,
            runtimeRequest: {
              ...runtimeRequest,
              ...entry.response,
              status: "resolved",
              resolvedAt,
            },
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

        // Droid keeps the session id when it compacts, and an automatic compaction reports
        // `session_compacted` twice. One key per turn folds those, and the manual `/compact`
        // turn, into a single item.
        const emitCompaction = Effect.fnUntraced(function* (context: ActiveTurn) {
          contextUsageAfterCompaction = true;
          const compactedAt = yield* DateTime.now;
          yield* emitItem({
            ...itemBase(context, `compaction:${context.turn.id}`, compactedAt),
            type: "compaction",
            driver: DROID_PROVIDER,
            status: "completed",
            title: "Context compacted",
            completedAt: compactedAt,
          });
        });

        const handleNotification = Effect.fnUntraced(function* (
          notice: DroidRpc.DroidRpcNotification,
        ) {
          const notification = notice.notification;
          const type = notification["type"];
          if (notice.sessionId !== undefined && nativeId !== null && notice.sessionId !== nativeId)
            return;
          const explicitTurnId = stringField(notification, "turnId");
          const isTaskCompletionNotice =
            type === "create_message" &&
            stringField(notification["message"], "id")?.startsWith("task-completion:") === true;
          if (
            active !== null &&
            type === "create_message" &&
            explicitTurnId !== undefined &&
            explicitTurnId !== active.turn.nativeTurnRef?.nativeId &&
            !isTaskCompletionNotice
          )
            return;
          if (
            active?.toolSearchStopped &&
            type === "agent_turn_completed" &&
            explicitTurnId === active.turn.nativeTurnRef?.nativeId
          ) {
            yield* Deferred.succeed(active.toolSearchCompletion, undefined);
            return;
          }
          if (type === "create_message") {
            const message = yield* decodeNotice(Message, notification["message"]);
            if (message === undefined) return;
            const isTaskCompletion = message.id.startsWith("task-completion:");
            const text = isTaskCompletion
              ? message.content.map((part) => part.text ?? "").join("")
              : "";
            const childId = /^task_id: (\S+)$/mu.exec(text)?.[1];
            const reason = /^reason: (\S+)$/mu.exec(text)?.[1];
            const child = childId === undefined ? undefined : children.get(childId);
            if (isTaskCompletion) {
              nativeNotificationTurnId = message.id;
              wakeOwner = active?.isWake ? active : (child?.context ?? wakeOwner ?? lastContext);
              if (child?.task.status === "running")
                yield* finishChild(
                  child,
                  reason === "completed"
                    ? "completed"
                    : reason === "cancelled"
                      ? "cancelled"
                      : "failed",
                  /^output: ([\s\S]*?)\n\nBackground task/mu.exec(text)?.[1] ?? null,
                );
            } else if (message.role === "user" && message.id === active?.input.message.messageId) {
              nativeNotificationTurnId = message.id;
            }
            if (isTaskCompletion && active === null && lastContext !== null) {
              wakeBuffer.push(notice);
              if (!wakePending) {
                wakePending = true;
                const generation = ++wakeGeneration;
                // This offer is static; later buffered completions cannot join its notification.
                const notification =
                  child === undefined
                    ? null
                    : backgroundWorkNotification([
                        {
                          kind: "subagent",
                          label: child.task.title ?? undefined,
                          outcome:
                            reason === "completed"
                              ? "completed"
                              : reason === "cancelled"
                                ? "cancelled"
                                : "failed",
                          childThreadId: child.task.childThreadId ?? undefined,
                        },
                      ]);
                yield* (
                  options.continuationRequests?.offer({
                    threadId: lastContext.input.threadId,
                    providerThreadId: lastContext.input.providerThread.id,
                    driver: DROID_PROVIDER,
                    detail: "Native Droid background task completed.",
                    ...(notification === null ? {} : { notification }),
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
          const context =
            nativeNotificationTurnId?.startsWith("task-completion:") && active.isWake
              ? (wakeOwner ?? active)
              : active;
          if (context.toolSearchStopped && type !== "agent_turn_completed") return;
          if (type === "child_session_available") {
            const child = yield* decodeNotice(
              Schema.Struct({ ...Child.fields, toolUseId: Schema.String }),
              notification,
            );
            if (child === undefined) return;
            if (reopenedChildren.has(child.childSessionId)) return;
            const existing = children.get(child.childSessionId);
            if (existing !== undefined && existing.task.status !== "running") {
              reopenedChildren.add(child.childSessionId);
              yield* registerChild(child, context, true);
              return;
            }
            yield* registerChild(child, context);
          } else if (type === "tool_call") {
            yield* completeProviderRetry(context);
            const toolCall = yield* decodeNotice(Schema.Struct({ toolUse: ToolUse }), notification);
            if (toolCall?.toolUse.name !== "TodoWrite") return;
            const steps = droidTodoSteps(toolCall.toolUse.input);
            if (steps !== undefined) yield* emitTodoList(context, toolCall.toolUse.id, steps);
          } else if (type === "create_message") {
            const message = yield* decodeNotice(Message, notification["message"]);
            if (message === undefined) return;
            if (message.role !== "assistant") return;
            yield* completeProviderRetry(context);
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
              const tool = yield* decodeNotice(ToolUse, part);
              if (tool === undefined) continue;
              context.tools.set(tool.id, tool);
              yield* emitTool(context, tool, "running");
              if (tool.name === "Task") {
                const childId = stringField(tool.input, "resume");
                const child = childId === undefined ? undefined : children.get(childId);
                if (
                  childId !== undefined &&
                  (child === undefined || child.task.status !== "running")
                ) {
                  const description =
                    stringField(tool.input, "description") ?? child?.task.title ?? undefined;
                  reopenedChildren.add(childId);
                  yield* registerChild(
                    {
                      childSessionId: childId,
                      toolUseId: tool.id,
                      ...(description === undefined ? {} : { description }),
                    },
                    context,
                    true,
                  );
                }
              }
              if (tool.name === "TodoWrite") {
                const steps = droidTodoSteps(tool.input);
                if (steps !== undefined) yield* emitTodoList(context, tool.id, steps);
              }
            }
          } else if (type === "assistant_text_delta" || type === "thinking_text_delta") {
            yield* completeProviderRetry(context);
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
            yield* completeProviderRetry(context);
            const toolUseId = stringField(notification, "toolUseId");
            if (toolUseId === undefined) return;
            const tool = context.tools.get(toolUseId);
            const output = stringField(notification, "content") ?? "";
            const toolInput = tool === undefined ? Option.none() : fields(tool.input);
            const notificationTurnId = stringField(notification, "turnId");
            let toolSearchFailure: ReturnType<typeof makeProviderFailure> | undefined;
            if (
              !context.interrupted &&
              (notificationTurnId === undefined ||
                notificationTurnId === context.turn.nativeTurnRef?.nativeId) &&
              tool?.name === "ToolSearch" &&
              Option.isSome(toolInput) &&
              Object.keys(toolInput.value).length === 0 &&
              notification["isError"] === true &&
              output === "Error: Invalid ToolSearch input: query: Required"
            ) {
              context.malformedToolSearchFailureIds.add(toolUseId);
              if (context.malformedToolSearchFailureIds.size === 2) {
                context.toolSearchStopped = true;
                context.toolSearchFailure = makeProviderFailure({
                  class: "provider_error",
                  message:
                    "Droid repeated a ToolSearch call without its required query. The turn was stopped.",
                  retryable: false,
                });
                toolSearchFailure = context.toolSearchFailure;
              }
            }
            const patchResult =
              tool?.name === "ApplyPatch" ? decodeApplyPatchResult(output) : Option.none();
            if (tool !== undefined) {
              yield* emitTool(
                context,
                tool,
                notification["isError"] === true ? "failed" : "completed",
                output,
                undefined,
                Option.isSome(patchResult)
                  ? patchResult.value.files.map((file) => ({
                      operation: file.display_operation,
                      path: file.file_path,
                    }))
                  : undefined,
              );
              if (tool.name === "TaskStop" && notification["isError"] !== true) {
                const childId = stringField(tool.input, "task_id");
                const child = childId === undefined ? undefined : children.get(childId);
                if (child !== undefined) yield* finishChild(child, "cancelled", output);
              }
              context.tools.delete(toolUseId);
              context.toolProgress.delete(toolUseId);
            }
            const child = childByToolUseId.get(toolUseId);
            if (output.startsWith("Task launched in background.") && child !== undefined) {
              child.background = true;
              yield* syncChildRoster();
            } else {
              if (child !== undefined && child.task.status === "running") {
                yield* finishChild(
                  child,
                  notification["isError"] === true ? "failed" : "completed",
                  output,
                );
              }
            }
            if (toolSearchFailure !== undefined) {
              const rpc = connection;
              if (rpc !== null) {
                yield* Effect.gen(function* () {
                  if (active !== context || connection !== rpc) return;
                  yield* rpc.request("droid.interrupt_session", {}, "5 seconds");
                  const completed = yield* Deferred.await(context.toolSearchCompletion).pipe(
                    Effect.timeoutOption("30 seconds"),
                  );
                  if (Option.isSome(completed)) {
                    yield* terminal("failed", false, undefined, toolSearchFailure, context);
                    return;
                  }
                  yield* closeStoppedConnection(context, rpc);
                  yield* terminal("failed", true, undefined, toolSearchFailure, context);
                }).pipe(
                  Effect.catchCause(() =>
                    Effect.gen(function* () {
                      yield* closeStoppedConnection(context, rpc);
                      yield* terminal(
                        context.interrupted ? "interrupted" : "failed",
                        true,
                        undefined,
                        toolSearchFailure,
                        context,
                      );
                    }),
                  ),
                  Effect.forkIn(sessionScope),
                );
              } else {
                yield* terminal("failed", true, undefined, toolSearchFailure, context);
              }
            }
          } else if (type === "tool_progress_update") {
            const progress = decodeToolProgressUpdate(notification);
            if (Option.isNone(progress)) {
              yield* Effect.logWarning("Skipping unknown native Droid notification", {
                type: "tool_progress_update",
              });
              return;
            }
            const { toolUseId, update } = progress.value;
            const { timestamp, fullOutput } = update;
            if (fullOutput === undefined) return;
            const tool = context.tools.get(toolUseId);
            if (tool?.name !== "Execute") return;
            const previous = context.toolProgress.get(toolUseId);
            const boundedOutput = boundedToolOutput(fullOutput);
            if (
              previous !== undefined &&
              (previous.output === boundedOutput || timestamp - previous.timestamp < 2_000)
            )
              return;
            // Persisted progress events reach every client, so cap their rate without timers.
            context.toolProgress.set(toolUseId, { timestamp, output: boundedOutput });
            yield* emitTool(context, tool, "running", fullOutput);
          } else if (type === "agent_turn_completed") {
            const turnId = stringField(notification, "turnId");
            const matching = turnId === context.turn.nativeTurnRef?.nativeId;
            if (!matching || active !== context) return;
            const reason = stringField(notification, "reason");
            const failure =
              reason === "model_usage_exhausted" || reason === "model_rate_limited"
                ? makeProviderFailure({
                    class: "usage_limit",
                    message: context.lastError ?? "Droid usage limit reached.",
                    code: reason,
                    retryable: false,
                  })
                : reason === "completed" ||
                    reason === "spec_handoff" ||
                    reason === "cancelled" ||
                    reason === "permission_rejected"
                  ? undefined
                  : makeProviderFailure({
                      class: "provider_error",
                      message: context.lastError ?? "Droid did not complete the turn.",
                      code: reason,
                      retryable: false,
                    });
            yield* terminal(
              context.interrupted
                ? "interrupted"
                : reason === "completed" || reason === "spec_handoff"
                  ? "completed"
                  : reason === "cancelled" || reason === "permission_rejected"
                    ? "cancelled"
                    : "failed",
              false,
              turnId,
              failure,
            );
          } else if (type === "error") {
            context.lastError = stringField(notification, "message");
          } else if (type === "llm_retry") {
            const attempt = notification["attempt"];
            const retryAttempt =
              typeof attempt === "number" && Number.isFinite(attempt)
                ? Math.max(1, Math.trunc(attempt))
                : 1;
            const retry = {
              attempt: retryAttempt,
              maxAttempts: null,
              retryDelayMs: null,
            } satisfies OrchestrationV2ProviderRetry;
            const reason = stringField(notification, "reason");
            const retryFailure = makeProviderFailure({
              class: "provider_error",
              message:
                reason === "rate_limited"
                  ? `Provider rate limited; retrying (attempt ${retryAttempt})`
                  : `Provider retrying (attempt ${retryAttempt})`,
              retryable: true,
            });
            if (context.providerRetry === undefined) {
              context.providerRetry = {
                itemOrdinal: ordinal(context, `terminal-failure:${context.turn.id}`),
                failure: retryFailure,
                retry,
                startedAt: yield* DateTime.now,
              };
            } else {
              context.providerRetry = {
                ...context.providerRetry,
                failure: retryFailure,
                retry,
              };
            }
            const providerRetry = context.providerRetry;
            yield* emit({
              type: "turn_item.updated",
              driver: DROID_PROVIDER,
              turnItem: makeProviderRetryTurnItem({
                driver: DROID_PROVIDER,
                threadId: context.input.threadId,
                runId: context.input.runId,
                nodeId: context.input.rootNodeId,
                providerThreadId: context.input.providerThread.id,
                providerTurnId: context.turn.id,
                itemOrdinal: providerRetry.itemOrdinal,
                failure: providerRetry.failure,
                retry: providerRetry.retry,
                status: "running",
                startedAt: providerRetry.startedAt,
                updatedAt: yield* DateTime.now,
              }),
            });
          } else if (type === "session_compacted") {
            yield* emitCompaction(context);
          } else if (
            ![
              "droid_working_state_changed",
              "working_state_changed",
              "session_token_usage_changed",
              "settings_updated",
              "session_title_updated",
              "mcp_status_changed",
              "hook_execution_started",
              "hook_execution_completed",
              "context_usage_changed",
              "token_usage_updated",
              "tool_execution_phase_changed",
              "permission_resolved",
            ].includes(typeof type === "string" ? type : "")
          ) {
            yield* Effect.logWarning("Skipping unknown native Droid notification", {
              type: typeof type === "string" ? type : "invalid",
            });
          }
        });

        const requireConnection = Effect.fnUntraced(function* () {
          if (connection === null) return yield* protocolError("No live native Droid session");
          return connection;
        });
        const readContextStats = Effect.fnUntraced(function* () {
          if (connection === null) return undefined;
          const breakdown = yield* connection
            .request("droid.get_context_breakdown", {}, "5 seconds")
            .pipe(
              Effect.flatMap((response) => decodeContextBreakdown(response.result)),
              Effect.option,
            );
          if (Option.isNone(breakdown) || !(breakdown.value.contextBudget >= 1)) return undefined;
          const maxTokens = Math.floor(breakdown.value.contextBudget);
          if (settingsModel !== null) contextWindows.set(settingsModel, maxTokens);
          const used = contextUsageAfterCompaction
            ? breakdown.value.usedTokens
            : (breakdown.value.lastCallCompactionTokens ?? breakdown.value.usedTokens);
          return { usedTokens: Math.max(0, Math.round(used)), maxTokens };
        });
        const applySettings = Effect.fnUntraced(function* (
          selection: ModelSelection,
          policy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
        ) {
          const rpc = yield* requireConnection();
          yield* rpc.request(
            "droid.update_session_settings",
            yield* resolveSettings(selection, policy),
          );
          settingsModel = selection.model;
          yield* readContextStats();
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
            const mcp = yield* mcpSessions.read(sessionInput.threadId);
            const env = McpProviderSession.withAgentDeviceEnvironment(
              {
                ...options.environment,
                FACTORY_DISABLE_SETTINGS_PERSISTENCE: "1",
              },
              mcp,
            );
            const cwd = yield* canonicalWorkspacePath(policy.cwd ?? providerHost.paths.cwd);
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
            const rpc = yield* DroidRpc.makeConnection({
              ...transportOptions,
              command: options.binaryPath,
              env,
            }).pipe(
              Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
              Effect.provideService(Scope.Scope, childScope),
            );
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
            const settings = yield* resolveSettings(selection, policy);
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
                      append: droidSystemPromptAppend(mcp !== undefined),
                    },
                  }
                : { sessionId: resumeId, mcpServers, autoRejectPermissionRequests: false },
            );
            const protocolVersion = response.factoryProtocolVersion;
            if (protocolVersion === undefined || !isAcceptedDroidProtocolVersion(protocolVersion)) {
              yield* closeConnection();
              return yield* protocolError(
                `The native Droid protocol version ${protocolVersion ?? "unknown"} is not supported`,
              );
            }
            if (!VERIFIED_PROTOCOLS.has(protocolVersion))
              yield* Effect.logWarning("Using an unverified native Droid protocol version", {
                version: protocolVersion,
              });
            const result = yield* decode(SessionResult, response.result);
            nativeId = resumeId ?? result.sessionId ?? null;
            if (nativeId === null)
              return yield* protocolError("Droid initialization did not return a session id");
            if (resumeId !== undefined) {
              yield* rpc.request("droid.update_session_settings", settings);
            }
            settingsModel = selection.model;
            yield* readContextStats();
            yield* rpc.notifications.pipe(
              Stream.runForEach((notice) => handleNotification(notice)),
              Effect.catchCause(() =>
                Effect.gen(function* () {
                  if (connection !== rpc) return;
                  const context = active;
                  connection = null;
                  yield* rpc.close;
                  wakePending = false;
                  wakeBuffer = [];
                  if (context?.toolSearchStopped === true) {
                    for (const child of children.values()) {
                      if (child.task.status === "running" && child.pendingCompletion !== null) {
                        const pending = child.pendingCompletion;
                        yield* finishChild(child, pending.status, pending.result);
                      }
                    }
                    yield* terminal("failed", true, undefined, context.toolSearchFailure, context);
                  } else {
                    yield* terminal("failed", true);
                    for (const child of children.values()) {
                      if (child.task.status === "running")
                        yield* finishChild(child, "failed", "The parent Droid process exited.");
                    }
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
            const previousRoster =
              providerThread?.id === input.existingProviderThread.id
                ? providerThread.pendingBackgroundTasks
                : input.existingProviderThread.pendingBackgroundTasks;
            providerThread = input.existingProviderThread;
            yield* openConnection(
              input.modelSelection,
              input.runtimePolicy,
              input.existingProviderThread.nativeThreadRef?.nativeId ?? undefined,
            );
            if (nativeId === null) return yield* protocolError("No native Droid session id");
            if (
              active === null &&
              settingsModel !== input.modelSelection.model &&
              !contextWindows.has(input.modelSelection.model)
            )
              yield* applySettings(input.modelSelection, input.runtimePolicy);
            providerThread = {
              ...input.existingProviderThread,
              pendingBackgroundTasks: previousRoster,
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
        const imagePathsFor = (message: ProviderAdapter.ProviderAdapterV2TurnInput["message"]) =>
          Effect.forEach(
            message.attachments.filter(isProviderNativeImageAttachment),
            (attachment) => {
              const imagePath = providerHost.resolveAttachmentPath(attachment);
              return imagePath === null
                ? protocolError(`Cannot resolve Droid image attachment: ${attachment.name}`)
                : Effect.succeed(imagePath);
            },
          );
        const messageText = (message: ProviderAdapter.ProviderAdapterV2TurnInput["message"]) =>
          providerMessageTextWithAttachmentPaths({
            text: message.text,
            attachments: message.attachments,
            resolveAttachmentPath: providerHost.resolveAttachmentPath,
            resolveExtractedTextPath: providerHost.resolveExtractedTextPath,
          });
        const startTurn = Effect.fnUntraced(function* (
          input: ProviderAdapter.ProviderAdapterV2TurnInput,
          compact = false,
        ) {
          if (active !== null) return yield* protocolError("A native Droid turn is already active");
          const isContinuation =
            input.message.createdBy === "agent" && input.message.creationSource === "provider";
          const imagePaths = isContinuation || compact ? [] : yield* imagePathsFor(input.message);
          if (!isContinuation && !compact && wakeBuffer.length > 0 && lastContext !== null) {
            const buffer = wakeBuffer;
            wakeBuffer = [];
            wakePending = false;
            wakeGeneration++;
            // Already-completed wake work remains attached to its original run,
            // never to the incoming user run. Its terminal is not re-emitted.
            active = lastContext;
            for (const notice of buffer) yield* handleNotification(notice);
            active = null;
          }
          const resumedId = input.providerThread.nativeThreadRef?.nativeId;
          if (connection === null)
            yield* openConnection(
              input.modelSelection,
              input.runtimePolicy,
              resumedId ?? undefined,
            );
          const rpc = yield* requireConnection();
          if (!isContinuation) yield* applySettings(input.modelSelection, input.runtimePolicy);
          const startedAt = yield* DateTime.now;
          const wakeTurnId = isContinuation
            ? wakeBuffer
                .map((notice) => stringField(notice.notification["message"], "id"))
                .find((id) => id?.startsWith("task-completion:"))
            : undefined;
          const context: ActiveTurn = {
            isWake: isContinuation,
            input,
            turn: {
              id: ids.derive.providerTurn({
                driver: DROID_PROVIDER,
                nativeTurnId: `turn:${input.attemptId}`,
              }),
              providerThreadId: input.providerThread.id,
              nodeId: input.rootNodeId,
              runAttemptId: input.attemptId,
              nativeTurnRef: nativeRef(wakeTurnId ?? input.message.messageId),
              ordinal: input.providerTurnOrdinal,
              status: "running",
              startedAt,
              completedAt: null,
            },
            ordinals: new Map(),
            itemStartedAt: new Map(),
            blocks: new Map(),
            tools: new Map(),
            toolProgress: new Map(),
            malformedToolSearchFailureIds: new Set(),
            toolSearchCompletion: yield* Deferred.make<void>(),
            toolSearchStopped: false,
            toolSearchFailure: undefined,
            lastError: undefined,
            providerRetry: undefined,
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
          yield* syncChildRoster();
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
              ...providerThread,
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
            yield* emitCompaction(context);
            yield* terminal("completed");
            return;
          }
          if (isContinuation) {
            wakeOwner = context;
            const buffer = wakeBuffer;
            wakeBuffer = [];
            wakePending = false;
            if (buffer.length === 0) return yield* terminal("completed");
            for (const notice of buffer) yield* handleNotification(notice);
            return;
          }
          nativeNotificationTurnId = input.message.messageId;
          contextUsageAfterCompaction = false;
          yield* rpc.request("droid.add_user_message", {
            messageId: input.message.messageId,
            text: yield* messageText(input.message),
            imagePaths,
            userMessageSource: "sdk",
            queuePlacement: "end_of_turn",
          });
        });
        const snapshot = (input: ProviderAdapter.ProviderAdapterV2ReadThreadSnapshotInput) =>
          Effect.succeed({
            providerThread: input.providerThread,
            providerTurns: [...turns.values()],
            messages: [...messages.values()],
            runtimeRequests: [...pending.values()].map((entry) => entry.request),
          } satisfies ProviderAdapter.ProviderAdapterV2ThreadSnapshot);
        const startTurnWithFailureCleanup = (
          input: ProviderAdapter.ProviderAdapterV2TurnInput,
          compact = false,
        ) =>
          Effect.suspend(() => {
            const previous = active;
            return startTurn(input, compact).pipe(
              Effect.onError(() =>
                Effect.suspend(() => {
                  const created = active;
                  return created !== null && created !== previous && created.input === input
                    ? terminal("failed", false, undefined, undefined, created)
                    : Effect.void;
                }),
              ),
            );
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
          hasPendingBackgroundWorkForThread: (thread) =>
            Effect.sync(
              () =>
                providerThread?.id === thread.id &&
                (wakePending ||
                  Array.from(children.values()).some(
                    (child) =>
                      child.task.providerThreadId === thread.id && child.task.status === "running",
                  )),
            ),
          getModelContextWindow: (selection) =>
            selection.instanceId === options.instanceId
              ? contextWindows.get(selection.model)
              : undefined,
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
            startTurnWithFailureCleanup(input).pipe(
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
          steerTurn: Effect.fn("DroidAdapter.steerTurn")(
            function* (input: ProviderAdapter.ProviderAdapterV2SteerInput) {
              if (active?.turn.id !== input.providerTurnId || active.toolSearchStopped)
                return yield* protocolError("The requested Droid turn is not active");
              const rpc = yield* requireConnection();
              yield* rpc.request("droid.add_user_message", {
                messageId: input.message.messageId,
                text: yield* messageText(input.message),
                imagePaths: yield* imagePathsFor(input.message),
                userMessageSource: "sdk",
                queuePlacement: "end_of_loop",
              });
            },
            (effect, input) =>
              effect.pipe(
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
          ),
          interruptTurn: Effect.fn("DroidAdapter.interruptTurn")(
            function* (input: ProviderAdapter.ProviderAdapterV2InterruptInput) {
              if (active === null) {
                return yield* protocolError("This Droid turn is no longer active");
              }
              if (active.turn.id !== input.providerTurnId)
                return yield* protocolError("The requested Droid turn is not active");
              if (active.toolSearchStopped) return;
              active.interrupted = true;
              const rpc = yield* requireConnection();
              yield* finishRequests();
              yield* rpc
                .request("droid.interrupt_session", {})
                .pipe(Effect.onError(() => terminal("interrupted")));
            },
            (effect, input) =>
              effect.pipe(
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
          ),
          respondToRuntimeRequest: Effect.fn("DroidAdapter.respondToRuntimeRequest")(
            function* (input: ProviderAdapter.ProviderAdapterV2RuntimeRequestResponseInput) {
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
                if (input.answers) entry.response = { answers: input.answers };
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
                // Record what Droid was actually told: a decision it did not offer becomes cancel.
                entry.response = {
                  decision:
                    selectedOption === "proceed_once"
                      ? "accept"
                      : selectedOption === "proceed_always"
                        ? "acceptForSession"
                        : "cancel",
                };
                yield* Deferred.succeed(entry.reply, { result: { selectedOption } });
              }
            },
            (effect, input) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                      driver: DROID_PROVIDER,
                      requestId: input.requestId,
                      cause,
                    }),
                ),
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
          rollbackThread: Effect.fn("DroidAdapter.rollbackThread")(
            function* (input: ProviderAdapter.ProviderAdapterV2RollbackThreadInput) {
              const fail = (detail: string) =>
                new ProviderAdapter.ProviderAdapterRollbackThreadError({
                  driver: DROID_PROVIDER,
                  providerThreadId: input.providerThread.id,
                  checkpointId: input.target.checkpointId,
                  cause: protocolError(detail),
                });
              if (active !== null)
                return yield* fail("Cannot rewind Droid while a provider turn is active");
              if (
                wakePending ||
                Array.from(children.values()).some(
                  (child) =>
                    child.task.providerThreadId === input.providerThread.id &&
                    child.task.status === "running",
                )
              )
                return yield* fail(
                  "Stop background work before rewinding the native Droid session",
                );
              const orderedTurns = [...input.providerThreadTurns].sort(
                (left, right) => left.ordinal - right.ordinal,
              );
              const targetTurn =
                input.target.type === "provider_turn" ? input.target.providerTurn : undefined;
              let boundaryOrdinal = 0;
              let nextTurn: OrchestrationV2ProviderTurn | undefined;
              if (targetTurn !== undefined) {
                if (targetTurn.providerThreadId !== input.providerThread.id)
                  return yield* fail(
                    "Rollback target belongs to a different Droid provider thread",
                  );
                const targetIndex = orderedTurns.findIndex((turn) => turn.id === targetTurn.id);
                if (targetIndex < 0)
                  return yield* fail(
                    "Rollback target was not found in Droid provider turn history",
                  );
                boundaryOrdinal = targetTurn.ordinal;
                nextTurn = orderedTurns[targetIndex + 1];
              } else {
                nextTurn = orderedTurns[0];
              }

              const messageId = nextTurn?.nativeTurnRef?.nativeId;
              if (messageId == null || messageId.startsWith("task-completion:"))
                return yield* fail("The next Droid turn has no rewindable user message id");
              yield* makeThread({
                threadId: input.providerThread.appThreadId ?? sessionInput.threadId,
                existingProviderThread: input.providerThread,
                modelSelection: sessionInput.modelSelection,
                runtimePolicy: sessionInput.runtimePolicy,
              });
              if (nativeId === null)
                return yield* fail("Droid provider thread has no native session id");
              const rpc = yield* requireConnection();
              const rewind = yield* rpc.request("droid.execute_rewind", {
                sessionId: nativeId,
                messageId,
                filesToRestore: [],
                filesToDelete: [],
                forkTitle: `Rollback ${input.providerThread.id}`,
              });
              const result = yield* decode(
                Schema.Struct({ newSessionId: Schema.String }),
                rewind.result,
              );
              yield* rpc.request("droid.load_session", {
                sessionId: result.newSessionId,
                autoRejectPermissionRequests: false,
              });

              const rolledBackTurns = new Set(
                Array.from(turns.values())
                  .filter(
                    (turn) =>
                      turn.providerThreadId === input.providerThread.id &&
                      (input.target.type === "thread_start" || turn.ordinal > boundaryOrdinal),
                  )
                  .map((turn) => turn.id),
              );
              for (const turnId of rolledBackTurns) turns.delete(turnId);
              for (const [messageId, turnId] of messageTurns) {
                if (rolledBackTurns.has(turnId)) {
                  messages.delete(messageId);
                  messageTurns.delete(messageId);
                }
              }
              nativeId = result.newSessionId;
              lastContext = null;
              wakeOwner = null;
              nativeNotificationTurnId = null;
              providerThread = {
                ...input.providerThread,
                nativeThreadRef: nativeRef(result.newSessionId),
                status: "idle",
                lastRunOrdinal: input.target.appRunOrdinal,
                updatedAt: yield* DateTime.now,
              };
              yield* emit({
                type: "provider_thread.updated",
                driver: DROID_PROVIDER,
                providerThread,
              });
              return {
                providerThread,
                providerTurns: [],
                messages: [],
                runtimeRequests: [],
              } satisfies ProviderAdapter.ProviderAdapterV2ThreadSnapshot;
            },
            (effect, input) =>
              effect.pipe(
                Effect.mapError((cause) =>
                  isRollbackThreadError(cause)
                    ? cause
                    : new ProviderAdapter.ProviderAdapterRollbackThreadError({
                        driver: DROID_PROVIDER,
                        providerThreadId: input.providerThread.id,
                        checkpointId: input.target.checkpointId,
                        cause,
                      }),
                ),
              ),
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
            startTurnWithFailureCleanup(input, true).pipe(
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
      },
      (effect, sessionInput) =>
        effect.pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: DROID_PROVIDER,
                providerSessionId: sessionInput.providerSessionId,
                cause,
              }),
          ),
        ),
    ),
  });
});
