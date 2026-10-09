import {
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2TurnItem,
  MessageId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderInstances from "../provider/ProviderInstanceRegistry.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";

export class ImportedDroidHistoryError extends Schema.TaggedError<ImportedDroidHistoryError>()(
  "ImportedDroidHistoryError",
  { threadId: ThreadId, cause: Schema.Defect() },
) {}
type ImportInput = {
  readonly threadId: ThreadId;
  readonly instanceId: ProviderInstanceId;
  readonly sessionId: string;
  readonly cwd: string;
};
// A no-op reference keeps lightweight orchestration tests independent of native
// imports. Production installs the file-backed implementation below.
export class ImportedDroidHistory extends Context.Reference<{
  readonly importThread: (input: ImportInput) => Effect.Effect<void, ImportedDroidHistoryError>;
  readonly refreshThread: (threadId: ThreadId) => Effect.Effect<void>;
}>("t3/project/ImportedDroidHistory", {
  defaultValue: () => ({ importThread: () => Effect.void, refreshThread: () => Effect.void }),
}) {}

const DROID = ProviderDriverKind.make("droid");
const make = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const instances = yield* ProviderInstances.ProviderInstanceRegistry;
  const sink = yield* EventSink.EventSinkV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const locks = new Map<ThreadId, { readonly lock: Semaphore.Semaphore; users: number }>();
  const revisions = new Map<ThreadId, Map<string, string>>();
  const withThreadLock = <A, E, R>(threadId: ThreadId, effect: Effect.Effect<A, E, R>) =>
    Effect.suspend(() => {
      const entry = locks.get(threadId) ?? { lock: Semaphore.makeUnsafe(1), users: 0 };
      locks.set(threadId, entry);
      entry.users++;
      return entry.lock
        .withPermits(1)(effect)
        .pipe(
          Effect.ensuring(
            Effect.sync(() => {
              if (--entry.users === 0) locks.delete(threadId);
            }),
          ),
        );
    });
  const importThread = Effect.fn("ImportedDroidHistory.importThread")(
    function* (input: ImportInput) {
      const instance = yield* instances.getInstance(input.instanceId);
      if (instance?.driverKind !== DROID || instance.sessionImport === undefined) return;
      const records = yield* projections.getThreadRecords(input.threadId, [
        "providerThreads",
        "messages",
        "turnItems",
        "runs",
      ]);
      // A reader must not replace streaming/approval-owned artifacts mid-turn.
      if (
        records.runs.some((run) =>
          ["preparing", "starting", "running", "waiting"].includes(run.status),
        )
      )
        return;
      const revisionKey = `${input.instanceId}\0${input.sessionId}\0${input.cwd}`;
      const revision = yield* (
        instance.sessionImport.getSessionHistoryRevision?.({
          cwd: input.cwd,
          sessionId: input.sessionId,
        }) ?? Effect.succeed(undefined)
      );
      if (revision !== undefined && revisions.get(input.threadId)?.get(revisionKey) === revision)
        return;
      const history = yield* instance.sessionImport.loadSessionHistory({
        cwd: input.cwd,
        sessionId: input.sessionId,
      });
      const now = yield* DateTime.now;
      const existingProvider = records.providerThreads.find(
        (thread) => thread.driver === DROID && thread.nativeThreadRef?.nativeId === input.sessionId,
      );
      const providerThread: OrchestrationV2ProviderThread =
        existingProvider === undefined
          ? {
              id: ids.derive.providerThread({
                driver: DROID,
                providerInstanceId: input.instanceId,
                nativeThreadId: input.sessionId,
              }),
              driver: DROID,
              providerInstanceId: input.instanceId,
              providerSessionId: null,
              appThreadId: input.threadId,
              ownerNodeId: null,
              nativeThreadRef: { driver: DROID, nativeId: input.sessionId, strength: "strong" },
              nativeConversationHeadRef: null,
              status: "not_loaded",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              contextUsage: null,
              nativeMetadata: { importedSessionCwd: input.cwd, itemIdentityVersion: 2 },
              createdAt: now,
              updatedAt: now,
            }
          : {
              ...existingProvider,
              nativeMetadata: { ...existingProvider.nativeMetadata, importedSessionCwd: input.cwd },
            };
      const events: OrchestrationV2DomainEvent[] = [];
      if (existingProvider?.nativeMetadata?.importedSessionCwd !== input.cwd)
        events.push({
          id: yield* ids.allocate.event({ threadId: input.threadId }),
          type: "provider-thread.updated",
          threadId: input.threadId,
          driver: DROID,
          providerInstanceId: input.instanceId,
          occurredAt: now,
          payload: providerThread,
        });
      const messages = new Map(records.messages.map((message) => [message.id, message]));
      const items = new Map(
        records.turnItems.flatMap((item) =>
          "messageId" in item ? [[item.messageId, item] as const] : [],
        ),
      );
      let ordinal = records.turnItems.reduce((max, item) => Math.max(max, item.ordinal), 0);
      for (const entry of history) {
        // Same identities as native streaming, not ordinal-based ACP import ids.
        const appMessageId = MessageId.make(entry.nativeId);
        const messageId = messages.has(appMessageId)
          ? appMessageId
          : ids.derive.messageFromProviderItem({ driver: DROID, nativeItemId: entry.nativeId });
        const previous = messages.get(messageId);
        if (previous?.text === entry.text && previous.role === entry.role && !previous.streaming)
          continue;
        const at = Option.match(
          entry.createdAt === undefined ? Option.none() : DateTime.make(entry.createdAt),
          {
            onNone: () => now,
            onSome: (value) => DateTime.toUtc(value),
          },
        );
        const message: OrchestrationV2ConversationMessage =
          previous === undefined
            ? {
                id: messageId,
                threadId: input.threadId,
                runId: null,
                nodeId: null,
                role: entry.role,
                text: entry.text,
                attachments: [],
                streaming: false,
                createdBy: entry.role === "user" ? "user" : "agent",
                creationSource: "provider",
                createdAt: at,
                updatedAt: at,
              }
            : { ...previous, text: entry.text, streaming: false };
        const previousItem = items.get(messageId);
        const base = previousItem ?? {
          id: ids.derive.turnItemFromProviderItem({ driver: DROID, nativeItemId: entry.nativeId }),
          threadId: input.threadId,
          runId: null,
          nodeId: null,
          providerThreadId: providerThread.id,
          providerTurnId: null,
          nativeItemRef: { driver: DROID, nativeId: entry.nativeId, strength: "strong" as const },
          parentItemId: null,
          ordinal: ++ordinal,
          status: "completed" as const,
          title: null,
          startedAt: at,
          completedAt: at,
          updatedAt: at,
        };
        const item: OrchestrationV2TurnItem =
          entry.role === "user"
            ? {
                ...base,
                type: "user_message",
                createdBy: message.createdBy,
                creationSource: "provider",
                messageId,
                inputIntent: "turn_start",
                text: entry.text,
                attachments: message.attachments,
              }
            : { ...base, type: "assistant_message", messageId, text: entry.text, streaming: false };
        events.push(
          {
            id: yield* ids.allocate.event({ threadId: input.threadId }),
            type: "message.updated",
            threadId: input.threadId,
            occurredAt: now,
            payload: message,
          },
          {
            id: yield* ids.allocate.event({ threadId: input.threadId }),
            type: "turn-item.updated",
            threadId: input.threadId,
            occurredAt: now,
            payload: item,
          },
        );
      }
      if (events.length > 0) yield* sink.write({ events });
      if (revision !== undefined) {
        const current = revisions.get(input.threadId) ?? new Map<string, string>();
        current.set(revisionKey, revision);
        revisions.set(input.threadId, current);
      }
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) => new ImportedDroidHistoryError({ threadId: input.threadId, cause }),
        ),
      ),
  );
  const refreshThread = Effect.fn("ImportedDroidHistory.refreshThread")(
    function* (threadId: ThreadId) {
      const { providerThreads } = yield* projections.getThreadRecords(threadId, [
        "providerThreads",
      ]);
      for (const provider of providerThreads) {
        const cwd = provider.nativeMetadata?.importedSessionCwd;
        const sessionId = provider.nativeThreadRef?.nativeId;
        if (provider.driver === DROID && cwd !== undefined && sessionId != null)
          yield* importThread({
            threadId,
            instanceId: provider.providerInstanceId,
            cwd,
            sessionId,
          });
      }
    },
    (effect, threadId) =>
      effect.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Unable to refresh imported Droid history", { threadId, cause }),
        ),
      ),
  );
  return {
    importThread: (input: ImportInput) => withThreadLock(input.threadId, importThread(input)),
    refreshThread: (threadId: ThreadId) => withThreadLock(threadId, refreshThread(threadId)),
  };
});
export const layer = Layer.effect(ImportedDroidHistory, make);
