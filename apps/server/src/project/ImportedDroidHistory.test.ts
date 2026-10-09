import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  EventId,
  OrchestrationV2AppThread,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import * as Stream from "effect/Stream";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Runtime from "../orchestration-v2/runtimeLayer.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ProviderInstances from "../provider/ProviderInstanceRegistry.ts";
import { makeDroidSessionManagement } from "../provider/DroidSessions.ts";
import * as ImportedHistory from "./ImportedDroidHistory.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ApplicationEvents from "../persistence/OrchestrationEventStore.ts";
import { subscribeOrchestrationV2Thread, listProviderSessionsForCwd } from "../ws.ts";

const decodeThread = Schema.decodeUnknownEffect(OrchestrationV2AppThread);
const encode = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const nativeId = "11111111-1111-4111-8111-111111111111";
const driver = ProviderDriverKind.make("droid");
const instanceId = ProviderInstanceId.make("droid");
const threadId = IdAllocator.deriveThreadFromProviderThread({
  driver,
  providerInstanceId: instanceId,
  nativeThreadId: nativeId,
});
const stores = Layer.mergeAll(
  Runtime.layerEventSink,
  ProjectionStore.layer,
  IdAllocator.layer,
).pipe(Layer.provideMerge(Sqlite.layerMemory));

it.effect(
  "persists full imported history, refreshes on open, and makes unchanged opens idempotent",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const home = yield* fs.makeTempDirectoryScoped();
      const directory = `${home}/.factory/sessions/project`;
      yield* fs.makeDirectory(directory, { recursive: true });
      const file = `${directory}/${nativeId}.jsonl`;
      const rows = [
        { type: "session_start", id: nativeId, cwd: "/project" },
        ...Array.from({ length: 151 }, (_, index) => ({
          type: "message",
          id: `message-${index}`,
          timestamp: "2026-10-01T10:00:00.000Z",
          message: { role: "assistant", content: [{ type: "text", text: `Message ${index}` }] },
        })),
      ];
      yield* fs.writeFileString(file, rows.map((row) => encode(row)).join("\n"));
      const manager = yield* makeDroidSessionManagement({ HOME: home });
      let loads = 0;
      let heldSession: string | undefined;
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const registry = Layer.mock(ProviderInstances.ProviderInstanceRegistry)({
        getInstance: () =>
          Effect.succeed({
            instanceId,
            driverKind: driver,
            enabled: true,
            displayName: undefined,
            continuationIdentity: { driverKind: driver, continuationKey: "droid" },
            sessionImport: {
              ...manager,
              loadSessionHistory: (input) =>
                Effect.gen(function* () {
                  loads++;
                  if (heldSession === input.sessionId) {
                    yield* Deferred.succeed(entered, undefined);
                    yield* Deferred.await(release);
                  }
                  return yield* manager.loadSessionHistory(input);
                }),
            },
            orchestrationAdapter: {
              instanceId,
              driver,
              getCapabilities: () => Effect.die("unused"),
              planSelectionTransition: () => Effect.die("unused"),
              openSession: () => Effect.die("must not launch Droid"),
            },
            textGeneration: {
              generateThreadTitle: () => Effect.die("unused"),
              generateBranchName: () => Effect.die("unused"),
              generateCommitMessage: () => Effect.die("unused"),
              generatePrContent: () => Effect.die("unused"),
            },
            snapshot: {
              getSnapshot: Effect.die("unused"),
              refresh: Effect.die("unused"),
              streamChanges: Stream.empty,
              resolveMaintenance: () => Effect.die("unused"),
              applyUsageLimits: () => Effect.void,
            },
          }),
      });
      yield* Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const now = yield* DateTime.now;
        const thread = yield* decodeThread({
          id: threadId,
          projectId: ProjectId.make("project"),
          title: "Imported",
          createdBy: "user",
          creationSource: "web",
          providerInstanceId: instanceId,
          modelSelection: { instanceId, model: "auto" },
          runtimeMode: "approval-required",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          activeProviderThreadId: null,
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
        yield* sink.write({
          events: [
            {
              id: EventId.make("create"),
              type: "thread.created",
              threadId,
              occurredAt: now,
              payload: thread,
            },
          ],
        });
        const history = yield* ImportedHistory.ImportedDroidHistory;
        const subscribe = subscribeOrchestrationV2Thread({ threadId }).pipe(
          Effect.flatMap(Stream.runHead),
          Effect.provide(
            Layer.merge(
              Layer.mock(ThreadManagement.ThreadManagementService)({
                ensureLegacyTranscript: () => Effect.void,
                getThreadSnapshot: () =>
                  projections.getThreadProjection(threadId).pipe(
                    Effect.map((projection) => ({
                      schemaVersion: 2,
                      snapshotSequence: 0,
                      projection,
                    })),
                    Effect.mapError(
                      (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
                    ),
                  ),
                streamStoredEventsFrom: () => Stream.empty,
              }),
              Layer.mock(ApplicationEvents.OrchestrationEventStore)({}),
            ),
          ),
        );
        yield* subscribe;
        assert.equal(loads, 0, "ordinary threads do not invoke native history");
        yield* history.importThread({ threadId, instanceId, cwd: "/project", sessionId: nativeId });
        assert.equal(yield* projections.getMessageCount(threadId), 151);
        const messages = (yield* projections.getThreadRecords(threadId, ["messages"])).messages;
        assert.equal(
          messages[0]?.id,
          ids.derive.messageFromProviderItem({ driver, nativeItemId: "message-0" }),
        );
        yield* fs.writeFileString(
          file,
          [
            ...rows,
            {
              type: "message",
              id: "late",
              message: { role: "assistant", content: [{ type: "text", text: "External update" }] },
            },
          ]
            .map((row) => encode(row))
            .join("\n"),
        );
        const open = Effect.gen(function* () {
          const management = yield* ThreadManagement.ThreadManagementService;
          return yield* management.getMessageCount(threadId);
        }).pipe(
          Effect.provide(
            ThreadManagement.layer.pipe(
              Layer.provide(
                Layer.mock(Orchestrator.OrchestratorV2)({
                  getMessageCount: (id) =>
                    projections
                      .getMessageCount(id)
                      .pipe(
                        Effect.mapError(
                          (cause) =>
                            new Orchestrator.OrchestratorProjectionError({ threadId: id, cause }),
                        ),
                      ),
                }),
              ),
            ),
          ),
        );
        assert.equal(yield* open, 151, "projection reads must not refresh history");
        const beforeListing = loads;
        const listing = yield* listProviderSessionsForCwd({ instanceId, cwd: "/project" }).pipe(
          Effect.provide(
            Layer.mock(ThreadManagement.ThreadManagementService)({
              getThreadShell: (id) =>
                projections
                  .getThreadShell(id)
                  .pipe(
                    Effect.mapError(
                      (cause) =>
                        new Orchestrator.OrchestratorProjectionError({ threadId: id, cause }),
                    ),
                  ),
            }),
          ),
        );
        assert.equal(listing.sessions[0]?.importedThreadId, threadId);
        assert.equal(loads, beforeListing, "listing must not re-import existing sessions");
        assert.equal(yield* projections.getMessageCount(threadId), 151);
        yield* subscribe;
        assert.equal(yield* open, 152);
        const afterRefresh = loads;
        yield* history.refreshThread(threadId);
        assert.equal(loads, afterRefresh, "unchanged size and mtime skip history reads");
        assert.equal(yield* open, 152);
        const childId = "22222222-2222-4222-8222-222222222222";
        const childThreadId = ids.derive.threadFromProviderThread({
          driver,
          providerInstanceId: instanceId,
          nativeThreadId: childId,
        });
        yield* fs.writeFileString(
          `${directory}/${childId}.jsonl`,
          [
            encode({
              type: "session_start",
              id: childId,
              cwd: "/project",
              callingSessionId: nativeId,
            }),
            encode({
              type: "message",
              id: "child-reply",
              message: { role: "assistant", content: [{ type: "text", text: "Child result" }] },
            }),
          ].join("\n"),
        );
        yield* sink.write({
          events: [
            {
              id: EventId.make("child-create"),
              type: "thread.created",
              threadId: childThreadId,
              occurredAt: now,
              payload: {
                ...thread,
                id: childThreadId,
                lineage: {
                  parentThreadId: threadId,
                  relationshipToParent: "subagent",
                  rootThreadId: threadId,
                },
              },
            },
          ],
        });
        yield* history.importThread({
          threadId: childThreadId,
          instanceId,
          cwd: "/project",
          sessionId: childId,
        });
        yield* history.importThread({
          threadId: childThreadId,
          instanceId,
          cwd: "/project",
          sessionId: childId,
        });
        assert.equal(yield* projections.getMessageCount(childThreadId), 1);
        assert.equal(
          (yield* projections.getThread(childThreadId)).lineage.parentThreadId,
          threadId,
        );
        assert.lengthOf(
          (yield* projections.getThreadRecords(childThreadId, ["providerThreads"])).providerThreads,
          1,
        );
        heldSession = nativeId;
        yield* fs.writeFileString(file, rows.map((row) => encode(row)).join("\n"));
        const parentRefresh = yield* history.refreshThread(threadId).pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        yield* fs.writeFileString(
          `${directory}/${childId}.jsonl`,
          [
            encode({ type: "session_start", id: childId, cwd: "/project" }),
            encode({
              type: "message",
              id: "child-new",
              message: {
                role: "assistant",
                content: [{ type: "text", text: "New child result" }],
              },
            }),
          ].join("\n"),
        );
        yield* history.refreshThread(childThreadId);
        assert.equal(
          yield* projections.getMessageCount(childThreadId),
          2,
          "another thread must not wait for the parent lock",
        );
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(parentRefresh);
        yield* fs.remove(file);
        yield* subscribe;
        assert.equal(yield* open, 152, "failed stat/refresh must not fail ordinary reads");
      }).pipe(
        Effect.provide(
          ImportedHistory.layer.pipe(Layer.provideMerge(Layer.merge(stores, registry))),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
