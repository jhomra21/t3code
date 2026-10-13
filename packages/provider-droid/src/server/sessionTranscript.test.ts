// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";

import {
  followDroidSubagent,
  makeDroidSubagentFollower,
  readDroidDefinitionModel,
  readDroidSessionAppend,
} from "./sessionTranscript.ts";

const withFile = <A, E>(use: (directory: string) => Effect.Effect<A, E>) =>
  Effect.promise(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "droid-session-read-"))).pipe(
    Effect.flatMap((directory) =>
      use(directory).pipe(
        Effect.ensuring(Effect.promise(() => NodeFSP.rm(directory, { recursive: true }))),
      ),
    ),
  );

const row = (value: object) => `${JSON.stringify(value)}\n`;
const assistantRow = (id: string, text: string) =>
  row({
    type: "message",
    id,
    message: { role: "assistant", content: [{ type: "text", text }] },
  });

describe("Droid session transcript", () => {
  it.effect("serializes final drains behind watch publication", () =>
    withFile((directory) =>
      Effect.scoped(
        Effect.gen(function* () {
          const sessionId = "43a7fb2d-79a1-4c49-a8d1-3b089cf7d202";
          yield* Effect.promise(() =>
            NodeFSP.writeFile(
              NodePath.join(directory, `${sessionId}.settings.json`),
              '{"model":"pinned"}',
            ),
          );
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          let publications = 0;
          const follower = yield* makeDroidSubagentFollower({
            directory,
            sessionId,
            readAppend: () =>
              Effect.succeed({ text: "", consumed: 0, restarted: false, inode: undefined }),
            onWork: () =>
              Effect.gen(function* () {
                publications++;
                if (publications === 1) {
                  yield* Deferred.succeed(entered, undefined);
                  yield* Deferred.await(release);
                }
              }),
          });
          const watching = yield* Effect.forkChild(follower.run(() => Effect.void));
          yield* Deferred.await(entered);
          const draining = yield* Effect.forkChild(follower.drain, { startImmediately: true });
          assert.isUndefined(draining.pollUnsafe());
          assert.equal(publications, 1);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(draining);
          assert.equal(publications, 2);
          yield* Fiber.interrupt(watching);
        }),
      ),
    ),
  );
  it.effect("shows a custom pinned model by its display name", () =>
    withFile((directory) =>
      Effect.gen(function* () {
        const droids = NodePath.join(directory, ".factory", "droids");
        yield* Effect.promise(() => NodeFSP.mkdir(droids, { recursive: true }));
        yield* Effect.promise(() =>
          NodeFSP.writeFile(
            NodePath.join(droids, "worker.md"),
            '---\nname: worker\nmodel: "custom:GPT-6 Luna-12"\n---\n',
          ),
        );
        assert.equal(
          readDroidDefinitionModel({ FACTORY_HOME_OVERRIDE: directory }, "worker"),
          "GPT-6 Luna",
        );
        assert.isUndefined(
          readDroidDefinitionModel({ FACTORY_HOME_OVERRIDE: directory }, "../../etc/passwd"),
        );
      }),
    ),
  );

  it.effect("does not consume a partial final JSONL row", () =>
    withFile((directory) =>
      Effect.gen(function* () {
        const file = NodePath.join(directory, "child.jsonl");
        const whole = assistantRow("reply", "complete");
        yield* Effect.promise(() => NodeFSP.writeFile(file, whole.slice(0, -1)));
        const partial = yield* readDroidSessionAppend(file, 0);
        assert.deepEqual(partial, {
          text: "",
          consumed: 0,
          restarted: false,
          inode: partial.inode,
        });
        yield* Effect.promise(() => NodeFSP.appendFile(file, "\n"));
        const complete = yield* readDroidSessionAppend(file, partial.consumed, partial.inode);
        assert.equal(complete.text, whole);
        assert.equal(complete.consumed, Buffer.byteLength(whole));
      }),
    ),
  );

  it.effect("restarts at byte zero after transcript truncation", () =>
    withFile((directory) =>
      Effect.gen(function* () {
        const file = NodePath.join(directory, "child.jsonl");
        const old = assistantRow("old", "old transcript");
        yield* Effect.promise(() => NodeFSP.writeFile(file, old));
        const first = yield* readDroidSessionAppend(file, 0);
        yield* Effect.promise(() => NodeFSP.writeFile(file, assistantRow("new", "replacement")));
        const restarted = yield* readDroidSessionAppend(file, first.consumed + 10, first.inode);
        assert.isTrue(restarted.restarted);
        assert.include(restarted.text, '"replacement"');
      }),
    ),
  );

  it.effect.each(["completed", "cancelled"] as const)(
    "stops following a child after its %s outcome",
    (reason) =>
      withFile((directory) =>
        Effect.scoped(
          Effect.gen(function* () {
            const sessionId = "43a7fb2d-79a1-4c49-a8d1-3b089cf7d202";
            const file = NodePath.join(directory, `${sessionId}.jsonl`);
            yield* Effect.promise(() =>
              NodeFSP.writeFile(
                NodePath.join(directory, "parent.jsonl"),
                row({ type: "session_start", id: "parent" }),
              ),
            );
            const completed = yield* Deferred.make<void>();
            const seen: string[] = [];
            const follower = followDroidSubagent({
              directory,
              sessionId,
              onWork: (work) =>
                Effect.gen(function* () {
                  seen.push(
                    ...work.steps.map((step) => (step.kind === "message" ? step.text : "")),
                  );
                  if (work.outcome !== undefined) yield* Deferred.succeed(completed, undefined);
                }),
            });
            const fiber = yield* Effect.forkChild(follower);
            yield* Effect.promise(() =>
              NodeFSP.writeFile(
                file,
                `${row({
                  type: "message",
                  timestamp: "2026-10-12T10:00:00.000Z",
                  message: { role: "user", content: [{ type: "text", text: "start" }] },
                })}${assistantRow("before", "before stop")}${row({ type: "agent_turn_outcome", reason })}`,
              ),
            );
            yield* Deferred.await(completed);
            yield* Fiber.join(fiber);
            yield* Effect.promise(() =>
              NodeFSP.appendFile(file, assistantRow("after", "after stop")),
            );
            assert.deepEqual(seen, ["before stop"]);
          }),
        ),
      ),
  );

  it.effect("fails on asynchronous watcher errors and removes the listener", () =>
    withFile((directory) =>
      Effect.scoped(
        Effect.gen(function* () {
          const sessionId = "43a7fb2d-79a1-4c49-a8d1-3b089cf7d202";
          const observed = yield* Deferred.make<void>();
          let watcher: NodeFS.FSWatcher | undefined;
          const watchDirectory = (
            path: string,
            listener: (event: string, name: string | Buffer | null) => void,
          ) => {
            watcher = NodeFS.watch(path, listener);
            return watcher;
          };
          const fiber = yield* Effect.forkChild(
            Effect.scoped(
              followDroidSubagent({
                directory,
                sessionId,
                onWork: () => Deferred.succeed(observed, undefined),
                watchDirectory,
              }),
            ),
          );
          yield* Deferred.await(observed);
          assert.isDefined(watcher);
          assert.isAbove(watcher.listenerCount("error"), 0);
          watcher.emit("error", new Error("synthetic watch failure"));
          const exit = yield* Fiber.await(fiber);
          assert.equal(exit._tag, "Failure");
          assert.equal(watcher.listenerCount("error"), 0);
        }),
      ),
    ),
  );
});
