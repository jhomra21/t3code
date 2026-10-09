import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { makeDroidSessionManagement } from "./DroidSessions.ts";

// Failures: last-100 truncation; stale repeated reads; hidden/tool rows copied
// as user prose; child identity replaced; traversal; another project's sessions.
const encode = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const parent = "11111111-1111-4111-8111-111111111111";
const child = "22222222-2222-4222-8222-222222222222";
describe("Droid native session files", () => {
  it.effect(
    "lists roots and children and reads every message freshly without a Droid process",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = yield* fs.makeTempDirectoryScoped();
        const directory = path.join(home, ".factory", "sessions", "project");
        yield* fs.makeDirectory(directory, { recursive: true });
        const rows = [
          { type: "session_start", id: parent, cwd: "/project", title: "Full history" },
          {
            type: "message",
            id: "system-context",
            message: {
              role: "user",
              visibility: "llm_only",
              content: [{ type: "text", text: "Hidden prompt wrapper" }],
            },
          },
          ...Array.from({ length: 151 }, (_, index) => ({
            type: "message",
            id: `message-${index}`,
            timestamp: "2026-10-01T10:00:00.000Z",
            message: {
              role: index % 2 === 0 ? "user" : "assistant",
              content: [{ type: "text", text: `Message ${index}` }],
            },
          })),
        ];
        const file = path.join(directory, `${parent}.jsonl`);
        yield* fs.writeFileString(file, rows.map((row) => encode(row)).join("\n") + "\n");
        yield* fs.writeFileString(
          path.join(directory, `${child}.jsonl`),
          [
            encode({
              type: "session_start",
              id: child,
              callingSessionId: parent,
              cwd: "/project",
              title: "Child",
            }),
            encode({
              type: "message",
              id: "child-message",
              message: { role: "assistant", content: [{ type: "text", text: "Child result" }] },
            }),
          ].join("\n"),
        );
        const manager = yield* makeDroidSessionManagement({ HOME: home });
        const listed = yield* manager.listSessions({ cwd: "/project" });
        assert.sameMembers(
          listed.sessions.map((session) => session.sessionId),
          [parent, child],
        );
        assert.isEmpty((yield* manager.listSessions({ cwd: "/other" })).sessions);
        assert.lengthOf(
          yield* manager.loadSessionHistory({ cwd: "/project", sessionId: parent }),
          151,
        );
        yield* fs.writeFileString(
          file,
          [
            ...rows,
            {
              type: "message",
              id: "late",
              message: {
                role: "assistant",
                content: [{ type: "text", text: "New external reply" }],
              },
            },
          ]
            .map((row) => encode(row))
            .join("\n") + '\n{"type":',
        );
        const refreshed = yield* manager.loadSessionHistory({ cwd: "/project", sessionId: parent });
        assert.lengthOf(refreshed, 152);
        assert.equal(refreshed.at(-1)?.text, "New external reply");
        assert.equal(refreshed[0]?.nativeId, "message-0");
        assert.isFalse(refreshed.some((message) => message.text === "Hidden prompt wrapper"));
        assert.equal(
          (yield* manager.loadSessionHistory({ cwd: "/project", sessionId: child }))[0]?.text,
          "Child result",
        );
        assert.equal(
          (yield* manager
            .loadSessionHistory({ cwd: "/project", sessionId: "../escape" })
            .pipe(Effect.result))._tag,
          "Failure",
        );
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
