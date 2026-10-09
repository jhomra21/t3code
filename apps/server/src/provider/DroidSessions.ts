import { AcpRegistryOperationError, type AcpRegistrySession } from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

const isSessionId = Schema.is(Schema.String.check(Schema.isUUID()));
const Header = Schema.Struct({
  type: Schema.Literal("session_start"),
  id: Schema.String,
  cwd: Schema.String,
  title: Schema.optionalKey(Schema.String),
});
const Row = Schema.Struct({
  type: Schema.Literal("message"),
  id: Schema.String,
  timestamp: Schema.optionalKey(Schema.String),
  message: Schema.Struct({
    role: Schema.Literals(["user", "assistant"]),
    visibility: Schema.optionalKey(Schema.String),
    content: Schema.Array(
      Schema.Struct({ type: Schema.String, text: Schema.optionalKey(Schema.String) }),
    ),
  }),
});
const decodeHeader = Schema.decodeUnknownOption(Schema.fromJsonString(Header));
const decodeRow = Schema.decodeUnknownOption(Schema.fromJsonString(Row));
const PAGE_SIZE = 100;

/** Read-only filesystem import; never loads a Droid session or runs its hooks. */
export const makeDroidSessionManagement = Effect.fn("makeDroidSessionManagement")(function* (
  environment: NodeJS.ProcessEnv,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home = environment.FACTORY_HOME_OVERRIDE ?? environment.HOME ?? environment.USERPROFILE;
  const root = home === undefined ? undefined : path.join(home, ".factory", "sessions");
  const sessionFiles = new Map<string, string>();
  const scan = Effect.fn("droid.sessions.scan")(function* (cwd: string, sessionId?: string) {
    if (root === undefined || !(yield* fs.exists(root))) return [];
    const directories = yield* fs.readDirectory(root);
    const sessions: Array<{ readonly file: string; readonly session: AcpRegistrySession }> = [];
    for (const directory of directories) {
      const folder = path.join(root, directory);
      if (!(yield* fs.stat(folder)).type.includes("Directory")) continue;
      const files = yield* fs.readDirectory(folder);
      for (const name of files) {
        if (sessionId !== undefined && name !== `${sessionId}.jsonl`) continue;
        if (!name.endsWith(".jsonl") || !isSessionId(name.slice(0, -6))) continue;
        const file = path.join(folder, name);
        const header = yield* Effect.gen(function* () {
          const handle = yield* fs.open(file, { flag: "r" });
          const bytes = yield* handle.readAlloc(64 * 1024);
          return Option.isNone(bytes)
            ? Option.none()
            : decodeHeader(new TextDecoder().decode(bytes.value).split("\n")[0] ?? "");
        }).pipe(
          Effect.scoped,
          Effect.orElseSucceed(() => Option.none()),
        );
        if (
          Option.isNone(header) ||
          header.value.id !== name.slice(0, -6) ||
          path.resolve(header.value.cwd) !== path.resolve(cwd)
        )
          continue;
        const stat = yield* fs.stat(file);
        sessions.push({
          file,
          session: {
            sessionId: header.value.id,
            cwd: header.value.cwd,
            additionalDirectories: [],
            title: header.value.title?.slice(0, 1024) || null,
            updatedAt: Option.isSome(stat.mtime) ? stat.mtime.value.toISOString() : null,
            importedThreadId: null,
          },
        });
      }
    }
    return sessions.toSorted(
      (left, right) =>
        (right.session.updatedAt ?? "").localeCompare(left.session.updatedAt ?? "") ||
        left.session.sessionId.localeCompare(right.session.sessionId),
    );
  });
  const listSessions = Effect.fn("droid.sessions.list")(
    function* (input: { readonly cwd: string; readonly cursor?: string }) {
      const offset = input.cursor === undefined ? 0 : Number(input.cursor);
      if (!Number.isSafeInteger(offset) || offset < 0)
        return yield* new AcpRegistryOperationError({
          reason: "session_list_unsupported",
          message: "Invalid native session cursor.",
        });
      const sessions = yield* scan(input.cwd);
      return {
        sessions: sessions.slice(offset, offset + PAGE_SIZE).map((entry) => entry.session),
        nextCursor: offset + PAGE_SIZE < sessions.length ? String(offset + PAGE_SIZE) : null,
        canLoad: true,
        canResume: true,
        canDelete: false,
      };
    },
    Effect.mapError(
      (cause) =>
        new AcpRegistryOperationError({
          reason: "session_list_unsupported",
          message: "Could not list native Droid session files.",
          cause,
        }),
    ),
  );
  const loadSessionHistory = Effect.fn("droid.sessions.history")(
    function* (input: { readonly cwd: string; readonly sessionId: string }) {
      if (!isSessionId(input.sessionId))
        return yield* new AcpRegistryOperationError({
          reason: "session_import_failed",
          message: "Invalid native Droid session id.",
        });
      const session = (yield* scan(input.cwd, input.sessionId))[0];
      if (session === undefined)
        return yield* new AcpRegistryOperationError({
          reason: "session_import_failed",
          message: "Native Droid session not found in this project.",
        });
      sessionFiles.set(`${input.cwd}\0${input.sessionId}`, session.file);
      const content = yield* fs.readFileString(session.file);
      return content.split("\n").flatMap((line) => {
        const row = decodeRow(line);
        if (
          Option.isNone(row) ||
          row.value.message.visibility === "llm_only" ||
          row.value.message.visibility === "hidden"
        )
          return [];
        const text = row.value.message.content
          .filter((part) => part.type === "text")
          .map((part) => part.text ?? "")
          .join("\n\n");
        return text.trim() === ""
          ? []
          : [
              {
                role: row.value.message.role,
                text,
                nativeId: row.value.id,
                ...(row.value.timestamp === undefined ? {} : { createdAt: row.value.timestamp }),
              },
            ];
      });
    },
    Effect.mapError(
      (cause) =>
        new AcpRegistryOperationError({
          reason: "session_import_failed",
          message: "Could not read the full native Droid transcript.",
          cause,
        }),
    ),
  );
  const getSessionHistoryRevision = Effect.fn("droid.sessions.revision")(
    function* (input: { readonly cwd: string; readonly sessionId: string }) {
      if (!isSessionId(input.sessionId))
        return yield* new AcpRegistryOperationError({
          reason: "session_import_failed",
          message: "Invalid native Droid session id.",
        });
      const key = `${input.cwd}\0${input.sessionId}`;
      const file = sessionFiles.get(key) ?? (yield* scan(input.cwd, input.sessionId))[0]?.file;
      if (file === undefined)
        return yield* new AcpRegistryOperationError({
          reason: "session_import_failed",
          message: "Native Droid session not found in this project.",
        });
      sessionFiles.set(key, file);
      const stat = yield* fs.stat(file);
      return `${stat.size}:${Option.isSome(stat.mtime) ? stat.mtime.value.getTime() : 0}`;
    },
    Effect.mapError(
      (cause) =>
        new AcpRegistryOperationError({
          reason: "session_import_failed",
          message: "Could not stat native Droid transcript.",
          cause,
        }),
    ),
  );
  return { listSessions, loadSessionHistory, getSessionHistoryRevision } satisfies NonNullable<
    ProviderInstance["sessionImport"]
  >;
});
