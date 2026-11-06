import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodePath from "@effect/platform-node/NodePath";
import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import { makeTargetedProviderUpdateAction } from "@t3tools/provider-core/server/maintenanceResolver";
import { droidMaintenanceResolver, enrichDroidSnapshot } from "./maintenance.ts";

const driver = ProviderDriverKind.make("droid");
const context = (path: string) => ({
  binaryPath: path,
  resolvedCommandPath: path,
  realCommandPath: path,
  env: {},
  platform: HostProcess.Platform.defaultValue(),
});

const installedDroid: ServerProvider = {
  instanceId: ProviderInstanceId.make("droid"),
  driver,
  enabled: true,
  installed: true,
  version: "0.240.0",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-10-14T00:00:00.000Z",
  models: [],
  slashCommands: [],
  skills: [],
};

it.layer(NodeServices.layer)("droid maintenance", (it) => {
  it.effect.each([
    { platform: "win32", path: "C:\\sandbox\\bin\\droid.exe", fails: false, removes: true },
    { platform: "win32", path: "C:\\sandbox\\bin\\droid.exe", fails: true, removes: true },
    { platform: "linux", path: "/home/user/.local/bin/droid", fails: false, removes: false },
    { platform: "darwin", path: "/home/user/.local/bin/droid", fails: false, removes: false },
  ] satisfies ReadonlyArray<{
    platform: NodeJS.Platform;
    path: string;
    fails: boolean;
    removes: boolean;
  }>)(
    "cleans only the Windows backup after a verified update: $platform, deletion fails=$fails",
    ({ platform, path, fails, removes }) => {
      const deleted: string[] = [];
      const files = new Set([
        "C:\\sandbox\\bin\\droid.exe.old",
        path,
        "C:\\sandbox\\bin\\other.exe.old",
      ]);
      const fileSystem = FileSystem.makeNoop({
        remove: (target, options) =>
          Effect.suspend(() => {
            deleted.push(target);
            expect(options?.recursive).not.toBe(true);
            if (fails) return FileSystem.makeNoop({}).remove(target);
            return Effect.sync(() => {
              files.delete(target);
            });
          }),
      });
      return Effect.gen(function* () {
        const capabilities = yield* droidMaintenanceResolver.resolve({
          ...context(path),
          platform,
        });
        expect(deleted).toEqual([]);
        if (removes) expect(capabilities.onUpdated).toBeDefined();
        yield* capabilities.onUpdated ?? Effect.void;
        expect(deleted).toEqual(removes ? ["C:\\sandbox\\bin\\droid.exe.old"] : []);
        expect(files.has("C:\\sandbox\\bin\\droid.exe.old")).toBe(!removes || fails);
        expect(files.has(path)).toBe(true);
        expect(files.has("C:\\sandbox\\bin\\other.exe.old")).toBe(true);
      }).pipe(
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.provide(NodePath.layerWin32),
      );
    },
  );

  it.effect.each([
    "C:\\Users\\user\\bin\\droid.exe",
    "D:/Users/Some User/bin/DROID.EXE",
    "C:\\tools\\droid.exe",
  ])("uses native update for Windows standalone %s", (executable) =>
    droidMaintenanceResolver.resolve({ ...context(executable), platform: "win32" }).pipe(
      Effect.map((capabilities) =>
        expect(capabilities.update).toMatchObject({
          executable,
          args: ["update"],
          lockKey: "droid-native",
        }),
      ),
    ),
  );

  it.effect.each([
    {
      realPath: "C:\\work\\node_modules\\@factory\\cli-win32-x64\\bin\\droid.exe",
      lockKey: null,
    },
    {
      realPath:
        "C:\\Users\\user\\AppData\\Local\\Yarn\\Data\\global\\node_modules\\droid\\bin\\droid.exe",
      lockKey: "yarn-global",
    },
    {
      realPath:
        "C:\\Users\\user\\AppData\\Local\\pnpm\\global\\5\\node_modules\\droid\\bin\\droid.exe",
      lockKey: "pnpm-global",
    },
  ])(
    "uses the package manager for a standalone launcher targeting $realPath",
    ({ realPath, lockKey }) =>
      droidMaintenanceResolver
        .resolve({
          ...context("C:\\Users\\user\\bin\\droid.exe"),
          platform: "win32",
          realCommandPath: realPath,
        })
        .pipe(
          Effect.map((capabilities) => {
            if (lockKey === null) expect(capabilities.update).toBeNull();
            else expect(capabilities.update).toMatchObject({ lockKey });
          }),
        ),
  );

  it.effect.each([
    {
      executable: "C:\\Users\\user\\bin\\droid.exe",
      realPath: "C:\\Users\\user\\bin\\droid.exe",
      native: true,
    },
    {
      executable: "D:/Users/Some User/bin/DROID.EXE",
      realPath: "D:/Users/Some User/bin/DROID.EXE",
      native: true,
    },
    {
      executable: "C:\\other\\bin\\droid.exe",
      realPath: "C:\\work\\node_modules\\@factory\\cli-win32-x64\\bin\\droid.exe",
      native: false,
    },
    {
      executable: "C:\\Users\\user\\node_modules\\@factory\\cli-win32-x64\\bin\\droid.exe",
      realPath: "C:\\work\\node_modules\\@factory\\cli-win32-x64\\bin\\droid.exe",
      native: false,
    },
    {
      executable: "C:\\Users\\user\\bin\\droid.exe",
      realPath: "C:\\work\\node_modules\\@factory\\cli-win32-x64\\bin\\droid.exe",
      native: false,
    },
  ])(
    "distinguishes Windows installer ownership for $executable",
    ({ executable, realPath, native }) =>
      droidMaintenanceResolver
        .resolve({
          ...context(executable),
          platform: "win32",
          realCommandPath: realPath,
        })
        .pipe(
          Effect.map((capabilities) => {
            if (native)
              expect(capabilities.update).toMatchObject({
                executable,
                args: ["update"],
                lockKey: "droid-native",
              });
            else expect(capabilities.update).toBeNull();
          }),
        ),
  );

  it.effect("uses Droid's native updater for its official binary path", () =>
    droidMaintenanceResolver.resolve(context("/home/user/.local/bin/droid")).pipe(
      Effect.map((capabilities) => {
        expect(capabilities.update).toMatchObject({
          executable: "/home/user/.local/bin/droid",
          args: ["update"],
          lockKey: "droid-native",
        });
      }),
    ),
  );

  it.effect("recognizes the native updater through the real path", () =>
    droidMaintenanceResolver
      .resolve({
        ...context("/tmp/droid-link"),
        realCommandPath: "/home/user/.local/bin/droid",
      })
      .pipe(
        Effect.map((capabilities) => {
          expect(capabilities.update).toMatchObject({
            executable: "/tmp/droid-link",
            args: ["update"],
            lockKey: "droid-native",
          });
        }),
      ),
  );

  it.effect("uses npm ownership when a native-looking path resolves to npm", () =>
    droidMaintenanceResolver
      .resolve({
        ...context("/home/user/.local/bin/droid"),
        realCommandPath: "/usr/local/lib/node_modules/droid/bin/droid",
      })
      .pipe(
        Effect.map((capabilities) => {
          expect(capabilities.update).toMatchObject({
            executable: "npm",
            lockKey: "npm-global:/usr/local",
          });
        }),
      ),
  );

  it.effect("reinstalls npm globals and pins targeted versions", () =>
    droidMaintenanceResolver.resolve(context("/usr/local/lib/node_modules/droid/bin/droid")).pipe(
      Effect.map((capabilities) => {
        expect(capabilities.update).toMatchObject({
          executable: "npm",
          args: expect.arrayContaining(["droid@latest"]),
        });
        expect(makeTargetedProviderUpdateAction(capabilities, "0.241.1")?.args).toContain(
          "droid@0.241.1",
        );
      }),
    ),
  );

  it.effect("uses bun for bun global installations", () =>
    droidMaintenanceResolver.resolve(context("/home/user/.bun/bin/droid")).pipe(
      Effect.map((capabilities) => {
        expect(capabilities.update).toMatchObject({
          executable: "bun",
          args: ["i", "-g", "droid@latest"],
        });
      }),
    ),
  );

  it.effect("keeps unproven project-local installs manual", () =>
    droidMaintenanceResolver.resolve(context("/work/project/node_modules/droid/bin/droid")).pipe(
      Effect.map((capabilities) => {
        expect(capabilities.update).toBeNull();
      }),
    ),
  );

  it.effect("gets npm's latest advisory only when update checks are enabled", () =>
    (() => {
      const fetches: string[] = [];
      const http = HttpClient.make((request) => {
        fetches.push(request.url);
        return Effect.succeed(
          HttpClientResponse.fromWeb(request, Response.json({ version: "0.241.1" })),
        );
      });
      return Effect.gen(function* () {
        const capabilities = yield* droidMaintenanceResolver.resolve(null);
        const enriched = yield* enrichDroidSnapshot({
          snapshot: installedDroid,
          maintenanceCapabilities: capabilities,
          enableProviderUpdateChecks: true,
        });
        expect(enriched.versionAdvisory.latestVersion).toBe("0.241.1");
        expect(fetches).toEqual(["https://registry.npmjs.org/droid/latest"]);

        fetches.length = 0;
        const disabled = yield* enrichDroidSnapshot({
          snapshot: installedDroid,
          maintenanceCapabilities: capabilities,
          enableProviderUpdateChecks: false,
        });
        expect(disabled.versionAdvisory.latestVersion).toBeNull();
        expect(fetches).toEqual([]);
      }).pipe(
        Effect.provide(ProviderLatestVersions.layer),
        Effect.provideService(HttpClient.HttpClient, http),
      );
    })(),
  );
});
