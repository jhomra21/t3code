import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
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

  it.effect("keeps native installer ownership ahead of npm realpath ownership", () =>
    droidMaintenanceResolver
      .resolve({
        ...context("/home/user/.local/bin/droid"),
        realCommandPath: "/usr/local/lib/node_modules/droid/bin/droid",
      })
      .pipe(
        Effect.map((capabilities) => {
          expect(capabilities.update).toMatchObject({
            executable: "/home/user/.local/bin/droid",
            args: ["update"],
            lockKey: "droid-native",
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
