import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { DroidSettings } from "../settings.ts";
import {
  makeDroidEnvironment,
  makeDroidInstanceEnvironment,
  makePendingDroidProvider,
  checkDroidProviderStatus,
} from "./status.ts";

const settings = Schema.decodeSync(DroidSettings)({});
const missingBinarySettings = Schema.decodeSync(DroidSettings)({
  enabled: true,
  binaryPath: "/definitely-missing/droid",
});

describe("Droid status", () => {
  it.effect("does not probe a disabled provider", () =>
    Effect.gen(function* () {
      const snapshot = yield* makePendingDroidProvider(settings);
      assert.isFalse(snapshot.enabled);
      assert.equal(snapshot.auth.status, "unknown");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("offers manual compaction only while enabled", () =>
    Effect.gen(function* () {
      const disabled = yield* makePendingDroidProvider(settings);
      const enabled = yield* makePendingDroidProvider(missingBinarySettings);
      assert.deepEqual(disabled.slashCommands, []);
      assert.deepEqual(
        enabled.slashCommands.map((command) => command.name),
        ["compact"],
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reports a missing CLI as not installed", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkDroidProviderStatus(missingBinarySettings, {}, process.cwd());
      assert.isFalse(snapshot.installed);
      assert.include(snapshot.message ?? "", "not found");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it("adds non-interactive settings without mutating the given environment", () => {
    const environment = makeDroidEnvironment({
      FACTORY_API_KEY: "inherited-factory-key",
      DROID_API_KEY: "inherited-droid-key",
      PATH: "/usr/bin",
    });
    assert.equal(environment.FACTORY_API_KEY, "inherited-factory-key");
    assert.equal(environment.DROID_API_KEY, "inherited-droid-key");
    assert.equal(environment.PATH, "/usr/bin");
    assert.equal(environment.FACTORY_DISABLE_SETTINGS_PERSISTENCE, "1");
    assert.equal(environment.DROID_DISABLE_AUTO_UPDATE, "1");
  });

  it.effect("isolates instance credentials from the host environment", () =>
    Effect.gen(function* () {
      const environment = yield* makeDroidInstanceEnvironment(
        [{ name: "FACTORY_API_KEY", value: "instance-factory-key", sensitive: true }],
        {
          FACTORY_API_KEY: "host-factory-key",
          DROID_API_KEY: "host-droid-key",
          PATH: "/usr/bin",
        },
      );
      assert.equal(environment.FACTORY_API_KEY, "instance-factory-key");
      assert.isUndefined(environment.DROID_API_KEY);
      assert.equal(environment.PATH, "/usr/bin");
    }),
  );
});
