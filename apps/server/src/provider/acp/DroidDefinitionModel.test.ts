// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { assert, describe, it } from "@effect/vitest";

import { readDroidDefinitionModel } from "./DroidSessionTranscript.ts";

const withHome = async (
  droids: Record<string, string>,
  run: (environment: NodeJS.ProcessEnv) => void,
) => {
  const home = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "droid-defs-"));
  try {
    await NodeFSP.mkdir(NodePath.join(home, ".factory", "droids"), { recursive: true });
    for (const [name, source] of Object.entries(droids)) {
      await NodeFSP.writeFile(NodePath.join(home, ".factory", "droids", `${name}.md`), source);
    }
    run({ FACTORY_HOME_OVERRIDE: home });
  } finally {
    await NodeFSP.rm(home, { recursive: true, force: true });
  }
};

describe("readDroidDefinitionModel", () => {
  it("shows a pinned custom model by its display name", () =>
    withHome(
      { luna: '---\nname: luna\nmodel: "custom:GPT-6 Luna-12"\ntools:\n  - Read\n---\nbody' },
      (environment) =>
        assert.strictEqual(
          readDroidDefinitionModel({ environment, droidName: "luna" }),
          "GPT-6 Luna",
        ),
    ));

  it("is undefined for inherited models, missing droids, and names that escape the folder", () =>
    withHome({ plain: "---\nname: plain\n---\nbody", other: "model: x" }, (environment) => {
      assert.isUndefined(readDroidDefinitionModel({ environment, droidName: "plain" }));
      assert.isUndefined(readDroidDefinitionModel({ environment, droidName: "other" }));
      assert.isUndefined(readDroidDefinitionModel({ environment, droidName: "missing" }));
      assert.isUndefined(readDroidDefinitionModel({ environment, droidName: "../../etc/passwd" }));
    }));
});
