import { assert, describe, it } from "@effect/vitest";

import { extractDroidSubagentUpdate } from "./DroidAcp.ts";

const extract = extractDroidSubagentUpdate({
  environment: { FACTORY_HOME_OVERRIDE: "/nonexistent" },
});

const task = (rawInput: Record<string, unknown>) =>
  extract({
    toolCallId: "call_1",
    status: "inProgress",
    data: {
      rawInput: { subagent_type: "worker-luna", description: "Do it", prompt: "p", ...rawInput },
    },
  });

describe("extractDroidSubagentUpdate effort", () => {
  it("shows Task complexity as the effort a reader expects", () => {
    assert.strictEqual(task({ complexity: "light" })?.effort, "low");
    assert.strictEqual(task({ complexity: "medium" })?.effort, "medium");
    assert.strictEqual(task({ complexity: "heavy" })?.effort, "high");
  });

  it("reports no effort when complexity is missing or not one Task accepts", () => {
    assert.isUndefined(task({})?.effort);
    assert.isUndefined(task({ complexity: "high" })?.effort);
    assert.isUndefined(task({ complexity: 3 })?.effort);
    assert.isUndefined(task({ complexity: "constructor" })?.effort);
  });
});
