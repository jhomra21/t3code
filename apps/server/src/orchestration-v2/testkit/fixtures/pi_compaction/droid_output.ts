import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertAssistantTextIncludes,
  assertSemanticProjectionIntegrity,
  projectionFor,
} from "../shared.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

export function assertDroidCompactionOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const compactRequests = transcript.entries.filter(
    (entry) =>
      entry.type === "expect_outbound" && field(entry.frame, "method") === "droid.compact_session",
  );
  assert.lengthOf(compactRequests, 1);
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertAssistantTextIncludes(projection, "compaction-marker-4Z7Q");
}
