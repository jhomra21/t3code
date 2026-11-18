import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { assertQueuedTurnOutput } from "./codex_output.ts";

export function assertDroidQueuedTurnOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertQueuedTurnOutput(result, transcript);
  assert.equal(transcript.provider, "droid");
}
