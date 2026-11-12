import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertAssistantTextIncludes,
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  projectionFor,
  MESSAGE_STEERING_INITIAL_PROMPT,
  MESSAGE_STEERING_STEER_PROMPT,
} from "../shared.ts";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

export function assertDroidMessageSteeringOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [
    MESSAGE_STEERING_INITIAL_PROMPT,
    MESSAGE_STEERING_STEER_PROMPT,
  ]);
  assertAssistantTextIncludes(projection, "steering fixture observed");
  assert.lengthOf(projection.providerTurns, 1);
  const steeringRequests = transcript.entries.flatMap((entry) =>
    entry.type === "expect_outbound" &&
    field(entry.frame, "method") === "droid.add_user_message" &&
    field(field(entry.frame, "params"), "queuePlacement") === "end_of_loop"
      ? [entry]
      : [],
  );
  assert.lengthOf(steeringRequests, 1);
}
