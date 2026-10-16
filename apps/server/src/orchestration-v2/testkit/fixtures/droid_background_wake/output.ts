import { assert } from "@effect/vitest";
import type { OrchestrationV2ThreadProjection, ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  projectionFor,
} from "../shared.ts";
import { DROID_BACKGROUND_WAKE_PROMPT } from "./input.ts";

function assistantTexts(
  projection: OrchestrationV2ThreadProjection,
  runId: string | undefined,
): ReadonlyArray<string> {
  return projection.turnItems.flatMap((item) =>
    item.runId === runId && item.type === "assistant_message" ? [item.text.trim()] : [],
  );
}

export function assertDroidBackgroundWakeOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [DROID_BACKGROUND_WAKE_PROMPT]);

  const [rootRun, wakeRun] = projection.runs;
  assert.deepEqual(assistantTexts(projection, rootRun?.id), ["STARTED"]);
  assert.deepEqual(assistantTexts(projection, wakeRun?.id), ["WAKE_DONE"]);
  const wakeMessage = projection.messages.find((message) => message.id === wakeRun?.userMessageId);
  assert.equal(wakeMessage?.createdBy, "agent");
  assert.equal(wakeMessage?.creationSource, "provider");

  assert.lengthOf(projection.subagents, 1);
  assert.equal(projection.subagents[0]?.status, "completed");
  assert.deepEqual(projection.providerThreads[0]?.pendingBackgroundTasks ?? [], []);
}
