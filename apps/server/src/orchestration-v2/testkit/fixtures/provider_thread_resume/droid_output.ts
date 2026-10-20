import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  projectionFor,
  PROVIDER_THREAD_RESUME_FIRST_PROMPT,
  PROVIDER_THREAD_RESUME_SECOND_PROMPT,
} from "../shared.ts";

const FIRST_FINAL = "provider thread resume fixture first turn complete";
const SECOND_FINAL = "provider thread resume fixture second turn complete";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * Droid's durable native thread id is its session id. After the idle release,
 * the respawned process must `droid.load_session` the id the first process
 * returned, and the resumed model still sees the first exchange.
 */
export function assertDroidProviderThreadResumeOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const outbound = (method: string) =>
    transcript.entries.flatMap((entry) =>
      entry.type === "expect_outbound" && field(entry.frame, "method") === method ? [entry] : [],
    );
  assert.lengthOf(
    outbound("droid.initialize_session"),
    1,
    "only the first process initializes a new session",
  );
  const sessionId = transcript.entries.flatMap((entry) =>
    entry.type === "emit_inbound" && field(entry.frame, "id") === "t3-0"
      ? [field(field(entry.frame, "result"), "sessionId")]
      : [],
  )[0];
  assert.isString(sessionId);

  assert.deepEqual(
    outbound("droid.load_session").map((entry) => [
      entry.label,
      field(field(entry.frame, "params"), "sessionId"),
    ]),
    [["droid.load_session@h2", sessionId]],
    "the respawned process must resume the session the first process created",
  );

  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [
    PROVIDER_THREAD_RESUME_FIRST_PROMPT,
    PROVIDER_THREAD_RESUME_SECOND_PROMPT,
  ]);
  assert.lengthOf(projection.providerThreads, 1, "resume must keep one provider thread");
  assert.equal(projection.providerThreads[0]?.nativeThreadRef?.nativeId, sessionId);
  const answers = projection.turnItems.flatMap((item) =>
    item.type === "assistant_message" ? [item.text] : [],
  );
  assert.lengthOf(answers, 2);
  assert.include(answers[1], FIRST_FINAL, "the resumed session must remember the first answer");
  assert.include(answers[1], SECOND_FINAL);
}
