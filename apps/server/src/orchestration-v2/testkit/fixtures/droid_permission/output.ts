import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertSemanticProjectionIntegrity,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  projectionFor,
} from "../shared.ts";
import { DROID_PERMISSION_APPROVED_FILE, DROID_PERMISSION_DECLINED_FILE } from "./input.ts";

/**
 * Supervised: both commands ask first. The approved one runs. Cancelling the
 * second answers Droid with its `cancel` option, so the command never runs and
 * the turn still settles.
 */
export function assertDroidPermissionOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assert.lengthOf(projection.runs, 2);

  const requests = projection.runtimeRequests.toSorted((left, right) =>
    left.createdAt < right.createdAt ? -1 : 1,
  );
  assert.deepEqual(
    requests.map((request) => [request.kind, request.status, request.decision]),
    [
      ["command", "resolved", "accept"],
      ["command", "resolved", "cancel"],
    ],
  );
  const approval = projection.turnItems.find((item) => item.type === "approval_request");
  const options = approval?.type === "approval_request" ? (approval.options ?? []) : [];
  assert.includeDeepMembers(
    options.map((option) => option.decision),
    ["accept", "cancel"],
  );

  const shells = projection.turnItems.filter((item) => item.type === "command_execution");
  assert.isTrue(
    shells.some(
      (item) =>
        item.type === "command_execution" &&
        item.input.includes(DROID_PERMISSION_APPROVED_FILE) &&
        item.status === "completed",
    ),
    "the approved command must run",
  );
  assert.isFalse(
    shells.some(
      (item) =>
        item.type === "command_execution" &&
        item.input.includes(DROID_PERMISSION_DECLINED_FILE) &&
        item.status === "completed",
    ),
    "the cancelled command must not complete",
  );
}
