import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertAssistantTextIncludes,
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  projectionFor,
} from "../shared.ts";

export function assertDroidChildApprovalOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertAssistantTextIncludes(projection, "PARENT_OK");

  assert.lengthOf(projection.runtimeRequests, 1);
  const request = projection.runtimeRequests[0];
  assert.isDefined(request);
  assert.equal(request.kind, "command");
  assert.equal(request.status, "resolved");
  assert.equal(request.decision, "accept");

  assert.lengthOf(projection.subagents, 1);
  const subagent = projection.subagents[0];
  assert.isDefined(subagent);
  assert.equal(subagent.status, "completed");
  assert.include(subagent.result ?? "", "CHILD_OK");

  const approvalItems = projection.turnItems.filter((item) => item.type === "approval_request");
  assert.lengthOf(approvalItems, 1);
  const approval = approvalItems[0];
  assert.isDefined(approval);
  assert.equal(approval.status, "completed");
  const approvalNode = projection.nodes.find((node) => node.id === approval.nodeId);
  assert.isDefined(approvalNode);
  assert.equal(approvalNode.parentNodeId, subagent.id);
}
