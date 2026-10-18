import type { OrchestratorFixtureInput } from "../shared.ts";

export const DROID_BACKGROUND_WAKE_PROMPT = [
  'Live-test a background subagent wake. Call the Task tool exactly once with subagent_type "worker", run_in_background true, description "bg sleep", and prompt "Run the shell command sleep 8 and then reply exactly BG_DONE".',
  "After the Task call returns, reply with exactly STARTED and stop. Do not poll, do not call TaskOutput, do not wait.",
  "When the background completion is reported later, reply with exactly WAKE_DONE and stop.",
].join("\n");

export function droidBackgroundWakeInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: DROID_BACKGROUND_WAKE_PROMPT },
      {
        type: "await_run_status",
        targetRunIndex: 2,
        status: "running",
        waitForTurnItemType: "assistant_message",
      },
      { type: "await_run_status", targetRunIndex: 2, status: "completed" },
    ],
  };
}
