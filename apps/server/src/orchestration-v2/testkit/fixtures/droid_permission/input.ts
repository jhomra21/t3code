import type { OrchestratorFixtureInput } from "../shared.ts";

export const DROID_PERMISSION_APPROVED_FILE = "droid-approved.txt";
export const DROID_PERMISSION_DECLINED_FILE = "droid-declined.txt";

/**
 * A Supervised Droid thread asks before each shell command. The first command
 * is approved and runs; the second is cancelled, which is the only way Droid
 * lets a request be refused.
 */
export function droidPermissionInput(): OrchestratorFixtureInput {
  return {
    runtimeMode: "approval-required",
    steps: [
      {
        type: "message",
        text: `Run the shell command \`touch ${DROID_PERMISSION_APPROVED_FILE}\` with your shell tool, then reply DONE.`,
      },
      { type: "approve_next_runtime_request" },
      {
        type: "message",
        text: `Run the shell command \`touch ${DROID_PERMISSION_DECLINED_FILE}\` with your shell tool once. If it is cancelled, do not retry; reply CANCELLED.`,
      },
      { type: "approve_next_runtime_request", decision: "cancel" },
    ],
  };
}
