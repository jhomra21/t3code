import type { OrchestratorFixtureInput } from "../shared.ts";

const DROID_CHILD_APPROVAL_FILE = "droid-child-approved.txt";

export function droidChildApprovalInput(): OrchestratorFixtureInput {
  return {
    runtimeMode: "approval-required",
    steps: [
      {
        type: "message",
        text: [
          'Call the Task tool exactly once with subagent_type "worker" in foreground mode (do not set run_in_background).',
          `The Task prompt must be: Run the shell command \`touch ${DROID_CHILD_APPROVAL_FILE}\` with your shell tool and reply exactly CHILD_OK.`,
          "Wait for Task to return, then reply exactly PARENT_OK.",
          "Do not run the shell command yourself.",
        ].join("\n"),
      },
      { type: "approve_next_runtime_request" },
    ],
  };
}
