import type { AcpToolCallState } from "../../provider/acp/AcpRuntimeModel.ts";
import type { AcpAdapterV2SubagentUpdate } from "./AcpAdapterV2.ts";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

/**
 * Factory Droid reports subagent launches over ACP as an ordinary `Task` tool
 * call (`rawInput.subagent_type`, `description`, `prompt`) with no child
 * session id, so the subagent is keyed by the tool call and has no transcript
 * of its own.
 */
export function extractDroidSubagentUpdate(
  tool: AcpToolCallState,
): AcpAdapterV2SubagentUpdate | undefined {
  if (tool.title !== "Task") return undefined;
  const input = tool.data.rawInput;
  if (!isRecord(input) || !("subagent_type" in input)) return undefined;
  const status =
    tool.status === "completed" ? "completed" : tool.status === "failed" ? "failed" : "running";
  return {
    nativeTaskId: tool.toolCallId,
    childSessionId: null,
    prompt: text(input.prompt) ?? text(input.description) ?? "",
    title: text(input.description) ?? text(input.subagent_type),
    model: null,
    status,
    result: status === "running" ? null : (tool.detail ?? null),
  };
}
