import { describe, expect, it } from "vite-plus/test";
import { groupNestedToolItems } from "./nestedTools.ts";

type Entry = {
  id: string;
  parentItemId: string | null;
  type: string;
  threadId: string;
  runId: string | null;
  providerTurnId: string | null;
  status: string;
  children?: Entry[];
};

function tool(id: string, parentItemId: string | null = null): Entry {
  return {
    id,
    parentItemId,
    type: "dynamic_tool",
    threadId: "thread",
    runId: "run",
    providerTurnId: null,
    status: "completed",
  };
}

function group(entries: Entry[]) {
  return groupNestedToolItems(
    entries,
    (entry) => entry,
    (entry, children, running) => ({
      ...entry,
      children,
      status: running ? "running" : entry.status,
    }),
  );
}

describe("groupNestedToolItems", () => {
  it("keeps concurrent groups, siblings and interleaved top-level items in source order", () => {
    const a = tool("a");
    const b = tool("b");
    const message = { ...tool("message"), type: "assistant_message" };
    const child = tool("a1", "a");
    const grandchild = { ...tool("a11", "a1"), status: "running" };
    expect(group([a, child, message, b, tool("b1", "b"), tool("a2", "a"), grandchild])).toEqual([
      {
        ...a,
        status: "running",
        children: [{ ...child, status: "running", children: [grandchild] }, tool("a2", "a")],
      },
      message,
      { ...b, children: [tool("b1", "b")] },
    ]);
    for (const status of ["pending", "waiting"]) {
      expect(group([a, { ...child, status }])[0]?.status).toBe("running");
    }
  });

  it("keeps missing, non-tool, foreign-turn and foreign-thread parents flat", () => {
    const message = { ...tool("message"), type: "assistant_message" };
    const entries = [
      tool("outer"),
      message,
      tool("missing", "absent"),
      tool("non-tool", "message"),
      { ...tool("foreign-run", "outer"), runId: "other" },
      { ...tool("foreign-turn", "outer"), providerTurnId: "other" },
      { ...tool("foreign-thread", "outer"), threadId: "other" },
      { ...tool("approval", "outer"), type: "approval_request" },
    ];
    expect(group(entries)).toEqual(entries);
  });

  it("breaks every cycle edge without losing items or unrelated valid children", () => {
    const entries = [tool("a", "b"), tool("b", "a"), tool("self", "self"), tool("child", "a")];
    expect(group(entries)).toEqual([
      { ...entries[0], children: [entries[3]] },
      entries[1],
      entries[2],
    ]);
  });

  it("accepts children before their parent and groups imported provider turns without a run", () => {
    const parent = { ...tool("outer"), runId: null, providerTurnId: "turn" };
    const child = { ...tool("child", "outer"), runId: null, providerTurnId: "turn" };
    expect(group([child, parent])).toEqual([{ ...parent, children: [child] }]);
    expect(
      group([
        { ...child, providerTurnId: null },
        { ...parent, providerTurnId: null },
      ]),
    ).toEqual([
      { ...child, providerTurnId: null },
      { ...parent, providerTurnId: null },
    ]);
  });
});
