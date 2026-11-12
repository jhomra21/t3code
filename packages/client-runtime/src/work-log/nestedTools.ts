type ToolItem = {
  readonly id: string;
  readonly parentItemId: string | null;
  readonly threadId: string;
  readonly runId: string | null;
  readonly providerTurnId: string | null;
  readonly type: string;
  readonly status: string;
};

export function groupNestedToolItems<T>(
  entries: ReadonlyArray<T>,
  itemForEntry: (entry: T) => ToolItem | undefined,
  withChildren: (entry: T, children: T[], running: boolean) => T,
): T[] {
  const items = entries.map(itemForEntry);
  if (!items.some((item) => item?.parentItemId != null)) return [...entries];
  const key = (item: ToolItem, id: string) =>
    JSON.stringify([item.threadId, item.runId, item.providerTurnId, id]);
  const byId = new Map<string, number | null>();
  for (const [index, item] of items.entries()) {
    if (!item) continue;
    const id = key(item, item.id);
    byId.set(id, byId.has(id) ? null : index);
  }
  const parents = items.map((item) => {
    if (!item || !isTool(item.type) || item.parentItemId === null) return undefined;
    const index = byId.get(key(item, item.parentItemId));
    const parent = index == null ? undefined : items[index];
    return parent && isTool(parent.type) && index != null ? index : undefined;
  });
  const visited = new Set<number>();
  for (let index = 0; index < entries.length; index += 1) {
    if (visited.has(index)) continue;
    const path = new Map<number, number>();
    let cursor: number | undefined = index;
    while (cursor !== undefined && !visited.has(cursor) && !path.has(cursor)) {
      path.set(cursor, path.size);
      cursor = parents[cursor];
    }
    const cycleStart = cursor === undefined ? undefined : path.get(cursor);
    for (const [member, position] of path) {
      if (cycleStart !== undefined && position >= cycleStart) parents[member] = undefined;
      visited.add(member);
    }
  }
  const children = entries.map(() => new Array<number>());
  for (const [index, parent] of parents.entries()) {
    if (parent !== undefined) children[parent]!.push(index);
  }
  const remaining = children.map((indices) => indices.length);
  const ready = remaining.flatMap((count, index) => (count === 0 ? [index] : []));
  const grouped = [...entries];
  const running = items.map(
    (item) =>
      item?.status === "running" || item?.status === "pending" || item?.status === "waiting",
  );
  // Bottom-up construction avoids recursion and repeated descendant scans on long tool chains.
  for (let cursor = 0; cursor < ready.length; cursor += 1) {
    const index = ready[cursor]!;
    const indices = children[index]!;
    if (indices.length > 0) {
      grouped[index] = withChildren(
        entries[index]!,
        indices.map((child) => grouped[child]!),
        running[index]!,
      );
    }
    const parent = parents[index];
    if (parent === undefined) continue;
    running[parent] ||= running[index]!;
    remaining[parent] = remaining[parent]! - 1;
    if (remaining[parent] === 0) ready.push(parent);
  }
  return grouped.filter((_, index) => parents[index] === undefined);
}

function isTool(type: string): boolean {
  return (
    type === "dynamic_tool" ||
    type === "command_execution" ||
    type === "file_change" ||
    type === "file_search" ||
    type === "web_search"
  );
}
