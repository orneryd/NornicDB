// A bounded call neighborhood with recursive, self-similar package/branch anchors.
export function indexGraph(graph) {
  const nodes = new Map(graph.nodes.map((node) => [node.id, node])),
    incoming = new Map(),
    outgoing = new Map();
  for (const edge of graph.links) {
    if (!nodes.has(edge.source) || !nodes.has(edge.target)) continue;
    for (const [map, key] of [
      [outgoing, edge.source],
      [incoming, edge.target],
    ]) {
      const list = map.get(key) || [];
      list.push(edge);
      map.set(key, list);
    }
  }
  return { nodes, incoming, outgoing, links: graph.links };
}
export function neighborhood(
  index,
  root,
  { direction = "both", depth = 2, maxNodes = 60, maxLinks = 180 } = {},
) {
  if (!index.nodes.has(root)) return { nodes: [], links: [] };
  const found = new Map([
      [root, { ...index.nodes.get(root), depth: 0, branch: root }],
    ]),
    queue = [root],
    tree = [];
  for (
    let offset = 0;
    offset < queue.length && found.size < maxNodes;
    offset++
  ) {
    const id = queue[offset],
      parent = found.get(id);
    if (parent.depth >= depth) continue;
    const ins = index.incoming.get(id) || [],
      outs = index.outgoing.get(id) || [],
      edges =
        direction === "in"
          ? ins
          : direction === "out"
            ? outs
            : [
                ...new Set([
                  ...ins.slice(0, 6),
                  ...outs.slice(0, 6),
                  ...ins,
                  ...outs,
                ]),
              ];
    let added = 0;
    for (const edge of edges) {
      const next = edge.source === id ? edge.target : edge.source;
      if (found.has(next)) continue;
      found.set(next, {
        ...index.nodes.get(next),
        depth: parent.depth + 1,
        branch: id === root ? next : parent.branch,
      });
      queue.push(next);
      tree.push({ ...edge });
      added++;
      if (found.size >= maxNodes || added >= (id === root ? 12 : 4)) break;
    }
  }
  const seen = new Set(tree.map((edge) => edge.source + "\0" + edge.target)),
    links = [...tree];
  for (const edge of index.links)
    if (
      found.has(edge.source) &&
      found.has(edge.target) &&
      !seen.has(edge.source + "\0" + edge.target) &&
      links.length < maxLinks
    ) {
      links.push({ ...edge });
      seen.add(edge.source + "\0" + edge.target);
    }
  return { nodes: [...found.values()], links };
}
export function fractalAnchors(nodes, root) {
  const anchors = new Map([[root, { x: 0, y: 0, z: 0 }]]),
    clusters = [];
  function place(items, level, center, radius, path = []) {
    if (items.length === 1) {
      anchors.set(items[0].id, { ...center });
      return;
    }
    const groups = new Map();
    for (const node of items) {
      const parts = [
          ...node.file.split("/").slice(0, -1),
          "branch:" + node.branch,
          node.file.split("/").at(-1),
          node.id,
        ],
        key = parts[level] || node.id,
        list = groups.get(key) || [];
      list.push(node);
      groups.set(key, list);
    }
    const entries = [...groups].sort(([a], [b]) => a.localeCompare(b));
    if (entries.length === 1) {
      place(items, level + 1, center, radius, path);
      return;
    }
    entries.forEach(([key, children], i) => {
      const polar = 1 - (2 * (i + 0.5)) / entries.length,
        angle = i * Math.PI * (3 - Math.sqrt(5)),
        ring = Math.sqrt(1 - polar * polar),
        position = {
          x: center.x + radius * ring * Math.cos(angle),
          y: center.y + radius * polar,
          z: center.z + radius * ring * Math.sin(angle),
        };
      if (children.length > 1)
        clusters.push({
          id: [...path, key].join("/"),
          label: key,
          position,
          radius: radius * 0.48,
          count: children.length,
          members: children.map((node) => node.id),
        });
      place(children, level + 1, position, radius * 0.42, [...path, key]);
    });
  }
  const remaining = nodes.filter((node) => node.id !== root);
  if (remaining.length) place(remaining, 0, { x: 0, y: 0, z: 0 }, 160);
  return { anchors, clusters };
}
