export interface LayoutLink {
  source: string | { id: string };
  target: string | { id: string };
}

export function packageForSymbol(node: { sourceFile?: string; label?: string; packageName?: string }): string {
  if (node.packageName?.trim()) return node.packageName.trim();
  const path = node.sourceFile?.replace(/\\/g, "/");
  if (path) {
    const separator = path.lastIndexOf("/");
    return separator >= 0 ? path.slice(0, separator) || "/" : ".";
  }
  return node.label?.match(/^([a-z][a-z0-9_]*)\./i)?.[1] ?? "unknown";
}

export function rootCameraFrame(
  nodes: Array<{ id: string; x?: number; y?: number; z?: number }>,
  rootId: string | null,
  width: number,
  height: number,
  fieldOfView: number,
): { target: { x: number; y: number; z: number }; position: { x: number; y: number; z: number } } {
  const root = nodes.find(node => node.id === rootId) ?? nodes[0];
  const target = { x: root?.x ?? 0, y: root?.y ?? 0, z: root?.z ?? 0 };
  const verticalTangent = Math.tan(fieldOfView * Math.PI / 360);
  const horizontalTangent = verticalTangent * Math.max(0.1, width / Math.max(height, 1));
  let distance = 160;
  for (const node of nodes) {
    if (!Number.isFinite(node.x) || !Number.isFinite(node.y) || !Number.isFinite(node.z)) continue;
    const extent = Math.max(
      Math.abs(node.x! - target.x) / horizontalTangent,
      Math.abs(node.y! - target.y) / verticalTangent,
    );
    distance = Math.max(distance, extent * 1.25 + Math.abs(node.z! - target.z) + 40);
  }
  return { target, position: { x: target.x, y: target.y, z: target.z + distance } };
}

export function callTreeLayout(
  links: LayoutLink[],
  rootId: string | null,
): Map<string, number> {
  const depths = new Map<string, number>();
  if (rootId == null) return depths;
  const children = new Map<string, string[]>();
  for (const link of links) {
    const source = typeof link.source === "string" ? link.source : link.source.id;
    const target = typeof link.target === "string" ? link.target : link.target.id;
    const list = children.get(source) ?? [];
    list.push(target);
    children.set(source, list);
    const reverse = children.get(target) ?? [];
    reverse.push(source);
    children.set(target, reverse);
  }
  depths.set(rootId, 0);
  const queue = [rootId];
  for (let head = 0; head < queue.length; head += 1) {
    const current = queue[head];
    for (const child of children.get(current) ?? []) {
      if (!depths.has(child)) {
        depths.set(child, depths.get(current)! + 1);
        queue.push(child);
      }
    }
  }
  return depths;
}

export function layoutCallBranches(
  nodeIds: string[],
  links: LayoutLink[],
  rootId: string | null,
  packages?: ReadonlyMap<string, string>,
  communities?: ReadonlyMap<string, string>,
): Map<string, { x: number; y: number; z: number }> {
  const eligible = new Set(nodeIds);
  const adjacency = new Map<string, Set<string>>();
  for (const link of links) {
    const source = typeof link.source === "string" ? link.source : link.source.id;
    const target = typeof link.target === "string" ? link.target : link.target.id;
    if (!eligible.has(source) || !eligible.has(target) || source === target) continue;
    if (!adjacency.has(source)) adjacency.set(source, new Set());
    if (!adjacency.has(target)) adjacency.set(target, new Set());
    adjacency.get(source)!.add(target);
    adjacency.get(target)!.add(source);
  }
  const orderedIds = [...eligible].sort();
  if (rootId != null && eligible.has(rootId)) {
    orderedIds.splice(orderedIds.indexOf(rootId), 1);
    orderedIds.unshift(rootId);
  }
  const positions = new Map<string, { x: number; y: number; z: number }>();
  let component = 0;
  let mainExtent = 0;
  const cloudRadius = Math.max(80, Math.cbrt(nodeIds.length) * 16);
  const connected = packages ? callTreeLayout(links, rootId) : new Map<string, number>();
  // Order: the call tree (placed first, from the root), then communities, then packages inside each
  // community. A group is a (community, package) pair; without communities it is just the package.
  const groupOf = (id: string) => {
    const packageName = packages?.get(id) ?? "unknown";
    // No communities: every package is its own top-level group, as before.
    const community = communities ? communities.get(id) ?? "" : packageName;
    return { community, packageName, key: JSON.stringify([community, packageName]) };
  };
  const packageMembers = new Map<string, number>(); // by group key
  for (const id of orderedIds) {
    if (connected.has(id)) continue;
    const { key } = groupOf(id);
    packageMembers.set(key, (packageMembers.get(key) ?? 0) + 1);
  }
  const groupKeys = [...packageMembers.keys()].sort();
  const communityNames = [...new Set(groupKeys.map(key => JSON.parse(key)[0] as string))].sort();
  const packagesByCommunity = new Map<string, string[]>();
  for (const key of groupKeys) {
    const [community] = JSON.parse(key) as [string, string];
    packagesByCommunity.set(community, [...(packagesByCommunity.get(community) ?? []), key]);
  }
  const communityMembers = new Map<string, number>();
  for (const [key, count] of packageMembers) {
    const [community] = JSON.parse(key) as [string, string];
    communityMembers.set(community, (communityMembers.get(community) ?? 0) + count);
  }
  const packageSlots = new Map<string, number>();
  const groupRadius = (count: number) => Math.max(60, Math.cbrt(count) * 16);
  const largestPackageRadius = Math.max(0, ...[...communityMembers.values()].map(count => groupRadius(count)));
  for (const seed of orderedIds) {
    if (positions.has(seed)) continue;
    const children = new Map<string, string[]>();
    const visited = new Set([seed]);
    const queue = [seed];
    for (let head = 0; head < queue.length; head += 1) {
      const current = queue[head];
      const descendants: string[] = [];
      for (const neighbor of [...(adjacency.get(current) ?? [])].sort()) {
        if (visited.has(neighbor)) continue;
        visited.add(neighbor);
        descendants.push(neighbor);
        queue.push(neighbor);
      }
      children.set(current, descendants);
    }
    const angle = component * 2.399963;
    const height = 1 - 2 * ((component * 0.61803398875) % 1);
    const radius = cloudRadius * Math.cbrt((component * 0.754877666) % 1);
    const horizontal = Math.sqrt(1 - height * height) * radius;
    let seedPosition = {
      x: component === 0 ? 0 : mainExtent + cloudRadius + 60 + Math.cos(angle) * horizontal,
      y: component === 0 ? 0 : Math.sin(angle) * horizontal,
      z: component === 0 ? 0 : height * radius,
    };
    if (component > 0 && packages) {
      const group = groupOf(seed);
      const communityIndex = communityNames.indexOf(group.community);
      const localIndex = (packageSlots.get(group.key) ?? 0) + 1;
      packageSlots.set(group.key, localIndex);
      // 1. the community's place around the call tree
      const communityHeight = 1 - 2 * (communityIndex + 0.5) / communityNames.length;
      const communityAngle = communityIndex * 2.399963;
      const communityDistance = mainExtent + largestPackageRadius + 100 + Math.sqrt(communityNames.length) * 24;
      const communityWidth = Math.sqrt(1 - communityHeight * communityHeight) * communityDistance;
      const center = {
        x: Math.cos(communityAngle) * communityWidth,
        y: Math.sin(communityAngle) * communityWidth,
        z: communityHeight * communityDistance,
      };
      // 2. the package's place inside its community
      const siblings = packagesByCommunity.get(group.community)!;
      const packageIndex = siblings.indexOf(group.key);
      const communityRadius = groupRadius(communityMembers.get(group.community)!);
      if (siblings.length > 1) {
        const packageHeight = 1 - 2 * (packageIndex + 0.5) / siblings.length;
        const packageAngle = packageIndex * 2.399963;
        const packageWidth = Math.sqrt(1 - packageHeight * packageHeight) * communityRadius;
        center.x += Math.cos(packageAngle) * packageWidth;
        center.y += Math.sin(packageAngle) * packageWidth;
        center.z += packageHeight * communityRadius;
      }
      // 3. the component inside its package
      const localRadius = groupRadius(packageMembers.get(group.key)!) * (siblings.length > 1 ? 0.5 : 1)
        * Math.cbrt((localIndex * 0.754877666) % 1);
      const localHeight = 1 - 2 * ((localIndex * 0.61803398875) % 1);
      const localWidth = Math.sqrt(1 - localHeight * localHeight) * localRadius;
      seedPosition = {
        x: center.x + Math.cos(localIndex * 2.399963) * localWidth,
        y: center.y + Math.sin(localIndex * 2.399963) * localWidth,
        z: center.z + localHeight * localRadius,
      };
    }
    positions.set(seed, seedPosition);
    const slots = [{ id: seed, direction: [1, 0, 0], depth: 0 }];
    for (let head = 0; head < slots.length; head += 1) {
      const slot = slots[head];
      const parent = positions.get(slot.id)!;
      const descendants = children.get(slot.id)!;
      descendants.forEach((child, index) => {
        const azimuth = index * 2.399963 + slot.depth * 0.73;
        let direction = slot.direction;
        if (descendants.length > 1 && slot.depth === 0) {
          const height = 1 - 2 * (index + 0.5) / descendants.length;
          const radius = Math.sqrt(1 - height * height);
          direction = [Math.cos(azimuth) * radius, Math.sin(azimuth) * radius, height];
        } else if (descendants.length > 1) {
          const [forwardX, forwardY, forwardZ] = direction;
          const tangent = Math.abs(forwardZ) < 0.9
            ? [-forwardY, forwardX, 0]
            : [0, -forwardZ, forwardY];
          const tangentLength = Math.hypot(...tangent);
          const basis = tangent.map(value => value / tangentLength);
          const other = [
            forwardY * basis[2] - forwardZ * basis[1],
            forwardZ * basis[0] - forwardX * basis[2],
            forwardX * basis[1] - forwardY * basis[0],
          ];
          const spread = 0.3 + 0.65 * Math.sqrt((index + 0.5) / descendants.length);
          const vector = direction.map((value, axis) => value + spread * (
            Math.cos(azimuth) * basis[axis] + Math.sin(azimuth) * other[axis]
          ));
          const magnitude = Math.hypot(...vector);
          direction = vector.map(value => value / magnitude);
        }
        let hash = 2166136261;
        for (const character of child) {
          hash = Math.imul(hash ^ character.charCodeAt(0), 16777619);
        }
        const variation = 0.75 + (hash >>> 0) / 4294967295 * 0.5;
        const length = (75 + Math.sqrt(descendants.length) * 12) * variation;
        positions.set(child, {
          x: parent.x + direction[0] * length,
          y: parent.y + direction[1] * length,
          z: parent.z + direction[2] * length,
        });
        slots.push({ id: child, direction, depth: slot.depth + 1 });
      });
    }
    if (component === 0) {
      for (const id of queue) {
        const point = positions.get(id)!;
        mainExtent = Math.max(mainExtent, Math.hypot(point.x, point.y, point.z));
      }
    }
    component += 1;
  }
  return positions;
}

export function layoutSymbolBundles(
  nodes: Array<{ id: string; kind: string; packageName?: string }>,
  links: LayoutLink[],
  rootId: string | null,
): Map<string, { anchorId: string | null; x: number; y: number; z: number }> {
  const adjacency = new Map(nodes.map(node => [node.id, new Set<string>()]));
  for (const link of links) {
    const source = typeof link.source === "string" ? link.source : link.source.id;
    const target = typeof link.target === "string" ? link.target : link.target.id;
    adjacency.get(source)?.add(target);
    adjacency.get(target)?.add(source);
  }
  const anchors = new Map<string, string>();
  const queue = nodes.filter(node => node.kind === "call" || node.id === rootId)
    .map(node => node.id).sort();
  if (rootId && queue.includes(rootId)) {
    queue.splice(queue.indexOf(rootId), 1);
    queue.unshift(rootId);
  }
  for (const id of queue) anchors.set(id, id);
  for (let head = 0; head < queue.length; head += 1) {
    const current = queue[head];
    for (const neighbor of [...(adjacency.get(current) ?? [])].sort()) {
      if (anchors.has(neighbor)) continue;
      anchors.set(neighbor, anchors.get(current)!);
      queue.push(neighbor);
    }
  }
  const groups = new Map<string, Array<{ id: string; kind: string }>>();
  for (const node of [...nodes].sort((first, second) => first.id.localeCompare(second.id))) {
    if (node.kind === "call" || node.id === rootId) continue;
    const key = JSON.stringify([anchors.get(node.id) ?? null, node.packageName ?? "unknown", node.kind]);
    const group = groups.get(key) ?? [];
    group.push(node);
    groups.set(key, group);
  }
  const offsets = new Map<string, { anchorId: string | null; x: number; y: number; z: number }>();
  const kindOrder = [...new Set(nodes.map(node => node.kind))].sort();
  for (const group of groups.values()) {
    const kindIndex = kindOrder.indexOf(group[0].kind);
    const angle = kindIndex * 2.399963;
    const radius = 12 + Math.cbrt(group.length) * 5;
    group.forEach((node, index) => {
      const height = 1 - 2 * (index + 0.5) / group.length;
      const width = Math.sqrt(1 - height * height);
      const azimuth = index * 2.399963;
      offsets.set(node.id, {
        anchorId: anchors.get(node.id) ?? null,
        x: Math.cos(angle) * (45 + radius) + Math.cos(azimuth) * width * radius,
        y: Math.sin(angle) * (45 + radius) + Math.sin(azimuth) * width * radius,
        z: (kindIndex % 2 === 0 ? 25 : -25) + height * radius,
      });
    });
  }
  return offsets;
}