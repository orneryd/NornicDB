// Decoded identities occupy data keys; ordinary object prototypes and grid
// control names are separate namespaces.
export function identityRecord<T = unknown>(
  entries: Iterable<readonly [string, T]> = [],
): Record<string, T> {
  const record: Record<string, T> = Object.create(null);
  for (const [key, value] of entries) record[key] = value;
  return record;
}

export function controlColumnName(preferred: string, dataNames: readonly string[]): string {
  const occupied = new Set(dataNames);
  let name = preferred;
  while (occupied.has(name)) name += "_";
  return name;
}
