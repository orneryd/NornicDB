export function createContentPolicy(policy) {
  const removed = new Set(policy.removed),
    directories = policy.removedDirectories || [];
  function sourcePath(file) {
    if (file.split("/").some((part) => part.startsWith(".flagged")))
      return null;
    if (removed.has(file)) return null;
    let published = policy.relocations[file] || file;
    if (!policy.relocations[file]) {
      for (const [from, to] of Object.entries(policy.folderRelocations || {}))
        if (file === from || file.startsWith(from + "/")) {
          published = to + file.slice(from.length);
          break;
        }
    }
    if (
      removed.has(published) ||
      directories.some(
        (dir) => published === dir || published.startsWith(dir + "/"),
      )
    )
      return null;
    return published;
  }
  return {
    sourcePath,
    linkPath: (file) => sourcePath(policy.consolidatedInto?.[file] || file),
  };
}
