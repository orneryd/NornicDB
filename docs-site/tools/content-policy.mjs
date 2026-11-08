export function createContentPolicy(policy) {
  const removed = new Set(policy.removed),
    directories = policy.removedDirectories || [];
  function sourcePath(file) {
    if (file.split("/").some((part) => part.startsWith(".flagged")))
      return null;
    if (removed.has(file)) return null;
    if (policy.relocations[file]) return policy.relocations[file];
    if (directories.some((dir) => file === dir || file.startsWith(dir + "/")))
      return null;
    for (const [from, to] of Object.entries(policy.folderRelocations || {}))
      if (file === from || file.startsWith(from + "/"))
        return to + file.slice(from.length);
    return file;
  }
  return {
    sourcePath,
    linkPath: (file) => sourcePath(policy.consolidatedInto?.[file] || file),
  };
}
export const isDevelopmentArchive = (relative) =>
  relative.startsWith("development/archive/");
