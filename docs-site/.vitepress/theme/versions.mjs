export function newestVersions(versions) {
  const numbers = (tag) =>
    String(tag).replace(/^v/, "").split(/[.-]/).slice(0, 3).map(Number);
  return [...versions].sort((a, b) => {
    const x = numbers(a.tag),
      y = numbers(b.tag);
    for (let i = 0; i < 3; i++) {
      const difference = (y[i] || 0) - (x[i] || 0);
      if (difference) return difference;
    }
    const aPre = a.tag.includes("-"),
      bPre = b.tag.includes("-");
    if (aPre !== bPre) return aPre ? 1 : -1;
    return String(b.tag).localeCompare(String(a.tag), "en", { numeric: true });
  });
}
