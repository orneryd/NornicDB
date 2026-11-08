// Link explicit code references, not ordinary prose that happens to name a
// short function such as "unsupported", "read", or "search".
export function codeReferences(markdown) {
  const found = new Set(markdown.match(/pkg\/[\w/.-]+\.go/g) || []);
  const identifier = /[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?/g;
  for (const token of markdown.match(identifier) || [])
    if (/[a-z][A-Z]|[A-Za-z_]\w*\.[A-Za-z_]\w*/.test(token)) found.add(token);
  for (const match of markdown.matchAll(
    /([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)\s*\(/g,
  ))
    found.add(match[1]);
  for (const match of markdown.matchAll(/(?<!`)`([^`\n]+)`(?!`)/g))
    for (const token of match[1].match(identifier) || [])
      if (/[A-Z_.]/.test(token)) found.add(token);
  return found;
}

export function referencesSymbol(references, node, nameCounts) {
  if (references.has(node.file)) return true;
  // Unqualified private helpers and ambiguous names also occur in query languages
  // and examples (for example Cypher max()). They are not source evidence.
  const qualified = (node.package?.split("/").at(-1) || "") + "." + node.name;
  return (
    (nameCounts.get(qualified) === 1 && references.has(qualified)) ||
    (node.name.includes(".") &&
      nameCounts.get(node.name) === 1 &&
      references.has(node.name))
  );
}
