// Precompute code postings at build time. Browsers load numeric postings rather
// than tokenize every function body or keep complete source bodies in memory.
export function terms(text) {
  const source = String(text),
    split = source
      .replace(/([a-z\d])([A-Z])/g, "$1 $2")
      .replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2")
      .replaceAll("_", " ");
  return [
    ...new Set([
      ...(source.toLowerCase().match(/[\p{L}\p{N}_]+/gu) || []),
      ...(split.toLowerCase().match(/[\p{L}\p{N}]+/gu) || []),
    ]),
  ];
}
export function createCodeIndex(documents) {
  const postings = Object.create(null);
  documents.forEach((doc, index) => {
    for (const term of terms(doc.title + " " + doc.file + " " + doc.text))
      (postings[term] ||= []).push(index);
  });
  return {
    format: "code-postings-v1",
    documents: documents.map((doc) => ({
      ...doc,
      text: doc.text.slice(0, 350),
    })),
    postings,
  };
}
export function searchCode(index, query, limit = 12) {
  const scores = new Map(),
    words = terms(query),
    dictionary = Object.keys(index.postings);
  for (const word of words) {
    const matches = index.postings[word]
      ? [word]
      : dictionary.filter((term) => term.startsWith(word)).slice(0, 64);
    for (const term of matches) {
      const ids = index.postings[term],
        weight = Math.log(1 + index.documents.length / (1 + ids.length));
      for (const id of ids) scores.set(id, (scores.get(id) || 0) + weight);
    }
  }
  const text = query.toLowerCase();
  return [...scores]
    .map(([id, score]) => ({
      id,
      score:
        score +
        (index.documents[id].title.toLowerCase().includes(text) ? 20 : 0),
    }))
    .sort((a, b) => b.score - a.score || a.id - b.id)
    .slice(0, limit)
    .map(({ id }) => index.documents[id]);
}
