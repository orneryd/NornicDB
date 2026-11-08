import { createHash } from "node:crypto";
export function indexRevision(documents) {
  const ordered = documents
    .map((doc) => ({
      id: doc.id,
      title: doc.title,
      url: doc.url,
      text: doc.text.replace(/\r/g, ""),
      category: doc.category,
      semantic: doc.semantic !== false,
    }))
    .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  return createHash("sha256").update(JSON.stringify(ordered)).digest("hex");
}
