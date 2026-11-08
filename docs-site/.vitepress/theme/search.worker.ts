import MiniSearch from "minisearch";
import { searchCode } from "./code-index.mjs";
const indexes = new Map<string, Promise<any>>();
self.onmessage = async (event) => {
  const { id, query, type, url } = event.data;
  try {
    if (!indexes.has(type))
      indexes.set(
        type,
        fetch(url)
          .then((response) => {
            if (!response.ok) throw Error("Index unavailable");
            return response.json();
          })
          .then((documents) => {
            if (type === "code") return documents;
            const index = new MiniSearch({
              fields: ["title", "text", "file"],
              storeFields: ["title", "url", "text", "category", "file"],
              searchOptions: { boost: { title: 4, file: 2 } },
            });
            index.addAll(documents);
            return index;
          })
          .catch((error) => {
            indexes.delete(type);
            throw error;
          }),
      );
    const index = await indexes.get(type)!;
    const results =
      type === "code"
        ? searchCode(index, query)
        : index
            .search(query, { prefix: true, fuzzy: 0.15 })
            .slice(0, 12)
            .map((row) => ({
              ...row,
              text: String(row.text || "").slice(0, 350),
            }));
    self.postMessage({ id, results });
  } catch {
    self.postMessage({
      id,
      error: "The browser search index could not be loaded.",
    });
  }
};
