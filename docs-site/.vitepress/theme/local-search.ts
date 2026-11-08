import { withBase } from "vitepress";
let worker: Worker | undefined,
  sequence = 0;
const pending = new Map<
  number,
  { resolve: (value: any) => void; reject: (error: Error) => void }
>();
export function localSearch(query: string, type: string): Promise<any[]> {
  if (!worker) {
    worker = new Worker(new URL("./search.worker.ts", import.meta.url), {
      type: "module",
    });
    worker.onmessage = (event) => {
      const request = pending.get(event.data.id);
      if (!request) return;
      pending.delete(event.data.id);
      event.data.error
        ? request.reject(new Error(event.data.error))
        : request.resolve(event.data.results);
    };
    worker.onerror = () => {
      for (const request of pending.values())
        request.reject(new Error("Browser index unavailable"));
      pending.clear();
      worker?.terminate();
      worker = undefined;
    };
  }
  const id = ++sequence,
    url = new URL(
      withBase(
        type === "code" ? "/code-search-index.json" : "/search-index.json",
      ),
      location.origin,
    ).href;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    worker!.postMessage({ id, query, type, url });
  });
}
