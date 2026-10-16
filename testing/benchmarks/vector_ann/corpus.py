#!/usr/bin/env python3
"""Build the vector ANN benchmark corpus: real documentation and code, embedded once.

Chunks the tracked Markdown and source files of one or more git repositories, embeds every chunk
with a configured model, holds out query chunks, and computes their exact nearest neighbours.
Every engine in the benchmark then loads the same vectors and is scored against the same ground
truth, so recall is comparable across engines and modes.

Embeddings are cached by chunk content (sha256) in <out>/embeddings-<model>.sqlite: a rerun, or a
larger corpus over the same repositories, only embeds chunks it has not seen.

Outputs in <out>/:
  vectors.npy   float32 [N, D]   corpus vectors, L2-normalized
  queries.npy   float32 [Q, D]   held-out query vectors (not in the corpus)
  truth.npy     int32   [Q, K]   exact top-K corpus row indices per query, by cosine
  meta.json                      sources, chunking, model, counts

Usage:
  corpus.py --roots ~/src/NornicDB:~/src/other --out bench-data/vector_ann/corpus [--count-only]

Embedders:
  titan  Amazon Titan Text Embeddings V2 on Bedrock (1024-d). AWS credentials from the usual
         chain (AWS_PROFILE, AWS_REGION / --aws-region).
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
import random
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import numpy as np

DOC_SUFFIXES = {".md", ".mdx", ".rst", ".txt", ".adoc"}
CODE_SUFFIXES = {
    ".go", ".py", ".rb", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".rs", ".java", ".kt", ".swift",
    ".c", ".h", ".cc", ".cpp", ".hpp", ".cs", ".php", ".scala", ".sql", ".sh", ".tf", ".hcl",
    ".yaml", ".yml", ".toml", ".proto", ".graphql", ".css", ".scss", ".vue", ".svelte", ".erb",
}
SKIP_PARTS = {"node_modules", "vendor", "dist", "build", ".next", "coverage", "__generated__", "generated"}
SKIP_NAMES = {"package-lock.json", "yarn.lock", "pnpm-lock.yaml", "go.sum", "Gemfile.lock", "poetry.lock",
              "Cargo.lock", "uv.lock", "composer.lock"}
MAX_FILE_BYTES = 256 * 1024


def tracked_files(root: Path):
    out = subprocess.run(["git", "-C", str(root), "ls-files", "-z"], capture_output=True, check=True).stdout
    for rel in out.decode("utf-8", "replace").split("\0"):
        if not rel:
            continue
        path = Path(rel)
        suffix = path.suffix.lower()
        if suffix not in DOC_SUFFIXES and suffix not in CODE_SUFFIXES:
            continue
        if path.name in SKIP_NAMES or ".min." in path.name or SKIP_PARTS.intersection(path.parts):
            continue
        full = root / rel
        try:
            if not full.is_file() or full.stat().st_size > MAX_FILE_BYTES:
                continue
        except OSError:
            continue
        yield rel, full, ("doc" if suffix in DOC_SUFFIXES else "code")


def chunk_text(text: str, target: int, limit: int, minimum: int):
    """Line-aligned chunks of about target characters, never over limit; tiny tails are dropped."""
    chunk, size = [], 0
    for line in text.splitlines(keepends=True):
        while len(line) > limit:  # one very long line (minified, data): hard-split it
            if chunk:
                yield "".join(chunk)
                chunk, size = [], 0
            yield line[:limit]
            line = line[limit:]
        if size + len(line) > limit and chunk:
            yield "".join(chunk)
            chunk, size = [], 0
        chunk.append(line)
        size += len(line)
        if size >= target:
            yield "".join(chunk)
            chunk, size = [], 0
    if chunk and size >= minimum:
        yield "".join(chunk)


def repo_name(root: Path) -> str:
    """The repository's name from its origin remote (a worktree's folder name can differ)."""
    url = subprocess.run(["git", "-C", str(root), "config", "--get", "remote.origin.url"],
                         capture_output=True, text=True).stdout.strip()
    name = url.rstrip("/").rsplit("/", 1)[-1].removesuffix(".git") if url else ""
    return name or root.name


def collect_chunks(roots, target, limit, minimum):
    seen, chunks = set(), []
    for root in roots:
        name = repo_name(root)
        for rel, full, kind in tracked_files(root):
            try:
                text = full.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            for piece in chunk_text(text, target, limit, minimum):
                if len(piece.strip()) < minimum:
                    continue
                body = f"{name}/{rel}\n{piece}"
                digest = hashlib.sha256(body.encode()).hexdigest()
                if digest in seen:
                    continue
                seen.add(digest)
                chunks.append({"sha": digest, "repo": name, "path": rel, "kind": kind, "text": body})
    return chunks


class EmbeddingCache:
    def __init__(self, path: Path):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("CREATE TABLE IF NOT EXISTS emb (sha TEXT PRIMARY KEY, dims INTEGER, vec BLOB)")
        self.lock = threading.Lock()

    def get_many(self, shas):
        found = {}
        for i in range(0, len(shas), 900):
            part = shas[i:i + 900]
            rows = self.db.execute(f"SELECT sha, vec FROM emb WHERE sha IN ({','.join('?' * len(part))})", part)
            for sha, blob in rows:
                found[sha] = np.frombuffer(blob, dtype=np.float32)
        return found

    def put(self, sha, vec):
        with self.lock:
            self.db.execute("INSERT OR REPLACE INTO emb VALUES (?, ?, ?)", (sha, len(vec), np.asarray(vec, np.float32).tobytes()))

    def commit(self):
        with self.lock:
            self.db.commit()


class TitanEmbedder:
    model = "amazon.titan-embed-text-v2:0"
    dims = 1024

    def __init__(self, region):
        import boto3
        from botocore.config import Config
        self.client = boto3.client("bedrock-runtime", region_name=region,
                                   config=Config(retries={"max_attempts": 12, "mode": "adaptive"}, max_pool_connections=64))

    def embed(self, text):
        body = json.dumps({"inputText": text, "dimensions": self.dims, "normalize": True})
        resp = self.client.invoke_model(modelId=self.model, body=body, contentType="application/json", accept="application/json")
        return json.loads(resp["body"].read())["embedding"]


def embed_all(chunks, embedder, cache, workers):
    shas = [c["sha"] for c in chunks]
    have = cache.get_many(shas)
    todo = [c for c in chunks if c["sha"] not in have]
    print(f"embeddings: {len(have)} cached, {len(todo)} to embed with {embedder.model}", flush=True)
    done, failed, start = 0, [], time.time()

    def work(chunk):
        return chunk["sha"], embedder.embed(chunk["text"])

    with concurrent.futures.ThreadPoolExecutor(workers) as pool:
        futures = {pool.submit(work, c): c for c in todo}
        for future in concurrent.futures.as_completed(futures):
            chunk = futures[future]
            try:
                sha, vec = future.result()
                cache.put(sha, vec)
                have[sha] = np.asarray(vec, np.float32)
            except Exception as error:  # noqa: BLE001 - report every failed chunk, then stop
                failed.append((chunk["repo"] + "/" + chunk["path"], str(error)[:200]))
            done += 1
            if done % 1000 == 0:
                cache.commit()
                rate = done / max(time.time() - start, 1e-9)
                print(f"  {done}/{len(todo)} ({rate:.0f}/s, {len(failed)} failed)", flush=True)
    cache.commit()
    if failed:
        for path, error in failed[:10]:
            print(f"  failed: {path}: {error}", file=sys.stderr)
        raise SystemExit(f"{len(failed)} chunks failed to embed; rerun to retry them (embedded chunks are cached)")
    return np.stack([have[s] for s in shas])


def exact_topk(corpus, queries, k, block=256):
    truth = np.empty((len(queries), k), dtype=np.int32)
    for i in range(0, len(queries), block):
        scores = queries[i:i + block] @ corpus.T
        idx = np.argpartition(-scores, k - 1, axis=1)[:, :k]
        order = np.argsort(-np.take_along_axis(scores, idx, axis=1), axis=1, kind="stable")
        truth[i:i + block] = np.take_along_axis(idx, order, axis=1)
    return truth


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--roots", default=os.environ.get("VECTOR_CORPUS_ROOTS", "."),
                    help="colon-separated git repositories to read (env VECTOR_CORPUS_ROOTS; default .)")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--embedder", default=os.environ.get("VECTOR_EMBEDDER", "titan"), choices=["titan"])
    ap.add_argument("--aws-region", default=os.environ.get("AWS_REGION", "us-east-1"))
    ap.add_argument("--max-chunks", type=int, default=int(os.environ.get("VECTOR_CORPUS_MAX_CHUNKS", "100000")),
                    help="sample at most this many chunks (seeded); 0 = all")
    ap.add_argument("--queries", type=int, default=int(os.environ.get("VECTOR_QUERIES", "1000")))
    ap.add_argument("--truth-k", type=int, default=100)
    ap.add_argument("--chunk-chars", type=int, default=1500)
    ap.add_argument("--max-chunk-chars", type=int, default=2400)
    ap.add_argument("--min-chunk-chars", type=int, default=200)
    ap.add_argument("--workers", type=int, default=int(os.environ.get("VECTOR_EMBED_WORKERS", "24")))
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--count-only", action="store_true", help="report the chunk counts and stop (no embedding)")
    args = ap.parse_args()

    roots = [Path(r).expanduser().resolve() for r in args.roots.split(":") if r]
    chunks = collect_chunks(roots, args.chunk_chars, args.max_chunk_chars, args.min_chunk_chars)
    by = {}
    for c in chunks:
        key = (c["repo"], c["kind"])
        by[key] = by.get(key, 0) + 1
    total_chars = sum(len(c["text"]) for c in chunks)
    print(f"{len(chunks)} unique chunks, {total_chars / 1e6:.1f}M characters (~{total_chars / 4 / 1e6:.1f}M tokens)")
    for (repo, kind), n in sorted(by.items()):
        print(f"  {repo:24} {kind:4} {n:7}")
    if args.count_only:
        return

    rng = random.Random(args.seed)
    want = args.max_chunks + args.queries if args.max_chunks else len(chunks)
    if len(chunks) > want:
        chunks = rng.sample(chunks, want)
    rng.shuffle(chunks)
    query_chunks, corpus_chunks = chunks[:args.queries], chunks[args.queries:]

    args.out.mkdir(parents=True, exist_ok=True)
    cache = EmbeddingCache(args.out / f"embeddings-{args.embedder}.sqlite")
    embedder = TitanEmbedder(args.aws_region)
    vectors = embed_all(query_chunks + corpus_chunks, embedder, cache, args.workers)
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
    queries, corpus = vectors[:len(query_chunks)], vectors[len(query_chunks):]
    print(f"exact top-{args.truth_k} for {len(queries)} queries over {len(corpus)} vectors...", flush=True)
    truth = exact_topk(corpus, queries, args.truth_k)

    np.save(args.out / "vectors.npy", corpus.astype(np.float32))
    np.save(args.out / "queries.npy", queries.astype(np.float32))
    np.save(args.out / "truth.npy", truth)
    kinds = {k: sum(1 for c in corpus_chunks if c["kind"] == k) for k in ("doc", "code")}
    meta = {
        "embedder": args.embedder, "model": embedder.model, "dimensions": int(corpus.shape[1]),
        "metric": "cosine", "vectors": int(len(corpus)), "queries": int(len(queries)), "truth_k": args.truth_k,
        "roots": [repo_name(r) for r in roots], "kinds": kinds, "seed": args.seed,
        "chunking": {"target": args.chunk_chars, "max": args.max_chunk_chars, "min": args.min_chunk_chars},
        "vectors_sha256": hashlib.sha256(corpus.astype(np.float32).tobytes()).hexdigest()[:16],
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (args.out / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
