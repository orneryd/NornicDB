#!/usr/bin/env python3
"""
Seed NornicDB with 10+ nodes (paragraph content each), wait for embeddings, then run search.
Usage: python scripts/seed_and_search.py [--base http://localhost:7474] [--db nornic] [--wait 8]
        [--user admin] [--password password]
No auth required when server is run with --no-auth.
"""
import argparse
import base64
import json
import os
import sys
import time
import urllib.request
import urllib.error

# 12 nodes with unique paragraph-length content for search testing
NODES = [
    {
        "title": "Docker and Kubernetes for Microservices",
        "content": "Running microservices in production often involves containerization with Docker and orchestration with Kubernetes. Docker images package your application and its dependencies, while Kubernetes handles deployment, scaling, and self-healing across a cluster. Best practices include using multi-stage builds to keep images small, setting resource limits, and defining health checks so the orchestrator can restart unhealthy pods.",
    },
    {
        "title": "Graph Databases and Cypher Queries",
        "content": "Graph databases store data as nodes and relationships, which makes them ideal for social networks, recommendation engines, and knowledge graphs. Cypher is a declarative query language for graph databases that reads like ASCII art: you describe the pattern you want to match, and the engine finds it. Common patterns include finding shortest paths, traversing relationships, and aggregating over subgraphs.",
    },
    {
        "title": "Vector Embeddings and Semantic Search",
        "content": "Vector embeddings turn text into dense numerical vectors so that similar meanings end up close in vector space. Models like BGE, OpenAI embeddings, or sentence-transformers produce these vectors. Semantic search then finds relevant documents by comparing the query embedding to stored document embeddings using cosine similarity or approximate nearest neighbor indexes like HNSW, rather than relying only on keyword match.",
    },
    {
        "title": "Hybrid Search: BM25 and Vector Fusion",
        "content": "Hybrid search combines keyword search (e.g. BM25) with vector similarity to get the benefits of both: exact term matches and semantic understanding. Results from each method are merged using Reciprocal Rank Fusion (RRF), which avoids normalizing different score scales. Tuning weights for short vs long queries can improve relevance, and an optional reranker can refine the top candidates.",
    },
    {
        "title": "LLM Reranking and Cross-Encoders",
        "content": "Reranking improves retrieval by passing the query and each candidate through a more expensive model that scores relevance directly. Cross-encoders process query and document together, unlike bi-encoders that encode them separately. Models like BGE-Reranker or Cohere Rerank can significantly improve precision at the top of the list, at the cost of extra latency and compute.",
    },
    {
        "title": "Memory and Decay in AI Agents",
        "content": "AI agents that persist memory need a way to prioritize and forget. Episodic memory captures specific events, semantic memory holds general knowledge, and procedural memory stores skills. Decay scores can be used to down-rank old or rarely accessed memories so that recent and frequently used information surfaces first during retrieval, similar to human forgetting curves.",
    },
    {
        "title": "Neo4j Bolt Protocol and Drivers",
        "content": "The Bolt protocol is a binary protocol used by Neo4j for client-server communication. It supports pipelining, connection pooling, and transaction bookmarks for causal consistency. Official drivers for Java, Python, JavaScript, and Go implement Bolt, so applications can run Cypher and get typed results. NornicDB implements Bolt for drop-in compatibility with existing Neo4j applications.",
    },
    {
        "title": "Full-Text and Vector Indexes",
        "content": "Full-text indexes tokenize and index text for fast keyword search with ranking (e.g. TF-IDF or BM25). Vector indexes store embeddings and support approximate nearest neighbor search. Building both on the same nodes allows hybrid search: the same corpus is queryable by keywords and by semantic similarity, and the two result sets can be fused for better recall and precision.",
    },
    {
        "title": "Local LLMs with GGUF and llama.cpp",
        "content": "GGUF is a format for distributing quantized language models that run efficiently on CPU and GPU. llama.cpp loads GGUF files and provides C APIs for inference, which Go or Python can call via CGO or bindings. Running models locally avoids API costs and keeps data on-premises; quantization (e.g. Q4_K_M) trades a small quality drop for much lower memory and faster inference.",
    },
    {
        "title": "Configuration and Feature Flags",
        "content": "Server configuration often comes from environment variables and config files, with precedence like CLI over env over file over defaults. Feature flags allow turning capabilities like embeddings, reranking, or plugins on or off without code changes. Documenting which env vars map to which options helps operators and ensures rerank or embedding settings are actually passed into the process.",
    },
    {
        "title": "WAL and Async Writes for Durability",
        "content": "Write-ahead logging (WAL) ensures that changes are recorded to a log before they are applied to the main store, so recovery after a crash replays the log. Async writes can improve throughput by batching and flushing in the background, with a trade-off between latency and durability. Configurable flush intervals and snapshot compaction keep the WAL from growing unbounded.",
    },
    {
        "title": "APOC and Plugin Extensions",
        "content": "APOC (Awesome Procedures On Cypher) is a library of procedures and functions that extend Cypher with utilities for collections, dates, JSON, and graph algorithms. Plugin systems allow loading shared libraries or scripts that register new procedures. NornicDB supports loading APOC-style plugins from a directory so users can add custom Cypher functions without recompiling the database.",
    },
]


def cypher_escape(s: str) -> str:
    """Escape a string for use inside Cypher single-quoted literals: double single quotes and escape backslashes."""
    if not isinstance(s, str):
        s = str(s)
    return s.replace("\\", "\\\\").replace("'", "''")


def request(
    base_url: str,
    path: str,
    body: dict,
    method: str = "POST",
    user: str | None = None,
    password: str | None = None,
) -> dict:
    url = f"{base_url.rstrip('/')}{path}"
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if user is not None and password is not None:
        creds = base64.b64encode(f"{user}:{password}".encode()).decode("ascii")
        req.add_header("Authorization", f"Basic {creds}")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body_bytes = e.read()
        body = body_bytes.decode("utf-8")
        try:
            err = json.loads(body)
        except Exception:
            err = {"message": body}
        raise SystemExit(f"HTTP {e.code} {path}: {err}")


def seed(
    base_url: str, db: str, user: str | None = None, password: str | None = None
) -> None:
    print(f"Seeding database '{db}' with {len(NODES)} nodes...")
    statements = []
    for i, node in enumerate(NODES):
        title = cypher_escape(node["title"])
        content = cypher_escape(node["content"])
        # Use inline literals so values are stored correctly (no parameter substitution dependency).
        stmt = {
            "statement": (
                f"CREATE (n:Article {{title: '{title}', content: '{content}'}}) RETURN n"
            ),
        }
        statements.append(stmt)
    body = {"statements": statements}
    out = request(base_url, f"/db/{db}/tx/commit", body, user=user, password=password)
    errors = out.get("errors") or []
    if errors:
        print("Cypher errors:", json.dumps(errors, indent=2))
        raise SystemExit(1)
    results = out.get("results") or []
    print(f"  Created {len(results)} nodes.")


def search(
    base_url: str,
    query: str,
    limit: int = 10,
    database: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> None:
    body = {"query": query, "limit": limit}
    if database:
        body["database"] = database
    out = request(base_url, "/nornicdb/search", body, user=user, password=password)
    if isinstance(out, list):
        results = out
    else:
        results = out.get("results") or out.get("Results") or []
    print(f"\nSearch: \"{query}\" (limit={limit})")
    print(f"  Returned {len(results)} result(s).")
    for i, r in enumerate(results[:10], 1):
        node = r.get("node") or r
        props = node.get("properties") or {}
        title = props.get("title") or r.get("title") or ""
        content = props.get("content") or r.get("content_preview") or ""
        score = r.get("score") or r.get("rrf_score") or 0
        preview = (content or "")[:80]
        print(f"  {i}. [{score:.4f}] {title!r}")
        if preview:
            print(f"      {preview}...")


def main() -> None:
    p = argparse.ArgumentParser(description="Seed NornicDB and run search.")
    p.add_argument("--base", default="http://localhost:7474", help="Base URL of NornicDB HTTP API")
    p.add_argument("--db", default="nornic", help="Database name")
    p.add_argument("--wait", type=int, default=8, help="Seconds to wait after seed before search")
    p.add_argument("--no-seed", action="store_true", help="Skip seeding, only run search")
    p.add_argument("--query", default="vector embeddings and semantic search", help="Search query")
    p.add_argument("--limit", type=int, default=10, help="Search result limit")
    p.add_argument(
        "--user",
        default=os.environ.get("API_USER", "admin"),
        help="HTTP Basic Auth user (default: API_USER env or 'admin')",
    )
    p.add_argument(
        "--password",
        default=os.environ.get("API_PASSWORD", "password"),
        help="HTTP Basic Auth password (default: API_PASSWORD env or 'password')",
    )
    args = p.parse_args()

    user = args.user or None
    password = args.password or None

    if not args.no_seed:
        seed(args.base, args.db, user=user, password=password)
        print(f"Waiting {args.wait}s for embeddings to generate...")
        time.sleep(args.wait)
    search(
        args.base,
        args.query,
        limit=args.limit,
        database=args.db,
        user=user,
        password=password,
    )
    print("\nDone.")


if __name__ == "__main__":
    main()
