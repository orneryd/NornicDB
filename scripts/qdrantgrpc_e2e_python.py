#!/usr/bin/env python3
"""
E2E driver-compatibility test using the official qdrant-client (gRPC).

This validates that real Qdrant SDK calls work end-to-end against NornicDB's
Qdrant-compatible gRPC endpoint (no REST required).

Usage (usually via the wrapper script):
  python3 scripts/qdrantgrpc_e2e_python.py --host 127.0.0.1 --grpc-port 6334
"""

from __future__ import annotations

import argparse
import time
from typing import Any, Callable, Dict


def stage(name: str, fn: Callable[[], Any]) -> Any:
    start = time.perf_counter()
    print(f"[PY-E2E] -> {name}")
    try:
        out = fn()
        dur = time.perf_counter() - start
        print(f"[PY-E2E] <- {name} OK ({dur:.6f}s)")
        return out
    except Exception as e:
        dur = time.perf_counter() - start
        print(f"[PY-E2E] <- {name} FAILED ({dur:.6f}s): {e}")
        raise


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--grpc-port", type=int, default=6334)
    ap.add_argument("--collection", default="py_e2e_col")
    args = ap.parse_args()

    # Import qdrant-client lazily so the wrapper can install deps first.
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as m

    client = stage(
        "client.connect(prefer_grpc)",
        lambda: QdrantClient(host=args.host, grpc_port=args.grpc_port, prefer_grpc=True),
    )

    # Some SDK versions use 'collections.get_collections()' naming, others 'get_collections()'.
    # We'll stick to the stable public APIs.

    dims = 4
    col = args.collection

    # Clean up any prior run
    def cleanup_collection() -> None:
        try:
            client.delete_collection(collection_name=col)
        except Exception:
            pass

    cleanup_collection()

    stage(
        "collections.create_collection",
        lambda: client.create_collection(
            collection_name=col,
            vectors_config=m.VectorParams(size=dims, distance=m.Distance.COSINE),
        ),
    )

    stage("collections.get_collection", lambda: client.get_collection(collection_name=col))
    stage("collections.get_collections", lambda: client.get_collections())

    # Upsert points (named vectors if supported by this SDK version).
    points = [
        m.PointStruct(
            id="p1",
            vector=[1.0, 0.0, 0.0, 0.0],
            payload={"tag": "first", "score": 10},
        ),
        m.PointStruct(
            id="p2",
            vector=[0.0, 1.0, 0.0, 0.0],
            payload={"tag": "second", "score": 20},
        ),
    ]

    stage("points.upsert", lambda: client.upsert(collection_name=col, points=points))

    stage(
        "points.retrieve",
        lambda: client.retrieve(collection_name=col, ids=["p1"], with_payload=True, with_vectors=True),
    )

    stage(
        "points.count(all)",
        lambda: client.count(collection_name=col, exact=True),
    )

    stage(
        "points.count(filter)",
        lambda: client.count(
            collection_name=col,
            exact=True,
            count_filter=m.Filter(
                must=[
                    m.FieldCondition(
                        key="tag",
                        match=m.MatchValue(value="first"),
                    )
                ]
            ),
        ),
    )

    stage(
        "points.search",
        lambda: client.search(
            collection_name=col,
            query_vector=[1.0, 0.0, 0.0, 0.0],
            limit=3,
            with_payload=True,
        ),
    )

    stage(
        "points.scroll",
        lambda: client.scroll(collection_name=col, limit=1, with_payload=True, with_vectors=False),
    )

    stage(
        "points.delete(filter)",
        lambda: client.delete(
            collection_name=col,
            points_selector=m.FilterSelector(
                filter=m.Filter(
                    must=[
                        m.FieldCondition(
                            key="tag",
                            match=m.MatchValue(value="second"),
                        )
                    ]
                )
            ),
        ),
    )

    stage(
        "points.count(after_delete)",
        lambda: client.count(collection_name=col, exact=True),
    )

    stage("collections.delete_collection", lambda: client.delete_collection(collection_name=col))
    print("[PY-E2E] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

