#!/usr/bin/env python3
"""
Qdrant → NornicDB migration (Python).

Streams collections + points from a source Qdrant instance and replays them
into NornicDB through NornicDB's Qdrant-compatible gRPC endpoint. Because
the destination speaks Qdrant gRPC, the same `qdrant-client` library writes
both sides — only the connection string differs.

Usage:
  pip install qdrant-client
  python migrate.py \
      --source-url http://qdrant.local:6333 \
      --target-host nornicdb.local \
      --target-grpc-port 6334 \
      --batch-size 256

Optional flags:
  --collections coll_a,coll_b   restrict to a subset
  --skip-existing               skip collections that already exist in target
  --target-api-key TOKEN        bearer token for NornicDB if Auth.Enabled=true
  --source-api-key TOKEN        same for source

The script:
  1. Lists collections on source.
  2. For each: reads its vector params (size, distance, named vectors),
     creates the matching collection in NornicDB if missing.
  3. Scrolls points in batches and Upserts them into NornicDB.
  4. Verifies the count matches.

Re-running is idempotent for collections that already match; points are
upserted by ID so duplicate runs only refresh.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Iterable

from qdrant_client import QdrantClient
from qdrant_client.http import models as m


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Qdrant → NornicDB migration")
    p.add_argument("--source-url", required=True, help="Qdrant source URL, e.g. http://qdrant:6333")
    p.add_argument("--source-api-key", default=None)
    p.add_argument("--target-host", default="localhost")
    p.add_argument("--target-grpc-port", type=int, default=6334)
    p.add_argument("--target-api-key", default=None)
    p.add_argument("--collections", default="", help="Comma-separated subset; empty = all")
    p.add_argument("--batch-size", type=int, default=256)
    p.add_argument("--skip-existing", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def collection_exists(client: QdrantClient, name: str) -> bool:
    try:
        client.get_collection(name)
        return True
    except Exception:
        return False


def replicate_collection_config(
    src: QdrantClient, dst: QdrantClient, name: str, dry_run: bool
) -> None:
    info = src.get_collection(name)
    cfg = info.config.params

    if isinstance(cfg.vectors, dict):
        vectors_config = {
            vec_name: m.VectorParams(size=v.size, distance=v.distance)
            for vec_name, v in cfg.vectors.items()
        }
    else:
        v = cfg.vectors
        vectors_config = m.VectorParams(size=v.size, distance=v.distance)

    print(f"  → create collection '{name}' (vectors={vectors_config})")
    if dry_run:
        return
    dst.create_collection(collection_name=name, vectors_config=vectors_config)


def migrate_points(
    src: QdrantClient,
    dst: QdrantClient,
    name: str,
    batch_size: int,
    dry_run: bool,
) -> int:
    total = 0
    offset = None
    while True:
        points, offset = src.scroll(
            collection_name=name,
            limit=batch_size,
            offset=offset,
            with_payload=True,
            with_vectors=True,
        )
        if not points:
            break

        upserts: list[m.PointStruct] = []
        for p in points:
            upserts.append(
                m.PointStruct(
                    id=p.id,
                    vector=p.vector,
                    payload=p.payload or {},
                )
            )

        if not dry_run:
            dst.upsert(collection_name=name, points=upserts, wait=True)
        total += len(upserts)
        print(f"  · {name}: upserted {total} points")

        if offset is None:
            break
    return total


def main() -> int:
    args = parse_args()

    src = QdrantClient(url=args.source_url, api_key=args.source_api_key)
    dst = QdrantClient(
        host=args.target_host,
        grpc_port=args.target_grpc_port,
        prefer_grpc=True,
        api_key=args.target_api_key,
    )

    requested = {c.strip() for c in args.collections.split(",") if c.strip()}
    src_collections = [c.name for c in src.get_collections().collections]
    if requested:
        src_collections = [n for n in src_collections if n in requested]

    if not src_collections:
        print("No collections to migrate.", file=sys.stderr)
        return 1

    print(f"Migrating {len(src_collections)} collection(s): {src_collections}")
    started = time.time()

    for name in src_collections:
        print(f"\n[{name}]")
        exists = collection_exists(dst, name)
        if exists and args.skip_existing:
            print(f"  · target already has '{name}', --skip-existing → skipping")
            continue
        if not exists:
            replicate_collection_config(src, dst, name, args.dry_run)

        src_count = src.count(collection_name=name, exact=True).count
        moved = migrate_points(src, dst, name, args.batch_size, args.dry_run)

        if not args.dry_run:
            dst_count = dst.count(collection_name=name, exact=True).count
            ok = "✓" if src_count == dst_count else "✗"
            print(f"  {ok} source={src_count} target={dst_count} (migrated {moved})")
            if src_count != dst_count:
                print("    !! count mismatch — investigate before proceeding", file=sys.stderr)

    print(f"\nDone in {time.time() - started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
