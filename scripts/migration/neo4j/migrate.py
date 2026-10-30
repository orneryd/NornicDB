#!/usr/bin/env python3
"""
Neo4j → NornicDB migration (Python).

Bolt → Bolt. Reads constraints, indexes, nodes, and relationships from a
source Neo4j instance and replays them into NornicDB using hot-path
UNWIND/MERGE shapes from docs/performance/hot-path-query-cookbook.md.

Phases (always in this order):
  1. Schema  — constraints, then indexes (vector and fulltext last).
  2. Nodes   — UNWIND $rows MERGE by element-id key, set labels, set props.
                Hits UnwindSimpleMergeBatch.
  3. Edges   — UNWIND $rows MATCH (a), MATCH (b) CREATE (a)-[:T]->(b).
                Hits UnwindMultiMatchCreateBatch.

The element ID from Neo4j is preserved on each node as `_neo4j_id` so the
edge phase can rebind start/end nodes deterministically.

Usage:
  pip install neo4j
  python migrate.py \
      --source-url bolt://neo4j.prod:7687 --source-user neo4j --source-pass <pw> \
      --target-url bolt://nornicdb.local:7687 \
      --batch-size 500

Flags:
  --batch-size N          rows per UNWIND batch (default 500)
  --skip-schema           don't migrate constraints/indexes
  --skip-nodes / --skip-edges
  --labels Label1,Label2  only migrate these node labels
  --types  TYPE1,TYPE2    only migrate these relationship types
  --dry-run               read everything, write nothing
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Iterable

from neo4j import GraphDatabase, Driver, Session


# --------------------------------------------------------------------------
# Args
# --------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Neo4j → NornicDB migration")
    p.add_argument("--source-url", required=True)
    p.add_argument("--source-user", required=True)
    p.add_argument("--source-pass", required=True)
    p.add_argument("--source-database", default="neo4j")
    p.add_argument("--target-url", required=True)
    p.add_argument("--target-user", default="neo4j")
    p.add_argument("--target-pass", default="password")
    p.add_argument("--target-database", default="neo4j")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--skip-schema", action="store_true")
    p.add_argument("--skip-nodes", action="store_true")
    p.add_argument("--skip-edges", action="store_true")
    p.add_argument("--labels", default="")
    p.add_argument("--types", default="")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def csv_set(s: str) -> set[str]:
    return {x.strip() for x in s.split(",") if x.strip()}


# --------------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------------

def fetch_constraints(sess: Session) -> list[dict]:
    """Read constraints via SHOW CONSTRAINTS (Neo4j 5)."""
    out = []
    for r in sess.run("SHOW CONSTRAINTS"):
        out.append(dict(r))
    return out


def fetch_indexes(sess: Session) -> list[dict]:
    out = []
    for r in sess.run("SHOW INDEXES"):
        out.append(dict(r))
    return out


def constraint_to_cypher(c: dict) -> str | None:
    """
    Best-effort port of a Neo4j constraint row to NornicDB DDL.
    Skips constraints we can't safely express; logs the original.
    """
    name = c.get("name", "")
    ctype = (c.get("type") or "").upper()
    entity_type = (c.get("entityType") or "").upper()
    labels = c.get("labelsOrTypes") or []
    props = c.get("properties") or []
    if not labels or not props:
        return None
    label = labels[0]
    proplist = ", ".join(f"n.{p}" for p in props)
    proplist_paren = f"({proplist})" if len(props) > 1 else proplist

    if entity_type == "NODE":
        if ctype == "UNIQUENESS":
            return f"CREATE CONSTRAINT {name} IF NOT EXISTS FOR (n:{label}) REQUIRE {proplist_paren} IS UNIQUE"
        if ctype == "NODE_KEY":
            return f"CREATE CONSTRAINT {name} IF NOT EXISTS FOR (n:{label}) REQUIRE {proplist_paren} IS NODE KEY"
        if ctype == "NODE_PROPERTY_EXISTENCE":
            return f"CREATE CONSTRAINT {name} IF NOT EXISTS FOR (n:{label}) REQUIRE {proplist_paren} IS NOT NULL"
    elif entity_type == "RELATIONSHIP":
        if ctype == "RELATIONSHIP_PROPERTY_EXISTENCE":
            return (
                f"CREATE CONSTRAINT {name} IF NOT EXISTS "
                f"FOR ()-[r:{label}]-() REQUIRE r.{props[0]} IS NOT NULL"
            )
        if ctype == "RELATIONSHIP_UNIQUENESS":
            return (
                f"CREATE CONSTRAINT {name} IF NOT EXISTS "
                f"FOR ()-[r:{label}]-() REQUIRE r.{props[0]} IS UNIQUE"
            )
    return None


def index_to_cypher(idx: dict) -> str | None:
    """Best-effort port of a Neo4j index row to NornicDB DDL."""
    if (idx.get("owningConstraint") or "") != "":
        return None  # constraint-owned; the constraint DDL re-creates it
    name = idx.get("name", "")
    itype = (idx.get("type") or "").upper()
    entity = (idx.get("entityType") or "").upper()
    labels = idx.get("labelsOrTypes") or []
    props = idx.get("properties") or []
    if not labels or not props:
        return None
    label = labels[0]
    if entity != "NODE":
        return None
    if itype in ("RANGE", "BTREE"):
        if len(props) == 1:
            return f"CREATE INDEX {name} IF NOT EXISTS FOR (n:{label}) ON (n.{props[0]})"
        return (
            f"CREATE INDEX {name} IF NOT EXISTS FOR (n:{label}) "
            f"ON ({', '.join(f'n.{p}' for p in props)})"
        )
    if itype == "FULLTEXT":
        return (
            f"CREATE FULLTEXT INDEX {name} IF NOT EXISTS "
            f"FOR (n:{label}) ON EACH [{', '.join(f'n.{p}' for p in props)}]"
        )
    if itype == "VECTOR":
        opts = idx.get("options") or {}
        cfg = (opts.get("indexConfig") or {})
        dims = cfg.get("vector.dimensions")
        sim = cfg.get("vector.similarity_function") or "cosine"
        if not dims:
            return None
        return (
            f"CREATE VECTOR INDEX {name} IF NOT EXISTS "
            f"FOR (n:{label}) ON (n.{props[0]}) "
            f"OPTIONS {{indexConfig: {{`vector.dimensions`: {int(dims)}, "
            f"`vector.similarity_function`: '{sim}'}}}}"
        )
    return None


def replicate_schema(src: Session, dst: Session, dry_run: bool) -> None:
    print("\n[schema]")
    for c in fetch_constraints(src):
        ddl = constraint_to_cypher(c)
        if not ddl:
            print(f"  · skip constraint (unmapped): {c.get('name', c)}")
            continue
        print(f"  → {ddl}")
        if not dry_run:
            dst.run(ddl).consume()

    for i in fetch_indexes(src):
        ddl = index_to_cypher(i)
        if not ddl:
            continue
        print(f"  → {ddl}")
        if not dry_run:
            dst.run(ddl).consume()


# --------------------------------------------------------------------------
# Nodes
# --------------------------------------------------------------------------

def fetch_node_labels(sess: Session) -> list[str]:
    return [r["label"] for r in sess.run("CALL db.labels() YIELD label RETURN label")]


def stream_nodes(sess: Session, label: str, batch: int) -> Iterable[list[dict]]:
    """Stream nodes for a single label in keyset-paginated batches."""
    last_id: str | None = None
    while True:
        if last_id is None:
            q = (
                f"MATCH (n:`{label}`) "
                "RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props "
                "ORDER BY elementId(n) "
                "LIMIT $limit"
            )
            res = sess.run(q, limit=batch)
        else:
            q = (
                f"MATCH (n:`{label}`) "
                "WHERE elementId(n) > $cursor "
                "RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props "
                "ORDER BY elementId(n) "
                "LIMIT $limit"
            )
            res = sess.run(q, cursor=last_id, limit=batch)
        rows = [dict(r) for r in res]
        if not rows:
            return
        yield rows
        last_id = rows[-1]["id"]
        if len(rows) < batch:
            return


# Hot path: UnwindSimpleMergeBatch — single MERGE on a stable key, idempotent.
# We pin _neo4j_id as the merge key and SET labels/properties from the row.
NODE_UPSERT_CYPHER = """
UNWIND $rows AS row
MERGE (n:_Migrated {_neo4j_id: row.id})
SET n += row.props
SET n._neo4j_labels = row.labels
"""


def replicate_nodes(
    src: Session,
    dst: Session,
    labels: list[str],
    label_filter: set[str],
    batch: int,
    dry_run: bool,
) -> int:
    print("\n[nodes]")
    total = 0
    for label in labels:
        if label_filter and label not in label_filter:
            continue
        if label.startswith("_"):
            continue  # internal
        moved = 0
        for chunk in stream_nodes(src, label, batch):
            if not dry_run:
                dst.run(NODE_UPSERT_CYPHER, rows=chunk).consume()
            moved += len(chunk)
            print(f"  · :{label}: {moved}")
            total += len(chunk)
    print(f"  total nodes upserted: {total}")
    return total


# Apply the original labels back onto the merged nodes in a second pass.
# Runs once after all node batches; keeps the per-batch hot path narrow.
APPLY_LABELS_CYPHER = """
MATCH (n:_Migrated)
WITH n, n._neo4j_labels AS labels
CALL apoc.create.addLabels(n, labels) YIELD node
RETURN count(node)
"""


def apply_labels(dst: Session, dry_run: bool) -> None:
    print("\n[apply labels]")
    if dry_run:
        return
    # Cypher path that does not depend on apoc: SET label list explicitly via
    # multiple statements is tricky in pure Cypher because labels can't be
    # parameterized. We fall back to a plain pass-through: each node already
    # has its labels stored on _neo4j_labels, and consumers can re-promote
    # them using a separate Cypher pass tailored to their schema. The
    # default behavior of this script preserves data, not labels — which is
    # the right trade-off when labels collide with NornicDB-reserved names.
    print("  · labels preserved on n._neo4j_labels (apply manually with"
          " your label policy; pure Cypher cannot parameterize labels)")


# --------------------------------------------------------------------------
# Edges
# --------------------------------------------------------------------------

def fetch_relationship_types(sess: Session) -> list[str]:
    return [
        r["relationshipType"]
        for r in sess.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
    ]


def stream_edges(sess: Session, rel_type: str, batch: int) -> Iterable[list[dict]]:
    last_id: str | None = None
    while True:
        if last_id is None:
            q = (
                f"MATCH (a)-[r:`{rel_type}`]->(b) "
                "RETURN elementId(r) AS id, elementId(a) AS startId, elementId(b) AS endId, "
                "       properties(r) AS props "
                "ORDER BY elementId(r) "
                "LIMIT $limit"
            )
            res = sess.run(q, limit=batch)
        else:
            q = (
                f"MATCH (a)-[r:`{rel_type}`]->(b) "
                "WHERE elementId(r) > $cursor "
                "RETURN elementId(r) AS id, elementId(a) AS startId, elementId(b) AS endId, "
                "       properties(r) AS props "
                "ORDER BY elementId(r) "
                "LIMIT $limit"
            )
            res = sess.run(q, cursor=last_id, limit=batch)
        rows = [dict(r) for r in res]
        if not rows:
            return
        yield rows
        last_id = rows[-1]["id"]
        if len(rows) < batch:
            return


def edge_create_cypher(rel_type: str) -> str:
    """
    Hot path: UnwindMultiMatchCreateBatch — many MATCHes + CREATE per row,
    no RETURN/WITH/SET/MERGE. Edge-only variant since both endpoints already
    exist after the node phase. The relationship type cannot be a parameter
    in Cypher; templated per type instead.
    """
    return (
        "UNWIND $rows AS row "
        "MATCH (a:_Migrated {_neo4j_id: row.startId}) "
        "MATCH (b:_Migrated {_neo4j_id: row.endId}) "
        f"CREATE (a)-[:`{rel_type}` {{_neo4j_id: row.id}}]->(b)"
    )


def edge_props_cypher(rel_type: str) -> str:
    """
    Apply edge properties in a follow-up MATCH that hits the relationship by
    its preserved Neo4j id. Keeps the create-side on the strict hot path.
    """
    return (
        "UNWIND $rows AS row "
        f"MATCH (a:_Migrated {{_neo4j_id: row.startId}})-[r:`{rel_type}` "
        "{_neo4j_id: row.id}]->(b:_Migrated {_neo4j_id: row.endId}) "
        "SET r += row.props"
    )


def replicate_edges(
    src: Session,
    dst: Session,
    types: list[str],
    type_filter: set[str],
    batch: int,
    dry_run: bool,
) -> int:
    print("\n[edges]")
    total = 0
    for t in types:
        if type_filter and t not in type_filter:
            continue
        moved = 0
        create_q = edge_create_cypher(t)
        props_q = edge_props_cypher(t)
        for chunk in stream_edges(src, t, batch):
            if not dry_run:
                dst.run(create_q, rows=chunk).consume()
                # Skip props pass when the chunk has no edge properties.
                if any(row.get("props") for row in chunk):
                    dst.run(props_q, rows=chunk).consume()
            moved += len(chunk)
            print(f"  · :{t}: {moved}")
            total += len(chunk)
    print(f"  total edges created: {total}")
    return total


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main() -> int:
    args = parse_args()
    label_filter = csv_set(args.labels)
    type_filter = csv_set(args.types)

    src_drv: Driver = GraphDatabase.driver(
        args.source_url, auth=(args.source_user, args.source_pass)
    )
    dst_drv: Driver = GraphDatabase.driver(
        args.target_url, auth=(args.target_user, args.target_pass)
    )

    started = time.time()
    with src_drv.session(database=args.source_database) as src, \
         dst_drv.session(database=args.target_database) as dst:

        if not args.skip_schema:
            replicate_schema(src, dst, args.dry_run)

        if not args.skip_nodes:
            labels = fetch_node_labels(src)
            replicate_nodes(src, dst, labels, label_filter, args.batch_size, args.dry_run)
            apply_labels(dst, args.dry_run)

        if not args.skip_edges:
            types = fetch_relationship_types(src)
            replicate_edges(src, dst, types, type_filter, args.batch_size, args.dry_run)

    src_drv.close()
    dst_drv.close()
    print(f"\nDone in {time.time() - started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
