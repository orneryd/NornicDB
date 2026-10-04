#!/usr/bin/env python3
"""Stream a Graphify code graph into local NornicDB over Neo4j Bolt.

Graphify's ``graph.json`` records only the start line of each symbol, so this
script re-reads the real source files and writes the FULL, untruncated symbol
body — including the comment block directly above the symbol and all comments
within the span — as a ``body`` property on every node. NornicDB's managed
embedding worker includes every string property in the embedding text, so the
ingested nodes become searchable via the vector search APIs without any extra
configuration.

Defaults target a local NornicDB test instance: ``bolt://localhost:7687`` (the
Bolt port; 7474 is the HTTP/UI port) with ``admin`` / ``password`` (override
with --uri/--user/--password or NEO4J_PASSWORD). The graph is imported into its
own ``graphify`` database, created automatically when missing.
"""

import argparse
import json
import os
import re
import time
from collections import defaultdict
from pathlib import Path

import ijson
from neo4j import GraphDatabase
from neo4j.exceptions import TransientError

# File suffixes whose line comments use "#".
HASH_COMMENT_SUFFIXES = {
    ".sh", ".bash", ".zsh", ".ksh", ".fish", ".py", ".rb", ".pl", ".pm", ".r",
    ".yaml", ".yml", ".toml", ".ini", ".cfg", ".conf", ".properties", ".mk",
    ".tf", ".tfvars", ".ps1", ".bat", ".cmd", ".cmake", ".dockerignore",
    ".gitignore", ".graphql",
}
# File suffixes whose line comments use "--" (SQL family).
DASH_COMMENT_SUFFIXES = {".sql"}
# Files with no useful "comment above" convention for lookback.
NO_LOOKBACK_SUFFIXES = {".md", ".mdx", ".html", ".xml", ".json", ".csv", ".txt"}

MAX_LOOKBACK_LINES = 200


def content_hash(props):
    """FNV-1a 64-bit over the canonical JSON of string-valued properties.

    Stable across the importer and the UI upload dialog (same canonical
    form), so both tools agree on whether a node or edge changed. Only
    string values are hashed — they are what the embedding worker turns
    into text. ``updated_at`` and ``props_hash`` themselves are metadata
    and excluded.
    """
    parts = []
    for key in sorted(props):
        if key in ("updated_at", "props_hash"):
            continue
        value = props[key]
        if isinstance(value, str):
            parts.append(
                json.dumps(key, ensure_ascii=False)
                + ":"
                + json.dumps(value, ensure_ascii=False)
            )
    canonical = "{" + ",".join(parts) + "}"
    digest = 0xCBF29CE484222325
    for byte in canonical.encode("utf-8"):
        digest ^= byte
        digest = (digest * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return f"{digest:016x}"


def graph_items(path, key):
    with path.open("rb") as graph_file:
        yield from ijson.items(graph_file, f"{key}.item", use_float=True)


def scalar_properties(data, *, edge=False):
    return {
        key: value for key, value in data.items()
        if isinstance(value, (str, int, float, bool))
        and not key.startswith("_")
        and (not edge or key not in ("source", "target"))
    }


def node_label(data):
    return re.sub(r"[^A-Za-z0-9_]", "", data.get("file_type", "Entity").capitalize()) or "Entity"


def relationship_type(data):
    return re.sub(r"[^A-Z0-9_]", "_", data.get("relation", "RELATED_TO").upper().replace(" ", "_").replace("-", "_")) or "RELATED_TO"


def parse_location(value):
    """Parse a Graphify source_location string like 'L17' into a 1-based line."""
    if not value or not value.startswith("L"):
        return None
    digits = value[1:]
    if not digits.isdigit():
        return None
    return int(digits)


def comment_style(source_file):
    """Return 'hash', 'dash' or 'c' for the lookback comment convention."""
    suffix = Path(source_file).suffix.lower()
    if source_file == "Dockerfile" or suffix in HASH_COMMENT_SUFFIXES:
        return "hash"
    if suffix in DASH_COMMENT_SUFFIXES:
        return "dash"
    return "c"


def comment_mask(lines, style):
    """Boolean mask of comment-only lines for a whole file."""
    mask = [False] * len(lines)
    if style == "none":
        return mask
    in_block = False
    for index, raw in enumerate(lines):
        stripped = raw.strip()
        if in_block:
            mask[index] = True
            if "*/" in stripped:
                in_block = False
            continue
        if style == "hash":
            if stripped.startswith("#"):
                mask[index] = True
            continue
        if style == "dash":
            if stripped.startswith("--"):
                mask[index] = True
            continue
        # C-style: // line comments and /* ... */ blocks (with * continuations).
        if stripped.startswith("//"):
            mask[index] = True
            continue
        if "/*" in stripped:
            mask[index] = True
            if "*/" not in stripped[stripped.index("/*") + 2:]:
                in_block = True
    return mask


def body_span(lines, start_line, mask, style):
    """Extend a symbol's start line upward through its attached comment block.

    Returns the 1-based line of the first comment line directly above the
    symbol, or None when there is no attached block. Comment lines plus at most
    one blank line at a time extend the walk; two consecutive blanks or any
    non-comment line end it, so license headers separated from the symbol by
    code or blank runs are not attached.
    """
    if start_line is None or start_line < 1 or style == "none":
        return None
    index = start_line - 1
    if index <= 0 or index >= len(lines):
        return None
    if mask[index]:
        return None
    seen_comment = False
    blanks = 0
    walk = index - 1
    walked = 0
    while walk >= 0 and walked < MAX_LOOKBACK_LINES:
        walked += 1
        if mask[walk]:
            seen_comment = True
            blanks = 0
            walk -= 1
            continue
        if lines[walk].strip() == "":
            if not seen_comment:
                return None
            blanks += 1
            if blanks > 1:
                break
            walk -= 1
            continue
        break
    if not seen_comment:
        return None
    return walk + 2  # first comment line (1-based)


def read_file_lines(repo_root, source_file, cache):
    if source_file in cache:
        return cache[source_file]
    path = Path(repo_root) / source_file
    lines = None
    if path.is_file():
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            lines = None
    cache[source_file] = lines
    return lines


def node_is_page(data):
    return data.get("node_kind") == "page"


def node_is_file_container(data):
    metadata = data.get("metadata") or {}
    return metadata.get("kind") == "file"


def node_is_entrypoint(data):
    metadata = data.get("metadata") or {}
    return metadata.get("kind") == "bash_entrypoint"


def node_is_filename_container(data):
    """Code-file container nodes whose label is the file basename (e.g. 'agg.go')."""
    source_file = data.get("source_file")
    if not source_file:
        return False
    return data.get("label") == Path(source_file).name and not data.get("node_kind")


def collect_symbol_lines(graph_path):
    """Map source_file -> sorted [(line, node_data)] from a streaming pass."""
    symbols = defaultdict(list)
    for data in graph_items(graph_path, "nodes"):
        line = parse_location(data.get("source_location"))
        symbols[data.get("source_file") or ""].append((line, data))
    for rows in symbols.values():
        rows.sort(key=lambda pair: (pair[0] is None, pair[0] or 0))
    return symbols


def file_comment_state(repo_root, source_file, lines, state_cache):
    """Return the (style, comment_mask) pair for a file, computed once."""
    key = (repo_root, source_file)
    if key in state_cache:
        return state_cache[key]
    style = comment_style(source_file)
    if Path(source_file).suffix.lower() in NO_LOOKBACK_SUFFIXES:
        style = "none"
    mask = comment_mask(lines, style)
    state_cache[key] = (style, mask)
    return style, mask


def body_for_node(repo_root, data, symbols, line_cache, state_cache):
    """Full untruncated body for one node, or None when not applicable."""
    source_file = data.get("source_file")
    if node_is_page(data):
        lines = read_file_lines(repo_root, source_file, line_cache)
        if lines is None:
            return None
        return "\n".join(lines)
    if node_is_file_container(data) or node_is_filename_container(data):
        return None
    start_line = parse_location(data.get("source_location"))
    if start_line is None or not source_file:
        return None
    if node_is_entrypoint(data):
        lines = read_file_lines(repo_root, source_file, line_cache)
        if lines is None:
            return None
        return "\n".join(lines)
    lines = read_file_lines(repo_root, source_file, line_cache)
    if lines is None or start_line > len(lines):
        return None
    style, mask = file_comment_state(repo_root, source_file, lines, state_cache)
    begin = body_span(lines, start_line, mask, style) or start_line
    end_line = len(lines)
    for line, other in symbols[source_file]:
        if line is None or other is data or line <= start_line:
            continue
        # Stop before the next symbol's attached comment block, not just its
        # start line, so the next symbol keeps its own doc comments.
        next_begin = body_span(lines, line, mask, style) or line
        end_line = next_begin - 1
        break
    if end_line < begin:
        end_line = start_line
    body = "\n".join(lines[begin - 1:end_line])
    return body.lstrip("\n")


def ensure_database(driver, database):
    """Create the target database when it does not exist yet."""
    with driver.session() as session:
        existing = set()
        try:
            existing = {row.get("name") for row in session.run("SHOW DATABASES")}
        except Exception:
            existing = set()
        if database not in existing:
            session.run(f"CREATE DATABASE `{database}`").consume()


def emit_enriched_graph(graph_path, out_path, repo_root):
    """Write graph.json with full `body` properties embedded on every node.

    Produces a self-contained artifact that the /graphify UI upload dialog can
    ingest without access to the repository files. Loading the whole graph into
    memory is intentional: the export is for scoped fixtures and enriched
    artifacts, not a streaming pipeline.
    """
    with graph_path.open("rb") as graph_file:
        data = json.load(graph_file)
    symbols = collect_symbol_lines(graph_path)
    line_cache, state_cache = {}, {}
    mtimes = manifest_mtimes(graph_path)
    bodied = 0
    for node in data.get("nodes", []):
        body = body_for_node(repo_root, node, symbols, line_cache, state_cache)
        if body:
            node["body"] = body
            bodied += 1
        node["updated_at"] = updated_at_for(node, repo_root, mtimes)
    with out_path.open("w", encoding="utf-8") as out_file:
        json.dump(data, out_file, ensure_ascii=False)
    print(
        f"Wrote enriched graph to {out_path} "
        f"({len(data.get('nodes', []))} nodes, {bodied} with bodies)"
    )


def manifest_mtimes(graph_path):
    """Map source_file -> recorded mtime from graphify's manifest, if present."""
    manifest = graph_path.parent / "manifest.json"
    mtimes = {}
    if manifest.is_file():
        try:
            with manifest.open("rb") as manifest_file:
                for path, info in json.load(manifest_file).items():
                    if isinstance(info, dict) and info.get("mtime") is not None:
                        mtimes[path] = info["mtime"]
        except (OSError, ValueError):
            pass
    return mtimes


def updated_at_for(data, repo_root, mtimes):
    """Best-effort modification time for a node's source file.

    Falls back to the current time when unknown, so first-time ingests and
    raw artifacts without a manifest still populate every node.
    """
    source_file = data.get("source_file")
    mtime = mtimes.get(source_file) if source_file else None
    if mtime is None and source_file:
        try:
            mtime = (Path(repo_root) / source_file).stat().st_mtime
        except OSError:
            mtime = None
    return mtime if mtime is not None else time.time()


def import_graph(graph_path, uri, user, password, batch_size, repo_root=".", database="nornicdbcode", sync=True):
    labels = {}
    node_batches = defaultdict(list)
    edge_batches = defaultdict(list)
    indexed_labels = set()
    node_count = edge_count = bodied_count = skipped_count = 0
    updated_nodes = 0
    symbols = collect_symbol_lines(graph_path)
    line_cache = {}
    state_cache = {}
    mtimes = manifest_mtimes(graph_path)

    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        ensure_database(driver, database)
        with driver.session(database=database) as session:
            def run_retry(query, **params):
                """Run a query, retrying transient MVCC conflicts.

                NornicDB's embedding worker rewrites node properties
                concurrently with ingestion, so commits can fail with
                ``Neo.TransientError.Transaction.Outdated``. Bounded
                exponential-backoff retries make ingestion resilient to that
                without changing any semantics.
                """
                delay = 0.05
                for attempt in range(12):
                    try:
                        return session.run(query, **params)
                    except TransientError:
                        if attempt == 11:
                            raise
                        time.sleep(delay)
                        delay = min(delay * 2, 1.0)

            def ensure_index(label):
                if label in indexed_labels:
                    return
                # Indexes are created before the first write so MERGE lookups
                # never fall back to full-label scans.
                run_retry(
                    f"CREATE INDEX graphify_{label.lower()}_id IF NOT EXISTS FOR (n:{label}) ON (n.id)"
                ).consume()
                indexed_labels.add(label)

            def write_nodes(label, rows):
                nonlocal updated_nodes
                ensure_index(label)
                # Create missing nodes with their full properties.
                run_retry(
                    f"UNWIND $rows AS row MERGE (n:{label} {{id: row.id}}) "
                    f"ON CREATE SET n += row.props",
                    rows=rows,
                ).consume()
                # Gentle update: rewrite only nodes whose content hash
                # differs from the artifact copy. Unchanged nodes are never
                # touched, so the embedding worker has nothing to re-embed
                # on repeat runs.
                result = run_retry(
                    f"UNWIND $rows AS row MATCH (n:{label} {{id: row.id}}) "
                    f"WHERE n.props_hash IS NULL OR n.props_hash <> row.props_hash "
                    f"SET n += row.props RETURN count(n) AS updated",
                    rows=rows,
                )
                record = result.single()
                updated_nodes += record.get("updated", 0) if record else 0
                rows.clear()

            def write_edges(source_label, target_label, relation, rows):
                # Edges are written in a single MERGE: the engine's batched
                # UNWIND-MERGE fast path handles this shape, and re-running
                # rewrites edge props in place. Edges carry no embeddings, so
                # an unconditional SET has no re-embedding cost (unlike nodes,
                # which stay hash-guarded).
                run_retry(
                    f"UNWIND $rows AS row "
                    f"MATCH (a:{source_label} {{id: row.src}}), (b:{target_label} {{id: row.tgt}}) "
                    f"MERGE (a)-[r:{relation}]->(b) SET r += row.props",
                    rows=rows,
                ).consume()
                rows.clear()

            for data in graph_items(graph_path, "nodes"):
                label = node_label(data)
                node_id = data["id"]
                labels[node_id] = label
                props = {
                    **scalar_properties(data),
                    "id": node_id,
                    "updated_at": updated_at_for(data, repo_root, mtimes),
                }
                body = body_for_node(repo_root, data, symbols, line_cache, state_cache)
                if body:
                    props["body"] = body
                    props["body_start_line"] = parse_location(data.get("source_location")) or 0
                    bodied_count += 1
                else:
                    skipped_count += 1
                props["props_hash"] = content_hash(props)
                rows = node_batches[label]
                rows.append({"id": node_id, "props": props})
                node_count += 1
                if len(rows) >= batch_size:
                    write_nodes(label, rows)

            # Graphify >=0.9.69 exports links; older graphs used edges. Probe
            # the links key first and fall back to edges when it is absent.
            link_key = "links"
            probe = 0
            for data in graph_items(graph_path, link_key):
                probe += 1
                for node_id in (data["source"], data["target"]):
                    if node_id not in labels:
                        labels[node_id] = "Entity"
                        rows = node_batches["Entity"]
                        props = {"id": node_id, "updated_at": time.time()}
                        props["props_hash"] = content_hash(props)
                        rows.append({"id": node_id, "props": props})
                        node_count += 1
                        if len(rows) >= batch_size:
                            write_nodes("Entity", rows)
            if probe == 0:
                link_key = "edges"
                for data in graph_items(graph_path, link_key):
                    for node_id in (data["source"], data["target"]):
                        if node_id not in labels:
                            labels[node_id] = "Entity"
                            rows = node_batches["Entity"]
                            props = {"id": node_id, "updated_at": time.time()}
                            props["props_hash"] = content_hash(props)
                            rows.append({"id": node_id, "props": props})
                            node_count += 1
                            if len(rows) >= batch_size:
                                write_nodes("Entity", rows)

            for label, rows in node_batches.items():
                if rows:
                    write_nodes(label, rows)

            print(f"Imported {node_count} nodes "
                  f"({bodied_count} with full source bodies, {skipped_count} without)", flush=True)

            incoming_edges = set()
            for data in graph_items(graph_path, link_key):
                source = data["source"]
                target = data["target"]
                source_label, target_label = labels[source], labels[target]
                relation = relationship_type(data)
                incoming_edges.add((source, target, relation))
                rows = edge_batches[(source_label, target_label, relation)]
                edge_props = scalar_properties(data, edge=True)
                edge_props["props_hash"] = content_hash(edge_props)
                rows.append({"src": source, "tgt": target, "props": edge_props})
                edge_count += 1
                if len(rows) >= batch_size:
                    write_edges(source_label, target_label, relation, rows)
                if edge_count % 10000 == 0:
                    print(f"Imported {edge_count} edges", flush=True)

            for (source_label, target_label, relation), rows in edge_batches.items():
                if rows:
                    write_edges(source_label, target_label, relation, rows)

            # Incremental sync: delete stale edges and nodes that no longer
            # appear in the artifact. Scoped to importer-managed labels so
            # unrelated data in the database is never touched.
            if sync and labels:
                managed_labels = sorted(set(labels.values()))
                label_clause = " OR ".join(
                    f"n:{label}" for label in managed_labels
                )
                existing_ids = set()
                for record in run_retry(
                    f"MATCH (n) WHERE n.id IS NOT NULL AND ({label_clause}) RETURN n.id AS id"
                ):
                    existing_ids.add(record.get("id"))
                stale_ids = [node_id for node_id in existing_ids if node_id not in labels]
                for batch in (stale_ids[i:i + batch_size] for i in range(0, len(stale_ids), batch_size)):
                    run_retry(
                        f"UNWIND $ids AS id MATCH (n) WHERE n.id = id AND ({label_clause}) DETACH DELETE n",
                        ids=batch,
                    ).consume()

                # Only relationships between importer-managed nodes are
                # candidates; edges touching unrelated data are never
                # considered stale.
                a_clause = " OR ".join(f"a:{label}" for label in managed_labels)
                b_clause = " OR ".join(f"b:{label}" for label in managed_labels)
                existing_edges = set()
                for record in run_retry(
                    f"MATCH (a)-[r]->(b) WHERE a.id IS NOT NULL AND b.id IS NOT NULL "
                    f"AND ({a_clause}) AND ({b_clause}) "
                    f"RETURN a.id AS src, b.id AS tgt, type(r) AS rel"
                ):
                    existing_edges.add((record.get("src"), record.get("tgt"), record.get("rel")))
                stale_edges = [edge for edge in existing_edges if edge not in incoming_edges]
                for batch in (stale_edges[i:i + batch_size] for i in range(0, len(stale_edges), batch_size)):
                    run_retry(
                        "UNWIND $rows AS row MATCH (a {id: row.src})-[r]->(b {id: row.tgt}) "
                        "WHERE type(r) = row.rel DELETE r",
                        rows=[{"src": src, "tgt": tgt, "rel": rel} for src, tgt, rel in batch],
                    ).consume()
                if stale_ids or stale_edges:
                    print(
                        f"Sync: removed {len(stale_ids)} stale nodes and "
                        f"{len(stale_edges)} stale edges", flush=True,
                    )

            print(
                f"Updated {updated_nodes} existing nodes; "
                f"everything else was left untouched", flush=True,
            )

    print(f"Imported {node_count} nodes and {edge_count} edges into {uri}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--graph", type=Path, default=Path("graphify-out/graph.json"))
    parser.add_argument("--uri", default="bolt://localhost:7687",
                        help="NornicDB/Neo4j Bolt URI (default: bolt://localhost:7687; 7474 is the HTTP/UI port)")
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default=os.environ.get("NEO4J_PASSWORD", "password"),
                        help="password (default: NEO4J_PASSWORD env or 'password')")
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--repo-root", default=".",
                        help="repository root for resolving graph source_file paths (default: .)")
    parser.add_argument("--database", default="nornicdbcode",
                        help="target NornicDB database, created if missing (default: nornicdbcode)")
    parser.add_argument("--no-sync", action="store_true",
                        help="skip deleting stale nodes and edges that disappeared from the graph")
    parser.add_argument("--out-graph", type=Path, default=None,
                        help="write an enriched graph.json (bodies embedded) to this path and skip ingestion")
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")
    if not args.graph.is_file():
        parser.error(f"graph not found: {args.graph}")
    if args.out_graph is not None:
        emit_enriched_graph(args.graph, args.out_graph, args.repo_root)
        return
    import_graph(args.graph, args.uri, args.user, args.password, args.batch_size,
                 args.repo_root, args.database, sync=not args.no_sync)


if __name__ == "__main__":
    main()