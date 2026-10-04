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
import os
import re
from collections import defaultdict
from pathlib import Path

import ijson
from neo4j import GraphDatabase

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
MAX_BODY_CHARS = 500_000  # hard cap on any single body (server chunks it anyway)


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
        return "\n".join(lines)[:MAX_BODY_CHARS]
    if node_is_file_container(data) or node_is_filename_container(data):
        return None
    start_line = parse_location(data.get("source_location"))
    if start_line is None or not source_file:
        return None
    if node_is_entrypoint(data):
        lines = read_file_lines(repo_root, source_file, line_cache)
        if lines is None:
            return None
        return "\n".join(lines)[:MAX_BODY_CHARS]
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
    return body.lstrip("\n")[:MAX_BODY_CHARS]


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


def import_graph(graph_path, uri, user, password, batch_size, repo_root=".", database="graphify"):
    labels = {}
    node_batches = defaultdict(list)
    edge_batches = defaultdict(list)
    indexed_labels = set()
    node_count = edge_count = bodied_count = skipped_count = 0
    symbols = collect_symbol_lines(graph_path)
    line_cache = {}
    state_cache = {}

    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        ensure_database(driver, database)
        with driver.session(database=database) as session:
            def ensure_index(label):
                if label in indexed_labels:
                    return
                # Indexes are created before the first write so MERGE lookups
                # never fall back to full-label scans.
                session.run(
                    f"CREATE INDEX graphify_{label.lower()}_id IF NOT EXISTS FOR (n:{label}) ON (n.id)"
                ).consume()
                indexed_labels.add(label)

            def write_nodes(label, rows):
                ensure_index(label)
                session.run(
                    f"UNWIND $rows AS row MERGE (n:{label} {{id: row.id}}) SET n += row.props",
                    rows=rows,
                ).consume()
                rows.clear()

            def write_edges(source_label, target_label, relation, rows):
                session.run(
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
                props = {**scalar_properties(data), "id": node_id}
                body = body_for_node(repo_root, data, symbols, line_cache, state_cache)
                if body:
                    props["body"] = body
                    props["body_start_line"] = parse_location(data.get("source_location")) or 0
                    bodied_count += 1
                else:
                    skipped_count += 1
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
                        rows.append({"id": node_id, "props": {"id": node_id}})
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
                            rows.append({"id": node_id, "props": {"id": node_id}})
                            node_count += 1
                            if len(rows) >= batch_size:
                                write_nodes("Entity", rows)

            for label, rows in node_batches.items():
                if rows:
                    write_nodes(label, rows)

            print(f"Imported {node_count} nodes "
                  f"({bodied_count} with full source bodies, {skipped_count} without)", flush=True)

            for data in graph_items(graph_path, link_key):
                source = data["source"]
                target = data["target"]
                source_label, target_label = labels[source], labels[target]
                relation = relationship_type(data)
                rows = edge_batches[(source_label, target_label, relation)]
                rows.append({"src": source, "tgt": target, "props": scalar_properties(data, edge=True)})
                edge_count += 1
                if len(rows) >= batch_size:
                    write_edges(source_label, target_label, relation, rows)
                if edge_count % 10000 == 0:
                    print(f"Imported {edge_count} edges", flush=True)

            for (source_label, target_label, relation), rows in edge_batches.items():
                if rows:
                    write_edges(source_label, target_label, relation, rows)

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
    parser.add_argument("--database", default="graphify",
                        help="target NornicDB database, created if missing (default: graphify)")
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")
    if not args.graph.is_file():
        parser.error(f"graph not found: {args.graph}")
    import_graph(args.graph, args.uri, args.user, args.password, args.batch_size,
                 args.repo_root, args.database)


if __name__ == "__main__":
    main()