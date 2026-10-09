#!/usr/bin/env python3
"""Load a Graphify code graph into a local NornicDB: the importer behind /graphify.

This is the same importer the Soraban/code-intelligence GitHub Action runs on every
push (scripts/graphify_ingest.py there), with this repository's defaults. Keep the two
in step: a fix to one belongs in the other.

Graphify's ``graph.json`` records only the start line of each symbol, so the importer
re-reads the real source files and writes the FULL, untruncated symbol ``body``
(its source without comment-only lines and docstrings) and a separate ``comments``
property (the comment block above the symbol, its docstring and the comments inside it)
on every node. Graphify's communities become ``Community`` nodes joined to their members
by ``IN_COMMUNITY`` edges. NornicDB's managed embedding worker includes every string
property in the embedding text, so ingested nodes become searchable through the vector
search APIs without any extra configuration.

* Every node carries a ``repo`` property and nodes are keyed by ``(id, repo)``.
* ``symbol_kind`` (function / class / file / external / other) tells a function defined
  in the code from a class, a file container or a referenced-only external symbol.
* The repository's main entry point (default ``cmd/nornicdb/main.go:main``) gets an
  extra ``:Main`` label; the /graphify page starts from it. Name another with --main
  (a node id, path/to/file:symbol, or a unique symbol name; ``none`` for no main).
* Re-runs are incremental: nodes are rewritten only when their content hash changes
  (so unchanged code is never re-embedded) and stale nodes and edges are removed, but
  only nodes this importer wrote, and never more than --max-delete-fraction of them.
* ``(:CodeRepository {name})`` records the last ingested commit; --check reports whether
  the ingest is needed.

Defaults target a local NornicDB: the HTTP API at http://localhost:7474 (use a
bolt:// URI for the Bolt port) with admin / password, database ``nornicdbcode``.
Override with --uri/--user/--password/--database or NORNICDB_URI / NORNICDB_USER /
NORNICDB_PASSWORD / NORNICDB_DATABASE.

    graphify extract . --code-only --no-cluster
    python scripts/graphify_local.py --graph graphify-out/graph.json
"""

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path

import ijson

# The stale-deletion guard only applies to graphs at least this large; a small
# repository legitimately loses a big share of its symbols in one commit.
MASS_DELETE_MIN_EXISTING = 200

# NornicDB rejects HTTP request bodies over 10 MB; stay well under it.
HTTP_MAX_REQUEST_BYTES = 8 * 1024 * 1024
# A single body larger than this cannot be sent over HTTP even alone in a
# batch, so it is dropped (the node itself is still ingested).
HTTP_MAX_BODY_BYTES = 6 * 1024 * 1024


class RetryableError(Exception):
    """Transient failure (MVCC conflict, load balancer 5xx, dropped connection)."""


class HttpResult:
    def __init__(self, records):
        self._records = records

    def __iter__(self):
        return iter(self._records)

    def single(self):
        return self._records[0] if self._records else None

    def consume(self):
        return None


class HttpSession:
    """Minimal stand-in for a neo4j Session over NornicDB's HTTP API."""

    def __init__(self, base_url, auth_header, database):
        self.url = f"{base_url}/db/{urllib.parse.quote(database, safe='')}/tx/commit"
        self.auth_header = auth_header

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def run(self, query, **params):
        body = json.dumps({"statements": [{"statement": query, "parameters": params}]},
                          ensure_ascii=False).encode("utf-8")
        if len(body) > HTTP_MAX_REQUEST_BYTES:
            # Split the batch (rows/ids list) in half until each request fits.
            for key, value in params.items():
                if isinstance(value, list) and len(value) > 1:
                    half = len(value) // 2
                    first = self.run(query, **{**params, key: value[:half]})
                    second = self.run(query, **{**params, key: value[half:]})
                    return HttpResult(list(first) + list(second))
        request = urllib.request.Request(self.url, data=body, method="POST", headers={
            "Authorization": self.auth_header,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        try:
            with urllib.request.urlopen(request, timeout=600) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", "replace")[:500]
            if error.code in (429, 502, 503, 504):
                raise RetryableError(f"HTTP {error.code}: {detail}") from error
            raise RuntimeError(f"HTTP {error.code} from {self.url}: {detail}") from error
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            raise RetryableError(str(error)) from error
        if payload.get("errors"):
            first = payload["errors"][0]
            message = f"{first.get('code')}: {first.get('message')}"
            if "TransientError" in (first.get("code") or ""):
                raise RetryableError(message)
            raise RuntimeError(message)
        result = (payload.get("results") or [{}])[0]
        columns = result.get("columns", [])
        return HttpResult([dict(zip(columns, row["row"])) for row in result.get("data", [])])


class HttpDriver:
    def __init__(self, uri, user, password):
        self.base_url = uri.rstrip("/")
        token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        self.auth_header = f"Basic {token}"

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def session(self, database=None):
        # Administrative statements (SHOW/CREATE DATABASE) work from any database.
        return HttpSession(self.base_url, self.auth_header, database or "system")


def is_http(uri):
    return uri.startswith(("http://", "https://"))


def connect(uri, user, password):
    if is_http(uri):
        return HttpDriver(uri, user, password)
    from neo4j import GraphDatabase
    return GraphDatabase.driver(uri, auth=(user, password))


def retryable_errors():
    try:
        from neo4j.exceptions import ServiceUnavailable, TransientError
    except ImportError:
        return (RetryableError,)
    return (RetryableError, TransientError, ServiceUnavailable)

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


def symbol_kind(data):
    """Classify a Graphify node as external | file | class | function | other.

    Graphify gives symbols that are only *referenced* from the repository (a gem,
    a library function, a stub) no ``source_file``. It flags callables with
    private ``_callable`` / ``_callable_class`` markers (dropped from the stored
    properties), but only for some languages: Ruby, Python and TypeScript have
    them, Go has none. Every language labels a function or method with a
    trailing ``()``, so that counts too. Stored as ``symbol_kind`` so queries can
    tell a function defined in this codebase from a class, a file container or
    an external symbol.
    """
    if not data.get("source_file"):
        return "external"
    if (node_is_page(data) or node_is_file_container(data) or node_is_entrypoint(data)
            or node_is_filename_container(data)):
        return "file"
    if data.get("_callable_class"):
        return "class"
    if data.get("_callable") or (data.get("label") or "").endswith("()"):
        return "function"
    return "other"


def is_test_source(source_file):
    """Test code, which is never picked as a default main (mirrors the /graphify page)."""
    path = (source_file or "").lower()
    name = path.rsplit("/", 1)[-1]
    if name.endswith("_test.go") or name.startswith("test_"):
        return True
    if re.search(r"\.(test|spec)\.(js|jsx|ts|tsx|mjs|cjs)$", name):
        return True
    return bool(re.search(r"(^|/)(test|tests|testing|spec|specs)(/|$)", path))


class MainSelectionError(Exception):
    """The main entry point could not be determined; nothing has been written."""


NO_MAIN = "none"


def graph_link_key(graph_path):
    return "links" if next(graph_items(graph_path, "links"), None) is not None else "edges"


def _symbol_name(name):
    """'.authenticate_request()' / 'authenticate_request' -> 'authenticate_request'."""
    name = (name or "").strip()
    if name.endswith("()"):
        name = name[:-2]
    return name[1:] if name.startswith(".") else name


def _describe(candidate):
    return (f"{candidate['source_file']}:{candidate['source_location']} {candidate['label']} "
            f"[{candidate['kind']}, {len(candidate['neighbors'])} connected]")


def collect_main_candidates(graph_path):
    """Every node's kind, plus connectivity for functions and classes.

    Connectivity is the number of distinct other nodes joined to a symbol by any
    edge except CONTAINS (the structural file-to-symbol link every symbol has).
    """
    nodes = {}
    for data in graph_items(graph_path, "nodes"):
        kind = symbol_kind(data)
        nodes[data["id"]] = {
            "id": data["id"], "label": data.get("label") or data["id"], "kind": kind,
            "source_file": data.get("source_file") or "",
            "source_location": data.get("source_location") or "",
            "db_label": node_label(data), "neighbors": set(),
        }
    for edge in graph_items(graph_path, graph_link_key(graph_path)):
        if relationship_type(edge) == "CONTAINS":
            continue
        source, target = edge["source"], edge["target"]
        if source == target:
            continue
        for end, other in ((source, target), (target, source)):
            node = nodes.get(end)
            if node is not None and node["kind"] in ("function", "class"):
                node["neighbors"].add(other)
    return nodes


def choose_main(graph_path, spec=""):
    """Pick the repository's main entry point; raises MainSelectionError.

    ``spec`` identifies it the way Graphify does: a node id
    (``app_routes_layout_rootlayout``), ``path/to/file:Symbol``
    (``worker/src/connect/worker/main.py:main``) or a bare symbol name
    (``main`` / ``main()`` / ``.run()``), which must be unique. With no spec the
    default is the non-test *function* defined in this codebase with the most
    connected nodes; external symbols, classes and file containers are never
    chosen by default.
    """
    nodes = collect_main_candidates(graph_path)
    spec = (spec or "").strip()
    if spec:
        chosen = _resolve_main_spec(spec, nodes)
        chosen = dict(chosen, how="explicit")
    else:
        functions = [n for n in nodes.values() if n["kind"] == "function"]
        if not functions:
            raise MainSelectionError(
                "No function defined in this repository was found in the graph, so there is no "
                "default main entry point. Pass one (--main / the action's `main` input), or "
                "--main none to ingest without one.")
        pool = [n for n in functions if not is_test_source(n["source_file"])] or functions
        ranked = sorted(pool, key=lambda n: (-len(n["neighbors"]), n["source_file"],
                                             parse_location(n["source_location"]) or 0, n["id"]))
        chosen = dict(ranked[0], how="default", runner_ups=[_describe(n) for n in ranked[1:6]])
    chosen["connected"] = len(chosen["neighbors"])
    del chosen["neighbors"]
    return chosen


def _resolve_main_spec(spec, nodes):
    import difflib

    def acceptable(matches):
        return [n for n in matches if n["kind"] in ("function", "class")]

    def pick(matches, what):
        good = acceptable(matches)
        if len(good) == 1:
            return good[0]
        if len(good) > 1:
            lines = "\n".join("  " + _describe(n) + "  id=" + n["id"]
                              for n in sorted(good, key=lambda n: (n["source_file"], n["source_location"]))[:10])
            raise MainSelectionError(
                f"main {what!r} is ambiguous ({len(good)} matches). Qualify it as "
                f"path/to/file:symbol or use the node id:\n{lines}")
        return None

    if spec in nodes:
        node = nodes[spec]
        if node["kind"] not in ("function", "class"):
            raise MainSelectionError(
                f"main {spec!r} is a {node['kind']} node, not a function or class defined in this "
                f"repository. Name a function (or class) from the code itself.")
        return node

    # path/to/file:Symbol -- try every ':' split, since symbols can contain '::'.
    files = {n["source_file"] for n in nodes.values() if n["source_file"]}
    for index, char in enumerate(spec):
        if char != ":":
            continue
        path, symbol = spec[:index], spec[index + 1:]
        if path in files and symbol:
            matches = [n for n in nodes.values()
                       if n["source_file"] == path and _symbol_name(n["label"]) == _symbol_name(symbol)]
            found = pick(matches, spec)
            if found:
                return found
            others = sorted({_describe(n) for n in nodes.values() if n["source_file"] == path
                             and n["kind"] in ("function", "class")})[:10]
            raise MainSelectionError(
                f"No function or class {symbol!r} is defined in {path}. Defined there:\n  "
                + "\n  ".join(others))

    matches = [n for n in nodes.values() if _symbol_name(n["label"]) == _symbol_name(spec)]
    found = pick(matches, spec)
    if found:
        return found
    names = sorted({_symbol_name(n["label"]) for n in nodes.values() if n["kind"] in ("function", "class")})
    close = difflib.get_close_matches(_symbol_name(spec), names, n=5)
    hint = f" Did you mean: {', '.join(close)}?" if close else ""
    raise MainSelectionError(
        f"main {spec!r} matches no function or class defined in this repository's graph "
        f"(external symbols cannot be the main entry point).{hint}")


class Communities:
    """Graphify's clustering: which community each node is in, and what it is called.

    Names are voted per community, because a graph that was re-clustered without
    re-labelling can carry stale names from an older numbering (the importer then
    uses the most common one and reports the mismatch).
    """

    def __init__(self):
        self.members = []  # (node id, node db label, community id)
        self.names = defaultdict(lambda: defaultdict(int))
        self.sizes = defaultdict(int)

    def add(self, node_id, label, community, name):
        if not isinstance(community, int) or isinstance(community, bool):
            return
        self.members.append((node_id, label, community))
        self.sizes[community] += 1
        if isinstance(name, str) and name.strip():
            self.names[community][name.strip()] += 1

    def ids(self):
        return sorted(self.sizes)

    def name(self, community):
        votes = self.names.get(community)
        if not votes:
            return ""
        return max(sorted(votes), key=lambda candidate: votes[candidate])

    def inconsistent(self):
        return sum(1 for votes in self.names.values() if len(votes) > 1)

    def unnamed(self):
        return sum(1 for community in self.sizes if not self.names.get(community))


def collect_symbol_lines(graph_path):
    """Map source_file -> sorted [(line, node_data)] from a streaming pass."""
    symbols = defaultdict(list)
    for data in graph_items(graph_path, "nodes"):
        # A rationale node is a docstring or comment inside a symbol, never the start of the next
        # one; treating it as a boundary would cut every documented function off at its docstring.
        if data.get("file_type") == "rationale":
            continue
        line = parse_location(data.get("source_location"))
        symbols[data.get("source_file") or ""].append((line, data))
    for rows in symbols.values():
        rows.sort(key=lambda pair: (pair[0] is None, pair[0] or 0))
    return symbols


def collect_docstring_lines(graph_path, link_key):
    """Map symbol id -> source lines of its docstrings (rationale_for edges)."""
    rationale_line = {}
    for data in graph_items(graph_path, "nodes"):
        if data.get("file_type") == "rationale":
            rationale_line[data["id"]] = parse_location(data.get("source_location"))
    found = defaultdict(list)
    for data in graph_items(graph_path, link_key):
        if data.get("relation") == "rationale_for" and rationale_line.get(data["source"]):
            found[data["target"]].append(rationale_line[data["source"]])
    return found


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


def symbol_range(repo_root, data, symbols, line_cache, state_cache):
    """(lines, begin, end, start_line, mask) of an ordinary symbol, or None.

    ``begin`` is the first line of the comment block attached above the symbol
    (or the symbol's own line), ``end`` the last line before the next symbol's
    attached comment block. All 1-based and inclusive.
    """
    source_file = data.get("source_file")
    start_line = parse_location(data.get("source_location"))
    if start_line is None or not source_file:
        return None
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
    return lines, begin, end_line, start_line, mask


def body_for_node(repo_root, data, symbols, line_cache, state_cache, docstring_lines=None):
    """Full untruncated body for one node, or None when not applicable.

    For an ordinary symbol the body is its source with the comment-only lines and docstrings taken
    out; those are stored separately as ``comments`` (see ``comments_for_node``).
    """
    source_file = data.get("source_file")
    if node_is_page(data):
        lines = read_file_lines(repo_root, source_file, line_cache)
        if lines is None:
            return None
        return "\n".join(lines)
    if node_is_file_container(data) or node_is_filename_container(data):
        return None
    if parse_location(data.get("source_location")) is None or not source_file:
        return None
    if node_is_entrypoint(data):
        lines = read_file_lines(repo_root, source_file, line_cache)
        if lines is None:
            return None
        return "\n".join(lines)
    if data.get("file_type") == "rationale":
        return rationale_body(repo_root, data, line_cache, state_cache)
    found = symbol_range(repo_root, data, symbols, line_cache, state_cache)
    if found is None:
        return None
    lines, begin, end_line, start_line, mask = found
    dropped = set()
    for index in range(begin, end_line + 1):
        if mask[index - 1]:
            dropped.add(index)
    for doc_line in (docstring_lines or {}).get(data.get("id"), ()):
        span = docstring_span(lines, doc_line)
        if span and begin <= span[0] and span[1] <= end_line:
            dropped.update(range(span[0], span[1] + 1))
    body = "\n".join(lines[index - 1] for index in range(begin, end_line + 1) if index not in dropped)
    return body.strip("\n")


DOCSTRING_OPENER = re.compile(r"""^\s*[rRuUbBfF]{0,2}(\"\"\"|\'\'\')""")


def docstring_span(lines, line):
    """(first line, last line, inner text) of the triple-quoted string opening on 1-based ``line``."""
    if line is None or line < 1 or line > len(lines):
        return None
    match = DOCSTRING_OPENER.match(lines[line - 1])
    if not match:
        return None
    quote = match.group(1)
    rest = lines[line - 1][match.end():]
    if quote in rest:
        return line, line, rest[:rest.index(quote)].strip()
    parts = [rest]
    for offset, raw in enumerate(lines[line:line + 400], start=line + 1):
        if quote in raw:
            parts.append(raw[:raw.index(quote)])
            return line, offset, "\n".join(part.rstrip() for part in parts).strip()
        parts.append(raw)
    return None


def docstring_at(lines, line):
    """Text of the triple-quoted string that opens on 1-based ``line``, or None."""
    span = docstring_span(lines, line)
    return span[2] if span else None


def rationale_body(repo_root, data, line_cache, state_cache):
    """A rationale node is one docstring or comment block; its body is exactly that text."""
    start_line = parse_location(data.get("source_location"))
    lines = read_file_lines(repo_root, data.get("source_file"), line_cache)
    if start_line is None or lines is None or start_line > len(lines):
        return None
    span = docstring_span(lines, start_line)
    if span:
        return "\n".join(lines[span[0] - 1:span[1]])
    _style, mask = file_comment_state(repo_root, data["source_file"], lines, state_cache)
    end = start_line
    while mask[end - 1] and end < len(lines) and mask[end]:
        end += 1
    return "\n".join(lines[start_line - 1:end]) if mask[start_line - 1] else lines[start_line - 1]


def comments_for_node(repo_root, data, symbols, docstring_lines, line_cache, state_cache):
    """Comments of one symbol as text, separate from its ``body``.

    The comment block attached above the symbol, its Python docstring (Graphify
    records each as a rationale node at the docstring's first line), and every
    comment-only line inside the symbol's span, in source order. None when the
    symbol has no comments or is not an ordinary symbol.
    """
    if (node_is_page(data) or node_is_file_container(data) or node_is_filename_container(data)
            or node_is_entrypoint(data) or data.get("file_type") == "rationale"):
        return None
    found = symbol_range(repo_root, data, symbols, line_cache, state_cache)
    if found is None:
        return None
    lines, begin, end_line, start_line, mask = found
    pieces = [(index, lines[index - 1]) for index in range(begin, end_line + 1) if mask[index - 1]]
    docstrings = []
    for doc_line in docstring_lines.get(data.get("id"), ()):
        text = docstring_at(lines, doc_line)
        if text:
            docstrings.append((doc_line, text))
    merged = sorted([(i, t) for i, t in pieces] + docstrings, key=lambda pair: pair[0])
    text = "\n".join(part for _, part in merged).strip()
    return text or None


def git_file_times(repo_root):
    """Map path (relative to repo_root) -> last commit time, from git history.

    In CI every file's mtime is the checkout time, so mtimes say nothing about
    when code changed. One ``git log`` pass gives the real last-commit time per
    file. Shallow clones only see HEAD, which would stamp every file with the
    same time, so they are skipped (checkout with ``fetch-depth: 0`` to enable).
    """
    def git(*args):
        return subprocess.run(
            ["git", "-C", str(repo_root), *args],
            capture_output=True, text=True, check=True,
        ).stdout
    try:
        if git("rev-parse", "--is-shallow-repository").strip() == "true":
            return {}
        log = git("log", "--relative", "--no-renames", "--format=%x00%ct", "--name-only")
    except (OSError, subprocess.CalledProcessError):
        return {}
    times, current = {}, None
    for line in log.splitlines():
        if line.startswith("\x00"):
            current = float(line[1:])
        elif line and current is not None:
            times.setdefault(line, current)
    return times


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


def updated_at_for(data, repo_root, file_times):
    """Best-effort last-change time for a node's source file.

    Prefers git commit time, then graphify's manifest / filesystem mtime, and
    falls back to the current time. ``updated_at`` is excluded from the
    content hash, so it never forces a rewrite on its own.
    """
    source_file = data.get("source_file")
    mtime = file_times.get(source_file) if source_file else None
    if mtime is None and source_file:
        try:
            mtime = (Path(repo_root) / source_file).stat().st_mtime
        except OSError:
            mtime = None
    return mtime if mtime is not None else time.time()


def ensure_database(driver, database):
    """Create the target database when it does not exist yet."""
    with driver.session() as session:
        existing = set()
        try:
            existing = {row.get("name") for row in session.run("SHOW DATABASES")}
        except Exception:
            existing = set()
        if database not in existing:
            session.run(f"CREATE DATABASE `{database}` IF NOT EXISTS").consume()


def friendly_error(error, uri):
    """One readable line for a failed call to NornicDB (no traceback to scroll through)."""
    text = str(error)
    if "Unauthorized" in text or "HTTP 401" in text or "HTTP 403" in text or "AuthError" in type(error).__name__:
        return (f"error: NornicDB at {uri} rejected the credentials. Check the username and password "
                f"(the nornicdb-user / nornicdb-password inputs).")
    if isinstance(error, (RetryableError, OSError)) or "ServiceUnavailable" in type(error).__name__:
        return f"error: cannot reach NornicDB at {uri}: {text}"
    return f"error: NornicDB at {uri} returned an error: {text}"


def last_ingested_state(uri, user, password, database, repo):
    """{'commit', 'main_spec'} recorded by the previous successful run, or None."""
    try:
        with connect(uri, user, password) as driver:
            with driver.session(database=database) as session:
                record = session.run(
                    "MATCH (r:CodeRepository {name: $repo}) "
                    "RETURN r.last_commit AS commit, r.main_spec AS main_spec",
                    repo=repo,
                ).single()
                return dict(record) if record else None
    except SystemExit:
        raise
    except Exception as error:
        # A database that does not exist yet is the normal first run. Anything else (bad credentials,
        # unreachable server, server error) would only fail later, after a long extraction, so fail now.
        if "DatabaseNotFound" in str(error):
            print(f"Database {database!r} does not exist yet; this will be the first ingest", file=sys.stderr)
            return None
        raise SystemExit(friendly_error(error, uri)) from error


def import_graph(graph_path, uri, user, password, batch_size, repo_root, database, repo,
                 commit=None, branch=None, sync=True, max_delete_fraction=0.5, main=None, main_spec="", skip_unchanged=True):
    labels = {}
    node_batches = defaultdict(list)
    edge_batches = defaultdict(list)
    indexed_labels = set()
    node_count = edge_count = bodied_count = skipped_count = 0
    updated_nodes = 0
    unchanged_nodes = unchanged_edges = 0  # already in the database with the same content hash
    symbols = collect_symbol_lines(graph_path)
    # Graphify >=0.9.69 exports links; older graphs used edges. Probe
    # the links key first and fall back to edges when it is absent.
    link_key = "links"
    if next(graph_items(graph_path, "links"), None) is None:
        link_key = "edges"
    docstring_lines = collect_docstring_lines(graph_path, link_key)
    communities = Communities()
    line_cache = {}
    state_cache = {}
    file_times = {**manifest_mtimes(graph_path), **git_file_times(repo_root)}
    max_body_bytes = HTTP_MAX_BODY_BYTES if is_http(uri) else None
    retry_on = retryable_errors()

    with connect(uri, user, password) as driver:
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
                    except retry_on:
                        if attempt == 11:
                            raise
                        time.sleep(delay)
                        delay = min(delay * 2, 1.0)

            def ensure_index(label):
                if label in indexed_labels:
                    return
                # Nodes are keyed by (id, repo): id alone is not unique across
                # repos in the shared database. Index the composite key so the
                # {id, repo} MERGE/MATCH patterns in write_nodes and write_edges
                # resolve through the composite equality path as an exact
                # LookupFull instead of an id lookup plus a residual repo
                # filter. A bare repo index is never created: it has the same
                # value for every node in a repo, so it would materialise the
                # whole label on every batch.
                run_retry(
                    f"CREATE INDEX graphify_{label.lower()}_id_repo IF NOT EXISTS FOR (n:{label}) ON (n.id, n.repo)"
                ).consume()
                indexed_labels.add(label)

            def write_nodes(label, rows):
                nonlocal updated_nodes
                ensure_index(label)
                # Create missing nodes with their full properties.
                run_retry(
                    f"UNWIND $rows AS row MERGE (n:{label} {{id: row.id, repo: $repo}}) "
                    f"ON CREATE SET n += row.props",
                    rows=rows, repo=repo,
                ).consume()
                # Gentle update: rewrite only nodes whose content hash
                # differs from the artifact copy. Unchanged nodes are never
                # touched, so the embedding worker has nothing to re-embed
                # on repeat runs.
                result = run_retry(
                    f"UNWIND $rows AS row MATCH (n:{label} {{id: row.id, repo: $repo}}) "
                    f"WHERE n.props_hash IS NULL OR n.props_hash <> row.props.props_hash "
                    f"SET n += row.props RETURN count(n) AS updated",
                    rows=rows, repo=repo,
                )
                # Over HTTP an oversized batch is split, yielding one count per part.
                updated_nodes += sum(record.get("updated", 0) for record in result)
                rows.clear()

            def write_edges(source_label, target_label, relation, rows):
                # Edges carry no embeddings, so an unconditional SET has no
                # re-embedding cost (unlike nodes, which stay hash-guarded).
                run_retry(
                    f"UNWIND $rows AS row "
                    f"MATCH (a:{source_label} {{id: row.src, repo: $repo}}), "
                    f"(b:{target_label} {{id: row.tgt, repo: $repo}}) "
                    f"MERGE (a)-[r:{relation}]->(b) SET r += row.props",
                    rows=rows, repo=repo,
                ).consume()
                rows.clear()

            # What this importer already wrote for the repo: id -> content hash. A node whose hash is
            # unchanged is neither sent nor touched, so a re-run (or one that resumes an interrupted
            # run) moves only what is new or different instead of re-sending the whole graph.
            existing_nodes = {}
            if skip_unchanged:
                for record in run_retry(
                    "MATCH (n) WHERE n.repo = $repo AND n.props_hash IS NOT NULL "
                    "RETURN n.id AS id, n.props_hash AS hash", repo=repo,
                ):
                    if record.get("id") is not None:
                        existing_nodes[record["id"]] = record.get("hash")

            def add_implicit_node(node_id):
                nonlocal unchanged_nodes
                labels[node_id] = "Entity"
                rows = node_batches["Entity"]
                props = {"id": node_id, "repo": repo, "symbol_kind": "external", "updated_at": time.time()}
                props["props_hash"] = content_hash(props)
                if skip_unchanged and existing_nodes.get(node_id) == props["props_hash"]:
                    unchanged_nodes += 1
                    return
                rows.append({"id": node_id, "props": props})
                if len(rows) >= batch_size:
                    write_nodes("Entity", rows)

            for data in graph_items(graph_path, "nodes"):
                label = node_label(data)
                node_id = data["id"]
                labels[node_id] = label
                # Never put per-run values (commit SHA, run id) on nodes: they
                # would change props_hash and re-embed every node every run.
                props = {
                    **scalar_properties(data),
                    "id": node_id,
                    "repo": repo,
                    "symbol_kind": symbol_kind(data),
                    "updated_at": updated_at_for(data, repo_root, file_times),
                }
                # Community membership is modelled as Community nodes + IN_COMMUNITY edges, not as
                # properties: a re-clustering renumbers and renames communities, which would rewrite
                # and re-embed every member node.
                communities.add(node_id, label, props.pop("community", None), props.pop("community_name", None))
                body = body_for_node(repo_root, data, symbols, line_cache, state_cache, docstring_lines)
                if body and max_body_bytes and len(body.encode("utf-8")) > max_body_bytes:
                    print(f"Skipping body of {node_id} ({data.get('source_file')}): "
                          f"larger than the HTTP request limit", file=sys.stderr)
                    body = None
                if body:
                    props["body"] = body
                    props["body_start_line"] = parse_location(data.get("source_location")) or 0
                    bodied_count += 1
                    comments = comments_for_node(repo_root, data, symbols, docstring_lines, line_cache, state_cache)
                    if comments:
                        props["comments"] = comments
                else:
                    skipped_count += 1
                props["props_hash"] = content_hash(props)
                node_count += 1
                if skip_unchanged and existing_nodes.get(node_id) == props["props_hash"]:
                    unchanged_nodes += 1
                    continue
                rows = node_batches[label]
                rows.append({"id": node_id, "props": props})
                if len(rows) >= batch_size:
                    write_nodes(label, rows)

            for data in graph_items(graph_path, link_key):
                for node_id in (data["source"], data["target"]):
                    if node_id not in labels:
                        add_implicit_node(node_id)
                        node_count += 1

            for label, rows in node_batches.items():
                if rows:
                    write_nodes(label, rows)

            if communities.members:
                if communities.inconsistent():
                    print(f"Warning: {communities.inconsistent()} communities carry more than one name "
                          f"(a graph re-clustered without re-labelling); using the most common. "
                          f"Run `graphify cluster-only` to relabel.", file=sys.stderr)
                if communities.unnamed():
                    print(f"Note: {communities.unnamed()} of {len(communities.sizes)} communities have no name.",
                          flush=True)
                community_rows = node_batches["Community"]
                for community in communities.ids():
                    node_id = f"community:{community}"
                    labels[node_id] = "Community"
                    name = communities.name(community)
                    size = communities.sizes[community]
                    props = {
                        "id": node_id, "repo": repo, "symbol_kind": "community",
                        "community": community, "name": name or f"Community {community}",
                        "summary": f"Community {community}: {name or 'unnamed'} ({size} members)",
                        "size": size, "updated_at": time.time(),
                    }
                    props["props_hash"] = content_hash(props)
                    node_count += 1
                    if skip_unchanged and existing_nodes.get(node_id) == props["props_hash"]:
                        unchanged_nodes += 1
                        continue
                    community_rows.append({"id": node_id, "props": props})
                    if len(community_rows) >= batch_size:
                        write_nodes("Community", community_rows)
                if community_rows:
                    write_nodes("Community", community_rows)

            print(f"Imported {node_count} nodes "
                  f"({bodied_count} with full source bodies, {skipped_count} without)", flush=True)

            # The main entry point carries a second label, :Main, so a database
            # (one per repository by default) can fetch it directly with
            # MATCH (n:Main). Exactly one node per repo holds it. Labels are not
            # part of props_hash, so tagging never re-embeds anything. It happens as soon as
            # the nodes exist (before the long edge phase), so a database has its main
            # during a big first ingest and after a run that fails later.
            run_retry(
                "MATCH (n:Main {repo: $repo}) WHERE n.id <> $id REMOVE n:Main",
                repo=repo, id=main["id"] if main else "",
            ).consume()
            if main:
                run_retry(
                    f"MATCH (n:{main['db_label']} {{id: $id, repo: $repo}}) SET n:Main",
                    id=main["id"], repo=repo,
                ).consume()

            existing_edges_hash = {}
            if skip_unchanged:
                for record in run_retry(
                    "MATCH (a)-[r]->(b) WHERE a.repo = $repo AND b.repo = $repo "
                    "RETURN a.id AS src, b.id AS tgt, type(r) AS rel, r.props_hash AS hash", repo=repo,
                ):
                    if record.get("src") is not None:
                        existing_edges_hash[(record["src"], record["tgt"], record["rel"])] = record.get("hash")
            incoming_edges = set()
            for data in graph_items(graph_path, link_key):
                source = data["source"]
                target = data["target"]
                source_label, target_label = labels[source], labels[target]
                relation = relationship_type(data)
                incoming_edges.add((source, target, relation))
                edge_props = scalar_properties(data, edge=True)
                edge_props["props_hash"] = content_hash(edge_props)
                edge_count += 1
                if skip_unchanged and existing_edges_hash.get((source, target, relation)) == edge_props["props_hash"]:
                    unchanged_edges += 1
                else:
                    rows = edge_batches[(source_label, target_label, relation)]
                    rows.append({"src": source, "tgt": target, "props": edge_props})
                    if len(rows) >= batch_size:
                        write_edges(source_label, target_label, relation, rows)
                if edge_count % 10000 == 0:
                    print(f"Imported {edge_count} edges", flush=True)

            member_props = {}
            member_props["props_hash"] = content_hash(member_props)
            for node_id, node_db_label, community in communities.members:
                community_id = f"community:{community}"
                incoming_edges.add((node_id, community_id, "IN_COMMUNITY"))
                edge_count += 1
                if skip_unchanged and existing_edges_hash.get((node_id, community_id, "IN_COMMUNITY")) == member_props["props_hash"]:
                    unchanged_edges += 1
                    continue
                rows = edge_batches[(node_db_label, "Community", "IN_COMMUNITY")]
                rows.append({"src": node_id, "tgt": community_id, "props": member_props})
                if len(rows) >= batch_size:
                    write_edges(node_db_label, "Community", "IN_COMMUNITY", rows)

            for (source_label, target_label, relation), rows in edge_batches.items():
                if rows:
                    write_edges(source_label, target_label, relation, rows)

            # Incremental sync: delete stale edges and nodes that no longer
            # appear in the artifact. Only nodes this importer wrote
            # (props_hash is set on every one) in this repo and its managed
            # labels are candidates, so other repositories, hand-made nodes and
            # data from other pipelines are never touched.
            stale_ids, stale_edges = [], []
            if sync and labels:
                # Every lookup below names its label, so it uses the (id) and (repo) indexes. An unlabelled
                # `MATCH (n {id: ...})` is a scan of every node per row, which turned deleting a few
                # thousand stale edges into minutes.
                managed_labels = sorted(set(labels.values()))
                existing_labels = {}  # id -> db label, for ids the importer wrote in this repo
                for label in managed_labels:
                    for record in run_retry(
                        f"MATCH (n:{label} {{repo: $repo}}) WHERE n.id IS NOT NULL AND n.props_hash IS NOT NULL "
                        f"RETURN n.id AS id",
                        repo=repo,
                    ):
                        existing_labels[record.get("id")] = label
                existing_ids = set(existing_labels)
                stale_ids = [node_id for node_id in existing_ids if node_id not in labels]
                existing_edges = set()
                for label in managed_labels:
                    for record in run_retry(
                        f"MATCH (a:{label} {{repo: $repo}})-[r]->(b) WHERE b.repo = $repo "
                        f"AND a.props_hash IS NOT NULL AND b.props_hash IS NOT NULL "
                        f"RETURN a.id AS src, b.id AS tgt, type(r) AS rel",
                        repo=repo,
                    ):
                        existing_edges.add((record.get("src"), record.get("tgt"), record.get("rel")))
                stale_edges = [edge for edge in existing_edges if edge not in incoming_edges]

                # A truncated or failed extraction would otherwise delete most
                # of a repository's graph. Refuse before deleting anything; the
                # run fails, last_commit is not advanced, and the next push retries.
                for what, stale, existing in (("nodes", stale_ids, existing_ids),
                                              ("edges", stale_edges, existing_edges)):
                    if (len(existing) >= MASS_DELETE_MIN_EXISTING
                            and len(stale) > max_delete_fraction * len(existing)):
                        raise SystemExit(
                            f"Refusing to sync {repo}: {len(stale)} of {len(existing)} existing {what} "
                            f"({len(stale) / len(existing):.0%}) are missing from this graph, over the "
                            f"--max-delete-fraction limit of {max_delete_fraction:.0%}. Check that the "
                            f"extraction was complete; rerun with --max-delete-fraction 1 to accept it."
                        )

                # An edge touching a stale node goes with the node (DETACH DELETE); the rest are deleted
                # per (source label, target label, relation), the same grouping they were written in.
                stale_id_set = set(stale_ids)
                by_shape = defaultdict(list)
                for src, tgt, rel in stale_edges:
                    if src in stale_id_set or tgt in stale_id_set:
                        continue
                    by_shape[(existing_labels[src], existing_labels.get(tgt, labels.get(tgt)), rel)].append(
                        {"src": src, "tgt": tgt})
                for (source_label, target_label, rel), rows in by_shape.items():
                    if target_label is None:
                        continue
                    for batch in (rows[i:i + batch_size] for i in range(0, len(rows), batch_size)):
                        run_retry(
                            f"UNWIND $rows AS row "
                            f"MATCH (a:{source_label} {{id: row.src, repo: $repo}})-[r:{rel}]->"
                            f"(b:{target_label} {{id: row.tgt, repo: $repo}}) DELETE r",
                            rows=batch, repo=repo,
                        ).consume()
                stale_by_label = defaultdict(list)
                for node_id in stale_ids:
                    stale_by_label[existing_labels[node_id]].append(node_id)
                for label, ids in stale_by_label.items():
                    for batch in (ids[i:i + batch_size] for i in range(0, len(ids), batch_size)):
                        run_retry(
                            f"UNWIND $ids AS id MATCH (n:{label} {{id: id, repo: $repo}}) "
                            f"WHERE n.props_hash IS NOT NULL DETACH DELETE n",
                            ids=batch, repo=repo,
                        ).consume()
                if stale_ids or stale_edges:
                    print(
                        f"Sync: removed {len(stale_ids)} stale nodes and "
                        f"{len(stale_edges)} stale edges", flush=True,
                    )

            # Recorded last, so a failed run is retried by the next --check.
            run_retry(
                "MERGE (r:CodeRepository {name: $repo}) "
                "SET r.last_commit = $commit, r.branch = $branch, "
                "r.last_ingested_at = $now, r.node_count = $nodes, r.edge_count = $edges, "
                "r.main_id = $main_id, r.main_label = $main_label, "
                "r.main_source_file = $main_file, r.main_spec = $main_spec",
                repo=repo, commit=commit, branch=branch, now=time.time(),
                nodes=node_count, edges=len(incoming_edges),
                main_id=main["id"] if main else "", main_label=main["label"] if main else "",
                main_file=main["source_file"] if main else "", main_spec=main_spec or "",
            ).consume()

            print(
                f"Updated {updated_nodes} existing nodes; "
                f"everything else was left untouched", flush=True,
            )
            if skip_unchanged:
                print(f"Skipped {unchanged_nodes} nodes and {unchanged_edges} edges already in the database "
                      f"with identical content", flush=True)

    print(f"Imported {node_count} nodes and {edge_count} edges for {repo} into {database}")


DEFAULT_DATABASE = "nornicdbcode"
DEFAULT_REPO = "orneryd/NornicDB"
DEFAULT_MAIN = "cmd/nornicdb/main.go:main"


def git_output(repo_root, *args):
    try:
        return subprocess.run(["git", "-C", str(repo_root), *args],
                              capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def repo_from_git(repo_root):
    """owner/name from the origin remote, or the project default."""
    match = re.search(r"([^/:]+/[^/]+?)(?:\.git)?$", git_output(repo_root, "remote", "get-url", "origin"))
    return match.group(1) if match else DEFAULT_REPO


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
    link_key = "links" if "links" in data else "edges"
    docstring_lines = collect_docstring_lines(graph_path, link_key)
    line_cache, state_cache = {}, {}
    file_times = {**manifest_mtimes(graph_path), **git_file_times(repo_root)}
    bodied = 0
    for node in data.get("nodes", []):
        body = body_for_node(repo_root, node, symbols, line_cache, state_cache, docstring_lines)
        if body:
            node["body"] = body
            bodied += 1
            comments = comments_for_node(repo_root, node, symbols, docstring_lines, line_cache, state_cache)
            if comments:
                node["comments"] = comments
        node["updated_at"] = updated_at_for(node, repo_root, file_times)
    with out_path.open("w", encoding="utf-8") as out_file:
        json.dump(data, out_file, ensure_ascii=False)
    print(
        f"Wrote enriched graph to {out_path} "
        f"({len(data.get('nodes', []))} nodes, {bodied} with bodies)"
    )


def write_github_output(name, value):
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            handle.write(f"{name}={value}\n")


def main():
    env = os.environ.get
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--graph", type=Path, default=Path("graphify-out/graph.json"))
    parser.add_argument("--uri", default=env("NORNICDB_URI", "http://localhost:7474"),
                        help="https://host (HTTP API) or bolt://host:7687 "
                             "(env NORNICDB_URI; default http://localhost:7474)")
    parser.add_argument("--user", default=env("NORNICDB_USER", "admin"),
                        help="user (env NORNICDB_USER; default admin)")
    parser.add_argument("--password", default=env("NORNICDB_PASSWORD") or env("NEO4J_PASSWORD") or "password",
                        help="password (env NORNICDB_PASSWORD or NEO4J_PASSWORD; default 'password')")
    parser.add_argument("--database", default=env("NORNICDB_DATABASE") or DEFAULT_DATABASE,
                        help=f"target database, created if missing (env NORNICDB_DATABASE; default {DEFAULT_DATABASE})")
    parser.add_argument("--repo", default=env("CODE_INTEL_REPO") or env("GITHUB_REPOSITORY"),
                        help="repository name stamped on every node (default: from the git origin remote)")
    parser.add_argument("--commit", default=env("GITHUB_SHA"),
                        help="commit being ingested (default: HEAD of --repo-root)")
    parser.add_argument("--branch", default=env("GITHUB_REF_NAME"),
                        help="branch being ingested (default: the branch checked out in --repo-root)")
    parser.add_argument("--repo-root", default=".",
                        help="root for resolving graph source_file paths (default: .)")
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--max-delete-fraction", type=float, default=0.5,
                        help="refuse to sync if more than this fraction of the repo's existing nodes (or edges) "
                             "would be deleted, which usually means a truncated extraction (default 0.5; 1 disables)")
    parser.add_argument("--main", default=env("CODE_INTEL_MAIN") or DEFAULT_MAIN, metavar="SYMBOL",
                        help="the repository's main entry point, tagged :Main in the database. Identify it as "
                             "Graphify does: a node id, path/to/file:symbol, or a unique symbol name "
                             "(main, main(), .run()). An empty value picks the non-test function defined in "
                             f"the code with the most connected nodes; 'none' ingests without one "
                             f"(env CODE_INTEL_MAIN; default {DEFAULT_MAIN})")
    parser.add_argument("--out-graph", type=Path, default=None,
                        help="write an enriched graph.json (bodies embedded) to this path and skip ingestion")
    parser.add_argument("--rewrite-all", action="store_true",
                        help="send every node and edge even when the database already has it with the same "
                             "content hash (default: skip those, which also resumes an interrupted run)")
    parser.add_argument("--no-sync", action="store_true",
                        help="skip deleting stale nodes and edges that disappeared from the graph")
    parser.add_argument("--check", action="store_true",
                        help="only report whether --commit differs from the last ingested commit; "
                             "prints and writes needed=true|false to $GITHUB_OUTPUT")
    parser.add_argument("--force", action="store_true",
                        help="with --check, always report needed=true")
    args = parser.parse_args()
    if not args.repo:
        args.repo = repo_from_git(args.repo_root)
    if not args.commit:
        args.commit = git_output(args.repo_root, "rev-parse", "HEAD") or None
    if not args.branch:
        args.branch = git_output(args.repo_root, "rev-parse", "--abbrev-ref", "HEAD") or None
    if ":" in args.database or args.database.startswith("_") or not args.database.strip():
        parser.error(f"invalid database name {args.database!r}: it cannot be empty, contain ':' or start with '_'")
    if not 0 <= args.max_delete_fraction <= 1:
        parser.error("--max-delete-fraction must be between 0 and 1")
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")

    if args.check:
        state = None if args.force else last_ingested_state(
            args.uri, args.user, args.password, args.database, args.repo)
        previous = state.get("commit") if state else None
        # Changing which function is main re-runs the ingest even on the same commit.
        def normalized(spec):
            spec = (spec or "").strip()
            return NO_MAIN if spec.lower() == NO_MAIN else spec
        main_changed = bool(state) and normalized(state.get("main_spec")) != normalized(args.main)
        needed = args.force or not args.commit or previous != args.commit or main_changed
        print(f"{args.repo}: last ingested {previous or 'never'}, current {args.commit or 'unknown'}"
              f"{', main changed' if main_changed and previous == args.commit else ''} "
              f"-> needed={str(needed).lower()}")
        write_github_output("needed", str(needed).lower())
        return

    if not args.graph.is_file():
        parser.error(f"graph not found: {args.graph}")
    if args.out_graph is not None:
        emit_enriched_graph(args.graph, args.out_graph, args.repo_root)
        return
    main = None
    if args.main.strip().lower() != NO_MAIN:
        try:
            main = choose_main(args.graph, args.main)
        except MainSelectionError as error:
            parser.exit(2, f"graphify_local.py: error: {error}\n")
        print(f"Main entry point ({main['how']}): {main['label']}  {main['source_file']}:{main['source_location']}"
              f"  [{main['kind']}, {main['connected']} connected nodes]  id={main['id']}", flush=True)
        if main["how"] == "default":
            print("  No main was specified, so this is the best-connected function in the code. Set "
                  "--main to pick the real entry point. Next best:", flush=True)
            for line in main["runner_ups"]:
                print("    " + line, flush=True)
    try:
        import_graph(args.graph, args.uri, args.user, args.password, args.batch_size,
                     args.repo_root, args.database, args.repo,
                     commit=args.commit, branch=args.branch, sync=not args.no_sync,
                     max_delete_fraction=args.max_delete_fraction, main=main,
                     main_spec=args.main.strip() if main else NO_MAIN,
                     skip_unchanged=not args.rewrite_all)
    except (RuntimeError, RetryableError, OSError) as error:
        raise SystemExit(friendly_error(error, args.uri)) from error


if __name__ == "__main__":
    main()
