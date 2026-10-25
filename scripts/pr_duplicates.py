#!/usr/bin/env python3
"""Find semantic duplicates of the code a pull request adds, using the NornicDB code graph.

For every function or method the PR adds or mostly rewrites, the script asks NornicDB's hybrid
search (``POST /nornicdb/search``) for similar existing code and keeps the results that really are
duplicates, on one of two bases:

* **code**: the bodies overlap (token, normalized-structure or receiver-insensitive similarity at
  least ``min_similarity``): a copy, possibly renamed;
* **behavior**: the bodies differ but the embeddings say they do the same thing. The search reports
  each vector hit's chunk similarity to the query; real duplicates and merely related code overlap
  in it (Titan V2: 0.26 to 0.72 for duplicates, up to 0.77 for unrelated code), so no absolute cut
  separates them. A hit counts when it is the query's
  top hit by similarity, stands ``semantic_min_gap`` above the median of the query's hits and
  ``semantic_min_lead`` ahead of the next one (calibrated on soraban-connect, see design.md).

Each added function is then classified:

* no match: nothing is reported;
* one existing match: a **reuse candidate** (job summary);
* two or more distinct existing matches: a **refactor target** (sticky PR comment).

Code the PR removes is retired: it never counts as a match, and a function moved between files is
recognised as a move. Everything goes over NornicDB's HTTP API (Bolt is not used), read-only, and
a failure to reach or use NornicDB is a warning, never a failed check, unless ``--fail-on-findings``
asks for the latter.

The head graph comes from ``graphify extract`` on the PR checkout (see ``pr-review/action.yml``).
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from collections import deque
import fnmatch
import json
from http.client import IncompleteRead
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import graphify_local as gi  # noqa: E402

COMMENT_MARKER = "<!-- code-intelligence:duplicates -->"

# Defaults, calibrated on this repository and soraban-api (see openspec/changes/pr-duplicate-detection/design.md).
DEFAULTS = {
    "min_body_lines": 4,
    "min_tokens": 40,
    "min_added_fraction": 0.5,
    "max_query_chars": 2000,
    "search_limit": 25,
    "min_similarity": 0.8,
    "max_matches": 5,
    "max_candidates": 40,
    "max_findings": 10,
    "deadline_seconds": 300,
    # Behavior matches (embedding similarity relative to the query's other hits); 0 disables.
    "semantic": 1,
    "semantic_min_gap": 0.15,
    "semantic_min_lead": 0.05,
    "semantic_min_hits": 5,
}


# Generated or boilerplate trees whose functions look alike by construction (every migration has a `change`).
DEFAULT_IGNORE = ("scripts/*", "db/migrate/*", "*/migrations/*", "vendor/*", "*/node_modules/*", "node_modules/*", "*.min.js",
                  # ANTLR outputs only; handwritten adapters, tests and grammar stay covered.
                  "pkg/cypher/antlr/cypher_lexer.go", "pkg/cypher/antlr/cypher_parser.go",
                  "pkg/cypher/antlr/cypherparser_listener.go",
                  "pkg/cypher/antlr/cypherparser_base_listener.go")


def ignored(path, patterns):
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


SEARCH_ATTEMPTS = 5


class AnalysisError(Exception):
    """NornicDB could not be reached or used. Reported as a warning, not a failure."""

    def __init__(self, message, kind="server"):
        super().__init__(message)
        self.kind = kind


# --------------------------------------------------------------------------------------
# Diff
# --------------------------------------------------------------------------------------

HUNK = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")
FUNCTION_DECLARATION = re.compile(
    r"^\s*(?:(?:async|export|public|private|static)\s+)*"
    r"(?:def|func|function|fn)\s+(?:\([^)]*\)\s*)?([A-Za-z_]\w*)\s*(?:\[[^]]*\])?\s*\("
)


@dataclass
class FileDiff:
    added: set = field(default_factory=set)      # new-file line numbers
    removed: set = field(default_factory=set)    # old-file line numbers
    removed_definitions: set = field(default_factory=set)
    deleted: bool = False
    created: bool = False


def _diff_path(raw):
    raw = raw.split("\t")[0].strip()
    if raw.startswith('"') and raw.endswith('"'):
        raw = raw[1:-1].encode().decode("unicode_escape")
    return raw


def parse_diff(text):
    """Parse ``git diff --unified=0 --no-renames`` into {path: FileDiff}."""
    files = {}
    old_path = new_path = None
    current = None
    binary = False
    in_hunk = False
    for line in text.splitlines():
        if line.startswith("diff --git "):
            old_path = new_path = current = None
            binary = False
            in_hunk = False
        elif line.startswith("Binary files ") or line.startswith("GIT binary patch"):
            binary = True
            current = None
        elif line.startswith("--- "):
            old_path = None if line[4:].startswith("/dev/null") else _diff_path(line[4:]).removeprefix("a/")
        elif line.startswith("+++ "):
            new_path = None if line[4:].startswith("/dev/null") else _diff_path(line[4:]).removeprefix("b/")
            if binary:
                continue
            path = new_path or old_path
            current = files.setdefault(path, FileDiff())
            current.deleted = new_path is None
            current.created = old_path is None
        elif line.startswith("@@") and current is not None:
            match = HUNK.match(line)
            if not match:
                continue
            in_hunk = True
            old_start, old_count, new_start, new_count = match.groups()
            old_count = 1 if old_count is None else int(old_count)
            new_count = 1 if new_count is None else int(new_count)
            current.removed.update(range(int(old_start), int(old_start) + old_count))
            current.added.update(range(int(new_start), int(new_start) + new_count))
        elif in_hunk and current is not None and line.startswith("-"):
            declaration = FUNCTION_DECLARATION.match(line[1:])
            if declaration:
                current.removed_definitions.add(declaration.group(1))
    return files


def git_diff(repo_root, base, head):
    out = subprocess.run(
        ["git", "-C", str(repo_root), "diff", "--unified=0", "--no-renames", "--no-color", f"{base}...{head}"],
        capture_output=True, text=True, check=False)
    if out.returncode != 0:
        raise AnalysisError(f"git diff {base}...{head} failed: {out.stderr.strip()[:300]}", "git")
    return out.stdout


# --------------------------------------------------------------------------------------
# Similarity
# --------------------------------------------------------------------------------------

TOKEN = re.compile(
    r"[A-Za-z_][A-Za-z0-9_]*[?!]?|\d+(?:\.\d+)?|\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'"
    r"|==|!=|<=|>=|=>|->|&&|\|\||\*\*|::|[^\sA-Za-z0-9_]")
KEYWORDS = frozenset("""
def class end if elif else unless then do while until for in return yield begin rescue ensure raise
try except finally catch throw with as lambda proc fn func function async await const let var val
self this nil null none true false and or not is new import from require include extend module case
when switch default break continue pass else_if elsif private public protected static
""".split())


def tokens(text):
    return TOKEN.findall(text)


def normalized_tokens(text):
    """Identifiers become ID and literals LIT, so renamed copies of the same logic still line up."""
    out = []
    for token in tokens(text):
        if token[0].isalpha() or token[0] == "_":
            out.append(token if token in KEYWORDS else "ID")
        elif token[0].isdigit() or token[0] in "\"'":
            out.append("LIT")
        else:
            out.append(token)
    return out


def shingles(seq, size):
    if len(seq) < size:
        return {tuple(seq)} if seq else set()
    return {tuple(seq[i:i + size]) for i in range(len(seq) - size + 1)}


def jaccard(left, right):
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


# Receiver access: `self.x` / `self._x` / `cls.x` / `this.x` and Ruby's `@x`. Dropping it lets a
# method that reads `self._session` line up with the same logic rewritten as a function taking
# `session` (or a Ruby method using `@items` with one using a local `items`).
RECEIVER = re.compile(r"\b(?:self|cls|this)\.(?:_(?=[A-Za-z]))?|(?<![\w@])@(?=[A-Za-z_])")


def similarity(left, right):
    """{'lexical', 'structural', 'receiverless', 'score'}: token 3-gram overlap, normalized 5-gram
    overlap, the same with receiver access dropped, and the largest.

    The receiverless reading is an addition, not a replacement: some copies line up only through
    their shared ``self`` tokens, and they keep their structural score."""
    lexical = jaccard(shingles(tokens(left), 3), shingles(tokens(right), 3))
    structural = jaccard(shingles(normalized_tokens(left), 5), shingles(normalized_tokens(right), 5))
    receiverless = jaccard(shingles(normalized_tokens(RECEIVER.sub("", left)), 5),
                           shingles(normalized_tokens(RECEIVER.sub("", right)), 5))
    return {"lexical": lexical, "structural": structural, "receiverless": receiverless,
            "score": max(lexical, structural, receiverless)}


# --------------------------------------------------------------------------------------
# Candidates: symbols the PR adds or mostly rewrites
# --------------------------------------------------------------------------------------

@dataclass
class Candidate:
    id: str
    label: str
    source_file: str
    line: int
    body: str
    comments: str
    code_lines: int
    added_fraction: float
    test: bool

    @property
    def query(self):
        return self.body


def _code_line_numbers(repo_root, data, symbols, docstrings, line_cache, state_cache):
    found = gi.symbol_range(repo_root, data, symbols, line_cache, state_cache)
    if found is None:
        return None
    lines, begin, end_line, start_line, mask = found
    dropped = {i for i in range(begin, end_line + 1) if mask[i - 1]}
    for doc_line in docstrings.get(data.get("id"), ()):
        span = gi.docstring_span(lines, doc_line)
        if span and begin <= span[0] and span[1] <= end_line:
            dropped.update(range(span[0], span[1] + 1))
    return [i for i in range(start_line, end_line + 1) if i not in dropped and lines[i - 1].strip()]


def select_candidates(graph_path, repo_root, diff, *, min_body_lines, min_added_fraction,
                      max_query_chars, min_tokens=0, ignore=()):
    """Functions and methods of the head graph whose body is mostly added lines, largest first."""
    link_key = gi.graph_link_key(graph_path)
    symbols = gi.collect_symbol_lines(graph_path)
    docstrings = gi.collect_docstring_lines(graph_path, link_key)
    line_cache, state_cache = {}, {}
    candidates = []
    for data in gi.graph_items(graph_path, "nodes"):
        path = data.get("source_file")
        file_diff = diff.get(path)
        if not file_diff or not file_diff.added or gi.symbol_kind(data) != "function":
            continue
        if ignored(path, ignore):
            continue
        numbers = _code_line_numbers(repo_root, data, symbols, docstrings, line_cache, state_cache)
        if not numbers:
            continue
        fraction = sum(1 for n in numbers if n in file_diff.added) / len(numbers)
        if fraction <= 0:
            continue
        if len(numbers) < min_body_lines or fraction < min_added_fraction:
            continue
        body = gi.body_for_node(repo_root, data, symbols, line_cache, state_cache, docstrings)
        if not body or len(tokens(body)) < min_tokens:
            continue
        comments = gi.comments_for_node(repo_root, data, symbols, docstrings, line_cache, state_cache) or ""
        candidates.append(Candidate(
            id=data["id"], label=data.get("label", ""), source_file=path,
            line=gi.parse_location(data.get("source_location")) or 1,
            body=body[:max_query_chars], comments=comments, code_lines=len(numbers),
            added_fraction=fraction, test=gi.is_test_source(path)))
    candidates.sort(key=lambda c: (-c.code_lines, c.source_file, c.line))
    return candidates


# --------------------------------------------------------------------------------------
# NornicDB over HTTP
# --------------------------------------------------------------------------------------

class NornicDB:
    """Cypher and search over the HTTP API with Basic or Bearer auth. Never opens a Bolt connection."""

    def __init__(self, uri, user, password, token=None, database="nornic", timeout=60, require_scores=False, repo=None, deadline=None,
                 min_rrf_score=0.01):
        self.driver = gi.HttpDriver(uri, user, password, token)
        self.base_url = self.driver.base_url
        self.database = database
        self.timeout = timeout
        self.require_scores = require_scores
        self.repo = repo
        self.deadline = deadline
        self.min_rrf_score = _number(min_rrf_score)
        if self.min_rrf_score is None or self.min_rrf_score <= 0:
            raise ValueError("The ranked-match RRF score floor must be finite and positive.")

    def cypher(self, statement, **params):
        try:
            with self.driver.session(database=self.database) as session:
                return list(session.run(statement, **params))
        except gi.RetryableError as error:
            raise AnalysisError(f"cannot reach NornicDB at {self.base_url}: {error}", "unreachable") from error
        except RuntimeError as error:
            raise AnalysisError(str(error), _kind_of(str(error))) from error

    def search(self, query, *, limit, kinds=("function",)):
        body = {
            "query": query, "database": self.database, "labels": ["Code"], "limit": limit,
            "filters": {"symbol_kind": list(kinds)},
            "include_properties": ["id", "label", "source_file", "source_location", "symbol_kind", "repo", "body"],
        }
        if self.repo:
            body["filters"]["repo"] = [self.repo]
        payload = self._search_page(body)
        items = payload if isinstance(payload, list) else payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            raise AnalysisError("Search returned no result list; duplicate analysis is incomplete.", "incomplete")
        if self.require_scores:
            matching = []
            for item in items:
                score = _number(item.get("rrf_score", item.get("score"))) if isinstance(item, dict) else None
                if score is None:
                    raise AnalysisError("Search returned a missing or invalid match score; analysis is incomplete.", "incomplete")
                if score >= self.min_rrf_score:
                    matching.append(item)
            items = matching
        return self._parse_hits(items)

    def _timeout(self):
        remaining = self.timeout if self.deadline is None else min(self.timeout, self.deadline - time.monotonic())
        if remaining <= 0:
            raise AnalysisError("Duplicate analysis deadline reached before search/comparison completed; increase CODE_INTEL_DEADLINE_SECONDS.", "incomplete")
        return remaining

    def _backoff(self, seconds):
        if self.deadline is not None and time.monotonic() + seconds >= self.deadline:
            raise AnalysisError("Duplicate analysis deadline reached while retrying search; increase CODE_INTEL_DEADLINE_SECONDS.", "incomplete")
        time.sleep(seconds)

    def _search_page(self, body):
        request = urllib.request.Request(
            self.base_url + "/nornicdb/search", data=json.dumps(body).encode("utf-8"), method="POST",
            headers={"Authorization": self.driver.auth_header, "Content-Type": "application/json",
                     "Accept": "application/json"})
        last = None
        for attempt in range(SEARCH_ATTEMPTS):
            try:
                with urllib.request.urlopen(request, timeout=self._timeout()) as response:
                    payload = json.load(response)
                break
            except urllib.error.HTTPError as error:
                detail = error.read().decode("utf-8", "replace")[:300]
                if error.code in (429, 502, 503, 504) and attempt < SEARCH_ATTEMPTS - 1:
                    # "search busy" is normal while the server is embedding a fresh ingest: back off 3, 6, 12 ... s
                    last = f"HTTP {error.code}"
                    self._backoff(3 * 2 ** attempt)
                    continue
                raise AnalysisError(f"HTTP {error.code} from /nornicdb/search: {detail}", _kind_of(f"HTTP {error.code}")) from error
            except (urllib.error.URLError, TimeoutError, ConnectionError, IncompleteRead) as error:
                last = str(error)
                if attempt < SEARCH_ATTEMPTS - 1:
                    self._backoff(3 * 2 ** attempt)
                    continue
                raise AnalysisError(f"cannot reach NornicDB at {self.base_url}: {last}", "unreachable") from error
        else:
            raise AnalysisError(f"cannot reach NornicDB at {self.base_url}: {last}", "unreachable")
        return payload

    @staticmethod
    def _parse_hits(items):
        hits = []
        for item in items:
            node = item.get("node") or {}
            props = node.get("properties") or {}
            if not props.get("body"):
                continue
            matches = [m for m in item.get("matches") or [] if isinstance(m, dict)]
            chunks = [score for m in matches if m.get("kind") == "chunk"
                      for score in (_number(m.get("score")),) if score is not None]
            terms = sorted({m["term"] for m in matches if m.get("kind") == "term" and isinstance(m.get("term"), str) and m["term"]})
            hits.append(Hit(
                id=props.get("id") or node.get("id", ""), label=props.get("label", ""),
                source_file=props.get("source_file", ""),
                line=gi.parse_location(props.get("source_location")) or 1,
                kind=props.get("symbol_kind", ""), repo=props.get("repo", ""), body=props["body"],
                score=_number(item.get("rrf_score", item.get("score"))) or 0.0,
                vector_rank=int(_number(item.get("vector_rank")) or 0),
                bm25_rank=int(_number(item.get("bm25_rank")) or 0),
                vector_similarity=max(chunks) if chunks else None, terms=terms))
        return hits


def _number(value):
    """A finite float from a search response field, or None when it is missing or malformed.

    A server that breaks the response contract must cost one value, not the whole analysis."""
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):  # OverflowError: a JSON integer too large for a float
        return None
    return number if number == number and number not in (float("inf"), float("-inf")) else None


def _kind_of(text):
    if "401" in text or "403" in text or "Unauthorized" in text:
        return "auth"
    if "404" in text or "DatabaseNotFound" in text:
        return "database"
    return "server"


@dataclass
class Hit:
    id: str
    label: str
    source_file: str
    line: int
    kind: str
    repo: str
    body: str
    score: float          # reciprocal-rank-fusion value, not a similarity
    vector_rank: int      # 1-based rank in the vector ranking, 0 when not retrieved by it
    bm25_rank: int        # 1-based rank in the keyword ranking, 0 when not retrieved by it
    vector_similarity: float | None = None  # best embedding chunk's similarity to the query
    terms: list = field(default_factory=list)  # query terms the keyword ranking matched


def ingested_commit(db, repo):
    """The commit the database was last ingested at, or None when the repo was never ingested."""
    try:
        rows = db.cypher("MATCH (r:CodeRepository {name: $repo}) RETURN r.last_commit AS commit", repo=repo)
    except AnalysisError as error:
        if error.kind == "database":
            return None
        raise
    return (rows[0]["commit"] if rows else None) or None


def resolved_owner_calls(graph_path, candidates):
    """Resolve existing-owner use from extracted AST calls, never guessed names."""
    callers = {candidate.id: candidate.source_file for candidate in candidates}
    calls = {}
    if not callers:
        return calls
    for edge in gi.graph_items(graph_path, gi.graph_link_key(graph_path)):
        source, target = edge.get("source"), edge.get("target")
        if (source in callers and isinstance(target, str)
                and edge.get("source_file") == callers[source]
                and edge.get("relation") == "calls" and edge.get("_origin") == "ast"
                and edge.get("confidence") == "EXTRACTED"):
            calls.setdefault(source, {})[target] = edge.get("source_location", "")
    return calls


def retired_ids(db, repo, diff, graph_path):
    """Indexed functions absent from the staged graph after their definition is removed."""
    files = [path for path, d in diff.items() if d.removed]
    live = {(data.get("source_file"), data.get("label"))
            for data in gi.graph_items(graph_path, "nodes")
            if data.get("source_file") in files and gi.symbol_kind(data) == "function"}
    retired = set()
    for start in range(0, len(files), 100):
        chunk = files[start:start + 100]
        rows = db.cypher(
            "MATCH (n:Code {repo: $repo}) WHERE n.source_file IN $files AND n.symbol_kind = 'function' "
            "RETURN n.id AS id, n.source_file AS file, n.source_location AS loc, n.label AS label", repo=repo, files=chunk)
        for row in rows:
            file_diff = diff[row["file"]]
            label = row.get("label")
            symbol = label.split("(", 1)[0].rsplit(".", 1)[-1] if label else None
            if file_diff.deleted or (symbol in file_diff.removed_definitions
                                     and label is not None
                                     and (row["file"], label) not in live):
                retired.add(row["id"])
    return retired


# --------------------------------------------------------------------------------------
# Matching and classification
# --------------------------------------------------------------------------------------

@dataclass
class Match:
    hit: Hit
    similarity: dict
    basis: str = "code"   # "code": the bodies overlap; "behavior": the embeddings say they do the same thing

    def evidence(self):
        h = self.hit
        return {"basis": self.basis, **{k: round(v, 3) for k, v in self.similarity.items()},
                "vector_similarity": None if h.vector_similarity is None else round(h.vector_similarity, 3),
                "vector_rank": h.vector_rank, "bm25_rank": h.bm25_rank, "rrf_score": round(h.score, 4),
                "matched_terms": len(h.terms)}


@dataclass
class Finding:
    candidate: Candidate
    matches: list  # the best matches, at most max_matches
    kind: str      # "refactor" (two or more matches) or "reuse" (exactly one)
    total: int = 0  # how many distinct existing matches there are in all


def behavior_standout(candidate, hits, *, min_gap, min_lead, min_hits):
    """The id of the hit whose embedding similarity stands out from the rest of the query's hits, or None.

    Related code all scores high with a general embedding model, so an absolute similarity says
    little; what marks a function that does the same thing is that it is clearly the closest one:
    the query's top hit by similarity, ``min_gap`` above the median of the hits and ``min_lead``
    ahead of the runner-up."""
    scored = sorted(((h.vector_similarity, h.id) for h in hits
                     if h.vector_similarity is not None and h.id != candidate.id), reverse=True)
    if len(scored) < max(2, min_hits):
        return None
    values = [value for value, _ in scored]
    middle = len(values) // 2
    median = values[middle] if len(values) % 2 else (values[middle - 1] + values[middle]) / 2
    top, top_id = scored[0]
    if top - median >= min_gap and top - values[1] >= min_lead:
        return top_id
    return None


def evaluate(candidate, hits, *, repo, retired, min_similarity, cross_repo=False, max_matches=5,
             min_tokens=0, ignore=(), semantic=True, semantic_min_gap=0.15, semantic_min_lead=0.05,
             semantic_min_hits=5, deadline=None, owner_calls=None, existing_owner_calls=None):
    """Apply the match rules to one candidate's search hits. Returns (Finding | None, moved)."""
    # A semantic-only match to an AST-resolved callee identifies existing reuse.
    # Copied bodies still qualify on code similarity, even if they also call it.
    owner_calls = owner_calls or {}
    reused = {hit.id for hit in hits if hit.id in owner_calls and hit.repo == repo
              and "." not in hit.label.split("(", 1)[0]
              and similarity(candidate.body, hit.body)["score"] < min_similarity}
    standout = behavior_standout(candidate, [hit for hit in hits if hit.id not in reused],
                                 min_gap=semantic_min_gap, min_lead=semantic_min_lead,
                                 min_hits=semantic_min_hits) if semantic else None
    matches, moved, seen = [], False, set()
    for hit in hits:
        if deadline is not None and time.monotonic() >= deadline:
            raise AnalysisError("Duplicate comparison deadline reached; increase CODE_INTEL_DEADLINE_SECONDS.", "incomplete")
        if hit.id == candidate.id or hit.id in seen or hit.kind != "function":
            continue
        if not cross_repo and hit.repo and hit.repo != repo:
            continue
        if gi.is_test_source(hit.source_file) != candidate.test or ignored(hit.source_file, ignore):
            continue
        if len(tokens(hit.body)) < min_tokens:
            continue
        if hit.id in reused:
            if existing_owner_calls is not None and hit.id not in retired:
                existing_owner_calls.append((candidate, hit, owner_calls[hit.id]))
            seen.add(hit.id)
            continue
        sim = similarity(candidate.body, hit.body)
        if sim["score"] >= min_similarity:
            basis = "code"
        elif hit.id == standout:
            basis = "behavior"
        else:
            continue
        if hit.id in retired:
            if basis == "code":
                moved = True  # the code this resembles is deleted by the same PR: a move, not a duplicate
            continue
        seen.add(hit.id)
        matches.append(Match(hit, sim, basis))
    # A removed owner only excludes itself. Other surviving owners still
    # require reuse or refactoring, even when this candidate is also a move.
    matches.sort(key=lambda m: (m.basis != "code", -m.similarity["score"], m.hit.source_file, m.hit.line))
    total = len(matches)
    if not matches:
        return None, moved
    return Finding(candidate, matches[:max_matches] if max_matches else matches,
                   "refactor" if total >= 2 else "reuse", total), False


@dataclass
class Report:
    repo: str
    head_sha: str
    base_sha: str
    ingested_commit: str = ""
    findings: list = field(default_factory=list)
    analyzed: int = 0
    skipped_by_cap: int = 0
    moved: int = 0
    warnings: list = field(default_factory=list)
    nothing_compared: str = ""
    existing_owner_calls: list = field(default_factory=list)

    @property
    def refactor_targets(self):
        return [f for f in self.findings if f.kind == "refactor"]

    @property
    def reuse_candidates(self):
        return [f for f in self.findings if f.kind == "reuse"]


def search_results(db, candidates, limit, workers, deadline, clock=time.monotonic):
    """Bounded prefetch; preserve candidate order and propagate every search failure."""
    if not 1 <= workers <= 8:
        raise ValueError("search_workers must be between 1 and 8")
    if workers == 1:
        for candidate in candidates:
            yield db.search(candidate.query, limit=limit)
        return
    iterator = iter(candidates)
    pool = ThreadPoolExecutor(max_workers=workers)
    pending = deque()
    try:
        for candidate in iterator:
            pending.append(pool.submit(db.search, candidate.query, limit=limit))
            if len(pending) == workers:
                break
        while pending:
            hits = pending.popleft().result(timeout=max(.001, deadline-clock()))
            yield hits
            candidate = next(iterator, None)
            if candidate is not None:
                pending.append(pool.submit(db.search, candidate.query, limit=limit))
    finally:
        for future in pending:
            future.cancel()
        pool.shutdown(wait=True, cancel_futures=True)


def analyze(db, repo, graph_path, repo_root, diff, *, head_sha="", base_sha="", options=None, ignore=(),
            cross_repo=False, clock=time.monotonic):
    opts = {**DEFAULTS, **(options or {})}
    report = Report(repo=repo, head_sha=head_sha, base_sha=base_sha)
    commit = ingested_commit(db, repo)
    if not commit:
        report.nothing_compared = (f"NornicDB has no record of {repo} in database '{db.database}', so nothing was "
                                   f"compared. Run the ingest workflow on the default branch first.")
        return report
    report.ingested_commit = commit
    if base_sha and not (commit == base_sha or commit.startswith(base_sha) or base_sha.startswith(commit)):
        report.warnings.append(
            f"The database was last ingested at {commit[:9]}, not at this PR's merge base {base_sha[:9]}, "
            f"so matches may be stale.")
    candidates = select_candidates(
        graph_path, repo_root, diff, min_body_lines=opts["min_body_lines"],
        min_added_fraction=opts["min_added_fraction"], max_query_chars=opts["max_query_chars"],
        min_tokens=opts["min_tokens"], ignore=ignore)
    if len(candidates) > opts["max_candidates"]:
        report.skipped_by_cap = len(candidates) - opts["max_candidates"]
        candidates = candidates[:opts["max_candidates"]]
        report.warnings.append(f"Only the {opts['max_candidates']} largest added functions were analyzed; "
                               f"{report.skipped_by_cap} were skipped.")
    retired = retired_ids(db, repo, diff, graph_path)
    calls = resolved_owner_calls(graph_path, candidates)
    started = clock()
    searches = search_results(db, candidates, opts["search_limit"],
                              int(opts.get("search_workers", 4)),
                              started + opts["deadline_seconds"], clock)
    try:
        for index, candidate in enumerate(candidates):
            if clock() - started > opts["deadline_seconds"]:
                skipped = len(candidates) - index
                report.skipped_by_cap += skipped
                report.warnings.append(f"The time limit of {opts['deadline_seconds']}s was reached; {skipped} added "
                                       f"functions were not analyzed.")
                break
            hits = next(searches)
            report.analyzed += 1
            finding, moved = evaluate(candidate, hits, repo=repo, retired=retired,
                                      min_similarity=opts["min_similarity"], cross_repo=cross_repo,
                                      max_matches=opts["max_matches"], min_tokens=opts["min_tokens"], ignore=ignore,
                                      semantic=bool(opts["semantic"]), semantic_min_gap=opts["semantic_min_gap"],
                                      semantic_min_lead=opts["semantic_min_lead"],
                                      semantic_min_hits=opts["semantic_min_hits"], deadline=getattr(db, "deadline", None),
                                      owner_calls=calls.get(candidate.id), existing_owner_calls=report.existing_owner_calls)
            if moved:
                report.moved += 1
            if finding:
                report.findings.append(finding)
    finally:
        searches.close()
    report.findings.sort(key=lambda f: (f.kind != "refactor", -len(f.matches), -f.matches[0].similarity["score"],
                                        f.candidate.source_file, f.candidate.line))
    return report


# --------------------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------------------

def _describe(match):
    """How a match was established, for the comment and the summary."""
    h = match.hit
    if match.basis == "behavior":
        similarity = f"{h.vector_similarity:.2f}" if h.vector_similarity is not None else "n/a"
        return f"same behavior: closest by embedding similarity ({similarity}), {round(match.similarity['score'] * 100)}% code overlap"
    return f"{round(match.similarity['score'] * 100)}% similar"


def _link(server, repo, sha, path, line):
    return f"{server}/{repo}/blob/{sha}/{urllib.parse.quote(path)}#L{line}"


def _basis(report):
    parts = []
    if report.head_sha:
        parts.append(f"PR commit `{report.head_sha[:9]}`")
    if report.ingested_commit:
        parts.append(f"compared against code ingested at `{report.ingested_commit[:9]}`")
    return ", ".join(parts)


def render_comment(report, *, server="https://github.com", max_findings=10):
    lines = [COMMENT_MARKER, "### Code Intelligence: refactor targets", ""]
    targets = report.refactor_targets
    if not targets:
        lines += ["No refactor targets found: nothing this PR adds has two or more existing implementations.", ""]
    else:
        lines += ["Code this PR adds already exists in more than one place. "
                  "Consider reusing or consolidating the implementations below.", ""]
        for finding in targets[:max_findings]:
            c = finding.candidate
            lines.append(f"**[`{c.label}`]({_link(server, report.repo, report.head_sha, c.source_file, c.line)})** "
                         f"in `{c.source_file}` is similar to {finding.total or len(finding.matches)} existing implementations"
                         f"{'' if (finding.total or len(finding.matches)) <= len(finding.matches) else f' (the closest {len(finding.matches)} are listed)'}:")
            for match in finding.matches:
                h = match.hit
                link = _link(server, report.repo, report.ingested_commit or "HEAD", h.source_file, h.line)
                lines.append(f"- [`{h.label}`]({link}) in `{h.source_file}` ({_describe(match)})")
            lines.append("")
        if len(targets) > max_findings:
            lines += [f"...and {len(targets) - max_findings} more refactor targets (see the job summary).", ""]
    for warning in report.warnings:
        lines += [f"> **Note:** {warning}", ""]
    lines.append(f"<sub>{_basis(report)}. Advisory only; this check never blocks the PR.</sub>")
    return "\n".join(lines)


def render_summary(report, *, server="https://github.com", max_findings=10):
    lines = ["## Code Intelligence: duplicate detection", ""]
    if report.nothing_compared:
        return "\n".join(lines + [report.nothing_compared, ""])
    lines += [f"Analyzed {report.analyzed} added or rewritten functions"
              f"{f' ({report.moved} recognised as moves)' if report.moved else ''}. {_basis(report)}.", ""]
    if not report.findings:
        lines += ["No duplicates found.", ""]
    for kind, title, items in (("refactor", "Refactor targets (two or more existing implementations)",
                                report.refactor_targets),
                               ("reuse", "Reuse candidates (one existing implementation)", report.reuse_candidates)):
        if not items:
            continue
        lines += [f"### {title}", ""]
        for finding in items[:max_findings]:
            c = finding.candidate
            lines.append(f"- [`{c.label}`]({_link(server, report.repo, report.head_sha, c.source_file, c.line)}) "
                         f"(`{c.source_file}`)")
            for match in finding.matches:
                h = match.hit
                link = _link(server, report.repo, report.ingested_commit or "HEAD", h.source_file, h.line)
                lines.append(f"  - [`{h.label}`]({link}) `{h.source_file}` ({_describe(match)})")
            if finding.total > len(finding.matches):
                lines.append(f"  - ...and {finding.total - len(finding.matches)} more")
        if len(items) > max_findings:
            lines.append(f"- ...and {len(items) - max_findings} more")
        lines.append("")
    if report.existing_owner_calls:
        lines += ["### AST-resolved existing owner calls", ""]
        for candidate, owner, location in report.existing_owner_calls:
            lines.append(f"- `{candidate.source_file}` `{candidate.label}` calls `{owner.label}` "
                         f"in `{owner.source_file}` ({location or 'AST call'}); no copied body at the configured code threshold.")
        lines.append("")
    for warning in report.warnings:
        lines += [f"> **Note:** {warning}", ""]
    return "\n".join(lines)


def report_json(report):
    def finding_json(f):
        return {"kind": f.kind, "total_matches": f.total or len(f.matches),
                "added": {"label": f.candidate.label, "file": f.candidate.source_file, "line": f.candidate.line},
                "matches": [{"label": m.hit.label, "file": m.hit.source_file, "line": m.hit.line,
                             "similarity": round(m.similarity["score"], 3), **m.evidence()} for m in f.matches]}
    return {"repo": report.repo, "head": report.head_sha, "ingested_commit": report.ingested_commit,
            "analyzed": report.analyzed, "moved": report.moved, "skipped_by_cap": report.skipped_by_cap,
            "refactor_targets": len(report.refactor_targets), "reuse_candidates": len(report.reuse_candidates),
            "warnings": report.warnings, "nothing_compared": report.nothing_compared,
            "findings": [finding_json(f) for f in report.findings],
            "existing_owner_calls": [{"caller": {"id": c.id, "file": c.source_file, "label": c.label},
                                      "owner": {"id": h.id, "file": h.source_file, "label": h.label},
                                      "call_location": location, "evidence": "extracted_ast_call"}
                                     for c, h, location in report.existing_owner_calls]}


# --------------------------------------------------------------------------------------
# GitHub pull request comment
# --------------------------------------------------------------------------------------

class GitHub:
    def __init__(self, api_url, token, repo, pr_number):
        self.api_url = api_url.rstrip("/")
        self.token = token
        self.repo = repo
        self.pr = pr_number

    def _request(self, method, path, body=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(self.api_url + path, data=data, method=method, headers={
            "Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github+json",
            "Content-Type": "application/json", "X-GitHub-Api-Version": "2022-11-28"})
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
        return json.loads(raw) if raw else None

    def find_comment(self):
        page = 1
        while True:
            comments = self._request("GET", f"/repos/{self.repo}/issues/{self.pr}/comments?per_page=100&page={page}")
            for comment in comments or []:
                if COMMENT_MARKER in (comment.get("body") or ""):
                    return comment
            if not comments or len(comments) < 100:
                return None
            page += 1

    def upsert(self, body, *, create):
        existing = self.find_comment()
        if existing:
            if (existing.get("body") or "") != body:
                self._request("PATCH", f"/repos/{self.repo}/issues/comments/{existing['id']}", {"body": body})
            return "updated"
        if create:
            self._request("POST", f"/repos/{self.repo}/issues/{self.pr}/comments", {"body": body})
            return "created"
        return "none"


def is_fork(event):
    pr = (event or {}).get("pull_request") or {}
    head = ((pr.get("head") or {}).get("repo") or {})
    base = ((pr.get("base") or {}).get("repo") or {})
    return bool(head.get("fork")) or bool(head.get("full_name") and base.get("full_name")
                                         and head["full_name"] != base["full_name"])


# --------------------------------------------------------------------------------------
# Command line
# --------------------------------------------------------------------------------------

def write_output(name, value):
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(f"{name}={value}\n")


def notice(level, message):
    print(f"::{level}::{message}" if os.environ.get("GITHUB_ACTIONS") else f"{level}: {message}", flush=True)


def build_parser():
    env = os.environ.get
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--graph", type=Path, default=Path("graphify-out/graph.json"), help="graph of the PR head")
    p.add_argument("--repo-root", default=".")
    p.add_argument("--base", default=env("PR_BASE_SHA"), help="PR base commit (the merge base is used)")
    p.add_argument("--head", default=env("PR_HEAD_SHA") or "HEAD")
    p.add_argument("--pr-number", type=int, default=int(env("PR_NUMBER") or 0))
    p.add_argument("--uri", default=env("NORNICDB_URI"))
    p.add_argument("--user", default=env("NORNICDB_USER"))
    p.add_argument("--password", default=env("NORNICDB_PASSWORD", ""))
    p.add_argument("--token", default=env("NORNICDB_TOKEN"))
    p.add_argument("--database", default=env("NORNICDB_DATABASE"))
    p.add_argument("--repo", default=env("CODE_INTEL_REPO") or env("GITHUB_REPOSITORY"))
    p.add_argument("--github-token", default=env("GITHUB_TOKEN"))
    p.add_argument("--github-api-url", default=env("GITHUB_API_URL", "https://api.github.com"))
    p.add_argument("--github-server-url", default=env("GITHUB_SERVER_URL", "https://github.com"))
    p.add_argument("--event-path", default=env("GITHUB_EVENT_PATH"))
    p.add_argument("--json-out", type=Path)
    p.add_argument("--no-comment", action="store_true", help="report to the job summary only")
    p.add_argument("--fail-on-findings", action="store_true", help="fail the job when refactor targets exist")
    p.add_argument("--cross-repo", action="store_true", help="also match code from other repositories")
    p.add_argument("--ignore", action="append", default=[],
                   help="glob of paths to skip, on top of the built-in ones: " + ", ".join(DEFAULT_IGNORE))
    for key, value in DEFAULTS.items():
        p.add_argument("--" + key.replace("_", "-"), dest=key, type=type(value), default=value)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    started = time.monotonic()
    event = {}
    if args.event_path and Path(args.event_path).is_file():
        event = json.loads(Path(args.event_path).read_text(encoding="utf-8"))
    if is_fork(event):
        notice("notice", "Pull request from a fork: no NornicDB credential is available, so duplicate detection was skipped.")
        write_output("refactor-targets", 0)
        return 0
    if not args.uri:
        notice("warning", "Set NORNICDB_URI; see docs/user-guides/graphify-local.md")
        return 1
    if not (args.token or args.password):
        notice("notice", "No NornicDB credential was provided, so duplicate detection was skipped.")
        write_output("refactor-targets", 0)
        return 0
    if args.token and not args.uri.startswith("https://") and not args.uri.startswith("http://localhost"):
        notice("warning", "A bearer token needs an https:// URI; duplicate detection was skipped.")
        return 0
    if not args.repo:
        notice("warning", "--repo (or GITHUB_REPOSITORY) is required.")
        return 0
    database = args.database or args.repo.split("/")[-1]
    try:
        db = NornicDB(args.uri, args.user, args.password, args.token, database)
        base = args.base
        if base:
            merge_base = subprocess.run(["git", "-C", args.repo_root, "merge-base", base, args.head],
                                        capture_output=True, text=True)
            base = merge_base.stdout.strip() or base
        else:
            raise AnalysisError("--base (the PR base commit) is required", "usage")
        head_sha = subprocess.run(["git", "-C", args.repo_root, "rev-parse", args.head],
                                  capture_output=True, text=True).stdout.strip() or args.head
        diff = parse_diff(git_diff(args.repo_root, base, args.head))
        options = {key: getattr(args, key) for key in DEFAULTS}
        options["deadline_seconds"] = max(1, options["deadline_seconds"] - int(time.monotonic() - started))
        report = analyze(db, args.repo, args.graph, args.repo_root, diff, head_sha=head_sha, base_sha=base,
                         options=options, ignore=tuple(DEFAULT_IGNORE) + tuple(args.ignore),
                         cross_repo=args.cross_repo)
    except AnalysisError as error:
        hint = {"auth": " Check the read-only credential (nornicdb-user / nornicdb-password).",
                "unreachable": " The check is advisory, so the PR is not blocked."}.get(error.kind, "")
        notice("warning", f"Duplicate detection was skipped: {error}.{hint}")
        write_output("refactor-targets", 0)
        return 0
    summary = render_summary(report, server=args.github_server_url, max_findings=args.max_findings)
    print(summary)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as handle:
            handle.write(summary + "\n")
    if args.json_out:
        args.json_out.write_text(json.dumps(report_json(report), indent=2), encoding="utf-8")
    write_output("refactor-targets", len(report.refactor_targets))
    write_output("reuse-candidates", len(report.reuse_candidates))
    if not args.no_comment and args.github_token and args.pr_number and not report.nothing_compared:
        try:
            body = render_comment(report, server=args.github_server_url, max_findings=args.max_findings)
            result = GitHub(args.github_api_url, args.github_token, args.repo, args.pr_number).upsert(
                body, create=bool(report.refactor_targets))
            print(f"PR comment: {result}")
        except (urllib.error.URLError, OSError, ValueError) as error:
            notice("warning", f"Could not post the PR comment: {error}")
    if args.fail_on_findings and report.refactor_targets:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
