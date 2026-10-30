"""Extract the issue-reproduction corpus for the differential ratchet (#754).

usage: python extract_issue_corpus.py [OUT]   (default: the corpus in
testing/cypher/tck/testdata/differential/issues.json)

Reads every issue of orneryd/NornicDB, open and closed, with the GitHub CLI
(gh api), and collects the Cypher statements in each issue's text: code
blocks (```cypher or unlabelled blocks that read as Cypher) and whole
statements quoted in table cells. Statements are split on ';', or at a line
that starts a new statement after a complete one. // comments are removed.

An issue's statements run in order on an empty graph, so its setup (CREATE,
MERGE) comes before the statements that read it, as in the issue.

Statements are left out when comparing them with Neo4j says nothing about
NornicDB's Cypher, or when they would make the run slow or unrepeatable:
- administration (databases, aliases, users, roles, privileges);
- procedures and functions Neo4j Community doesn't have (APOC, NornicDB's
  own, GDS, GenAI, plugins), LOAD CSV, client commands (:param);
- NornicDB's own schema objects (decay and promotion profiles, constraint
  contracts, value-list constraints), and constraints only Neo4j Enterprise
  creates (key, existence and property type constraints);
- answers that differ between runs: rand(), randomUUID(), timestamp(), the
  current date or time, id(), elementId();
- data generators larger than MAX_RANGE rows.

The output is JSON: [{"issue", "title", "statements": [{"id", "query"}]}].
A statement's id is a hash of the issue number, its position and its text.
"""
import hashlib
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT = os.path.join(HERE, "..", "..", "..", "testing", "cypher", "tck", "testdata", "differential", "issues.json")
REPO = "orneryd/NornicDB"
MAX_RANGE = 2000

# A statement's first clause; after a complete statement, a line starting with
# one of these begins the next statement.
NEW_STATEMENT = re.compile(
    r"^(OPTIONAL\s+MATCH|MATCH|CREATE|MERGE|UNWIND|CALL|SHOW|DROP|FOREACH|EXPLAIN|PROFILE|USE|RETURN|WITH)\b", re.I)
RETURNS = re.compile(r"\bRETURN\b", re.I)
WRITES = re.compile(r"\b(CREATE|MERGE|SET|DELETE|REMOVE|FOREACH)\b|^\s*(CALL|SHOW|DROP)\b", re.I)
CLAUSE_END = re.compile(r"\b(RETURN|CREATE|MERGE|SET|DELETE|REMOVE|YIELD|FOREACH|FINISH)\b|^\s*(CALL|SHOW|DROP)\b", re.I)
SKIP = [
    re.compile(r"\b(CREATE|DROP|ALTER|START|STOP|RENAME)\s+(OR\s+REPLACE\s+)?(COMPOSITE\s+)?(DATABASE|ALIAS|USER|ROLE)S?\b", re.I),
    re.compile(r"\b(GRANT|DENY|REVOKE)\b", re.I),
    re.compile(r"\bSHOW\s+(CURRENT\s+)?(USERS?|ROLES?|PRIVILEGES|DATABASES?|ALIASES|SERVERS|SETTINGS|TRANSACTIONS)\b", re.I),
    re.compile(r"\b(apoc|nornicdb|nornic|gds|genai|custom|graphrag|heimdall)\.", re.I),
    re.compile(r"\bCALL\s+(?!(db|dbms|tx)\.)[A-Za-z_][\w]*\.", re.I),
    re.compile(r"\b(reveal|decayScore|decay_score|embed)\s*\(", re.I),
    re.compile(r"\bLOAD\s+CSV\b", re.I),
    re.compile(r"\b(rand|randomUUID|timestamp|id|elementId)\s*\(", re.I),
    re.compile(r"\b(date|datetime|time|localtime|localdatetime)(\.(realtime|statement|transaction))?\s*\(\s*\)", re.I),
    re.compile(r"\b(TERMINATE|SHOW)\s+TRANSACTIONS?\b", re.I),
    re.compile(r"\bPOLIC(Y|IES)\b", re.I),
    # NornicDB's own schema objects (Neo4j rejects them; a graph reset doesn't
    # remove them, so they would carry into later issues) and procedures.
    re.compile(r"\b(DECAY|PROMOTION)\s+PROFILES?\b", re.I),
    re.compile(r"\bREQUIRE\s*\{|\bCONSTRAINT\s+CONTRACTS?\b", re.I),
    re.compile(r"\bREQUIRE\b.*\bIN\s*\[", re.I),
    re.compile(r"\bdb\.index\.(vector|fulltext)\.(drop|embed|createRelationshipIndex)\b", re.I),
    re.compile(r"\bdb\.(retrieve|rretrieve|rerank|infer|temporal\.\w+|txlog\.\w+|index\.stats)\b", re.I),
    # Constraints Neo4j Community can't create (Enterprise Edition only).
    re.compile(r"\bIS\s+(NODE|RELATIONSHIP|REL)\s+KEY\b", re.I),
    re.compile(r"\bREQUIRE\b.*\bIS\s+(NOT\s+NULL\b|::|TYPED\b)", re.I),
]


def issues():
    out = subprocess.run(
        ["gh", "api", "--paginate", f"repos/{REPO}/issues?state=all&per_page=100",
         "--jq", ".[] | select(.pull_request == null) | {number, title, body}"],
        check=True, capture_output=True, text=True, encoding="utf-8").stdout
    return sorted((json.loads(line) for line in out.splitlines() if line.strip()), key=lambda i: i["number"])


def strip_comment(line):
    quote = None
    for index, char in enumerate(line):
        if quote:
            if char == quote:
                quote = None
        elif char in "'\"`":
            quote = char
        elif line.startswith("//", index):
            return line[:index]
    return line


def split_block(code):
    lines = [strip_comment(line).rstrip() for line in code.splitlines()]
    text = "\n".join(lines)
    if ";" in text:
        parts = []
        depth, quote, current = 0, None, []
        for char in text:
            if quote:
                if char == quote:
                    quote = None
            elif char in "'\"`":
                quote = char
            elif char in "([{":
                depth += 1
            elif char in ")]}":
                depth -= 1
            elif char == ";" and depth == 0:
                parts.append("".join(current))
                current = []
                continue
            current.append(char)
        parts.append("".join(current))
        return [" ".join(part.split()) for part in parts if part.strip()]
    statements, current = [], []
    for line in lines:
        if not line.strip():
            if current:
                statements.append(current)
                current = []
            continue
        if current and complete_before(" ".join(current), line.strip()):
            statements.append(current)
            current = []
        current.append(line.strip())
    if current:
        statements.append(current)
    return [" ".join(" ".join(s).split()) for s in statements]


def complete_before(statement, line):
    """Whether line starts a new statement after statement. RETURN ends a
    statement; after a write, a clause other than RETURN or WITH starts a
    new one."""
    if not NEW_STATEMENT.match(line):
        return False
    if RETURNS.search(statement):
        return True
    return WRITES.search(statement) is not None and re.match(r"^(RETURN|WITH)\b", line, re.I) is None


def starts_statement(text):
    """Whether text starts with a statement's first clause keyword, followed
    by a space, a parenthesis or nothing (not call.go:12)."""
    return re.match(NEW_STATEMENT.pattern + r"(\s|\(|$)", text, re.I) is not None


def reads_as_cypher(code):
    lines = [line.strip() for line in code.splitlines() if line.strip() and not line.strip().startswith("//")]
    return bool(lines) and starts_statement(lines[0])


def statements(body):
    found = []
    for lang, code in re.findall(r"```([A-Za-z0-9_+-]*)[^\n]*\n(.*?)```", body or "", re.S):
        if lang.lower() == "cypher" or (lang == "" and reads_as_cypher(code)):
            found.extend(split_block(code))
    for line in (body or "").splitlines():
        if not line.lstrip().startswith("|"):
            continue
        for span in re.findall(r"`([^`]+)`", line):
            span = span.strip()
            if starts_statement(span) and CLAUSE_END.search(span):
                found.append(" ".join(span.split()))
    return found


def keep(statement):
    if len(statement) > 4000 or not starts_statement(statement) or "..." in statement or "…" in statement:
        return False
    if any(pattern.search(statement) for pattern in SKIP):
        return False
    for low, high in re.findall(r"\brange\s*\(\s*(-?\d+)\s*,\s*(-?\d+)", statement):
        if abs(int(high) - int(low)) > MAX_RANGE:
            return False
    return True


def main():
    out_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUT
    corpus = []
    total = 0
    for issue in issues():
        kept = []
        for index, statement in enumerate(s for s in statements(issue["body"]) if keep(s)):
            digest = hashlib.sha1(f"{issue['number']}:{index}:{statement}".encode()).hexdigest()[:12]
            kept.append({"id": digest, "query": statement})
        if kept:
            corpus.append({"issue": issue["number"], "title": issue["title"], "statements": kept})
            total += len(kept)
    with open(out_path, "w", encoding="utf-8", newline="\n") as out:
        json.dump(corpus, out, indent=1, ensure_ascii=False)
        out.write("\n")
    print(f"{len(corpus)} issues, {total} statements")


if __name__ == "__main__":
    main()
