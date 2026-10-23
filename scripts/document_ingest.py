"""Revision-bound documentation chunks for the canonical code-intelligence corpus."""
import hashlib
import json
import subprocess
import time
import uuid

import code_intelligence as ci
import graphify_local as gi


def document_chunks(text, maximum=2048):
    """Preserve line citations, including lines longer than the chunk budget."""
    parts, size, start, end = [], 0, 1, 1
    for line_number, line in enumerate(text.splitlines(keepends=True), 1):
        for offset in range(0, len(line), maximum):
            piece = line[offset:offset + maximum]
            if parts and size + len(piece) > maximum:
                yield start, end, "".join(parts)
                parts, size = [], 0
            if not parts:
                start = line_number
            parts.append(piece)
            size += len(piece)
            end = line_number
    if parts:
        yield start, end, "".join(parts)


def ingest_documents(root, revision):
    uri, user, password, token = ci.connection()
    repo, database = ci.identity(root)
    commit = ci.git(root, "rev-parse", f"{revision}^{{commit}}")
    paths = ci.git(root, "ls-tree", "-rz", "--name-only", commit).split("\0")
    rows = []
    for source in filter(None, paths):
        if not source.lower().endswith((".md", ".mdx", ".rst")):
            continue
        result = subprocess.run(["git", "-C", str(root), "show", f"{commit}:{source}"],
                                capture_output=True)
        if result.returncode:
            raise ci.CheckError(f"Cannot read documentation at {commit[:12]}:{source}")
        text = result.stdout.decode("utf-8-sig", errors="strict")
        for ordinal, (start, end, content) in enumerate(document_chunks(text)):
            if not content.strip():
                continue
            digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
            key = hashlib.sha256(json.dumps([repo, source, ordinal, start, end, digest]).encode()).hexdigest()
            rows.append({"id": "doc:" + key, "props": {
                "repo": repo, "source_file": source, "start_line": start, "end_line": end,
                "source_location": f"{start}-{end} (chunk {ordinal})", "content_hash": digest, "content": content,
            }})
    ids = [row["id"] for row in rows]
    # HttpSession splits oversized list parameters; NOT IN must never be split.
    if len(json.dumps(ids).encode()) > gi.HTTP_MAX_REQUEST_BYTES // 2:
        raise ci.CheckError("Documentation manifest exceeds the atomic reconciliation budget; no import started.")
    owner = str(uuid.uuid4())
    with gi.connect(uri, user, password, token) as driver:
        with driver.session(database=database) as session:
            # Match the code importer's indexed MERGE path; a full-label scan
            # for every chunk makes even an unchanged refresh needlessly slow.
            session.run("CREATE INDEX swarm_code_document_id IF NOT EXISTS "
                        "FOR (d:CodeDocument) ON (d.id)").consume()
            session.run("CREATE INDEX swarm_code_document_repo IF NOT EXISTS "
                        "FOR (d:CodeDocument) ON (d.repo)").consume()
            acquired = session.run(
                "MERGE (r:CodeDocumentRepository:SwarmWorkflow {name: $repo}) "
                "WITH r WHERE r.owner IS NULL OR r.expires_at < $now "
                "SET r.owner=$owner, r.expires_at=$expires, r.status='building', r.desired_commit=$commit "
                "RETURN r.owner AS owner", repo=repo, owner=owner, now=time.time(),
                expires=time.time() + 3600, commit=commit).single()
            if not acquired or acquired["owner"] != owner:
                raise ci.CheckError("Another documentation refresh owns this repository; retry after it completes.")
            try:
                for offset in range(0, len(rows), 100):
                    count = session.run(
                        "MATCH (r:CodeDocumentRepository {name: $repo}) "
                        "WHERE r.owner=$owner AND r.expires_at > $now "
                        "WITH r UNWIND $rows AS row MERGE (d:CodeDocument {id: row.id}) "
                        "ON CREATE SET d += row.props RETURN count(d) AS count",
                        repo=repo, owner=owner, now=time.time(), rows=rows[offset:offset + 100]).single()
                    if not count or count["count"] != len(rows[offset:offset + 100]):
                        raise ci.CheckError("Documentation writer lost its lease; readiness remains incomplete.")
                # Delete only this importer's stale chunks after a complete, verified manifest.
                session.run(
                    "MATCH (r:CodeDocumentRepository {name: $repo}) "
                    "WHERE r.owner=$owner AND r.expires_at > $now "
                    "WITH r MATCH (d:CodeDocument {repo: $repo}) WHERE NOT d.id IN $ids DETACH DELETE d",
                    repo=repo, owner=owner, now=time.time(), ids=ids).consume()
                ready = session.run(
                    "MATCH (r:CodeDocumentRepository {name: $repo}) "
                    "WHERE r.owner=$owner AND r.expires_at > $now "
                    "SET r.last_commit=$commit, r.status='ready', r.owner=null, r.chunk_count=$count "
                    "RETURN r.last_commit AS commit", repo=repo, owner=owner, now=time.time(),
                    commit=commit, count=len(rows)).single()
                if not ready or ready["commit"] != commit:
                    raise ci.CheckError("Documentation readiness could not be committed.")
            except Exception:
                session.run("MATCH (r:CodeDocumentRepository {name: $repo}) WHERE r.owner=$owner "
                            "SET r.status='failed', r.owner=null", repo=repo, owner=owner).consume()
                raise
    print(f"Documentation ingestion complete: {len(rows)} chunks for {repo} at {commit[:12]}. "
          "Embedding completion is tracked separately by NornicDB.")
