#!/usr/bin/env python3
"""Build the cited Nornic intent-help corpus from skills and linked AGENTS guidance."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import posixpath
import re
import subprocess
from urllib.parse import unquote, urlsplit

import document_ingest as documents
import swarm_local

HELP_REPOSITORY = "nornic/intent-help"
TEXT_SUFFIXES = {".md", ".mdx", ".rst", ".txt"}
LINK = re.compile(r"\[[^\]]*\]\(\s*(?:<([^>]+)>|([^\s)]+))(?:\s+[^)]*)?\)")
REFERENCE = re.compile(r"^\s*\[[^\]]+\]:\s*(?:<([^>]+)>|(\S+))", re.MULTILINE)


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def snapshot(root, revision="HEAD"):
    commit = git(root, "rev-parse", f"{revision}^{{commit}}")
    paths = set(filter(None, git(root, "ls-tree", "-rz", "--name-only", commit).split("\0")))
    return commit, paths


def read(root, commit, path):
    return subprocess.check_output(["git", "-C", str(root), "show", f"{commit}:{path}"]).decode("utf-8-sig")


def guidance_paths(root, commit, paths, roots=("AGENTS.md",)):
    """Follow local Markdown links recursively without leaving the Git snapshot."""
    pending, visited, missing, references = list(roots), set(), set(), set()
    while pending:
        path = pending.pop()
        if path in visited:
            continue
        if path not in paths:
            missing.add(path)
            continue
        visited.add(path)
        text = read(root, commit, path)
        for match in list(LINK.finditer(text)) + list(REFERENCE.finditer(text)):
            target = match.group(1) or match.group(2)
            parts = urlsplit(target)
            if parts.scheme or parts.netloc:
                references.add(target)
                continue
            if not parts.path:
                continue
            decoded = unquote(parts.path)
            if decoded.startswith("/"):
                resolved = posixpath.normpath(decoded.lstrip("/"))
            else:
                resolved = posixpath.normpath(posixpath.join(posixpath.dirname(path), decoded))
            if resolved == ".." or resolved.startswith("../"):
                missing.add(f"{path} -> {target} (outside repository)")
                continue
            if PurePosixPath(resolved).suffix.lower() not in TEXT_SUFFIXES:
                continue
            if resolved not in visited:
                pending.append(resolved)
    if "AGENTS.md" not in visited:
        raise RuntimeError("The guidance snapshot has no AGENTS.md; no import started.")
    return sorted(visited), sorted(missing), sorted(references)


def corpus_rows(sources, repository=HELP_REPOSITORY):
    rows, manifest = [], []
    for root, source_repo, commit, paths in sources:
        manifest.append({"repository": source_repo, "revision": commit, "paths": sorted(paths)})
        for path in sorted(paths):
            content = read(root, commit, path)
            for ordinal, (start, end, text) in enumerate(documents.document_chunks(content)):
                if not text.strip():
                    continue
                key = hashlib.sha256(json.dumps([repository, source_repo, path, ordinal, text]).encode()).hexdigest()
                rows.append({"id": "doc:" + key, "props": {
                    "repo": repository, "source_repository": source_repo, "source_revision": commit,
                    "source_file": path, "start_line": start, "end_line": end,
                    "source_location": f"{start}-{end} (chunk {ordinal})",
                    "content_hash": hashlib.sha256(text.encode()).hexdigest(), "content": text,
                }})
    revision = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    return rows, manifest, revision


def ingest(tools_root=None):
    if tools_root is None:
        tools_root, _, _ = swarm_local.paths()
    env = swarm_local.private_env(tools_root)
    skills_root = Path(env.get("SWARM_SKILLS_ROOT", "~/src/useful-prompts")).expanduser().resolve()
    guidance_root = Path(env.get("SWARM_GUIDANCE_ROOT", str(swarm_local.REPO))).expanduser().resolve()
    if not skills_root.is_dir():
        raise RuntimeError(f"Clone git@github.com:tommyvo/useful-prompts.git into {skills_root}, or set SWARM_SKILLS_ROOT.")
    skills_commit, skills_paths = snapshot(skills_root)
    guidance_commit, guidance_files = snapshot(guidance_root)
    selected, missing, references = guidance_paths(guidance_root, guidance_commit, guidance_files, ("AGENTS.md", "docs/development/swarm-local.md"))
    # Include all platform variants with their original paths; no skill installers
    # or fetched document instructions are executed by this ingestion command.
    skills = sorted(path for path in skills_paths if PurePosixPath(path).suffix.lower() in TEXT_SUFFIXES)
    rows, manifest, revision = corpus_rows([
        (skills_root, "tommyvo/useful-prompts", skills_commit, skills),
        (guidance_root, git(guidance_root, "config", "--get", "remote.origin.url").removesuffix(".git").replace("git@github.com:", "").replace("https://github.com/", ""), guidance_commit, selected),
    ], env.get("NORNIC_HELP_REPO", HELP_REPOSITORY))
    uri = (env.get("SWARM_NORNICDB_URL") or env.get("NORNICDB_URI", "")).rstrip("/")
    if not uri:
        raise RuntimeError(f"Set SWARM_NORNICDB_URL or NORNICDB_URI in {tools_root / 'env'}.")
    # Use the host application's configured connection, including the explicitly
    # configured LAN test endpoint, rather than changing hook connection policies.
    documents.publish_documents(rows, env.get("NORNIC_HELP_REPO", HELP_REPOSITORY),
        env.get("NORNIC_HELP_DATABASE", env.get("NORNIC_CODE_DATABASE", "nornicdbcode")), revision,
        (uri, env.get("NORNICDB_USER", ""), env.get("NORNICDB_PASSWORD", ""), env.get("NORNICDB_TOKEN", "")))
    report = {"manifest": manifest, "manifest_revision": revision, "chunks": len(rows),
              "missing_local_links": missing, "external_references_not_fetched": references,
              "note": "Committed snapshots only. Embedding completion is tracked separately."}
    report_path = tools_root / "intent-help-manifest.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Ingested {len(skills)} skill/prompt documents and {len(selected)} linked guidance files. Manifest: {report_path}")
    for path in missing:
        print(f"Missing local guidance link: {path}")
    print(f"External references retained without fetching: {len(references)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tools-root", type=Path, help="persistent harness directory containing the private env file")
    args = parser.parse_args()
    ingest(args.tools_root.expanduser().resolve() if args.tools_root else None)
