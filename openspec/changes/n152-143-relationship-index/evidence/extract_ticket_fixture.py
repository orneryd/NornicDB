#!/usr/bin/env python3
"""Extract the NOR-42 / N152-143 ticket fixtures verbatim.

The ticket JSON is an exact copy of the Plane work-item payload captured for
N152-143. This script preserves it byte-for-byte and extracts every embedded
<pre> fixture (test sources, check scripts and run logs) exactly as the issue
displays it, together with a manifest of hashes and provenance.

Each fixture is the HTML-unescaped <pre> content with only its single
<code>…</code> wrapper element removed; nothing is reformatted. The raw ticket
JSON is copied unchanged.

A run log is written as `<name>.log.txt` so the evidence stays committable:
the repository ignores `*.log` (.gitignore:39) and weakening that rule is not
this change's business. The original ticket label is recorded in the manifest.
"""

from __future__ import annotations

import hashlib
import html
import json
import os
import re

SOURCE = "/tmp/n152-143-ticket.json"
OUT = os.path.dirname(os.path.abspath(__file__))
TICKET_COPY = os.path.join(OUT, "ticket", "n152-143-ticket.json")
FIXTURES = os.path.join(OUT, "fixtures")
MANIFEST = os.path.join(FIXTURES, "MANIFEST.json")

PRE = re.compile(r"<pre[^>]*>(.*?)</pre>", re.S)
TAG = re.compile(r"<[^>]+>")
CODE_WRAPPER = re.compile(r"^\s*<code[^>]*>(.*)</code>\s*$", re.S)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def label_for(segment: str) -> str:
    """Last non-empty text line of the HTML between two fixtures."""
    text = html.unescape(TAG.sub("\n", segment))
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line:
            return line
    return ""


def destination(label: str) -> str:
    """Map a fixture label to an evidence path without losing its identity."""
    if label.startswith("/tmp/"):
        relative = os.path.join("tmp", os.path.basename(label))
    elif label.startswith("/"):
        relative = os.path.join("abs", label.lstrip("/").replace("/", "_"))
    else:
        relative = os.path.join("testdata", "repo", label)
    if relative.endswith(".log"):
        # .gitignore:39 ignores *.log; the bytes are unchanged, the name is not.
        relative += ".txt"
    return relative


def main() -> None:
    raw = open(SOURCE, "rb").read()
    issue = json.loads(raw)
    os.makedirs(os.path.dirname(TICKET_COPY), exist_ok=True)
    os.makedirs(FIXTURES, exist_ok=True)
    with open(TICKET_COPY, "wb") as handle:
        handle.write(raw)

    body = issue["description_html"]
    fixtures = []
    previous_end = 0
    for index, match in enumerate(PRE.finditer(body)):
        label = label_for(body[previous_end:match.start()])
        previous_end = match.end()
        content = html.unescape(match.group(1))
        wrapped = CODE_WRAPPER.match(content)
        if wrapped:
            content = wrapped.group(1)
        payload = content.encode("utf-8")
        path = os.path.join(FIXTURES, destination(label))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as handle:
            handle.write(payload)
        fixtures.append(
            {
                "index": index,
                "label": label,
                "evidence_path": os.path.relpath(path, OUT),
                "bytes": len(payload),
                "sha256": sha256(payload),
                "code_wrapper_removed": bool(wrapped),
                "html_span": [match.start(), match.end()],
            }
        )

    manifest = {
        "ticket_source": SOURCE,
        "ticket_copy": os.path.relpath(TICKET_COPY, OUT),
        "ticket_bytes": len(raw),
        "ticket_sha256": sha256(raw),
        "issue": {
            key: issue.get(key)
            for key in (
                "id",
                "name",
                "sequence_id",
                "created_at",
                "updated_at",
                "priority",
                "project",
                "workspace",
                "state",
                "labels",
                "created_by",
                "updated_by",
                "archived_at",
                "deleted_at",
            )
        },
        "description_html_bytes": len(body.encode("utf-8")),
        "description_html_sha256": sha256(body.encode("utf-8")),
        "extraction": (
            "html.unescape(<pre> inner HTML), single <code>…</code> wrapper "
            "removed, written as UTF-8 bytes; a *.log label gets a .txt suffix "
            "because .gitignore:39 ignores *.log"
        ),
        "fixtures": fixtures,
    }
    with open(MANIFEST, "w") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(json.dumps(manifest["fixtures"], indent=2))


if __name__ == "__main__":
    main()
