#!/usr/bin/env python3
# Author: Codex NOR-285 implementation agent, 2026-10-26.
"""Copy frozen query contracts without running the originating application."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

AUTHOR = "Codex NOR-285 implementation agent, 2026-10-26"
ROOT = "rust/crates/pd-graph/"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repository")
    parser.add_argument("--revision", default="d5eb96b")
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("workload.json"))
    parser.add_argument("--corpus-inventory", type=Path, required=True,
                        help="Verified immutable maintained export's path-to-file-SHA256 JSON")
    args = parser.parse_args()
    repository = args.repository
    selected_revision = args.revision
    def show(path):
        return subprocess.check_output(
            ["git", "-C", repository, "show", f"{selected_revision}:{path}"]
        )
    def load(path):
        return json.loads(show(ROOT + path))
    revision = subprocess.check_output(
        ["git", "-C", repository, "rev-parse", selected_revision], text=True
    ).strip()
    related = load("src/retrieval/related/query_goldens.json")
    related.update(load("src/retrieval/holdings/verified_queries.json"))
    retrieval = load("tests/retrieval/query_goldens.json")
    retrieval.update(load("tests/retrieval/verified_queries.json"))
    walk_keys = [key for key in retrieval if key.startswith("graph_queries.WALK:")]
    if len(walk_keys) != 1:
        raise ValueError(f"Ambiguous frozen WALK: {walk_keys}")
    definitions = load("src/semantic/definitions/defaults.json")
    classes = {d["id"]: d["identity_class"] for d in definitions if d.get("identity_class")}
    cases = []
    def add(name, family, statement, parameters):
        cases.append({"name": name, "family": family, "statement": statement,
                      "statement_sha256": hashlib.sha256(statement.encode()).hexdigest(),
                      "parameters": parameters})
    add("same_party", "SAME_PARTY", related["related.SAME_PARTY"], {
        "rejected": "rejected_proposed_name", "classes": classes,
        "class_names": ["brand", "organisation", "other", "person", "place"],
        "collection_ids": None, "limit": 50, "per_item": 20})
    for depth in [4, 3, 2, 1]:
        add(f"walk_doc_d{depth}", "WALK", retrieval[walk_keys[0]], {
            "start_id": "doc10", "depth": depth, "kinds": [], "at": None,
            "collection_ids": None, "max_nodes": 50})
    add("topic_groups", "TOPIC_GROUPS", related["related.TOPIC_GROUPS"], {
        "collection_id": "project1", "per_group": 20})
    for variant, start in [("doc", "doc10"), ("case", "case3")]:
        add("related_" + variant, "RELATED", related["related.RELATED"], {
            "start_id": start, "limit": 20, "collection_ids": None})
    for variant, start in [("doc", "doc10"), ("identity", "__FIXTURE_PARTY_ID__")]:
        add("suggest_" + variant, "SUGGEST", related["related.SUGGEST"], {
            "start_id": start, "limit": 20})
    expected = "0caa2b3923cdb2f064197ddcfc7763143303ec67170cb7e68bc609eaa32ae21f"
    if revision.startswith("d5eb96b") and cases[0]["statement_sha256"] != expected:
        raise ValueError("SAME_PARTY is not the reported 63,177-byte bound-holder statement")
    inventory = json.loads(args.corpus_inventory.read_text(encoding="utf-8"))
    if not inventory or any(not isinstance(path, str) or not isinstance(sha, str)
                            or not re.fullmatch(r"[0-9a-f]{64}", sha)
                            for path, sha in inventory.items()):
        raise ValueError("Invalid maintained-export inventory")
    manifest = json.dumps(inventory, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    output = {"author": AUTHOR, "source_revision": revision,
              "corpus_digest_algorithm": "sha256-canonical-file-map-v1",
              "corpus_sha256": hashlib.sha256(manifest).hexdigest(),
              "documents": 5280, "cases": cases,
              "provenance": "Frozen statement contracts and seed definitions; statements are copied byte-for-byte, never assembled by this script."}
    destination = args.output
    destination.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for case in cases:
        print(case["name"], len(case["statement"].encode()), case["statement_sha256"])


if __name__ == "__main__":
    main()
