#!/usr/bin/env python3
# Author: Codex NOR-285 implementation agent, 2026-10-26.
"""Decode lab output artifacts and compare complete ordered result captures.

This is evidence processing only. It never starts an engine or executes Cypher.
"""
import argparse
import base64
import gzip
import hashlib
import json
import re
from pathlib import Path

AUTHOR = "Codex NOR-285 implementation agent, 2026-10-26"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def packet_bytes(path):
    # Linux captures hash LF packets. Use that same identity for either checkout;
    # only CRLF line endings change, never artifact bytes or statement contents.
    return path.read_bytes().replace(b"\r\n", b"\n")


def require_corpus_identity(value):
    # Counts and declared arm hashes do not identify the admitted export. Bind
    # every capture to the committed packet that the Go admission step verified.
    for name in ["workload-current.json", "workload.json"]:
        data = packet_bytes(Path(__file__).with_name(name))
        if digest(data) == value.get("packet_sha256"):
            packet = json.loads(data)
            actual = value.get("corpus_sha256")
            if (not isinstance(actual, str) or not re.fullmatch(r"[0-9a-f]{64}", actual)
                    or actual != packet["corpus_sha256"]
                    or value.get("corpus_digest_algorithm") != packet["corpus_digest_algorithm"]):
                raise ValueError("Missing or mismatched verified corpus identity")
            return
    raise ValueError("Capture does not identify a committed frozen packet")


def output_text(text):
    # `go test -json` splits long Output records. Concatenating Output values
    # reassembles the original fixture's artifact line before decoding it.
    chunks = []
    for line in text.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if isinstance(event, dict) and isinstance(event.get("Output"), str):
            chunks.append(event["Output"])
    return "".join(chunks) if chunks else text


def collect(source, destination):
    text = output_text(source.read_text(encoding="utf-8-sig"))
    decoder = json.JSONDecoder()
    records = []
    selected = None
    captures = {}
    for match in re.finditer(r"NOR285 ", text):
        record, _ = decoder.raw_decode(text[match.end():])
        value = record["value"]
        if record["kind"] == "input":
            require_corpus_identity(value)
            selected = value["case"]
        if record["kind"] == "artifact":
            if selected is None:
                raise ValueError("Artifact has no preceding case identity")
            name = value["name"]
            if Path(name).name != name:
                raise ValueError("Artifact name escapes its case directory")
            data = base64.b64decode(value["base64"], validate=True)
            if len(data) != value["bytes"] or digest(data) != value["sha256"]:
                raise ValueError(f"Corrupt artifact: {selected}/{name}")
            directory = destination / selected
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / name
            if path.exists() and path.read_bytes() != data:
                raise ValueError(f"Refusing to overwrite different artifact: {path}")
            path.write_bytes(data)
            record = dict(record, value={k: v for k, v in value.items() if k != "base64"})
            if name == "result.json.gz":
                raw = gzip.decompress(data)
                result = json.loads(raw)
                captures[selected] = {"sha256": digest(raw), "bytes": len(raw),
                                      "columns": result["columns"], "rows": len(result["rows"])}
        if selected is not None:
            record = dict(record, case=selected)
        records.append(record)
    if not records:
        raise ValueError("No fixture output records found; not a profile result")
    for record in records:
        if record["kind"] == "result":
            actual = captures.get(record["value"]["case"])
            if actual is None or actual["sha256"] != record["value"]["sha256"]:
                raise ValueError("Result record does not match its complete artifact")
    destination.mkdir(parents=True, exist_ok=True)
    summary = {"author": AUTHOR, "source_log": str(source),
               "source_log_sha256": digest(source.read_bytes()),
               "records": records, "complete_results": captures}
    (destination / "collection.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"records": len(records), "complete_results": captures}, indent=2))


def compare(left, right):
    first = json.loads((left / "collection.json").read_text(encoding="utf-8"))
    second = json.loads((right / "collection.json").read_text(encoding="utf-8"))
    def identities(summary):
        return {record["value"]["case"]: record["value"]
                for record in summary["records"] if record["kind"] == "input"}
    one, two = identities(first), identities(second)
    if not one or one.keys() != two.keys():
        raise ValueError("Arm case populations differ or are empty")
    outcomes = []
    for case in sorted(one):
        require_corpus_identity(one[case])
        require_corpus_identity(two[case])
        for field in ["source_revision", "packet_sha256", "statement_sha256", "parameters", "documents", "parser",
                      "corpus_digest_algorithm", "corpus_sha256"]:
            if one[case][field] != two[case][field]:
                raise ValueError(f"Input differs for {case}: {field}")
        if case not in first["complete_results"] or case not in second["complete_results"]:
            raise ValueError(f"Incomplete result: {case}")
        a = gzip.decompress((left / case / "result.json.gz").read_bytes())
        b = gzip.decompress((right / case / "result.json.gz").read_bytes())
        # Preserve all array/row/column order. No sorting, sampling or tolerance.
        equal = a == b
        outcomes.append({"case": case, "identical_bytes": equal,
                         "corpus_sha256": one[case]["corpus_sha256"],
                         "left_sha256": digest(a), "right_sha256": digest(b),
                         "left_bytes": len(a), "right_bytes": len(b)})
    report = {"author": AUTHOR, "left": str(left), "right": str(right),
              "all_equal": all(row["identical_bytes"] for row in outcomes), "cases": outcomes}
    print(json.dumps(report, indent=2))
    if not report["all_equal"]:
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    decode = subcommands.add_parser("collect")
    decode.add_argument("source", type=Path)
    decode.add_argument("destination", type=Path)
    check = subcommands.add_parser("compare")
    check.add_argument("left", type=Path)
    check.add_argument("right", type=Path)
    args = parser.parse_args()
    if args.command == "collect":
        collect(args.source, args.destination)
    else:
        compare(args.left, args.right)


if __name__ == "__main__":
    main()
