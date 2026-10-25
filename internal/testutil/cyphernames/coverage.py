"""Generate execution columns solely by joining runtime frames to source sites.

The source population is parsed Go call expressions and Python AST calls, plus
lexical JS/TS peer calls. Discovery of arbitrary new non-owner writers remains
a smoke heuristic in inventory.py, not universal detection or data-flow proof.
"""
import argparse
import ast
import base64
import gzip
import hashlib
import json
import pathlib
import re
import subprocess
from completion import obligations

ROOT = pathlib.Path(__file__).resolve().parents[3]


def peer_sites():
    files = subprocess.check_output(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT).decode().split("\0")
    for file in sorted(set(files)):
        if not file.startswith(("scripts/", "ui/")) or any(p in file for p in ("node_modules/", "_probe", "test_", "cypher_names.py", "cypherNames.mjs")):
            continue
        path = ROOT / file
        if path.suffix not in (".py", ".mjs", ".ts", ".tsx", ".js"):
            continue
        source = path.read_text(encoding="utf-8")
        if path.suffix == ".py":
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "quote_name":
                    funcs = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.lineno <= node.lineno <= n.end_lineno]
                    yield dict(Runtime="python", File=file, Function=funcs[-1].name if funcs else "<module>", Line=node.lineno, End=node.end_lineno, Callee="quote_name", Source=ast.get_source_segment(source, node))
        else:
            for number, line in enumerate(source.splitlines(), 1):
                if line.lstrip().startswith(("//", "*")):
                    continue
                if re.match(r"\s*(?:(?:private|public|protected|static|async)\s+)*quoteCypherIdentifier\([^)]*\)\s*:", line):
                    continue  # A TS method declaration is not a call site.
                # One row per written call; same-line calls share observed
                # reach, with the explicit per-call role retained in records.
                for call in re.finditer(r"\b(?:quoteName|quoteCypherIdentifier)\([^\n]*?\)", line):
                    prefix = "\n".join(source.splitlines()[:number])
                    # Method headers can span lines (the browser API does).
                    funcs = re.findall(r"(?:function\s+(\w+)|^\s*(?:(?:private|public|protected|static)\s+)*(?:async\s+)?(\w+)\s*\(|const\s+(\w+)\s*=\s*\([^\n]*\)\s*=>)", prefix, re.M)
                    function = next((a or b or c for a,b,c in reversed(funcs) if (a or b or c) not in {"if", "for", "while", "switch", "catch", "with"}), "<module>")
                    yield dict(Runtime="js", File=file, Function=function, Line=number, End=number, Callee=call[0].split("(")[0], Source=call[0])


def generate(sites, records):
    dispositions = json.loads((ROOT / "internal/testutil/cyphernames/source_dispositions.json").read_text())
    rows = []
    for site in sorted(sites, key=lambda s: (s["File"], s["Line"], s["Source"])):
        observed = [r for r in records if r["File"] == site["File"] and site["Line"] <= r["Line"] <= site["End"]]
        # No test-name lookup, file-membership credit, or source-disposition
        # override may ever set this column.
        disposition = dispositions.get(site["File"], {}).get(site["Function"], "")
        rows.append(dict(site, exercised=bool(observed), roles=sorted({r["Role"] for r in observed}), inputs=sorted({r["Input"] for r in observed}), source_disposition=disposition, disposition="" if observed else disposition or "Not exercised by this recorded suite; no execution claim."))
    return rows


def observed_sites(sites, records):
    """Recorded delegation frames generate rows without a hand-written map."""
    extra = {}
    for record in records:
        file, line = record["File"], record["Line"]
        # VM probes append their harness after the production program under
        # the same source URL. JS reach is admitted only at a lexical peer
        # call in the original file, never at appended harness frames.
        if record["Runtime"] == "js":
            continue
        if any(s["File"] == file and s["Line"] <= line <= s["End"] for s in sites):
            continue
        path = ROOT / file
        if not path.is_file():
            continue
        lines = path.read_text(encoding="utf-8").splitlines()
        if not 1 <= line <= len(lines):
            continue
        extra[(file, line)] = dict(Runtime=record["Runtime"], File=file, Function=record["Function"],
                                  Line=line, End=line, Callee="<recorded delegation>",
                                  Source=lines[line-1].strip())
    return list(extra.values())


def controls(evidence):
    before = generate([evidence["site"]], [json.loads(r) for r in evidence["before"] if r])
    after = generate([evidence["site"]], [json.loads(r) for r in evidence["after"] if r])
    assert before[0]["exercised"] and not after[0]["exercised"], (before, after)
    return dict(test_symbol=evidence["test_symbol"], before=before, after=after)


def main():
    p = argparse.ArgumentParser(); p.add_argument("--sites"); p.add_argument("--recording"); p.add_argument("--control", action="append"); p.add_argument("--artifact")
    args = p.parse_args()
    if args.artifact:
        artifact = json.loads(pathlib.Path(args.artifact).read_text())
        assert artifact["subjects_passed"], "recorded subject suite did not pass"
        sites = artifact["sites"] + list(peer_sites())
        records = [json.loads(line) for line in artifact["recording"].splitlines()]
        control_evidence = artifact["controls"]
        passed_subjects = artifact.get('passed_subjects', [])
    else:
        if not (args.sites and args.recording and args.control):
            p.error("supply --artifact, or --sites/--recording/--control")
        sites = json.loads(pathlib.Path(args.sites).read_text()) + list(peer_sites())
        records = [json.loads(line) for line in pathlib.Path(args.recording).read_text().splitlines()]
        control_evidence = [json.loads(pathlib.Path(path).read_text()) for path in args.control]
        passed_subjects = []  # Legacy reach-only inputs cannot complete semantic obligations.
    records = [r for r in records if r["Runtime"] != "js" or any(s["Runtime"] == "js" and s["File"] == r["File"] and s["Line"] <= r["Line"] <= s["End"] for s in sites)]
    sites += observed_sites(sites, records)
    rows = generate(sites, records)
    assert rows and records, "empty call-site population or recording"
    assert {r["Runtime"] for r in records} == {"go", "js", "python"}, "missing runtime recording"
    assert any(r["Role"] == "property-ref" and r["Input"] == "written" for r in records), "DDL reader not recorded"
    hashes = {f: hashlib.sha256((ROOT/f).read_bytes()).hexdigest() for f in sorted({s["File"] for s in sites})}
    payload = dict(limits=__doc__, sources=hashes, records=records, rows=rows, removal_controls=[controls(evidence) for evidence in control_evidence],
                   counts=dict(total=len(rows), exercised=sum(r["exercised"] for r in rows), source_disposition=sum(not r["exercised"] and bool(r["source_disposition"]) for r in rows), not_exercised=sum(not r["exercised"] and not r["source_disposition"] for r in rows)))
    catalog = json.loads((ROOT / 'internal/testutil/cyphernames/completion_contracts.json').read_text())
    payload['obligations'], failures = obligations(rows, records, passed_subjects, catalog)
    payload['passed_subjects'] = passed_subjects
    payload['completion'] = dict(complete=not failures, required=len(rows),
        satisfied=sum(o['status'] == 'complete' for o in payload['obligations']),
        reviewed_exclusions=sum(o['status'] == 'reviewed_exclusion' for o in payload['obligations']), failures=failures)
    print("NAME_EMISSION_LEDGER " + base64.b64encode(gzip.compress(json.dumps(payload, separators=(",", ":")).encode(), mtime=0)).decode())
    print("NAME_EMISSION_COUNTS " + json.dumps(payload["counts"]))
    print('NAME_EMISSION_COMPLETION ' + json.dumps(payload['completion']))
    if failures:
        raise SystemExit('Name-emission completion failed: ' + '\n'.join(failures))


if __name__ == "__main__":
    main()
