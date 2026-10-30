#!/usr/bin/env python3
"""Surgically attach fixing-commit evidence to the 35-issue ledger,
preserving the original file formatting (minimal diff)."""
import json
import re
import sys

PATH = "testing/cypher/tck/testdata/issues.json"

REGRESSION_BASELINE = {
    "default": "2da13483",
    448: "1025caa3",
    481: "1025caa3",
    470: None,
    482: None,
    487: None,
}

DIRECT_FIXING = {
    448: ["3ca7b4ce", "fd05ef8a"],
    461: ["03f2d765"],
    462: ["81542d88"],
    470: ["9f3aee59"],
    475: ["f0c927df"],
    480: ["9f3aee59"],
    487: ["7748cec1"],
}

# Program-level delivery evidence for the proposal issue.
PROGRAM_REPRODUCTIONS = {
    482: ["TestSetRoutesConverge", "TestMutationFamiliesRollBackOnRejection"],
    487: [
        "TestCollectNodesWithStreaming_LabelLimitPrefersLabelLookup",
        "TestTraversalLimitStopsStreamingStartNodes",
        "BenchmarkStatementRouting",
    ],
}

PROGRAM_SERIES = "program:#521-series"

QUALIFICATION = {
    "tck_ratchet": "7794/7794 scenario/mode outcomes at upstream revision 370fe27f417730dca2ef712dd1c0c5dadcb99ef8; zero expected gaps, setup blocks and harness errors; 192 fully-vetted features (make cypher-tck-ratchet, make cypher-tck-vetted).",
    "full_suite": "go test ./... -p 2 -count=1 -timeout=15m passes.",
    "build": "go build ./... passes.",
    "race": "Scoped race suites pass: pkg/cypher (excluding four timing-threshold tests invalid under instrumentation), pkg/storage, pkg/server, pkg/bolt, pkg/linkpredict.",
    "benchmark": "M2 Max BenchmarkStatementRouting/(autocommit|explicit_tx)/simple_match_limit stays within the recorded band with unchanged allocations.",
}


def entry_block_idx(text: str, number: int):
    """Return (start, end) offsets of the entry object in the current text."""
    marker = f'"number": {number}'
    start = text.index(marker)
    obj_start = text.rfind("{", 0, start)
    depth = 0
    in_str = False
    esc = False
    for i in range(obj_start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return obj_start, i + 1
    raise ValueError(f"unterminated entry for issue {number}")


def main() -> int:
    with open(PATH, encoding="utf-8") as fh:
        original = fh.read()
    text = original
    doc = json.loads(original)
    issues = doc["issues"]

    status_re = re.compile(r'\n(\s*)"status": "([a-z-]+)"')

    for entry in issues:
        number = int(entry["number"])
        obj_start, obj_end = entry_block_idx(text, number)
        block = text[obj_start:obj_end]
        status_match = status_re.search(block)
        if not status_match:
            raise ValueError(f"no status in entry {number}")
        indent = status_match.group(1)
        status = status_match.group(2)
        if status == "benchmarked":
            block = block.replace(
                f'\n{indent}"status": "benchmarked"',
                f'\n{indent}"status": "verified"',
                1,
            )
            status = "verified"
        if status == "in-progress" and number == 482:
            block = block.replace(
                f'\n{indent}"status": "in-progress"',
                f'\n{indent}"status": "verified"',
                1,
            )
            status = "verified"
        repros = PROGRAM_REPRODUCTIONS.get(number)
        if repros is not None:
            block = block.replace(
                f'\n{indent}"local_reproductions": []',
                f'\n{indent}"local_reproductions": {json.dumps(repros)}',
                1,
            )
        baseline = REGRESSION_BASELINE.get(number, REGRESSION_BASELINE["default"])
        fixing = DIRECT_FIXING.get(number, [PROGRAM_SERIES])
        baseline_json = "null" if baseline is None else f'"{baseline}"'
        fixing_json = json.dumps(fixing)
        insertion = (
            f'\n{indent}"failing_baseline_commit": {baseline_json},'
            f'\n{indent}"fixing_commits": {fixing_json},'
        )
        block = block.replace(
            f'\n{indent}"status": "{status}"',
            insertion + f'\n{indent}"status": "{status}"',
            1,
        )
        text = text[:obj_start] + block + text[obj_end:]

    program = {
        "issue": 521,
        "url": "https://github.com/orneryd/NornicDB/issues/521",
        "title": "Converge Cypher execution",
        "openspec": "openspec/changes/archive/2026-09-27-converge-cypher-execution",
        "fixing_commit_series_note": (
            f"Entries whose fixing_commits contains the marker "
            f"{PROGRAM_SERIES!r} were fixed by the #521 convergence commit "
            "series between the failing baseline commit and qualification; "
            "their local_reproductions are the passing artifacts."
        ),
        "qualification": QUALIFICATION,
    }
    program_text = json.dumps(program, indent=2)[:-1]
    idx = text.rstrip().rfind("}")
    text = text[:idx] + ',\n  "program": ' + program_text + "}" + text[idx:]

    with open(PATH, "w", encoding="utf-8") as fh:
        fh.write(text)

    check = json.load(open(PATH, encoding="utf-8"))
    assert "program" in check
    print(f"updated {PATH} (formatting preserved); program key added")
    return 0


if __name__ == "__main__":
    sys.exit(main())
