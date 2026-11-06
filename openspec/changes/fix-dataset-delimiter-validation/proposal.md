# Correct offline dataset delimiter validation

## Problem

On current release `8f5835d7f`, the public dataset-validator CLI rejects
`RETURN '{' AS x` and a brace inside a line comment with exit status 1.
`validate_cypher_syntax` compares raw character counts, so content in a quoted
extent is incorrectly treated as Cypher structure. The neighboring parentheses
and brackets checks have the same reproduced defect.

## Proposed outcome

Quoted and comment content must not cause the approved delimiter check to fail.
Actual structural imbalance must still produce the existing actionable line
message and nonzero exit status. Preserve JSONL handling, report formatting and
all other quality checks. This remains a bounded offline validation aid, not a
full Cypher parser or a replacement for database query admission.

The maintainer approved all three delimiter families in v1.4.4. NOR-406 is a
separate retrieval-profiling investigation with different owners and evidence
requirements; it is not combined here.

## Verification

Use deterministic scanner controls and the real JSONL CLI in temporary
fixtures that are removed on exit. Check single/double/backtick quoted extents,
supported escapes, comments, actual imbalance and mixed-record line attribution
within the approved scope. Full make-test and advisory duplicate analysis now
run in CI by maintainer policy; no local full-suite push gate is claimed.
