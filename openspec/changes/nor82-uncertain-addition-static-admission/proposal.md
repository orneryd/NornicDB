# NOR-82 / N152-149: static admission of an uncertain addition read by head/toUpper

Issue: `NOR-82` ("Preserve reference type checks for uncertain addition results"),
ready card `N152-149`, approved ticket `/tmp/n152-149-ticket.json`
(`a18ab909-185d-4a7e-ab72-e9555816d767`, sequence 149). Stage: reproduction and
extraction only. No production behavior is proposed as decided here; the parent
owns design acceptance.

## Problem and contract

Neo4j 5.26.30 rejects this statement when it compiles it, in RUN, EXPLAIN and
PROFILE, before any data is read:

```
UNWIND [] AS s MATCH (n) RETURN toUpper(head(n.k + date())) AS a
Type mismatch: expected String but was Any or Date
```

The reconstructed variant with `duration('P1D')` is rejected the same way
(`expected String but was Any or Duration`). NornicDB on the reproduced source
accepts both, in both parsers, in auto-commit and in an explicit transaction,
for RUN, EXPLAIN and PROFILE: 24 cells that should reject, out of the 48 NOR-82
cells. The two reconstructed additions `n.k + []` and `n.k + n.j` are accepted by
the same reference run, so they are corrected acceptance controls, not defects
(24 matching cells).

The contract under test is narrow: the compile-time verdict for this probe
family must equal Neo4j 5.26.30's for both parsers, auto-commit and explicit
transactions, and RUN/EXPLAIN/PROFILE; the two acceptance controls must stay
accepted. Runtime semantics on non-empty data, plan contents and the sibling
issues are outside this card.

## Why the card is retained instead of closed

The difference is reproduced on unmodified source and is a missing rejection
(acceptance where the reference rejects). The sibling failures in the same
matrix are the opposite direction (false rejections) and are separate:
NOR-79 `parameter_list_comprehension_subscript`, NOR-80
`delete_case_count_subquery`, NOR-81 `any_over_timestamp` (12 cells each).
NOR-82 does not inherit, fix or excuse any of them.

## Implementation area census (reuse only; parent decides)

The behavior in question already has one owner; no second type checker and no
broad temporal/arithmetic rewrite is implied.

- `pkg/cypher/static_operator_types.go` — `checkOperator` (134),
  `checkOperatorAlternatives` (180), `checkSingleOperator` (224) and its `+`
  rules (the `left.temporal()` branch at 250 names what a temporal operand
  admits), `staticAdditionWithUnknown` (296), `additionResultTypes` (312),
  `temporalListUnion` (157) as the in-owner precedent for yielding `Any`.
- `pkg/cypher/static_operand.go` — the operand type and its consumer methods:
  `elementOperand` (360), `listPart` (376), `elementOfType` (562),
  `derivedFunctionOperand` (859) for `head`.
- `pkg/cypher/static_function_argument_types.go` — the consumer check
  `validateStaticFunctionVariablesTyped` (817) and
  `staticArgumentType.accepts` (995) for `toUpper`.

The parent chooses where the correction belongs. Two facts bound the choice: the
reference reports its mismatch at `toUpper` (so its `head(...)` result type is
the union `Any or Date`), and NornicDB's chain loses the non-list alternative
because `head` of the union `Date or List<T>` reads only the list alternative
and types its element as unknown. Recording this is analysis, not acceptance.

Deletion targets: none. Compatibility risk: over-rejecting the two acceptance
controls (`n.k + []`, `n.k + n.j`) and other `n.k + <temporal>` consumers that
Neo4j accepts.

## Evidence and limits

`evidence.md` holds the extracted fixture, the recorded cell matrix, the recorded
reference transcript for the four NOR-82 probes, and the source-level trace with
limits. The reference is the ticket's recorded Neo4j 5.26.30 run; this card takes
no fresh reference measurement and runs no build, test or benchmark. The 64
inherited H2 false accepts the issue's acceptance criterion names are still
unrecovered, so no claim is made about other uncertain-string consumers. The
configured code graph and duplicate tooling were unavailable (MCP handshake
timeouts), so owner reuse is a source-level census, not an indexed verdict.
