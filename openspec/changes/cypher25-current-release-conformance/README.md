# cypher25-current-release-conformance

## Outcome and scope

NornicDB currently rejects `CYPHER 25` and has additional semantic gaps.
The reference target is Neo4j 2026.09.0; current Enterprise differential
acceptance has not been run.

Implement new features inline in the existing pipeline with one shared grammar.
There is no language default setting and no header is required: `CYPHER 5` /
`CYPHER 25` headers are optional and accepted directly by both parsers.
The SRD parser accepts `LET`/`FILTER`/`FOR` and correlated unscoped `CALL`
without a header; the ANTLR parser keeps the strict Cypher 5.26 contract
(those additive clauses need `CYPHER 25`) while consuming the preamble itself.
Preserve existing queries/APIs; no automatic default cutover, persisted
database-language migration, default-language DDL, separate executor or
retry-as-5. Retained constructs removed upstream are documented NornicDB
extensions. The first implemented slice is this shared grammar foundation;
the remaining inventory in [tasks](tasks.md) stays open.

## Artifacts

- [Proposal](proposal.md): scope and dependencies.
- [Audit](audit.md): code-backed findings, 43 cases per parser, release matrix.
- [Source research and equivalent 5/25 queries](language-selection-research.md):
  pinned public source; requested local checkout was unavailable.
- [Design](design.md): shared implementation surfaces and acceptance.
- [Tasks](tasks.md): dependency-ordered, unchecked implementation checklist.
- [Language selection spec](specs/cypher-language-selection/spec.md).
- [Query semantics spec](specs/cypher-current-query-semantics/spec.md).
- [Values/schema/search spec](specs/cypher-current-values-schema-search/spec.md).
- [Release conformance spec](specs/cypher-release-conformance/spec.md).
- [Probe source](evidence/probe.go.txt) and [raw results](evidence/probe-results.jsonl).

## Evidence and integration

The existing framing regression passes because it expects 25 rejection.
The probe produced 86 executions: 70 errors, 16 result-returning executions,
including two wrong PROPERTY_EXISTS results. These are gap probes, not a
coverage percentage. Source-derived equivalent examples have not been run
against a live Neo4j server.

Extend, rather than duplicate, the [Enterprise CDC/impersonation/Query API plan](../enterprise-cdc-impersonation-query-api/README.md)
for current server APIs. Its 5.26.30 oracle is distinct from this target.
Dependencies include #935, #936, #937 and #938.

Validate this plan:

```sh
OPENSPEC_TELEMETRY=0 npx --yes @fission-ai/openspec validate cypher25-current-release-conformance --strict
```

All remaining implementation tasks stay unchecked until independently
verified. The shared grammar makes new syntax available unprefixed through the
SRD parser; the optional `CYPHER 5` / `CYPHER 25` header still works on both
parsers.
