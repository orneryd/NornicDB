## Integration Decision

FGAC (#935) owns the canonical persisted privilege records and evaluator. F-PRIV
provides its complete operation-classification/admission boundary. They converge
as one implementation; do not build a second permissions engine or preserve
the old RBAC model or checks as alternative authorities. The 1.5.0 cutover
replaces them together. Define and verify the approved migration of existing
role/allowlist/database privilege records through the canonical migration owner
already required by the enterprise plan; do not invent privilege mappings or
keep dual evaluators to avoid making that decision. Protocol parsing and error
rendering remain transport concerns; classification and permission meaning have one owner.

## Population and Classification

Refresh the executable inventory of commands, procedures, functions and
transport routes against the current integration base. Include aliases, parser
variants, native fast paths, nested execution, impersonation, and operations
registered at runtime. Record each operation's class/effects, canonical evaluator
entry, consumers and regression evidence. Distinguish an explicit reviewed class
from a blanket default that merely assigns every operation read permission.
Unclassified operations are denied until explicitly classified and authorized.

Keep the supplied 1,669-operation inventory as historical failing evidence.
Reconcile additions, removals and changed classifications after #1350/#1364;
new counts must not silently replace or be presented as the old baseline.

## Shared Boundaries and Deletion Targets

- Every transport admits the executing principal through the same canonical
  contract before database resolution/initialization can disclose or change
  inaccessible state. Preserve the existing supported unauthenticated-mode
  contract; this work does not invent a new authentication policy.
- Commands/callables carry reviewed effects through normal and nested execution;
  path-selector execution and `WRITE_RECORD` use those same effects. Rewritten
  internal execution must preserve the accepted client/internal boundary.
- Graph visibility follows #935 across queries, paths, functions, procedures,
  indexed search and caches. Implement the requested commit-time visibility
  validation at the transaction owner; pin its exact timing, observed policy
  revision and rollback effects from PD's requirement and failing evidence
  before implementation. A statement-entry check alone is insufficient proof.
- Own-transaction termination is classified separately from general graph reads
  and from terminating another principal's transaction. Pin supported Neo4j
  Enterprise behavior in the reference harness.
- Replace the scattered classification/check branches and direct routes that
  bypass the canonical evaluator. Identify exact deletion sites from the new
  inventory; preserve transport adapters that translate the same contract.

## Review and Proof

The full family remains the review unit under Rule 73. Preserve both prior review
reports and their failing probes, resolve every retained finding, then review the
integrated source once as a whole. A passing helper test, one transport, a branch
review, static inventory coverage or one corrected P1 does not establish family
acceptance. If that integrated correction fails review, follow Rule 73 rather
than presenting another leaf-only correction as complete.

Evidence must identify immutable head/base, corpus of operations, public-path
regressions, parser and transaction modes, reference version, skips and actual
results. Authorization tests must assert both returned data/errors and persistent
side effects in a fresh session. Measure affected hot-path latency/allocation
against the refreshed workload baseline. A spec-only edit runs document checks;
it does not claim implementation or security acceptance.
