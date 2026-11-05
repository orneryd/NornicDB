## Why

[FGAC #935](https://github.com/orneryd/NornicDB/issues/935) needs one privilege
model across all execution paths. F-PRIV addresses the same causal problem:
scattered ad-hoc checks replace a complete operation classification, leaving
wrongly classified and unchecked operations. The maintainer directed that this
refactor be performed with FGAC rather than as a separate partial correction.

The supplied 17 October 2026 family inventory recorded **1,669 operations**:
227 correctly checked, 319 misclassified, 1,120 unchecked, and 3 unknown because
they are installed at runtime. These are historical inventory counts, not a
fresh audit of this branch and not evidence that any operation is fixed.

## What Changes

- Make complete command, procedure, function and transport-route classification
  a shared FGAC enforcement foundation. Operations without an explicit reviewed
  class are denied; runtime registration cannot escape this rule.
- Replace the old RBAC model and checks with the canonical privilege
  records/evaluator used by FGAC in the same 1.5.0 convergence effort, including
  executing-principal identity, graph visibility, operation effects, transaction
  handling and transport admission. Remove replaced checks
  rather than retaining competing authorities.
- Carry all unresolved Codex and Daybreak family-review findings, including the
  reported Daybreak P1 on #1393, into the integrated acceptance boundary.
- Include PD's requested commit-time visibility check, path-selector and
  `WRITE_RECORD` corrections, HTTP database non-disclosure/admission ordering,
  own-transaction termination semantics, and index/APOC-export classifications.
- Refresh the inventory and rebase the implementation route onto current
  `develop`, including changes from merged #1350 and #1364.

## Recorded Status and Scheduling

As supplied by the maintainer on 17 October, both last family reviews found
problems. F-PRIV is blocked under Rule 73 pending a complete root-cause
correction and passing whole-family review. The existing stacked proposals
[#1352](https://github.com/orneryd/NornicDB/pull/1352) and
[#1393](https://github.com/orneryd/NornicDB/pull/1393) are inputs, not independent
merge approvals. **#1352 does not merge alone.**

The follow-up maintainer conversation sets the planned release to **1.5.0**,
with FGAC and replacement of the old RBAC model/checks together. Requirements
previously described as 1.4.5 follow-ups are carried into this combined scope.
Implementation resumes after 1.4.4 ships, as 1.5.0/FGAC work. This spec addition
changes neither the 1.4.4 release contents nor the recorded unresolved status.

## Capabilities and Impact

New capability: `privilege-enforcement`, the shared admission/classification
contract consumed by #935. Existing FGAC requirements remain in force, including
TRAVERSE/READ/MATCH, deny-wins, property rules, writes, native graph/search paths,
cache isolation, administration and impersonation. Collection-limited access
alone does not satisfy the full #935 contract.

Affected implementation families include `pkg/auth`, `pkg/cypher`, `pkg/bolt`,
`pkg/server`, native GraphQL/MCP/Qdrant routes, dynamic callable registration,
transaction commit/termination and the dependent UI/impersonation plans. Exact
operations and owners must come from the refreshed inventory, not this list.
No runtime code or privilege migration is implemented by this change.
