# Repair release verification baselines

## Problem

Release verification records an immutable comparison base, but the documentation corpus tests read the moving origin/main ref instead. A later upstream documentation commit can fail an otherwise unchanged recorded candidate. The prune layout test also assumes that measuring preparation work guarantees CPU time for a later budgeted pass, contradicting the supported zero-work outcome after an admission wait exhausts that budget.

## Correction

- Use the existing VERIFY_BASE supplied by make verify for documentation retention and inventory checks; direct npm tests retain their origin/main default. Invalid explicit bases fail visibly.
- Require a zero-progress prune cycle to have consumed its runtime budget and preserve a monotonic cursor. Keep bounded completion, exact deletion totals and final retention checks. Production pruning and budgets remain unchanged.
- Exercise moved upstream refs, genuinely missing baseline documentation, invalid refs, normal pruning and controlled budget/admission behavior. Retain negative controls showing early no-work returns and permanent starvation are rejected.

## Scope

Supports NOR-10 and the release/v1.5.0 verification gate. No runtime API, storage format, checker, branch history or repository policy changes. Integrated CI on the final PR head is separate from local focused evidence.
