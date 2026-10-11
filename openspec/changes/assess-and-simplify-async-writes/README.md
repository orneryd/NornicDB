# Assess and simplify the async write path

OpenSpec change proposal: decide, from measurements, whether the `AsyncEngine`
write cache buys anything on the write path — and if it does not, remove it and
the shape/schema-based routing, then tune the single transactional write path
(Badger + WAL) toward Memgraph-scale ingest.

Companion plan (owner draft): `docs/plans/always-async-writes-plan.md` (the
"keep async, make it correct" alternative). This change is its **step 0** plus
the removal decision gate; the two are alternatives, not a sequence.

- [proposal.md](proposal.md) — why, what changes, impact.
- [design.md](design.md) — routing inventory, measurement matrix, decision
  gates, removal and tuning designs, risks.
- [tasks.md](tasks.md) — measurement-first tasks; implementation tasks gated on
  results.
- [specs/write-path-routing/spec.md](specs/write-path-routing/spec.md) —
  candidate observable requirements the chosen path must meet.
