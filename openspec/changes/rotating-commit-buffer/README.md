# Rotating commit buffer (write-behind at the Badger commit path)

OpenSpec change proposal: replace the removed write-behind cache with a
rotating **commit buffer** at the single choke point where all Cypher writes
land today — `BadgerEngine`'s transaction commit.

- [proposal.md](proposal.md) — why, what changes, impact.
- [design.md](design.md) — architecture, rotation algorithm, correctness
  contract, decisions.
- [tasks.md](tasks.md) — ordered implementation tasks.
