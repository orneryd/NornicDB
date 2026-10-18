# Parked graph-expression evidence

These are historical/probe source snapshots and raw evidence, not additional build packages.
Go source under this evidence directory has a `.go.txt` suffix so `go test ./...` does not discover archived packages. Remove only that final `.txt` when restoring a selected snapshot into its original test/source path. Every snapshot keeps its base.txt and tracked-changes.patch; the original local worktrees are retained unchanged.

The live develop probe locations are the normal pkg/cypher, pkg/server, testing/cypher/tck and scripts/cypher-tck/differential paths in this WIP branch. The in-progress production code in those paths is unfinished and untested, and depends on #1385's static-owner API plus its original-comprehension-form extension. Do not merge or move #1468 to this WIP head.

The old queue-priority/stop scripts in the historical evidence were used in earlier rounds, are NOT a restart procedure, and must not be run. Never hold, move or remove another agent's jobs. On resumption submit only new necessary work in normal queue order.

The primary current parking record is ../park-1.5.0-1468.md. The old ready-r2-history and gate-stop statuses are historical and superseded; the integrated fix has not passed verification or review.