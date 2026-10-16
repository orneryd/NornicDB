# Local ANTLR Go runtime replacement

This directory starts from `github.com/antlr4-go/antlr/v4` version `v4.13.1`,
upstream tag commit `4d7e18847d8881a176db3d7cca909e3f3942a05f`.
The official Go module archive SHA256 is
`2fb455ab1f46e53a3cfa678306a6e405f291b34858222db9240335c018ae9554`.
The upstream module sum is
`h1:SqQKkuVZ+zWkMMNkjy5FZe5mr5WURWnlpmOuzYWrPrQ=`;
this sum identifies the upstream archive, not these modified local bytes.
`UPSTREAM_SHA256SUMS` identifies every retained original file before edits.
The upstream BSD license and source notices are retained in `LICENSE` and files.

Local changes implement optional, simulator-owned cancellation work callbacks,
bounded collection storage and complete-only cache publication. The callback
borrows the statement's existing cancellation budget. It is not stored in shared
ATN, DFA, prediction contexts, collection comparators or published configurations.
No generated grammar source is changed. Nil callers use the same storage owners.

The `antlr.stats` build tag has a conditional limitation: upstream statistics
registration can copy its global accumulated prefix. Normal `nostatistics`
responsiveness evidence does not establish a bound for statistics-tag builds.
Native allocation, GC and scheduler pauses are measured, not claimed preemptible.

Implementation and validation are in progress. This is not an acceptance report.
