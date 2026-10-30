// Author: #1683 lane (Claude), 2026-10-30. Scratch-only diagnostic seam; never on the PR branch.
package storage

// NOR148DiagCommitHook, when set, sees every finished commit timestamp, whether
// it was marked reader-neutral, and the finishing goroutine's stack.
var NOR148DiagCommitHook func(ts uint64, neutral bool, stack string)
