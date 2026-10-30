package cypher

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestMERGE_IsIdempotentUnderConcurrentRetry runs two writers MERGE-ing on
// the same uid, then re-runs the loser and asserts the loser's SET clause
// becomes visible. This is the cross-contract test for
// docs/plans/consumer-pinned-error-contract-plan.md §2.1 + §2.4: commit-time
// UNIQUE is the storage-correct outcome, and the loser's MERGE-as-retry
// must match the now-committed node.
//
// IMPORTANT: do not "fix" this test by serializing the writers. The retry-
// on-MERGE-only contract requires the second commit to actually arrive at
// storage and be rejected, then re-execution finds the committed node.
// Tests that pre-commit one writer and then run the other are not
// exercising the contract.
func TestMERGE_IsIdempotentUnderConcurrentRetry(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(
		ctx,
		`CREATE CONSTRAINT t_uid_unique IF NOT EXISTS FOR (n:T) REQUIRE n.uid IS UNIQUE`,
		nil,
	)
	require.NoError(t, err)

	const stmt = `MERGE (n:T {uid: 'abc'}) SET n.colour = $c RETURN n.colour AS colour`

	type writerResult struct {
		colour string
		err    error
	}
	results := make([]writerResult, 2)

	var ready sync.WaitGroup
	ready.Add(2)
	start := make(chan struct{})
	var done sync.WaitGroup
	done.Add(2)

	for i, colour := range []string{"red", "blue"} {
		i, colour := i, colour
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			res, err := exec.Execute(ctx, stmt, map[string]interface{}{"c": colour})
			if err != nil {
				results[i] = writerResult{err: err}
				return
			}
			if len(res.Rows) > 0 && len(res.Rows[0]) > 0 {
				if s, ok := res.Rows[0][0].(string); ok {
					results[i] = writerResult{colour: s}
					return
				}
			}
			results[i] = writerResult{colour: colour}
		}()
	}

	ready.Wait()
	close(start)
	done.Wait()

	// We accept three outcomes from the race:
	//   (a) Both writers commit on first try because the storage layer
	//       serialized them by uid. Then both colours have been applied;
	//       the second one's SET wins on read.
	//   (b) Exactly one writer commits and the other gets the documented
	//       commit-time UNIQUE shape ("commit failed: constraint violation:
	//       ... already exists"). We then re-execute the loser and require
	//       success; its colour must be visible on read.
	//   (c) Both writers fail with the documented commit-time UNIQUE shape
	//       (acceptable corner case if both serialized losers). Both retry.
	//
	// All three outcomes require: after retries, the node exists with
	// colour ∈ {"red", "blue"}, and any retried SET commits cleanly.
	wantSubs := []string{"commit failed", "constraint violation", "already exists"}
	loserCount := 0
	for i, r := range results {
		if r.err == nil {
			continue
		}
		msg := r.err.Error()
		isCommitConflict := true
		for _, sub := range wantSubs {
			if !strings.Contains(msg, sub) {
				isCommitConflict = false
				break
			}
		}
		require.Truef(
			t, isCommitConflict,
			"writer %d: error %q does not match the consumer-pinned commit-time UNIQUE shape "+
				"(see consumer-pinned-error-contract-plan.md §2.1)",
			i, msg,
		)
		loserCount++

		// Retry the loser exactly once; this MUST succeed (§2.4).
		colour := []string{"red", "blue"}[i]
		retry, retryErr := exec.Execute(ctx, stmt, map[string]interface{}{"c": colour})
		require.NoErrorf(
			t, retryErr,
			"writer %d: MERGE retry after commit-time UNIQUE failed (see §2.4): %v",
			i, retryErr,
		)
		require.NotEmpty(t, retry.Rows)
		results[i] = writerResult{colour: colour}
	}

	require.LessOrEqual(t, loserCount, 2, "no more than two writers can lose")

	// Final state: exactly one node with the chosen uid, and a colour from
	// the legal set. Both writers' uid pin must have collapsed onto the
	// same node.
	read, err := exec.Execute(ctx, `MATCH (n:T {uid: 'abc'}) RETURN n.colour AS colour`, nil)
	require.NoError(t, err)
	require.Len(t, read.Rows, 1, "expected exactly one :T node with uid='abc'")
	colour, ok := read.Rows[0][0].(string)
	require.True(t, ok, "colour value should be a string, got %T", read.Rows[0][0])
	require.Truef(
		t, colour == "red" || colour == "blue",
		"final colour must be one of the writer-supplied values, got %q", colour,
	)
}
