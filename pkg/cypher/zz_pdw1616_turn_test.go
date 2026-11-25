package cypher

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const pdw1616Turn = "MERGE (t:PDTurn {id: $id}) SET t.taken = coalesce(t.taken, 0) + 1"

func TestPDW1616TurnWait(t *testing.T) {
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:       t.TempDir(),
		EngineOptions: storage.EngineOptions{LockWait: storage.LockWaitOptions{Timeout: 30 * time.Second}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	store := storage.NewNamespacedEngine(engine, "test")
	exec := NewStorageExecutor(store)
	ctx := t.Context()
	run := func(e *StorageExecutor, c context.Context, q string, p map[string]any) *ExecuteResult {
		r, err := e.Execute(c, q, p)
		require.NoError(t, err)
		return r
	}
	run(exec, ctx, "CREATE CONSTRAINT pdw_turn FOR (t:PDTurn) REQUIRE t.id IS UNIQUE", nil)

	cases := []struct {
		name, end        string
		delay            time.Duration
		waiters, writers int
		seed, timeout    bool
	}{
		{"short-commit", "COMMIT", 25 * time.Millisecond, 1, 0, true, false},
		{"short-rollback", "ROLLBACK", 25 * time.Millisecond, 1, 0, false, false},
		{"medium-commit-new", "COMMIT", 300 * time.Millisecond, 1, 0, false, false},
		{"medium-rollback-existing", "ROLLBACK", 300 * time.Millisecond, 1, 0, true, false},
		{"writers-commit", "COMMIT", 2 * time.Second, 4, 2, true, false},
		{"writers-rollback", "ROLLBACK", 2 * time.Second, 4, 2, true, false},
		{"long-commit", "COMMIT", 28 * time.Second, 1, 0, true, false},
		{"long-rollback", "ROLLBACK", 28 * time.Second, 1, 0, true, false},
		{"timeout", "ROLLBACK", 31 * time.Second, 1, 0, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run(exec, ctx, pdw1616Turn, map[string]any{"id": tc.name + "-baseline"})
			p := map[string]any{"id": tc.name}
			if tc.seed {
				run(exec, ctx, pdw1616Turn, p)
			}
			holder := NewStorageExecutor(store)
			run(holder, WithClientTransactionTimeout(ctx), "BEGIN", nil)
			run(holder, ctx, pdw1616Turn, p)
			held := time.Now()

			// Writers use private keys. Keep another transaction open across the
			// release, then end it only after all same-key contenders finish.
			other := NewStorageExecutor(store)
			run(other, WithClientTransactionTimeout(ctx), "BEGIN", nil)
			run(other, ctx, pdw1616Turn, map[string]any{"id": tc.name + "-unrelated-open"})
			finished := false
			defer func() {
				if !finished {
					_, _ = holder.Execute(context.Background(), "ROLLBACK", nil)
					_, _ = other.Execute(context.Background(), "ROLLBACK", nil)
				}
			}()

			var wg sync.WaitGroup
			var commits, failures atomic.Int64
			stop, cancel := context.WithCancel(ctx)
			defer cancel()
			for i := 0; i < tc.writers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					w := NewStorageExecutor(store)
					writerP := map[string]any{"id": fmt.Sprintf("%s-writer-%d", tc.name, i)}
					for stop.Err() == nil {
						if _, err := w.Execute(ctx, pdw1616Turn, writerP); err != nil {
							failures.Add(1)
						} else {
							commits.Add(1)
						}
						time.Sleep(time.Millisecond)
					}
				}(i)
			}

			type outcome struct {
				id                     int
				start, mergeDone, done time.Time
				mergeErr, endErr       error
			}
			results := make(chan outcome, tc.waiters)
			waiting := make(chan struct{}, 256)
			restore := storage.SetUniqueKeyLockWaitHookForTesting(func() {
				select {
				case waiting <- struct{}{}:
				default:
				}
			})
			defer restore()
			for i := 0; i < tc.waiters; i++ {
				go func(i int) {
					c := NewStorageExecutor(store)
					_, err := c.Execute(WithClientTransactionTimeout(ctx), "BEGIN", nil)
					out := outcome{id: i, start: time.Now(), mergeErr: err}
					if err == nil {
						_, out.mergeErr = c.Execute(ctx, pdw1616Turn, p)
					}
					out.mergeDone = time.Now()
					if out.mergeErr == nil {
						_, out.endErr = c.Execute(ctx, "COMMIT", nil)
					} else {
						_, out.endErr = c.Execute(ctx, "ROLLBACK", nil)
					}
					out.done = time.Now()
					results <- out
				}(i)
			}
			for i := 0; i < tc.waiters; i++ {
				select {
				case <-waiting:
				case <-time.After(5 * time.Second):
					t.Fatal("contender did not reach the storage wait hook")
				}
			}
			enrolled := time.Now()
			// Compare a unique key while the same-key holder is still open.
			unrelatedStart := time.Now()
			run(exec, ctx, pdw1616Turn, map[string]any{"id": tc.name + "-unrelated-probe"})
			unrelated := time.Since(unrelatedStart)
			time.Sleep(time.Until(enrolled.Add(tc.delay)))
			releaseStart := time.Now()
			run(holder, ctx, tc.end, nil)
			released := time.Now()
			for i := 0; i < tc.waiters; i++ {
				select {
				case out := <-results:
					t.Logf("PDW1616 turn=%s D=%v end=%s contender=%d writers=%d holderHeld=%v overlapToReleaseStart=%v releaseCall=%v merge=%v afterReleaseReturn=%v contenderEnd=%v unrelated=%v mergeErr=%v endErr=%v",
						tc.name, tc.delay, tc.end, out.id, tc.writers, releaseStart.Sub(held), releaseStart.Sub(out.start),
						released.Sub(releaseStart), out.mergeDone.Sub(out.start), out.mergeDone.Sub(released), out.done.Sub(out.mergeDone),
						unrelated, out.mergeErr, out.endErr)
					if tc.timeout {
						require.ErrorIs(t, out.mergeErr, storage.ErrLockWaitTimeout)
					} else {
						require.NoError(t, out.mergeErr)
						require.NoError(t, out.endErr)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("contender did not finish after holder ended")
				}
			}
			cancel()
			wg.Wait()
			run(other, ctx, "ROLLBACK", nil)
			finished = true
			want := int64(tc.waiters)
			if tc.seed {
				want++
			}
			if tc.end == "COMMIT" {
				want++
			}
			if tc.timeout {
				want -= int64(tc.waiters)
			}
			r := run(exec, ctx, "MATCH (t:PDTurn {id: $id}) RETURN t.taken", p)
			require.Equal(t, [][]any{{want}}, r.Rows)
			t.Logf("PDW1616 turn=%s writerCommits=%d writerFailures=%d taken=%d", tc.name, commits.Load(), failures.Load(), want)
		})
	}
}
