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

// Retain both of the reviewer's probes: the normal manager wrappers and the
// supported native namespace view, which does not consult the manager mutex.
func TestTypedCountDropWhileOtherDatabaseWrites(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprintf("native=%v", native), func(t *testing.T) {
			dir := t.TempDir()
			f := openTypedCountScope(t, dir, true)
			for _, name := range []string{"scope_drop", "scope_busy"} {
				typedCountRun(t, f.admin, "CREATE DATABASE "+name)
			}
			typedCountSeed(t, f.executor(t, "scope_drop"), 1)
			typedCountSeed(t, f.executor(t, "scope_busy"), 2)
			var wg sync.WaitGroup
			var stop atomic.Bool
			var ack atomic.Int64
			errs := make(chan error, 8)
			stopWriters := func() { stop.Store(true); wg.Wait() }
			defer stopWriters()
			for i := 0; i < 8; i++ {
				e := f.executor(t, "scope_busy")
				if native {
					e = NewStorageExecutor(storage.NewNamespacedEngine(f.engine, "scope_busy"))
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					for !stop.Load() {
						_, err := e.Execute(context.Background(), "CREATE (:Busy)", nil)
						if err != nil {
							errs <- err
							return
						}
						ack.Add(1)
					}
				}()
			}
			deadline := time.Now().Add(5 * time.Second)
			for ack.Load() < 40 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			require.GreaterOrEqual(t, ack.Load(), int64(40), "writers must be active before DROP")
			before := ack.Load()
			type dropResult struct {
				err     error
				elapsed time.Duration
			}
			finished := make(chan dropResult, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go func() {
				started := time.Now()
				_, err := f.admin.Execute(ctx, "DROP DATABASE scope_drop", nil)
				finished <- dropResult{err, time.Since(started)}
			}()
			var result dropResult
			starved := false
			select {
			case result = <-finished:
			case <-time.After(5 * time.Second):
				starved = true
			}
			// The completion observation happens before stopping the unrelated writers.
			atCompletion := ack.Load()
			stopWriters()
			if starved {
				select {
				case result = <-finished:
				case <-time.After(10 * time.Second):
					t.Fatal("DROP remains blocked after writers stopped")
				}
			}
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}
			t.Logf("DROP latency=%s; native=%v; B ACKs before=%d at completion=%d total=%d; stopped-to-finish=%v", result.elapsed, native, before, atCompletion, ack.Load(), starved)
			require.NoError(t, result.err)
			require.False(t, starved, "DROP must finish while B writers are still running")
			typedCountRun(t, f.admin, "CREATE DATABASE scope_drop")
			typedCountShapes(t, f.executor(t, "scope_drop"), 0)
			busy := f.executor(t, "scope_busy")
			typedCountWant(t, busy, "MATCH (n:Busy) RETURN count(n) AS n", ack.Load())
			typedCountWant(t, busy, "MATCH (n) RETURN count(n) AS n", ack.Load()+2)
			typedCountShapes(t, busy, 2)
			f.close(t)
			f = openTypedCountScope(t, dir, true)
			typedCountShapes(t, f.executor(t, "scope_drop"), 0)
			typedCountWant(t, f.executor(t, "scope_busy"), "MATCH (n:Busy) RETURN count(n) AS n", ack.Load())
			typedCountShapes(t, f.executor(t, "scope_busy"), 2)
		})
	}
}
