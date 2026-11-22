package bolt

// Batch 4b, 48.8 (untracked; never committed): TestBoltApplicationMemoryReclaimsCompletedWork's
// body, repeated 300 times at each of two load levels in one process. When
// Reserved() is not zero at the test's read, the probe logs: the value; the
// goroutines then alive that have a frame in pkg/bolt or pkg/memory (deduplicated
// by their stack, with counts), which are the server work still holding
// reservations (the policy keeps no list of leases, so a lease's owner and size
// cannot be read from it); and how long Reserved() takes to reach zero after
// the read (polled every 1 ms, up to 2 s), with the values seen on the way.
// Load levels: "idle" (nothing else running) and "busy" (2 x GOMAXPROCS
// goroutines spinning for the whole level).

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/testutil/memorypressure"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func b4bReclaimOnce(t *testing.T) (int64, func() int64, error) {
	store := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = store.Close() })
	executor := &cypherQueryExecutor{executor: cypher.NewStorageExecutor(store)}
	_, err := executor.Execute(context.Background(), "RETURN 1 AS ok", nil)
	require.NoError(t, err)
	server := New(&Config{Port: 0, MaxConnections: 4, ReadBufferSize: 8192, WriteBufferSize: 8192}, executor)
	policy := memorypressure.New(t)
	require.NoError(t, policy.Refresh())
	work, err := policy.Reserve(64 << 10)
	require.NoError(t, err)
	memorypressure.Garbage()
	work.Release()
	require.NoError(t, policy.Refresh())
	server.SetMemoryPolicy(policy)
	port := serveBoltIntegrationServer(t, server)
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	query := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		session := driver.NewSession(ctx, neo4j.SessionConfig{})
		defer session.Close(ctx)
		result, err := session.Run(ctx, "RETURN 1 AS ok", nil)
		if err != nil {
			return err
		}
		records, err := result.Collect(ctx)
		if err == nil && (len(records) != 1 || records[0].Values[0] != int64(1)) {
			return fmt.Errorf("unexpected records: %v", records)
		}
		return err
	}
	err = query()
	var neoErr *neo4j.Neo4jError
	require.ErrorAs(t, err, &neoErr)
	require.Equal(t, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", neoErr.Code)
	ctx, cancel := context.WithCancel(context.Background())
	ticks := time.NewTicker(25 * time.Millisecond)
	done := make(chan struct{})
	go func() { defer close(done); policy.Run(ctx, ticks.C) }()
	t.Cleanup(func() { cancel(); ticks.Stop(); <-done })
	deadline := time.Now().Add(3 * time.Second)
	for {
		err = query()
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	return policy.Reserved(), policy.Reserved, err
}

func b4bServerGoroutines() []string {
	buf := make([]byte, 8<<20)
	n := runtime.Stack(buf, true)
	counts := map[string]int{}
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		if !strings.Contains(g, "nornicdb/pkg/bolt.") && !strings.Contains(g, "nornicdb/pkg/memory.") {
			continue
		}
		if strings.Contains(g, "b4bServerGoroutines") || strings.Contains(g, "TestB4bReclaim") {
			continue
		}
		var frames []string
		for _, line := range strings.Split(g, "\n")[1:] {
			if strings.HasPrefix(line, "\t") {
				continue
			}
			line = strings.TrimSpace(line)
			if strings.HasSuffix(line, ")") {
				if i := strings.LastIndex(line, "("); i > 0 {
					line = line[:i]
				}
			}
			if strings.HasPrefix(line, "created by ") {
				if i := strings.Index(line, " in goroutine"); i > 0 {
					line = line[:i]
				}
			}
			frames = append(frames, strings.TrimPrefix(line, "github.com/orneryd/nornicdb/"))
			if len(frames) == 16 {
				break
			}
		}
		state := strings.SplitN(strings.SplitN(g, "\n", 2)[0], "[", 2)
		key := strings.Join(frames, " < ")
		if len(state) == 2 {
			key = "[" + state[1] + " " + key
		}
		counts[key]++
	}
	var out []string
	for k, c := range counts {
		out = append(out, fmt.Sprintf("x%d %s", c, k))
	}
	sort.Strings(out)
	return out
}

func TestB4bReclaim(t *testing.T) {
	for _, level := range []string{"idle", "busy"} {
		t.Run(level, func(t *testing.T) {
			var stop atomic.Bool
			if level == "busy" {
				for i := 0; i < 2*runtime.GOMAXPROCS(0); i++ {
					go func() {
						x := 0
						for !stop.Load() {
							x++
						}
						_ = x
					}()
				}
			}
			defer stop.Store(true)
			const runs = 300
			fails, queryErrs := 0, 0
			values := map[int64]int{}
			var drains []string
			for i := 0; i < runs; i++ {
				t.Run(fmt.Sprint(i), func(t *testing.T) {
					reserved, read, err := b4bReclaimOnce(t)
					if err != nil {
						queryErrs++
						t.Logf("B4B 48.8 %s run=%d query error=%v", level, i, err)
						return
					}
					if reserved == 0 {
						return
					}
					fails++
					values[reserved]++
					goroutines := b4bServerGoroutines()
					start := time.Now()
					var seen []string
					last := reserved
					drained := time.Duration(-1)
					for time.Since(start) < 2*time.Second {
						v := read()
						if v != last {
							seen = append(seen, fmt.Sprintf("%d@%s", v, time.Since(start).Round(10*time.Microsecond)))
							last = v
						}
						if v == 0 {
							drained = time.Since(start)
							break
						}
						time.Sleep(time.Millisecond)
					}
					drains = append(drains, drained.Round(10*time.Microsecond).String())
					t.Logf("B4B 48.8 %s run=%d reserved=%d drainedToZeroAfter=%s values=%v server goroutines at the read: %s",
						level, i, reserved, drained.Round(10*time.Microsecond), seen, strings.Join(goroutines, " | "))
				})
			}
			t.Logf("B4B 48.8 TOTAL level=%s runs=%d nonzero=%d queryErrors=%d values=%v drainTimes=%v", level, runs, fails, queryErrs, values, drains)
		})
	}
}
