package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestQueryTextCachesBorrowCancellation(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cache_work"))
	query := "RETURN " + strings.Repeat("variable", 8192)
	executor.markCachedValidSyntax(query, syntaxValidNornic)
	require.True(t, executor.hasCachedValidSyntax(query, syntaxValidNornic))
	require.Equal(t, upperASCII(query), executor.cachedUpperQuery(query))
	for _, stage := range []struct {
		name string
		run  func(*pipelineCancellation)
	}{
		{"syntax_hit", func(probe *pipelineCancellation) {
			require.False(t, executor.hasCachedValidSyntaxWithCancellation(query, syntaxValidNornic, probe))
		}},
		{"syntax_mark", func(probe *pipelineCancellation) {
			executor.markCachedValidSyntaxWithCancellation(query, syntaxValidANTLR, probe)
		}},
		{"uppercase_hit", func(probe *pipelineCancellation) {
			require.Empty(t, executor.cachedUpperQueryWithCancellation(query, probe))
		}},
	} {
		t.Run(stage.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			probe := &pipelineCancellation{ctx: ctx, remaining: 1}
			stage.run(probe)
			require.ErrorIs(t, probe.err, context.Canceled)
		})
	}
	for i := range executor.ensureSyntaxValidationCache().shards {
		require.Nil(t, executor.ensureSyntaxValidationCache().shards[i].cache.probe)
	}
	require.Nil(t, executor.ensureUpperQueryCache().cache.probe)
	require.False(t, executor.hasCachedValidSyntax(query, syntaxValidANTLR), "aborted marking must not grant another validator's acceptance")
}

func TestQueryTextCacheEvictionAndLockCancellation(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cache_limit"))
	cache := executor.ensureUpperQueryCache()
	cache.max = 2
	for _, query := range []string{"return first", "return second", "return third"} {
		require.Equal(t, upperASCII(query), executor.cachedUpperQuery(query))
	}
	require.Equal(t, 2, cache.cache.Len())
	index := pipelineStringMap[int]{}
	require.True(t, putQueryTextCache(&index, 0, "disabled", 1, nil))
	require.Zero(t, index.Len())
	require.True(t, putQueryTextCache(&index, 1, "same", 1, nil))
	require.True(t, putQueryTextCache(&index, 1, "same", 2, nil))
	require.Equal(t, 1, index.Len())
	require.True(t, putQueryTextCache(&index, 1, "replacement", 3, nil))
	require.Equal(t, 1, index.Len())
	var mutex sync.RWMutex
	for _, read := range []bool{false, true} {
		mutex.Lock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		deadline, _ := ctx.Deadline()
		probe := &pipelineCancellation{ctx: ctx}
		require.False(t, lockQueryTextCache(&mutex, read, probe))
		require.ErrorIs(t, probe.err, context.DeadlineExceeded)
		require.Less(t, time.Since(deadline), 250*time.Millisecond)
		mutex.Unlock()
		cancel()
	}
	for _, read := range []bool{false, true} {
		require.True(t, lockQueryTextCache(&mutex, read, nil))
		if read {
			mutex.RUnlock()
		} else {
			mutex.Unlock()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	probe := &pipelineCancellation{ctx: ctx, remaining: 1}
	require.False(t, putQueryTextCache(&index, 1, "aborted", 4, probe))
	require.Equal(t, 1, index.Len())
	for i := 0; i < 100; i++ {
		executor.markCachedValidSyntax(fmt.Sprintf("RETURN name%d", i), syntaxValidNornic)
	}
}

func TestQueryTextCacheLargeKeyExactDeadline(t *testing.T) {
	query := "RETURN " + strings.Repeat("largeName", 16<<20)
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cache_deadline"))
	executor.markCachedValidSyntax(query, syntaxValidNornic)
	for _, stage := range []struct {
		name string
		run  func(*pipelineCancellation)
	}{
		{"lookup", func(probe *pipelineCancellation) {
			executor.hasCachedValidSyntaxWithCancellation(query, syntaxValidNornic, probe)
		}},
		{"mark", func(probe *pipelineCancellation) {
			executor.markCachedValidSyntaxWithCancellation(query, syntaxValidANTLR, probe)
		}},
		{"uppercase", func(probe *pipelineCancellation) { executor.cachedUpperQueryWithCancellation(query, probe) }},
	} {
		t.Run(stage.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()
			deadline, _ := ctx.Deadline()
			probe := &pipelineCancellation{ctx: ctx}
			stage.run(probe)
			returned := time.Now()
			require.ErrorIs(t, probe.err, context.DeadlineExceeded)
			require.Less(t, returned.Sub(deadline), 250*time.Millisecond)
			t.Logf("stage=%s deadline=%s returned=%s overrun=%s", stage.name, deadline.Format(time.RFC3339Nano), returned.Format(time.RFC3339Nano), returned.Sub(deadline))
		})
	}
}

func TestQueryTextCacheCancellationPhases(t *testing.T) {
	base := newTestMemoryEngine(t)
	query := "return " + strings.Repeat("name ", 1024) + " 123"
	for _, stage := range []string{"lookup", "mark", "uppercase", "evict"} {
		t.Run(stage, func(t *testing.T) {
			runPipelineCancelPointStage(t, func(ctx context.Context) error {
				for phase := 1; phase <= pipelineCancellationInterval; phase++ {
					executor := NewStorageExecutor(storage.NewNamespacedEngine(base, "cache_phase"))
					probe := &pipelineCancellation{ctx: ctx, remaining: phase}
					switch stage {
					case "lookup":
						executor.markCachedValidSyntax(query, syntaxValidNornic)
						if executor.hasCachedValidSyntaxWithCancellation(query, syntaxValidANTLR, probe) {
							t.Fatal("ungranted validator accepted")
						}
					case "mark":
						executor.markCachedValidSyntaxWithCancellation(query, syntaxValidNornic, probe)
					case "uppercase":
						executor.cachedUpperQueryWithCancellation(query, probe)
					case "evict":
						index := pipelineStringMap[int]{}
						index.Set("old", 1)
						putQueryTextCache(&index, 1, query, 2, probe)
					}
					if probe.err != nil {
						return probe.err
					}
				}
				return nil
			})
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := &pipelineCancellation{ctx: ctx}
	executor := NewStorageExecutor(storage.NewNamespacedEngine(base, "healthy_cache_phase"))
	executor.markCachedValidSyntaxWithCancellation("RETURN 123", syntaxValidNornic, probe)
	require.True(t, executor.hasCachedValidSyntaxWithCancellation("RETURN 456", syntaxValidNornic, probe))
	require.Equal(t, "RETURN VALUE", executor.cachedUpperQueryWithCancellation("return value", probe))
	require.Equal(t, "RETURN VALUE", executor.cachedUpperQueryWithCancellation("return value", probe))
	require.Empty(t, executor.cachedUpperQueryWithCancellation("", probe))
	executor.markCachedValidSyntaxWithCancellation("", syntaxValidNornic, probe)
	require.False(t, executor.hasCachedValidSyntaxWithCancellation("", syntaxValidNornic, probe))
}

type queryCacheSampleContext struct {
	context.Context
	mu          sync.Mutex
	cancel      context.CancelFunc
	calls, stop int
	cancelled   time.Time
}

func (ctx *queryCacheSampleContext) Err() error {
	ctx.mu.Lock()
	ctx.calls++
	stop := ctx.calls == ctx.stop
	if stop {
		ctx.cancelled = time.Now()
	}
	ctx.mu.Unlock()
	if stop {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func (ctx *queryCacheSampleContext) receipt() (int, time.Time) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	return ctx.calls, ctx.cancelled
}

// Move cancellation across samples, including later cache operations that share
// their leaf helper with earlier lookups. A source-site-only replay stops at the
// first such leaf and cannot reach a later parent's failure unwind.
func TestQueryTextCacheLateCancellation(t *testing.T) {
	base := newTestMemoryEngine(t)
	query := "return " + strings.Repeat("name ", 1024) + " 123"
	for _, stage := range []string{"lookup", "mark", "uppercase"} {
		for phase := 1; phase <= pipelineCancellationInterval; phase++ {
			run := func(stop int) int {
				executor := NewStorageExecutor(storage.NewNamespacedEngine(base, "cache_late"))
				if stage == "lookup" {
					executor.markCachedValidSyntax(query, syntaxValidNornic)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				sampled := &queryCacheSampleContext{Context: ctx, cancel: cancel, stop: stop}
				probe := &pipelineCancellation{ctx: sampled, remaining: phase}
				switch stage {
				case "lookup":
					executor.hasCachedValidSyntaxWithCancellation(query, syntaxValidANTLR, probe)
				case "mark":
					executor.markCachedValidSyntaxWithCancellation(query, syntaxValidNornic, probe)
				case "uppercase":
					executor.cachedUpperQueryWithCancellation(query, probe)
				}
				if stop > 0 {
					require.ErrorIs(t, probe.err, context.Canceled)
					_, cancelled := sampled.receipt()
					require.Less(t, time.Since(cancelled), 250*time.Millisecond)
				} else {
					require.NoError(t, probe.err)
				}
				calls, _ := sampled.receipt()
				return calls
			}
			for stop, calls := 1, run(0); stop <= calls; stop++ {
				run(stop)
			}
		}
	}
}
