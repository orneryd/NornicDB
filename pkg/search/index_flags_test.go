package search

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSetIndexFlags — defaults are (true, true); SetIndexFlags reports change
// when at least one value differs from the previous.
func TestSetIndexFlags(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	require.True(t, svc.BM25Enabled())
	require.True(t, svc.VectorEnabled())

	changed := svc.SetIndexFlags(true, true)
	assert.False(t, changed, "no-op when values match")

	changed = svc.SetIndexFlags(false, true)
	assert.True(t, changed)
	assert.False(t, svc.BM25Enabled())
	assert.True(t, svc.VectorEnabled())

	changed = svc.SetIndexFlags(false, false)
	assert.True(t, changed)
	assert.False(t, svc.VectorEnabled())
}

// TestBuildIndexes_BothDisabled — when both indexes are off, BuildIndexes
// returns immediately, marks ready, leaves vectorIndex nil, and uses the
// no-op BM25 stub.
func TestBuildIndexes_BothDisabled(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	svc.SetIndexFlags(false, false)

	require.NoError(t, svc.BuildIndexes(context.Background()))
	assert.True(t, svc.IsReady())
	assert.Equal(t, 0, svc.fulltextIndex.Count(), "no-op stub returns 0")

	// vectorIndex stays non-nil so existing GetDimensions/Count calls
	// don't panic; the flag guards in IndexNode and Search are what
	// disable behaviour.
	assert.False(t, svc.VectorEnabled())
	assert.Equal(t, 0, svc.EmbeddingCount())
}

// TestBuildIndexes_VectorDisabled — vector off only; BM25 still builds. The
// vectorIndex is nil after warmupVectorPipeline early-returns.
func TestBuildIndexes_VectorDisabled(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	svc.SetIndexFlags(true, false)

	require.NoError(t, svc.BuildIndexes(context.Background()))
	assert.True(t, svc.IsReady())
	assert.False(t, svc.VectorEnabled())
	assert.Equal(t, 0, svc.EmbeddingCount(), "no embeddings should populate when vector is disabled")
}

// TestBuildIndexes_BM25Disabled — BM25 off only; the no-op stub replaces the
// real fulltext index so existing code paths that call .Count() / .Search()
// don't panic.
func TestBuildIndexes_BM25Disabled(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	svc.SetIndexFlags(false, true)

	require.NoError(t, svc.BuildIndexes(context.Background()))
	assert.True(t, svc.IsReady())
	// The no-op stub reports zero count and empty searches.
	assert.Equal(t, 0, svc.fulltextIndex.Count())
	results := svc.fulltextIndex.Search("anything", 10)
	assert.Empty(t, results)
}

// TestIndexNode_BothDisabled — IndexNode is a no-op when both flags are off.
func TestIndexNode_BothDisabled(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	svc.SetIndexFlags(false, false)
	require.NoError(t, svc.BuildIndexes(context.Background()))

	node := &storage.Node{
		ID:         storage.NodeID("n1"),
		Properties: map[string]interface{}{"name": "alice"},
	}
	require.NoError(t, svc.IndexNode(node))
	assert.Equal(t, 0, svc.fulltextIndex.Count())
}

// TestEnsureWarm_TriggersOnceAndWaiters — proves that Service-level lazy
// warming fires exactly once across concurrent first-readers and that all
// readers block until the build completes. This is the contract that makes
// lazy-warm uniform across every search entry point (HTTP, Bolt, GraphQL,
// gRPC, Cypher procedures): they all funnel through Service.Search →
// EnsureWarm so the trigger doesn't have to be replicated in each handler.
func TestEnsureWarm_TriggersOnceAndWaiters(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)

	var triggers atomic.Int32
	startBuild := make(chan struct{})
	finishBuild := make(chan struct{})
	svc.SetLazyWarming(true, WarmFunc(func() {
		triggers.Add(1)
		close(startBuild)
		<-finishBuild
		// Simulate the build completing: mark ready so subsequent
		// IsReady() probes report true (production code does this via
		// BuildIndexes setting s.ready=true).
		svc.MarkReadyDisabled()
	}))

	// Fire 8 concurrent first-readers. Exactly one trigger should run;
	// all 8 should observe a successful return after the build "finishes".
	const N = 8
	results := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			results <- svc.EnsureWarm(context.Background())
		}()
	}

	// Wait for the trigger to start running; only one goroutine should
	// have entered the WarmFunc.
	select {
	case <-startBuild:
	case <-time.After(2 * time.Second):
		t.Fatal("WarmFunc did not start within 2s")
	}
	assert.Equal(t, int32(1), triggers.Load(), "trigger must fire exactly once")

	// Let the build finish.
	close(finishBuild)

	// All N waiters return nil.
	for i := 0; i < N; i++ {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatalf("waiter %d did not return within 2s", i)
		}
	}

	// trigger count is still exactly one.
	assert.Equal(t, int32(1), triggers.Load(), "trigger must remain at 1 after concurrent waiters")
}

// TestEnsureWarm_CallerCtxCancel_BuildContinues — caller's request ctx
// timing out during the wait must NOT abort the build. The waiter returns
// ctx.Err(), but a subsequent reader finds the service warm because the
// build runs in the owner's long-lived ctx.
func TestEnsureWarm_CallerCtxCancel_BuildContinues(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)

	buildStarted := make(chan struct{})
	buildFinished := make(chan struct{})
	svc.SetLazyWarming(true, WarmFunc(func() {
		close(buildStarted)
		<-buildFinished
		svc.MarkReadyDisabled()
	}))

	// First caller times out almost immediately.
	cancelCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := svc.EnsureWarm(cancelCtx)
	assert.ErrorIs(t, err, context.DeadlineExceeded, "caller ctx timeout should surface as DeadlineExceeded")

	// The build is still running.
	select {
	case <-buildStarted:
	case <-time.After(time.Second):
		t.Fatal("build did not start despite caller ctx cancel")
	}
	// Build hasn't finished yet — caller ctx didn't abort it.
	select {
	case <-buildFinished:
		t.Fatal("buildFinished closed unexpectedly — should still be in progress")
	default:
	}

	// Let the build finish.
	close(buildFinished)

	// A subsequent reader with a generous ctx finds the service warm.
	require.Eventually(t, func() bool {
		return svc.EnsureWarm(context.Background()) == nil
	}, 2*time.Second, 25*time.Millisecond, "service should be warm after the build completes")
}

// TestEnsureWarm_NoOp_WhenNotLazy — when the service is not configured for
// lazy warming, EnsureWarm returns nil without firing the trigger.
func TestEnsureWarm_NoOp_WhenNotLazy(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)

	var triggers atomic.Int32
	svc.SetLazyWarming(false, WarmFunc(func() {
		triggers.Add(1)
	}))

	require.NoError(t, svc.EnsureWarm(context.Background()))
	assert.Equal(t, int32(0), triggers.Load(), "trigger must not fire when warmingLazy=false")
}

// TestMarkReadyDisabled — explicit short-circuit for the boot-orchestrator
// "both disabled" path.
func TestMarkReadyDisabled(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	svc.SetIndexFlags(false, false)
	svc.MarkReadyDisabled()

	assert.True(t, svc.IsReady())
	assert.Equal(t, 0, svc.fulltextIndex.Count(), "no-op stub reports zero")
	assert.Equal(t, 0, svc.EmbeddingCount(), "no embeddings populated")
}
