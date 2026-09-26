package cypher

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type countingEmbedder struct {
	calls int32
	err   error
}

func (e *countingEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	atomic.AddInt32(&e.calls, 1)
	if e.err != nil {
		return nil, e.err
	}
	if text == "" {
		return nil, nil
	}
	return []float32{float32(len(text)), 1}, nil
}

func (e *countingEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	if text == "" {
		return []string{}, nil
	}
	return []string{text}, nil
}

type blockingEmbedder struct {
	started chan struct{}
	release chan struct{}
	result  []float32
	err     error
}

func (b *blockingEmbedder) Embed(ctx context.Context, _ string) ([]float32, error) {
	select {
	case b.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.release:
	}
	if b.err != nil {
		return nil, b.err
	}
	return b.result, nil
}

func (b *blockingEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}

func TestExecuteInternal_UseClauseBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "internal_use"))
	ctx := context.Background()

	_, err := exec.executeInternal(ctx, "USE", nil)
	require.EqualError(t, err, "USE clause requires a database name")

	res, err := exec.executeInternal(ctx, "USE foo RETURN 1 AS x", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, int64(1), res.Rows[0][0])

	// Remaining query empty after USE returns database info.
	res, err = exec.executeInternal(ctx, "USE foo", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"database"}, res.Columns)
	require.Equal(t, "foo", res.Rows[0][0])

	// Non-namespaced storage should reject USE.
	nonNamespaced := NewStorageExecutor(newTestMemoryEngine(t))
	_, err = nonNamespaced.executeInternal(ctx, "USE bar RETURN 1", nil)
	require.ErrorContains(t, err, "USE bar is not supported by this storage backend")
}

func TestVectorQueryEmbedCache_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "embed_cache"))
	ctx := context.Background()

	_, err := exec.embedVectorQueryText(ctx, "hello")
	require.EqualError(t, err, "no embedder configured")

	em := &countingEmbedder{}
	exec.embedder = em

	v1, err := exec.embedVectorQueryText(ctx, "  Hello   World ")
	require.NoError(t, err)
	require.NotEmpty(t, v1)
	require.EqualValues(t, 1, atomic.LoadInt32(&em.calls))

	v2, err := exec.embedVectorQueryText(ctx, "hello world")
	require.NoError(t, err)
	require.Equal(t, v1, v2)
	require.EqualValues(t, 1, atomic.LoadInt32(&em.calls), "second call should hit cache via canonical key")

	// Empty canonical key path bypasses cache and embeds raw text each time.
	_, err = exec.embedVectorQueryText(ctx, "   ")
	require.NoError(t, err)
	require.EqualValues(t, 2, atomic.LoadInt32(&em.calls), "blank but non-empty text follows raw embed path")

	em.err = errors.New("embed failed")
	_, err = exec.embedVectorQueryText(ctx, "distinct text")
	require.EqualError(t, err, "embed failed")
}

func TestVectorQueryEmbedCache_InflightAndEvictionBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "embed_inflight"))

	blk := &blockingEmbedder{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		result:  []float32{9, 9},
	}
	exec.embedder = blk

	done := make(chan error, 1)
	go func() {
		_, err := exec.embedVectorQueryText(context.Background(), "same key")
		done <- err
	}()
	<-blk.started

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := exec.embedVectorQueryText(cancelCtx, "same key")
	require.ErrorIs(t, err, context.Canceled)

	close(blk.release)
	require.NoError(t, <-done)

	// Inflight error propagation branch.
	blk2 := &blockingEmbedder{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		err:     errors.New("inflight boom"),
	}
	exec.embedder = blk2
	done2 := make(chan error, 1)
	go func() {
		_, err := exec.embedVectorQueryText(context.Background(), "error key")
		done2 <- err
	}()
	<-blk2.started
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(blk2.release)
	}()
	_, err = exec.embedVectorQueryText(context.Background(), "error key")
	require.EqualError(t, err, "inflight boom")
	require.EqualError(t, <-done2, "inflight boom")

	// Cache capacity branch: when cache is full, it resets before insert.
	full := make(map[string][]float32, maxVectorQueryEmbedCacheEntries)
	for i := 0; i < maxVectorQueryEmbedCacheEntries; i++ {
		full[fmt.Sprintf("k-%d", i)] = []float32{1}
	}
	exec.vectorQueryEmbedMu.Lock()
	exec.vectorQueryEmbedCache = full
	exec.vectorQueryEmbedMu.Unlock()

	exec.embedder = &countingEmbedder{}
	_, err = exec.embedVectorQueryText(context.Background(), "fresh key")
	require.NoError(t, err)
	exec.vectorQueryEmbedMu.Lock()
	defer exec.vectorQueryEmbedMu.Unlock()
	require.LessOrEqual(t, len(exec.vectorQueryEmbedCache), 2)
	require.Contains(t, exec.vectorQueryEmbedCache, canonicalizeVectorQueryText("fresh key"))
}

func TestVectorQueryHelpers_AndHotPathTraceMarks(t *testing.T) {
	require.Equal(t, "hello world", canonicalizeVectorQueryText("  Hello\n world  "))
	require.Equal(t, "", canonicalizeVectorQueryText(" \t \n "))
	require.Nil(t, cloneFloat32Slice(nil))

	src := []float32{1, 2, 3}
	clone := cloneFloat32Slice(src)
	require.Equal(t, src, clone)
	src[0] = 9
	require.EqualValues(t, 1, clone[0], "clone must be independent")

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "trace_marks"))
	// First round exercises nil-state initialization branches.
	exec.resetHotPathTrace()
	exec.setFabricBatchedApplyRowsUsed(false)
	exec.markOuterIndexTopKUsed()
	exec.setFabricBatchedApplyRowsUsed(true)
	exec.markSimpleMatchLimitFastPathUsed()
	exec.markCompoundQueryFastPathUsed()
	exec.markTraversalStartSeedTopKUsed()
	exec.markTraversalEndSeedTopKUsed()
	exec.markUnwindSimpleMergeBatchUsed()
	exec.markUnwindMergeChainBatchUsed()
	exec.markUnwindFixedChainLinkBatchUsed()
	exec.markUnwindMultiMatchCreateBatchUsed()
	exec.markCallTailTraversalFastPathUsed()
	exec.markMergeSchemaLookupUsed()
	exec.markMergeScanFallbackUsed()

	// Second round exercises non-nil fast path branches.
	exec.markOuterIndexTopKUsed()
	exec.setFabricBatchedApplyRowsUsed(true)
	exec.markSimpleMatchLimitFastPathUsed()
	exec.markCompoundQueryFastPathUsed()
	exec.markTraversalStartSeedTopKUsed()
	exec.markTraversalEndSeedTopKUsed()
	exec.markUnwindSimpleMergeBatchUsed()
	exec.markUnwindMergeChainBatchUsed()
	exec.markUnwindFixedChainLinkBatchUsed()
	exec.markUnwindMultiMatchCreateBatchUsed()
	exec.markCallTailTraversalFastPathUsed()
	exec.markMergeSchemaLookupUsed()
	exec.markMergeScanFallbackUsed()

	trace := exec.LastHotPathTrace()
	require.True(t, trace.OuterIndexTopK)
	require.True(t, trace.FabricBatchedApplyRows)
	require.True(t, trace.SimpleMatchLimitFastPath)
	require.True(t, trace.CompoundQueryFastPath)
	require.True(t, trace.TraversalStartSeedTopK)
	require.True(t, trace.TraversalEndSeedTopK)
	require.True(t, trace.UnwindSimpleMergeBatch)
	require.True(t, trace.UnwindMergeChainBatch)
	require.True(t, trace.UnwindFixedChainLinkBatch)
	require.True(t, trace.UnwindMultiMatchCreateBatch)
	require.True(t, trace.CallTailTraversalFastPath)
	require.True(t, trace.MergeSchemaLookupUsed)
	require.True(t, trace.MergeScanFallbackUsed)
}
