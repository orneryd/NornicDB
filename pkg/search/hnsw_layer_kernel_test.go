package search

import (
	"context"
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// TestSearchLayerHeapKernel_SingleAndMultiEntryAgree pins the unified layer
// expansion kernel: the build-path wrapper (searchLayerHeap) and the
// query-path wrapper (searchLayerHeapPooledFromEntriesWithContext) must
// produce the same closest-first candidate order for the same entry point,
// and the multi-entry wrapper must stay idempotent under duplicate entries.
func TestSearchLayerHeapKernel_SingleAndMultiEntryAgree(t *testing.T) {
	const (
		dims = 32
		n    = 200
		ef   = 12
	)
	idx := NewHNSWIndex(dims, HNSWConfig{M: 16, EfConstruction: 64})
	for i := 0; i < n; i++ {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = float32((i*13 + j*7) % 97)
		}
		require.NoError(t, idx.Add(fmt.Sprintf("v%d", i), vec))
	}

	query := make([]float32, dims)
	for j := range query {
		query[j] = float32((j*5 + 3) % 89)
	}

	idx.mu.RLock()
	ep := idx.entryPoint
	ids := idx.searchLayerHeap(query, ep, ef, 0)
	items, err := idx.searchLayerHeapPooledFromEntriesWithContext(context.Background(), query, []uint32{ep}, ef, 0)
	idx.mu.RUnlock()
	require.NoError(t, err)

	require.Equal(t, len(ids), len(items), "both wrappers must return the same candidate count")
	for i := range ids {
		require.Equal(t, ids[i], items[i].id, "candidate %d diverges between wrappers", i)
	}

	// Multi-entry seeding is idempotent: passing the same entry point twice
	// must not duplicate candidates.
	idx.mu.RLock()
	items2, err := idx.searchLayerHeapPooledFromEntriesWithContext(context.Background(), query, []uint32{ep, ep}, ef, 0)
	idx.mu.RUnlock()
	require.NoError(t, err)
	require.Equal(t, len(items), len(items2), "duplicate entry points must not duplicate candidates")
	for i := range items {
		require.Equal(t, items[i].id, items2[i].id)
	}

	// An empty entry list yields an empty result without error (defensive
	// guard; the production caller always supplies the entry point first).
	emptyItems, err := idx.searchLayerHeapPooledFromEntriesWithContext(context.Background(), query, nil, ef, 0)
	require.NoError(t, err)
	require.Empty(t, emptyItems)

	// Returned buffers belong to their pools; releasing them mirrors the
	// production callers and must be safe.
	idx.releaseCandidateIDs(ids)
	idx.itemsPool.Put(items[:0])
	idx.itemsPool.Put(items2[:0])
}

// TestSearchLayerHeapKernel_ContextCancellation pins that the context-aware
// instantiation reports cancellation while the build-path instantiation is
// not context-observable (its probes are compiled out).
func TestSearchLayerHeapKernel_ContextCancellation(t *testing.T) {
	const dims = 32
	idx := NewHNSWIndex(dims, HNSWConfig{M: 16, EfConstruction: 64})
	for i := 0; i < 100; i++ {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = float32((i*11 + j*3) % 53)
		}
		require.NoError(t, idx.Add(fmt.Sprintf("v%d", i), vec))
	}
	query := make([]float32, dims)
	for j := range query {
		query[j] = float32((j*2 + 1) % 41)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	idx.mu.RLock()
	_, err := idx.searchLayerHeapPooledFromEntriesWithContext(cancelled, query, []uint32{idx.entryPoint}, 12, 0)
	idx.mu.RUnlock()
	require.Error(t, err, "cancelled context must abort the query-path expansion")
}

// TestSearchLayerHeapKernel_ModeOwnsItsContext pins the typed mode contract of
// the shared layer kernel: the only context a kernel call can observe is the
// context carried by the mode value it is given. layerNoCtx therefore carries
// no context field at all (zero-sized, so its probes and its quantized-query
// lookup compile out), while layerWithCtx owns the caller's query context and
// is the only mode that reads it.
func TestSearchLayerHeapKernel_ModeOwnsItsContext(t *testing.T) {
	require.Zero(t, unsafe.Sizeof(layerNoCtx{}), "the no-context mode must stay zero-sized")
	require.NoError(t, layerNoCtx{}.popProbe())
	require.NoError(t, layerNoCtx{}.neighborProbe())
	require.Nil(t, layerNoCtx{}.quantizedQuery(), "the no-context mode has no context to read a scorer from")

	scorer := &hnswQuantizedQuery{}
	ctx := context.WithValue(context.Background(), hnswQuantizedQueryKey{}, scorer)
	mode := layerWithCtx{ctx: ctx}
	require.Same(t, scorer, mode.quantizedQuery(), "the context mode must read the scorer from its own context")
	require.NoError(t, mode.popProbe())
	require.NoError(t, mode.neighborProbe())

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	cancelledMode := layerWithCtx{ctx: cancelledCtx}
	require.ErrorIs(t, cancelledMode.popProbe(), context.Canceled)
	require.ErrorIs(t, cancelledMode.neighborProbe(), context.Canceled)

	// The canonical generic entry takes the mode value itself, so the
	// build-path instantiation has no context argument to supply.
	const (
		dims = 32
		n    = 64
		ef   = 12
	)
	idx := NewHNSWIndex(dims, HNSWConfig{M: 16, EfConstruction: 64})
	for i := 0; i < n; i++ {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = float32((i*9 + j*5) % 71)
		}
		require.NoError(t, idx.Add(fmt.Sprintf("v%d", i), vec))
	}
	query := make([]float32, dims)
	for j := range query {
		query[j] = float32((j*3 + 7) % 67)
	}

	idx.mu.RLock()
	entry := idx.entryPoint
	ids, _, err := searchLayerHeapKernel[layerNoCtx](idx, layerNoCtx{}, query, entry, nil, ef, 0, true)
	want := idx.searchLayerHeap(query, entry, ef, 0)
	idx.mu.RUnlock()
	require.NoError(t, err)
	require.Equal(t, want, ids, "the typed no-context mode must reproduce the build-path candidate order")
	idx.releaseCandidateIDs(ids)
	idx.releaseCandidateIDs(want)
}
