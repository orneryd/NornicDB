// Author: #1683 review-correction lane (DeepSeek), 2026-11-02.
package search

import (
	"context"
	"fmt"
	"math"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestFulltextRetentionAdmitsResidentCorpusAgainstCurrentUsage holds the
// application meter in one deterministic band: the live corpus plus one
// headroom fits the limit, while a second corpus does not. A read has already
// materialized the corpus when it publishes it, so the runtime usage the meter
// observes contains it; publishing must therefore admit the resident copy
// against current usage and report its byte estimate in status only.
//
// The band is exercised through the public service API: a real pressure
// observation releases the resident corpus, the read that repopulates it must
// refill retention, and unchanged reads must reuse it. When admission charges
// the corpus a second time, that refill is denied, so every read repopulates
// the whole corpus and releases it again (release/refill/rescan churn). An
// independent phase keeps the refill hysteresis: a released corpus is not
// refilled while less than two headrooms remain.
func TestFulltextRetentionAdmitsResidentCorpusAgainstCurrentUsage(t *testing.T) {
	priorLimit := debug.SetMemoryLimit(math.MaxInt64)
	t.Cleanup(func() { debug.SetMemoryLimit(priorLimit) })
	const (
		headroom         = int64(1 << 20)
		liveUsed         = int64(16 << 20)
		margin           = int64(7 << 19) // 3.5 MiB of capacity above liveUsed
		documents        = 48
		documentEstimate = int64(48 << 10)
	)
	// capacity is what the meter counts as available at liveUsed. The whole
	// corpus (48 documents of documentEstimate plus their view overhead) is
	// ~2.3 MiB, so one headroom is left over and 2 * corpus does not fit.
	capacity := margin
	limit := liveUsed + capacity

	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for i := 0; i < documents; i++ {
		_, err := store.CreateNode(t.Context(), &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("retention:doc-%02d", i)),
			Properties: map[string]any{"text": "hotel services"},
		})
		require.NoError(t, err)
	}

	var used atomic.Int64
	used.Store(liveUsed)
	policy := memory.NewPolicy(limit, headroom, time.Hour, func() (memory.Sample, error) {
		return memory.Sample{Used: used.Load()}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())

	options := DefaultServiceOptions("")
	options.MemoryPolicy = policy
	svc := NewService(store, DefaultVectorDimensions, &options)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })

	var builds atomic.Int64
	scope := FulltextDocumentScope{Name: "retention", Definition: "text"}
	build := func(node *storage.Node, _ *storage.Edge) *FulltextDocument {
		builds.Add(1)
		text, _ := node.Properties["text"].(string)
		return &FulltextDocument{Text: text, Tokens: 2, RetainedBytes: documentEstimate}
	}
	read := func() {
		t.Helper()
		require.NoError(t, svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(ctx context.Context, corpus FulltextDocuments) error {
			require.Equal(t, documents, corpus.Count())
			frequencies, err := corpus.BodyDocumentFrequencies(ctx, []string{"hotel"})
			require.NoError(t, err)
			require.Equal(t, documents, frequencies["hotel"])
			return nil
		}))
	}
	// state reads the one published retention record of this projection.
	state := func() FulltextRetentionStatus {
		t.Helper()
		retained := svc.FulltextRetention()
		require.Len(t, retained, 1)
		require.Equal(t, "retention", retained[0].Index)
		require.False(t, retained[0].Relationships)
		require.Equal(t, "application", retained[0].MemorySource, "the application meter must be the one that decides")
		require.Equal(t, limit, retained[0].MemoryLimit)
		return retained[0]
	}
	inBand := func(status FulltextRetentionStatus) {
		t.Helper()
		require.Positive(t, status.Bytes, "a retained corpus reports its byte estimate")
		require.GreaterOrEqual(t, capacity, status.Bytes+headroom,
			"the live corpus and one headroom must fit the observed band")
		require.Less(t, capacity, 2*status.Bytes, "the observed band must be too small for a second corpus")
		require.Equal(t, uint64(status.Bytes), svc.IndexSizeBytes("bm25"),
			"the retained byte estimate stays observable without being charged again")
	}

	// A first read materializes and publishes the whole corpus.
	read()
	published := state()
	require.True(t, published.Retained)
	require.False(t, published.StorageReads)
	require.Equal(t, documents, published.Documents)
	require.Equal(t, liveUsed, published.MemoryUsed, "the resident corpus is already part of observed usage")
	inBand(published)

	// A genuine physical observation above the limit releases the corpus.
	used.Store(limit - headroom + 1)
	require.NoError(t, policy.Refresh())
	released := state()
	require.True(t, released.StorageReads, "physical pressure must release the resident corpus")
	require.False(t, released.Retained)
	require.Zero(t, released.Documents)

	// Usage returns to the band; the released corpus stays released until a
	// read repopulates it.
	used.Store(liveUsed)
	require.NoError(t, policy.Refresh())
	require.True(t, state().StorageReads)

	// The next read repopulates the corpus from storage once and must refill
	// retention, because that corpus already fits the observed band.
	before := builds.Load()
	read()
	require.Equal(t, int64(documents), builds.Load()-before, "a released corpus is repopulated from storage once")
	refilled := state()
	require.True(t, refilled.Retained,
		"a read that repopulated the resident corpus against current usage must retain it")
	require.False(t, refilled.StorageReads)
	require.Equal(t, documents, refilled.Documents)
	require.Equal(t, liveUsed, refilled.MemoryUsed)
	inBand(refilled)

	// Repeating the read must reuse the admitted corpus: no repopulation, no
	// release, and a reported estimate that stops changing.
	var reusedBytes int64
	for i := 0; i < 3; i++ {
		before = builds.Load()
		read()
		require.Equal(t, before, builds.Load(), "an unchanged read reused the retained corpus instead of rescanning storage")
		steady := state()
		require.True(t, steady.Retained, "retention must not oscillate between release and refill")
		require.False(t, steady.StorageReads)
		require.Equal(t, liveUsed, steady.MemoryUsed)
		inBand(steady)
		if i == 0 {
			// The reuse counts the bodies of the view it maintains, so its
			// estimate includes that view's one retained frequency set; every
			// later equal read must report the same estimate.
			reusedBytes = steady.Bytes
			continue
		}
		require.Equal(t, reusedBytes, steady.Bytes, "an unchanged read must not change the reported estimate")
	}

	// The refill keeps its own hysteresis: a released corpus is not refilled
	// while less than two headrooms remain, and the read still serves the
	// complete corpus from storage.
	used.Store(limit - headroom + 1)
	require.NoError(t, policy.Refresh())
	require.True(t, state().StorageReads, "physical pressure must release the resident corpus")
	used.Store(limit - headroom - headroom/2) // capacity is one and a half headrooms
	require.NoError(t, policy.Refresh())
	read()
	hysteresis := state()
	require.True(t, hysteresis.StorageReads, "refill must wait for two headrooms")
	require.False(t, hysteresis.Retained)

	// With the band restored, the next read refills the released corpus.
	used.Store(liveUsed)
	require.NoError(t, policy.Refresh())
	before = builds.Load()
	read()
	require.Equal(t, before+int64(documents), builds.Load(), "the released corpus is repopulated once")
	inBand(state())
	require.True(t, state().Retained)
}
