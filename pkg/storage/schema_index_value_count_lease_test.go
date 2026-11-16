package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// scheduledPendingSource inserts one deterministic step at the schema's
// pending-read admission: the step runs on the reader's own goroutine, at the
// entry of lockPendingWrites and before the buffer is locked, so a test orders
// real work (a write-behind flush, a cancellation) against the estimate's
// committed and pending reads without a sleep or a second goroutine's schedule.
type scheduledPendingSource struct {
	pendingWriteSource
	at func()
}

var _ pendingWriteSource = scheduledPendingSource{}

func (s scheduledPendingSource) lockPendingWrites(ctx context.Context) (pendingWriteView, error) {
	if s.at != nil {
		s.at()
	}
	return s.pendingWriteSource.lockPendingWrites(ctx)
}

// schedulePendingAdmission runs at the schema's pending-read admission.
func schedulePendingAdmission(t *testing.T, schema *SchemaManager, engine *BadgerEngine, at func()) {
	t.Helper()
	require.NotNil(t, engine.writeBehind, "fixture must have a write-behind buffer")
	schema.attachPendingWrites(scheduledPendingSource{pendingWriteSource: engine.writeBehind, at: at}, "test")
}

// compositeEstimateFixture is a write-behind engine whose schema declares the
// (tenant, id) composite index and the arity-1 tenant index, with one Pair node
// acknowledged but still buffered: the committed postings are empty and the
// pending view holds the node.
func compositeEstimateFixture(t *testing.T) (*BadgerEngine, *SchemaManager) {
	t.Helper()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddCompositeIndex("pair_tenant_id", "Pair", []string{"tenant", "id"}))
	require.NoError(t, schema.AddPropertyIndex("pair_tenant", "Pair", []string{"tenant"}))
	bufferedCreate(t, engine, &Node{
		ID:         "test:p1",
		Labels:     []string{"Pair"},
		Properties: map[string]interface{}{"tenant": "t1", "id": "i1"},
	})
	require.Positive(t, engine.writeBehind.PendingOps(), "fixture node must still be buffered")
	return engine, schema
}

func compositeEstimateIndex(t *testing.T, schema *SchemaManager, name string) *CompositeIndex {
	t.Helper()
	idx, exists := schema.GetCompositeIndex(name)
	require.True(t, exists, name)
	return idx
}

// TestCompositeIndexValueCountCountsFlushedNodeOnce reproduces section40: the
// composite estimate read the committed posting list before it acquired
// beginPendingRead, so a write-behind flush that committed a buffered node in
// that window retired the node from the pending view after the postings were
// copied, and the node was in neither term of the count. The schedules are
// ordered by the admission hook itself (a real FlushWriteBehind flush, or the
// request's cancellation), never by timing.
func TestCompositeIndexValueCountCountsFlushedNodeOnce(t *testing.T) {
	shapes := []struct {
		name   string
		index  string
		values []interface{}
		full   bool
	}{
		{name: "arity-1/full", index: "pair_tenant", values: []interface{}{"t1"}, full: true},
		{name: "arity-2/full", index: "pair_tenant_id", values: []interface{}{"t1", "i1"}, full: true},
		{name: "arity-2/prefix", index: "pair_tenant_id", values: []interface{}{"t1"}, full: false},
	}
	for _, shape := range shapes {
		t.Run(shape.name+"/buffered-only", func(t *testing.T) {
			_, schema := compositeEstimateFixture(t)
			idx := compositeEstimateIndex(t, schema, shape.index)
			count, indexed, err := schema.CompositeIndexValueCount(t.Context(), idx, shape.values, shape.full)
			require.NoError(t, err)
			require.True(t, indexed)
			require.Equal(t, 1, count, "the buffered write is the only population")
		})
		t.Run(shape.name+"/committed-only", func(t *testing.T) {
			engine, schema := compositeEstimateFixture(t)
			idx := compositeEstimateIndex(t, schema, shape.index)
			require.NoError(t, engine.FlushWriteBehind(context.Background()))
			count, indexed, err := schema.CompositeIndexValueCount(t.Context(), idx, shape.values, shape.full)
			require.NoError(t, err)
			require.True(t, indexed)
			require.Equal(t, 1, count, "the committed posting is the only population")
		})
		t.Run(shape.name+"/flush-at-admission", func(t *testing.T) {
			engine, schema := compositeEstimateFixture(t)
			idx := compositeEstimateIndex(t, schema, shape.index)
			var flushErr error
			schedulePendingAdmission(t, schema, engine, func() {
				flushErr = engine.FlushWriteBehind(context.Background())
			})
			count, indexed, err := schema.CompositeIndexValueCount(t.Context(), idx, shape.values, shape.full)
			require.NoError(t, flushErr, "scheduled flush")
			require.NoError(t, err)
			require.True(t, indexed)
			// The flush moved the node from the pending view into the committed
			// postings while the estimate ran. Exactly one of the two views must
			// count it: reading the postings before the lease loses it from both.
			require.Equal(t, 1, count, "a flush between the committed posting read and pending admission loses the node from both views")
			ids, err := schema.CompositeIndexLookup(t.Context(), idx, shape.values, shape.full)
			require.NoError(t, err)
			require.Len(t, ids, count, "the estimate must not count an entry the merged lookup returns once")
		})
		t.Run(shape.name+"/cancelled-at-admission", func(t *testing.T) {
			engine, schema := compositeEstimateFixture(t)
			idx := compositeEstimateIndex(t, schema, shape.index)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			schedulePendingAdmission(t, schema, engine, cancel)
			count, indexed, err := schema.CompositeIndexValueCount(ctx, idx, shape.values, shape.full)
			require.ErrorIs(t, err, context.Canceled)
			require.False(t, indexed)
			require.Zero(t, count)
			// The error is the request's cancellation, not an unreadable index:
			// the same buffered node is still counted for a live caller.
			liveCount, liveIndexed, liveErr := schema.CompositeIndexValueCount(t.Context(), idx, shape.values, shape.full)
			require.NoError(t, liveErr)
			require.True(t, liveIndexed)
			require.Equal(t, 1, liveCount)
		})
	}
}

// TestPropertyIndexValueCountReadsCommittedPostingsInsidePendingLease is the
// control for the schedules above: the scalar owner already reads its committed
// posting list inside the pending lease, so the same flush at admission leaves
// the node counted exactly once. It shows the schedule moves a buffered node
// between the views and that only the composite owner lost it.
func TestPropertyIndexValueCountReadsCommittedPostingsInsidePendingLease(t *testing.T) {
	engine, schema := compositeEstimateFixture(t)
	var flushErr error
	schedulePendingAdmission(t, schema, engine, func() {
		flushErr = engine.FlushWriteBehind(context.Background())
	})
	count, indexed, err := schema.PropertyIndexValueCount(t.Context(), "Pair", "tenant", "t1")
	require.NoError(t, flushErr, "scheduled flush")
	require.NoError(t, err)
	require.True(t, indexed)
	require.Equal(t, 1, count, "the scalar owner counts the flushed node once")
}
