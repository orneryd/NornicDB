package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The transaction's incidence read is a bulk read, so the caller's context is
// the read's lifetime: a canceled caller stops the native enumeration before
// it publishes a relationship list, a cancellation part-way through stops the
// enumeration instead of finishing it, and the same transaction still answers a
// healthy caller afterwards.
func TestTransactionIncidentEdgesCarriesCallerContext(t *testing.T) {
	const population = 3 * bulkReadCancellationInterval
	engine := newWriteBehindTestEngine(t, time.Hour)
	hub := seedIncidentContextGraph(t, engine, population)

	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	healthy, err := tx.IncidentEdges(t.Context(), hub)
	require.NoError(t, err, "healthy control materializes the incidence list")
	require.Len(t, healthy, population)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	edges, err := tx.IncidentEdges(canceled, hub)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, edges, "a canceled bulk read publishes no partial relationship list")

	inner, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	mid := &cancelAtCheck{Context: inner, cancel: stop, cancelAt: bulkReadCancellationInterval + 8}
	edges, err = tx.IncidentEdges(mid, hub)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, edges)
	require.GreaterOrEqual(t, mid.checks, bulkReadCancellationInterval+8, "the read polls its caller inside the enumeration")
	require.Less(t, mid.checks, population, "the enumeration ends at the cancellation instead of materializing the population")

	healthy, err = tx.IncidentEdges(t.Context(), hub)
	require.NoError(t, err, "the canceled read leaves the transaction healthy")
	require.Len(t, healthy, population)
}
