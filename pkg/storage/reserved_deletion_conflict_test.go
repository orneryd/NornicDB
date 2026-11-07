package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// BEGIN can include a peer's reserved MVCC version without having observed
// that peer's deletion. Reject its second removal whether the peer is still
// buffered or has replayed; otherwise each acknowledgement decrements counts.
func TestReservedDeletionPublicationConflicts(t *testing.T) {
	for _, implicit := range []bool{false, true} {
		for _, replay := range []bool{false, true} {
			for _, route := range []string{"opposite-cascade", "same-node", "relationship"} {
				t.Run(fmt.Sprintf("implicit=%t/replayed=%t/%s", implicit, replay, route), func(t *testing.T) {
					b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
					setupCascadeBoundaryGraph(t, b)
					entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
					var once, releaseOnce sync.Once
					unblock := func() { releaseOnce.Do(func() { close(release) }) }
					require.NoError(t, b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
						return func(StoredEntityPublication) (func(), error) {
							once.Do(func() { close(entered); <-release })
							return nil, nil
						}, nil
					}, nil))
					older, err := b.BeginTransaction(t.Context())
					require.NoError(t, err)
					t.Cleanup(func() { _ = older.Rollback() })
					require.NoError(t, older.SetImplicit(true))
					remove := func(tx *BadgerTransaction, second bool) error {
						if route == "relationship" {
							return tx.DeleteEdge(t.Context(), "test:r2")
						}
						node := NodeID("test:n2")
						if second && route == "opposite-cascade" {
							node = "test:n3"
						}
						return tx.DeleteGraphNode(t.Context(), node, CascadeEdges)
					}
					require.NoError(t, remove(older, false))
					var commitErr error
					go func() { defer close(done); commitErr = older.Commit() }()
					t.Cleanup(func() {
						unblock()
						select {
						case <-done:
						case <-time.After(10 * time.Second):
							t.Error("older commit did not settle")
						}
					})
					select {
					case <-entered:
					case <-time.After(10 * time.Second):
						t.Fatal("observer did not enter")
					}
					stale, err := b.BeginTransaction(t.Context())
					require.NoError(t, err)
					t.Cleanup(func() { _ = stale.Rollback() })
					require.NoError(t, stale.SetImplicit(implicit))
					require.NoError(t, remove(stale, true))
					unblock()
					select {
					case <-done:
						require.NoError(t, commitErr)
					case <-time.After(10 * time.Second):
						t.Fatal("older commit did not finish")
					}
					if replay {
						require.NoError(t, b.FlushWriteBehind(context.Background()))
					}
					require.ErrorIs(t, stale.Commit(), ErrConflict)
					requireCascadeBoundaryCounts(t, b)
					nodes, err := b.AllNodes(t.Context())
					require.NoError(t, err)
					count, err := b.NodeCount(t.Context())
					require.NoError(t, err)
					require.EqualValues(t, len(nodes), count)
					require.NoError(t, b.Close(), "retirement must fold balanced derived counts")
				})
			}
		}
	}
}

func TestFrameCreatedTombstonedIdentityCanBeDeleted(t *testing.T) {
	for _, cascade := range []bool{false, true} {
		t.Run(fmt.Sprint(cascade), func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
			setupCascadeBoundaryGraph(t, b)
			require.NoError(t, b.DeleteEdge(t.Context(), "test:r2"))
			require.NoError(t, b.FlushWriteBehind(context.Background()))
			tx, err := b.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.SetImplicit(true))
			require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: "test:r2", StartNode: "test:n2", EndNode: "test:n3", Type: "LINK"}))
			if cascade {
				require.NoError(t, tx.DeleteGraphNode(t.Context(), "test:n2", CascadeEdges))
			} else {
				require.NoError(t, tx.DeleteEdge(t.Context(), "test:r2"))
			}
			require.NoError(t, tx.Commit())
			requireCascadeBoundaryCounts(t, b)
			require.NoError(t, b.Close())
		})
	}
}
