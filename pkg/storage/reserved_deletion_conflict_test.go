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
			for _, route := range []string{"opposite-cascade", "same-node", "relationship", "node-update", "relationship-update"} {
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
					stage := func(tx *BadgerTransaction, second bool) error {
						if second && route == "node-update" {
							before, err := tx.GetNode(t.Context(), "test:n2")
							if err != nil {
								return err
							}
							after := CopyNode(before)
							after.Properties["v"] = int64(2)
							return tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: before, After: after, Policy: ClientNodeEdits})
						}
						if second && route == "relationship-update" {
							before, err := tx.GetEdge(t.Context(), "test:r2")
							if err != nil {
								return err
							}
							after := copyEdge(before)
							after.Properties["v"] = int64(2)
							return tx.UpdateEdge(t.Context(), EdgeUpdateRequest{Before: before, After: after})
						}
						if route == "relationship" || route == "relationship-update" {
							return tx.DeleteEdge(t.Context(), "test:r2")
						}
						node := NodeID("test:n2")
						if second && route == "opposite-cascade" {
							node = "test:n3"
						}
						return tx.DeleteGraphNode(t.Context(), node, CascadeEdges)
					}
					require.NoError(t, stage(older, false))
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
					require.NoError(t, stage(stale, true))
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
	for _, route := range []string{"node", "relationship", "relationship-cascade"} {
		t.Run(route, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
			setupCascadeBoundaryGraph(t, b)
			if route == "node" {
				require.NoError(t, b.DeleteNode(t.Context(), "test:n2"))
			} else {
				require.NoError(t, b.DeleteEdge(t.Context(), "test:r2"))
			}
			require.NoError(t, b.FlushWriteBehind(context.Background()))
			tx, err := b.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.SetImplicit(true))
			if route == "node" {
				_, err := tx.CreateNode(t.Context(), &Node{ID: "test:n2"})
				require.NoError(t, err)
			} else {
				require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: "test:r2", StartNode: "test:n2", EndNode: "test:n3", Type: "LINK"}))
			}
			if route != "relationship" {
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

// A stored tombstone is not absence when the BEGIN cut includes an acknowledged
// re-create that is still in write-behind. Such ordinary follow-on mutations
// must succeed without waiting for physical replay.
func TestAcknowledgedRecreationCanBeMutatedBeforeReplay(t *testing.T) {
	for _, kind := range []string{"node", "relationship"} {
		for _, remove := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/delete=%t", kind, remove), func(t *testing.T) {
				b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
				setupCascadeBoundaryGraph(t, b)
				if kind == "node" {
					require.NoError(t, b.DeleteNode(t.Context(), "test:n2"))
				} else {
					require.NoError(t, b.DeleteEdge(t.Context(), "test:r2"))
				}
				require.NoError(t, b.FlushWriteBehind(context.Background()))
				if kind == "node" {
					bufferedCreate(t, b, &Node{ID: "test:n2", Properties: map[string]any{"v": int64(1)}})
					head, err := b.GetNodeCurrentHead(t.Context(), "test:n2")
					require.NoError(t, err)
					require.True(t, head.Tombstoned, "the acknowledged re-create must remain unapplied")
				} else {
					bufferedWrite(t, b, func(tx *BadgerTransaction) error {
						return tx.CreateEdge(t.Context(), &Edge{ID: "test:r2", StartNode: "test:n2", EndNode: "test:n3", Type: "LINK", Properties: map[string]any{"v": int64(1)}})
					})
					head, err := b.GetEdgeCurrentHead(t.Context(), "test:r2")
					require.NoError(t, err)
					require.True(t, head.Tombstoned, "the acknowledged re-create must remain unapplied")
				}
				tx, err := b.BeginTransaction(t.Context())
				require.NoError(t, err)
				t.Cleanup(func() { _ = tx.Rollback() })
				require.NoError(t, tx.SetImplicit(true))
				if kind == "node" {
					if remove {
						require.NoError(t, tx.DeleteGraphNode(t.Context(), "test:n2", CascadeEdges))
					} else {
						before, err := tx.GetNode(t.Context(), "test:n2")
						require.NoError(t, err)
						after := CopyNode(before)
						after.Properties["v"] = int64(2)
						require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: before, After: after, Policy: ClientNodeEdits}))
					}
				} else if remove {
					require.NoError(t, tx.DeleteEdge(t.Context(), "test:r2"))
				} else {
					before, err := tx.GetEdge(t.Context(), "test:r2")
					require.NoError(t, err)
					after := copyEdge(before)
					after.Properties["v"] = int64(2)
					require.NoError(t, tx.UpdateEdge(t.Context(), EdgeUpdateRequest{Before: before, After: after}))
				}
				require.NoError(t, tx.Commit())
				if !remove {
					if kind == "node" {
						updated, err := b.GetNode(t.Context(), "test:n2")
						require.NoError(t, err)
						require.EqualValues(t, 2, updated.Properties["v"])
					} else {
						updated, err := b.GetEdge(t.Context(), "test:r2")
						require.NoError(t, err)
						require.EqualValues(t, 2, updated.Properties["v"])
					}
				}
				require.NoError(t, b.FlushWriteBehind(context.Background()))
				nodes, err := b.AllNodes(t.Context())
				require.NoError(t, err)
				nodeCount, err := b.NodeCount(t.Context())
				require.NoError(t, err)
				require.EqualValues(t, len(nodes), nodeCount)
				edges, err := b.AllEdges(t.Context())
				require.NoError(t, err)
				edgeCount, err := b.EdgeCountByType(t.Context(), "LINK")
				require.NoError(t, err)
				require.EqualValues(t, len(edges), edgeCount)
				require.NoError(t, b.Close())
			})
		}
	}
}

func TestRelationshipUsesAcknowledgedRecreatedEndpoint(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	setupCascadeBoundaryGraph(t, b)
	require.NoError(t, b.DeleteNode(t.Context(), "test:n2"))
	require.NoError(t, b.FlushWriteBehind(context.Background()))
	bufferedCreate(t, b, &Node{ID: "test:n2", Properties: map[string]any{"v": int64(1)}})
	head, err := b.GetNodeCurrentHead(t.Context(), "test:n2")
	require.NoError(t, err)
	require.True(t, head.Tombstoned)
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: "test:r2", StartNode: "test:n2", EndNode: "test:n3", Type: "LINK"}))
	require.NoError(t, tx.Commit())
	require.NoError(t, b.FlushWriteBehind(context.Background()))
	edge, err := b.GetEdge(t.Context(), "test:r2")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:n2"), edge.StartNode)
	require.Equal(t, NodeID("test:n3"), edge.EndNode)
	count, err := b.EdgeCountByType(t.Context(), "LINK")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	require.NoError(t, b.Close())
}
