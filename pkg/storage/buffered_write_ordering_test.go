package storage

import (
	"context"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A write that commits after an acknowledged write-behind commit is ordered
// after it, whichever path it takes: a direct, bulk or explicit-transaction
// write never loses to the older commit's replay. Read right after the write,
// after the flush and after the caches drop, the entity is what the later
// write left: deleted (and free for a re-create), or the later body.
func TestSynchronousWritesOrderAfterBufferedCommits(t *testing.T) {
	type route struct {
		name    string
		node    func(*testing.T, *BadgerEngine, *Node) error
		edge    func(*testing.T, *BadgerEngine, *Edge) error
		delNode func(*testing.T, *BadgerEngine, NodeID) error
		delEdge func(*testing.T, *BadgerEngine, EdgeID) error
		create  func(*testing.T, *BadgerEngine, *Node, *Edge) error
	}
	explicit := func(t *testing.T, b *BadgerEngine, apply func(*BadgerTransaction) error) error {
		t.Helper()
		tx, err := b.BeginTransaction(t.Context())
		require.NoError(t, err)
		if err := apply(tx); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	}
	routes := []route{
		{
			name: "direct",
			node: func(_ *testing.T, b *BadgerEngine, n *Node) error {
				return b.UpdateNode(t.Context(), NodeUpdateRequest{After: n})
			},
			edge: func(_ *testing.T, b *BadgerEngine, e *Edge) error {
				return b.UpdateEdge(t.Context(), EdgeUpdateRequest{After: e})
			},
			delNode: func(_ *testing.T, b *BadgerEngine, id NodeID) error { return b.DeleteNode(t.Context(), id) },
			delEdge: func(_ *testing.T, b *BadgerEngine, id EdgeID) error { return b.DeleteEdge(t.Context(), id) },
			create: func(_ *testing.T, b *BadgerEngine, n *Node, e *Edge) error {
				if n != nil {
					_, err := b.CreateNode(t.Context(), n)
					return err
				}
				return b.CreateEdge(t.Context(), e)
			},
		},
		{
			name: "bulk",
			node: func(_ *testing.T, b *BadgerEngine, n *Node) error {
				return b.UpdateNode(t.Context(), NodeUpdateRequest{After: n})
			},
			edge: func(_ *testing.T, b *BadgerEngine, e *Edge) error {
				return b.UpdateEdge(t.Context(), EdgeUpdateRequest{After: e})
			},
			delNode: func(_ *testing.T, b *BadgerEngine, id NodeID) error {
				return b.BulkDeleteNodes(t.Context(), []NodeID{id})
			},
			delEdge: func(_ *testing.T, b *BadgerEngine, id EdgeID) error {
				return b.BulkDeleteEdges(t.Context(), []EdgeID{id})
			},
			create: func(_ *testing.T, b *BadgerEngine, n *Node, e *Edge) error {
				if n != nil {
					return b.BulkCreateNodes(t.Context(), []*Node{n})
				}
				return b.BulkCreateEdges(t.Context(), []*Edge{e})
			},
		},
		{
			name: "explicit transaction",
			node: func(t *testing.T, b *BadgerEngine, n *Node) error {
				return explicit(t, b, func(tx *BadgerTransaction) error {
					return tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: n, Policy: ClientNodeEdits})
				})
			},
			edge: func(t *testing.T, b *BadgerEngine, e *Edge) error {
				return explicit(t, b, func(tx *BadgerTransaction) error { return tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: e}) })
			},
			delNode: func(t *testing.T, b *BadgerEngine, id NodeID) error {
				return explicit(t, b, func(tx *BadgerTransaction) error { return tx.DeleteGraphNode(t.Context(), id, CascadeEdges) })
			},
			delEdge: func(t *testing.T, b *BadgerEngine, id EdgeID) error {
				return explicit(t, b, func(tx *BadgerTransaction) error { return tx.DeleteEdge(t.Context(), id) })
			},
			create: func(t *testing.T, b *BadgerEngine, n *Node, e *Edge) error {
				return explicit(t, b, func(tx *BadgerTransaction) error {
					if n != nil {
						_, err := tx.CreateNode(t.Context(), n)
						return err
					}
					return tx.CreateEdge(t.Context(), e)
				})
			},
		},
	}
	edge := func(v string) *Edge {
		return &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"v": v}}
	}
	for _, buffered := range []bool{false, true} {
		for _, r := range routes {
			for _, kind := range []string{"node", "relationship"} {
				for _, op := range []string{"update", "delete", "hidden delete"} {
					t.Run(fmt.Sprintf("write-behind=%v/%s/%s/%s", buffered, r.name, kind, op), func(t *testing.T) {
						b, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour}))
						require.NoError(t, err)
						t.Cleanup(func() { _ = b.Close() })
						if buffered {
							// The order of the writes decides, not a flush racing them.
							b.writeBehind.SetDelayBounds(time.Hour, time.Hour)
						}
						for _, id := range []NodeID{"test:a", "test:b", "test:n"} {
							_, err := b.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"v": "initial"}})
							require.NoError(t, err)
						}
						require.NoError(t, b.CreateEdge(t.Context(), edge("initial")))
						require.NoError(t, b.FlushWriteBehind(context.Background()))
						if op == "hidden delete" {
							if kind == "node" {
								require.NoError(t, b.SetManualSuppression(context.Background(), StoredEntityNodeKind, "test:n", true))
							} else {
								require.NoError(t, b.SetManualSuppression(context.Background(), StoredEntityEdgeKind, "test:r", true))
							}
						}
						// An acknowledged, unflushed commit of the entity.
						tx, err := b.BeginTransaction(t.Context())
						require.NoError(t, err)
						require.NoError(t, tx.SetImplicit(true))
						if kind == "node" {
							require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: &Node{ID: "test:n", Properties: map[string]any{"v": "acknowledged"}}, Policy: ClientNodeEdits}))
						} else {
							require.NoError(t, tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: edge("acknowledged")}))
						}
						require.NoError(t, tx.Commit())

						want := "later"
						if op == "update" {
							if kind == "node" {
								require.NoError(t, r.node(t, b, &Node{ID: "test:n", Properties: map[string]any{"v": want}}))
							} else {
								require.NoError(t, r.edge(t, b, edge(want)))
							}
						} else {
							if kind == "node" {
								require.NoError(t, r.delNode(t, b, "test:n"))
								_, err = b.GetStoredNode(t.Context(), "test:n")
							} else {
								require.NoError(t, r.delEdge(t, b, "test:r"))
								_, err = b.GetStoredEdge(t.Context(), "test:r")
							}
							require.ErrorIs(t, err, ErrNotFound, "a deleted entity is gone")
							want = "replacement"
							if kind == "node" {
								require.NoError(t, r.create(t, b, &Node{ID: "test:n", Properties: map[string]any{"v": want}}, nil), "a deleted ID is free")
							} else {
								require.NoError(t, r.create(t, b, nil, edge(want)), "a deleted ID is free")
							}
						}
						read := func(stage string) {
							t.Helper()
							if kind == "node" {
								n, err := b.GetStoredNode(t.Context(), "test:n")
								require.NoError(t, err, stage)
								require.Equal(t, want, n.Properties["v"], stage)
							} else {
								e, err := b.GetStoredEdge(t.Context(), "test:r")
								require.NoError(t, err, stage)
								require.Equal(t, want, e.Properties["v"], stage)
							}
						}
						read("after the write")
						require.NoError(t, b.FlushWriteBehind(context.Background()))
						b.invalidateCachesAfterRestore()
						read("after the flush")
					})
				}
			}
		}
	}
}

// Deleting a node removes the relationships an acknowledged, unflushed commit
// gave it, on every delete path: none is left dangling after the flush.
func TestSynchronousDeleteRemovesBufferedRelationships(t *testing.T) {
	for _, path := range []string{"direct", "bulk", "explicit transaction"} {
		t.Run(path, func(t *testing.T) {
			b, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			b.writeBehind.SetDelayBounds(time.Hour, time.Hour)
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := b.CreateNode(t.Context(), &Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, b.FlushWriteBehind(context.Background()))
			tx, err := b.BeginTransaction(t.Context())
			require.NoError(t, err)
			require.NoError(t, tx.SetImplicit(true))
			require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: "test:s", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))
			require.NoError(t, tx.Commit())
			switch path {
			case "direct":
				require.NoError(t, b.DeleteNode(t.Context(), "test:a"))
			case "bulk":
				require.NoError(t, b.BulkDeleteNodes(t.Context(), []NodeID{"test:a"}))
			default:
				del, err := b.BeginTransaction(t.Context())
				require.NoError(t, err)
				require.NoError(t, del.DeleteGraphNode(t.Context(), "test:a", CascadeEdges))
				require.NoError(t, del.Commit())
			}
			check := func(stage string) {
				t.Helper()
				_, err := b.GetStoredEdge(t.Context(), "test:s")
				require.ErrorIs(t, err, ErrNotFound, stage)
				incoming, err := b.ReadStoredAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "test:b"})
				require.NoError(t, err, stage)
				require.Empty(t, incoming, stage)
				count, err := b.EdgeCount(t.Context())
				require.NoError(t, err, stage)
				require.Zero(t, count, stage)
			}
			check("after the delete")
			require.NoError(t, b.FlushWriteBehind(context.Background()))
			b.invalidateCachesAfterRestore()
			check("after the flush")
		})
	}
}
