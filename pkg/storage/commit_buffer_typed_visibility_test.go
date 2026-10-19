package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteBehind_TypedNodeEdgeIdentityAPI(t *testing.T) {
	for _, shared := range []bool{true, false} {
		t.Run(fmt.Sprintf("shared=%v", shared), func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			node := &Node{ID: "test:collision", Labels: []string{"Person"}}
			_, err := b.CreateNode(node)
			require.NoError(t, err)
			_, err = b.CreateNode(&Node{ID: "test:peer"})
			require.NoError(t, err)
			id := EdgeID("test:edge-control")
			if shared {
				id = EdgeID(node.ID)
			}
			edge := &Edge{ID: id, StartNode: node.ID, EndNode: "test:peer", Type: "REL"}
			require.NoError(t, b.CreateEdge(edge))
			readEdge := func() (*Edge, error) {
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetNamespace("test"))
				return tx.GetEdge(id)
			}
			before, err := readEdge()
			require.NoError(t, err, "SETUP: persisted graph and public transaction reader")
			require.Equal(t, edge.ID, before.ID)
			t.Log("SETUP PASS: initial graph persisted; public tx.GetEdge accepts identity")
			bufferedDeleteNode(t, b, node.ID)
			_, err = readEdge()
			require.ErrorIs(t, err, ErrNotFound, "detach-delete initially hides edge")
			bufferedCreate(t, b, &Node{ID: node.ID, Labels: node.Labels, Properties: map[string]any{"recreated": true}})
			require.Greater(t, b.writeBehind.PendingOps(), 0, "mutations stay unflushed")
			_, err = b.GetNode(node.ID)
			require.NoError(t, err)
			t.Run("point", func(t *testing.T) {
				_, err := readEdge()
				require.ErrorIs(t, err, ErrNotFound, "node recreation cannot revive detached edge")
			})
			t.Run("type", func(t *testing.T) {
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetNamespace("test"))
				edges, err := tx.GetEdgesByType("REL")
				require.NoError(t, err)
				require.Empty(t, edges)
			})
		})
	}
}

func TestWriteBehindBuffer_NewestTypedStateConverges(t *testing.T) {
	for _, kind := range []string{"node", "edge"} {
		for _, newestDeleted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/newestDeleted=%v", kind, newestDeleted), func(t *testing.T) {
				apply := newBlockingApply()
				w := NewWriteBehindBuffer(time.Hour, 0, apply.fn)
				t.Cleanup(func() {
					close(apply.release)
					err := w.Flush()
					w.Close()
					require.NoError(t, err)
				})
				node := &Node{ID: "test:state", Labels: []string{"Person"}}
				edge := &Edge{ID: "test:state", StartNode: "test:from", EndNode: "test:to", Type: "REL"}
				live := Operation{Type: OpCreateNode, Node: node}
				dead := Operation{Type: OpDeleteNode, NodeID: node.ID}
				if kind == "edge" {
					live = Operation{Type: OpCreateEdge, Edge: edge}
					// No body or type entry in the pure-deletion generation.
					dead = Operation{Type: OpDeleteNode, NodeID: "test:unrelated", DeletedEdgeIDs: []EdgeID{edge.ID}}
				}
				older, newer := dead, live
				if newestDeleted {
					older, newer = live, dead
				}
				w.AppendCommit(bufferedCommit{ops: []Operation{older}})
				w.FlushAsync()
				select {
				case <-apply.started:
				case <-time.After(5 * time.Second):
					t.Fatal("older generation did not start blocked apply")
				}
				w.AppendCommit(bufferedCommit{ops: []Operation{newer}})
				t.Run("point", func(t *testing.T) {
					var found, deleted bool
					if kind == "node" {
						_, found, deleted = w.GetNode(node.ID)
					} else {
						_, found, deleted = w.GetEdge(edge.ID)
					}
					require.True(t, found)
					require.Equal(t, newestDeleted, deleted)
				})
				t.Run("boolean", func(t *testing.T) {
					if kind == "node" {
						require.Equal(t, newestDeleted, w.NodeDeleted(node.ID))
					} else {
						require.Equal(t, newestDeleted, w.EdgeDeleted(edge.ID))
					}
				})
				t.Run("lookup", func(t *testing.T) {
					var found bool
					if kind == "node" {
						_, found = w.LookupNode(node.ID)
					} else {
						_, found = w.LookupEdge(edge.ID)
					}
					require.Equal(t, !newestDeleted, found)
				})
				t.Run("typed-overlay", func(t *testing.T) {
					want := 1
					if newestDeleted {
						want = 0
					}
					if kind == "node" {
						rows, touched := w.LabelOverlay("Person")
						require.True(t, touched[node.ID])
						require.Len(t, rows, want)
					} else {
						rows, touched := w.TypeOverlay("REL")
						require.True(t, touched[edge.ID])
						require.Len(t, rows, want)
					}
				})
				t.Run("all-overlay", func(t *testing.T) {
					want := 1
					if newestDeleted {
						want = 0
					}
					if kind == "node" {
						rows, touched := w.AllNodesOverlay()
						require.True(t, touched[node.ID])
						require.Len(t, rows, want)
					} else {
						rows, touched := w.AllEdgesOverlay()
						require.True(t, touched[edge.ID])
						require.Len(t, rows, want)
					}
				})
				if kind == "edge" {
					t.Run("adjacency", func(t *testing.T) {
						rows, suppressed, nodeDeleted := w.EdgeAdjacencyOverlay(edge.StartNode, true)
						require.False(t, nodeDeleted)
						if newestDeleted {
							require.Empty(t, rows)
							require.Contains(t, suppressed, edge.ID)
						} else {
							require.Equal(t, []*Edge{edge}, rows)
						}
					})
				}
			})
		}
	}
}

func TestCommitBuffer_TypedIdentityRevivesOnlyMatchingKind(t *testing.T) {
	for _, reviveNode := range []bool{true, false} {
		t.Run(fmt.Sprintf("reviveNode=%v", reviveNode), func(t *testing.T) {
			node := &Node{ID: "test:shared", Labels: []string{"Person"}}
			edge := &Edge{ID: EdgeID(node.ID), StartNode: node.ID, EndNode: "test:peer", Type: "REL"}
			generation := newCommitBuffer()
			generation.AddCommit(bufferedCommit{ops: []Operation{{Type: OpCreateNode, Node: node}, {Type: OpCreateEdge, Edge: edge}}})
			generation.AddCommit(bufferedCommit{ops: []Operation{{Type: OpDeleteNode, NodeID: node.ID, OldNode: node, DeletedEdgeIDs: []EdgeID{edge.ID}}}})
			if reviveNode {
				generation.AddCommit(bufferedCommit{ops: []Operation{{Type: OpCreateNode, Node: node}}})
			} else {
				generation.AddCommit(bufferedCommit{ops: []Operation{{Type: OpCreateEdge, Edge: edge}}})
			}
			w := &WriteBehindBuffer{active: generation}
			_, nodeFound, nodeDeleted := w.GetNode(node.ID)
			_, edgeFound, edgeDeleted := w.GetEdge(edge.ID)
			require.True(t, nodeFound)
			require.True(t, edgeFound)
			require.Equal(t, !reviveNode, nodeDeleted)
			require.Equal(t, reviveNode, edgeDeleted)
			require.Equal(t, nodeDeleted, w.NodeDeleted(node.ID))
			require.Equal(t, edgeDeleted, w.EdgeDeleted(edge.ID))
			nodes, _ := w.AllNodesOverlay()
			edges, _ := w.AllEdgesOverlay()
			if reviveNode {
				require.Equal(t, []*Node{node}, nodes)
				require.Empty(t, edges)
			} else {
				require.Empty(t, nodes)
				require.Equal(t, []*Edge{edge}, edges)
			}
		})
	}
}

func TestWriteBehindBuffer_LegacyGenericDeleteKeepsKindsIndependent(t *testing.T) {
	w := NewWriteBehindBuffer(time.Hour, 0, func(*CommitBuffer) error { return nil })
	defer w.Close()
	node := &Node{ID: "test:legacy", Labels: []string{"Person"}}
	edge := &Edge{ID: EdgeID(node.ID), StartNode: node.ID, EndNode: "test:peer", Type: "REL"}
	w.AppendNode(node)
	w.AppendEdge(edge)
	w.AppendDelete(string(node.ID))
	require.True(t, w.NodeDeleted(node.ID))
	require.True(t, w.EdgeDeleted(edge.ID))
	w.AppendNode(node)
	require.False(t, w.NodeDeleted(node.ID))
	require.True(t, w.EdgeDeleted(edge.ID), "legacy generic delete still hides the other kind")
	w.AppendEdge(edge)
	require.False(t, w.EdgeDeleted(edge.ID))
	require.False(t, w.NodeDeleted(node.ID))
	require.Len(t, w.LabelNodes("Person"), 1)
	require.Len(t, w.TypeEdges("REL"), 1)
}

func TestWriteBehindBuffer_AppliedTypedStateRemainsUntilRetirement(t *testing.T) {
	old := newCommitBuffer()
	old.AddCommit(bufferedCommit{ops: []Operation{{Type: OpDeleteNode, NodeID: "test:gone", DeletedEdgeIDs: []EdgeID{"test:gone"}}}})
	old.markApplied()
	w := &WriteBehindBuffer{active: newCommitBuffer(), draining: []*CommitBuffer{old}}
	check := func(retained bool) {
		_, nodeFound, nodeDeleted := w.GetNode("test:gone")
		_, edgeFound, edgeDeleted := w.GetEdge("test:gone")
		require.Equal(t, retained, nodeFound)
		require.Equal(t, retained, edgeFound)
		require.Equal(t, retained, nodeDeleted)
		require.Equal(t, retained, edgeDeleted)
		require.Equal(t, retained, w.NodeDeleted("test:gone"))
		require.Equal(t, retained, w.EdgeDeleted("test:gone"))
		nodes, touchedNodes := w.AllNodesOverlay()
		edges, touchedEdges := w.AllEdgesOverlay()
		require.Empty(t, nodes)
		require.Empty(t, edges)
		require.Equal(t, retained, touchedNodes["test:gone"])
		require.Equal(t, retained, touchedEdges["test:gone"])
	}
	// Applied is replay progress. Readers retain tombstones until the same
	// write lock that guards snapshot capture removes the generation.
	check(true)
	w.removeDrained(old)
	check(false)
}

func TestWriteBehindBuffer_NewestEdgeBodyControlsTypeAndEndpoints(t *testing.T) {
	old := newCommitBuffer()
	before := &Edge{ID: "test:moved", StartNode: "test:old-start", EndNode: "test:old-end", Type: "OLD"}
	old.AddCommit(bufferedCommit{ops: []Operation{{Type: OpCreateEdge, Edge: before}}})
	active := newCommitBuffer()
	after := &Edge{ID: before.ID, StartNode: "test:new-start", EndNode: "test:new-end", Type: "NEW"}
	active.AddCommit(bufferedCommit{ops: []Operation{{Type: OpUpdateEdge, Edge: after, OldEdge: before}}})
	w := &WriteBehindBuffer{active: active, draining: []*CommitBuffer{old}}
	require.Empty(t, w.TypeEdges("OLD"))
	require.Equal(t, []*Edge{after}, w.TypeEdges("NEW"))
	for _, outgoing := range []bool{true, false} {
		oldEndpoint, newEndpoint := before.EndNode, after.EndNode
		if outgoing {
			oldEndpoint, newEndpoint = before.StartNode, after.StartNode
		}
		rows, suppressed, nodeDeleted := w.EdgeAdjacencyOverlay(oldEndpoint, outgoing)
		require.False(t, nodeDeleted)
		require.Empty(t, rows)
		require.Contains(t, suppressed, before.ID)
		rows, suppressed, nodeDeleted = w.EdgeAdjacencyOverlay(newEndpoint, outgoing)
		require.False(t, nodeDeleted)
		require.Equal(t, []*Edge{after}, rows)
		require.Contains(t, suppressed, after.ID)
	}
}
