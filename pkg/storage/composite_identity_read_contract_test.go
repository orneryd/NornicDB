package storage

import (
	"context"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompositeIdentityReadContract(t *testing.T) {
	for _, namespaced := range []bool{false, true} {
		name := "raw"
		if namespaced {
			name = "namespaced"
		}
		t.Run(name, func(t *testing.T) {
			prefix := ""
			if !namespaced {
				prefix = "raw:"
			}
			same, localTarget := NodeID(prefix+"same"), NodeID(prefix+"target")
			relation := EdgeID(prefix + "same-rel")
			engines := make(map[string]Engine)
			names := map[string]string{"one": "one", "one_alias": "one", "two": "two"}
			modes := map[string]string{"one": "read_write", "one_alias": "read", "two": "read_write"}
			var version MVCCVersion
			for _, database := range []string{"one", "two"} {
				var engine Engine
				if namespaced {
					base, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
					require.NoError(t, err)
					engine = NewNamespacedEngine(runtimeFixtureOwner.Own(t, base), database)
				} else {
					engine = runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, NewMemoryEngine(), ErrStorageClosed))
				}
				engines[database] = engine
				for _, id := range []NodeID{same, localTarget} {
					_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"P"}, Properties: map[string]interface{}{"db": database}})
					require.NoError(t, err)
				}
				require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: relation, StartNode: same, EndNode: localTarget, Type: "R"}))
				if provider, ok := engine.(MVCCHeadEngine); ok {
					head, err := provider.GetEdgeCurrentHead(context.Background(), relation)
					require.NoError(t, err)
					if head.Version.Compare(version) > 0 {
						version = head.Version
					}
				}
			}
			engines["one_alias"] = engines["one"]
			composite := NewCompositeEngine(engines, names, modes)
			composite.SetCompositeName("comp")
			readers := map[string]func(context.Context, NodeID) (*Node, error){
				"node": func(ctx context.Context, id NodeID) (*Node, error) { return composite.GetNode(ctx, id) },
				"projected": func(ctx context.Context, id NodeID) (*Node, error) {
					return composite.GetNodeProjected(ctx, id, []string{"db"})
				},
				"effective": composite.GetNodeLatestEffective,
				"latest":    func(ctx context.Context, id NodeID) (*Node, error) { return composite.GetNodeLatestVisible(ctx, id) },
				"snapshot": func(ctx context.Context, id NodeID) (*Node, error) {
					return composite.GetNodeVisibleAt(ctx, id, version)
				},
			}
			for label, read := range readers {
				t.Run(label, func(t *testing.T) {
					for _, database := range []string{"one", "two"} {
						node, err := read(context.Background(), NodeID("4:"+database+":"+string(same)))
						require.NoError(t, err)
						require.Equal(t, same, node.ID)
						require.Equal(t, database, node.RuntimeMetadata().Database)
						require.Equal(t, database, node.Properties["db"])
					}
					_, err := read(context.Background(), NodeID("4:missing:"+string(same)))
					require.ErrorIs(t, err, ErrNotFound)
					_, err = read(context.Background(), NodeID("5:one:"+string(same)))
					require.ErrorIs(t, err, ErrNotFound)
				})
			}
			for _, database := range []string{"one", "two"} {
				source, target := NodeID("4:"+database+":"+string(same)), NodeID("4:"+database+":"+string(localTarget))
				adjacency := []func() ([]*Edge, error){
					func() ([]*Edge, error) {
						return composite.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Node: source})
					},
					func() ([]*Edge, error) {
						return composite.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: target})
					},
					func() ([]*Edge, error) {
						return composite.ReadAdjacentEdgesVisibleAt(context.Background(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: source}, version)
					},
					func() ([]*Edge, error) {
						return composite.ReadAdjacentEdgesVisibleAt(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: target}, version)
					},
					func() ([]*Edge, error) {
						return composite.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: source, Other: target})
					},
					func() ([]*Edge, error) {
						return composite.ReadAdjacentEdgesVisibleAt(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: source, Other: target}, version)
					},
				}
				for _, read := range adjacency {
					edges, err := read()
					require.NoError(t, err)
					require.Len(t, edges, 1)
					require.Equal(t, database, edges[0].Database)
				}
				edge, err := composite.GetEdgeBetween(t.Context(), source, target, "R")
				require.NoError(t, err)
				require.Equal(t, database, edge.Database)
				degree, err := composite.GetOutDegree(t.Context(), source)
				require.NoError(t, err)
				require.Equal(t, 1, degree)
				degree, err = composite.GetInDegree(t.Context(), target)
				require.NoError(t, err)
				require.Equal(t, 1, degree)
				if namespaced {
					_, err := composite.GetNodeCurrentHead(context.Background(), source)
					require.NoError(t, err)
				}
				for _, read := range []func(context.Context, EdgeID) (*Edge, error){func(ctx context.Context, id EdgeID) (*Edge, error) { return composite.GetEdge(ctx, id) }, composite.GetEdgeLatestEffective, func(ctx context.Context, id EdgeID) (*Edge, error) { return composite.GetEdgeLatestVisible(ctx, id) }} {
					edge, err := read(context.Background(), EdgeID("5:"+database+":"+string(relation)))
					require.NoError(t, err)
					require.Equal(t, database, edge.Database)
				}
			}
			edges, err := composite.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: NodeID("4:one:" + string(same)), Other: NodeID("4:two:" + string(localTarget))})
			require.NoError(t, err)
			require.Empty(t, edges)
			one, two := NodeID("4:one:"+string(same)), NodeID("4:two:"+string(same))
			batch, err := composite.BatchGetNodes(context.Background(), []NodeID{one, two, NodeID("4:missing:" + string(same))})
			require.NoError(t, err)
			require.Len(t, batch, 2)
			require.Equal(t, "one", batch[one].RuntimeMetadata().Database)
			require.Equal(t, "two", batch[two].RuntimeMetadata().Database)
			seen := make(map[string]bool)
			require.NoError(t, composite.StreamNodesWithOptions(context.Background(), StreamNodesOptions{}, func(node *Node) error {
				seen[node.IdentityKey()] = true
				return nil
			}))
			require.Len(t, seen, 4, "alias reads once; different databases remain distinct")
			seen = make(map[string]bool)
			require.NoError(t, composite.StreamEdgesByType(context.Background(), "R", func(edge *Edge) error {
				seen[edge.IdentityKey()] = true
				return nil
			}))
			require.Len(t, seen, 2)
			if !namespaced {
				node, err := engines["one"].GetNode(t.Context(), same)
				require.NoError(t, err)
				require.Empty(t, node.RuntimeMetadata().Database, "composite provenance must not mutate constituent records")
			}
		})
	}
}
