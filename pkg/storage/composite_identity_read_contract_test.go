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
					_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"P"}, Properties: map[string]interface{}{"db": database}})
					require.NoError(t, err)
				}
				require.NoError(t, engine.CreateEdge(&Edge{ID: relation, StartNode: same, EndNode: localTarget, Type: "R"}))
				if provider, ok := engine.(MVCCHeadEngine); ok {
					head, err := provider.GetEdgeCurrentHead(relation)
					require.NoError(t, err)
					if head.Version.Compare(version) > 0 {
						version = head.Version
					}
				}
			}
			engines["one_alias"] = engines["one"]
			composite := NewCompositeEngine(engines, names, modes)
			composite.SetCompositeName("comp")
			readers := map[string]func(NodeID) (*Node, error){
				"node":      composite.GetNode,
				"projected": func(id NodeID) (*Node, error) { return composite.GetNodeProjected(id, []string{"db"}) },
				"effective": composite.GetNodeLatestEffective,
				"latest":    composite.GetNodeLatestVisible,
				"snapshot":  func(id NodeID) (*Node, error) { return composite.GetNodeVisibleAt(id, version) },
			}
			for label, read := range readers {
				t.Run(label, func(t *testing.T) {
					for _, database := range []string{"one", "two"} {
						node, err := read(NodeID("4:" + database + ":" + string(same)))
						require.NoError(t, err)
						require.Equal(t, same, node.ID)
						require.Equal(t, database, node.RuntimeMetadata().Database)
						require.Equal(t, database, node.Properties["db"])
					}
					_, err := read(NodeID("4:missing:" + string(same)))
					require.ErrorIs(t, err, ErrNotFound)
					_, err = read(NodeID("5:one:" + string(same)))
					require.ErrorIs(t, err, ErrNotFound)
				})
			}
			for _, database := range []string{"one", "two"} {
				source, target := NodeID("4:"+database+":"+string(same)), NodeID("4:"+database+":"+string(localTarget))
				adjacency := []func() ([]*Edge, error){
					func() ([]*Edge, error) { return composite.GetOutgoingEdges(source) },
					func() ([]*Edge, error) { return composite.GetIncomingEdges(target) },
					func() ([]*Edge, error) { return composite.GetOutgoingEdgesVisibleAt(source, version) },
					func() ([]*Edge, error) { return composite.GetIncomingEdgesVisibleAt(target, version) },
					func() ([]*Edge, error) { return composite.GetEdgesBetween(source, target) },
					func() ([]*Edge, error) { return composite.GetEdgesBetweenVisibleAt(source, target, version) },
				}
				for _, read := range adjacency {
					edges, err := read()
					require.NoError(t, err)
					require.Len(t, edges, 1)
					require.Equal(t, database, edges[0].Database)
				}
				require.Equal(t, database, composite.GetEdgeBetween(source, target, "R").Database)
				require.Equal(t, 1, composite.GetOutDegree(source))
				require.Equal(t, 1, composite.GetInDegree(target))
				if namespaced {
					_, err := composite.GetNodeCurrentHead(source)
					require.NoError(t, err)
				}
				for _, read := range []func(EdgeID) (*Edge, error){composite.GetEdge, composite.GetEdgeLatestEffective, composite.GetEdgeLatestVisible} {
					edge, err := read(EdgeID("5:" + database + ":" + string(relation)))
					require.NoError(t, err)
					require.Equal(t, database, edge.Database)
				}
			}
			edges, err := composite.GetEdgesBetween(NodeID("4:one:"+string(same)), NodeID("4:two:"+string(localTarget)))
			require.NoError(t, err)
			require.Empty(t, edges)
			one, two := NodeID("4:one:"+string(same)), NodeID("4:two:"+string(same))
			batch, err := composite.BatchGetNodes([]NodeID{one, two, NodeID("4:missing:" + string(same))})
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
				node, err := engines["one"].GetNode(same)
				require.NoError(t, err)
				require.Empty(t, node.RuntimeMetadata().Database, "composite provenance must not mutate constituent records")
			}
		})
	}
}
