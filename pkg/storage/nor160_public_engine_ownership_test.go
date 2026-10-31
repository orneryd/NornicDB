// Author: Codex, NOR-160 public storage read ownership, 29 October 2026.
package storage

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

type nor160PublicEngineFixture struct {
	t      *testing.T
	engine Engine
	a, b   NodeID
	edge   EdgeID
}

type nor160PublicEngineCase struct {
	name string
	read func() (any, error)
	full bool
}

func nor160PublicEngineFixtureFor(t *testing.T, kind string) nor160PublicEngineFixture {
	t.Helper()
	base, tx := nor160OwnershipFixture(t, true, false)
	require.NoError(t, tx.Rollback())
	f := nor160PublicEngineFixture{t: t, engine: base, a: "test:a", b: "test:b", edge: "test:e"}
	switch kind {
	case "badger":
		f.engine = base.BadgerEngine
	case "memory":
	case "namespaced":
		f.engine = NewNamespacedEngine(base, "test")
		f.a, f.b, f.edge = "a", "b", "e"
	case "wal":
		wal := NewWALEngine(base, nil)
		t.Cleanup(wal.stopEntityWait)
		f.engine = wal
	case "traced":
		f.engine = NewTracedEngine(base)
	case "composite":
		namespace := NewNamespacedEngine(base, "test")
		f.engine = NewCompositeEngine(map[string]Engine{"test": namespace}, map[string]string{"test": "test"}, map[string]string{"test": "read_write"})
		f.a, f.b, f.edge = "a", "b", "e"
	default:
		t.Fatalf("unclassified public engine factory %q", kind)
	}
	return f
}

func nor160PublicEngineCases(f nor160PublicEngineFixture) []nor160PublicEngineCase {
	e, ctx := f.engine, context.Background()
	nodes := func(run func(func(*Node) error) error) (any, error) {
		var values []*Node
		err := run(func(n *Node) error { values = append(values, n); return nil })
		return values, err
	}
	edges := func(run func(func(*Edge) error) error) (any, error) {
		var values []*Edge
		err := run(func(edge *Edge) error { values = append(values, edge); return nil })
		return values, err
	}
	cases := []nor160PublicEngineCase{
		{"GetNode", func() (any, error) { return e.GetNode(f.a) }, true},
		{"GetEdge", func() (any, error) { return e.GetEdge(f.edge) }, false},
		{"GetNodesByLabel", func() (any, error) { return e.GetNodesByLabel("Held") }, false},
		{"GetFirstNodeByLabel", func() (any, error) { return e.GetFirstNodeByLabel("Held") }, false},
		{"GetOutgoingEdges", func() (any, error) { return e.GetOutgoingEdges(f.a) }, false},
		{"GetIncomingEdges", func() (any, error) { return e.GetIncomingEdges(f.b) }, false},
		{"GetEdgesBetween", func() (any, error) { return e.GetEdgesBetween(f.a, f.b) }, false},
		{"GetEdgeBetween", func() (any, error) { return e.GetEdgeBetween(f.a, f.b, "T"), nil }, false},
		{"GetEdgesByType", func() (any, error) { return e.GetEdgesByType("T") }, false},
		{"AllNodes", func() (any, error) { return e.AllNodes() }, false},
		{"GetAllNodes", func() (any, error) { return e.GetAllNodes(), nil }, false},
		{"AllEdges", func() (any, error) { return e.AllEdges() }, false},
		{"BatchGetNodes", func() (any, error) { return e.BatchGetNodes([]NodeID{f.a, f.b}) }, false},
		{"StreamNodesWithOptions/full", func() (any, error) {
			return nodes(func(visit func(*Node) error) error {
				return e.StreamNodesWithOptions(ctx, StreamNodesOptions{WithEmbeddings: true}, visit)
			})
		}, false},
		{"StreamNodesWithOptions/query", func() (any, error) {
			return nodes(func(visit func(*Node) error) error {
				return e.StreamNodesWithOptions(ctx, StreamNodesOptions{QueryNodes: true}, visit)
			})
		}, false},
		{"StreamNodesWithOptions/projected", func() (any, error) {
			return nodes(func(visit func(*Node) error) error {
				return e.StreamNodesWithOptions(ctx, StreamNodesOptions{Projection: []string{"nested", "bytes"}, StripEmbeddings: true}, visit)
			})
		}, false},
		{"GetStoredNode/helper", func() (any, error) { return GetStoredNode(e, f.a) }, true},
		{"GetStoredEdge/helper", func() (any, error) { return GetStoredEdge(e, f.edge) }, false},
		{"GetStoredOutgoingEdges/helper", func() (any, error) { return GetStoredOutgoingEdges(e, f.a) }, false},
		{"GetStoredIncomingEdges/helper", func() (any, error) { return GetStoredIncomingEdges(e, f.b) }, false},
		{"GetStoredNodesByLabel/helper", func() (any, error) { return GetStoredNodesByLabel(e, "Held") }, false},
		{"MatchEdgesBetween/helper", func() (any, error) {
			return MatchEdgesBetween(e, f.a, f.b, "T", []string{"nested"}, func(*Edge) bool { return true })
		}, false},
	}
	if r, ok := e.(QueryNodeReader); ok {
		cases = append(cases, nor160PublicEngineCase{"GetQueryNode", func() (any, error) { return r.GetQueryNode(f.a) }, false})
	}
	if r, ok := e.(ProjectedNodeReader); ok {
		cases = append(cases,
			nor160PublicEngineCase{"GetNodeProjected/nil", func() (any, error) { return r.GetNodeProjected(f.a, nil) }, true},
			nor160PublicEngineCase{"GetNodeProjected/selected", func() (any, error) { return r.GetNodeProjected(f.a, []string{"nested", "bytes"}) }, false},
			nor160PublicEngineCase{"GetNodeProjected/empty", func() (any, error) { return r.GetNodeProjected(f.a, []string{}) }, false})
	}
	if r, ok := e.(NodeWithoutEmbeddingsReader); ok {
		cases = append(cases, nor160PublicEngineCase{"GetNodeWithoutEmbeddings", func() (any, error) { return r.GetNodeWithoutEmbeddings(f.a) }, false})
	}
	if r, ok := e.(BatchNodeWithoutEmbeddingsReader); ok {
		cases = append(cases, nor160PublicEngineCase{"BatchGetNodesWithoutEmbeddings", func() (any, error) { return r.BatchGetNodesWithoutEmbeddings([]NodeID{f.a, f.b}) }, false})
	}
	if r, ok := e.(NodeEmbeddingMetaReader); ok {
		cases = append(cases, nor160PublicEngineCase{"GetNodeEmbeddingMeta", func() (any, error) { return r.GetNodeEmbeddingMeta(f.a) }, false})
	}
	if r, ok := e.(AdjacentEdgesEngine); ok {
		cases = append(cases, nor160PublicEngineCase{"GetAdjacentEdges", func() (any, error) { out, in, err := r.GetAdjacentEdges(f.a); return []any{out, in}, err }, false})
	}
	if r, ok := e.(EdgeHeaderReader); ok {
		cases = append(cases,
			nor160PublicEngineCase{"OutgoingEdgeHeaders", func() (any, error) {
				out, answered, err := r.OutgoingEdgeHeaders(f.a)
				if err == nil && !answered {
					return e.GetOutgoingEdges(f.a)
				}
				return out, err
			}, false},
			nor160PublicEngineCase{"IncomingEdgeHeaders", func() (any, error) {
				in, answered, err := r.IncomingEdgeHeaders(f.b)
				if err == nil && !answered {
					return e.GetIncomingEdges(f.b)
				}
				return in, err
			}, false})
	}
	if r, ok := e.(ProjectedLabelNodeReader); ok {
		for _, mask := range []struct {
			name   string
			fields []string
		}{{"nil", nil}, {"selected", []string{"nested", "bytes"}}, {"empty", []string{}}} {
			cases = append(cases, nor160PublicEngineCase{"StreamNodesByLabelProjected/" + mask.name, func() (any, error) {
				return nodes(func(visit func(*Node) error) error { return r.StreamNodesByLabelProjected("Held", mask.fields, visit) })
			}, false})
		}
	}
	if r, ok := e.(StreamingEngine); ok {
		cases = append(cases,
			nor160PublicEngineCase{"StreamNodes", func() (any, error) {
				return nodes(func(visit func(*Node) error) error { return r.StreamNodes(ctx, visit) })
			}, false},
			nor160PublicEngineCase{"StreamEdges", func() (any, error) {
				return edges(func(visit func(*Edge) error) error { return r.StreamEdges(ctx, visit) })
			}, false},
			nor160PublicEngineCase{"StreamNodeChunks", func() (any, error) {
				var out []*Node
				err := r.StreamNodeChunks(ctx, 1, func(chunk []*Node) error { out = append(out, chunk...); return nil })
				return out, err
			}, false})
	}
	if r, ok := e.(NodeWithoutEmbeddingsStreamer); ok {
		cases = append(cases, nor160PublicEngineCase{"StreamNodesWithoutEmbeddings", func() (any, error) {
			return nodes(func(visit func(*Node) error) error { return r.StreamNodesWithoutEmbeddings(ctx, visit) })
		}, false})
	}
	if r, ok := e.(EdgesBetweenMatcher); ok {
		cases = append(cases, nor160PublicEngineCase{"MatchEdgesBetween", func() (any, error) {
			return r.MatchEdgesBetween(f.a, f.b, "T", []string{"nested"}, func(*Edge) bool { return true })
		}, false})
	}
	if r, ok := e.(StoredEntityReader); ok {
		cases = append(cases,
			nor160PublicEngineCase{"GetStoredNode", func() (any, error) { return r.GetStoredNode(f.a) }, true},
			nor160PublicEngineCase{"GetStoredEdge", func() (any, error) { return r.GetStoredEdge(f.edge) }, false},
			nor160PublicEngineCase{"GetStoredOutgoingEdges", func() (any, error) { return r.GetStoredOutgoingEdges(f.a) }, false},
			nor160PublicEngineCase{"GetStoredIncomingEdges", func() (any, error) { return r.GetStoredIncomingEdges(f.b) }, false},
			nor160PublicEngineCase{"GetStoredNodesByLabel", func() (any, error) { return r.GetStoredNodesByLabel("Held") }, false},
			nor160PublicEngineCase{"StreamStoredNodesByLabelProjected", func() (any, error) {
				return nodes(func(visit func(*Node) error) error {
					return r.StreamStoredNodesByLabelProjected("Held", []string{"nested", "bytes"}, visit)
				})
			}, false},
			nor160PublicEngineCase{"StreamStoredEdgesByType", func() (any, error) {
				return edges(func(visit func(*Edge) error) error { return r.StreamStoredEdgesByType(ctx, "T", visit) })
			}, false})
	}
	if r, ok := e.(MVCCVisibilityEngine); ok {
		version := MVCCVersion{CommitTimestamp: time.Now().Add(time.Second), CommitSequence: ^uint64(0)}
		cases = append(cases,
			nor160PublicEngineCase{"GetNodeLatestVisible", func() (any, error) { return r.GetNodeLatestVisible(f.a) }, false},
			nor160PublicEngineCase{"GetEdgeLatestVisible", func() (any, error) { return r.GetEdgeLatestVisible(f.edge) }, false},
			nor160PublicEngineCase{"GetNodeVisibleAt", func() (any, error) { return r.GetNodeVisibleAt(f.a, version) }, false},
			nor160PublicEngineCase{"GetEdgeVisibleAt", func() (any, error) { return r.GetEdgeVisibleAt(f.edge, version) }, false})
	}
	if r, ok := e.(MVCCLatestEffectiveEngine); ok {
		cases = append(cases,
			nor160PublicEngineCase{"GetNodeLatestEffective", func() (any, error) { return r.GetNodeLatestEffective(f.a) }, false},
			nor160PublicEngineCase{"GetEdgeLatestEffective", func() (any, error) { return r.GetEdgeLatestEffective(f.edge) }, false})
	}
	if r, ok := e.(MVCCIndexedVisibilityEngine); ok {
		version := MVCCVersion{CommitTimestamp: time.Now().Add(time.Second), CommitSequence: ^uint64(0)}
		cases = append(cases,
			nor160PublicEngineCase{"GetNodesByLabelVisibleAt", func() (any, error) { return r.GetNodesByLabelVisibleAt("Held", version) }, false},
			nor160PublicEngineCase{"GetOutgoingEdgesVisibleAt", func() (any, error) { return r.GetOutgoingEdgesVisibleAt(f.a, version) }, false},
			nor160PublicEngineCase{"GetIncomingEdgesVisibleAt", func() (any, error) { return r.GetIncomingEdgesVisibleAt(f.b, version) }, false},
			nor160PublicEngineCase{"GetEdgesByTypeVisibleAt", func() (any, error) { return r.GetEdgesByTypeVisibleAt("T", version) }, false},
			nor160PublicEngineCase{"GetEdgesBetweenVisibleAt", func() (any, error) { return r.GetEdgesBetweenVisibleAt(f.a, f.b, version) }, false})
	}
	if r, ok := e.(interface {
		BatchGetNodesLatestVisible([]NodeID) (map[NodeID]*Node, error)
	}); ok {
		cases = append(cases, nor160PublicEngineCase{"BatchGetNodesLatestVisible", func() (any, error) { return r.BatchGetNodesLatestVisible([]NodeID{f.a, f.b}) }, false})
	}
	if r, ok := e.(interface{ IterateNodes(func(*Node) bool) error }); ok {
		cases = append(cases, nor160PublicEngineCase{"IterateNodes", func() (any, error) {
			var out []*Node
			err := r.IterateNodes(func(n *Node) bool { out = append(out, n); return true })
			return out, err
		}, false})
	}
	if r, ok := e.(MVCCEnumerationEngine); ok {
		cases = append(cases,
			nor160PublicEngineCase{"IterateLatestVisibleNodes", func() (any, error) { return nodes(r.IterateLatestVisibleNodes) }, false},
			nor160PublicEngineCase{"IterateLatestVisibleEdges", func() (any, error) { return edges(r.IterateLatestVisibleEdges) }, false})
	}
	if r, ok := e.(PrefixStreamingEngine); ok {
		cases = append(cases, nor160PublicEngineCase{"StreamNodesByPrefix", func() (any, error) {
			return nodes(func(visit func(*Node) error) error { return r.StreamNodesByPrefix(ctx, "", visit) })
		}, false})
	}
	if r, ok := e.(ProjectedPrefixNodeReader); ok {
		for _, mask := range []struct {
			name   string
			fields []string
		}{{"nil", nil}, {"selected", []string{"nested", "bytes"}}, {"empty", []string{}}} {
			cases = append(cases, nor160PublicEngineCase{"StreamNodesByPrefixProjected/" + mask.name, func() (any, error) {
				return nodes(func(visit func(*Node) error) error {
					return r.StreamNodesByPrefixProjected(ctx, "", mask.fields, visit)
				})
			}, false})
		}
	}
	if r, ok := e.(PrefixNodeWithoutEmbeddingsReader); ok {
		cases = append(cases, nor160PublicEngineCase{"StreamNodesByPrefixWithoutEmbeddings", func() (any, error) {
			return nodes(func(visit func(*Node) error) error { return r.StreamNodesByPrefixWithoutEmbeddings(ctx, "", visit) })
		}, false})
	}
	if r, ok := e.(ScopedLabelNodeReader); ok {
		version := MVCCVersion{CommitTimestamp: time.Now().Add(time.Second), CommitSequence: ^uint64(0)}
		cases = append(cases,
			nor160PublicEngineCase{"GetNodesByLabelInScope", func() (any, error) { return r.GetNodesByLabelInScope("test:", "Held") }, false},
			nor160PublicEngineCase{"GetFirstNodeByLabelInScope", func() (any, error) { return r.GetFirstNodeByLabelInScope("test:", "Held") }, false},
			nor160PublicEngineCase{"StreamNodesByLabelProjectedInScope", func() (any, error) {
				return nodes(func(visit func(*Node) error) error {
					return r.StreamNodesByLabelProjectedInScope("test:", "Held", []string{"nested", "bytes"}, visit)
				})
			}, false},
			nor160PublicEngineCase{"GetNodesByLabelVisibleAtInScope", func() (any, error) { return r.GetNodesByLabelVisibleAtInScope("test:", "Held", version) }, false})
	}
	if r, ok := e.(ScopedStoredEntityReader); ok {
		cases = append(cases,
			nor160PublicEngineCase{"GetStoredNodesByLabelInScope", func() (any, error) { return r.GetStoredNodesByLabelInScope("test:", "Held") }, false},
			nor160PublicEngineCase{"StreamStoredNodesByLabelProjectedInScope", func() (any, error) {
				return nodes(func(visit func(*Node) error) error {
					return r.StreamStoredNodesByLabelProjectedInScope("test:", "Held", []string{"nested", "bytes"}, visit)
				})
			}, false},
			nor160PublicEngineCase{"StreamStoredEdgesByTypeInScope", func() (any, error) {
				return edges(func(visit func(*Edge) error) error { return r.StreamStoredEdgesByTypeInScope(ctx, "test:", "T", visit) })
			}, false})
	}
	if r, ok := e.(EdgeTypeStreamer); ok {
		cases = append(cases, nor160PublicEngineCase{"StreamEdgesByType", func() (any, error) {
			return edges(func(visit func(*Edge) error) error { return r.StreamEdgesByType(ctx, "T", visit) })
		}, false})
	}
	if r, ok := e.(ScopedEdgeTypeStreamer); ok {
		cases = append(cases, nor160PublicEngineCase{"StreamEdgesByTypeInScope", func() (any, error) {
			return edges(func(visit func(*Edge) error) error { return r.StreamEdgesByTypeInScope(ctx, "test:", "T", visit) })
		}, false})
	}
	if r, ok := e.(interface{ GetStoredEdgesByType(string) ([]*Edge, error) }); ok {
		cases = append(cases, nor160PublicEngineCase{"GetStoredEdgesByType", func() (any, error) { return r.GetStoredEdgesByType("T") }, false})
	}
	if r, ok := e.(ReadSnapshotter); ok {
		var snapshot ReadSnapshotView
		open := func() (ReadSnapshotView, error) {
			if snapshot != nil {
				return snapshot, nil
			}
			var err error
			snapshot, err = r.OpenReadSnapshot(ctx)
			if err == nil {
				f.t.Cleanup(snapshot.Close)
			}
			return snapshot, err
		}
		cases = append(cases,
			nor160PublicEngineCase{"ReadSnapshot/IterateNodes", func() (any, error) {
				view, err := open()
				if err != nil {
					return nil, err
				}
				var out []*Node
				err = view.IterateNodes(func(n *Node) bool { out = append(out, n); return true })
				return out, err
			}, false},
			nor160PublicEngineCase{"ReadSnapshot/StreamEdges", func() (any, error) {
				view, err := open()
				if err != nil {
					return nil, err
				}
				return edges(func(visit func(*Edge) error) error { return view.StreamEdges(ctx, visit) })
			}, false})
	}
	return cases
}

func TestNOR160CallerOwnedPublicEngineReadAPIs(t *testing.T) {
	for _, kind := range []string{"badger", "memory", "namespaced", "wal", "traced", "composite"} {
		// Build the API list from actual public capabilities, then give each
		// registered API a fresh store so a red cannot contaminate later cases.
		t.Run(kind, func(t *testing.T) {
			probe := nor160PublicEngineFixtureFor(t, kind)
			for index, description := range nor160PublicEngineCases(probe) {
				t.Run(description.name, func(t *testing.T) {
					fixture := nor160PublicEngineFixtureFor(t, kind)
					cases := nor160PublicEngineCases(fixture)
					tc := cases[index]
					require.Equal(t, description.name, tc.name, "capability registration changed")
					for pass := 0; pass < 3; pass++ {
						result, err := tc.read()
						require.NoError(t, err)
						require.NotEmpty(t, result)
						if tc.full {
							node, ok := result.(*Node)
							require.True(t, ok)
							require.Equal(t, []float32{7, 8, 9}, node.NamedEmbeddings["original"])
							require.Equal(t, [][]float32{{1, 2, 3}, {4, 5, 6}}, node.ChunkEmbeddings)
						}
						want := nor160CapturePublicImages(result)
						readownership.Mutate(result)
						again, err := tc.read()
						require.NoError(t, err)
						require.Equal(t, want, nor160CapturePublicImages(again), "public %s/%s changed after caller mutation, pass %d", kind, tc.name, pass)
					}
				})
			}
		})
	}
}
