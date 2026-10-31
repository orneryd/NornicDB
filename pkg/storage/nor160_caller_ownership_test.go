// Author: Codex, NOR-160 caller-owned read API contracts, 29 October 2026.
package storage

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

func nor160OwnershipProperties() map[string]any {
	return map[string]any{
		"key": "original", "nested": map[string]any{"map": map[string]any{"value": "original"}, "list": []any{map[string]any{"value": "original"}, []byte{1, 2}}},
		"bytes": []byte{1, 2}, "strings": []string{"a", "b"}, "ints": []int{1, 2}, "int32s": []int32{1, 2},
		"int64s": []int64{1, 2}, "float32s": []float32{1, 2}, "float64s": []float64{1, 2}, "bools": []bool{true, false},
	}
}

func nor160OwnershipFixture(t *testing.T, embeddings, oversized bool) (*MemoryEngine, *BadgerTransaction) {
	t.Helper()
	engine := newTestMemoryEngine(t)
	writer, err := engine.BeginTransaction()
	require.NoError(t, err)
	for _, id := range []NodeID{"test:a", "test:b"} {
		node := &Node{ID: id, Labels: []string{"Held"}, Properties: nor160OwnershipProperties()}
		if oversized {
			node.Properties["large"] = strings.Repeat("x", 8192)
		}
		if embeddings {
			node.ChunkEmbeddings = [][]float32{{1, 2, 3}, {4, 5, 6}}
			node.NamedEmbeddings = map[string][]float32{"original": {7, 8, 9}}
			node.EmbedMeta = map[string]any{"chunk_count": int64(2), "nested": map[string]any{"list": []any{"text", []byte{1, 2}}}}
		}
		_, err = writer.CreateNode(node)
		require.NoError(t, err)
	}
	require.NoError(t, writer.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: nor160OwnershipProperties()}))
	require.NoError(t, writer.Commit())
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	t.Cleanup(func() { _ = tx.Rollback() })
	return engine, tx
}

func TestNOR160CallerOwnedNodeReadAPIs(t *testing.T) {
	point := func(read func(*BadgerTransaction) (*Node, error)) func(*BadgerTransaction) ([]*Node, error) {
		return func(tx *BadgerTransaction) ([]*Node, error) { node, err := read(tx); return []*Node{node}, err }
	}
	stream := func(stored bool, properties []string) func(*BadgerTransaction) ([]*Node, error) {
		return func(tx *BadgerTransaction) ([]*Node, error) {
			var nodes []*Node
			visit := func(node *Node) error { nodes = append(nodes, node); return nil }
			var err error
			if stored {
				err = tx.StreamStoredNodesByLabelProjected("Held", properties, visit)
			} else {
				err = tx.StreamNodesByLabelProjected("Held", properties, visit)
			}
			return nodes, err
		}
	}
	options := func(opts StreamNodesOptions) func(*BadgerTransaction) ([]*Node, error) {
		return func(tx *BadgerTransaction) ([]*Node, error) {
			var nodes []*Node
			err := tx.StreamNodesWithOptions(context.Background(), opts, func(node *Node) error { nodes = append(nodes, node); return nil })
			return nodes, err
		}
	}
	cases := []struct {
		name string
		read func(*BadgerTransaction) ([]*Node, error)
		full bool
	}{
		{"GetNode", point(func(tx *BadgerTransaction) (*Node, error) { return tx.GetNode("test:a") }), true},
		{"GetQueryNode", point(func(tx *BadgerTransaction) (*Node, error) { return tx.GetQueryNode("test:a") }), false},
		{"GetStoredNode", point(func(tx *BadgerTransaction) (*Node, error) { return tx.GetStoredNode("test:a") }), true},
		{"GetNodeProjected/nil", point(func(tx *BadgerTransaction) (*Node, error) { return tx.GetNodeProjected("test:a", nil) }), true},
		{"GetNodeProjected/selected", point(func(tx *BadgerTransaction) (*Node, error) {
			return tx.GetNodeProjected("test:a", []string{"nested", "bytes"})
		}), false},
		{"GetNodeProjected/empty", point(func(tx *BadgerTransaction) (*Node, error) { return tx.GetNodeProjected("test:a", []string{}) }), false},
		{"GetNodesByLabel", func(tx *BadgerTransaction) ([]*Node, error) { return tx.GetNodesByLabel("Held") }, false},
		{"GetStoredNodesByLabel", func(tx *BadgerTransaction) ([]*Node, error) { return tx.GetStoredNodesByLabel("Held") }, false},
		{"GetFirstNodeByLabel", point(func(tx *BadgerTransaction) (*Node, error) { return tx.GetFirstNodeByLabel("Held") }), false},
		{"AllNodes", (*BadgerTransaction).AllNodes, false},
		{"GetAllNodes", func(tx *BadgerTransaction) ([]*Node, error) { return tx.GetAllNodes(), nil }, false},
		{"StreamNodesByLabelProjected/nil", stream(false, nil), false},
		{"StreamNodesByLabelProjected/selected", stream(false, []string{"nested", "bytes"}), false},
		{"StreamNodesByLabelProjected/empty", stream(false, []string{}), false},
		{"StreamStoredNodesByLabelProjected/nil", stream(true, nil), false},
		{"StreamStoredNodesByLabelProjected/selected", stream(true, []string{"nested", "bytes"}), false},
		{"StreamNodesWithOptions/full", options(StreamNodesOptions{Prefix: "test:", WithEmbeddings: true}), true},
		{"StreamNodesWithOptions/query-label", options(StreamNodesOptions{Label: "Held", QueryNodes: true}), false},
		{"StreamNodesWithOptions/projected", options(StreamNodesOptions{Projection: []string{"nested", "bytes"}, StripEmbeddings: true}), false},
		{"LookupNodesByEquality", func(tx *BadgerTransaction) ([]*Node, error) {
			nodes, answered, err := tx.LookupNodesByEquality("Held", map[string]any{"key": "original"})
			if err == nil && !answered {
				// Embedded/large populations legitimately decline retention.
				return tx.GetNodesByLabel("Held")
			}
			return nodes, err
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, shape := range []string{"properties", "embedded", "over-budget"} {
				t.Run(shape, func(t *testing.T) {
					_, tx := nor160OwnershipFixture(t, shape != "properties", shape == "over-budget")
					// First delivery, then both full-image-primed and repeated reads.
					for pass := 0; pass < 3; pass++ {
						if pass > 0 {
							_, err := tx.GetNode("test:a")
							require.NoError(t, err)
						}
						nodes, err := tc.read(tx)
						require.NoError(t, err)
						require.NotEmpty(t, nodes)
						if tc.name == "GetNodeProjected/selected" || tc.name == "GetNodeProjected/empty" {
							for _, node := range nodes {
								if tc.name == "GetNodeProjected/selected" {
									require.Len(t, node.Properties, 2)
									require.Contains(t, node.Properties, "nested")
									require.Contains(t, node.Properties, "bytes")
								} else {
									require.Empty(t, node.Properties)
								}
							}
						}
						if tc.full && shape != "properties" {
							for _, node := range nodes {
								require.Equal(t, [][]float32{{1, 2, 3}, {4, 5, 6}}, node.ChunkEmbeddings, "full-node completeness is independent of cache admission")
								require.Equal(t, []float32{7, 8, 9}, node.NamedEmbeddings["original"])
							}
						}
						want := readownership.Capture(nodes)
						readownership.Mutate(nodes)
						again, err := tc.read(tx)
						require.NoError(t, err)
						require.Equal(t, want, readownership.Capture(again), "caller mutation changed a later %s read, pass %d", tc.name, pass)
					}
				})
			}
		})
	}
}

func TestNOR160CallerOwnedEdgeReadAPIs(t *testing.T) {
	point := func(read func(*BadgerTransaction) (*Edge, error)) func(*BadgerTransaction) ([]*Edge, error) {
		return func(tx *BadgerTransaction) ([]*Edge, error) { edge, err := read(tx); return []*Edge{edge}, err }
	}
	stream := func(stored bool) func(*BadgerTransaction) ([]*Edge, error) {
		return func(tx *BadgerTransaction) ([]*Edge, error) {
			var edges []*Edge
			visit := func(edge *Edge) error { edges = append(edges, edge); return nil }
			var err error
			if stored {
				err = tx.StreamStoredEdgesByType(context.Background(), "T", visit)
			} else {
				err = tx.StreamEdgesByType(context.Background(), "T", visit)
			}
			return edges, err
		}
	}
	cases := []struct {
		name string
		read func(*BadgerTransaction) ([]*Edge, error)
	}{
		{"GetEdge", point(func(tx *BadgerTransaction) (*Edge, error) { return tx.GetEdge("test:e") })},
		{"GetStoredEdge", point(func(tx *BadgerTransaction) (*Edge, error) { return tx.GetStoredEdge("test:e") })},
		{"GetOutgoingEdges", func(tx *BadgerTransaction) ([]*Edge, error) {
			return tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: "test:a"})
		}},
		{"GetIncomingEdges", func(tx *BadgerTransaction) ([]*Edge, error) {
			return tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadIncoming, Node: "test:b"})
		}},
		{"GetStoredOutgoingEdges", func(tx *BadgerTransaction) ([]*Edge, error) { return tx.GetStoredOutgoingEdges("test:a") }},
		{"GetStoredIncomingEdges", func(tx *BadgerTransaction) ([]*Edge, error) { return tx.GetStoredIncomingEdges("test:b") }},
		{"IncidentEdges", func(tx *BadgerTransaction) ([]*Edge, error) { return tx.IncidentEdges("test:a") }},
		{"OutgoingEdgeHeaders", func(tx *BadgerTransaction) ([]*Edge, error) {
			edges, answered, err := tx.OutgoingEdgeHeaders("test:a")
			require.True(t, answered)
			return edges, err
		}},
		{"IncomingEdgeHeaders", func(tx *BadgerTransaction) ([]*Edge, error) {
			edges, answered, err := tx.IncomingEdgeHeaders("test:b")
			require.True(t, answered)
			return edges, err
		}},
		{"GetEdgesByType", func(tx *BadgerTransaction) ([]*Edge, error) { return tx.GetEdgesByType("T") }},
		{"AllEdges", (*BadgerTransaction).AllEdges},
		{"GetEdgesBetween", func(tx *BadgerTransaction) ([]*Edge, error) {
			return tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadBetween, Node: "test:a", Other: "test:b"})
		}},
		{"GetEdgeBetween", func(tx *BadgerTransaction) ([]*Edge, error) {
			return []*Edge{tx.GetEdgeBetween("test:a", "test:b", "T")}, nil
		}},
		{"MatchEdgesBetween", func(tx *BadgerTransaction) ([]*Edge, error) {
			return tx.MatchEdgesBetween("test:a", "test:b", "T", []string{"nested"}, func(*Edge) bool { return true })
		}},
		{"StreamEdgesByType", stream(false)},
		{"StreamStoredEdgesByType", stream(true)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, tx := nor160OwnershipFixture(t, false, false)
			for pass := 0; pass < 3; pass++ {
				// Prime the point image before header reads so their optional
				// properties are part of the reachable result, not omitted.
				if pass > 0 || strings.Contains(tc.name, "Headers") {
					_, err := tx.GetEdge("test:e")
					require.NoError(t, err)
				}
				edges, err := tc.read(tx)
				require.NoError(t, err)
				require.Len(t, edges, 1)
				want := readownership.Capture(edges)
				readownership.Mutate(edges)
				again, err := tc.read(tx)
				require.NoError(t, err)
				require.Equal(t, want, readownership.Capture(again), "caller mutation changed a later %s read, pass %d", tc.name, pass)
			}
		})
	}
}

func TestNOR160ReturnedReferencesCanMutateDuringReads(t *testing.T) {
	_, tx := nor160OwnershipFixture(t, true, false)
	node, err := tx.GetNode("test:a")
	require.NoError(t, err)
	wantNode := readownership.Capture(node)
	node, err = tx.GetQueryNode("test:a")
	require.NoError(t, err)
	edge, err := tx.GetEdge("test:e")
	require.NoError(t, err)
	wantEdge := readownership.Capture(edge)
	var worker sync.WaitGroup
	worker.Add(1)
	go func() {
		defer worker.Done()
		for i := 0; i < 100; i++ {
			readownership.Mutate(node)
			readownership.Mutate(edge)
		}
	}()
	defer worker.Wait()
	for i := 0; i < 100; i++ {
		freshNode, err := tx.GetNode("test:a")
		require.NoError(t, err)
		require.Equal(t, wantNode, readownership.Capture(freshNode))
		freshEdge, err := tx.GetEdge("test:e")
		require.NoError(t, err)
		require.Equal(t, wantEdge, readownership.Capture(freshEdge))
	}
	worker.Wait()
}

func TestNOR160CallerOwnedCopiesPreserveValueTypes(t *testing.T) {
	type namedList []map[string]any
	type scalar struct {
		Items namedList
		Bytes [2][]byte
	}
	for _, read := range []struct {
		name string
		copy func(map[string]any) any
	}{
		{"CopyNode", func(values map[string]any) any { return CopyNode(&Node{Properties: values, EmbedMeta: values}) }},
		{"CopyEdge", func(values map[string]any) any { return CopyEdge(&Edge{Properties: values}) }},
	} {
		t.Run(read.name, func(t *testing.T) {
			values := nor160OwnershipProperties()
			values["typed"] = &scalar{Items: namedList{{"nested": []int64{1, 2}}}, Bytes: [2][]byte{{3, 4}, {5, 6}}}
			values["nil-map"] = map[string]any(nil)
			values["nil-list"] = []any(nil)
			want := readownership.Capture(values)
			result := read.copy(values)
			readownership.Mutate(result)
			require.Equal(t, want, readownership.Capture(values))
			require.IsType(t, &scalar{}, values["typed"])
		})
	}
}

func TestNOR160CallerOwnedBufferedReadAPIs(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	bufferedWrite(t, engine, func(writer *BadgerTransaction) error {
		for _, id := range []NodeID{"test:a", "test:b"} {
			if _, err := writer.CreateNode(&Node{ID: id, Labels: []string{"Held"}, Properties: nor160OwnershipProperties(), NamedEmbeddings: map[string][]float32{"original": {1, 2, 3}}}); err != nil {
				return err
			}
		}
		return writer.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: nor160OwnershipProperties()})
	})
	require.Positive(t, engine.writeBehind.PendingOps(), "the test must exercise live buffered images")
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	cases := []struct {
		name string
		read func() (any, error)
	}{
		{"GetNode", func() (any, error) { return tx.GetNode("test:a") }},
		{"GetQueryNode", func() (any, error) { return tx.GetQueryNode("test:a") }},
		{"GetNodeProjected/nil", func() (any, error) { return tx.GetNodeProjected("test:a", nil) }},
		{"GetNodeProjected/selected", func() (any, error) { return tx.GetNodeProjected("test:a", []string{"nested"}) }},
		{"GetStoredNode", func() (any, error) { return tx.GetStoredNode("test:a") }},
		{"GetEdge", func() (any, error) { return tx.GetEdge("test:e") }},
		{"GetStoredEdge", func() (any, error) { return tx.GetStoredEdge("test:e") }},
		{"GetOutgoingEdges", func() (any, error) {
			return tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: "test:a"})
		}},
		{"GetIncomingEdges", func() (any, error) {
			return tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadIncoming, Node: "test:b"})
		}},
		{"GetEdgesByType", func() (any, error) { return tx.GetEdgesByType("T") }},
		{"AllEdges", func() (any, error) { return tx.AllEdges() }},
		{"AllNodes", func() (any, error) {
			nodes, err := tx.AllNodes()
			// The buffered overlay is a map; AllNodes promises a population,
			// not row order. Compare all values without imposing an order.
			sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
			return nodes, err
		}},
		{"GetEdgesBetween", func() (any, error) {
			return tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadBetween, Node: "test:a", Other: "test:b"})
		}},
		{"GetEdgeBetween", func() (any, error) { return tx.GetEdgeBetween("test:a", "test:b", "T"), nil }},
		{"MatchEdgesBetween", func() (any, error) {
			return tx.MatchEdgesBetween("test:a", "test:b", "T", []string{"nested"}, func(*Edge) bool { return true })
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.read()
			require.NoError(t, err)
			require.NotEmpty(t, result)
			want := readownership.Capture(result)
			readownership.Mutate(result)
			again, err := tc.read()
			require.NoError(t, err)
			require.Equal(t, want, readownership.Capture(again), "buffered state changed through %s without a write", tc.name)
		})
	}
}
