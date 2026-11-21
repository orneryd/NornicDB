package cypher

// Close-out lane L3 (v1.5.2 verification campaign): S7-NOR-119 (Plane N15-3).
// Report: retiring relationships by setting a relationship property without an
// anchored node takes 5-60 s. The original statement, parameters and data size
// were never recorded, so this is a representative substitute (stated as such
// in the lane notes): 20,000 nodes, 100,000 relationships of one type, a
// retirement that matches 10% of them by a relationship property, no anchor.
// PASS: the unanchored retirement updates exactly the matching relationships
// in under 5 s (the report's lower bound) on each parser. Timings of an
// unanchored read of the same predicate and of a node retirement of the same
// size are logged for the ratio. Test-side only.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	closeoutL3RetireNodes  = 20000
	closeoutL3RetireEdges  = 100000
	closeoutL3RetireGroups = 10
	closeoutL3RetireBudget = 5 * time.Second
)

func TestCloseoutL3_S7_NOR119_UnanchoredRelationshipRetirement(t *testing.T) {
	ctx := context.Background()
	ns := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")

	buildStart := time.Now()
	nodes := make([]*storage.Node, 0, 5000)
	for i := 0; i < closeoutL3RetireNodes; i++ {
		nodes = append(nodes, &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("e%06d", i)),
			Labels:     []string{"Endpoint"},
			Properties: map[string]interface{}{"id": int64(i), "source": fmt.Sprintf("s%d", i%closeoutL3RetireGroups)},
		})
		if len(nodes) == cap(nodes) {
			require.NoError(t, ns.BulkCreateNodes(ctx, nodes))
			nodes = make([]*storage.Node, 0, 5000)
		}
	}
	if len(nodes) > 0 {
		require.NoError(t, ns.BulkCreateNodes(ctx, nodes))
	}
	edges := make([]*storage.Edge, 0, 5000)
	for i := 0; i < closeoutL3RetireEdges; i++ {
		start := i % closeoutL3RetireNodes
		end := (i*7 + 3) % closeoutL3RetireNodes
		edges = append(edges, &storage.Edge{
			ID:         storage.EdgeID(fmt.Sprintf("r%07d", i)),
			StartNode:  storage.NodeID(fmt.Sprintf("e%06d", start)),
			EndNode:    storage.NodeID(fmt.Sprintf("e%06d", end)),
			Type:       "AUTO",
			Properties: map[string]interface{}{"id": int64(i), "source": fmt.Sprintf("s%d", i%closeoutL3RetireGroups)},
		})
		if len(edges) == cap(edges) {
			require.NoError(t, ns.BulkCreateEdges(ctx, edges))
			edges = make([]*storage.Edge, 0, 5000)
		}
	}
	if len(edges) > 0 {
		require.NoError(t, ns.BulkCreateEdges(ctx, edges))
	}
	t.Logf("CLOSEOUT_L3 S7-NOR-119 built nodes=%d edges=%d in %v", closeoutL3RetireNodes, closeoutL3RetireEdges, time.Since(buildStart))

	want := int64(closeoutL3RetireEdges / closeoutL3RetireGroups)
	for index, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		index, parser := index, parser
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec := NewStorageExecutor(ns)
			src := fmt.Sprintf("s%d", index+1)
			params := map[string]interface{}{"src": src}

			readStart := time.Now()
			res, err := exec.Execute(ctx, "MATCH ()-[r:AUTO]->() WHERE r.source = $src RETURN count(r) AS c", params)
			readTime := time.Since(readStart)
			require.NoError(t, err)
			require.Equal(t, want, closeoutL3Int(res.Rows[0][0]), "control: the predicate matches 10%% of relationships")

			nodeStart := time.Now()
			_, err = exec.Execute(ctx, "MATCH (n:Endpoint) WHERE n.source = $src SET n.retired = true", params)
			nodeTime := time.Since(nodeStart)
			require.NoError(t, err)

			retireStart := time.Now()
			_, err = exec.Execute(ctx, "MATCH ()-[r:AUTO]->() WHERE r.source = $src SET r.retired = true", params)
			retireTime := time.Since(retireStart)
			require.NoError(t, err)

			check, err := exec.Execute(ctx, "MATCH ()-[r:AUTO]->() WHERE r.retired = true AND r.source = $src RETURN count(r) AS c", params)
			require.NoError(t, err)
			t.Logf("CLOSEOUT_L3 S7-NOR-119 parser=%s edges=%d matched=%d retireRel=%v readRel=%v retireNodes(%d)=%v ratioRetireToRead=%.2f retired=%v",
				parser, closeoutL3RetireEdges, want, retireTime, readTime, closeoutL3RetireNodes/closeoutL3RetireGroups, nodeTime,
				float64(retireTime)/float64(readTime), check.Rows[0][0])
			require.Equal(t, want, closeoutL3Int(check.Rows[0][0]), "every matching relationship is retired")
			require.Less(t, retireTime, closeoutL3RetireBudget, "unanchored retirement of %d of %d relationships", want, closeoutL3RetireEdges)
		})
	}
}

func closeoutL3Int(v interface{}) int64 {
	switch typed := v.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	}
	return -1
}

// The item's own test matrix (Plane N15-3: selectivity sparse/dense, no
// matches, all matches, existing property values, anchored equivalent), on the
// same unanchored shape. Stand-in: the item records no statement or size.
func TestCloseoutL3_S7_NOR119_ItemMatrix(t *testing.T) {
	ctx := context.Background()
	ns := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	nodes := make([]*storage.Node, 0, closeoutL3RetireNodes)
	for i := 0; i < closeoutL3RetireNodes; i++ {
		nodes = append(nodes, &storage.Node{ID: storage.NodeID(fmt.Sprintf("m%06d", i)), Labels: []string{"Endpoint"}, Properties: map[string]interface{}{"id": int64(i)}})
	}
	require.NoError(t, ns.BulkCreateNodes(ctx, nodes))
	edges := make([]*storage.Edge, 0, 5000)
	for i := 0; i < closeoutL3RetireEdges; i++ {
		edges = append(edges, &storage.Edge{
			ID:         storage.EdgeID(fmt.Sprintf("q%07d", i)),
			StartNode:  storage.NodeID(fmt.Sprintf("m%06d", i%closeoutL3RetireNodes)),
			EndNode:    storage.NodeID(fmt.Sprintf("m%06d", (i*7+3)%closeoutL3RetireNodes)),
			Type:       "AUTO",
			Properties: map[string]interface{}{"id": int64(i), "bucket": int64(i % 100)},
		})
		if len(edges) == cap(edges) {
			require.NoError(t, ns.BulkCreateEdges(ctx, edges))
			edges = make([]*storage.Edge, 0, 5000)
		}
	}
	if len(edges) > 0 {
		require.NoError(t, ns.BulkCreateEdges(ctx, edges))
	}
	cases := []struct {
		name, where string
		want        int64
	}{
		{"none", "r.bucket = 1000", 0},
		{"sparse_1pct", "r.bucket = 7", closeoutL3RetireEdges / 100},
		{"dense_50pct", "r.bucket < 50", closeoutL3RetireEdges / 2},
		{"all", "r.bucket >= 0", closeoutL3RetireEdges},
		{"existing_values_again", "r.bucket = 7", closeoutL3RetireEdges / 100},
	}
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		parser := parser
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec := NewStorageExecutor(ns)
			stamp := "retired_" + parser
			for _, c := range cases {
				start := time.Now()
				res, err := exec.Execute(ctx, "MATCH ()-[r:AUTO]->() WHERE "+c.where+" SET r."+stamp+" = true RETURN count(r) AS c", nil)
				elapsed := time.Since(start)
				require.NoError(t, err, c.name)
				got := closeoutL3Int(res.Rows[0][0])
				t.Logf("CLOSEOUT_L3 S7-NOR-119 matrix parser=%s case=%s edges=%d updated=%d elapsed=%v", parser, c.name, closeoutL3RetireEdges, got, elapsed)
				require.Equal(t, c.want, got, "%s: updated relationships", c.name)
				require.Less(t, elapsed, closeoutL3RetireBudget, "%s: unanchored retirement", c.name)
			}
			check, err := exec.Execute(ctx, "MATCH ()-[r:AUTO]->() WHERE r."+stamp+" = true RETURN count(r) AS c", nil)
			require.NoError(t, err)
			require.Equal(t, int64(closeoutL3RetireEdges), closeoutL3Int(check.Rows[0][0]), "stored values after the all case")
			anchorStart := time.Now()
			res, err := exec.Execute(ctx, "MATCH (n:Endpoint {id: 5})-[r:AUTO]->() SET r.anchored_"+parser+" = true RETURN count(r) AS c", nil)
			require.NoError(t, err)
			t.Logf("CLOSEOUT_L3 S7-NOR-119 matrix parser=%s case=anchored_equivalent updated=%v elapsed=%v", parser, res.Rows[0][0], time.Since(anchorStart))
		})
	}
}
