package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkTransactionDeleteNodeCascade deletes, one transaction each, a node
// that 32 outgoing and 32 incoming relationships connect, a quarter of them
// suppressed: the cascade a node delete reads and buffers.
func BenchmarkTransactionDeleteNodeCascade(b *testing.B) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	const degree = 32
	seed := func(i int) NodeID {
		hub := NodeID(fmt.Sprintf("test:hub-%d", i))
		nodes := []*Node{{ID: hub, Labels: []string{"Hub"}}}
		edges := make([]*Edge, 0, 2*degree)
		for j := 0; j < degree; j++ {
			out := NodeID(fmt.Sprintf("test:out-%d-%d", i, j))
			in := NodeID(fmt.Sprintf("test:in-%d-%d", i, j))
			nodes = append(nodes, &Node{ID: out, Labels: []string{"Peer"}}, &Node{ID: in, Labels: []string{"Peer"}})
			edges = append(edges,
				&Edge{ID: EdgeID(fmt.Sprintf("test:out-edge-%d-%d", i, j)), StartNode: hub, EndNode: out, Type: "OUT"},
				&Edge{ID: EdgeID(fmt.Sprintf("test:in-edge-%d-%d", i, j)), StartNode: in, EndNode: hub, Type: "IN"},
			)
		}
		require.NoError(b, engine.BulkCreateNodes(nodes))
		require.NoError(b, engine.BulkCreateEdges(edges))
		for j := 0; j < degree; j += 2 {
			require.NoError(b, engine.SetEdgeManuallySuppressed(EdgeID(fmt.Sprintf("test:out-edge-%d-%d", i, j)), true))
		}
		return hub
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		hub := seed(i)
		b.StartTimer()
		tx, err := engine.BeginTransaction()
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.SetNamespace("test"); err != nil {
			b.Fatal(err)
		}
		if err := tx.DeleteNode(hub); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
