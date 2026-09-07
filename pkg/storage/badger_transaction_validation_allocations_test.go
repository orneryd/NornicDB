// SPDX-License-Identifier: MIT
package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const highDegreeValidationEdgesPerDirection = 512

func seedHighDegreeValidationGraph(tb testing.TB, engine *BadgerEngine, edgesPerDirection int) NodeID {
	tb.Helper()
	center := NodeID("test:validation-center")
	nodes := make([]*Node, 1, 1+2*edgesPerDirection)
	edges := make([]*Edge, 0, 2*edgesPerDirection)
	nodes[0] = &Node{ID: center, Labels: []string{"Node"}}
	for i := 0; i < edgesPerDirection; i++ {
		outNode := NodeID(fmt.Sprintf("test:validation-out-%d", i))
		inNode := NodeID(fmt.Sprintf("test:validation-in-%d", i))
		nodes = append(nodes,
			&Node{ID: outNode, Labels: []string{"Node"}},
			&Node{ID: inNode, Labels: []string{"Node"}},
		)
		edges = append(edges,
			&Edge{ID: EdgeID(fmt.Sprintf("test:validation-out-edge-%d", i)), StartNode: center, EndNode: outNode, Type: "LINKS"},
			&Edge{ID: EdgeID(fmt.Sprintf("test:validation-in-edge-%d", i)), StartNode: inNode, EndNode: center, Type: "LINKS"},
		)
	}
	require.NoError(tb, engine.BulkCreateNodes(nodes))
	require.NoError(tb, engine.BulkCreateEdges(edges))
	return center
}

func runHighDegreeDeleteValidation(tb testing.TB, engine *BadgerEngine, nodeID NodeID) {
	tb.Helper()
	tx, err := engine.BeginTransaction()
	require.NoError(tb, err)
	require.NoError(tb, tx.SetNamespace("test"))
	tx.operations = append(tx.operations, Operation{Type: OpDeleteNode, NodeID: nodeID})
	err = tx.validateSnapshotIsolationConflicts()
	_ = tx.Rollback()
	require.NoError(tb, err)
}

func measureHighDegreeDeleteValidation(tb testing.TB, engine *BadgerEngine, nodeID NodeID) testing.BenchmarkResult {
	tb.Helper()
	return testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runHighDegreeDeleteValidation(b, engine, nodeID)
		}
	})
}

func TestTransactionHighDegreeDeleteValidationMemoryBound(t *testing.T) {
	engine := createTestBadgerEngine(t)
	nodeID := seedHighDegreeValidationGraph(t, engine, highDegreeValidationEdgesPerDirection)
	result := measureHighDegreeDeleteValidation(t, engine, nodeID)

	require.LessOrEqual(t, result.AllocsPerOp(), int64(12_000),
		"validation must not create a view or repeat a head read per adjacent edge: %s", result.String())
	require.LessOrEqual(t, result.AllocedBytesPerOp(), int64(512<<10),
		"validation memory must remain bounded as node degree grows: %s", result.MemString())
}

func BenchmarkTransactionHighDegreeDeleteValidation(b *testing.B) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	nodeID := seedHighDegreeValidationGraph(b, engine, highDegreeValidationEdgesPerDirection)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runHighDegreeDeleteValidation(b, engine, nodeID)
	}
}
