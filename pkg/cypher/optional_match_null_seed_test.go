package cypher

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// BUG: an OPTIONAL MATCH whose pattern reuses a variable that an earlier
// OPTIONAL MATCH bound to null (no match) enumerated the pattern as if the
// variable were unbound — a full node scan per null row (seconds per row on a
// server holding other databases). A pattern with a null-bound variable cannot
// match, so the row is null-extended without touching storage.
func TestBug_OptionalMatchNullSeedDoesNotMatch(t *testing.T) {
	ctx := context.Background()
	scans := &fullScanCountingEngine{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")}
	exec := NewStorageExecutor(scans)
	_, err := exec.Execute(ctx, `
		CREATE (q:Q {id: 1}),
		       (s1:S {id: 1})-[:SET]->(q), (s2:S {id: 2})-[:SET]->(q),
		       (qq1:QQ {id: 10})-[:SECTION]->(s1),
		       (:CM {id: 100})-[:ON]->(qq1),
		       (:CM {id: 200})-[:ON]->(:QQ {id: 99})`, nil)
	require.NoError(t, err)

	scans.allNodes.Store(0)
	res, err := exec.Execute(ctx, `
		MATCH (:Q {id: 1})<-[:SET]-(s:S)
		OPTIONAL MATCH (s)<-[:SECTION]-(qq:QQ)
		OPTIONAL MATCH (qq)<-[:ON]-(cm:CM)
		RETURN s.id AS s, qq.id AS qq, cm.id AS cm ORDER BY s`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{int64(1), int64(10), int64(100)},
		{int64(2), nil, nil}, // s2 has no question: qq and cm are null, cm 200 must not join
	}, res.Rows)
	require.Zero(t, scans.allNodes.Load(), "a null seed must not trigger a full node scan")

	res, err = exec.Execute(ctx, `
		MATCH (:Q {id: 1})<-[:SET]-(s:S)
		OPTIONAL MATCH (s)<-[:SECTION]-(qq:QQ)
		OPTIONAL MATCH (qq)<-[r:ON]-(cm:CM)
		RETURN count(s), count(qq), count(r), count(cm)`, nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{int64(2), int64(1), int64(1), int64(1)}, res.Rows[0])
}

// fullScanCountingEngine counts full node scans; it exposes no transaction
// support, so the executor reads through it directly.
type fullScanCountingEngine struct {
	storage.Engine
	allNodes atomic.Int64
}

func (e *fullScanCountingEngine) AllNodes() ([]*storage.Node, error) {
	e.allNodes.Add(1)
	return e.Engine.AllNodes()
}
