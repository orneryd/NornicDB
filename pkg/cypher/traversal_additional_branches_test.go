package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type countErrEngine struct {
	storage.Engine
	edgeCountErr        error
	edgeCountByTypeErr  error
	edgeCountByLabelErr error
	getEdgesByTypeErr   error
}

func (e *countErrEngine) EdgeCount() (int64, error) {
	if e.edgeCountErr != nil {
		return 0, e.edgeCountErr
	}
	return e.Engine.EdgeCount()
}

func (e *countErrEngine) EdgeCountByType(edgeType string) (int64, error) {
	if e.edgeCountByTypeErr != nil {
		return 0, e.edgeCountByTypeErr
	}
	return e.Engine.EdgeCountByType(edgeType)
}

func (e *countErrEngine) EdgeCountByStartLabel(label, edgeType string) (int64, error) {
	if e.edgeCountByLabelErr != nil {
		return 0, e.edgeCountByLabelErr
	}
	return e.Engine.EdgeCountByStartLabel(label, edgeType)
}

func (e *countErrEngine) EdgeCountByEndLabel(label, edgeType string) (int64, error) {
	if e.edgeCountByLabelErr != nil {
		return 0, e.edgeCountByLabelErr
	}
	return e.Engine.EdgeCountByEndLabel(label, edgeType)
}

func (e *countErrEngine) GetEdgesByType(edgeType string) ([]*storage.Edge, error) {
	if e.getEdgesByTypeErr != nil {
		return nil, e.getEdgesByTypeErr
	}
	return e.Engine.GetEdgesByType(edgeType)
}

func TestTraversalStartPropertyScan_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "trav_scan_more")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Ada"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Lin", "age": int64(33)}})
	require.NoError(t, err)

	nodes, used, err := exec.tryCollectNodesFromStartPropertyScan(ctx, nodePatternInfo{}, "n.name = 'Ada'")
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	// Parameterized equality path must fail-open and let full WHERE evaluation handle it.
	nodes, used, err = exec.tryCollectNodesFromStartPropertyScan(ctx, nodePatternInfo{variable: "n", labels: []string{"Person"}}, "n.name = $name")
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	// No matches in pushdown path should fail-open (used=false) to preserve semantics.
	nodes, used, err = exec.tryCollectNodesFromStartPropertyScan(ctx, nodePatternInfo{variable: "n", labels: []string{"Person"}}, "n.name = 'Missing'")
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	// IS NOT NULL with no matches also fail-open.
	nodes, used, err = exec.tryCollectNodesFromStartPropertyScan(ctx, nodePatternInfo{variable: "n", labels: []string{"Person"}}, "n.unknown IS NOT NULL")
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	// Empty label list path should scan all nodes and still filter correctly.
	nodes, used, err = exec.tryCollectNodesFromStartPropertyScan(ctx, nodePatternInfo{variable: "n"}, "n.age IS NOT NULL")
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("p2"), nodes[0].ID)
}

func TestTryFastRelationshipCount_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "trav_count_more")

	for _, id := range []storage.NodeID{"a", "b", "c", "d"} {
		_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}

	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e1", Type: "R", StartNode: "a", EndNode: "b"}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e2", Type: "S", StartNode: "a", EndNode: "c"}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e3", Type: "R", StartNode: "c", EndNode: "d"}))

	exec := NewStorageExecutor(store)
	matches := &TraversalMatch{Relationship: RelationshipPattern{Variable: "r", Types: []string{"R", "S"}}}

	count, ok, err := exec.tryFastRelationshipCount(matches, returnItem{expr: "SUM(*)"})
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, count)

	count, ok, err = exec.tryFastRelationshipCount(matches, returnItem{expr: "COUNT(x)"})
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, count)

	count, ok, err = exec.tryFastRelationshipCount(matches, returnItem{expr: "COUNT(r)"})
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 3, count)

	count, ok, err = exec.tryFastRelationshipCount(&TraversalMatch{Relationship: RelationshipPattern{Variable: "r"}}, returnItem{expr: "COUNT(*)"})
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 3, count)

	edgeCountErrExec := NewStorageExecutor(&countErrEngine{Engine: store, edgeCountErr: errors.New("edge count failed")})
	_, ok, err = edgeCountErrExec.tryFastRelationshipCount(&TraversalMatch{Relationship: RelationshipPattern{Variable: "r"}}, returnItem{expr: "COUNT(*)"})
	require.True(t, ok)
	require.EqualError(t, err, "edge count failed")

	getEdgesErrExec := NewStorageExecutor(&countErrEngine{Engine: store, edgeCountByTypeErr: errors.New("type count failed")})
	_, ok, err = getEdgesErrExec.tryFastRelationshipCount(matches, returnItem{expr: "COUNT(r)"})
	require.True(t, ok)
	require.EqualError(t, err, "type count failed")
}
