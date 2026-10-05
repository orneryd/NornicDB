package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// countingHeaderEngine counts the relationship listings a traversal makes.
type countingHeaderEngine struct {
	*storage.NamespacedEngine
	headers, full int
	decline       bool
}

func (e *countingHeaderEngine) OutgoingEdgeHeaders(id storage.NodeID) ([]*storage.Edge, bool, error) {
	e.headers++
	if e.decline {
		return nil, false, nil
	}
	return e.NamespacedEngine.OutgoingEdgeHeaders(id)
}

func (e *countingHeaderEngine) IncomingEdgeHeaders(id storage.NodeID) ([]*storage.Edge, bool, error) {
	e.headers++
	return e.NamespacedEngine.IncomingEdgeHeaders(id)
}

func (e *countingHeaderEngine) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	e.full++
	return e.NamespacedEngine.GetOutgoingEdges(id)
}

func (e *countingHeaderEngine) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	e.full++
	return e.NamespacedEngine.GetIncomingEdges(id)
}

func TestTraversalRelationshipsNeedOnlyHeaders(t *testing.T) {
	rel := func(variable string, properties map[string]interface{}) *TraversalMatch {
		return &TraversalMatch{Relationship: RelationshipPattern{Variable: variable, Properties: properties}}
	}
	require.True(t, traversalRelationshipsNeedOnlyHeaders(rel("", nil)))
	require.False(t, traversalRelationshipsNeedOnlyHeaders(rel("r", nil)))
	require.False(t, traversalRelationshipsNeedOnlyHeaders(rel("", map[string]interface{}{"w": 1})))
	path := rel("", nil)
	path.PathVariable = "p"
	require.False(t, traversalRelationshipsNeedOnlyHeaders(path))
	chained := rel("", nil)
	chained.IsChained = true
	require.False(t, traversalRelationshipsNeedOnlyHeaders(chained))
}

// A traversal over anonymous relationships lists them as headers and gives
// the same answers; a named relationship is read in full, as is one storage
// declines to list as headers.
func TestTraversalListsAnonymousRelationshipsAsHeaders(t *testing.T) {
	ctx := context.Background()
	store := &countingHeaderEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")}
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "CREATE (a:P {id: 1}), (b:P {id: 2}), (c:P {id: 3}) CREATE (a)-[:K {w: 5}]->(b), (a)-[:K]->(c), (b)-[:K]->(c), (c)-[:L]->(a)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (p:P)-[:K]->() RETURN p.id AS id, count(*) AS d ORDER BY d DESC, id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(1)}}, result.Rows)
	require.Positive(t, store.headers)

	result, err = exec.Execute(ctx, "MATCH (p:P)<-[:K]-() RETURN p.id AS id, count(*) AS d ORDER BY id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(1)}, {int64(3), int64(2)}}, result.Rows)

	store.headers, store.full = 0, 0
	result, err = exec.Execute(ctx, "MATCH (p:P)-[r:K]->() RETURN p.id AS id, sum(r.w) AS w ORDER BY id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(5)}, {int64(2), int64(0)}}, result.Rows)
	require.Zero(t, store.headers)

	store.decline = true
	result, err = exec.Execute(ctx, "MATCH (p:P)-[:K]->() RETURN p.id AS id, count(*) AS degree ORDER BY degree DESC, id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(1)}}, result.Rows)
	require.Positive(t, store.full)
}
