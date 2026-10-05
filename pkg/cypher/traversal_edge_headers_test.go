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

// Inside an explicit transaction the degree count lists relationship headers
// and gives the rows the full read gives, including a relationship the
// transaction created and without one it deleted.
func TestTransactionTraversalListsRelationshipHeaders(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := exec.Execute(ctx, "CREATE (a:P {id: 1}), (b:P {id: 2}), (c:P {id: 3}) CREATE (a)-[:K {w: 5}]->(b), (a)-[:K]->(c), (b)-[:K]->(c), (c)-[:L]->(a)", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (c:P {id: 3}), (b:P {id: 2}) CREATE (c)-[:K]->(b)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (:P {id: 1})-[r:K]->(:P {id: 3}) DELETE r", nil)
	require.NoError(t, err)
	out, err := exec.Execute(ctx, "MATCH (p:P)-[:K]->() RETURN p.id AS id, count(*) AS d ORDER BY id", nil)
	require.NoError(t, err)
	in, err := exec.Execute(ctx, "MATCH (p:P)<-[:K]-() RETURN p.id AS id, count(*) AS d ORDER BY id", nil)
	require.NoError(t, err)
	named, err := exec.Execute(ctx, "MATCH (p:P)-[r:K]->() RETURN p.id AS id, count(r) AS d ORDER BY id", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	want := [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(1)}, {int64(3), int64(1)}}
	require.Equal(t, want, out.Rows)
	require.Equal(t, named.Rows, out.Rows)
	require.Equal(t, [][]interface{}{{int64(2), int64(2)}, {int64(3), int64(1)}}, in.Rows)
}

// The transaction wrapper passes a declined or failed listing through, and
// strips its namespace only from answered headers.
func TestTransactionWrapperUserEdgeHeaders(t *testing.T) {
	w := &transactionStorageWrapper{namespace: "test", separator: ":"}
	edges, answered, err := w.userEdgeHeaders(nil, false, nil)
	require.NoError(t, err)
	require.False(t, answered)
	require.Nil(t, edges)
	_, answered, err = w.userEdgeHeaders(nil, true, storage.ErrNotFound)
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.True(t, answered)
	edges, _, _ = w.userEdgeHeaders([]*storage.Edge{{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "K"}}, true, nil)
	require.Equal(t, storage.EdgeID("e"), edges[0].ID)
	require.Equal(t, storage.NodeID("a"), edges[0].StartNode)
	root := &transactionStorageWrapper{}
	edges, _, _ = root.userEdgeHeaders([]*storage.Edge{{ID: "e"}}, true, nil)
	require.Equal(t, storage.EdgeID("e"), edges[0].ID)
}
