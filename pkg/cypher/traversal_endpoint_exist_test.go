package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// countingEndpointEngine counts the node reads and endpoint checks a
// traversal makes.
type countingEndpointEngine struct {
	*storage.NamespacedEngine
	reads, checks int
	hidden        storage.NodeID
}

func (e *countingEndpointEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	e.reads++
	return e.NamespacedEngine.GetNode(id)
}

func (e *countingEndpointEngine) RelationshipEndpointVisible(id storage.NodeID) (bool, bool) {
	e.checks++
	if id == e.hidden {
		return false, true
	}
	return e.NamespacedEngine.RelationshipEndpointVisible(id)
}

func TestTraversalEndpointsNeedOnlyExist(t *testing.T) {
	end := func(variable string, labels []string, properties map[string]interface{}) *TraversalMatch {
		return &TraversalMatch{EndNode: nodePatternInfo{variable: variable, labels: labels, properties: properties}}
	}
	require.True(t, traversalEndpointsNeedOnlyExist(end("", nil, nil)))
	require.False(t, traversalEndpointsNeedOnlyExist(end("b", nil, nil)))
	require.False(t, traversalEndpointsNeedOnlyExist(end("", []string{"P"}, nil)))
	require.False(t, traversalEndpointsNeedOnlyExist(end("", nil, map[string]interface{}{"k": 1})))
	path := end("", nil, nil)
	path.PathVariable = "p"
	require.False(t, traversalEndpointsNeedOnlyExist(path))
	chained := end("", nil, nil)
	chained.IsChained = true
	require.False(t, traversalEndpointsNeedOnlyExist(chained))
}

// A degree count over anonymous end nodes asks storage whether each end node
// is visible instead of reading it, and gives the same answers; a named end
// node is still read.
func TestDegreeCountChecksAnonymousEndpointsWithoutReading(t *testing.T) {
	ctx := context.Background()
	store := &countingEndpointEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")}
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "CREATE (a:P {id: 1}), (b:P {id: 2}), (c:P {id: 3}) CREATE (a)-[:K]->(b), (a)-[:K]->(c), (b)-[:K]->(c)", nil)
	require.NoError(t, err)

	store.reads, store.checks = 0, 0
	result, err := exec.Execute(ctx, "MATCH (p:P)-[:K]->() RETURN p.id AS id, count(*) AS d ORDER BY d DESC, id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(1)}}, result.Rows)
	require.Equal(t, 3, store.checks)

	// An end node storage reports as not visible doesn't count.
	cNode, err := exec.Execute(ctx, "MATCH (c:P {id: 3}) RETURN c", nil)
	require.NoError(t, err)
	store.hidden = cNode.Rows[0][0].(*storage.Node).ID
	result, err = exec.Execute(ctx, "MATCH (p:P)-[:K]->() RETURN p.id AS id, count(*) AS degree ORDER BY degree DESC, id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(1)}}, result.Rows)
	store.hidden = ""

	store.reads, store.checks = 0, 0
	result, err = exec.Execute(ctx, "MATCH (p:P)-[:K]->(q) RETURN p.id AS id, q.id AS q ORDER BY id, q", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(2)}, {int64(1), int64(3)}, {int64(2), int64(3)}}, result.Rows)
	require.Zero(t, store.checks)
}
