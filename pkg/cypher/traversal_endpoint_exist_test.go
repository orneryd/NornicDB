package cypher

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// countingEndpointEngine counts the node reads and endpoint checks a
// traversal makes.
type countingEndpointEngine struct {
	*storage.NamespacedEngine
	// Atomic: a parallel traversal reads endpoints from several goroutines.
	reads, checks atomic.Int64
	projected     atomic.Int64
	hidden        storage.NodeID
}

// reset zeroes the counters between statements.
func (e *countingEndpointEngine) reset() {
	e.reads.Store(0)
	e.checks.Store(0)
	e.projected.Store(0)
}

func (e *countingEndpointEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	e.reads.Add(1)
	return e.NamespacedEngine.GetNode(id)
}

func (e *countingEndpointEngine) GetNodeProjected(id storage.NodeID, properties []string) (*storage.Node, error) {
	e.reads.Add(1)
	e.projected.Add(1)
	return e.NamespacedEngine.GetNodeProjected(id, properties)
}

func (e *countingEndpointEngine) RelationshipEndpointVisible(id storage.NodeID) (bool, bool) {
	e.checks.Add(1)
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

func TestTraversalEndpointsNeedOnlyLabels(t *testing.T) {
	end := func(variable string, labels []string, properties map[string]interface{}) *TraversalMatch {
		return &TraversalMatch{EndNode: nodePatternInfo{variable: variable, labels: labels, properties: properties}}
	}
	require.True(t, traversalEndpointsNeedOnlyLabels(end("", []string{"P"}, nil)))
	require.False(t, traversalEndpointsNeedOnlyLabels(end("", nil, nil))) // no labels → exist-only
	require.False(t, traversalEndpointsNeedOnlyLabels(end("", []string{"P"}, map[string]interface{}{"k": 1})))
	// A named endpoint is labels-only only when the planner proved its
	// properties are never read.
	require.False(t, traversalEndpointsNeedOnlyLabels(end("b", []string{"P"}, nil)))
	namedUnused := end("b", []string{"P"}, nil)
	namedUnused.UnusedPropertyVariables = map[string]bool{"b": true}
	require.True(t, traversalEndpointsNeedOnlyLabels(namedUnused))
	namedRead := end("b", []string{"P"}, nil)
	namedRead.UnusedPropertyVariables = map[string]bool{} // b's properties are read
	require.False(t, traversalEndpointsNeedOnlyLabels(namedRead))
	path := end("", []string{"P"}, nil)
	path.PathVariable = "p"
	require.False(t, traversalEndpointsNeedOnlyLabels(path))
	chained := end("", []string{"P"}, nil)
	chained.IsChained = true
	require.False(t, traversalEndpointsNeedOnlyLabels(chained))
}

// A count over a named labelled endpoint whose properties the planner proved
// unread loads the endpoint by ID + labels (a projected read), not a full
// node, and returns the same rows (#1110 step 1).
func TestNamedLabelledEndpointCountReadsOnlyLabels(t *testing.T) {
	ctx := context.Background()
	store := &countingEndpointEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")}
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, `
		CREATE (a:P {id: 1}),
		       (b:Q {id: 2, text: 'y'}),
		       (c:Q {id: 3, text: 'z'})
		CREATE (a)-[:K]->(b), (a)-[:K]->(c)
	`, nil)
	require.NoError(t, err)

	store.reset()
	result, err := exec.Execute(ctx, "MATCH (a:P)-[:K]->(l:Q) WHERE a.id >= 1 RETURN count(l) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	require.Greater(t, store.projected.Load(), int64(0), "named labelled endpoints must use the projected labels-only read")
	require.Equal(t, store.reads.Load(), store.projected.Load(), "no full node read for a count-only named endpoint")
}

// A named labelled endpoint whose property is read still loads the full node.
func TestNamedLabelledEndpointPropertyReadStillLoadsFullNode(t *testing.T) {
	ctx := context.Background()
	store := &countingEndpointEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")}
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, `
		CREATE (a:P {id: 1}),
		       (b:Q {id: 2, text: 'y'})
		CREATE (a)-[:K]->(b)
	`, nil)
	require.NoError(t, err)

	store.reset()
	result, err := exec.Execute(ctx, "MATCH (a:P)-[:K]->(l:Q) RETURN l.text AS t", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"y"}}, result.Rows)
	require.Zero(t, store.projected.Load(), "a property read must not use the labels-only read")
}

// A count over a labelled anonymous endpoint reads only the endpoint's labels
// (a projected read), not the whole node, and returns the same rows.
func TestLabelledEndpointCountReadsOnlyLabels(t *testing.T) {
	ctx := context.Background()
	store := &countingEndpointEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")}
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, `
		CREATE (a:P {id: 1, text: 'x'}),
		       (b:P {id: 2, text: 'y'}),
		       (c:Q {id: 3, text: 'z'})
		CREATE (a)-[:K]->(b), (a)-[:K]->(c)
	`, nil)
	require.NoError(t, err)

	store.reset()
	result, err := exec.Execute(ctx, "MATCH (p:P)-[:K]->(:Q) RETURN p.id AS id, count(*) AS d ORDER BY d DESC, id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(1)}}, result.Rows)
	require.EqualValues(t, 2, store.reads.Load(), "one labels-only read for the anonymous labelled endpoint")
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

	store.reset()
	result, err := exec.Execute(ctx, "MATCH (p:P)-[:K]->() RETURN p.id AS id, count(*) AS d ORDER BY d DESC, id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(1)}}, result.Rows)
	require.EqualValues(t, 3, store.checks.Load())

	// An end node storage reports as not visible doesn't count.
	cNode, err := exec.Execute(ctx, "MATCH (c:P {id: 3}) RETURN c", nil)
	require.NoError(t, err)
	store.hidden = cNode.Rows[0][0].(*storage.Node).ID
	result, err = exec.Execute(ctx, "MATCH (p:P)-[:K]->() RETURN p.id AS id, count(*) AS degree ORDER BY degree DESC, id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(1)}}, result.Rows)
	store.hidden = ""

	store.reset()
	result, err = exec.Execute(ctx, "MATCH (p:P)-[:K]->(q) RETURN p.id AS id, q.id AS q ORDER BY id, q", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(2)}, {int64(1), int64(3)}, {int64(2), int64(3)}}, result.Rows)
	require.Zero(t, store.checks.Load())
}

// Inside an explicit transaction the degree count gives the same rows as the
// full read, counting an end node the transaction created and dropping one it
// deleted.
func TestDegreeCountInTransactionChecksEndpoints(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := exec.Execute(ctx, "CREATE (a:P {id: 1}), (b:P {id: 2}), (c:P {id: 3}) CREATE (a)-[:K]->(b), (a)-[:K]->(c), (b)-[:K]->(c)", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (a:P {id: 1}) CREATE (a)-[:K]->(:P {id: 4})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (c:P {id: 3}) DETACH DELETE c", nil)
	require.NoError(t, err)
	degrees, err := exec.Execute(ctx, "MATCH (p:P)-[:K]->() RETURN p.id AS id, count(*) AS d ORDER BY d DESC, id", nil)
	require.NoError(t, err)
	named, err := exec.Execute(ctx, "MATCH (p:P)-[:K]->(q) RETURN p.id AS id, count(q) AS d ORDER BY d DESC, id", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	require.Equal(t, [][]interface{}{{int64(1), int64(2)}}, degrees.Rows)
	require.Equal(t, named.Rows, degrees.Rows)
}

// The parallel traversal (one worker per slice of start nodes) gives the
// sequential rows for the endpoint-check, labels-only and full-read shapes;
// under -race it also checks the workers share no unsynchronized state.
func TestParallelTraversalMatchesSequentialForEndpointShapes(t *testing.T) {
	keepParallelConfig(t)
	ctx := context.Background()
	store := &countingEndpointEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")}
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "UNWIND range(1, 40) AS i CREATE (a:P {id: i})-[:K]->(:Q {id: i}), (a)-[:K]->(:P {id: 100 + i})", nil)
	require.NoError(t, err)
	queries := []string{
		"MATCH (p:P)-[:K]->() RETURN p.id AS id, count(*) AS d ORDER BY d DESC, id",
		"MATCH (p:P)-[:K]->(:Q) RETURN p.id AS id, count(*) AS d ORDER BY d DESC, id",
		"MATCH (p:P)-[:K]->(q) RETURN p.id AS id, q.id AS q ORDER BY id, q",
	}
	sequential := make([][][]interface{}, len(queries))
	SetParallelConfig(ParallelConfig{Enabled: false})
	for i, query := range queries {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		sequential[i] = result.Rows
	}
	SetParallelConfig(ParallelConfig{Enabled: true, MaxWorkers: 8, MinBatchSize: 1})
	// A fresh executor, so no cached result answers in place of the traversal.
	parallel := NewStorageExecutor(store)
	for i, query := range queries {
		store.reset()
		result, err := parallel.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, sequential[i], result.Rows, query)
		if i == 0 {
			require.EqualValues(t, 80, store.checks.Load(), "every anonymous end node checked once, from the workers")
		}
	}
}
