package cypher

// gh640_merge_whole_pattern_test.go — regression tests for #640 (consolidated
// with #694): MERGE relationship-pattern semantics and the findMergeNode
// AllNodes() scan.
//
// Pinned against neo4j:5.26.30-community (issue table):
//   - When no match for the WHOLE relationship pattern exists, Cypher creates
//     the whole pattern: every endpoint not bound by an earlier clause is
//     created fresh, even if a node with the same label and properties exists.
//   - The bound forms (MATCH ... MERGE / MERGE ... MERGE) keep get-or-create
//     semantics on the existing node.
//   - A creating MERGE of a labelled node must not scan every node in the
//     database (the removed AllNodes() fallback cost 85 ms per row at 20k
//     nodes).

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newGh640Executor(t *testing.T) *StorageExecutor {
	t.Helper()
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	store := storage.NewNamespacedEngine(base, "gh640")
	return NewStorageExecutor(store)
}

// seedGh640T creates the issue's starting node (:T {id: 1}) with an extra
// marker property so tests can distinguish it from later duplicates.
func seedGh640T(t *testing.T, exec *StorageExecutor, ctx context.Context, seedID string) {
	t.Helper()
	result, err := exec.Execute(ctx, "CREATE (:T {id: 1, seed: $seed})", map[string]interface{}{"seed": seedID})
	require.NoError(t, err)
	require.Equal(t, 1, result.Stats.NodesCreated)
}

func gh640Count(t *testing.T, exec *StorageExecutor, ctx context.Context, query string) int64 {
	t.Helper()
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err, "query failed: %s", query)
	require.Len(t, result.Rows, 1, "expected one row: %s", query)
	require.Len(t, result.Rows[0], 1)
	value, ok := result.Rows[0][0].(int64)
	require.True(t, ok, "expected int64, got %T (%v)", result.Rows[0][0], result.Rows[0][0])
	return value
}

// gh640Label returns the labels of the node bound to $var by pattern, as
// returned values. It requires exactly one row.
func gh640Labels(t *testing.T, exec *StorageExecutor, ctx context.Context, query string) []interface{} {
	t.Helper()
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err, "query failed: %s", query)
	require.Len(t, result.Rows, 1)
	labels, ok := result.Rows[0][0].([]interface{})
	require.True(t, ok, "expected []interface{} labels, got %T (%v)", result.Rows[0][0], result.Rows[0][0])
	return labels
}

func TestGh640_CreateMergeSetReturnsMergedVariable(t *testing.T) {
	exec := newGh640Executor(t)
	ctx := context.Background()
	result, err := exec.Execute(ctx, "CREATE (a:T {id: 1}) MERGE (n:X {k: a.id}) SET n.extra = 2 RETURN n.k AS k, n.extra AS extra", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(2)}}, result.Rows)
}

func TestGh640_MatchForeachMergeReturnsOuterRow(t *testing.T) {
	exec := newGh640Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:T {id: 1})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (a:T) FOREACH (v IN [1, 2] | MERGE (:X {k: v})) RETURN a.id AS id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	require.EqualValues(t, 2, gh640Count(t, exec, ctx, "MATCH (n:X) RETURN count(n) AS c"))
}

func TestGh640_ForeachCompoundUpdates(t *testing.T) {
	for _, update := range []string{
		"MERGE (w:W {id: i}) ON CREATE SET w.x = i",
		"MERGE (w:W {id: i}) ON CREATE SET w.x = i SET w.y = 1",
		"CREATE (w:W {id: i}) SET w.y = 1",
		"MERGE (w:W {id: i}) SET w.y = 1",
	} {
		t.Run(update, func(t *testing.T) {
			exec := newGh640Executor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:T {id: 1})", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "MATCH (t:T) FOREACH (i IN [1, 2] | "+update+") RETURN t.id", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			stored, err := exec.Execute(ctx, "MATCH (w:W) RETURN w.id, w.x, w.y ORDER BY w.id", nil)
			require.NoError(t, err)
			require.Len(t, stored.Rows, 2)
			for index, row := range stored.Rows {
				require.Equal(t, int64(index+1), row[0])
				if strings.Contains(update, "w.x") {
					require.Equal(t, int64(index+1), row[1])
				}
				if strings.Contains(update, "w.y") {
					require.Equal(t, int64(1), row[2])
				}
			}
		})
	}
}

func TestGh640_CreateMergeOnCreateSet(t *testing.T) {
	exec := newGh640Executor(t)
	result, err := exec.Execute(context.Background(), "CREATE (a:B) MERGE (n:UK {k: 2}) ON CREATE SET n.k = 1 RETURN n.k", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}

func TestGh640_MergeWholePatternCreatesFreshEndpoints(t *testing.T) {
	// Issue statement 1: fresh database with (:T {id: 1}), then
	// MERGE (a:T {id: 1})-[:R]->(b:BC {id: 2}).
	// Neo4j: nodes_created = 2; the existing T stays unlinked and a new
	// (:T {id: 1})-[:R]->(:BC {id: 2}) pair is created.
	t.Run("statement_1", func(t *testing.T) {
		exec := newGh640Executor(t)
		ctx := context.Background()
		seedGh640T(t, exec, ctx, "seed-1")

		result, err := exec.Execute(ctx, "MERGE (a:T {id: 1})-[:R]->(b:BC {id: 2}) RETURN labels(b) AS l", nil)
		require.NoError(t, err)
		require.Equal(t, 2, result.Stats.NodesCreated)
		require.Equal(t, []interface{}{"BC"}, gh640Labels(t, exec, ctx, "MERGE (a:T {id: 1})-[:R]->(b:BC {id: 2}) RETURN labels(b) AS l"))

		// Graph shape: 2 T nodes, 1 BC node, 1 R edge; the seed T stays unlinked.
		require.EqualValues(t, 2, gh640Count(t, exec, ctx, "MATCH (n:T) RETURN count(n) AS c"))
		require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH (n:BC) RETURN count(n) AS c"))
		require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH ()-[r:R]->() RETURN count(r) AS c"))
		require.EqualValues(t, 0, gh640Count(t, exec, ctx, "MATCH (t:T {seed: 'seed-1'})-[:R]->() RETURN count(*) AS c"))

		// Idempotency: a second identical MERGE matches the created pair.
		result, err = exec.Execute(ctx, "MERGE (a:T {id: 1})-[:R]->(b:BC {id: 2}) RETURN labels(b) AS l", nil)
		require.NoError(t, err)
		require.Equal(t, 0, result.Stats.NodesCreated)
		require.EqualValues(t, 2, gh640Count(t, exec, ctx, "MATCH (n:T) RETURN count(n) AS c"))
		require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH ()-[r:R]->() RETURN count(r) AS c"))
	})

	// Issue statement 2: end node has a label but no properties.
	t.Run("statement_2", func(t *testing.T) {
		exec := newGh640Executor(t)
		ctx := context.Background()
		seedGh640T(t, exec, ctx, "seed-2")

		result, err := exec.Execute(ctx, "MERGE (a:T {id: 1})-[:R]->(b:BC) RETURN count(*) AS c", nil)
		require.NoError(t, err)
		require.EqualValues(t, 2, result.Stats.NodesCreated)
		require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH ()-[r:R]->() RETURN count(r) AS c"))
		require.EqualValues(t, 2, gh640Count(t, exec, ctx, "MATCH (n:T) RETURN count(n) AS c"))
		require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH (n:BC) RETURN count(n) AS c"))
		require.EqualValues(t, 0, gh640Count(t, exec, ctx, "MATCH (t:T {seed: 'seed-2'})-[:R]->() RETURN count(*) AS c"))
	})

	// Issue statement 3: self-referencing properties, distinct variables.
	// Neo4j creates a NEW pair linked by :R; the existing node keeps no loop.
	t.Run("statement_3", func(t *testing.T) {
		exec := newGh640Executor(t)
		ctx := context.Background()
		seedGh640T(t, exec, ctx, "seed-3")

		result, err := exec.Execute(ctx, "MERGE (a:T {id: 1})-[:R]->(b:T {id: 1}) RETURN count(*) AS c", nil)
		require.NoError(t, err)
		require.Equal(t, 2, result.Stats.NodesCreated)
		require.EqualValues(t, 3, gh640Count(t, exec, ctx, "MATCH (n:T) RETURN count(n) AS c"))
		require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH ()-[r:R]->() RETURN count(r) AS c"))
		require.EqualValues(t, 0, gh640Count(t, exec, ctx, "MATCH (t)-[r:R]->(t) RETURN count(r) AS c"))
		require.EqualValues(t, 0, gh640Count(t, exec, ctx, "MATCH (t:T {seed: 'seed-3'})-[:R]->() RETURN count(*) AS c"))
	})

	// Issue statement 4: bound start via MATCH keeps get-or-create semantics.
	t.Run("statement_4_bound_start", func(t *testing.T) {
		exec := newGh640Executor(t)
		ctx := context.Background()
		seedGh640T(t, exec, ctx, "seed-4")

		result, err := exec.Execute(ctx, "MATCH (a:T {id: 1}) MERGE (a)-[:R]->(b:BC {id: 2}) RETURN labels(b) AS l", nil)
		require.NoError(t, err)
		require.Equal(t, 1, result.Stats.NodesCreated)
		require.Equal(t, []interface{}{"BC"}, result.Rows[0][0])
		require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH (n:T) RETURN count(n) AS c"))
		require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH (n:BC) RETURN count(n) AS c"))
		require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH (t:T {seed: 'seed-4'})-[:R]->(:BC) RETURN count(*) AS c"))

		// Idempotency: the second run creates nothing.
		result, err = exec.Execute(ctx, "MATCH (a:T {id: 1}) MERGE (a)-[:R]->(b:BC {id: 2}) RETURN labels(b) AS l", nil)
		require.NoError(t, err)
		require.Equal(t, 0, result.Stats.NodesCreated)
	})

	// Issue statement 5: bound start via a preceding MERGE keeps get-or-create.
	t.Run("statement_5_merge_then_merge", func(t *testing.T) {
		exec := newGh640Executor(t)
		ctx := context.Background()
		seedGh640T(t, exec, ctx, "seed-5")

		result, err := exec.Execute(ctx, "MERGE (a:T {id: 1}) MERGE (a)-[:R]->(b:BC {id: 2}) RETURN labels(b) AS l", nil)
		require.NoError(t, err)
		require.Equal(t, 1, result.Stats.NodesCreated)
		require.Equal(t, []interface{}{"BC"}, result.Rows[0][0])
		require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH (n:T) RETURN count(n) AS c"))
		require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH (t:T {seed: 'seed-5'})-[:R]->(:BC) RETURN count(*) AS c"))

		// Idempotency: nothing new on the second run.
		result, err = exec.Execute(ctx, "MERGE (a:T {id: 1}) MERGE (a)-[:R]->(b:BC {id: 2}) RETURN labels(b) AS l", nil)
		require.NoError(t, err)
		require.Equal(t, 0, result.Stats.NodesCreated)
	})
}

func TestGh640_MergeSelfLoopSameVariable(t *testing.T) {
	// (a:SM {id: 10})-[:R]->(a) references the pattern's own variable: the end
	// is the start node, so the pattern is a self-loop on one get-or-created
	// node, matching on the second run.
	exec := newGh640Executor(t)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "MERGE (a:SM {id: 10})-[:R]->(a)", nil)
	require.NoError(t, err)
	require.Equal(t, 1, result.Stats.NodesCreated)
	require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH (n:SM) RETURN count(n) AS c"))
	require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH (t)-[r:R]->(t) RETURN count(r) AS c"))

	result, err = exec.Execute(ctx, "MERGE (a:SM {id: 10})-[:R]->(a)", nil)
	require.NoError(t, err)
	require.Equal(t, 0, result.Stats.NodesCreated)
	require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH (n:SM) RETURN count(n) AS c"))
	require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH ()-[r:R]->() RETURN count(r) AS c"))
}

// gh640CountingEngine counts AllNodes calls made by the executor under it.
type gh640CountingEngine struct {
	storage.Engine
	allNodesCalls atomic.Int64
}

func (e *gh640CountingEngine) AllNodes() ([]*storage.Node, error) {
	e.allNodesCalls.Add(1)
	return e.Engine.AllNodes()
}

func TestGh640_FindMergeNodeDoesNotScanAllNodes(t *testing.T) {
	// A labelled, property-carrying MERGE lookup must stay on label/schema
	// paths: the removed AllNodes() fallback made bulk creating-MERGE
	// ingestion quadratic (85 ms per row at 20k nodes, issue measurement).
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	counter := &gh640CountingEngine{Engine: storage.NewNamespacedEngine(base, "gh640-scan")}
	exec := NewStorageExecutor(counter)
	ctx := context.Background()

	// Seed 200 unrelated nodes so a global scan would be detectable by count.
	for i := 0; i < 200; i++ {
		_, err := exec.Execute(ctx, "CREATE (:Unrelated {v: $v})", map[string]interface{}{"v": int64(i)})
		require.NoError(t, err)
	}

	// First run: all 50 rows create. Second run: all 50 rows match.
	for run := 0; run < 2; run++ {
		result, err := exec.Execute(ctx, "UNWIND range(1, 50) AS x MERGE (:M {v: x})", nil)
		require.NoError(t, err)
		expectedCreated := 50
		if run == 1 {
			expectedCreated = 0
		}
		require.Equal(t, expectedCreated, result.Stats.NodesCreated)
	}
	require.EqualValues(t, 50, gh640Count(t, exec, ctx, "MATCH (n:M) RETURN count(n) AS c"))
	require.Zero(t, counter.allNodesCalls.Load(), "labelled MERGE lookups must not scan all nodes")
}

func TestGh640_WholePatternMatchesExistingSelfLoop(t *testing.T) {
	// Distinct endpoint variables may bind the same node: an existing
	// self-loop whose endpoints satisfy both node patterns is the
	// whole-pattern match, so MERGE creates nothing new.
	exec := newGh640Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (n:T {id: 1})-[:R]->(n)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MERGE (a:T {id: 1})-[:R]->(b:T {id: 1}) RETURN count(*) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, 0, result.Stats.NodesCreated)
	require.Equal(t, 0, result.Stats.RelationshipsCreated)
	require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH (n:T) RETURN count(n) AS c"))
	require.EqualValues(t, 1, gh640Count(t, exec, ctx, "MATCH (t)-[r:R]->(t) RETURN count(r) AS c"))
}
