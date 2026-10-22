package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// sweepCancellationContext cancels at its cancelAt-th observation (Err), or
// only counts them when cancelAt is zero.
type sweepCancellationContext struct {
	context.Context
	mu       sync.Mutex
	cancelAt int
	calls    int
	done     chan struct{}
	err      error
}

func newSweepCancellationContext(cancelAt int) *sweepCancellationContext {
	return &sweepCancellationContext{Context: context.Background(), cancelAt: cancelAt, done: make(chan struct{})}
}

func (c *sweepCancellationContext) Done() <-chan struct{} { return c.done }

func (c *sweepCancellationContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.calls++
	if c.cancelAt > 0 && c.calls >= c.cancelAt {
		c.err = context.Canceled
		close(c.done)
	}
	return c.err
}

func (c *sweepCancellationContext) canceled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err != nil
}

// sweepCancellationFixture's engine closes with the returned func, so a sweep
// keeps one engine alive at a time.
// sweepCancellationFixture's close returns what the store holds once the
// statement's transaction is over: a run commits all of its writes or none.
func sweepCancellationFixture(t *testing.T, explicit bool) (*StorageExecutor, func() string) {
	t.Helper()
	engine := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(engine, "sweep")
	executor := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	_, err := executor.Execute(context.Background(), `
		UNWIND range(1, 3) AS i
		CREATE (p:P {id: i, name: 'p' + toString(i)})-[:R {w: i}]->(q:Q {id: i})
		CREATE (p)-[:N]->(q)`, nil)
	require.NoError(t, err)
	// An index on :P(id) routes the seeks below through their index paths.
	_, err = executor.Execute(context.Background(), "CREATE INDEX sweep_p_id FOR (p:P) ON (p.id)", nil)
	require.NoError(t, err)
	if explicit {
		_, err = executor.Execute(context.Background(), "BEGIN", nil)
		require.NoError(t, err)
	}
	return executor, func() string {
		if explicit {
			_, _ = executor.Execute(context.Background(), "ROLLBACK", nil)
		}
		stored := sweepStoredGraph(t, store)
		require.NoError(t, engine.Close())
		return stored
	}
}

func sweepStoredGraph(t *testing.T, store storage.Engine) string {
	t.Helper()
	nodes, err := store.AllNodes()
	require.NoError(t, err)
	edges, err := store.AllEdges()
	require.NoError(t, err)
	// IDs are generated per fixture: entities are told apart by content.
	described := make(map[storage.NodeID]string, len(nodes))
	entries := make([]string, 0, len(nodes)+len(edges))
	for _, node := range nodes {
		described[node.ID] = fmt.Sprint(node.Labels, node.Properties)
		entries = append(entries, "node "+described[node.ID])
	}
	for _, edge := range edges {
		entries = append(entries, fmt.Sprint("edge ", edge.Type, edge.Properties, described[edge.StartNode], described[edge.EndNode]))
	}
	sort.Strings(entries)
	return strings.Join(entries, "\n")
}

// sweepCancellationPoints is every observation, or 64 spread over them
// including the first and the last: a parser can observe its context
// hundreds of times per statement.
func sweepCancellationPoints(observations int) []int {
	const limit = 64
	points := make([]int, 0, min(observations, limit))
	for i := 0; i < min(observations, limit); i++ {
		at := 1 + i*(observations-1)/max(min(observations, limit)-1, 1)
		if len(points) == 0 || points[len(points)-1] != at {
			points = append(points, at)
		}
	}
	return points
}

// Every observation of a statement's context is a point it can be canceled
// at. For materialized statements across the pipeline's operators, adapters,
// mutations and result completion, each observation in turn cancels: Execute
// then fails with the cancellation itself, never with success or another
// error a truncated population could cause. Only an auto-commit write
// canceled after its commit succeeds (TestStatementCommitCancellationBoundary).
func TestMaterializedPopulationCancellationSweep(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "custom.sweep_rows", Mode: ProcedureModeRead, Returns: []ProcedureColumn{{Name: "x", Type: "INTEGER"}}}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{Columns: []string{"x"}, Rows: [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}}, nil
	}))
	parameters := map[string]interface{}{"n": int64(3), "ids": []interface{}{int64(1), int64(2), int64(3)}, "one": int64(1), "nested": []interface{}{[]interface{}{int64(1), int64(2)}, map[string]interface{}{"a": int64(1), "b": int64(2)}, int64(3)}}
	queries := []string{
		"UNWIND range(1,$n) AS x WITH x ORDER BY x RETURN x",
		"UNWIND range(1,$n) AS x WITH x, count(*) AS c RETURN x, c ORDER BY x",
		"UNWIND range(1,$n) AS x WITH DISTINCT x % 2 AS y WHERE y >= 0 RETURN y",
		"UNWIND range(1,$n) AS x RETURN DISTINCT x % 2 AS y",
		"UNWIND range(1,$n) AS x RETURN collect(x) AS c, sum(x) AS s, percentileCont(x, 0.5) AS p",
		"UNWIND range(1,$n) AS x WITH * ORDER BY x RETURN *",
		"UNWIND range(1,$n) AS x WITH x LIMIT 2 WHERE x > 0 RETURN x",
		"UNWIND range(1,$n) AS x RETURN x UNION RETURN 1 AS x",
		"RETURN 1 AS a, 2 AS b UNION ALL UNWIND range(1,$n) AS x RETURN x AS b, x AS a",
		"UNWIND range(1,$n) AS `a b` WITH `a b` ORDER BY `a b` RETURN *",
		"RETURN $nested AS payload",
		"MATCH (p:P) RETURN p ORDER BY p.id",
		"MATCH (p:P) RETURN p {.*} AS m ORDER BY m.id",
		"MATCH (p:P)-[r:R]->(q:Q) RETURN p.id, q.id, r.w ORDER BY p.id",
		"MATCH (p:P) OPTIONAL MATCH (p)-[:R]->(q:Q) RETURN p.id, collect(q.id)",
		"UNWIND range(1,$n) AS x OPTIONAL MATCH (m:Absent) RETURN x",
		"MATCH (p:P) WITH p MATCH (p)-[:R]->(q) RETURN p.id, q.id",
		"MATCH (p:P), (q:Q) RETURN p.id, q.id ORDER BY p.id, q.id",
		"MATCH (p:P)-[r:R*1..2]-(q) RETURN p.id, size(r)",
		"MATCH path = (a:P)-[:R]->(b:Q) RETURN path",
		"MATCH (a:P {id: 1}), (b:Q {id: 3}) MATCH path = shortestPath((a)-[*..4]-(b)) RETURN length(path)",
		"MATCH (p:P) RETURN [(p)-[:R]->(q) | q.id] AS ids",
		"MATCH (p:P) RETURN p.id, EXISTS { MATCH (p)-[:R]->() } AS has, COUNT { MATCH (p)-[:R]->() } AS c, COLLECT { MATCH (p)-[:R]->(q) RETURN q.id } AS qs",
		"MATCH (p:P) WITH collect(p) AS ps UNWIND ps AS p RETURN p.id",
		"MATCH (p:P) CALL { WITH p MATCH (p)-[:R]->(q) RETURN q } RETURN p.id, q.id",
		"UNWIND range(1,$n) AS x CALL { RETURN 1 AS y UNION RETURN 2 AS y } RETURN x, y",
		"UNWIND [0, 1] AS marker CALL { UNWIND range(1,$n) AS x RETURN x } RETURN marker, x",
		"CALL custom.sweep_rows() YIELD x WHERE x > 0 RETURN x",
		"CALL custom.sweep_rows() YIELD x AS y RETURN *",
		"UNWIND [0, 1] AS marker CALL custom.sweep_rows() YIELD x RETURN marker, x",
		"CALL db.labels() YIELD label RETURN label ORDER BY label",
		"UNWIND range(1,$n) AS x MATCH (p:P {id: x}) RETURN p.id",
		"UNWIND range(1,$n) AS x MATCH (p:P {id: x})-[:R]->(q) RETURN q.id",
		"MATCH (p:P) WITH p ORDER BY p.id MATCH (p)-[:R]->(q) RETURN q.id",
		"MATCH (p:P) MATCH (q:Q) WHERE q.id = p.id RETURN p.id, q.id",
		"UNWIND range(1,$n) AS x CALL { WITH x MATCH (p:P {id: x}) RETURN p } RETURN p.id",
		"CALL custom.sweep_rows() YIELD x MATCH (p:P {id: x}) RETURN p.id",
		"CALL custom.sweep_rows() YIELD x CALL { WITH x RETURN x * 2 AS y } RETURN x, y",
		"CALL custom.sweep_rows() YIELD x RETURN x ORDER BY x DESC LIMIT 2",
		"CALL db.labels() YIELD label WITH label MATCH (n) WHERE label IN labels(n) RETURN label, count(n)",
		"CALL apoc.cypher.run('UNWIND range(1, 3) AS x RETURN x', {}) YIELD value RETURN value",
		"CALL apoc.export.csv.query('UNWIND range(1, 3) AS x RETURN x', null, {stream: true}) YIELD data RETURN data",
		"MATCH (p:P) WHERE EXISTS { MATCH (p)-[:R]->(q:Q) WHERE q.id = p.id } RETURN p.id",
		"UNWIND range(1,$n) AS x CREATE (:W {x: x}) RETURN x",
		"MATCH (p:P) SET p.seen = true RETURN p.id",
		"UNWIND range(1,$n) AS x MERGE (m:M {x: x}) RETURN m.x",
		"UNWIND range(1,$n) AS x MATCH (d:Q {id: x}) DETACH DELETE d RETURN x",
		"MATCH (p:P)-[r:N]->() DELETE r RETURN count(*)",
		"MATCH (p:P) FOREACH (i IN [1, 2] | CREATE (:F {i: i}))",
		"MATCH (p:P) REMOVE p.name RETURN p.id",
		"UNWIND range(1,$n) AS x CALL { CREATE (:T) } IN TRANSACTIONS OF 1 ROWS",
		"MATCH (p:P), (q:Q) WHERE p.id = q.id RETURN p.id, q.id",
		"MATCH (p:P) RETURN p.id LIMIT 10",
		"MATCH (p:P) WHERE p.id IN [1, 2, 3] RETURN p.id",
		"MATCH (q:Q)<-[:R]-(p:P) RETURN q.id, count(p)",
		"MATCH (p:P)-[:R]->(q:Q) WHERE q.id > 0 RETURN p.id ORDER BY p.id LIMIT 2",
		"PROFILE MATCH (p:P) RETURN p.id",
		"PROFILE UNWIND range(1,$n) AS x CREATE (:PW {x: x}) RETURN x",
		"MERGE (m:P {id: 99}) RETURN m.id",
		"UNWIND range(1,$n) AS x MATCH (p:P), (q:Q) WHERE p.id = x AND q.id = x MERGE (p)-[:M]->(q) RETURN x",
		"BEGIN UNWIND range(1, 3) AS x CREATE (:S {x: x}) COMMIT",
		"MATCH (p:P) WHERE p.id IN $ids RETURN p.id",
		"MATCH (p:P) WHERE p.id IN $ids OR p.id = $one RETURN p.id",
		"MATCH (p:P) WHERE p.id = 1 OR p.id = 2 RETURN p.id",
		"MATCH (p:P) WHERE p.id >= 1 RETURN p.id",
		"MATCH (p:P) WHERE p.id IS NOT NULL RETURN p.id",
		"MATCH (p:P {id: 2}) RETURN p.name",
		"MATCH (q:Q)<-[:R]-(p:P) RETURN q.id, count(p), sum(p.id), avg(p.id), collect(p.id)",
		"MATCH (p:P)-[r:R]->(q:Q) RETURN count(r), sum(r.w), min(r.w), max(r.w), avg(r.w), collect(r.w), collect(DISTINCT r.w)",
		"MATCH (p:P)-[:R]->(q:Q)-[:N]-(x) RETURN count(*)",
		"MATCH (p:P)-[:R]->(q:Q) WITH p MATCH (p)-[:N]->(z) RETURN p.id, z.id",
		"MATCH (p:P) OPTIONAL MATCH (p)-[:R]->(q:Q) WHERE q.id > 1 RETURN p.id, q.id",
		"MATCH (p:P) WITH p ORDER BY p.id LIMIT 2 MATCH (p)-[:R]->(q) RETURN q.id ORDER BY q.id",
		"MATCH (p:P)-[:R]->(q:Q) RETURN p.id ORDER BY q.id DESC LIMIT 2",
		"UNWIND [1, 2] AS x MERGE (p:P {id: x}) ON MATCH SET p.seen = x RETURN p.id",
		"MATCH (q:Q) WITH q LIMIT 2 DETACH DELETE q",
		"MATCH (p:P)-[r:N]->(q) WHERE p.id > 1 DELETE r RETURN count(*)",
		"MATCH (p:P) WHERE p.id IN [1, 2] DETACH DELETE p RETURN count(*)",
		"UNWIND $ids AS x MATCH (p:P {id: x}), (q:Q {id: x}) MERGE (p)-[:B]->(q) RETURN count(*)",
		"MATCH (a:P {id: 1}), (b:Q {id: 3}) RETURN [path IN allShortestPaths((a)-[*..4]-(b)) | length(path)] AS lengths",
		"BEGIN CALL { UNWIND range(1, 3) AS x CREATE (n:CS {x: x}) RETURN n } CASE WHEN false THEN ROLLBACK ELSE RETURN n COMMIT",
	}
	type sweepCase struct {
		query    string
		explicit bool
	}
	cases := make([]sweepCase, 0, len(queries)+2)
	for _, query := range queries {
		cases = append(cases, sweepCase{query: query})
	}
	cases = append(cases,
		sweepCase{query: "UNWIND range(1,$n) AS x RETURN {value: x} AS v", explicit: true},
		sweepCase{query: "MATCH (p:P) SET p.tx = true RETURN p.id ORDER BY p.id", explicit: true},
	)
	for _, tc := range cases {
		_, closeBaseline := sweepCancellationFixture(t, tc.explicit)
		baseline := closeBaseline()
		counting := newSweepCancellationContext(0)
		executor, closeFixture := sweepCancellationFixture(t, tc.explicit)
		_, err := executor.Execute(counting, tc.query, parameters)
		closeFixture()
		require.NoError(t, err, tc.query)
		observations := counting.calls
		require.Positive(t, observations, tc.query)
		for _, at := range sweepCancellationPoints(observations) {
			ctx := newSweepCancellationContext(at)
			executor, closeFixture := sweepCancellationFixture(t, tc.explicit)
			_, err := executor.Execute(ctx, tc.query, parameters)
			stored := closeFixture()
			if !ctx.canceled() {
				// A run can observe its context fewer times (parallel or
				// map-ordered passes); nothing canceled it.
				require.NoError(t, err, "%s: observation %d of %d", tc.query, at, observations)
				continue
			}
			if err == nil {
				// Canceled after its commit: only a committed write keeps its
				// result, and its writes are stored.
				require.False(t, tc.explicit, "%s: an explicit transaction's statement commits nothing", tc.query)
				require.True(t, QueryPermissionRequirements(tc.query).Write, "%s: a canceled read must fail", tc.query)
				require.NotEqual(t, baseline, stored, "%s: success at observation %d without a commit", tc.query, at)
				continue
			}
			require.ErrorIs(t, err, context.Canceled, "%s: canceled at observation %d of %d", tc.query, at, observations)
			if !strings.Contains(tc.query, "IN TRANSACTIONS") {
				// Batches of CALL IN TRANSACTIONS commit independently.
				require.Equal(t, baseline, stored, "%s: a failed statement stores nothing (observation %d)", tc.query, at)
			}
		}
	}
}
