package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newAsyncStackTestExecutor keeps its historical name to avoid call-site
// churn; it now builds the server's storage stack
// (Badger -> WAL -> Namespaced). Every auto-commit write commits through the
// single transactional route.
func newAsyncStackTestExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	return newAsyncStackExecutor(t)
}

// newAsyncStackExecutor is the server's storage stack (Badger, WAL,
// namespace) for a test or a benchmark.
func newAsyncStackExecutor(t testing.TB) *StorageExecutor {
	t.Helper()
	dir := t.TempDir()
	badger, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	wal, err := storage.NewWAL(dir+"/wal", nil)
	require.NoError(t, err)
	engine := storage.NewWALEngine(badger, wal)
	t.Cleanup(func() {
		_ = wal.Close()
		_ = badger.Close()
	})
	return NewStorageExecutor(storage.NewNamespacedEngine(engine, "test"))
}

func TestGh713AsyncCreateReturnPlanning(t *testing.T) {
	for _, route := range []string{"autocommit", "explicit transaction"} {
		for _, test := range []struct {
			name       string
			projection string
			columns    []string
			rows       [][]interface{}
		}{
			{"zero limit", "n.x AS value LIMIT 0", []string{"value"}, nil},
			{"parameter skip", "n.x AS value SKIP $skip", []string{"value"}, nil},
			{"distinct", "DISTINCT n.x AS value", []string{"value"}, [][]interface{}{{int64(1)}}},
			{"parameter column", "$p", []string{"$p"}, [][]interface{}{{int64(7)}}},
			{"typed float parameter", "$whole", []string{"$whole"}, [][]interface{}{{float64(7)}}},
			{"parameter map", "$payload", []string{"$payload"}, [][]interface{}{{map[string]interface{}{"value": int64(2)}}}},
			{"quoted alias", "n.x + $p AS `expr value`", []string{"expr value"}, [][]interface{}{{int64(8)}}},
			{"aggregation control", "collect(n.x) AS values, count(*) AS total", []string{"values", "total"}, [][]interface{}{{[]interface{}{int64(1)}, int64(1)}}},
		} {
			t.Run(route+"/"+test.name, func(t *testing.T) {
				_, ctx := newUnitExecutor(t)
				exec := newAsyncStackTestExecutor(t)
				if route == "explicit transaction" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				params := map[string]interface{}{"p": int64(7), "skip": int64(1), "whole": float64(7), "payload": map[string]interface{}{"value": int64(2)}}
				query := "CREATE (n:Value {x: 1}) RETURN " + test.projection
				result, err := exec.Execute(ctx, query, params)
				require.NoError(t, err)
				require.Equal(t, test.columns, result.Columns)
				require.Len(t, result.Rows, len(test.rows))
				if len(test.rows) > 0 {
					require.Equal(t, test.rows, result.Rows)
				}
				if route == "explicit transaction" {
					_, err := exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
				stored, err := exec.Execute(ctx, "MATCH (n:Value) RETURN count(n) AS count, sum(n.x) AS total", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1), int64(1)}}, stored.Rows)
			})
		}
	}
}

func TestGh713AsyncCreateReturnFailureDoesNotPublish(t *testing.T) {
	for _, route := range []string{"autocommit", "explicit transaction"} {
		t.Run(route, func(t *testing.T) {
			_, ctx := newUnitExecutor(t)
			exec := newAsyncStackTestExecutor(t)
			if route == "explicit transaction" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			query := "CREATE (n:Value {x: 1}) RETURN n.x / 0 AS value"
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			require.Contains(t, statusText(err), "Neo.ClientError.Statement.ArithmeticError")
			if route == "explicit transaction" {
				_, err = exec.Execute(ctx, "ROLLBACK", nil)
				require.NoError(t, err)
			}
			stored, err := exec.Execute(ctx, "MATCH (n:Value) RETURN count(n) AS count", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, stored.Rows)
		})
	}
}

// Every RETURN item after a plain CREATE is evaluated, not only items that
// reference a created variable, on every CREATE route: executeCreate, the
// multi-CREATE executor and the auto-commit node-only async fast path (#551).
func TestCreateReturnEvaluatesEveryItem(t *testing.T) {
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"server stack": newAsyncStackTestExecutor,
	}
	for stack, build := range stacks {
		for _, mode := range []string{"auto-commit", "explicit transaction"} {
			t.Run(stack+"/"+mode, func(t *testing.T) {
				exec := build(t)
				ctx := context.Background()
				if mode == "explicit transaction" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				params := map[string]interface{}{"a": int64(7)}
				for _, tc := range []struct {
					q    string
					want [][]interface{}
				}{
					{"CREATE (:P {v: 1}) RETURN 1 AS ok", [][]interface{}{{int64(1)}}},
					{"CREATE (p:P {v: 1}) RETURN 1 AS ok", [][]interface{}{{int64(1)}}},
					{"CREATE (p:P {v: 1}) RETURN p.v AS v, 1 AS ok", [][]interface{}{{int64(1), int64(1)}}},
					{"CREATE (p:P {v: 1}) RETURN 'x' AS s, 2 + 3 AS n, $a AS a", [][]interface{}{{"x", int64(5), int64(7)}}},
					{"CREATE (p:P {v: 1})-[:R]->(:Q) RETURN 1 AS ok", [][]interface{}{{int64(1)}}},
					{"CREATE (p:P {v: 2}) RETURN p.v + 1 AS v1, toString(p.v) AS s", [][]interface{}{{int64(3), "2"}}},
					{"CREATE (ab:P {v: 3}), (a:P {v: 4}) RETURN ab.v AS x, a.v AS y", [][]interface{}{{int64(3), int64(4)}}},
					{"CREATE (a:P {v: 5}) CREATE (:Q) RETURN 1 AS ok, a.v AS v", [][]interface{}{{int64(1), int64(5)}}},
					{"CREATE (a:P {v: 6}) RETURN count(*) AS c, count(a) AS ca", [][]interface{}{{int64(1), int64(1)}}},
					{"CREATE (a:P {v: 1}), (b:Q {v: 3}) RETURN a.v + b.v AS s", [][]interface{}{{int64(4)}}},
					{"CREATE (a:P {v: 2}), (b:Q {v: 3}) RETURN a.v * b.v AS m, [a.v, b.v] AS l", [][]interface{}{{int64(6), []interface{}{int64(2), int64(3)}}}},
					{"CREATE (a:P {name: 'x'}), (b:Q {name: 'y'}) RETURN a.name + b.name AS s", [][]interface{}{{"xy"}}},
					{"CREATE (a:P {v: 5})-[r:R {w: 6}]->(:Q) RETURN r.w + a.v AS s, [r.w, a.v, [a.v]] AS l", [][]interface{}{{int64(11), []interface{}{int64(6), int64(5), []interface{}{int64(5)}}}}},
				} {
					res, err := exec.Execute(ctx, tc.q, params)
					require.NoError(t, err, tc.q)
					assert.Equal(t, tc.want, res.Rows, tc.q)
				}
			})
		}
	}
}

// projectCreateReturn covers every kind of RETURN item after CREATE:
// relationship variables, their properties and functions, paths, count() of a
// null expression, and expressions over several created variables.
func TestProjectCreatedReturnItemBranches(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()

	res, err := exec.Execute(ctx, "CREATE (a:P {v: 1})-[r:R {w: 2}]->(b:Q {v: 3}) RETURN r.w AS w, r.missing AS m, type(r) AS t, a.v + b.v AS s, count(a.missing) AS c0, count(r) AS c1", nil)
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{int64(2), nil, "R", int64(4), int64(0), int64(1)}}, res.Rows)

	res, err = exec.Execute(ctx, "CREATE (a:P {v: 5})-[r:R {w: 6}]->(:Q) RETURN r.w + a.v AS s, a.v * r.w AS m", nil)
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{int64(11), int64(30)}}, res.Rows)

	res, err = exec.Execute(ctx, "CREATE (a:P)-[r:R]->(:Q) RETURN r", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	_, isEdge := res.Rows[0][0].(*storage.Edge)
	assert.True(t, isEdge, "RETURN r yields the created relationship")

	res, err = exec.Execute(ctx, "CREATE p = (:P)-[:R]->(:Q) RETURN length(p) AS l", nil)
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{int64(1)}}, res.Rows)

	node := &storage.Node{ID: "n1", Properties: map[string]interface{}{"v": int64(9)}}
	query := "CREATE (x) RETURN x.v AS v, 1 + 2 AS sum, count(*) AS count"
	out := createOutcome{cypher: query, returnIdx: strings.Index(query, "RETURN"), nodes: map[string]*storage.Node{"x": node}, result: &ExecuteResult{}}
	require.NoError(t, exec.projectCreateReturn(ctx, &out))
	require.Equal(t, []string{"v", "sum", "count"}, out.result.Columns)
	require.Equal(t, [][]interface{}{{int64(9), int64(3), int64(1)}}, out.result.Rows)
}

// MATCH ... CREATE ... RETURN uses the same projection: items over several
// variables (matched and created) are evaluated as one expression (#569).
func TestMatchCreateReturnOverSeveralVariables(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Seed {v: 10})", nil)
	require.NoError(t, err)
	res, err := exec.Execute(ctx, "MATCH (s:Seed) CREATE (a:P {v: 1})-[r:R {w: 2}]->(s) RETURN s.v + a.v AS x, r.w * a.v AS y, [s.v, a.v] AS l, a.v AS v, count(*) AS c", nil)
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{int64(11), int64(2), []interface{}{int64(10), int64(1)}, int64(1), int64(1)}}, res.Rows)
}

func TestCreateProjectionCanonicalTypedParameters(t *testing.T) {
	params := map[string]interface{}{
		"whole":   float64(7),
		"payload": map[string]interface{}{"values": []interface{}{float64(3), int64(4)}},
	}
	for _, query := range []string{
		"CREATE (n:P {v: 1}) RETURN $whole, $payload, n.v",
		"CREATE (n:P {v: 1}) CREATE (m:Q) RETURN $whole, $payload, n.v",
		"CREATE (n:P {v: 1}) SET n.extra = true RETURN $whole, $payload, n.v",
	} {
		t.Run(query, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			ctx := context.WithValue(context.Background(), paramsKey, params)
			var result *ExecuteResult
			var err error
			if strings.Contains(query, "CREATE (m") {
				result, err = exec.Execute(ctx, query, params)
			} else if strings.Contains(query, " SET ") {
				result, err = exec.Execute(ctx, query, params)
			} else {
				result, err = exec.Execute(ctx, query, getParamsFromContext(ctx))
			}
			require.NoError(t, err)
			require.Equal(t, []string{"$whole", "$payload", "n.v"}, result.Columns)
			require.Equal(t, [][]interface{}{{params["whole"], params["payload"], int64(1)}}, result.Rows)
			require.NotNil(t, result.Stats)
		})
	}
}

func TestCreateProjectionCanonicalRowsAndPaths(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	result, err := exec.Execute(ctx, "CREATE p = (a:P)-[r:R]->(b:Q) WITH p AS path, a, r, 2 AS scalar CREATE (c:C) RETURN DISTINCT length(path) AS hops, type(r) AS kind, scalar, c LIMIT 1", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"hops", "kind", "scalar", "c"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Equal(t, []interface{}{int64(1), "R", int64(2)}, result.Rows[0][:3])
	require.IsType(t, &storage.Node{}, result.Rows[0][3])
	require.Equal(t, 3, result.Stats.NodesCreated)
	require.Equal(t, 1, result.Stats.RelationshipsCreated)

	result, err = exec.Execute(ctx, "CREATE (n:N {v: 3}) RETURN * LIMIT 0", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.Equal(t, []string{"n"}, result.Columns)
	require.Empty(t, result.Rows)
	require.Equal(t, 1, result.Stats.NodesCreated)
}
