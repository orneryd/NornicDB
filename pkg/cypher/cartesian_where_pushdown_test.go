package cypher

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func BenchmarkCartesianMatchEqualityJoin(b *testing.B) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "cartesian-bench")
	const nodeCount = 500
	for i := 0; i < nodeCount; i++ {
		key := strconv.Itoa(i)
		for _, node := range []*storage.Node{
			{ID: storage.NodeID("left-" + key), Labels: []string{"CartesianLeft"}, Properties: map[string]interface{}{"key": key}},
			{ID: storage.NodeID("right-" + key), Labels: []string{"CartesianRight"}, Properties: map[string]interface{}{"key": key}},
		} {
			if _, err := store.CreateNode(node); err != nil {
				b.Fatal(err)
			}
		}
	}
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	ctx := context.Background()
	query := "MATCH (a:CartesianLeft), (b:CartesianRight) WHERE a.key = b.key RETURN count(*) AS c"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(ctx, query, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0] != int64(nodeCount) {
			b.Fatalf("got rows %v, want count %d", result.Rows, nodeCount)
		}
	}
}

func BenchmarkGh728CartesianPreparedMembership(b *testing.B) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "cartesian-membership-bench")
	b.Cleanup(func() { require.NoError(b, store.Close()) })
	for index := 0; index < 32; index++ {
		key := "key-" + strconv.Itoa(index)
		for _, side := range []string{"Left", "Right"} {
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(side + key), Labels: []string{"Prepared" + side}, Properties: map[string]interface{}{"key": key}})
			require.NoError(b, err)
		}
	}
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	query := "MATCH (a:PreparedLeft), (b:PreparedRight) WHERE a.key IN $keys OR b.key IN $keys RETURN a.key AS key"

	for _, length := range []int{64, 4096} {
		b.Run("keys="+strconv.Itoa(length), func(b *testing.B) {
			keys := make([]interface{}, length)
			for index := range keys {
				keys[index] = "key-" + strconv.Itoa(index)
			}
			ctx := withExpressionFailureSlot(withQueryParams(context.Background(), map[string]interface{}{"keys": keys}))
			apply := func() {
				result, err := exec.Execute(ctx, query, getParamsFromContext(ctx))
				if err != nil || len(result.Rows) != 1024 {
					b.Fatalf("expected 1024 rows, got result=%v err=%v", result, err)
				}
			}
			apply()
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				apply()
			}
			b.StopTimer()
		})
	}
}

func TestGh713CartesianAdaptiveWorkers(t *testing.T) {
	for _, test := range []struct{ rows, complexity, cores, workers int }{
		{0, 1, 12, 1}, {1024, 1, 12, 1}, {65535, 1, 12, 1},
		{65536, 1, 12, 4}, {65536, 2, 12, 8}, {262144, 1, 12, 12},
		{1048576, 1, 64, 64}, {1048576, 2, 128, 128}, {65536, 1, 1, 1},
	} {
		require.Equal(t, test.workers, cartesianAggregateWorkers(test.rows, test.complexity, test.cores))
	}
}

func TestGh713CartesianPartitionEquivalence(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	patterns := []struct {
		variable string
		nodes    []*storage.Node
	}{
		{"a", []*storage.Node{{ID: "large", Properties: map[string]interface{}{"key": "first", "value": int64(9007199254740993)}}, {ID: "small", Properties: map[string]interface{}{"key": "second", "value": int64(2)}}, {ID: "missing", Properties: map[string]interface{}{"key": "first"}}}},
		{"b", []*storage.Node{{ID: "one"}, {ID: "two"}}},
	}
	for _, clause := range []string{
		"RETURN count(*) AS total", "RETURN sum(a.value) AS total", "RETURN count(a.value) AS total",
		"RETURN sum(1) AS total", "RETURN sum(a.value) + count(*) AS total",
		"RETURN count(*) + coalesce(a.value, 0) AS total",
		"RETURN a.key AS key, sum(a.value) AS total ORDER BY key SKIP 1 LIMIT 1",
		"RETURN a.key AS key, count(*) AS total", "RETURN a.key AS key, count(*) AS total LIMIT 0",
	} {
		t.Run(clause, func(t *testing.T) {
			plan := returnProjectionPlanFor(clause)
			var rows []pipelineRow
			for _, combination := range exec.buildCartesianProduct(patterns) {
				values := make(pipelineRow, len(combination))
				for variable, node := range combination {
					if node != nil {
						values[variable] = node
					} else {
						values[variable] = nil
					}
				}
				rows = append(rows, values)
			}
			expected, err := exec.projectMergeReturnSource(withExpressionFailureSlot(context.Background()), nil, clause, pipelineRowsSource(rows))
			require.NoError(t, err)
			if clause == "RETURN sum(a.value) AS total" {
				require.Equal(t, [][]interface{}{{int64(18014398509481990)}}, expected.Rows)
			}
			for _, workers := range []int{1, 2, 4, 8} {
				ctx := withExpressionFailureSlot(context.Background())
				groups, handled, err := exec.tryCartesianAggregatePartitions(ctx, patterns, plan, workers)
				require.True(t, handled)
				require.NoError(t, err)
				result, err := exec.projectMergeReturnSource(ctx, nil, clause, nil, groups)
				require.NoError(t, err)
				require.Equal(t, expected.Columns, result.Columns)
				require.Equal(t, expected.Rows, result.Rows)
			}
		})
	}
	for _, clause := range []string{"RETURN sum(0.5)", "RETURN avg(a.value)", "RETURN collect(a.value)", "RETURN count(DISTINCT a.key)", "RETURN sum(a.value + 1)", "RETURN labels(a), count(*)"} {
		groups, handled, err := exec.tryCartesianAggregatePartitions(context.Background(), patterns, returnProjectionPlanFor(clause), 4)
		require.False(t, handled, clause)
		require.NoError(t, err)
		require.Nil(t, groups)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, handled, err := exec.tryCartesianAggregatePartitions(ctx, patterns, returnProjectionPlanFor("RETURN count(*)"), 4)
	require.True(t, handled)
	require.ErrorIs(t, err, context.Canceled)
}

type cartesianCancelContext struct {
	context.Context
	calls atomic.Int64
}

func (ctx *cartesianCancelContext) Err() error {
	if ctx.calls.Add(1) >= 10 {
		return context.Canceled
	}
	return nil
}

func TestGh713CartesianPartitionNullAndCancellation(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	patterns := []struct {
		variable string
		nodes    []*storage.Node
	}{
		{"a", []*storage.Node{nil, {Properties: map[string]interface{}{"key": "first", "value": int64(3)}}}},
		{"b", []*storage.Node{{}, {}}},
		{"c", []*storage.Node{{}, {}}},
	}
	for _, clause := range []string{"RETURN count(a.value) AS total", "RETURN sum(a.value) AS total", "RETURN a.key AS key, count(*) AS total"} {
		for _, workers := range []int{1, 2, 8} {
			ctx := withExpressionFailureSlot(context.Background())
			groups, handled, err := exec.tryCartesianAggregatePartitions(ctx, patterns, returnProjectionPlanFor(clause), workers)
			require.True(t, handled)
			require.NoError(t, err)
			result, err := exec.projectMergeReturnSource(ctx, nil, clause, nil, groups)
			require.NoError(t, err)
			switch clause {
			case "RETURN count(a.value) AS total":
				require.Equal(t, [][]interface{}{{int64(4)}}, result.Rows)
			case "RETURN sum(a.value) AS total":
				require.Equal(t, [][]interface{}{{int64(12)}}, result.Rows)
			default:
				require.Equal(t, [][]interface{}{{nil, int64(4)}, {"first", int64(4)}}, result.Rows)
			}
		}
	}
	patterns[0].nodes = append(patterns[0].nodes, patterns[0].nodes...)
	_, handled, err := exec.tryCartesianAggregatePartitions(&cartesianCancelContext{Context: context.Background()}, patterns, returnProjectionPlanFor("RETURN count(*)"), 4)
	require.True(t, handled)
	require.ErrorIs(t, err, context.Canceled)
}

func BenchmarkGh713CartesianPartitionWorkers(b *testing.B) {
	exec := NewStorageExecutor(newTestMemoryEngine(b))
	patterns := []struct {
		variable string
		nodes    []*storage.Node
	}{{variable: "a"}, {variable: "b"}}
	for index := range patterns {
		for value := 0; value < 512; value++ {
			patterns[index].nodes = append(patterns[index].nodes, &storage.Node{Properties: map[string]interface{}{"value": int64(value), "key": strconv.Itoa(value % 16)}})
		}
	}
	for _, clause := range []string{"RETURN count(*)", "RETURN sum(a.value)", "RETURN a.key, count(*)"} {
		for _, mode := range []string{"serial", "adaptive"} {
			b.Run(clause+"/"+mode, func(b *testing.B) {
				workers := 0
				if mode == "serial" {
					workers = 1
				}
				plan := returnProjectionPlanFor(clause)
				ctx := withExpressionFailureSlot(context.Background())
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					groups, handled, err := exec.tryCartesianAggregatePartitions(ctx, patterns, plan, workers)
					if err != nil || !handled || len(groups) == 0 {
						b.Fatalf("unexpected partitions: %v, %v", groups, err)
					}
					var total int64
					for _, group := range groups {
						projection := group.projections[len(group.projections)-1]
						value, ok := projection.states[0].result(ctx, exec)
						if !ok {
							b.Fatal("invalid aggregate state")
						}
						total += value.(int64)
					}
					want := int64(512 * 512)
					if clause == "RETURN sum(a.value)" {
						want *= 511
						want /= 2
					}
					if total != want {
						b.Fatalf("aggregate total %d, want %d", total, want)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "cores")
			})
		}
	}
}

func BenchmarkGh713CartesianParallel(b *testing.B) {
	for _, size := range []int{32, 512} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "cartesian-parallel-bench")
			b.Cleanup(func() { require.NoError(b, store.Close()) })
			for index := 0; index < size; index++ {
				for _, side := range []string{"Left", "Right"} {
					_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(side + strconv.Itoa(index)), Labels: []string{"Parallel" + side}, Properties: map[string]interface{}{"key": "group-" + strconv.Itoa(index%16), "value": int64(index)}})
					require.NoError(b, err)
				}
			}
			exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
			for _, test := range []struct {
				name, clause string
				rows         int
				total        int64
			}{
				{"count", "RETURN count(*) AS total", 1, int64(size * size)},
				{"sum", "RETURN sum(a.value) AS total", 1, int64(size * size * (size - 1) / 2)},
				{"grouped", "RETURN a.key AS key, count(*) AS total", 16, int64(size * size)},
			} {
				b.Run(test.name, func(b *testing.B) {
					ctx := withExpressionFailureSlot(context.Background())
					query := "MATCH (a:ParallelLeft), (b:ParallelRight) " + test.clause

					apply := func() {
						result, err := exec.Execute(ctx, query, getParamsFromContext(ctx))
						if err != nil || len(result.Rows) != test.rows {
							b.Fatalf("unexpected aggregate result: %v, %v", result, err)
						}
						var total int64
						for _, row := range result.Rows {
							value, ok := row[len(row)-1].(int64)
							if !ok {
								b.Fatalf("unexpected aggregate row: %v", row)
							}
							total += value
						}
						if total != test.total {
							b.Fatalf("unexpected total: %d", total)
						}
					}
					apply()
					b.ReportAllocs()
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						apply()
					}
					b.StopTimer()
				})
			}
		})
	}
}

func BenchmarkGh713CartesianAggregation(b *testing.B) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "cartesian-aggregate-bench")
	for index := 0; index < 32; index++ {
		key := "key-" + strconv.Itoa(index)
		for _, side := range []string{"Left", "Right"} {
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(side + key), Labels: []string{"Aggregate" + side}, Properties: map[string]interface{}{"key": key}})
			require.NoError(b, err)
		}
	}
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	for _, test := range []struct {
		name, clause string
		rows         int
		count        int64
	}{
		{"count", "RETURN count(*) AS total", 1, 1024},
		{"grouped", "RETURN a.key AS key, count(*) AS total", 32, 32},
	} {
		b.Run(test.name, func(b *testing.B) {
			ctx := withExpressionFailureSlot(context.Background())
			query := "MATCH (a:AggregateLeft), (b:AggregateRight) " + test.clause

			apply := func() {
				result, err := exec.Execute(ctx, query, getParamsFromContext(ctx))
				if err != nil || len(result.Rows) != test.rows {
					b.Fatalf("unexpected aggregate result: %v, %v", result, err)
				}
				for _, row := range result.Rows {
					if row[len(row)-1] != test.count {
						b.Fatalf("unexpected aggregate row: %v", row)
					}
				}
			}
			apply()
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				apply()
			}
			b.StopTimer()
		})
	}
}

func TestGh728CartesianMembershipParameterFreshness(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "cartesian-membership-test")
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, key := range []string{"key-1", "key-2"} {
		for _, side := range []string{"Left", "Right"} {
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(side + key), Labels: []string{"Prepared" + side}, Properties: map[string]interface{}{"key": key}})
			require.NoError(t, err)
		}
	}
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	query := "MATCH (a:PreparedLeft), (b:PreparedRight) WHERE a.key IN $keys OR b.key IN $keys RETURN a.key AS left, b.key AS right"
	keys := []interface{}{"key-1"}
	ctx := withExpressionFailureSlot(withQueryParams(context.Background(), map[string]interface{}{"keys": keys}))
	apply := func() [][]interface{} {
		result, err := exec.Execute(ctx, query, getParamsFromContext(ctx))
		require.NoError(t, err)
		require.NoError(t, getExpressionFailure(ctx))
		return result.Rows
	}
	want := [][]interface{}{{"key-1", "key-1"}, {"key-1", "key-2"}, {"key-2", "key-1"}}
	require.ElementsMatch(t, want, apply())
	keys[0] = "no-match"
	require.Empty(t, apply())
	keys[0] = "key-1"
	require.ElementsMatch(t, want, apply())
}

func TestGh713LocalProjectionAvoidsContextCopies(t *testing.T) {
	exec := &StorageExecutor{}
	node := &storage.Node{ID: "local-node", Properties: map[string]interface{}{"key": "local-key"}}
	outer := &storage.Node{ID: "outer-node", Properties: map[string]interface{}{"key": "outer-key"}}
	ctx := withQueryParams(withValueBindings(context.Background(), map[string]interface{}{"a": outer, "extra": int64(9)}), map[string]interface{}{"unused": int64(1), "shadow": int64(4)})
	values := pipelineRow{"a": node, "$shadow": int64(7)}
	for _, test := range []struct {
		expression string
		want       interface{}
	}{
		{"a", node},
		{"a.key", "local-key"},
		{"$shadow", int64(7)},
	} {
		t.Run(test.expression, func(t *testing.T) {
			allocations := testing.AllocsPerRun(1000, func() {
				value, evaluated := exec.evaluateRowExpressionWithContext(ctx, test.expression, values)
				if !evaluated || value != test.want {
					t.Fatalf("got %v (%v), want %v", value, evaluated, test.want)
				}
			})
			require.Zero(t, allocations)
		})
	}
	for _, expression := range []string{"extra", "$unused"} {
		value, evaluated := exec.evaluateRowExpressionWithContext(ctx, expression, values)
		require.True(t, evaluated)
		if expression == "extra" {
			require.Equal(t, int64(9), value)
		} else {
			require.Equal(t, int64(1), value)
		}
	}
	require.Equal(t, pipelineRow{"a": node, "$shadow": int64(7)}, values)
}

func TestGh713SharedReturnBorrowedSourceOwnership(t *testing.T) {
	exec := &StorageExecutor{}
	for _, test := range []struct {
		clause string
		rows   [][]interface{}
		code   string
	}{
		{"RETURN value AS value", [][]interface{}{{int64(3)}, {int64(1)}, {int64(2)}}, ""},
		{"RETURN *", [][]interface{}{{int64(3)}, {int64(1)}, {int64(2)}}, ""},
		{"RETURN value AS value ORDER BY value SKIP 1 LIMIT 1", [][]interface{}{{int64(2)}}, ""},
		{"RETURN DISTINCT value AS value ORDER BY value", [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, ""},
		{"RETURN value / 0 AS value", nil, "Neo.ClientError.Statement.ArithmeticError"},
		{"RETURN missing AS value", nil, "Neo.ClientError.Statement.SyntaxError"},
	} {
		t.Run(test.clause, func(t *testing.T) {
			ctx := withExpressionFailureSlot(context.Background())
			values := pipelineRow{}
			yielded := 0
			source := func(yield func(pipelineRow) bool) bool {
				for _, value := range []int64{3, 1, 2, 3} {
					values["value"] = value
					yielded++
					if !yield(values) {
						break
					}
				}
				values["value"] = int64(99)
				return true
			}
			result, err := exec.projectMergeReturnSource(ctx, nil, test.clause, source)
			if test.code != "" {
				require.Error(t, err)
				require.True(t, strings.HasPrefix(statusText(err), test.code), statusText(err))
				require.Equal(t, 1, yielded)
				return
			}
			require.NoError(t, err)
			require.NoError(t, getExpressionFailure(ctx))
			require.Equal(t, []string{"value"}, result.Columns)
			want := test.rows
			if test.clause == "RETURN value AS value" || test.clause == "RETURN *" {
				want = append(append([][]interface{}{}, want...), []interface{}{int64(3)})
			}
			require.Equal(t, want, result.Rows)
			require.Equal(t, 4, yielded)
		})
	}
	for _, clause := range []string{"RETURN value", "RETURN *"} {
		ctx := withExpressionFailureSlot(context.Background())
		result, handled := exec.pipelineApplyReturnSource(ctx, nil, clause, func(func(pipelineRow) bool) bool { return false }, false)
		require.False(t, handled)
		require.Nil(t, result)
	}
}

func TestGh713CartesianSharedProjectionWindows(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "cartesian-projection-test")
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, key := range []string{"key-1", "key-2"} {
		for _, side := range []string{"Left", "Right"} {
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(side + key), Labels: []string{"Projection" + side}, Properties: map[string]interface{}{"key": key}})
			require.NoError(t, err)
		}
	}
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	for _, test := range []struct {
		name, tail, code string
		columns          []string
		rows             [][]interface{}
	}{
		{"literal window", "RETURN a.key AS left, b.key AS right ORDER BY left, right SKIP 1 LIMIT 1", "", []string{"left", "right"}, [][]interface{}{{"key-1", "key-2"}}},
		{"arithmetic window", "RETURN a.key AS left, b.key AS right ORDER BY left, right SKIP 1 + 1 LIMIT 1 + 1", "", []string{"left", "right"}, [][]interface{}{{"key-2", "key-1"}, {"key-2", "key-2"}}},
		{"parameter window", "RETURN a.key AS left, b.key AS right ORDER BY left, right SKIP $skip LIMIT $limit", "", []string{"left", "right"}, [][]interface{}{{"key-2", "key-1"}}},
		{"parameter zero", "RETURN a.key AS `left key` LIMIT $zero", "", []string{"left key"}, [][]interface{}{}},
		{"aggregate parameter zero", "RETURN count(*) AS total LIMIT $zero", "", []string{"total"}, [][]interface{}{}},
		{"aggregate count", "RETURN count(*) AS total", "", []string{"total"}, [][]interface{}{{int64(4)}}},
		{"aggregate distinct", "RETURN count(DISTINCT a.key) AS total", "", []string{"total"}, [][]interface{}{{int64(2)}}},
		{"aggregate quoted arithmetic", "RETURN count(*) + 1 AS `total count` LIMIT 1", "", []string{"total count"}, [][]interface{}{{int64(5)}}},
		{"aggregate grouped window", "RETURN a.key AS key, count(*) AS total ORDER BY key SKIP $limit LIMIT $limit", "", []string{"key", "total"}, [][]interface{}{{"key-2", int64(2)}}},
		{"aggregate window failure", "RETURN count(*) AS total LIMIT 1 / 0", "Neo.ClientError.Statement.ArithmeticError", nil, nil},
		{"projection failure", "RETURN 1 / 0 AS value", "Neo.ClientError.Statement.ArithmeticError", nil, nil},
		{"window failure", "RETURN a.key AS left LIMIT 1 / 0", "Neo.ClientError.Statement.ArithmeticError", nil, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			query := "MATCH (a:ProjectionLeft), (b:ProjectionRight) " + test.tail
			ctx := withExpressionFailureSlot(withQueryParams(context.Background(), map[string]interface{}{"skip": int64(2), "limit": int64(1), "zero": int64(0)}))

			stats := &QueryStats{}
			buffer := &ExecuteResult{Columns: test.columns, Stats: stats}
			result, err := exec.Execute(ctx, query, getParamsFromContext(ctx))
			publicResult, publicErr := exec.Execute(context.Background(), query, map[string]interface{}{"skip": int64(2), "limit": int64(1), "zero": int64(0)})
			if test.code != "" {
				require.Error(t, err)
				require.True(t, strings.HasPrefix(statusText(err), test.code), statusText(err))
				require.Error(t, publicErr)
				require.True(t, strings.HasPrefix(statusText(publicErr), test.code), statusText(publicErr))
				return
			}
			require.NoError(t, err)
			require.NoError(t, publicErr)
			require.Equal(t, test.columns, publicResult.Columns)
			require.Equal(t, test.rows, publicResult.Rows)
			require.NoError(t, getExpressionFailure(ctx))
			require.NotSame(t, buffer, result)
			require.Equal(t, test.columns, result.Columns)
			require.Equal(t, test.rows, result.Rows)
			require.NotSame(t, stats, result.Stats)
		})
	}
	for _, test := range []struct {
		clause string
		rows   [][]interface{}
	}{
		{"RETURN count(*) AS total", [][]interface{}{{int64(0)}}},
		{"RETURN count(*) AS total LIMIT 0", [][]interface{}{}},
	} {
		t.Run("empty "+test.clause, func(t *testing.T) {
			query := "MATCH (a:ProjectionMissing), (b:ProjectionRight) " + test.clause
			ctx := withExpressionFailureSlot(context.Background())
			buffer := &ExecuteResult{Stats: &QueryStats{}}
			result, err := exec.Execute(ctx, query, getParamsFromContext(ctx))
			require.NoError(t, err)
			require.NotSame(t, buffer, result)
			require.Equal(t, []string{"total"}, result.Columns)
			require.Equal(t, test.rows, result.Rows)
			publicResult, publicErr := exec.Execute(context.Background(), query, nil)
			require.NoError(t, publicErr)
			require.Equal(t, result.Columns, publicResult.Columns)
			require.Equal(t, result.Rows, publicResult.Rows)
		})
	}
}

func TestCartesianWherePushdown_InAndEqualityJoin(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, k := range []string{"k1", "k2", "k3"} {
		_, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID("nornic:o-" + k),
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"joinKey": k,
			},
		})
		require.NoError(t, err)
	}
	for _, row := range []struct {
		id   string
		key  string
		lang string
	}{
		{"t-k1-es", "k1", "es"},
		{"t-k1-fr", "k1", "fr"},
		{"t-k2-es", "k2", "es"},
		{"t-k9-es", "k9", "es"},
	} {
		_, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID("nornic:" + row.id),
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"joinKey": row.key,
				"lang":    row.lang,
			},
		})
		require.NoError(t, err)
	}

	res, err := exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.joinKey IN ['k1','k2'] AND t.joinKey = o.joinKey
RETURN count(*) AS c
`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(3), res.Rows[0][0])

	res, err = exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE t.joinKey = o.joinKey AND o.joinKey IN ['k1','k2']
RETURN o.joinKey AS k, count(*) AS c
ORDER BY k
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"k", "c"}, res.Columns)
	require.Len(t, res.Rows, 2)
	got := map[string]int64{}
	for _, row := range res.Rows {
		got[row[0].(string)] = row[1].(int64)
	}
	require.Equal(t, int64(2), got["k1"])
	require.Equal(t, int64(1), got["k2"])

}

func TestCartesianWherePushdown_NullConstraint(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	patternMatches := []struct {
		variable string
		nodes    []*storage.Node
	}{
		{variable: "o", nodes: []*storage.Node{
			{ID: "o-1", Properties: map[string]interface{}{"joinKey": "k1"}},
			{ID: "o-2", Properties: map[string]interface{}{}},
			{ID: "o-3", Properties: map[string]interface{}{"joinKey": nil}},
		}},
		{variable: "t", nodes: []*storage.Node{
			{ID: "t-1", Properties: map[string]interface{}{"joinKey": "k1"}},
			{ID: "t-2", Properties: map[string]interface{}{"joinKey": "k2"}},
		}},
	}

	filtered := exec.applyCartesianWherePushdown(context.Background(), patternMatches, "o.joinKey IS NOT NULL AND t.joinKey = o.joinKey")
	require.Len(t, filtered, 2)
	require.Len(t, filtered[0].nodes, 1)
	require.Equal(t, "o-1", string(filtered[0].nodes[0].ID))
	require.Len(t, filtered[1].nodes, 1)
	require.Equal(t, "t-1", string(filtered[1].nodes[0].ID))
	joined, ok := exec.buildCombinationsUsingWhereJoin(filtered, "o.joinKey IS NOT NULL AND t.joinKey = o.joinKey")
	require.True(t, ok)
	require.Len(t, joined, 1)
	require.Equal(t, "o-1", string(joined[0]["o"].ID))
	require.Equal(t, "t-1", string(joined[0]["t"].ID))
}

func TestCartesianWherePushdown_ContradictoryNullConstraint(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	patternMatches := []struct {
		variable string
		nodes    []*storage.Node
	}{
		{variable: "o", nodes: []*storage.Node{
			{ID: "o-1", Properties: map[string]interface{}{"joinKey": "k1"}},
			{ID: "o-2", Properties: map[string]interface{}{}},
		}},
		{variable: "t", nodes: []*storage.Node{
			{ID: "t-1", Properties: map[string]interface{}{"joinKey": "k1"}},
		}},
	}

	filtered := exec.applyCartesianWherePushdown(context.Background(), patternMatches, "o.joinKey IS NULL AND o.joinKey IS NOT NULL AND t.joinKey = o.joinKey")
	require.Len(t, filtered, 2)
	require.Len(t, filtered[0].nodes, 0)
	require.Len(t, filtered[1].nodes, 1)
}

func TestCartesianWherePushdown_SingleNodeComparisons(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	patternMatches := []struct {
		variable string
		nodes    []*storage.Node
	}{
		{variable: "a", nodes: []*storage.Node{
			{ID: "a-1", Properties: map[string]interface{}{"age": int64(20)}},
			{ID: "a-2", Properties: map[string]interface{}{"age": int64(40)}},
		}},
		{variable: "b", nodes: []*storage.Node{
			{ID: "b-1", Properties: map[string]interface{}{"age": int64(20)}},
			{ID: "b-2", Properties: map[string]interface{}{"age": int64(40)}},
		}},
	}

	filtered := exec.applyCartesianWherePushdown(context.Background(), patternMatches, "a.age >= 32 AND b.age < 30")
	require.Len(t, filtered[0].nodes, 1)
	require.Equal(t, storage.NodeID("a-2"), filtered[0].nodes[0].ID)
	require.Len(t, filtered[1].nodes, 1)
	require.Equal(t, storage.NodeID("b-1"), filtered[1].nodes[0].ID)
	require.Len(t, exec.buildCartesianProduct(filtered), 1)

	for _, clause := range []string{"a.age >= b.age", "size(a.age) > 1", "a.age >= $minimum"} {
		if _, ok := parseCartesianSingleNodeComparisonTerm(clause); ok {
			t.Fatalf("unexpected pushdown eligibility for %q", clause)
		}
	}
}

func TestMatchCreate_BatchJoinWithDualInFilters(t *testing.T) {
	// Wrap the in-memory engine in a NamespacedEngine so the executor's
	// transactionStorageWrapper sees a non-empty namespace and prefixes
	// IDs uniformly. Without the wrapper, freshly-minted edge UUIDs would
	// land in BadgerTransaction unprefixed and trip the per-tx namespace
	// pin (see ErrCrossNamespaceTransaction).
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, k := range []string{"k1", "k2", "k3"} {
		_, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID("o-" + k),
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"joinKey": k,
			},
		})
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{
			ID:     storage.NodeID("t-" + k),
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"joinKey": k,
			},
		})
		require.NoError(t, err)
	}

	params := map[string]interface{}{"keys": []interface{}{"k1", "k2"}}
	res, err := exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.joinKey IN $keys
  AND t.joinKey IN $keys
  AND o.joinKey = t.joinKey
  AND NOT (o)-[:TRANSLATES_TO]->(t)
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS created_pairs
`, params)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	require.Equal(t, int64(2), res.Rows[0][0])

	res2, err := exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.joinKey IN $keys
  AND t.joinKey IN $keys
  AND o.joinKey = t.joinKey
  AND NOT (o)-[:TRANSLATES_TO]->(t)
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS created_pairs
`, params)
	require.NoError(t, err)
	require.Len(t, res2.Rows, 1)
	require.Len(t, res2.Rows[0], 1)
	require.Equal(t, int64(0), res2.Rows[0][0])
}

func TestBuildCombinationsUsingWhereJoin_TwoVarEquality(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())

	o1 := &storage.Node{ID: "nornic:o1", Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"joinKey": "k1"}}
	o2 := &storage.Node{ID: "nornic:o2", Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"joinKey": "k2"}}
	t1 := &storage.Node{ID: "nornic:t1", Labels: []string{"TranslatedText"}, Properties: map[string]interface{}{"joinKey": "k1"}}
	t2 := &storage.Node{ID: "nornic:t2", Labels: []string{"TranslatedText"}, Properties: map[string]interface{}{"joinKey": "k2"}}
	t3 := &storage.Node{ID: "nornic:t3", Labels: []string{"TranslatedText"}, Properties: map[string]interface{}{"joinKey": "k3"}}

	patternMatches := []struct {
		variable string
		nodes    []*storage.Node
	}{
		{variable: "o", nodes: []*storage.Node{o1, o2}},
		{variable: "t", nodes: []*storage.Node{t1, t2, t3}},
	}

	joined, ok := exec.buildCombinationsUsingWhereJoin(
		patternMatches,
		"o.joinKey IN ['k1','k2'] AND t.joinKey IN ['k1','k2'] AND o.joinKey = t.joinKey",
	)
	require.True(t, ok)
	require.Len(t, joined, 2)

	got := map[string]string{}
	for _, row := range joined {
		require.Contains(t, row, "o")
		require.Contains(t, row, "t")
		got[string(row["o"].ID)] = string(row["t"].ID)
	}
	require.Equal(t, "nornic:t1", got["nornic:o1"])
	require.Equal(t, "nornic:t2", got["nornic:o2"])
}

func TestBuildCombinationsUsingWhereJoin_ThreeVarEqualityChain(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())

	o1 := &storage.Node{ID: "nornic:o1", Properties: map[string]interface{}{"joinKey": "k1"}}
	o2 := &storage.Node{ID: "nornic:o2", Properties: map[string]interface{}{"joinKey": "k2"}}
	t1 := &storage.Node{ID: "nornic:t1", Properties: map[string]interface{}{"joinKey": "k1"}}
	t2 := &storage.Node{ID: "nornic:t2", Properties: map[string]interface{}{"joinKey": "k2"}}
	a1 := &storage.Node{ID: "nornic:a1", Properties: map[string]interface{}{"joinKey": "k1"}}
	a2 := &storage.Node{ID: "nornic:a2", Properties: map[string]interface{}{"joinKey": "k2"}}
	a3 := &storage.Node{ID: "nornic:a3", Properties: map[string]interface{}{"joinKey": "k3"}}

	patternMatches := []struct {
		variable string
		nodes    []*storage.Node
	}{
		{variable: "o", nodes: []*storage.Node{o1, o2}},
		{variable: "t", nodes: []*storage.Node{t1, t2}},
		{variable: "a", nodes: []*storage.Node{a1, a2, a3}},
	}

	joined, ok := exec.buildCombinationsUsingWhereJoin(
		patternMatches,
		"o.joinKey IN ['k1','k2'] AND t.joinKey = o.joinKey AND a.joinKey = t.joinKey",
	)
	require.True(t, ok)
	require.Len(t, joined, 2)
	for _, row := range joined {
		require.Contains(t, row, "o")
		require.Contains(t, row, "t")
		require.Contains(t, row, "a")
		ok := row["o"].Properties["joinKey"] == row["t"].Properties["joinKey"] &&
			row["t"].Properties["joinKey"] == row["a"].Properties["joinKey"]
		require.True(t, ok)
	}
}
