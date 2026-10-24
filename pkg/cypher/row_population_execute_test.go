package cypher

import (
	"context"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The inventory's adapter, validation, union and completion populations are
// exercised through Execute. The context expires only inside the consumer,
// so a producer's check cannot accidentally satisfy this regression.
func TestMaterializedPopulationExecuteCancellation(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "custom.population_rows", Mode: ProcedureModeRead, Returns: []ProcedureColumn{{Name: "x", Type: "INTEGER"}}}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		rows := make([][]interface{}, 4096)
		for i := range rows {
			rows[i] = []interface{}{int64(i + 1)}
		}
		return &ExecuteResult{Columns: []string{"x"}, Rows: rows}, nil
	}))
	const input = "UNWIND range(1,$n) AS x WITH x ORDER BY x "
	cases := []rowConsumerCase{
		{"bound_pattern_validation", input + "OPTIONAL MATCH (m:AbsentRowCancel) RETURN x", "validateBoundPatternNodes", 2},
		{"deleted_view_projection", "UNWIND range(1,$n) AS x MATCH (d:PopulationVictim) DELETE d RETURN x", "replaceDeletedEntityViews", 2},
		{"deleted_read_validation", "MATCH(d:PopulationVictim) DELETE d WITH 1 AS marker UNWIND range(1,$n) AS x WITH DISTINCT x RETURN x", "validateDeletedEntityReads", 2},
		{"procedure_projection", "UNWIND [0] AS marker CALL custom.population_rows() YIELD x RETURN x", "pipelineApplyProcedureCall", 3},
		{"union_distinct", "UNWIND range(1,$n) AS x RETURN x UNION RETURN 1 AS x", "appendDistinctUnionRows", 2},
		{"union_column_adaptation", "RETURN 1 AS a,2 AS b UNION ALL UNWIND range(1,$n) AS x RETURN x AS b,x AS a", "reorderUnionRows", 2},
		{"quoted_final_projection", "UNWIND range(1,$n) AS `a b` WITH `a b` ORDER BY `a b` RETURN *", "sortColumnsByName", 2},
		{"result_entity_walk", input + "RETURN x", "materializedRows", 2},
		{"result_nested_walk", "RETURN $nested AS payload", "materializedValue", 2},
	}
	for _, tc := range cases {
		for _, deadline := range []bool{false, true} {
			mode := "cancel"
			if deadline {
				mode = "deadline"
			}
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				executor := rowCancellationFixture(t, 1)
				if strings.HasPrefix(tc.name, "deleted_") {
					_, err := executor.Execute(context.Background(), "CREATE (:PopulationVictim)", nil)
					require.NoError(t, err)
				}
				nested := make([]interface{}, 4096)
				for i := range nested {
					nested[i] = int64(i)
				}
				parameters := map[string]interface{}{"n": int64(4096), "nested": nested}
				ctx := &rowConsumerContext{Context: context.Background(), consumer: tc.consumer, deadline: deadline, after: tc.after, done: make(chan struct{})}
				_, err := executor.Execute(ctx, tc.query, parameters)
				want := context.Canceled
				if deadline {
					want = context.DeadlineExceeded
				}
				require.ErrorIs(t, err, want, "Execute must stop this population and must never report success")
				require.Equal(t, tc.after, ctx.checks, "cancellation must occur in the inventoried consumer")
				require.False(t, ctx.triggered.IsZero())
				require.Less(t, time.Since(ctx.triggered), time.Second)
			})
		}
	}
}

// This checker installs the same real WithTimeoutCause used by the database
// MaxQueryTime owner. Waiting at the metadata read makes expiry deterministic
// without sleeping in engine code or racing the query's producer.
type populationCompletionLimit struct {
	limit   time.Duration
	limited context.Context
	entered atomic.Bool
}

func (*populationCompletionLimit) CheckQueryRate() error       { return nil }
func (*populationCompletionLimit) GetQueryLimits() interface{} { return nil }
func (l *populationCompletionLimit) CheckQueryLimits(parent context.Context) (context.Context, context.CancelFunc, error) {
	limited, cancel := context.WithTimeoutCause(parent, l.limit, nornicerrors.ErrDatabaseQueryTimeLimit)
	l.limited = limited
	return &populationCompletionDeadline{Context: limited, owner: l}, cancel, nil
}

type populationCompletionDeadline struct {
	context.Context
	owner *populationCompletionLimit
}

func (c *populationCompletionDeadline) Err() error {
	var pcs [40]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "snapshotMapKeyOrders") {
			c.owner.entered.Store(true)
			<-c.Done()
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}

type populationCompletionEngine struct {
	*storage.NamespacedEngine
	limit *populationCompletionLimit
}

func (e populationCompletionEngine) GetInnerEngine() storage.Engine { return e.NamespacedEngine }
func (e populationCompletionEngine) GetQueryLimitChecker() interface {
	CheckQueryRate() error
	CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
	GetQueryLimits() interface{}
} {
	return e.limit
}

func TestMaterializedPopulationExplicitTransactionDatabaseDeadline(t *testing.T) {
	limit := &populationCompletionLimit{limit: 2 * time.Second}
	engine := populationCompletionEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "population_deadline"), limit: limit}
	executor := NewStorageExecutorWithQueryCachePolicy(engine, 0, 0)
	parent := context.Background()
	_, err := executor.Execute(parent, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = executor.Execute(context.Background(), "ROLLBACK", nil) })
	started := time.Now()
	_, err = executor.Execute(parent, "UNWIND range(1,$n) AS x RETURN {value:x}", map[string]interface{}{"n": int64(4096)})
	require.ErrorIs(t, err, context.DeadlineExceeded, "deferred explicit-transaction completion is inside the database budget")
	require.NoError(t, parent.Err(), "the caller has no deadline")
	require.True(t, limit.entered.Load(), "the database deadline must expire inside metadata completion")
	require.ErrorIs(t, context.Cause(limit.limited), nornicerrors.ErrDatabaseQueryTimeLimit)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, nornicerrors.TransactionTimedOut, code)
	require.Less(t, time.Since(started), limit.limit+time.Second)
}
