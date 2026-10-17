package cypher

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// generousLimitChecker admits every statement under a database time limit
// that never expires during a test; its cleanup cancels the limited context.
type generousLimitChecker struct{ released atomic.Int64 }

func (checker *generousLimitChecker) CheckQueryRate() error       { return nil }
func (checker *generousLimitChecker) GetQueryLimits() interface{} { return nil }
func (checker *generousLimitChecker) CheckQueryLimits(ctx context.Context) (context.Context, context.CancelFunc, error) {
	limited, cancel := context.WithTimeout(ctx, time.Hour)
	return limited, func() { cancel(); checker.released.Add(1) }, nil
}

type generousLimitedEngine struct {
	*storage.NamespacedEngine
	checker *generousLimitChecker
}

func (engine generousLimitedEngine) GetInnerEngine() storage.Engine {
	return engine.NamespacedEngine
}
func (engine generousLimitedEngine) GetQueryLimitChecker() interface {
	CheckQueryRate() error
	CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
	GetQueryLimits() interface{}
} {
	return engine.checker
}

// Admission's cleanup cancels the admitted context as the statement ends; an
// execution error the statement returned is its outcome, not that
// cancellation (sol-r1 #1391 P1, executor_entry.go admission cleanup).
func TestAdmissionCleanupKeepsTheStatementError(t *testing.T) {
	checker := &generousLimitChecker{}
	executor := NewStorageExecutor(generousLimitedEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "admission_cleanup"), checker: checker})
	ctx := context.Background()
	_, err := executor.Execute(ctx, "CREATE CONSTRAINT admission_unique IF NOT EXISTS FOR (n:AdmissionUnique) REQUIRE n.k IS UNIQUE", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "CREATE (:AdmissionUnique {k: 1})", nil)
	require.NoError(t, err)

	for _, query := range []string{
		"UNWIND [1, 0] AS x RETURN 1 / x AS y",
		"CREATE (:AdmissionUnique {k: 1})",
	} {
		t.Run(query, func(t *testing.T) {
			released := checker.released.Load()
			_, err := executor.Execute(ctx, query, nil)
			require.Error(t, err)
			require.NotErrorIs(t, err, context.Canceled, "admission cleanup is not the statement's cancellation: %v", err)
			require.NotErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, released+1, checker.released.Load(), "the admitted statement releases its admission")
		})
	}
	result, err := executor.Execute(ctx, "RETURN 1 AS one", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"one"}, result.Columns)
}

// A statement whose deadline ends once its commit is durable keeps its
// result, with its quoted names restored in full: the deferred restoration
// does not introduce the deadline's error (sol-r1 #1391 P1, quoted_variables
// restore after commit).
func TestCommittedStatementRestoresQuotedNamesAfterItsDeadline(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "committed_restore"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var once sync.Once
	hook := func(context.Context) { once.Do(cancel) }
	statementCommittedHook.Store(&hook)
	defer statementCommittedHook.Store(nil)

	result, err := executor.Execute(ctx, "CREATE (`odd name`:CommittedRestore {v: 1}) RETURN `odd name`.v AS `odd value`", nil)
	statementCommittedHook.Store(nil)
	require.Error(t, ctx.Err(), "the caller's context ended inside the commit")
	require.NoError(t, err, "a durable write is not reported as failed")
	require.NotNil(t, result)
	require.Equal(t, []string{"odd value"}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	stored, err := executor.Execute(context.Background(), "MATCH (n:CommittedRestore) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), stored.Rows[0][0], "the write is durable")
}

// A worker's recovered failure reaches the probe the statement was admitted
// with, not only the root above it (sol-r1 #1391 P2, recoverWorkerPanic).
func TestRecoveredWorkerFailureReachesTheAdmittedProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	preparation := withPipelineCancellation(ctx)
	admittedContext, admittedCancel := context.WithTimeout(preparation, time.Hour)
	defer admittedCancel()
	admitted := admitStatementProbe(admittedContext)
	require.NotSame(t, contextPipelineProbe(preparation), contextPipelineProbe(admitted))
	require.Same(t, contextPipelineProbe(preparation), contextPipelineProbe(admitted).parent)

	worker := pipelineWorkerContext(admitted)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer recoverWorkerPanic(worker)
		panic("worker failure")
	}()
	<-done
	require.NoError(t, admitted.Err(), "the statement is live")
	for name, owner := range map[string]context.Context{"admitted": admitted, "preparation": preparation} {
		err := statementCancellationErr(owner)
		require.Error(t, err, name)
		require.Contains(t, err.Error(), "worker failure", name)
	}
	got, err := statementDeadlineOutcome(admitted, &ExecuteResult{Columns: []string{"x"}}, nil)
	require.Nil(t, got, "a result missing a worker's rows is not returned")
	require.ErrorContains(t, err, "worker failure")
}

// pollCountingContext counts how often a probe samples it and ends after a
// given number of samples.
type pollCountingContext struct {
	context.Context
	polls, cancelAfter int
}

func (ctx *pollCountingContext) Err() error {
	ctx.polls++
	if ctx.cancelAfter > 0 && ctx.polls > ctx.cancelAfter {
		return context.Canceled
	}
	return nil
}

func (ctx *pollCountingContext) Done() <-chan struct{} { return make(chan struct{}) }

// A large string's native hash scans all of it: building a membership or
// equivalence index over many large strings samples the deadline in
// proportion to the bytes hashed, and stops part-way when it ends (sol-r1
// #1391 P2, predicate_helpers.go large-string exclusion).
func TestLargeStringIndexesChargeTheirBytes(t *testing.T) {
	large := strings.Repeat("x", 64<<10)
	items := make([]interface{}, 1024)
	for i := range items {
		items[i] = large[:len(large)-i%2]
	}
	perItem := len(large) / pipelineTextCostUnit
	minimumPolls := len(items) * perItem / pipelineCancellationInterval

	build := map[string]func(*pipelineCancellation) bool{
		"membership": func(probe *pipelineCancellation) bool {
			set, _ := buildComparableMembershipIndexWithCancellation(items, probe)
			return set != nil
		},
		"equivalence": func(probe *pipelineCancellation) bool {
			index := cypherValueIndex{probe: probe}
			for _, item := range items {
				index.add(item)
				if index.cancelled() {
					return false
				}
			}
			return true
		},
	}
	for name, run := range build {
		t.Run(name+"/live", func(t *testing.T) {
			ctx := &pollCountingContext{Context: context.Background()}
			require.True(t, run(&pipelineCancellation{ctx: ctx}))
			require.GreaterOrEqual(t, ctx.polls, minimumPolls, "the deadline is sampled per bytes hashed, not per item")
		})
		t.Run(name+"/cancelled during the build", func(t *testing.T) {
			ctx := &pollCountingContext{Context: context.Background(), cancelAfter: 3}
			require.False(t, run(&pipelineCancellation{ctx: ctx}))
			require.Equal(t, 4, ctx.polls, "the build stops at the first sample after the deadline")
		})
	}
	require.True(t, isComparableValue(large), "a large string keeps its native index; its cost is charged")
}
