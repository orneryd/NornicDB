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

// A statement's outcome is decided once, by its durable commit: canceled or
// expired before the commit it fails with that error and stores nothing; once
// its writes are committed it keeps its result. Every frame a statement can
// run through (PROFILE, USE routing, an explicit COMMIT, a transaction
// script, a CASE script) and its completion (the map-order metadata of a map
// result) shares that one outcome.
func TestStatementOutcomeMatrix(t *testing.T) {
	const write = "UNWIND range(1, 3) AS x CREATE (:CommitBoundary {x: x}) RETURN {value: x} AS payload"
	frames := []outcomeFrame{
		{name: "auto_commit", query: write, rows: 3},
		{name: "profile", query: "PROFILE " + write},
		{name: "use", query: "USE nornic " + write, rows: 3, routed: true},
		{name: "profile_use", query: "PROFILE USE nornic " + write, routed: true},
		{name: "explicit_commit", prepare: []string{"BEGIN", write}, query: "COMMIT", before: "precanceled", transactionControl: true},
		{name: "script", query: "BEGIN " + write + " COMMIT", transactionControl: true},
		{name: "case_script", query: "BEGIN CALL { UNWIND range(1, 3) AS x CREATE (n:CommitBoundary {x: x}) RETURN n } CASE WHEN false THEN ROLLBACK ELSE RETURN n COMMIT", before: "executeCaseRollbackTransactionScript", transactionControl: true},
	}
	for _, frame := range frames {
		for _, point := range []string{"before_commit", "at_commit", "after_commit"} {
			if point == "at_commit" && frame.transactionControl {
				// An explicit transaction's commit (COMMIT, a script's) does not
				// observe the statement's context: it completes once started.
				continue
			}
			for _, budget := range []string{"caller", "database"} {
				if budget == "database" && (frame.routed || frame.transactionControl) {
					// A routed statement runs under the target database's limits;
					// transaction control is not under the query time limit.
					continue
				}
				t.Run(frame.name+"/"+point+"/"+budget, func(t *testing.T) {
					runOutcomeMatrixCase(t, frame, point, budget)
				})
			}
		}
	}
}

type outcomeFrame struct {
	name, query string
	prepare     []string
	// before names the frame that first observes the statement before its
	// commit ("" for the clause pipeline), or "precanceled".
	before             string
	rows               int
	routed             bool
	transactionControl bool
}

func runOutcomeMatrixCase(t *testing.T, frame outcomeFrame, point, budget string) {
	fixture := newOutcomeMatrixFixture(t, budget == "database")
	for _, query := range frame.prepare {
		_, err := fixture.executor.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		if fixture.executor.txContext != nil && fixture.executor.txContext.active {
			_, _ = fixture.executor.Execute(context.Background(), "ROLLBACK", nil)
		}
	})
	trigger := ""
	switch point {
	case "before_commit":
		trigger = frame.before
		if trigger == "" {
			trigger = "runPipelineClauseRows"
		}
	case "at_commit":
		trigger = ".CommitContext"
	case "after_commit":
		fixture.armCommitNotification()
	}
	ctx := context.Background()
	if budget == "caller" {
		caller := &outcomePointContext{trigger: trigger}
		caller.Context, caller.cancel = context.WithCancel(context.Background())
		if point == "after_commit" {
			fixture.fire.Store(&caller.cancel)
		}
		if trigger == "precanceled" {
			caller.cancel()
		}
		ctx = caller
	} else {
		fixture.limit.trigger = trigger
	}
	result, err := fixture.executor.Execute(ctx, frame.query, nil)
	stored := fixture.committedNodes(t)
	if point == "after_commit" {
		require.NoError(t, err, "a committed statement keeps its result")
		require.NotNil(t, result)
		if frame.rows > 0 {
			require.Len(t, result.Rows, frame.rows)
		}
		require.Equal(t, int64(3), stored)
		if budget == "caller" {
			require.ErrorIs(t, ctx.Err(), context.Canceled, "the caller canceled after the commit")
		} else {
			require.ErrorIs(t, context.Cause(fixture.limit.limited), nornicerrors.ErrDatabaseQueryTimeLimit, "the database deadline expired after the commit")
		}
		return
	}
	want := context.Canceled
	if budget == "database" {
		want = context.DeadlineExceeded
	}
	require.ErrorIs(t, err, want, "an uncommitted statement fails with its cancellation: %v", err)
	if budget == "database" {
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, nornicerrors.TransactionTimedOut, code)
	}
	require.Nil(t, result)
	require.Zero(t, stored, "nothing the statement wrote is stored")
	if budget == "caller" && trigger != "precanceled" {
		require.True(t, ctx.(*outcomePointContext).fired.Load(), "the cancellation must reach %s", trigger)
	} else if budget == "database" {
		require.True(t, fixture.limit.fired.Load(), "the deadline must expire at %s", trigger)
	}
}

// outcomePointContext cancels the caller's context the first time a frame
// named trigger observes it.
type outcomePointContext struct {
	context.Context
	cancel  context.CancelFunc
	trigger string
	fired   atomic.Bool
}

func (c *outcomePointContext) Err() error {
	if c.trigger != "" && c.trigger != "precanceled" && !c.fired.Load() && stackHasFrame(c.trigger) {
		c.fired.Store(true)
		c.cancel()
	}
	return c.Context.Err()
}

func stackHasFrame(name string) bool {
	var pcs [48]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(3, pcs[:])])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, name) {
			return true
		}
		if !more {
			return false
		}
	}
}

// outcomeLimit is the database MaxQueryTime owner's real WithTimeoutCause,
// with a short limit; the statement waits for it to expire at the trigger.
type outcomeLimit struct {
	trigger string
	limited context.Context
	fired   atomic.Bool
}

func (*outcomeLimit) CheckQueryRate() error       { return nil }
func (*outcomeLimit) GetQueryLimits() interface{} { return nil }
func (l *outcomeLimit) CheckQueryLimits(parent context.Context) (context.Context, context.CancelFunc, error) {
	limited, cancel := context.WithTimeoutCause(parent, time.Second, nornicerrors.ErrDatabaseQueryTimeLimit)
	l.limited = limited
	return &outcomeLimitContext{Context: limited, owner: l}, cancel, nil
}

type outcomeLimitContext struct {
	context.Context
	owner *outcomeLimit
}

func (c *outcomeLimitContext) Err() error {
	if c.owner.trigger != "" && !c.owner.fired.Load() && stackHasFrame(c.owner.trigger) {
		c.owner.fired.Store(true)
		<-c.Done()
	}
	return c.Context.Err()
}

type outcomeLimitEngine struct {
	*storage.NamespacedEngine
	limit *outcomeLimit
}

func (e outcomeLimitEngine) GetInnerEngine() storage.Engine { return e.NamespacedEngine }
func (e outcomeLimitEngine) GetQueryLimitChecker() interface {
	CheckQueryRate() error
	CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
	GetQueryLimits() interface{}
} {
	return e.limit
}

type outcomeMatrixFixture struct {
	executor *StorageExecutor
	observer *StorageExecutor
	limit    *outcomeLimit
	// fire runs from the engine's commit notification: after the commit
	// applied the statement's writes.
	fire  atomic.Pointer[context.CancelFunc]
	armed atomic.Bool
}

func newOutcomeMatrixFixture(t *testing.T, database bool) *outcomeMatrixFixture {
	t.Helper()
	fixture := &outcomeMatrixFixture{}
	base := newTestMemoryEngine(t)
	stores := map[string]storage.Engine{}
	for _, name := range []string{"system", "nornic", "other"} {
		stores[name] = storage.NewNamespacedEngine(base, name)
	}
	nornic := stores["nornic"].(*storage.NamespacedEngine)
	nornic.OnNodeCreated(func(*storage.Node) {
		if !fixture.armed.Load() {
			return
		}
		if database {
			// The deadline expires while the committed statement completes.
			if limited := fixture.limit.limited; limited != nil {
				fixture.limit.fired.Store(true)
				<-limited.Done()
			}
			return
		}
		if cancel := fixture.fire.Load(); cancel != nil {
			(*cancel)()
		}
	})
	var engine storage.Engine = nornic
	if database {
		fixture.limit = &outcomeLimit{}
		engine = outcomeLimitEngine{NamespacedEngine: nornic, limit: fixture.limit}
	}
	fixture.executor = NewStorageExecutorWithQueryCachePolicy(engine, 0, 0)
	fixture.executor.SetDatabaseManager(administrationReviewManager{stores: stores})
	fixture.observer = NewStorageExecutorWithQueryCachePolicy(nornic, 0, 0)
	return fixture
}

func (f *outcomeMatrixFixture) armCommitNotification() { f.armed.Store(true) }

func (f *outcomeMatrixFixture) committedNodes(t *testing.T) int64 {
	t.Helper()
	f.armed.Store(false)
	result, err := f.observer.Execute(context.Background(), "MATCH (n:CommitBoundary) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	return result.Rows[0][0].(int64)
}

// A CASE script canceled while it walks its CALL's rows rolls back the
// private transaction it began: its writes are discarded and the storage
// snapshot it held is released.
func TestCaseScriptCancellationReleasesTransaction(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "case_cancel")
	executor := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	baseline := base.ActiveReaders()
	ctx := &rowConsumerContext{Context: context.Background(), consumer: "executeCaseRollbackTransactionScript", after: 1, done: make(chan struct{})}
	_, err = executor.Execute(ctx, "BEGIN CALL { UNWIND range(1, 4096) AS x CREATE (n:ScriptCancel {x: x}) RETURN n } CASE WHEN false THEN ROLLBACK ELSE RETURN n COMMIT", nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, ctx.checks, "must cancel in the CASE pass over the CALL's rows")
	require.Eventually(t, func() bool { return base.ActiveReaders() == baseline }, 5*time.Second, 10*time.Millisecond, "the script's transaction must be rolled back")
	result, err := executor.Execute(context.Background(), "MATCH (n:ScriptCancel) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), result.Rows[0][0])
	_, err = executor.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err, "no transaction is left open")
	_, err = executor.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)
}
