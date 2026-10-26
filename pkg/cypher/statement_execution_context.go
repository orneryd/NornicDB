// Author: Codex, NOR-180 terminal outcome retention, 29 October 2026.
package cypher

import (
	"context"
	"sync/atomic"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
)

// statementOutcome belongs to the semantic statement, not a registered Execute
// frame or a result object. Dispatch, PROFILE, Fabric and batched subqueries can
// re-enter execution or rebuild results without losing a successful commit.
// Publication is monotonic: a later independent execution error still returns
// that error, but cancellation or accounting during deferred completion must
// not report rollback for writes already stored. No publication occurs for rolled-back, read-only or
// still-open explicit writes. Fabric participants may publish concurrently.
type statementOutcome struct {
	state atomic.Pointer[statementOutcomeState]
}

// A snapshot is immutable after publication. Commit truth and required terminal
// failures share the existing context allocation but remain independent facts.
type statementOutcomeState struct {
	committed bool
	failures  nornicerrors.TerminalOutcomes
}

var committedStatementOutcome = statementOutcomeState{committed: true}

func (o *statementOutcome) committed() bool {
	state := o.state.Load()
	return state != nil && state.committed
}

// record is the only state writer. A terminal failure never publishes a commit,
// and publishing a commit never clears a previously recorded terminal failure.
func (o *statementOutcome) record(committed bool, outcome error) {
	for {
		previous := o.state.Load()
		next := &committedStatementOutcome
		if previous != nil {
			if outcome == nil && previous.committed {
				return
			}
			next = &statementOutcomeState{committed: committed || previous.committed, failures: previous.failures}
		}
		if outcome != nil {
			if previous == nil {
				next = &statementOutcomeState{}
			}
			failures, changed := next.failures.Record(outcome)
			if !changed {
				return
			}
			next.failures = failures
		}
		if o.state.CompareAndSwap(previous, next) {
			return
		}
	}
}

func (o *statementOutcome) hasTerminalFailures() bool {
	state := o.state.Load()
	return state != nil && !state.failures.Empty()
}

func (o *statementOutcome) retainTerminalFailures(primary error) error {
	if state := o.state.Load(); state != nil {
		return state.failures.Retain(primary)
	}
	return primary
}

// Keep the outcome in the existing statement-clock/provider context allocation.
// Transparent routing explicitly shares an outcome; public Execute calls start
// fresh outcomes even if their caller retained an earlier execution context.
type statementExecutionContext struct {
	context.Context
	instant time.Time // Value exposes its stable address without boxing the clock.
	binding *QueryEmbedderBinding
	state   *queryEmbedderState
	outcome *statementOutcome
	owned   statementOutcome
}

func (e *StorageExecutor) newStatementExecutionContext(ctx context.Context, instant time.Time, outcome *statementOutcome) *statementExecutionContext {
	statement := &statementExecutionContext{Context: ctx, instant: instant, binding: e.queryEmbedder}
	statement.outcome = outcome
	if statement.outcome == nil {
		statement.outcome = &statement.owned
	}
	if outer, ok := ctx.Value(queryEmbeddingStatementKey{}).(*statementExecutionContext); ok {
		if outer.binding == e.queryEmbedder {
			statement.state = outer.state
			return statement
		}
	}
	statement.state = e.currentQueryEmbedder()
	return statement
}

func (c *statementExecutionContext) Value(key any) any {
	switch key.(type) {
	case temporalStatementTimeKey:
		return &c.instant
	case queryEmbeddingStatementKey:
		return c
	}
	return c.Context.Value(key)
}

// Only successful commit/direct-effect owners publish this outcome. In
// particular, a write-shaped query, a returned result or a batch being started
// does not establish that its writes committed.
func publishStatementCommit(ctx context.Context) {
	if statement, ok := ctx.Value(queryEmbeddingStatementKey{}).(*statementExecutionContext); ok {
		statement.outcome.record(true, nil)
	}
}

// A direct execution has no pending transaction: its completed mutations are
// already applied. Use the shared permission classifier, whose literal-aware
// scan and procedure contracts distinguish writes from reads containing words
// such as 'CREATE'. The conservative analyzer flags alone cannot establish that.
func (e *StorageExecutor) publishDirectStatementCommit(ctx context.Context, query string) {
	permissions := QueryPermissionRequirements(context.Background(), query, AcceptedSource)
	if permissions.Write || permissions.Schema {
		publishStatementCommit(ctx)
	}
}
