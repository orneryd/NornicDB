package cypher

import (
	"context"
	"sync/atomic"
	"time"
)

// statementOutcome belongs to the semantic statement, not a registered Execute
// frame or a result object. Dispatch, PROFILE, Fabric and batched subqueries can
// re-enter execution or rebuild results without losing a successful commit.
// Publication is monotonic: a later independent execution error still returns
// that error, but cancellation or accounting during deferred completion must
// not report rollback for writes already stored. No publication occurs for rolled-back, read-only or
// still-open explicit writes. Fabric participants may publish concurrently.
type statementOutcome struct {
	committed atomic.Bool
}

// Keep the outcome in the existing statement-clock/provider context allocation.
// Transparent routing explicitly shares an outcome; public Execute calls start
// fresh outcomes even if their caller retained an earlier execution context.
type statementExecutionContext struct {
	context.Context
	instant any // Box the clock once, rather than on each Value call.
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
		return c.instant
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
		statement.outcome.committed.Store(true)
	}
}

// A direct execution has no pending transaction: its completed mutations are
// already applied. Use the shared permission classifier, whose literal-aware
// scan and procedure contracts distinguish writes from reads containing words
// such as 'CREATE'. The conservative analyzer flags alone cannot establish that.
func (e *StorageExecutor) publishDirectStatementCommit(ctx context.Context, query string) {
	permissions := QueryPermissionRequirements(query)
	if permissions.Write || permissions.Schema {
		publishStatementCommit(ctx)
	}
}
