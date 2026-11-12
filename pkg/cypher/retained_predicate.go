package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/queryprofile"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// A retained predicate is compiled in one statement and evaluated by searches
// that can run after that statement has ended: db.retrieve's where, which every
// ranked continuation page checks again in a statement of its own.
//
// It keeps the compiling statement's values: its parameters, the calling row
// (with any correlated Fabric record), its bound statement and clause plans,
// its database and the caller's permissions, so every page applies it as the
// first page does. It runs in the lifetime and the execution of the search
// that asks: that search's cancellation and deadline, the running statement,
// transaction and memory scope it belongs to, and that statement's executor,
// so its graph reads see that statement's storage (its transaction while one
// is open). So the ended statement can neither stop the predicate, nor
// receive its errors, nor lend it a transaction that has ended.
//
// An evaluation that is cancelled or fails admits nothing and returns the
// error: a candidate the predicate could not decide is never returned.

// retrievalExecutorKey carries the executor of the statement running a
// retrieval (runSearchRequest), the executor a retained predicate evaluates
// through for that statement's searches.
type retrievalExecutorKey struct{}

func withRetrievalExecutor(ctx context.Context, e *StorageExecutor) context.Context {
	return context.WithValue(ctx, retrievalExecutorKey{}, e)
}

// retainedEvaluator is the executor a retained predicate evaluates through for
// the asking search: the executor of the statement that asks, or, for a page
// pulled outside a Cypher statement, committed, the compiling executor's
// view of committed storage.
func retainedEvaluator(asking context.Context, committed *StorageExecutor) *StorageExecutor {
	if executor, ok := asking.Value(retrievalExecutorKey{}).(*StorageExecutor); ok && executor != nil {
		return executor
	}
	return committed
}

// committedView is e reading committed storage: e itself, unless e is bound to
// a transaction's storage, when it is a clone over the storage under it that
// owns nothing of the transaction: not its context, nor its node lookup cache,
// which can map nodes only the transaction has.
func (e *StorageExecutor) committedView() *StorageExecutor {
	wrapper, ok := e.storage.(*transactionStorageWrapper)
	if !ok {
		return e
	}
	committed := e.cloneWithStorage(wrapper.underlying)
	committed.txContext = nil
	committed.nodeLookupCache = make(map[string]*storage.Node)
	committed.nodeLookupCacheMu = &sync.RWMutex{}
	return committed
}

// withRetainedBindings is ctx with e's correlated Fabric record bindings added
// to its value scope (a name the scope binds keeps its value), so a retained
// predicate keeps them whichever executor evaluates it.
func (e *StorageExecutor) withRetainedBindings(ctx context.Context) context.Context {
	if len(e.fabricRecordBindings) == 0 {
		return ctx
	}
	scope := valueBindingsLayer(ctx, len(e.fabricRecordBindings))
	for name, value := range e.fabricRecordBindings {
		if _, bound := scope[name]; !bound {
			scope[name] = value
		}
	}
	return withValueBindings(ctx, scope)
}

// retainedPredicateContext is the context of one evaluation of a retained
// predicate: the asking search's lifetime, the compiling statement's values,
// and a slot for the evaluation's own errors.
type retainedPredicateContext struct {
	context.Context // the asking search
	compiling       context.Context
	failure         expressionFailure
}

func newRetainedPredicateContext(asking, compiling context.Context) *retainedPredicateContext {
	return &retainedPredicateContext{Context: asking, compiling: compiling}
}

// Value reads the statement lifetime from the asking search and everything
// else from the compiling statement.
func (c *retainedPredicateContext) Value(key any) any {
	switch key.(type) {
	case expressionOperandFailureKey:
		return &c.failure
	case statementContextKey, ctxKeyRunningTransaction, memoryScopeKey, expressionFailureKey,
		rowFallbackExpressionKey, ctxKeyTxStorageType, compositeStatementKey, retrievalExecutorKey,
		queryprofile.ObserverKey, profileRuntimeFrameKey:
		return c.Context.Value(key)
	}
	return c.compiling.Value(key)
}

// outcome is the evaluation's answer: holds, unless the asking search was
// cancelled or the evaluation recorded an error, which is then the answer.
func (c *retainedPredicateContext) outcome(holds bool) (bool, error) {
	if err := c.Context.Err(); err != nil {
		return false, err
	}
	if err := getExpressionFailure(c); err != nil {
		return false, err
	}
	return holds, nil
}

// compileRetainedWhere compiles a retrieval's where in the statement ctx, as
// a retained predicate: for node candidates, bound as node, and with
// relationships also for relationship candidates (relationshipWherePredicate).
// One predicate serves both kinds then: a node candidate reads relationship,
// start and end as null, a relationship candidate reads node as null.
func (e *StorageExecutor) compileRetainedWhere(ctx context.Context, where string, relationships bool) (
	node func(context.Context, *storage.Node) (bool, error),
	relationship func(context.Context, *storage.Edge, *storage.Node, *storage.Node) (bool, error),
) {
	compiling := withStatementClausePlans(e.withRetainedBindings(ctx), where)
	committed := e.committedView()
	nodeCompiling := compiling
	if relationships {
		scope := valueBindingsLayer(compiling, 3)
		scope["relationship"], scope["start"], scope["end"] = nil, nil, nil
		nodeCompiling = withValueBindings(compiling, scope)
		relationship = func(asking context.Context, edge *storage.Edge, start, end *storage.Node) (bool, error) {
			evaluation := newRetainedPredicateContext(asking, compiling)
			return evaluation.outcome(retainedEvaluator(asking, committed).relationshipWherePredicate(evaluation, where, edge, start, end))
		}
	}
	predicate := e.compileNodeWherePredicate(nodeCompiling, "node", where)
	node = func(asking context.Context, candidate *storage.Node) (bool, error) {
		evaluation := newRetainedPredicateContext(asking, nodeCompiling)
		return evaluation.outcome(predicate.holds(retainedEvaluator(asking, committed), evaluation, candidate))
	}
	return node, relationship
}
