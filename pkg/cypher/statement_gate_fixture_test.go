package cypher

// The statement gate is a registered expression function tests use to interleave or
// cancel a statement at the point the query itself names. It is a test fixture,
// not a production seam: the query calls it like any other scalar function, so
// the pause lands in the statement's own row pipeline — after the reads and
// writes the query already performed and before the ones after it — inside the
// statement's real transaction.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// statementGateFunction is the gate's public query name. It avoids the
// __nornic_ prefix, which the statement pre-pass rejects as internal.
const statementGateFunction = "nornicdb.testgate"

// statementGateHook is what a case runs when its statement evaluates the gate.
// It receives the statement's own context.
type statementGateHook func(workCtx context.Context) error

// statementGate counts its evaluations and, at the evaluation its case names,
// runs the case's hook at the selected evaluation.
type statementGate struct {
	calls   atomic.Int64
	at      int64
	onFirst statementGateHook

	mu  sync.Mutex
	err error
}

// statementGateSlot publishes the gate a running case owns. The function
// registry has no unregister, so the registered dispatcher below keeps no
// gate: a case stores its gate here and clears the slot in cleanup, leaving
// the process-wide registration fixture-free.
var statementGateSlot atomic.Pointer[statementGate]

func init() {
	cypherfn.Register(statementGateFunction, func(workCtx context.Context, call cypherfn.Context, args []string) (interface{}, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("%s() takes one argument", statementGateFunction)
		}
		value, err := call.Eval(args[0])
		if err != nil {
			return nil, err
		}
		gate := statementGateSlot.Load()
		if gate == nil {
			return nil, fmt.Errorf("%s() has no active gate", statementGateFunction)
		}
		return gate.call(workCtx, value), nil
	}, cypherfn.OpaqueReads)
}

// newStatementGate returns a gate that runs onFirst when the statement first
// evaluates it and returns its input value.
func newStatementGate(onFirst statementGateHook) *statementGate {
	return newStatementGateAt(1, onFirst)
}

// newStatementGateAt returns a gate that runs hook at the statement's at-th
// evaluation (counting from 1). A case gates a later evaluation to act after the statement has
// already performed the earlier rows' work.
func newStatementGateAt(at int64, hook statementGateHook) *statementGate {
	if at < 1 {
		panic("newStatementGateAt: at must be at least 1")
	}
	return &statementGate{
		at:      at,
		onFirst: hook,
	}
}

// install publishes the gate for the calling case and clears it afterwards.
func (g *statementGate) install(t *testing.T) *statementGate {
	t.Helper()
	// The unknown-function check reads the catalog's names; the gate is one
	// for the case that installs it, and only for it.
	builtInCypherFunctions[statementGateFunction] = struct{}{}
	require.True(t, statementGateSlot.CompareAndSwap(nil, g), "statement gates require an exclusive serial test scope")
	t.Cleanup(func() {
		statementGateSlot.CompareAndSwap(g, nil)
		delete(builtInCypherFunctions, statementGateFunction)
	})
	return g
}

// Calls is how often the statement evaluated the gate.
func (g *statementGate) Calls() int64 { return g.calls.Load() }

// Err is the hook error, if the hook failed.
func (g *statementGate) Err() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.err
}

func (g *statementGate) call(workCtx context.Context, value interface{}) interface{} {
	if g.calls.Add(1) != g.at {
		return value
	}
	if g.onFirst != nil {
		if err := g.onFirst(workCtx); err != nil {
			g.mu.Lock()
			g.err = err
			g.mu.Unlock()
		}
	}
	return value
}

// countRows reads exec's durable state for one label.
func countRows(t *testing.T, exec *StorageExecutor, label string) [][]interface{} {
	t.Helper()
	result, err := exec.Execute(context.Background(), "MATCH (n:"+label+") RETURN count(n)", nil)
	require.NoError(t, err)
	return result.Rows
}

// stagedNodesInStatement reads, through the statement's own transaction, the
// nodes of label the statement has staged and not yet committed. A case calls
// it from its gate hook, before it cancels, to establish what the statement had
// actually written when the cancellation arrived.
func stagedNodesInStatement(workCtx context.Context, label string) ([]*storage.Node, error) {
	wrapper, ok := workCtx.Value(ctxKeyTxStorage).(*transactionStorageWrapper)
	if !ok || wrapper == nil {
		return nil, fmt.Errorf("%s() ran outside the statement's transaction", statementGateFunction)
	}
	if err := workCtx.Err(); err != nil {
		return nil, fmt.Errorf("%s() observed a canceled statement context: %w", statementGateFunction, err)
	}
	return wrapper.tx.GetNodesByLabel(workCtx, label)
}

// stagedWriteBeforeCancel establishes, from inside a gate hook, what the
// statement had actually written when the cancellation reached it: want nodes of
// label are staged in the statement's own transaction and none of them is
// committed. A case calls it before it cancels, and asserts the staged nodes'
// values afterwards.
func stagedWriteBeforeCancel(workCtx context.Context, committed *StorageExecutor, label string, want int) ([]*storage.Node, error) {
	staged, err := stagedNodesInStatement(workCtx, label)
	if err != nil {
		return nil, err
	}
	if len(staged) != want {
		return nil, fmt.Errorf("the statement had staged %d %s nodes before the cancellation, want %d", len(staged), label, want)
	}
	result, err := committed.Execute(context.Background(), "MATCH (n:"+label+") RETURN count(n)", nil)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		return nil, fmt.Errorf("committed read of %s returned %d rows", label, len(result.Rows))
	}
	count, ok := result.Rows[0][0].(int64)
	if !ok {
		return nil, fmt.Errorf("committed read of %s returned %T, want int64", label, result.Rows[0][0])
	}
	if count != 0 {
		return nil, fmt.Errorf("committed storage already held %d of the statement's %s nodes before the cancellation", count, label)
	}
	return staged, nil
}
