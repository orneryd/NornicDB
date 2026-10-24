package fabric

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A worker's panic is carried to the goroutine that waits for the workers:
// the first one is re-panicked there, each one stops the other workers, and
// a relay whose workers didn't panic rethrows nothing.
func TestPanicRelayCarriesTheFirstWorkerPanic(t *testing.T) {
	var stopped atomic.Int32
	relay := &panicRelay{stop: func() { stopped.Add(1) }}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer relay.capture()
			panic(fmt.Sprint("worker ", i))
		}(i)
	}
	wg.Wait()
	require.True(t, relay.panicked())
	require.EqualValues(t, 2, stopped.Load(), "each panic stops the other workers")
	require.Panics(t, relay.rethrow)

	quiet := &panicRelay{}
	require.NotPanics(t, quiet.rethrow)
	require.False(t, quiet.panicked())
}

// panickingCypherExecutor panics on its panicOn query and answers the others
// as its inner executor does.
type panickingCypherExecutor struct {
	inner   *mockCypherExecutor
	panicOn string
}

func (p *panickingCypherExecutor) ExecuteQuery(ctx context.Context, dbName string, engine storage.Engine, query string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	if query == p.panicOn {
		panic("injected fragment panic")
	}
	return p.inner.ExecuteQuery(ctx, dbName, engine, query, params)
}

func (p *panickingCypherExecutor) ExecuteQueryWithRecord(ctx context.Context, dbName string, engine storage.Engine, query string, params map[string]interface{}, record map[string]interface{}) ([]string, [][]interface{}, error) {
	return p.ExecuteQuery(ctx, dbName, engine, query, params)
}

// A panic in a UNION branch run on its own goroutine reaches the caller of
// the UNION, which ends the statement, instead of ending the process; the
// other branch's rows are closed.
func TestUnionBranchPanicReachesTheStatement(t *testing.T) {
	for _, branch := range []string{"lhs", "rhs"} {
		t.Run(branch, func(t *testing.T) {
			panicOn := "RETURN 'a' AS v"
			if branch == "rhs" {
				panicOn = "RETURN 'b' AS v"
			}
			executor := &panickingCypherExecutor{panicOn: panicOn, inner: &mockCypherExecutor{results: map[string]*ResultStream{
				"RETURN 'a' AS v": {Columns: []string{"v"}, Rows: [][]interface{}{{"a"}}},
				"RETURN 'b' AS v": {Columns: []string{"v"}, Rows: [][]interface{}{{"b"}}},
			}}}
			catalog := NewCatalog()
			catalog.Register("db1", &LocationLocal{DBName: "db1"})
			exec := NewFabricExecutor(catalog, newTestLocalExecutorFor(executor), nil)
			withAccessMode(t, func(string, bool) bool { return false })
			union := &FragmentUnion{
				LHS:     &FragmentExec{GraphName: "db1", Query: "RETURN 'a' AS v"},
				RHS:     &FragmentExec{GraphName: "db1", Query: "RETURN 'b' AS v"},
				Columns: []string{"v"},
			}
			require.PanicsWithValue(t, "injected fragment panic", func() {
				_, _, _ = exec.executeUnionRows(context.Background(), nil, union, nil, "")
			})
			require.PanicsWithValue(t, "injected fragment panic", func() {
				_, _ = exec.executeUnionParallel(context.Background(), nil, union, nil, "")
			})
		})
	}
}

// A storage operation's statement on a remote constituent runs in the
// statement's remote session for it (RunInStatement): a read in a read
// session that claims nothing, a write in the write session after claiming
// the remote, a later read in the write session, and a write to a second
// remote is refused before it reaches it. Committing the statement ends the
// session.
func TestRemoteStatementOperationsRunInTheStatementSession(t *testing.T) {
	var mu sync.Mutex
	var statements []string
	engine := storage.NewStatementRemoteEngine(func(_ context.Context, statement string, _ map[string]interface{}) ([]string, [][]interface{}, error) {
		mu.Lock()
		defer mu.Unlock()
		statements = append(statements, statement)
		return []string{"c"}, [][]interface{}{{int64(1)}}, nil
	})
	remote := NewRemoteFragmentExecutor()
	defer func() { _ = remote.Close() }()
	withAccessMode(t, func(query string, _ bool) bool { return strings.HasPrefix(query, "CREATE") })
	tx := NewFabricTransaction("remote-statement")
	ctx := context.Background()

	_, _, err := remote.RunInStatement(ctx, tx, "remote:far", engine, ctx, "MATCH (n) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Empty(t, tx.WriteShard(), "a read claims nothing")
	_, _, err = remote.RunInStatement(ctx, tx, "remote:far", engine, ctx, "CREATE (n)", nil)
	require.NoError(t, err)
	require.Equal(t, "remote:far", tx.WriteShard(), "a write claims the remote")
	_, _, err = remote.RunInStatement(ctx, tx, "remote:far", engine, ctx, "MATCH (n) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	_, _, err = remote.RunInStatement(ctx, tx, "remote:other", engine, ctx, "CREATE (n)", nil)
	require.ErrorIs(t, err, ErrSecondWriteShard)
	mu.Lock()
	require.Len(t, statements, 3, "the refused write never reaches the remote")
	mu.Unlock()
	require.NoError(t, tx.Commit(nil, nil))
}
