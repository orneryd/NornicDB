package cypher

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTransactionScriptRunsOnPrivateExecutor pins the Finding-1 fix: a
// one-statement transaction script (BEGIN … COMMIT/ROLLBACK) opens its
// transaction on a private executor, so the shared per-database executor is
// never left inside a transaction. Previously the script mutated the shared
// executor's txContext, and concurrent auto-commit statements collided with
// it (acknowledged-write loss, mid-run panics).
func TestTransactionScriptRunsOnPrivateExecutor(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := WithClientStatement(context.Background())

	_, err := exec.Execute(ctx, "BEGIN CREATE (:ScriptProbe {v: 1}) COMMIT", nil)
	require.NoError(t, err)
	require.Nil(t, exec.txContext, "the shared executor must not be left inside a transaction")

	_, err = exec.Execute(ctx, "BEGIN MATCH (n:ScriptProbe) RETURN count(n) AS c ROLLBACK", nil)
	require.NoError(t, err)
	require.Nil(t, exec.txContext)

	res, err := exec.Execute(context.Background(), "MATCH (n:ScriptProbe) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, res.Rows)
}

// TestTransactionScriptIsolatedFromConcurrentClients drives the reviewer's
// reproduction shape: a script loop racing plain auto-commit writes on one
// shared executor. Every acknowledged CREATE must be stored and the process
// must not panic; the final count equals the number of successful creates.
func TestTransactionScriptIsolatedFromConcurrentClients(t *testing.T) {
	exec, _ := newTestExecutor(t)
	baseCtx := context.Background()

	var wg sync.WaitGroup
	var created atomic.Int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 150; i++ {
			_, err := exec.Execute(WithClientStatement(baseCtx), "BEGIN MATCH (n:ZZNothing) RETURN count(n) ROLLBACK", nil)
			if err != nil {
				t.Errorf("script iteration %d: %v", i, err)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			result, err := exec.Execute(WithClientStatement(baseCtx), "CREATE (:ZZVictim) RETURN 1 AS ok", nil)
			if err != nil {
				t.Errorf("victim create %d: %v", i, err)
				return
			}
			require.Equal(t, 1, len(result.Rows), "victim create %d", i)
			created.Add(1)
		}
	}()

	wg.Wait()
	require.Nil(t, exec.txContext)

	count, err := exec.Execute(baseCtx, "MATCH (n:ZZVictim) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{created.Load()}}, count.Rows)
}
