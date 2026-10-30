package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// bareBeginWriteShapes are write statements that, in auto-commit, each take a
// different route: the transaction-less pure CREATE path, the implicit
// transaction, and the compound MATCH...CREATE path. After a bare BEGIN all of
// them must run in the transaction that BEGIN opened, not in a nested one.
var bareBeginWriteShapes = []struct {
	name  string
	query string
}{
	{"pure_create", "CREATE (:BareTx {k: 'new'})"},
	{"create_with_relationship", "CREATE (:BareTx {k: 'new'})-[:R]->(:BareTx {k: 'new2'})"},
	{"merge", "MERGE (:BareTx {k: 'new'})"},
	{"match_set", "MATCH (s:Seed) SET s.touched = true"},
	{"match_create", "MATCH (s:Seed) CREATE (s)-[:R]->(:BareTx {k: 'new'})"},
	{"unwind_create", "UNWIND [1, 2, 3] AS i CREATE (:BareTx {k: i})"},
}

func bareBeginCount(t *testing.T, exec *StorageExecutor, query string) int64 {
	t.Helper()
	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	n, ok := res.Rows[0][0].(int64)
	require.True(t, ok, "count is %T", res.Rows[0][0])
	return n
}

const (
	bareBeginNewNodes = "MATCH (n:BareTx) RETURN count(n)"
	bareBeginTouched  = "MATCH (s:Seed) WHERE s.touched = true RETURN count(s)"
)

// A bare BEGIN followed by separate statements: every statement runs in the
// one transaction BEGIN opened. Nothing is visible to another session before
// COMMIT, the transaction object is the same before and after the statement,
// and ROLLBACK discards everything.
func TestBareBegin_StatementsRunInTheTransactionBeginOpened(t *testing.T) {
	for _, shape := range bareBeginWriteShapes {
		for _, end := range []string{"ROLLBACK", "COMMIT"} {
			t.Run(shape.name+"/"+end, func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "bare_begin")
				session := NewStorageExecutor(store)
				other := NewStorageExecutor(store)
				ctx := context.Background()

				_, err := session.Execute(ctx, "CREATE (:Seed {k: 'seed'})", nil)
				require.NoError(t, err)

				_, err = session.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				require.NotNil(t, session.txContext)
				require.True(t, session.txContext.active)
				opened := session.txContext.tx
				openedID := session.txContext.txID
				t.Cleanup(func() {
					if session.txContext != nil && session.txContext.active {
						_, _ = session.handleRollback()
					}
				})

				_, err = session.Execute(ctx, shape.query, nil)
				require.NoError(t, err)

				// Same transaction, still open: the statement did not open, commit
				// or replace anything.
				require.NotNil(t, session.txContext)
				require.True(t, session.txContext.active, "statement closed the explicit transaction")
				require.Same(t, opened, session.txContext.tx, "statement ran in a different transaction")
				require.Equal(t, openedID, session.txContext.txID)

				// Own writes are visible inside, nothing is visible outside.
				inside := bareBeginCount(t, session, bareBeginNewNodes) + bareBeginCount(t, session, bareBeginTouched)
				require.Positive(t, inside, "the transaction does not see its own write")
				require.Zero(t, bareBeginCount(t, other, bareBeginNewNodes), "write escaped the open transaction")
				require.Zero(t, bareBeginCount(t, other, bareBeginTouched), "write escaped the open transaction")

				_, err = session.Execute(ctx, end, nil)
				require.NoError(t, err)
				require.True(t, session.txContext == nil || !session.txContext.active)

				after := bareBeginCount(t, other, bareBeginNewNodes) + bareBeginCount(t, other, bareBeginTouched)
				if end == "ROLLBACK" {
					require.Zero(t, after, "ROLLBACK left data behind: the statement was committed by a nested transaction")
				} else {
					require.Equal(t, inside, after)
				}
			})
		}
	}
}

// The same guarantee for the one-request script form "BEGIN <query> COMMIT|ROLLBACK".
func TestBareBegin_ScriptBodyRunsInTheTransactionBeginOpened(t *testing.T) {
	for _, shape := range bareBeginWriteShapes {
		for _, end := range []string{"ROLLBACK", "COMMIT"} {
			t.Run(shape.name+"/"+end, func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "bare_begin_script")
				session := NewStorageExecutor(store)
				other := NewStorageExecutor(store)
				ctx := context.Background()

				_, err := session.Execute(ctx, "CREATE (:Seed {k: 'seed'})", nil)
				require.NoError(t, err)

				_, err = session.Execute(ctx, "BEGIN\n"+shape.query+"\n"+end, nil)
				require.NoError(t, err)
				require.True(t, session.txContext == nil || !session.txContext.active, "script left a transaction open")

				after := bareBeginCount(t, other, bareBeginNewNodes) + bareBeginCount(t, other, bareBeginTouched)
				if end == "ROLLBACK" {
					require.Zero(t, after, "ROLLBACK left data behind: the body was committed by a nested transaction")
				} else {
					require.Positive(t, after)
				}
			})
		}
	}
}

// A failing statement after a bare BEGIN must not leave earlier writes of the
// same transaction behind once the client rolls back.
func TestBareBegin_FailedStatementThenRollbackLeavesNothing(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "bare_begin_fail")
	session := NewStorageExecutor(store)
	other := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := session.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if session.txContext != nil && session.txContext.active {
			_, _ = session.handleRollback()
		}
	})
	_, err = session.Execute(ctx, "CREATE (:BareTx {k: 'before-failure'})", nil)
	require.NoError(t, err)
	_, err = session.Execute(ctx, "SHOW WHATEVER", nil)
	require.Error(t, err)
	if session.txContext != nil && session.txContext.active {
		_, err = session.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)
	}
	require.Zero(t, bareBeginCount(t, other, bareBeginNewNodes))
}
