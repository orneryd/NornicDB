package cypher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestRequestIdentityAndUserListings: a nil identity leaves the context as
// it is, and the auth store's nil users are skipped.
func TestRequestIdentityAndUserListings(t *testing.T) {
	ctx := context.Background()
	require.Equal(t, ctx, WithRequestIdentity(ctx, nil))
	require.Nil(t, requestIdentityFromContext(ctx))

	listings := UserListingsFromAuth([]*auth.User{
		nil,
		{Username: "ann", Roles: []auth.Role{"admin"}, Disabled: true},
	})
	require.Equal(t, []UserListing{{Name: "ann", Roles: []string{"admin"}, Suspended: true}}, listings)
}

// TestShowUsersWithoutUserStore: without a user store, SHOW USERS and SHOW
// CURRENT USER list the signed-in user.
func TestShowUsersWithoutUserStore(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := WithRequestIdentity(context.Background(), &RequestIdentity{
		User: &AuthenticatedUser{Name: "bob", Roles: []string{"reader", "admin"}},
	})
	for _, query := range []string{"SHOW USERS", "SHOW CURRENT USER"} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{"bob", []string{"admin", "reader"}, false, false, nil}}, result.Rows, query)
	}
}

// TestShowDefaultDatabaseWithoutDatabaseManager: SHOW DEFAULT DATABASE fails
// as SHOW DATABASES does when there is no database manager.
func TestShowDefaultDatabaseWithoutDatabaseManager(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, showErr := exec.Execute(context.Background(), "SHOW DATABASES", nil)
	require.Error(t, showErr)
	_, err := exec.Execute(context.Background(), "SHOW DEFAULT DATABASE", nil)
	require.Error(t, err)
	require.Equal(t, showErr.Error(), err.Error())
}

// TestRunningTransactionRegistryEdges: a transaction registered without a
// database is on the default one; a statement started in a terminated
// transaction is cancelled at once; ids that aren't
// <database>-transaction-<n> terminate nothing.
func TestRunningTransactionRegistryEdges(t *testing.T) {
	ctx := context.Background()
	tx := &runningTransaction{}
	runningTransactions.register(ctx, tx, "", time.Now())
	defer runningTransactions.end(tx)
	require.Equal(t, "nornic", tx.database)
	require.True(t, strings.HasPrefix(tx.id(), "nornic-transaction-"))

	tx.terminated.Store(true)
	statement := &statementContext{parent: ctx, tx: tx}
	tx.startQuery("RETURN 1", statement, time.Now())
	require.ErrorIs(t, statement.Err(), context.Canceled)
	tx.endQuery()

	for _, id := range []string{"nornic", "nornic-transaction-x", "nornic-transaction-"} {
		_, found := runningTransactions.terminate(id)
		require.False(t, found, id)
	}
}

// TestShowTransactionsIDsAndStatus: SHOW / TERMINATE TRANSACTIONS take a
// string or a list of strings; anything else is a type error. A terminated
// transaction is listed as terminated until it ends.
func TestShowTransactionsIDsAndStatus(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	for _, query := range []string{
		"SHOW TRANSACTIONS 1",
		"SHOW TRANSACTIONS ['test-transaction-1', 2]",
		"TERMINATE TRANSACTIONS 1",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "String or List<String>", query)
	}

	tx := runningTransactions.begin(ctx, "covtx")
	defer runningTransactions.end(tx)
	_, found := runningTransactions.terminate(tx.id())
	require.True(t, found)
	result, err := exec.Execute(ctx, "SHOW TRANSACTIONS $id YIELD transactionId, currentQueryId, status", map[string]interface{}{"id": tx.id()})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, tx.id(), result.Rows[0][0])
	require.Equal(t, "", result.Rows[0][1])
	require.True(t, strings.HasPrefix(result.Rows[0][2].(string), "Terminated with reason:"), result.Rows[0][2])
}

// TestTransactionIDFilterRejectsNonExpression: a SHOW TRANSACTIONS id that
// isn't an expression is a SyntaxError.
func TestTransactionIDFilterRejectsNonExpression(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, filtered, err := exec.transactionIDFilter(context.Background(), "SHOW TRANSACTIONS )(", "SHOW")
	require.True(t, filtered)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.SyntaxError")
}

// aliasDatabaseManager is the mock database manager with aliases.
type aliasDatabaseManager struct {
	*mockDatabaseManager
	aliases map[string]map[string]string
}

func (m *aliasDatabaseManager) ListAliases(databaseName string) map[string]string {
	return m.aliases[databaseName]
}

// TestShowDatabasesListsAliases: SHOW DATABASES lists each database's
// aliases, sorted.
func TestShowDatabasesListsAliases(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	manager := &aliasDatabaseManager{
		mockDatabaseManager: newMockDatabaseManager(),
		aliases:             map[string]map[string]string{"nornic": {"zeta": "nornic", "alpha": "nornic"}},
	}
	require.NoError(t, manager.CreateDatabase("nornic"))
	exec.SetDatabaseManager(manager)
	result, err := exec.Execute(context.Background(), "SHOW DATABASES YIELD name, aliases WHERE name = 'nornic'", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"nornic", []string{"alpha", "zeta"}}}, result.Rows)
}

// TestShowIndexesRelationshipFulltextIndex: a fulltext index on
// relationship types is listed as a RELATIONSHIP index of those types.
func TestShowIndexesRelationshipFulltextIndex(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX relNotes FOR ()-[r:KNOWS]-() ON EACH [r.note]", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties WHERE name = 'relNotes'", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"relNotes", "FULLTEXT", "RELATIONSHIP", []string{"KNOWS"}, []string{"note"}}}, result.Rows)
}

// TestShowTailSyntaxErrors: a YIELD without items, and a YIELD SKIP that
// isn't an integer, are SyntaxErrors.
func TestShowTailSyntaxErrors(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	for _, query := range []string{
		"SHOW INDEXES YIELD WHERE name = 'x'",
		"SHOW INDEXES YIELD * SKIP 1.5",
		"SHOW INDEXES YIELD * LIMIT 'a'",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "Neo.ClientError.Statement.SyntaxError", query)
	}
}

// TestShowProceduresArgumentAndReturnDescriptions: SHOW PROCEDURES describes
// a procedure's arguments and returned columns.
func TestShowProceduresArgumentAndReturnDescriptions(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name:      "cov.echo",
		Signature: "cov.echo(value :: STRING) :: (echoed :: STRING)",
		Mode:      ProcedureModeRead,
		Params:    []ProcedureParam{{Name: "value", Type: "STRING"}},
		Returns:   []ProcedureColumn{{Name: "echoed", Type: "STRING"}},
		MinArgs:   1,
		MaxArgs:   1,
	}, func(ctx context.Context, exec *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{Columns: []string{"echoed"}, Rows: [][]interface{}{{args[0]}}}, nil
	}))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	result, err := exec.Execute(context.Background(), "SHOW PROCEDURES YIELD name, argumentDescription, returnDescription WHERE name = 'cov.echo'", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{
		"cov.echo",
		[]interface{}{map[string]interface{}{"name": "value", "type": "STRING", "description": "", "isDeprecated": false}},
		[]interface{}{map[string]interface{}{"name": "echoed", "type": "STRING", "description": "", "isDeprecated": false}},
	}}, result.Rows)
}

// TestShowListingHelpers: the SHOW listing helpers' edge cases.
func TestShowListingHelpers(t *testing.T) {
	require.Equal(t, []string{"a", "b"}, showStringList([]interface{}{"a", int64(1), "b"}))
	require.Equal(t, []string{"a"}, showStringList([]string{"a"}))
	require.Nil(t, showStringList("a"))

	// Rows are left as they are when a name is missing or not a string.
	short := &ExecuteResult{Columns: []string{"x", "name"}, Rows: [][]interface{}{{"b", "b"}, {"a"}}}
	sortShowRowsByName(short)
	require.Equal(t, [][]interface{}{{"b", "b"}, {"a"}}, short.Rows)
	mixed := &ExecuteResult{Columns: []string{"name"}, Rows: [][]interface{}{{"b"}, {int64(1)}}}
	sortShowRowsByName(mixed)
	require.Equal(t, [][]interface{}{{"b"}, {int64(1)}}, mixed.Rows)

	arguments, returns := functionSignatureDescriptions("pi")
	require.Equal(t, []interface{}{}, arguments)
	require.Equal(t, "", returns)
	arguments, returns = functionSignatureDescriptions("f(a :: INTEGER")
	require.Equal(t, []interface{}{}, arguments)
	require.Equal(t, "", returns)
	arguments, returns = functionSignatureDescriptions("f(a :: INTEGER, , b) :: INTEGER")
	require.Equal(t, []interface{}{
		map[string]interface{}{"name": "a", "type": "INTEGER", "description": "", "isDeprecated": false},
		map[string]interface{}{"name": "b", "type": "ANY", "description": "", "isDeprecated": false},
	}, arguments)
	require.Equal(t, "INTEGER", returns)
}

// TestShowSchemaValueHelpers: index providers and config values as Neo4j
// prints them.
func TestShowSchemaValueHelpers(t *testing.T) {
	require.Equal(t, "text-2.0", showIndexProvider("TEXT"))
	require.Equal(t, "point-1.0", showIndexProvider("POINT"))
	require.Equal(t, "", showIndexProvider("OTHER"))
	require.Equal(t, "{`a`: true,`b`: 3,`c`: 0.5,`d`: 'x',`e`: 7,`f`: 0.25}", schemaConfigLiteral(map[string]interface{}{
		"a": true, "b": 3, "c": 0.5, "d": "x", "e": int64(7), "f": float32(0.25),
	}))
	require.Nil(t, showConstraintCreateStatement(storage.Constraint{Name: "c", Type: storage.ConstraintType("OTHER"), Label: "L"}))
}
