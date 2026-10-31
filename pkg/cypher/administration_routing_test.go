package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var errSystemUnavailable = errors.New("system database unavailable")

// systemRoutingManager is administrationReviewManager with a system
// database that can't be opened (err) or is the given engine.
type systemRoutingManager struct {
	administrationReviewManager
	err    error
	system storage.Engine
}

func (m systemRoutingManager) GetStorageForUse(name, token string) (interface{}, error) {
	if name == "system" {
		if m.err != nil {
			return nil, m.err
		}
		return m.system, nil
	}
	return m.administrationReviewManager.GetStorageForUse(name, token)
}

// An administration command as an explicit transaction's first statement
// makes it an administration transaction on the system database, whatever
// database it was opened on, as in Neo4j: COMMIT keeps the grant.
func TestAdministrationTransactionOpenedOnUserDatabase(t *testing.T) {
	ctx := context.Background()
	exec, system := administrationTransactionWithGrant(t, "nornic", "GRANT EXECUTE PROCEDURE db.ping ON DBMS TO viewer")
	_, err := exec.handleCommit(context.Background())
	require.NoError(t, err)
	grants, err := auth.NewProcedurePrivilegeStore(system).List(ctx)
	require.NoError(t, err)
	require.Len(t, grants, 1)
}

// An administration command whose system database can't be reached, or
// can't open the administration transaction, fails and writes nothing.
func TestAdministrationRoutingFailures(t *testing.T) {
	ctx := context.Background()
	grant := "GRANT EXECUTE PROCEDURE db.ping ON DBMS TO viewer"
	routed := func(t *testing.T, system storage.Engine, err error) *StorageExecutor {
		exec, _ := administrationReviewExecutor(t, "nornic")
		exec.SetDatabaseManager(systemRoutingManager{administrationReviewManager: exec.dbManager.(administrationReviewManager), err: err, system: system})
		return exec
	}

	t.Run("system database unavailable", func(t *testing.T) {
		_, err := routed(t, nil, errSystemUnavailable).Execute(ctx, grant, nil)
		require.ErrorIs(t, err, errSystemUnavailable)
	})
	t.Run("system database opens no transaction", func(t *testing.T) {
		base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		t.Cleanup(func() { _ = base.Close() })
		// nonTransactionalEngine (executor_misc_test.go) opens no transaction.
		exec := routed(t, nonTransactionalEngine{Engine: storage.NewNamespacedEngine(base, "system")}, nil)
		_, err := exec.handleBegin(admittedTestContext())
		require.NoError(t, err)
		_, err = exec.Execute(ctx, grant, nil)
		require.Error(t, err)
	})
	t.Run("initial snapshot can't be released", func(t *testing.T) {
		exec, system := administrationReviewExecutor(t, "nornic")
		_, err := exec.handleBegin(admittedTestContext())
		require.NoError(t, err)
		require.NoError(t, exec.txContext.tx.(*storage.BadgerTransaction).Rollback())
		_, err = exec.Execute(ctx, grant, nil)
		require.Error(t, err)
		grants, err := auth.NewProcedurePrivilegeStore(system).List(ctx)
		require.NoError(t, err)
		require.Empty(t, grants)
	})
}

// executeInternal applies the same transaction admission, administration
// routing and system statement policy as Execute.
func TestExecuteInternalAdmissionRoutingAndPolicy(t *testing.T) {
	ctx := context.Background()

	exec, system := administrationReviewExecutor(t, "nornic")
	_, err := exec.executeInternal(ctx, "GRANT EXECUTE PROCEDURE db.ping ON DBMS TO viewer", nil)
	require.NoError(t, err, "routed to the system database")
	grants, err := auth.NewProcedurePrivilegeStore(system).List(ctx)
	require.NoError(t, err)
	require.Len(t, grants, 1)

	failed, _ := administrationReviewExecutor(t, "nornic")
	_, err = failed.handleBegin(admittedTestContext())
	require.NoError(t, err)
	failed.txContext.failed = errors.New("an earlier statement failed")
	_, err = failed.executeInternal(ctx, "RETURN 1 AS one", nil)
	requireStatusCode(t, err, "Neo.TransientError.Transaction.QueryExecutionFailedOnTransaction")

	onSystem, _ := administrationReviewExecutor(t, "system")
	_, err = onSystem.executeInternal(ctx, "CREATE (:Internal)", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SemanticError")

	// A procedure's administration command in an explicit transaction whose
	// statement carries no storage wrapper (called directly).
	tx, _ := administrationReviewExecutor(t, "nornic")
	_, err = tx.handleBegin(admittedTestContext())
	require.NoError(t, err)
	_, err = tx.executeUserStatement(ctx, "GRANT EXECUTE PROCEDURE db.ping ON DBMS TO viewer", nil)
	requireStatusCode(t, err, "Neo.ClientError.Transaction.ForbiddenDueToTransactionType")
}

type transactionDatabasesStub struct {
	composite map[string]bool
	aliases   map[string]string
}

func (s transactionDatabasesStub) IsCompositeDatabase(name string) bool { return s.composite[name] }
func (s transactionDatabasesStub) ResolveDatabase(name string) (string, error) {
	if target, ok := s.aliases[name]; ok {
		return target, nil
	}
	return name, nil
}

// TransactionDatabaseError allows the transaction's database by name
// (any case) or alias and a composite's constituents, and refuses any other
// database, with or without a database manager.
func TestTransactionDatabaseError(t *testing.T) {
	stub := transactionDatabasesStub{composite: map[string]bool{"comp": true}, aliases: map[string]string{"a": "nornic"}}
	require.NoError(t, TransactionDatabaseError(t.Context(), stub, "nornic", "NORNIC", "RETURN 1"))
	require.NoError(t, TransactionDatabaseError(t.Context(), stub, "nornic", "a", "RETURN 1"))
	require.NoError(t, TransactionDatabaseError(t.Context(), stub, "comp", "comp.x", "RETURN 1"))
	requireStatusCode(t, TransactionDatabaseError(t.Context(), stub, "nornic", "other", "CREATE (:X)"), "Neo.ClientError.Statement.AccessMode")
	requireStatusCode(t, TransactionDatabaseError(t.Context(), nil, "nornic", "other", "RETURN 1"), "Neo.ClientError.Statement.AccessMode")
}

// On the system database a system procedure reads a view that holds no
// graph: none of the records the database keeps, as in Neo4j, where
// db.labels() lists nothing there. The view keeps the database's name.
func TestSystemProceduresSeeNoSystemRecords(t *testing.T) {
	ctx := context.Background()
	exec, system := administrationReviewExecutor(t, "system")
	_, err := system.CreateNode(&storage.Node{ID: "user:probe", Labels: []string{"_User"}, Properties: map[string]any{"username": "probe"}})
	require.NoError(t, err)
	for _, statement := range []string{"CALL db.labels() YIELD label RETURN label", "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey"} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		require.Empty(t, result.Rows, statement)
	}
	// In an explicit transaction too: the statement's transaction storage
	// is left out of the procedure's context.
	_, err = exec.handleBegin(admittedTestContext())
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "CALL db.labels() YIELD label RETURN label", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)

	view := systemGraphView{Engine: system}
	require.Equal(t, "system", view.Namespace())
	_, err = view.GetNode("user:probe")
	require.ErrorIs(t, err, storage.ErrNotFound)
	_, err = view.GetEdge("e")
	require.ErrorIs(t, err, storage.ErrNotFound)
	_, err = view.GetFirstNodeByLabel("_User")
	require.ErrorIs(t, err, storage.ErrNotFound)
	nodes, err := view.GetNodesByLabel("_User")
	require.NoError(t, err)
	require.Empty(t, nodes)
	nodes, err = view.AllNodes()
	require.NoError(t, err)
	require.Empty(t, nodes)
	require.Empty(t, view.GetAllNodes())
	edges, err := view.AllEdges()
	require.NoError(t, err)
	require.Empty(t, edges)
	for _, read := range []func() ([]*storage.Edge, error){
		func() ([]*storage.Edge, error) { return view.GetOutgoingEdges("user:probe") },
		func() ([]*storage.Edge, error) { return view.GetIncomingEdges("user:probe") },
		func() ([]*storage.Edge, error) { return view.GetEdgesBetween("a", "b") },
		func() ([]*storage.Edge, error) { return view.GetEdgesByType("R") },
	} {
		edges, err := read()
		require.NoError(t, err)
		require.Empty(t, edges)
	}
	require.Nil(t, view.GetEdgeBetween("a", "b", "R"))
	visited := 0
	require.NoError(t, view.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{}, func(*storage.Node) error { visited++; return nil }))
	require.Zero(t, visited)
	require.Zero(t, view.GetInDegree("user:probe"))
	require.Zero(t, view.GetOutDegree("user:probe"))
	require.NotNil(t, view.GetSchema())
	batch, err := view.BatchGetNodes([]storage.NodeID{"user:probe"})
	require.NoError(t, err)
	require.Empty(t, batch)
	for _, count := range []func() (int64, error){
		view.NodeCount, view.EdgeCount,
		func() (int64, error) { return view.EdgeCountByType("R") },
		func() (int64, error) { return view.EdgeCountByStartLabel("_User", "R") },
		func() (int64, error) { return view.EdgeCountByEndLabel("_User", "R") },
	} {
		n, err := count()
		require.NoError(t, err)
		require.Zero(t, n)
	}
	other := withoutTransactionStorage{context.WithValue(context.WithValue(ctx, probeContextKey{}, "kept"), ctxKeyTxStorage, "wrapper")}
	require.Nil(t, other.Value(ctxKeyTxStorage))
	require.Equal(t, "kept", other.Value(probeContextKey{}))
}

type probeContextKey struct{}

// The administration transaction takes over the caller's transaction
// metadata; metadata it can't take fails the command. executeInternal
// reports a routing failure as Execute does, and an administration
// transaction's commands write through its system view.
func TestAdministrationTransactionTakeover(t *testing.T) {
	ctx := context.Background()
	grant := "GRANT EXECUTE PROCEDURE db.ping ON DBMS TO viewer"

	exec, _ := administrationReviewExecutor(t, "nornic")
	_, err := exec.handleBegin(admittedTestContext())
	require.NoError(t, err)
	exec.txContext.tx.(*storage.BadgerTransaction).Metadata = map[string]interface{}{"oversized": strings.Repeat("x", 4096)}
	_, err = exec.Execute(ctx, grant, nil)
	require.Error(t, err)

	unavailable, _ := administrationReviewExecutor(t, "nornic")
	unavailable.SetDatabaseManager(systemRoutingManager{administrationReviewManager: unavailable.dbManager.(administrationReviewManager), err: errSystemUnavailable})
	_, err = unavailable.executeInternal(ctx, grant, nil)
	require.ErrorIs(t, err, errSystemUnavailable)

	admin, _ := administrationReviewExecutor(t, "nornic")
	_, err = admin.handleBegin(admittedTestContext())
	require.NoError(t, err)
	_, err = admin.Execute(ctx, grant, nil)
	require.NoError(t, err)
	view, err := admin.privilegeStorage(true)
	require.NoError(t, err)
	require.Same(t, admin.txContext.storageWrapper, view)
}
