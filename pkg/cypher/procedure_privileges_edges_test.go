package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// systemOnlyManager resolves the system database to engine, or fails.
type systemOnlyManager struct {
	DatabaseManagerInterface
	engine interface{}
	err    error
}

func (m systemOnlyManager) GetStorageForUse(name string, _ string) (interface{}, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.engine, nil
}

// failingScanEngine fails node scans, or node creation, when switched on.
type failingScanEngine struct {
	storage.Engine
	failScan, failCreate, failGet bool
}

var errSystemStorage = errors.New("system storage unavailable")

func (e *failingScanEngine) AllNodes() ([]*storage.Node, error) {
	if e.failScan {
		return nil, errSystemStorage
	}
	return e.Engine.AllNodes()
}

func (e *failingScanEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if e.failScan {
		return nil, errSystemStorage
	}
	return e.Engine.GetNodesByLabel(label)
}

func (e *failingScanEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if e.failCreate {
		return "", errSystemStorage
	}
	return e.Engine.CreateNode(node)
}

// The privilege commands' edges: other EXECUTE privileges and a missing ON;
// the system database from the database manager, or unavailable; storage
// errors; REVOKE from a missing role; SHOW ROLES / PRIVILEGE forms and a role
// filter.
func TestProcedurePrivilegeCommandEdges(t *testing.T) {
	ctx := context.Background()
	_, ok, _ := parseProcedurePrivilegeCommand("GRANT EXECUTE FUNCTION db.x ON DBMS TO r")
	require.False(t, ok)
	_, ok, err := parseProcedurePrivilegeCommand("GRANT EXECUTE PROCEDURE db.x TO r")
	require.True(t, ok)
	require.ErrorContains(t, err, "expected ON DBMS")

	require.Equal(t, []string{"a", "b"}, showPrivilegeRoles("SHOW ROLES role_a, `b` PRIVILEGES"))
	require.Equal(t, []string{"a"}, showPrivilegeRoles("SHOW ROLE a PRIVILEGE"))
	require.Nil(t, showPrivilegeRoles("SHOW ROLE a"))
	require.Nil(t, showPrivilegeRoles("SHOW PRIVILEGES"))

	// A nil grant check attaches nothing.
	require.Equal(t, ctx, WithProcedureGrantChecker(ctx, nil))

	// No storage at all.
	_, err = (&StorageExecutor{}).systemStorage()
	require.Error(t, err)

	// The system database through the database manager.
	systemBase := newTestMemoryEngine(t)
	system := storage.NewNamespacedEngine(systemBase, "system")
	require.NoError(t, auth.NewRoleStore(system).CreateRole(ctx, "a"))
	require.NoError(t, auth.NewRoleStore(system).CreateRole(ctx, "b"))
	user := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = user.Close() })
	exec := NewStorageExecutor(user)
	exec.SetDatabaseManager(systemOnlyManager{engine: system})
	_, err = exec.executeProcedurePrivilegeCommand(ctx, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO a, b")
	require.NoError(t, err)
	require.True(t, allowsBoosted(t, system, "b", "db.cdc.query"))
	result, err := exec.executeShowPrivileges(ctx, "SHOW ROLE a PRIVILEGES")
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "a", result.Rows[0][5])

	_, err = exec.executeProcedurePrivilegeCommand(ctx, "GRANT EXECUTE PROCEDURE db.x TO a")
	require.ErrorContains(t, err, "expected ON DBMS")
	_, err = exec.executeProcedurePrivilegeCommand(ctx, "REVOKE EXECUTE PROCEDURE db.x ON DBMS FROM nobody")
	require.NoError(t, err) // REVOKE from a role that does not exist changes nothing

	// The database manager can't resolve the system database.
	exec.SetDatabaseManager(systemOnlyManager{err: errSystemStorage})
	_, err = exec.executeProcedurePrivilegeCommand(ctx, "GRANT EXECUTE PROCEDURE db.x ON DBMS TO a")
	require.ErrorIs(t, err, errSystemStorage)
	_, err = exec.executeShowPrivileges(ctx, "SHOW PRIVILEGES")
	require.ErrorIs(t, err, errSystemStorage)

	// Storage errors reading roles or grants, and writing a grant.
	failing := &failingScanEngine{Engine: system}
	exec.SetDatabaseManager(systemOnlyManager{engine: storage.Engine(failing)})
	failing.failCreate = true
	_, err = exec.executeProcedurePrivilegeCommand(ctx, "GRANT EXECUTE PROCEDURE db.y ON DBMS TO a")
	require.ErrorIs(t, err, errSystemStorage)
	failing.failCreate, failing.failGet = false, true
	_, err = exec.executeProcedurePrivilegeCommand(ctx, "GRANT EXECUTE PROCEDURE db.y ON DBMS TO a")
	require.ErrorIs(t, err, errSystemStorage)
	failing.failScan = true
	_, err = exec.executeShowPrivileges(ctx, "SHOW PRIVILEGES")
	require.ErrorIs(t, err, errSystemStorage)
	// A REVOKE checks for immutable grants first: a read that fails is its
	// error, not "no grant".
	failing.failScan, failing.failGet = false, true
	_, err = exec.executeProcedurePrivilegeCommand(ctx, "REVOKE EXECUTE PROCEDURE db.y ON DBMS FROM a")
	require.ErrorIs(t, err, errSystemStorage)
}

func (e *failingScanEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if e.failGet {
		return nil, errSystemStorage
	}
	return e.Engine.GetNode(id)
}

func (e *failingScanEngine) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return auth.WithStoreTransaction(ctx, e.Engine, func(view storage.Engine) error {
		staged := *e
		staged.Engine = view
		return mutate(&staged)
	})
}
