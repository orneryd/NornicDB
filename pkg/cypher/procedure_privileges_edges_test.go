package cypher

import (
	"context"
	"errors"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
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

// This fixture owns one physical system namespace. Statement admission now
// resolves its selected graph before executing transaction control as well.
func (m systemOnlyManager) ResolveDatabase(name string) (string, error) { return name, nil }
func (m systemOnlyManager) IsCompositeDatabase(string) bool             { return false }

func (m systemOnlyManager) GetStorageForUse(name string, _ string) (interface{}, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.engine, nil
}

var errSystemStorage = errors.New("system storage unavailable")

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
	user := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
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
	faults := storagefault.NewMutationFailureEngine(system, "", errSystemStorage, 0, auth.WithStoreTransaction)
	exec.SetDatabaseManager(systemOnlyManager{engine: storage.Engine(faults)})
	faults.Select(storagefault.MutationCreate, "", 0)
	_, err = exec.executeProcedurePrivilegeCommand(ctx, "GRANT EXECUTE PROCEDURE db.y ON DBMS TO a")
	require.ErrorIs(t, err, errSystemStorage)
	faults.Select(storagefault.MutationGet, "", 0)
	_, err = exec.executeProcedurePrivilegeCommand(ctx, "GRANT EXECUTE PROCEDURE db.y ON DBMS TO a")
	require.ErrorIs(t, err, errSystemStorage)
	faults.Select(storagefault.MutationScan, "", 0)
	_, err = exec.executeShowPrivileges(ctx, "SHOW PRIVILEGES")
	require.ErrorIs(t, err, errSystemStorage)
	// A REVOKE checks for immutable grants first: a read that fails is its
	// error, not "no grant".
	faults.Select(storagefault.MutationGet, "", 0)
	_, err = exec.executeProcedurePrivilegeCommand(ctx, "REVOKE EXECUTE PROCEDURE db.y ON DBMS FROM a")
	require.ErrorIs(t, err, errSystemStorage)
}
