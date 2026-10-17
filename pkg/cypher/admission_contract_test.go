package cypher

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// requireForbidden requires Neo4j's Forbidden status with exactly message.
func requireForbidden(t *testing.T, err error, message string) {
	t.Helper()
	var status interface {
		error
		BoltErrorCode() string
	}
	require.True(t, errors.As(err, &status), "%v", err)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", status.BoltErrorCode())
	require.Equal(t, message, status.Error())
}

// Graph admission is the one decision on a selection: a nil resolver admits
// all, a refusal is Neo4j's database-access text naming the executing user
// (the statement's, else the request's principal), never the database.
func TestAdmitGraphNamesThePrincipal(t *testing.T) {
	refuse := func(string, string) bool { return false }
	require.NoError(t, AdmitGraph(context.Background(), nil, "tenant"))
	require.NoError(t, AdmitGraph(context.Background(), func(_, permission string) bool { return permission == "access" }, "tenant"))

	statement := WithRequestIdentity(context.Background(), &RequestIdentity{User: &AuthenticatedUser{Name: "alice", Roles: []string{"reader", "editor"}}})
	requireForbidden(t, AdmitGraph(statement, refuse, "tenant"), "Database access is not allowed for user 'alice' with roles [editor, reader].")
	request := auth.WithRequestPrincipalName(auth.WithRequestPrincipalRoles(context.Background(), []string{"viewer"}), "bob")
	requireForbidden(t, AdmitGraph(request, refuse, "tenant"), "Database access is not allowed for user 'bob' with roles [viewer].")
	requireForbidden(t, AdmitGraph(context.Background(), refuse, "tenant"), "Database access is not allowed for user '' with no roles.")

	require.NoError(t, AdmitGraphFor("carol", nil, nil, "tenant"))
	requireForbidden(t, AdmitGraphFor("carol", []string{"auditor"}, refuse, "tenant"), "Database access is not allowed for user 'carol' with roles [auditor].")
	var none context.Context
	user, roles := principalOf(none)
	require.Empty(t, user)
	require.Nil(t, roles)
}

// An operation is admitted on its graph first, then for its declared effect.
func TestAdmitOperationGraphThenEffect(t *testing.T) {
	ctx := WithRequestIdentity(context.Background(), &RequestIdentity{User: &AuthenticatedUser{Name: "alice", Roles: []string{"reader"}}})
	readOnly := func(_, permission string) bool { return permission == "access" || permission == "read" }
	require.NoError(t, AdmitOperation(ctx, readOnly, "tenant", PrivilegeRead))
	require.NoError(t, AdmitOperation(ctx, nil, "tenant", PrivilegeWrite), "no resolver, no authentication")
	requireForbidden(t, AdmitOperation(ctx, readOnly, "tenant", PrivilegeWrite), "Write operations are not allowed for user 'alice' with roles [reader].")
	requireForbidden(t, AdmitOperation(ctx, func(string, string) bool { return false }, "tenant", PrivilegeRead), "Database access is not allowed for user 'alice' with roles [reader].")
}

// A request's operation is decided on the database its server-issued scope
// selects: the scope's default for no selection, an alias's canonical
// database, a refusal for a selection outside it; with no request
// authorization at all, the default database runs unchecked.
func TestAdmitRequestOperationSelectsFromTheScope(t *testing.T) {
	ctx, database, err := AdmitRequestOperation(context.Background(), "nornic", "", PrivilegeWrite)
	require.NoError(t, err)
	require.Equal(t, "nornic", database)
	require.NotNil(t, ctx)

	scope := auth.NewRequestDatabaseScope("nornic", map[string]string{"nornic": "nornic", "reports": "tenant"})
	allows := func(database string, permission auth.Permission) bool {
		return permission == "access" || database == "tenant" && permission == auth.PermRead || database == "nornic"
	}
	scoped := auth.WithRequestDatabaseScope(auth.WithRequestEntitlementResolver(
		auth.WithRequestPrincipalName(context.Background(), "alice"), allows), scope)

	admitted, database, err := AdmitRequestOperation(scoped, "nornic", "", PrivilegeWrite)
	require.NoError(t, err)
	require.Equal(t, "nornic", database, "no selection: the scope's default")
	require.NoError(t, authorizeDatabase(admitted, "nornic", "write"), "the admitted context carries the resolver")

	_, database, err = AdmitRequestOperation(scoped, "nornic", "reports", PrivilegeRead)
	require.NoError(t, err)
	require.Equal(t, "tenant", database, "an alias selects its canonical database")
	_, _, err = AdmitRequestOperation(scoped, "nornic", "reports", PrivilegeWrite)
	requireForbidden(t, err, "Write operations are not allowed for user 'alice' with no roles.")
	_, database, err = AdmitRequestOperation(scoped, "nornic", "secret", PrivilegeRead)
	requireForbidden(t, err, "Database access is not allowed for user 'alice' with no roles.")
	require.Empty(t, database)

	// A request that carries only part of its authorization is refused.
	partial := auth.WithRequestDatabaseScope(context.Background(), scope)
	_, _, err = AdmitRequestOperation(partial, "nornic", "", PrivilegeRead)
	requireForbidden(t, err, "Database access is not allowed for user '' with no roles.")
	modeOnly := auth.WithRequestDatabaseAccessMode(context.Background(), auth.FullDatabaseAccessMode)
	_, _, err = AdmitRequestOperation(modeOnly, "nornic", "", PrivilegeRead)
	requireForbidden(t, err, "Database access is not allowed for user '' with no roles.")
}

type namespaceList []string

func (n namespaceList) ListNamespaces() []string { return n }

// The physical user databases are the default, every standard database the
// manager lists and every storage namespace, without system and composites,
// each once and sorted.
func TestPhysicalUserDatabases(t *testing.T) {
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	require.NoError(t, mgr.CreateDatabase("tenant"))
	require.NoError(t, mgr.CreateCompositeDatabase("combined", []multidb.ConstituentRef{{Alias: "t", DatabaseName: "tenant", Type: "local", AccessMode: "read"}}))
	manager := &testDatabaseManagerAdapter{manager: mgr}
	require.Equal(t, []string{"archive", "nornic", "tenant"},
		PhysicalUserDatabases(manager, namespaceList{"archive", "system", "", "tenant"}, "nornic"))
	require.Equal(t, []string{"nornic"}, PhysicalUserDatabases(nil, nil, "nornic"))
	require.Empty(t, PhysicalUserDatabases(nil, nil, "system"))
}

// Storage an executor owns is closed once, however often it is released; a
// failed close is logged as the storage-cleanup event, and an executor with
// nothing to release does nothing.
func TestReleaseOwnedStorageOnce(t *testing.T) {
	var logs bytes.Buffer
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	exec.SetLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	closes := 0
	exec.SetOwnedStorageCleanup(func() error { closes++; return errors.New("remote gone") })
	exec.ReleaseOwnedStorage()
	exec.ReleaseOwnedStorage()
	require.Equal(t, 1, closes)
	require.Contains(t, logs.String(), "cypher.operation.storage_cleanup_failed")
	require.Contains(t, logs.String(), "remote gone")

	NewStorageExecutor(newTestMemoryEngine(t)).ReleaseOwnedStorage()
	var none *StorageExecutor
	none.ReleaseOwnedStorage()
}
