package auth

import (
	"path/filepath"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const fgacTestReadProperty = "graph.node.property.read"

var fgacTestActions = []FineGrainedAction{
	{ID: fgacTestReadProperty, ResourceKind: FineGrainedNode, Property: true},
	{ID: "graph.node.create", ResourceKind: FineGrainedNode, Creates: true},
	{ID: "embedding.failures.retry", ResourceKind: FineGrainedDatabase},
	{ID: "embedding.clear", ResourceKind: FineGrainedDatabase},
	{ID: "embedding.worker.settings.update", ResourceKind: FineGrainedDatabase, Property: true},
}

func TestFineGrainedEntireGraphActionDistinguishesRestrictedReaders(t *testing.T) {
	policy, err := CompileFineGrainedPolicy(fgacTestActions, FineGrainedCatalog{
		Definitions: []FineGrainedEntitlement{
			{ID: "read-home", Action: fgacTestReadProperty, Scope: FineGrainedScope{Kind: FineGrainedNode, HomeDatabase: true}},
			{ID: "omit-draft", Action: fgacTestReadProperty, Deny: true, Scope: FineGrainedScope{
				Kind: FineGrainedNode, DatabaseID: "home", Properties: []string{"draft"},
			}},
		},
		Assignments: map[string][]string{"reader": {"read-home"}, "restricted": {"omit-draft"}},
	})
	require.NoError(t, err)
	reader := policy.ForRoles([]string{"reader"})
	require.True(t, reader.AllowsEntireGraphAction(fgacTestReadProperty, "home", "home"))
	require.False(t, reader.AllowsEntireGraphAction(fgacTestReadProperty, "other", "home"))
	restricted := policy.ForRoles([]string{"reader", "restricted"})
	require.False(t, restricted.AllowsEntireGraphAction(fgacTestReadProperty, "home", "home"))
}

func TestFineGrainedActionAndResourceScope(t *testing.T) {
	catalog := FineGrainedCatalog{
		Definitions: []FineGrainedEntitlement{
			{ID: "document-title", Action: fgacTestReadProperty, Scope: FineGrainedScope{
				Kind: FineGrainedNode, DatabaseID: "db-1", Types: []string{"Document"},
				EntityIDs: []string{"doc-1"}, Properties: []string{"title"},
			}},
			{ID: "block-title", Action: fgacTestReadProperty, Deny: true, Scope: FineGrainedScope{
				Kind: FineGrainedNode, Properties: []string{"title"},
			}},
			{ID: "retry-one-database", Action: "embedding.failures.retry",
				Scope: FineGrainedScope{Kind: FineGrainedDatabase, DatabaseID: "db-1"}},
			{ID: "worker-count", Action: "embedding.worker.settings.update",
				Scope: FineGrainedScope{Kind: FineGrainedDatabase, DatabaseID: "db-1", Properties: []string{"workers"}}},
		},
		Assignments: map[string][]string{
			"reviewer": {"document-title"},
			"hold":     {"block-title"},
			"operator": {"retry-one-database", "worker-count"},
		},
	}
	policy, err := CompileFineGrainedPolicy(fgacTestActions, catalog)
	require.NoError(t, err)
	catalog.Definitions[0].Scope.Properties[0] = "salary"
	delete(catalog.Assignments, "reviewer")

	document := FineGrainedResource{Kind: FineGrainedNode, DatabaseID: "db-1",
		Types: []string{"Tracked", "Document"}, EntityID: "doc-1", Property: "title"}
	reviewer := policy.ForRoles([]string{"reviewer", "operator"})
	require.True(t, reviewer.Allows(fgacTestReadProperty, document))
	for _, tc := range []struct {
		name string
		edit func(*FineGrainedResource)
	}{
		{"other database", func(r *FineGrainedResource) { r.DatabaseID = "db-2" }},
		{"other type", func(r *FineGrainedResource) { r.Types = []string{"Other"} }},
		{"other entity", func(r *FineGrainedResource) { r.EntityID = "doc-2" }},
		{"other property", func(r *FineGrainedResource) { r.Property = "salary" }},
		{"missing property", func(r *FineGrainedResource) { r.Property = "" }},
		{"relationship", func(r *FineGrainedResource) { r.Kind = FineGrainedRelationship }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := document
			tc.edit(&target)
			require.False(t, reviewer.Allows(fgacTestReadProperty, target))
		})
	}
	require.False(t, policy.ForRoles([]string{"reviewer", "hold"}).Allows(fgacTestReadProperty, document))
	require.False(t, policy.ForRoles([]string{"hold", "reviewer"}).Allows(fgacTestReadProperty, document))
	require.False(t, policy.ForRoles([]string{"admin"}).Allows(fgacTestReadProperty, document))
	db := FineGrainedResource{Kind: FineGrainedDatabase, DatabaseID: "db-1"}
	require.True(t, reviewer.Allows("embedding.failures.retry", db))
	require.False(t, reviewer.Allows("embedding.clear", db))
	require.False(t, reviewer.Allows("embedding.unknown", db))
	db.Property = "workers"
	require.True(t, reviewer.Allows("embedding.worker.settings.update", db))
	db.Property = "provider"
	require.False(t, reviewer.Allows("embedding.worker.settings.update", db))
	db.Property = ""
	require.False(t, reviewer.Allows("embedding.worker.settings.update", db))
}

func TestFineGrainedPolicyRejectsInvalidReferencesAndScope(t *testing.T) {
	for _, ent := range []FineGrainedEntitlement{
		{ID: "unknown-action", Action: "unknown", Scope: FineGrainedScope{Kind: FineGrainedDBMS}},
		{ID: "wrong-kind", Action: fgacTestReadProperty, Scope: FineGrainedScope{Kind: FineGrainedDatabase}},
		{ID: "entity-across-databases", Action: fgacTestReadProperty, Scope: FineGrainedScope{
			Kind: FineGrainedNode, EntityIDs: []string{"doc-1"},
		}},
		{ID: "already-created", Action: "graph.node.create", Scope: FineGrainedScope{
			Kind: FineGrainedNode, DatabaseID: "db-1", EntityIDs: []string{"doc-1"},
		}},
		{ID: "property-on-database", Action: "embedding.clear", Scope: FineGrainedScope{
			Kind: FineGrainedDatabase, Properties: []string{"title"},
		}},
	} {
		t.Run(ent.ID, func(t *testing.T) {
			_, err := CompileFineGrainedPolicy(fgacTestActions, FineGrainedCatalog{Definitions: []FineGrainedEntitlement{ent}})
			require.ErrorIs(t, err, ErrInvalidFineGrainedPolicy)
		})
	}
	_, err := CompileFineGrainedPolicy(fgacTestActions, FineGrainedCatalog{
		Assignments: map[string][]string{"reviewer": {"missing"}},
	})
	require.ErrorIs(t, err, ErrInvalidFineGrainedPolicy)
}

func TestFineGrainedCatalogFollowsRoleLifecycle(t *testing.T) {
	ctx := t.Context()
	base := runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	system := storage.NewNamespacedEngine(base, "system")
	roles := NewRoleStore(system)
	legacy := NewRoleEntitlementsStore(system)
	require.NoError(t, roles.CreateRole(ctx, "operator"))
	require.NoError(t, legacy.Set(ctx, "operator", []string{"read"}))

	initial, revision, err := roles.ReadFineGrainedCatalog(ctx)
	require.NoError(t, err)
	require.Empty(t, revision)
	require.Empty(t, initial.Definitions)

	catalog := FineGrainedCatalog{
		Definitions: []FineGrainedEntitlement{{ID: "retry", Action: "embedding.failures.retry",
			Scope: FineGrainedScope{Kind: FineGrainedDatabase, DatabaseID: "db-1"}}},
		Assignments: map[string][]string{"operator": {"retry"}},
	}
	require.NoError(t, roles.ReplaceFineGrainedCatalog(ctx, revision, fgacTestActions, catalog))
	fresh := NewRoleStore(system)
	persisted, revision, err := fresh.ReadFineGrainedCatalog(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, revision)
	require.Equal(t, catalog, persisted)
	require.Equal(t, []string{"read"}, PermissionsForRole("operator", legacy))
	require.True(t, EntitlementsAllow([]string{"admin"}, "write"))

	require.ErrorIs(t, roles.ReplaceFineGrainedCatalog(ctx, "", fgacTestActions, FineGrainedCatalog{}), ErrFineGrainedCatalogChanged)
	require.ErrorIs(t, roles.ReplaceFineGrainedCatalog(ctx, revision, fgacTestActions, FineGrainedCatalog{
		Definitions: catalog.Definitions, Assignments: map[string][]string{"missing-role": {"retry"}},
	}), ErrRoleNotFound)

	runtime := NewFineGrainedRuntime(roles, fgacTestActions, nil, nil)
	runtime.SetSecurityDirectory(nil, RoleMutationStores{Entitlements: legacy})
	require.NoError(t, runtime.RenameRole(ctx, "operator", "renamed"))
	persisted, renamedRevision, err := fresh.ReadFineGrainedCatalog(ctx)
	require.NoError(t, err)
	require.NotEqual(t, revision, renamedRevision)
	require.Equal(t, map[string][]string{"renamed": {"retry"}}, persisted.Assignments)
	require.Equal(t, []string{"read"}, PermissionsForRole("renamed", legacy))
	require.Empty(t, PermissionsForRole("operator", legacy))
	require.ErrorIs(t, roles.ReplaceFineGrainedCatalog(ctx, revision, fgacTestActions, FineGrainedCatalog{}), ErrFineGrainedCatalogChanged)

	require.NoError(t, runtime.DropRole(ctx, "renamed"))
	require.NoError(t, roles.CreateRole(ctx, "renamed"))
	persisted, _, err = fresh.ReadFineGrainedCatalog(ctx)
	require.NoError(t, err)
	require.Empty(t, persisted.Assignments)
	require.Equal(t, catalog.Definitions, persisted.Definitions)
	policy, err := CompileFineGrainedPolicy(fgacTestActions, persisted)
	require.NoError(t, err)
	require.False(t, policy.ForRoles([]string{"renamed"}).Allows("embedding.failures.retry",
		FineGrainedResource{Kind: FineGrainedDatabase, DatabaseID: "db-1"}))
}

func TestFineGrainedCatalogFailureRollsBackRoleMutation(t *testing.T) {
	ctx := t.Context()
	base := runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	system := storage.NewNamespacedEngine(base, "system")
	fault := storagefault.NewMutationFailureEngine(system, "", errRoleMutation, 0, WithStoreTransaction)
	roles := NewRoleStore(fault)
	require.NoError(t, roles.CreateRole(ctx, "operator"))
	catalog := FineGrainedCatalog{
		Definitions: []FineGrainedEntitlement{{ID: "retry", Action: "embedding.failures.retry",
			Scope: FineGrainedScope{Kind: FineGrainedDatabase, DatabaseID: "db-1"}}},
		Assignments: map[string][]string{"operator": {"retry"}},
	}
	require.NoError(t, roles.ReplaceFineGrainedCatalog(ctx, "", fgacTestActions, catalog))
	_, before, err := roles.ReadFineGrainedCatalog(ctx)
	require.NoError(t, err)

	fault.Operation, fault.ID = storagefault.MutationUpdate, fineGrainedCatalogID
	runtime := NewFineGrainedRuntime(roles, fgacTestActions, nil, nil)
	require.ErrorIs(t, runtime.RenameRole(ctx, "operator", "renamed"), errRoleMutation)
	require.True(t, roles.Exists("operator"))
	require.False(t, roles.Exists("renamed"))
	require.ErrorIs(t, roles.ReplaceFineGrainedCatalog(ctx, before, fgacTestActions, FineGrainedCatalog{}), errRoleMutation)
	persisted, revision, err := NewRoleStore(system).ReadFineGrainedCatalog(ctx)
	require.NoError(t, err)
	require.Equal(t, before, revision)
	require.Equal(t, catalog, persisted)
}

func TestFineGrainedCatalogSurvivesRestartAndNativeBackup(t *testing.T) {
	dir := t.TempDir()
	openRoles := func(t *testing.T, dir string) (*storage.BadgerEngine, *RoleStore) {
		t.Helper()
		engine, err := runtimeFixtureOwner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
		require.NoError(t, err)
		return engine, NewRoleStore(storage.NewNamespacedEngine(engine, "system"))
	}
	engine, roles := openRoles(t, dir)
	require.NoError(t, roles.CreateRole(t.Context(), "operator"))
	catalog := FineGrainedCatalog{
		Definitions: []FineGrainedEntitlement{{
			ID: "read-report-title", Action: fgacTestReadProperty,
			Scope: FineGrainedScope{
				Kind: FineGrainedNode, DatabaseID: "stable-db", Types: []string{"Report"},
				EntityIDs: []string{"report-handle"}, Properties: []string{"title"},
			},
		}},
		Assignments: map[string][]string{"operator": {"read-report-title"}},
	}
	require.NoError(t, roles.ReplaceFineGrainedCatalog(t.Context(), "", fgacTestActions, catalog))
	_, revision, err := roles.ReadFineGrainedCatalog(t.Context())
	require.NoError(t, err)
	backup := filepath.Join(t.TempDir(), "fgac-backup.badger")
	require.NoError(t, engine.Backup(backup))
	require.NoError(t, engine.Close())

	assertRestoredCatalog := func(t *testing.T, reloaded *RoleStore) {
		t.Helper()
		require.NoError(t, reloaded.Load(t.Context()))
		persisted, reloadedRevision, err := reloaded.ReadFineGrainedCatalog(t.Context())
		require.NoError(t, err)
		require.True(t, reloaded.Exists("operator"))
		require.Equal(t, revision, reloadedRevision)
		require.Equal(t, catalog, persisted)
		policy, err := CompileFineGrainedPolicy(fgacTestActions, persisted)
		require.NoError(t, err)
		access := policy.ForRoles([]string{"operator"})
		target := FineGrainedResource{
			Kind: FineGrainedNode, DatabaseID: "stable-db", Types: []string{"Report"},
			EntityID: "report-handle", Property: "title",
		}
		require.True(t, access.Allows(fgacTestReadProperty, target))
		target.Property = "internal"
		require.False(t, access.Allows(fgacTestReadProperty, target))
	}
	t.Run("restart", func(t *testing.T) {
		_, reloaded := openRoles(t, dir)
		assertRestoredCatalog(t, reloaded)
	})
	t.Run("native backup", func(t *testing.T) {
		restored, reloaded := openRoles(t, t.TempDir())
		require.NoError(t, restored.Restore(backup))
		assertRestoredCatalog(t, reloaded)
	})
}

func TestFineGrainedCatalogDormantDuringRBACRoleChanges(t *testing.T) {
	ctx := t.Context()
	base := runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	system := storage.NewNamespacedEngine(base, "system")
	roles := NewRoleStore(system)
	require.NoError(t, roles.CreateRole(ctx, "operator"))
	require.NoError(t, roles.CreateRole(ctx, "retained"))
	catalog := FineGrainedCatalog{
		Definitions: []FineGrainedEntitlement{{ID: "retry", Action: "embedding.failures.retry",
			Scope: FineGrainedScope{Kind: FineGrainedDatabase, DatabaseID: "db-1"}}},
		Assignments: map[string][]string{"operator": {"retry"}, "retained": {"retry"}},
	}
	require.NoError(t, roles.ReplaceFineGrainedCatalog(ctx, "", fgacTestActions, catalog))
	before, err := system.GetNode(ctx, fineGrainedCatalogID)
	require.NoError(t, err)

	fault := storagefault.NewMutationFailureEngine(system, storagefault.MutationGet, errRoleMutation, 0, WithStoreTransaction)
	fault.ID = fineGrainedCatalogID
	rbac := NewRoleStore(fault)
	require.NoError(t, rbac.Load(ctx))
	require.NoError(t, rbac.DeleteRole(ctx, "operator"))
	require.NoError(t, rbac.CreateRole(ctx, "operator"))
	require.NoError(t, rbac.RenameRole(ctx, "retained", "renamed"))
	require.True(t, rbac.Exists("operator"))
	require.True(t, rbac.Exists("renamed"))
	after, err := system.GetNode(ctx, fineGrainedCatalogID)
	require.NoError(t, err)
	require.Equal(t, before, after)

	reactivated, _, err := roles.PrepareFineGrainedCatalog(ctx)
	require.NoError(t, err)
	require.Equal(t, catalog.Definitions, reactivated.Definitions)
	require.Equal(t, map[string][]string{"renamed": {"retry"}}, reactivated.Assignments)
}
