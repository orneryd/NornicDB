package auth

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestEntitlementsCatalogAndRolePermissionStrings(t *testing.T) {
	all := AllEntitlements()
	if len(all) != 11 {
		t.Fatalf("expected 11 entitlements, got %d", len(all))
	}

	global := 0
	perDB := 0
	for _, e := range all {
		if e.ID == "" || e.Name == "" {
			t.Fatalf("entitlement should have id/name: %#v", e)
		}
		switch e.Category {
		case EntitlementCategoryGlobal:
			global++
		case EntitlementCategoryPerDatabase:
			perDB++
		default:
			t.Fatalf("unexpected entitlement category %q", e.Category)
		}
	}
	if global != 7 || perDB != 4 {
		t.Fatalf("unexpected entitlement counts: global=%d perDB=%d", global, perDB)
	}

	if got := GlobalEntitlementIDs(); len(got) != 7 {
		t.Fatalf("expected 7 global IDs, got %d", len(got))
	}
	if got := PerDatabaseEntitlementIDs(); len(got) != 4 {
		t.Fatalf("expected 4 per-db IDs, got %d", len(got))
	}

	rolePerms := RolePermissionsAsStrings()
	if len(rolePerms) == 0 {
		t.Fatalf("expected role permissions map")
	}
	if _, ok := rolePerms[string(RoleAdmin)]; !ok {
		t.Fatalf("expected admin role in permissions map")
	}
}

func TestRequestRBACContextRoundTrip(t *testing.T) {
	base := context.Background()
	if got := RequestPrincipalRolesFromContext(base); got != nil {
		t.Fatalf("expected nil roles without context, got %#v", got)
	}
	if got := RequestDatabaseAccessModeFromContext(base); got != nil {
		t.Fatalf("expected nil db access mode without context, got %#v", got)
	}
	if got := RequestResolvedAccessResolverFromContext(base); got != nil {
		t.Fatalf("expected nil resolver without context, got non-nil resolver")
	}
	if got := RequestEntitlementResolverFromContext(base); got != nil {
		t.Fatalf("expected nil entitlement resolver without context, got non-nil resolver")
	}

	ctx := WithRequestPrincipalRoles(base, []string{"admin", "editor"})
	ctx = WithRequestDatabaseAccessMode(ctx, FullDatabaseAccessMode)
	// One resolver: read and write come from it too.
	ctx = WithRequestEntitlementResolver(ctx, func(db string, permission Permission) bool {
		return db == "system" || (db != "forbidden" && permission == PermRead)
	})

	roles := RequestPrincipalRolesFromContext(ctx)
	if !reflect.DeepEqual(roles, []string{"admin", "editor"}) {
		t.Fatalf("unexpected roles from context: %#v", roles)
	}
	mode := RequestDatabaseAccessModeFromContext(ctx)
	if !mode.CanAccessDatabase("anything") {
		t.Fatalf("expected full access mode from context")
	}
	resolver := RequestResolvedAccessResolverFromContext(ctx)
	if resolver == nil {
		t.Fatalf("expected resolver in context")
	}
	got := resolver("system")
	if !got.Read || !got.Write {
		t.Fatalf("unexpected resolved access from resolver: %#v", got)
	}
	if got := resolver("nornic"); !got.Read || got.Write {
		t.Fatalf("unexpected resolved access on nornic: %#v", got)
	}
	if got := resolver("forbidden"); got.Read || got.Write {
		t.Fatalf("unexpected resolved access on forbidden: %#v", got)
	}
	entitlementResolver := RequestEntitlementResolverFromContext(ctx)
	if entitlementResolver == nil || !entitlementResolver("system", PermAdmin) || entitlementResolver("nornic", PermAdmin) {
		t.Fatalf("unexpected entitlement resolver result")
	}
}

func TestRoleEntitlementsStore_SetLoadAndPermissions(t *testing.T) {
	eng := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer eng.Close()
	ctx := context.Background()

	store := NewRoleEntitlementsStore(eng)
	if err := store.Set(ctx, "", []string{"read"}); err != ErrInvalidRoleName {
		t.Fatalf("expected ErrInvalidRoleName, got %v", err)
	}

	if err := store.Set(ctx, "Admin", []string{"read", "write"}); err != nil {
		t.Fatalf("set admin override: %v", err)
	}
	got := store.Get("admin")
	if !reflect.DeepEqual(got, []string{"read", "write"}) {
		t.Fatalf("unexpected stored entitlements: %#v", got)
	}

	// Ensure Get returns copy.
	got[0] = "mutated"
	got2 := store.Get("admin")
	if got2[0] != "read" {
		t.Fatalf("store should return copy, got %#v", got2)
	}

	all := ReadRoleCatalog(nil, store).EntitlementOverrides
	if !reflect.DeepEqual(all["admin"], []string{"read", "write"}) {
		t.Fatalf("unexpected all entitlements map: %#v", all)
	}

	loaded := NewRoleEntitlementsStore(eng)
	if err := loaded.Load(ctx); err != nil {
		t.Fatalf("load from storage failed: %v", err)
	}
	if !reflect.DeepEqual(loaded.Get("admin"), []string{"read", "write"}) {
		t.Fatalf("loaded entitlements mismatch: %#v", loaded.Get("admin"))
	}

	if err := loaded.Set(ctx, "admin", nil); err != nil {
		t.Fatalf("clearing admin override failed: %v", err)
	}
	if ids := loaded.Get("admin"); ids != nil {
		t.Fatalf("expected nil override after delete, got %#v", ids)
	}

	// Fallback behavior without override.
	viewer := PermissionsForRole("viewer", loaded)
	if len(viewer) == 0 {
		t.Fatalf("expected built-in fallback permissions for viewer")
	}
	if custom := PermissionsForRole("custom-role", loaded); custom != nil {
		t.Fatalf("expected nil for unknown custom role, got %#v", custom)
	}

	union := PermissionsForRoles([]string{"viewer", "editor", "viewer"}, loaded)
	if len(union) == 0 {
		t.Fatalf("expected merged permissions for multiple roles")
	}
}

func TestRoleEntitlementParsingHelpers(t *testing.T) {
	if got := roleFromEntitlementNodeID("bad"); got != "" {
		t.Fatalf("expected empty role for invalid ID, got %q", got)
	}
	if got := roleFromEntitlementNodeID("role_entitlement:  AdMin "); got != "admin" {
		t.Fatalf("expected normalized role 'admin', got %q", got)
	}

	if got := entitlementIDsFromProperties(nil); got != nil {
		t.Fatalf("expected nil entitlements for nil props, got %#v", got)
	}
	if got := entitlementIDsFromProperties(map[string]any{"entitlements": `["read","write"]`}); !reflect.DeepEqual(got, []string{"read", "write"}) {
		t.Fatalf("unexpected json string entitlements: %#v", got)
	}
	if got := entitlementIDsFromProperties(map[string]any{"entitlements": []any{" READ ", "", "Write"}}); !reflect.DeepEqual(got, []string{"read", "write"}) {
		t.Fatalf("unexpected []any entitlements normalization: %#v", got)
	}
	if got := entitlementIDsFromProperties(map[string]any{"entitlements": 123}); got != nil {
		t.Fatalf("expected nil for unsupported entitlement type, got %#v", got)
	}
}

func TestRoleEntitlementsStore_LoadAndUpdatePaths(t *testing.T) {
	eng := newSystemEngine(t)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{
		ID:     storage.NodeID(roleEntitlementPrefix + "loaded"),
		Labels: []string{roleEntitlementLabel, roleEntitlementSystems},
		Properties: map[string]any{
			"entitlements": `["read"]`,
		},
	})
	if err != nil {
		t.Fatalf("create entitlement node: %v", err)
	}
	_, err = eng.CreateNode(&storage.Node{
		ID:     storage.NodeID("system:bad-entitlement"),
		Labels: []string{roleEntitlementLabel},
	})
	if err != nil {
		t.Fatalf("create invalid entitlement node: %v", err)
	}

	store := NewRoleEntitlementsStore(eng)
	if err := store.Load(ctx); err != nil {
		t.Fatalf("load entitlements: %v", err)
	}
	if !reflect.DeepEqual(store.Get("loaded"), []string{"read"}) {
		t.Fatalf("unexpected loaded entitlements: %#v", store.Get("loaded"))
	}

	// Every entitlement write requires its role, an empty one too.
	if err := store.Set(ctx, "ephemeral", nil); !errors.Is(err, ErrRoleNotFound) {
		t.Fatalf("a set for a missing role must be refused, got %v", err)
	}
	if err := NewRoleStore(eng).CreateRole(ctx, "loaded"); err != nil {
		t.Fatalf("create loaded role: %v", err)
	}

	if err := store.Set(ctx, "loaded", []string{"write"}); err != nil {
		t.Fatalf("update loaded role entitlements: %v", err)
	}
	if !reflect.DeepEqual(store.Get("loaded"), []string{"write"}) {
		t.Fatalf("expected updated entitlements, got %#v", store.Get("loaded"))
	}

	node, err := eng.GetNode(storage.NodeID(roleEntitlementPrefix + "loaded"))
	if err != nil {
		t.Fatalf("get loaded entitlement node: %v", err)
	}
	createdAt := node.CreatedAt
	if err := store.Set(ctx, "loaded", []string{"read", "write"}); err != nil {
		t.Fatalf("second entitlement update failed: %v", err)
	}
	node2, err := eng.GetNode(storage.NodeID(roleEntitlementPrefix + "loaded"))
	if err != nil {
		t.Fatalf("get updated entitlement node: %v", err)
	}
	if !node2.CreatedAt.Equal(createdAt) {
		t.Fatalf("Set should preserve CreatedAt on update")
	}
}

func TestPrivilegesStore_SaveLoadResolveAndPutMatrix(t *testing.T) {
	eng := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer eng.Close()
	ctx := context.Background()

	store := NewPrivilegesStore(eng)
	if err := store.SavePrivilege(ctx, " Viewer ", "neo4j", true, false); err != nil {
		t.Fatalf("save privilege failed: %v", err)
	}

	resolved := ResolveDatabaseAccess([]string{"ROLE_viewer"}, "neo4j", store, nil)
	if !resolved.Read || resolved.Write {
		t.Fatalf("unexpected resolved access from matrix: %#v", resolved)
	}

	// Non-matrix DB for built-in editor should fall back to role permissions.
	fallback := ResolveDatabaseAccess([]string{"editor"}, "otherdb", store, nil)
	if !fallback.Read || !fallback.Write {
		t.Fatalf("expected editor fallback read+write, got %#v", fallback)
	}

	matrix := store.Matrix()
	if len(matrix) != 1 || matrix[0].Role != "viewer" || matrix[0].Database != "neo4j" {
		t.Fatalf("unexpected matrix contents: %#v", matrix)
	}

	loaded := NewPrivilegesStore(eng)
	if err := loaded.Load(ctx); err != nil {
		t.Fatalf("load privileges failed: %v", err)
	}
	resolvedLoaded := ResolveDatabaseAccess([]string{"viewer"}, "neo4j", loaded, nil)
	if !resolvedLoaded.Read || resolvedLoaded.Write {
		t.Fatalf("unexpected loaded resolved access: %#v", resolvedLoaded)
	}

	entries := []struct {
		Role     string `json:"role"`
		Database string `json:"database"`
		Read     bool   `json:"read"`
		Write    bool   `json:"write"`
	}{
		{Role: "admin", Database: "system", Read: true, Write: true},
		{Role: "viewer", Database: "analytics", Read: true, Write: false},
	}
	if err := loaded.PutMatrix(ctx, entries); err != nil {
		t.Fatalf("put matrix failed: %v", err)
	}
	if got := ResolveDatabaseAccess([]string{"viewer"}, "neo4j", loaded, nil); got.Write || !got.Read {
		t.Fatalf("expected fallback read-only on neo4j after matrix replace, got %#v", got)
	}
	if got := ResolveDatabaseAccess([]string{"viewer"}, "analytics", loaded, nil); !got.Read || got.Write {
		t.Fatalf("expected explicit analytics privilege, got %#v", got)
	}

	if err := loaded.SavePrivilege(ctx, "viewer", "locked", false, false); err != nil {
		t.Fatalf("save explicit deny privilege failed: %v", err)
	}
	if got := ResolveDatabaseAccess([]string{"viewer"}, "locked", loaded, nil); got.Read || got.Write {
		t.Fatalf("expected explicit deny to override global viewer fallback, got %#v", got)
	}
}

func TestPrivilegesParsingHelpers(t *testing.T) {
	if role, db := roleDbFromNodeID("bad"); role != "" || db != "" {
		t.Fatalf("expected empty role/db for invalid id, got role=%q db=%q", role, db)
	}
	if role, db := roleDbFromNodeID("db_priv:viewer:neo4j"); role != "viewer" || db != "neo4j" {
		t.Fatalf("unexpected parse result role=%q db=%q", role, db)
	}

	if got := privFromProperties(map[string]any{"read": true, "write": false}); !got.Read || got.Write {
		t.Fatalf("unexpected bool property conversion: %#v", got)
	}
	if got := privFromProperties(map[string]any{"read": float64(0), "write": float64(1)}); got.Read || !got.Write {
		t.Fatalf("unexpected numeric property conversion: %#v", got)
	}
}

func TestPrivilegesStore_LoadAndUpdatePaths(t *testing.T) {
	eng := newSystemEngine(t)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{
		ID:     storage.NodeID(dbPrivPrefix + "loaded:neo4j"),
		Labels: []string{dbPrivLabel, dbPrivSystems},
		Properties: map[string]any{
			"read":  true,
			"write": false,
		},
	})
	if err != nil {
		t.Fatalf("create privilege node: %v", err)
	}
	_, err = eng.CreateNode(&storage.Node{
		ID:     storage.NodeID("system:bad-privilege"),
		Labels: []string{dbPrivLabel},
	})
	if err != nil {
		t.Fatalf("create invalid privilege node: %v", err)
	}

	store := NewPrivilegesStore(eng)
	if err := store.Load(ctx); err != nil {
		t.Fatalf("load privileges: %v", err)
	}
	if got := grantedAccess(store, []string{"loaded"}, "neo4j"); !got.Read || got.Write {
		t.Fatalf("unexpected loaded privilege resolution: %#v", got)
	}
	if err := NewRoleStore(eng).CreateRole(ctx, "loaded"); err != nil {
		t.Fatalf("create loaded role: %v", err)
	}

	if err := store.SavePrivilege(ctx, " Loaded ", "neo4j", true, true); err != nil {
		t.Fatalf("update privilege: %v", err)
	}
	if got := grantedAccess(store, []string{"loaded"}, "neo4j"); !got.Read || !got.Write {
		t.Fatalf("expected updated privilege resolution, got %#v", got)
	}

	node, err := eng.GetNode(storage.NodeID(dbPrivPrefix + "loaded:neo4j"))
	if err != nil {
		t.Fatalf("get loaded privilege node: %v", err)
	}
	createdAt := node.CreatedAt
	if err := store.SavePrivilege(ctx, "loaded", "neo4j", false, true); err != nil {
		t.Fatalf("second privilege update failed: %v", err)
	}
	node2, err := eng.GetNode(storage.NodeID(dbPrivPrefix + "loaded:neo4j"))
	if err != nil {
		t.Fatalf("get updated privilege node: %v", err)
	}
	if !node2.CreatedAt.Equal(createdAt) {
		t.Fatalf("SavePrivilege should preserve CreatedAt on update")
	}

	if err := store.PutMatrix(ctx, nil); err != nil {
		t.Fatalf("put empty matrix: %v", err)
	}
	if matrix := store.Matrix(); len(matrix) != 0 {
		t.Fatalf("expected empty matrix after clear, got %#v", matrix)
	}
}
