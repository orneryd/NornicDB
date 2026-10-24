package auth

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestAllowlistStore_DeleteRenameAndHelpers(t *testing.T) {
	ctx := context.Background()
	eng := newSystemTestEngine(t)

	store := NewAllowlistStore(eng)
	roles := NewRoleStore(eng)
	if err := roles.CreateRole(ctx, "analyst"); err != nil {
		t.Fatalf("create analyst role failed: %v", err)
	}
	if err := store.SaveRoleDatabases(ctx, "analyst", []string{"neo4j", "system"}); err != nil {
		t.Fatalf("save analyst allowlist failed: %v", err)
	}
	// A role's allowlist entry moves and goes with the role (changeRole).
	if err := roles.RenameRoleWithStores(ctx, "analyst", "scientist", RoleMutationStores{Allowlist: store}); err != nil {
		t.Fatalf("rename allowlist role failed: %v", err)
	}
	al := store.Allowlist()
	if _, ok := al["analyst"]; ok {
		t.Fatalf("old role should be removed after rename")
	}
	if !reflect.DeepEqual(al["scientist"], []string{"neo4j", "system"}) {
		t.Fatalf("unexpected copied databases after rename: %#v", al["scientist"])
	}

	if err := roles.DeleteRoleWithStores(ctx, "scientist", RoleMutationStores{Allowlist: store}); err != nil {
		t.Fatalf("delete role allowlist failed: %v", err)
	}
	if err := store.SaveRoleDatabases(ctx, "scientist", []string{"neo4j"}); !errors.Is(err, ErrRoleNotFound) {
		t.Fatalf("an allowlist for a deleted role must be refused, got %v", err)
	}

	has, err := store.HasAllowlistData(ctx)
	if err != nil {
		t.Fatalf("has allowlist data failed: %v", err)
	}
	if has {
		t.Fatalf("expected no allowlist data after deletion")
	}

	if got := roleFromNodeID("role_db_access:myrole"); got != "myrole" {
		t.Fatalf("unexpected role parse result: %q", got)
	}
	if got := roleFromNodeID("bad"); got != "" {
		t.Fatalf("expected empty role for invalid ID, got %q", got)
	}
	if got := databasesFromProperties(nil); got != nil {
		t.Fatalf("expected nil databases for nil properties, got %#v", got)
	}
	if got := databasesFromProperties(map[string]any{"databases": `[
		"neo4j",
		"system"
	]`}); !reflect.DeepEqual(got, []string{"neo4j", "system"}) {
		t.Fatalf("unexpected parsed databases: %#v", got)
	}
}

func TestAllowlistDatabaseAccessMode_NormalizationPaths(t *testing.T) {
	mode := NewAllowlistDatabaseAccessMode(
		map[string][]string{"viewer": {"neo4j"}, "editor": {}},
		[]string{" role_viewer "},
	)
	if !mode.CanSeeDatabase("neo4j") || mode.CanAccessDatabase("system") {
		t.Fatalf("expected normalized role_viewer access only to neo4j")
	}

	mode2 := NewAllowlistDatabaseAccessMode(
		map[string][]string{"viewer": {"neo4j"}, "editor": {}},
		[]string{"editor"},
	)
	if !mode2.CanAccessDatabase("anything") {
		t.Fatalf("empty role allowlist entry should mean all databases")
	}
}

func TestAllowlistStore_CopyAndSeedSkipPaths(t *testing.T) {
	ctx := context.Background()
	eng := newSystemTestEngine(t)

	store := NewAllowlistStore(eng)
	for _, role := range []string{"analyst", "global"} {
		if err := NewRoleStore(eng).CreateRole(ctx, role); err != nil {
			t.Fatalf("create %s role failed: %v", role, err)
		}
	}
	if err := store.SaveRoleDatabases(ctx, "analyst", []string{"neo4j"}); err != nil {
		t.Fatalf("save analyst allowlist failed: %v", err)
	}

	alist := store.Allowlist()
	alist["analyst"][0] = "mutated"
	if got := store.Allowlist()["analyst"][0]; got != "neo4j" {
		t.Fatalf("allowlist should return copies, got %q", got)
	}

	if err := store.SeedIfEmpty(ctx, []string{"neo4j", "system"}); err != nil {
		t.Fatalf("seed should skip when data exists: %v", err)
	}
	if _, ok := store.Allowlist()["admin"]; ok {
		t.Fatalf("seed should not add builtin roles when allowlist already has data")
	}

	if err := store.SaveRoleDatabases(ctx, "global", nil); err != nil {
		t.Fatalf("save nil databases failed: %v", err)
	}
	if got := store.Allowlist()["global"]; got != nil {
		t.Fatalf("expected nil database list for global role, got %#v", got)
	}

	node, err := eng.GetNode(storage.NodeID(roleDbAccessPrefix + "analyst"))
	if err != nil {
		t.Fatalf("get analyst allowlist node: %v", err)
	}
	createdAt := node.CreatedAt
	if err := store.SaveRoleDatabases(ctx, "analyst", []string{"system"}); err != nil {
		t.Fatalf("update analyst allowlist failed: %v", err)
	}
	node2, err := eng.GetNode(storage.NodeID(roleDbAccessPrefix + "analyst"))
	if err != nil {
		t.Fatalf("get updated analyst allowlist node: %v", err)
	}
	if !node2.CreatedAt.Equal(createdAt) {
		t.Fatalf("SaveRoleDatabases should preserve CreatedAt")
	}
}

func TestAllowlistStore_LoadAndHasDataHelpers(t *testing.T) {
	ctx := context.Background()
	eng := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer eng.Close()

	store := NewAllowlistStore(eng)
	has, err := store.HasAllowlistData(ctx)
	if err != nil {
		t.Fatalf("HasAllowlistData on empty store: %v", err)
	}
	if has {
		t.Fatalf("expected no allowlist data on empty store")
	}

	_, err = eng.CreateNode(&storage.Node{
		ID:     storage.NodeID("role_db_access:reader"),
		Labels: []string{roleDbAccessLabel, roleDbAccessSystems},
		Properties: map[string]any{
			"databases": `["neo4j","system"]`,
		},
	})
	if err != nil {
		t.Fatalf("create allowlist node: %v", err)
	}
	_, err = eng.CreateNode(&storage.Node{
		ID:     storage.NodeID("system:bad-node"),
		Labels: []string{roleDbAccessLabel},
		Properties: map[string]any{
			"databases": `["ignored"]`,
		},
	})
	if err != nil {
		t.Fatalf("create invalid allowlist node: %v", err)
	}
	_, err = eng.CreateNode(&storage.Node{
		ID:     storage.NodeID("role_db_access:other"),
		Labels: []string{"Other"},
		Properties: map[string]any{
			"databases": `["ignored"]`,
		},
	})
	if err != nil {
		t.Fatalf("create unlabeled node: %v", err)
	}

	if err := store.Load(ctx); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := store.Allowlist()["reader"]; !reflect.DeepEqual(got, []string{"neo4j", "system"}) {
		t.Fatalf("unexpected loaded reader allowlist: %#v", got)
	}
	if got := databasesFromProperties(map[string]any{"databases": 123}); got != nil {
		t.Fatalf("expected nil for wrong databases type, got %#v", got)
	}

	has, err = store.HasAllowlistData(ctx)
	if err != nil {
		t.Fatalf("HasAllowlistData with store data: %v", err)
	}
	if !has {
		t.Fatalf("expected allowlist data to be detected")
	}
}
