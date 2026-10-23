package auth

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestRoleStore_AllRoles_Create_Delete(t *testing.T) {
	ctx := context.Background()
	eng := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer eng.Close()

	store := NewRoleStore(eng)
	if err := store.Load(ctx); err != nil {
		t.Fatalf("Load: %v", err)
	}
	all := store.AllRoles()
	if len(all) != 3 {
		t.Errorf("expected 3 built-in roles, got %v", all)
	}

	if err := store.CreateRole(ctx, "analyst"); err != nil {
		t.Fatalf("CreateRole analyst: %v", err)
	}
	if err := store.CreateRole(ctx, "admin"); err != ErrRoleExists {
		t.Errorf("CreateRole admin should fail with ErrRoleExists, got %v", err)
	}
	all2 := store.AllRoles()
	if len(all2) != 4 {
		t.Errorf("expected 4 roles after create, got %v", all2)
	}

	if err := store.DeleteRole(ctx, "admin"); err != ErrCannotDeleteBuiltinRole {
		t.Errorf("DeleteRole admin should fail, got %v", err)
	}
	if err := store.DeleteRole(ctx, "analyst"); err != nil {
		t.Fatalf("DeleteRole analyst: %v", err)
	}
	all3 := store.AllRoles()
	if len(all3) != 3 {
		t.Errorf("expected 3 roles after delete, got %v", all3)
	}
}

func TestRoleStore_RenameRole(t *testing.T) {
	ctx := context.Background()
	eng := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer eng.Close()

	store := NewRoleStore(eng)
	if err := store.Load(ctx); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := store.CreateRole(ctx, "oldname"); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	if err := store.RenameRole(ctx, "oldname", "newname"); err != nil {
		t.Fatalf("RenameRole: %v", err)
	}
	if store.Exists("oldname") {
		t.Error("oldname should not exist after rename")
	}
	if !store.Exists("newname") {
		t.Error("newname should exist after rename")
	}
}

func TestRoleStore_ErrorAndLoadPaths(t *testing.T) {
	ctx := context.Background()
	eng := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer eng.Close()

	_, err := eng.CreateNode(&storage.Node{
		ID:     storage.NodeID(rolePrefix + "loadedrole"),
		Labels: []string{roleLabel, roleSystems},
	})
	if err != nil {
		t.Fatalf("create role node: %v", err)
	}
	_, err = eng.CreateNode(&storage.Node{
		ID:     storage.NodeID("system:bad-role-node"),
		Labels: []string{roleLabel},
	})
	if err != nil {
		t.Fatalf("create invalid role node: %v", err)
	}

	store := NewRoleStore(eng)
	if err := store.Load(ctx); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !store.Exists("loadedrole") {
		t.Fatalf("expected loaded role to exist")
	}

	if err := store.CreateRole(ctx, "   "); err != ErrInvalidRoleName {
		t.Fatalf("expected ErrInvalidRoleName, got %v", err)
	}
	if err := store.CreateRole(ctx, "LoadedRole"); err != ErrRoleExists {
		t.Fatalf("expected ErrRoleExists for duplicate role, got %v", err)
	}
	if err := store.CreateRole(ctx, "  MixedCase "); err != nil {
		t.Fatalf("CreateRole normalized role: %v", err)
	}
	if !store.Exists("mixedcase") {
		t.Fatalf("normalized role should exist")
	}

	if err := store.DeleteRole(ctx, "missing"); err != ErrRoleNotFound {
		t.Fatalf("expected ErrRoleNotFound, got %v", err)
	}

	if err := store.RenameRole(ctx, "admin", "custom"); err != ErrCannotDeleteBuiltinRole {
		t.Fatalf("expected built-in rename error, got %v", err)
	}
	if err := store.RenameRole(ctx, "mixedcase", "viewer"); err != ErrRoleExists {
		t.Fatalf("expected ErrRoleExists for built-in rename target, got %v", err)
	}
	if err := store.RenameRole(ctx, "mixedcase", ""); err != ErrRoleExists {
		t.Fatalf("expected ErrRoleExists for empty new name, got %v", err)
	}
	if err := store.RenameRole(ctx, "missing", "other"); err != ErrRoleNotFound {
		t.Fatalf("expected ErrRoleNotFound, got %v", err)
	}
	if err := store.RenameRole(ctx, "mixedcase", "loadedrole"); err != ErrRoleExists {
		t.Fatalf("expected ErrRoleExists for duplicate rename target, got %v", err)
	}
	if !store.Exists("  MIXEDCASE  ") {
		t.Fatalf("Exists should normalize user-defined role names")
	}
	if !store.Exists("  ADMIN  ") {
		t.Fatalf("Exists should recognize normalized built-in roles")
	}
}
