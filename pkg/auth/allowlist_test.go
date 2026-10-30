package auth

import (
	"context"
	"testing"
)

func TestNewAllowlistDatabaseAccessMode(t *testing.T) {
	allowlist := map[string][]string{
		"admin":  {"nornic", "system"},
		"viewer": {"nornic"},
	}

	// Admin can access nornic and system
	mode := NewAllowlistDatabaseAccessMode(allowlist, []string{"admin"})
	if !mode.CanAccessDatabase("nornic") || !mode.CanAccessDatabase("system") {
		t.Error("admin should access nornic and system")
	}
	if mode.CanAccessDatabase("other") {
		t.Error("admin should not access other when not in allowlist")
	}

	// Viewer can access only nornic
	mode2 := NewAllowlistDatabaseAccessMode(allowlist, []string{"viewer"})
	if !mode2.CanAccessDatabase("nornic") {
		t.Error("viewer should access nornic")
	}
	if mode2.CanAccessDatabase("system") {
		t.Error("viewer should not access system")
	}

	// Role with no allowlist entry = all databases
	mode3 := NewAllowlistDatabaseAccessMode(allowlist, []string{"editor"})
	if !mode3.CanAccessDatabase("nornic") || !mode3.CanAccessDatabase("system") || !mode3.CanAccessDatabase("any") {
		t.Error("role with no allowlist should access all databases")
	}

	// Empty roles = deny all (returns DenyAll singleton)
	mode4 := NewAllowlistDatabaseAccessMode(allowlist, nil)
	if mode4.CanAccessDatabase("nornic") {
		t.Error("nil roles should deny")
	}
}

func TestAllowlistStore_Load_Save_SeedIfEmpty(t *testing.T) {
	ctx := context.Background()
	eng := newSystemEngine(t)

	store := NewAllowlistStore(eng)

	// Load on empty storage -> empty allowlist
	if err := store.Load(ctx); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(store.Allowlist()) != 0 {
		t.Errorf("expected empty allowlist, got %d entries", len(store.Allowlist()))
	}

	// SeedIfEmpty should create admin, editor, viewer with empty list (all databases)
	if err := store.SeedIfEmpty(ctx, []string{"nornic", "system"}); err != nil {
		t.Fatalf("SeedIfEmpty: %v", err)
	}
	al := store.Allowlist()
	if len(al) != 3 {
		t.Errorf("after seed expected 3 roles, got %d", len(al))
	}
	for _, role := range builtinRoleNames {
		dbs, ok := al[role]
		if !ok {
			t.Errorf("role %q missing after seed", role)
			continue
		}
		if len(dbs) != 0 {
			t.Errorf("role %q expected empty list (all databases), got %v", role, dbs)
		}
	}

	// SaveRoleDatabases for one role
	if err := NewRoleStore(eng).CreateRole(ctx, "analyst"); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	if err := store.SaveRoleDatabases(ctx, "analyst", []string{"nornic"}); err != nil {
		t.Fatalf("SaveRoleDatabases: %v", err)
	}
	al2 := store.Allowlist()
	if dbs, ok := al2["analyst"]; !ok || len(dbs) != 1 || dbs[0] != "nornic" {
		t.Errorf("analyst allowlist: got %v", al2["analyst"])
	}

	// Reload from storage (simulate restart)
	store2 := NewAllowlistStore(eng)
	if err := store2.Load(ctx); err != nil {
		t.Fatalf("Load store2: %v", err)
	}
	al3 := store2.Allowlist()
	if len(al3) != 4 {
		t.Errorf("after reload expected 4 roles (admin,editor,viewer,analyst), got %d", len(al3))
	}
}
