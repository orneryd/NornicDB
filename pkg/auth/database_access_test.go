package auth

import "testing"

func TestPrivilegeDatabaseRef(t *testing.T) {
	ref := PrivilegeDatabaseRef{Name: "nornic", OwningDatabaseName: "nornic"}
	if ref.Name != "nornic" || ref.OwningDatabaseName != "nornic" {
		t.Errorf("PrivilegeDatabaseRef: got Name=%q Owning=%q", ref.Name, ref.OwningDatabaseName)
	}
}

func TestFullDatabaseAccessMode(t *testing.T) {
	if !FullDatabaseAccessMode.CanSeeDatabase("nornic") {
		t.Error("Full: CanSeeDatabase(nornic) want true")
	}
	if !FullDatabaseAccessMode.CanAccessDatabase("nornic") {
		t.Error("Full: CanAccessDatabase(nornic) want true")
	}
	if !FullDatabaseAccessMode.CanAccessDatabase("system") {
		t.Error("Full: CanAccessDatabase(system) want true")
	}
}

func TestDenyAllDatabaseAccessMode(t *testing.T) {
	if DenyAllDatabaseAccessMode.CanSeeDatabase("nornic") {
		t.Error("DenyAll: CanSeeDatabase(nornic) want false")
	}
	if DenyAllDatabaseAccessMode.CanAccessDatabase("nornic") {
		t.Error("DenyAll: CanAccessDatabase(nornic) want false")
	}
	if DenyAllDatabaseAccessMode.CanAccessDatabase("") {
		t.Error("DenyAll: CanAccessDatabase(\"\") want false")
	}
}

func TestResolvedAccess(t *testing.T) {
	ra := ResolvedAccess{Read: true, Write: false}
	if !ra.Read || ra.Write {
		t.Errorf("ResolvedAccess: got Read=%v Write=%v", ra.Read, ra.Write)
	}
}

func TestResolveDatabaseReadAccess(t *testing.T) {
	access := map[string]ResolvedAccess{
		"nornic":     {Read: true},
		"named":      {Read: true},
		"write_only": {Write: true},
	}
	resolve := func(database string) ResolvedAccess { return access[database] }

	for _, database := range []string{"nornic", "named"} {
		accessible, readable := ResolveDatabaseReadAccess(FullDatabaseAccessMode, resolve, database)
		if !accessible || !readable {
			t.Fatalf("%s: got accessible=%v readable=%v", database, accessible, readable)
		}
	}
	accessible, readable := ResolveDatabaseReadAccess(FullDatabaseAccessMode, resolve, "write_only")
	if !accessible || readable {
		t.Fatalf("write_only: got accessible=%v readable=%v", accessible, readable)
	}
	accessible, readable = ResolveDatabaseReadAccess(DenyAllDatabaseAccessMode, resolve, "nornic")
	if accessible || readable {
		t.Fatalf("deny-all: got accessible=%v readable=%v", accessible, readable)
	}
	accessible, readable = ResolveDatabaseReadAccess(nil, nil, "nornic")
	if !accessible || !readable {
		t.Fatalf("authorization disabled: got accessible=%v readable=%v", accessible, readable)
	}
}
