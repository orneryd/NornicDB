package multidb

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestExistsOrIsConstituent_StandardDB(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	mgr, err := NewDatabaseManager(t.Context(), base, nil)
	if err != nil {
		t.Fatalf("NewDatabaseManager failed: %v", err)
	}
	defer mgr.Close()

	if err := mgr.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "mydb"}); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	if !mgr.ExistsOrIsConstituent("mydb") {
		t.Fatal("expected ExistsOrIsConstituent to return true for standard database")
	}
}

func TestExistsOrIsConstituent_CompositeDB(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	mgr, err := NewDatabaseManager(t.Context(), base, nil)
	if err != nil {
		t.Fatalf("NewDatabaseManager failed: %v", err)
	}
	defer mgr.Close()

	if err := mgr.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "shard_a"}); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	if err := mgr.CreateCompositeDatabase(t.Context(), "comp", []ConstituentRef{
		{Alias: "a", DatabaseName: "shard_a", Type: "local", AccessMode: "read_write"},
	}); err != nil {
		t.Fatalf("CreateCompositeDatabase failed: %v", err)
	}

	if !mgr.ExistsOrIsConstituent("comp") {
		t.Fatal("expected ExistsOrIsConstituent to return true for composite database")
	}
}

func TestExistsOrIsConstituent_ConstituentDotted(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	mgr, err := NewDatabaseManager(t.Context(), base, nil)
	if err != nil {
		t.Fatalf("NewDatabaseManager failed: %v", err)
	}
	defer mgr.Close()

	if err := mgr.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "shard_a"}); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	if err := mgr.CreateCompositeDatabase(t.Context(), "translations", []ConstituentRef{
		{Alias: "tr", DatabaseName: "shard_a", Type: "local", AccessMode: "read_write"},
	}); err != nil {
		t.Fatalf("CreateCompositeDatabase failed: %v", err)
	}

	if !mgr.ExistsOrIsConstituent("translations.tr") {
		t.Fatal("expected ExistsOrIsConstituent to return true for dotted constituent reference")
	}
}

func TestExistsOrIsConstituent_ConstituentDotted_CaseInsensitive(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	mgr, err := NewDatabaseManager(t.Context(), base, nil)
	if err != nil {
		t.Fatalf("NewDatabaseManager failed: %v", err)
	}
	defer mgr.Close()

	if err := mgr.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "shard_a"}); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	if err := mgr.CreateCompositeDatabase(t.Context(), "translations", []ConstituentRef{
		{Alias: "TR", DatabaseName: "shard_a", Type: "local", AccessMode: "read_write"},
	}); err != nil {
		t.Fatalf("CreateCompositeDatabase failed: %v", err)
	}

	if !mgr.ExistsOrIsConstituent("translations.tr") {
		t.Fatal("expected case-insensitive constituent alias match")
	}
}

func TestExistsOrIsConstituent_Unknown(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	mgr, err := NewDatabaseManager(t.Context(), base, nil)
	if err != nil {
		t.Fatalf("NewDatabaseManager failed: %v", err)
	}
	defer mgr.Close()

	if mgr.ExistsOrIsConstituent("doesnotexist") {
		t.Fatal("expected ExistsOrIsConstituent to return false for unknown database")
	}
}

func TestExistsOrIsConstituent_DottedButNotComposite(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	mgr, err := NewDatabaseManager(t.Context(), base, nil)
	if err != nil {
		t.Fatalf("NewDatabaseManager failed: %v", err)
	}
	defer mgr.Close()

	if err := mgr.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "notcomposite"}); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	if mgr.ExistsOrIsConstituent("notcomposite.alias") {
		t.Fatal("expected false for dotted name where base is not a composite")
	}
}

func TestExistsOrIsConstituent_DottedWrongAlias(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	mgr, err := NewDatabaseManager(t.Context(), base, nil)
	if err != nil {
		t.Fatalf("NewDatabaseManager failed: %v", err)
	}
	defer mgr.Close()

	if err := mgr.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "shard_a"}); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	if err := mgr.CreateCompositeDatabase(t.Context(), "comp", []ConstituentRef{
		{Alias: "a", DatabaseName: "shard_a", Type: "local", AccessMode: "read_write"},
	}); err != nil {
		t.Fatalf("CreateCompositeDatabase failed: %v", err)
	}

	if mgr.ExistsOrIsConstituent("comp.nonexistent") {
		t.Fatal("expected false for dotted name with wrong alias")
	}
}

func TestExistsOrIsConstituent_Alias(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	mgr, err := NewDatabaseManager(t.Context(), base, nil)
	if err != nil {
		t.Fatalf("NewDatabaseManager failed: %v", err)
	}
	defer mgr.Close()

	if err := mgr.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "realdb"}); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	if err := mgr.CreateAlias(t.Context(), "myalias", "realdb"); err != nil {
		t.Fatalf("CreateAlias failed: %v", err)
	}

	if !mgr.ExistsOrIsConstituent("myalias") {
		t.Fatal("expected ExistsOrIsConstituent to return true for database alias")
	}
}

func TestGetStorageWithAuth_ConstituentDotted_Local(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	mgr, err := NewDatabaseManager(t.Context(), base, nil)
	if err != nil {
		t.Fatalf("NewDatabaseManager failed: %v", err)
	}
	defer mgr.Close()

	if err := mgr.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "shard_local"}); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	if err := mgr.CreateCompositeDatabase(t.Context(), "cmp", []ConstituentRef{
		{Alias: "a", DatabaseName: "shard_local", Type: "local", AccessMode: "read_write"},
	}); err != nil {
		t.Fatalf("CreateCompositeDatabase failed: %v", err)
	}

	engine, err := mgr.GetStorageWithAuth("cmp.a", "Bearer token")
	if err != nil {
		t.Fatalf("GetStorageWithAuth for dotted constituent failed: %v", err)
	}
	if engine == nil {
		t.Fatal("expected non-nil engine for dotted constituent")
	}
}

func TestGetStorageWithAuth_ConstituentDotted_RemoteFactory(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	var gotAuth string

	mgr, err := NewDatabaseManager(t.Context(), base, &Config{
		RemoteEngineFactory: func(_ ConstituentRef, authToken string) (storage.Engine, error) {
			gotAuth = authToken
			return fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil
		},
	})
	if err != nil {
		t.Fatalf("NewDatabaseManager failed: %v", err)
	}
	defer mgr.Close()

	if err := mgr.CreateCompositeDatabase(t.Context(), "cmp_remote", []ConstituentRef{
		{Alias: "r", DatabaseName: "tenant_r", Type: "remote", AccessMode: "read_write", URI: "http://remote"},
	}); err != nil {
		t.Fatalf("CreateCompositeDatabase failed: %v", err)
	}

	engine, err := mgr.GetStorageWithAuth("cmp_remote.r", "Bearer forwarded")
	if err != nil {
		t.Fatalf("GetStorageWithAuth for dotted remote constituent failed: %v", err)
	}
	if engine == nil {
		t.Fatal("expected non-nil remote engine for dotted constituent")
	}
	if gotAuth != "Bearer forwarded" {
		t.Fatalf("expected forwarded auth token, got %q", gotAuth)
	}
}
