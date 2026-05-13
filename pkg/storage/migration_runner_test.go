package storage

import (
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

func TestSchemaVersion_FreshDatabase(t *testing.T) {
	eng := newTestEngine(t)
	v, err := eng.readSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v != storageVersionCurrent {
		t.Errorf("expected schema version %d after init, got %d", storageVersionCurrent, v)
	}
}

func TestSchemaVersion_WriteAndRead(t *testing.T) {
	eng := newTestEngine(t)
	if err := eng.writeSchemaVersion(42); err != nil {
		t.Fatal(err)
	}
	v, err := eng.readSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v != 42 {
		t.Errorf("expected 42, got %d", v)
	}
}

func TestRunOnStartMigrations_SkipsAlreadyApplied(t *testing.T) {
	eng := newTestEngine(t)
	if err := eng.RunOnStartMigrations(true); err != nil {
		t.Fatal(err)
	}
	v, err := eng.readSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v != storageVersionCurrent {
		t.Errorf("expected version %d, got %d", storageVersionCurrent, v)
	}
}

// writeLegacyNodeBytes writes a node record with legacy DecayScore/LastAccessed/AccessCount
// fields directly to Badger, simulating a pre-1.1.0 database.
func writeLegacyNodeBytes(t *testing.T, eng *BadgerEngine, id string, decayScore float64, lastAccessed time.Time, accessCount int64) {
	t.Helper()
	legacy := &legacyNodeForMigration{
		ID:           NodeID(id),
		DecayScore:   decayScore,
		LastAccessed: lastAccessed,
		AccessCount:  accessCount,
	}
	data, err := encodeValue(legacy)
	if err != nil {
		t.Fatal(err)
	}
	eng.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(nodeKey(NodeID(id)), data)
	})
}

func TestRunOnStartMigrations_AppliesWhenVersionZero(t *testing.T) {
	eng := newTestEngine(t)

	if err := eng.writeSchemaVersion(0); err != nil {
		t.Fatal(err)
	}

	lastAccessed := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	writeLegacyNodeBytes(t, eng, "nornic:legacy1", 0.75, lastAccessed, 100)

	if err := eng.RunOnStartMigrations(true); err != nil {
		t.Fatal(err)
	}

	v, err := eng.readSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v != storageVersionCurrent {
		t.Errorf("expected version %d after migration, got %d", storageVersionCurrent, v)
	}

	meta, err := eng.GetAccessMeta("nornic:legacy1")
	if err != nil {
		t.Fatal(err)
	}
	if meta == nil {
		t.Fatal("expected AccessMetaEntry after migration")
	}
	if meta.Fixed.AccessCount != 100 {
		t.Errorf("expected AccessCount 100, got %d", meta.Fixed.AccessCount)
	}
}

func TestSchemaVersion_InvalidLengthReturnsError(t *testing.T) {
	eng := newTestEngine(t)
	err := eng.db.Update(func(txn *badger.Txn) error {
		return txn.Set(mvccSchemaVersionKey(), []byte{1, 2, 3})
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = eng.readSchemaVersion()
	if err == nil {
		t.Fatal("expected invalid schema version length error")
	}
}

func TestSchemaVersion_LegacyEdgeBetweenReadyMarkerRecovers(t *testing.T) {
	eng := newTestEngine(t)
	err := eng.db.Update(func(txn *badger.Txn) error {
		return txn.Set(mvccSchemaVersionKey(), []byte{1})
	})
	if err != nil {
		t.Fatal(err)
	}

	v, err := eng.readSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v != 0 {
		t.Fatalf("expected legacy one-byte marker to be treated as version 0, got %d", v)
	}

	if err := eng.RunOnStartMigrations(true); err != nil {
		t.Fatal(err)
	}

	v, err = eng.readSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v != storageVersionCurrent {
		t.Fatalf("expected recovered schema version %d after migrations, got %d", storageVersionCurrent, v)
	}
	if edgeBetweenIndexReadyKey[1] == mvccSchemaVersionKey()[1] {
		t.Fatal("edge-between ready key must not share schema-version subkey")
	}
}
