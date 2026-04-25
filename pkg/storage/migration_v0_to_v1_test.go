package storage

import (
	"testing"
	"time"
)

func TestMigrateV0ToV1_ExtractsAccessState(t *testing.T) {
	eng := newTestEngine(t)
	if err := eng.writeSchemaVersion(0); err != nil {
		t.Fatal(err)
	}

	lastAccessed := time.Date(2025, 3, 10, 12, 0, 0, 0, time.UTC)
	writeLegacyNodeBytes(t, eng, "nornic:scored", 0.85, lastAccessed, 42)

	if err := eng.migrateV0ToV1(); err != nil {
		t.Fatal(err)
	}

	meta, err := eng.GetAccessMeta("nornic:scored")
	if err != nil {
		t.Fatal(err)
	}
	if meta == nil {
		t.Fatal("expected AccessMetaEntry")
	}
	if meta.Fixed.AccessCount != 42 {
		t.Errorf("expected AccessCount 42, got %d", meta.Fixed.AccessCount)
	}
	if meta.Fixed.LastAccessedAt != lastAccessed.UnixNano() {
		t.Errorf("expected LastAccessedAt %d, got %d", lastAccessed.UnixNano(), meta.Fixed.LastAccessedAt)
	}
	if meta.TargetScope != "NODE" {
		t.Errorf("expected scope NODE, got %s", meta.TargetScope)
	}
	if meta.MutationCount != 1 {
		t.Errorf("expected MutationCount 1, got %d", meta.MutationCount)
	}
}

func TestMigrateV0ToV1_SkipsZeroAccessState(t *testing.T) {
	eng := newTestEngine(t)
	if err := eng.writeSchemaVersion(0); err != nil {
		t.Fatal(err)
	}

	// Create a node with no access state (all zeros).
	writeLegacyNodeBytes(t, eng, "nornic:nostate", 0, time.Time{}, 0)

	if err := eng.migrateV0ToV1(); err != nil {
		t.Fatal(err)
	}

	meta, err := eng.GetAccessMeta("nornic:nostate")
	if err != nil {
		t.Fatal(err)
	}
	if meta != nil {
		t.Error("expected no AccessMetaEntry for zero-access-state node")
	}
}

func TestMigrateV0ToV1_Idempotent(t *testing.T) {
	eng := newTestEngine(t)
	if err := eng.writeSchemaVersion(0); err != nil {
		t.Fatal(err)
	}

	writeLegacyNodeBytes(t, eng, "nornic:idem", 0, time.Time{}, 10)

	if err := eng.migrateV0ToV1(); err != nil {
		t.Fatal(err)
	}

	if err := eng.migrateV0ToV1(); err != nil {
		t.Fatal(err)
	}

	meta, err := eng.GetAccessMeta("nornic:idem")
	if err != nil {
		t.Fatal(err)
	}
	if meta == nil {
		t.Fatal("expected AccessMetaEntry")
	}
	if meta.Fixed.AccessCount != 10 {
		t.Errorf("expected AccessCount 10, got %d", meta.Fixed.AccessCount)
	}
}

func TestMigrateV0ToV1_MultipleNodes(t *testing.T) {
	eng := newTestEngine(t)
	if err := eng.writeSchemaVersion(0); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		id := "nornic:multi" + string(rune('a'+i))
		writeLegacyNodeBytes(t, eng, id, 0, time.Time{}, int64(i*10))
	}

	if err := eng.migrateV0ToV1(); err != nil {
		t.Fatal(err)
	}

	// Node with idx=0 has AccessCount=0, should be skipped.
	meta, err := eng.GetAccessMeta("nornic:multia")
	if err != nil {
		t.Fatal(err)
	}
	if meta != nil {
		t.Error("expected no entry for zero-access node")
	}

	for i := 1; i < 5; i++ {
		id := "nornic:multi" + string(rune('a'+i))
		meta, err := eng.GetAccessMeta(id)
		if err != nil {
			t.Fatal(err)
		}
		if meta == nil {
			t.Errorf("expected entry for %s", id)
			continue
		}
		if meta.Fixed.AccessCount != int64(i*10) {
			t.Errorf("%s: expected AccessCount %d, got %d", id, i*10, meta.Fixed.AccessCount)
		}
	}
}

func TestMigrateV0ToV1_WritesSchemaVersion(t *testing.T) {
	eng := newTestEngine(t)
	if err := eng.writeSchemaVersion(0); err != nil {
		t.Fatal(err)
	}

	if err := eng.migrateV0ToV1(); err != nil {
		t.Fatal(err)
	}

	v, err := eng.readSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Errorf("expected version 1, got %d", v)
	}
}
