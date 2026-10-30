package storage

import (
	"strings"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

func TestMVCCSnapshotNodeStripsEmbeddingsAndDeepCopies(t *testing.T) {
	original := &Node{
		ID:     NodeID("mvcc-helper-node"),
		Labels: []string{"Doc", "Versioned"},
		Properties: map[string]any{
			"title": "original",
		},
		EmbedMeta: map[string]any{
			"model": "mini",
		},
		NamedEmbeddings: map[string][]float32{
			"title": {1, 2, 3},
		},
		ChunkEmbeddings:            [][]float32{{4, 5, 6}},
		EmbeddingsStoredSeparately: true,
	}

	snapshot := mvccSnapshotNode(original)
	if snapshot == nil {
		t.Fatal("mvccSnapshotNode() returned nil")
	}
	if snapshot == original {
		t.Fatal("mvccSnapshotNode() returned original pointer")
	}
	if snapshot.ID != original.ID {
		t.Fatalf("snapshot ID = %q, want %q", snapshot.ID, original.ID)
	}
	if len(snapshot.Labels) != 2 || snapshot.Labels[0] != "Doc" || snapshot.Labels[1] != "Versioned" {
		t.Fatalf("snapshot labels = %#v, want preserved labels", snapshot.Labels)
	}
	if snapshot.Properties["title"] != "original" {
		t.Fatalf("snapshot title = %#v, want original", snapshot.Properties["title"])
	}
	if snapshot.ChunkEmbeddings != nil {
		t.Fatalf("snapshot ChunkEmbeddings = %#v, want nil", snapshot.ChunkEmbeddings)
	}
	if snapshot.NamedEmbeddings != nil {
		t.Fatalf("snapshot NamedEmbeddings = %#v, want nil", snapshot.NamedEmbeddings)
	}
	if snapshot.EmbeddingsStoredSeparately {
		t.Fatal("snapshot should not mark embeddings stored separately")
	}
	if snapshot.EmbedMeta["model"] != "mini" {
		t.Fatalf("snapshot EmbedMeta = %#v, want copied metadata", snapshot.EmbedMeta)
	}

	original.Labels[0] = "Mutated"
	original.Properties["title"] = "changed"
	original.EmbedMeta["model"] = "other"
	if snapshot.Labels[0] != "Doc" {
		t.Fatalf("snapshot labels mutated with original: %#v", snapshot.Labels)
	}
	if snapshot.Properties["title"] != "original" {
		t.Fatalf("snapshot properties mutated with original: %#v", snapshot.Properties)
	}
	if snapshot.EmbedMeta["model"] != "mini" {
		t.Fatalf("snapshot embed meta mutated with original: %#v", snapshot.EmbedMeta)
	}

	snapshot.Properties["title"] = "snapshot-only"
	if original.Properties["title"] != "changed" {
		t.Fatalf("original properties changed when mutating snapshot: %#v", original.Properties)
	}

	if mvccSnapshotNode(nil) != nil {
		t.Fatal("mvccSnapshotNode(nil) should return nil")
	}
}

func TestEncodeDecodeMVCCHeadCompactRoundTrip(t *testing.T) {
	version := MVCCVersion{CommitTimestamp: time.Unix(1700000000, 123).UTC(), CommitSequence: 42}
	encoded, err := encodeMVCCHead(MVCCHead{Version: version})
	if err != nil {
		t.Fatalf("encodeMVCCHead() error = %v", err)
	}
	if len(encoded) != mvccHeadCompactMinLen {
		t.Fatalf("compact encoding length = %d, want %d", len(encoded), mvccHeadCompactMinLen)
	}
	if encoded[1] != 0 {
		t.Fatalf("compact flags = %08b, want 0", encoded[1])
	}

	decoded, err := decodeMVCCHead(encoded)
	if err != nil {
		t.Fatalf("decodeMVCCHead() error = %v", err)
	}
	if decoded.Version.Compare(version) != 0 {
		t.Fatalf("decoded version = %v, want %v", decoded.Version, version)
	}
	if decoded.FloorVersion.Compare(version) != 0 {
		t.Fatalf("decoded floor = %v, want normalized version %v", decoded.FloorVersion, version)
	}
	if decoded.Tombstoned {
		t.Fatal("decoded tombstone = true, want false")
	}

	floor := MVCCVersion{CommitTimestamp: version.CommitTimestamp.Add(-time.Minute), CommitSequence: 7}
	encodedWithFloor, err := encodeMVCCHead(MVCCHead{Version: version, FloorVersion: floor, Tombstoned: true})
	if err != nil {
		t.Fatalf("encodeMVCCHead(with floor) error = %v", err)
	}
	if len(encodedWithFloor) != mvccHeadCompactFullLen {
		t.Fatalf("full encoding length = %d, want %d", len(encodedWithFloor), mvccHeadCompactFullLen)
	}
	if encodedWithFloor[1] != mvccHeadFlagTombstoned|mvccHeadFlagHasFloor {
		t.Fatalf("full flags = %08b, want tombstone+floor bits", encodedWithFloor[1])
	}

	decodedWithFloor, err := decodeMVCCHead(encodedWithFloor)
	if err != nil {
		t.Fatalf("decodeMVCCHead(with floor) error = %v", err)
	}
	if !decodedWithFloor.Tombstoned {
		t.Fatal("decoded tombstone = false, want true")
	}
	if decodedWithFloor.Version.Compare(version) != 0 {
		t.Fatalf("decoded version = %v, want %v", decodedWithFloor.Version, version)
	}
	if decodedWithFloor.FloorVersion.Compare(floor) != 0 {
		t.Fatalf("decoded floor = %v, want %v", decodedWithFloor.FloorVersion, floor)
	}
}

func TestDecodeMVCCHeadErrorsAndLegacyFallback(t *testing.T) {
	if _, err := decodeMVCCHead([]byte{mvccHeadCompactVersion, 0x00, 0x01}); err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("decodeMVCCHead(truncated) error = %v, want truncated error", err)
	}

	missingFloor := make([]byte, mvccHeadCompactMinLen)
	missingFloor[0] = mvccHeadCompactVersion
	missingFloor[1] = mvccHeadFlagHasFloor
	if _, err := decodeMVCCHead(missingFloor); err == nil || !strings.Contains(err.Error(), "missing floor") {
		t.Fatalf("decodeMVCCHead(missing floor) error = %v, want missing floor error", err)
	}

	legacyVersion := MVCCVersion{CommitTimestamp: time.Unix(1710000000, 0).UTC(), CommitSequence: 9}
	legacyData, err := msgpack.Marshal(MVCCHead{Version: legacyVersion, Tombstoned: true})
	if err != nil {
		t.Fatalf("msgpack.Marshal() error = %v", err)
	}
	decoded, err := decodeMVCCHead(legacyData)
	if err != nil {
		t.Fatalf("decodeMVCCHead(legacy) error = %v", err)
	}
	if !decoded.Tombstoned {
		t.Fatal("legacy decoded tombstone = false, want true")
	}
	if decoded.Version.Compare(legacyVersion) != 0 {
		t.Fatalf("legacy decoded version = %v, want %v", decoded.Version, legacyVersion)
	}
	if decoded.FloorVersion.Compare(legacyVersion) != 0 {
		t.Fatalf("legacy decoded floor = %v, want normalized version %v", decoded.FloorVersion, legacyVersion)
	}
}

func TestBadgerEngineEffectiveMVCCPruneOptions(t *testing.T) {
	engine := &BadgerEngine{retentionPolicy: RetentionPolicy{MaxVersionsPerKey: 5, TTL: 2 * time.Hour}}
	defaults := engine.effectiveMVCCPruneOptions(MVCCPruneOptions{})
	if defaults.MaxVersionsPerKey != 5 {
		t.Fatalf("default MaxVersionsPerKey = %d, want 5", defaults.MaxVersionsPerKey)
	}
	if defaults.MinRetentionAge != 2*time.Hour {
		t.Fatalf("default MinRetentionAge = %s, want 2h", defaults.MinRetentionAge)
	}

	override := engine.effectiveMVCCPruneOptions(MVCCPruneOptions{MaxVersionsPerKey: 2, MinRetentionAge: 30 * time.Minute})
	if override.MaxVersionsPerKey != 2 {
		t.Fatalf("override MaxVersionsPerKey = %d, want 2", override.MaxVersionsPerKey)
	}
	if override.MinRetentionAge != 30*time.Minute {
		t.Fatalf("override MinRetentionAge = %s, want 30m", override.MinRetentionAge)
	}

	engine.retentionPolicy = RetentionPolicy{MaxVersionsPerKey: -1, TTL: -time.Minute}
	normalized := engine.effectiveMVCCPruneOptions(MVCCPruneOptions{})
	if normalized.MaxVersionsPerKey != DefaultRetentionPolicyMaxVersionsPerKey {
		t.Fatalf("normalized MaxVersionsPerKey = %d, want %d", normalized.MaxVersionsPerKey, DefaultRetentionPolicyMaxVersionsPerKey)
	}
	if normalized.MinRetentionAge != 0 {
		t.Fatalf("normalized MinRetentionAge = %s, want 0", normalized.MinRetentionAge)
	}
}
