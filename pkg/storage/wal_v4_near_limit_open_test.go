package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// nearLimitHistoricalEntry supplies stable metadata to the existing historical
// record writer. Payload data is historical JSON/gob, not today's typed codec.
func nearLimitHistoricalEntry(seq uint64, data []byte) WALEntry {
	return WALEntry{Sequence: seq, Timestamp: time.Unix(1_794_000_000+int64(seq), 0).UTC(), Operation: OpEntityCommit, Checksum: crc32Checksum(data), Database: "test", Data: data}
}

// nearLimitHistoricalCommit builds a historical semantic commit near the
// single-frame boundary. The payload uses the historical
// JSON/base64 envelope around the historical gob transaction; the current typed
// recovery encoder must not be used to manufacture historical producer bytes.
func nearLimitHistoricalCommit(t *testing.T, logicalID, id, body string, chunks int) []byte {
	t.Helper()
	dim := 1024
	vector := make([]float32, dim)
	for i := range vector {
		vector[i] = 0.25
	}
	vectors := make([][]float32, chunks)
	for i := range vectors {
		vectors[i] = vector
	}
	gobTransaction, err := encodeV4GobEntityTransaction("test", logicalID, []Operation{{
		Type: OpCreateNode,
		Node: &Node{
			ID:              NodeID(id),
			Labels:          []string{"Doc"},
			Properties:      map[string]any{"body": body},
			ChunkEmbeddings: vectors,
		},
	}}, CommitModeImplicit)
	require.NoError(t, err)
	data, err := json.Marshal(walEntityCommitData{Payload: gobTransaction})
	require.NoError(t, err)
	return data
}

// TestV4StartupUpgradesNearLimitHistoricalRecord opens a store whose old journal
// holds a semantic commit within a few bytes of the frame bound. The current
// reader must retain the entire converted body and every surrounding record,
// without relying on the current encoder to construct historical bytes.
func TestV4StartupUpgradesNearLimitHistoricalRecord(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true}))
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:existing", Properties: map[string]any{"value": "old"}})
	require.NoError(t, err)
	require.NoError(t, engine.writeSchemaVersion(storageVersionCatalogV4))
	require.NoError(t, engine.Close())

	const chunks = 1 << 10
	want := int(walMaxEntrySize) - 1
	low, high := 0, want
	bodyLen, size := 0, 0
	var near []byte
	// The old JSON envelope expands binary as base64. Search without building
	// whole framed copies on every attempt. Checksum decimal width may change
	// by a few bytes, so qualify a bounded near-limit interval, not one modulus.
	for low <= high {
		middle := low + (high-low)/2
		candidate := nearLimitHistoricalCommit(t, "near-limit", "test:near", strings.Repeat("x", middle), chunks)
		entry := nearLimitHistoricalEntry(2, candidate)
		entry.Data = nil
		header, err := json.Marshal(&entry)
		require.NoError(t, err)
		candidateSize := 8 + len(header) + len(candidate)
		if candidateSize <= want {
			near, bodyLen, size = candidate, middle, candidateSize
			low = middle + 1
		} else {
			high = middle - 1
		}
	}
	require.LessOrEqual(t, size, want)
	require.LessOrEqual(t, want-size, 16, "historical record must sit within 16 bytes of the frame limit")

	records := [][]byte{
		nearLimitHistoricalCommit(t, "before", "test:before", "before", 0),
		near,
		nearLimitHistoricalCommit(t, "after", "test:after", "after", 0),
	}
	var journal []byte
	for i, data := range records {
		record := priorWALRecord(t, 5, nearLimitHistoricalEntry(uint64(i+1), data))
		journal = append(journal, record...)
	}
	walDir := filepath.Join(dir, "wal")
	require.NoError(t, os.MkdirAll(walDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(walDir, "wal.log"), journal, 0o600))

	engine, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, AllowStorageUpgrade: true}))
	require.NoError(t, err, "the stored near-limit record converts and the store opens")
	for _, id := range []string{"test:before", "test:after"} {
		_, err := engine.GetStoredNode(t.Context(), NodeID(id))
		require.NoError(t, err, "%s is recovered from the converted journal", id)
	}
	nearNode, err := engine.GetStoredNode(t.Context(), "test:near")
	require.NoError(t, err, "the near-limit record is recovered")
	require.Equal(t, strings.Repeat("x", bodyLen), nearNode.Properties["body"])
	require.Len(t, nearNode.ChunkEmbeddings, chunks, "the widened record is carried whole")
	wantVector := make([]float32, 1024)
	for i := range wantVector {
		wantVector[i] = 0.25
	}
	for _, vector := range nearNode.ChunkEmbeddings {
		require.Equal(t, wantVector, vector)
	}
	require.NoError(t, engine.Close())

	converted, err := os.ReadFile(filepath.Join(walDir, "wal.log"))
	require.NoError(t, err)
	versions := walFrameVersions(t, converted)
	require.Positive(t, versions[walFormatVersionCurrent], "upgrade publishes current framing")
	entries, readErr := ReadWALEntriesFromDir(t.Context(), walDir)
	require.NoError(t, readErr)
	require.Len(t, entries, 3, "every historical record is retained")
	archives, err := filepath.Glob(walDir + ".before-v4-*")
	require.NoError(t, err)
	preV4, err := filepath.Glob(walDir + ".pre-v4-*")
	require.NoError(t, err)
	require.Len(t, append(archives, preV4...), 1, "the pre-upgrade journal is retained once")

	engine, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true}))
	require.NoError(t, err, "the converted journal opens again")
	nearNode, err = engine.GetStoredNode(t.Context(), "test:near")
	require.NoError(t, err)
	require.Len(t, nearNode.ChunkEmbeddings, chunks)
	require.NoError(t, engine.Close())
}
