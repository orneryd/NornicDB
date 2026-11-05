package search

import (
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// The persisted relationship-vector snapshot is MessagePack, and retains
// every vector bit and its associated chunk spans across a restart.
func TestRelationshipVectorSnapshotUsesMessagePack(t *testing.T) {
	engine := relationshipTestEngine(t)
	svc := NewService(engine, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetPersistenceEnabled(true)
	path := filepath.Join(t.TempDir(), "bm25.v2")
	svc.SetFulltextIndexPath(path)
	want := &relationshipEntry{
		Source: math.MaxUint64,
		Vectors: [][]float32{
			{math.Float32frombits(0x80000000), math.Float32frombits(0x00000001), 0.5},
			{1, -1, 0},
		},
		Spans: []embeddingutil.ChunkSpan{{Chunk: 0, Property: "reason", Start: 1, End: 5}},
	}
	rs := &relationshipSearch{
		entries:       map[string]*relationshipEntry{"r1": want},
		staged:        map[string]relationshipEntry{},
		syncDirectory: storage.SyncDir,
	}
	require.NoError(t, svc.saveRelationshipVectors(rs))

	file, err := os.Open(path + relationshipVectorsFileSuffix)
	require.NoError(t, err)
	var stored relationshipVectorsFile
	require.NoError(t, msgpack.NewDecoder(file).Decode(&stored))
	require.NoError(t, file.Close())
	require.Equal(t, relationshipVectorsFormat, stored.Format)
	require.Equal(t, want.Source, stored.Entries["r1"].Source)
	require.Equal(t, want.Spans, stored.Entries["r1"].Spans)
	require.Len(t, stored.Entries["r1"].Vectors, len(want.Vectors))
	for i := range want.Vectors {
		require.Len(t, stored.Entries["r1"].Vectors[i], len(want.Vectors[i]))
		for j, value := range want.Vectors[i] {
			require.Equal(t, math.Float32bits(value), math.Float32bits(stored.Entries["r1"].Vectors[i][j]))
		}
	}
	loaded, err := svc.loadRelationshipVectors()
	require.NoError(t, err)
	require.Equal(t, stored.Entries, loaded)
}

// A stale temporary-path symlink must not let a save overwrite a sibling.
func TestRelationshipVectorSnapshotDoesNotFollowTempSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require privileges on Windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "bm25.v2")
	victim := filepath.Join(dir, "victim")
	original := []byte("unrelated existing file")
	require.NoError(t, os.WriteFile(victim, original, 0o600))
	require.NoError(t, os.Symlink(filepath.Base(victim), path+relationshipVectorsFileSuffix+".tmp"))
	svc := NewService(relationshipTestEngine(t), 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetPersistenceEnabled(true)
	svc.SetFulltextIndexPath(path)
	svc.EnableRelationshipSearch(nil)
	err := svc.saveRelationshipVectors(svc.relationshipSearch())
	got, readErr := os.ReadFile(victim)
	require.NoError(t, readErr)
	require.Equal(t, original, got, "a save must not overwrite the symlink target")
	if err == nil {
		info, statErr := os.Lstat(path + relationshipVectorsFileSuffix)
		require.NoError(t, statErr)
		require.Zero(t, info.Mode()&os.ModeSymlink)
	}
}
