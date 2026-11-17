package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// BenchmarkWALTruncateCoveredLargePayloadRetirement measures the preflight of a
// snapshot truncation whose covered records are large and already sealed in
// segments: only the small active tail survives, so the preflight must visit
// the covered payloads rather than materialize them. Fixture setup, the
// retained-tail checks and cleanup are untimed.
func BenchmarkWALTruncateCoveredLargePayloadRetirement(b *testing.B) {
	b.Cleanup(config.WithWALEnabled())
	const (
		payloadBytes = 20 << 10
		coveredCount = 480
		segmentSize  = 60
		retainedTail = 4
	)
	payload := strings.Repeat("x", payloadBytes)
	root := b.TempDir()
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		dir := filepath.Join(root, "wal")
		require.NoError(b, os.RemoveAll(dir))
		wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "none", MaxEntries: segmentSize})
		require.NoError(b, err)
		for i := 0; i < coveredCount+retainedTail; i++ {
			require.NoError(b, wal.Append(OpCheckpoint, map[string]any{"payload": payload, "index": i}))
		}
		cut := wal.Sequence() - retainedTail
		b.StartTimer()
		if err := wal.ManageSnapshot(b.Context(), cut, WALSnapshotTruncate); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		manifest, err := accessWALManifest(dir, walReadManifest, nil)
		require.NoError(b, err)
		require.Empty(b, manifest.Segments, "covered segments must be retired")
		kept, err := ReadWALEntries(b.Context(), walActivePath(dir), nil)
		require.NoError(b, err)
		require.Len(b, kept, retainedTail)
		for i, entry := range kept {
			require.Equal(b, cut+1+uint64(i), entry.Sequence)
			require.Greater(b, len(entry.Data), payloadBytes, "retained tail lost its payload")
		}
		require.NoError(b, wal.Close())
	}
}
