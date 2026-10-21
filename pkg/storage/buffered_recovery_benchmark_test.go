package storage

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// BenchmarkBufferedRecovery measures recovery only; fixture creation and engine
// opens are excluded. Every run verifies the replay count and final label count.
func BenchmarkBufferedRecovery(b *testing.B) {
	for _, count := range []int{16, 1024} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			b.ReportAllocs()
			b.StopTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				dir := b.TempDir()
				engine := newWriteBehindTestEngine(b, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
				wal := engine.bufferedCommitWAL
				for i := 0; i < count; i++ {
					bufferedCreate(b, engine, &Node{ID: NodeID("test:" + strconv.Itoa(i)), Labels: []string{"L"}, Properties: map[string]any{"payload": strings.Repeat("x", 8<<10)}})
				}
				crashClose(b, engine, wal)
				engine = newWriteBehindTestEngine(b, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
				wal = engine.bufferedCommitWAL
				b.StartTimer()
				replayed, err := RecoverBufferedCommits(engine, wal)
				b.StopTimer()
				require.NoError(b, err)
				require.Equal(b, count, replayed)
				got, err := engine.NodeCountByLabelInNamespace("test", "L")
				require.NoError(b, err)
				require.EqualValues(b, count, got)
				require.NoError(b, engine.Close())
				require.NoError(b, wal.Close())
			}
		})
	}
}
