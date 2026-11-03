package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPublicRecoverBufferedNativeWrapperAndMixedSemantic(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "buffered", true: "mixed_semantic"}[mixed], func(t *testing.T) {
			source := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true})
			source.writeBehind.SetDelayBounds(time.Hour, time.Hour)
			wal := source.bufferedCommitWAL
			require.NoError(t, source.BulkCreateNodes(t.Context(), []*Node{{ID: "tenant:a", Properties: map[string]any{"v": int64(11)}}, {ID: "tenant:b"}}))
			expected := 2
			if mixed {
				wrapped := fixtureowner.OwnChecked(t, NewWALEngine(source, wal))
				_, err := wrapped.CreateNode(t.Context(), &Node{ID: "tenant:c", Properties: map[string]any{"v": int64(12)}})
				require.NoError(t, err)
				expected++
			}
			require.NoError(t, wal.Sync())
			entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
			require.NoError(t, err)
			require.NoError(t, source.FlushWriteBehind(t.Context()))
			require.NoError(t, source.Close())
			require.NoError(t, wal.Close())
			recovered, result, err := fixtureowner.OwnResult3[*MemoryEngine, ReplayResult](t, ErrStorageClosed)(RecoverFromWALWithResult(wal.Config().Dir, ""))
			require.NoError(t, err)
			require.Zero(t, result.Failed, "automatic recovery's MemoryEngine must expose its actual physical owner")
			require.Positive(t, result.Applied)
			for _, entry := range entries {
				require.NoError(t, ReplayWALEntry(recovered, entry))
			}
			count, err := recovered.NodeCount(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, expected, count)
			node, err := recovered.GetNode(t.Context(), "tenant:a")
			require.NoError(t, err)
			require.EqualValues(t, 11, node.Properties["v"])
			dir := t.TempDir()
			disk := openLargeCommitTestEngine(t, dir)
			for _, entry := range entries {
				require.NoError(t, ReplayWALEntry(disk, entry))
			}
			require.NoError(t, disk.Close())
			reopened := openLargeCommitTestEngine(t, dir)
			for _, entry := range entries {
				require.NoError(t, ReplayWALEntry(reopened, entry))
			}
			count, err = reopened.NodeCount(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, expected, count)
		})
	}
}
