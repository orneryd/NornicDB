package bolt

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCloseoutRebuildIncludesPostSnapshotBoltWrites(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(fmt.Sprintf("strict=%t", strict), func(t *testing.T) {
			root := t.TempDir()
			cfg := journalConfig()
			cfg.Database.StrictDurability = strict
			if !strict {
				cfg.Database.WALSyncMode = "batch"
			}
			cfg.Storage.Mode = "low"
			cfg.Database.WALSnapshotInterval = 200 * time.Millisecond
			lab := journalOpen(t, root, cfg)
			lab.mustRun("CREATE (:CloseoutSnapshotSeed {k: 1})", nil)
			_, err := lab.run("CREATE (:CloseoutRefused {text: $payload})",
				map[string]any{"payload": strings.Repeat("x", 40<<20)}, false)
			require.Error(t, err, "the 40 MiB property exceeds the low-memory store limit")
			require.Equal(t, int64(0), lab.count("MATCH (n:CloseoutRefused) RETURN count(n)", nil))
			journal := lab.db.GetBaseStorageForManager().(*storage.WALEngine)
			require.NoError(t, journal.EnableAutoCompaction(filepath.Join(root, "snapshots")))
			var snapshot string
			require.Eventually(t, func() bool {
				files, err := filepath.Glob(filepath.Join(root, "snapshots", "snapshot-*.json"))
				if err != nil || len(files) == 0 {
					return false
				}
				snapshot = files[len(files)-1]
				return true
			}, 15*time.Second, 50*time.Millisecond, "automatic compaction must publish a snapshot")

			lab = lab.reopen(cfg)
			const size = 1536 << 10
			raw := make([]byte, size)
			for i := range 70 {
				_, err := rand.New(rand.NewSource(int64(i))).Read(raw)
				require.NoError(t, err)
				params := map[string]any{"i": int64(i), "blob": base64.StdEncoding.EncodeToString(raw)}
				_, err = lab.run("CREATE (:CloseoutAfter {i: $i, blob: $blob})", params, i%2 != 0)
				require.NoError(t, err, "write %d must be acknowledged", i)
			}
			require.Equal(t, int64(70), lab.count("MATCH (n:CloseoutAfter) RETURN count(n)", nil))
			lab.close()

			fresh, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, fresh.Close()) })
			ctx := context.Background()
			replayed, _, err := storage.RecoverIntoEngine(ctx, fresh, filepath.Join(root, "wal"), snapshot)
			require.NoError(t, err)
			require.Zero(t, replayed.Failed, "%v", replayed.Errors)
			store := storage.NewNamespacedEngine(fresh, "nornic")
			seed, err := store.GetNodesByLabel(ctx, "CloseoutSnapshotSeed")
			require.NoError(t, err)
			require.Len(t, seed, 1, "the snapshot contributes its seed")
			after, err := store.GetNodesByLabel(ctx, "CloseoutAfter")
			require.NoError(t, err)
			require.Len(t, after, 70)
			keys := make(map[int64]bool, len(after))
			for _, node := range after {
				key, ok := node.Properties["i"].(int64)
				require.True(t, ok, "%v", node.Properties["i"])
				keys[key] = true
			}
			for i := range 70 {
				require.True(t, keys[int64(i)], "acknowledged ID %d missing after replay", i)
			}
		})
	}
}
