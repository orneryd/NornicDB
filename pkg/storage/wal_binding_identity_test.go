package storage

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestWALPropertyIdentityForeignAndRestoreRotation(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	_, data := r2BindingData(t, 1)
	for _, mode := range []string{"matching-import", "snapshot", "native-restore"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			b := reviewBindingEngine(t, dir)
			_, err := b.CreateNode(&Node{ID: "perf:seed", Properties: map[string]any{"property_000": int64(0)}})
			require.NoError(t, err)
			before := b.walPropertySource.identity
			switch mode {
			case "matching-import":
				payload, err := json.Marshal(data)
				require.NoError(t, err)
				require.NoError(t, ReplayWALEntry(b, WALEntry{Operation: OpBufferedCommit, Data: payload}))
			case "snapshot":
				path := filepath.Join(t.TempDir(), "snapshot.bin")
				require.NoError(t, SaveSnapshot(&Snapshot{Timestamp: time.Now(), Nodes: []*Node{{ID: "perf:snapshot"}}}, path))
				w, err := NewWAL(t.TempDir(), DefaultWALConfig())
				require.NoError(t, err)
				require.NoError(t, w.Close())
				result, _, err := RecoverIntoEngine(b, w.Config().Dir, path)
				require.NoError(t, err)
				require.Zero(t, result.Failed)
			case "native-restore":
				path := filepath.Join(t.TempDir(), "backup.bin")
				require.NoError(t, b.Backup(path))
				require.NoError(t, b.Restore(path))
			}
			after := b.walPropertySource.identity
			require.NotEqual(t, before, after)
			require.True(t, b.walPropertySource.foreign)
			assertPropertyDictionaryBijection(t, b)
			require.NoError(t, b.Close())
			b = reviewBindingEngine(t, dir)
			require.Equal(t, after, b.walPropertySource.identity)
			require.True(t, b.walPropertySource.foreign)
			assertPropertyDictionaryBijection(t, b)
		})
	}
}

func TestWALPropertyOriginalImportAllocationBounds(t *testing.T) {
	for _, count := range []int{1, 160} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			source, data := r2BindingData(t, count)
			data.PropertyBindings = nil
			for _, stamped := range []bool{false, true} {
				data.PropertyDictionaryID = ""
				if stamped {
					data.PropertyDictionaryID = source.walPropertySource.identity
				}
				allocs := testing.AllocsPerRun(32, func() {
					commit, err := source.importBufferedWALRecord(&data, walOwnDirectoryAtOpen)
					if err != nil {
						t.Fatal(err)
					}
					for key, body := range data.Writes {
						if &body[0] != &commit.writes[key][0] {
							t.Fatal("original bodies were copied")
						}
					}
				})
				require.Zero(t, allocs, "proven original identity must add zero allocations per record")
				t.Logf("keys=%d stamped=%v owner_allocations=%.0f", count, stamped, allocs)
			}
		})
	}
}
