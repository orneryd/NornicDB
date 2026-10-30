package multidb

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestRejectedManagedGrowthNeverReplays(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, entity := range []string{"node", "edge"} {
		t.Run(entity, func(t *testing.T) {
			dir := t.TempDir()
			base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			wal, err := storage.NewWAL(dir, storage.DefaultWALConfig())
			require.NoError(t, err)
			manager, err := NewDatabaseManager(t.Context(), storage.NewWALEngine(base, wal), nil)
			require.NoError(t, err)
			defer manager.Close()
			const db = "rejected_replay"
			require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: db}))
			view, err := manager.GetStorage(db)
			require.NoError(t, err)
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err = view.CreateNode(&storage.Node{ID: id})
				require.NoError(t, err)
			}
			if entity == "node" {
				_, err = view.CreateNode(&storage.Node{ID: "n", Properties: map[string]any{"payload": "small"}})
			} else {
				err = view.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": "small"}})
			}
			require.NoError(t, err)
			before, _, _ := manager.GetStorageSize(context.Background(), db)
			require.NoError(t, manager.SetDatabaseLimits(t.Context(), db, &Limits{Storage: StorageLimits{MaxBytes: before}}))
			if entity == "node" {
				err = view.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "n", Properties: map[string]any{"payload": strings.Repeat("x", 4096)}}})
			} else {
				err = view.UpdateEdge(storage.EdgeUpdateRequest{After: &storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": strings.Repeat("x", 4096)}}})
			}
			require.ErrorIs(t, err, ErrStorageLimitExceeded)
			// Remove the quota before real recovery: absence of the rejected payload
			// must be a journal property, not another admission check during replay.
			require.NoError(t, manager.SetDatabaseLimits(t.Context(), db, nil))
			require.NoError(t, manager.Close())
			recovered, _, err := fixtureowner.OwnResult3[*storage.MemoryEngine, storage.ReplayResult](t, storage.ErrStorageClosed)(storage.RecoverFromWALWithResult(dir, ""))
			require.NoError(t, err)
			defer recovered.Close()
			if entity == "node" {
				n, e := storage.GetStoredNode(recovered, storage.NodeID(db+":n"))
				require.NoError(t, e)
				require.Equal(t, "small", n.Properties["payload"])
			} else {
				e, readErr := storage.GetStoredEdge(recovered, storage.EdgeID(db+":e"))
				require.NoError(t, readErr)
				require.Equal(t, "small", e.Properties["payload"])
			}
		})
	}
}
