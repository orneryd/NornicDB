package nornicdb

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenRecoversAcceptedEntityWALInBothWriteModes(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "buffered"}[buffered], func(t *testing.T) {
			dir := t.TempDir()
			walDir := filepath.Join(dir, "wal")
			wal, err := storage.NewWAL(walDir, &storage.WALConfig{Dir: walDir, SyncMode: "immediate"})
			require.NoError(t, err)
			payload, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{Namespace: "nornic", LogicalID: "application-startup", Operations: []storage.Operation{{Type: storage.OpCreateNode, Node: &storage.Node{ID: "nornic:accepted", Labels: []string{"AcceptedStartup"}, Properties: map[string]any{"value": int64(42)}}}}, Mode: storage.CommitModeImplicit})
			require.NoError(t, err)
			require.NoError(t, wal.AppendWithDatabase(storage.OpEntityCommit, map[string]any{"payload": payload}, "nornic"))
			require.NoError(t, wal.Close())
			cfg := DefaultConfig()
			cfg.Memory.EmbeddingEnabled = false
			cfg.Memory.AutoLinksEnabled = false
			cfg.Database.AsyncWritesEnabled = buffered
			cfg.Database.AsyncFlushInterval = time.Hour
			cfg.Database.WALAutoCompactionEnabled = false
			db, err := Open(dir, cfg)
			require.NoError(t, err)
			defer db.Close()
			require.Equal(t, buffered, db.IsAsyncWritesEnabled())
			result, err := db.ExecuteCypher(context.Background(), "MATCH (n:AcceptedStartup) RETURN n.value", nil)
			require.NoError(t, err)
			require.Equal(t, [][]any{{int64(42)}}, result.Rows)
		})
	}
}
