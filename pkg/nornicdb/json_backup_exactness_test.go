package nornicdb

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Hide native backup capabilities to exercise the public legacy JSON route.
type jsonBackupEngine struct{ storage.Engine }

func TestJSONIngestionBackupRoundTrip(t *testing.T) {
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	db, err := Open("", config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	_, err = db.ExecuteCypher(ctx, "CREATE (a:ExactBackup {v:9007199254740993, decimal:1.25, whole:2.0})-[:EXACT {v:-9007199254740993, decimal:2.5, whole:3.0}]->(:ExactBackupEnd)", nil)
	require.NoError(t, err)
	namespaced := db.storage.(*storage.NamespacedEngine)
	db.storage = storage.NewNamespacedEngine(jsonBackupEngine{namespaced.GetInnerEngine()}, namespaced.Namespace())
	path := filepath.Join(t.TempDir(), "backup.json")
	require.NoError(t, db.Backup(ctx, path))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), "9007199254740993")
	require.NoError(t, db.Restore(ctx, path))
	result, err := db.ExecuteCypher(ctx, "MATCH (a:ExactBackup)-[r:EXACT]->() RETURN a.v, a.decimal, r.v, r.decimal, a.whole, r.whole", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(9007199254740993), 1.25, int64(-9007199254740993), 2.5, float64(2), float64(3)}}, result.Rows)
	result, err = db.ExecuteCypher(ctx, "MATCH (a:ExactBackup {v:$v}) RETURN a.v", map[string]interface{}{"v": int64(9007199254740993)})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(9007199254740993)}}, result.Rows)
}
