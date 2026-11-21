package nornicdb_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

func TestCloseoutL5Upgrade144(t *testing.T) {
	const graph = "MATCH (a:CloseoutUpgrade)-[:LINK]->(b:CloseoutUpgrade) RETURN a.id, a.name, b.id, b.count"
	const fixture = "/work/tnrun/v152-closeout-l5-h90-b6d8e762a/store"
	open := func(t *testing.T, path string, allowUpgrade bool) *nornicdb.DB {
		t.Helper()
		cfg := nornicdb.DefaultConfig()
		cfg.Auth.Enabled = false
		cfg.Memory.DecayEnabled = false
		cfg.Memory.AutoLinksEnabled = false
		cfg.Memory.EmbeddingEnabled = false
		cfg.Database.AsyncWritesEnabled = false
		cfg.Database.MVCCLifecycleEnabled = false
		cfg.Database.WALAutoCompactionEnabled = false
		cfg.Database.AllowStorageUpgrade = allowUpgrade
		db, err := nornicdb.Open(path, cfg)
		require.NoError(t, err)
		return db
	}
	check := func(t *testing.T, db *nornicdb.DB) {
		t.Helper()
		result, err := db.ExecuteCypher(t.Context(), graph, nil)
		require.NoError(t, err)
		require.Equal(t, [][]any{{int64(1), "kept", int64(2), int64(7)}}, result.Rows)
	}
	t.Run("fresh-control", func(t *testing.T) {
		db := open(t, t.TempDir(), false)
		defer func() { require.NoError(t, db.Close()) }()
		result, err := db.ExecuteCypher(t.Context(), "RETURN 1", nil)
		require.NoError(t, err)
		require.Equal(t, [][]any{{int64(1)}}, result.Rows)
	})
	switch os.Getenv("CLOSEOUT_L5_UPGRADE_MODE") {
	case "write":
		require.NoError(t, os.MkdirAll(filepath.Dir(fixture), 0755))
		require.NoError(t, os.Mkdir(fixture, 0755), "retain any previous fixture")
		db := open(t, fixture, false)
		_, err := db.ExecuteCypher(t.Context(), "CREATE (:CloseoutUpgrade {id:1, name:'kept'})-[:LINK]->(:CloseoutUpgrade {id:2, count:7})", nil)
		require.NoError(t, err)
		check(t, db)
		require.NoError(t, db.Close())
		require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(fixture), "ready"), []byte("b6d8e762a\n"), 0644))
		t.Log("L5 H-90 stored and verified the graph on the historical writer")
	case "read":
		marker, err := os.ReadFile(filepath.Join(filepath.Dir(fixture), "ready"))
		require.NoError(t, err, "writer must complete before the reader")
		require.Equal(t, "b6d8e762a\n", string(marker))
		copied := filepath.Join(t.TempDir(), "store")
		require.NoError(t, os.CopyFS(copied, os.DirFS(fixture)))
		for i := range 2 {
			db := open(t, copied, true)
			check(t, db)
			require.NoError(t, db.Close())
			t.Logf("L5 H-90 open=%d preserved nodes, properties and edge", i+1)
		}
	default:
		t.Fatal("CLOSEOUT_L5_UPGRADE_MODE must be write or read")
	}
}
