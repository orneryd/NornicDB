package storage_test

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPublicDatabaseOpenRepairsHistoricalDerivedKeys(t *testing.T) {
	directory := storage.HistoricalAmbiguousUpgradeFixtureForTesting(t)
	for attempt := 0; attempt < 2; attempt++ {
		cfg := nornicdb.DefaultConfig()
		cfg.Auth.Enabled = false
		cfg.Memory.DecayEnabled = false
		cfg.Memory.AutoLinksEnabled = false
		cfg.Memory.EmbeddingEnabled = false
		cfg.Database.DefaultDatabase = "hist"
		cfg.Database.AllowStorageUpgrade = true
		cfg.Database.AsyncWritesEnabled = false
		cfg.Database.MVCCLifecycleEnabled = false
		cfg.Database.WALAutoCompactionEnabled = false
		db, err := nornicdb.Open(directory, cfg)
		require.NoError(t, err)
		func() {
			defer func() { require.NoError(t, db.Close()) }()
			result, err := db.ExecuteCypher(t.Context(), "MATCH (n:Damaged) RETURN count(n) AS total", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
			edges, err := db.ExecuteCypher(t.Context(), "MATCH ()-[r:LINK]->(n:Damaged) WHERE n.sealed = 'kept' RETURN n.sealed AS sealed", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"kept"}}, edges.Rows)
		}()
	}
}
