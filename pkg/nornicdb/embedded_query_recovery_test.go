package nornicdb

import (
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedCypherMutationsSurviveJournalRebuild(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		name := "durable"
		if buffered {
			name = "write_behind"
		}
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "data")
			cfg := DefaultConfig()
			cfg.Database.AsyncWritesEnabled = buffered
			cfg.Database.StrictDurability = !buffered
			cfg.Database.WALAutoCompactionEnabled = false
			cfg.Memory.DecayEnabled = false
			cfg.Memory.AutoLinksEnabled = false
			cfg.Memory.EmbeddingEnabled = false
			db, err := Open(root, cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			// The shared embedded executor owns auto-commit statements. A
			// separately scoped executor owns the explicit transaction lifetime.
			explicit := cypher.NewStorageExecutor(db.GetStorage())
			t.Cleanup(func() { _, _ = explicit.Execute(t.Context(), "ROLLBACK", nil) })
			for _, query := range []string{
				"CREATE (:RecoveryProbe {key:'p'}), (:RecoveryProbe {key:'q'}), (:RecoveryProbe {key:'gone'})",
				"MATCH (p:RecoveryProbe {key:'p'}) SET p.hidden = true",
				"MATCH (p:RecoveryProbe {key:'p'}), (q:RecoveryProbe {key:'q'}) CREATE (p)-[:LINK {key:'pq'}]->(q)",
				"MATCH (n:RecoveryProbe {key:'gone'}) DELETE n",
			} {
				_, err := db.ExecuteCypher(t.Context(), query, nil)
				require.NoError(t, err)
			}
			for _, query := range []string{
				"BEGIN",
				"MATCH (:RecoveryProbe {key:'p'})-[r:LINK]->() SET r.value = 2",
				"CREATE (:RecoveryProbe {key:'explicit'})",
				"COMMIT",
			} {
				_, err := explicit.Execute(t.Context(), query, nil)
				require.NoError(t, err)
			}
			for _, query := range []string{
				"BEGIN",
				"CREATE (:RecoveryProbe {key:'rolled_back'})",
				"ROLLBACK",
			} {
				_, err := explicit.Execute(t.Context(), query, nil)
				require.NoError(t, err)
			}
			require.NoError(t, db.Close())
			rebuilt, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = rebuilt.Close() })
			result, _, err := storage.RecoverIntoEngine(t.Context(), rebuilt, filepath.Join(root, "wal"), "")
			require.NoError(t, err)
			require.Zero(t, result.Failed)
			scope := storage.NewNamespacedEngine(rebuilt, cfg.Database.DefaultDatabase)
			nodes, err := scope.GetNodesByLabel(t.Context(), "RecoveryProbe")
			require.NoError(t, err)
			require.Len(t, nodes, 3, "acknowledged embedded writes must have replayable mutation records")
			byKey := make(map[string]*storage.Node)
			for _, node := range nodes {
				byKey[node.Properties["key"].(string)] = node
			}
			require.Equal(t, true, byKey["p"].Properties["hidden"])
			require.Contains(t, byKey, "explicit")
			require.NotContains(t, byKey, "gone")
			require.NotContains(t, byKey, "rolled_back")
			edges, err := scope.GetEdgesByType(t.Context(), "LINK")
			require.NoError(t, err)
			require.Len(t, edges, 1)
			require.Equal(t, byKey["p"].ID, edges[0].StartNode)
			require.Equal(t, byKey["q"].ID, edges[0].EndNode)
			require.EqualValues(t, 2, edges[0].Properties["value"])
		})
	}
}
