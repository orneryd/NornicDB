package nornicdb

import (
	"context"
	"encoding/json"
	"math"
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
	_, err = db.ExecuteCypher(ctx, "CREATE (a:ExactBackup {v:9007199254740993, decimal:1.25, whole:2.0, large_whole:$large, negative_zero:$zero})-[:EXACT {v:-9007199254740993, decimal:2.5, whole:3.0}]->(:ExactBackupEnd)", map[string]interface{}{"large": float64(1 << 60), "zero": math.Copysign(0, -1)})
	require.NoError(t, err)
	namespaced := db.storage.(*storage.NamespacedEngine)
	db.storage = storage.NewNamespacedEngine(jsonBackupEngine{namespaced.GetInnerEngine()}, namespaced.Namespace())
	path := filepath.Join(t.TempDir(), "backup.json")
	require.NoError(t, db.Backup(ctx, path))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), "9007199254740993")
	var envelope struct {
		Version string `json:"version"`
	}
	require.NoError(t, json.Unmarshal(data, &envelope))
	require.Equal(t, "2.0", envelope.Version)
	require.NoError(t, db.Restore(ctx, path))
	result, err := db.ExecuteCypher(ctx, "MATCH (a:ExactBackup)-[r:EXACT]->() RETURN a.v, a.decimal, r.v, r.decimal, a.whole, r.whole", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(9007199254740993), 1.25, int64(-9007199254740993), 2.5, float64(2), float64(3)}}, result.Rows)
	result, err = db.ExecuteCypher(ctx, "MATCH (a:ExactBackup {v:$v}) RETURN a.v", map[string]interface{}{"v": int64(9007199254740993)})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(9007199254740993)}}, result.Rows)
	result, err = db.ExecuteCypher(ctx, "MATCH (a:ExactBackup) RETURN a.large_whole, a.negative_zero", nil)
	require.NoError(t, err)
	require.Equal(t, float64(1<<60), result.Rows[0][0])
	require.True(t, math.Signbit(result.Rows[0][1].(float64)))
}

func TestNumericWireBackupLegacyIntegralTokensRestoreExactly(t *testing.T) {
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	db, err := Open("", config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	properties := map[string]interface{}{
		"large_whole": float64(1 << 60), "integer": int64(9007199254740993),
		"whole": float64(2), "zero": math.Copysign(0, -1),
	}
	// Backup/import semantics preserve the stored digits. Unlike internal
	// schema or replication state, legacy FLOAT kind is not reconstructed.
	raw, err := json.Marshal(map[string]interface{}{
		"version": "1.0",
		"nodes":   []*storage.Node{{ID: "legacy", Labels: []string{"LegacyBackup"}, Properties: properties}},
		"edges":   []*storage.Edge{{ID: "r", Type: "LEGACY", StartNode: "legacy", EndNode: "legacy", Properties: properties}},
	})
	require.NoError(t, err)
	require.Contains(t, string(raw), `"large_whole":1152921504606847000`)
	path := filepath.Join(t.TempDir(), "legacy.json")
	require.NoError(t, os.WriteFile(path, raw, 0600))
	require.NoError(t, db.Restore(context.Background(), path))
	result, err := db.ExecuteCypher(context.Background(), "MATCH (a:LegacyBackup)-[r:LEGACY]->() RETURN a.large_whole, a.integer, a.whole, a.zero, r.large_whole, r.integer", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1152921504606847000), int64(9007199254740993), int64(2), int64(0), int64(1152921504606847000), int64(9007199254740993)}}, result.Rows)
}
