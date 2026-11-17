package nornicdb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func openBufferedRelationshipRelabelFixture(t *testing.T) (*DB, *storage.BadgerEngine, *Node, *Node) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Database.AsyncWritesEnabled = true
	cfg.Database.StrictDurability = false
	cfg.Database.AsyncFlushInterval = time.Hour
	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := t.Context()
	x, err := db.CreateNodeWithID(ctx, "restore-x", []string{"Probe"}, map[string]interface{}{"id": "x"})
	require.NoError(t, err)
	y, err := db.CreateNodeWithID(ctx, "restore-y", []string{"After"}, map[string]interface{}{"id": "y"})
	require.NoError(t, err)
	base, ok := storage.UnwrapEngine(db.GetStorage()).(*storage.BadgerEngine)
	require.True(t, ok)
	require.NoError(t, base.FlushWriteBehind(ctx))
	_, err = db.ExecuteCypher(ctx, "MATCH (y {id:'y'}) REMOVE y:After SET y:Held", nil)
	require.NoError(t, err)
	require.NoError(t, base.FlushWriteBehind(ctx))
	_, err = db.ExecuteCypher(ctx, "MATCH (x {id:'x'}), (y {id:'y'}) CREATE (x)-[:OTHER]->(y)", nil)
	require.NoError(t, err)
	return db, base, x, y
}

func TestJSONRestoreRelabelKeepsAcknowledgedRelationshipCounts(t *testing.T) {
	db, base, x, y := openBufferedRelationshipRelabelFixture(t)
	ctx := t.Context()
	raw, err := json.Marshal(map[string]interface{}{
		"version": "1.0",
		"nodes": []*storage.Node{
			{ID: storage.NodeID(x.ID), Labels: x.Labels, Properties: x.Properties, CreatedAt: x.CreatedAt},
			{ID: storage.NodeID(y.ID), Labels: y.Labels, Properties: y.Properties, CreatedAt: y.CreatedAt},
		},
		"edges": []*storage.Edge{},
	})
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "restore.json")
	require.NoError(t, os.WriteFile(path, raw, 0600))
	// No read or manual drain intervenes between the acknowledged edge and restore.
	require.NoError(t, db.Restore(ctx, path))
	require.NoError(t, base.FlushWriteBehind(ctx))
	requireRelabelledRelationshipCounts(t, db)
}

func requireRelabelledRelationshipCounts(t *testing.T, db *DB) {
	t.Helper()
	ctx := t.Context()
	for _, tc := range []struct {
		label string
		want  int64
	}{{"Held", 0}, {"After", 1}} {
		counted, err := db.ExecuteCypher(ctx, "MATCH ()-[r:OTHER]->(:"+tc.label+") RETURN count(r)", nil)
		require.NoError(t, err)
		traversed, err := db.ExecuteCypher(ctx, "MATCH (a)-[r:OTHER]->(b) WHERE b:"+tc.label+" RETURN size(collect(r))", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{tc.want}}, traversed.Rows, tc.label)
		require.Equal(t, traversed.Rows, counted.Rows, tc.label)
	}
}
