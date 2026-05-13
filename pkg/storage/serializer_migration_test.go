package storage

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// writeLegacyGobNode is a test helper that writes a header-less gob body
// for a node directly under the prefixNode key — what pre-msgpack engines
// produced. The migration tool's whole purpose is to rewrite these.
func writeLegacyGobNode(t *testing.T, db *badger.DB, node *Node) {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(node))
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(nodeKey(node.ID), buf.Bytes())
	}))
}

func writeLegacyGobEdge(t *testing.T, db *badger.DB, edge *Edge) {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(edge))
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(edgeKey(edge.ID), buf.Bytes())
	}))
}

func TestMigrateBadgerToMsgpack_GobToMsgpack(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)

	writeLegacyGobNode(t, db, &Node{
		ID:         NodeID("test:node-1"),
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice"},
	})
	writeLegacyGobNode(t, db, &Node{
		ID:         NodeID("test:node-2"),
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Bob"},
	})
	writeLegacyGobEdge(t, db, &Edge{
		ID:        EdgeID("test:edge-1"),
		StartNode: NodeID("test:node-1"),
		EndNode:   NodeID("test:node-2"),
		Type:      "KNOWS",
	})

	stats, err := MigrateBadgerToMsgpackWithDB(db, dir, SerializerMigrationOptions{
		BatchSize: 10,
	})
	require.NoError(t, err)
	require.True(t, stats.HasLegacyData)
	require.Greater(t, stats.NodesConverted+stats.EdgesConverted, 0)

	// Re-running the migration finds nothing left to convert.
	stats2, err := MigrateBadgerToMsgpackWithDB(db, dir, SerializerMigrationOptions{
		BatchSize: 10,
	})
	require.NoError(t, err)
	require.Equal(t, 0, stats2.NodesConverted+stats2.EdgesConverted+stats2.EmbeddingsConverted)
	require.Greater(t, stats2.SkippedExisting, 0)
	require.NoError(t, db.Close())

	// Reopen via the engine and confirm reads still work.
	base2, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)
	defer base2.Close()
	engine2 := NewNamespacedEngine(base2, "test")
	node, err := engine2.GetNode(NodeID("node-1"))
	require.NoError(t, err)
	require.Equal(t, NodeID("node-1"), node.ID)
}

func TestMigrateBadgerToMsgpackWithDB_EdgeCases(t *testing.T) {
	t.Run("nil db fails fast", func(t *testing.T) {
		_, err := MigrateBadgerToMsgpackWithDB(nil, "", SerializerMigrationOptions{})
		require.ErrorContains(t, err, "nil badger db")
	})

	t.Run("empty database reports no legacy data", func(t *testing.T) {
		dir := t.TempDir()
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		stats, err := MigrateBadgerToMsgpackWithDB(db, dir, SerializerMigrationOptions{})
		require.NoError(t, err)
		require.False(t, stats.HasLegacyData)
		require.Equal(t, 0, stats.TotalScanned)
	})

	t.Run("dry run counts conversions without rewriting payloads", func(t *testing.T) {
		dir := t.TempDir()
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		writeLegacyGobNode(t, db, &Node{
			ID:         NodeID("test:dry-run-node"),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"name": "draft"},
		})

		before, hasData, err := detectStoredSerializer(db)
		require.NoError(t, err)
		require.True(t, hasData)
		require.Equal(t, detectedGob, before)

		stats, err := MigrateBadgerToMsgpackWithDB(db, dir, SerializerMigrationOptions{
			DryRun: true,
		})
		require.NoError(t, err)
		require.True(t, stats.HasLegacyData)
		require.Greater(t, stats.NodesConverted, 0)
		require.Equal(t, 0, stats.SkippedExisting)

		after, hasData, err := detectStoredSerializer(db)
		require.NoError(t, err)
		require.True(t, hasData)
		require.Equal(t, detectedGob, after)
	})
}

func TestDetectStoredSerializer(t *testing.T) {
	t.Run("nil db and empty db", func(t *testing.T) {
		_, _, err := detectStoredSerializer(nil)
		require.ErrorContains(t, err, "nil badger db")

		dir := t.TempDir()
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		serializer, hasData, err := detectStoredSerializer(db)
		require.NoError(t, err)
		require.False(t, hasData)
		require.Equal(t, detectedNone, serializer)
	})

	t.Run("detects gob and msgpack payloads", func(t *testing.T) {
		gobDir := t.TempDir()
		gobDB, err := badger.Open(badger.DefaultOptions(gobDir).WithLogger(nil))
		require.NoError(t, err)
		writeLegacyGobNode(t, gobDB, &Node{ID: "detect:gob", Labels: []string{"Test"}})

		serializer, hasData, err := detectStoredSerializer(gobDB)
		require.NoError(t, err)
		require.True(t, hasData)
		require.Equal(t, detectedGob, serializer)
		require.NoError(t, gobDB.Close())

		msgpackDir := t.TempDir()
		msgpackEngine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: msgpackDir})
		require.NoError(t, err)
		t.Cleanup(func() { _ = msgpackEngine.Close() })
		_, err = msgpackEngine.CreateNode(&Node{ID: "detect:msgpack", Labels: []string{"Test"}})
		require.NoError(t, err)

		serializer, hasData, err = detectStoredSerializer(msgpackEngine.db)
		require.NoError(t, err)
		require.True(t, hasData)
		require.Equal(t, detectedMsgpack, serializer)
	})
}

func TestMigrateBadgerToMsgpack_DryRun(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)

	writeLegacyGobNode(t, db, &Node{
		ID:         NodeID("test:dry1"),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	})
	require.NoError(t, db.Close())

	stats, err := MigrateBadgerToMsgpack(dir, SerializerMigrationOptions{DryRun: true, BatchSize: 10})
	require.NoError(t, err)
	require.True(t, stats.HasLegacyData)
	require.Greater(t, stats.NodesConverted, 0)
}

func TestMigrateBadgerToMsgpack_InvalidDir(t *testing.T) {
	_, err := MigrateBadgerToMsgpack("/nonexistent/badger/dir", SerializerMigrationOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "open badger")
}
