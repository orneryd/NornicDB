package storage

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testIDCounter int64

func TestBadgerEngine_Backup(t *testing.T) {
	t.Run("backup empty database", func(t *testing.T) {
		// Create temporary directory for database
		dbDir := t.TempDir()
		backupPath := filepath.Join(t.TempDir(), "backup.bin")

		// Create engine
		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dbDir))
		require.NoError(t, err)
		defer engine.Close()

		// Create backup
		err = engine.Backup(backupPath)
		require.NoError(t, err)

		// Verify backup file exists
		info, err := os.Stat(backupPath)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, info.Size(), int64(0))
	})

	t.Run("backup with data", func(t *testing.T) {
		// Create temporary directory for database
		dbDir := t.TempDir()
		backupPath := filepath.Join(t.TempDir(), "backup.bin")

		// Create engine and add data
		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dbDir))
		require.NoError(t, err)
		defer engine.Close()

		// Add test nodes
		for i := 0; i < 100; i++ {
			node := &Node{
				ID:         NodeID(generateUniqueTestID()),
				Labels:     []string{"TestNode"},
				Properties: map[string]interface{}{"index": i, "name": "test"},
			}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
		}

		// Add test edges
		nodes, _ := engine.GetNodesByLabel("TestNode")
		for i := 0; i < len(nodes)-1; i++ {
			edge := &Edge{
				ID:         EdgeID(generateUniqueTestID()),
				StartNode:  nodes[i].ID,
				EndNode:    nodes[i+1].ID,
				Type:       "CONNECTS",
				Properties: map[string]interface{}{"weight": i},
			}
			require.NoError(t, engine.CreateEdge(edge))
		}

		// Create backup
		err = engine.Backup(backupPath)
		require.NoError(t, err)

		// Verify backup file exists and has content
		info, err := os.Stat(backupPath)
		require.NoError(t, err)
		assert.Greater(t, info.Size(), int64(1000)) // Should be substantial
	})

	t.Run("backup to invalid path", func(t *testing.T) {
		dbDir := t.TempDir()
		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dbDir))
		require.NoError(t, err)
		defer engine.Close()

		// Try to backup to invalid path
		err = engine.Backup("/nonexistent/dir/backup.bin")
		assert.Error(t, err)
	})

	t.Run("backup closed engine", func(t *testing.T) {
		dbDir := t.TempDir()
		backupPath := filepath.Join(t.TempDir(), "backup.bin")

		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dbDir))
		if engine != nil {
			fixtureowner.Own(t, engine)
		}
		require.NoError(t, err)
		engine.Close()

		// Try to backup closed engine
		err = engine.Backup(backupPath)
		assert.ErrorIs(t, err, ErrStorageClosed)
	})
}

func TestBadgerEngine_RestoreNativeStream(t *testing.T) {
	sourceDir := t.TempDir()
	source, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(sourceDir))
	require.NoError(t, err)
	defer source.Close()

	nodeID := NodeID("backup:n1")
	_, err = source.CreateNode(&Node{
		ID:         nodeID,
		Labels:     []string{"Backup"},
		Properties: map[string]interface{}{"name": "source"},
	})
	require.NoError(t, err)
	require.NoError(t, source.CreateEdge(&Edge{
		ID:        EdgeID("backup:e1"),
		StartNode: nodeID,
		EndNode:   nodeID,
		Type:      "SELF",
	}))

	backupPath := filepath.Join(t.TempDir(), "nornicdb.backup")
	require.NoError(t, source.Backup(backupPath))
	backupBytes, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	require.NotEmpty(t, backupBytes)
	require.NotEqual(t, byte('{'), backupBytes[0])

	target, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	defer target.Close()
	require.NoError(t, target.Restore(backupPath))

	node, err := target.GetNode(nodeID)
	require.NoError(t, err)
	require.Equal(t, "source", node.Properties["name"])
	edges, err := target.GetOutgoingEdges(nodeID)
	require.NoError(t, err)
	require.Len(t, edges, 1)
}

func TestBadgerEngine_RestoreReloadsSchema(t *testing.T) {
	source, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	defer source.Close()

	sourceSchema := source.GetSchemaForNamespace("nornic")
	require.NoError(t, sourceSchema.AddUniqueConstraint("doc_key", "Doc", "key"))
	_, err = source.CreateNode(&Node{
		ID: NodeID("nornic:doc1"), Labels: []string{"Doc"},
		Properties: map[string]interface{}{"key": "k1"},
	})
	require.NoError(t, err)

	backupPath := filepath.Join(t.TempDir(), "schema.backup")
	require.NoError(t, source.Backup(backupPath))

	targetDir := t.TempDir()
	target, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(targetDir))
	if target != nil {
		fixtureowner.Own(t, target)
	}
	require.NoError(t, err)
	require.NoError(t, target.Restore(backupPath))
	require.Len(t, target.GetSchemaForNamespace("nornic").ExportDefinition().Constraints, 1)
	_, err = target.CreateNode(&Node{
		ID: NodeID("nornic:doc2"), Labels: []string{"Doc"},
		Properties: map[string]interface{}{"key": "k1"},
	})
	require.Error(t, err)
	require.NoError(t, target.Close())

	reopened, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(targetDir))
	require.NoError(t, err)
	defer reopened.Close()
	require.Len(t, reopened.GetSchemaForNamespace("nornic").ExportDefinition().Constraints, 1)
}

func TestBadgerEngine_RestoreReplacesTargetSystemRecords(t *testing.T) {
	source, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	defer source.Close()
	_, err = source.CreateNode(&Node{
		ID: NodeID("system:source"), Properties: map[string]interface{}{"source_key": "source"},
	})
	require.NoError(t, err)
	backupPath := filepath.Join(t.TempDir(), "system.backup")
	require.NoError(t, source.Backup(backupPath))

	targetDir := t.TempDir()
	target, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(targetDir))
	if target != nil {
		fixtureowner.Own(t, target)
	}
	require.NoError(t, err)
	_, err = target.CreateNode(&Node{
		ID: NodeID("system:target"), Properties: map[string]interface{}{"target_key": "target"},
	})
	require.NoError(t, err)
	require.NoError(t, target.Restore(backupPath))
	_, err = target.GetNode(NodeID("system:target"))
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, target.Close())

	reopened, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(targetDir))
	require.NoError(t, err)
	defer reopened.Close()
	_, err = reopened.GetNode(NodeID("system:source"))
	require.NoError(t, err)
	_, err = reopened.GetNode(NodeID("system:target"))
	require.ErrorIs(t, err, ErrNotFound)
}

func generateUniqueTestID() string {
	id := atomic.AddInt64(&testIDCounter, 1)
	return prefixTestID(fmt.Sprintf("test-backup-%d-%d", os.Getpid(), id))
}
