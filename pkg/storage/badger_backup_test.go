package storage

import (
	"fmt"
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
		engine, err := NewBadgerEngine(dbDir)
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
		engine, err := NewBadgerEngine(dbDir)
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
		engine, err := NewBadgerEngine(dbDir)
		require.NoError(t, err)
		defer engine.Close()

		// Try to backup to invalid path
		err = engine.Backup("/nonexistent/dir/backup.bin")
		assert.Error(t, err)
	})

	t.Run("backup closed engine", func(t *testing.T) {
		dbDir := t.TempDir()
		backupPath := filepath.Join(t.TempDir(), "backup.bin")

		engine, err := NewBadgerEngine(dbDir)
		require.NoError(t, err)
		engine.Close()

		// Try to backup closed engine
		err = engine.Backup(backupPath)
		assert.ErrorIs(t, err, ErrStorageClosed)
	})
}

func generateUniqueTestID() string {
	id := atomic.AddInt64(&testIDCounter, 1)
	return prefixTestID(fmt.Sprintf("test-backup-%d-%d", os.Getpid(), id))
}
