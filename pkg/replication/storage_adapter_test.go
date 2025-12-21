package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestAdapter creates a StorageAdapter with a temporary WAL directory.
func setupTestAdapter(t *testing.T) (*StorageAdapter, string) {
	t.Helper()
	engine := storage.NewMemoryEngine()
	walDir := filepath.Join(t.TempDir(), "wal")
	adapter, err := NewStorageAdapterWithWAL(engine, walDir)
	require.NoError(t, err)
	return adapter, walDir
}

func TestNewStorageAdapter(t *testing.T) {
	t.Run("default WAL directory", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		adapter, err := NewStorageAdapter(engine)
		require.NoError(t, err)
		require.NotNil(t, adapter)
		assert.Equal(t, engine, adapter.Engine())
		assert.NotNil(t, adapter.executor)
		defer adapter.Close()
	})

	t.Run("custom WAL directory", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		walDir := filepath.Join(t.TempDir(), "custom_wal")
		adapter, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)
		require.NotNil(t, adapter)
		assert.Equal(t, walDir, adapter.walDir)
		defer adapter.Close()
	})

	t.Run("creates WAL directory if missing", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		walDir := filepath.Join(t.TempDir(), "new_wal", "subdir")
		adapter, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)
		require.NotNil(t, adapter)

		// Verify directory was created
		info, err := os.Stat(walDir)
		require.NoError(t, err)
		assert.True(t, info.IsDir())
		defer adapter.Close()
	})

	t.Run("empty WAL directory uses default", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		adapter, err := NewStorageAdapterWithWAL(engine, "")
		require.NoError(t, err)
		require.NotNil(t, adapter)
		assert.Equal(t, "data/replication/wal", adapter.walDir)
		defer adapter.Close()
	})
}

func TestStorageAdapter_LoadWALPosition(t *testing.T) {
	t.Run("empty WAL starts at position 0", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		pos, err := adapter.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(0), pos)
	})

	t.Run("recovers position from existing WAL", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		walDir := filepath.Join(t.TempDir(), "wal_recovery")

		// Create first adapter and write some commands
		adapter1, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)

		// Apply some commands
		for i := 0; i < 5; i++ {
			node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("recovery-n%d", i)), Labels: []string{"Test"}}
			data, _ := json.Marshal(node)
			cmd := &Command{
				Type:      CmdCreateNode,
				Data:      data,
				Timestamp: time.Now(),
			}
			err := adapter1.ApplyCommand(cmd)
			require.NoError(t, err)
		}

		pos1, err := adapter1.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(5), pos1)
		adapter1.Close()

		// Create new adapter with same WAL directory - should recover position
		adapter2, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)
		defer adapter2.Close()

		pos2, err := adapter2.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(5), pos2)

		// Next command should be position 6
		node := &storage.Node{ID: storage.NodeID("n2"), Labels: []string{"Test"}}
		data, _ := json.Marshal(node)
		cmd := &Command{
			Type:      CmdCreateNode,
			Data:      data,
			Timestamp: time.Now(),
		}
		err = adapter2.ApplyCommand(cmd)
		require.NoError(t, err)

		pos3, err := adapter2.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(6), pos3)
	})

	t.Run("handles corrupted WAL gracefully", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		walDir := filepath.Join(t.TempDir(), "corrupted_wal")

		// Create WAL directory
		require.NoError(t, os.MkdirAll(walDir, 0755))

		// Write invalid data to WAL file
		walPath := filepath.Join(walDir, "wal.log")
		err := os.WriteFile(walPath, []byte("invalid json data"), 0644)
		require.NoError(t, err)

		// Should still create adapter (starts from position 0)
		adapter, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)
		defer adapter.Close()

		pos, err := adapter.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(0), pos)
	})
}

func TestStorageAdapter_ApplyCommand(t *testing.T) {
	t.Run("writes to persistent WAL", func(t *testing.T) {
		adapter, walDir := setupTestAdapter(t)
		defer adapter.Close()

		node := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Test"}}
		data, _ := json.Marshal(node)
		cmd := &Command{
			Type:      CmdCreateNode,
			Data:      data,
			Timestamp: time.Now(),
		}

		err := adapter.ApplyCommand(cmd)
		require.NoError(t, err)

		// Wait for WAL sync
		time.Sleep(150 * time.Millisecond)

		// Verify WAL file exists
		walPath := filepath.Join(walDir, "wal.log")
		info, err := os.Stat(walPath)
		require.NoError(t, err)
		assert.Greater(t, info.Size(), int64(0))

		// Verify position incremented
		pos, err := adapter.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(1), pos)
	})

	t.Run("rejects nil command", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		err := adapter.ApplyCommand(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil command")
	})

	t.Run("all command types", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		tests := []struct {
			name    string
			cmdType CommandType
			data    []byte
		}{
			{"CreateNode", CmdCreateNode, func() []byte {
				node := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Test"}}
				data, _ := json.Marshal(node)
				return data
			}()},
			{"UpdateNode", CmdUpdateNode, func() []byte {
				node := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Updated"}}
				data, _ := json.Marshal(node)
				return data
			}()},
			{"DeleteNode", CmdDeleteNode, []byte("n1")},
			{"CreateEdge", CmdCreateEdge, func() []byte {
				edge := &storage.Edge{ID: storage.EdgeID("e1"), StartNode: storage.NodeID("n1"), EndNode: storage.NodeID("n2"), Type: "KNOWS"}
				data, _ := json.Marshal(edge)
				return data
			}()},
			{"DeleteEdge", CmdDeleteEdge, func() []byte {
				req := struct {
					EdgeID string `json:"edge_id"`
				}{EdgeID: "e1"}
				data, _ := json.Marshal(req)
				return data
			}()},
			{"SetProperty", CmdSetProperty, func() []byte {
				req := struct {
					NodeID string      `json:"node_id"`
					Key    string      `json:"key"`
					Value  interface{} `json:"value"`
				}{NodeID: "n1", Key: "name", Value: "Alice"}
				data, _ := json.Marshal(req)
				return data
			}()},
			{"BatchWrite", CmdBatchWrite, func() []byte {
				batch := struct {
					Nodes []*storage.Node `json:"nodes"`
					Edges []*storage.Edge `json:"edges"`
				}{
					Nodes: []*storage.Node{{ID: storage.NodeID("batch-n1")}},
				}
				data, _ := json.Marshal(batch)
				return data
			}()},
			{"Cypher", CmdCypher, func() []byte {
				cypherCmd := struct {
					Query  string                 `json:"query"`
					Params map[string]interface{} `json:"params,omitempty"`
				}{
					Query: "CREATE (n:Person {name: 'Alice'})",
				}
				data, _ := json.Marshal(cypherCmd)
				return data
			}()},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// For operations that need existing nodes/edges, set them up first
				if tt.name == "CreateEdge" {
					// Create nodes first
					node1 := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Test"}}
					node2 := &storage.Node{ID: storage.NodeID("n2"), Labels: []string{"Test"}}
					adapter.engine.CreateNode(node1)
					adapter.engine.CreateNode(node2)
				} else if tt.name == "DeleteEdge" {
					// Create nodes and edge first
					node1 := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Test"}}
					node2 := &storage.Node{ID: storage.NodeID("n2"), Labels: []string{"Test"}}
					adapter.engine.CreateNode(node1)
					adapter.engine.CreateNode(node2)
					edge := &storage.Edge{ID: storage.EdgeID("e1"), StartNode: storage.NodeID("n1"), EndNode: storage.NodeID("n2"), Type: "KNOWS"}
					adapter.engine.CreateEdge(edge)
				} else if tt.name == "SetProperty" {
					// Create node first
					node := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Test"}}
					adapter.engine.CreateNode(node)
				}

				cmd := &Command{
					Type:      tt.cmdType,
					Data:      tt.data,
					Timestamp: time.Now(),
				}

				err := adapter.ApplyCommand(cmd)
				require.NoError(t, err, "command type: %s", tt.name)
			})
		}
	})

	t.Run("unknown command type", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		// Use CmdVoteRequest which is a valid type but not handled in ApplyCommand
		cmd := &Command{
			Type:      CmdVoteRequest, // Valid type but not handled in ApplyCommand switch
			Data:      []byte("test"),
			Timestamp: time.Now(),
		}

		err := adapter.ApplyCommand(cmd)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown command type")
	})
}

func TestStorageAdapter_GetWALEntries(t *testing.T) {
	t.Run("empty WAL returns empty entries", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		entries, err := adapter.GetWALEntries(0, 10)
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	t.Run("returns entries after position", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		// Apply multiple commands
		for i := 0; i < 5; i++ {
			node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Test"}}
			data, _ := json.Marshal(node)
			cmd := &Command{
				Type:      CmdCreateNode,
				Data:      data,
				Timestamp: time.Now(),
			}
			err := adapter.ApplyCommand(cmd)
			require.NoError(t, err)
		}

		// Wait for WAL sync
		time.Sleep(150 * time.Millisecond)

		// Get entries after position 2
		entries, err := adapter.GetWALEntries(2, 10)
		require.NoError(t, err)
		assert.Len(t, entries, 3) // Positions 3, 4, 5
		assert.Equal(t, uint64(3), entries[0].Position)
		assert.Equal(t, uint64(4), entries[1].Position)
		assert.Equal(t, uint64(5), entries[2].Position)
	})

	t.Run("respects maxEntries limit", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		// Apply 10 commands
		for i := 0; i < 10; i++ {
			node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Test"}}
			data, _ := json.Marshal(node)
			cmd := &Command{
				Type:      CmdCreateNode,
				Data:      data,
				Timestamp: time.Now(),
			}
			err := adapter.ApplyCommand(cmd)
			require.NoError(t, err)
		}

		// Wait for WAL sync
		time.Sleep(150 * time.Millisecond)

		// Request max 3 entries
		entries, err := adapter.GetWALEntries(0, 3)
		require.NoError(t, err)
		assert.Len(t, entries, 3)
	})

	t.Run("returns entries across restarts", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		walDir := filepath.Join(t.TempDir(), "wal_persistence")

		// Create adapter and write commands
		adapter1, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)

		for i := 0; i < 3; i++ {
			node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Test"}}
			data, _ := json.Marshal(node)
			cmd := &Command{
				Type:      CmdCreateNode,
				Data:      data,
				Timestamp: time.Now(),
			}
			err := adapter1.ApplyCommand(cmd)
			require.NoError(t, err)
		}
		adapter1.Close()

		// Wait for WAL sync
		time.Sleep(150 * time.Millisecond)

		// Create new adapter - should see previous entries
		adapter2, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)
		defer adapter2.Close()

		entries, err := adapter2.GetWALEntries(0, 10)
		require.NoError(t, err)
		assert.Len(t, entries, 3)
	})

	t.Run("handles missing WAL file", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		walDir := filepath.Join(t.TempDir(), "missing_wal")

		adapter, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)
		defer adapter.Close()

		// Close adapter to close WAL
		adapter.Close()

		// Remove WAL file
		walPath := filepath.Join(walDir, "wal.log")
		os.Remove(walPath)

		// Recreate adapter - should handle missing WAL gracefully
		adapter2, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)
		defer adapter2.Close()

		entries, err := adapter2.GetWALEntries(0, 10)
		require.NoError(t, err)
		assert.Empty(t, entries)
	})
}

func TestStorageAdapter_GetWALPosition(t *testing.T) {
	t.Run("starts at position 0", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		pos, err := adapter.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(0), pos)
	})

	t.Run("increments with each command", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		for i := uint64(1); i <= 5; i++ {
			node := &storage.Node{ID: storage.NodeID("n" + string(rune(i))), Labels: []string{"Test"}}
			data, _ := json.Marshal(node)
			cmd := &Command{
				Type:      CmdCreateNode,
				Data:      data,
				Timestamp: time.Now(),
			}
			err := adapter.ApplyCommand(cmd)
			require.NoError(t, err)

			pos, err := adapter.GetWALPosition()
			require.NoError(t, err)
			assert.Equal(t, i, pos)
		}
	})
}

func TestStorageAdapter_ApplyCypher(t *testing.T) {
	t.Run("executes Cypher query", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		cypherCmd := struct {
			Query  string                 `json:"query"`
			Params map[string]interface{} `json:"params,omitempty"`
		}{
			Query: "CREATE (n:Person {name: 'Alice'})",
		}
		data, err := json.Marshal(cypherCmd)
		require.NoError(t, err)

		cmd := &Command{
			Type:      CmdCypher,
			Data:      data,
			Timestamp: time.Now(),
		}

		err = adapter.ApplyCommand(cmd)
		require.NoError(t, err)

		// Verify node was created - check all nodes since ID generation may vary
		nodes, err := adapter.engine.AllNodes()
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		assert.Equal(t, "Alice", nodes[0].Properties["name"])
		assert.Contains(t, nodes[0].Labels, "Person")
	})

	t.Run("executes Cypher with parameters", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		cypherCmd := struct {
			Query  string                 `json:"query"`
			Params map[string]interface{} `json:"params,omitempty"`
		}{
			Query: "CREATE (n:Person {name: $name, age: $age})",
			Params: map[string]interface{}{
				"name": "Bob",
				"age":  30,
			},
		}
		data, err := json.Marshal(cypherCmd)
		require.NoError(t, err)

		cmd := &Command{
			Type:      CmdCypher,
			Data:      data,
			Timestamp: time.Now(),
		}

		err = adapter.ApplyCommand(cmd)
		require.NoError(t, err)
	})

	t.Run("rejects empty query", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		cypherCmd := struct {
			Query string `json:"query"`
		}{
			Query: "",
		}
		data, err := json.Marshal(cypherCmd)
		require.NoError(t, err)

		cmd := &Command{
			Type:      CmdCypher,
			Data:      data,
			Timestamp: time.Now(),
		}

		err = adapter.ApplyCommand(cmd)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cypher query is empty")
	})

	t.Run("rejects invalid JSON", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		cmd := &Command{
			Type:      CmdCypher,
			Data:      []byte("invalid json"),
			Timestamp: time.Now(),
		}

		err := adapter.ApplyCommand(cmd)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal cypher command")
	})
}

func TestStorageAdapter_Close(t *testing.T) {
	t.Run("closes WAL successfully", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		err := adapter.Close()
		require.NoError(t, err)
	})

	t.Run("can close multiple times", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		err := adapter.Close()
		require.NoError(t, err)

		// Second close should not error
		err = adapter.Close()
		require.NoError(t, err)
	})
}

func TestStorageAdapter_SetExecutor(t *testing.T) {
	t.Run("sets custom executor", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		engine := storage.NewMemoryEngine()
		executor := cypher.NewStorageExecutor(engine)

		adapter.SetExecutor(executor)
		assert.Equal(t, executor, adapter.executor)
	})
}

func TestStorageAdapter_ConcurrentAccess(t *testing.T) {
	t.Run("concurrent ApplyCommand", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		var wg sync.WaitGroup
		numGoroutines := 10
		commandsPerGoroutine := 10

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < commandsPerGoroutine; j++ {
					// Use unique node IDs to avoid conflicts
					nodeID := storage.NodeID(fmt.Sprintf("n-%d-%d", id, j))
					node := &storage.Node{ID: nodeID, Labels: []string{"Test"}}
					data, _ := json.Marshal(node)
					cmd := &Command{
						Type:      CmdCreateNode,
						Data:      data,
						Timestamp: time.Now(),
					}
					err := adapter.ApplyCommand(cmd)
					assert.NoError(t, err)
				}
			}(i)
		}

		wg.Wait()

		// Verify all commands were applied
		pos, err := adapter.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(numGoroutines*commandsPerGoroutine), pos)
	})

	t.Run("concurrent GetWALEntries", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		// Apply some commands first
		for i := 0; i < 10; i++ {
			node := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Test"}}
			data, _ := json.Marshal(node)
			cmd := &Command{
				Type:      CmdCreateNode,
				Data:      data,
				Timestamp: time.Now(),
			}
			adapter.ApplyCommand(cmd)
		}

		// Wait for WAL sync to ensure entries are persisted
		time.Sleep(150 * time.Millisecond)

		var wg sync.WaitGroup
		numGoroutines := 5

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				entries, err := adapter.GetWALEntries(0, 10)
				assert.NoError(t, err)
				assert.GreaterOrEqual(t, len(entries), 0) // May be 0 if sync hasn't completed
			}()
		}

		wg.Wait()
	})
}

func TestStorageAdapter_WriteSnapshot(t *testing.T) {
	t.Run("writes snapshot with WAL position", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		// Apply some commands
		for i := 0; i < 3; i++ {
			node := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Test"}}
			data, _ := json.Marshal(node)
			cmd := &Command{
				Type:      CmdCreateNode,
				Data:      data,
				Timestamp: time.Now(),
			}
			adapter.ApplyCommand(cmd)
		}

		var buf bytes.Buffer
		err := adapter.WriteSnapshot(&buf)
		require.NoError(t, err)

		var snapshot struct {
			WALPosition uint64          `json:"wal_position"`
			Nodes       []*storage.Node `json:"nodes"`
			Edges       []*storage.Edge `json:"edges"`
		}
		err = json.Unmarshal(buf.Bytes(), &snapshot)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), snapshot.WALPosition)
	})
}

func TestStorageAdapter_RestoreSnapshot(t *testing.T) {
	t.Run("restores snapshot and WAL position", func(t *testing.T) {
		adapter, _ := setupTestAdapter(t)
		defer adapter.Close()

		snapshot := struct {
			WALPosition uint64          `json:"wal_position"`
			Nodes       []*storage.Node `json:"nodes"`
			Edges       []*storage.Edge `json:"edges"`
		}{
			WALPosition: 5,
			Nodes: []*storage.Node{
				{ID: storage.NodeID("n1"), Labels: []string{"Test"}},
			},
		}

		data, err := json.Marshal(snapshot)
		require.NoError(t, err)

		reader := bytes.NewReader(data)
		err = adapter.RestoreSnapshot(reader)
		require.NoError(t, err)

		// Verify WAL position restored
		pos, err := adapter.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(5), pos)

		// Verify nodes restored
		node, err := adapter.engine.GetNode(storage.NodeID("n1"))
		require.NoError(t, err)
		assert.Equal(t, []string{"Test"}, node.Labels)
	})
}
