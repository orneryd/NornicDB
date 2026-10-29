package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"os/exec"
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
// defaultWALDirProbeEnv marks the re-executed child process used to verify the
// relative default WAL directory contract outside the package working directory.
const defaultWALDirProbeEnv = "NORNICDB_REPLICATION_DEFAULT_WAL_DIR_PROBE"

type adapterFixtureOptions struct {
	backing    storage.Engine
	standalone bool
	timeout    time.Duration
}
type adapterFixture struct {
	adapter *StorageAdapter
	walDir  string
	engine  *ReplicatedEngine
}

func setupTestAdapter(t *testing.T, options ...adapterFixtureOptions) *adapterFixture {
	t.Helper()
	var option adapterFixtureOptions
	if len(options) > 0 {
		option = options[0]
	}
	backing := option.backing
	if backing == nil {
		native := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
		backing = storage.NewNamespacedEngine(native, "test")
	}
	walDir := filepath.Join(t.TempDir(), "wal")
	adapter, err := NewStorageAdapterWithWAL(backing, walDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = adapter.Close() })
	fixture := &adapterFixture{adapter: adapter, walDir: walDir}
	if option.standalone {
		config := DefaultConfig()
		config.Mode = ModeStandalone
		config.NodeID = "test-node"
		replicator, err := NewReplicator(config, adapter)
		require.NoError(t, err)
		t.Cleanup(func() { _ = replicator.Shutdown() })
		require.NoError(t, replicator.Start(context.Background()))
		fixture.engine = NewReplicatedEngine(backing, replicator, option.timeout)
	}
	return fixture
}

func TestNewStorageAdapter(t *testing.T) {
	t.Run("default WAL directory", func(t *testing.T) {
		if os.Getenv(defaultWALDirProbeEnv) == "1" {
			// Child process: the working directory is a scratch dir, so the
			// relative default WAL path is created there and never in the checkout.
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			engine := storage.NewNamespacedEngine(base, "test")
			adapter, err := NewStorageAdapter(engine)
			require.NoError(t, err)
			require.NotNil(t, adapter)
			require.Equal(t, "data/replication/wal", adapter.walDir)
			require.Equal(t, engine, adapter.Engine())

			info, statErr := os.Stat(filepath.Join("data", "replication", "wal", "wal.log"))
			require.NoError(t, statErr, "default WAL file must be created under the working directory")
			require.True(t, info.Mode().IsRegular())

			node := &storage.Node{ID: storage.NodeID("default-dir"), Labels: []string{"Test"}}
			require.NoError(t, adapter.ApplyCommand(&Command{
				Type:      CmdCreateNode,
				Data:      fixtureowner.Encode(t, storage.EncodeNodePayload, node),
				Timestamp: time.Now(),
			}))
			require.NoError(t, adapter.FlushWAL())

			pos, posErr := adapter.GetWALPosition()
			require.NoError(t, posErr)
			require.Equal(t, uint64(1), pos)
			require.NoError(t, adapter.Close())
			return
		}

		// The default WAL directory is the relative path "data/replication/wal",
		// resolved against the process working directory. Running this contract in
		// the package directory would open whatever checkout-local WAL already
		// exists there (possibly written by an older release), so re-exec this
		// subtest with a scratch working directory. The checkout is never opened,
		// created in, or modified, and no pre-existing data is deleted.
		cmd := exec.Command(os.Args[0], "-test.run=^TestNewStorageAdapter$/^default_WAL_directory$", "-test.v")
		cmd.Dir = t.TempDir()
		cmd.Env = append(os.Environ(), defaultWALDirProbeEnv+"=1")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "default WAL directory probe failed:\n%s", out)
		require.Contains(t, string(out), "--- PASS: TestNewStorageAdapter/default_WAL_directory")
	})

	t.Run("custom WAL directory", func(t *testing.T) {
		baseEngine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		walDir := filepath.Join(t.TempDir(), "custom_wal")
		adapter, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)
		require.NotNil(t, adapter)
		assert.Equal(t, walDir, adapter.walDir)
		defer adapter.Close()
	})

	t.Run("creates WAL directory if missing", func(t *testing.T) {
		baseEngine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))

		engine := storage.NewNamespacedEngine(baseEngine, "test")
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
		if os.Getenv(defaultWALDirProbeEnv) == "1" {
			// Child process: an empty walDir must resolve to the relative default
			// path under the (scratch) working directory, never the checkout.
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			engine := storage.NewNamespacedEngine(base, "test")
			adapter, err := NewStorageAdapterWithWAL(engine, "")
			require.NoError(t, err)
			require.NotNil(t, adapter)
			require.Equal(t, "data/replication/wal", adapter.walDir)
			walPath := filepath.Join("data", "replication", "wal", "wal.log")
			info, statErr := os.Stat(walPath)
			require.NoError(t, statErr)
			require.True(t, info.Mode().IsRegular())
			require.NoError(t, adapter.Close())
			return
		}

		// Same relative-default-path contract as the subtest above; run it in a
		// scratch working directory so a checkout-local WAL is never opened.
		cmd := exec.Command(os.Args[0], "-test.run=^TestNewStorageAdapter$/^empty_WAL_directory_uses_default$", "-test.v")
		cmd.Dir = t.TempDir()
		cmd.Env = append(os.Environ(), defaultWALDirProbeEnv+"=1")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "empty WAL directory probe failed:\n%s", out)
		require.Contains(t, string(out), "--- PASS: TestNewStorageAdapter/empty_WAL_directory_uses_default")
	})
}

func TestStorageAdapter_ApplyCommand(t *testing.T) {
	t.Run("writes to persistent WAL", func(t *testing.T) {
		adapterState := setupTestAdapter(t)
		adapter, walDir := adapterState.adapter, adapterState.walDir
		defer adapter.Close()

		node := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Test"}}
		data := fixtureowner.Encode(t, storage.EncodeNodePayload, node)
		cmd := &Command{
			Type:      CmdCreateNode,
			Data:      data,
			Timestamp: time.Now(),
		}

		err := adapter.ApplyCommand(cmd)
		require.NoError(t, err)

		// Flush WAL to ensure async write completes
		require.NoError(t, adapter.FlushWAL())

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
		adapter := setupTestAdapter(t).adapter
		defer adapter.Close()

		err := adapter.ApplyCommand(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil command")
	})

	t.Run("all command types", func(t *testing.T) {
		adapter := setupTestAdapter(t).adapter
		defer adapter.Close()

		tests := []struct {
			name    string
			cmdType CommandType
			data    []byte
		}{
			{"CreateNode", CmdCreateNode, func() []byte {
				node := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Test"}}
				return fixtureowner.Encode(t, storage.EncodeNodePayload, node)
			}()},
			{"UpdateNode", CmdUpdateNode, func() []byte {
				node := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Updated"}}
				return fixtureowner.Encode(t, storage.EncodeNodePayload, node)
			}()},
			{"DeleteNode", CmdDeleteNode, []byte("n1")},
			{"CreateEdge", CmdCreateEdge, func() []byte {
				edge := &storage.Edge{ID: storage.EdgeID("e1"), StartNode: storage.NodeID("n1"), EndNode: storage.NodeID("n2"), Type: "KNOWS"}
				return fixtureowner.Encode(t, storage.EncodeEdgePayload, edge)
			}()},
			{"DeleteEdge", CmdDeleteEdge, func() []byte {
				req := struct {
					EdgeID string
				}{EdgeID: "e1"}
				return fixtureowner.Encode[any](t, storage.EncodeCommandPayload, req)
			}()},
			{"SetProperty", cmdRetiredSetProperty, func() []byte {
				req := struct {
					NodeID string
					Key    string
					Value  interface{}
				}{NodeID: "n1", Key: "name", Value: "Alice"}
				return fixtureowner.Encode[any](t, storage.EncodeCommandPayload, req)
			}()},
			{"BatchWrite", cmdRetiredBatchWrite, func() []byte {
				nodeBytes := fixtureowner.Encode(t, storage.EncodeNodePayload, &storage.Node{ID: storage.NodeID("batch-n1")})
				batch := struct {
					Nodes [][]byte
					Edges [][]byte
				}{
					Nodes: [][]byte{nodeBytes},
				}
				return fixtureowner.Encode[any](t, storage.EncodeCommandPayload, batch)
			}()},
			{"Cypher", cmdRetiredCypher, func() []byte {
				cypherCmd := struct {
					Query  string
					Params map[string]interface{}
				}{
					Query: "CREATE (n:Person {name: 'Alice'})",
				}
				return fixtureowner.Encode[any](t, storage.EncodeCommandPayload, cypherCmd)
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
				if isRetiredCommand(tt.cmdType) {
					require.ErrorIs(t, err, ErrRetiredCommand)
				} else {
					require.NoError(t, err, "command type: %s", tt.name)
				}
			})
		}
	})

	t.Run("unknown command type", func(t *testing.T) {
		adapter := setupTestAdapter(t).adapter
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
		adapter := setupTestAdapter(t).adapter
		defer adapter.Close()

		entries, err := adapter.GetWALEntries(0, 10)
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	t.Run("returns entries after position", func(t *testing.T) {
		adapter := setupTestAdapter(t).adapter
		defer adapter.Close()

		// Apply multiple commands
		for i := 0; i < 5; i++ {
			node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Test"}}
			data := fixtureowner.Encode(t, storage.EncodeNodePayload, node)
			cmd := &Command{
				Type:      CmdCreateNode,
				Data:      data,
				Timestamp: time.Now(),
			}
			err := adapter.ApplyCommand(cmd)
			require.NoError(t, err)
		}

		require.NoError(t, adapter.FlushWAL())

		// Get entries after position 2
		entries, err := adapter.GetWALEntries(2, 10)
		require.NoError(t, err)
		assert.Len(t, entries, 3) // Positions 3, 4, 5
		assert.Equal(t, uint64(3), entries[0].Position)
		assert.Equal(t, uint64(4), entries[1].Position)
		assert.Equal(t, uint64(5), entries[2].Position)
	})

	t.Run("respects maxEntries limit", func(t *testing.T) {
		adapter := setupTestAdapter(t).adapter
		defer adapter.Close()

		// Apply 10 commands
		for i := 0; i < 10; i++ {
			node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Test"}}
			data := fixtureowner.Encode(t, storage.EncodeNodePayload, node)
			cmd := &Command{
				Type:      CmdCreateNode,
				Data:      data,
				Timestamp: time.Now(),
			}
			err := adapter.ApplyCommand(cmd)
			require.NoError(t, err)
		}

		require.NoError(t, adapter.FlushWAL())

		// Request max 3 entries
		entries, err := adapter.GetWALEntries(0, 3)
		require.NoError(t, err)
		assert.Len(t, entries, 3)
	})

	t.Run("returns entries across restarts", func(t *testing.T) {
		baseEngine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		walDir := filepath.Join(t.TempDir(), "wal_persistence")

		// Create adapter and write commands
		adapter1, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)

		for i := 0; i < 3; i++ {
			node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Test"}}
			data := fixtureowner.Encode(t, storage.EncodeNodePayload, node)
			cmd := &Command{
				Type:      CmdCreateNode,
				Data:      data,
				Timestamp: time.Now(),
			}
			err := adapter1.ApplyCommand(cmd)
			require.NoError(t, err)
		}
		require.NoError(t, adapter1.FlushWAL())
		require.NoError(t, adapter1.Close())

		// Create new adapter - should see previous entries
		adapter2, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)
		defer adapter2.Close()

		entries, err := adapter2.GetWALEntries(0, 10)
		require.NoError(t, err)
		assert.Len(t, entries, 3)
	})

	t.Run("handles missing WAL file", func(t *testing.T) {
		baseEngine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))

		engine := storage.NewNamespacedEngine(baseEngine, "test")
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
		adapter := setupTestAdapter(t).adapter
		defer adapter.Close()

		pos, err := adapter.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(0), pos)
	})

	t.Run("increments with each command", func(t *testing.T) {
		adapter := setupTestAdapter(t).adapter
		defer adapter.Close()

		for i := uint64(1); i <= 5; i++ {
			node := &storage.Node{ID: storage.NodeID("n" + string(rune(i))), Labels: []string{"Test"}}
			data := fixtureowner.Encode(t, storage.EncodeNodePayload, node)
			cmd := &Command{
				Type:      CmdCreateNode,
				Data:      data,
				Timestamp: time.Now(),
			}
			err := adapter.ApplyCommand(cmd)
			require.NoError(t, err)

			// Flush WAL to ensure async write completes
			require.NoError(t, adapter.FlushWAL())

			pos, err := adapter.GetWALPosition()
			require.NoError(t, err)
			assert.Equal(t, i, pos)
		}
	})

	t.Run("recovers position from existing WAL", func(t *testing.T) {
		baseEngine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		walDir := filepath.Join(t.TempDir(), "wal_recovery")

		// Create first adapter and write some commands
		adapter1, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)

		// Apply some commands
		for i := 0; i < 5; i++ {
			node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("recovery-n%d", i)), Labels: []string{"Test"}}
			data := fixtureowner.Encode(t, storage.EncodeNodePayload, node)
			cmd := &Command{
				Type:      CmdCreateNode,
				Data:      data,
				Timestamp: time.Now(),
			}
			err := adapter1.ApplyCommand(cmd)
			require.NoError(t, err)
		}

		// Flush WAL to ensure all async writes complete
		require.NoError(t, adapter1.FlushWAL())

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
		data := fixtureowner.Encode(t, storage.EncodeNodePayload, node)
		cmd := &Command{
			Type:      CmdCreateNode,
			Data:      data,
			Timestamp: time.Now(),
		}
		err = adapter2.ApplyCommand(cmd)
		require.NoError(t, err)

		// Flush WAL to ensure async write completes
		require.NoError(t, adapter2.FlushWAL())

		pos3, err := adapter2.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(6), pos3)
	})

	t.Run("handles corrupted WAL gracefully", func(t *testing.T) {
		// The recovery/error contract under test:
		//   * an active WAL whose tail is genuinely damaged in the *current*
		//     format (v5 [magic][version][len][payload][crc][trailer]) is
		//     repaired by truncating only the damaged tail, and
		//   * the intact prefix survives, so the recovered position is the
		//     last complete record rather than 0,
		//   * while an unreadable file header is still rejected instead of
		//     being silently discarded (old/unknown formats are never
		//     reinterpreted).
		// Records are produced by a real writer so the fixture is v5, not a
		// hand-rolled imitation of it.
		baseEngine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		walDir := filepath.Join(t.TempDir(), "corrupted_wal")

		writer, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)
		for i := 0; i < 2; i++ {
			node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("corrupt-n%d", i)), Labels: []string{"Test"}}
			require.NoError(t, writer.ApplyCommand(&Command{
				Type:      CmdCreateNode,
				Data:      fixtureowner.Encode(t, storage.EncodeNodePayload, node),
				Timestamp: time.Now(),
			}))
		}
		require.NoError(t, writer.FlushWAL())
		require.NoError(t, writer.Close())

		walPath := filepath.Join(walDir, "wal.log")
		good, err := os.ReadFile(walPath)
		require.NoError(t, err)
		require.NotEmpty(t, good)
		goodSize := int64(len(good))

		// Damaged tail: the first 9 bytes of a third record are a valid v5
		// header with a plausible payload length, but the payload/checksum/
		// trailer never made it to disk (torn write). This is exactly what a
		// crash mid-append leaves behind.
		require.GreaterOrEqual(t, len(good), 9)
		torn := append([]byte(nil), good[:9]...)
		require.Positive(t, binary.LittleEndian.Uint32(torn[5:9]))
		require.NoError(t, os.WriteFile(walPath, append(append([]byte{}, good...), torn...), 0600))

		recovered, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err, "torn tail in the current format must be repaired, not rejected")

		pos, err := recovered.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(2), pos, "repair must preserve every complete record")

		// Appending after the repair must succeed: the damaged tail is gone,
		// so the log stays readable.
		node := &storage.Node{ID: storage.NodeID("post-repair"), Labels: []string{"Test"}}
		require.NoError(t, recovered.ApplyCommand(&Command{
			Type:      CmdCreateNode,
			Data:      fixtureowner.Encode(t, storage.EncodeNodePayload, node),
			Timestamp: time.Now(),
		}))
		require.NoError(t, recovered.FlushWAL())
		require.NoError(t, recovered.Close())

		reopened, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)
		defer reopened.Close()
		pos, err = reopened.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(3), pos)

		// Damage inside the last record's payload (CRC mismatch): also a torn
		// tail, and the prefix before it must still be recovered.
		require.NoError(t, reopened.Close())
		current, err := os.ReadFile(walPath)
		require.NoError(t, err)
		require.Greater(t, len(current), len(good))
		damaged := append([]byte{}, current...)
		lastHeader := int(goodSize)
		require.Greater(t, len(damaged), lastHeader+9)
		require.Positive(t, binary.LittleEndian.Uint32(damaged[lastHeader+5:lastHeader+9]))
		damaged[lastHeader+9] ^= 0xFF // the third record's payload, never padding
		require.NoError(t, os.WriteFile(walPath, damaged, 0600))

		repaired, err := NewStorageAdapterWithWAL(engine, walDir)
		require.NoError(t, err)
		pos, err = repaired.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(2), pos, "the record with the bad checksum is dropped, the prefix is kept")
		require.NoError(t, repaired.Close())

		// An unreadable header is not a torn tail: the file cannot be
		// attributed to the current format at all, so startup must fail
		// loudly and leave the bytes untouched. Old formats stay rejected.
		require.NoError(t, os.WriteFile(walPath, []byte("invalid json data"), 0600))
		before, err := os.ReadFile(walPath)
		require.NoError(t, err)

		unreadable, err := NewStorageAdapterWithWAL(engine, walDir)
		if unreadable != nil {
			_ = unreadable.Close()
		}
		require.Error(t, err)
		require.ErrorContains(t, err, "unsupported WAL format")
		after, readErr := os.ReadFile(walPath)
		require.NoError(t, readErr)
		require.Equal(t, before, after, "an unrecognized WAL must never be truncated")

		// Guard against accidental fixture regressions: the records the
		// writer produced really are the current format.
		require.Equal(t, []byte("WALE"), good[:4])
	})
}

func TestStorageAdapter_RetiredCypherAndSupportedQueries(t *testing.T) {
	t.Run("executes Cypher query", func(t *testing.T) {
		adapter := setupTestAdapter(t).adapter
		defer adapter.Close()

		cypherCmd := struct {
			Query  string
			Params map[string]interface{}
		}{
			Query: "CREATE (n:Person {name: 'Alice'})",
		}
		data := fixtureowner.Encode[any](t, storage.EncodeCommandPayload, cypherCmd)

		cmd := &Command{
			Type:      cmdRetiredCypher,
			Data:      data,
			Timestamp: time.Now(),
		}

		require.ErrorIs(t, adapter.ApplyCommand(cmd), ErrRetiredCommand)
		_, err := cypher.NewStorageExecutor(adapter.engine).Execute(context.Background(), cypherCmd.Query, cypherCmd.Params)
		require.NoError(t, err)

		// Verify node was created - check all nodes since ID generation may vary
		nodes, listErr := adapter.engine.AllNodes()
		require.NoError(t, listErr)
		require.Len(t, nodes, 1)
		assert.Equal(t, "Alice", nodes[0].Properties["name"])
		assert.Contains(t, nodes[0].Labels, "Person")
	})

	t.Run("executes Cypher with parameters", func(t *testing.T) {
		adapter := setupTestAdapter(t).adapter
		defer adapter.Close()

		cypherCmd := struct {
			Query  string
			Params map[string]interface{}
		}{
			Query: "CREATE (n:Person {name: $name, age: $age})",
			Params: map[string]interface{}{
				"name": "Bob",
				"age":  30,
			},
		}
		data := fixtureowner.Encode[any](t, storage.EncodeCommandPayload, cypherCmd)

		cmd := &Command{
			Type:      cmdRetiredCypher,
			Data:      data,
			Timestamp: time.Now(),
		}

		require.ErrorIs(t, adapter.ApplyCommand(cmd), ErrRetiredCommand)
		_, err := cypher.NewStorageExecutor(adapter.engine).Execute(context.Background(), cypherCmd.Query, cypherCmd.Params)
		require.NoError(t, err)
	})

	t.Run("rejects empty query", func(t *testing.T) {
		adapter := setupTestAdapter(t).adapter
		defer adapter.Close()

		cypherCmd := struct {
			Query string
		}{
			Query: "",
		}
		data := fixtureowner.Encode[any](t, storage.EncodeCommandPayload, cypherCmd)

		cmd := &Command{
			Type:      cmdRetiredCypher,
			Data:      data,
			Timestamp: time.Now(),
		}

		err := adapter.ApplyCommand(cmd)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrRetiredCommand)
	})

	t.Run("rejects invalid JSON", func(t *testing.T) {
		adapter := setupTestAdapter(t).adapter
		defer adapter.Close()

		cmd := &Command{
			Type:      cmdRetiredCypher,
			Data:      []byte("invalid json"),
			Timestamp: time.Now(),
		}

		err := adapter.ApplyCommand(cmd)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrRetiredCommand)
	})
}

func TestStorageAdapter_Close(t *testing.T) {
	for _, test := range []struct {
		name          string
		walAlreadyNil bool
		closes        int
	}{
		{name: "closes WAL successfully", closes: 1},
		{name: "can close multiple times", closes: 2},
		{name: "WAL already nil", walAlreadyNil: true, closes: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := setupTestAdapter(t).adapter
			if test.walAlreadyNil {
				adapter.walMu.Lock()
				if adapter.wal != nil {
					_ = adapter.wal.Close()
					adapter.wal = nil
				}
				adapter.walMu.Unlock()
			}
			for range test.closes {
				require.NoError(t, adapter.Close())
			}
		})
	}
}

func TestStorageAdapter_ConcurrentAccess(t *testing.T) {
	t.Run("concurrent ApplyCommand", func(t *testing.T) {
		adapter := setupTestAdapter(t).adapter
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
					data := fixtureowner.Encode(t, storage.EncodeNodePayload, node)
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

		// Flush WAL to ensure all async writes complete
		require.NoError(t, adapter.FlushWAL())

		// Verify all commands were applied
		pos, err := adapter.GetWALPosition()
		require.NoError(t, err)
		assert.Equal(t, uint64(numGoroutines*commandsPerGoroutine), pos)
	})

	t.Run("concurrent GetWALEntries", func(t *testing.T) {
		adapter := setupTestAdapter(t).adapter
		defer adapter.Close()

		// Apply some commands first
		for i := 0; i < 10; i++ {
			node := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Test"}}
			data := fixtureowner.Encode(t, storage.EncodeNodePayload, node)
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
		adapter := setupTestAdapter(t).adapter
		defer adapter.Close()

		// Apply some commands
		for i := 0; i < 3; i++ {
			node := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Test"}}
			data := fixtureowner.Encode(t, storage.EncodeNodePayload, node)
			cmd := &Command{
				Type:      CmdCreateNode,
				Data:      data,
				Timestamp: time.Now(),
			}
			adapter.ApplyCommand(cmd)
		}

		// Flush WAL to ensure all async writes complete
		require.NoError(t, adapter.FlushWAL())

		var buf bytes.Buffer
		err := adapter.WriteSnapshot(&buf)
		require.NoError(t, err)

		var snapshot storage.ReplicationSnapshot
		err = storage.DecodeCommandPayload(buf.Bytes(), &snapshot)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), snapshot.WALPosition)
	})
}

func TestStorageAdapter_RestoreSnapshot(t *testing.T) {
	t.Run("restores snapshot and WAL position", func(t *testing.T) {
		adapter := setupTestAdapter(t).adapter
		defer adapter.Close()

		snapshot := storage.ReplicationSnapshot{
			WALPosition: 5,
			Nodes: []*storage.Node{
				{ID: storage.NodeID("n1"), Labels: []string{"Test"}},
			},
		}

		data := fixtureowner.Encode[any](t, storage.EncodeCommandPayload, snapshot)

		reader := bytes.NewReader(data)
		err := adapter.RestoreSnapshot(reader)
		require.NoError(t, err)

		// Verify WAL position restored
		pos, posErr := adapter.GetWALPosition()
		require.NoError(t, posErr)
		assert.Equal(t, uint64(5), pos)

		// Verify nodes restored
		node, getErr := adapter.engine.GetNode(storage.NodeID("n1"))
		require.NoError(t, getErr)
		assert.Equal(t, []string{"Test"}, node.Labels)
	})
}
