package nornicdb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-412: Open reads the cluster configuration once, and that one result
// selects the journal record policy: a standalone server writes large
// records, every cluster member writes the standard ones.

func TestClusterStartupResolution(t *testing.T) {
	for _, tc := range []struct {
		mode, clusterDir, dataDir string
		wantPolicy                storage.WALRecordPolicy
		wantReplication           bool
		wantWALDir                string
	}{
		{mode: "", dataDir: "data", wantPolicy: storage.WALRecordsLarge, wantWALDir: filepath.Join("data", "replication", "wal")},
		{mode: "standalone", dataDir: "data", wantPolicy: storage.WALRecordsLarge, wantWALDir: filepath.Join("data", "replication", "wal")},
		{mode: "standalone", dataDir: "", wantPolicy: storage.WALRecordsLarge, wantWALDir: ""},
		{mode: "standalone", clusterDir: "elsewhere", dataDir: "", wantPolicy: storage.WALRecordsLarge, wantWALDir: filepath.Join("elsewhere", "wal")},
		{mode: "ha_standby", dataDir: "data", wantReplication: true, wantWALDir: filepath.Join("data", "replication", "wal")},
		{mode: "raft", dataDir: "", wantReplication: true, wantWALDir: filepath.Join("data", "replication", "wal")},
		{mode: "multi_region", clusterDir: "elsewhere", dataDir: "data", wantReplication: true, wantWALDir: filepath.Join("elsewhere", "wal")},
		// An unknown mode is not standalone: it keeps the standard policy and
		// replication start refuses it.
		{mode: "not-a-mode", dataDir: "data", wantReplication: true, wantWALDir: filepath.Join("data", "replication", "wal")},
	} {
		t.Run(tc.mode+"/"+tc.clusterDir+"/"+tc.dataDir, func(t *testing.T) {
			t.Setenv("NORNICDB_CLUSTER_MODE", tc.mode)
			t.Setenv("NORNICDB_CLUSTER_DATA_DIR", tc.clusterDir)
			startup := resolveClusterStartup(tc.dataDir)
			require.Equal(t, tc.wantPolicy, startup.walRecordPolicy())
			require.Equal(t, tc.wantWALDir, startup.replicationWALDir)
			require.Equal(t, tc.wantReplication, startup.replication != nil)
			if startup.replication != nil {
				require.Equal(t, replication.ReplicationMode(tc.mode), startup.replication.Mode)
				require.Equal(t, filepath.Dir(tc.wantWALDir), startup.replication.DataDir)
			}
		})
	}
}

// startupFrameVersions returns the version byte of every frame in the WAL
// files under dir.
func startupFrameVersions(t *testing.T, dir string) map[uint8]int {
	t.Helper()
	versions := map[uint8]int{}
	require.NoError(t, filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".log") && !strings.HasSuffix(path, ".wal") {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for offset := 0; offset+9 <= len(raw) && string(raw[offset:offset+4]) == "WALE"; {
			versions[raw[offset+4]]++
			size := int(binary.LittleEndian.Uint32(raw[offset+5:]))
			offset += (9 + size + 4 + 8 + 7) &^ 7
		}
		return nil
	}))
	return versions
}

// startupUpdateRecord returns the semantic version of the journal record of
// the last node update in the server's journal, and how many chunk vectors
// its node image carries.
func startupUpdateRecord(t *testing.T, dir string) (version uint16, vectors int) {
	t.Helper()
	entries, err := storage.ReadWALEntriesFromDir(context.Background(), filepath.Join(dir, "wal"))
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.Operation != storage.OpEntityCommit {
			continue
		}
		rendered, err := storage.EntryPayloadJSON(entry)
		require.NoError(t, err)
		var data struct {
			Payload []byte `json:"payload"`
		}
		require.NoError(t, json.Unmarshal([]byte(rendered), &data))
		envelope, ops, err := storage.DecodeEntityTransaction(data.Payload)
		require.NoError(t, err)
		for _, op := range ops {
			if op.Type == storage.OpUpdateNode && op.Node != nil {
				version, vectors = envelope.Version, len(op.Node.ChunkEmbeddings)
			}
		}
	}
	return version, vectors
}

// startupReplicatedUpdate returns the same for the node update a cluster
// member replicated: the command its replication journal holds.
func startupReplicatedUpdate(t *testing.T, dir string) (version uint16, vectors int) {
	t.Helper()
	entries, err := storage.ReadWALEntriesFromDir(context.Background(), filepath.Join(dir, "replication", "wal"))
	require.NoError(t, err)
	for _, entry := range entries {
		var record struct {
			Command *replication.Command `json:"cmd"`
		}
		if json.Unmarshal(entry.Data, &record) != nil || record.Command == nil {
			continue
		}
		envelope, ops, err := storage.DecodeEntityTransaction(record.Command.Data)
		if err != nil {
			continue // not a transaction command
		}
		for _, op := range ops {
			if op.Type == storage.OpUpdateNode && op.Node != nil {
				version, vectors = envelope.Version, len(op.Node.ChunkEmbeddings)
			}
		}
	}
	return version, vectors
}

func startupClusterEnv(t *testing.T, mode, dir string) {
	t.Helper()
	env := map[string]string{
		"NORNICDB_CLUSTER_MODE": mode, "NORNICDB_CLUSTER_DATA_DIR": filepath.Join(dir, "replication"),
		"NORNICDB_CLUSTER_NODE_ID": "startup-node", "NORNICDB_CLUSTER_BIND_ADDR": "127.0.0.1:0", "NORNICDB_CLUSTER_ADVERTISE_ADDR": "127.0.0.1:0",
		"NORNICDB_CLUSTER_REPLICATION_SECRET": "", "NORNICDB_CLUSTER_TLS_ENABLED": "false",
		"NORNICDB_CLUSTER_HA_ROLE": "primary", "NORNICDB_CLUSTER_HA_PEER_ADDR": "127.0.0.1:1", "NORNICDB_CLUSTER_HA_SYNC_MODE": "async",
		"NORNICDB_CLUSTER_HA_AUTO_FAILOVER": "false", "NORNICDB_CLUSTER_RAFT_BOOTSTRAP": "true", "NORNICDB_CLUSTER_RAFT_PEERS": "",
		"NORNICDB_CLUSTER_REGION_ID": "startup-region",
	}
	for key, value := range env {
		t.Setenv(key, value)
	}
}

const startupDims = 64

func startupConfig() *Config {
	c := DefaultConfig()
	c.Database.AsyncWritesEnabled = false
	c.Database.StrictDurability = true
	c.Database.WALSyncMode = "immediate"
	c.Database.WALAutoCompactionEnabled = false
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingEnabled = false
	c.Memory.EmbeddingDimensions = startupDims
	c.EmbeddingWorker.PropertiesInclude = []string{"text"}
	c.EmbeddingWorker.IncludeLabels = false
	return c
}

// startupWorkload writes through the server's storage engine, the engine its
// database manager hands every database: it seeds one embedded node, updates
// it in a transaction without touching its vectors (committed the way a
// session commits, through the engine's commit submitter), and commits one
// transaction larger than a journal frame. It returns that transaction's
// outcome.
func startupWorkload(t *testing.T, db *DB) error {
	t.Helper()
	ctx := context.Background()
	if replicator := db.GetReplicator(); replicator != nil {
		waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		require.NoError(t, replicator.WaitForLeader(waitCtx))
		cancel()
	}
	store := db.GetStorage()
	vectors := make([][]float32, 4)
	for i := range vectors {
		vectors[i] = make([]float32, startupDims)
		for j := range vectors[i] {
			vectors[i][j] = float32(i*startupDims+j) / 7
		}
	}
	_, err := store.CreateNode(ctx, &storage.Node{
		ID: "startup-emb", Labels: []string{"Emb"}, Properties: map[string]any{"text": "kept", "flag": false},
		ChunkEmbeddings: vectors,
		EmbedMeta:       map[string]any{"has_embedding": true, "chunk_count": 4, "embedding_dimensions": startupDims, "embedding_model": "synthetic"},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		stored, err := store.GetNode(ctx, "startup-emb")
		return err == nil && len(stored.ChunkEmbeddings) == 4
	}, 30*time.Second, 20*time.Millisecond, "the seeded node holds its vectors")

	base := db.GetBaseStorageForManager()
	tx, err := storage.BeginEntityTransaction(ctx, unwrapToBadgerEngine(base), storage.CommitModeExplicit)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("nornic"))
	node, err := tx.GetNode(ctx, "nornic:startup-emb")
	require.NoError(t, err)
	node = storage.CopyNode(node)
	node.Properties["flag"] = true
	require.NoError(t, tx.UpdateGraphNode(ctx, storage.NodeUpdateRequest{After: node}))
	prepared, err := tx.PrepareEntityCommit(ctx, storage.EntityCommitRequest{LogicalID: "startup-update"})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	receipt, err := base.(storage.EntityCommitSubmitter).SubmitEntityCommit(ctx, prepared, nil)
	require.NoError(t, err)
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	if err := receipt.Wait(waitCtx); err != nil {
		t.Logf("update receipt: %v", err) // a Raft receipt can report pending although it applies
	}
	cancel()
	require.Eventually(t, func() bool {
		stored, err := store.GetNode(ctx, "startup-emb")
		return err == nil && stored.Properties["flag"] == true
	}, 30*time.Second, 20*time.Millisecond, "the update applied")

	large, text := make([]*storage.Node, 900), strings.Repeat("p", 20<<10)
	for i := range large {
		large[i] = &storage.Node{ID: storage.NodeID("startup-large-" + strconv.Itoa(i)), Labels: []string{"Large"}, Properties: map[string]any{"text": text}}
	}
	return store.BulkCreateNodes(ctx, large)
}

// What the server's Open writes in each cluster mode: a standalone server
// journals the update without its kept vectors (semantic version 5) and
// commits the large transaction as a spanned record (frame version 7); a
// member of any cluster mode journals the whole image (version 3) and writes
// no spanned frame in either of its journals.
func TestOpenJournalRecordsByClusterMode(t *testing.T) {
	for _, mode := range []string{"", "standalone", "ha_standby", "raft", "multi_region"} {
		t.Run("mode="+mode, func(t *testing.T) {
			dir := t.TempDir()
			startupClusterEnv(t, mode, dir)
			db, err := Open(dir, startupConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			largeErr := startupWorkload(t, db)
			version, vectors := startupUpdateRecord(t, dir)
			frames := startupFrameVersions(t, dir)
			t.Logf("mode=%q: update record version %d with %d vectors; frame versions %v; large transaction: %v", mode, version, vectors, frames, largeErr)

			if mode == "" || mode == "standalone" {
				require.Nil(t, db.GetReplicator())
				require.Equal(t, uint16(5), version)
				require.Zero(t, vectors, "the update's record leaves out the vectors it kept")
				require.NoError(t, largeErr, "a single node commits a transaction larger than one frame")
				require.Positive(t, frames[7], "as a spanned record")
				return
			}
			require.NotNil(t, db.GetReplicator())
			require.Equal(t, uint16(3), version)
			require.Equal(t, 4, vectors, "the update's record carries the node's whole image")
			version, vectors = startupReplicatedUpdate(t, dir)
			require.Equal(t, uint16(3), version, "and so does the command it replicates")
			require.Equal(t, 4, vectors)
			require.Zero(t, frames[7], "a cluster member writes no spanned frame")
			require.Positive(t, frames[6])
		})
	}
}

// startupEnvFlip changes the cluster environment the first time storage
// touches the server's logger, which happens after Open has resolved the
// cluster configuration and before replication would start.
type startupEnvFlip struct {
	slog.Handler
	flip    func()
	flipped *atomic.Bool
}

func (h startupEnvFlip) WithAttrs(attrs []slog.Attr) slog.Handler {
	if h.flipped.CompareAndSwap(false, true) {
		h.flip()
	}
	return startupEnvFlip{Handler: h.Handler.WithAttrs(attrs), flip: h.flip, flipped: h.flipped}
}

// The cluster configuration Open starts with is the one it finishes with: a
// change of the environment during Open changes neither the journal record
// policy nor whether and where replication starts.
func TestOpenResolvesClusterConfigurationOnce(t *testing.T) {
	for _, tc := range []struct{ name, start, flipTo string }{
		{"standalone stays standalone", "standalone", "ha_standby"},
		{"cluster member stays a member", "ha_standby", "standalone"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, other := t.TempDir(), t.TempDir()
			startupClusterEnv(t, tc.start, dir)
			var flipped atomic.Bool
			config := startupConfig()
			config.Logger = slog.New(startupEnvFlip{Handler: slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}), flipped: &flipped, flip: func() {
				_ = os.Setenv("NORNICDB_CLUSTER_MODE", tc.flipTo)
				_ = os.Setenv("NORNICDB_CLUSTER_DATA_DIR", filepath.Join(other, "replication"))
			}})
			db, err := Open(dir, config)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			require.True(t, flipped.Load(), "the environment changed while Open ran")
			_, statErr := os.Stat(filepath.Join(other, "replication"))
			require.True(t, os.IsNotExist(statErr), "nothing was started from the changed environment")

			largeErr := startupWorkload(t, db)
			version, vectors := startupUpdateRecord(t, dir)
			frames := startupFrameVersions(t, dir)
			if tc.start == "standalone" {
				require.Nil(t, db.GetReplicator())
				require.Equal(t, uint16(5), version)
				require.NoError(t, largeErr)
				require.Positive(t, frames[7])
				return
			}
			require.NotNil(t, db.GetReplicator(), "replication started as resolved")
			_, statErr = os.Stat(filepath.Join(dir, "replication", "wal"))
			require.NoError(t, statErr, "in the resolved directory")
			require.Equal(t, uint16(3), version)
			require.Equal(t, 4, vectors)
			require.Zero(t, frames[7])
		})
	}
}
