package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// These databases and their 20 unapplied commits were produced by the actual
// old release and develop writers, rather than a current-codec simulation.
func TestV4StartupUpgradeMigratesGenuineWAL(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, name := range []string{"release", "develop"} {
		t.Run(name, func(t *testing.T) {
			root := fixtureowner.ExtractZip(t, filepath.Join("testdata", "wal-pre-identity", name+".zip"), "")
			dataDir := filepath.Join(root, "data")
			walDir := filepath.Join(dataDir, "wal")
			require.NoError(t, os.Rename(filepath.Join(root, "wal"), walDir))
			old, err := os.ReadFile(filepath.Join(walDir, "wal.log"))
			require.NoError(t, err)
			engine := reviewBindingEngine(t, dataDir)
			node, err := engine.GetNode("upgrade:a")
			require.NoError(t, err)
			require.Equal(t, int64(20), node.Properties["value"], "all acknowledged writes are recovered before V4")
			stamp, ok := node.Properties["stamp"].(time.Time)
			require.True(t, ok)
			require.True(t, time.Date(2026, 10, 23, 1, 2, 3, 4000, time.UTC).Equal(stamp))
			assertPropertyDictionaryBijection(t, engine)
			archives, err := filepath.Glob(walDir + ".pre-v4-*")
			require.NoError(t, err)
			require.Len(t, archives, 1)
			backup, err := os.ReadFile(filepath.Join(archives[0], "wal.log"))
			require.NoError(t, err)
			require.Equal(t, old, backup)
			wal, err := NewWAL(walDir, DefaultWALConfig())
			require.NoError(t, err)
			replayed, err := RecoverBufferedCommitsAtOpen(engine, wal)
			require.NoError(t, err)
			require.Zero(t, replayed, "restart must not replay already settled commits")
			require.NoError(t, wal.Close())
			require.NoError(t, engine.Close())
			reopened := reviewBindingEngine(t, dataDir)
			node, err = reopened.GetNode("upgrade:a")
			require.NoError(t, err)
			require.Equal(t, int64(20), node.Properties["value"])
		})
	}
}

// A pre-binding writer may acknowledge a newly allocated node and edge before
// their numeric dictionaries reach Badger. The migration must recover both the
// entities and their traversal keys from that same-store WAL.
func TestV4StartupUpgradeRestoresHistoricalPendingNewIDs(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	engine := reviewBindingEngine(t, dir)
	_, err := engine.CreateNode(&Node{ID: "legacy:a"})
	require.NoError(t, err)
	require.NoError(t, engine.FlushWriteBehind(nil))
	engine.writeBehind.Close()
	engine.writeBehind = NewWriteBehindBuffer(time.Hour, 1000000, engine.replayWriteBehind)
	require.NoError(t, engine.writeSchemaVersion(storageVersionEdgeAdjacencyV3))
	engine.storageVersion = storageVersionEdgeAdjacencyV3
	capture, err := NewWAL(filepath.Join(dir, "capture-wal"), DefaultWALConfig())
	require.NoError(t, err)
	engine.SetBufferedCommitWAL(capture)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("legacy"))
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(&Node{ID: "legacy:new", Labels: []string{"New"}})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{ID: "legacy:edge", StartNode: "legacy:a", EndNode: "legacy:new", Type: "LINK"}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	// A later unflushed record refers to the first record's numeric IDs.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("legacy"))
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "legacy:new", Labels: []string{"New"}, Properties: map[string]any{"value": "later"}}, Policy: ClientNodeEdits}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	entries, err := ReadWALEntriesFromDir(capture.Config().Dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	var old []byte
	for i, entry := range entries {
		data, err := decodeBufferedWALRecord(entry)
		require.NoError(t, err)
		data.NumericBindings, data.PropertyBindings, data.PropertyDictionaryID = nil, nil, ""
		if i > 0 {
			// The allocating record owns these pairs; the later record can
			// legitimately reuse them without carrying another pair.
			for key := range data.Writes {
				if len(key) > 0 && (key[0] == prefixIDDictNodeForward || key[0] == prefixIDDictEdgeForward || key[0] == prefixIDDictNodeReverse || key[0] == prefixIDDictEdgeReverse) {
					delete(data.Writes, key)
				}
			}
		}
		payload, err := json.Marshal(data)
		require.NoError(t, err)
		require.NotContains(t, string(payload), `\ufffd`, "the fixture must not already have lost physical key bytes")
		entry.Data, entry.Checksum, entry.PropertyEpoch = payload, crc32Checksum(payload), nil
		old = append(old, priorWALRecord(t, 3, entry)...)
	}
	crashClose(t, engine, capture)
	walDir := filepath.Join(dir, "wal")
	require.NoError(t, os.MkdirAll(walDir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(walDir, "wal.log"), old, 0600))
	engine = reviewBindingEngine(t, dir)
	node, err := engine.GetNode("legacy:new")
	require.NoError(t, err)
	require.Equal(t, []string{"New"}, node.Labels)
	require.Equal(t, "later", node.Properties["value"])
	edge, err := engine.GetEdge("legacy:edge")
	require.NoError(t, err)
	require.Equal(t, NodeID("legacy:a"), edge.StartNode)
	require.Equal(t, NodeID("legacy:new"), edge.EndNode)
	out, err := engine.GetOutgoingEdges("legacy:a")
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, EdgeID("legacy:edge"), out[0].ID)
}

func TestV4StartupConvertsOlderWALWhenSchemaAlreadyV4(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	root := fixtureowner.ExtractZip(t, filepath.Join("testdata", "wal-pre-identity", "release.zip"), "")
	dataDir := filepath.Join(root, "data")
	engine := reviewBindingEngine(t, dataDir)
	require.NoError(t, engine.Close())
	walDir := filepath.Join(dataDir, "wal")
	require.NoError(t, os.Rename(filepath.Join(root, "wal"), walDir))
	engine = reviewBindingEngine(t, dataDir)
	node, err := engine.GetNode("upgrade:a")
	require.NoError(t, err)
	require.Equal(t, int64(20), node.Properties["value"])
	wal, err := NewWAL(walDir, DefaultWALConfig())
	require.NoError(t, err)
	require.NoError(t, wal.Close())
}

// Earlier storage versions had headerless WALs but did not yet have the
// acknowledged buffered-commit format. Their logs still need conversion while
// the existing V1-to-V4 storage arms migrate the stored graph.
func TestV4StartupConvertsHeaderlessWALFromV1Store(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	engine := freshV1Engine(t)
	writeV1NodeBody(t, engine, &Node{ID: "legacy:node", Labels: []string{"Item"}, Properties: map[string]any{"name": "kept"}})
	dir := engine.dataDir
	require.NoError(t, engine.Close())
	walDir := filepath.Join(dir, "wal")
	require.NoError(t, os.MkdirAll(walDir, 0700))
	entry := WALEntry{Sequence: 8, Timestamp: time.Unix(8, 0).UTC(), Operation: OpTxBegin, Data: []byte(`{"tx_id":"legacy"}`), Checksum: 0x227d2204}
	old, err := json.Marshal(entry)
	require.NoError(t, err)
	old = append(old, '\n')
	require.NoError(t, os.WriteFile(filepath.Join(walDir, "wal.log"), old, 0600))
	engine, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, AllowStorageUpgrade: true, CatalogUpgradeV4: graphOnlyCatalogUpgradeV4()}))
	require.NoError(t, err)
	node, err := engine.GetNode("legacy:node")
	require.NoError(t, err)
	require.Equal(t, "kept", node.Properties["name"])
	entries, err := ReadWALEntriesFromDir(walDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(8), entries[0].Sequence)
	archives, err := filepath.Glob(walDir + ".pre-v4-*")
	require.NoError(t, err)
	require.Len(t, archives, 1)
	backup, err := os.ReadFile(filepath.Join(archives[0], "wal.log"))
	require.NoError(t, err)
	require.Equal(t, old, backup)
}
