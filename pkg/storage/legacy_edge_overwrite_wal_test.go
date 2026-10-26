package storage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestOperationLegacyReplacementFlagIsDecodeOnly(t *testing.T) {
	var decoded Operation
	require.NoError(t, json.Unmarshal([]byte(`{"type":"create_edge","replaces_existing":true}`), &decoded))
	require.True(t, decoded.ReplacesExisting)

	encoded, err := json.Marshal(Operation{Type: OpCreateEdge, EdgeID: "test:r"})
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "replaces_existing", "current writers never emit the legacy flag")
}

// TestHistoricalEdgeOverwriteWALKeepsCounts records the exact buffered-WAL
// shape acknowledged by the short-lived 68db5d16d writer. The field remains
// readable even though current CREATE admission never writes it.
func TestHistoricalEdgeOverwriteWALKeepsCounts(t *testing.T) {
	dir := t.TempDir()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal := engine.bufferedCommitWAL
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
	edge := &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "INGEST"}
	require.NoError(t, engine.CreateEdge(edge))
	require.NoError(t, engine.SetManualSuppression(StoredEntityEdgeKind, string(edge.ID), true))

	var raw []byte
	require.NoError(t, engine.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(edgeKey(edge.ID))
		if err != nil {
			return err
		}
		raw, err = item.ValueCopy(nil)
		return err
	}))
	data := WALBufferedCommitData{
		TxID: "historical-overwrite", Namespace: "test",
		Ops:    []Operation{{Type: OpCreateEdge, EdgeID: edge.ID, Edge: edge}},
		Writes: map[string][]byte{string(edgeKey(edge.ID)): raw},
	}
	encoded, err := json.Marshal(data)
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(encoded, &payload))
	payload["ops"].([]any)[0].(map[string]any)["replaces_existing"] = true
	require.NoError(t, wal.AppendWithDatabase(OpBufferedCommit, payload, "test"))
	require.NoError(t, wal.Sync())

	crashClose(t, engine, wal)
	engine = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal = engine.bufferedCommitWAL
	t.Cleanup(func() { _ = wal.Close(); _ = engine.Close() })
	_, err = RecoverBufferedCommits(engine, wal)
	require.NoError(t, err)

	for _, read := range []func() (int64, error){
		engine.EdgeCount,
		func() (int64, error) { return engine.EdgeCountByType("INGEST") },
		func() (int64, error) { return engine.EdgeCountByStartLabel("Doc", "INGEST") },
		func() (int64, error) { return engine.EdgeCountByEndLabel("Doc", "INGEST") },
	} {
		count, err := read()
		require.NoError(t, err)
		require.EqualValues(t, 1, count)
	}
	edges, err := CollectStoredEdges(context.Background(), engine)
	require.NoError(t, err)
	require.Len(t, edges, 1)
	_, err = engine.GetEdge(edge.ID)
	require.ErrorIs(t, err, ErrNotFound)
	require.True(t, engine.manuallySuppressed(manualSuppressionEdge, string(edge.ID)))
}

// The legacy CREATE flag is not involved in ordinary UPDATE bookkeeping.
// Retain the upstream visible-update count coverage that the policy cut had
// previously removed with the replacement-admission tests.
func TestEdgeUpdateKeepsPhysicalCounts(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		for _, move := range []bool{false, true} {
			name := "sync"
			if writeBehind {
				name = "buffered"
			}
			if move {
				name += "/move"
			}
			t.Run(name, func(t *testing.T) {
				engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
					DataDir: t.TempDir(), WriteBehind: writeBehind, WriteBehindInterval: time.Hour,
				}))
				require.NoError(t, err)
				t.Cleanup(func() { _ = engine.Close() })
				for _, node := range []*Node{
					{ID: "test:a", Labels: []string{"Source"}},
					{ID: "test:b", Labels: []string{"Target"}},
					{ID: "test:c", Labels: []string{"Other"}},
				} {
					_, err := engine.CreateNode(node)
					require.NoError(t, err)
				}
				edge := &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "INGEST"}
				require.NoError(t, engine.CreateEdge(edge))
				updated := copyEdge(edge)
				if move {
					updated.Type, updated.StartNode = "MOVED", "test:c"
				}
				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.UpdateEdge(updated))
				require.NoError(t, tx.Commit())
				verify := func(stage string) {
					t.Helper()
					count, err := engine.EdgeCount()
					require.NoError(t, err)
					require.EqualValues(t, 1, count, stage)
					count, err = engine.EdgeCountByType(updated.Type)
					require.NoError(t, err)
					require.EqualValues(t, 1, count, stage)
					label := "Source"
					if move {
						label = "Other"
					}
					count, err = engine.EdgeCountByStartLabel(label, updated.Type)
					require.NoError(t, err)
					require.EqualValues(t, 1, count, stage)
					count, err = engine.EdgeCountByEndLabel("Target", updated.Type)
					require.NoError(t, err)
					require.EqualValues(t, 1, count, stage)
					if move {
						count, err = engine.EdgeCountByType("INGEST")
						require.NoError(t, err)
						require.Zero(t, count, stage+": old type removed")
						count, err = engine.EdgeCountByStartLabel("Source", "INGEST")
						require.NoError(t, err)
						require.Zero(t, count, stage+": old positional entry removed")
					}
				}
				verify("acknowledged")
				require.NoError(t, engine.FlushWriteBehind())
				verify("flushed")
			})
		}
	}
}
