package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
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

// TestHistoricalEdgeOverwriteWALKeepsCounts exercises the historical
// replacement flag inside a current buffered record. Older WAL frame versions
// are rejected; a current record still needs the source numeric bindings.
func TestHistoricalEdgeOverwriteWALKeepsCounts(t *testing.T) {
	dir := t.TempDir()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal := engine.bufferedCommitWAL
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
	edge := &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "INGEST"}
	require.NoError(t, engine.CreateEdge(t.Context(), edge))
	require.NoError(t, engine.SetManualSuppression(context.Background(), StoredEntityEdgeKind, string(edge.ID), true))

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
	var err error
	data.NumericBindings, err = captureWALNumericBindings(engine.idDict, data.Writes, nil, nil)
	require.NoError(t, err)
	encoded, err := json.Marshal(data)
	require.NoError(t, err)
	// The legacy flag is decoded only, so the fixture injects it into the
	// record's own JSON text. json.Number keeps every typed number's original
	// lexeme: decoding these leaves as float64 would fabricate FLOAT spellings
	// (a whole commit_sequence written as "0.0") that no writer ever produced,
	// and the current reader rightly refuses a FLOAT literal for a uint64.
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var payload map[string]any
	require.NoError(t, decoder.Decode(&payload))
	version, ok := payload["version"].(map[string]any)
	require.True(t, ok)
	sequence, ok := version["commit_sequence"].(json.Number)
	require.True(t, ok, "fixture keeps the record's own integer lexeme")
	require.Equal(t, strconv.FormatUint(data.Version.CommitSequence, 10), sequence.String())
	ops, ok := payload["ops"].([]any)
	require.True(t, ok)
	require.Len(t, ops, 1)
	operation, ok := ops[0].(map[string]any)
	require.True(t, ok)
	require.NotContains(t, operation, "replaces_existing", "current writers never emit the legacy flag")
	operation["replaces_existing"] = true
	require.NoError(t, wal.AppendWithDatabase(OpBufferedCommit, payload, "test"))
	require.NoError(t, wal.Sync())

	crashClose(t, engine, wal)
	engine = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal = engine.bufferedCommitWAL
	t.Cleanup(func() { _ = wal.Close(); _ = engine.Close() })
	_, err = RecoverBufferedCommits(engine, wal)
	require.NoError(t, err)

	for _, read := range []func() (int64, error){
		func() (int64, error) { return engine.EdgeCount(t.Context()) },
		func() (int64, error) { return engine.EdgeCountByType(context.Background(), "INGEST") },
		func() (int64, error) { return engine.EdgeCountByStartLabel(context.Background(), "Doc", "INGEST") },
		func() (int64, error) { return engine.EdgeCountByEndLabel(context.Background(), "Doc", "INGEST") },
	} {
		count, err := read()
		require.NoError(t, err)
		require.EqualValues(t, 1, count)
	}
	edges, err := CollectStoredEdges(context.Background(), engine)
	require.NoError(t, err)
	require.Len(t, edges, 1)
	_, err = engine.GetEdge(t.Context(), edge.ID)
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
					_, err := engine.CreateNode(t.Context(), node)
					require.NoError(t, err)
				}
				edge := &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "INGEST"}
				require.NoError(t, engine.CreateEdge(t.Context(), edge))
				updated := copyEdge(edge)
				if move {
					updated.Type, updated.StartNode = "MOVED", "test:c"
				}
				tx, err := engine.BeginTransaction(t.Context())
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: updated}))
				require.NoError(t, tx.Commit())
				verify := func(stage string) {
					t.Helper()
					count, err := engine.EdgeCount(t.Context())
					require.NoError(t, err)
					require.EqualValues(t, 1, count, stage)
					count, err = engine.EdgeCountByType(context.Background(), updated.Type)
					require.NoError(t, err)
					require.EqualValues(t, 1, count, stage)
					label := "Source"
					if move {
						label = "Other"
					}
					count, err = engine.EdgeCountByStartLabel(context.Background(), label, updated.Type)
					require.NoError(t, err)
					require.EqualValues(t, 1, count, stage)
					count, err = engine.EdgeCountByEndLabel(context.Background(), "Target", updated.Type)
					require.NoError(t, err)
					require.EqualValues(t, 1, count, stage)
					if move {
						count, err = engine.EdgeCountByType(context.Background(), "INGEST")
						require.NoError(t, err)
						require.Zero(t, count, stage+": old type removed")
						count, err = engine.EdgeCountByStartLabel(context.Background(), "Source", "INGEST")
						require.NoError(t, err)
						require.Zero(t, count, stage+": old positional entry removed")
					}
				}
				verify("acknowledged")
				require.NoError(t, engine.FlushWriteBehind(context.Background()))
				verify("flushed")
			})
		}
	}
}
