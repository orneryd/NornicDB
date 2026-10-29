package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newTxlogExecutor returns an executor for database "test" over a WAL-backed
// store, and the WAL.
func newTxlogExecutor(t *testing.T) (*StorageExecutor, *storage.WAL) {
	t.Helper()
	dir := t.TempDir()
	badger, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	// The store is still reachable through the executor's engine chain, so its
	// lifetime stays owned by this test rather than the wrapper below.
	t.Cleanup(func() { badger.Close() })
	wal, err := storage.NewWAL(filepath.Join(dir, "wal"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { wal.Close() })
	return NewStorageExecutor(storage.NewNamespacedEngine(storage.NewWALEngine(badger, wal), "test")), wal
}

func TestTxlogProceduresRenderTypedWALAsJSON(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	exec, wal := newTxlogExecutor(t)
	_, err := wal.AppendWithDatabaseReturningSeq(storage.OpCreateNode, storage.WALNodeData{
		TxID: "typed-tx", Node: &storage.Node{ID: "test:alice", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}},
	}, "test")
	require.NoError(t, err)
	require.NoError(t, wal.Sync())
	for _, query := range []string{
		"CALL db.txlog.entries() YIELD txId, payload RETURN txId, payload",
		"CALL db.txlog.byTxId('typed-tx') YIELD txId, payload RETURN txId, payload",
	} {
		result, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, "typed-tx", result.Rows[0][0])
		payload, ok := result.Rows[0][1].(string)
		require.True(t, ok)
		var decoded storage.WALNodeData
		require.NoError(t, json.Unmarshal([]byte(payload), &decoded))
		require.Equal(t, storage.NodeID("test:alice"), decoded.Node.ID)
		require.Equal(t, "Alice", decoded.Node.Properties["name"])
	}
}

func TestTxlogProceduresReadCompressedBufferedCommitByID(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	wal, err := storage.NewWAL(filepath.Join(dir, "wal"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	t.Cleanup(func() { _ = engine.Close() })
	engine.SetBufferedCommitWAL(wal)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))
	for i := 0; i < 40; i++ {
		_, err = tx.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:n%d", i)), Properties: map[string]any{"name": "Alice"}})
		require.NoError(t, err)
	}
	transactionID := tx.ID
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	entries, err := storage.ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte{0x28, 0xb5, 0x2f, 0xfd}, entries[0].Data[:4], "the real buffered payload is compressed")

	exec := NewStorageExecutor(storage.NewNamespacedEngine(storage.NewWALEngine(engine, wal), "test"))
	result, err := exec.Execute(context.Background(), "CALL db.txlog.byTxId($tx) YIELD txId, payload RETURN txId, payload", map[string]any{"tx": transactionID})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, transactionID, result.Rows[0][0])
	var payload struct {
		TxID string              `json:"tx_id"`
		Ops  []storage.Operation `json:"ops"`
	}
	require.NoError(t, json.Unmarshal([]byte(result.Rows[0][1].(string)), &payload))
	require.Equal(t, transactionID, payload.TxID)
	require.Len(t, payload.Ops, 40)
}

// TestTxlogProceduresMatchTheirSignature pins db.txlog.entries and
// db.txlog.byTxId to their declared signature (#953): the documented
// columns, optional arguments evaluated like any procedure's, and entries
// of the current database only.
func TestTxlogProceduresMatchTheirSignature(t *testing.T) {
	exec, wal := newTxlogExecutor(t)
	ctx := context.Background()
	_, err := wal.AppendWithDatabaseReturningSeq(storage.OpTxBegin, storage.WALTxData{TxID: "tx-1", Metadata: nil}, "test")
	require.NoError(t, err)
	_, err = wal.AppendWithDatabaseReturningSeq(storage.OpTxBegin, storage.WALTxData{TxID: "tx-other", Metadata: nil}, "other")
	require.NoError(t, err)
	_, err = wal.AppendWithDatabaseReturningSeq(storage.OpTxCommit, storage.WALTxData{TxID: "tx-1", OpCount: 0}, "test")
	require.NoError(t, err)
	_, err = wal.AppendWithDatabaseReturningSeq(storage.OpTxCommit, storage.WALTxData{TxID: "tx-other", OpCount: 0}, "other")
	require.NoError(t, err)
	require.NoError(t, wal.Sync())

	t.Run("entries() yields the declared columns of this database's entries", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.txlog.entries() YIELD txId, db, kind, seq, timestamp, payload RETURN txId, db, kind, seq, timestamp, payload ORDER BY seq", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2)
		require.Equal(t, []interface{}{"tx-1", "test", "tx_begin", int64(1)}, result.Rows[0][:4])
		require.Equal(t, []interface{}{"tx-1", "test", "tx_commit", int64(3)}, result.Rows[1][:4])
		_, err = time.Parse(time.RFC3339Nano, result.Rows[0][4].(string))
		require.NoError(t, err)
		require.Contains(t, result.Rows[0][5], `"tx-1"`)
	})
	t.Run("a range, as parameters, after WITH", func(t *testing.T) {
		result, err := exec.Execute(ctx, "WITH $from AS from CALL db.txlog.entries(from, $to) YIELD seq RETURN collect(seq) AS seqs", map[string]interface{}{"from": int64(2), "to": int64(3)})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{[]interface{}{int64(3)}}}, result.Rows)
		result, err = exec.Execute(ctx, "CALL db.txlog.entries(1, 0) YIELD seq RETURN count(*) AS entries", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows, "toSeq 0 is no upper bound")
	})
	t.Run("byTxId with and without a limit", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.txlog.byTxId($tx) YIELD kind RETURN collect(kind) AS kinds", map[string]interface{}{"tx": "tx-1"})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{[]interface{}{"tx_begin", "tx_commit"}}}, result.Rows)
		result, err = exec.Execute(ctx, "MATCH (n) WITH count(n) AS ignored CALL db.txlog.byTxId('tx-1', 1) YIELD kind RETURN kind", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"tx_begin"}}, result.Rows)
	})
	t.Run("another database's transaction is not returned", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.txlog.byTxId('tx-other') YIELD seq RETURN seq", nil)
		require.NoError(t, err)
		require.Empty(t, result.Rows)
	})
}

// TestTxlogEntriesWithoutRangeReturnsTheMostRecent checks that
// db.txlog.entries() returns this database's 1,000 most recent entries.
func TestTxlogEntriesWithoutRangeReturnsTheMostRecent(t *testing.T) {
	exec, wal := newTxlogExecutor(t)
	var last uint64
	for i := 0; i < 2*txlogRecentEntries+100; i++ {
		seq, err := wal.AppendWithDatabaseReturningSeq(storage.OpTxBegin, storage.WALTxData{TxID: "tx", Metadata: nil}, "test")
		require.NoError(t, err)
		last = seq
		_, err = wal.AppendWithDatabaseReturningSeq(storage.OpTxBegin, storage.WALTxData{TxID: "tx", Metadata: nil}, "other")
		require.NoError(t, err)
	}
	require.NoError(t, wal.Sync())
	result, err := exec.Execute(context.Background(), "CALL db.txlog.entries() YIELD seq, db RETURN count(*) AS entries, max(seq) AS last, min(seq) AS first, collect(DISTINCT db) AS dbs", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(txlogRecentEntries), int64(last), int64(last) - 2*(txlogRecentEntries-1), []interface{}{"test"}}}, result.Rows)
}

// TestTxlogProcedureErrors checks the arguments and states the procedures
// reject.
func TestTxlogProcedureErrors(t *testing.T) {
	exec, _ := newTxlogExecutor(t)
	ctx := context.Background()
	for _, test := range []struct {
		query   string
		params  map[string]interface{}
		message localization.MessageID
	}{
		{"CALL db.txlog.entries(0)", nil, localization.MessageCypherSpecializedCallsTxlogFromSequencePositive},
		{"CALL db.txlog.entries(1, -1)", nil, localization.MessageCypherSpecializedCallsTxlogToSequenceNegative},
		{"CALL db.txlog.entries(3, 2)", nil, localization.MessageCypherSpecializedCallsTxlogSequenceOrder},
		{"CALL db.txlog.entries($from)", map[string]interface{}{"from": "x"}, localization.MessageCypherSpecializedCallsTxlogArgumentType},
		{"CALL db.txlog.entries(1, $to)", map[string]interface{}{"to": "x"}, localization.MessageCypherSpecializedCallsTxlogArgumentType},
		{"CALL db.txlog.byTxId('')", nil, localization.MessageCypherSpecializedCallsTxlogIDEmpty},
		{"CALL db.txlog.byTxId($tx)", map[string]interface{}{"tx": int64(5)}, localization.MessageCypherSpecializedCallsTxlogArgumentType},
		{"CALL db.txlog.byTxId('tx', $limit)", map[string]interface{}{"limit": "x"}, localization.MessageCypherSpecializedCallsTxlogArgumentType},
	} {
		t.Run(test.query, func(t *testing.T) {
			_, err := exec.Execute(ctx, test.query, test.params)
			var localized *localization.LocalizedError
			require.ErrorAs(t, err, &localized)
			require.Equal(t, test.message, localized.Message.ID)
		})
	}

	t.Run("an unreadable WAL wraps its cause", func(t *testing.T) {
		exec, wal := newTxlogExecutor(t)
		segments := filepath.Join(wal.Config().Dir, "segments")
		require.NoError(t, os.MkdirAll(segments, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(segments, "manifest.json"), []byte("{"), 0o644))
		for query, message := range map[string]localization.MessageID{
			"CALL db.txlog.entries()":   localization.MessageCypherSpecializedCallsTxlogReadEntriesFailed,
			"CALL db.txlog.byTxId('x')": localization.MessageCypherSpecializedCallsTxlogFindEntriesFailed,
		} {
			_, err := exec.Execute(ctx, query, nil)
			var localized *localization.LocalizedError
			require.ErrorAs(t, err, &localized, query)
			require.Equal(t, message, localized.Message.ID)
			require.ErrorIs(t, err, io.ErrUnexpectedEOF, "the manifest's decoding error is the cause")
		}
	})

	t.Run("a cancelled query stops", func(t *testing.T) {
		exec, wal := newTxlogExecutor(t)
		_, err := wal.AppendWithDatabaseReturningSeq(storage.OpTxBegin, storage.WALTxData{TxID: "tx", Metadata: nil}, "test")
		require.NoError(t, err)
		require.NoError(t, wal.Sync())
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err = exec.callDBTxlog(cancelled, nil, txlogBySequence)
		require.ErrorIs(t, err, context.Canceled)
		_, err = exec.callDBTxlog(cancelled, []interface{}{"tx"}, txlogByID)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("without a WAL", func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
		_, err := exec.Execute(ctx, "CALL db.txlog.entries()", nil)
		var localized *localization.LocalizedError
		require.ErrorAs(t, err, &localized)
		require.Equal(t, localization.MessageCypherSpecializedCallsWALUnavailable, localized.Message.ID)
	})
}
