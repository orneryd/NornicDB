package cypher

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newTxlogExecutor returns an executor for database "test" over a WAL-backed
// store, and the WAL.
func newTxlogExecutor(t *testing.T) (*StorageExecutor, *storage.WAL) {
	t.Helper()
	dir := t.TempDir()
	badger, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { badger.Close() })
	wal, err := storage.NewWAL(filepath.Join(dir, "wal"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { wal.Close() })
	return NewStorageExecutor(storage.NewNamespacedEngine(storage.NewWALEngine(badger, wal), "test")), wal
}

// TestTxlogProceduresMatchTheirSignature pins db.txlog.entries and
// db.txlog.byTxId to their declared signature (#953): the documented
// columns, optional arguments evaluated like any procedure's, and entries
// of the current database only.
func TestTxlogProceduresMatchTheirSignature(t *testing.T) {
	exec, wal := newTxlogExecutor(t)
	ctx := context.Background()
	_, err := wal.AppendTxBegin("test", "tx-1", nil)
	require.NoError(t, err)
	_, err = wal.AppendTxBegin("other", "tx-other", nil)
	require.NoError(t, err)
	_, err = wal.AppendTxCommit("test", "tx-1", 0)
	require.NoError(t, err)
	_, err = wal.AppendTxCommit("other", "tx-other", 0)
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
		seq, err := wal.AppendTxBegin("test", "tx", nil)
		require.NoError(t, err)
		last = seq
		_, err = wal.AppendTxBegin("other", "tx", nil)
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
		_, err := wal.AppendTxBegin("test", "tx", nil)
		require.NoError(t, err)
		require.NoError(t, wal.Sync())
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err = exec.callDbTxlogEntries(cancelled, nil)
		require.ErrorIs(t, err, context.Canceled)
		_, err = exec.callDbTxlogByTxID(cancelled, []interface{}{"tx"})
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
