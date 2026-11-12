package cypher

// PROFILE covers the read the two transaction log procedures perform: the WAL
// entries the visit hands them. The WAL handle and directory resolution is
// configuration, and the visit's segment listing is storage's own setup, so the
// procedures report the entries they read and claim no other access.
//
// Each case runs through the public entry point on both parsers, unobserved,
// with the collector PROFILE installs in the context, and under PROFILE itself,
// and holds the statement to the population the WAL handed over - every entry
// before the database, sequence-range and transaction-id selections, and
// including the older entries a range-less call keeps out of its rows.

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// txlogProfileFixture returns an executor for database "test" over a WAL holding
// four entries, all of one transaction id: three belong to "test" and one to
// "other". Only the database selection drops the last entry, and only the
// statement's own arguments drop any of the rest, so the population the WAL
// returns and the rows the procedure publishes cannot be confused.
func txlogProfileFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	t.Cleanup(config.WithWALEnabled())
	exec, wal := newTxlogExecutor(t)
	for _, database := range []string{"test", "test", "test", "other"} {
		_, err := wal.AppendWithDatabaseReturningSeq(storage.OpTxBegin, storage.WALTxData{TxID: "alpha", Metadata: nil}, database)
		require.NoError(t, err)
	}
	require.NoError(t, wal.Sync())
	return exec
}

// TestProfileTxlogProceduresReportTheEntriesTheyRead is one statement per
// selection: both procedures read the whole WAL visit, and only the argument and
// the transaction id decide which entries become rows.
func TestProfileTxlogProceduresReportTheEntriesTheyRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := txlogProfileFixture(t)
		database := exec.databaseName()
		require.Equal(t, "test", database)

		t.Run("entries", func(t *testing.T) {
			rows, collector, plan := profiledProcedureStatement(t, exec,
				"CALL db.txlog.entries(1) YIELD seq, txId RETURN seq, txId", nil)
			require.Len(t, rows, 3, "the procedure publishes this database's three entries")
			read := profiledProcedureRead(t, collector, plan, profileKindWALEntryScan, database, 4)
			require.NotEqual(t, int64(len(rows)), read.ActualRows,
				"the record is every entry the WAL handed over, including the other database's")
			require.EqualValues(t, 4, read.DBHits, "reading an entry is one hit")
			require.EqualValues(t, 4, plan.TotalDBHits)
		})

		t.Run("byTxId", func(t *testing.T) {
			rows, collector, plan := profiledProcedureStatement(t, exec,
				"CALL db.txlog.byTxId('alpha') YIELD seq, txId RETURN seq, txId", nil)
			require.Len(t, rows, 3)
			profiledProcedureRead(t, collector, plan, profileKindWALEntryScan, database, 4)
			require.EqualValues(t, 4, plan.TotalDBHits)
		})

		t.Run("a transaction the WAL holds no entry for", func(t *testing.T) {
			// The read population is the whole visit and the published
			// population is empty: the record states the entries read instead of
			// reporting no measurement for the empty result.
			rows, collector, plan := profiledProcedureStatement(t, exec,
				"CALL db.txlog.byTxId('missing') YIELD seq, txId RETURN seq, txId", nil)
			require.Empty(t, rows)
			read := profiledProcedureRead(t, collector, plan, profileKindWALEntryScan, database, 4)
			require.EqualValues(t, 4, read.ActualRows)
			require.EqualValues(t, 4, plan.TotalDBHits)
		})

		t.Run("byTxId limit", func(t *testing.T) {
			// The visit stops once the statement has its rows, so the record is
			// what the WAL actually delivered, not the whole log.
			rows, collector, plan := profiledProcedureStatement(t, exec,
				"CALL db.txlog.byTxId('alpha', 2) YIELD seq, txId RETURN seq, txId", nil)
			require.Len(t, rows, 2, "the limit keeps two entries")
			read := profiledProcedureRead(t, collector, plan, profileKindWALEntryScan, database, 2)
			require.Equal(t, int64(len(rows)), read.ActualRows, "the bounded visit read exactly the entries it published")
			require.EqualValues(t, 2, plan.TotalDBHits)
		})
	})
}

// A WAL holding no entry is an observed zero, not a missing measurement: the
// statement reports the empty population it read and keeps its hit total
// certified.
func TestProfileTxlogProcedureOnAnEmptyWALReportsAnObservedZero(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Cleanup(config.WithWALEnabled())
		exec, _ := newTxlogExecutor(t)
		rows, collector, plan := profiledProcedureStatement(t, exec,
			"CALL db.txlog.entries(1) YIELD seq, txId RETURN seq, txId", nil)
		require.Empty(t, rows)
		read := profiledProcedureRead(t, collector, plan, profileKindWALEntryScan, exec.databaseName(), 0)
		require.Zero(t, read.ActualRows)
		require.Zero(t, read.DBHits)
		require.Zero(t, plan.TotalDBHits)
	})
}

// A WAL the visit cannot read fails both procedures with the same cause with
// and without PROFILE: the observation adds no row and hides no error.
func TestProfileTxlogProcedureFailuresKeepTheirCause(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Cleanup(config.WithWALEnabled())
		exec, wal := newTxlogExecutor(t)
		segments := filepath.Join(wal.Config().Dir, "segments")
		require.NoError(t, os.MkdirAll(segments, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(segments, "manifest.json"), []byte("{"), 0o644))
		for _, query := range []string{
			"CALL db.txlog.entries(1) YIELD seq RETURN seq",
			"CALL db.txlog.byTxId('alpha') YIELD seq RETURN seq",
		} {
			t.Run(query, func(t *testing.T) {
				plain, plainErr := exec.Execute(t.Context(), query, nil)
				require.ErrorIs(t, plainErr, io.ErrUnexpectedEOF, query)
				if plain != nil {
					require.Empty(t, plain.Rows, "a failed read publishes no row")
				}
				profiled, profiledErr := exec.Execute(t.Context(), "PROFILE "+query, nil)
				require.ErrorIs(t, profiledErr, io.ErrUnexpectedEOF, query)
				require.Equal(t, plainErr.Error(), profiledErr.Error(), "PROFILE reports the statement's own failure")
				if profiled != nil {
					require.Empty(t, profiled.Rows, "PROFILE publishes no row either")
				}
			})
		}
	})
}
