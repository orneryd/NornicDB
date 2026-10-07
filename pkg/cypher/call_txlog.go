package cypher

import (
	"context"
	"errors"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// Transaction Log Query Procedures
// ========================================
//
// db.txlog.entries(fromSeq = null, toSeq = null) and
// db.txlog.byTxId(txId, limit = null) read the current database's WAL
// entries (#953). Both yield
//
//	txId :: STRING, db :: STRING, kind :: STRING, seq :: INTEGER,
//	timestamp :: STRING, payload :: STRING
//
// as their registered signatures and docs/operations/wal-compaction.md
// declare: txId is the entry's transaction id ("" when it has none), db its
// database, kind its operation, seq its WAL sequence, timestamp its RFC 3339
// time in UTC and payload its JSON data. Arguments are the procedures'
// evaluated arguments, so parameters and variables work like literals.
// Entries are visited one at a time; only the rows returned are kept.

// txlogColumns are the columns both txlog procedures yield.
var txlogColumns = []string{"txId", "db", "kind", "seq", "timestamp", "payload"}

// txlogRecentEntries is how many of the most recent entries
// db.txlog.entries() returns when called without a range.
const txlogRecentEntries = 1000

// errTxlogLimitReached stops a WAL visit once enough rows are collected.
var errTxlogLimitReached = errors.New("txlog limit reached")

// txlogWAL returns the WAL directory and the database whose entries the
// txlog procedures return. An empty database (an executor not bound to one)
// returns entries of every database.
func (e *StorageExecutor) txlogWAL() (string, string, error) {
	wal, database := e.resolveWALAndDatabase()
	if wal == nil {
		return "", "", localizedError(localization.CypherSpecializedCallsWALUnavailable(), nil)
	}
	// NewWAL always sets a configuration with a directory.
	return wal.Config().Dir, database, nil
}

// txlogInteger reads an optional integer argument: absent or null is
// (0, false).
func txlogInteger(args []interface{}, index int, name string) (int64, bool, error) {
	if index >= len(args) || args[index] == nil {
		return 0, false, nil
	}
	value, ok := coerceInt64(args[index])
	if !ok {
		return 0, false, localizedError(localization.CypherSpecializedCallsTxlogArgumentType(name, "INTEGER", neo4jProvidedValue(args[index])), nil)
	}
	return value, true, nil
}

// txlogRow is the row of one WAL entry.
func txlogRow(entry storage.WALEntry) []interface{} {
	return []interface{}{
		storage.GetEntryTxID(entry),
		entry.Database,
		string(entry.Operation),
		int64(entry.Sequence),
		entry.Timestamp.UTC().Format(time.RFC3339Nano),
		string(entry.Data),
	}
}

// callDbTxlogEntries implements db.txlog.entries(fromSeq = null, toSeq =
// null): the current database's entries from fromSeq (inclusive, at least 1)
// to toSeq (inclusive; null or 0 is no upper bound). Without either it
// returns the most recent txlogRecentEntries entries.
func (e *StorageExecutor) callDbTxlogEntries(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	fromSeq, hasFrom, err := txlogInteger(args, 0, "fromSeq")
	if err != nil {
		return nil, err
	}
	toSeq, hasTo, err := txlogInteger(args, 1, "toSeq")
	if err != nil {
		return nil, err
	}
	if hasFrom && fromSeq < 1 {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogFromSequencePositive(), nil)
	}
	if hasTo && toSeq < 0 {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogToSequenceNegative(), nil)
	}
	if !hasFrom {
		fromSeq = 1
	}
	if toSeq > 0 && toSeq < fromSeq {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogSequenceOrder(), nil)
	}
	walDir, database, err := e.txlogWAL()
	if err != nil {
		return nil, err
	}

	recent := !hasFrom && !hasTo
	var rows [][]interface{}
	err = storage.VisitWALEntriesAfterFromDir(walDir, uint64(fromSeq-1), func(entry storage.WALEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if (toSeq > 0 && entry.Sequence > uint64(toSeq)) || (database != "" && entry.Database != database) {
			return nil
		}
		rows = append(rows, txlogRow(entry))
		// Keep only the most recent window, so memory stays bounded.
		if recent && len(rows) > 2*txlogRecentEntries {
			rows = append(rows[:0], rows[len(rows)-txlogRecentEntries:]...)
		}
		return nil
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, localizedError(localization.CypherSpecializedCallsTxlogReadEntriesFailed(err), err)
	}
	if recent && len(rows) > txlogRecentEntries {
		rows = rows[len(rows)-txlogRecentEntries:]
	}
	return &ExecuteResult{Columns: txlogColumns, Rows: rows}, nil
}

// callDbTxlogByTxID implements db.txlog.byTxId(txId, limit = null): the
// current database's entries of transaction txId, at most limit of them
// (null, 0 or less is no limit).
func (e *StorageExecutor) callDbTxlogByTxID(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	var txID string
	if len(args) > 0 && args[0] != nil {
		text, ok := args[0].(string)
		if !ok {
			return nil, localizedError(localization.CypherSpecializedCallsTxlogArgumentType("txId", "STRING", neo4jProvidedValue(args[0])), nil)
		}
		txID = text
	}
	if txID == "" {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogIDEmpty(), nil)
	}
	limit, _, err := txlogInteger(args, 1, "limit")
	if err != nil {
		return nil, err
	}
	walDir, database, err := e.txlogWAL()
	if err != nil {
		return nil, err
	}

	var rows [][]interface{}
	err = storage.VisitWALEntriesAfterFromDir(walDir, 0, func(entry storage.WALEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if (database != "" && entry.Database != database) || storage.GetEntryTxID(entry) != txID {
			return nil
		}
		rows = append(rows, txlogRow(entry))
		if limit > 0 && int64(len(rows)) >= limit {
			return errTxlogLimitReached
		}
		return nil
	})
	if err != nil && !errors.Is(err, errTxlogLimitReached) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, localizedError(localization.CypherSpecializedCallsTxlogFindEntriesFailed(err), err)
	}
	return &ExecuteResult{Columns: txlogColumns, Rows: rows}, nil
}
