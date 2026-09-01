package cypher

import (
	"context"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// Transaction Log Query Procedures
// ========================================

// callDbTxlogEntries implements db.txlog.entries
// Syntax: CALL db.txlog.entries(fromSeq, toSeq) YIELD sequence, operation, timestamp, tx_id, data
//
// Returns WAL entries in the specified sequence range (inclusive).
// Parameters:
//   - fromSeq: Starting sequence number (required)
//   - toSeq: Ending sequence number (optional, 0 = no limit)
func (e *StorageExecutor) callDbTxlogEntries(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parse: CALL db.txlog.entries(fromSeq, toSeq)
	upper := strings.ToUpper(cypher)
	startIdx := strings.Index(upper, "DB.TXLOG.ENTRIES(")
	if startIdx == -1 {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogInvalidSyntax("db.txlog.entries"), nil)
	}

	// Extract parameters
	paramStart := startIdx + len("DB.TXLOG.ENTRIES(")
	paramEnd := strings.Index(cypher[paramStart:], ")")
	if paramEnd == -1 {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogClosingParenthesis("db.txlog.entries"), nil)
	}
	paramsStr := strings.TrimSpace(cypher[paramStart : paramStart+paramEnd])

	// Parse parameters (comma-separated)
	parts := strings.Split(paramsStr, ",")
	if len(parts) < 1 {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogEntriesArgumentRequired(), nil)
	}

	fromSeqStr := strings.TrimSpace(parts[0])
	fromSeq, err := strconv.ParseUint(fromSeqStr, 10, 64)
	if err != nil {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogInvalidSequence("fromSeq", err), err)
	}
	if fromSeq == 0 {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogFromSequencePositive(), nil)
	}

	var toSeq uint64
	if len(parts) > 1 {
		toSeqStr := strings.TrimSpace(parts[1])
		if toSeqStr != "" && toSeqStr != "0" {
			toSeq, err = strconv.ParseUint(toSeqStr, 10, 64)
			if err != nil {
				return nil, localizedError(localization.CypherSpecializedCallsTxlogInvalidSequence("toSeq", err), err)
			}
		}
	}

	// Get WAL directory
	wal, _ := e.resolveWALAndDatabase()
	if wal == nil {
		return nil, localizedError(localization.CypherSpecializedCallsWALUnavailable(), nil)
	}

	// Get WAL directory from config
	walCfg := wal.Config()
	if walCfg == nil {
		return nil, localizedError(localization.CypherSpecializedCallsWALConfigUnavailable(), nil)
	}
	walDir := walCfg.Dir
	if walDir == "" {
		return nil, localizedError(localization.CypherSpecializedCallsWALDirectoryNotConfigured(), nil)
	}

	// Read entries
	var entries []storage.WALEntry
	if toSeq > 0 {
		if toSeq < fromSeq {
			return nil, localizedError(localization.CypherSpecializedCallsTxlogSequenceOrder(), nil)
		}
		entries, err = storage.ReadWALEntriesRangeFromDir(walDir, fromSeq, toSeq)
	} else {
		entries, err = storage.ReadWALEntriesAfterFromDir(walDir, fromSeq-1) // -1 because After is exclusive
	}
	if err != nil {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogReadEntriesFailed(err), err)
	}

	// Build result
	columns := []string{"sequence", "operation", "timestamp", "tx_id", "database", "data"}
	rows := make([][]interface{}, len(entries))
	for i, entry := range entries {
		txID := storage.GetEntryTxID(entry)
		rows[i] = []interface{}{
			entry.Sequence,
			string(entry.Operation),
			entry.Timestamp,
			txID,
			entry.Database,
			string(entry.Data), // JSON string
		}
	}

	return &ExecuteResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}

// callDbTxlogByTxID implements db.txlog.byTxId
// Syntax: CALL db.txlog.byTxId(txId, maxEntries) YIELD sequence, operation, timestamp, tx_id, data
//
// Returns WAL entries for a specific transaction ID.
// Parameters:
//   - txId: Transaction ID (required)
//   - maxEntries: Maximum number of entries to return (optional, 0 = all)
func (e *StorageExecutor) callDbTxlogByTxID(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parse: CALL db.txlog.byTxId('txId', maxEntries)
	upper := strings.ToUpper(cypher)
	startIdx := strings.Index(upper, "DB.TXLOG.BYTXID(")
	if startIdx == -1 {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogInvalidSyntax("db.txlog.byTxId"), nil)
	}

	// Extract parameters
	paramStart := startIdx + len("DB.TXLOG.BYTXID(")
	paramEnd := strings.Index(cypher[paramStart:], ")")
	if paramEnd == -1 {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogClosingParenthesis("db.txlog.byTxId"), nil)
	}
	paramsStr := strings.TrimSpace(cypher[paramStart : paramStart+paramEnd])

	// Parse parameters (comma-separated, first is string, second is optional int)
	parts := strings.Split(paramsStr, ",")
	if len(parts) < 1 {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogByIDArgumentRequired(), nil)
	}

	// Extract txId (remove quotes if present)
	txIDStr := strings.TrimSpace(parts[0])
	txIDStr = strings.Trim(txIDStr, `"'`)
	if txIDStr == "" {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogIDEmpty(), nil)
	}

	var maxEntries int
	if len(parts) > 1 {
		maxStr := strings.TrimSpace(parts[1])
		if maxStr != "" && maxStr != "0" {
			max, err := strconv.Atoi(maxStr)
			if err == nil && max > 0 {
				maxEntries = max
			}
		}
	}

	// Get WAL directory
	wal, _ := e.resolveWALAndDatabase()
	if wal == nil {
		return nil, localizedError(localization.CypherSpecializedCallsWALUnavailable(), nil)
	}

	walCfg := wal.Config()
	if walCfg == nil {
		return nil, localizedError(localization.CypherSpecializedCallsWALConfigUnavailable(), nil)
	}
	walDir := walCfg.Dir
	if walDir == "" {
		return nil, localizedError(localization.CypherSpecializedCallsWALDirectoryNotConfigured(), nil)
	}

	// Find entries by tx_id
	entries, err := storage.FindWALEntriesByTxID(walDir, txIDStr, maxEntries)
	if err != nil {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogFindEntriesFailed(err), err)
	}

	// Build result
	columns := []string{"sequence", "operation", "timestamp", "tx_id", "database", "data"}
	rows := make([][]interface{}, len(entries))
	for i, entry := range entries {
		txID := storage.GetEntryTxID(entry)
		rows[i] = []interface{}{
			entry.Sequence,
			string(entry.Operation),
			entry.Timestamp,
			txID,
			entry.Database,
			string(entry.Data), // JSON string
		}
	}

	return &ExecuteResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}
