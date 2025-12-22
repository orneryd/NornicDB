// Package cypher - Transaction support for Cypher queries.
//
// Implements BEGIN/COMMIT/ROLLBACK for Neo4j-compatible transaction control.
package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TransactionContext holds the active transaction for a Cypher session.
type TransactionContext struct {
	tx     interface{} // *storage.BadgerTransaction (MemoryEngine now wraps BadgerEngine)
	engine storage.Engine
	active bool
}

// parseTransactionStatement checks if query is BEGIN/COMMIT/ROLLBACK.
func (e *StorageExecutor) parseTransactionStatement(cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(strings.TrimSpace(cypher))

	switch {
	case upper == "BEGIN" || upper == "BEGIN TRANSACTION":
		return e.handleBegin()
	case upper == "COMMIT" || upper == "COMMIT TRANSACTION":
		return e.handleCommit()
	case upper == "ROLLBACK" || upper == "ROLLBACK TRANSACTION":
		return e.handleRollback()
	default:
		return nil, nil // Not a transaction statement
	}
}

// handleBegin starts a new explicit transaction.
func (e *StorageExecutor) handleBegin() (*ExecuteResult, error) {
	if e.txContext != nil && e.txContext.active {
		return nil, fmt.Errorf("transaction already active")
	}

	// Unwrap AsyncEngine, WALEngine, and NamespacedEngine to get underlying engine for transactions
	engine := e.storage
	if asyncEngine, ok := engine.(*storage.AsyncEngine); ok {
		engine = asyncEngine.GetEngine()
	}
	if walEngine, ok := engine.(*storage.WALEngine); ok {
		engine = walEngine.GetEngine()
	}
	// Unwrap NamespacedEngine to get the underlying engine (transactions work on the base engine)
	if namespacedEngine, ok := engine.(*storage.NamespacedEngine); ok {
		engine = namespacedEngine.GetInnerEngine()
	}

	// Start transaction based on engine type
	// Both BadgerEngine and MemoryEngine (which wraps BadgerEngine) use BadgerTransaction
	switch eng := engine.(type) {
	case *storage.BadgerEngine:
		tx, err := eng.BeginTransaction()
		if err != nil {
			return nil, fmt.Errorf("failed to start transaction: %w", err)
		}
		e.txContext = &TransactionContext{
			tx:     tx,
			engine: eng,
			active: true,
		}
	case *storage.MemoryEngine:
		tx, err := eng.BeginTransaction()
		if err != nil {
			return nil, fmt.Errorf("failed to start transaction: %w", err)
		}
		e.txContext = &TransactionContext{
			tx:     tx,
			engine: eng,
			active: true,
		}
	default:
		return nil, fmt.Errorf("engine does not support transactions")
	}

	return &ExecuteResult{
		Columns: []string{"status"},
		Rows:    [][]interface{}{{"Transaction started"}},
	}, nil
}

// handleCommit commits the active transaction.
func (e *StorageExecutor) handleCommit() (*ExecuteResult, error) {
	if e.txContext == nil || !e.txContext.active {
		return nil, fmt.Errorf("no active transaction")
	}

	// Commit based on transaction type
	// All engines now use BadgerTransaction (MemoryEngine wraps BadgerEngine)
	var err error
	switch tx := e.txContext.tx.(type) {
	case *storage.BadgerTransaction:
		err = tx.Commit()
	default:
		return nil, fmt.Errorf("unknown transaction type")
	}

	e.txContext.active = false
	e.txContext = nil

	if err != nil {
		return nil, fmt.Errorf("commit failed: %w", err)
	}

	return &ExecuteResult{
		Columns: []string{"status"},
		Rows:    [][]interface{}{{"Transaction committed"}},
	}, nil
}

// handleRollback rolls back the active transaction.
func (e *StorageExecutor) handleRollback() (*ExecuteResult, error) {
	if e.txContext == nil || !e.txContext.active {
		return nil, fmt.Errorf("no active transaction")
	}

	// Rollback based on transaction type
	// All engines now use BadgerTransaction (MemoryEngine wraps BadgerEngine)
	var err error
	switch tx := e.txContext.tx.(type) {
	case *storage.BadgerTransaction:
		err = tx.Rollback()
	default:
		return nil, fmt.Errorf("unknown transaction type")
	}

	e.txContext.active = false
	e.txContext = nil

	if err != nil {
		return nil, fmt.Errorf("rollback failed: %w", err)
	}

	return &ExecuteResult{
		Columns: []string{"status"},
		Rows:    [][]interface{}{{"Transaction rolled back"}},
	}, nil
}

// executeInTransaction executes a query within the active transaction.
// Uses the same transactionStorageWrapper pattern as implicit transactions,
// routing writes through the transaction for atomicity and rollback support.
func (e *StorageExecutor) executeInTransaction(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	// All engines now use BadgerTransaction (MemoryEngine wraps BadgerEngine)
	tx, ok := e.txContext.tx.(*storage.BadgerTransaction)
	if !ok {
		return nil, fmt.Errorf("unknown transaction type")
	}

	// Create a transactional wrapper that routes writes through the transaction
	// This ensures all operations within the transaction are atomic and can be rolled back
	txWrapper := &transactionStorageWrapper{tx: tx, underlying: e.storage}

	// Pass the wrapper through context (same pattern as implicit transactions)
	// This is thread-safe and allows getStorage() to automatically use the transaction
	txCtx := context.WithValue(ctx, ctxKeyTxStorage, txWrapper)

	// Execute the query - getStorage() will automatically use the transaction wrapper
	return e.executeQueryAgainstStorage(txCtx, cypher, upperQuery)
}

// executeQueryAgainstStorage executes query with current storage context.
func (e *StorageExecutor) executeQueryAgainstStorage(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	// Route query to appropriate handler
	// upperQuery is passed in to avoid redundant conversion
	upper := upperQuery

	// Normalize whitespace for compound query detection
	normalizedUpper := strings.ReplaceAll(strings.ReplaceAll(upper, "\n", " "), "\t", " ")

	// Check for CREATE...WITH...DELETE pattern first (special handling)
	if strings.HasPrefix(upper, "CREATE") &&
		findKeywordIndex(cypher, "WITH") > 0 &&
		findKeywordIndex(cypher, "DELETE") > 0 {
		return e.executeCompoundCreateWithDelete(ctx, cypher)
	}

	// Check for MATCH...CREATE...WITH...DELETE pattern (special handling)
	// This MUST come before the generic DELETE check below
	if strings.HasPrefix(upper, "MATCH") &&
		findKeywordIndex(cypher, "CREATE") > 0 &&
		findKeywordIndex(cypher, "WITH") > 0 &&
		findKeywordIndex(cypher, "DELETE") > 0 {
		return e.executeCompoundMatchCreate(ctx, cypher)
	}

	// Check for DELETE queries (MATCH...DELETE, DETACH DELETE)
	// But NOT if it's a MATCH...CREATE...DELETE pattern (handled above)
	hasCreate := findKeywordIndex(cypher, "CREATE") > 0
	if !hasCreate && (strings.Contains(normalizedUpper, " DELETE ") || strings.HasSuffix(normalizedUpper, " DELETE") ||
		strings.Contains(normalizedUpper, "DETACH DELETE")) {
		return e.executeDelete(ctx, cypher)
	}

	// Check for SET queries (MATCH...SET but NOT MERGE...SET which is handled by executeMerge)
	// Also exclude ON CREATE SET / ON MATCH SET from MERGE
	// IMPORTANT: Also exclude compound MATCH...MERGE...SET queries which are handled by executeCompoundMatchMerge
	if strings.Contains(normalizedUpper, " SET ") &&
		!strings.HasPrefix(upper, "MERGE") &&
		!strings.Contains(normalizedUpper, "ON CREATE SET") &&
		!strings.Contains(normalizedUpper, "ON MATCH SET") &&
		findKeywordIndex(cypher, "MERGE") <= 0 {
		return e.executeSet(ctx, cypher)
	}

	// Check for REMOVE queries (MATCH...REMOVE)
	if strings.Contains(normalizedUpper, " REMOVE ") {
		return e.executeRemove(ctx, cypher)
	}

	switch {
	case strings.HasPrefix(upper, "CREATE CONSTRAINT"),
		strings.HasPrefix(upper, "CREATE FULLTEXT INDEX"),
		strings.HasPrefix(upper, "CREATE VECTOR INDEX"),
		strings.HasPrefix(upper, "CREATE INDEX"):
		// Schema commands - constraints and indexes
		return e.executeSchemaCommand(ctx, cypher)
	case strings.HasPrefix(upper, "CREATE"):
		return e.executeCreate(ctx, cypher)
	case strings.HasPrefix(upper, "MATCH"):
		// Check for shortestPath queries first
		if isShortestPathQuery(cypher) {
			query, err := e.parseShortestPathQuery(cypher)
			if err != nil {
				return nil, err
			}
			return e.executeShortestPathQuery(query)
		}
		// Check for compound MATCH...OPTIONAL MATCH queries
		if findKeywordIndex(cypher, "OPTIONAL MATCH") > 0 {
			return e.executeCompoundMatchOptionalMatch(ctx, cypher)
		}
		// Check for compound MATCH...CREATE queries
		if findKeywordIndex(cypher, "CREATE") > 0 {
			return e.executeCompoundMatchCreate(ctx, cypher)
		}
		// Check for compound MATCH...MERGE queries
		if findKeywordIndex(cypher, "MERGE") > 0 {
			return e.executeCompoundMatchMerge(ctx, cypher)
		}
		return e.executeMatch(ctx, cypher)
	case strings.HasPrefix(upper, "OPTIONAL MATCH"):
		return e.executeOptionalMatch(ctx, cypher)
	case strings.HasPrefix(upper, "MERGE"):
		return e.executeMerge(ctx, cypher)
	case strings.HasPrefix(upper, "DELETE"), strings.HasPrefix(upper, "DETACH DELETE"):
		return e.executeDelete(ctx, cypher)
	case strings.HasPrefix(upper, "SET"):
		return e.executeSet(ctx, cypher)
	case strings.HasPrefix(upper, "RETURN"):
		return e.executeReturn(ctx, cypher)
	case strings.HasPrefix(upper, "CALL"):
		return e.executeCall(ctx, cypher)
	case strings.HasPrefix(upper, "SHOW"):
		// Handle SHOW commands (indexes, constraints, procedures, etc.)
		switch {
		case strings.HasPrefix(upper, "SHOW INDEX"):
			return e.executeShowIndexes(ctx, cypher)
		case strings.HasPrefix(upper, "SHOW CONSTRAINT"):
			return e.executeShowConstraints(ctx, cypher)
		case strings.HasPrefix(upper, "SHOW PROCEDURE"):
			return e.executeShowProcedures(ctx, cypher)
		case strings.HasPrefix(upper, "SHOW FUNCTION"):
			return e.executeShowFunctions(ctx, cypher)
		case strings.HasPrefix(upper, "SHOW DATABASE"):
			return e.executeShowDatabase(ctx, cypher)
		default:
			return nil, fmt.Errorf("unsupported SHOW command in transaction: %s", cypher)
		}
	case strings.HasPrefix(upper, "DROP"):
		// DROP INDEX/CONSTRAINT - treat as no-op (NornicDB manages indexes internally)
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	case strings.HasPrefix(upper, "UNWIND"):
		return e.executeUnwind(ctx, cypher)
	case strings.HasPrefix(upper, "WITH"):
		return e.executeWith(ctx, cypher)
	case strings.HasPrefix(upper, "FOREACH"):
		return e.executeForeach(ctx, cypher)
	default:
		return nil, fmt.Errorf("unsupported query type in transaction: %s", cypher)
	}
}
