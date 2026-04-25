// Package cypher - Transaction support for Cypher queries.
//
// Implements BEGIN/COMMIT/ROLLBACK for Neo4j-compatible transaction control.
package cypher

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
)

var firstUseGraphPattern = regexp.MustCompile(`(?is)\bUSE\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)`)

// TransactionContext holds the active transaction for a Cypher session.
type TransactionContext struct {
	tx              interface{} // *storage.BadgerTransaction (MemoryEngine now wraps BadgerEngine)
	engine          storage.Engine
	active          bool
	wal             *storage.WAL
	walSeqStart     uint64
	database        string
	txID            string
	fabricRemoteExe *fabric.RemoteFragmentExecutor
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

	// Unwrap engine wrappers (Async/WAL/Namespaced) recursively.
	engine := e.storage
	visited := map[storage.Engine]bool{}
	for engine != nil && !visited[engine] {
		visited[engine] = true
		if asyncEngine, ok := engine.(*storage.AsyncEngine); ok {
			engine = asyncEngine.GetEngine()
			continue
		}
		if walEngine, ok := engine.(*storage.WALEngine); ok {
			engine = walEngine.GetEngine()
			continue
		}
		if namespacedEngine, ok := engine.(*storage.NamespacedEngine); ok {
			engine = namespacedEngine.GetInnerEngine()
			continue
		}
		if wrapper, ok := engine.(interface{ GetInnerEngine() storage.Engine }); ok {
			engine = wrapper.GetInnerEngine()
			continue
		}
		break
	}

	// Composite engines use FabricTransaction coordinator semantics.
	// This supports explicit BEGIN/COMMIT/ROLLBACK at API level for composite routes.
	if _, ok := engine.(*storage.CompositeEngine); ok {
		e.txContext = &TransactionContext{
			tx:     fabric.NewFabricTransaction(fmt.Sprintf("fabtx-%d", time.Now().UnixNano())),
			engine: engine,
			active: true,
		}
		return &ExecuteResult{
			Columns: []string{"status"},
			Rows:    [][]interface{}{{"Transaction started"}},
		}, nil
	}

	// Start transaction for any engine that supports BeginTransaction.
	txEngine, ok := engine.(interface {
		BeginTransaction() (*storage.BadgerTransaction, error)
	})
	if !ok {
		return nil, fmt.Errorf("engine does not support transactions")
	}
	tx, err := txEngine.BeginTransaction()
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	if err := tx.SetDeferredConstraintValidation(true); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to configure transaction: %w", err)
	}
	txCtx := &TransactionContext{
		tx:     tx,
		engine: engine,
		active: true,
		txID:   tx.ID,
	}
	if wal, dbName := e.resolveWALAndDatabase(); wal != nil {
		walSeq, walErr := wal.AppendTxBegin(dbName, tx.ID, nil)
		if walErr != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("failed to write WAL tx begin: %w", walErr)
		}
		txCtx.wal = wal
		txCtx.walSeqStart = walSeq
		txCtx.database = dbName
	}
	e.txContext = txCtx

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
	var (
		err     error
		receipt *storage.Receipt
		opCount int
	)
	switch tx := e.txContext.tx.(type) {
	case *storage.BadgerTransaction:
		opCount = tx.OperationCount()
		err = tx.Commit()
	case *fabric.FabricTransaction:
		err = tx.Commit(nil, nil)
	default:
		return nil, fmt.Errorf("unknown transaction type")
	}

	wal := e.txContext.wal
	walSeqStart := e.txContext.walSeqStart
	txID := e.txContext.txID
	dbName := e.txContext.database

	if err == nil && wal != nil && walSeqStart > 0 {
		commitSeq, walErr := wal.AppendTxCommit(dbName, txID, opCount)
		if walErr == nil {
			receipt, _ = storage.NewReceipt(
				txID,
				walSeqStart,
				commitSeq,
				dbName,
				time.Now().UTC(),
			)
		}
	}

	if err != nil {
		if wal != nil && walSeqStart > 0 {
			_, _ = wal.AppendTxAbort(dbName, txID, err.Error())
		}
		if e.txContext.fabricRemoteExe != nil {
			_ = e.txContext.fabricRemoteExe.Close()
			e.txContext.fabricRemoteExe = nil
		}
		e.txContext.active = false
		e.txContext = nil
		return nil, fmt.Errorf("commit failed: %w", err)
	}

	if e.txContext.fabricRemoteExe != nil {
		_ = e.txContext.fabricRemoteExe.Close()
		e.txContext.fabricRemoteExe = nil
	}
	e.txContext.active = false
	e.txContext = nil

	result := &ExecuteResult{
		Columns: []string{"status"},
		Rows:    [][]interface{}{{"Transaction committed"}},
	}
	if receipt != nil {
		result.Metadata = map[string]interface{}{
			"receipt": receipt,
		}
	}
	return result, nil
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
	case *fabric.FabricTransaction:
		// If already rolled back, treat as idempotent rollback success.
		if tx.State() != "open" {
			err = nil
		} else {
			err = tx.Rollback(nil)
		}
	default:
		return nil, fmt.Errorf("unknown transaction type")
	}

	if e.txContext.wal != nil && e.txContext.walSeqStart > 0 {
		_, _ = e.txContext.wal.AppendTxAbort(e.txContext.database, e.txContext.txID, "rollback")
	}
	if e.txContext.fabricRemoteExe != nil {
		_ = e.txContext.fabricRemoteExe.Close()
		e.txContext.fabricRemoteExe = nil
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
	parsedCypher, inlineEmbeddingEnabled := stripWithEmbeddingSuffix(cypher)
	if inlineEmbeddingEnabled {
		cypher = parsedCypher
		upperQuery = strings.ToUpper(cypher)
	}

	if ftx, ok := e.txContext.tx.(*fabric.FabricTransaction); ok {
		if looksLikeWriteQuery(cypher) {
			if graph := extractFirstUseGraph(cypher); graph != "" {
				if _, err := ftx.GetOrOpen(graph, true); err != nil {
					return nil, err
				}
			}
		}
		// For composite/fabric transactions, route through shared fabric gateway when
		// query has multi-graph CALL { USE ... } patterns so write-shard constraints are
		// enforced across statements within the same explicit transaction.
		params := getParamsFromContext(ctx)
		if e.shouldUseFabricPlanner(cypher) {
			return e.executeViaFabricWithTx(ctx, cypher, params, ftx, false)
		}
		// Non-fabric-shaped queries still run on the scoped engine in explicit tx session.
		return e.executeWithoutTransaction(ctx, cypher, upperQuery)
	}

	// All engines now use BadgerTransaction (MemoryEngine wraps BadgerEngine)
	tx, ok := e.txContext.tx.(*storage.BadgerTransaction)
	if !ok {
		return nil, fmt.Errorf("unknown transaction type")
	}

	// Recursive execution paths, such as UNWIND ... MATCH ... MERGE fallback
	// routing, can re-enter Execute while the transaction wrapper is already in
	// context. Reuse it so database namespace metadata is not lost by wrapping
	// the same Badger transaction a second time. Only reuse the wrapper that
	// belongs to the active explicit transaction; stale wrappers from reused
	// contexts must fall back to a fresh wrapper for the current tx.
	if txWrapper, ok := ctx.Value(ctxKeyTxStorage).(*transactionStorageWrapper); ok &&
		txWrapper != nil &&
		txWrapper.tx == tx {
		txExec := e.cloneWithStorage(txWrapper)
		result, err := txExec.executeQueryAgainstStorage(ctx, cypher, upperQuery)
		if err != nil {
			return nil, err
		}
		if inlineEmbeddingEnabled {
			if err := txExec.applyInlineEmbeddingMutations(ctx, txWrapper.snapshotMutatedNodeIDs()); err != nil {
				return nil, err
			}
		}
		return result, nil
	}

	// Create a transactional wrapper that routes writes through the transaction.
	// IMPORTANT: carry namespace info so writes remain correctly prefixed in multi-db.
	engines := e.resolveImplicitTxEngines()
	separator := ":"
	if engines.namespace == "" {
		separator = ""
	}
	txWrapper := &transactionStorageWrapper{
		tx:             tx,
		underlying:     e.storage,
		namespace:      engines.namespace,
		separator:      separator,
		mutatedNodeIDs: make(map[string]struct{}),
	}

	// Pass the wrapper through context (same pattern as implicit transactions)
	// This is thread-safe and allows getStorage() to automatically use the transaction
	txCtx := context.WithValue(ctx, ctxKeyTxStorage, txWrapper)
	txExec := e.cloneWithStorage(txWrapper)

	// Execute the query - getStorage() will automatically use the transaction wrapper
	result, err := txExec.executeQueryAgainstStorage(txCtx, cypher, upperQuery)
	if err != nil {
		return nil, err
	}
	if inlineEmbeddingEnabled {
		if err := txExec.applyInlineEmbeddingMutations(txCtx, txWrapper.snapshotMutatedNodeIDs()); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func looksLikeWriteQuery(cypher string) bool {
	upper := strings.ToUpper(cypher)
	return strings.Contains(upper, "CREATE") ||
		strings.Contains(upper, "MERGE") ||
		strings.Contains(upper, "DELETE") ||
		strings.Contains(upper, "SET ") ||
		strings.Contains(upper, "REMOVE ")
}

func extractFirstUseGraph(cypher string) string {
	m := firstUseGraphPattern.FindStringSubmatch(cypher)
	if len(m) < 2 {
		return ""
	}
	return strings.TrimSpace(m[1])
}

// executeQueryAgainstStorage executes query with current storage context.
func (e *StorageExecutor) executeQueryAgainstStorage(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	e.decayMismatchLogged = false

	ctx, cleanup := setRevealOnEngine(ctx, e.storage, hasRevealCall(cypher))
	defer cleanup()

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
		!isCreateProcedureCommand(cypher) &&
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
	case isCreateProcedureCommand(cypher):
		return e.executeCreateProcedure(ctx, cypher)
	case strings.HasPrefix(upper, "CREATE CONSTRAINT"),
		strings.HasPrefix(upper, "CREATE RANGE INDEX"),
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
		case strings.HasPrefix(upper, "SHOW FULLTEXT INDEX"),
			strings.HasPrefix(upper, "SHOW RANGE INDEX"),
			strings.HasPrefix(upper, "SHOW VECTOR INDEX"):
			return e.executeShowIndexes(ctx, cypher)
		case strings.HasPrefix(upper, "SHOW INDEX"):
			return e.executeShowIndexes(ctx, cypher)
		case strings.HasPrefix(upper, "SHOW CONSTRAINT"):
			return e.executeShowConstraints(ctx, cypher)
		case strings.HasPrefix(upper, "SHOW PROCEDURE"):
			return e.executeShowProcedures(ctx, cypher)
		case strings.HasPrefix(upper, "SHOW FUNCTION"):
			return e.executeShowFunctions(ctx, cypher)
		case strings.HasPrefix(upper, "SHOW DATABASES"):
			return e.executeShowDatabases(ctx, cypher)
		case strings.HasPrefix(upper, "SHOW DATABASE"):
			return e.executeShowDatabase(ctx, cypher)
		case strings.HasPrefix(upper, "SHOW ALIASES"):
			return e.executeShowAliases(ctx, cypher)
		case strings.HasPrefix(upper, "SHOW LIMITS"):
			return e.executeShowLimits(ctx, cypher)
		default:
			return nil, fmt.Errorf("unsupported SHOW command in transaction: %s", cypher)
		}
	case strings.HasPrefix(upper, "DROP"):
		if isDropProcedureCommand(cypher) {
			return e.executeDropProcedure(ctx, cypher)
		}
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
