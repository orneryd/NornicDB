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
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

var firstUseGraphPattern = regexp.MustCompile(`(?is)\bUSE\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)`)

// TransactionContext holds the active transaction for a Cypher session.
type TransactionContext struct {
	tx              interface{} // *storage.BadgerTransaction (MemoryEngine now wraps BadgerEngine)
	engine          storage.Engine
	storageWrapper  *transactionStorageWrapper
	active          bool
	wal             *storage.WAL
	walSeqStart     uint64
	database        string
	txID            string
	fabricRemoteExe *fabric.RemoteFragmentExecutor
	// running is the transaction's SHOW TRANSACTIONS entry (#718).
	running *runningTransaction
	// failed is the error of the first statement that failed in the
	// transaction. A failed transaction stays open so ROLLBACK discards what
	// it wrote; any other statement is refused and COMMIT rolls it back, as in
	// Neo4j (#683).
	failed error
}

// failTransaction records err as the active explicit transaction's failure
// and returns err. The first failure wins: it is the cause COMMIT reports. It
// is the one rule for a statement that fails inside a transaction, whether it
// ran through Execute or an inline transaction script (#683).
func (e *StorageExecutor) failTransaction(err error) error {
	if tx := e.txContext; err != nil && tx != nil && tx.active && tx.failed == nil {
		tx.failed = err
	}
	return err
}

// abortTransaction fails the active transaction with err and ends it: what an
// inline transaction script (BEGIN … COMMIT in one request) does when its
// statement fails, since no later ROLLBACK can reach it. It returns err.
func (e *StorageExecutor) abortTransaction(err error) error {
	e.failTransaction(err)
	_, _ = e.handleRollback()
	return err
}

// queryOnFailedTransactionError is the error of a statement sent to a
// transaction a previous statement failed in.
func queryOnFailedTransactionError(cause error) error {
	return newSemanticError("Neo.TransientError.Transaction.QueryExecutionFailedOnTransaction", "QueryExecutionFailedOnTransaction",
		"The transaction was marked as failed because a query failed: "+cause.Error())
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
		return nil, localizedError(localization.CypherTransactionsAlreadyActive(), nil)
	}

	// Establish the acknowledged-write visibility boundary before opening the
	// underlying snapshot. Explicit transactions previously unwrapped the async
	// engine directly, so recently acknowledged writes remained only in its
	// cache and were invisible until the background flush fired.
	engines := e.resolveImplicitTxEngines()

	// Unwrap engine wrappers (Async/WAL/Namespaced) recursively.
	engine := e.storage
	namespaceHint := ""
	visited := map[storage.Engine]bool{}
	for engine != nil && !visited[engine] {
		visited[engine] = true
		if namespacedEngine, ok := engine.(*storage.NamespacedEngine); ok {
			if namespaceHint == "" {
				namespaceHint = namespacedEngine.Namespace()
			}
		}
		if wrapper, ok := engine.(storage.EngineUnwrapper); ok {
			engine = wrapper.GetInnerEngine()
			continue
		}
		break
	}

	// Composite engines use FabricTransaction coordinator semantics.
	// This supports explicit BEGIN/COMMIT/ROLLBACK at API level for composite routes.
	if _, ok := engine.(*storage.CompositeEngine); ok {
		var fabricTx *fabric.FabricTransaction
		openSnapshot := func() error {
			fabricTx = fabric.NewFabricTransaction(fmt.Sprintf("fabtx-%d", time.Now().UnixNano()))
			return nil
		}
		if engines.asyncEngine != nil {
			if err := engines.asyncEngine.FlushBeforeSnapshot(openSnapshot); err != nil {
				return nil, localizedError(localization.CypherTransactionsStartFailed(err), err)
			}
		} else if err := openSnapshot(); err != nil {
			return nil, localizedError(localization.CypherTransactionsStartFailed(err), err)
		}
		e.txContext = &TransactionContext{
			tx:     fabricTx,
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
		return nil, localizedError(localization.CypherTransactionsEngineUnsupported(), nil)
	}
	if namespaceHint != "" {
		if primer, ok := txEngine.(interface{ EnsureNamespaceMVCC(string) error }); ok {
			if err := primer.EnsureNamespaceMVCC(namespaceHint); err != nil {
				return nil, localizedError(localization.CypherTransactionsPrimeNamespaceFailed(err), err)
			}
		}
	}
	tx, err := beginTransactionSnapshot(engines.asyncEngine, txEngine)
	if err != nil {
		return nil, localizedError(localization.CypherTransactionsStartFailed(err), err)
	}
	if namespaceHint != "" {
		if err := tx.SetNamespace(namespaceHint); err != nil {
			_ = tx.Rollback()
			return nil, localizedError(localization.CypherTransactionsPinNamespaceFailed(err), err)
		}
	}
	if err := tx.SetDeferredConstraintValidation(true); err != nil {
		_ = tx.Rollback()
		return nil, localizedError(localization.CypherTransactionsConfigureFailed(err), err)
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
			return nil, localizedError(localization.CypherTransactionsWALBeginFailed(walErr), walErr)
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
		return nil, localizedError(localization.CypherTransactionsNoActive(), nil)
	}
	// A transaction TERMINATE TRANSACTIONS ended doesn't commit (#718).
	if e.txContext.running != nil && e.txContext.running.terminated.Load() {
		_, _ = e.handleRollback()
		return nil, transactionTerminatedError()
	}
	// A transaction a statement failed in can't commit: it is rolled back.
	if cause := e.txContext.failed; cause != nil {
		if _, err := e.handleRollback(); err != nil {
			return nil, err
		}
		return nil, newSemanticError("Neo.ClientError.Transaction.TransactionMarkedAsFailed", "TransactionMarkedAsFailed",
			"The transaction was rolled back because a statement in it failed: "+cause.Error())
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
		return nil, localizedError(localization.CypherTransactionsUnknownType(), nil)
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
		runningTransactions.end(e.txContext.running)
		e.txContext = nil
		// Wire contract: substring "commit failed" is matched by downstream Bolt classifiers.
		// See docs/plans/consumer-pinned-error-contract-plan.md §2.1.
		return nil, localizedError(localization.CypherTransactionsCommitFailed(err), err)
	}
	if e.txContext.storageWrapper != nil {
		txExec := e.cloneWithStorage(e.txContext.storageWrapper)
		txExec.promoteNodeLookupCacheTo(e)
	}

	if e.txContext.fabricRemoteExe != nil {
		_ = e.txContext.fabricRemoteExe.Close()
		e.txContext.fabricRemoteExe = nil
	}
	e.txContext.active = false
	runningTransactions.end(e.txContext.running)
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
		return nil, localizedError(localization.CypherTransactionsNoActive(), nil)
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
		return nil, localizedError(localization.CypherTransactionsUnknownType(), nil)
	}

	if e.txContext.wal != nil && e.txContext.walSeqStart > 0 {
		_, _ = e.txContext.wal.AppendTxAbort(e.txContext.database, e.txContext.txID, "rollback")
	}
	if e.txContext.fabricRemoteExe != nil {
		_ = e.txContext.fabricRemoteExe.Close()
		e.txContext.fabricRemoteExe = nil
	}

	e.txContext.active = false
	runningTransactions.end(e.txContext.running)
	e.txContext = nil

	if err != nil {
		return nil, localizedError(localization.CypherTransactionsRollbackFailed(err), err)
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
		return nil, localizedError(localization.CypherTransactionsUnknownType(), nil)
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
			mutated := txWrapper.snapshotMutatedNodeIDs()
			if err := txExec.applyInlineEmbeddingMutations(ctx, mutated); err != nil {
				return nil, err
			}
			txWrapper.clearMutatedNodeIDs(mutated)
		}
		return result, nil
	}

	// Reuse one storage adapter for the explicit transaction lifetime. Besides
	// preserving namespace and mutation state, this prevents every statement
	// (including RETURN literals) from allocating and reseeding a 1000-entry
	// transaction lookup cache.
	txWrapper := e.txContext.storageWrapper
	if txWrapper == nil || txWrapper.tx != tx {
		engines := e.resolveImplicitTxEngines()
		separator := ":"
		if engines.namespace == "" {
			separator = ""
		}
		txWrapper = &transactionStorageWrapper{
			tx:             tx,
			underlying:     e.storage,
			namespace:      engines.namespace,
			separator:      separator,
			mutatedNodeIDs: make(map[string]struct{}),
		}
		e.txContext.storageWrapper = txWrapper
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
		mutated := txWrapper.snapshotMutatedNodeIDs()
		if err := txExec.applyInlineEmbeddingMutations(txCtx, mutated); err != nil {
			return nil, err
		}
		txWrapper.clearMutatedNodeIDs(mutated)
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
	// Single router: the transaction only changes which storage view e.storage is.
	return e.executeWithoutTransaction(ctx, cypher, upperQuery)
}
