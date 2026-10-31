// Package cypher - Transaction support for Cypher queries.
//
// Implements BEGIN/COMMIT/ROLLBACK for Neo4j-compatible transaction control.
package cypher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type transactionStatementOriginKey struct{}

// WithClientStatement marks text supplied as a client statement, not protocol transaction control.
func WithClientStatement(ctx context.Context) context.Context {
	return context.WithValue(ctx, transactionStatementOriginKey{}, true)
}

// WithTransactionControl permits a transaction owner to issue its lifecycle command.
func WithTransactionControl(ctx context.Context) context.Context {
	return context.WithValue(ctx, transactionStatementOriginKey{}, false)
}

// TransactionContext holds the active transaction for a Cypher session.
type TransactionContext struct {
	controller     *StorageExecutor
	outsideStorage storage.Engine
	// Captured at BEGIN, independent of user-editable tx.setMetaData values.
	coordinatorOwned bool
	tx               interface{}    // *storage.BadgerTransaction (MemoryEngine now wraps BadgerEngine)
	engine           storage.Engine // Engine owning this transaction's commit.
	storageWrapper   *transactionStorageWrapper
	active           bool
	wal              *storage.WAL
	walSeqStart      uint64
	database         string
	txID             string
	fabricRemoteExe  *fabric.RemoteFragmentExecutor
	// running is the transaction's SHOW TRANSACTIONS entry (#718).
	running *runningTransaction

	// fabricLocalTxExec holds, per local constituent (shard name), the
	// executor of this transaction's sub-transaction on it. Every statement
	// of the transaction runs there, so it sees earlier statements' writes
	// and COMMIT / ROLLBACK ends all of them.
	fabricLocalTxExec map[string]*StorageExecutor
	// failed is the error of the first statement that failed in the
	// transaction. A failed transaction stays open so ROLLBACK discards what
	// it wrote; any other statement is refused and COMMIT rolls it back, as in
	// Neo4j (#683).
	failed error
	// storageRolledBack records early cleanup after a memory failure. The
	// protocol transaction stays failed until its owner receives COMMIT or
	// ROLLBACK, while staged storage and its budget are released immediately.
	storageRolledBack bool

	// statementKind keeps administration commands apart from data statements.
	statementKind string
}

// failTransaction records err as the active explicit transaction's failure
// and returns err. The first failure wins: it is the cause COMMIT reports. It
// is the one rule for a statement that fails inside a transaction, whether it
// ran through Execute or an inline transaction script (#683). A composite
// transaction's participants end their part with it: each local one fails
// with the same cause, and each remote sub-transaction is rolled back. ctx is
// the failed statement's context, whose deadline bounds the remote rollbacks.
func (e *StorageExecutor) failTransaction(ctx context.Context, err error) error {
	if tx := e.txContext; err != nil && tx != nil && tx.active && tx.failed == nil {
		tx.failed = err
		if code, _ := nornicerrors.Neo4jStatus(err); code == transactionMemoryLimitCode || code == memoryPoolOutOfMemoryCode {
			if storageTx, ok := tx.tx.(*storage.BadgerTransaction); ok {
				if rollbackErr := storageTx.Rollback(); rollbackErr == nil {
					tx.storageRolledBack = true
					if tx.running != nil {
						tx.running.releaseMemory()
					}
				}
			}
		}
		// The transaction can no longer commit: the MERGE key locks it holds,
		// itself or through a participant's sub-transaction, must not keep
		// other transactions' MERGEs of those keys waiting until the client
		// rolls it back. Local keys are released first: they never wait on a
		// remote.
		if releaser, ok := tx.tx.(interface{ ReleaseMergeKeys() }); ok {
			releaser.ReleaseMergeKeys()
		}
		for _, participant := range tx.fabricLocalTxExec {
			participant.failTransaction(ctx, err)
		}
		if tx.fabricRemoteExe != nil {
			// A remote participant records its own rollback error (a
			// deadline included); the transaction keeps reporting err.
			rollbackCtx, cancel := remoteFailureRollbackContext(ctx)
			_ = tx.fabricRemoteExe.RollbackParticipants(rollbackCtx)
			cancel()
		}
	}
	return err
}

// remoteFailureRollbackContext bounds a failed transaction's remote
// rollbacks: by the statement's deadline (its transaction or statement
// timeout), and never longer than the remote client's request timeout. It
// isn't cancelled with the statement, which already failed.
func remoteFailureRollbackContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(storage.RemoteRequestTimeout)
	if statementDeadline, ok := ctx.Deadline(); ok && statementDeadline.Before(deadline) {
		deadline = statementDeadline
	}
	return context.WithDeadline(context.WithoutCancel(ctx), deadline)
}

// abortTransaction fails the active transaction with err and ends it: what an
// inline transaction script (BEGIN … COMMIT in one request) does when its
// statement fails, since no later ROLLBACK can reach it. It returns err.
func (e *StorageExecutor) abortTransaction(ctx context.Context, err error) error {
	e.failTransaction(ctx, err)
	_, _ = e.handleRollback()
	return err
}

// queryOnFailedTransactionError is the error of a statement sent to a
// transaction a previous statement failed in.
func queryOnFailedTransactionError(cause error) error {
	return newSemanticError("Neo.TransientError.Transaction.QueryExecutionFailedOnTransaction", "QueryExecutionFailedOnTransaction",
		"The transaction was marked as failed because a query failed: "+cause.Error())
}

// bareTransactionCommand returns the command word, as typed, when cypher is a
// bare transaction command (BEGIN, COMMIT or ROLLBACK, optionally followed by
// TRANSACTION; classifyStatement names it), else "".
func bareTransactionCommand(cypher string) string {
	switch classifyStatement(cypher).command {
	case commandBegin, commandCommit, commandRollback:
		body := statementBody(cypher)
		start := queryGapEnd(body, 0)
		// As typed: Neo4j's SyntaxError quotes the input ('begin').
		return body[start:scanReservedWord(body, start)]
	}
	return ""
}

// parseTransactionStatement runs a bare BEGIN, COMMIT or ROLLBACK.
func (e *StorageExecutor) parseTransactionStatement(ctx context.Context, cypher string) (*ExecuteResult, error) {
	switch classifyStatement(cypher).command {
	case commandBegin:
		return e.handleBegin(ctx)
	case commandCommit:
		return e.handleCommit(ctx)
	case commandRollback:
		return e.handleRollback()
	default:
		return nil, nil // Not a transaction statement
	}
}

// handleBegin starts a new explicit transaction.
func (e *StorageExecutor) handleBegin(ctx context.Context) (*ExecuteResult, error) {
	if e.txContext != nil && e.txContext.active {
		return nil, localizedError(localization.CypherTransactionsAlreadyActive(), nil)
	}

	// Establish the acknowledged-write visibility boundary before opening the
	// underlying snapshot. Writes are committed synchronously by their
	// statements, so a snapshot opened here observes every acknowledged write.

	// Unwrap engine wrappers (WAL/Namespaced) recursively.
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
		if err := openSnapshot(); err != nil {
			return nil, localizedError(localization.CypherTransactionsStartFailed(err), err)
		}
		e.txContext = &TransactionContext{
			controller: e, outsideStorage: e.storage,
			tx:     fabricTx,
			engine: engine,
			active: true,
		}
		e.storage = &transactionOwnedStorage{Engine: e.storage, owner: e.txContext}
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
	// With write-behind buffering, drain acknowledged autocommit writes so
	// the explicit snapshot observes every acknowledged statement.
	if drainer, ok := engine.(interface{ FlushWriteBehind(context.Context) error }); ok {
		if err := drainer.FlushWriteBehind(ctx); err != nil {
			return nil, localizedError(localization.CypherTransactionsStartFailed(err), err)
		}
	}
	tx, err := beginTransactionSnapshot(txEngine)
	if err != nil {
		return nil, localizedError(localization.CypherTransactionsStartFailed(err), err)
	}
	if namespaceHint != "" {
		if err := tx.SetNamespace(namespaceHint); err != nil {
			_ = tx.Rollback()
			return nil, localizedError(localization.CypherTransactionsPinNamespaceFailed(err), err)
		}
	}
	// An explicit transaction checks each write against the constraints
	// when its statement runs, as Neo4j does: the statement that breaks a
	// constraint fails with ConstraintValidationFailed (and fails the
	// transaction), not the COMMIT (#700). COMMIT checks again under the
	// constraint keys' locks, against what other writers committed since.
	if err := tx.SetValidationSetting(storage.DeferredConstraintValidation, false); err != nil {
		_ = tx.Rollback()
		return nil, localizedError(localization.CypherTransactionsConfigureFailed(err), err)
	}
	txCtx := &TransactionContext{
		controller: e, outsideStorage: e.storage,
		tx:     tx,
		engine: e.storage,
		active: true,
		txID:   tx.ID,
	}
	if wal, dbName := e.resolveWALAndDatabase(); wal != nil {
		walSeq, walErr := wal.AppendWithDatabaseReturningSeq(storage.OpTxBegin, storage.WALTxData{TxID: tx.ID, Metadata: nil}, dbName)
		if walErr != nil {
			_ = tx.Rollback()
			return nil, localizedError(localization.CypherTransactionsWALBeginFailed(walErr), walErr)
		}
		txCtx.wal = wal
		txCtx.walSeqStart = walSeq
		txCtx.database = dbName
	}
	e.txContext = txCtx
	e.storage = e.transactionStorageFor(tx)

	return &ExecuteResult{
		Columns: []string{"status"},
		Rows:    [][]interface{}{{"Transaction started"}},
	}, nil
}

// handleCommit commits the active transaction.
func (e *StorageExecutor) handleCommit(ctx context.Context) (*ExecuteResult, error) {
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
		err = e.commitStorageTransaction(ctx, tx)
	case *fabric.FabricTransaction:
		err = tx.Commit(nil, nil)
		if err == nil {
			publishStatementCommit(ctx)
		}
	default:
		return nil, localizedError(localization.CypherTransactionsUnknownType(), nil)
	}

	wal := e.txContext.wal
	walSeqStart := e.txContext.walSeqStart
	txID := e.txContext.txID
	dbName := e.txContext.database

	if err == nil && wal != nil && walSeqStart > 0 {
		commitSeq, walErr := wal.AppendWithDatabaseReturningSeq(storage.OpTxCommit, storage.WALTxData{TxID: txID, OpCount: opCount}, dbName)
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
			_, _ = wal.AppendWithDatabaseReturningSeq(storage.OpTxAbort, storage.WALTxData{TxID: txID, Reason: err.Error()}, dbName)
		}
		if e.txContext.fabricRemoteExe != nil {
			_ = e.txContext.fabricRemoteExe.Close()
			e.txContext.fabricRemoteExe = nil
		}
		_, localTx := e.txContext.tx.(*storage.BadgerTransaction)
		e.txContext.active = false
		runningTransactions.end(e.txContext.running)
		if e.txContext.outsideStorage != nil {
			e.storage = e.txContext.outsideStorage
		}
		e.txContext = nil
		// Wire contract: substring "commit failed" is matched by downstream Bolt classifiers.
		// See docs/plans/consumer-pinned-error-contract-plan.md §2.1.
		failure := localizedError(localization.CypherTransactionsCommitFailed(err), err)
		if commitFailureWroteNothing(localTx, err) {
			failure = nornicerrors.MarkCommitRolledBack(failure)
		}
		return nil, failure
	}
	var completionErr error
	if e.txContext.storageWrapper != nil {
		txExec := e.cloneWithStorage(e.txContext.storageWrapper)
		txExec.promoteNodeLookupCacheTo(e)
		completionErr = e.txContext.storageWrapper.completeCommitActions()
		txExec.refreshSearchAfterCommit(e.txContext.storageWrapper)
	}

	if e.txContext.fabricRemoteExe != nil {
		_ = e.txContext.fabricRemoteExe.Close()
		e.txContext.fabricRemoteExe = nil
	}
	e.txContext.active = false
	runningTransactions.end(e.txContext.running)
	if e.txContext.outsideStorage != nil {
		e.storage = e.txContext.outsideStorage
	}
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
	return result, completionErr
}

// commitFailureWroteNothing reports whether a failed COMMIT left nothing of
// the transaction stored, so its outcome is known (the Bolt session keeps its
// connection). A local transaction (BadgerTransaction.Commit) fails before
// it writes anything on:
//   - a constraint violation, checked before the write;
//   - a transaction over the store's size limit (storage.IsTransactionTooBig),
//     raised while its writes are staged in the one Badger transaction, which
//     is then discarded.
//
// The Fabric coordinator ends readers before its sole writer and explicitly
// marks failures that committed no writes. Unknown write outcomes remain unmarked.
func commitFailureWroteNothing(localTx bool, err error) bool {
	if errors.Is(err, fabric.ErrCommitKeptNothing) {
		return true
	}
	if !localTx {
		return false
	}
	var violation *storage.ConstraintViolationError
	return errors.As(err, &violation) || storage.IsTransactionTooBig(err)
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
		if !e.txContext.storageRolledBack {
			err = tx.Rollback()
		}
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
		_, _ = e.txContext.wal.AppendWithDatabaseReturningSeq(storage.OpTxAbort, storage.WALTxData{TxID: e.txContext.txID, Reason: "rollback"}, e.txContext.database)
	}
	if e.txContext.fabricRemoteExe != nil {
		_ = e.txContext.fabricRemoteExe.Close()
		e.txContext.fabricRemoteExe = nil
	}

	e.txContext.active = false
	runningTransactions.end(e.txContext.running)
	if e.txContext.outsideStorage != nil {
		e.storage = e.txContext.outsideStorage
	}
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

	if ftx, ok := e.txContext.tx.(*fabric.FabricTransaction); ok {
		// Shared USE syntax selects Fabric routing; its statement contract supplies
		// write intent. prepareExecDispatch enlists only the resolved participant,
		// preserving the one-writer rule across statements without guessing from text.
		params := getParamsFromContext(ctx)
		if e.shouldUseFabricPlanner(ctx, cypher) {
			return e.executeViaFabricWithTx(ctx, cypher, params, ftx, false)
		}
		// Non-fabric-shaped queries still run on the scoped engine in explicit tx session.
		return e.executeWithoutTransaction(ctx, cypher, upperQuery)
	}

	parsedCypher, inlineEmbeddingEnabled := stripWithEmbeddingSuffix(cypher)
	if inlineEmbeddingEnabled {
		cypher = parsedCypher
		upperQuery = upperASCII(cypher)
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
		if state := e.resolvedStatementModeOf(ctx); state != nil {
			var release func()
			txExec, release = txExec.bindResolvedStatementMode(state)
			defer release()
		}
		result, err := txExec.executeQueryAgainstStorage(ctx, cypher, upperQuery)
		if err != nil {
			return result, err
		}
		if inlineEmbeddingEnabled {
			if err := txExec.completeInlineEmbedding(ctx, txWrapper); err != nil {
				return nil, err
			}
		}
		return result, nil
	}

	// Reuse one storage adapter for the explicit transaction lifetime. Besides
	// preserving namespace and mutation state, this prevents every statement
	// (including RETURN literals) from allocating and reseeding a 1000-entry
	// transaction lookup cache.
	txWrapper := e.transactionStorageFor(tx)

	// Pass the wrapper through context (same pattern as implicit transactions)
	// This is thread-safe and allows getStorage() to automatically use the transaction
	txCtx := context.WithValue(ctx, ctxKeyTxStorage, txWrapper)
	txExec := e.cloneWithStorage(txWrapper)
	if state := e.resolvedStatementModeOf(ctx); state != nil {
		var release func()
		txExec, release = txExec.bindResolvedStatementMode(state)
		defer release()
	}

	// Execute the query - getStorage() will automatically use the transaction wrapper
	result, err := txExec.executeQueryAgainstStorage(txCtx, cypher, upperQuery)
	if err != nil {
		return result, err
	}
	if inlineEmbeddingEnabled {
		if err := txExec.completeInlineEmbedding(txCtx, txWrapper); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// transactionStorageFor is the transaction's sole storage-adapter owner.
// Runtime Fabric promotion and ordinary explicit execution reuse this adapter;
// neither creates another transaction or terminal lifecycle.
func (e *StorageExecutor) transactionStorageFor(tx *storage.BadgerTransaction) *transactionStorageWrapper {
	if wrapper := e.txContext.storageWrapper; wrapper != nil && wrapper.tx == tx {
		return wrapper
	}
	engines := e.resolveImplicitTxEngines()
	separator := ":"
	if engines.namespace == "" {
		separator = ""
	}
	wrapper := &transactionStorageWrapper{
		tx: tx, underlying: e.storage, namespace: engines.namespace, separator: separator,
		mutatedNodeIDs: make(map[string]struct{}),
	}
	wrapper.initNamespacePrefix()
	wrapper.owner = e.txContext
	e.txContext.storageWrapper = wrapper
	return wrapper
}

func looksLikeWriteQuery(cypher string) bool {
	upper := upperASCII(classificationText(cypher))
	return strings.Contains(upper, "CREATE") ||
		strings.Contains(upper, "MERGE") ||
		strings.Contains(upper, "DELETE") ||
		strings.Contains(upper, "SET ") ||
		strings.Contains(upper, "REMOVE ")
}

// executeQueryAgainstStorage executes query with current storage context.
func (e *StorageExecutor) executeQueryAgainstStorage(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	e.decayMismatchLogged = false
	// Single router: the transaction only changes which storage view e.storage is.
	return e.executeWithoutTransaction(ctx, cypher, upperQuery)
}
