package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// storageMembership is discovered at the first owned storage boundary. A
// transaction adapter is a boundary even when its transaction has ended: looking
// through it would let a retained view open an unrelated transaction underneath.
type storageMembership struct {
	statement   *compositeStatement
	storage     *transactionStorageWrapper
	transaction *TransactionContext
}

func (m storageMembership) owned() bool {
	return m.statement != nil || m.storage != nil || m.transaction != nil
}

// membershipOfStorage uses the storage layer's canonical transparent-wrapper
// contract. It never unwraps past a transaction to infer ownership from its base.
func membershipOfStorage(engine storage.Engine) storageMembership {
	for engine != nil {
		switch view := engine.(type) {
		case *transactionStorageWrapper:
			return storageMembership{statement: view.statement.Load(), storage: view, transaction: view.owner}
		case *transactionOwnedStorage:
			return storageMembership{transaction: view.owner}
		case interface{ StatementOwner() any }:
			if owner, ok := view.StatementOwner().(*compositeStatement); ok && owner != nil {
				return storageMembership{statement: owner}
			}
		}
		inner, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			break
		}
		next := inner.GetInnerEngine()
		if next == engine {
			break
		}
		engine = next
	}
	return storageMembership{}
}

// membership is the one entrance check. Storage is authoritative; context and
// executor state only supply membership when the storage chain is unowned (for
// example while routing a fragment onto its participant).
func (e *StorageExecutor) membership(ctx context.Context) storageMembership {
	if bound := membershipOfStorage(e.storage); bound.owned() {
		return bound
	}
	if wrapper, _ := ctx.Value(ctxKeyTxStorage).(*transactionStorageWrapper); wrapper != nil {
		return membershipOfStorage(wrapper)
	}
	if statement := compositeStatementFrom(ctx); statement != nil {
		return storageMembership{statement: statement}
	}
	if e.txContext != nil && e.txContext.active {
		return storageMembership{storage: e.txContext.storageWrapper, transaction: e.txContext}
	}
	return storageMembership{}
}

// transactionOwnedStorage carries an explicit composite transaction before a
// statement selects its participants. Native transactions use their own adapter.
type transactionOwnedStorage struct {
	storage.Engine
	owner *TransactionContext
}

func (s *transactionOwnedStorage) GetInnerEngine() storage.Engine { return s.Engine }

// SubscribeEvents retains the engine event capability across the composite
// transaction ownership view; registration still reaches the publisher once.
func (s *transactionOwnedStorage) SubscribeEvents(handlers storage.EventHandlers) error {
	return storage.SubscribeEvents(s.Engine, handlers)
}

var _ storage.EventSubscriber = (*transactionOwnedStorage)(nil)

func ownedTransactionControlError() error {
	return newSemanticError("Neo.ClientError.Transaction.TransactionTypeNotSupported", "NestedTransactionControl",
		"Transaction control is not allowed: transaction already active on storage owned by a statement or transaction")
}

// Only the executor that opened an explicit transaction may end it. Statement
// executors, clones and rebuilt executors are members, never lifecycle owners.
func (e *StorageExecutor) checkTransactionControl(ctx context.Context, command string) error {
	member := e.membership(ctx)
	if !member.owned() {
		return nil
	}
	if !strings.EqualFold(command, "BEGIN") && member.transaction != nil &&
		member.transaction.controller == e && member.transaction == e.txContext &&
		compositeStatementFrom(ctx) == nil && ctx.Value(ctxKeyTxStorage) == nil {
		return nil
	}
	return ownedTransactionControlError()
}

// compositeEngineOf follows transparent wrappers to the router without crossing
// a native transaction boundary.
func compositeEngineOf(engine storage.Engine) *storage.CompositeEngine {
	for engine != nil {
		if root, ok := engine.(*storage.CompositeEngine); ok {
			return root
		}
		if _, owned := engine.(*transactionStorageWrapper); owned {
			return nil
		}
		inner, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			return nil
		}
		next := inner.GetInnerEngine()
		if next == engine {
			return nil
		}
		engine = next
	}
	return nil
}
