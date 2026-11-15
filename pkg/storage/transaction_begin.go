package storage

import (
	"context"
	"fmt"
)

// BeginRequest is the typed request a native transaction begin carries. It
// selects the lifecycle admission the engine takes before it captures the
// transaction's snapshot; it never changes the transaction's own semantics
// afterwards.
type BeginRequest uint8

const (
	// OrdinaryBegin opens an ordinary transaction: an explicit BEGIN or a data
	// autocommit. Its snapshot is captured with no lifecycle admission, so the
	// transaction keeps its own snapshot and its commit-time conflict guards
	// (snapshot isolation, the schema snapshot check, unique keys).
	OrdinaryBegin BeginRequest = iota
	// AutocommitSchemaBegin opens an autocommit schema statement. The engine
	// applies the write-behind commits acknowledged before it and then holds
	// its exclusive lifecycle write barrier from before the snapshot until the
	// statement's publication or rollback, so no data commit and no peer schema
	// publication can land inside the statement's window: the population its
	// fill reads stays current to its commit. The transaction owns that lease
	// and releases it exactly once, at its terminal outcome.
	AutocommitSchemaBegin
)

// admitTransactionBegin takes the admission a begin request owns before the
// snapshot is captured: none for an ordinary begin, the engine's existing
// exclusive lifecycle write barrier for an autocommit schema statement. The
// caller returns the lease to the engine if the begin fails and transfers it to
// the transaction otherwise.
func (b *BadgerEngine) admitTransactionBegin(ctx context.Context, request BeginRequest) (*writeBarrierLease, error) {
	switch request {
	case OrdinaryBegin:
		return nil, nil
	case AutocommitSchemaBegin:
		// Acknowledged write-behind commits are applied before the exclusive
		// hold, so the snapshot this statement fills from holds every write
		// acknowledged before it. The public flush takes the barrier shared, so
		// it can never run inside this statement's exclusive hold.
		if err := b.FlushWriteBehind(ctx); err != nil {
			return nil, err
		}
		lease, err := b.beginBarrierWrite(ctx, true, false)
		if err != nil {
			return nil, err
		}
		return &lease, nil
	default:
		return nil, fmt.Errorf("invalid transaction begin request: %d", request)
	}
}
