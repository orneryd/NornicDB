package multidb

import (
	"context"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// The reservation belongs to the logical command, not its ACK waiter or
// snapshot. Only definitive rejection or ordered publication retires it.
type accountingReservation struct {
	mu              sync.Mutex
	owner           *sizeTrackingEngine
	growth          accountingBatch
	closed, pending bool
}

type accountingReservationKey struct{}

func positiveAccountingBatch(batch accountingBatch) accountingBatch {
	batch.nodes.count = max(0, batch.nodes.count)
	batch.edges.count = max(0, batch.edges.count)
	batch.nodes.bytes = max(0, batch.nodes.bytes)
	batch.edges.bytes = max(0, batch.edges.bytes)
	return batch
}

func (b *accountingBatch) add(other accountingBatch, sign int64) {
	b.nodes.count += sign * other.nodes.count
	b.nodes.bytes += sign * other.nodes.bytes
	b.edges.count += sign * other.edges.count
	b.edges.bytes += sign * other.edges.bytes
}

func (t *sizeTrackingEngine) reserve(batch accountingBatch, existing *accountingReservation) *accountingReservation {
	if existing == nil {
		existing = &accountingReservation{owner: t}
	}
	existing.mu.Lock()
	defer existing.mu.Unlock()
	t.reservationMu.Lock()
	defer t.reservationMu.Unlock()
	t.reserved.add(existing.growth, -1)
	existing.growth = positiveAccountingBatch(batch)
	t.reserved.add(existing.growth, 1)
	return existing
}

func (r *accountingReservation) resolve(error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.owner.reservationMu.Lock()
	r.owner.reserved.add(r.growth, -1)
	r.owner.reservationMu.Unlock()
	r.closed = true
}

func (r *accountingReservation) markPending() { r.mu.Lock(); r.pending = true; r.mu.Unlock() }
func (r *accountingReservation) isPending() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pending && !r.closed
}

func (t *sizeTrackingEngine) reservedExcept(except *accountingReservation) accountingBatch {
	if except != nil {
		except.mu.Lock()
		defer except.mu.Unlock()
	}
	t.reservationMu.Lock()
	defer t.reservationMu.Unlock()
	reserved := t.reserved
	if except != nil && !except.closed {
		reserved.add(except.growth, -1)
	}
	return reserved
}

type managedAccountingTransaction interface {
	accountingTransaction
	ReplayCommand() storage.ManagedReplay
	SetCommandLifetime(storage.ManagedCommandLifetime)
	RoutesManagedCommand() bool
}

func (t *sizeTrackingEngine) commandLifetime(tx managedAccountingTransaction, reservation *accountingReservation) storage.ManagedCommandLifetime {
	replay := tx.ReplayCommand()
	return storage.ManagedCommandLifetime{
		LocalApply: func(ctx context.Context, published func()) error {
			ctx = context.WithValue(ctx, accountingReservationKey{}, reservation)
			ctx = storage.WithManagedLocalApply(storage.WithCommandPublication(ctx, published))
			return t.commitTransaction(ctx, tx)
		},
		Pending:  reservation.markPending,
		Resolved: reservation.resolve,
		Replay: func(ctx context.Context, published func()) error {
			ctx = context.WithValue(ctx, accountingReservationKey{}, reservation)
			ctx = storage.WithCommandPublication(ctx, published)
			ctx = storage.WithManagedLocalApply(ctx)
			return t.commitTransaction(ctx, storage.NewManagedReplay(t.Engine, replay))
		},
	}
}

func (t *sizeTrackingEngine) nativeCommandLifetime(tx *storage.ManagedWriteTransaction) (storage.ManagedCommandLifetime, error) {
	replay, err := tx.NativeReplay()
	if err != nil {
		return storage.ManagedCommandLifetime{}, err
	}
	apply := func(ctx context.Context, published func()) error {
		ctx = storage.WithManagedLocalApply(storage.WithCommandPublication(ctx, published))
		return t.commitTransaction(ctx, storage.NewManagedReplay(t.Engine, replay))
	}
	return storage.ManagedCommandLifetime{LocalApply: apply, Replay: apply}, nil
}
