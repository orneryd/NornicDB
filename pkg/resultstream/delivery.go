package resultstream

import (
	"context"
	"sync"
	"sync/atomic"
)

type deliveryKey struct{}

// Delivery tracks cursors whose page has not reached the transport yet. A
// failed delivery discards them through the registry; success preserves durable
// cursors across requests and connections. The zero value is ready to use.
type Delivery struct {
	mu       sync.Mutex
	failed   bool
	discards []func()
	pending  atomic.Bool
}

// WithDelivery attaches transport delivery tracking to stream operations.
func WithDelivery(ctx context.Context, delivery *Delivery) context.Context {
	return context.WithValue(ctx, deliveryKey{}, delivery)
}

// DeliveryFromContext returns the transport tracker attached to this operation.
func DeliveryFromContext(ctx context.Context) *Delivery {
	delivery, _ := ctx.Value(deliveryKey{}).(*Delivery)
	return delivery
}

func (d *Delivery) register(discard func()) {
	d.mu.Lock()
	failed := d.failed
	if !failed {
		d.discards = append(d.discards, discard)
		d.pending.Store(true)
	}
	d.mu.Unlock()
	if failed {
		discard()
	}
}

// Fail retires every undelivered cursor, including a page produced after failure.
func (d *Delivery) Fail() {
	d.mu.Lock()
	d.failed = true
	discards := d.discards
	d.discards = nil
	d.pending.Store(false)
	d.mu.Unlock()
	for _, discard := range discards {
		discard()
	}
}

// Delivered acknowledges buffered pages. The tracker can serve the next batch.
func (d *Delivery) Delivered() {
	if !d.pending.Load() {
		return
	}
	d.mu.Lock()
	d.discards = nil
	d.pending.Store(false)
	d.mu.Unlock()
}

func (r *Registry) trackDelivery(ctx context.Context, scope Scope, qid string) {
	if delivery := DeliveryFromContext(ctx); delivery != nil {
		delivery.register(func() { _ = r.Discard(scope, qid) })
	}
}
