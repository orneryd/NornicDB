package knowledgepolicy

import (
	"context"
	"sync"
	"time"
)

// AccessMetaStore is the persistence interface for AccessMetaEntry.
type AccessMetaStore interface {
	GetAccessMeta(entityID string) (*AccessMetaEntry, error)
	PutAccessMeta(entityID string, entry *AccessMetaEntry) error
}

// AccessFlusher periodically drains the accumulator and persists deltas.
type AccessFlusher struct {
	accumulator *AccessAccumulator
	store       AccessMetaStore
	interval    time.Duration
	mu          sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}
}

// NewAccessFlusher creates a flusher. Default interval is 2 seconds.
func NewAccessFlusher(accumulator *AccessAccumulator, store AccessMetaStore, interval time.Duration) *AccessFlusher {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &AccessFlusher{
		accumulator: accumulator,
		store:       store,
		interval:    interval,
	}
}

// Start begins the flush loop. It exits immediately if the accumulator is disabled.
func (f *AccessFlusher) Start(ctx context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.accumulator.enabled {
		return
	}
	if f.cancel != nil {
		return
	}

	ctx, f.cancel = context.WithCancel(ctx)
	f.done = make(chan struct{})

	go f.run(ctx)
}

// Stop stops the flush loop and performs a final flush.
func (f *AccessFlusher) Stop() {
	f.mu.Lock()
	cancel := f.cancel
	done := f.done
	f.cancel = nil
	f.mu.Unlock()

	if cancel != nil {
		cancel()
		<-done
	}
}

func (f *AccessFlusher) run(ctx context.Context) {
	defer close(f.done)
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			f.flush()
			return
		case <-ticker.C:
			f.flush()
		}
	}
}

// Flush performs a single drain-and-persist cycle. Exported for testing.
func (f *AccessFlusher) Flush() {
	f.flush()
}

func (f *AccessFlusher) flush() {
	merged := f.accumulator.DrainAll()
	if len(merged) == 0 {
		return
	}

	now := time.Now().UnixNano()
	for entityID, delta := range merged {
		entry, err := f.store.GetAccessMeta(entityID)
		if err != nil {
			continue
		}
		if entry == nil {
			entry = &AccessMetaEntry{
				TargetID:    entityID,
				TargetScope: ScopeNode,
			}
		}

		entry.Fixed.AccessCount += delta.accessCount
		entry.Fixed.TraversalCount += delta.traversalCount
		if delta.lastAccessedAt > entry.Fixed.LastAccessedAt {
			entry.Fixed.LastAccessedAt = delta.lastAccessedAt
		}
		if delta.lastTraversedAt > entry.Fixed.LastTraversedAt {
			entry.Fixed.LastTraversedAt = delta.lastTraversedAt
		}

		if delta.overflow != nil {
			if entry.Overflow == nil {
				entry.Overflow = make(map[string]interface{})
			}
			for k, v := range delta.overflow {
				if existing, ok := entry.Overflow[k]; ok {
					if ev, ok := existing.(int64); ok {
						entry.Overflow[k] = ev + v
					} else {
						entry.Overflow[k] = v
					}
				} else {
					entry.Overflow[k] = v
				}
			}
		}

		entry.LastMutatedAt = now
		entry.MutationCount++

		_ = f.store.PutAccessMeta(entityID, entry)
	}
}
