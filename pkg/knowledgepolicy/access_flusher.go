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

// EntityMetaLookup provides node metadata needed by flusher property suppression.
type EntityMetaLookup interface {
	GetEntityMeta(entityID string) (labels []string, propertyKeys []string, createdAtNanos, versionAtNanos int64, err error)
}

// ScorerFunc returns a Scorer for the given namespace. Returns nil if none.
type ScorerFunc func(namespace string) *Scorer

// EmbedInvalidateFunc is called when property suppression state changes.
type EmbedInvalidateFunc func(entityID string)

// AccessFlusher periodically drains the accumulator and persists deltas.
type AccessFlusher struct {
	accumulator     *AccessAccumulator
	store           AccessMetaStore
	interval        time.Duration
	mu              sync.Mutex
	cancel          context.CancelFunc
	done            chan struct{}
	flushNow        chan struct{}
	scorerFunc      ScorerFunc
	entityMeta      EntityMetaLookup
	embedInvalidate EmbedInvalidateFunc
}

// NewAccessFlusher creates a flusher. Default interval is 2 seconds.
func NewAccessFlusher(accumulator *AccessAccumulator, store AccessMetaStore, interval time.Duration) *AccessFlusher {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	f := &AccessFlusher{
		accumulator: accumulator,
		store:       store,
		interval:    interval,
		flushNow:    make(chan struct{}, 1),
	}
	accumulator.SetOnBufferFull(func() {
		select {
		case f.flushNow <- struct{}{}:
		default:
		}
	})
	return f
}

// SetPropertySuppression configures the flusher for property-level decay evaluation.
func (f *AccessFlusher) SetPropertySuppression(scorerFn ScorerFunc, meta EntityMetaLookup, embedInvalid EmbedInvalidateFunc) {
	f.scorerFunc = scorerFn
	f.entityMeta = meta
	f.embedInvalidate = embedInvalid
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
		case <-f.flushNow:
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

		if f.evaluatePropertySuppression(entityID, entry, now) {
			// Suppression state changed — trigger re-embedding.
			if f.embedInvalidate != nil {
				f.embedInvalidate(entityID)
			}
		}

		_ = f.store.PutAccessMeta(entityID, entry)
	}
}

// evaluatePropertySuppression checks each property with a decay rule and writes
// nil markers for suppressed properties (or removes them if restored).
// Returns true if any suppression state changed.
func (f *AccessFlusher) evaluatePropertySuppression(entityID string, entry *AccessMetaEntry, nowNanos int64) bool {
	if f.scorerFunc == nil || f.entityMeta == nil {
		return false
	}

	ns := extractNamespace(entityID)
	scorer := f.scorerFunc(ns)
	if scorer == nil {
		return false
	}

	labels, propKeys, createdAt, versionAt, err := f.entityMeta.GetEntityMeta(entityID)
	if err != nil || len(propKeys) == 0 {
		return false
	}

	changed := false
	for _, key := range propKeys {
		res := scorer.ScoreProperty(entityID, labels, key, entry, createdAt, versionAt, nowNanos)
		overflowKey := "_suppress:" + key

		if entry.Overflow == nil {
			entry.Overflow = make(map[string]interface{})
		}

		_, wasSuppressed := entry.Overflow[overflowKey]
		if res.SuppressionEligible {
			if !wasSuppressed {
				entry.Overflow[overflowKey] = nil
				changed = true
			}
		} else {
			if wasSuppressed {
				delete(entry.Overflow, overflowKey)
				changed = true
			}
		}
	}
	return changed
}

func extractNamespace(entityID string) string {
	for i := 0; i < len(entityID); i++ {
		if entityID[i] == ':' {
			return entityID[:i]
		}
	}
	return "nornic"
}
