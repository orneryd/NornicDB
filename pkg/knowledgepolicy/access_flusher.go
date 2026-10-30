package knowledgepolicy

import (
	"context"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/util"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// EntityMeta describes the storage metadata the flusher needs to resolve
// access-time policy bindings and property visibility.
type EntityMeta struct {
	Scope          ScopeType
	Labels         []string
	EdgeType       string
	PropertyKeys   []string
	CreatedAtNanos int64
	VersionAtNanos int64
}

// EntityMetaLookup provides entity metadata needed by flusher policy and
// property suppression evaluation.
type EntityMetaLookup interface {
	GetEntityMeta(entityID string) (EntityMeta, error)
}

// ScorerFunc returns a Scorer for the given namespace. Returns nil if none.
type ScorerFunc func(namespace string) *Scorer

// EmbedInvalidateFunc is called when property suppression state changes.
type EmbedInvalidateFunc func(entityID string)

// SuppressionRecheckFunc re-evaluates entity visibility after access metadata changes.
type SuppressionRecheckFunc func(entityID string, meta EntityMeta)

// AccessFlusher periodically drains the accumulator and persists deltas.
type AccessFlusher struct {
	accumulator        *AccessAccumulator
	store              AccessMetaStore
	interval           time.Duration
	mu                 sync.Mutex
	active             *accessFlusherRun // mu; replaced only after actual completion
	outcomes           util.WorkLifetime // shared outcome accumulator; no second cancellation root
	flushNow           chan struct{}
	scorerFunc         ScorerFunc
	entityMeta         EntityMetaLookup
	embedInvalidate    EmbedInvalidateFunc
	suppressionRecheck SuppressionRecheckFunc

	// metrics is the knowledge-policy observability handle. Optional —
	// nil-safe, zero-cost when unset. See SetMetrics.
	metrics *observability.KnowledgePolicyMetrics
}

// accessFlusherRun is enrolled before foreign context construction. The done
// signal belongs to this exact run, never to a later Start's mutable field.
type accessFlusherRun struct {
	done     chan struct{}
	cancel   context.CancelFunc // mu; published once construction returns
	stopping bool               // mu; a stop during construction is delivered on publication
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

// SetSuppressionRecheck configures a callback that re-evaluates entity visibility
// after access metadata mutations have been persisted.
func (f *AccessFlusher) SetSuppressionRecheck(fn SuppressionRecheckFunc) {
	f.suppressionRecheck = fn
}

// SetMetrics attaches a knowledge-policy metrics handle. Nil-safe; a nil
// handle falls back to the package-level global
// (observability.GetKnowledgePolicyMetrics) at fire time so late-registered
// metrics still flow through.
func (f *AccessFlusher) SetMetrics(m *observability.KnowledgePolicyMetrics) {
	f.metrics = m
}

// resolveMetrics returns the injected handle or falls back to the global.
func (f *AccessFlusher) resolveMetrics() *observability.KnowledgePolicyMetrics {
	if f.metrics != nil {
		return f.metrics
	}
	return observability.GetKnowledgePolicyMetrics()
}

// BufferFullness returns the current accumulator buffer occupancy as a
// fraction in [0, 1]. Exposed so the observability layer can install it as
// a GaugeFunc callback.
func (f *AccessFlusher) BufferFullness() float64 {
	if f == nil || f.accumulator == nil {
		return 0
	}
	return f.accumulator.BufferFullness()
}

// Start begins the flush loop. It exits immediately if the accumulator is disabled.
func (f *AccessFlusher) Start(ctx context.Context) {
	f.mu.Lock()
	if !f.accumulator.enabled || f.active != nil {
		f.mu.Unlock()
		return
	}
	run := &accessFlusherRun{done: make(chan struct{})}
	f.active = run
	f.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	f.mu.Lock()
	run.cancel = cancel
	stopping := run.stopping
	f.mu.Unlock()
	if stopping {
		cancel()
	}
	go f.run(ctx, run)
}

// Stop seals this run before signaling, joins its actual completion and
// returns every accumulated native outcome. A closing run cannot be replaced.
func (f *AccessFlusher) Stop() error {
	f.mu.Lock()
	run := f.active
	var cancel context.CancelFunc
	if run != nil {
		run.stopping = true
		cancel = run.cancel
	}
	f.mu.Unlock()
	if run != nil {
		if cancel != nil {
			cancel()
		}
		<-run.done
		f.mu.Lock()
		if f.active == run {
			f.active = nil
		}
		f.mu.Unlock()
	}
	return f.outcomes.Err()
}

func (f *AccessFlusher) run(ctx context.Context, run *accessFlusherRun) {
	defer close(run.done)
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
	start := time.Now()
	merged := f.accumulator.DrainAll()

	// Emit batch-size histogram even for empty batches (the zero-row short-
	// circuit is a useful signal: frequent zero-row flushes = timer-driven
	// waste).
	metrics := f.resolveMetrics()
	if metrics != nil {
		metrics.ObserveAccessFlushBatchRows(context.Background(), float64(len(merged)))
	}

	if len(merged) == 0 {
		if metrics != nil {
			metrics.ObserveAccessFlushDuration(context.Background(), time.Since(start).Seconds())
		}
		return
	}

	// Wrap the non-empty flush in a span so downstream storage writes nest
	// beneath it (one coherent trace per flush batch).
	flushCtx := context.Background()
	var span trace.Span
	flushCtx, span = otel.Tracer("nornicdb/knowledge_policy").Start(
		flushCtx, "nornicdb.knowledge_policy.flush",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	propertySuppressionChanges := 0

	defer func() {
		span.SetAttributes(
			attribute.Int("batch.row_count", len(merged)),
			attribute.Int("batch.property_suppression_changes", propertySuppressionChanges),
		)
		span.End()
		if metrics != nil {
			metrics.ObserveAccessFlushDuration(flushCtx, time.Since(start).Seconds())
		}
	}()

	now := time.Now().UnixNano()
	for entityID, delta := range merged {
		result, err := f.store.ReadAccessMeta(AccessMetaReadRequest{Mode: AccessMetaReadPoint, EntityID: entityID})
		if err != nil {
			f.outcomes.RecordError("access metadata read", err)
			continue
		}
		entry := result.Entry
		if entry == nil {
			entry = &AccessMetaEntry{
				TargetID:    entityID,
				TargetScope: ScopeNode,
			}
		}

		meta := EntityMeta{Scope: entry.TargetScope}
		if f.entityMeta != nil {
			if resolved, err := f.entityMeta.GetEntityMeta(entityID); err == nil {
				meta = resolved
				if meta.Scope != "" {
					entry.TargetScope = meta.Scope
				}
			} else {
				f.outcomes.RecordError("access entity metadata", err)
			}
		}

		appliedFixed := f.applyOnAccessMutations(entityID, entry, meta, now)

		if !appliedFixed["accessCount"] {
			entry.Fixed.AccessCount = int64(counter(entry.Fixed.AccessCount).merge(delta.accessCount))
		}
		if !appliedFixed["traversalCount"] {
			entry.Fixed.TraversalCount = int64(counter(entry.Fixed.TraversalCount).merge(delta.traversalCount))
		}
		if !appliedFixed["lastAccessedAt"] {
			entry.Fixed.LastAccessedAt = int64(timestamp(entry.Fixed.LastAccessedAt).merge(delta.lastAccessedAt))
		}
		if !appliedFixed["lastTraversedAt"] {
			entry.Fixed.LastTraversedAt = int64(timestamp(entry.Fixed.LastTraversedAt).merge(delta.lastTraversedAt))
		}

		// Custom values merge onto the stored ones by their own kind's rule. A
		// stored value that isn't an int64 is replaced, as before.
		if len(delta.counters) > 0 || len(delta.timestamps) > 0 {
			if entry.Overflow == nil {
				entry.Overflow = make(map[string]interface{})
			}
			for k, v := range delta.counters {
				stored, _ := entry.Overflow[k].(int64)
				entry.Overflow[k] = int64(counter(stored).merge(v))
			}
			for k, v := range delta.timestamps {
				stored, ok := entry.Overflow[k].(int64)
				if !ok {
					entry.Overflow[k] = int64(v)
					continue
				}
				entry.Overflow[k] = int64(timestamp(stored).merge(v))
			}
		}

		entry.LastMutatedAt = now
		entry.MutationCount++

		if f.evaluatePropertySuppression(entityID, entry, meta, now) {
			propertySuppressionChanges++
			// Suppression state changed — trigger re-embedding.
			if f.embedInvalidate != nil && entry.TargetScope != ScopeEdge {
				f.embedInvalidate(entityID)
			}
		}

		f.outcomes.RecordError("access metadata write", f.store.PutAccessMeta(entityID, entry))
		if f.suppressionRecheck != nil {
			f.suppressionRecheck(entityID, meta)
		}
	}
}

// evaluatePropertySuppression checks each property with a decay rule and writes
// nil markers for suppressed properties (or removes them if restored).
// Returns true if any suppression state changed.
func (f *AccessFlusher) evaluatePropertySuppression(entityID string, entry *AccessMetaEntry, meta EntityMeta, nowNanos int64) bool {
	if f.scorerFunc == nil || f.entityMeta == nil {
		return false
	}

	ns := extractNamespace(entityID)
	scorer := f.scorerFunc(ns)
	if scorer == nil {
		return false
	}

	if len(meta.PropertyKeys) == 0 {
		return false
	}

	changed := false
	for _, key := range meta.PropertyKeys {
		res := scoreEntityProperty(scorer, entityID, meta, key, entry, nowNanos)
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
