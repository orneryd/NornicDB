package testfixture

import (
	"context"
	"sync"

	"github.com/orneryd/nornicdb/internal/queryprofile"
)

type queryReadKey struct{ kind, detail string }

// QueryReadObserver records request-owned access counters in component tests
// that cannot import the Cypher collector. Its zero value is ready for use;
// bind it with queryprofile.WithObserver(ctx, &reads). It retains no context.
type QueryReadObserver struct {
	mu        sync.Mutex
	counters  map[queryReadKey]*queryprofile.ReadCounter
	order     []queryReadKey
	callbacks []string
}

// Access implements queryprofile.Observer and reuses one counter per read key.
func (o *QueryReadObserver) Access(_ context.Context, kind, detail string) *queryprofile.ReadCounter {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.counters == nil {
		o.counters = make(map[queryReadKey]*queryprofile.ReadCounter)
	}
	key := queryReadKey{kind, detail}
	if counter := o.counters[key]; counter != nil {
		return counter
	}
	counter := new(queryprofile.ReadCounter)
	o.counters[key] = counter
	o.order = append(o.order, key)
	return counter
}

// UnobservedRead records an actual opaque callback invocation.
func (o *QueryReadObserver) UnobservedRead(_ context.Context, category, name string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if name != "" {
		category += ": " + name
	}
	o.callbacks = append(o.callbacks, category)
}

// Reads distinguishes an absent read from a known zero population.
func (o *QueryReadObserver) Reads(kind, detail string) (rows, hits int64, recorded bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	counter, recorded := o.counters[queryReadKey{kind, detail}]
	if !recorded {
		return 0, 0, false
	}
	rows, hits = counter.Values()
	return rows, hits, true
}

// RecordedKinds returns read kinds in acquisition order, detached from the owner.
func (o *QueryReadObserver) RecordedKinds() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	kinds := make([]string, len(o.order))
	for i, key := range o.order {
		kinds[i] = key.kind
	}
	return kinds
}

// Callbacks returns detached invocation records, including repeated invocations.
func (o *QueryReadObserver) Callbacks() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.callbacks...)
}

// This component ledger has no clause scope to freeze.
func (o *QueryReadObserver) WithLifetime(lifetime context.Context, _ context.Context) context.Context {
	return queryprofile.WithObserver(lifetime, o)
}
