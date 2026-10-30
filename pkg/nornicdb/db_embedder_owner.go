package nornicdb

import (
	"context"
	"fmt"
	"reflect"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
)

// beginProviderUse is the admission/cancellation owner for foreground provider
// users. Close seals admission under the same lock before cancelling the DB
// lifetime, and waits outside that lock. Providers which honor context cancel
// promptly; an uncancellable call retains its model until it actually returns.
func (db *DB) beginProviderUse(ctx context.Context) (context.Context, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := bulk.ObserveContext(ctx); err != nil {
		return nil, nil, err
	}
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, nil, ErrClosed
	}
	db.foregroundWg.Add(1)
	db.mu.RUnlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(db.LifetimeContext(), cancel)
	return ctx, func() {
		stop()
		cancel()
		db.foregroundWg.Done()
	}, nil
}

// BeginProviderUse retains an owned model during a constructor's raw health
// call, before a published wrapper can supply this same foreground guard.
func (db *DB) BeginProviderUse(ctx context.Context) (context.Context, func(), error) {
	return db.beginProviderUse(ctx)
}

// providerResource is the single release path for a provider, whether it is
// still being checked, installed, replaced, or shared through a cache/registry.
type providerResource struct {
	provider   interface{}
	name       string
	completion lifecycle.OnceCompletion
	reported   bool // guarded by DB.mu
}

func sameEmbedder(a, b embed.Embedder) bool {
	return sameProvider(a, b)
}

func sameProvider(a, b interface{}) bool { return a != nil && reflect.TypeOf(a).Comparable() && a == b }

func (r *providerResource) close() error {
	return r.completion.Do(func() error {
		if closer, ok := r.provider.(interface{ Close() error }); ok {
			return closer.Close()
		}
		return nil
	})
}

// OwnEmbedder gives the database ownership of a constructed provider before its
// health check or installation. The returned idempotent release function frees
// an unsuccessful attempt before retrying. Successful attempts leave release to
// DB.Close, after initialization and queue workers have joined. Cache wrappers
// and registry aliases share the underlying provider's owner.
//
// Initialization must run through RunBackgroundTask so Close also joins model
// construction and health checks. A closing DB refuses and releases new models;
// an already owned provider remains under its existing shutdown owner.
func (db *DB) OwnEmbedder(provider embed.Embedder) (release func() error, accepted bool) {
	for {
		cached, ok := provider.(*embed.CachedEmbedder)
		if !ok {
			break
		}
		provider = cached.Unwrap()
	}
	return db.OwnProvider("embedder", provider)
}

// OwnProvider transfers one constructed provider to the canonical DB lifetime.
// Construction must be enrolled in RunBackgroundTask (the server driver joins
// its constructor work too). A rejected or failed attempt is released through
// the same idempotent path and its cleanup outcome is retained until DB.Close.
func (db *DB) OwnProvider(name string, provider interface{}) (release func() error, accepted bool) {
	if provider == nil {
		return func() error { return nil }, false
	}
	db.mu.Lock()
	// Comparable identities cover pointer-backed model providers without making
	// a non-comparable custom value panic when used as an interface/map key.
	for _, resource := range db.ownedProviders {
		if sameProvider(provider, resource.provider) {
			accepted = !db.closed
			db.mu.Unlock()
			return func() error { return db.releaseProvider(resource) }, accepted
		}
	}
	resource := &providerResource{provider: provider, name: name}
	accepted = !db.closed
	if accepted {
		db.ownedProviders = append(db.ownedProviders, resource)
	}
	db.mu.Unlock()
	if !accepted {
		_ = db.releaseProvider(resource)
	}
	return func() error { return db.releaseProvider(resource) }, accepted
}

func (db *DB) releaseProvider(resource *providerResource) error {
	err := resource.close()
	var attributed error
	if err != nil {
		attributed = fmt.Errorf("%s close: %w", resource.name, err)
	}
	db.mu.Lock()
	// A caller constructing after the DB outcome was published owns its own
	// rejected attempt; it cannot mutate the completed DB outcome retroactively.
	published := false
	if db.closeDone != nil {
		select {
		case <-db.closeDone:
			published = true
		default:
		}
	}
	report := !resource.reported && !published
	if report {
		resource.reported = true
	}
	for i, owned := range db.ownedProviders {
		if owned == resource {
			db.ownedProviders = append(db.ownedProviders[:i], db.ownedProviders[i+1:]...)
			break
		}
	}
	db.mu.Unlock()
	if report {
		db.recordBackgroundError(attributed)
	}
	return err
}

// Accepted asynchronous work reports before its DB retention is released.
func (db *DB) recordBackgroundError(err error) {
	db.ownedWork.Record("", err)
}

// Called only after cancellation and joining every DB-owned model user. Retain
// replaced providers until here: an in-flight query may still hold its snapshot.
func (db *DB) closeOwnedProviders() []error {
	db.mu.Lock()
	resources := db.ownedProviders
	db.ownedProviders = nil
	db.mu.Unlock()
	for _, resource := range resources {
		_ = db.releaseProvider(resource)
	}
	if err := db.ownedWork.Err(); err != nil {
		return []error{err}
	}
	return nil
}

// SetInferenceManager adopts the model before publishing it to db.infer and
// binds every manager alias to DB's existing foreground provider admission.
func (db *DB) SetInferenceManager(manager *heimdall.Manager) bool {
	if manager == nil {
		return false
	}
	manager.SetLifetime(db.beginProviderUse)
	_, accepted := db.OwnProvider("Heimdall inference manager", manager)
	if !accepted {
		return false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return false
	}
	db.cypherExecutor.SetInferenceManager(manager)
	return true
}
