package nornicdb

import (
	"context"
	"reflect"
	"sync"

	"github.com/orneryd/nornicdb/pkg/embed"
)

// beginProviderUse is the admission/cancellation owner for foreground provider
// users. Close seals admission under the same lock before cancelling the DB
// lifetime, and waits outside that lock. Providers which honor context cancel
// promptly; an uncancellable call retains its model until it actually returns.
func (db *DB) beginProviderUse(ctx context.Context) (context.Context, func(), error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		db.mu.RUnlock()
		return nil, nil, err
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

// embedderResource is the single release path for a provider, whether it is
// still being checked, installed, replaced, or shared through a cache/registry.
type embedderResource struct {
	provider embed.Embedder
	once     sync.Once
	err      error
}

func sameEmbedder(a, b embed.Embedder) bool {
	return a != nil && reflect.TypeOf(a).Comparable() && a == b
}

func (r *embedderResource) close() error {
	r.once.Do(func() {
		if closer, ok := r.provider.(interface{ Close() error }); ok {
			r.err = closer.Close()
		}
	})
	return r.err
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
	if provider == nil {
		return func() error { return nil }, false
	}
	db.mu.Lock()
	// Comparable identities cover pointer-backed model providers without making
	// a non-comparable custom value panic when used as an interface/map key.
	for _, resource := range db.ownedEmbedders {
		if sameEmbedder(provider, resource.provider) {
			accepted = !db.closed
			db.mu.Unlock()
			return func() error { return db.releaseEmbedder(resource) }, accepted
		}
	}
	resource := &embedderResource{provider: provider}
	accepted = !db.closed
	if accepted {
		db.ownedEmbedders = append(db.ownedEmbedders, resource)
	}
	db.mu.Unlock()
	if !accepted {
		_ = resource.close()
	}
	return func() error { return db.releaseEmbedder(resource) }, accepted
}

func (db *DB) releaseEmbedder(resource *embedderResource) error {
	err := resource.close()
	db.mu.Lock()
	for i, owned := range db.ownedEmbedders {
		if owned == resource {
			db.ownedEmbedders = append(db.ownedEmbedders[:i], db.ownedEmbedders[i+1:]...)
			break
		}
	}
	db.mu.Unlock()
	return err
}

// Called only after cancellation and joining every DB-owned model user. Retain
// replaced providers until here: an in-flight query may still hold its snapshot.
func (db *DB) closeOwnedEmbedders() []error {
	db.mu.Lock()
	resources := db.ownedEmbedders
	db.ownedEmbedders = nil
	db.mu.Unlock()
	var errs []error
	for _, resource := range resources {
		if err := resource.close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}
