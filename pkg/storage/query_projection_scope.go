package storage

import (
	"context"
	"sync"
	"sync/atomic"
)

type queryProjectionScopeKey struct{}
type queryProjectionScope struct {
	engine *BadgerEngine
	active atomic.Bool
	reveal bool
}
type queryProjectionScopeLink struct {
	scope *queryProjectionScope
	next  *queryProjectionScopeLink
}

// Borrowed handles retain the lease and its release guard, never the owning
// request context. Keep the context in a separate allocation for that boundary.
type queryProjectionLease struct {
	scope queryProjectionScope
	link  queryProjectionScopeLink
	once  sync.Once
}

type queryProjectionContext struct {
	context.Context
	link *queryProjectionScopeLink
}

func (ctx *queryProjectionContext) Value(key any) any {
	if _, ok := key.(queryProjectionScopeKey); ok {
		return ctx.link
	}
	return ctx.Context.Value(key)
}

func (lease *queryProjectionLease) release() {
	lease.once.Do(func() {
		lease.scope.active.Store(false)
		lease.scope.engine.endQueryRevealScope(lease.scope.reveal)
	})
}

// BeginQueryRevealScopeContext owns the existing physical-store reveal mutex,
// recording an opaque, live lease on ctx. Nested work on the same physical
// engine reuses that lease, preserving the outer query's reveal mode; distinct
// engines acquire independent scopes. Database property policy is not cached
// in this marker: projections still resolve their supplied database engine.
// Release exactly once with defer; contexts borrowed from it must not outlive it.
// For example: ctx, release := BeginQueryRevealScopeContext(ctx, engine, false).
func BeginQueryRevealScopeContext(ctx context.Context, engine Engine, reveal bool) (context.Context, func()) {
	be, _ := UnwrapEngine(engine).(*BadgerEngine)
	if be == nil {
		return ctx, func() {}
	}
	scopes, _ := ctx.Value(queryProjectionScopeKey{}).(*queryProjectionScopeLink)
	for link := scopes; link != nil; link = link.next {
		if link.scope.engine == be && link.scope.active.Load() {
			return ctx, func() {}
		}
	}
	be.beginQueryRevealScope(reveal)
	lease := &queryProjectionLease{}
	lease.scope.engine, lease.scope.reveal = be, reveal
	lease.scope.active.Store(true)
	lease.link.scope, lease.link.next = &lease.scope, scopes
	return &queryProjectionContext{Context: ctx, link: &lease.link}, lease.release
}

// RevealScopes is an opaque, copyable handle to the genuine query reveal leases
// recorded on an actual context. The zero value records no lease and cannot
// manufacture one; only QueryRevealScopes reads existing leases from a caller
// context. Holding the handle instead of that context keeps request cancellation
// and request values out of the borrowing owner, and it reuses the existing
// lease links, so capturing metadata allocates nothing.
type RevealScopes struct {
	link *queryProjectionScopeLink
}

// Mode reports the reveal mode of a live lease for engine's physical store.
// A missing or released lease returns owned=false; a handle cannot create one.
// For example: reveal, owned := QueryRevealScopes(ctx).Mode(engine).
func (scopes RevealScopes) Mode(engine Engine) (reveal, owned bool) {
	be, _ := UnwrapEngine(engine).(*BadgerEngine)
	if be == nil {
		return false, false
	}
	for link := scopes.link; link != nil; link = link.next {
		if link.scope.engine == be && link.scope.active.Load() {
			return link.scope.reveal, true
		}
	}
	return false, false
}

// QueryRevealScopes captures the query reveal leases currently recorded on ctx.
// ctx must be the actual caller context; a context without leases yields the
// zero RevealScopes handle, which inherits nothing. Capture once where the
// owning statement is known and keep the handle; do not retain ctx for it.
func QueryRevealScopes(ctx context.Context) RevealScopes {
	link, _ := ctx.Value(queryProjectionScopeKey{}).(*queryProjectionScopeLink)
	return RevealScopes{link: link}
}

// InheritQueryRevealScopes carries the genuine live leases held by scopes into a
// derived evaluator context without changing its cancellation, deadline or
// values. An empty handle cannot manufacture a lease; released leases are
// ignored. Only use this while the owner is active, e.g. when restoring a
// statement's expression-failure context before a nested service call.
func InheritQueryRevealScopes(ctx context.Context, scopes RevealScopes) context.Context {
	if scopes.link == nil {
		return ctx
	}
	current, _ := ctx.Value(queryProjectionScopeKey{}).(*queryProjectionScopeLink)
	changed := false
	for link := scopes.link; link != nil; link = link.next {
		if !link.scope.active.Load() {
			continue
		}
		found := false
		for existing := current; existing != nil; existing = existing.next {
			if existing.scope == link.scope {
				found = true
				break
			}
		}
		if !found {
			current = &queryProjectionScopeLink{scope: link.scope, next: current}
			changed = true
		}
	}
	if !changed {
		return ctx
	}
	return context.WithValue(ctx, queryProjectionScopeKey{}, current)
}

// BeginQueryResultProjectionContext joins an already owned query scope or
// acquires a normal one for an external query. It is safe for a service called
// by a scoped Cypher procedure: it never recursively acquires the same RWMutex.
// For example: ctx, view, release := BeginQueryResultProjectionContext(ctx, engine);
// defer release(); return view.Node(node). A reused scope never reuses another
// database's policy or namespace. Do not retain ctx or view after release.
func BeginQueryResultProjectionContext(ctx context.Context, engine Engine) (context.Context, QueryResultProjection, func()) {
	view := NewQueryResultProjection(engine)
	if view.engine == nil {
		return ctx, view, func() {}
	}
	ctx, release := BeginQueryRevealScopeContext(ctx, engine, false)
	view.now = DecayScoringTime()
	return ctx, view, release
}
