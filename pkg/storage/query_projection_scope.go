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
}
type queryProjectionScopeLink struct {
	scope *queryProjectionScope
	next  *queryProjectionScopeLink
}

// BeginQueryRevealScopeContext owns the existing physical-store reveal mutex,
// recording an opaque, live lease on ctx. Nested work on the same physical
// engine reuses that lease, preserving the outer query's reveal mode; distinct
// engines acquire independent scopes. Database property policy is not cached
// in this marker: projections still resolve their supplied database engine.
// Release exactly once with defer; contexts borrowed from it must not outlive it.
// For example: ctx, release := BeginQueryRevealScopeContext(ctx, engine, false).
func BeginQueryRevealScopeContext(ctx context.Context, engine Engine, reveal bool) (context.Context, func()) {
	be, _ := OwnedCapability[*BadgerEngine](UnwrapEngine(engine))
	if be == nil {
		return ctx, func() {}
	}
	scopes, _ := ctx.Value(queryProjectionScopeKey{}).(*queryProjectionScopeLink)
	for link := scopes; link != nil; link = link.next {
		if link.scope.engine == be && link.scope.active.Load() {
			return ctx, func() {}
		}
	}
	release := be.BeginQueryRevealScope(reveal)
	scope := &queryProjectionScope{engine: be}
	scope.active.Store(true)
	ctx = context.WithValue(ctx, queryProjectionScopeKey{}, &queryProjectionScopeLink{scope: scope, next: scopes})
	var once sync.Once
	return ctx, func() { once.Do(func() { scope.active.Store(false); release() }) }
}

// InheritQueryRevealScopes carries genuine live leases from an owning statement
// into a derived evaluator context without changing its cancellation or values.
// An arbitrary context cannot manufacture a lease; released leases are ignored.
// Only use this while the owner is active, e.g. when restoring a statement's
// expression-failure context before a nested service call.
func InheritQueryRevealScopes(ctx, owner context.Context) context.Context {
	if owner == nil {
		return ctx
	}
	inherited, _ := owner.Value(queryProjectionScopeKey{}).(*queryProjectionScopeLink)
	current, _ := ctx.Value(queryProjectionScopeKey{}).(*queryProjectionScopeLink)
	changed := false
	for link := inherited; link != nil; link = link.next {
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
