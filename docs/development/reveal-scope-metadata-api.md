# Reveal scope lease handles

In v1.4.4 the Go reveal-lease inheritance API takes an opaque typed handle
instead of the owning statement's `context.Context`. Migrate callers with
`storage.QueryRevealScopes`; there is no compatibility wrapper and no
context-accepting overload of `InheritQueryRevealScopes`.

```go
owner, release := storage.BeginQueryRevealScopeContext(ctx, engine, reveal)
defer release()

// Capture once, at the point that owns the leases, and keep the handle.
scopes := storage.QueryRevealScopes(owner)

// The borrowing context keeps its own cancellation, deadline and values.
nested := storage.InheritQueryRevealScopes(callerCtx, scopes)
```

| Operation | Call |
| --- | --- |
| Capture the leases of an actual context | `QueryRevealScopes(ctx) RevealScopes` |
| Carry captured leases into a derived context | `InheritQueryRevealScopes(ctx, scopes) context.Context` |
| No lease | `RevealScopes{}` (the zero handle) |

`QueryRevealScopes` reads leases that are already recorded on the actual caller
context. Pass a real context; the call has no nil fallback, and a context
without leases yields the zero handle, which inherits nothing. The handle keeps
the existing lease links and no request context, so capturing it allocates
nothing and cannot manufacture, refresh or replace a lease.

`InheritQueryRevealScopes` returns `ctx` unchanged when the handle holds no live
lease. Otherwise it returns a derived context that keeps the caller's
cancellation, deadline and values and reuses the owning lease objects. Lease
identity, one lease per physical engine, active-only inheritance, release-once
semantics, and database property policy and namespace resolution from the
supplied projection engine are unchanged. Borrowed work must still finish before
its owner releases the scope; a released lease is ignored.

Cypher is the only production caller. `setRevealOnEngine` captures the handle
from the actual statement context when it records reveal ownership for
expression-failure restoration, and `bindExecutionReadScope` captures it for a
nested statement-valued procedure. The expression-failure slot stores the owning
engine and the typed handle under its existing mutex, so it no longer retains a
request context; `clearRevealScope` clears both. `executeWithoutTransaction`
inherits through `InheritQueryRevealScopes(ctx, scope.scopes)`.

No storage format, HTTP or Bolt contract changed.
