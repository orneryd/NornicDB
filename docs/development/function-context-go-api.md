# Registered Go function context and observations for v1.5.0

`fn.Func` receives the actual caller context separately from expression
bindings. The request bindings retain their existing layout.

```go
type Func func(workCtx context.Context, scope fn.Context, args []string) (interface{}, error)

value, found, err := fn.EvaluateFunction(workCtx, name, args, scope)
```

`fn.Register` takes the function and the read-observation policy that says how
its database access is observed:

```go
fn.Register(name string, implementation fn.Func, observation fn.ReadObservation)
```

Migrate registrations, direct handler calls and registry dispatch to the new
argument order. Supply the actual statement/caller context, rather than a
background context in production. There is no old-signature forwarding adapter.
For example, a contextual identity registration can preserve lazy evaluation:

```go
fn.Register("example.identity", func(workCtx context.Context, scope fn.Context, args []string) (interface{}, error) {
    if err := workCtx.Err(); err != nil {
        return nil, err
    }
    if len(args) != 1 {
        return nil, fmt.Errorf("expected one argument")
    }
    return scope.Eval(args[0])
}, fn.OpaqueReads)
```

Receiving the context is necessary but does not automatically interrupt a handler.
Query-sized work must observe it at its actual work boundaries and preserve
ordinary errors and accepted-write outcomes. Current casing boundary checks do
not claim interruption inside the library mapper. The independently deferred
NOR-187 casing defect is not changed by this API migration.

## Read-observation policy

The policy type itself is canonical in `internal/queryprofile`:
`queryprofile.ReadObservation` with `queryprofile.OpaqueReads` (the zero value)
and `queryprofile.CanonicalReads`. Every registration owner names that one type
and validates through `ReadObservation.Valid()`, so a policy value means the
same thing wherever it is declared:

- `fn.ReadObservation`, `fn.OpaqueReads`, `fn.CanonicalReads` —
  aliases/constants for the Cypher function registry.
- `registry.ReadObservation`, `registry.OpaqueReads`, `registry.CanonicalReads`
  — aliases/constants for the APOC callable registry.

`fn.ReadObservation` is the typed policy a registration declares for the
database reads its implementation performs. `fn.OpaqueReads` is the zero value
and the policy for every custom registration:

- `fn.OpaqueReads` — the callback's complete database access is not visible to
  the caller's reader observations. Each actual invocation is reported through
  the existing opaque-callback hook (`queryprofile.Callback`), so `PROFILE`
  reports the statement's total database hits as unobserved instead of
  certifying them from the reads it can see. Pass this for a custom handler, a
  plugin closure, or a built-in that reads storage through a path that does not
  report logical rows and access units.
- `fn.CanonicalReads` — the implementation's reads are already complete: it
  either reads no database state (a pure value function) or reports its own
  reads through the canonical observation owners. Its actual invocations are
  not reported as opaque callbacks.

Policy belongs to the registration, not to the name. Registering an existing
name replaces the implementation and the policy together under the registry
lock, so a replacement of a built-in is opaque unless its own registration
says otherwise. A built-in name is never trusted on the strength of the name.
The policy lives in the finite registry entry rather than in per-call state: a
registration costs one-time registry space, and a canonical registration skips
the opaque-callback hook entirely.

Procedure registrations declare the same policy. `RegisterBuiltIn`,
`RegisterUser` and `RegisterUserProcedure` store `fn.OpaqueReads`, whatever
name they register, including the name of a built-in; only a reviewed
package-private built-in registration opts a handler into
`fn.CanonicalReads`. The graph-token listings `db.labels`,
`db.relationshipTypes` and `db.propertyKeys` declare it, and report the node
or relationship population each one actually read. The schema procedures
`db.indexes`, `db.index.stats` and `db.constraints` report their catalog
acquisitions; vector-index state requests use the one resolved search service.

### Admitted APOC callables

The APOC callable registry admits a Go function together with the same policy,
and stores it next to the admitted function:

```go
handler, err := registry.Compile(function, registry.OpaqueReads)   // custom implementation
bundled, err := registry.Compile(apocmath.Lcm, registry.CanonicalReads) // reviewed pure implementation
observation := handler.ReadObservation()
```

`registry.Register` and `registry.RegisterFunction` take the observation in the
same position, so a published descriptor carries the policy it was admitted
under. Every bundled registration in `apoc` states its own policy explicitly:
a reviewed implementation that reads no database state declares
`registry.CanonicalReads`, and a storage-backed adapter declares
`registry.OpaqueReads` until its reads are instrumented. A custom registration,
a raw plugin function and anything this repository does not review stay
`registry.OpaqueReads`. Plugin transport preserves an explicitly admitted
callable and its declared policy; it does not attest that the declaration is
correct. The Go host is responsible for its explicit policy declarations, just
as for `fn.Register`. Helpers that invoke caller-supplied functions remain
opaque because those callbacks can perform their own graph reads.

Admission is final for the pair. `registry.Compile` returns an already admitted
`*registry.Callable` unchanged when the requested policy is the one it was
admitted with, and rejects a redeclaration under a different policy
(`registry.ErrConflictingReadObservation`): dispatch always uses
`ReadObservation()` and never retags an admitted handler.
`registry.AdmissionObservation(function)` is that rule for registrars that
accept either an admitted handler or a raw implementation.
`registry.DescribeNonScalar` keeps a non-scalar declaration as noninvocable
opaque metadata.

Query dispatch reports an opaque callback only when the implementation actually
runs. A call the admitted owner refuses before entering the implementation — a
metadata descriptor, a policy that cannot express the signature, an argument
count the signature does not accept, an argument the consumer's argument policy
cannot bind — reports `registry.ErrNotInvoked` (still an
`registry.ErrUnsupportedSignature`), and no opaque callback is recorded for it,
so a built-in that answered instead is never charged with an unobserved read.

### Invocation phase versus error cause

`registry.InvocationStarted(err)` is the phase proof of one call, and the
invocation boundary owns it. Pass the error that call returned:

- `nil` — the call returned, so the implementation ran.
- The outermost pre-entry rejection receipt the boundary returns — the
  implementation never ran, and nothing is reported for it.
- Any other error — the implementation ran and reported that error.

The receipt is a private, sealed error type that `CallWithPolicy` and
`InvokeValues` return outermost for every pre-entry rejection; no implementation
can construct one. `errors.Is(err, registry.ErrNotInvoked)` answers a different
question: it classifies the cause chain, so it also matches a rejection that a
nested call produced and the implementation returned, or wrapped in its own
error. That match describes some rejected call in the chain — never proof that
this call was refused — and dispatch must not use it to decide whether to
report an invocation. The boundary keeps the two readings apart: an
implementation's own error that carries a rejection is returned as that call's
entered result under an ordinary carrier, so `errors.Is`/`errors.As` still
classify it while `InvocationStarted` reports it as entered. A rejected
signature, policy, argument count or argument binding never reports an opaque
callback.

A panicking implementation was entered, so the adapter reports one opaque
invocation for it before the panic reaches the existing recovery. The panic is
not recovered, replaced or swallowed at that boundary, and neither the query
result nor its error changes.

HTTP/Bolt/Cypher syntax, Badger keys, persisted values, WAL and storage version
are unchanged. Rebuild Go consumers and plugins against the new signatures.

## Shared case-function registration

`fn.CaseFunction(name, mapper, wrongArity)` owns argument evaluation, null and
type handling, and cancellation before/after the Unicode mapper. Native
`upper`/`lower` and `toUpper`/`toLower` register this same owner directly.
`fn.UpperCaseText` and `fn.LowerCaseText` create the existing language-neutral
Unicode mapping per call; they do not share mutable mapper state.

The optional `fn.WrongArityError` policy accepts the invoked function name,
expected argument count as text, and actual count. Passing nil retains the
`toUpper`/`toLower` null-on-wrong-count contract. The strict names pass the
existing canonical argument-error constructor directly, without a forwarding
policy adapter. For example:

```go
fn.Register("customUpper", fn.CaseFunction("customUpper", fn.UpperCaseText, nil), fn.OpaqueReads)
fn.Register("customLower", fn.CaseFunction("customLower", fn.LowerCaseText, nil), fn.OpaqueReads)
```

The mapper remains atomic. Cancellation stops argument/mapper boundaries; it
does not interrupt `golang.org/x/text` inside one mapping operation.
