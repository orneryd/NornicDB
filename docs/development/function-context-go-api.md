# Registered Go function context for v1.4.4

`fn.Func` now receives the actual caller context separately from expression
bindings. The request bindings retain their existing layout.

```go
type Func func(workCtx context.Context, scope fn.Context, args []string) (interface{}, error)

value, found, err := fn.EvaluateFunction(workCtx, name, args, scope)
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
})
```

Receiving the context is necessary but does not automatically interrupt a handler.
Query-sized work must observe it at its actual work boundaries and preserve
ordinary errors and accepted-write outcomes. Current casing boundary checks do
not claim interruption inside the library mapper. The independently deferred
NOR-187 casing defect is not changed by this API migration.

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
fn.Register("customUpper", fn.CaseFunction("customUpper", fn.UpperCaseText, nil))
fn.Register("customLower", fn.CaseFunction("customLower", fn.LowerCaseText, nil))
```

The mapper remains atomic. Cancellation stops argument/mapper boundaries; it
does not interrupt `golang.org/x/text` inside one mapping operation.
