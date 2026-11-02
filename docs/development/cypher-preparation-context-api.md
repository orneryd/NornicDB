# Cypher preparation contexts

Go callers provide a nonnil operation context to `Parser.Parse(ctx, query)`,
`IsRetrySafeMergeCommitQuery(ctx, info)` and
`MergeUniqueConflictIsRetrySafe(ctx, statements, commitErr)`. The parser returns
its existing Query/error pair and never returns a partial Query on cancellation.
`Executor.ParseAndValidate` passes its original context into the same parser.

```go
query, err := cypher.NewParser().Parse(ctx, "RETURN 1 AS value")
if err != nil {
    return err
}
_ = query
```

Retry classification remains boolean. A canceled classification returns false;
it does not replace a failed commit's authoritative error or alter its durable
outcome. Bolt RUN and COMMIT pass their actual operation contexts to that same
classifier owner.

Private schema-drop, syntax/dependency cache, projection and Cypher 5 import
preparation reuse the existing contextual owners. Cancellation cannot turn a
missing preparation result into a fabricated syntax error or publish a partial
cache verdict. The canonical lightweight tokenizer polls between lexical work;
it does not add another lexer or grammar pipeline. Quoted tokens expand a bounded
source view while one scan body retains delimiter/escape state. Existing
context-free quoted utilities keep their distinct two/three-result lexical
contracts; they are not request-preparation fallback entry points. Opaque
interpolation callbacks, numeric/comment primitives and standard-library
unquoting are not claimed to be interruptible.

Credential capture used by diagnostic redaction retains independent completion:
when the incoming context is cancellable it uses `context.WithoutCancel` for
that one capture operation, preserving values. Canceled diagnostics remain
redacted. Explicit BEGIN also retains its separate snapshot lifetime while its
pre-snapshot drain keeps the request context.

These Go API changes do not alter HTTP/Bolt syntax, stored keys, payload formats
or the storage version.
