# Rendering localized Go messages and errors

The v1.4.4 Go API uses `localization.Manager.Render` as the single rendering owner. Pass a typed `localization.RenderRequest` to select the message or error boundary. HTTP, Bolt, GraphQL, and MCP language behavior is unchanged.

```go
text, tag, err := manager.Render(ctx,
    localization.MessageRequest(localization.DatabaseNotFound("analytics")))

text, tag, err = manager.Render(ctx,
    localization.ErrorRequest(localizedError))
```

Migrate the former `localizedError.Render(ctx, manager)` call to the second form. `pkg/errors.Localized` remains an alias of `localization.LocalizedError`; its `Error`, `Unwrap`, identity, and cause contracts remain available. The old error `Render` method is retired.

`MessageRequest` captures a message value; its template data retains the existing shallow-copy behavior. `ErrorRequest` references the error's message, preserving its existing lifetime and nil-error behavior. Rendering uses no error code or cause fields and does not change the original error.

Custom implementations of `cypher.ProcedureMetadataRenderer` now accept `localization.RenderRequest` instead of `localization.Message`. The request exposes `Purpose` and `Message *localization.Message`, so custom renderers can inspect the original ID, data, and plural count. Procedure descriptions pass `MessageText`. Callers should retain the original request context for locale selection.

When the caller already owns an immutable message, it can pass its address directly:

```go
request := localization.RenderRequest{
    Purpose: localization.MessageText,
    Message: &ownedMessage,
}
text, tag, err := manager.Render(ctx, request)
```

This avoids copying another message for an escaping interface consumer. `SHOW PROCEDURES` uses its fresh descriptor-list snapshot in this way. Keep the pointed-to message immutable while rendering; changing a retained snapshot header does not change the procedure registry.

| Request | Manager | Result |
|---|---|---|
| `ErrorText`, nil message | Any | Empty text, American English, no error |
| `ErrorText`, present message | nil | Its English fallback or message ID, no error |
| `MessageText`, nonempty fallback | nil | The English fallback, no error |
| `MessageText`, empty fallback or nil message | nil | The message ID and a missing-manager error |
| Present message | Present | Catalog rendering using request preferences and process defaults |

A zero request has `MessageText` semantics with an empty message. Catalog rendering keeps input data and structured diagnostics immutable, so the same error can be rendered concurrently in different languages.

## Validation scope

The request migration preserves nil policies, English/Spanish/pseudo-locale output, error identity, and immutable structured diagnostics. Tests also exercise actual MCP HTTP typed errors and an external `ProcedureMetadataRenderer` implementation that retains descriptor snapshots without changing registry headers.

On Apple M2 Max, darwin/arm64, Go 1.26.9, GOMAXPROCS=1, six samples per version in ABBA order compared the frozen pre-migration tree `2ddf47c34ee19b9a796f49de3130ff21f3f27db5` with the migration. Both public benchmark builds used `noui,nolocalllm`.

| Public workload | Before | After | Allocations/op |
|---|---:|---:|---:|
| Uncached localized `SHOW PROCEDURES`, 124 rows | 352.617 µs | 355.201 µs | 6,468 → 6,468 |
| MCP HTTP typed tool error, nil manager | 5.412 µs | 5.402 µs | 57 → 57 |
| MCP HTTP typed tool error, Spanish catalog | 7.934 µs | 7.952 µs | 91 → 91 |

Timing ranges overlap for all three workloads; these measurements establish no meaningful public-path change, rather than a speedup. The SHOW control counted 88 rendering callbacks on each of two repeated executions with the result cache disabled. Both versions produced identical row and HTTP body digests. MCP allocation sizes remained 9,368 and 10,920 B/op; SHOW sizes varied within the same approximately 548 KB/op range.

Isolated nil-boundary microbenchmarks do show added typed-dispatch overhead: message/nil-manager 3.179 → 3.881 ns, error/nil-manager 2.763 → 3.873 ns, and nil-error 2.613 → 2.972 ns, all with zero allocations. These are disclosed separately from the complete public workloads. Catalog, plural, structured-diagnostic and non-nil-error microbenchmarks retain their allocation counts and have overlapping timing ranges.
