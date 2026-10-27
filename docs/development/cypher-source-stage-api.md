# Explicit source stages for Go readers

Cypher source is projected once before compilation. External callers pass
`cypher.RawSource`; compiler consumers pass `cypher.AcceptedSource` for text
already projected by the execution source owner. The constants alias the same
canonical type exported by `antlr.SourceStage` and `fabric.SourceStage`.

The required final argument applies to `ResultColumns`,
`StatementResultColumns`, `StatementResultColumnsContext`, `StatementFacts`,
`StatementFactsWith`, `QueryPermissionRequirements` and
`RegisteredProceduresForCalls`, `ResultProjection` and `ParseKnowledgePolicyDDL`. `resultmetadata.FactsProvider` and the rewriting
header interface carry the same explicit stage. For example:

```go
columns, err := executor.StatementResultColumnsContext(ctx, clientQuery, cypher.RawSource)
permissions := cypher.QueryPermissionRequirements(ctx, clientQuery, cypher.RawSource)
command, owned, err := cypher.ParseKnowledgePolicyDDL(ctx, clientQuery, cypher.RawSource)
```

`AuthorizeReadOnlyQuery` remains a raw-client-source boundary. HTTP and Bolt
query syntax is unchanged. A generated literal backslash followed by `u0062`
must remain literal compiler text; passing it as raw source would apply an
unwanted second Unicode projection. Source identity is never guessed from
contents or context. Raw metadata spans and participant obligations refer to
original client bytes; cached execution products keep accepted coordinates.

`ParseKnowledgePolicyDDL` also requires the original context as its first argument.
Execution retains the source view through nested policy blocks so persisted WHEN
predicates and ON ACCESS expressions keep their original spelling. Accepted
standalone input without a retained original is emitted as safe raw source before
storage, preserving once-only evaluation after native reopen.

ANTLR syntax readers use the same explicit stage on one API:

```go
syntax, err := antlr.ParseSyntax(ctx, clientQuery, antlr.StatementEntry, false, antlr.RawSource)
compiled, err := antlr.ParseSyntax(ctx, acceptedText, antlr.FragmentEntry, false, antlr.AcceptedSource)
```

The separate `ParseSyntaxView` entry point is retired. A caller holding a source
view checks `view.Err` and passes `view.Text` with `antlr.AcceptedSource`. The
request source retains original spelling; syntax cache identity includes the
explicit stage, entry and dialect. Cancellation remains synchronous through the
owned ANTLR character/token stream interfaces; the third-party runtime is unchanged.
