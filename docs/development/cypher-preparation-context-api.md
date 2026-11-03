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

## USE and clause-scanner migration

The existing Go APIs below now take the actual nonnil operation context first
and return an explicit error. Rebuild callers and handle that error before using
any observation; cancellation is not a missing keyword, a generic pattern, an
empty split, or a cached parsing verdict.

```go
func (*ASTBuilder) Build(context.Context, string) (*AST, error)
func (*QueryInfo) GetAST(context.Context) (*AST, error)
func ParseIndexHints(context.Context, string) ([]IndexHint, string, error)
func FindKeywordIndex(context.Context, string, string) (int, error)
func ContainsKeyword(context.Context, string, string) (bool, error)
func SplitByKeyword(context.Context, string, string) ([]string, error)
func SplitByMatch(context.Context, string) ([]string, error)
func SplitByCreate(context.Context, string) ([]string, error)
func ExtractLimit(context.Context, string) (int, bool, error)
func ExtractSkip(context.Context, string) (int, bool, error)
func ExtractLimitString(context.Context, string) (string, error)
func ExtractSkipString(context.Context, string) (string, error)
func DetectQueryPattern(context.Context, string) (PatternInfo, error)
```

`GetAST` propagates construction errors instead of discarding them. Failed or
canceled construction does not mark its AST built; a healthy call may retry on
the same `QueryInfo`. Keyword and pipeline-template caches likewise do not
publish canceled preparation results.

Fabric retains its existing callback type names and setters:

```go
type ClauseKeywordNameReader func(context.Context, string, int, int, string) bool
type CallImportReader func(context.Context, string, bool, string) ([]string, error)
```

Both callbacks receive each planning operation's context. Bind the existing
stateless reader functions, not a closure or planner field retaining a request
context. The keyword-name predicate remains boolean; its consumer must retain
and check that same context before interpreting or publishing the observation.
Import callback errors retain their identity.

USE reference preparation and standalone ORDER rewriting use the same operation
context as their canonical keyword scanners. The public regression uses a valid
32MiB whitespace USE query in both parser modes, measures retirement from actual
cancellation, and checks healthy execution and same-executor retry. The 100ms
regression bound is workload-specific, not a universal bound on every primitive.

### Measured preparation cost

The maintainer accepted the measured CPU cost of this cancellation correction.
On Go 1.26.9, Darwin arm64, Apple M2 Max, twelve alternating paired samples
per parser/workload compared frozen, type-compatible pre-migration and current
inputs. Every timed public query returned exactly `[[int64(7)]]`.
The query was `USE` followed by the stated whitespace, a quoted database name,
and `RETURN 7 AS k ORDER BY k`. Result and plan caches were disabled; preparation
caches were warm, and CPU profiling ran during every sample.

| Parser | Spaces | Baseline median ns/op | Current median ns/op | Paired geometric effect | Paired bootstrap 95% interval |
| --- | ---: | ---: | ---: | ---: | --- |
| nornic | 1 | 17,754.5 | 18,494.5 | +3.19% | +1.42% to +4.27% |
| nornic | 1,024 | 28,300 | 29,075 | +3.99% | +2.09% to +7.25% |
| nornic | 32,768 | 333,188.5 | 332,804 | -0.02% | -0.64% to +0.57% |
| antlr | 1 | 17,963 | 18,699 | +3.91% | +3.52% to +4.23% |
| antlr | 1,024 | 28,566.5 | 29,361.5 | +2.57% | +1.43% to +3.57% |
| antlr | 32,768 | 331,773 | 332,571 | +0.26% | -0.13% to +0.62% |

Allocations were unchanged: 69/72/72 per operation for nornic and 70/73/73
for antlr. Small-query overhead is measurable, not noise. Large-query results
do not establish equivalence or nondegradation. These shared-machine
measurements qualify only these workloads, not cold preparation, full 32MiB
healthy throughput, whole ingestion or final merge readiness. The baseline
restores the complete type-compatible scanner migration population rather than
isolating a single scanner call.
