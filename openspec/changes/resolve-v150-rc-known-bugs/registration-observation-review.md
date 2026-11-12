# Registered function observation contract

The operator approved `fn.Register(name, implementation, observation)` and
canonical-caller migration. The old two-argument signature is retired without an
adapter. One registry entry owns both implementation and typed policy, replaced
and read together under the existing mutex. Invalid policies fail before
publication. No name-based trust or function-pointer classification is used.

The public both-parser reproduction returns `ABC` for `toUpper('abc')` but the
predecessor falsely marks this source-owned value function as an opaque callback.
Its corrected public control and integrated PROFILE/EXPLAIN/read controls pass
under race instrumentation (16.693 s). The complete function package passes under
race instrumentation (1.601 s), including replacement of a built-in name,
concurrent registration/invocation, cancellation, nil/error admission and nested
argument reads. Custom registrations use `OpaqueReads`; source-reviewed functions
whose actual reads are complete use `CanonicalReads`. Replacing a built-in with
an opaque callback replaces the policy too.

All repository registration callers and the Go API guide migrate. Pure value
functions reuse their existing evaluator/converter; no scalar implementation is
copied. Canonical dispatch remains `EvaluateFunction` and `FunctionResult`.

## Source classification and remaining coverage

Value-only built-ins, entity scalars over already-bound values, and `reveal`'s
canonical argument evaluation are observed. GDS asNode/asNodes call the existing
point-read owner, which records each actual node access. The following remain
opaque pending reader instrumentation; being bundled does not prove completeness:

- GDS link-prediction neighbor reads (`ReadAdjacentEdges`).
- Graph-projection existence/property lookups.
- Composite graph-name/property catalog enumeration.
- Native APOC/plugin callback implementations not carrying an observation proof.

The operator requires built-in route coverage; these entries remain work, not
accepted exceptions. Custom callbacks may explicitly remain unobserved.

## Ordinary lookup measurements

Apple M2 Max / Go1.27.3, three 200,000-operation samples per name, serial
CPU/memory captures. The unchanged pre-migration function source at `7a1f80f7a`
is byte-identical to `29dd66a9f` for this package. Medians:

| Name shape | Before ns/op | Current ns/op | B/op before/current | allocs/op before/current |
| --- | ---: | ---: | ---: | ---: |
| Lower-case ASCII | 52.14 | 47.96 | 0 / 0 | 0 / 0 |
| Upper-case ASCII | 94.56 | 82.72 | 24 / 24 | 1 / 1 |
| Lower-case Unicode | 67.73 | 67.73 | 0 / 0 | 0 / 0 |
| Upper-case Unicode | 88.86 | 90.30 | 24 / 24 | 1 / 1 |

Ranges overlap; no speed improvement is claimed. No per-call bytes/allocations
are added. A separate same-implementation policy control measures 36.47 ns opaque
and 34.79 ns canonical, both zero bytes/allocations. Registry entries carry finite
one-time policy storage; this is not returned-row or per-invocation retained state.
Logs/captures: `/tmp/v150-function-policy-{before,after}.*`.
