# APOC integer GCD and LCM

Author: Codex NOR-184 implementation lane, 29 October 2026.

`apoc.math.gcd`, `apoc.number.gcd`, `apoc.math.lcm` and `apoc.number.lcm` are
NornicDB extensions. The checked APOC Core 5.26.30 and Extended 5.26.4 sources do
not define these functions or an overflow policy.

GCD is nonnegative, with `gcd(0,0)=0`. LCM is nonnegative and is zero when either
integer operand is zero. Both namespaces use the same implementation. A result
larger than MaxInt64 is an error; arithmetic never silently wraps or panics.

| Input | GCD | LCM |
| --- | --- | --- |
|12,-18|6|36|
|0,0|0|0|
|MinInt64,0|error|0|
|MinInt64,2|2|error|
|MaxInt64,MaxInt64|MaxInt64|MaxInt64|
|MaxInt64,2|1|error|
|4,000,000,000 and 6,000,000,000|2,000,000,000|12,000,000,000|

Registered Cypher and root `apoc.Call` invocations use the existing checked
integer conversion policy: signed and unsigned integers within int64, and finite
whole-valued floating inputs within int64, are accepted. Null, fractional or
nonfinite floats, out-of-range numbers, text and other nonnumeric values are
errors. Argument validation happens before arithmetic, including a zero operand.

The four Go exports (`math.Gcd`, `math.Lcm`, `number.GCD`, `number.LCM`) now return
`(int64,error)` so their full int64 input domain can report an unrepresentable
nonnegative result. Callers must handle the error:

```go
value, err := apocmath.Gcd(a, b)
if err != nil {
    return err
}
```

The unsigned magnitude of MinInt64 is representable internally; it is not
narrowed to a signed result. GCD reports an error only if that final magnitude
exceeds int64. LCM checks its reduced product before multiplying.

## Registered Go handlers

Author: Codex NOR-184 lane, checked invocation contract revision.

Published handlers are now opaque `*registry.Callable` values. This is a Go API
compatibility change: `apoc.Function.Handler`, `registry.FunctionDescriptor.Handler`,
`nornicdb.PluginFunction.Handler` and `cypher.PluginFunctionLookup` no longer expose
arbitrary raw functions. `FunctionDescriptor.Function` is removed. Registrations
still accept Go functions, but validate their result protocol before publication.
The shared Go protocol permits a single capability value, such as a function;
it does not impose Cypher's narrower result-value domain. Cypher rejects declared
function, channel and unsafe-pointer results before invoking the producer.

```go
handler, err := registry.Compile(apocmath.Lcm)
if err != nil {
    return err
}
value, err := handler.Call([]interface{}{int64(12), int64(18)})
if err != nil {
    return err
}
```

Use `Signature()` and `Symbol()` for metadata. Published handlers expose no
raw function view. Ordinary and error-bearing functions both use `Call`,
`CallWithPolicy` or `InvokeValues`, converging on the same checked invoker;
raw functions and reflection results remain private. `CallWithPolicy` selects
argument admission for Go, root APOC or Cypher consumers, without providing an
alternative invocation implementation. A caller may supply an already-bound
prefix such as its own context; the callable never creates a request lifetime.
An unsupported error-result position fails registration. Existing non-scalar
declarations can retain type metadata but cannot execute.

The bundled plugin's `Functions` method now returns `(map, error)` so failed
initialization/admission is reported by plugin loading. The host also continues
to accept legacy single-map providers and their direct-function or descriptor
transports. Plugin binaries must be rebuilt against the matching Go and NornicDB
version. Cypher's context forwarding, cancellation and scalar input policies are
unchanged; ordinary and error-bearing invocations share one execution and result owner.
