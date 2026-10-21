# Pre-decode application-admission audit

The audited release base is `b8fca9d0dd5a76f5f43d9f5d3e53bbb841c26045`.
The family boundary is client payload accumulation and decoding in the Bolt and
HTTP servers, including delegated HTTP handlers and upgraded GraphQL messages.
Transaction-pool accounting remains a separate execution-stage owner.

The subsequent [middleware/ingress audit](middleware-admission-audit.md) records
the outer metadata owner, zero-query control exemption, timeout-worker retention
and Bolt HTTP-upgrade boundary. It supersedes the earlier assumption that the
routed body guard was the first application decoder.

## Graph and source method

Used the shared `graphify` container with one maintained repository worktree
`/repos/nornic-1625` and one graph root `/graphs/nornic-1625`. The initial
code-only extraction at the release base used two workers. Subsequent heads use
checkout/update in that same copy/root; no seeded or per-round copies.

The base graph contains 4,654 extracted code files, 57,079 nodes and 1,625,479
edges, including 76,916 `calls` edges. The production Go projection has 26,700
nodes and 30,652 distinct call pairs. Graph JSON SHA-256:
`b7aa6595a1f92bf9f23b9769b57a5c46599b8d2185ea3a7c3cb4bfcfe3f9c9a3`.
Extraction warns about one ARM C header with syntax errors, 44 files with no
symbols, and duplicate macOS test labels. None establishes a missing Go decoder.

Twenty structural owner queries select exact source-file/function pairs, then
return direct callees and reverse `calls` layers through depth four:

- Bolt `readMessage`, `handleMessage`, `dispatchMessage`, `dispatchInner`,
  `parseHelloAuth`, `handleRun`, `handleBegin`, `handlePull`, `handleDiscard`,
  `parseRunMessage`, `parseStreamingOptions`.
- HTTP `readJSON`, `admitTransactionRequest`, `decodeTransactionRequest`,
  `buildRouter`, `wrapWithMiddleware`.
- GraphQL `NewHandler`, `ServeHTTP`; policy `ReserveWireRequest`, `Reserve`.

The maintained-root audit script and JSON retain the complete query results.
The final code graph is refreshed in that root; its exact-head receipts and
additional owner queries are recorded with the PR verification evidence.

Cross-checks use scoped `rg` inside the worktree, excluding test/generated code:

```text
rg -n 'decodePackStream|parseRunMessage|parseStreamingOptions|parseHelloAuth|dispatchInner' pkg/bolt -g '*.go' -g '!**/*test.go'
rg -n 's\.readJSON\(|json.NewDecoder|json.Unmarshal|ReadAll|ParseMultipart|ParseForm' pkg/server pkg/mcp pkg/heimdall pkg/graphql -g '*.go' -g '!**/*test.go' -g '!generated/**'
rg -n 'r.Body|r.URL.Query|NewDefaultServer|ServeHTTP|ReserveWireRequest|readWireMessage|GrowWireRequest' pkg/bolt pkg/server pkg/graphql pkg/mcp pkg/heimdall pkg/memory -g '*.go' -g '!**/*test.go' -g '!generated/**'
```

Graph/source disagreement: the graph resolves `ReserveWireRequest -> Reserve`
and `readTransactionRequest -> decodeTransactionRequest`, but omits receiver
field/interface edges, including the incoming calls to `readJSON` and the
policy. Scoped source searches find those 27 `readJSON` call sites and the
delegated handlers. Source and pinned gqlgen transport inspection also find GET
variables and post-upgrade JSON that a search for `r.Body` alone misses. No
additional admission consumer remains unique to the completed graph inventory.
The graph is evidence of structure, not proof that unresolved calls are absent.

## Bolt entry points

| Client message | Client decode at the base | Final reservation owner |
|---|---|---|
| HELLO | `handleHello -> parseHelloAuth -> decodePackStreamMap` | `readWireMessage -> Policy.ReserveWireRequest/GrowWireRequest`, then dispatch recheck |
| BEGIN | `handleBegin -> decodePackStreamMap`, including nested transaction metadata | same owner, released after setup |
| RUN | `handleRun -> parseRunMessage`, query/parameters/extras | same owner, replacing the RUN-only gate; result retention remains separate |
| PULL, DISCARD | `parseStreamingOptions`, including recursively decoded unknown option values | same owner before options decoding |
| ROUTE | payload is ignored; no PackStream map decode | accumulated payload bytes still receive the shared wire bound |
| COMMIT, ROLLBACK, RESET, GOODBYE | valid messages have no decoded fields | valid bodyless control messages remain available to release work; extra bytes receive the wire bound |
| LOGON, LOGOFF, unknown signatures | no supported handler in the current Bolt dispatch | rejected as unsupported; any accumulated payload is still bounded |
| Handshake | fixed magic/version words | fixed framing, no arbitrary nested client decode |

Both synchronous `handleMessage` and asynchronous `readMessage` use
`readWireMessage`. It reads only the two signature bytes before admission,
extends one lease before buffer growth, and carries it with a queued message.
Dispatch rechecks cached capacity without double-reserving. Handling, read
failure, enqueue cancellation and connection teardown each release through the
idempotent lease; teardown joins the reader before draining queued reservations.
A partial message rejected during accumulation receives the existing transient
memory error and closes, since its unread remainder cannot be dispatched safely.
No GC or physical sampler runs on these request/reader paths. The wire charge
includes chunk headers and the final terminator. `WireRequest.Move` transfers
the reader's scope into the queue; `WireRequest.Run` encloses dispatch, including
early state checks, authentication, decoding and response errors.

With the policy disabled, there is no new aggregate Bolt limit. With it enabled,
the accumulated four-times-wire forecast, the other outstanding leases and
fresh physical capacity impose the bound. There is no new 10 MiB default.

## HTTP body and delegated entry points

`buildRouter -> wrapWithMiddleware -> metadataAdmissionMiddleware` surrounds
every application middleware; `bodyAdmissionMiddleware` guards bodies inside
the actual timeout worker and borrows the same owner.
One policy reservation covers the body and actual handler lifetime; the enabled
path also applies the existing `MaxRequestSize`. The request line (including its
raw query), host and headers are charged independently of that body cap. Known
wire sizes are never clamped to a smaller limit. Body reads extend/recheck the
same scope before returning bytes to a decoder, including unknown-length bodies.
A centralized response writer preserves the transient memory error if growth
fails during a read. `memory.HandleHTTPRequest` binds the scope to the request
context, so delegated GraphQL handlers borrow the existing owner. Standalone
GraphQL handlers use this same helper without adding a new hard body cap.
Default-disabled behavior is
unchanged. Bodyless transaction setup preserves its existing gate, while a bare
explicit commit, rollback and health request can finish without admission.

The base has 31 native JSON decoder sites: 27 calls to `readJSON`, two direct
database-config decoders, the embedding retry decoder and the common transaction
request decoder. Delegated MCP has three body decoder sites and Heimdall has two.

| Source/entry-point family | Native request consumers |
|---|---|
| `server_auth.go` | token, password, profile, API token, users/user-by-ID, roles/role-by-ID, database access, privileges, role entitlements |
| `server_admin.go` | backup, restore |
| `server_db.go` | implicit transaction, open/execute explicit transaction, commit-with-statements through `readTransactionRequest` |
| `server_dbconfig.go` | database lifecycle and database override PUT |
| `server_nornicdb.go` | search, similar, search rebuild, embedding failure retry |
| `server_graph.go` | neighborhood, path, temporal and diff requests |
| `server_gpu.go` | GPU test |
| `server_gdpr.go` | export, delete |
| `server_retention.go` | policy collection/item, holds, erasures |
| `mcp.Server.ServeHTTP` | JSON-RPC MCP, initialize and tool call, including database-pinned routes |
| `heimdall.Handler` | chat completions and autocomplete |
| `graphql.Handler` | gqlgen POST/multipart body decoding; GET variables use the same request-admission owner with query-string wire size |

URL/header parsing done by `net/http`, and response JSON from upstream services,
are not request-body decoding. The audit distinguishes those from client body
consumers rather than treating every `json.Unmarshal` in the repository as an
ingress decoder.

## Upgraded GraphQL messages

An HTTP upgrade has no JSON request body; the common body gate cannot protect
later frames. gqlgen v0.17.94 provides `WebsocketImplementation.Accept` and
`WebsocketConn.NextReader` plus the optional `WebsocketReadLimiter`. The enabled
handler configures an owned adapter through these supported interfaces; no
third-party source or generated code is patched. Its streaming reader admits
accumulated message bytes before returning a buffered reader to gqlgen. This
covers connection initialization, subscription/operation variables, ping/pong
payloads, completion and invalid/unknown message envelopes before JSON decode.
The existing gqlgen read limit remains independently enforced.

The reservation includes request metadata and the full accumulated frame. It
lasts through synchronous envelope/initialization/operation setup, then ends
before waiting for the next frame, or on error/disconnect. An unconditional
handler scope owns the upgraded connection even if gqlgen returns from init
without closing it. Context cancellation closes the socket to unblock framing;
the reservation remains owned until the read/setup work stops.
Transport closure cannot release credit while a synchronous consumer still
holds a reader. For upgrades reached through HTTP admission, the frame reader
resets that same request scope; query/header admission cannot leave a second
reservation behind while the upgraded connection is idle.
Pressure/capacity rejection sends close code 1013 with the existing transient
memory status as its reason. Disabled admission uses `NewDefaultServer` unchanged.

## Ownership and error-path coverage

`memory.WireRequest` is the common reserve-and-release owner. `Run` defers
cleanup across every callback return and panic; `Move` explicitly hands a
framed Bolt message to its queue, and `Reset` ends one upgraded frame. Cleanup
is idempotent and returns the underlying lease once.

| Consumer/path | Unconditional owner and release boundary | Public matrix coverage |
|---|---|---|
| Bolt HELLO, BEGIN, RUN, PULL, DISCARD | reader defer until Move; queued/synchronous dispatch Run until setup returns | malformed payload/handler error, pressure rejection, partial-message disconnect |
| Bolt ROUTE, LOGON and unsupported messages | same framing scope even when there is no decoder; failed dispatch returns its scope | unsupported/ignored payload, pressure, partial disconnect |
| Bolt enqueue failure, pending queue and teardown | failed enqueue closes the scope; teardown joins the reader then drains owned messages | existing queued disconnect, RESET/cancellation and result-ownership checks |
| HTTP transaction, auth, search and delegated decoders | one middleware Run inside the actual timeout worker | decoder error, auth error, bounded/chunked body, dropped body, pressure during read |
| GraphQL POST and GET | same HTTP scope, including full query/header bytes; standalone handlers use the common HTTP helper | invalid JSON/variables, successful POST, GET above body cap, large headers, query with body, standalone known/chunked bodies |
| Upgraded GraphQL, both protocols | handler Run plus connection close on every return, including pre-run init failure; frame Reset before idle read | invalid init map shape, invalid envelope/first message, partial/drop/cancel, pressure/capacity, operation error and normal close |

The public `TestPredecodeReservationLifetime` matrix is compiled in the protocol
packages to use their existing fixtures. Every completed/error/drop row asserts
zero retained reservation. Timeout replies do not release a still-running body
worker early; its Run ends when that worker actually stops. Failed upgrade and
pre-admission rejection own no lease. Fixed handshake/framing bytes do not enter
an arbitrary client decoder.

`TestGraphQLHTTPReservationComposition` also exercises both upgraded protocols
after an outer HTTP reservation, including a query string. It checks zero credit
while idle as well as after disconnect. The socket-backed paused-consumer test
checks that transport close retains credit until decoding actually returns.

Pinned API sources:
[adapter](https://github.com/99designs/gqlgen/blob/v0.17.94/graphql/handler/transport/websocket_adapter.go),
[transport](https://github.com/99designs/gqlgen/blob/v0.17.94/graphql/handler/transport/websocket.go),
[defaults](https://github.com/99designs/gqlgen/blob/v0.17.94/graphql/handler/server.go).
