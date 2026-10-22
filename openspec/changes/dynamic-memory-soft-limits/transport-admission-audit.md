# Listener-to-admission ownership audit

This closes the Rule 73 request-ingress family after the reviews of #1625 at
8da780ed4 and 34d35e4ea. The failed method started inventory at buildRouter and
treated every transport wrapper as bounded protocol setup. That missed the
installed cleartext handler's proportional initial-body buffer. The causal unit
is the complete installed handler and every supported transport that reaches the
shared owner, including the first request and later requests on an upgraded
connection. The existing policy, release/timeout ownership, recovery, config,
failed-Open and pool-snapshot corrections are preserved.

The source inventory is pinned to 34d35e4ead44108f8159d6b4c4573d50c167e6eb and its
local correction. Shared graph queries are read-only against release44a40b862,
57,150 nodes. No exact-PR graph, graph copy, extract or update was made.

## Complete database-ingress population

| Protocol/stage | Reads or buffers before the shared owner | Bound and ownership |
|---|---|---|
| HTTP TCP accept | listener/socket state, net/http fixed read/write buffers | fixed transport bookkeeping; no application decoder |
| TLS handshake, HTTP and Bolt listeners | native crypto/tls record/handshake parsing | native length checks; no request-body accumulator; HTTP ALPN selects h2 without a body-reading middleware |
| HTTP/1.1 request start | request line/URL parsing, header parsing, fixed read-ahead | implicit Go1.26.9 MaxHeaderBytes=1MiB plus4096-byte read allowance; native body read-ahead is bounded by that buffer |
| TLS h2 | preface/SETTINGS, HPACK headers, DATA framing and stream pipe | finite frame/header/table/flow-control bounds; h2 header-list limit derives from MaxHeaderBytes plus320 bytes; native stream/connection upload windows default1MiB; application admission is per stream |
| h2c prior knowledge | HTTP/1-compatible PRI prefix, six-byte SM preface, then normal h2 framing | fixed preface, same h2 bounds; no initial application body ReadAll |
| HTTP/1.1 h2c Upgrade at34d35e4ea | HTTP2-Settings base64 decode and io.ReadAll(r.Body), before application dispatch | settings are header-bounded; the body was unbounded and uncharged, even above MaxRequestSize; this is the confirmed failure |
| Corrected cleartext protocol selection | only ContentLength, TransferEncoding length and a bounded Upgrade-header token scan | **zero-byte upgrade-body cap**: any nonzero/unknown length or transfer encoding plus h2c proposal goes directly to the existing HTTP/1 application handler; no body read, copy, query parse or lease in the selector |
| Bodyless h2c Upgrade | header-bounded settings decode; empty-body ReadAll; fixed protocol setup | unchanged 101 and stream1 behavior; subsequent streams enter the same application handler; no connection-lifetime reservation |
| Raw Bolt before messages | fixed configured sniff/read buffer (default256KiB, including possible read-ahead), Peek(5), optional TLS, fixed20-byte negotiation and chunk/signature headers | fixed framing storage; request-derived growth begins only after readWireMessage reserves the accumulated wire size |
| Bolt-port HTTP discovery/WebSocket upgrade | same fixed sniff buffer, then wsHeaderReader before http.ReadRequest | setup WireRequest charges bytes before parser growth; cookie/auth/origin/upgrade parsing remains inside that owner; release on return |
| Bolt over WebSocket | gorilla fixed framing/read buffer, bounded control frames, then streamed Bolt chunks | compression explicitly disabled; message growth uses readWireMessage's owner; WebSocket frame cap remains independent |
| Main-server GraphQL HTTP/multipart/upgrade | native HTTP framing as above, then application metadata admission | gqlgen GET/POST/multipart/JSON and gorilla upgrade validation run inside the shared owner; subsequent frames reserve before growing the payload buffer |
| Standalone GraphQL | host-provided native HTTP framing, then Handler.ServeHTTP | existing shared HTTP/frame helpers own decoding; outside callers' additional wrappers are not installed by this server |

The canonical boundary is `Server.Start`'s installed transport handler. The
cleartext selector permits only bounded setup to precede the unchanged
`metadataAdmissionMiddleware -> memory.HandleHTTPRequest -> instrumentedMux ->
trusted proxy -> base path -> localization -> metrics -> recovery -> logging ->
timeout/continuation -> rate limit -> CORS -> security -> body guard -> mux ->
authentication/route handler` chain. TLS h2 and ordinary HTTP/1 reach that same
chain. Rejected or body-bearing upgrade requests receive normal HTTP/1 responses;
they do not receive101 and cannot make h2c buffer their payload. The existing
application/request-size checks therefore run before decoding it. This rule is
independent of whether application admission is enabled; disabling the policy
preserves ordinary HTTP/1 handling and default unlimited Bolt framing.

Both metadata and body guards now use the same canonical bodyless-control
classifier. Native h2 represents an empty body with a non-nil requestBody even
when ContentLength is zero; testing reader identity in the inner guard had
re-admitted exempt health requests under pressure. The shared framing/path rule
preserves health, rollback and bare COMMIT on HTTP/1 and h2, while any query,
nonzero/unknown length or transfer encoding removes that exemption. This is the
same ownership contract across transports, not a separate h2 endpoint exception.

Native HTTP/1 response cleanup can discard only a bounded transport remainder
(Go's maxPostHandlerReadBytes), and h2 DATA pipes have finite flow control. These
are not unbounded initial-body accumulators. This change does not claim that
socket/TLS/native framing buffers are charged application allocations.

## Every installed wrapper and delegated body decoder

| Wrapper/consumer | Position and result |
|---|---|
| h2c.NewHandler | only constructed by the canonical cleartext handler; body-bearing Upgrade proposals bypass the adapter and enter ordinary admission |
| tracing/request IDs | instrumentedMux extracts traceparent/baggage only after metadata reservation; no separate request-ID wrapper is installed |
| security, CORS, proxy, localization, logging, recovery, metrics, timeout | all inside metadata ownership; security shares one decoded query map; timeout workers retain the same owner until they stop |
| gzip/compression | EnableCompression is a response config field, not an installed request-decompression wrapper; scoped production search finds no gzip.NewReader/Content-Encoding request adapter in server/GraphQL/MCP/Heimdall; WebSocket compression is disabled |
| multipart | gqlgen MultipartForm is installed inside its shared HTTP gate, after the main-server gate when delegated; no outer multipart reader |
| GraphQL WebSocket | gorilla upgrade runs inside the HTTP owner; supported gqlgen adapter charges frame bytes before accumulation/JSON; invalid init/drop/cancel cleanup retained |
| Bolt WebSocket | owned HTTP parser charged before decoding; framing has fixed buffers; PackStream growth remains inside the shared message owner |
| MCP/Heimdall/static UI/route auth | registered behind the same admitted mux; no pre-mount body wrapper or alternate database HTTP listener |

Additional listener search found distinct observability telemetry/optional
loopback pprof listeners and the optional Qdrant/Nornic gRPC listener. They do not
wrap the database HTTP application. Telemetry uses its own fixed-route mux and
response gzip negotiation; pprof is a separate opt-in debug API. The gRPC server
sets an explicit MaxRecvMsgSize (default64MiB) and MaxConcurrentStreams before
protobuf/interceptors. Their independent existing protocols/diagnostic behavior
are recorded as separate component boundaries; this correction does not silently
move database admission into those components or change their contracts.

## Repeatable population/source extraction

Run inside the implementation worktree, never from the harness/project root:

```text
rg -n 'net.Listen|http.Server\{|ListenAndServe|ServeTLS|ServeConn|h2c.NewHandler|ConfigureServer' pkg/server pkg/bolt pkg/graphql pkg/qdrantgrpc pkg/observability cmd/nornicdb -g '*.go' -g '!**/*test.go'
rg -n 'gzip.NewReader|gzip.NewWriter|compress/|Content-Encoding|MultipartForm|ParseMultipart|RequestID|request.ID|otelhttp|instrumentedMux' pkg/server pkg/bolt pkg/graphql pkg/mcp pkg/heimdall cmd/nornicdb -g '*.go' -g '!**/*test.go'
rg -n 'ReadAll|NextReader|ReadMessage|EnableCompression|ReadBufferSize|Grow|ReserveWireRequest|MaxRecvMsgSize' pkg/bolt/transport_ws.go pkg/bolt/transport_select.go pkg/bolt/wire_admission.go pkg/bolt/wsconn.go pkg/graphql/handler.go pkg/graphql/websocket_admission.go pkg/qdrantgrpc/server.go
```

Shared Graphify commands (GRAPHIFY_MAX_GRAPH_BYTES=600000000, graph
`/graphs/nornic-shared-release/graphify-out/graph.json`, query-only):

- `query '.wrapWithMiddleware()' --budget 2000`: complete40 nodes/48 edges;
  establishes application-wrapper population, including production instrumentation.
- `query '.peekTransport()' --budget 2000`:512 nodes; truncated to77; scoped source
  establishes actual sniff/TLS/Bolt/WebSocket order instead of inferring absence.

The graph has external-call labels, but does not contain the pinned h2c library's
implementation or prove that its body read is bounded. Scoped Server.Start and
upstream source expose the hidden initial-body buffer; header-only searches miss
that causal edge. Graph-discovered instrumentation and continuation delivery are
retained in the source cross-check. Query receipts are in
`sig/1625-bot/r73-ingress-{wrap,bolt}-graph.txt`.

Pinned primary sources: [Go HTTP server limits/read-ahead](https://github.com/golang/go/blob/go1.26.9/src/net/http/server.go),
[h2c preface/Upgrade](https://github.com/golang/net/blob/v0.59.0/http2/h2c/h2c.go),
[h2 framing/header/flow control](https://github.com/golang/net/blob/v0.59.0/http2/server.go),
[h2 default windows](https://github.com/golang/net/blob/v0.59.0/http2/config.go).

## Integrated public proof

Fail-first runs overlay only the new public tests on exact34d35e4ea. Real TCP
Upgrade requests use a valid canonical HTTP2-Settings value, a16MiB body, both
Content-Length and chunked framing, the4KiB policy and the normal10MiB request
cap. Expect:100-continue records whether any body was requested before admission;
the tiny-policy rows must reject before100 with zero payload sent. Separate
generous-policy rows exercise the10MiB cap rather than a memory rejection, and
small payloads execute through the ordinary transaction API after negotiation is
declined, including with admission disabled.

The real Server.Start regressions also use bodyless h2c Upgrade/stream1,
prior-knowledge h2c, TLS ALPN h2 and HTTP/1.1. On the same open connections, admitted
requests finish with zero retained credit, later pressure rejects normal work,
and bodyless health remains available. This proves request/stream ownership rather
than connection ownership. The handoff records exact SHAs, fail-first/green job
IDs and carried accepted evidence. No local build, test or workload is run.

An additional real h2 control matrix exercises health, rollback and COMMIT with
bare, query and body variants under pressure. All completed/rejected rows must
return credit to zero; accepted transaction controls must release their work.
