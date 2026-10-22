# Middleware and ingress metadata ownership audit

The shared release graph is read-only at `/graphs/nornic-shared-release/graphify-out/graph.json`, release/v1.4.4@44a40b862. Queries report 57,150 nodes. The integration base for this correction is PR #1625@8da780ed4f4bc888187e1525f0c3172dca1701aa. Its local source delta is checked with scoped rg; no exact-PR graph, extraction, update, copy or root was created. `sig/codeintel.sh` remains decommissioned by user order (its wrapper exits 2 when the decommission marker is present), so the replacement graph/source method is used.

## Boundary and changed causal owner

The earlier owner began at routed body handling, after application middleware had already run. Testing only decoder responses and retained reservations could not show that upstream query/header expansion had escaped admission. The complete causal unit is now the public request boundary and its downstream synchronous and timeout-worker consumers. `metadataAdmissionMiddleware -> memory.HandleHTTPRequest` reserves before the entire existing middleware chain, snapshots the original metadata size, and binds one WireRequest owner in context. Body guards and delegated GraphQL borrow it; no second reservation is made. Body guards stay in the actual worker. Timeout dispatch retains the owner before launching a goroutine, so a response/cancellation cannot release credit before its decoder stops. Run/Close and the last retained worker jointly end the scope once.

The explicit control exception uses only method, path, raw-query length, ContentLength and TransferEncoding. GET/HEAD `/health`, DELETE `/db/<db>/tx/<id>`, and POST `/db/<db>/tx/<id>/commit` remain available with no body, no transfer encoding and a **zero-byte raw-query cap**. Configured base-path prefixes are recognized by substring inspection. Any query or body removes the exception. Exempt requests still pass through normal security/authentication checks. Other requests are charged even when there is no body or the endpoint will reject them. Disabled application policy preserves ordinary admission behavior; the bounded cleartext protocol-selection rule applies independently.

## Every application layer before the old body boundary

| Layer, in original outer-to-inner order | Request parsing/allocation | Current admission boundary |
|---|---|---|
| production instrumentation (`instrumentedMux`) | extracts traceparent/baggage headers through the configured OTel propagator, creates spans and labels | moved from Start's outer wrapper into the admitted application chain; verified through actual Server.Start |
| trusted proxy | parses RemoteAddr as IP; deletes untrusted forwarded headers | after raw metadata snapshot/admission |
| base path | creates request/URL views and original/base-path headers; reads forwarded prefix | after admission; normalized paths cannot shrink the charged original size |
| localization | `language.ParseAcceptLanguage` expands client header preferences | after admission |
| metrics | fixed counters and a deferred decrement; no client decoder | inside shared owner |
| recovery | fixed defer; logs a recovered panic | inside shared owner |
| logging | response wrapper; localized method/path/status record | inside shared owner |
| timeout and continuation delivery | path comparisons, fixed context/delivery state, goroutine and response buffer; no query/body decoding | inside metadata owner; timeout worker explicitly retains it before start |
| rate limit | reads proxy/IP headers and trims the first IP; per-IP limiter bookkeeping | after admission and trusted-proxy filtering |
| CORS | reads origin; may copy accepted origin into response headers and return 204 | after admission; accepted-capacity preflight behavior unchanged |
| security | validates headers and Authorization token; URL-decodes query token and five URL parameters; validates URL values | after admission; one query map is shared by all six security checks; original 401/400 decisions remain |
| body guard, route auth and delegated handlers | body JSON/multipart, cookies/Basic/JWT, route query parameters | same owner before these decoders; see the existing predecode consumer audit |

Pre-admission rejection renders the server's configured/default memory error from context; it never parses unadmitted Accept-Language or query data to localize the rejection. The ordinary net/http server's native request-line/header parsing precedes Handler dispatch and retains its existing transport bounds; this audit does not claim a kernel/TLS/native-server allocation guard.

`Server.Start` sets no explicit `MaxHeaderBytes`: Go 1.26.9 uses its finite implicit 1 MiB default (with a 4096-byte read-buffer allowance). The x/net v0.59.0 HTTP/2 configuration overrides only concurrent streams; its framer derives `MaxHeaderListSize` from the same HTTP server's header limit, adding 320 bytes of HTTP/2 field overhead. Both TLS and h2c use that base HTTP server. HTTP2-Settings base64 decoding is bounded by these native header limits. The pinned h2c adapter's `io.ReadAll(r.Body)` was not header-bounded; the complete installed-handler audit now owns this boundary. One outer cleartext selector permits only bodyless upgrades (zero-byte cap, no transfer encoding), processing all body-bearing proposals as ordinary HTTP/1 through the same admission and body limits. No client payload reaches h2c's initial-body buffer. See the [listener-to-admission audit](transport-admission-audit.md) for the whole protocol population, including prior knowledge and TLS h2.

Pinned primary sources: [Go default and read allowance](https://github.com/golang/go/blob/go1.26.9/src/net/http/server.go#L892-L906), [HTTP/2 framer bound](https://github.com/golang/net/blob/v0.59.0/http2/server.go#L500-L506), [header-list conversion](https://github.com/golang/net/blob/v0.59.0/http2/config.go#L118-L126), and [h2c initial-body buffering](https://github.com/golang/net/blob/v0.59.0/http2/h2c/h2c.go#L185-L195).

## GraphQL and Bolt entry points

Main-server GraphQL GET/POST is inside this same chain. GET variables, POST/multipart bodies and authentication data are already charged before security or gqlgen sees them. Standalone GraphQL handlers keep the existing shared HTTP gate; the gqlgen WebSocket adapter admits each frame and retains synchronous setup credit. Upgraded connections reset frame credit before idle reads and on every error/drop/invalid-init return.

Raw Bolt performs fixed transport sniffing/version negotiation before message admission; these do not parse arbitrary client metadata. `readWireMessage` owns variable message accumulation for both readers and dispatch rechecks that same owner. HELLO, BEGIN, RUN, PULL and DISCARD decode only after charging; unsupported/ignored LOGON/ROUTE payloads remain bounded. Fixed empty terminal controls remain available to release work.

The inventory additionally found Bolt-over-WebSocket HTTP ingress: `handleConnection -> acceptWebSocket -> http.ReadRequest -> cookie/Authorization extraction -> Upgrader.Upgrade`. This port owns its HTTP parser. A fixed-buffer streaming reader grows a WireRequest before returning bytes to that parser; capacity rejection returns 503 with the existing transient memory code. The lease spans header/cookie/origin parsing and upgrade setup, then ends on every return. A reused reader stops charging the completed HTTP upgrade before handing off to WebSocket framing; subsequent Bolt messages use the normal message owner. Discovery and upgrade use this boundary. Default-disabled transport behavior and the existing WebSocket frame cap are unchanged.

## Shared graph queries and source cross-check

Commands run with `MSYS_NO_PATHCONV=1 docker exec -e GRAPHIFY_MAX_GRAPH_BYTES=600000000 graphify graphify ... --graph /graphs/nornic-shared-release/graphify-out/graph.json`:

- `query '.wrapWithMiddleware()' --budget 2000`: 40 nodes/48 edges, complete result (CLI reports 3051 tokens because it preserves edges).
- `path '.wrapWithMiddleware()' '.ValidateRequest()' --directed`: three-hop path through NewSecurityMiddleware and SecurityMiddleware. The graph does not express the runtime nesting order; source establishes that.
- `query '.requestTimeoutMiddleware()' --budget 2000`: 26 nodes.
- `query '.localizationMiddleware()' --budget 2000`: 73 nodes, including MCP and localization package structure; source selects the HTTP receiver.
- `query '.trustedProxyMiddleware()' --budget 2000`: 22 nodes.
- `query '.readMessage()' --budget 2000`: ambiguous test/production labels. Qualified `pkg/bolt/server.go::readMessage()` still broadened to 20,900 nodes and was truncated; no completeness inference is made from that result.
- `query '.acceptWebSocket()' --budget 2000`: 117 nodes, bounded/truncated output; exact source establishes the HTTP cookie/header path.

The initial natural-language query broadened to 27,589 nodes and was discarded as inventory proof. The narrower root query exposed continuationDeliveryMiddleware and instrumentedMux, absent from the initial named-layer source list. Checking Server.Start exposed the production instrumentation mount outside buildRouter; the public production-mount probe covers that distinction. The broader scoped Bolt source search exposed the owned HTTP parser and cookie extraction; a search confined to PackStream decoders would miss them. External receiver/interface edges do not reliably expose net/url or language parser internals, so scoped source reads are authoritative for those expansion points. No unresolved graph edge is treated as evidence of absence.

Repeatable scoped source queries (inside this worktree):

```text
rg -n 'func .*Middleware|ValidateRequest|bodyAdmission|metadataAdmission|TimeoutHandler' pkg/server/server_router.go pkg/server/server_middleware.go pkg/server/body_admission.go pkg/server/metadata_admission.go pkg/security/middleware.go
rg -n 'Query\(|Parse|Decode|Cookie\(|Split|ToLower|Header.Get|r.Body' pkg/security/middleware.go pkg/server/server_middleware.go pkg/server/localization.go pkg/server/basepath.go pkg/server/trusted_proxy.go pkg/server/search_continuation_delivery.go pkg/bolt/transport_ws.go pkg/bolt/discovery.go
rg -n 'decodePackStream|parseRunMessage|parseStreamingOptions|readWireMessage|dispatchInner|acceptWebSocket|http.ReadRequest' pkg/bolt -g '*.go' -g '!**/*test.go'
rg -n 'HandleHTTPRequest|WireRequest|ServeHTTP|NextReader' pkg/memory/wire_http.go pkg/memory/wire_request.go pkg/graphql/handler.go pkg/graphql/websocket_admission.go
rg -n 'instrumentedMux|propagator.Extract|http.Server\{|MaxHeader|http2Config|h2c.NewHandler' pkg/server/server.go pkg/server/server_router.go pkg/server/instrumented_mux.go
```

## Public proof

Exact 8da780ed4 fail-first probes exercise main HTTP, GraphQL GET/POST and large-query health, plus allocation ordering through the public router. An 8192-value query caused 8293/8292 allocations before the old security rejection; the regression bounds rejected-request allocations below 256 and requires the memory 503 before the security 401. Additional exact-base probes show Bolt HTTP discovery/upgrade accepting an oversized query before any policy check. Config probes reject legacy zero/zero timing pairs at the old head while explicit YAML/env zero pairs already fail correctly.

The integrated regression also exercises live-worker timeout retention, all control operations under pressure, rejection of their query/body variants, unchanged 401/400 security outcomes, both GraphQL WebSocket protocols, and the earlier lifetime/size matrices. Every completed/error/drop row returns its reservation to zero. All execution and formatting checks run through tnrun on TrueNAS; the handoff records exact SHAs, job IDs and outcomes.
