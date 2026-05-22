# Connecting Drivers to NornicDB Bolt

NornicDB speaks the Neo4j Bolt protocol on port `:7687`. Any official Neo4j driver — Java, Python, JavaScript, Go, .NET — connects without modification.

## Table of Contents

1. [Wire-Level Transports](#wire-level-transports)
2. [URL Schemes](#url-schemes)
3. [Authentication](#authentication)
4. [Driver Examples](#driver-examples)
5. [Browser-Based Tools](#browser-based-tools)
6. [Operator-Configurable Knobs](#operator-configurable-knobs)
7. [Troubleshooting](#troubleshooting)

---

## Wire-Level Transports

Every accepted connection on the Bolt port goes through a 5-byte sniff that picks one of four transports:

| First bytes on the wire                 | Wire-level transport | Server-side label |
| --------------------------------------- | -------------------- | ----------------- |
| Bolt magic preamble `60 60 B0 17`       | raw TCP              | `tcp`             |
| TLS handshake (`0x16`), then Bolt magic | TLS + raw            | `tcp_tls`         |
| `GET ` (HTTP/1.1 upgrade)               | WebSocket            | `ws`              |
| TLS handshake, then `GET `              | TLS + WebSocket      | `ws_tls`          |

A WebSocket BinaryMessage carries the same Bolt bytes that a raw TCP session would; the WebSocket frame is just a wrapper. JS drivers reassemble multi-frame messages by concatenating BinaryMessage payloads.

## URL Schemes

The official Neo4j drivers (Java, Go, Python, .NET, JavaScript) accept only the canonical Bolt schemes — `bolt`, `bolt+s`, `bolt+ssc`, `neo4j`, `neo4j+s`, `neo4j+ssc`. They do NOT take `ws://` / `wss://` URLs; the browser build of the JS driver picks the WebSocket transport internally based on the runtime, while the Node build picks raw TCP. Either way you pass a `bolt://` URL.

| Driver scheme        | Runtime    | Wire transport produced                                          |
| -------------------- | ---------- | ---------------------------------------------------------------- |
| `bolt://host:7687`   | Node / JVM | `tcp`                                                            |
| `bolt://host:7687`   | Browser JS | `ws`                                                             |
| `bolt+s://host:7687` | Node / JVM | `tcp_tls`                                                        |
| `bolt+s://host:7687` | Browser JS | `ws_tls`                                                         |
| `bolt+ssc://`        | Any        | Same as `bolt+s` (driver disables cert verification client-side) |
| `neo4j://`           | Node / JVM | `tcp` + driver-side routing                                      |
| `neo4j://`           | Browser JS | `ws` + routing                                                   |
| `neo4j+s://`         | —          | `tcp_tls` / `ws_tls` + routing                                   |
| `neo4j+ssc://`       | —          | Same as `neo4j+s`, no client-side cert verification              |

The routing wrappers (`neo4j://*`) ask the server for a routing table via the ROUTE message; NornicDB returns a single-server table pointing at the listener's address.

If you're a third-party tool that does its own WebSocket dial (bypassing the Neo4j driver), the server _also_ accepts unwrapped `ws://` / `wss://` upgrades on the same port — it means writing Bolt bytes into BinaryMessage frames yourself. That path produces the same wire-level transport the JS driver's browser build produces internally; the only difference is whether a `bolt://` URL or a `ws://` URL drove the dial.

## Authentication

NornicDB accepts three Bolt HELLO auth schemes:

- **`scheme: "basic"`** — username + password validated by `pkg/auth.Authenticate`. Same credentials as the HTTP API.
- **`scheme: "bearer"`** — JWT credentials validated by `pkg/auth.ValidateToken`. Works for any token issued by `/auth/token`, `/auth/api-token`, or the OAuth callback.
- **`scheme: "none"`** — anonymous. Granted only when `RequireAuth=false` (or `AllowAnonymous=true`), or when the WebSocket transport carried an implicit bearer (see below).

### WebSocket-only: implicit bearer from HTTP credentials

A WebSocket upgrade is an HTTP request, so it carries cookies and headers. Two of those are honored as a fallback when HELLO is `scheme: "none"`:

1. **`Cookie: nornicdb_token=<jwt>`** — set by `/auth/token` and the OAuth callback. Browsers attach this automatically on same-hostname WS upgrades.
2. **`Authorization: Bearer <jwt>`** — third-party clients (MCP servers, scripts, integrations) that already mint JWTs via `/auth/api-token`.

When both are present, the cookie wins. When the HELLO carries `scheme: "bearer"` or `"basic"` explicitly, **HELLO always wins** and neither HTTP source is consulted. Other HTTP-API credential surfaces (`X-API-Key`, `?token=`, `?api_key=`) are NOT honored on WS upgrades.

The raw TCP transports (`bolt://`, `bolt+s://`) have no HTTP layer; the implicit-bearer path is unreachable there. Drivers on raw TCP must always use HELLO basic or bearer.

## Driver Examples

### JavaScript (browser or Node.js)

```javascript
import neo4j from "neo4j-driver";

// Browser: same-origin cookie carries the JWT; auth.none() suffices.
// The driver's URL scheme is bolt:// — its browser build picks the
// WebSocket transport internally, so the wire ends up as ws:// frames.
const driver = neo4j.driver(
  "bolt://localhost:7687",
  // neo4j-driver's auth.none() is browser-only; in TS pass the literal.
  { scheme: "none" },
);

// Node.js: explicit bearer (no implicit-cookie path on raw TCP).
const driverNode = neo4j.driver(
  "bolt://localhost:7687",
  neo4j.auth.bearer(process.env.NORNICDB_TOKEN),
);

const session = driver.session();
try {
  const result = await session.run("RETURN 1 AS x");
  console.log(result.records[0].get("x"));
} finally {
  await session.close();
}
await driver.close();
```

For TLS:

```javascript
// In a browser served over HTTPS this becomes a wss:// frame on the wire.
const driver = neo4j.driver(
  "bolt+s://nornicdb.example.com:7687",
  neo4j.auth.basic("alice", "alice-password"),
);
```

### Go

```go
import "github.com/neo4j/neo4j-go-driver/v5/neo4j"

driver, err := neo4j.NewDriverWithContext(
    "bolt://localhost:7687",
    neo4j.BasicAuth("alice", "alice-password", ""),
)
if err != nil { return err }
defer driver.Close(ctx)

session := driver.NewSession(ctx, neo4j.SessionConfig{})
defer session.Close(ctx)

result, err := session.Run(ctx, "RETURN 1 AS x", nil)
if err != nil { return err }
record, _ := result.Single(ctx)
fmt.Println(record.Values[0])
```

For TLS: use `bolt+s://` or `bolt+ssc://` (the latter when your CA isn't system-trusted, e.g. development with self-signed certs). The driver handles the TLS handshake; NornicDB's listener sniffs the TLS first byte and recurses on the decrypted stream.

### Python

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("alice", "alice-password"),
)

with driver.session() as session:
    result = session.run("RETURN 1 AS x")
    print(result.single()["x"])

driver.close()
```

For bearer auth:

```python
import neo4j

driver = neo4j.GraphDatabase.driver(
    "bolt+s://nornicdb.example.com:7687",
    auth=neo4j.bearer_auth(jwt_token),
)
```

### Java

```java
Driver driver = GraphDatabase.driver(
    "bolt+s://nornicdb.example.com:7687",
    AuthTokens.bearer(jwtToken));

try (Session session = driver.session()) {
    Result result = session.run("RETURN 1 AS x");
    System.out.println(result.single().get("x").asLong());
}
driver.close();
```

## Browser-Based Tools

Browsers cannot open raw TCP sockets, so browser-based Cypher tooling (Neo4j Browser, Bloom, custom React/Vue UIs using `neo4j-driver` in-browser) must use `bolt://` or `bolt+s://`.

The NornicDB UI itself runs the official `neo4j-driver` browser build with a `bolt://` URL and `auth.none()`; the driver's browser channel turns that into a `ws://` upgrade on the wire, the same-origin `nornicdb_token` cookie rides along, and the server's HELLO handler promotes the session to that cookie's claims. No additional config is required for first-party browser clients — sign in via `/auth/token` (or finish OAuth via `/auth/oauth/callback`) and Cypher queries Just Work.

For cross-origin browser clients (e.g. a UI on `https://app.example.com` connecting to `wss://bolt.example.com:7687` — the actual WebSocket upgrade the browser produces from a `bolt+s://` driver URL), the cookie's `SameSite=Lax` setting prevents the browser from attaching it to the WS upgrade. Two options:

1. Use `Authorization: Bearer <jwt>` instead. Cross-origin requests can carry custom headers (subject to CORS preflight on `OPTIONS`, which the WS handshake bypasses for `GET /`).
2. Reconfigure the cookie with `SameSite=None; Secure` and a parent-domain scope (`Domain=.example.com`). This requires server-side changes to `/auth/token` and is an operator decision.

## Operator-Configurable Knobs

| Setting                                    | Default | Effect                                                           |
| ------------------------------------------ | ------- | ---------------------------------------------------------------- |
| `NORNICDB_BOLT_TLS_ENABLED`                | `false` | Enables `bolt+s://` and `wss://`.                                |
| `NORNICDB_BOLT_TLS_CERT` / `_KEY`          | (unset) | Cert and key paths; rotation re-reads every 5 s (atomic rename). |
| `NORNICDB_BOLT_TLS_REQUIRE`                | `false` | Reject every plaintext connection (raw and ws).                  |
| `NORNICDB_BOLT_TLS_CLIENT_CA`              | (unset) | mTLS: verify client certs against this CA.                       |
| `NORNICDB_BOLT_TLS_CLIENT_AUTH_MODE`       | `none`  | `none` / `request` / `request_verify` / `require_verify`.        |
| `NORNICDB_BOLT_WEBSOCKET_ENABLED`          | `true`  | `false` ⇒ `426 Upgrade Required` on real WS upgrades.            |
| `NORNICDB_BOLT_WEBSOCKET_ALLOWED_ORIGINS`  | `*`     | Comma-separated origin allowlist for the WS upgrade.             |
| `NORNICDB_BOLT_WEBSOCKET_MAX_MESSAGE_SIZE` | `65536` | Per-frame limit (Neo4j parity).                                  |
| `NORNICDB_BOLT_WEBSOCKET_PING_INTERVAL`    | `30s`   | Server-side WS ping cadence (post-HELLO only).                   |
| `NORNICDB_BOLT_WEBSOCKET_PONG_TIMEOUT`     | `60s`   | Pong arrival deadline.                                           |
| `NORNICDB_BOLT_SNIFF_TIMEOUT`              | `5s`    | Bound on the transport-sniff peek.                               |
| `NORNICDB_BOLT_AUTH_TIMEOUT`               | `30s`   | Bound on the pre-HELLO handshake/auth window.                    |

See `docs/operations/configuration.md` for the YAML and CLI equivalents.

## Troubleshooting

### `Handshake failed: invalid magic number: 47455420`

This means a client opened a WS upgrade against an old NornicDB build that didn't multiplex transports. Bytes `47 45 54 20` are `"GET "`. Upgrade NornicDB to the post-2026-05 build that ships Bolt-over-WS.

### `426 Upgrade Required`

The server has `WebSocketEnabled=false` and the client is trying to upgrade. Either set `NORNICDB_BOLT_WEBSOCKET_ENABLED=true` or switch the driver URL to `bolt://` / `bolt+s://`.

### `An unencrypted connection attempt was made where encryption is required.`

The server has `RequireTLS=true` and the connection arrived as plaintext (raw `bolt://` or a `ws://` upgrade). Switch the driver URL to `bolt+s://` (browser builds will produce `wss://` on the wire automatically).

### `Authentication required` on a WS connection from the browser

The cookie is missing or expired. Either log in again at `/login` or send `Authorization: Bearer <jwt>` explicitly. Check `document.cookie` (the cookie itself is HttpOnly, but its presence is signaled by a successful `/auth/me` response).

### `Access to database 'nornic' is not allowed.`

Bolt successfully authenticated, but the principal's roles don't include access to the requested database. Either:

- The user is missing the `admin` / `editor` / `viewer` role bound to the database, or
- `RequireAuth=true` is set without a `DatabaseAccessModeResolver` wired (rare; see operator docs).
