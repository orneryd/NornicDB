# Reverse Proxy and TLS Termination

NornicDB supports an authenticated HTTP deployment behind a trusted reverse
proxy. The proxy accepts HTTPS from clients and forwards HTTP to NornicDB on a
private connection.

The trust boundary is explicit. NornicDB accepts `X-Forwarded-*`, `X-Real-IP`,
and forwarded-prefix headers only when the request's immediate peer matches
`NORNICDB_HTTP_TRUSTED_PROXIES`. Forwarding headers from every other peer are
removed before routing, authentication, cookie, audit, and rate-limit logic.

## Required NornicDB settings

For a proxy on another container or host:

```bash
NORNICDB_ADDRESS=0.0.0.0
NORNICDB_HTTP_TRUSTED_PROXIES=172.20.0.10/32
NORNICDB_CORS_ENABLED=false
NORNICDB_BOLT_ENABLED=false
NORNICDB_QDRANT_GRPC_ENABLED=false
```

Replace `172.20.0.10/32` with the source IP or narrow CIDR that NornicDB sees
for the proxy connection. Hostnames are not accepted. Multiple entries are
comma-separated:

```bash
NORNICDB_HTTP_TRUSTED_PROXIES=172.20.0.10/32,172.21.0.0/24
```

For a proxy running on the same machine, keep NornicDB on loopback and trust
only loopback:

```bash
NORNICDB_ADDRESS=127.0.0.1
NORNICDB_HTTP_TRUSTED_PROXIES=127.0.0.1/32,::1/128
NORNICDB_CORS_ENABLED=false
```

When the browser application has a different origin from NornicDB, enable CORS
and list exact HTTPS origins instead of disabling it:

```bash
NORNICDB_CORS_ENABLED=true
NORNICDB_CORS_ORIGINS=https://console.example.com
```

Authenticated startup rejects wildcard CORS on a non-loopback listener. It also
rejects cleartext HTTP on a non-loopback listener unless trusted proxies are
configured. Declaring trusted proxies does not create a firewall: restrict the
backend port so only those proxies can reach it.

`NORNICDB_HTTP_TRUSTED_PROXIES` applies only to HTTP. If Bolt is enabled on a
non-loopback address, configure native Bolt TLS and require it. If Qdrant gRPC
is enabled, bind it to loopback or provide a separately secured transport.

## Nginx configuration

This example terminates TLS at Nginx and deliberately overwrites client-supplied
forwarding headers:

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}

upstream nornicdb_http {
    server 172.20.0.20:7474;
}

server {
    listen 443 ssl http2;
    server_name db.example.com;

    ssl_certificate     /etc/nginx/tls/fullchain.pem;
    ssl_certificate_key /etc/nginx/tls/privkey.pem;

    location / {
        proxy_pass http://nornicdb_http;
        proxy_http_version 1.1;

        proxy_set_header Host              $http_host;
        proxy_set_header X-Forwarded-Host  $http_host;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-Port  $server_port;
        proxy_set_header X-Forwarded-For   $remote_addr;
        proxy_set_header X-Real-IP         $remote_addr;

        proxy_set_header Upgrade    $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
    }
}
```

Use `$remote_addr`, not `$proxy_add_x_forwarded_for`, at this trust boundary so
a client cannot prepend a false address that becomes the apparent client IP.

## GraphQL

GraphQL does not have a separate listener. `/graphql` and
`/graphql/playground` use the same HTTP server, authentication middleware,
trusted-proxy boundary, CORS policy, native HTTPS configuration, and base path
as the REST API and built-in interface. The HTTP Nginx configuration above
already proxies GraphQL; no GraphQL-specific ingress variables are required.

## Qdrant gRPC

Qdrant-compatible and NornicSearch gRPC share a separate HTTP/2 listener. They
do not consume HTTP forwarding headers. For a public authenticated listener,
prefer native TLS so encryption remains end-to-end:

```bash
NORNICDB_QDRANT_GRPC_ENABLED=true
NORNICDB_QDRANT_GRPC_LISTEN_ADDR=0.0.0.0:6334
NORNICDB_QDRANT_GRPC_TLS_ENABLED=true
NORNICDB_QDRANT_GRPC_TLS_CERT=/tls/grpc.crt
NORNICDB_QDRANT_GRPC_TLS_KEY=/tls/grpc.key
```

For mTLS, additionally configure:

```bash
NORNICDB_QDRANT_GRPC_TLS_CLIENT_CA=/tls/client-ca.crt
NORNICDB_QDRANT_GRPC_TLS_CLIENT_AUTH_MODE=require_verify
```

The other client-auth modes are `none`, `request`, and `request_verify`.
Certificates are loaded with a TLS 1.2 minimum and support the same atomic-file
rotation behavior as Bolt certificates.

TLS can instead terminate at an HTTP/2-capable gRPC proxy when NornicDB's
plaintext backend listener is restricted to loopback:

```bash
NORNICDB_QDRANT_GRPC_ENABLED=true
NORNICDB_QDRANT_GRPC_LISTEN_ADDR=127.0.0.1:6334
```

```nginx
server {
    listen 8443 ssl http2;
    server_name db.example.com;

    ssl_certificate     /etc/nginx/tls/fullchain.pem;
    ssl_certificate_key /etc/nginx/tls/privkey.pem;

    location / {
        grpc_pass grpc://127.0.0.1:6334;
    }
}
```

Nginx and NornicDB must run on the same host, or as sidecars sharing one network
namespace, for the loopback boundary to hold. Plaintext gRPC to a separate
container requires a public backend listener and is rejected when
authentication is enabled. `NORNICDB_HTTP_TRUSTED_PROXIES` does not apply to
gRPC. Client `authorization` and `x-api-key` metadata must be preserved by the
proxy and are validated by NornicDB. Native TLS and mTLS authenticate the
transport; they do not replace NornicDB application authentication or RBAC.

## Bolt and the built-in interface

The built-in interface uses Bolt over WebSocket on the Bolt port. When the
interface is loaded over HTTPS, its Neo4j driver connects with `bolt+s://`,
which produces a `wss://` connection on the wire.

For Nginx and NornicDB in separate containers, use native Bolt TLS and TCP
passthrough. Nginx forwards the encrypted bytes without terminating TLS:

```bash
NORNICDB_BOLT_ENABLED=true
NORNICDB_BOLT_TLS_ENABLED=true
NORNICDB_BOLT_TLS_REQUIRE=true
NORNICDB_BOLT_TLS_CERT=/tls/server.crt
NORNICDB_BOLT_TLS_KEY=/tls/server.key
```

```nginx
stream {
    upstream nornicdb_bolt {
        server 172.20.0.20:7687;
    }

    server {
        listen 7687;
        proxy_pass nornicdb_bolt;
    }
}
```

The NornicDB certificate must be valid for the public hostname. TLS termination
at Nginx with a plaintext Bolt backend is supported only when Nginx and
NornicDB share a host and NornicDB binds Bolt to `127.0.0.1`. A separate
container requires a non-loopback backend listener; use native Bolt TLS there.
Bolt has no equivalent of trusted HTTP forwarding headers, so
`NORNICDB_HTTP_TRUSTED_PROXIES` cannot authorize or secure that connection.

Set `NORNICDB_BOLT_ENABLED=false` when Bolt is not needed. This removes the
listener and Bolt discovery endpoints; the built-in interface will report that
Bolt is disabled instead of attempting a connection.

All Compose files shipped with NornicDB forward the proxy-facing authentication,
HTTP, HTTPS, CORS, Bolt TLS/mTLS, Bolt WebSocket, and Qdrant gRPC variables from
the host.
Export values before invoking Compose, for example:

```bash
export NORNICDB_NO_AUTH=false
export NORNICDB_AUTH='admin:replace-with-a-secret'
export NORNICDB_HTTP_TRUSTED_PROXIES='172.20.0.0/16'
export NORNICDB_CORS_ORIGINS='https://db.example.com'
export NORNICDB_BOLT_ENABLED=false
docker compose -f docker/docker-compose.cuda.yml up -d
```

Overridden HTTP, HTTPS, and Bolt ports are also reflected in Docker's published
ports. When native TLS is enabled, mount the directory containing the referenced
certificate, key, and optional client CA into the NornicDB container as a
read-only volume; environment variables pass paths, not file contents.

Verify the public endpoint:

```bash
curl --fail --show-error https://db.example.com/health
curl --fail --show-error \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"REDACTED"}' \
  -D - https://db.example.com/auth/token -o /dev/null
```

The authentication response must include a `nornicdb_token` cookie with the
`Secure` and `HttpOnly` attributes.

## URL prefix

To expose NornicDB at `https://example.com/nornicdb`, set:

```bash
NORNICDB_BASE_PATH=/nornicdb
```

Preserve the prefix when proxying:

```nginx
location /nornicdb/ {
    proxy_pass http://nornicdb_http;
    proxy_set_header Host              $http_host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-For   $remote_addr;
}
```

## Native HTTPS

If the backend connection must also be encrypted, enable NornicDB's HTTPS
listener instead of using trusted cleartext proxy mode:

```bash
NORNICDB_ADDRESS=0.0.0.0
NORNICDB_HTTPS_ENABLED=true
NORNICDB_HTTPS_PORT=7473
NORNICDB_HTTP_TLS_CERT=/tls/server.crt
NORNICDB_HTTP_TLS_KEY=/tls/server.key
NORNICDB_CORS_ENABLED=false
```

Equivalent YAML:

```yaml
server:
  http_address: "0.0.0.0"
  https:
    enabled: true
    port: 7473
    cert_file: /tls/server.crt
    key_file: /tls/server.key
```

When HTTPS is enabled, both certificate and key are required. The HTTPS port is
used unless `--http-port` is explicitly supplied.

## Troubleshooting startup

`security configuration: wildcard CORS origin is not allowed`

: Disable CORS for same-origin deployments or set exact HTTPS origins.

`security configuration: public plaintext HTTP listener is not allowed`

: Set `NORNICDB_HTTP_TRUSTED_PROXIES` for TLS termination at a trusted proxy, or
enable native HTTPS.

`security configuration: public Bolt listener must require TLS`

: Disable Bolt when only HTTP is proxied, bind it to loopback, or configure Bolt
TLS with `NORNICDB_BOLT_TLS_ENABLED=true`, `NORNICDB_BOLT_TLS_REQUIRE=true`, and
certificate and key paths.
