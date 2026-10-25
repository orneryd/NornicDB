# HTTP Request Security

## Overview

NornicDB applies built-in HTTP request validation to protect exposed endpoints from common input-driven attacks before request handlers run.

The current protection set focuses on:

- token injection,
- HTTP header injection and response splitting,
- server-side request forgery (SSRF),
- protocol smuggling,
- unsafe callback and redirect targets.

This protection is enabled through the HTTP middleware layer and is intended to be active by default in normal server operation.

## What Is Protected

The HTTP security layer validates the parts of a request most commonly used to smuggle attacker-controlled values into downstream systems.

Automatically validated inputs include:

- all incoming HTTP headers,
- `Authorization` credentials such as Bearer and Basic tokens,
- query parameter tokens used by SSE or WebSocket-style flows,
- URL-like parameters such as `callback`, `redirect`, `redirect_uri`, `url`, and `webhook`.

This means requests can be rejected before application logic processes a dangerous token, forwards an unsafe URL, or reflects an injected header.

## Validation Behavior

### Token Validation

Token validation is designed to reject values that can break protocol boundaries or carry script payloads.

It blocks:

- CRLF injection such as `\r\n`,
- newline injection such as `\n`,
- HTML or script payloads such as `<script>`,
- JavaScript-style protocol payloads such as `javascript:`,
- protocol injection via `data:`, `file:`, or `ftp:`,
- null byte injection such as `\x00`,
- oversized token values above 8192 bytes.

Typical rejected examples:

- `token\r\nX-Malicious: header`
- `token\nX-Evil: value`
- `<script>alert('xss')</script>`
- `javascript:alert('xss')`
- `data:text/html,<script>...`
- `file:///etc/passwd`
- `token\x00evil`

Approximate performance: `1-2 µs` per validation.

### URL Validation

URL validation is intended to stop NornicDB from accepting user-supplied callback or webhook targets that could be used to reach internal infrastructure.

It blocks:

- private IPv4 ranges such as `10.0.0.0/8`, `172.16.0.0/12`, and `192.168.0.0/16`,
- localhost and loopback targets in production,
- link-local targets such as `169.254.0.0/16`,
- cloud metadata endpoints used in AWS, Azure, and GCP environments,
- unsafe schemes such as `file://`, `gopher://`, `dict://`, and `ftp://`,
- plain HTTP URLs in production when secure transport is required.

Typical rejected examples:

- `http://192.168.1.1/steal`
- `http://10.0.0.1/internal`
- `http://172.16.0.1/admin`
- `http://169.254.169.254/latest/meta-data/`
- `http://169.254.169.254/metadata/instance`
- `http://169.254.169.254/computeMetadata/`
- `http://127.0.0.1:8080`
- `http://localhost:3000`
- `file:///etc/passwd`
- `gopher://internal:70/`

Approximate performance: `5-10 µs` per validation.

### Header Validation

Header validation protects against request and response boundary corruption.

It blocks:

- CRLF injection,
- newline injection,
- null bytes,
- excessively long header values above 4096 bytes.

Typical rejected examples:

- `Value\r\nX-Injected: evil`
- `Value\nX-Injected: evil`
- `Value\x00evil`

Approximate performance: `0.5-1 µs` per validation.

## Environment-Specific Behavior

The middleware changes behavior depending on the runtime environment so local development stays usable without weakening default production posture.

### Production

Use either of the following:

```bash
NORNICDB_ENV=production
```

or:

```bash
NODE_ENV=production
```

Production behavior:

- blocks `localhost` and `127.0.0.1`,
- blocks HTTP URLs when HTTPS is expected,
- blocks private IP ranges,
- blocks cloud metadata services.

### Development

Use either of the following:

```bash
NORNICDB_ENV=development
```

or:

```bash
NODE_ENV=development
```

Development behavior:

- allows `localhost` and `127.0.0.1`,
- allows HTTP callback targets for local workflows,
- still blocks non-local private IP targets,
- still blocks cloud metadata endpoints.

### Allow HTTP Override

```bash
NORNICDB_ALLOW_HTTP=true
```

This permits HTTP targets even when production-style restrictions would normally reject them.

Use this only when you explicitly need it for a controlled environment. It is not recommended for internet-facing deployments.

## How It Is Integrated

The protection is enforced through middleware in the HTTP server stack.

Integration point:

```go
securityMiddleware := security.NewSecurityMiddleware()
handler := securityMiddleware.ValidateRequest(mux)
```

That means user-facing endpoints benefit from the same request validation rules without each handler needing to reimplement them.

## Using The Validators Directly

If you add a custom endpoint or perform outbound HTTP requests using user-supplied values, you can apply the same validation functions directly.

```go
import "github.com/orneryd/nornicdb/pkg/security"

webhookURL := r.URL.Query().Get("webhook")
if err := security.ValidateURL(webhookURL, security.URLPolicy{}); err != nil {
	return fmt.Errorf("invalid webhook URL: %w", err)
}

if err := security.ValidateToken(apiKey); err != nil {
	return fmt.Errorf("invalid token: %w", err)
}

if err := security.ValidateHeaderValue(customHeader); err != nil {
	return fmt.Errorf("invalid header: %w", err)
}
```

This is useful when:

- building custom integrations,
- validating webhook targets,
- accepting callback URLs from users,
- processing externally supplied API credentials.

### Go helper migration

`ValidateURL(url, isDevelopment, allowHTTP)` now takes `ValidateURL(url, policy)`.
Use `security.URLPolicy{}` for production defaults; use `AllowHTTP: true` with
`HostPolicy: security.URLDevelopmentHost` for the former development mode.
See the [URL policy reference](../../pkg/security/README.md#go-helper-migration)
for all mode mappings and explicit policy limits.

## Tested Attack Coverage

The current test coverage includes more than 30 attack-oriented scenarios across unit and integration tests.

Coverage includes:

- token injection and script payload rejection,
- SSRF attempts to private and metadata endpoints,
- protocol smuggling using non-HTTP schemes,
- header injection and response-splitting patterns,
- development versus production behavior differences.

Current test inventory:

- 19 unit tests in the security package,
- integration coverage for middleware behavior,
- end-to-end request rejection tests in the server layer,
- benchmark coverage for validator and middleware overhead.

## Performance

Representative benchmark results:

```text
BenchmarkValidateToken-10       1000000     1.2 µs/op
BenchmarkValidateURL-10         200000      7.5 µs/op
BenchmarkValidateHeader-10      2000000     0.8 µs/op
BenchmarkMiddleware-10          500000      3.2 µs/op
```

Typical total middleware overhead is about `3-4 µs` per request, which is negligible relative to normal network and handler latency.

## Operational Guidance

Recommended:

- run internet-facing deployments in production mode,
- use HTTPS in production,
- monitor logs for repeated validation failures,
- validate user-supplied callback and webhook URLs before any outbound request,
- keep security-sensitive dependencies current.

Avoid:

- disabling validation for convenience,
- allowing HTTP targets in production unless there is a strong operational reason,
- permitting private-network callback targets without explicit review,
- returning raw validator failure details directly to end users when generic client-facing errors are sufficient.

## Compliance Relevance

This request validation layer helps support:

- OWASP Top 10 protections related to injection and SSRF,
- PCI DSS controls for injection-resistant input handling,
- GDPR Article 32 expectations for appropriate technical safeguards,
- SOC 2 logical access and system protection controls,
- HIPAA technical safeguard expectations for secure processing.

## Implementation References

If you need the underlying code or tests, the implementation lives in:

- `pkg/security/validation.go`
- `pkg/security/validation_test.go`
- `pkg/security/middleware.go`
- `pkg/security/middleware_test.go`
- `pkg/server/security_integration_test.go`

## External References

- OWASP SSRF Prevention Cheat Sheet
- OWASP CSRF Prevention Cheat Sheet
- CWE-918: Server-Side Request Forgery
- CWE-352: Cross-Site Request Forgery
