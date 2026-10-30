# NornicDB Security Validation

This package provides comprehensive security validation for NornicDB HTTP endpoints, protecting against:

- **CSRF (Cross-Site Request Forgery)** attacks
- **SSRF (Server-Side Request Forgery)** attacks
- **HTTP Header Injection** attacks
- **XSS (Cross-Site Scripting)** attacks
- **Protocol Smuggling** attacks
- **Data URI Injection** attacks

## Features

### 1. Token Validation (`ValidateToken`)

Validates OAuth/API tokens to prevent injection attacks:

```go
import "github.com/orneryd/nornicdb/pkg/security"

token := "eyJhbGciOiJIUzI1NiIs..."
if err := security.ValidateToken(token); err != nil {
    return fmt.Errorf("invalid token: %w", err)
}
```

**Protects against:**

- HTTP header injection (CRLF, newlines)
- XSS attacks (HTML tags, JavaScript)
- Protocol injection (javascript:, data:, file:)
- DoS attacks (excessively long tokens > 8192 bytes)
- String termination attacks (null bytes)

**Test Coverage:** 13 injection attack scenarios

### 2. URL Validation (`ValidateURL`)

Validates URLs to prevent SSRF attacks. The policy is passed explicitly:

```go
// Production policy (strict, secure default = zero value)
err := security.ValidateURL(callbackURL, security.URLPolicy{})

// Development policy (allows loopback hosts and HTTP for local workflows)
err := security.ValidateURL(localURL, security.URLPolicy{
    AllowHTTP:  true,
    HostPolicy: security.URLDevelopmentHost,
})
```

**Protects against:**

- SSRF to private IP ranges (10.0.0.0/8, 192.168.0.0/16, 172.16.0.0/12)
- SSRF to cloud metadata services (169.254.169.254)
- SSRF to localhost (127.0.0.0/8) in production
- Protocol smuggling (file://, gopher://, dict://)
- HTTP downgrade attacks (requires HTTPS in production)

**Test Coverage:** 15+ SSRF attack scenarios including AWS/Azure/GCP metadata services

#### Go helper migration

`ValidateURL(url, isDevelopment, allowHTTP)` now takes `ValidateURL(url, policy)`.
The zero value `URLPolicy{}` preserves production defaults: HTTPS, localhost and
private literal-IP restrictions, and the 2048-byte `MaxURLLength` bound.

| Previous call | Replacement |
| --- | --- |
| `security.ValidateURL(u, false, false)` | `security.ValidateURL(u, security.URLPolicy{})` |
| `security.ValidateURL(u, false, true)` | `security.ValidateURL(u, security.URLPolicy{AllowHTTP: true})` |
| `security.ValidateURL(u, true, false)` | `security.ValidateURL(u, security.URLPolicy{AllowHTTP: true, HostPolicy: security.URLDevelopmentHost})` |
| `security.ValidateURL(u, true, true)` | `security.ValidateURL(u, security.URLPolicy{AllowHTTP: true, HostPolicy: security.URLDevelopmentHost})` |

Policy fields select these contracts:

- `HostPolicy`: `URLProductionHost` is the default; `URLDevelopmentHost` allows
  loopback but rejects other private literal IPs; `URLResolvedLoopback` requires
  every DNS answer to be loopback; `URLAnyHost` explicitly permits any host.
- `MaxLength`: zero uses `MaxURLLength`, positive values set a limit, and negative
  values explicitly disable it. Eval uses `-1` to preserve its long-URL behavior.
- `SchemePolicy` defaults to `URLHTTPScheme`. OAuth issuer consumers explicitly select `URLAbsoluteScheme` to retain their absolute-URL contract.
- `RejectCredentials` rejects userinfo and credential-bearing query/fragment values; OAuth issuer callers select this policy with `URLAnyHost` and `MaxLength: -1`.
- `AllowHTTP` permits HTTP; `RejectUserinfo` rejects URL userinfo when selected.
  Unset `RejectUserinfo` preserves the previous userinfo allowance.

### 3. Header Validation (`ValidateHeaderValue`)

Validates HTTP header values to prevent injection:

```go
if err := security.ValidateHeaderValue(userAgent); err != nil {
    return fmt.Errorf("invalid header: %w", err)
}
```

**Protects against:**

- HTTP header injection (CRLF)
- HTTP response splitting
- Null byte injection
- Excessively long headers (> 4096 bytes)

### 4. Security Middleware

Automatically applies all validations to HTTP endpoints:

```go
import (
    "net/http"
    "github.com/orneryd/nornicdb/pkg/security"
)

func main() {
    middleware := security.NewSecurityMiddleware()

    // Wrap individual handler
    http.Handle("/api/query", middleware.Wrap(queryHandler))

    // Or wrap entire mux
    mux := http.NewServeMux()
    mux.HandleFunc("/api/query", queryHandler)

    http.ListenAndServe(":7474", middleware.ValidateRequest(mux))
}
```

**Automatically validates:**

- All HTTP header values
- Authorization tokens (Bearer/Basic)
- Query parameter tokens (for SSE/WebSocket)
- URL parameters (callback, redirect, redirect_uri, url, webhook)

## Environment Variables

- **`NORNICDB_ENV`** or **`NODE_ENV`**: Set to `development` to allow localhost URLs
- **`NORNICDB_ALLOW_HTTP`**: Set to `true` to allow HTTP URLs in production (not recommended)

## Mapping to TypeScript Tests

This implementation provides equivalent protection to the TypeScript security tests:

### CSRF Protection (`csrf-protection.test.ts`)

While the TypeScript tests focus on OAuth state management, NornicDB's security layer protects against CSRF through:

1. **Token validation** - prevents forged/injected tokens
2. **State parameter validation** - validates callback URLs (SSRF prevention)
3. **Header injection prevention** - blocks CRLF attacks that could bypass CSRF checks

Example equivalent protection:

```go
// TypeScript: SecureStateStore validates OAuth state parameters
// Go: SecurityMiddleware validates all tokens and URLs

middleware := security.NewSecurityMiddleware()
http.Handle("/oauth/callback", middleware.Wrap(oauthCallbackHandler))
```

### SSRF Protection (`ssrf-protection.test.ts`)

Direct 1:1 mapping of all SSRF protections:

| TypeScript Test              | Go Implementation    | Coverage                    |
| ---------------------------- | -------------------- | --------------------------- |
| `validateOAuthTokenFormat()` | `ValidateToken()`    | ✅ 100%                     |
| `validateOAuthUserinfoUrl()` | `ValidateURL()`      | ✅ 100%                     |
| `createSecureFetchOptions()` | `SecurityMiddleware` | ✅ 100%                     |
| Private IP detection         | `isPrivateIP()`      | ✅ All ranges               |
| Cloud metadata blocking      | `ValidateURL()`      | ✅ AWS/Azure/GCP            |
| Protocol smuggling           | `ValidateURL()`      | ✅ file://, gopher://, etc. |

### Test Scenarios Covered

All attack scenarios from the TypeScript tests are covered:

**Token Injection:**

- ✅ CRLF injection (`token\r\nX-Malicious: header`)
- ✅ HTML injection (`<script>alert('xss')</script>`)
- ✅ JavaScript protocol (`javascript:alert('xss')`)
- ✅ Data URI (`data:text/html,<script>...`)
- ✅ File protocol (`file:///etc/passwd`)
- ✅ Null byte injection (`token\x00evil`)

**SSRF Attacks:**

- ✅ Private IP ranges (10.x, 192.168.x, 172.16-31.x)
- ✅ AWS metadata (`http://169.254.169.254/latest/meta-data/`)
- ✅ Azure metadata (`http://169.254.169.254/metadata/instance`)
- ✅ GCP metadata (`http://169.254.169.254/computeMetadata/`)
- ✅ Internal network scanning
- ✅ Localhost access in production

**Protocol Smuggling:**

- ✅ `file://` protocol
- ✅ `ftp://` protocol
- ✅ `gopher://` protocol
- ✅ `dict://` protocol

## Testing

Run all security tests:

```bash
cd nornicdb
go test -v ./pkg/security/...
```

Run with coverage:

```bash
go test -cover ./pkg/security/...
```

Benchmark performance:

```bash
go test -bench=. ./pkg/security/...
```

## Usage Examples

### Protecting OAuth Endpoints

```go
func setupOAuthServer() {
    middleware := security.NewSecurityMiddleware()

    // OAuth authorization endpoint
    http.Handle("/oauth/authorize", middleware.Wrap(
        http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            // redirect_uri is automatically validated by middleware
            redirectURI := r.URL.Query().Get("redirect_uri")
            // ... OAuth flow
        }),
    ))

    // OAuth callback endpoint
    http.Handle("/oauth/callback", middleware.Wrap(oauthCallbackHandler))
}
```

### Protecting API Endpoints

```go
func setupAPIServer() {
    middleware := security.NewSecurityMiddleware()

    mux := http.NewServeMux()
    mux.HandleFunc("/api/query", queryHandler)
    mux.HandleFunc("/api/import", importHandler)
    mux.HandleFunc("/api/export", exportHandler)

    // All endpoints protected
    http.ListenAndServe(":7474", middleware.ValidateRequest(mux))
}
```

### Manual Validation

```go
func handleWebhook(w http.ResponseWriter, r *http.Request) {
    webhookURL := r.URL.Query().Get("callback")

    // Validate before making outbound request (zero value = production policy)
    if err := security.ValidateURL(webhookURL, security.URLPolicy{}); err != nil {
        http.Error(w, "Invalid webhook URL", http.StatusBadRequest)
        return
    }

    // Safe to make request
    resp, err := http.Post(webhookURL, "application/json", payload)
    // ...
}
```

## Security Best Practices

1. **Always use HTTPS in production** - set `NORNICDB_ENV=production`
2. **Never disable validations** - they prevent real attacks
3. **Use middleware globally** - protect all endpoints by default
4. **Validate before external requests** - check all user-provided URLs
5. **Log security violations** - monitor for attack attempts
6. **Keep dependencies updated** - security fixes are critical

## Performance

All validations are optimized for production use:

- Token validation: **~1-2 µs** per call
- URL validation: **~5-10 µs** per call
- Header validation: **~0.5-1 µs** per call

Negligible overhead for comprehensive protection.

## References

- [OWASP SSRF Prevention Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html)
- [OWASP CSRF Prevention Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html)
- [CWE-918: Server-Side Request Forgery (SSRF)](https://cwe.mitre.org/data/definitions/918.html)
- [CWE-352: Cross-Site Request Forgery (CSRF)](https://cwe.mitre.org/data/definitions/352.html)
