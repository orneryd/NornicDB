# Security Guide

**Comprehensive security features to protect your NornicDB deployment.**

## 🚀 Quick Start

NornicDB v1.0.0 includes **automatic security protection** on all HTTP endpoints. No configuration required for basic protection.

```bash
# Production mode (default) - strict security
NORNICDB_ENV=production

# Development mode - relaxed for local development
NORNICDB_ENV=development
```

## 📚 Documentation

- **[HTTP Security Implementation](http-security.md)** - Complete implementation details
- **[Query Cache Security](query-cache-security.md)** - Query analysis and caching security model
- **[LLM & AST Security](llm-ast-security.md)** - Safe patterns for LLM integration and plugin security
- **[Cluster Security](../operations/cluster-security.md)** - Multi-node authentication
- **[Compliance Guide](../compliance/)** - GDPR, HIPAA, SOC2

## 🔒 Security Features

### HTTP Security Middleware ⭐ NEW in v1.0.0

All HTTP endpoints are automatically protected against:

| Attack Type            | Protection                                     | Status    |
| ---------------------- | ---------------------------------------------- | --------- |
| **CSRF**               | Token validation, injection prevention         | ✅ Active |
| **SSRF**               | Private IP blocking, metadata service blocking | ✅ Active |
| **XSS**                | Script tag filtering, protocol validation      | ✅ Active |
| **Header Injection**   | CRLF/null byte filtering                       | ✅ Active |
| **Protocol Smuggling** | file://, gopher://, ftp:// blocked             | ✅ Active |

### Query Analysis Security

The query cache system uses conservative keyword detection:

| Concern                        | Status        | Notes                              |
| ------------------------------ | ------------- | ---------------------------------- |
| **Write ops hidden as reads**  | ✅ Protected  | Not possible in valid Cypher       |
| **Cache poisoning**            | ✅ Protected  | Keys include query + parameters    |
| **Read ops marked as writes**  | ⚡ Accepted   | Performance impact only, not security |

See **[Query Cache Security](query-cache-security.md)** for full details.

### Authentication & Authorization

- **JWT Authentication** - Stateless token-based auth
- **RBAC** - Role-based access control
- **API Keys** - Service-to-service authentication

### Data Protection

- **Field-Level Encryption** - AES-256-GCM encryption
- **TLS/HTTPS** - Required in production mode
- **Audit Logging** - Complete operation history

## 🛡️ Attack Prevention

### SSRF Protection

Automatically blocks requests to:

```
❌ Private IPs (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
❌ Localhost (127.0.0.0/8) - in production
❌ Link-local (169.254.0.0/16)
❌ AWS/Azure/GCP metadata services
❌ Dangerous protocols (file://, gopher://, ftp://)
```

### Token Validation

Automatically validates:

```
✅ Bearer tokens in Authorization header
✅ Query parameter tokens (SSE/WebSocket)
✅ OAuth state parameters
✅ Callback/redirect URLs
```

## 🔧 Configuration

### Environment Variables

```bash
# Production mode (strict) - DEFAULT
NORNICDB_ENV=production

# Development mode (allows localhost)
NORNICDB_ENV=development

# Allow HTTP (not recommended for production)
NORNICDB_ALLOW_HTTP=true
```

### Production vs Development

| Feature                 | Production | Development |
| ----------------------- | ---------- | ----------- |
| Block localhost         | ✅ Yes     | ❌ No       |
| Require HTTPS           | ✅ Yes     | ❌ No       |
| Block private IPs       | ✅ Yes     | ✅ Yes      |
| Block metadata services | ✅ Yes     | ✅ Yes      |

## 📖 Usage Examples

### Automatic Protection (Default)

```go
// No code changes needed - middleware is active!
// All endpoints automatically protected
server := nornicdb.NewServer()
server.Start() // Security middleware included
```

### Manual Validation (Optional)

```go
import "github.com/orneryd/nornicdb/pkg/security"

// Validate external URLs before making requests
if err := security.ValidateURL(webhookURL, false, false); err != nil {
    return fmt.Errorf("invalid webhook: %w", err)
}

// Validate tokens before processing
if err := security.ValidateToken(apiKey); err != nil {
    return fmt.Errorf("invalid token: %w", err)
}
```

## 📊 Test Coverage

- **19 unit tests** covering 30+ attack scenarios
- **8 integration tests** with full HTTP stack
- **Performance:** < 10µs overhead per request

## 🔗 See Also

- **[HTTP Security Implementation](http-security.md)** - Full technical details
- **[Compliance Guide](../compliance/)** - Regulatory compliance
- **[Operations Security](../operations/cluster-security.md)** - Cluster authentication
- **[OWASP SSRF Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html)**
- **[OWASP CSRF Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html)**

---

**Secure your deployment** → **[Implementation Details](http-security.md)**
