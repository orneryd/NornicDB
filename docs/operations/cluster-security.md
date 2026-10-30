# Cluster Security

**Configure authentication and security for NornicDB clusters.**

## Overview

NornicDB clusters use the same authentication system for both client connections and inter-node communication:

- **Bolt Protocol Authentication** - Standard Neo4j-compatible auth (port 7687)
- **JWT Bearer Tokens** - Recommended for cluster inter-node auth
- **Service Accounts** - Non-human identities for automation
- **RBAC Integration** - Full role-based access control

## Supported Auth Schemes

| Scheme | Principal | Description |
|--------|-----------|-------------|
| `basic` | username | Username/password authentication |
| `basic` | (empty) | JWT token in credentials field |
| `bearer` | (ignored) | JWT token in credentials field |
| `none` | - | Anonymous (if enabled, grants viewer role) |

## Authentication Methods

### Basic Authentication

Username/password authentication via Bolt protocol:

```go
// Go
driver, _ := neo4j.NewDriverWithContext(
    "bolt://localhost:7687",
    neo4j.BasicAuth("admin", "password", ""),
)
```

```python
# Python
driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("admin", "password")
)
```

```typescript
// JavaScript/TypeScript
const driver = neo4j.driver(
    'bolt://localhost:7687',
    neo4j.auth.basic('admin', 'password')
);
```

### JWT Bearer Tokens (Recommended for Clusters)

For cluster nodes, JWT tokens provide stateless, scalable authentication:

#### Step 1: Generate a Shared JWT Secret

```bash
# Generate a 48-byte (384-bit) secret (min 32 bytes required)
openssl rand -base64 48
# Example: K7xB2mN9pQr4sT6uV8wY0zA3bC5dE7fG9hI1jK3lM5nO7pQ9rS1tU3vW5xY7zA==
```

**⚠️ Important**: This secret must be identical on ALL cluster nodes.

#### Step 2: Configure All Nodes

```bash
# .env file for ALL cluster nodes
NORNICDB_AUTH_JWT_SECRET=K7xB2mN9pQr4sT6uV8wY0zA3bC5dE7fG9hI1jK3lM5nO7pQ9rS1tU3vW5xY7zA==
NORNICDB_AUTH=admin/your-strong-password
```

#### Step 3: Generate Cluster Tokens

Cluster tokens are generated through the embedded Go API. There is no dedicated HTTP route for cluster-token generation; for general API tokens see `POST /auth/api-token` (admin permission required), but inter-node tokens are scoped via `pkg/auth.Authenticator.GenerateClusterToken`:

```go
// Token never expires (default)
token, _ := authenticator.GenerateClusterToken("node-2", auth.RoleAdmin)

// Token with custom expiry
token7d, _ := authenticator.GenerateClusterTokenWithExpiry(
    "node-2", auth.RoleAdmin, 7*24*time.Hour)
```

#### Step 4: Connect Using JWT

```go
// Go - Empty username signals JWT authentication
driver, _ := neo4j.NewDriverWithContext(
    "bolt://node1.cluster.local:7687",
    neo4j.BasicAuth("", token, ""),  // Empty username = JWT
)
```

```python
# Python
driver = GraphDatabase.driver(
    "bolt://node1.cluster.local:7687",
    auth=("", token)  # Empty username = JWT
)
```

```typescript
// JavaScript/TypeScript
const driver = neo4j.driver(
    'bolt://node1.cluster.local:7687',
    neo4j.auth.basic('', token)  // Empty username = JWT
);
```

## Cluster Node Authentication

### Service Account Setup

Service accounts for cluster nodes are users with admin role and a strong password. Create them through the standard user-management API:

```bash
# Via HTTP API (admin permission required)
curl -X POST http://localhost:7474/auth/users \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "cluster-node-1",
    "password": "secure-generated-password",
    "roles": ["admin"]
  }'
```

Via Go code:
```go
// Create service account for cluster node
authenticator.CreateUser("cluster-node-1", "secure-password",
    []auth.Role{auth.RoleAdmin})
```

### Docker Compose with Auth

```yaml
version: '3.8'

services:
  nornicdb-node1:
    image: nornicdb:latest
    environment:
      NORNICDB_AUTH_JWT_SECRET: ${SHARED_JWT_SECRET}
      NORNICDB_AUTH: "admin/${ADMIN_PASSWORD}"
    secrets:
      - jwt-secret

secrets:
  jwt-secret:
    external: true
```

## Permissions

| Query Type | Required Permission |
|------------|---------------------|
| `MATCH ... RETURN` | `read` |
| `CREATE`, `MERGE`, `SET` | `write` |
| `DELETE` | `delete` |
| `CREATE INDEX` | `schema` |

## Security Best Practices

### 1. Use Strong Secrets

```bash
# Generate secure secrets
openssl rand -base64 32  # Service account
openssl rand -base64 48  # JWT secret
```

### 2. Network Isolation

```yaml
# Isolate cluster network
networks:
  cluster-internal:
    internal: true  # No external access
```

### 3. Secret Rotation

Rotate cluster credentials by updating the user's password and roles through the standard auth API. Rotate the shared `NORNICDB_JWT_SECRET` quarterly via your secrets manager and restart cluster nodes in a controlled rolling order.

## Troubleshooting

### Authentication Failures

```bash
# List all users (admin permission required)
curl -X GET http://localhost:7474/auth/users \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

### Permission Denied

```bash
# Update the user's roles
curl -X PUT http://localhost:7474/auth/users/cluster-node-1 \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"roles": ["admin"]}'
```

### Debug Logging

```bash
export NORNICDB_LOG_LEVEL=debug
```

## See Also

- **[Clustering Guide](../user-guides/clustering.md)** - Full clustering setup
- **[RBAC](../compliance/rbac.md)** - Authentication details
- **[Scaling](scaling.md)** - Performance scaling


