# Role-Based Access Control (RBAC)

**Fine-grained access control with JWT authentication.**

## Overview

NornicDB implements role-based access control (RBAC) to meet compliance requirements:

- **JWT Authentication** - Stateless token-based auth
- **4 Built-in Roles** - Admin, Editor, Viewer, None
- **Permission System** - Read, Write, Admin permissions
- **Account Security** - Lockout, password policies

## Roles and Permissions

### Built-in Roles

| Role | Read | Write | Admin | Description |
|------|------|-------|-------|-------------|
| `admin` | ✅ | ✅ | ✅ | Full access, user management |
| `editor` | ✅ | ✅ | ❌ | Read and write data |
| `viewer` | ✅ | ❌ | ❌ | Read-only access |
| `none` | ❌ | ❌ | ❌ | No access (disabled) |

### Permission Mapping

```go
// Permissions
auth.PermRead   // Read nodes, edges, run queries
auth.PermWrite  // Create, update, delete data
auth.PermAdmin  // User management, configuration
```

## Configuration

### Server Configuration

```yaml
# nornicdb.yaml
auth:
  enabled: true
  
  # JWT settings
  jwt_secret: "${NORNICDB_JWT_SECRET}"  # Min 32 chars
  jwt_expiry: 24h
  
  # Password policy
  min_password_length: 12
  require_uppercase: true
  require_number: true
  require_special: true
  
  # Security
  max_failed_attempts: 5
  lockout_duration: 15m
```

### Environment Variables

```bash
# Required: JWT signing secret (min 32 characters)
export NORNICDB_JWT_SECRET="your-super-secret-jwt-key-min-32-chars"

# Optional: Disable auth for development
export NORNICDB_NO_AUTH=true
```

## User Management

### User Storage and Persistence

NornicDB stores all user accounts in the **system database** for persistence and security:

- **Persistent Storage**: Users are stored as nodes in the system database with labels `["_User", "_System"]`
- **Automatic Loading**: All users are automatically loaded from the system database on server startup
- **Backup Integration**: User accounts are included in database backups automatically
- **GDPR Compliance**: User data can be exported and deleted via GDPR endpoints
- **Security**: Internal database IDs are never exposed in API responses

**Storage Details:**
- Users are stored as graph nodes (not separate tables)
- Node ID format: `user:{username}` (e.g., `user:admin`)
- All user operations (create, update, delete) are persisted immediately
- In-memory cache provides fast lookups while maintaining persistence

### Create Users

```bash
# Via Web UI (Admin Panel)
# Navigate to /security/admin as an admin user

# Via API
curl -X POST http://localhost:7474/auth/users \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "alice",
    "password": "SecurePass123!",
    "roles": ["viewer"]
  }'
```

**Response:**
```json
{
  "username": "alice",
  "email": "alice@localhost",
  "roles": ["viewer"],
  "created_at": "2024-12-01T10:30:00Z",
  "disabled": false
}
```

**Note:** Internal database IDs are never included in responses for security reasons.

### Manage Users

```bash
# List all users (admin only)
curl -X GET http://localhost:7474/auth/users \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Get specific user
curl -X GET http://localhost:7474/auth/users/alice \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Update user roles
curl -X PUT http://localhost:7474/auth/users/alice \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"roles": ["editor"]}'

# Disable user
curl -X PUT http://localhost:7474/auth/users/alice \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"disabled": true}'

# Delete user
curl -X DELETE http://localhost:7474/auth/users/alice \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

### User Profile Management

Users can manage their own profiles via the Security page (`/security`):

**Change Password:**
```bash
curl -X POST http://localhost:7474/auth/password \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "old_password": "OldPass123!",
    "new_password": "NewSecurePass456!"
  }'
```

**Update Profile:**
```bash
curl -X PUT http://localhost:7474/auth/profile \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "alice@example.com",
    "metadata": {
      "department": "Engineering",
      "team": "Backend"
    }
  }'
```

## Authentication

### Login (Get Token)

```bash
# OAuth 2.0 password grant
curl -X POST http://localhost:7474/auth/token \
  -d "grant_type=password&username=alice&password=SecurePass123!"

# Response
{
  "access_token": "eyJhbGciOiJIUzI1NiIs...",
  "token_type": "Bearer",
  "expires_in": 86400
}
```

### Using Tokens

```bash
# Authorization header
curl http://localhost:7474/db/neo4j/tx/commit \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIs..."

# Or API key header
curl http://localhost:7474/db/neo4j/tx/commit \
  -H "X-API-Key: your-api-key"
```

## API Key Authentication

For service-to-service communication:

```bash
# Create API key
nornicdb apikey create --name "backend-service" --role editor

# Use API key
curl http://localhost:7474/nornicdb/search \
  -H "X-API-Key: ndb_sk_abc123..."
```

## Endpoint Protection

### Protected Endpoints

| Endpoint | Required Permission |
|----------|-------------------|
| `GET /health` | None (public) |
| `GET /status` | `read` |
| `GET /metrics` | `read` |
| `POST /db/neo4j/tx/commit` | `read` or `write` |
| `POST /nornicdb/search` | `read` |
| `DELETE /nornicdb/gdpr/*` | `admin` |
| `POST /auth/users` | `admin` |

### Code Example

```go
// Check permissions in handler
func (s *Server) handleProtectedEndpoint(w http.ResponseWriter, r *http.Request) {
    claims := r.Context().Value(claimsKey).(*auth.Claims)
    
    if !claims.HasPermission(auth.PermWrite) {
        http.Error(w, "Forbidden", http.StatusForbidden)
        return
    }
    
    // Handle request...
}
```

## Security Features

### Account Lockout

After 5 failed login attempts, accounts are locked for 15 minutes:

```go
// Attempt login
token, user, err := auth.Authenticate("alice", "wrongpass", ip, agent)
// After 5 failures: ErrAccountLocked

// Check lockout status
user.IsLocked()     // true
user.LockedUntil    // time.Time
```

### Password Hashing

Passwords are hashed using bcrypt with automatic salt generation:

- **Algorithm**: bcrypt with configurable cost factor (default: 10)
- **Salt**: Automatically generated and embedded in the hash (no separate salt storage needed)
- **Storage**: Password hashes are stored in the system database, never in plain text
- **Security**: Internal database IDs and password hashes are never exposed in API responses

```go
// Passwords are never stored in plain text
// Bcrypt automatically salts passwords
// Uses bcrypt.DefaultCost (10) - configurable via BcryptCost
hash, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
// Salt is embedded in the hash - no separate storage needed
```

**Configuration:**
```yaml
auth:
  bcrypt_cost: 10  # Higher = more secure but slower (4-31, default: 10)
  min_password_length: 8  # Minimum password length
```

### Session Management

```go
// Logout invalidates token (adds to blacklist)
auth.Logout(token)

// Validate token checks blacklist
claims, err := auth.ValidateToken(token)
// Returns ErrTokenRevoked if blacklisted
```

## Compliance Mapping

| Requirement | NornicDB Feature |
|-------------|------------------|
| GDPR Art.32 | Access controls, authentication |
| HIPAA §164.312(a)(1) | Unique user identification |
| HIPAA §164.312(d) | Person or entity authentication |
| FISMA AC-2 | Account management |
| SOC2 CC6.1 | Logical access controls |

## Audit Integration

All authentication events are logged:

```json
{
  "timestamp": "2024-12-01T10:30:00Z",
  "event_type": "LOGIN",
  "user_id": "usr_abc123",
  "username": "alice",
  "ip_address": "192.168.1.100",
  "user_agent": "Mozilla/5.0...",
  "success": true
}
```

See **[Audit Logging](audit-logging.md)** for details.

## See Also

- **[User Storage in System Database](../../plans/user-storage-in-system-db.md)** - User persistence architecture
- **[Encryption](encryption.md)** - Data protection
- **[Audit Logging](audit-logging.md)** - Compliance trails
- **[HIPAA Compliance](hipaa-compliance.md)** - Healthcare requirements
- **[Backup & Restore](../operations/backup-restore.md)** - Database backup (includes users)

