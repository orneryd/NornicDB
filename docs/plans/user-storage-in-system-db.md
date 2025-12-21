# User Storage in System Database

**Status:** ✅ **IMPLEMENTED** - Users are now stored persistently in the system database.

This document describes how NornicDB stores user accounts in the system database for persistence, security, and compliance.

## Overview

NornicDB stores all user accounts in the **system database** for persistence, security, and compliance. This follows industry-standard practices used by PostgreSQL, MySQL, Neo4j, and other enterprise systems.

## Implementation Status

✅ **Completed Features:**
- Users stored as nodes in system database with labels `["_User", "_System"]`
- Automatic loading of users on server startup
- All user operations (create, update, delete) persist immediately
- In-memory cache for fast authentication lookups
- Password security with bcrypt (salt embedded in hash)
- Internal database IDs never exposed in API responses
- User data included in database backups automatically
- GDPR export/deletion support for user accounts

## Architecture

### User Node Structure

Users are stored as nodes in the system database with:

- **Labels**: `["_User", "_System"]`
- **Node ID**: `user:{username}` (e.g., `user:admin`)
- **Properties**:
  ```json
  {
    "username": "admin",
    "email": "admin@example.com",
    "password_hash": "$2a$10$...",  // bcrypt hash (includes salt)
    "roles": ["admin"],
    "created_at": "2024-01-01T00:00:00Z",
    "updated_at": "2024-01-01T00:00:00Z",
    "last_login": "2024-01-01T00:00:00Z",
    "failed_logins": 0,
    "locked_until": null,
    "disabled": false,
    "metadata": {}
  }
  ```

**Note:** Internal database IDs are stored but never exposed in API responses for security.

### How It Works

1. **Storage Engine**: Authenticator requires a storage engine (system database storage)
2. **Startup Loading**: All users are automatically loaded from system database on server startup
3. **In-Memory Cache**: Users are cached in memory for fast authentication lookups
4. **Persistence**: All user operations (create, update, delete) are immediately persisted to the system database
5. **Backup Integration**: User accounts are automatically included in database backups

## Usage

### Server Initialization

The authenticator is automatically initialized with system database storage:

```go
// System database storage is automatically used
systemStorage, err := dbManager.GetStorage("system")
if err != nil {
    return fmt.Errorf("failed to get system database storage: %w", err)
}

authenticator, err := auth.NewAuthenticator(authConfig, systemStorage)
```

**Note:** Storage engine is required - there is no in-memory-only mode. For testing, use `storage.NewMemoryEngine()` via dependency injection.

### Testing

For unit tests, use a memory storage engine:

```go
memoryStorage := storage.NewMemoryEngine()
authenticator, err := auth.NewAuthenticator(config, memoryStorage)
```

## Security Features

✅ **Password Security:**
- Passwords hashed with bcrypt (salt automatically embedded in hash)
- Password hashes never serialized in JSON responses
- Minimum password length enforced (default: 8 characters)

✅ **Data Protection:**
- Internal database IDs never exposed in API responses
- User data stored in isolated system database
- Account lockout after failed login attempts
- Failed login tracking and lockout duration

✅ **Compliance:**
- Users included in database backups automatically
- GDPR export/deletion endpoints support user accounts
- Audit trail for all user operations in system database
- User changes tracked with timestamps

## Performance

- **In-Memory Cache**: Fast authentication lookups (O(1))
- **Storage Writes**: Only on create/update/delete (infrequent operations)
- **Storage Reads**: Only on server startup (once per restart)
- **Impact**: Minimal - authentication performance unchanged

## Backup and Recovery

User accounts are automatically included in database backups:

```bash
# Backup includes all user accounts
nornicdb backup --output backup.db

# Restore includes user accounts
nornicdb restore --input backup.db
```

**Note:** When restoring from backup, all user accounts are restored along with the database data.

## Benefits

1. ✅ **Persistence**: Users survive server restarts
2. ✅ **Backup Integration**: Users included in database backups automatically
3. ✅ **Recovery**: Can restore users from backup
4. ✅ **Compliance**: GDPR export/deletion works automatically for user accounts
5. ✅ **Audit Trail**: User changes tracked in database with timestamps
6. ✅ **Security**: Internal IDs never exposed, passwords properly hashed
7. ✅ **Standard Practice**: Matches industry standards (PostgreSQL, MySQL, Neo4j, etc.)

## Industry Standards

This implementation follows the same pattern as major database systems:

- **PostgreSQL**: `pg_authid` system table
- **MySQL**: `mysql.user` system table
- **Neo4j**: `:User` nodes in system database
- **MongoDB**: `admin.users` collection
- **LDAP**: `ou=users` organizational unit

## Related Documentation

- **[RBAC Guide](../compliance/rbac.md)** - Complete authentication and authorization documentation
- **[Backup & Restore](../operations/backup-restore.md)** - Database backup procedures (includes users)
- **[GDPR Compliance](../compliance/gdpr-compliance.md)** - User data export and deletion
- **[Multi-Database Guide](../user-guides/multi-database.md)** - System database overview

