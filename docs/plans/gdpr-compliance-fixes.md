# GDPR Compliance Fixes - Implementation Plan

## Current Issues

The current GDPR export and delete implementations have several critical gaps:

### ❌ Issues with Current Implementation

1. **Only handles nodes with `owner_id` property**
   - Assumes all user data has `owner_id` property
   - Misses data without this property
   - Misses data that references user in other ways (e.g., `created_by`, `author`, `user_id`)

2. **Doesn't handle relationships (edges)**
   - Only processes nodes, not relationships
   - Relationships connected to user's nodes are not exported/deleted
   - Relationships owned by the user are not handled

3. **Doesn't handle user account information**
   - User account in auth system is not exported/deleted
   - User metadata, roles, login history not included

4. **Doesn't handle audit logs**
   - Audit logs containing user data are not exported/deleted
   - Compliance records are not included

5. **Doesn't handle indirect references**
   - Nodes that mention the user (e.g., comments, mentions)
   - Nodes created by the user (if tracked differently)
   - Relationships where user is start or end node

## What GDPR Actually Requires

### Article 15 - Right of Access
**Must export ALL personal data**, including:
- ✅ All nodes owned by or referencing the user
- ✅ All relationships connected to user's nodes
- ✅ User account information
- ✅ Audit logs
- ✅ Any data that can identify the user

### Article 17 - Right to Erasure
**Must delete ALL personal data**, including:
- ✅ All nodes owned by or referencing the user
- ✅ All relationships connected to user's nodes
- ✅ User account in auth system
- ✅ Audit logs (or anonymize them)
- ✅ Search indexes
- ✅ Any data that can identify the user

## Proposed Solution

### 1. Enhanced User Data Identification

Instead of only checking `owner_id`, identify user data through multiple methods:

```go
func isUserData(node *storage.Node, userID string) bool {
    // Direct ownership
    if owner, ok := node.Properties["owner_id"].(string); ok && owner == userID {
        return true
    }
    
    // Created by user
    if createdBy, ok := node.Properties["created_by"].(string); ok && createdBy == userID {
        return true
    }
    
    // Author field
    if author, ok := node.Properties["author"].(string); ok && author == userID {
        return true
    }
    
    // User ID field
    if uid, ok := node.Properties["user_id"].(string); ok && uid == userID {
        return true
    }
    
    // Email match (if node has email matching user's email)
    // ... etc
    
    return false
}
```

### 2. Include Relationships

```go
func (db *DB) ExportUserData(ctx context.Context, userID, format string) ([]byte, error) {
    // ... collect nodes ...
    
    // Also collect relationships
    var userEdges []map[string]interface{}
    err := storage.StreamEdgesWithFallback(ctx, db.storage, 1000, func(e *storage.Edge) error {
        // Include if:
        // - Connected to user's nodes
        // - Owned by user (has owner_id property)
        // - Created by user
        if isUserEdge(e, userID, userNodeIDs) {
            userEdges = append(userEdges, ...)
        }
        return nil
    })
    
    // Include in export
    return json.Marshal(map[string]interface{}{
        "user_id": userID,
        "nodes": userData,
        "relationships": userEdges,  // ← ADD THIS
        "user_account": userAccountInfo,  // ← ADD THIS
        "audit_logs": auditLogs,  // ← ADD THIS
        "exported_at": time.Now(),
    })
}
```

### 3. Include User Account Information

```go
func (db *DB) ExportUserData(ctx context.Context, userID, format string) ([]byte, error) {
    // ... collect graph data ...
    
    // Get user account from auth system
    userAccount := db.getUserAccountInfo(userID)  // ← ADD THIS
    
    return json.Marshal(map[string]interface{}{
        "user_id": userID,
        "user_account": userAccount,  // Username, email, roles, created_at, etc.
        "nodes": userData,
        "relationships": userEdges,
        "audit_logs": auditLogs,
        "exported_at": time.Now(),
    })
}
```

### 4. Delete User Account

```go
func (db *DB) DeleteUserData(ctx context.Context, userID string) error {
    // ... delete graph data ...
    
    // Also delete user account from auth system
    if db.auth != nil {
        // Find username from userID
        username := db.getUsernameFromID(userID)
        if username != "" {
            db.auth.DeleteUser(username)  // ← ADD THIS
        }
    }
    
    return nil
}
```

### 5. Handle Relationships in Delete

```go
func (db *DB) DeleteUserData(ctx context.Context, userID string) error {
    // Collect user's node IDs first
    var userNodeIDs []storage.NodeID
    // ... collect nodes ...
    
    // Delete relationships connected to user's nodes
    var edgesToDelete []storage.EdgeID
    err := storage.StreamEdgesWithFallback(ctx, db.storage, 1000, func(e *storage.Edge) error {
        // Delete if connected to any user node
        if contains(userNodeIDs, e.StartNode) || contains(userNodeIDs, e.EndNode) {
            edgesToDelete = append(edgesToDelete, e.ID)
        }
        // Also check if edge is owned by user
        if isUserEdge(e, userID, nil) {
            edgesToDelete = append(edgesToDelete, e.ID)
        }
        return nil
    })
    
    // Delete edges
    for _, edgeID := range edgesToDelete {
        db.storage.DeleteEdge(edgeID)
    }
    
    // Delete nodes
    // ... existing code ...
    
    return nil
}
```

## Implementation Steps

### Phase 1: Enhance Node Identification
- [ ] Add helper function `isUserData()` with multiple property checks
- [ ] Update `ExportUserData` to use enhanced identification
- [ ] Update `DeleteUserData` to use enhanced identification
- [ ] Update `AnonymizeUserData` to use enhanced identification

### Phase 2: Add Relationship Support
- [ ] Add `StreamEdgesWithFallback` or similar to storage interface
- [ ] Collect relationships in `ExportUserData`
- [ ] Delete relationships in `DeleteUserData`
- [ ] Anonymize relationships in `AnonymizeUserData`

### Phase 3: Add User Account Support
- [ ] Add method to get user account info from auth system
- [ ] Include user account in export
- [ ] Delete user account in delete operation
- [ ] Handle OAuth users appropriately

### Phase 4: Add Audit Log Support
- [ ] Export audit logs for user
- [ ] Delete or anonymize audit logs
- [ ] Ensure audit logs are searchable by user ID

### Phase 5: Testing & Validation
- [ ] Test with nodes that have different property names
- [ ] Test with relationships
- [ ] Test with user account
- [ ] Test with audit logs
- [ ] Validate GDPR compliance

## Configuration Options

Add configuration for GDPR behavior:

```go
type GDPRConfig struct {
    // Property names to check for user identification
    UserIDProperties []string  // ["owner_id", "created_by", "author", "user_id"]
    
    // Include audit logs in export
    IncludeAuditLogs bool
    
    // Include user account in export
    IncludeUserAccount bool
    
    // Delete audit logs or anonymize
    DeleteAuditLogs bool  // false = anonymize
    
    // Delete user account or disable
    DeleteUserAccount bool  // false = disable only
}
```

## Example: Complete GDPR Export

```json
{
  "user_id": "user-123",
  "exported_at": "2024-12-01T10:00:00Z",
  "user_account": {
    "id": "usr-abc123",
    "username": "alice",
    "email": "alice@example.com",
    "roles": ["editor"],
    "created_at": "2024-01-01T00:00:00Z",
    "last_login": "2024-11-30T15:30:00Z"
  },
  "nodes": [
    {
      "id": "node-1",
      "labels": ["Document"],
      "properties": {"title": "My Document", "owner_id": "user-123"},
      "created_at": "2024-01-15T10:00:00Z"
    }
  ],
  "relationships": [
    {
      "id": "edge-1",
      "type": "CREATED",
      "start_node": "user-123",
      "end_node": "node-1",
      "properties": {},
      "created_at": "2024-01-15T10:00:00Z"
    }
  ],
  "audit_logs": [
    {
      "timestamp": "2024-01-15T10:00:00Z",
      "event_type": "node_create",
      "user_id": "user-123",
      "resource": "node-1",
      "action": "CREATE"
    }
  ]
}
```

## Example: Complete GDPR Delete

```json
{
  "status": "completed",
  "user_id": "user-123",
  "deleted_at": "2024-12-01T10:00:00Z",
  "summary": {
    "nodes_deleted": 42,
    "relationships_deleted": 156,
    "user_account_deleted": true,
    "audit_logs_anonymized": 1234
  }
}
```

## Migration Notes

- Existing code that relies on `owner_id` will continue to work
- New implementation will find more user data (better compliance)
- May need to update applications to set `owner_id` or other properties
- Consider adding database migration to add `owner_id` to existing nodes

## Testing Checklist

- [ ] Export includes nodes with `owner_id`
- [ ] Export includes nodes with `created_by`
- [ ] Export includes nodes with `author`
- [ ] Export includes relationships
- [ ] Export includes user account
- [ ] Export includes audit logs
- [ ] Delete removes all nodes
- [ ] Delete removes all relationships
- [ ] Delete removes user account
- [ ] Delete handles search indexes
- [ ] Anonymize preserves structure
- [ ] Anonymize removes PII
- [ ] Multi-database support (system database)

