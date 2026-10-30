# Multi-Database E2E Test Script

This directory contains utilities and scripts for testing multi-database functionality.

## Files

- `run_multi_db_e2e_test.sh` - Complete E2E test script that runs all test steps
- `../cmd/cleanup-orphaned/` - Utility to clean up orphaned (unprefixed) nodes

## Prerequisites

1. **Stop NornicDB server** (required for cleanup):
   ```bash
   # Stop the server if it's running
   pkill -f nornicdb
   # Or use your process manager
   ```

2. **Install dependencies**:
   - `jq` - JSON processor (install via `brew install jq` on macOS)
   - `curl` - HTTP client (usually pre-installed)

3. **Set environment variables** (optional):
   ```bash
   export SERVER_URL="http://localhost:7474"
   export AUTH_USER="admin"
   export AUTH_PASS="password"
   export DEFAULT_DB="nornic"  # or "neo4j" if you configured it
   ```

## Step 1: Clean Up Orphaned Nodes

Before running the E2E test, clean up any orphaned (unprefixed) nodes that may exist from previous test runs:

```bash
# 1. Stop the NornicDB server
pkill -f nornicdb

# 2. Build the cleanup utility
go build -o /tmp/cleanup-orphaned ./cmd/cleanup-orphaned

# 3. Run cleanup (replace with your data directory)
/tmp/cleanup-orphaned /usr/local/var/nornicdb/data

# When prompted, type "yes" to confirm deletion
```

**What this does:**
- Scans all nodes in the database
- Identifies orphaned nodes (unprefixed, non-system nodes)
- Deletes them to ensure a clean state
- Preserves system nodes (metadata, migration status)

## Step 2: Start NornicDB Server

```bash
# Start the server
./nornicdb serve

# Or with custom configuration
./nornicdb serve --default-database nornic
```

Wait for the server to fully start (you'll see "Server started" message).

## Step 3: Run E2E Test Script

```bash
# Make script executable (if not already)
chmod +x scripts/run_multi_db_e2e_test.sh

# Run the test
./scripts/run_multi_db_e2e_test.sh
```

**What the script does:**

1. **Verifies In-Place Upgrade** - Checks that existing data is accessible
2. **Creates First Database** - Creates `test_db_a`
3. **Inserts Data in First Database** - Creates nodes and relationships
4. **Queries First Database** - Verifies data exists
5. **Creates Second Database** - Creates `test_db_b`
6. **Inserts Data in Second Database** - Creates different nodes
7. **Queries Second Database** - Verifies data exists
8. **Verifies Isolation** - Confirms databases don't see each other's data
9. **Creates Composite Database** - Creates `test_composite` spanning both databases
10. **Queries Composite Database** - Verifies unified view across databases
11. **Cleanup** - Drops all test databases

## Expected Output

The script will print colored output:
- 🔵 **Blue** - Test step headers
- ✅ **Green** - Success messages
- ❌ **Red** - Error messages
- ℹ️ **Yellow** - Info messages

Each step will show:
- The query being executed
- The JSON response from the server
- Summary information (counts, etc.)

## Troubleshooting

### Issue: "Server is not responding"

**Solution**: Make sure NornicDB is running:
```bash
./nornicdb serve
```

### Issue: "jq: command not found"

**Solution**: Install jq:
```bash
# macOS
brew install jq

# Linux (Ubuntu/Debian)
sudo apt-get install jq

# Linux (RHEL/CentOS)
sudo yum install jq
```

### Issue: "Cannot acquire directory lock"

**Solution**: The server is still running. Stop it first:
```bash
pkill -f nornicdb
# Wait a few seconds, then try again
```

### Issue: "Database 'neo4j' not found"

**Solution**: The script uses `nornic` as the default database. If you configured NornicDB to use `neo4j`, set:
```bash
export DEFAULT_DB="neo4j"
./scripts/run_multi_db_e2e_test.sh
```

### Issue: Authentication errors

**Solution**: Check your credentials:
```bash
export AUTH_USER="admin"
export AUTH_PASS="password"
# Or update the script with your credentials
```

## Manual Testing

If you prefer to run tests manually, see:
- `docs/development/MULTI_DB_E2E_TEST.md` - Complete manual test guide with all queries

## Verification

After running the script, verify:

1. **All test databases were dropped**:
   ```bash
   curl -s -u admin:password http://localhost:7474/db/system/tx/commit \
     -H "Content-Type: application/json" \
     -d '{"statements":[{"statement":"SHOW DATABASES"}]}' | jq '.results[0].data'
   ```
   Should only show `nornic` and `system`.

2. **Default database still has data**:
   ```bash
   curl -s -u admin:password http://localhost:7474/db/nornic/tx/commit \
     -H "Content-Type: application/json" \
     -d '{"statements":[{"statement":"MATCH (n) RETURN count(n) as count"}]}' | jq '.results[0].data'
   ```

3. **No orphaned nodes** (requires server to be stopped):
   ```bash
   /tmp/inspect-nornicdb /usr/local/var/nornicdb/data
   ```
   Should show only system nodes in the `<unprefixed>` namespace (or no unprefixed nodes at all).

## Next Steps

After successful E2E test:

1. **Performance Testing** - Test with larger datasets
2. **Concurrent Access** - Test multiple clients accessing different databases
3. **Schema Operations** - Test indexes/constraints in individual databases
4. **Error Handling** - Test error scenarios

## Related Documentation

- [Multi-Database User Guide](../docs/user-guides/multi-database.md)
- [E2E Test Guide](../docs/development/MULTI_DB_E2E_TEST.md)
- [Multi-Database Architecture](../docs/architecture/MULTI_DB_IMPLEMENTATION_SPEC.md)

