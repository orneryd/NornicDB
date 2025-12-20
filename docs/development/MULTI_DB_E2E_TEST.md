# Multi-Database End-to-End Test Guide

This guide provides a comprehensive test sequence to verify multi-database functionality, including in-place upgrade verification, database creation, data isolation, composite databases, and proper cleanup.

## Prerequisites

1. **Start NornicDB** with an existing dataset (to verify in-place upgrade)
   ```bash
   ./nornicdb serve
   ```

2. **Connect to NornicDB** using your preferred client:
   - Cypher Shell: `./nornicdb cypher-shell`
   - Web UI: `http://localhost:7474`
   - Bolt client: `bolt://localhost:7687`

### ⚠️ Important: Default Database Name

**NornicDB uses `"nornic"` as the default database name** (Neo4j uses `"neo4j"`).

If you're using a Neo4j-compatible client that defaults to `"neo4j"`, you have two options:

#### Option 1: Configure NornicDB to use `"neo4j"` as default (Recommended for Neo4j compatibility)

**Via Config File:**
```yaml
database:
  default_database: "neo4j"
```

**Via Environment Variable:**
```bash
export NORNICDB_DEFAULT_DATABASE=neo4j
# Or Neo4j-compatible:
export NEO4J_dbms_default__database=neo4j
```

**Via Command Line:**
```bash
./nornicdb serve --default-database neo4j
```

Then restart NornicDB.

#### Option 2: Switch to `"nornic"` database in your client

**In Web UI:**
- Look for a database selector/dropdown in the UI
- Select `"nornic"` from the list
- Or use the connection URL: `http://localhost:7474/db/nornic/tx/commit`

**In Cypher Shell:**
```cypher
:USE nornic
```

**In Drivers:**
```python
# Python
driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    database="nornic"  # Specify nornic instead of neo4j
)
```

**In HTTP API:**
```
POST /db/nornic/tx/commit
```

---

## Test Sequence

### Step 1: Verify In-Place Upgrade

**Purpose**: Confirm that existing data was automatically migrated to the default database namespace.

**⚠️ Before running this query**: Make sure you're connected to the correct database:
- If you configured NornicDB to use `"neo4j"` as default, you're already connected
- If using default `"nornic"`, switch to it in your client (see Prerequisites above)

```cypher
-- List all databases (should show default database and system database)
SHOW DATABASES
```

**Expected Result**: 
- Should show at least `nornic` (or `neo4j` if you configured it) and `system`
- The default database should be marked as `default: true`
- If you see an error "Database 'neo4j' not found", see Troubleshooting section below

```cypher
-- Verify existing data is accessible in default database
-- (Assuming you have existing data - adjust query based on your dataset)
MATCH (n)
RETURN count(n) as node_count
```

**Expected Result**: 
- Should return the count of nodes from your existing dataset
- Confirms data was migrated to default database namespace

```cypher
-- Check that we're in the default database context
-- (In Cypher Shell, you can also use :USE nornic to explicitly switch)
MATCH (n)
RETURN labels(n) as labels, count(n) as count
LIMIT 10
```

**Expected Result**: 
- Should show your existing data's labels and counts
- Confirms default database contains migrated data

---

### Step 2: Create First Database

**Purpose**: Create a new database other than the default.

```cypher
-- Create first test database
CREATE DATABASE test_db_a
```

**Expected Result**: 
- Success message or empty result (command succeeded)

```cypher
-- Verify database was created
SHOW DATABASES
```

**Expected Result**: 
- Should now show `test_db_a` in the list
- Should show `nornic` (default), `system`, and `test_db_a`

---

### Step 3: Insert Data in First Database

**Purpose**: Add test data to the first database.

```cypher
-- Switch to first database
-- Note: In Cypher Shell, use :USE test_db_a
-- In drivers, specify database="test_db_a"
-- For this test, we'll use explicit database context

-- Create nodes in test_db_a
CREATE (alice:Person {name: "Alice", id: "a1", db: "test_db_a"})
CREATE (bob:Person {name: "Bob", id: "a2", db: "test_db_a"})
CREATE (company:Company {name: "Acme Corp", id: "a3", db: "test_db_a"})
CREATE (alice)-[:WORKS_FOR]->(company)
CREATE (bob)-[:WORKS_FOR]->(company)
RETURN alice, bob, company
```

**Expected Result**: 
- Should create 3 nodes and 2 relationships
- Should return the created nodes

**Note**: If using Cypher Shell, you need to switch databases first:
```cypher
:USE test_db_a
CREATE (alice:Person {name: "Alice", id: "a1", db: "test_db_a"})
CREATE (bob:Person {name: "Bob", id: "a2", db: "test_db_a"})
CREATE (company:Company {name: "Acme Corp", id: "a3", db: "test_db_a"})
CREATE (alice)-[:WORKS_FOR]->(company)
CREATE (bob)-[:WORKS_FOR]->(company)
RETURN alice, bob, company
```

---

### Step 4: Query First Database

**Purpose**: Verify data exists in the first database and can be queried.

```cypher
-- Query all nodes in test_db_a
:USE test_db_a
MATCH (n)
RETURN n.name as name, labels(n) as labels, n.db as db
ORDER BY n.name
```

**Expected Result**: 
- Should return 3 rows: Alice, Acme Corp, Bob
- All should have `db: "test_db_a"`

```cypher
-- Query relationships
:USE test_db_a
MATCH (p:Person)-[r:WORKS_FOR]->(c:Company)
RETURN p.name as person, c.name as company, type(r) as relationship
```

**Expected Result**: 
- Should return 2 rows: Alice -> Acme Corp, Bob -> Acme Corp

```cypher
-- Count nodes by label
:USE test_db_a
MATCH (n)
RETURN labels(n)[0] as label, count(n) as count
ORDER BY label
```

**Expected Result**: 
- Should return: Company: 1, Person: 2

---

### Step 5: Create Second Database

**Purpose**: Create another database to test isolation.

```cypher
-- Create second test database
CREATE DATABASE test_db_b
```

**Expected Result**: 
- Success message or empty result

```cypher
-- Verify both databases exist
SHOW DATABASES
```

**Expected Result**: 
- Should show `test_db_a`, `test_db_b`, `nornic`, and `system`

---

### Step 6: Insert Data in Second Database

**Purpose**: Add different test data to the second database.

```cypher
-- Switch to second database
-- Note: In Cypher Shell, use :USE test_db_b

-- Create nodes in test_db_b
:USE test_db_b
CREATE (charlie:Person {name: "Charlie", id: "b1", db: "test_db_b"})
CREATE (diana:Person {name: "Diana", id: "b2", db: "test_db_b"})
CREATE (order:Order {order_id: "ORD-001", amount: 1000, db: "test_db_b"})
CREATE (charlie)-[:PLACED]->(order)
CREATE (diana)-[:PLACED]->(order)
RETURN charlie, diana, order
```

**Expected Result**: 
- Should create 3 nodes and 2 relationships
- Should return the created nodes

**Note**: If using Cypher Shell:
```cypher
:USE test_db_b
CREATE (charlie:Person {name: "Charlie", id: "b1", db: "test_db_b"})
CREATE (diana:Person {name: "Diana", id: "b2", db: "test_db_b"})
CREATE (order:Order {order_id: "ORD-001", amount: 1000, db: "test_db_b"})
CREATE (charlie)-[:PLACED]->(order)
CREATE (diana)-[:PLACED]->(order)
RETURN charlie, diana, order
```

---

### Step 7: Query Second Database

**Purpose**: Verify data exists in the second database.

```cypher
-- Query all nodes in test_db_b
:USE test_db_b
MATCH (n)
RETURN n.name as name, n.order_id as order_id, labels(n) as labels, n.db as db
ORDER BY n.name
```

**Expected Result**: 
- Should return 3 rows: Charlie, Diana, ORD-001
- All should have `db: "test_db_b"`

```cypher
-- Query relationships
:USE test_db_b
MATCH (p:Person)-[r:PLACED]->(o:Order)
RETURN p.name as person, o.order_id as order, type(r) as relationship
```

**Expected Result**: 
- Should return 2 rows: Charlie -> ORD-001, Diana -> ORD-001

```cypher
-- Count nodes by label
:USE test_db_b
MATCH (n)
RETURN labels(n)[0] as label, count(n) as count
ORDER BY label
```

**Expected Result**: 
- Should return: Order: 1, Person: 2

---

### Step 8: Verify Isolation

**Purpose**: Confirm that databases are isolated and don't see each other's data.

```cypher
-- Switch back to test_db_a
-- Note: In Cypher Shell, use :USE test_db_a

-- Query test_db_a - should NOT see test_db_b data
MATCH (n)
RETURN n.name as name, n.order_id as order_id, labels(n) as labels, n.db as db
ORDER BY n.name
```

**Expected Result**: 
- Should return only: Alice, Acme Corp, Bob
- Should NOT return Charlie, Diana, or ORD-001
- All should have `db: "test_db_a"`

```cypher
-- Verify no Order nodes in test_db_a
MATCH (o:Order)
RETURN count(o) as order_count
```

**Expected Result**: 
- Should return: `order_count: 0`

```cypher
-- Switch to test_db_b
-- Note: In Cypher Shell, use :USE test_db_b

-- Query test_db_b - should NOT see test_db_a data
MATCH (n)
RETURN n.name as name, labels(n) as labels, n.db as db
ORDER BY n.name
```

**Expected Result**: 
- Should return only: Charlie, Diana, ORD-001
- Should NOT return Alice, Bob, or Acme Corp
- All should have `db: "test_db_b"`

```cypher
-- Verify no Company nodes in test_db_b
MATCH (c:Company)
RETURN count(c) as company_count
```

**Expected Result**: 
- Should return: `company_count: 0`

```cypher
-- Switch to default database (nornic)
-- Note: In Cypher Shell, use :USE nornic

-- Verify default database still has original data
MATCH (n)
RETURN count(n) as node_count
```

**Expected Result**: 
- Should return the count of your original migrated data
- Should NOT include data from test_db_a or test_db_b

```cypher
-- Verify default database doesn't see test databases' data
MATCH (n)
WHERE n.db IN ["test_db_a", "test_db_b"]
RETURN count(n) as test_db_nodes
```

**Expected Result**: 
- Should return: `test_db_nodes: 0`

---

### Step 9: Create Composite Database

**Purpose**: Create a composite database that spans both test databases.

```cypher
-- Create composite database
CREATE COMPOSITE DATABASE test_composite
  ALIAS db_a FOR DATABASE test_db_a
  ALIAS db_b FOR DATABASE test_db_b
```

**Expected Result**: 
- Success message or empty result

```cypher
-- Verify composite database was created
SHOW COMPOSITE DATABASES
```

**Expected Result**: 
- Should show `test_composite` in the list

```cypher
-- Show constituents of composite database
SHOW CONSTITUENTS FOR COMPOSITE DATABASE test_composite
```

**Expected Result**: 
- Should show 2 constituents: `db_a` -> `test_db_a`, `db_b` -> `test_db_b`

---

### Step 10: Query Composite Database

**Purpose**: Verify that composite database provides unified view across both databases.

```cypher
-- Switch to composite database
-- Note: In Cypher Shell, use :USE test_composite

-- Query all Person nodes across both databases
MATCH (p:Person)
RETURN p.name as name, p.db as db, labels(p) as labels
ORDER BY p.name
```

**Expected Result**: 
- Should return 4 rows: Alice, Bob, Charlie, Diana
- Should show `db: "test_db_a"` for Alice and Bob
- Should show `db: "test_db_b"` for Charlie and Diana

```cypher
-- Count all nodes across both databases
MATCH (n)
RETURN count(n) as total_nodes
```

**Expected Result**: 
- Should return: `total_nodes: 6` (3 from test_db_a + 3 from test_db_b)

```cypher
-- Count nodes by label across both databases
MATCH (n)
RETURN labels(n)[0] as label, count(n) as count
ORDER BY label
```

**Expected Result**: 
- Should return: Company: 1, Order: 1, Person: 4

```cypher
-- Query relationships across both databases
MATCH (p:Person)-[r]->(target)
RETURN p.name as person, type(r) as relationship, labels(target)[0] as target_type, target.name as target_name, target.order_id as order_id
ORDER BY p.name
```

**Expected Result**: 
- Should return 4 rows:
  - Alice -> WORKS_FOR -> Company (Acme Corp)
  - Bob -> WORKS_FOR -> Company (Acme Corp)
  - Charlie -> PLACED -> Order (ORD-001)
  - Diana -> PLACED -> Order (ORD-001)

```cypher
-- Test write operation (should route to appropriate constituent)
CREATE (new_person:Person {name: "Eve", id: "composite-1", db: "test_composite"})
RETURN new_person
```

**Expected Result**: 
- Should create a new Person node
- Node will be routed to one of the constituent databases

```cypher
-- Verify new node exists in composite view
MATCH (p:Person {name: "Eve"})
RETURN p.name as name, p.db as db
```

**Expected Result**: 
- Should return: Eve with `db: "test_composite"` or the actual constituent database name

```cypher
-- Verify schema merging (should show indexes/constraints from both constituents)
SHOW INDEXES
```

**Expected Result**: 
- Should show indexes from both test_db_a and test_db_b (if any were created)

```cypher
-- Verify schema merging for constraints
SHOW CONSTRAINTS
```

**Expected Result**: 
- Should show constraints from both test_db_a and test_db_b (if any were created)

---

### Step 11: Verify Composite Database Isolation

**Purpose**: Confirm that composite database doesn't affect individual databases.

```cypher
-- Switch back to test_db_a
-- Note: In Cypher Shell, use :USE test_db_a

-- Verify test_db_a still has its original data
MATCH (n)
RETURN count(n) as node_count
```

**Expected Result**: 
- Should return: `node_count: 3` (Alice, Bob, Acme Corp)
- Should NOT include data from test_db_b

```cypher
-- Switch to test_db_b
-- Note: In Cypher Shell, use :USE test_db_b

-- Verify test_db_b still has its original data
MATCH (n)
RETURN count(n) as node_count
```

**Expected Result**: 
- Should return: `node_count: 3` (Charlie, Diana, ORD-001)
- Should NOT include data from test_db_a

---

### Step 12: Cleanup - Drop Composite Database

**Purpose**: Remove composite database first (before dropping constituent databases).

```cypher
-- Drop composite database
DROP COMPOSITE DATABASE test_composite
```

**Expected Result**: 
- Success message or empty result

```cypher
-- Verify composite database was dropped
SHOW COMPOSITE DATABASES
```

**Expected Result**: 
- Should return empty result or not show `test_composite`

```cypher
-- Verify constituent databases still exist
SHOW DATABASES
```

**Expected Result**: 
- Should still show `test_db_a` and `test_db_b`
- Confirms dropping composite doesn't affect constituents

---

### Step 13: Cleanup - Drop Test Databases

**Purpose**: Remove test databases in proper order.

```cypher
-- Drop first test database
DROP DATABASE test_db_a
```

**Expected Result**: 
- Success message or empty result

```cypher
-- Verify first database was dropped
SHOW DATABASES
```

**Expected Result**: 
- Should NOT show `test_db_a`
- Should still show `test_db_b`, `nornic`, and `system`

```cypher
-- Drop second test database
DROP DATABASE test_db_b
```

**Expected Result**: 
- Success message or empty result

```cypher
-- Verify second database was dropped
SHOW DATABASES
```

**Expected Result**: 
- Should NOT show `test_db_b`
- Should only show `nornic` (default) and `system`

```cypher
-- Verify default database still has original data
MATCH (n)
RETURN count(n) as node_count
```

**Expected Result**: 
- Should return the count of your original migrated data
- Confirms cleanup didn't affect default database

---

## Test Summary

### ✅ What This Test Verifies

1. **In-Place Upgrade**: Existing data is accessible in default database after upgrade
2. **Database Creation**: Can create multiple databases
3. **Data Isolation**: Databases don't see each other's data
4. **Composite Databases**: Can create virtual database spanning multiple databases
5. **Cross-Database Queries**: Composite database provides unified view
6. **Schema Merging**: Composite database merges schemas from constituents
7. **Write Routing**: Writes to composite database are routed correctly
8. **Proper Cleanup**: Can drop composite and constituent databases in correct order

### 🔍 Key Observations

- **Isolation**: Each database maintains complete data isolation
- **Composite View**: Composite database provides transparent access to all constituents
- **No Data Loss**: Dropping composite database doesn't affect constituent databases
- **Default Database**: Original migrated data remains intact throughout all operations

### ⚠️ Important Notes

1. **Database Switching**: 
   - In Cypher Shell, use `:USE database_name`
   - In drivers, specify `database="database_name"` in connection/session
   - In HTTP API, use `/db/database_name/tx/commit`

2. **Cleanup Order**: 
   - Always drop composite databases BEFORE dropping constituent databases
   - Dropping a constituent database that's part of a composite will fail

3. **Default Database**: 
   - The default database (`nornic`) is protected and cannot be dropped
   - The `system` database is internal and cannot be dropped

4. **Data Persistence**: 
   - All data persists across server restarts
   - Database metadata is stored in the `system` database

---

## Troubleshooting

### Issue: "Database 'neo4j' not found"

**Cause**: Your client is trying to connect to `"neo4j"` (Neo4j's default), but NornicDB uses `"nornic"` as the default database name.

**Solutions**:

1. **Configure NornicDB to use `"neo4j"` as default** (Recommended):
   
   **Option A: Config File** (`nornicdb.yaml`):
   ```yaml
   database:
     default_database: "neo4j"
   ```
   Then restart NornicDB.

   **Option B: Environment Variable**:
   ```bash
   export NORNICDB_DEFAULT_DATABASE=neo4j
   # Or Neo4j-compatible:
   export NEO4J_dbms_default__database=neo4j
   ```
   Then restart NornicDB.

   **Option C: Command Line**:
   ```bash
   ./nornicdb serve --default-database neo4j
   ```

2. **Switch to `"nornic"` database in your client**:
   
   **In Web UI**:
   - Look for a database selector/dropdown
   - Select `"nornic"` from the list
   - Or manually change the URL from `/db/neo4j/` to `/db/nornic/`
   
   **In Cypher Shell**:
   ```cypher
   :USE nornic
   ```
   
   **In Drivers**:
   ```python
   # Python
   driver = GraphDatabase.driver(
       "bolt://localhost:7687",
       database="nornic"  # Use nornic instead of neo4j
   )
   ```

3. **Verify available databases**:
   ```cypher
   SHOW DATABASES
   ```
   This will show you which databases exist. The default database will be marked with `default: true`.

### Issue: "database not found"
- **Solution**: Verify database exists with `SHOW DATABASES`
- **Solution**: Check you're using the correct database name (case-sensitive)
- **Solution**: If you see `"nornic"` in the list but your client is looking for `"neo4j"`, see the issue above

### Issue: "cannot drop database: database is in use"
- **Solution**: Ensure no active connections are using the database
- **Solution**: Close all sessions/connections to the database

### Issue: "cannot drop database: database is a constituent"
- **Solution**: Drop the composite database first, then drop the constituent

### Issue: "composite database not found"
- **Solution**: Verify composite database exists with `SHOW COMPOSITE DATABASES`
- **Solution**: Check spelling and case sensitivity

### Issue: Query returns unexpected results
- **Solution**: Verify you're connected to the correct database (`:USE database_name`)
- **Solution**: Check that data was created in the expected database
- **Solution**: Verify isolation by querying each database individually
- **Solution**: If using Web UI, check the database selector shows the correct database

---

## Next Steps

After completing this test:

1. **Performance Testing**: Test with larger datasets to verify performance
2. **Concurrent Access**: Test multiple clients accessing different databases simultaneously
3. **Schema Operations**: Test creating indexes/constraints in individual databases and verify merging
4. **Error Handling**: Test error scenarios (dropping non-existent databases, etc.)
5. **Resource Limits**: Test per-database resource limits if configured

---

## Related Documentation

- [Multi-Database User Guide](../user-guides/multi-database.md)
- [Multi-Database Architecture](../architecture/MULTI_DB_IMPLEMENTATION_SPEC.md)
- [Composite Databases Guide](../user-guides/multi-database.md#composite-databases)
- [Future Features](../architecture/MULTI_DB_FUTURE_FEATURES.md)

