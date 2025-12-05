# ⚠️ Breaking Change: Storage Encoding Migration (Pre-v1.0)

**Effective:** December 2024  
**Affects:** All users upgrading from builds prior to December 5, 2024

## What Changed

NornicDB has switched from **JSON encoding** to **Gob encoding** for internal storage. This change provides:
- ✅ **Type preservation** - `int64` stays `int64` (JSON converted everything to `float64`)
- ✅ **Neo4j compatibility** - Numeric types now match Neo4j behavior exactly
- ✅ **Better performance** - Gob is faster than JSON for Go types

## Impact

**Your existing data will NOT be readable** after upgrading. The new code expects Gob-encoded data but your database contains JSON-encoded data.

### Symptoms of This Issue
- `MATCH (n) RETURN n` returns 0 rows despite showing node count > 0
- Silent deserialization failures
- Empty query results

## Migration Steps

### Option 1: Fresh Start (Recommended for Development)

If you don't need to preserve your data:

```bash
# Stop NornicDB
docker-compose down nornicdb

# Clear the data directory (check your docker-compose.yml for the correct path)
rm -rf ./data/nornicdb/*
# or
rm -rf ./data/your-volume-name/*

# Start fresh
docker-compose up -d nornicdb
```

### Option 2: Export and Re-Import (For Production Data)

**Before upgrading**, export your data using the OLD version:

#### Step 1: Export with Old Version
```bash
# Using the OLD NornicDB version, run:
curl -X POST http://localhost:7474/db/neo4j/tx/commit \
  -H "Content-Type: application/json" \
  -d '{
    "statements": [{
      "statement": "MATCH (n) RETURN n"
    }]
  }' > nodes_backup.json

curl -X POST http://localhost:7474/db/neo4j/tx/commit \
  -H "Content-Type: application/json" \
  -d '{
    "statements": [{
      "statement": "MATCH ()-[r]->() RETURN r"
    }]
  }' > edges_backup.json
```

Or use the Cypher shell:
```cypher
// Export all nodes with labels and properties
CALL apoc.export.json.all("backup.json", {useTypes: true})
```

#### Step 2: Upgrade and Clear Data
```bash
# Pull/build new version
docker-compose build nornicdb

# Clear old data
rm -rf ./data/nornicdb/*

# Start new version
docker-compose up -d nornicdb
```

#### Step 3: Re-Import Data

Using the backup JSON, create import statements:

```bash
# Simple Python script to convert backup to CREATE statements
python3 << 'EOF'
import json

with open('nodes_backup.json') as f:
    data = json.load(f)
    
for result in data.get('results', []):
    for row in result.get('data', []):
        node = row['row'][0]
        labels = ':'.join(node.get('labels', []))
        props = json.dumps({k: v for k, v in node.items() if k not in ['labels', '_nodeId', 'id', 'embedding']})
        print(f"CREATE (n:{labels} {props});")
EOF
```

Then run the generated CREATE statements against the new NornicDB.

### Option 3: Neo4j Migration Script

If you have significant data, use our migration script:

```bash
cd nornicdb/scripts
go run migrate_neo4j_to_nornic.go \
  --source "bolt://localhost:7687" \
  --target "bolt://localhost:7688" \
  --batch-size 1000
```

## Verifying Migration Success

After migration, verify your data:

```cypher
// Check node count
MATCH (n) RETURN count(n)

// Check a sample of nodes
MATCH (n) RETURN n LIMIT 10

// Verify numeric types are preserved
MATCH (n) WHERE n.age IS NOT NULL RETURN n.age, apoc.meta.type(n.age)
```

## Questions?

- Open an issue on GitHub
- Check the [troubleshooting guide](./troubleshooting/)

---

**Note:** This is a pre-v1.0 breaking change. After v1.0 release, we will maintain backward compatibility and provide automatic migration tools.
