# Quick start

Run a local database, write a graph, and read it back. This example uses the CPU image and binds the database ports to your computer's loopback interface.

## 1. Start the database

Create a password and a stable JWT signing secret. Keep both outside source control.

```bash
export NORNICDB_PASSWORD='replace-with-a-long-random-password'
export NORNICDB_JWT_SECRET='replace-with-at-least-32-random-characters'

docker run -d --name nornicdb \
  -p 127.0.0.1:7474:7474 -p 127.0.0.1:7687:7687 \
  -v nornicdb-data:/data \
  -e NORNICDB_AUTH="admin:${NORNICDB_PASSWORD}" \
  -e NORNICDB_AUTH_JWT_SECRET="${NORNICDB_JWT_SECRET}" \
  timothyswt/nornicdb-amd64-cpu:latest
```

The named volume retains data when the container is removed. For GPU images and platform-specific commands, see [image selection](getting-started/image-quick-reference.md).

## 2. Check the server

```bash
curl --fail http://localhost:7474/health
```

Open [the database browser](http://localhost:7474) and sign in with `admin` and the password you supplied. HTTP uses port `7474`; Neo4j-compatible Bolt clients use `bolt://localhost:7687`.

## 3. Write and query a graph

```bash
curl --fail -u "admin:${NORNICDB_PASSWORD}" \
  -H 'Content-Type: application/json' \
  http://localhost:7474/db/nornic/tx/commit \
  -d '{"statements":[{"statement":"MERGE (a:Person {name: \"Alice\"}) MERGE (b:Person {name: \"Bob\"}) MERGE (a)-[:KNOWS]->(b) RETURN a.name, b.name"}]}'
```

The response contains `results` and an `errors` array. A successful HTTP response alone does not establish query success: check that `errors` is empty.

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN a.name, b.name
```

Expected names: `Alice` and `Bob`. Repeating the `MERGE` example matches the same nodes and relationship.

## 4. Choose your next task

- [Write application queries](user-guides/cypher-queries.md)
- [Connect a Neo4j-compatible driver](api-reference/bolt-protocol.md)
- [Configure hybrid search](user-guides/hybrid-search.md)
- [Inspect code and related documentation](explore)
- [Configure authentication and deployment](operations/configuration.md)

## Stop or restart

```bash
docker stop nornicdb
docker start nornicdb
```

The volume is retained. Use [backup and restore](operations/backup-restore.md) before changing or deleting persistent data.
