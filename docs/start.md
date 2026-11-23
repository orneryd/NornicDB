# Quick start

Run a local database, write a graph, and read it back. This example uses the CPU image and binds the database ports to your computer's loopback interface.

## 1. Start the database

The image defaults to unauthenticated testing and logs a warning when it starts in that mode. Set `NORNICDB_NO_AUTH=false` for this authenticated setup. Supplying credentials alone does not change the testing default.

Create a password and a stable JWT signing secret. Put a TLS certificate for `localhost` and its private key in `./tls/public.crt` and `./tls/private.key`. The container listens on its own network interface, so HTTPS and Bolt TLS are required even when Docker publishes the ports only on your computer's loopback interface. Use a certificate trusted by your browser and Bolt client. Keep the private key and credentials outside source control.

```bash
export NORNICDB_PASSWORD='replace-with-a-long-random-password'
export NORNICDB_JWT_SECRET='replace-with-at-least-32-random-characters'

docker run -d --name nornicdb \
  -p 127.0.0.1:7473:7473 -p 127.0.0.1:7687:7687 \
  -v nornicdb-data:/data \
  --mount "type=bind,source=${PWD}/tls,target=/tls,readonly" \
  -e NORNICDB_NO_AUTH=false \
  -e NORNICDB_AUTH="admin:${NORNICDB_PASSWORD}" \
  -e NORNICDB_AUTH_JWT_SECRET="${NORNICDB_JWT_SECRET}" \
  -e NORNICDB_CORS_ENABLED=false \
  -e NORNICDB_HTTPS_ENABLED=true \
  -e NORNICDB_HTTP_TLS_CERT=/tls/public.crt \
  -e NORNICDB_HTTP_TLS_KEY=/tls/private.key \
  -e NORNICDB_BOLT_TLS_ENABLED=true \
  -e NORNICDB_BOLT_TLS_REQUIRE=true \
  -e NORNICDB_TLS_DIR=/tls \
  timothyswt/nornicdb-amd64-cpu:latest
```

The named volume retains data when the container is removed. For GPU images and platform-specific commands, see [image selection](getting-started/image-quick-reference.md).

## 2. Check the server

```bash
curl --fail https://localhost:7473/health
```

Open [the database browser](https://localhost:7473) and sign in with `admin` and the password you supplied. Neo4j-compatible Bolt clients use `bolt+s://localhost:7687`.

## 3. Write and query a graph

```bash
curl --fail -u "admin:${NORNICDB_PASSWORD}" \
  -H 'Content-Type: application/json' \
  https://localhost:7473/db/nornic/tx/commit \
  -d '{"statements":[{"statement":"MERGE (a:Person {name: \"Alice\"}) MERGE (b:Person {name: \"Bob\"}) MERGE (a)-[:KNOWS]->(b) RETURN a.name, b.name"}]}'
```

The response contains `results` and an `errors` array. A successful HTTP response alone does not establish query success: check that `errors` is empty.

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN a.name, b.name
```

Expected names: `Alice` and `Bob`. Repeating the `MERGE` example matches the same nodes and relationship.

To save the project for later, [star NornicDB on GitHub](https://github.com/orneryd/NornicDB).

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
