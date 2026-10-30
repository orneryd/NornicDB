# NornicDB Issue Index

Use this page when you know a symptom, but not the right document yet.

## Quick Triage

- Is the server up? `curl http://localhost:7474/health`
- Can you authenticate? `POST /auth/token`
- Is the issue API-specific? Check the [API Reference](api-reference/README.md).
- Is the issue operational? Check the [Operations Guide](operations/README.md).

## By Symptom

### Startup & Connectivity

- Server won't start / exits immediately
    - [Troubleshooting → Server won't start (corruption after crash)](operations/troubleshooting.md#server-wont-start-until-data-is-deleted-corruption-after-crash)
    - [Deployment](operations/deployment.md)
- Cannot connect to HTTP/Bolt
    - [Troubleshooting → Cannot connect to server](operations/troubleshooting.md#cannot-connect-to-server)
    - [Quick Start](getting-started/quick-start.md)

### Auth & Access

- `401 Unauthorized`
    - [Troubleshooting → 401 Unauthorized](operations/troubleshooting.md#401-unauthorized)
    - [Security Guide](security/README.md)
- `403 Forbidden` / role mismatch
    - [Troubleshooting → 403 Forbidden](operations/troubleshooting.md#403-forbidden)
    - [Per-Database RBAC](security/per-database-rbac.md)
    - [Entitlements](security/entitlements.md)

### Query & API Errors

- Cypher behavior/function confusion
    - [Cypher Queries](user-guides/cypher-queries.md)
    - [Cypher Functions Reference](api-reference/cypher-functions/README.md)
- HTTP endpoint mismatch
    - [OpenAPI Reference](api-reference/openapi.md)
- Neo4j compatibility question
    - [Feature Parity](neo4j-migration/feature-parity.md)
    - [Cypher Compatibility](neo4j-migration/cypher-compatibility.md)

### Performance & Capacity

- Slow queries
    - [Troubleshooting → Slow queries](operations/troubleshooting.md#slow-queries)
    - [Hot-Path Cypher Cookbook](performance/hot-path-query-cookbook.md)
    - [HTTP Optimization Options](performance/http-optimization-options.md)
- High memory usage / OOM
    - [Troubleshooting → High memory usage](operations/troubleshooting.md#high-memory-usage)
    - [Low-Memory Mode](operations/low-memory-mode.md)
- High CPU usage
    - [Troubleshooting → High CPU usage](operations/troubleshooting.md#high-cpu-usage)
    - [Performance Overview](performance/README.md)

### Data Integrity & Recovery

- Data not persisting
    - [Troubleshooting → Data not persisting](operations/troubleshooting.md#data-not-persisting)
    - [Backup & Restore](operations/backup-restore.md)
- Corruption / recovery after crash
    - [Troubleshooting → Server won't start (corruption after crash)](operations/troubleshooting.md#server-wont-start-until-data-is-deleted-corruption-after-crash)
    - [WAL Compaction](operations/wal-compaction.md)

### Search, Embeddings, and AI

- Embeddings not generating
    - [Troubleshooting → Embeddings not generating](operations/troubleshooting.md#embeddings-not-generating)
    - [Vector Embeddings](features/vector-embeddings.md)
- Vector/hybrid search quality issues
    - [Vector Search](user-guides/vector-search.md)
    - [Hybrid Search](user-guides/hybrid-search.md)
    - [Search Performance](performance/searching.md)
- Agent/MCP integration issues
    - [AI Agents Overview](ai-agents/README.md)
    - [MCP Integration](features/mcp-integration.md)
    - [Heimdall MCP Tools](user-guides/heimdall-mcp-tools.md)

### Security & Compliance

- HTTP hardening / SSRF / CSRF / XSS
    - [HTTP Security](security/http-security.md)
- Encryption/audit/RBAC requirements
    - [Compliance Overview](compliance/README.md)
    - [Encryption](compliance/encryption.md)
    - [Audit Logging](compliance/audit-logging.md)
    - [RBAC](compliance/rbac.md)

## If You Still Can't Find It

- Browse by role/task in the [Documentation Hub](README.md).
- Use the section indexes:
    - [User Guides](user-guides/README.md)
    - [Operations](operations/README.md)
    - [API Reference](api-reference/README.md)
    - [Security](security/README.md)
