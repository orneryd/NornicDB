# NornicDB Documentation Hub

This is the primary navigation page for NornicDB docs.

If you know your goal, start in **"Find by task"**. If you are debugging, start in **"Find by issue"**.

## Start Here

- New user: [Getting Started](getting-started/README.md)
- Migrating from Neo4j: [Neo4j Migration](neo4j-migration/README.md)
- Building apps and queries: [User Guides](user-guides/README.md)
- Driving NornicDB from an agent: [Agent Skills](skills/README.md)
- Looking for endpoint/function specs: [API Reference](api-reference/README.md)
- Running in production: [Operations](operations/README.md)
- Security/compliance review: [Security](security/README.md), [Compliance](compliance/README.md)
- Performance optimization: [Performance](performance/README.md), [Hot-Path Cypher Cookbook](performance/hot-path-query-cookbook.md)

## Find by Task

- Install and run: [Quick Start](getting-started/quick-start.md)
- Run first queries: [First Queries](getting-started/first-queries.md)
- Learn Cypher usage patterns: [Cypher Queries](user-guides/cypher-queries.md)
- Manage NornicDB-specific schema constraints and contracts: [Managing Constraints](user-guides/managing-constraints.md)
- Use vector/hybrid search: [Vector Search](user-guides/vector-search.md), [Hybrid Search](user-guides/hybrid-search.md)
- Configure retention enforcement: [Retention Policies](user-guides/retention-policies.md)
- Prepare for audit review of background workers and version history: [Background Workers, MVCC, and Audit Evidence](compliance/background-workers-mvcc-audit-guide.md)
- Configure deployment/runtime: [Configuration](operations/configuration.md), [Environment Variables](operations/environment-variables.md)
- Deploy with Docker: [Docker Deployment](getting-started/docker-deployment.md), [Operations Docker](operations/docker.md)
- Observe/operate production: [Monitoring](operations/monitoring.md), [Backup & Restore](operations/backup-restore.md)
- Offline bulk import: [nornicdb-admin](operations/admin-tool.md)
- Migrate from Neo4j drivers/schemas: [Migration Guide](neo4j-migration/README.md)

## Find by Issue

- Symptom-driven index: [Issue Index](issues-index.md)
- Ops troubleshooting: [Operations Troubleshooting](operations/troubleshooting.md)

## Find by Role

### Application Developers

- [Getting Started](getting-started/README.md)
- [User Guides](user-guides/README.md)
- [API Reference](api-reference/README.md)

### Platform/SRE Teams

- [Operations](operations/README.md)
- [Security](security/README.md)
- [Compliance](compliance/README.md)

### Auditors And Reviewers

- [Compliance](compliance/README.md)
- [Background Workers, MVCC, and Audit Evidence](compliance/background-workers-mvcc-audit-guide.md)
- [Audit Logging](compliance/audit-logging.md)

### Contributors

- [Development](development/README.md)
- [Architecture](architecture/README.md)
- [Advanced](advanced/README.md)

## Documentation Map

- [Getting Started](getting-started/README.md)
- [User Guides](user-guides/README.md)
- [Agent Skills](skills/README.md)
- [API Reference](api-reference/README.md)
- [Features](features/README.md)
- [Operations](operations/README.md)
- [Security](security/README.md)
- [Compliance](compliance/README.md)
- [Performance](performance/README.md)
- [Architecture](architecture/README.md)
- [Development](development/README.md)
- [AI Agents](ai-agents/README.md)
- [Neo4j Migration](neo4j-migration/README.md)
- [Packaging](packaging/README.md)

## Canonical Source Policy

To avoid drift and duplicated guidance:

- API behavior is canonical in [API Reference](api-reference/README.md).
- Operational runbooks are canonical in [Operations](operations/README.md).
- Security controls are canonical in [Security](security/README.md).
- Compliance interpretation is canonical in [Compliance](compliance/README.md).
- How-to walkthroughs are canonical in [User Guides](user-guides/README.md).

Section READMEs should summarize and link to canonical pages instead of duplicating full procedures.
