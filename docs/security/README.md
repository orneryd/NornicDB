# Security Guide

This section is the canonical source for runtime security controls.

## Start Here

- HTTP hardening controls (CSRF/SSRF/XSS): [http-security.md](http-security.md)
- Query-cache threat model: [query-cache-security.md](query-cache-security.md)
- LLM/plugin safety model: [llm-ast-security.md](llm-ast-security.md)
- Per-database authorization: [per-database-rbac.md](per-database-rbac.md)
- Entitlements reference: [entitlements.md](entitlements.md)

## Security by Domain

- Transport/runtime hardening: [http-security.md](http-security.md)
- Authentication and access control: [per-database-rbac.md](per-database-rbac.md)
- Permission mapping: [entitlements.md](entitlements.md)
- Cluster auth and node security: [../operations/cluster-security.md](../operations/cluster-security.md)
- Regulatory overlays: [../compliance/README.md](../compliance/README.md)

## Common Questions

- "How are HTTP endpoints protected by default?" → [http-security.md](http-security.md)
- "What permissions are required for endpoint X?" → [entitlements.md](entitlements.md)
- "How do I recover from RBAC lockout?" → [per-database-rbac.md](per-database-rbac.md)
- "Which compliance docs map to these controls?" → [../compliance/README.md](../compliance/README.md)

## Incident Routing

- Auth failures (`401`/`403`): [../operations/troubleshooting.md#authentication-issues](../operations/troubleshooting.md#authentication-issues)
- Misconfigured security env/config: [../operations/configuration.md](../operations/configuration.md)
- Security symptom triage map: [../issues-index.md](../issues-index.md)
