# Develop with NornicDB

Use NornicDB as a managed Go library, integrate through a server protocol, or extend the existing storage and query APIs.

## Build an application

- [Embed in Go](embedding.md): dependency selection, opening/querying/closing a DB, ownership, transactions, and process-wide settings.
- [Canonical API reference](../api-reference/README.md): supported Go interfaces and protocol contracts.
- [Transactions](../user-guides/transactions.md): visibility, conflicts, commit outcomes, and driver examples.
- [Multiple databases](../user-guides/multi-database.md): namespace and routing boundaries.
- [Plugins](../features/plugin-system.md) and [APOC capabilities](../features/apoc-procedure-capabilities.md): supported extension points.

## Integrate below the DB API

Lower-level callers own more of the lifecycle. Read the applicable contract before reaching into storage or constructing an executor:

| Integration | Contract |
| --- | --- |
| Query classification/preparation | [Preparation context](cypher-preparation-context-api.md), [source stages](cypher-source-stage-api.md), [statement API migration](../api-reference/README.md#embedded-go-query-readers-v144-migration). |
| Stored entities | [Bulk reads](storage-bulk-read-go-api.md), [read delivery](storage-read-delivery.md), [transaction deletion](transaction-node-deletion-api.md). |
| Indexes | [Full-text Go API](fulltext-index-api.md), [index publication](index-publication.md), [cancellation](index-read-cancellation.md). |
| Catalogs/functions/plugins | [Catalog contexts](catalog-context-api.md), [function context](function-context-go-api.md), [plugin metadata](plugin-host-metadata-api.md). |
| Background and accepted work | [Work lifetime](work-lifetime.md), [snapshot/replay completion](replay-snapshot-completion.md), [replicated commits](replicated-entity-commits.md). |

## Operate an embedded store

Read [durability](../operations/durability.md), [backup/restore](../operations/backup-restore.md), [memory accounting](../operations/memory-management.md), and [low-memory mode](../operations/low-memory-mode.md). A library call does not install HTTP authorization, container limits, or an application shutdown policy.

## Contribute to NornicDB

The [contribution guide](contributing.md) covers repository setup, branch policy, validation, and PR evidence. Use [local build setup](setup.md), [coding conventions](code-style.md), and [testing](testing.md) for contributor details.
