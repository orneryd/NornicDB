# Fine-grained access control

FGAC is opt-in. With it disabled, NornicDB uses the existing role entitlements,
per-database allowlist, and database privileges. With it enabled, authenticated
users keep their role memberships and the runtime evaluates the privileges and
fine-grained entitlements assigned to those roles. A matching deny takes
precedence over grants across roles.

## Enable FGAC

In the server configuration, set:

```yaml
auth:
  fgac_enabled: true
```

The equivalent environment setting is `NORNICDB_FGAC_ENABLED=true`. Restart the
server to change modes. FGAC requires authentication and a writable system role
store. On first activation with an empty catalog, NornicDB grants the built-in
administrator explicit control of the registered operations. The automatic
`PUBLIC` membership grants access to the user's home database and execution
access to procedures and functions. Each procedure still enforces the access
required by its underlying operation. These public privileges can be revoked.

Before enabling FGAC on an existing installation, open **Database Access** to
review policies and map existing role memberships to the new entitlements.
The old role privileges are saved independently. A staged replacement must
preserve an explicit policy-management grant for the installation administrator
before activation. To recover access through existing RBAC credentials, start
with FGAC disabled, repair the staged catalog, and reenable it.

## Configure graph privileges over Bolt

Run security administration statements on the `system` database. In this
example, the application database is `nornic`:

```cypher
CREATE ROLE article_reader IF NOT EXISTS;
GRANT ACCESS ON DATABASE nornic TO article_reader;
GRANT TRAVERSE ON GRAPH nornic NODES Article TO article_reader;
GRANT READ {title, summary} ON GRAPH nornic NODES Article TO article_reader;
GRANT ROLE article_reader TO alice;
SHOW ROLE article_reader PRIVILEGES AS COMMANDS;
```

Alice can match `Article` nodes. Cypher sees unreadable properties as absent,
including while evaluating predicates, projection, and aggregation. A
relationship is visible only when its traversal and both endpoints' traversal
are allowed. A write that fails authorization aborts its Cypher transaction.

To select rows by stored data, use a property rule:

```cypher
GRANT MATCH {title} ON GRAPH nornic
FOR (a:Article) WHERE a.published = true TO public_article_reader;
```

The rule evaluates `published` against the stored entity. The grant exposes
`title` and traversal. Inspect the current account's effective NornicDB rules
at `GET /auth/fgac/effective`. Use `SHOW USER PRIVILEGES` to inspect its
Cypher-managed privileges.

## Define reusable entitlements

With FGAC enabled, open **Security → Database Access**. Select a role or choose
**New role**, then open **Entitlements** to review the role's current access.
Assign an existing definition or choose **Create and assign**.
Choose an action, decide whether it grants or denies access, and set its
database, entity type, entity handle, or property scope as applicable. Review
the draft, then choose **Save changes**. The **Members** view shows current
members and supports adding or removing users. The entitlement library lets
you maintain a definition reused by several roles.

The editor offers individual actions for managed embedding, Heimdall, and
knowledge policy operations. Feature actions and access to the graph used by
the feature are independent grants. The existing Database Access layout is
shown when FGAC is disabled so administrators can continue editing RBAC.

Caller-initiated embedding retries and regeneration use the caller's current
operation and graph grants. The worker checks access to the input fields
before it contacts the provider and checks access again before publishing the
vector. An interrupted caller attempt is parked after restart and can be
requeued by an authorized retry. Existing managed generation continues to
use the database's configured service settings.

Administrators can retrieve the draft and revision through
`GET /auth/fgac/catalog` and replace the reusable definitions and assignments
through `PUT /auth/fgac/catalog`. The write accepts the returned revision and the
updated catalog. A stale revision returns `409`; a successful write returns
`204`. Cypher-managed privileges are edited using Cypher privilege commands.

For example, a catalog definition can grant a single property on a single node:

```json
{
  "id": "approved-article-title",
  "action": "graph.node.property.read",
  "scope": {
    "kind": "node",
    "databaseId": "<stable database ID>",
    "entityIds": ["<stable entity handle>"],
    "properties": ["title"]
  }
}
```

Assign that definition's ID to one or more role names in the catalog's
`assignments` object. Add a corresponding traversal grant for the same node,
as well as database access. If present, entity type, entity handle, property,
and predicate selectors all apply. An omitted selector covers that dimension.
Set `"deny": true` to define an explicit denial.

An administrator can resolve a Cypher `elementId()` using
`GET /auth/fgac/entity?element_id=<URL-encoded element ID>` or the lookup in the
editor. Use the returned database ID and entity handle in a definition. An
ordinary property update preserves the handle. Deleting and recreating the
database or entity creates a new identity.

## Data paths and compatibility

HTTP, Bolt, GraphQL, MCP, and Qdrant operations bind to the principal's active
policy revision. Policy changes invalidate retained search pages and Bolt
results. Search ranking uses a corpus assembled from visible entities and
properties. An FGAC request against a remote composite constituent requires a
trusted policy-aware execution path, so local FGAC refuses remote graph
execution. Scoped policies still apply on local constituents.

`database.cdc.read` permits access to a database's retained change feed,
including the historical values it records. Grant it to roles intended to read
that entire feed. History queries and REST graph snapshots instead project
entities and properties using the caller's policy.

The [Enterprise comparison harness](../../testing/cypher/fgac/README.md) checks
documented Bolt behavior against pinned Neo4j Enterprise 5.26 LTS and current
Enterprise releases. NornicDB actions, reusable role assignments, and exact
entity handles are NornicDB extensions.
