# Collection access

Collection access limits what a login reads from a database to the documents of the collections its roles are granted, and to the nodes derived from those documents. It's administered entirely in Cypher, on the `system` database, by an administrator.

## Concepts

- **Collection:** a node a document belongs to through a membership relationship, for example `(d:Document)-[:MEMBER_OF]->(c:Collection)`.
- **Access scope:** a graph's declaration of the membership pattern and of how other nodes derive their visibility from documents or collections. A graph has at most one scope.
- **Collection rule:** a `GRANT` or `DENY MATCH` privilege whose pattern names one collection.
- **Restricted:** a login is restricted on a database when any of its roles holds a collection rule there. This holds whatever other roles it has. Under Bolt impersonation, the impersonated user's roles decide.

A restricted login sees:

- documents in at least one granted collection, and in no denied one (a DENY wins);
- nodes derived from a visible document or collection, through the scope's `DERIVE` rules (the most specific entry for a node's labels applies, and chains of any length are followed);
- relationships whose two ends are both visible;
- no unfiled documents;
- nodes the statement, or its explicit transaction, created (see [Writes](#writes)).

**Pending the owner's decision:** today an ordinary graph privilege that any of the login's roles holds, PUBLIC included (`GRANT MATCH {*} ON GRAPH lib NODES Label`, or a property rule such as `FOR (n:Definition) WHERE n.level = 'library'`), widens a restricted login's view. The nodes it matches are visible whatever the collection rules say, except a document hidden by a collection `DENY`. Whether such a grant should widen a restricted login's view is still open, and this behaviour may change.

## Roles

```cypher
CREATE ROLE agent_a IF NOT EXISTS
GRANT ROLE agent_a TO alice
REVOKE ROLE agent_a FROM alice
RENAME ROLE agent_a TO agents
DROP ROLE agents IF EXISTS
SHOW ROLES WITH USERS
```

Role commands run outside explicit transactions. They go through the same role owner as the `/auth/roles` API, but use Cypher: `GRANT ROLE` and `REVOKE ROLE` change each user under one lock and transaction, while `PUT /auth/users/{name}` replaces a user's whole role list.

## Access scope

```cypher
CREATE OR REPLACE ACCESS SCOPE collections ON GRAPH lib
  MEMBERSHIP (d:Document)-[:MEMBER_OF]->(c:Collection) KEY d.id, c.id
  DERIVE (x:Document) FROM (x)-[:ATTACHMENT_OF]->(d)
  DERIVE (x:Version) FROM DOCUMENT x.document_id
  DERIVE (x:Summary) WHERE x.owner_kind = 'collection' FROM COLLECTION x.owner_id
  DERIVE (x:Identity) FROM (x)<-[:RESOLVES_TO]-(t:Mention)

SHOW ACCESS SCOPES ON GRAPH lib
DROP ACCESS SCOPE collections IF EXISTS ON GRAPH lib
```

A `DERIVE` source is one of these, and a node's sources combine by ANY:

- **`DOCUMENT x.p`:** a property holding a document key or a list of them.
- **`COLLECTION x.p`:** a property holding a collection key or a list of them.
- **A path to a document:** a fixed pattern ending at the membership's document, `(x)…(d)`.
- **A path to another node:** a fixed pattern ending at a node whose visibility `x` follows, `(x)…(t:Label)`.

## Collection rules

```cypher
GRANT MATCH {*} ON GRAPH lib FOR (n:Document) WHERE (n)-[:MEMBER_OF]->(:Collection {id: 'A'}) TO agent_a
DENY MATCH {*} ON GRAPH lib FOR (n:Document) WHERE (n)-[:MEMBER_OF]->(:Collection {id: 'B'}) TO agent_a
REVOKE GRANT MATCH {*} ON GRAPH lib FOR (n:Document) WHERE (n)-[:MEMBER_OF]->(:Collection {id: 'A'}) FROM agent_a
GRANT MATCH {*} ON GRAPH lib FOR (n:Definition) WHERE n.level = 'library' TO PUBLIC
```

`SHOW PRIVILEGES`, `SHOW ROLE r PRIVILEGES` and `SHOW USER [name] PRIVILEGES` list the rules.

## Introspection

- `SHOW CURRENT USER YIELD collectionLimited` returns a `LIST<STRING>`: the databases the executing user can access where its view is restricted (`[]` when none). It appears only when yielded. On other users' rows of `SHOW USERS … YIELD collectionLimited` it is `null`.
- `SHOW USER PRIVILEGES` lists the executing user's own privileges (procedure, impersonation and graph) through its roles and PUBLIC, without administrator rights.

## What a restricted login can use

- **Served through its view:**
  - Cypher over Bolt, HTTP, GraphQL and Heimdall Query;
  - search: `db.retrieve`, `db.rretrieve`, `db.retrieveRelationships`, `db.retrieveGraph`, `db.rretrieveGraph` and their continuations, `db.index.vector.queryNodes` and `queryRelationships`, `db.index.fulltext.queryNodes` and `queryRelationships`, and `MATCH … ORDER BY vector.similarity.* … LIMIT`. Each ranks the visible corpus alone: keyword plans and BM25 statistics (prefix expansions, IDF, average length), vector rank lists, reciprocal-rank and graph fusion, and match term weights hold only what the login sees. So its results, scores, ranks and top-K are the ones a database holding only the visible items would return, for nodes and relationships, keyword, vector and hybrid;
  - the graph routes;
  - GDS aggregation projections (`gds.graph.project` as an aggregation), in the login's own catalog. A saved projection is refused once it holds a node the login can no longer see.
- **Refused:**
  - routes that read storage or indexes around the executor: REST `/nornicdb/search` and `/similar`, GraphQL search and similarity, MCP tools, Heimdall discovery and statistics, Qdrant gRPC, the direct HTTP operations (GDPR, embedding, decay and status endpoints);
  - multi-graph statements;
  - the label/type form of `gds.graph.project`;
  - `db.index.stats`.

## Writes

Writes follow the database's write privilege, as for any login, and see the login's view. As a result:

- a unique-constraint error, or a `MERGE` matching a value a hidden node holds, can reveal that the value exists;
- `DETACH DELETE` of a visible node is refused while it has relationships to hidden nodes;
- a login may move a visible document between collections it can see;
- a statement can always address the nodes it created, and so can the later statements of its explicit transaction. That covers `MERGE … SET n += $map`, `MERGE … ON CREATE SET`, `CREATE (n $map)`, `CREATE (n) SET n += $map`, and `MATCH … SET` on a node an earlier statement of the transaction created. Nothing checks where a created node lands: a login can create a node outside its collections, or with none. Once the transaction ends, the scope alone decides what the login sees, so it can't read such a node back. A created node is never a source another node derives its visibility from, so creating one can't reveal an existing node.

Grant restricted logins no write privilege where they must be read-only.

## Limits

- `TRAVERSE`, `READ`, property sets and relationship segments are not supported.
- A restricted search's latency scales with the whole index; its results, scores and ranks don't depend on hidden data.
- Membership is evaluated per statement.
