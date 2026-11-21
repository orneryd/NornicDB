# NOR-12 Optional fine-grained access control

Implementation base is `release/v1.5.2`. See [NOR-12 in Plane](https://buttocks.tail2f19fb.ts.net/nornicdb/projects/02564fe1-72c5-416b-83e6-a13d299d12be/issues/636d96a9-8ea9-417d-982e-4bb6a9a75890)
for the planned acceptance matrix and the [operator guide](../../../docs/security/fine-grained-access-control.md)
for configuration and examples.

- [x] Compile and persist scoped definitions, role assignments, and deny precedence.
- [x] Apply catalog revision checks and migrate assignments with role mutations.
- [x] Bind database, entity, type, property, and predicate targets through the owning storage operation.
- [x] Apply scoped graph reads, writes, schema privileges, and database lifecycle decisions.
- [x] Implement Cypher privilege editing, role membership, native account operations, and privilege introspection.
- [x] Bind the HTTP, Bolt, GraphQL, MCP, and Qdrant data paths and retained result delivery.
- [x] Register individual Heimdall, managed embedding, and knowledge policy operations.
- [x] Integrate Bolt and HTTP Query API impersonation with the executing principal. Integrate effective-permission inspection and the enabled-mode role editor.
- [x] Qualify the documented public impersonation matrix against 5.26.30 and 2026.10.2 Enterprise under [NOR-11](https://github.com/orneryd/NornicDB/issues/936).
  - [x] Compare official-driver auto-commit, explicit transactions and routing. Use a nondefault home database to verify omitted database selection on both direct and routed sessions.
  - [x] Compare HTTP auto-commit, begin with and without a statement, subsequent statements, commit and rollback. Compare each release's response codes and summary fields using its Query API compatibility profile.
  - [x] Compare named and wildcard grants, deny and revoke precedence, suspended and missing targets, and delegation when the target's own password change is pending.
  - [x] Verify isolation across executing accounts and ordinary requests, actor ownership of retained HTTP transactions, denial after revocation, Bolt buffered-result scope, and audit attribution of actor and target. Verify mutation of the administration catalog is refused while executing as another account.
  - [x] Run the reusable fixture on both release pins. The default-database runs passed 22 Bolt and 21 HTTP cases on 5.26.30, and 24 Bolt and 21 HTTP cases on 2026.10.2. Each distinct-home run passed eight Bolt and 21 HTTP cases.
- [x] Configure opt-in startup, existing RBAC mode, bootstrap privileges, and operator recovery.
- [x] Exercise the role-first administration flow in the browser, including mobile layout, draft conflict recovery, and disabled-mode routing.
- [x] Create a role, save a database, entity, and property-scoped entitlement, and assign a member against the bundled server at phone width.
- [x] Verify role and member changes against the HTTP server and verify caller-scoped embedding admission and restart handling.
- [x] Verify catalog restart and recovery into a new Badger engine from a native backup.
- [x] Compare 15 authenticated Bolt cases against both pinned Enterprise references on Linux with TLS. Compare two additional current-Enterprise native-tag cases.
- [x] Keep FGAC maps, catalog routes, and the catalog editor unloaded when FGAC is off.
- [x] Keep dormant policy out of RBAC role mutations and reconcile role identity upon activation.
- [x] Verify the RBAC browser flow makes no FGAC requests or editor downloads.
- [ ] Complete administrator-approved broad CI on the final release PR head. Local verification is focused or package-level.
- [x] Refresh active users, roles and policy after live restore or replicated system commits, including retained requests.
- [x] Constrain Qdrant updates and drops to the authorized collection, including recreation under the same name.
- [x] Implement Cypher 25 authorization-rule administration and validate rule-derived roles and listings against current Enterprise.
- [x] Implement named provider bindings, validate provider administration against both references, and exercise the configured OAuth provider end to end against a test issuer.
- [x] Expand the pinned Bolt fixtures to 16 cases on 5.26.30 and 18 on 2026.10.2.
- [x] Verify forced HNSW selection excludes decay-suppressed entities, entities outside traversal scope, and embeddings outside the managed-vector read scope.
- [x] Qualify named provider binding replacement and removal on both pinned Enterprise versions. Compare native-tag, list, and transaction-time rules on 2026.10.2 with native ABAC configured.
- [x] Verify same-statement Cypher vector scoring with reveal enabled and graph masking still enforced. Compare ordinary results before and after the reveal statement and verify refusal without the reveal entitlement.

## Reuse review

- The embedding retry paths share the existing queue method. Caller retries
  carry authorization in the operation context. The worker tests use the
  existing recording provider and real namespaced pending-index operations.
- The graph cancellation tests share one fixture and still cover cancellation at
  entry and during incoming and outgoing reads. Heimdall action prompts use the
  existing raw generation method before interpreting the response.
- The FGAC graph decorator authorizes and masks entities. The transaction
  decorator maps namespace IDs and includes staged writes. Those decorators
  use the underlying storage operations at separate enforcement boundaries.
- Search reads and rebuilds, neighborhood and path requests, and GDPR export
  and deletion share authorization owners. Their different request shapes and
  side effects require distinct handlers.
