# Neo4j Enterprise FGAC comparison harness

This directory holds the focused, reproducible NOR-12 compatibility harness for
documented Neo4j Enterprise fine-grained access control (FGAC) behavior.

The harness sends the same synthetic authorization flows to a pinned Neo4j
Enterprise reference and to a NornicDB candidate over Bolt. Every case asserts a
normal documented outcome and the candidate must match the reference outcome.
The fixture covers the flows listed below. Enable FGAC on the candidate
before running the comparison.

The common fixture includes user-provider inspection. The 2026.10.2 section
also checks rule-derived database access and authorization-rule inspection.
The harness selects that section after verifying the reference version.

## Pinned references and opt-in

`references.json` is the only owner of the reference pins:

| Version | Image | Digest |
|---|---|---|
| 5.26.30 | `neo4j:5.26.30-enterprise` | `sha256:8befcd4e…19891c` |
| 2026.10.2 | `neo4j:2026.10.2-enterprise` | `sha256:941ba98b…7e25a9` |

The exact digests are in the JSON and are validated by the substrate tests.
The reference preflight requires an Enterprise edition and one of these
versions; Community is refused. Record the candidate revision with the comparison result.

The comparison never runs implicitly. Its test is skipped unless
`NORNICDB_FGAC_EE_EVALUATION=1` is set, and `run-fgac-comparison.sh` also
requires `FGAC_EE_EVALUATION=1`. Running an Enterprise image is an evaluation
choice; this repository does not run Docker, pull images or start databases
during ordinary tests.

## Endpoint configuration

Use a disposable test endpoint for each side. Use `bolt+s://` or `neo4j+s://` with a trusted certificate for TLS. The harness creates and removes
synthetic `fgac_*` users, roles, and rules plus the `FGACDoc` / `FGACOther`
labels.

| Variable | Meaning | Default |
|---|---|---|
| `FGAC_EE_EVALUATION` | Explicit evaluation opt-in for the shell entry point | required |
| `NORNICDB_FGAC_EE_EVALUATION` | Explicit evaluation opt-in for the Go test | required |
| `NORNICDB_NEO4J_REFERENCE_URI` | Reference Bolt URI | required unless starting a container |
| `NORNICDB_NEO4J_REFERENCE_USER` | Reference admin user | `neo4j` |
| `NORNICDB_NEO4J_REFERENCE_PASSWORD` | Reference admin password | required |
| `NORNICDB_NEO4J_REFERENCE_DATABASE` | Reference target database | `neo4j` |
| `NORNICDB_FGAC_CANDIDATE_URI` | Candidate Bolt URI | required |
| `NORNICDB_FGAC_CANDIDATE_HTTP_URI` | Optional candidate HTTP origin for impersonation cases | disabled |
| `NORNICDB_FGAC_CANDIDATE_ROUTED_URI` | Candidate routing URI using `neo4j://` or `neo4j+s://` | disabled |
| `NORNICDB_NEO4J_REFERENCE_ROUTED_URI` | Reference routing URI using `neo4j://` or `neo4j+s://` | disabled |
| `NORNICDB_NEO4J_REFERENCE_HTTP_URI` | Reference HTTP origin for impersonation cases when supplying a reference server | disabled |
| `NORNICDB_FGAC_CANDIDATE_USER` | Candidate admin user | `admin` |
| `NORNICDB_FGAC_CANDIDATE_PASSWORD` | Candidate admin password | required |
| `NORNICDB_FGAC_CANDIDATE_DATABASE` | Candidate target database | `nornic` |
| `NORNICDB_FGAC_REFERENCE_VERSION` | Optional pinned version assertion | any pinned version |
| `NORNICDB_FGAC_SYNTHETIC_PASSWORD` | Password for synthetic users | `fgac-synthetic-password` |
| `NORNICDB_FGAC_FIXTURE` | Fixture path | `fixtures/authorization_flows.json` |
| `NORNICDB_FGAC_REFERENCES` | Pinned reference path | `references.json` |

### Operator-provided endpoints

```bash
export FGAC_EE_EVALUATION=1
export NORNICDB_NEO4J_REFERENCE_URI='bolt://127.0.0.1:7687'
export NORNICDB_NEO4J_REFERENCE_PASSWORD='<reference admin password>'
export NORNICDB_FGAC_CANDIDATE_URI='bolt://127.0.0.1:17687'
export NORNICDB_FGAC_CANDIDATE_PASSWORD='<candidate admin password>'
bash testing/cypher/fgac/run-fgac-comparison.sh
```

### Pinned reference container

The image must already be present locally. Pulling is a separate explicit
choice, and the container accepts the Enterprise evaluation license only when
the operator opts in:

```bash
export FGAC_EE_EVALUATION=1
export FGAC_START_REFERENCE=1
export FGAC_REFERENCE_VERSION=5.26.30
export FGAC_REFERENCE_PASSWORD='<initial neo4j password>'
export NORNICDB_FGAC_CANDIDATE_URI='bolt://127.0.0.1:17687'
export NORNICDB_FGAC_CANDIDATE_PASSWORD='<candidate admin password>'
# Only when you intend to fetch the pinned image:
# export FGAC_ALLOW_IMAGE_PULL=1
bash testing/cypher/fgac/run-fgac-comparison.sh
```

Run the script once per reference version. The container uses an ephemeral
loopback port, `NEO4J_ACCEPT_LICENSE_AGREEMENT=eval`, and is removed on exit.

## Asserted flows

`fixtures/authorization_flows.json` creates two `FGACDoc` nodes and one
`FGACOther` node, then creates synthetic roles and users:

| Case | Documented flow |
|---|---|
| `role-grant-visible` | `GRANT ROLE` assigns the native role; `SHOW USERS` reports it |
| `access-grant-returns-scalar` | Database `ACCESS` permits a scalar calculation |
| `access-deny-refuses-session-work` | `DENY ACCESS` refuses that calculation |
| `traverse-and-property-read` | `TRAVERSE` on the label plus `READ` on `id`/`title` returns both documents |
| `cross-role-deny-hides-documents` | A `DENY MATCH` held by one role overrides another role's `GRANT` |
| `role-privileges-introspection` | `SHOW ROLE … PRIVILEGES` reports the granted actions |
| `read-only-role-write-denied` | A read-only principal is refused an ordinary `CREATE` |
| `read-only-role-write-side-effect-absent` | The refused write left no node in a fresh session |
| `transaction-rollback-on-scoped-write-denial` | A permitted write followed by a scoped write denial fails the transaction |
| `transaction-rollback-side-effect-absent` | Neither transaction write is visible in a fresh session |
| `property-rule-filters-before-projection` | A rule uses a stored property to select nodes without returning it |
| `writer-assigns-property-to-new-node` | A creator may set an allowed property on a node it bound in that statement |
| `new-node-property-is-stored` | A separate session reads the final committed property |
| `impersonation-uses-target-role-for-read` | Bolt evaluates property visibility using the impersonated account |
| `impersonation-uses-target-role-for-write` | Bolt applies the impersonated account's read-only role to a write |
| `impersonation-autocommit-reuses-session` | Driver auto-commit applies the target scope on successive queries |
| `impersonation-autocommit-default-database` | Omitting the database selects the target account's home database |
| `impersonation-routed-read` | Driver routing and execution apply the target scope |

The documented behavior is taken from the Neo4j Operations Manual:

- [Fine-grained access control and privilege commands](https://neo4j.com/docs/operations-manual/5/authentication-authorization/manage-privileges/)
- [Read privileges (`TRAVERSE`, `READ`, `MATCH`)](https://neo4j.com/docs/operations-manual/5/authentication-authorization/privileges-reads/)
- [Write privileges (`CREATE`, `SET PROPERTY`)](https://neo4j.com/docs/operations-manual/5/authentication-authorization/privileges-writes/)
- [Database `ACCESS` privilege](https://neo4j.com/docs/operations-manual/5/authentication-authorization/database-administration/)
- [User and role administration](https://neo4j.com/docs/operations-manual/5/authentication-authorization/manage-users/)

## Substrate checks

```bash
go test ./testing/cypher/fgac
go vet ./testing/cypher/fgac
bash -n testing/cypher/fgac/run-fgac-comparison.sh
```

These check the fixture, pins, outcome normalization, configuration opt-in and
script syntax without a database, Docker or image pull. They are substrate
sanity checks, not full repository verification.

## Limits

- The harness proves the reference is a pinned Enterprise version and edition
  through `CALL dbms.components()`. Bolt cannot report the image digest, so the
  digest pin itself is enforced by the launcher and `references.json`.
- The Enterprise reference runs only during an opt-in comparison. A setup
  command that a pinned version rejects fails the comparison.
- NornicDB behavior is compared, not assumed. Until the candidate implements a
  documented flow, the comparison reports the setup or case mismatch.
- The fixture uses each endpoint's default target database and scoped synthetic
  teardown. Do not point either endpoint at a database whose `FGACDoc` or
  `FGACOther` data must be preserved.

### Impersonation comparison

To include driver routing, configure both routing URIs and ensure both
servers advertise addresses reachable by the client. The fixture sets the
synthetic reader's home database to the configured target database. For a
meaningful omitted-database check, supply a target database that differs
from the authenticated actor's home database.

To include HTTP Query API cases, supply the candidate HTTP origin.
With a supplied Enterprise reference, set its HTTP origin as well.
The managed reference container publishes HTTP when the candidate origin
is set. The protocol checks compare field presence, result rows and Neo4j
error codes. Transaction IDs, bookmark token contents and elapsed times
vary between servers.

Build or start the candidate with `NORNICDB_QUERY_API_COMPATIBILITY=5.26`
for the LTS reference and `NORNICDB_QUERY_API_COMPATIBILITY=2026.10` for
the current reference. The HTTP matrix compares status, Neo4j code and
response fields with the corresponding reference. It also checks that a
refused request publishes no data, transaction or bookmark.
