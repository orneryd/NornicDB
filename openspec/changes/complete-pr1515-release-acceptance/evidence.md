# Current follow-up evidence

Behavioral source: merged release `04c87de3e232fece79ebf3eb5af78e5b3fe5e09f`; follow-up head `ce4dd07bc75d46fa72479aeb5a9829e7447f2439` changes only tracking documentation. No source changed during these runs.

| Check | Result | Seconds |
| --- | --- | ---: |
| `NORNICDB_PARSER=nornic GOFLAGS=-count=1 go test ./cmd/nornicdb -run '^TestRemoteConstituentCredentials$' -count=1 -v -timeout=8m` | PASS | 66.613 |
| Same command with `NORNICDB_PARSER=antlr` | PASS | 74.589 |
| Focused credential persistence/runtime/wire tests in pkg/multidb, pkg/fabric, pkg/storage | PASS | 4.462 |

The public matrix builds and starts actual authenticated remote and coordinator processes. It checks explicit and URI credentials over HTTP/Bolt remotes, HTTP/GraphQL/Bolt ingress, implicit success/error endings, explicit HTTP/Bolt commit/rollback, and direct constituent selection. Remote wrong-password controls reject authentication. Every HTTP transaction phase must retain correct authentication. Native inline remote DDL supplies persisted setup for the ANTLR runtime restart; this does not claim that native extension is in the ANTLR administration grammar.

The focused command selects TestSolReview1513PersistedCredentials, TestRemoteCredentialPersistenceEveryWriter, TestRemoteCredentialPersistenceInput, TestRemoteCredentialPersistenceUpgrade, TestRemoteCredentialPersistenceNewInputRequiresKey, TestRemoteRuntimeCredentials, TestRemoteRuntimeCredentialErrorsAreRedacted, TestCredentialUsernameOnlyCatalogPersistence, TestCatalogRemoteRuntimeCredentials, TestRemoteResolvedAliasesKeepDistinctCredentials, TestRemoteAuthenticationPreservesAdmittedCredentialBytesOnWire and TestRemoteEngineBoltAuthConstruction. Normal go test success proves only selected tests that exist; no blanket migration/reference qualification follows.

DeepSeek read-only review completed in74.201s. Parent source review rejected proposed redundant helper tests: real Bolt URI composition already runs through an authenticating remote, and the wire fixture rejects mismatched Basic/Bearer credentials. The supported explicit V3-to-V4 upgrade contract supersedes the earlier startup-rewrite warning wording. The oidc transaction fixture is lifecycle evidence, not proof of user/password preservation. Successful authenticated remote writes with durable commit/rollback outcomes remain a separate item; the read matrix does not prove those writes.

Graph refresh and the enabled duplicate checker remain pending. The first importer attempt failed at the restarting endpoint; the second targets the exact immutable merged release revision. No performance or complete-AC pass is claimed.
