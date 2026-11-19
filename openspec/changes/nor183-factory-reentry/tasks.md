# TODO

- [x] Reproduce four remote/composite factory callback deadlocks on e8cd436e540ed421327bb72478d4d98479f7a2ac before edits (worker evidence).
- [x] Review immutable runtime constituent publication and all changed production code.
- [x] Preserve original listener and pending-listener regression sources.
- [x] Add multi-remote composite public reads, auth, routing and ownership assertions.
- [x] Independently run the final affected multidb package on macOS: PASS 39.020s.
- [x] Independently run the final affected multidb package on Linux amd64: PASS 71.317s.
- [x] Reconcile the credential-URL structural inventory with the reviewed storage-view split: ten stale `manager.go:*DatabaseManager.GetStorageWithAuth` fingerprints replaced by the ten current `storage_view.go` fingerprints, with the old→new mapping and classification in `url-inventory-review.md`. Focused `go test ./pkg/observability -run TestCredentialURLSinkStructure -count=1 -v` PASS 1.17s (raw_egresses=893) and `go test ./pkg/observability -count=1` PASS 16.667s; no runtime, scanner, assertion or non-egress change.
- [x] Run the unchanged staged duplicate checker: 52 functions, no duplicates, matching release baseline (173.83s).
- [x] Publish draft release/v1.5.2 PR #1867 and put complete test sources/commands/results inline in both existing Plane descriptions.
- [ ] Let CI verify the broader release contracts; no full local suite, race or stress run.

- [x] Parent independently reproduced URL-inventory failure at the published head (1.601s), reviewed all ten old/new mappings, and verified the correction (2.022s package command, focused test1.61s). Runtime and scanner/assertion/non-egress source remain unchanged.

- [x] Reconcile the lexical population with the published NOR-183 storage-view split (parent evidence: immutable base e8cd436e5 with clean tracked source PASS 3.082s at 4095 candidates; published head 027814f2a FAIL 3.027s). The explicit `NORNICDB_LEXICAL_POPULATION_DISCOVERY=1` run reports exactly one removed member (`pkg/multidb/manager.go:*DatabaseManager.GetStorageWithAuth`) and three added `pkg/multidb/storage_view.go` owners (`*DatabaseManager.GetStorageWithAuth`, `*DatabaseManager.buildCompositeStorageSelection`, `*DatabaseManager.resolveStorageSelection`), net 4095→4097 with no other member added, removed or changed. Each new owner is classified `credential-runtime` from its own source with its own evidence tests instead of the carried-over `credential-output` row; `url-inventory-review.md` records the retracted "pre-existing" attribution. No runtime, scanner, assertion or URL-manifest change. `go test ./internal/cyphertext -count=1` PASS 2.368s; `go test ./pkg/observability -run 'TestCredentialURLSinkStructure|TestCredentialFieldPolicyPopulation' -count=1` PASS 1.278s (raw_egresses=893, field-policy population PASS); `go test ./pkg/multidb -count=1` PASS 37.477s.

- [x] Parent final cross-package ledger checks passed on macOS (cyphertext4.304s/observability2.681s) and Linux amd64 (cyphertext5.046s/observability2.057s): FoundationLexicalPopulation, CredentialURLSinkStructure, CredentialFieldPolicyPopulation. Original runtime code and guard/scanner source remain unchanged; CI must rerun at the metadata correction head.
