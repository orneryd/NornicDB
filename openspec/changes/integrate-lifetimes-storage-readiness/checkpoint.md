# PR #1693 compilation-only checkpoint

The maintainer requested saving and pushing the integrated work with compilation
as the checkpoint gate. This is a draft checkpoint, not merge readiness.

## Verified at this checkpoint

- `go test ./... -run '^$' -count=1` completed successfully on the integrated Go
  source. This compiles all default test binaries and executes no tests.
- `git diff --check` passes. Hook/checker configuration and scripts are unchanged.
- Focused schema/count/temporal race controls pass (storage 4.082s). The full
  adminimport package passed (3.093s) in the last package run.
- The approved initializer source-order model has focused parent proof (9.707s)
  and isolated full-model normal/race proof (40.209s / 151.120s).

## Integrated scope

Context-first storage/native transaction, metadata/auth/schema, service and APOC
callers; typed publication positions and receipts; request-versus-completion
ownership; SubgraphAll nodes/rels map/error results; private accounting/embedding
read contexts; schema preparation/admission, buffered named-count and temporal
preparation cancellation corrections; native WAL acceptance/recovery/retention;
encoder scratch and canonical empty-map allocation reductions; fixture migration
without skipped assertions. Existing Badger keys/payload layouts, WAL wire framing
and storage versions are not migrated by this checkpoint.

The maintainer accepted documenting the measured direct-WAL correctness cost.
The isolated map/scratch candidate measured 31,005 versus 28,318 ns/op (+9.49%)
for default direct single-node writes, and 30,903 versus 28,664 ns/op (+7.81%)
with write-behind configured. This is not a <=5% or whole-ingest performance pass.
The separate ID-format allocation patch remains unintegrated.

## Known failures and unfinished work

- Last complete storage run failed after 465.188s, with 19 root failure groups.
  Preserve and fix them: authoritative observed-WAL dispatch, replay/retention
  test consumers, remote default deadlines/transport contracts, closed-engine
  result contracts, embedding helper capabilities and projection-scope lifetime.
- The strict configured context census has not passed. Its last source-pinned
  run predates later corrections; the source-order diagnostic still has four
  Cypher and one Bolt same-package admission callsites without a proof.
- Independent lifetime/accepted-completion records require exact review and
  freshness controls. No blanket exception or census weakening is approved.
- The context-first Fabric callback migration is approved. Its typed upstream
  closure exposed additional public APIs; that complete inventory and any
  additional contract decision remain unfinished. Production scanner edits
  have not been integrated. The valid 32 MiB USE-query cancellation reproduction
  and healthy controls are preserved.
- Pending-node index staging cancellation is under investigation.
- A fresh full `make test`, release-base reconciliation and final PR/Linear
  qualification remain required before marking merge-ready. Broader verification
  stays in CI. The duplicate checker is advisory this round, with its inventory
  unresolved and no script/rule edits.

## Resume evidence and isolated work

The durable evidence root is
`~/.local/share/nornicdb-pr-readiness/pr144-storage-integration/`; start with
`current-resume.json` and the `round3-*-failures.log` records. Required original
REDs, source pins, rejected experiments and qualified packet receipts are retained.

Unintegrated worker work remains in registered PR worktrees; do not delete those
worktrees or copy their entire dirty seed over this checkpoint. Review and apply
only incremental packets with matching preimages, then compile/test the result.

| Work | Worktree suffix / job | State at checkpoint |
| --- | --- | --- |
| WAL authority/replay/retention fallout | pr1693-api-storage | Partial follow-up preserved; final packet qualification unfinished |
| Remote call lifetime/default deadline | pr1693-remote-call-context / 90c335e463504892b8a781dafe48db2f | Worker-owned, not integrated |
| Helper/capability/projection contracts | pr1693-helper-context-contracts / e1c24a78425f453bb3738ec00d65a422 | Worker-owned, not integrated |
| Pending staging index | pr1693-schema-preparation-context / aea92eefcc7e4060a6a2f4ad65cc223d | New task; earlier schema packet is integrated |
| Remaining source-order proof | pr1693-private-read-context | Capability-boundary proposal is unapproved; no extra discharge integrated |
| USE scanner migration | pr1693-use-preparation-context | RED/healthy tests and incomplete exported-frontier inventory preserved |

No merge is authorized by this checkpoint. Keep PR #1693 draft and NOR-71 open.
