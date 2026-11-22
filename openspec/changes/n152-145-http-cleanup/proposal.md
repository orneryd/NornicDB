# N152-145: Preserve the observed HTTP transaction outcome

## Product outcome

A composite HTTP statement whose remote participant has already aborted returns
its original arithmetic error without inventing a second TransactionNotFound
cleanup failure. Local writes still roll back, active successful transactions
still send rollback, and later requests to retired transactions still return404.

## Root cause and approved correction

The server retires its session on statement failure but leaves a stale
`transaction` object in its response. The HTTP client discards that response
when returning its first error, so it cannot observe retirement. Its later
DELETE then produces a misleading cleanup failure.

The complete correction stays with the existing two owners:

1. `rollbackRequestTransaction` removes the transaction metadata and commit URL
   after `RollbackAndDelete`; begin/execute handlers refresh expiry only while
   metadata remains. The manager retires the session even when completion returns
   an error, and that original cleanup error remains in the response.
2. `doRequestToURL` retains the decoded response with its error and composes all
   decoded errors in order through `pkg/errors/compose.Outcomes`. This is the
   canonical dependency-free owner used by storage; importing the higher
   `pkg/errors.CombineTerminalOutcomes` wrapper would create an import cycle.
3. `remoteCypherTx.QueryCypher` records an immutable observed abort only for a
   decoded successful HTTP response with statement errors and no transaction
   key. The atomic observation is separate from ordinary `closed`: rollback
   needs no second DELETE, but commit must return the observed failure, never
   success. Query and lease renewal cannot reuse the observed-aborted handle.

A transport, body-read, decode, or non-success HTTP error does not establish
closure and still requires the ordinary rollback attempt. A response retaining
transaction metadata also still requires rollback. No rollback404 is ignored.

The official HTTP API documents missing `transaction` as closure and notes that
an authentication error can leave a transaction open:
https://neo4j.com/docs/http-api/current/transactions/
No fresh Neo4j runtime comparison is claimed.

## Rejected investigation proposal

DeepSeek job f808e4f11ffe4c0fb2ce9202c85a3582 completed in666.422s. Its source trace
and exact fixture recovery were accepted. Its proposed HTTP200-error alone =>
`closed=true` correction was rejected: it ignores still-open metadata, would
make Commit falsely succeed via the ordinary closed guard, and loses additional
remote cleanup errors. The raw proposal is preserved at
`/tmp/n152-145-rejected-worker-proposal.md`; investigation evidence files remain
historical rather than final implementation instructions.

## Scope and evidence

- Exact original two-server composite fixture is preserved byte-for-byte; original
  RED on70ea82df61970944f017290e59ec1cea7f2d61fa failed both parser d=0 cases,
  with d=1 rollback/later404/local-zero controls passing (2.346s).
- Storage/server/fabric production source is identical between that base and the
  integrated implementation base f77ec7706de099588ac232b32b6908860511d3da.
- New client boundaries cover observed abort, retained transaction metadata,
  multiple error entries, non-success/decode/transport uncertainty, rollback
  failure preservation, and rejection of commit after observed abort.
- Server checks cover truthful metadata on both begin-with-statements and execute.
- Existing composite cleanup tests must continue to report real cleanup failures.
- Full qualification and indexed duplicate discovery remain separately pending.
