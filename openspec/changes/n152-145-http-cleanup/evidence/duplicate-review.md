# Final canonical owner review

Base: f77ec7706de099588ac232b32b6908860511d3da. Graph discovery was incomplete;
manual source review does not establish an empty or successful indexed search.

| Owner/candidate | Contract | Decision |
| --- | --- | --- |
| storage.remoteCypherTx | Remote wire lifecycle and observed state | Extend with one immutable HTTP abort observation, published by QueryCypher and read by query/completion/lease. Ordinary closed state remains separate so aborted commit cannot become success. |
| httpTransport.doRequestToURL | Decode response and preserve errors | Retain transaction-key presence, HTTP status and decoded response on error; compose every error in wire order. All five callers inspect or return the error; only QueryCypher consumes the observed transaction state. |
| pkg/errors/compose.Outcomes | Shared dependency-free primary/cleanup error composition | Reuse; the higher pkg/errors wrapper imports storage and would create a cycle. No second aggregation algorithm or error-code parser. |
| server.rollbackRequestTransaction | Retire request session and publish cleanup outcomes | Clear stale transaction/commit metadata at this owner; begin and execute refresh expiry only for retained metadata. Existing cleanup errors remain in response. |
| txsession.Manager.rollbackAndDelete | Authoritative session retirement | Reuse unchanged: it deletes the session after completion attempt, including a reported cleanup failure. |
| fabric.remoteParticipant and RemoteFragmentExecutor | Exactly-once completion, fan-out, stored outcome reporting | Unchanged; no HTTP-specific suppression or duplicate state added here. |
| cypher.TransactionContext / retirement | Local rollback and transaction failure semantics | Unchanged; no blanket remote rollback skip. |
| Existing explicit HTTP wire test fixture | Open/execute/commit/delete request ledger | Reused by new client tests; no second baseline transport fixture. |

The worker's HTTP200-only/closed-boolean proposal was rejected. Error alone does
not prove closure; the HTTP contract permits an error with retained transaction
metadata. An ordinary closed flag would also make later Commit return nil.
Missing transaction metadata is the documented response fact. Only a decoded
successful HTTP error response supplies that fact; uncertain transport/decode
and non-success responses retain the normal rollback attempt. No 404 is masked.

No performance optimization is claimed. The new checks are required lifecycle
observation; no alternative decoder, state coordinator or fallback is introduced.

## Public secondary-error propagation review

The decoded error object returns through `remoteCypherTx.QueryCypher` and
`RemoteFragmentExecutor.executeRemoteQuery` unchanged apart from credential-safe
projection; `RemoteFragmentExecutor.Execute` wraps it with `%w`, and Fabric's
execution branch returns that same cause. `server.statementFailure` delegates
status/message rendering to `statementError` / `errors.RenderStatus`.
`observability.redactClientError` explicitly rebuilds all multi-unwrapped causes,
so it does not discard the secondary decoded error. These remote localized
errors carry the storage message identifier, not a Neo4j status-code descriptor;
`neo4jStatusPlain` takes the first embedded Neo4j prefix and keeps the rest of the
composed message. The `renderStatus` localized replacement requires a matching
status code and does not replace this composed message with one storage child.

Runtime coverage pairs the direct multi-error client test with the existing
public genuine cleanup-fault guard and the exact two-server original defect.
No synthetic remote-storage cleanup failure is represented as actual storage
failure, and no fresh Neo4j remote runtime qualification is claimed.
