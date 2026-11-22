# N152-64 verification and ownership evidence

Worktree `/private/tmp/n152-64-forward` at `18b36a85d10e775b9d5d3500ecf3482682754a8d`
with the parent's approved transport correction applied but uncommitted
(`pkg/replication/transport.go` and `pkg/replication/handlers.go` modified,
`pkg/replication/transport_forward.go` new). The test lane added
`pkg/replication/transport_forward_context_test.go` (810 lines, sha256
`bfaca12d0c1c24fb8da5bc59f839c1a76da57fbe9ca006340e2015ee202254ab`) and these
OpenSpec records. No production file was edited by this lane.

## Established baseline (not re-measured here)

The parent recorded the fail-first baseline before the correction: the two
existing real-TCP forwarding tests failed against `18b36a85d`
(`ForwardApply` leader preparation did not observe the follower's cancellation
or deadline) and the corrected head passes the focused run
(`/tmp/n152-64-parent-first.log`, `ok ... 1.749s`). Reverting production code to
re-measure the failing baseline is outside this lane's authorization, so the
pre-correction reproduction above is the inherited evidence, not a fresh one.

## New regressions and the behavior each pins

`TestForwardApplyPayloadInlinesCommandForOldAndNewReaders` — a new payload is a
top-level `Command` plus `DeadlineUnixNano` and never a nested `Command`
wrapper; a pre-correction `Command` decoder reads every durable field from a new
payload; a current decoder reads an old bare `Command` payload with no deadline.

`TestForwardApplyWireKeepsEveryCommandFieldAndSignedDeadline` — production
`SendForwardApply` into the production leader handler over real TCP keeps `Type`,
`Data`, `Timestamp` and `RequestID`, and the leader prepares with the caller's
exact absolute deadline (nanosecond equality) and the remaining budget.

`TestForwardApplyDeadlineTamperFailsSignatureWithoutConsumingNonce` — the
signature is still the 32-byte V2 HMAC over the whole payload; rewriting only the
inlined deadline while keeping the signature fails with the localized
`replication.rpc.invalid_signature` message; the rejected forgery does not
consume the nonce it borrowed, so the authentic message with that same nonce
still verifies once (no replay false positive); an authentic but already expired
deadline is authenticated, since expiry belongs to the forwarded handler.

`TestForwardApplyExpiredSignedRequestNeverInvokesPreparation` — an authentic
request whose signed budget is spent is answered on the wire
`ClusterMsgForwardApplyResponse` with `context deadline exceeded` and
`HandleForwardApply` is never called; the same connection then prepares a live
request and preserves every command field.

`TestForwardPreparationStopsOnPeerEndWithoutDeadline` (subtests `peer EOF`,
`listener owner close`) — a request with no signed deadline gives preparation no
deadline at all; closing the peer socket and closing the owning transport each
end preparation with `context.Canceled`, the handler returns, and the accepted
connection is retired.

`TestForwardApplyQueuedCallerKeepsOriginalBudget` (subtests) — a caller whose
budget expires while queued behind the connection's request gate returns
`context.DeadlineExceeded`, never reaches preparation, and does not retire the
connection; a caller admitted after a measured 200 ms wait still carries the
deadline fixed at its call (exact nanosecond equality), not a restarted budget.

`TestForwardedAcceptedWorkCompletesExactlyOnceAfterCallerCancelAndFollowerClose`
— real Raft leader and follower transports: after the accepted boundary the
follower's cancellation and the follower transport's close both leave the
accepted apply context live, and the released work reaches its durable node with
WAL position exactly 1 (the pre-existing accepted control was standalone-only).

`TestForwardConnectionServesReverseRPCWhilePreparationBlocked` — with preparation
blocked, the leader's inbound reader still serves a reverse `SendHeartbeat` on
the same connection, the peer observes the reverse request before any forwarded
reply, and the reverse response is routed to its waiting caller while
preparation is still running; the forwarded reply follows.

`TestForwardReplyFlushesBeforeNextInboundRequestIsDispatched` — a request that
the server has already read waits: after a socket-read observer proves the read
loop took the queued frame, the heartbeat handler does not run for 150 ms while
preparation is blocked; after release the forwarded reply is written first and
the queued request second, and the connection then answers the next request
immediately.

`TestForwardIdleReadDeadlineSuspendedDuringPreparationThenRestored` — with a
250 ms idle bound, preparation outlives three idle bounds without cancellation or
disconnection and its reply is still delivered; after the reply the restored
bound retires the idle connection, which the peer observes as a closed socket.

`TestForwardConnectionCloseJoinsInFlightHandler` — a handler that deliberately
ignores cancellation keeps `Close` from returning for the whole 200 ms
observation window; releasing the handler lets `Close` return successfully, and
the handler has returned by then.

## Runs

Environment for every run: `GOMAXPROCS=2 GOMEMLIMIT=1500MiB`, `-p=1`,
`-tags=noui,nolocalllm`, `-count=1`.

- `go vet -tags=noui,nolocalllm ./pkg/replication` — PASS (3.3 s).
- `gofmt -l pkg/replication/transport_forward_context_test.go` — no output.
- Focused, first run (`-timeout=6m -run '<eleven new tests>'`, not verbose) —
  exit 1, 7.2 s. Every test failed in one cleanup assertion: the listening
  goroutine's result was `context.Canceled`, because a transport's listening
  owner context is canceled before cleanup runs. The cleanup now accepts either
  documented end of the accept loop (admission stop or that cancellation); no
  behavioral assertion was involved.
- Focused, second run (`-v`) — exit 1, 10.9 s. One real defect in the test
  lane: `TestForwardPreparationStopsOnPeerEndWithoutDeadline/peer_EOF` asserted
  the handler had returned immediately after the preparation context ended. The
  handler's return is not ordered after the context cancellation, so the
  assertion was wrong, not the implementation; it now waits, bounded, for the
  handler's actual return, which is the contract being tested. Every other
  assertion in all eleven tests passed in that run.
- Focused, third run (`-v`) — PASS, 2.418 s package runtime. All eleven tests and
  the two subtests of the lifetime and queued-budget tests passed.
- Focused `-race` (`-timeout=10m`, same `-run`) — PASS, `ok ... 3.850s`
  package runtime, no data race reported.
- Complete package `go test -timeout=15m ./pkg/replication` — PASS,
  `ok ... 97.690s`.
- Earlier run logs are retained by the workspace tool
  (`workspace-1795309198997970000.log`, `...-1795309228534086000.log`,
  `...-1795309253210974000.log`, `...-1795309271703323000.log`,
  `...-1795309336612734000.log`). The linker emitted the pre-existing duplicate
  `-lobjc` warning in every run.

## Ownership review

The new file adds no production API, no exported symbol and no duplicate
behavior: it drives the current owners (`SendForwardApply`,
`RegisterClusterHandlers`, `readLoopWithContext`, `startForward`,
`awaitForward`, `dispatchRequest`, `ClusterConnection.Close`) and reuses the
package's existing helpers (`setupTestAdapter`, `publicationNodeCommand`,
`newForwardPreparationGate`, `newAcceptedApplyGate`, `startForwardLeader`,
`startForwardFollower`, `awaitReplicationValue`, `requirePreparationStops`,
`requireAcceptedDiagnosticCause`, `requireReplicationRPCError`,
`replayTestTransport`, `coverageReplicator`). Its only additions are test-local
fixtures: a recording `HandleForwardApply` owner, a wire-format peer that signs
and frames like a follower, and a socket-read observer installed through the
existing `listenNative` construction hook.

## Limits

These are focused package regressions, not release qualification. The full
repository suite, `make verify`, allocation benchmarks, conformance and the
Docker differential checks were not run. No pre-correction re-measurement of the
new tests was performed, because reverting the parent's production fix is
outside this lane's scope; the inherited fail-first evidence is the two existing
TCP forwarding tests. Tests that depend on ordering make one bounded
observation (150–200 ms) after proving the data reached the reader; the windows
are contract waits, not retries, and they are the only timed assertions besides
the actual idle/queue budgets (250 ms idle, 200 ms queue hold, 2× and 3× idle
suspension). The wire peer speaks the real frame format but is not a second
NornicDB node, so no multi-node mesh, TLS or auth-secret rotation is covered.
