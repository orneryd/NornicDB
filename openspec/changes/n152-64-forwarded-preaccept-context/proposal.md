# Forward the caller's bounded context to preaccept (N152-64)

## Required outcome

A forwarded write preaccept must inherit the originating caller's signed
absolute deadline and the forwarding connection's lifetime. Once the leader
crosses the accepted boundary, the durable work stays owned by the replicator:
the caller's cancellation, its spent budget, or the follower connection ending
cannot cancel, discard or repeat it. The leader's inbound reader must keep
serving the connection while a forwarded preparation runs, so a reverse RPC on
the same connection is still answered, the next inbound request keeps its order
behind the forwarded reply, and the idle read timeout stays an idle bound rather
than a preparation bound. Connection shutdown joins the one enrolled forwarded
handler.

## Scope

Implementation (already applied by the parent in this worktree, not part of the
unit-test lane):

- `pkg/replication/transport.go`: `SendForwardApply` establishes the caller's
  budget before serialization or the connection's request gate and forwards a
  `forwardApplyRequest` carrying the absolute deadline; the read loop dispatches
  `ClusterMsgForwardApply` asynchronously through one enrolled handler, routes
  pending reverse-RPC responses first, awaits the active forward before the next
  inbound request, and shares the reply publication path; the idle read deadline
  is suspended while a forward is active and restored afterwards.
- `pkg/replication/handlers.go`: the forwarded handler decodes
  `forwardApplyRequest`, applies the signed deadline to the preparation context,
  and never calls `HandleForwardApply` for an already spent budget.
- `pkg/replication/transport_forward.go`: the payload keeps `Command` inlined
  (`msgpack:",inline"`) so an old reader still decodes it, owns the shared
  request dispatch and reply publication, and owns the active-forward state.

Test lane (this change): `pkg/replication/transport_forward_context_test.go`
plus these OpenSpec records. No production behavior is changed and no assertion
is weakened.

## Evidence summary

Eleven focused regressions cover the payload contract in both compatibility
directions, every durable command field over real TCP, the V2 signature over the
whole payload with a tampered inlined deadline rejected as a forgery (and the
borrowed nonce left unconsumed, so the failure is never mistaken for replay), an
authentic but spent deadline kept out of preparation, no invented deadline plus
cancellation on peer EOF and on owner close, the caller's original budget kept
across a queued wait, accepted durable work completed exactly once after the
caller cancels and the follower transport closes, a reverse RPC answered while
preparation is blocked, inbound order behind the forwarded reply, the idle read
deadline suspended then restored, and connection `Close` joining the in-flight
handler. See `evidence.md` for the exact commands, runtimes and skips.
