# Parent review and bounded acceptance

Base: `18b36a85d10e775b9d5d3500ecf3482682754a8d`.

The original real-TCP cancellation and deadline reproductions failed on the base.
The corrected path preserves the signed absolute budget in the flattened payload,
links preparation to the connection/listener lifetime, and enrolls one asynchronous
forward handler in the connection's existing WorkLifetime. The reader can observe
EOF and route reverse RPC responses. A subsequent request waits for the prior reply
flush; the idle read deadline is suspended only while the forward is active.

The accepted storage/Raft owner is unchanged. Independent tests cover real-TCP
accepted work surviving both caller cancellation and follower transport shutdown,
with exactly one WAL publication. Old Command decoders read the flattened request;
new decoders accept the old bare command with no invented deadline. The existing
HMAC authenticates the deadline as part of the payload. An earlier nested-wrapper
compatibility probe and unsigned-deadline-only proposal were not adopted.

Parent reviewed every new test and corrected cleanup ordering in the Close-join
regression: its non-cancelable test handler must be released before transport
cleanup joins it even if an assertion exits early. After that change, independently
ran the forward/accepted/all-RPC focused population under `-race`: PASS, 4.161s.
Worker ran the full replication package: PASS, 97.690s; focused race PASS, 3.850s;
vet PASS. These are `noui,nolocalllm` runs, GOMAXPROCS=2, GOMEMLIMIT=1500MiB, -p=1,
-count=1. No repository-wide or release-platform qualification is claimed.

## Owner and duplicate review

- `exchangeClusterRPC` remains the sole request encode/exchange/decode owner.
  `SendForwardApply` only establishes and carries its budget before gate waiting.
- The original synchronous reply block was moved, not copied, into
  `dispatchRequest`; both ordinary and asynchronous forward dispatch use it.
- Existing `lifecycle.WorkLifetime` admission/join and `AfterFuncJoined` cancellation
  are reused; no new lifecycle implementation or accepted-write settlement owner.
- `forwardApplyRequest` adds wire-only metadata; durable Command, HMAC version,
  storage acceptance and `RaftReplicator.HandleForwardApply` are unchanged.
- The existing staged duplicate checker was run in this worktree. It reported
  incomplete advisory analysis because `/db/nornicdbcode/tx/commit` timed out.
  Manual source/caller comparison above is complete for the changed owners, but
  graph-backed duplicate discovery remains unresolved; do not call it a clean pass.

## Mixed ticket scope

This PR addresses forwarding preparation only. The ticket's work-census receiver
population and field-selector concerns remain separate. Existing field selector
unit tests explicitly count declarations, so changing it to uses requires contract
resolution; `minimum_members` was not an approved new requirement. Preserve those
items in Plane rather than closing the whole ticket when this PR merges.
