# Refresh reconciliation source review

CI43197244835 capturecensus fails only TestStorageReconciliationRecords:
notifyNodeMutated, notifyEdgeMutated, removeNodeFromSearch and
refreshSearchAfterCommit changed source after their explicit-context migration.
The complete capture-census job reports no other failing test.

The parent and independent read-only reviewer inspected the four complete
current bodies and their refreshSearch/RefreshIndexes consumers. Each receives
an existing caller context; none creates Background, TODO or another fresh
context. The admitted native commit/completion still uses its accepted lifetime.
Refresh attribution reads only metadata from the original scope, then actual
storage reads use the captured search service lifetime. No new asynchronous
context capture or retained statement values were introduced. The node mutation
callback is invoked in its existing conditional branch and now reports its
actual opaque invocation; its direct-call inventory is updated accordingly.

Update exactly these four existing retirement source digests and their factual
call/rationale fields. Verify raw function-byte SHA256 independently against CI
and preserve the existing zero-fresh-context proofs, record population, policies,
provider rules and analyzer. These records are tombstones for retired fresh
context ownership, not acceptance of the replacement path. The normal compiled
census still checks every current flow.

Runtime controls remain the isolated 2e280ce03 focused package pass and integrated
race/source-lifetime proof. Full compiled census revalidation is left to CI as
requested. No full local census pass is claimed.
