# Preserve BEGIN cleanup outcomes

NOR-180's WAL terminal and transport matrices are already present in the release.
Current v1.5.1 reproduction identifies a remaining error-composition gap in native
transaction admission: when WAL setup fails and cleanup also reports a closed
transaction, only the WAL error reaches the caller. This does not establish graph
durability loss.

Use the existing CombineTerminalOutcomes owner in openTransaction's deferred
cleanup, shared by explicit BEGIN and implicit transactions. Preserve the primary
BEGIN classification and both error identities. Successful admission is unchanged;
the change runs only after setup failure. No new error renderer or helper is needed.

When namespace/configuration setup fails, the native handle must retire and its
failure must remain observable. When WAL setup and rollback both fail, both causes
must remain discoverable with errors.Is. When rollback succeeds, do not fabricate
a cleanup error. Public Execute regressions cover both transaction modes.
