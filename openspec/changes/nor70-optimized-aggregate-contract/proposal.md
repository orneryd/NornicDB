# NOR-70: one Cypher execution API

Remove the standalone query pattern detector and executor. Call StorageExecutor.Execute for grouped relationship aggregates. Preserve typed aggregates, complete columns and rows, null-only groups, ordering, pagination, and explicit transaction behavior. The canonical executor already implements these semantics.
