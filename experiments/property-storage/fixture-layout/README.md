# Comparable read fixtures

Record LSM table placement before comparing read latency. A body-cache hit can
still require an index lookup: several overlapping L0 tables and one compacted
non-overlapping level do different work. Matching rows and feature flags alone
does not establish comparable physical preparation.

`main.go` defaults to read-only inspection. `-mode settle` runs Badger's supported
`Flatten(4)` operation through a Nornic engine on an **offline copy**. It keeps the
comparison's table encoder and field-layout annotation. The engine owns close;
the tool performs no application writes.

Build the same source against each comparison module, using Go's file-list build
from that module's working directory:

```sh
cp experiments/property-storage/fixture-layout/main.go /tmp/fixture-layout.go
cd /baseline
GOWORK=off go build -tags noui,nolocalllm -o /tmp/fixture-stock /tmp/fixture-layout.go
cd /src
GOWORK=off go build -tags noui,nolocalllm -o /tmp/fixture-new /tmp/fixture-layout.go
```

`prepare.py` takes both binaries and the four frozen input directories, copies
them beneath `--copy-root`, settles the copies, checks paired level sets and saves
their levels, key counts and byte sizes in `--receipt-dir`. It sets the existing
experimental flag only for the new engine. Input stores remain unchanged.

`Flatten` consolidates tables into one level; that level need not be L6. The
receipt records what happened. Table counts may differ because compression changes
table sizes. Verify all corpus fields after preparation, before timing.

Pass `--fixture-receipt <receipt-dir>/metadata.json` to `projected-query/run_lab.py`.
The runner checks that both selected source paths belong to that receipt and
their recorded settled levels match. It embeds the receipt in the saved benchmark
metadata. Each trial still opens another copy of the prepared source.

Settled read measurements do not qualify ingestion, startup compaction, churn,
value-log GC, crash recovery or release readiness. Keep competing builds, tests
and profilers outside timing runs.
