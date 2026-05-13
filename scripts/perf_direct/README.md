added a **direct engine latency benchmark** that bypasses Bolt/HTTP entirely.

### New test

`~/src/NornicDB/scripts/perf_direct/main.go`

### How to run

Direct Badger (disk writes, no protocol overhead):

```
go run ./scripts/perf_direct --clean --iterations=100 --mode=badger
```

Async engine (shows cache‑first writes + explicit flush timing):

```
go run ./scripts/perf_direct --clean --iterations=100 --mode=async
```

If you want worst‑case disk latency (fsync on every write):

```
go run ./scripts/perf_direct --clean --iterations=100 --mode=badger --sync-writes
```

### What it measures

- Single node create
- Batch node create (`BulkCreateNodes`)
- Node update
- Relationship create
- (Async mode only) flush duration

I also updated the plan doc to reference this new direct‑engine benchmark command.

Want me to add a matching shell wrapper (like the Bolt/HTTP scripts) or collect results into a CSV?
