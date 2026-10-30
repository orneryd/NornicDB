# pprof Quick Guide

This guide covers the pprof surface NornicDB exposes when the opt-in listener is enabled, including CPU, goroutine, mutex, and block profiles.

Enable the listener explicitly:

```bash
export NORNICDB_PPROF_ENABLED=true
export NORNICDB_PPROF_LISTEN=127.0.0.1:9091
```

For Docker or headless container profiling, publish the pprof port and bind the listener inside the container:

```bash
docker run --rm \
   -p 7474:7474 \
   -p 7687:7687 \
   -p 9091:9091 \
   -e NORNICDB_PPROF_ENABLED=true \
   -e NORNICDB_PPROF_LISTEN=0.0.0.0:9091 \
   nornicdb-amd64-cpu-headless
```

Available endpoints include:

- `/debug/pprof/profile`
- `/debug/pprof/goroutine`
- `/debug/pprof/mutex`
- `/debug/pprof/block`
- `/debug/pprof/heap`
- `/debug/pprof/allocs`

When pprof is enabled, NornicDB also turns on mutex and block profiling so `/debug/pprof/mutex` and `/debug/pprof/block` have useful data immediately.

## You're in pprof interactive mode - here's what to do:

### Step 1: Find Top CPU Consumers

```pprof
(pprof) top10
```

This shows the top CPU consumers. Look for:

- HNSW and search functions
- GC-related functions such as `runtime.gcBgMarkWorker`
- vector operations
- lock-heavy code showing up in sampled CPU work

### Step 2: Check the Full Call Chain

```pprof
(pprof) top20 -cum
```

Use cumulative time to find the higher-level path that leads into the hotspot.

### Step 3: Check for GC Overhead

```pprof
(pprof) top20 | grep -E "(gc|runtime)"
```

Look for:

- `runtime.gcBgMarkWorker`
- `runtime.mallocgc`
- high runtime percentages indicating GC or allocation pressure

### Step 4: Inspect Goroutine Growth

Use the goroutine profile when queues are growing, requests look stuck, or background work appears to stop draining.

```bash
go tool pprof http://127.0.0.1:9091/debug/pprof/goroutine
```

Inside pprof:

```pprof
(pprof) top20 -cum
(pprof) traces
```

What to look for:

- many goroutines blocked in the same stack
- repeated wait paths in flush, storage, or search work
- channels, mutexes, or I/O waits dominating the traces

For raw stack dumps instead of pprof's aggregated view:

```bash
curl http://127.0.0.1:9091/debug/pprof/goroutine?debug=2
```

### Step 5: Focus on a Specific Function

```pprof
(pprof) list searchWithEf
```

Shows line-by-line sampled time inside the selected function.

### Step 6: Check Mutex Contention

```bash
go tool pprof http://127.0.0.1:9091/debug/pprof/mutex
```

Useful commands:

```pprof
(pprof) top20
(pprof) top20 -cum
(pprof) list <function>
```

What to look for:

- `sync.(*Mutex).Lock`
- `sync.(*RWMutex).Lock`
- `sync.(*RWMutex).RLock`
- lock-heavy stacks in storage, cache, search, or knowledge-policy flush paths

### Step 7: Check Blocking Waits

```bash
go tool pprof http://127.0.0.1:9091/debug/pprof/block
```

Use the block profile when the process is not CPU-hot but requests or background work still look stalled.

### Step 8: Generate Visual Reports

```pprof
(pprof) web
```

Or generate SVG:

```pprof
(pprof) svg > /tmp/profile.svg
```

### Step 9: Exit and Use the HTML UI

```pprof
(pprof) exit
```

Then generate a web UI:

```bash
go tool pprof -http=:8080 cpu.prof
```

## Quick Commands Reference

| Command | Purpose |
|---------|---------|
| `top10` | Top 10 consumers |
| `top20 -cum` | Top 20 with cumulative time |
| `list <function>` | Line-by-line breakdown |
| `traces` | Stack-oriented goroutine or blocking analysis |
| `web` | Visual call graph |
| `svg > file.svg` | Generate SVG graph |
| `png > file.png` | Generate PNG graph |
| `help` | Show all commands |
| `exit` or `quit` | Exit pprof |

## What to Look For

### GC Problems

- `runtime.gcBgMarkWorker` > 10% of total time
- `runtime.mallocgc` in top functions
- frequent runtime-heavy paths in profiles

### Allocation Hotspots

- functions with high `flat` time that allocate
- `make()` in hot paths
- `append()` without pre-allocation

### Lock Contention

- `sync.(*RWMutex).RLock` or `sync.(*Mutex).Lock` high in mutex profiles
- cumulative time dominated by lock acquisition
- lock-heavy call chains in storage or background work

### Goroutine Pressure

- large numbers of goroutines in identical blocked stacks
- repeated background-worker stacks that do not drain over time
- blocked flush, search, or storage work paired with growing queues

### Knowledge Policy Hot Paths

Correlate pprof with these telemetry signals:

- `nornicdb_knowledge_policy_access_flush_duration_seconds`
- `nornicdb_knowledge_policy_access_flush_buffer_fullness`
- `nornicdb_knowledge_policy_suppressions_total`
- `nornicdb_knowledge_policy_reconcile_total`

If flush duration rises and mutex or block profiles show heavy waiting, investigate the access flusher, suppression-recheck path, or downstream storage writes first.

## Next Steps After Profiling

1. **If GC is the problem:**
   - check memory profile: `go tool pprof -alloc_space mem.prof`
   - look for allocation hotspots
   - apply pooling or reuse strategies, then re-profile

2. **If allocations are the problem:**
   - use `list <function>` to find exact lines
   - add pools or pre-allocate buffers
   - re-profile to verify the improvement

3. **If locks are the problem:**
   - reduce lock scope
   - prefer read-only paths where possible
   - only consider lock-free redesigns when profiling proves they are necessary

4. **If goroutines are the problem:**
   - inspect repeated blocked stacks
   - correlate with queue depth, flush duration, and suppression metrics
   - verify that the issue is a real backlog and not expected background concurrency

