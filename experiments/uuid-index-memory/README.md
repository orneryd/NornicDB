# Million-UUID identity/index memory spike — NOR-53

Date: October 22, 2026 (America/Phoenix). Base: release/v1.4.4 commit
`ba8d6d13b48452ec4e38d826869ad63a64063848`.

## Supported conclusion

The current identity dictionary's retained heap can be reduced substantially by
changing its in-memory representation while preserving the same exact string IDs
and numeric assignments. The numeric-hash byte-arena candidate reduces the measured million-ID retained heap by 35.6%. No evidence here requires a storage migration. No
production code or stored format is changed by this spike.

This is a representation experiment plus probes of real production owners, not
a million-node end-to-end database benchmark or a production implementation.
Warm/cold database query latency, WAL costs and durable database size do not
follow from these measurements. Candidate selection remains subject to the
lifetime/reclamation and integration gates below.

## Canonical owners and root cause

| Owner | Current representation / contract |
| --- | --- |
| `pkg/storage/id_dictionary.go` | 64 shards each for node and edge dictionaries; `map[NodeID]uint64` and `map[uint64]NodeID`, plus pending transaction records |
| `pkg/storage/badger_nodes.go`, `badger_helpers.go` | Direct node-body reads construct the existing Badger key; compact secondary indexes resolve numeric IDs through the dictionary |
| `pkg/storage/schema.go` | `PropertyIndex` aliases `CompositeIndex`; arity-1 values map canonical `interface{}` values to `[]NodeID`; ordered views are additional derived state |
| `pkg/storage/property_key_dictionary.go` | Per-namespace dictionaries of property **names**, distinct from property-value indexes; includes persistence bookkeeping and noted names |

`loadFromBadger` and pair insertion already share the same allocated ID string
between forward and reverse maps. There is no justified saving from merely
interning those two copies: they are already shared. Both forward and reverse
lookups are needed by current consumers. The file's introductory historical
"no reverse map" comment does not describe the current structs/methods.

The memory sources are resident hash-table capacity, pointer-bearing string
headers, ID bytes, and (for property-value indexes) interface keys and separately
allocated posting slices. Random UUID entropy does not disappear; the proposed
savings come from representation overhead, not lossy compression.

## Actual current-owner measurements

The overlay probe compiles the unchanged production package and invokes its
actual pair insertion/lookup and schema index APIs. It creates one million
`nornic:<UUID>` identifiers, releases generator temporaries, forces GC/scavenging
and subtracts the pre-fixture heap. Each owner runs in a fresh test process.

| Actual owner | Retained heap | Heap objects | GC-scannable heap |
| --- | ---: | ---: | ---: |
| ID dictionary, committed node pairs | 152.26 MiB | 1,008,746 | 106.43 MiB |
| One unique-valued property index | 172.20 MiB | 3,004,164 | 123.25 MiB |
| Property-name dictionary, one million distinct names | 205.39 MiB | 1,012,332 | 159.53 MiB |

All one million identity pairs are checked in both directions; property-value
lookups are sampled every 997 entries. Property-name hydration uses the real
struct/maps under its lock, not its transaction allocator: this isolates committed
resident representation cost. Its pending/noted populations are not measured.
A million distinct property names is an adversarial cardinality fixture, **not**
an assertion that normal fixed-schema graphs allocate one name per node.

These measurements are separate, not additive full-database totals: real owners
can share string bytes and other state. The property index fixture does not build
its optional sorted-key view or model multiple indexes. Sampled pprof tables
identify allocation owners; they are not exact replacements for heap deltas.


A second real-owner probe inserts and verifies one million pairs, removes **all**
of them through `nodePairRemoveByID`, confirms every forward/reverse map is empty,
and forces GC/scavenging. It still retains **106.46 MiB** above the
pre-fixture heap. This directly reproduces idle retained capacity without any
node bodies or embeddings. It is a dictionary-owner fixture, not an assertion
that every database deletion immediately removes its retained identity/history.
See [`results/storage-idle-after-delete.txt`](results/storage-idle-after-delete.txt).

## Representation experiments

All alternatives are research-only, outside the database module. The baseline
reproduces the committed 64-shard map-pair shape and publication lock order;
empty pending maps and the unused edge dictionary are omitted. Compare it with
the actual-owner result above to establish its scope. No node bodies, vectors,
Badger cache, durable files, input array or returned result corpus is retained.

- **Paged reverse:** keep the forward string map, replace the reverse map with
  pages of string headers indexed by numeric ID.
- **Packed bytes + paged reverse:** retain exact variable-length ID bytes once in
  a per-shard arena, with an open-addressed forward offset table and numeric
  reverse pages. Every candidate compares complete keys on hash collisions.
- **Packed bytes + numeric reverse hash:** replace reverse pages with
  `map[uint64]uint64` references. Memory depends on entries, not the numeric-ID span.
- **Append-only string arena + numeric reverse hash:** generate the same candidate
  using standard `strings.Builder`, letting reverse lookups return exact immutable
  substrings without copying. No project-owned unsafe code or dependency patch.

These prototype alternatives are experiment controls, not production feature
flags or parallel query paths. A future approved representation must replace the
single owner's internals while preserving its callers and contracts.

### Repeated dense corpus results

Three fresh processes per cell; medians with min/max available in
[`results/summary.json`](results/summary.json). MiB means 1,048,576 bytes.

| Layout | Retained MiB | Scannable MiB | Objects | Forward hit ns | Reverse hit ns | Reverse bytes/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Current map-pair shape | 152.23 | 106.39 | 1,008,493 | 507.2 | 100.1 | 0.000 |
| Packed bytes + numeric hash | 98.04 | 0.10 | 4,474 | 535.9 | 400.0 | 48.000 |
| String arena + numeric hash | 98.06 | 0.10 | 4,481 | 424.2 | 148.9 | 0.001 |
| Forward map + reverse pages (rejected) | 117.07 | 71.25 | 1,005,556 | 476.8 | 125.3 | 0.000 |
| Packed bytes + reverse pages (rejected) | 70.06 | 0.05 | 1,467 | 572.3 | 445.2 | 48.000 |

The measured heap includes both lookup directions, reverse directory/hash maps,
arena capacity slack, records and hash slots. It is not just UUID payload size.
`scan_heap_bytes` measures Go's scannable heap metric, not GC pause time.
Byte-arena reverse results allocate copies; string-arena results alias immutable
backing storage and therefore introduce the lifetime issue below.

### Sparse numeric IDs and churn

The sparse fixture uses 4,096 entries assigned `i * 65537 + 1`; it does not allocate
a 1M-entry sparse page table that could exhaust the host.

| Layout | 4,096 sparse entries: retained MiB |
| --- | ---: |
| Current map-pair shape | 0.62 |
| Packed bytes + numeric hash | 0.46 |
| String arena + numeric hash | 0.45 |
| Forward map + reverse pages (rejected) | 72.55 |
| Packed bytes + reverse pages (rejected) | 32.46 |

This rules out direct paging as a universal replacement. Numeric assignments can
have holes; a clean design cannot quietly change assignments or rely on dense
high-water counters. The hashed reverse candidate preserves sparse IDs without
an allocation proportional to the gaps.

| Layout | After deleting half, MiB | After replacement inserts, MiB | After serial rebuild, MiB | Median process peak, MiB |
| --- | ---: | ---: | ---: | ---: |
| Current map-pair shape | 129.35 | 152.39 | Not run | 305.24 |
| Packed bytes + numeric hash | 98.05 | 129.56 | 98.06 | 331.83 |
| String arena + numeric hash | 98.06 | 129.60 | 98.07 | 355.12 |

Churn removes every other ID, confirms deletion, inserts a different exact ID at
the same numeric assignment and checks all pairs again. The serial rebuild copies
only live pairs, checks all rebuilt pairs, and measures its retained heap and
process peak. It is a research operation, not a proposed background job.
`VmHWM` includes build, checks, churn and rebuild; it is a whole-process peak,
not a precise simultaneous-live-dictionary counter. Returned strings kept by
external consumers are **not** retained in this fixture.

## Compatibility and performance evidence

Fixtures cover lower/upper-case UUID spellings as distinct IDs, unhyphenated
strings, arbitrary text, namespaces, Unicode, embedded NUL, long strings, duplicate
insertion, hash collisions, sparse IDs, removal/reuse, and disjoint concurrent
mutations. Supported public node IDs are nonempty; the experiment's paged control
uses empty string as an absence marker and is not a general string interner.

The Go race detector passes for the experiment fixtures. Existing identity and
property-key dictionary tests are run independently against the unmodified
production package. These establish the existing baseline, not candidate
transactional correctness. They do not replace implementation-level concurrency,
persistence, rollback, recycling, repair or restore tests.

Forward and reverse timing samples are 262,144 requests against an independently
generated 8,192-key hot sample drawn from the full corpus. Misses prepend a distinct
prefix. Query generation is outside timed loops. Timings include the interface
call and verification branch, and are **not** uniformly cold million-key random
lookups. Builds/deletes/reinserts include deterministic ID generation. This shared
host has substantial timing variability; the raw ranges are reported without a
performance pass or claimed throughput improvement. No controlled production
performance budget has passed.

## Architecture decision and next gate

1. **Keep every durable byte and public identity contract unchanged.** The current
   durable forward/reverse entries already contain everything needed to reconstruct
   a compact memory view. Reuse existing numeric IDs, counters and recycling rules.
   No migration is currently justified.
2. **Keep ownership in `idDictionary`.** Evaluate one canonical compact pair store
   behind its existing node/edge operations, preserving lock/publication and pending
   transaction semantics. Do not add codecs to Cypher callers, new lookup modes,
   a second ID dictionary or UUID-only execution paths.
3. **Reject sparse-sensitive reverse paging.** The numeric reverse hash control
   demonstrates that the savings need not depend on dense numeric IDs.
4. **Do not ship either growing arena prototype.** The byte version copies reverse
   strings; the string version can pin a whole backing buffer when a caller retains
   one substring. Deletions leave dead records; rebuilding temporarily retains old
   and new state. Both prototypes also have a 32-bit per-shard offset limit that
   must not become a new product limit. A production design needs a single segmented
   immutable record store with explicit bounded reclamation and compatible handle
   widths, with measured retained references and growth/reclamation peaks. This is
   memory management in the canonical owner, not a storage migration. The empty-map
   reproduction also makes capacity reclamation an explicit requirement: merely
   reducing live UUID bytes does not solve idle retained hash-table capacity.
5. **Treat property names and value postings as separate causal owners.** A UUID
   dictionary change cannot be claimed to fix `CompositeIndex` allocations. Reuse
   existing numeric identity for postings only after measuring the complete combined
   cost and exact ordering/value semantics; do not create another intern table or
   branch queries by value type. No property-index replacement was implemented or
   proven by this spike.
6. **Review these findings with Tim before implementation.** Next evidence must
   include controlled latency under reads/writes, retained returned IDs across growth,
   repeated churn, safe reclamation and parallel publication. Then test the actual
   storage consumers, old database reopen, transaction rollback/commit, numeric
   recycling, repair and backup/restore. Finally profile real million-node graphs
   with realistic edges/indexes; report total process memory and disk bytes.

## Reproduction

Use Go 1.26.9, Linux/amd64, standard toolchain; no new runtime dependencies.
Record CPU/toolchain/environment with each run. `run.py` sets GOMAXPROCS=4,
GOGC=100 and GOMEMLIMIT=off; each measurement is a separate sequential process.

From this directory:

```sh
go build -o /tmp/uuid-memory main.go
go test -race -count=1 -timeout=30s -v
python3 run.py --binary /tmp/uuid-memory --modes baseline,packed-hash --output verified.jsonl
python3 rejected.py --binary /tmp/uuid-memory
python3 builder_variant.py --go /absolute/path/to/go --directory /tmp/uuid-builder
python3 run.py --binary /tmp/uuid-builder/uuid-memory --modes packed-hash --output builder.jsonl
python3 summarize.py
python3 probe.py --go /absolute/path/to/go --mode dictionary --profile /tmp/dictionary.pprof
python3 probe.py --go /absolute/path/to/go --mode property --profile /tmp/property.pprof
python3 probe.py --go /absolute/path/to/go --mode property-names
python3 probe.py --go /absolute/path/to/go --mode dictionary --delete-all
```

The overlay test has no permanent entry under `pkg/storage`; its `.go.txt` source
is mapped there only by the temporary Go build overlay. All experiment source is
isolated from `go test ./...` and production imports by its own module boundary.
Only the standard-library builder implementation is used; third-party source is
unmodified. Reproducing changes the result files; preserve prior evidence first.
