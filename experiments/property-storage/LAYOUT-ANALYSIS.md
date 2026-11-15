# Layout inspection and per-entity property lookup

`layout/main.go` opens closed stores read-only, counts live KV families, examines
current node property frames and reads manifest-selected SST indexes. It reports
hashes/statistics, not property contents. The scan includes system metadata nodes.
Its `northwind` property bucket means non-document nodes, including those few
system nodes. Historical-node decoding is supported, but these insert-only stores
have no live `0x0c` node-history values: this receipt cannot quantify history reuse.

## Measurements

- All 4,352 document nodes have six properties. Northwind application nodes have
  mostly five or eight. Empty/small edge maps need an equally cheap representation.
- Document bodies: 53.97 MiB logical typed values, 19.34 MiB property frames;
  272 distinct bodies. This repetition follows the fixture's sixteen replicas.
- Random binary: 17.01 MiB, 4,352 distinct values, 7.99996 bits/byte across encoded
  bytes. It remains ordinary MessagePack instead of being expanded by compression.
- Metrics: 9.58 MiB logical, 1.15 MiB encoded, 61 distinct arrays. These low-range
  arrays are synthetic; a global dictionary cannot be justified from them alone.
- 31,737 column-transposed blocks occupy 40.45 MiB and save 4.66 MiB against
  plain Zstd/raw on the exact same reconstructed blocks, at Zstd level one.
  This is an incremental codec comparison, not a full rewrite or latency result.
- 669 transposed blocks save at most 1%, just 6,251 bytes in total. That is a
  possible decode-CPU tradeoff to measure; no codec threshold was changed here.
- Inbound adjacency values repeat the corresponding outbound values: 14.86 MiB
  of logical repeated payload. These bytes are already block-compressed; removing
  one orientation would add read indirection and is not automatically beneficial.

Logical duplicate bytes and entropy are not estimates of recoverable physical
space. SST compression, versions, indexes, key boundaries and read locality matter.

## Per-node/per-edge KV design

A specialized map must belong to the individual entity version. Nodes and edges
can have unrelated keys and types; namespace name tokens are only key identifiers,
not a fixed schema. Preserve the existing public arbitrary property map.

Use Badger's immutable sorted-entry ideas: compact tokens, restart points for
wide records, and payload spans. Do not instantiate a DB, memtable, table builder
or skiplist per entity, and do not turn each requested property into another
transaction.Get. Those choices add allocation, seeks and fan-out to point reads.

A sorted KV directory still needs offsets or lengths to locate payloads. The
fork had an NPR1 prototype with a per-record sorted directory, but its
12-byte header and 24-byte entry cost 156 bytes for a six-property record before
values. The current tokenized list has much smaller key overhead. NPR1 never entered the physical Nornic path and was removed: its overhead did not help fix the measured regression.

First remove unnecessary payload copies from the current token walk. MessagePack's
Skip reads omitted strings/binaries/extensions into decoder scratch. A bounded
seek over those length-framed values can preserve the wire format, per-entity
key flexibility and strict decoding of selected values. Nested containers retain
the canonical walk. This runtime experiment uses the existing default-off flag.

Only if CPU profiles still show property enumeration as significant should a
compact record-local KV directory compete against this token walk. Compare six,
eight, thirty-two and wider independent key sets; selected/absent keys; large
random and compressed values; points, batches and streams; and concurrent reads
and writes. Keep snapshot authority, typed values and caller-owned result data.
Gate acceptance on p95/p99 and throughput before size or memory.

Even a perfect directory inside a compressed SST block cannot avoid decompressing
that block. True byte-range I/O needs independently addressable storage ranges or
blocks. It must be measured against the additional index and lookup costs.

Avoid global content-addressed blobs for now: fixture body reuse is high, but a
hash lookup per requested property could sacrifice the user's first priority.
Reuse immutable encoding buffers across write representations before introducing
new read indirection, and quantify real update/history workloads separately.