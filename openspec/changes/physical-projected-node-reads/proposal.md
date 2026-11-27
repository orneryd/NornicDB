# Opt-in physical projected node reads

## Problem

PR #1826 currently narrows property deserialization but still calls Badger
`Item.Value`, which reads the entire value-log record or reconstructs the
entire SST block. A newly created encrypted node with a 2 MiB unrelated
property reads 2,097,422 logical value-log bytes when the caller requests
only a small scalar. The current experimental column layout groups bytes for
compression but is not physically addressable by property.

## Contract

`CREATE DATABASE name OPTIONS {projected: true}` opts a new persistent,
isolated database into an additive Badger format with a durable per-node,
per-version property-key-ID locator. `projected` is immutable after creation
and defaults to `false`. Existing stores, the standard layout and ordinary
Badger APIs retain their current behavior and bytes unless the new option is
selected. The option is independent of the existing `layout` choice.

The locator is a compact sorted directory of `(property ID, relative value
offset, length, checksum)` alongside the node's Badger value. The physical
address is the current Badger value pointer plus that relative offset. Badger
already changes the value pointer when it relocates a value during GC and
changes SST references during compaction; the directory travels with the
value and is regenerated when a value is rewritten. No long-lived raw file
address or per-read map is stored in NornicDB. The directory and requested
regions have checksums so the selected-read path preserves Badger's current
accidental-corruption detection. This is not a claim of cryptographic
authentication: Badger's existing AES-CTR value-log records are not AEAD.

The fork's bounded value-range API is the physical read primitive. Its
plaintext path borrows only selected mmap bytes; its encrypted path advances
the existing AES-CTR stream to the selected region and allocates/decrypts
only those bytes. Whole-record checksum mode keeps its full-read behavior;
the projected record's own per-region checksums cover the selective mode.
Small values that Badger stores inline in an SST may still load their block;
we must measure whether forcing them into the value log yields a net benefit
before changing the placement policy.

When a query proves a finite property demand, the opted-in store reads and
decodes only the node metadata and requested property regions. Full-node
consumers still receive complete values. A locator remains valid across
flush, compaction, value-log relocation, reopen, backup/restore and WAL or
snapshot recovery. Badger owns physical offsets and rebuilds them whenever
it rewrites the containing storage unit. NornicDB supplies property-key IDs,
not filesystem offsets.

## Performance evidence and gate

Current Mac M2 Max warm-device benchmark, experimental layout, 2 MiB random
unrelated property, node cache evicted each operation:

- Plaintext: 3,882–4,780 ns/op, ~3.5 KiB/op, 54 allocations.
- Encrypted: 385,600–419,829 ns/op, ~2.11 MiB/op, 58 allocations.
- Badger's value-log counter reports 2,097,416 logical bytes/op in both
  cases. It is not a physical disk-byte counter; the encrypted path copies
  and decrypts the entire value.

Compare matched before/after cold and warm projected reads, full reads,
single and bulk writes, updates, compaction, total bytes, allocations,
retained memory and p95/p99 latency. Include plaintext and encryption,
small and large properties, compressible and incompressible values, and
concurrent readers/writers. A new path that merely saves deserialization
while moving the cost to write latency or retained RAM does not satisfy this
change. The default-off path must remain within measurement noise.

## Out of scope

- Retrofitting physical locators into existing databases.
- Changing Cypher semantics, FGAC visibility, ordinary Badger APIs or the
  property-key dictionary's name/ID authority.
