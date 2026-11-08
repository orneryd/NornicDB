# NOR-413: CALL reads and warmed node caches

The original reproduction is an auto-commit property read containing an inline,
read-only `CALL { ... }` subquery over nodes with stored chunk vectors. The
statement does not consume those vectors. PR #1755 removes the blanket CALL
refusal only for that class of statement, using the existing statement-effect
owner. Procedures and external native plugin functions keep full reads because
they can consume vectors or return full nodes that meet MATCH rows.

The remaining cache defect is independent of statement classification: a full
read populated the full-node cache, and `GetQueryNode` subsequently copied the
entire cached vector payload. The fix selects the lightweight query image in
both point and label reads. Its first miss decodes the body and embedding
metadata without loading separately stored chunks. Full reads continue using
the full image and never borrow a partial query image.

Full and query images share an estimated byte budget, 256 MiB per Badger engine
by default (`BadgerOptions.NodeCacheMaxBytes` for embedded configuration). This
includes property, label, embedding-metadata, chunk-vector and named-vector
estimates. The existing entry-count bounds still apply. Admission evicts
individual images, and a single image exceeding the budget is not copied into
the cache. Writes, deletes, prefix cleanup, restore and close maintain the same
accounting owner. This is a cache estimate, not a process/RSS or total database
memory limit; the separate versioned body cache retains its own existing budget.

Point and scan decoders bind embedding provenance through the same helper.
That preserves node-image parity for native consumers when a full image is too
large to cache. Full-vector results, stored vectors, and separately managed
embedding writeback remain authoritative in storage. Inline vectors and pending
write-behind images retain their existing public read behavior.

## Reproductions and controls

- On unchanged release storage code, eight warmed query reads of a node with
  128 chunks of 2,048 floats allocate 8,420,864 bytes, exceeding the regression's
  512 KiB bound. Both body-overflow and worker-sidecar cases return stored chunks
  from the warmed full cache. The new cache selection fixes both.
- `TestNOR413CallSubqueryAfterFullReadsKeepsAllocationBound` warms full reads
  before executing the reported CALL-subquery chain, asserts the rows and the
  allocation bound, then verifies all chunks through full reads again.
- Cache-pressure coverage inserts more vector data than a 96 KiB test budget,
  checks raw retained vector payloads as well as accounting, and verifies restore
  releases the budget. Replacement/generation controls prevent stale images from
  returning after a write.
- Existing native procedure/plugin, vector-search membership, writer, Fabric,
  Bolt/HTTP, named-vector streaming and embedding-writeback regressions remain
  required controls. This change does not classify READ procedures as vector-free.

## Fresh local benchmark samples

Go 1.27.3, Windows/amd64, Intel Core i9-9900KF, `GOMAXPROCS=2`,
500 ms samples. Baseline storage is byte-identical to original PR head
`23ad2767072f46a4a36a9fde05bed957de7db0cb`; runtime candidate is
`444cc0950f36f37c854d2bebe73012358bc850df`. The first three samples:

| Warm read | Baseline ns/op | Candidate ns/op | Baseline B/op | Candidate B/op | Allocs baseline to candidate |
| --- | ---: | ---: | ---: | ---: | ---: |
| Metadata, 1 MiB stored chunks | 164,131-199,748 | 493-497 | 1,052,608-1,052,609 | 832 | 135 to 6 |
| Full vectors, same payload | 167,140-185,440 | 179,068-192,794 | 1,052,609-1,052,610 | 1,052,609-1,052,610 | 135 to 135 |
| Ordinary non-vector cache hit | 319-328 | 326-330 | 496 | 496 | 4 to 4 |

These descriptive samples establish the allocation change. Full-vector timing
ranges overlap; this is not a statistical claim about small latency changes.
Static analysis was also running on the host, so timings should not be used as
production capacity estimates. Additional alternating baseline/candidate rounds
are retained with the verification receipts.

Focused storage coverage exercises the new estimation, removal, reset, cache
selection and source-binding helpers at 100%; cache admission is 91.3% and the
configured-limit/default helper 66.7%. The focused run's package-wide coverage
is 20.3%; that is not a claim of full storage-package coverage.

## Verification limits

This report distinguishes current candidate checks from the earlier Linux lab
receipts in the PR discussion. The unavailable original application workload and
live-server heap were not replayed. Local tests use Go 1.27.3 on Windows/amd64,
`GOMAXPROCS=2`, CGO and `noui`; no `nolocalllm` tag is used.

The release's four `TestEmbeddingFree*` wrapper cases fail with `invalid data`
on unchanged storage source as well as the candidate. They are retained failures,
not a passing full-suite claim. Required final CI and Graphify results are linked
from PR #1755's latest status comment.
