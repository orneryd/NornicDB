# Stored relationship snapshot transcoding: PR #1541

Baseline: release/v1.4.4 `02d0d727f21869766dea106c678794bda25edd7f`, after #1536.
Candidate: the integrated PR #1541 source, with the compact-header and projected
relationship decoder callers converged before measurement. Final commit and
frozen test receipts are recorded in the PR verification comment.

## Workload and measurement

Apple M2 Max, Darwin arm64, Go 1.26.9, benchmark default 12 logical CPUs. The same
portable `BenchmarkSnapshotStoredGraph` fixture is compiled into both binaries:
30,000 persisted nodes and 300,000 persisted relationships, twelve relationship
properties, hidden relationships included. Pressure adds 24 million pointer-rich
objects and a Go memory limit at six percent above measured live heap usage.
Measured pressure live heap was approximately 1,652 MiB on both sides.

Four fresh processes in baseline/candidate/candidate/baseline order, one operation
per normal and pressure subbenchmark, CPU profiles enabled for every process.
Fixture setup is excluded from benchmark timing and allocation reporting. CPU
profiles include setup; snapshot analysis filters `writeSnapshotRecords`,
`snapshotEdgeTranscoder` and `snapshotFrameWriter` stacks. Concurrent source audit
work can affect timing. These are two samples per side, not a production latency
qualification or a statistical variance estimate.

| Median per snapshot | Release baseline | Candidate | Change |
| --- | ---: | ---: | ---: |
| Normal wall time | 1.141 s | 0.799 s | -30.0% |
| Normal allocated bytes | 688,640,252 | 242,409,536 | -64.8% |
| Normal allocations | 10,591,482.5 | 1,892,177 | -82.1% |
| Pressure wall time | 1.351 s | 0.841 s | -37.8% |
| Pressure allocated bytes | 637,692,408 | 192,165,260 | -69.9% |
| Pressure allocations | 10,564,750.5 | 1,863,129 | -82.4% |
| Pressure process CPU per edge | 22,946.5 ns | 9,382 ns | -59.1% |
| Pressure GC CPU per edge | 19,102 ns | 6,040 ns | -68.4% |
| Output per relationship | 867.6 bytes | 867.6 bytes | unchanged |

Command for each compiled side:

```text
-test.run=^$ -test.bench=^BenchmarkSnapshotStoredGraph$ -test.benchtime=1x
-test.benchmem -test.timeout=30m -test.cpuprofile=<profile>
```

Raw logs, four CPU profiles, commands and exit receipts are retained under
`~/.local/share/nornicdb-pr-readiness/pr1541-evidence/`. Historical TrueNAS
measurements in the original PR description do not qualify this restack.

## Product and convergence evidence

- The public `WriteSnapshot` path retains the shared header, framing, byte limit,
  CRC, successful entity counters, completion footer and flush owners. The raw
  encoder executes synchronously while Badger owns the borrowed body.
- Complete-frame differential tests cover direct, WAL, transparent wrapper,
  namespace and write-behind overlay paths, hidden relationships, large persistent
  values, temporal extension fallback and fast/general recovery equivalence.
- A fail-first current-reader test reproduced narrow integer widening: stored
  `d0 05` decodes and re-encodes as `d3 0000000000000005`. Such integers and
  uint64 values within signed range use the general decoder. Accepted values
  retain their exact encoded bytes. A 20-second identity fuzz run passed 42,940
  executions; the regression table includes unsigned overflow boundaries.
- Duplicate findings retired both compact-header and whole-edge-scan forwarding
  wrappers, then consolidated projected compact decoding into the existing V2
  decoder. Selected-property and early endpoint filtering remain intact.
- Fresh unchanged duplicate audit: 38 functions, zero findings against the graph
  ingested from baseline02d0d727f. Storage census ordering and reviewed lexical
  population pass; only derived census source position and one reviewed lexical
  population replacement changed. No guards or checker rules were weakened.

No stored key, property payload schema, snapshot format version, WAL version or
migration arm changes. Property map iteration order is not a stable format
contract; fast and general snapshot edge maps use the same sorted-key policy.
The fresh `GOFLAGS=-count=1 make test` run passed all 125 packages in
1,650.335 seconds (exit 0). Staged tree
`adb7a0e5aa6f8494ca9e62bc8b2d38f6df90d70e` and all 5,328 Go/module inputs
were unchanged before and after the run; their SHA256 is
`f808e2df020c0c0ae93b04a416c706d4fe4a1242bbb4b6c8f92df5076beb300c`.
This final receipt/compiler wording update changes Markdown only. Broader
`make verify`, race, allocation-ratchet and compatibility qualification were not
run for this PR under the maintainer-directed local `make test` gate.
