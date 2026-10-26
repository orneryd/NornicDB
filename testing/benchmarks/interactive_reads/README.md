<!-- Author: Codex NOR-285 implementation agent, 2026-10-26. -->
# Complete interactive-read profiling fixture

This opt-in fixture measures the public `StorageExecutor.Execute` path for five
complete read families at 5,280 documents. It changes no product implementation.
Use only the lab via `sig/tnrun.sh`. Tests skip unless `NOR285_LAB=1` is explicit.

`workload-current.json` is the primary workload: the current 0.4.3 statements
from revision `67e180c85f726db12c0e51ee4ec76da990a386ed`. Its SAME_PARTY is
70,555 bytes, SHA256
`9c0318d7a01a3e3f393b1d11f6da62f23934d7e14b6d5ff280a3cfe8b20d146c`.
Measure these statements on b6, d663 and the later candidate using one maintained
input. The previously reported roughly 291 seconds is context, not our baseline.

`workload.json` is a historical reference from source revision `d5eb96b`.
Its SAME_PARTY is the complete 63,177-byte bound-holder
statement with SHA256
`0caa2b3923cdb2f064197ddcfc7763143303ec67170cb7e68bc609eaa32ae21f`,
matching the supplied full-scale report. `freeze_workload.py` copies versioned
JSON contracts and seed definitions without executing their originating program.
It preserves statement bytes, including every result limit and predicate.

Set `NOR285_CORPUS` to a complete, maintained saved Badger database directory.
The original saved input is unmaintained. Run the canonical Rust holdings
maintenance first, then gracefully stop and retain the maintained store. The
fixture copies that maintained store to a disposable directory and verifies
5,280 document nodes and a complete holdings marker before any measured read.
It deliberately rejects an unmaintained corpus; the old pre-maintenance saved
corpus alone is insufficient. Corpus transport/provenance must be recorded with
the job. Never point the fixture at a live database directory.

Select one case with `NOR285_CASE`: `same_party`, `walk_doc_d4`, `walk_doc_d3`,
`walk_doc_d2`, `walk_doc_d1`, `topic_groups`, `related_case`, `related_doc`,
`suggest_doc`, or `suggest_identity`. Each case runs once in a fresh process
with the result cache disabled. Identity selection is deterministic and its
actual parameter is recorded. Run the identical frozen fixture on product
release `d663ce8f1` and retained-syntax head `20abd9091`, with matched parser,
resource settings and input bytes. Compilation, copying, opening, validation,
serialization and analysis are outside the Execute timing window.

The normal `nornicdb.Open` owner supplies the executor, WAL, Badger and storage
event subscriptions. Durability matches the input deployment: strict durability,
immediate WAL sync and synchronous writes. Search indexes finish warming before
measurement. The Go cost probe disables embeddings, decay and automatic links;
the separate Rust acceptance retains the original full deployment configuration.

The fixture retains a whole-query CPU profile, cumulative allocation profiles
before/after the read, exact Go allocation counters, public result statistics,
ordered columns and complete JSON rows. Profile setup and background engine work
can contribute to sampled allocation differences; exact runtime allocation
counters cover Execute. CPU and allocation flat/cumulative summaries use the
measured test executable. Raw profiles and gzip-compressed complete results are
emitted as hashed base64 artifacts in `NOR285` JSON output records for collection.
No result sorting, row sampling, deadline, input cap, or product instrumentation
is added. Four families must report zero graph writes. TOPIC_GROUPS retains its
original temporary PDTopicLink/PD_TOPIC_LINK writes and APOC Louvain call. Its
statistics are recorded, and checks outside timing require no scratch objects
before/after and unchanged total node/relationship counts. `NOR285_STATEMENTS`
defaults to `current`; `historical` explicitly selects the older reference.

`TestNOR285FrozenWorkload` validates the attributed packet and its identity from
the supplied report. `TestNOR285CorpusPlumbing` is a separate opt-in smoke test
(`NOR285_PLUMBING=1`, `NOR285_CORPUS=<saved-copy-source>`). It opens a disposable
copy, checks counts and exercises a small parameterized public read. The old
unmaintained snapshot is allowed only for this labelled plumbing test. It does
not run any of the five full workloads or produce a workload timing.

`TestNOR285CorpusCensus` (`NOR285_CENSUS=1`) hashes a GNU tar of the allocated
historical unmaintained input with the `database/` member name, compares it with
the reported input hash, and records complete node counts by label, relationship
counts by type and logical kind, document-owned node distributions, corrections
and required start nodes. It observes the population; it creates no substitute
data. A tar mismatch is evidence to resolve, not an equivalence claim.

`prepare_corpus_source.py` appends two separately named ignored lab tests to a
fresh exact-current source extraction after checking all preimages. Preparation
uses `test(test_nor285_prepare_maintained_corpus)` and exports only after graceful
shutdown. Its sole operator is the bench owner; never prepare duplicate corpora.
The read selector `test(test_nor285_maintained_read_comparison)` consumes a private
restoration of the maintained TAR through `PD_NATIVE_STORE_IN` and requires
`NOR285_EXPECTED_HOLDINGS_COUNTS=<preparation>/results/holdings_elements.json`.
It checks 5,280 documents, the complete built marker and identical holdings
counts, then uses the existing current plan's canonical constants/parameters for
the five families and all ten variants. It saves complete ordered results and
checks scratch cleanup, net graph counts and maintained state. No maintenance
or catch-up runs in that selector. `PD_PERF_OUT` is a file in a fresh job dir.

The original `test(test_full_approved_comparison)` remains byte-for-byte unchanged
and still requires the observed unmaintained TAR for each end-to-end acceptance
arm. Both selectors run on b6, d663 and the pinned later candidate. Native
engine commit and binary SHA checks remain mandatory. The adapter changes no
application reader, writer or maintenance implementation.

No measured speedup or causal attribution is claimed before lab profiles exist.
