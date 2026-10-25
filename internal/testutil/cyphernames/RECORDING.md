# Recorder method and scope

[Author: #1486 lane (Codex). Recorder design and interpretation below are the lane’s.]

QuoteName/NameText accept exact decoded names. Optional slot roles are observation metadata. The ordinary build has an inlineable no-op hook; lab binaries use `-tags=cypheremission`. runtime.Callers captures production frames through delegating helpers. Primitive scanner and recorder implementation frames are skipped; catalogued emission-owner frames and actual consumers remain observable. Input is marked decoded; the DDL reference reader records its written-input boundary separately. Trace/record sets are synchronized and record each distinct site/role/input once, without recording name values.

JS/TS and Python production peers contain only their escaping implementation, with no recorder lookups or branches. The JS probes bind the test adapter's recording wrapper in place of the production import. Python's adapter replaces both standalone and package imports before importing the consumer. Each wrapper calls the single shipped escaping implementation. The adapters record actual caller frames; VM probes preserve source line numbers and filename, and the browser method carries its original source URL/line offset. Captured statements execute in the Go subject harness; this proves those constructions and observations, not an interactive browser or live migration.

The Go catalog is generated from #1667's loaded compiler program and the
nameflow client's exact owner-call query. Source-only or inactive Go hits cannot
become executable rows. The recorder requires that generated catalog at the
same Git head and retains its author and shared-engine source facts in the
capture. It has no maintained Go parser or construction enumerator. The lab
pipeline supplies it with `NORNIC_NAME_COMPILED_CATALOG`; direct recording
without a compiled catalog fails before running subjects.

Python AST calls and lexical JS/TS peer calls remain peer-discovery inputs.
The recorder joins source spans to observed production frames. Go/Python
delegation frames also generate rows directly from runtime reach. JS frames are
admitted only at peer-call sites in the original source, excluding appended VM
harness frames. File membership, test-symbol text and source dispositions never
set exercised. Each runtime's retained-test control runs the same test before
and after removing only its production quote call, then generates both ledger
columns against the original census row.

The historical 79-group file is source reconciliation only. It has no test-to-code execution mappings. Group 71 separates normalized ingestion vocabulary, unrestricted stored-type readback, and database administration. Current execution evidence is the generated external ledger attached to the handoff, not a committed run output.

The job prints `NAME_EMISSION_RECORDING` (gzip/base64 JSON) before ledger generation. Decode that artifact and pass it to `coverage.py --artifact <path>` to regenerate the ledger without rerunning its successful subjects. The artifact retains the Go census, raw runtime frames, control evidence and subject-suite status; replay refuses a failed subject suite. Source hashes in the generated ledger must be checked against the recorded source before carrying evidence to another head.

For an affected-package refresh, `NORNIC_NAME_SUBJECT_PACKAGE=./pkg/cypher` selects that package's named suite. An unknown package fails. Discovery and completion still cover the full population, so a partial capture cannot complete rows whose evidence is absent. A handoff may join fresh records with successful unchanged-package captures only after replacing affected subjects/probes and changed producer frames, recording the origins and checking source fingerprints; completion runs again over the full joined population.

Completion is a separate, failing policy over **every discovered row**, including recorded delegation rows. It is not a list of review findings. Each executable site requires recorded reach and an exact-site semantic contract: concrete assertion/helper source fingerprints, a named observation, an actual passing `go test -json` outcome, and the same test caller/probe in the row's runtime provenance. A passing symbol or a source file alone grants neither reach nor semantic completion. The assertion program is source-pinned so removing its comparison while retaining its test symbol invalidates the contract. This is program-level semantic evidence, not assertion-line or branch coverage.

`completion_contracts.json` classifies the current generated site IDs; it never authors an exercised value. IDs include runtime, file, function, callee, source and exact span. A new or moved executable call inherits no contract from its file/function/test and fails until its semantic observation is supplied. The only alternative is an exact-site narrow exclusion with its review reference and reviewed head. The older source-disposition strings remain descriptive and cannot exempt a new row. `NAME_EMISSION_COMPLETION` reports the generated obligations and fails the process on any incomplete member. The current review's seven calls are members of this population, not its boundary.

Go's lab-only recorder retains the actual test caller separately from production frames. Peer adapters retain the invoked probe path. The runner saves real passing test outcomes and AST-extracted assertion candidates before completion generation. The catalog assigns semantic meaning after inspecting those concrete programs; its classification is independently constrained by the runtime provenance and passing outcome. Source-only catalog/policy changes can replay a saved successful capture. `runtimecheck --policy-controls` exercises new-row, removed-call, missing/foreign outcome, removed-assertion, unreviewed-exclusion and empty-population controls.

Limits: reach is at the runtime source-line/call-stack granularity; multiple calls on one line cannot establish each argument or branch separately. It is not branch, value, field, transport or universal emitter coverage. Non-owner discovery is a lexical smoke heuristic, including obvious standalone colon/backtick concatenations in .mjs/.ts/.py. It cannot establish arbitrary data flow, computed fragments, aliases/indirection or unsupported shell/PowerShell languages. These limits must be carried into the PR body.

Run `go run ./internal/testutil/cyphernames/cmd/runtimecheck` on the lab worker for the supported-runtime matrix. It parses and imports all maintained Python files in this layer (and the Qdrant Python migration), then checks ordinary emission on actual Python 3.10 and 3.12. Node 20 checks the maintained JS peers/probes and executes the migration and extracted browser bodies. Portable runtimes are unpacked in a disposable lab directory. Optional network drivers/stream parsing are captured adapters; no live migration or browser interaction is claimed. The shared name population includes one space, two spaces and a tab.
