# Evidence and limits

Base43a39d64f9374dce08ea5fd17b87b17c268137e9 for the original reproduction;
the current integrated tree is the same proposal on release head7c61a23f5.
Original exact Plane reproduction saved before edits. Original public
export/import fails four ambiguous-label cases, six ordinary controls pass
(macOS0.726s). Expanded 22case reproduction with the original export source
overlaid fails14cases, eight controls pass (0.942s): delimiter
positions/customUnicode, empty/trimmed labels and protected existing output.
Most failures assert actual restored label membership/order; the
protected-output case requires early rejection. Final source passes22cases in
1.387s. No production change to the importer.

The existing splitList consumer trims and drops empty elements. The preflight
therefore also rejects empty/surrounding-whitespace labels; internal whitespace
and newlines remain valid. This follows the approved explicit unsupported-value
option and avoids reporting successful lossy exports. All label validation
finishes before writeEntityCSV opens/truncates nodes.csv; existing package
files are unchanged on this validation error, and a fresh directory has no
package files. No general atomicity promise for unrelated I/O failures.

## One invariant, one diagnostic (NOR-280 correction)

The first proposal introduced two IDs
(adminimport.export_label_array_delimiter_unsupported,
adminimport.export_label_whitespace_unsupported), two typed constructors, two
catalog entries per active locale and two early-return branches. The read-only
follow-up showed neither per-ID constructor is reusable elsewhere, so those two
branches were competing construction of one decision: a label can violate both
predicates (for example " A;B "), which meant the reason a user saw was decided
by evaluation order, and each branch asserted a single cause for its whole
rejected population.

The preflight now evaluates one predicate -
label == "" || TrimSpace(label) != label || ContainsRune(label, delimiter) -
and returns one existing-Message owner, AdminExportLabelUnsupported(label,
delimiter), ID adminimport.export_label_unsupported. The rejected set is exactly
the union of the two former branches, so nothing is masked: the same labels are
rejected, at the same boundary, with the same ExitUnsupported6 class, before any
package file is created or replaced. What is removed is the second owner of one
invariant, not a finding.

The message states the contract instead of asserting one cause. It names the
quoted label and quoted delimiter, then requires nonempty labels with no leading
or trailing whitespace and no chosen array delimiter, and advises a suitable
--array-delimiter for a delimiter conflict, or a nonempty label without leading
or trailing whitespace, or another export format, for a normalization conflict.
A whitespace-only label is therefore never described as containing the
delimiter; the delimiter is named as the configuration the contract is stated
against. Message, strconv.Quote and strconv.QuoteRune stay the owners; no helper
or abstraction was added, and the importer, encoder, output ordering and CLI
error class are unchanged.

Rendered en-US text (byte-identical to the English fallback, checked):

    label "A;B" cannot be written to Neo4j CSV with array delimiter ';': labels must be nonempty, must not have leading or trailing whitespace, and must not contain the array delimiter; choose a suitable --array-delimiter for a delimiter conflict, or use a nonempty label without leading or trailing whitespace, or another export format, for a normalization conflict

For label " A" the same text renders with `" A"` quoted, i.e. it states the
whitespace rule and the delimiter rule without claiming the label contains the
delimiter. es-ES renders `la etiqueta ... no se puede escribir en CSV de Neo4j
con el delimitador de matrices ';' ...` and en-XA renders the same sentence
wrapped in `[!! ... !!]`.

Source scope: the two typed constants became
MessageAdminExportLabelUnsupported, the two constructors became one, the two
per-locale entries became one in en-US, es-ES and en-XA, and the generated
manifest was regenerated with the existing generator
(go generate ./pkg/localization). gofmt realigned the adminimport constant
block because the removed name was its widest entry; no other message ID, text
or constructor changed, and the original source tests were not edited.

## Public paths and other exporters

The offline Cobra action calls ExportNeo4jCSV directly and returns its error;
main maps the existing adminimport.Error ExitCode to the process status. The
command test opens a real completed Badger database and executes that Cobra
action: default/custom delimiters and edge whitespace report exit class6;
English, Spanish and pseudo-locale render quoted label data, one line, and
delimiter guidance. Three scenarios pass1.010s. This checks the command and
status mapping in process, not an external binary process exit.

APOC graph CSV has section headers _id/_labels/properties; flat apoc/export.Csv
uses id/labels/property columns. Those are separate public formats; this
importer recognizes label membership only through its :LABEL header contract.
The DB admin CSV found in current source is ExportUserData/exportUserDataCSV,
not the selected Neo4j graph package. No inferred shared owner or unrelated
format rewrite. Early code-graph discovery identified the current public, node
encoder and shared entity writer bodies; it is advisory, not a checker pass.

## macOS checks (consolidated source)

NOR-280 public 22 cases: pass0.723s. Real Cobra localized error cases:
pass1.010s. Affected packages, one run each: adminimport2.170s,
localization3.843s, adminCLI3.176s, scripts/localization_catalog0.592s,
scripts/localization_inventory1.461s. go run ./scripts/localization_catalog
-check passes after regeneration; go vet on adminimport/localization/adminCLI
and gofmt -l are clean. make verify-list uses origin/release/v1.5.2 and is only
a preview. Full local make verify, repository tests, race/stress, VitePress
build and unrelated UI/LLM gates defer to CI under the user's focused-check
policy; the canonical duplicate checker, reingestion and the staged release gate
remain with the parent after this freeze. Docs-site node_modules is absent; no
package install/build is claimed.

## Bounded valid-data cost

Existing BenchmarkCSVEntityExport,1000entities,10iterationsx3, GOMAXPROCS2,
GOGC50,GOMEMLIMIT1500MiB, Go1.27.4/M2Max. Both phases CPU-profiled; profiles
include setup/test process outside benchmark timers. This is ordinary valid
Doc labels, not unsupported-input performance or a universal CSV guarantee.
The consolidation keeps the same per-label predicate set, so no new benchmark
was requested or run.

| Case | Before median ns/op (range) | After median ns/op (range) | Median B/op before/after | Allocations before/after |
| --- | --- | --- | --- | --- |
| nodes | 968500 (953388–977354) | 1010592 (968038–1032008) | 409895/409895 | 18014/18014 |
| relationships, unchanged control | 1043017 (1025775–1097912) | 992475 (979562–994900) | 457972/457991 | 18014/18014 |

Short sequential samples are variable: no latency-neutrality or speedup claim.
The added valid-label scan allocates no additional objects in this workload.
Raw samples/profiles preserved; no assumed bottleneck from insufficient CPU
samples. Linux/current-head cross-check remains pending and is parent-owned.

## Parent final integrated qualification

Current integration base7c61a23f5054f020c0c355593cc55c4dfdd6ba35; relevant original exporter/importer bytes match shipped v1.5.1. Current-base overlay reproduces14failures/8controls on macOS0.847s and Linux0.881s. The final consolidated source was staged at65110533606cb95ea5ed33fe962ce219736eed35 before these parent-only documentation additions.

Command: GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1500MiB go test -tags=noui,nolocalllm ./pkg/adminimport ./pkg/localization ./cmd/nornicdb-admin ./scripts/localization_catalog ./scripts/localization_inventory -count=1 -timeout=5m -p1. Mac PASS1.918s/4.172s/2.739s/0.327s/0.260s; Linux PASS4.714s/8.447s/4.921s/0.061s/0.010s. Linux used one2CPU/4GiB Radeon container; exit0/notOOM, logs/inspect retained and normal removal verified. Affected vet and catalog-check pass. Original test sources were unchanged by diagnostic consolidation.

The final canonical staged checker and broader exact-head CI are pending at documentation freeze. make verify-list BASE=origin/release/v1.5.2 completed as a preview and selects docs too; its fixture tests emit missing-sed-file warnings, while the final selection completed. Full local verification, race/stress, differential, ratchet and docs-site build were not run; no broad readiness claim is made.
