# Pinned final-sigma context profile

Target: Neo4j 5.26.30, immutable image `sha256:174ca8edaafb189d2676027a738815b04afd56d7dc720bd08bbb37afb469483e` (repository digest `sha256:3388e05ee53c8313d01acdf33e63ad175af95a92226dc8551160564439ce2c8c`), JDK `21.0.12+8-LTS`. Go 1.27.4 uses Unicode 17. JDK Character data is Unicode15; compiled word categories are separately observed data and must not be inferred from Character categories.

`profile.json` pins the compressed snapshot hashes used by the generator. Bulk reflected snapshots and complete source/trace evidence are local review artifacts under `~/.codex/artifacts/n152-58-20261121/solution-investigation`; they are not product files. `neo4j-sigma-decisions.json.gz` is the 20,801-row deterministic regression corpus containing input plus actual Java final-sigma decisions. It includes escaped unpaired UTF16 units, decoded in tests with the existing `internal/jsontext` owner. The test constructs every non-Sigma output byte with the unchanged general mapper; this is sigma-profile parity, not general Java lowercase parity.

## Reproduction

On a mesh host, apply `~/.codex/mesh/POLICY.md`: acquire the host's validation slot, inspect resource/runner contention, use a task-owned container with the exact image, and preserve results/clean only owned resources. Do not start a database or expose ports merely to extract this data.

The committed extraction recipes enumerate U+0000..U+10FFFF. `scripts/sigma_context_snapshot.go` captures Go category bits; `scripts/SigmaContextSnapshot.java` captures Java category bits and the pinned word iterator's observed `lookupCategory(int)` result. No iterator implementation is copied. The category bits are Letter/Mc/Number/Mn-or-Me/Cf/Pd-or-Pc/Lu-or-Ll-or-Lt in bits 0..6.

With Go 1.27.4 and the pinned Java runtime, run from the repository root (replace the external directory as needed):

```sh
mkdir -p /absolute/artifact/snapshots
go run scripts/sigma_context_snapshot.go /absolute/artifact/snapshots/go-context.bin
java --add-opens java.base/sun.text=ALL-UNNAMED scripts/SigmaContextSnapshot.java /absolute/artifact/snapshots
python3 - /absolute/artifact/snapshots <<'PYTHON'
import gzip, pathlib, sys
for path in pathlib.Path(sys.argv[1]).glob('*.bin'):
    path.with_suffix('.bin.gz').write_bytes(gzip.compress(path.read_bytes(), mtime=0))
PYTHON
```

The Go recipe was executed and its 1,114,112 raw bytes matched the captured profile. The consolidated Java recipe is a documented reproduction route; the original separate Java extraction programs produced the captured snapshots. Compare compressed hashes with `profile.json` before regeneration (gzip implementation/header differences may require comparing decompressed bytes and normalizing with the original Python environment).

Regenerate from the verified external snapshot directory:

```
python3 scripts/generate_sigma_context.py --snapshots /absolute/artifact/snapshots
```

The current generic Go category baseline requires 83 contiguous correction ranges (2225 bytes of generated Go). This replaces the prototype's hardcoded CJK and legacy cased predicates. The profile describes only context; x/text remains the sole general lowercase mapper. The generator's legacy additional-cased membership is the pinned JDK21 ConditionalSpecialCasing profile, not the current Unicode Other_Cased property.

Run the complete classifier equivalence gate (every code point) and owner oracle:

```
GOMAXPROCS=2 GOMEMLIMIT=1500MiB N15258_SIGMA_PROFILE_DIR=/absolute/artifact/snapshots go test -p=1 -tags=noui,nolocalllm ./pkg/cypher/fn -run '^TestLowerCaseSigma' -count=1
```

Without the environment variable, only bulk-profile replay skips; the 20,801-row corpus and always-on Unicode17 provenance invariant still run. Toolchain Unicode changes require regeneration and review; a skipped bulk replay is not a full profile pass. The original 53 Unicode-expectation fixture and RED remain in the research archive; the live owner test explicitly uses Neo4j's medial-sigma expectation for the U0345-only row.

## Provenance and limits

Official JDK21u sources examined: `ConditionalSpecialCasing.java`, `RuleBasedBreakIterator.java`, and `BreakIteratorRules.java`. Exact downloaded hashes are in the research archive's `solutions/source-manifest.json`. They carry GPLv2/Classpath-exception notices; no implementation source was transplanted into the production adapter. The small generated table is observed classification data with an explicit pinned-runtime extraction recipe. This record does not make a general license-compatibility claim about copying OpenJDK source.

Original public/HTTP fail-first logs, rejected prototypes, exact call-order traces and matched benchmark binaries/profiles remain in task evidence. Correctness costs must be reported per workload: the old owner is semantically wrong on boundary/long-lookahead inputs, so its faster timing is not a correct alternative. No whole-Unicode proof or release qualification follows solely from the sampled corpus.
