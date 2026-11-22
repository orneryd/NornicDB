# Preserve final-sigma casing through the shared Cypher lowercase function

`lower()` and `toLower()` use one casing function. Its current mapper can lose context at a transform buffer boundary or stop looking ahead before reaching a cased letter. The mapper's word rules also differ from the observed Neo4j profile. For example, `AΣ-B` becomes `aσ-b` and `AΣ:B` becomes `aς:b` on Neo4j 5.26.30 with JDK 21.0.12.

The shared function will decide final sigma using the pinned Java word and cased context profile. A forward scan retains the last cased character and whether another cased character preceded it. The supplementary boundary rule follows the observed reverse-cursor behavior, including input start and U+FFFF. General lowercasing continues through x/text. The frozen oracle records sigma decisions separately from general mapping differences such as U+1C89.

The original investigation and fail-first evidence are preserved in the N152-58 research archive. The live regression fixture updates the U+0345-only expectation to the approved Neo4j profile and adds the supplementary and punctuation controls. Existing aliases call the same casing function. The HTTP test covers a literal unpaired UTF-16 unit through the repository's JSON codec.

## Tasks

- [x] Preserve the historical witness and reproduce the bug at the release base through the casing function, both parsers and the HTTP transaction route.
- [x] Verify the streaming implementation against the frozen oracle and the pinned context profile.
- [x] Confirm public behavior, race, vet and scoped release verification. Indexed analysis was attempted and remains blocked by graph ingestion.
- [x] Compare representative workloads and allocations against the release base.
- [ ] Publish the release fix and record the evidence on N152-58.
