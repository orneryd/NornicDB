# PR #1690 duplicate findings and dispositions

## Inventory

The unchanged installed checker analyzed commit
`332543ff5066e145365ff8ed937eac8ab885499a` against release base
`b098f7e708b4c701a743ffb0310a9b179a84c8f2`, using remote
`192.168.1.30:7474`, database `nornicdbcode`.
It completed in 200.1 seconds: **76 functions analyzed, two reuse findings,
zero refactor targets, zero skipped candidates**. Analyzer SHA256:
`e8a5f56a53f2ee9ab3abc6a3ee0e499491f55eca4aa5992a718d0be729fd51d7`.

The graph baseline was `35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6`, older than
the PR base. Each disposition below therefore compares complete current source
and callers, rather than treating indexed bodies as authoritative. The earlier
60-second incomplete run and canceled pre-commit binding preparation are not
clean results. No checker, threshold, exclusion or hook was modified.

## Finding 1: duplicate constraint-violation assertions

**Reported pair:** `nor136AssertUniqueViolation` in
`pkg/storage/nor136_restart_integrity_test.go:689` versus `relatedViolation`
in `pkg/storage/pr1670_related_acked_key_integrity_test.go:90`.
Code-overlap score 0.852; this was an actual duplicated assertion contract.

Both required a non-nil error, extracted `*ConstraintViolationError`, compared
its constraint type, and checked the same token in both the violation message
and the rendered error. The NOR-136 helper merely hardcoded `ConstraintUnique`.
It had exactly three callers, all rejecting duplicate `CreateNode` writes in the
label-scoped UNIQUE transition test.

**Disposition: consolidated.** Retire `nor136AssertUniqueViolation`; all three
callers invoke the existing parameterized `relatedViolation` with
`ConstraintUnique`. No forwarding helper remains. This retains every assertion;
it does not add unverified label/property expectations that neither original
helper tested.

Inspection of that owner also found overlapping typed extraction in the existing
`asConstraintViolation` owner and a list-constraint forwarding helper.
`relatedViolation` now obtains the typed error through `asConstraintViolation`
and still owns its additional type/message assertions. List tests call the same
extractor directly; `requireConstraintListViolation` is retired. The list
concurrency check also uses that owner. Typed error extraction is implemented
once, and all callers retain their original rejection and equality expectations.

**Validation:** pending final integrated checks. Named affected tests:
`TestNOR143UniqueLabelScopedHolderTransitions`, `TestRelatedDirect*`,
`TestConstraintList*`, `TestUniqueList*`, and relationship-constraint validation. Native UNIQUE
controls cover write-behind off/on, LowMemory off/on, released keys without an
intervening flush, actual-holder rejection and reopen.

## Finding 2: overlapping corruption-fixture tests

**Reported pair:** `TestPR1655CommitReadCorruptionKeepsPublishedBody` at
`pkg/storage/pr1655_commit_read_fixture_test.go:355` versus
`TestPR1655CommitReadCorruptionIsRestorable` at line 316 of the same file.
Behavior candidate with code overlap 0.397; source inspection confirmed shared
fixture/seed/corruption setup with complementary assertions.

**Disposition: consolidated.** `TestPR1655CommitReadCorruptionIsRestorable`
now runs one setup and the union of both assertion sets. The second test is
retired, without a renamed forwarding wrapper or copied setup.

Before healing the corrupt head, the surviving test requires:

- The physical body still contains the previously committed value.
- The write barrier is released, no work is buffered, and the namespace MVCC
  sequence has not advanced.
- Stored malformed bytes produce the same decoder error observed by the native
  head read, and the fixture fault has not fired without being armed.

It then restores the saved head, checks its original version, reads the untouched
body through the ordinary path, and again requires the unarmed fault to remain
inert. These are fixture-integrity controls; public failed-commit behavior remains
covered by the separate Execute/Bolt/HTTP fault tests using this fixture.

The helpers themselves (`ReadPhysicalNodeForTest`, `pr1655AssertNoLeaks`, head
read/write helpers and seeding) are reused directly. All old assertions remain;
only the duplicated setup and independent test name disappear. Repository search
found no additional consumers of the retired test name.

**Validation:** pending final integrated checks. Run `TestPR1655CommitRead*`
and the existing PR1655 public protocol controls.

## Scope of the result

Both returned findings have concrete consolidation changes; neither is dismissed
as historical or merely similar. The production runtime, storage formats and
measured cost harness are unchanged by these test consolidations. This inventory
covers the completed report above, not an unbounded claim of no duplication
anywhere in the repository. Final verification results belong in the PR result
comment and the acceptance record.
