# Bound-plan direct-mechanism census client

[Author: #1640 lane (Codex).]

This test-only domain client consumes #1667 `internal/workcensus` at the exact
pin recorded in the PR body and native job receipt. The shared engine owns
loading, SSA, CHA/VTA, function identities, source boundaries and `Validate`.
The private `population.go` and `symbols.go` enumerators are deleted.

`TestBoundPlanPopulationInventory` observes conservative source coverage. Its
old 42,690 subjects are context, not an admission-owner population. The former
1,193/1,235 reachable candidates are superseded after independent audit #14.
They must not be individually classified as direct mechanism members.

`compiled_test.go` queries the shared compiler objects across `./...` for:

- Direct calls/references to exact text/row/name-set inference APIs.
- Actual calls returning a typed fact, including tuple results and opaque
  constructors. A fact-returning signature without an invocation is not a member.
- Direct field reads, writes and constructions on the declared fact types,
  including indexed membership edits and empty fact values. Fact-bearing fields
  in arbitrarily named containing records are selected by their compiler types,
  without selecting unrelated sibling fields or reachable callers. The compiler
  supplies fields for the public resultmetadata product and the exact private
  representations; a new field does not escape a manual field-name list.
- Direct fact-method references and their concrete implementation, including
  getters used without exposing a public field. This is an exact typed port,
  not transitive caller reachability or a fact-typed signature match.
- Implementations of the three exact `resultmetadata` publication interfaces.
- Function arguments installed at the exact old Fabric import-reader port.

There are no entry roots, reachability membership, fact-typed signature matches,
general diagnostic-caller matches or transitive name/value propagation. Fixture
and corpus records do not become owners. Canonical constructor/immutable-copy
witnesses are distinguished from noncanonical mechanisms in the semantic DATA;
they are retained in the gate so a new outside-owner writer cannot hide.

`TestBoundPlanPopulationDetectsIndependentDeciders` overlays an arbitrarily named
two producers and six consumers in a new `pkg/server` file. The producers write
a `resultmetadata.Statement` header and an opaque `Names` value from row keys;
the consumers read a fact field
directly and through embedding, call the opaque header getter, and invoke the
public provider interface, and read/write a containing record fact field.
The same exact typed query must find all eight,
shared validation must reject their unlisted IDs, and removal must restore
the exact original ID/digest set. The row-key, same-spelling-field and unrelated containing-field decoys stay
outside the population. A producer need not be reachable to be a second owner.

The historical rule-owner ledger is domain DATA, not another extractor. Each
semantic disposition needs its own source reason and public consumer proof by
owner class. Counts and fingerprints grant no behavioral correctness. Approval
DATA is being replaced after the direct-method change; no completed population
approval or readiness is claimed. Authored/normalized-source dependencies remain
separate, explicit OPEN rows on the lower credentials owner.

All Go execution and formatting runs through `sig/tnrun.sh`, never locally.

[Author: #1640 lane (Codex), 29 October. Engine allocation: Nornic2.]

The generic engine is now an analysis tool, not a product dependency. These
client tests require the explicit `boundplan_census_operator` tag and the
hash-checked frozen-engine overlay; normal product builds/tests import no
workcensus package. The analysis operator is outside the product tree at
`sig/tnrun-operator-assets/1640-resume-direct-20261029`. Its engine manifest
is byte-identical to the #1665 resume operator. No engine implementation is
vendored into this PR. The committed count and approval IDs are historical
until refreshed on the reconciled source; no old count is relabeled current.
