# Shared work census

#1667 owns this package. It adopts #1615's WIP design (source SHA256
`79489b3af152434d2a763d44707ae767451ea26cc1bb220804dee5b75d44e445`)
as one reusable `Extract(Config) (Inventory, error)` / `Validate` implementation.
#1615, credentials, #1640/#1583, #1644 and #1486 supply roots, selectors and classification policy as data after rebasing. The production engine contains no storage or statement domain predicates.

`go/packages` loads production types and syntax. `go/ssa` builds source functions;
VTA refines CHA's interface/function-value graph. Reachability starts at the
first client's explicit protocol, Fabric and Cypher statement roots in `storage_client_test.go`.
Nested callbacks are retained even when their dependency implementation is not
loaded. Generic instances share one source member and union their reachability.
Package scope, entrypoints, selectors and overlays are configuration, independent of review. See [doc.go](doc.go) for the JSON-compatible Config/Root/Selector/Policy API and examples for all clients.

Extraction records every reachable `for`/`range`, bulk primitive, iterator and
reader/collector entrance in the selected packages. It recognizes typed node/ID
results as well as streaming/iteration/collector APIs, including `GetAllNodes`.
Every member has a source identity and a SHA256 of its formatted syntax. The gate
rejects additions, removals, changed code, changed roots, unreviewed classes and
missing reasons. Bounded classifications also pin the inferred bound and proof.
Discovery has no approval operation and is deliberately unsuccessful.

The storage manifest classifies all extracted storage work, including the
conservative graph's compatibility and maintenance branches:

- **contextual statement owner**: #1667's node population enumeration,
  constraint candidate preparation, label/count readers, wrappers and collectors.
  Observation may be in the shared cursor, emission or pending-index owner.
- **bounded**: a reviewed fixed outer enumeration. Nested work remains separately
  inventoried; the bound makes no claim about the size of a nested operation.
- **non-statement boundary**: requires extracted unreachable source. The storage
  work selection is reachable-only, so its reviewed manifest has no such rows.
- **owned by another layer**: requires a named layer whose pinned source bodies
  contain every actual callee. A loop cannot delegate its enumeration to a nested
  call. Future #1615 work is not present ownership evidence; the storage client
  currently makes no delegated claims.
- **local statement work**: reachable compatibility branches, general value/codec,
  relationship/embedding, schema/routing, staging and publication operations
  outside this layer's node-population observations. These are local source work,
  with no claim of cancellation or already completed #1615 ownership. They remain
  inventoried for the general statement-work client.

`Policy.ClassKinds` gives custom classes explicit `statement`, `non-statement`,
`bounded`, `delegated` or `review` semantics. Conventional meanings cannot be
redefined. `Validate` checks actual reachability, engine-produced `BoundKnown`
evidence, and `Member.OwnerLayer` against `Policy.Layers` and the independently
extracted `Inventory.Definitions` symbol/body digests. Changing prose or facts in
an approved row cannot override actual facts. Review-only populations, such as
all-source envelope syntax, make no statement-lifetime claim.

Slice bounds never inherit a base slice's current length: reslicing can expand to
capacity. Only a fixed view extent proves a maximum; runtime highs, unknown
producers and omitted highs remain unknown without further proof. Map capacities,
mutated/escaping induction variables and unknown length writers are not bounds.

The independent envelope population covers every node-format use, varint decoder
and byte-slice framing candidate in storage, including unreachable recovery and
migration functions. It over-approximates unrelated key and property codecs.
Classes are emitter, detection/migration, shared decoder, and unrelated codec.
`SplitNodeEnvelope` has seven grammar sites and four consumers (11 shared-decoder
members). Consumers retain their copy/emission responsibilities. A format-detection
switch with a new length reader contributes new members even without a splitter.

Three production overlays exercise the **same extractor and manifests**: a new
statement-reachable helper with a raw iterator; the actual buffered constraint
call restored to its Background adapter; and a switch-based framing decoder.
Each must be rejected for its own unlisted or changed source member. They share
one overlay load for cost, and are asserted independently.

Run focused tests through the repository's lab runner. The gate selector is
`^(TestStorageWorkCensus|TestStorageWorkCensusMutationControls|TestCensusClassificationIsStrict)$`.
The generic fixture suite is `^(TestGenericCensus.*|TestCensus.*)$`; it covers all-source declarations, aliases/initializers, unused methods, generic/registry/opaque callbacks and the shared negative controls. For review evidence, select exactly `^TestStorageWorkCensusDiscovery$`; its failure
means extraction finished without granting approval. A normal full test selection
skips discovery. Manifests are reviewed source, never regenerated by the gate.

This is a conservative source census, not a proof of all runtime dispatch.
Reflection, external plugins, dependency bodies and alternate build tags require
separate semantic review. The selected source is checked with graphify and rg.
Cancellation behavior is tested through public validation and DDL paths; source
membership alone is not a behavioral or performance result.


Maintained language peers use `Config.Documents` and the same selectors and
validator. The optional CGO adapter uses published [Tree-sitter Go bindings](https://github.com/tree-sitter/go-tree-sitter)
with the maintained JavaScript and TypeScript grammars. It enumerates named
functions, arrows, methods and closures from actual syntax, rather than regex
matches in comments/strings. Members report byte and UTF-16 coordinates. These
are syntax-only all-source candidates; their Go type/SSA fields are unavailable.
A CGO-disabled tool handles Go and explicit lexical peers; it refuses syntax peers needing unavailable grammars.

`SourceRoots` discovers client-selected maintained source independently of package loading. `IncludeSyntax` exposes the selected Go AST/owner/ancestors, original bytes and loaded `types.Info` for domain def/use and method-set analyses, without another loader or traversal. Type assertions, type switches and go statements are distinct selectors. `RequireSources` pins canonical CRLF/LF-normalized documents, even without an old gate; `FactKeys`/`ClassFact` check domain facts and `ForbiddenClasses` require exact operator `Assignments` for unresolved defects. None grants behavioral credit. See the package documentation for API and limitations.
The peer mutation test plants the credentials lane's punctuation-only function
and requires rejection of that function by the same Extract/Validate gate.

The stored-entity authority census (`testing/storagecensus`) consumes the shared
`LoadProgram(Config)` view; its effect and acquisition rules remain domain
analysis. `Extract(...IncludeProgram:true)` returns that same packages/SSA/VTA
view with a syntax inventory. Work delegation can require exact observation-call
witnesses via `Policy.RequireDelegationWitnesses`, `LayerMember.Observations` and
`Policy.Controls`. `ExtractWithControl` mutates the pinned call source and runs
the same extraction again. The calling row fails if its observation is removed,
even after the callee body pin is refreshed. This is source evidence; public
behavioral obligations remain separate. See the package documentation.

Containing-owner proof is separate from CALL delegation. A client supplies an
`observing-owner` class, the selected work's `ContainingOwnerWitness`, exact
`Definition.ID`/body pin, named layer membership, directly selected observation,
`ScopeWitness` binding/type/argument or receiver, and a matching `SourceControl`.
Lexical closure captures use the ancestor's binding identity; shadowed, mutated
or escaping scopes fail closed. Removing that observation through
`ExtractWithControl` leaves the work selected and makes ordinary `Validate`
reject its owner witness even after body pins are refreshed. This source proof
does not replace runtime cancellation/dominance subjects.

All-source rootless clients may opt into `Config.SourceOnly` (all selectors must
use `Reachability: "all"`). It shares the typed loader/traversal but skips
SSA/CHA/VTA. `IncludeSyntax` still exposes def/use, aliases and the actual loaded
`GoSyntax.Package` scope, even for an empty compiled stub. Unknown indirect or
interface calls are retained conservatively with `UnresolvedTargets`; they
cannot discharge delegated/observation claims. Request full extraction for VTA
refinement. Source-only refuses roots and `IncludeProgram`. Repeated/blank
declarations have distinct lexical identities; local declaration kind metadata
remains var/const/type, independently of the containing function.
