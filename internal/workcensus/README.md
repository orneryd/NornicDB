# Shared work census

Author: #1667 implementation lane (Codex). This foundation contains the generic
engine and fixture tests. Client roots, selectors, manifests and semantic gates
are maintained by their respective layers; no storage population or OPEN rows
are included here.

The engine adopts #1615's WIP design (source SHA256
`79489b3af152434d2a763d44707ae767451ea26cc1bb220804dee5b75d44e445`) as
one `Extract(Config) (Inventory, error)` / `Validate` implementation. Statement
work, credentials, bound plans, capabilities and trim provenance are client data,
not production predicates in the engine. See [doc.go](doc.go) for the complete API.

`go/packages` supplies typed syntax, `go/ssa` supplies functions and VTA refines
CHA for interface/function-value reachability. Roots, selected packages, source
roots, build flags and selectors are supplied as data. `SourceOnly` supports
rootless all-source queries without building SSA/CHA/VTA; selected syntax still
carries loaded type/package information. Optional published Tree-sitter grammars
support JavaScript/TypeScript syntax with CGO; pure-Go mode explicitly refuses
those syntax inputs and supports declared lexical peers.

`LoadProgram` supplies the same packages/FileSet/SSA/function/call-graph view to
analysis clients such as `testing/storagecensus`, which retains its domain effect
and acquisition interpretation. No second generic loader is needed.

`Validate` rejects duplicate, stale, changed, unclassified or unreasoned rows and
root changes. Class consistency checks use engine reachability, sound bound
facts and named source owners. Delegated-call and containing-owner witnesses
are distinct; exact observation identities and padded source-removal controls
prevent a surviving repeated call from impersonating a removed observation.
Clients retain semantic dominance and public behavior obligations.

Generic fixture tests cover aliases/initializers, arbitrary declarations,
generic/registry/opaque callbacks, generated/inactive/cache-path identities,
source and observation removal, bounds and contradiction controls, source-only
loading, typed scope, syntax peers and synthetic effect attribution. Run them
through the approved lab workflow; this README grants no execution permission.

Reflection, external plugins, invisible dependency bodies and alternate builds
are declared approximation limits. Syntax or lexical membership never proves
runtime cancellation, bounded cost, ownership or client correctness.
