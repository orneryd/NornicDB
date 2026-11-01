// Package workcensus is the shared source census engine owned by #1667 in B0.
// Statement work (#1615), source readers (credentials), bound-plan producers
// (#1640/#1583), capabilities (#1644), trim provenance (#1486), and storage
// context/envelope populations supply client data to this
// implementation. None needs its own AST or SSA extractor.
//
// # Loading and selecting
//
// Config supplies Dir, package Patterns, BuildFlags, Tests, Env, additional
// go/packages LoadMode bits, source Packages, Roots, Selectors and Overlay.
// Patterns select the packages whose SSA bodies are built. Dependencies outside
// that initial selection can have only export information; their unavailable
// bodies remain an explicit approximation boundary. A whole-module reachability
// client loads ./... even when Packages limits the selected source members to
// a smaller domain. Limiting member selection is different from omitting a
// maintained producer, registry implementation or intermediate caller body.
// IncludeInactive permits syntax-only selection of excluded build files;
// ExcludeGenerated is an explicit exclusion, reported in Inventory.Sources.
// Standard and client ContextTypes/ContextFields affect diagnostic Scope only.
// Context and checkpoint naming policy is not part of reachability/selection.
//
// Roots match package/function identity and optionally a declared method
// receiver; Required roots fail when missing. Method instances are normalized
// to source origins. VTA refines CHA over all visible source functions, including
// registry initializers and function values. Reachability unions generic
// instances and retains lexical callbacks through opaque dependency bodies.
//
// Selectors are JSON-compatible data, with no executable predicate hooks.
// Reachability="reachable" selects statement work; "all" visits all declarations
// in the selected source packages independently of the call graph. Node selectors
// cover declarations (including arbitrary unused methods), lexical function
// literals, types/aliases, global variable/constant initializers, imports, files,
// loops, calls, identifiers, string/byte indexes and slices, maps, fields,
// comparisons and other supported syntax. Callee package/name, receiver/result
// types, underlying type shapes, source file/owner and build-state filters are
// generic data. Imported aliases and generic Index/IndexList calls resolve via
// go/types and conservative alias/VTA targets; inactive files have syntax but
// no fabricated type information.
//
// Independent populations use different Groups. The first matching rule in a
// group wins per possible call target; alias targets union their kinds once per
// source node. A canonical adapter can therefore retain an
// existing primitive Kind without adding an extractor. For example:
//
//	config := workcensus.Config{
//	    Dir: checkout, Patterns: []string{"./pkg/query/..."},
//	    Roots: []workcensus.Root{{Package: "example/pkg/query", Name: "Execute", Required: true}},
//	    Selectors: []workcensus.Selector{
//	        {Kind: "range", Node: "range", Group: "work"},
//	        {Kind: "copy", Node: "call", Builtin: true, Names: []string{"copy"}, Group: "work"},
//	        {Kind: "text-index", Node: "index", OperandTypes: []string{"string"}, Reachability: "all"},
//	        {Kind: "producer", Node: "declaration", Reachability: "all"},
//	    },
//	}
//	actual, err := workcensus.Extract(config)
//
// Config.Documents adds maintained JavaScript, TypeScript or TSX source paths.
// They use the same all-source selector/manifest API through the published
// Tree-sitter grammars. Named functions, assigned arrows, object/class methods,
// closures, declarations and source operations are selectable. They report byte
// offsets plus UTF16Offset/UTF16End and UTF-16 columns, with Typed/Reachable false.
// No Go semantic type/reachability claim is made for these languages. The optional
// language adapter requires CGO; a pure-Go census explicitly refuses syntax
// documents needing those grammars.
//
// Config.SourceRoots discovers maintained files before package loading. Clients
// supply extension-to-language maps, directory/basename exclusions and syntax or
// lexical Mode. Excluded paths are reported in Sources. New Overlay files enter
// that same discovery, including previously unseen packages under plugins. Go
// files outside the loaded composition are parsed once as syntax-only sources;
// they never acquire fabricated type or reachability facts. Lexical documents
// use Node="text", Pattern (Go regexp) and optional FoldCase. A named language
// and explicit lexical mode make comments/values/declarations an approximation,
// rather than silently treating an unsupported language as an empty population.
// Syntax JS/TS/TSX still requires CGO; lexical peers and Go do not.
//
// IncludeSyntax exposes Member.Syntax: the selected ast.Node, enclosing owner,
// ancestors, exact source bytes, FileSet and the already-loaded types.Info.
// This view is read-only, in-process and omitted from JSON. A capability client
// can use go/types Implements/NewMethodSet on selected TypeSpecs, including
// aliases/promoted/pointer implementations and their package scopes. A trim
// client joins selected identifiers/assignments/calls through Info.Defs/Uses
// object identities, with imported/dot/function aliases resolved by the engine.
// Clients keep domain analysis, never another packages.Load, parser, whole-tree
// walker, identity generator or manifest checker. Go type assertions (including
// type-switch nil targets), type switches and go statements are distinct nodes;
// AssertionType, CommaOK, OperandType and exact start/end coordinates are facts.
// Identical same-line expressions retain distinct lexical ordinals and offsets.
//
// Policy.RequireSources pins the complete source population, including files
// with no selected operation. File fingerprints normalize CRLF/LF; coordinates
// still refer to the original bytes. Existing client IDs crosswalk to shared IDs
// using relative file, kind and exact source span, then reviewed population
// differences; retaining a private hash/manifest algorithm is unnecessary.
// Domain Facts may be attached only to engine-owned Members. Policy.FactKeys
// compares exact value sets; ClassFact requires the reviewed class to agree with
// a domain-produced fact. This validates consistency, not the domain inference.
// ForbiddenClasses require Member.Assignment matching an exact Policy.Assignment
// (member, digest, class, named owner and operator reference). Such an unresolved
// routing obligation gives no callee-delegation or behavioral-fix credit. Owner
// digests and required Subjects pin repair obligations independently of execution.
//
// A source-reader client can supply strings.Split/Index/Fields and regexp method
// selectors, typed string/[]uint8 index/slice/range selectors, and broad loop
// candidates. A bound-plan client can select every declaration and body, with
// additional field/type/map/signature candidates. Broad declaration selection
// makes producers without familiar names or fact tokens visible. These are
// review candidates, not a determination that a string contains query text or
// that a map owns scope facts.
//
// # Identity, metadata and validation
//
// Source IDs are relative file + source owner + kind + lexical ordinal, never
// line numbers. Closures have lexical owner ordinals. Generic instantiations and
// SSA range-over-function callbacks do not duplicate source syntax. Members have
// formatted-syntax Digest and enclosing OwnerDigest, source positions, available
// signatures/underlying types, declared callees/references, and optional call
// arguments/types, offsets, context diagnostics and conservative bound evidence.
// Source declarations include global initializers and alias definitions.
//
// Validate(actual, approved, Policy) rejects new, stale, changed, duplicate and
// unclassified members, missing reasons, and changed/duplicate root populations.
// Policy supplies the class set, ClassKinds, named Layers, and optional required
// OwnerDigest, ReviewOwner and behavioral Subjects with allowed owner/subject
// lists. ClassKinds maps each custom class to statement, non-statement, bounded,
// delegated, or review. Conventional class meanings cannot be overridden.
// A reachable member cannot be non-statement; a statement class requires
// reachability. A bounded class requires engine BoundKnown evidence and the
// exact inferred maximum/reason, not just matching human claims. Slice views
// never inherit the base current length; only a proven fixed extent is bounded.
// Delegated work requires Member.OwnerLayer and an OwnerLayer in Policy.Layers.
// Every actual call target must occur in that layer with a body digest matching
// Inventory.Definitions. Loops cannot delegate their enumeration to nested calls;
// invisible bodies and invented/stale targets cannot establish ownership. These
// consistency checks do not turn source evidence into execution credit.
//
//	failures := workcensus.Validate(actual, approved, workcensus.Policy{
//	    Classes: []string{"reviewed", "bounded"},
//	    ClassKinds: map[string]string{"reviewed": "review"},
//	    RequireOwnerDigest: true,
//	})
//
// Discovery never grants a class or rewrites an approval manifest. Production
// overlays in Config let negative controls use exactly the same Extract and
// Validate calls as their positive gate. Storage is the first client, with data
// in storage_client_test.go and reviewed storage.json/envelope.json manifests.
//
// # Delegation witnesses and controls
//
// A work client enables Policy.RequireDelegationWitnesses. Each callee in its
// OwnerLayer.Members then supplies Observations: CallWitness{MemberID, Digest,
// Callee, RemovalControl}. The client selects observation calls as ordinary
// members. Validate checks the exact call site, its actual target, and its
// OwnerSymbol against the delegated source function, independently of the body
// pin. A nested closure cannot impersonate a direct observation in its parent.
// Policy.Controls supplies SourceControl{ID, MemberID, Replacement}; every
// witness must name its matching control. ExtractWithControl applies the
// type-correct replacement to the engine's source span and calls Extract again.
// The client runs ordinary Validate and asserts the delegated row's observation
// failure. The fixture refreshes the changed callee body pin and removes the
// observation approval too, proving rejection independently of stale hashes.
// A control that does not compile is invalid evidence. Source presence is not
// control-flow dominance or runtime work-budget/cancellation proof; those remain
// executable public obligations of the client.
//
// # Containing-owner witnesses
//
// Work inside a lower-owned function is not a delegated call. A client maps its
// class to ClassKinds[...]= "observing-owner" and supplies Member.ContainingOwner:
// ContainingOwnerWitness{OwnerID, OwnerDigest, Observation, Scope}. OwnerID is the
// exact containing Definition.ID, including a closure's lexical identity. The
// named OwnerLayer must contain that owner and its current body digest. The
// selected observation must occur directly in that same body and name a matching
// SourceControl; an observation in a nested callee or parent cannot substitute.
// ScopeWitness pins the actual argument (or receiver), its type and engine-owned
// ValueDefinition.Reference, optionally through a typed field path. A closure
// may name a captured lexical ancestor's binding. Shadowed, reassigned or
// escaping bindings fail closed. Computed aliases and opaque scope providers
// require separate client proof; the engine does not infer those identities.
// ExtractWithControl removes the observation while retaining the selected work.
// Validate then reports OWNER OBSERVATION MISSING for that work even if ordinary
// source/body pins are refreshed. This establishes the source witness and its
// removal contract, not dominance or the runtime observation's semantics.
//
// # Loaded analysis clients
//
// IncludeProgram returns Inventory.Program with the same loaded packages,
// FileSet, SSA program, function population and VTA/CHA graph used by Extract.
// LoadProgram(Config) supplies that view directly to SSA-effect clients without
// a redundant syntax inventory. It owns loading, building and generic function
// enumeration; callers retain domain-specific generic-instance/effect policy.
// The stored-entity authority census uses it while retaining its own effect,
// acquisition and publication interpretation. #1644 can inspect the loaded type
// scopes/method sets; #1486 uses selected syntax/typed object identities. No
// second packages.Load, SSA build, call graph or generic source walk is needed.
//
// An all-source, rootless client may set SourceOnly=true with every selector's
// Reachability="all". It uses the same typed loader and source traversal without
// building SSA/CHA/VTA. IncludeSyntax still supplies types.Info and the actual
// loaded GoSyntax.Package, including empty compiled package scopes. Direct calls
// and source-assignment aliases remain visible. Indirect and interface targets
// not resolved by this mode have UnresolvedTargets=true and remain conservative
// call candidates despite callee-name/package filters. They cannot prove
// delegation or an observation target. Full extraction remains available for
// VTA target refinement. SourceOnly refuses Roots and IncludeProgram; it makes
// no reachability/context-access claim. Syntax-only inactive files retain their
// explicit boundary and have no fabricated types or package scopes.
//
// Owner identities use lexical declaration ordinals for repeated names,
// including blank ValueSpecs, blank functions and init declarations. Comment or
// line movement preserves IDs; reordering different declarations changes their
// pinned rows. Each local var/const/type declaration has its own declaration kind
// rather than inheriting its containing function's kind, in both typed and
// syntax-only traversal.
//
// # Approximation boundaries
//
// Inventory records Roots, Sources, Definitions, InvisibleBodies and Limitations. Generated
// and inactive files are explicit; inactive semantic filters cannot resolve
// types. Reflection, plugins and dependency bodies not loaded into SSA require
// semantic review. Source generic signatures remain uninstantiated, although
// reachability unions their visible instances; synthetic wrappers without source
// are reported separately. Broad declaration selectors cover polymorphic
// producers that an exact instantiated-type selector cannot decide.
//
// An all-source syntax census is not a universal call graph, an invisible-body
// proof, a cancellation dominance proof or evidence of scope/header semantics.
// Each client retains its own product-level behavioral controls and reviewed
// dispositions. Source membership and fingerprints establish review coverage.
package workcensus
