// Package capturecensus checks the native read/capture boundary using typed
// facts extracted by workcensus. It owns storage policy, not source extraction.
// Reachability witnesses describe possible source paths, not runtime dominance.
package capturecensus

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"fmt"
	"go/ast"
	"go/types"
	"sort"

	"github.com/orneryd/nornicdb/internal/workcensus"
)

// Policy identifies type and owner objects in the compiled package. Names are
// policy data; receiver identifiers, pointer syntax and declaration order are
// never used as identities. Alternate owners describe distinct read contracts.
type Policy struct {
	Package, Native                   string
	Entities, IDs, Views              []string
	Capture, Historical, Head, Scalar []string
	Publication                       []string
	BorrowedOptions                   []string
	ContextOwners                     []string
	CrossUnitOwners                   map[string]CrossUnitOwner
	OutOfContract                     map[string]ScopeEntry
	ExcludedPackages                  map[string]ScopeEntry
	TrackedDefects                    map[string]ScopeEntry
	DeferredPublicAPI                 ScopeEntry
	// PublicPackages are packages whose exported API, like the policy
	// package's, takes no request context in 1.4.4 (the tracked deferral);
	// the request walk stops at their exported entry points too.
	PublicPackages []string
	// PublicEntries are single exported functions of other packages that are
	// the same deferred boundary (by symbol).
	PublicEntries          []string
	ContextSources         []ContextSource
	NilContextDispositions map[string]NilDisposition
	// NilDetached are the nil flows of a merged detached-path convention, by
	// witness root, each at its exact uses (KindNilDetached).
	NilDetached map[string]NilDisposition
	// ReviewedHeldValues are reviewed per-site records, by the function
	// creating the fresh context (ReviewedHeldValue).
	ReviewedHeldValues map[string]ReviewedHeldValue
	ExtraResults       []TypeName
	Distinct           map[string]Disposition
}

type TypeName struct{ Package, Name string }

// Disposition pins a read-based distinction. An edit to the body invalidates
// the decision. Owners, when supplied, must still be reachable in extracted facts.
type Disposition struct {
	ID, Digest, Reason string
	Owners             []string
}

type Declaration struct {
	Symbol, File, Digest, Signature, Receiver, Binding string
	Line                                               int
	Public, Native, Read                               bool
}

type Consumer struct {
	Caller, Callee, File string
	Line, Column, Offset int
}

type Read struct {
	Declaration
	Responsibility string
	Path           []string
}

type Inventory struct {
	SourceDeclarations []SourceDeclaration
	Declarations       []Declaration
	Reads              []Read
	Consumers          []Consumer
	OwnerCalls         []Consumer
	ContextSites       []ContextSite
	// NilContextsElsewhere lists nil contexts passed to calls that reach no
	// owner: findings for their own owners, not violations of this boundary.
	NilContextsElsewhere []string
	// NilContextsDisposed lists in-family nil flows accepted by a reviewed
	// disposition entry, with its reason and reviewer.
	NilContextsDisposed []string
	// NilContextBoundary lists the functions a nil context must never reach:
	// the context owners plus every context reader of the native engine.
	NilContextBoundary []string
	// Acceptances lists every finding the census did not fail, by kind, from
	// its one recorder; AcceptanceCounts sums them.
	Acceptances []Acceptance
	// BoundaryCrossings lists every edge the in-package request walks cut (a
	// caller outside the package, or a dynamic call into an exported method),
	// with any request holder beyond it: the exclusion population of the
	// public API stop.
	BoundaryCrossings []BoundaryCrossing
	Limitations       []string
	Violations        []string
}

type SourceDeclaration struct{ ID, Kind, File, Symbol string }

// Extract uses the shared engine's typed all-source selectors. In particular it
// includes unused methods, aliases, literals and arbitrary-name functions; it
// does not parse files, build another call graph, or supply familiar reader names.
func Extract(c workcensus.Config, policy Policy) (Inventory, error) {
	c.SourceOnly, c.IncludeSyntax, c.IncludeProgram = false, true, true
	c.Roots = nil
	c.Selectors = []workcensus.Selector{
		{Node: "declaration", Kind: "declaration", Reachability: "all"},
		{Node: "call", Kind: "call", Reachability: "all"},
	}
	i, err := workcensus.Extract(c)
	if err != nil {
		return Inventory{}, err
	}
	return Analyze(i, policy), nil
}

// Analyze consumes the same shared extraction in production guards and planted
// controls. Caller/callee objects, including alias and function-value candidates,
// are supplied by the engine. Unknown indirect calls remain approximation limits.
func Analyze(i workcensus.Inventory, policy Policy) Inventory {
	out := Inventory{Limitations: i.Limitations}
	var log acceptanceLog // the census's one acceptance recorder
	objects := map[string]*types.Func{}
	roots := map[string]workcensus.Root{}
	var actualDistinct, approvedDistinct workcensus.Inventory
	actualDistinct.Definitions = i.Definitions
	for _, decision := range policy.Distinct {
		approvedDistinct.Members = append(approvedDistinct.Members, workcensus.Member{ID: decision.ID, Digest: decision.Digest, Class: "distinct responsibility", Reason: decision.Reason})
	}
	for _, m := range i.Members {
		if m.Kind == "declaration" {
			out.SourceDeclarations = append(out.SourceDeclarations, SourceDeclaration{m.ID, m.Node, m.File, m.OwnerSymbol})
		}
		if m.Syntax == nil || m.Syntax.Info == nil {
			continue
		}
		s := m.Syntax
		switch n := s.Node.(type) {
		case *ast.FuncDecl:
			obj, ok := s.Info.Defs[n.Name].(*types.Func)
			if !ok {
				continue
			}
			name := obj.FullName()
			if obj.Name() == "init" || obj.Name() == "_" {
				name = m.OwnerID
			}
			if _, seen := objects[name]; seen {
				continue
			}
			objects[name] = obj
			if _, reviewed := policy.Distinct[name]; reviewed {
				actualDistinct.Members = append(actualDistinct.Members, m)
			}
			root := workcensus.Root{Package: obj.Pkg().Path(), Name: obj.Name(), Required: true}
			if recv := obj.Type().(*types.Signature).Recv(); recv != nil {
				typ := types.Unalias(recv.Type())
				if ptr, ok := typ.(*types.Pointer); ok {
					typ = types.Unalias(ptr.Elem())
				}
				if n, ok := typ.(*types.Named); ok {
					root.Receiver = n.Obj().Name()
				}
			}
			roots[name] = root
			sig := obj.Type().(*types.Signature)
			d := Declaration{Symbol: name, File: m.File, Line: m.Line, Digest: m.Digest, Signature: sig.String(), Public: obj.Exported()}
			if recv := sig.Recv(); recv != nil {
				d.Receiver, d.Binding = types.TypeString(recv.Type(), nil), recv.Name()
				d.Native = named(recv.Type(), policy.Package, policy.Native)
			}
			// Free functions with a native parameter are also consumers, including
			// ones declared in a newly added package outside storage.
			native := d.Native
			for p := 0; p < sig.Params().Len(); p++ {
				native = native || named(sig.Params().At(p).Type(), policy.Package, policy.Native)
			}
			d.Read = d.Public && native && readShape(sig, policy)
			out.Declarations = append(out.Declarations, d)
		case *ast.TypeSpec:
			obj, ok := s.Info.Defs[n.Name].(*types.TypeName)
			if !ok {
				continue
			}
			for _, option := range policy.BorrowedOptions {
				if !named(obj.Type(), policy.Package, option) {
					continue
				}
				st, ok := obj.Type().Underlying().(*types.Struct)
				if !ok {
					continue
				}
				callback, request := borrowedFields(st, map[types.Type]bool{})
				if callback && request {
					out.Violations = append(out.Violations, fmt.Sprintf("%s: request context shares borrowed callback options", obj.Name()))
				}
			}
		}
	}
	out.Violations = append(out.Violations, workcensus.Validate(actualDistinct, approvedDistinct, workcensus.Policy{Classes: []string{"distinct responsibility"}, ClassKinds: map[string]string{"distinct responsibility": "review"}})...)
	// The engine owns method/closure/registry reachability and wrappers. This
	// client supplies typed source roots and responsibility targets as data.
	if i.Program == nil {
		out.Violations = append(out.Violations, "compiled program missing")
		return out
	}
	reads := map[string]bool{}
	for _, d := range out.Declarations {
		if !d.Read {
			continue
		}
		reads[d.Symbol] = true
		obj := objects[d.Symbol]
		sig := obj.Type().(*types.Signature)
		role, owners := "captured live entity read", policy.Capture
		if tupleNamed(sig.Params(), policy.Package, []string{"MVCCVersion"}) {
			role, owners = "explicit logical historical snapshot", policy.Historical
		} else if tupleNamed(sig.Results(), policy.Package, []string{"MVCCHead"}) {
			role, owners = "logical MVCC head metadata", policy.Head
		} else if scalarResult(sig) {
			// Counts still consume a read owner; an entity reader cannot borrow
			// this responsibility merely by adding a call to a count helper.
			role, owners = "scalar graph degree", policy.Scalar
		}
		population, err := i.Program.ReachableFrom([]workcensus.Root{roots[d.Symbol]})
		if err != nil {
			out.Violations = append(out.Violations, err.Error())
		}
		path := ownerPath(population, owners, roots)
		if decision, ok := policy.Distinct[d.Symbol]; ok {
			bodies := policy
			bodies.IDs = nil
			for result := 0; result < sig.Results().Len(); result++ {
				if shape(sig.Results().At(result).Type(), bodies, false) {
					out.Violations = append(out.Violations, fmt.Sprintf("%s: entity delivery cannot be a distinct responsibility", d.Symbol))
				}
			}
			log.accept(KindDistinctDecision, d.Symbol, decision.Reason, "reviewed data", nil)
			role, owners = decision.Reason, decision.Owners
			path = ownerPath(population, owners, roots)
			if len(owners) == 0 {
				path = []string{d.Symbol}
			}
		}
		out.Reads = append(out.Reads, Read{d, role, path})
		if len(path) == 0 {
			out.Violations = append(out.Violations, fmt.Sprintf("%s: no %s owner", d.Symbol, role))
		}
	}
	owners := map[string]bool{}
	for _, xs := range [][]string{policy.Capture, policy.Historical, policy.Head, policy.Scalar, policy.Publication} {
		for _, x := range xs {
			owners[x] = true
		}
	}
	for _, fn := range i.Program.Functions {
		obj, ok := fn.Object().(*types.Func)
		if !ok {
			continue
		}
		symbol := obj.FullName()
		if !reads[symbol] && !owners[symbol] {
			continue
		}
		for _, site := range i.Program.SourceCallSites(fn) {
			pos := i.Program.FileSet.PositionFor(site.Pos(), false)
			c := Consumer{Caller: site.Parent().String(), Callee: symbol, File: pos.Filename, Line: pos.Line, Column: pos.Column, Offset: pos.Offset}
			if reads[symbol] {
				out.Consumers = append(out.Consumers, c)
			}
			if owners[symbol] {
				out.OwnerCalls = append(out.OwnerCalls, c)
			}
		}
	}
	sites, violations, boundary, crossings := contextSites(i.Program, policy, &log)
	out.ContextSites, out.NilContextBoundary, out.BoundaryCrossings = sites, boundary, crossings
	out.NilContextsElsewhere, out.NilContextsDisposed = log.keys(KindOutsideBoundary), log.keys(KindNilDisposition)
	out.Acceptances = log.all()
	out.Violations = append(out.Violations, violations...)
	sort.Slice(out.Declarations, func(a, b int) bool { return out.Declarations[a].Symbol < out.Declarations[b].Symbol })
	sort.Slice(out.Reads, func(a, b int) bool { return out.Reads[a].Symbol < out.Reads[b].Symbol })
	sort.Strings(out.Violations)
	return out
}

func named(t types.Type, pkg, name string) bool {
	t = types.Unalias(t)
	if p, ok := t.(*types.Pointer); ok {
		t = types.Unalias(p.Elem())
	}
	n, ok := t.(*types.Named)
	return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == pkg && n.Obj().Name() == name
}

func tupleNamed(t *types.Tuple, pkg string, ns []string) bool {
	for a := 0; a < t.Len(); a++ {
		for _, n := range ns {
			if named(t.At(a).Type(), pkg, n) {
				return true
			}
		}
	}
	return false
}

func shape(t types.Type, p Policy, callback bool) bool {
	t = types.Unalias(t)
	for _, n := range append(append(append([]string(nil), p.Entities...), p.IDs...), p.Views...) {
		if named(t, p.Package, n) {
			return true
		}
	}
	switch x := t.(type) {
	case *types.Pointer:
		return shape(x.Elem(), p, callback)
	case *types.Slice:
		return shape(x.Elem(), p, callback)
	case *types.Array:
		return shape(x.Elem(), p, callback)
	case *types.Chan:
		return shape(x.Elem(), p, callback)
	case *types.Map:
		return shape(x.Key(), p, callback) || shape(x.Elem(), p, callback)
	case *types.Signature:
		for a := 0; a < x.Results().Len(); a++ {
			if shape(x.Results().At(a).Type(), p, false) {
				return true
			}
		}
		if callback {
			for a := 0; a < x.Params().Len(); a++ {
				if shape(x.Params().At(a).Type(), p, false) {
					return true
				}
			}
		}
	}
	return false
}

func borrowedFields(t types.Type, seen map[types.Type]bool) (callback, request bool) {
	t = types.Unalias(t)
	if seen[t] {
		return
	}
	seen[t] = true
	if named(t, "context", "Context") {
		return false, true
	}
	switch x := t.Underlying().(type) {
	case *types.Signature:
		return true, false
	case *types.Pointer:
		return borrowedFields(x.Elem(), seen)
	case *types.Struct:
		for f := 0; f < x.NumFields(); f++ {
			cb, ctx := borrowedFields(x.Field(f).Type(), seen)
			callback = callback || cb
			request = request || ctx
		}
	}
	return
}

func readShape(s *types.Signature, p Policy) bool {
	// Entity input values are writes or predicates over a supplied entity, not
	// reads of that entity. Callback inputs are public read delivery surfaces.
	entityInput := false
	for a := 0; a < s.Params().Len(); a++ {
		t := s.Params().At(a).Type()
		if cb, ok := types.Unalias(t).Underlying().(*types.Signature); ok && shape(cb, p, true) {
			return true
		}
		q := p
		q.IDs = nil
		q.Views = nil
		entityInput = entityInput || shape(t, q, false)
	}
	if entityInput {
		return false
	}
	for a := 0; a < s.Results().Len(); a++ {
		for _, extra := range p.ExtraResults {
			if named(s.Results().At(a).Type(), extra.Package, extra.Name) {
				return true
			}
		}
		if shape(s.Results().At(a).Type(), p, false) {
			return true
		}
	}
	// Per-ID boolean, label, degree, metadata and current-head reads do not
	// return an entity body, but still have a read admission responsibility.
	hasID := false
	for a := 0; a < s.Params().Len(); a++ {
		q := p
		q.Entities = nil
		q.Views = nil
		hasID = hasID || shape(s.Params().At(a).Type(), q, false)
	}
	if hasID {
		for a := 0; a < s.Results().Len(); a++ {
			if !types.Identical(s.Results().At(a).Type(), types.Universe.Lookup("error").Type()) {
				return true
			}
		}
	}
	return false
}

func scalarResult(s *types.Signature) bool {
	if s.Results().Len() == 0 {
		return false
	}
	b, ok := s.Results().At(0).Type().Underlying().(*types.Basic)
	return ok && b.Info()&types.IsInteger != 0
}

func ownerPath(population workcensus.Reachability, owners []string, roots map[string]workcensus.Root) []string {
	for _, owner := range owners {
		if root, ok := roots[owner]; ok {
			if path := population.PathTo(root); len(path) > 0 {
				return path
			}
		}
	}
	return nil
}
