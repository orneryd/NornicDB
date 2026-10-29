package capabilitycensus

import (
	"go/ast"
	"go/types"
	"sort"

	"github.com/orneryd/nornicdb/internal/workcensus"
)

// DirectMechanisms selects uses of the mechanism, not its transitive callers,
// data-flow neighborhood or containing loops. The broad typed inventory remains
// context. Each returned site still requires a body-read semantic disposition.
// Author: Lane #1644 (Codex), applying Nornic2's population-method boundary.
func DirectMechanisms(result Result) []Site {
	var core *types.Interface
	packages := map[*types.Package]bool{}
	for _, member := range result.Inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		if pkg := member.Syntax.Package; pkg != nil {
			packages[pkg] = true
			if pkg.Path() == storagePackage {
				if object := pkg.Scope().Lookup("Engine"); object != nil {
					core, _ = object.Type().Underlying().(*types.Interface)
				}
			}
		}
	}
	functions := map[*types.Func]string{}
	owners := map[*types.Func]bool{}
	var storedMethods []*types.Func
	var observedMethods []*types.Func
	var capabilities []*types.Interface
	searchFields := map[types.Object]bool{}
	searchAdapterFields := map[types.Object]bool{}
	requiredSearchOwners := map[*types.Func]bool{}
	requiredCoreMethods := map[*types.Func]bool{}
	configurationFields := map[types.Object]bool{}
	var implementations []types.Type
	if core != nil {
		for _, name := range []string{"GetNode", "GetEdge"} {
			if selection := types.NewMethodSet(core).Lookup(nil, name); selection != nil {
				if fn, ok := selection.Obj().(*types.Func); ok {
					requiredCoreMethods[fn.Origin()] = true
				}
			}
		}
	}
	for _, implementation := range result.Types {
		implementations = append(implementations, implementation.Type())
	}
	for _, implementation := range result.StoredViews {
		implementations = append(implementations, implementation.Type())
	}
	for pkg := range packages {
		for _, anchor := range []struct{ path, receiver, method, family string }{
			{storagePackage, "CompositeEngine", "getConstituent", "constituent-admission"},
			{"github.com/orneryd/nornicdb/pkg/cypher", "StorageExecutor", "getNodeCached", "read-outcome-boundaries"},
		} {
			if pkg.Path() != anchor.path {
				continue
			}
			if receiver := pkg.Scope().Lookup(anchor.receiver); receiver != nil {
				if selection := types.NewMethodSet(types.NewPointer(receiver.Type())).Lookup(pkg, anchor.method); selection != nil {
					if fn, ok := selection.Obj().(*types.Func); ok {
						functions[fn.Origin()] = anchor.family
					}
				}
			}
		}
		// Exact field identities define the configuration handoff population.
		// Declarations or unrelated same-named setters do not classify a site.
		anchors := map[string][]string{}
		if pkg.Path() == storagePackage {
			anchors["CompositeEngine"] = []string{"constituents", "constituentNames", "accessModes", "labelRouting"}
			anchors["QueryAnalyzer"] = []string{"labelRouting"}
		}
		if pkg.Path() == "github.com/orneryd/nornicdb/pkg/multidb" {
			anchors["LabelRouting"] = []string{"labelMap"}
		}
		for name, fields := range anchors {
			if typ := pkg.Scope().Lookup(name); typ != nil {
				for _, name := range fields {
					if field, _, _ := types.LookupFieldOrMethod(typ.Type(), true, pkg, name); field != nil {
						configurationFields[field] = true
					}
				}
			}
		}
		if pkg.Path() == "github.com/orneryd/nornicdb/pkg/search" {
			// Exact typed API anchors, not receiver/name patterns. The public
			// terminal's affected consumers are direct calls to these formerly
			// lossy hydration/filter callbacks, not the transitive search graph.
			if service := pkg.Scope().Lookup("Service"); service != nil {
				set := types.NewMethodSet(types.NewPointer(service.Type()))
				// This public hydration owner intentionally needs full endpoint
				// nodes for RelationshipWhere. Its required core reads belong to
				// the terminal-error class, despite retaining the correct view.
				if selection := set.Lookup(pkg, "SearchRelationships"); selection != nil {
					if fn, ok := selection.Obj().(*types.Func); ok {
						requiredSearchOwners[fn.Origin()] = true
					}
				}
				for _, name := range []string{"engine", "pointRead", "pointReadOnce"} {
					if field, _, _ := types.LookupFieldOrMethod(service.Type(), true, pkg, name); field != nil {
						searchFields[field] = true
						// Calls through the retained function/once are part of the
						// binding owner. Core reads on the issued Engine remain
						// legitimate owner calls, not bypass population members.
						if !engineType(field.Type(), core) {
							searchAdapterFields[field] = true
						}
					}
				}
				for _, name := range []string{"getNodeWithoutEmbeddings", "readSearchNode", "batchGetNodesWithoutEmbeddings", "hitNodes", "enrichResults", "enrichIndexResults", "filterByTypeAndProperties", "filterByType", "filterByProperties", "filterCandidatesByType", "adaptiveVectorSearch", "adaptiveBM25Search", "completeVectorSearch", "completeBM25Search"} {
					if selection := set.Lookup(pkg, name); selection != nil {
						if fn, ok := selection.Obj().(*types.Func); ok {
							functions[fn.Origin()] = "direct-search-read-consumers"
						}
					}
				}
			}
		}
		if pkg.Path() != storagePackage {
			continue
		}
		for _, name := range []string{"ReadCapabilityForEngine", "readCapabilityForEngine", "UnwrapEngine"} {
			if fn, ok := pkg.Scope().Lookup(name).(*types.Func); ok {
				functions[fn.Origin()] = "raw-discovery-or-unwrap"
			}
		}
		for _, name := range []string{"engineCapability", "OwnedCapability", "BindReadOperation"} {
			if fn, ok := pkg.Scope().Lookup(name).(*types.Func); ok {
				owners[fn.Origin()] = true
			}
		}
		for _, name := range []string{"EngineUnwrapper", "StoredEntityReader", "ScopedStoredEntityReader"} {
			if object := pkg.Scope().Lookup(name); object != nil {
				if contract, ok := object.Type().Underlying().(*types.Interface); ok {
					for i := 0; i < contract.NumMethods(); i++ {
						method := contract.Method(i)
						if name == "EngineUnwrapper" {
							functions[method.Origin()] = "raw-discovery-or-unwrap"
						} else {
							storedMethods = append(storedMethods, method)
						}
					}
				}
			}
		}
		for _, name := range []string{"ObservedNodeUpdater", "ObservedEdgeUpdater"} {
			if object := pkg.Scope().Lookup(name); object != nil {
				if contract, ok := object.Type().Underlying().(*types.Interface); ok {
					for i := 0; i < contract.NumMethods(); i++ {
						observedMethods = append(observedMethods, contract.Method(i))
					}
				}
			}
		}
		for _, name := range pkg.Scope().Names() {
			object, ok := pkg.Scope().Lookup(name).(*types.TypeName)
			if !ok {
				continue
			}
			contract, ok := object.Type().Underlying().(*types.Interface)
			if !ok || contract.NumMethods() == 0 {
				continue
			}
			// Protocol identity comes from the complete method set of a real
			// Engine/retained view. Scalar metadata and commit contracts count
			// too; their methods need not carry a Node or Edge value.
			if core != nil && engineType(core, contract) {
				capabilities = append(capabilities, contract)
				continue
			}
			for _, implementation := range implementations {
				if engineType(implementation, contract) {
					capabilities = append(capabilities, contract)
					break
				}
			}
		}
	}
	declarations := map[string]*types.Func{}
	for _, member := range result.Inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		if node, ok := member.Syntax.Node.(*ast.FuncDecl); ok {
			if fn, ok := member.Syntax.Info.Defs[node.Name].(*types.Func); ok {
				declarations[owner(member)] = fn
			}
		}
	}
	protocol := func(typ types.Type) bool {
		if typ == nil {
			return false
		}
		for _, contract := range capabilities {
			if engineType(typ, contract) {
				return true
			}
		}
		// A new narrow interface may precede a named protocol. Match every
		// signature to a real view's method set, including scalar metadata.
		if target, ok := typ.Underlying().(*types.Interface); ok && target.NumMethods() > 0 {
			for _, implementation := range implementations {
				if _, pointer := implementation.(*types.Pointer); !pointer {
					implementation = types.NewPointer(implementation)
				}
				set := types.NewMethodSet(implementation)
				matches := true
				for i := 0; i < target.NumMethods(); i++ {
					method := target.Method(i)
					actual := set.Lookup(method.Pkg(), method.Name())
					matches = matches && actual != nil && types.Identical(actual.Obj().Type(), method.Type())
				}
				if matches {
					return true
				}
			}
		}
		return false
	}
	// A Close/Sync-shaped assertion on an embedder or log writer is not storage
	// discovery merely because an Engine has the same method. The asserted
	// operand's complete static interface must admit a real compiled storage view.
	// any remains an open carrier; a disjoint service interface is excluded.
	storageOperand := func(typ types.Type) bool {
		if typ == nil {
			return false
		}
		contract, ok := typ.Underlying().(*types.Interface)
		if !ok {
			return false
		}
		if core != nil && engineType(core, contract) {
			return true
		}
		for _, implementation := range implementations {
			if engineType(implementation, contract) {
				return true
			}
		}
		return false
	}
	var sites []Site
	seen := map[struct{ family, key string }]bool{}
	emit := func(m workcensus.Member, family string) {
		key := struct{ family, key string }{family, m.ID}
		if seen[key] {
			return
		}
		seen[key] = true
		proof := "structural-policy"
		if family == "direct-capability-assertions" {
			proof = "capability-boundary"
		}
		if family == "direct-legacy-stored-reads" {
			proof = "stored-fact-operation"
		}
		if family == "direct-observed-dispatch" {
			proof = "exact-owner"
		}
		if family == "direct-search-read-consumers" {
			proof = "public-search-terminal"
		}
		if family == "configuration-handoffs" {
			proof = "configuration-identity"
		}
		if family == "constituent-admission" || family == "read-outcome-boundaries" {
			proof = "public-read-outcome"
		}
		sites = append(sites, Site{Population: family, Contract: family, Proof: proof, Key: m.ID, Path: memberPath(m), Function: owner(m), Line: m.Line, Detail: m.Code, Digest: m.Digest, Kind: m.Kind, Evidence: SiteEvidence{OwnerDigest: m.OwnerDigest, State: m.State, OperandType: m.OperandType, AssertionType: m.AssertionType, Signature: m.Signature, ReceiverType: m.ReceiverType, Callees: m.Callees}})
	}
	for _, member := range result.Inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		info := member.Syntax.Info
		if fn := declarations[owner(member)]; fn != nil && owners[fn.Origin()] {
			continue
		}
		switch node := member.Syntax.Node.(type) {
		case *ast.AssignStmt:
			for _, lhs := range node.Lhs {
				if configurationFields[object(info, lhs)] {
					emit(member, "configuration-handoffs")
					break
				}
				if selected, ok := lhs.(*ast.SelectorExpr); ok && searchFields[object(info, selected)] {
					emit(member, "direct-search-read-consumers")
					break
				}
			}
		case *ast.CompositeLit:
			for _, element := range node.Elts {
				if entry, ok := element.(*ast.KeyValueExpr); ok {
					if key, ok := entry.Key.(*ast.Ident); ok && configurationFields[info.Uses[key]] {
						emit(member, "configuration-handoffs")
						break
					}
					if key, ok := entry.Key.(*ast.Ident); ok && searchFields[info.Uses[key]] {
						emit(member, "direct-search-read-consumers")
						break
					}
				}
			}
		case *ast.TypeAssertExpr:
			// The type-switch itself is its one canonical source site.
			if node.Type == nil {
				continue
			}
			if engineType(info.TypeOf(node.X), core) || storageOperand(info.TypeOf(node.X)) && protocol(info.TypeOf(node.Type)) {
				emit(member, "direct-capability-assertions")
			}
		case *ast.TypeSwitchStmt:
			var operand ast.Expr
			switch assign := node.Assign.(type) {
			case *ast.AssignStmt:
				operand = assign.Rhs[0].(*ast.TypeAssertExpr).X
			case *ast.ExprStmt:
				operand = assign.X.(*ast.TypeAssertExpr).X
			}
			if operand == nil || !storageOperand(info.TypeOf(operand)) {
				continue
			}
			selected := false
			for _, statement := range node.Body.List {
				for _, typ := range statement.(*ast.CaseClause).List {
					selected = selected || protocol(info.TypeOf(typ))
				}
			}
			if selected {
				emit(member, "direct-capability-assertions")
			}
		case *ast.CallExpr:
			if method, ok := node.Fun.(*ast.SelectorExpr); ok && configurationFields[object(info, method.X)] {
				if signature, ok := info.TypeOf(node.Fun).Underlying().(*types.Signature); ok && signature.Results().Len() == 0 {
					for _, argument := range node.Args {
						switch info.TypeOf(argument).Underlying().(type) {
						case *types.Slice, *types.Map:
							emit(member, "configuration-handoffs")
						}
					}
				}
			}
			if fn := declarations[owner(member)]; fn != nil && requiredSearchOwners[fn.Origin()] {
				if callee, ok := object(info, node.Fun).(*types.Func); ok && requiredCoreMethods[callee.Origin()] {
					emit(member, "direct-search-read-consumers")
					continue
				}
			}
			if searchAdapterFields[object(info, node.Fun)] {
				emit(member, "direct-search-read-consumers")
				continue
			}
			if method, ok := node.Fun.(*ast.SelectorExpr); ok {
				if receiver, ok := method.X.(*ast.SelectorExpr); ok && searchAdapterFields[object(info, receiver)] {
					emit(member, "direct-search-read-consumers")
					continue
				}
			}
			fn, ok := object(info, node.Fun).(*types.Func)
			if !ok {
				for _, symbol := range member.Callees {
					for anchor, family := range functions {
						if anchor.FullName() == symbol {
							emit(member, family)
						}
					}
				}
				continue
			}
			if family := functions[fn.Origin()]; family != "" {
				emit(member, family)
				continue
			}
			if signature, ok := fn.Type().(*types.Signature); ok && signature.Recv() != nil {
				for anchor, family := range functions {
					if fn.Name() == anchor.Name() && types.Identical(fn.Type(), anchor.Type()) && engineType(signature.Recv().Type(), core) {
						emit(member, family)
						break
					}
				}
				for _, method := range storedMethods {
					if fn.Name() == method.Name() && types.Identical(fn.Type(), method.Type()) {
						emit(member, "direct-legacy-stored-reads")
						break
					}
				}
				for _, method := range observedMethods {
					if fn.Name() == method.Name() && types.Identical(fn.Type(), method.Type()) {
						emit(member, "direct-observed-dispatch")
						break
					}
				}
			}
		}
	}
	for _, member := range errorlessEntityReadUses(result.Inventory, core) {
		emit(member, "read-outcome-boundaries")
	}
	sites = append(sites, StoredFactOwners(result.Inventory)...)
	sites = append(sites, EngineDeletionViews(result)...)
	sort.Slice(sites, func(i, j int) bool {
		if sites[i].Population != sites[j].Population {
			return sites[i].Population < sites[j].Population
		}
		return sites[i].Key < sites[j].Key
	})
	return sites
}
