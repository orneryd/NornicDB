package capabilitycensus

import (
	"go/ast"
	"go/types"

	"github.com/orneryd/nornicdb/internal/workcensus"
)

// Author: Lane #1644 (Codex). These queries interpret the common engine's
// compiled objects. The source inventory is a cross-check, not the population.
// No query supplies a reviewed disposition or reason.
type compiledDomain struct {
	core      *types.Interface
	methods   map[string][]types.Type
	protocols map[*types.Func]string
	objects   map[string][]*types.Func
	mutations map[*types.Func]bool
	observed  []*types.Interface
	views     []*types.Interface
}

func newCompiledDomain(inventory workcensus.Inventory, core *types.Interface, methods map[string][]types.Type) *compiledDomain {
	d := &compiledDomain{core: core, methods: methods, protocols: map[*types.Func]string{}, objects: map[string][]*types.Func{}, mutations: map[*types.Func]bool{}}
	packages := map[*types.Package]bool{}
	functions := map[*types.Func]bool{}
	for _, member := range inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		if pkg := member.Syntax.Package; pkg != nil {
			packages[pkg] = true
		}
		if id, ok := member.Syntax.Node.(*ast.Ident); ok {
			if fn, ok := member.Syntax.Info.ObjectOf(id).(*types.Func); ok && !functions[fn] {
				functions[fn] = true
				d.objects[fn.FullName()] = append(d.objects[fn.FullName()], fn)
			}
		}
	}
	for pkg := range packages {
		if pkg.Path() != storagePackage && pkg.Path() != "github.com/orneryd/nornicdb/pkg/cypher" {
			continue
		}
		if pkg.Path() == storagePackage {
			for _, name := range []string{"StoredReadProvider", "StoredEntityReader", "ReadSnapshotView", "constraintGraphView"} {
				if obj := pkg.Scope().Lookup(name); obj != nil {
					if contract, ok := obj.Type().Underlying().(*types.Interface); ok {
						d.views = append(d.views, contract)
					}
				}
			}
			for _, name := range []string{"ObservedNodeUpdater", "ObservedEdgeUpdater"} {
				if obj := pkg.Scope().Lookup(name); obj != nil {
					if contract, ok := obj.Type().Underlying().(*types.Interface); ok {
						d.observed = append(d.observed, contract)
					}
				}
			}
			for _, name := range []string{"UpdateObservedNode", "UpdateObservedEdge"} {
				if fn, ok := pkg.Scope().Lookup(name).(*types.Func); ok {
					d.mutations[fn.Origin()] = true
				}
			}
		}
		// Anchor existing protocol declarations by their actual package-scope
		// objects. A same-spelled function elsewhere is never a protocol call.
		for symbol, role := range lookupPolicy {
			if fn, ok := pkg.Scope().Lookup(symbol).(*types.Func); ok {
				d.protocols[fn.Origin()] = role
			}
		}
		for _, symbol := range pkg.Scope().Names() {
			obj, ok := pkg.Scope().Lookup(symbol).(*types.TypeName)
			if !ok {
				continue
			}
			_, isInterface := obj.Type().Underlying().(*types.Interface)
			if !engineType(obj.Type(), core) && !capabilityType(obj.Type(), core, methods) && !(pkg.Path() == storagePackage && isInterface) && !(pkg.Path() == "github.com/orneryd/nornicdb/pkg/cypher" && symbol == "StorageExecutor") {
				continue
			}
			set := types.NewMethodSet(types.NewPointer(obj.Type()))
			for i := 0; i < set.Len(); i++ {
				fn, ok := set.At(i).Obj().(*types.Func)
				if !ok {
					continue
				}
				if role, ok := lookupPolicy[fn.Name()]; ok {
					d.protocols[fn.Origin()] = role
				}
			}
		}
	}
	return d
}

func (d *compiledDomain) policy(obj types.Object) string {
	if fn, ok := obj.(*types.Func); ok {
		return d.protocols[fn.Origin()]
	}
	return ""
}

// Entity values and their collections/callbacks identify read data flow. Names
// are exact declarations in the loaded storage package, never function patterns.
func entityValue(typ types.Type) bool {
	if typ == nil {
		return false
	}
	typ = types.Unalias(typ)
	if named, ok := typ.(*types.Named); ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == storagePackage {
		switch named.Obj().Name() {
		case "Node", "Edge", "NodeID", "EdgeID", "EntityIdentity":
			return true
		}
		return storedFactType(named)
	}
	switch value := typ.(type) {
	case *types.Pointer:
		return entityValue(value.Elem())
	case *types.Slice:
		return entityValue(value.Elem())
	case *types.Array:
		return entityValue(value.Elem())
	case *types.Map:
		return entityValue(value.Key()) || entityValue(value.Elem())
	case *types.Signature:
		for i := 0; i < value.Params().Len(); i++ {
			if entityValue(value.Params().At(i).Type()) {
				return true
			}
		}
		for i := 0; i < value.Results().Len(); i++ {
			if entityValue(value.Results().At(i).Type()) {
				return true
			}
		}
	}
	return false
}

func (d *compiledDomain) read(signature *types.Signature) bool {
	if signature == nil {
		return false
	}
	for i := 0; i < signature.Results().Len(); i++ {
		if entityValue(signature.Results().At(i).Type()) {
			return true
		}
	}
	for i := 0; i < signature.Params().Len(); i++ {
		parameter := signature.Params().At(i).Type()
		switch parameter.Underlying().(type) {
		case *types.Slice, *types.Array, *types.Map, *types.Signature:
			if entityValue(parameter.Underlying()) {
				return true
			}
		}
		if callback, ok := parameter.Underlying().(*types.Signature); ok && entityValue(callback) {
			return true
		}
	}
	if signature.Recv() != nil && engineType(signature.Recv().Type(), d.core) {
		// Scalar operations and iterator setup on a real Engine also own work;
		// an identically spelled service operation is a different typed object.
		return true
	}
	return false
}

func (d *compiledDomain) call(member workcensus.Member, call *ast.CallExpr) (policy string, read bool) {
	info := member.Syntax.Info
	policy = d.policy(object(info, call.Fun))
	if typ := info.TypeOf(call.Fun); typ != nil {
		if signature, ok := typ.Underlying().(*types.Signature); ok {
			read = d.read(signature)
		}
	}
	for _, symbol := range member.Callees {
		for _, fn := range d.objects[symbol] {
			if role := d.policy(fn); policy == "" {
				policy = role
			}
			read = read || d.read(fn.Type().(*types.Signature))
		}
	}
	return policy, read
}

func (d *compiledDomain) mutation(fn *types.Func) bool {
	if d.mutations[fn.Origin()] {
		return true
	}
	sig := fn.Type().(*types.Signature)
	if sig.Recv() == nil {
		return false
	}
	for _, contract := range d.observed {
		if !types.Implements(sig.Recv().Type(), contract) {
			continue
		}
		for i := 0; i < contract.NumMethods(); i++ {
			method := contract.Method(i)
			if fn.Name() == method.Name() && types.Identical(fn.Type(), method.Type()) {
				return true
			}
		}
	}
	// Ordinary update fallbacks are the actual Engine update contracts. A
	// same-spelled service taking string is not an implementation of either.
	for i := 0; d.core != nil && i < d.core.NumMethods(); i++ {
		method := d.core.Method(i)
		if (method.Name() == "UpdateNode" || method.Name() == "UpdateEdge") && fn.Name() == method.Name() && types.Identical(fn.Type(), method.Type()) {
			return true
		}
	}
	return false
}

func (d *compiledDomain) carries(typ types.Type) bool {
	return engineType(typ, d.core) || capabilityType(typ, d.core, d.methods) || storedFactType(typ)
}

// A metadata query's actual instantiated interface supplies its implementations.
// Namespace/lease/limit methods are not selected from their identifier spelling.
func metadataContracts(inventory workcensus.Inventory, domain *compiledDomain) []*types.Interface {
	var contracts []*types.Interface
	for _, member := range inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		call, ok := member.Syntax.Node.(*ast.CallExpr)
		if !ok {
			continue
		}
		policy := domain.policy(object(member.Syntax.Info, call.Fun))
		if policy != "view-metadata" && policy != "wrapper-metadata" {
			continue
		}
		var identifier *ast.Ident
		switch function := callBase(call.Fun).(type) {
		case *ast.Ident:
			identifier = function
		case *ast.SelectorExpr:
			identifier = function.Sel
		}
		instance, ok := member.Syntax.Info.Instances[identifier]
		if !ok {
			continue
		}
		for i := 0; i < instance.TypeArgs.Len(); i++ {
			if contract, ok := instance.TypeArgs.At(i).Underlying().(*types.Interface); ok && contract.NumMethods() > 0 {
				contracts = append(contracts, contract)
			}
		}
	}
	return contracts
}

func implementsMetadataMethod(fn *types.Func, contracts []*types.Interface) bool {
	signature := fn.Type().(*types.Signature)
	if signature.Recv() == nil {
		return false
	}
	receiver := signature.Recv().Type()
	set := types.NewMethodSet(receiver)
	for _, contract := range contracts {
		if !types.Implements(receiver, contract) {
			continue
		}
		for i := 0; i < contract.NumMethods(); i++ {
			method := contract.Method(i)
			if implementation := set.Lookup(method.Pkg(), method.Name()); implementation != nil {
				if actual, ok := implementation.Obj().(*types.Func); ok && actual.Origin() == fn.Origin() {
					return true
				}
			}
		}
	}
	return false
}

func compiledPopulation(result *Result, core *types.Interface, methods map[string][]types.Type) error {
	domain := newCompiledDomain(result.Inventory, core, methods)
	commits := newCommitTypes(result.Inventory)
	metadata := metadataContracts(result.Inventory, domain)
	metadataFunctions := map[*types.Func]bool{}
	declarations := map[string]workcensus.Member{}
	definitionKeys := map[*types.Func]string{}
	reads, lookups := map[string]bool{}, map[string]bool{}
	commitRoles := map[*types.Func]string{}
	for _, member := range result.Inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		if fn, ok := member.Syntax.Node.(*ast.FuncDecl); ok {
			declarations[owner(member)] = member
			if obj, ok := member.Syntax.Info.Defs[fn.Name].(*types.Func); ok {
				definitionKeys[obj.Origin()] = owner(member)
				reads[owner(member)] = domain.read(obj.Type().(*types.Signature))
				commitRoles[obj.Origin()] = commits.declaration(obj)
				policy := domain.policy(obj)
				metadataFunctions[obj.Origin()] = policy == "membership" || policy == "view-metadata" || policy == "wrapper-metadata" || implementsMetadataMethod(obj, metadata)
				for i := 0; i < obj.Type().(*types.Signature).Results().Len(); i++ {
					lookups[owner(member)] = lookups[owner(member)] || domain.carries(obj.Type().(*types.Signature).Results().At(i).Type())
				}
			}
		}
	}
	// Resolve adapters from actual call/return types. Propagation joins compiler
	// objects, including canonical alias targets; spelling never selects a caller.
	for changed := true; changed; {
		changed = false
		for _, member := range result.Inventory.Members {
			if member.Syntax == nil || member.Syntax.Info == nil {
				continue
			}
			key := owner(member)
			call, ok := member.Syntax.Node.(*ast.CallExpr)
			if !ok {
				continue
			}
			policy, read := domain.call(member, call)
			var targets []*types.Func
			if fn, ok := object(member.Syntax.Info, call.Fun).(*types.Func); ok {
				targets = append(targets, fn)
			}
			for _, symbol := range member.Callees {
				targets = append(targets, domain.objects[symbol]...)
			}
			// A generic source loop has type parameters, while its actual call
			// signature can carry nodes. Join that instantiation to the canonical
			// source owner, including alias targets supplied by the common engine.
			for _, target := range targets {
				if targetKey, ok := definitionKeys[target.Origin()]; ok && read && !reads[targetKey] {
					reads[targetKey] = true
				}
				if decl, ok := declarations[key]; ok {
					caller := decl.Syntax.Info.Defs[decl.Syntax.Node.(*ast.FuncDecl).Name].(*types.Func)
					if _, known := definitionKeys[target.Origin()]; known && metadataFunctions[caller.Origin()] && !metadataFunctions[target.Origin()] {
						metadataFunctions[target.Origin()] = true
						changed = true
					}
				}
			}
			reads[key] = reads[key] || read
			lookups[key] = lookups[key] || policy != ""
			decl, ok := declarations[key]
			if !ok {
				continue
			}
			fn := decl.Syntax.Info.Defs[decl.Syntax.Node.(*ast.FuncDecl).Name].(*types.Func)
			if policy != "" && lookupAdapterCandidate(fn, core) && domain.policy(fn) == "" {
				domain.protocols[fn.Origin()] = "binding"
				changed = true
			}
			role := commits.call(member, call)
			if callee, ok := object(member.Syntax.Info, call.Fun).(*types.Func); ok {
				if role == "" {
					role = commitRoles[callee.Origin()]
				}
			}
			if role != "" && (commits.producer(fn.Type().(*types.Signature)) || lookupAdapterCandidate(fn, core)) {
				merged := mergeCommitConsumer(commitRoles[fn.Origin()], role)
				if merged != commitRoles[fn.Origin()] {
					commitRoles[fn.Origin()] = merged
					changed = true
				}
			}
		}
	}
	liveFields := map[*types.Var]bool{}
	for _, member := range result.Inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		decl, ok := declarations[owner(member)]
		if !ok {
			continue
		}
		fn, ok := decl.Syntax.Info.Defs[decl.Syntax.Node.(*ast.FuncDecl).Name].(*types.Func)
		if !ok {
			continue
		}
		if !metadataFunctions[fn.Origin()] {
			continue
		}
		if id, ok := member.Syntax.Node.(*ast.Ident); ok {
			if field, ok := member.Syntax.Info.ObjectOf(id).(*types.Var); ok && field.IsField() {
				liveFields[field] = true
			}
		}
	}
	emit := func(m workcensus.Member, population, contract string) {
		result.Sites = append(result.Sites, Site{Population: population, Key: m.ID, Path: memberPath(m), Function: owner(m), Line: m.Line, Contract: contract, Proof: proof(contract), Detail: m.Code, Digest: m.Digest, Kind: m.Kind, Evidence: SiteEvidence{OwnerDigest: m.OwnerDigest, State: m.State, OperandType: m.OperandType, AssertionType: m.AssertionType, Signature: m.Signature, ReceiverType: m.ReceiverType, Callees: m.Callees, ArgTypes: m.ArgTypes, ElementType: m.ElementType}})
	}
	for _, m := range result.Inventory.Members {
		if m.Syntax == nil || m.Syntax.Info == nil {
			continue
		}
		info, key := m.Syntax.Info, owner(m)
		switch node := m.Syntax.Node.(type) {
		case *ast.FuncDecl:
			fn, ok := info.Defs[node.Name].(*types.Func)
			if !ok {
				continue
			}
			signature := fn.Type().(*types.Signature)
			if domain.mutation(fn) {
				emit(m, "observed_mutations", "observed-owner")
			}
			if contract := batchReadShape(signature); contract != "" {
				emit(m, "batch_point_pairs", contract)
			}
			if signature.Recv() != nil && engineType(signature.Recv().Type(), core) {
				emit(m, "engine_operations", "engine-operation")
			}
			if lookups[key] || domain.policy(fn) != "" {
				emit(m, "lookups", "binding")
			}
			if reads[key] {
				emit(m, "fallbacks_streams", "read-owner")
			}
			if role := commitRoles[fn.Origin()]; role != "" {
				emit(m, "commit_owners", role)
			}
		case *ast.TypeSpec:
			obj, _ := info.Defs[node.Name].(*types.TypeName)
			if obj == nil {
				continue
			}
			if _, iface := obj.Type().Underlying().(*types.Interface); iface {
				continue
			}
			stored := engineType(obj.Type(), core)
			for _, contract := range domain.views {
				stored = stored || engineType(obj.Type(), contract)
			}

			if stored {
				emit(m, "stored_views", "stored-mode")
				typeKey := obj.Pkg().Path() + "." + obj.Name()
				if obj.Parent() != obj.Pkg().Scope() {
					typeKey = m.ID
				}
				result.StoredViews[typeKey] = obj
			}
		case *ast.TypeAssertExpr:
			contract := ""
			if decl, ok := declarations[key]; ok {
				if fn, ok := decl.Syntax.Info.Defs[decl.Syntax.Node.(*ast.FuncDecl).Name].(*types.Func); ok && domain.policy(fn) == "exact" {
					contract = "exact"
				}
			}
			if contract == "" && capabilityAssertion(info, node, core, methods) {
				contract = "capability-assertion"
			}
			if contract != "" {
				emit(m, "assertions", contract)
			}
			if node.Type != nil && storedFactType(info.TypeOf(node.Type)) {
				emit(m, "stored_fact_consumers", "stored-mode")
			}
		case *ast.TypeSwitchStmt:
			found := false
			for _, statement := range node.Body.List {
				for _, candidate := range statement.(*ast.CaseClause).List {
					found = found || capabilityType(info.TypeOf(candidate), core, methods)
				}
			}
			if found {
				emit(m, "assertions", "capability-assertion")
			}
		case *ast.ForStmt, *ast.RangeStmt, *ast.GoStmt:
			if reads[key] || lookups[key] {
				emit(m, "fallbacks_streams", "scoped-loop")
			}
		case *ast.AssignStmt:
			for _, left := range node.Lhs {
				selection, ok := left.(*ast.SelectorExpr)
				if !ok {
					continue
				}
				if selected := info.Selections[selection]; selected != nil && selected.Kind() == types.FieldVal && (domain.carries(selected.Type()) || liveFields[selected.Obj().(*types.Var)]) {
					emit(m, "lookups", "binding")
					break
				}
			}
		case *ast.CompositeLit:
			record, ok := info.TypeOf(node).Underlying().(*types.Struct)
			if !ok {
				continue
			}
			for i, value := range node.Elts {
				var field *types.Var
				if pair, ok := value.(*ast.KeyValueExpr); ok {
					if name, ok := pair.Key.(*ast.Ident); ok {
						field, _ = info.ObjectOf(name).(*types.Var)
					}
				} else if i < record.NumFields() {
					field = record.Field(i)
				}
				if field != nil && (liveFields[field] || domain.carries(field.Type())) {
					emit(m, "lookups", "binding")
					break
				}
			}
		case *ast.CallExpr:
			policy, read := domain.call(m, node)
			if independentPhysicalProgression(m, core) {
				policy = "independent-physical-walk"
			}
			if policy != "" {
				emit(m, "lookups", policy)
			}
			if policy == "stored-mode" || unindexedRequestMembership(m) {
				contract := "stored-mode"
				if unindexedRequestMembership(m) {
					contract = "unindexed-request-membership"
				}
				emit(m, "stored_fact_consumers", contract)
			}
			if read {
				emit(m, "fallbacks_streams", "read-call")
			}
			role := commits.call(m, node)
			if fn, ok := object(info, node.Fun).(*types.Func); ok {
				if role == "" {
					role = commitRoles[fn.Origin()]
				}
			}
			if role != "" {
				emit(m, "commit_owners", role)
			}
			if fn, ok := object(info, node.Fun).(*types.Func); ok && domain.mutation(fn) {
				emit(m, "observed_mutations", "observed")
			}

		case *ast.Ident:
			obj := info.ObjectOf(node)
			if obj == nil {
				continue
			}
			if storedFactType(obj.Type()) {
				emit(m, "stored_fact_consumers", "stored-mode")
			}
			if fn, ok := obj.(*types.Func); ok {
				if domain.mutation(fn) {
					emit(m, "observed_mutations", "observed")
				}
				if policy := domain.policy(fn); policy != "" {
					emit(m, "lookups", policy)
				}
				if role := commitRoles[fn.Origin()]; role != "" {
					emit(m, "commit_owners", role)
				}
			}
		}
	}
	return nil
}
