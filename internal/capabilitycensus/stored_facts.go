package capabilitycensus

import (
	"go/ast"
	"go/types"
	"sort"

	"github.com/orneryd/nornicdb/internal/workcensus"
)

// StoredFactOwners selects direct fact operations and field uses from compiled
// objects. Workcensus owns enumeration, identities, enclosing definitions and
// body digests. One canonical function body is the review member; its direct
// accesses are evidence, never transitive membership or semantic approval.
// Author: Lane #1644 (Codex).
func StoredFactOwners(inventory workcensus.Inventory) []Site {
	facts := map[types.Type]bool{}
	fields := map[types.Object]bool{}
	presence := map[*types.Func]bool{}
	packages := map[*types.Package]bool{}
	declarations := map[string]workcensus.Member{}
	parents := map[string]string{}
	for _, definition := range inventory.Definitions {
		parents[definition.ID] = definition.ParentID
	}
	for _, member := range inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		if pkg := member.Syntax.Package; pkg != nil {
			packages[pkg] = true
		}
		if _, ok := member.Syntax.Node.(*ast.FuncDecl); ok {
			declarations[member.OwnerID] = member
		}
	}
	for pkg := range packages {
		if pkg.Path() != storagePackage {
			continue
		}
		// Exact declared domain types, resolved in each compiled package
		// instance. Aliases preserve identity; unrelated same-shaped types do not.
		for _, name := range []string{"StoredNodeFact", "StoredEdgeFact", "StoredReadIdentity", "StoredReference", "StoredMetadata", "StoredNodeProjection", "StoredEdgeProjection", "StoredProjection", "StoredFullRead", "StoredReadResult"} {
			object := pkg.Scope().Lookup(name)
			if object == nil {
				continue
			}
			facts[types.Unalias(object.Type())] = true
			if record, ok := object.Type().Underlying().(*types.Struct); ok {
				for i := 0; i < record.NumFields(); i++ {
					fields[record.Field(i)] = true
				}
			}
		}
		if fn, ok := pkg.Scope().Lookup("StoredNodePresent").(*types.Func); ok {
			presence[fn.Origin()] = true
		}
	}
	var contains func(types.Type) bool
	contains = func(typ types.Type) bool {
		if typ == nil {
			return false
		}
		typ = types.Unalias(typ)
		if facts[typ] {
			return true
		}
		switch value := typ.(type) {
		case *types.Pointer:
			return contains(value.Elem())
		case *types.Slice:
			return contains(value.Elem())
		case *types.Array:
			return contains(value.Elem())
		case *types.Map:
			return contains(value.Key()) || contains(value.Elem())
		case *types.Tuple:
			for i := 0; i < value.Len(); i++ {
				if contains(value.At(i).Type()) {
					return true
				}
			}
		}
		return false
	}
	accesses := map[string][]string{}
	for _, member := range inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		info := member.Syntax.Info
		selected := false
		switch node := member.Syntax.Node.(type) {
		case *ast.CallExpr:
			// Includes interface calls and calls through retained function
			// values: the actual result type carries the fact contract.
			selected = contains(info.TypeOf(node))
			if fn, ok := object(info, node.Fun).(*types.Func); ok {
				selected = selected || presence[fn.Origin()]
			}
		case *ast.Ident:
			selected = fields[info.Uses[node]]
		case *ast.CompositeLit:
			selected = contains(info.TypeOf(node))
		case *ast.TypeAssertExpr:
			if node.Type != nil {
				selected = contains(info.TypeOf(node.Type))
			}
		}
		if !selected {
			continue
		}
		// Follow the engine's definition tree, including closure parents. No
		// source walk or display-name parsing supplies containing identities.
		id := member.OwnerID
		for id != "" {
			if _, ok := declarations[id]; ok {
				break
			}
			id = parents[id]
		}
		if id != "" {
			accesses[id] = append(accesses[id], member.ID)
		}
	}
	var out []Site
	for id, uses := range accesses {
		declaration := declarations[id]
		sort.Strings(uses)
		out = append(out, Site{Population: "stored-fact-owner-bodies", Key: declaration.ID, Path: memberPath(declaration), Function: owner(declaration), Line: declaration.Line, Kind: declaration.Kind, Digest: declaration.Digest, Contract: "direct-stored-facts", Proof: "stored-fact-operation", Detail: declaration.Code, Evidence: SiteEvidence{OwnerDigest: declaration.OwnerDigest, State: declaration.State, Callees: uses}})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
