package storage

import (
	"go/ast"
	"go/types"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
)

// The canonical definition tree retains closures as separate owners. Join its
// ParentID edges to the selected method declaration instead of guessing from
// a closure's display name or reparsing its enclosing source.
func readCensusDeclarations(inventory workcensus.Inventory) map[string]*ast.FuncDecl {
	declarations := map[string]*ast.FuncDecl{}
	parents := map[string]string{}
	for _, definition := range inventory.Definitions {
		parents[definition.ID] = definition.ParentID
	}
	for _, member := range inventory.Members {
		if member.Syntax == nil {
			continue
		}
		if fn, ok := member.Syntax.Node.(*ast.FuncDecl); ok {
			declarations[member.OwnerID] = fn
		}
	}
	for _, definition := range inventory.Definitions {
		id := definition.ID
		for id != "" {
			if fn := declarations[id]; fn != nil {
				declarations[definition.ID] = fn
				break
			}
			id = parents[id]
		}
	}
	return declarations
}

func readCensusNamed(typ types.Type, name string) bool {
	if typ == nil {
		return false
	}
	typ = types.Unalias(typ)
	if ptr, ok := typ.(*types.Pointer); ok {
		typ = types.Unalias(ptr.Elem())
	}
	named, ok := typ.(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "github.com/orneryd/nornicdb/pkg/storage" && named.Obj().Name() == name
}

func readCensusPath(member workcensus.Member) string {
	path, _, _ := strings.Cut(member.ID, ":")
	return path
}
