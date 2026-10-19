package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This checks the production call boundary, independently of the queries in the
// inventory and independently of PROFILE instrumentation. A new direct caller
// of a scan/count/aggregation kernel cannot evade it by omitting diagnostics.
func TestRelationshipAccessProgramExecutionBoundary(t *testing.T) {
	type function struct {
		name, position string
		owner, typed, paths, io bool
		calls map[string]bool
	}
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	functions := map[string]*function{}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") { continue }
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil { continue }
			item := &function{name:fn.Name.Name, position:fset.Position(fn.Pos()).String(), calls:map[string]bool{}}
			if fn.Recv != nil {
				ast.Inspect(fn.Recv, func(n ast.Node) bool { if id, ok := n.(*ast.Ident); ok && id.Name == "relationshipAccessExecution" { item.owner = true }; return true })
			}
			ast.Inspect(fn.Type.Params, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && (id.Name == "TraversalMatch" || id.Name == "boundDegreeShape") { item.typed = true }
				return true
			})
			if fn.Type.Results != nil {
				ast.Inspect(fn.Type.Results, func(n ast.Node) bool { if a, ok := n.(*ast.ArrayType); ok { if id, ok := a.Elt.(*ast.Ident); ok && id.Name == "PathResult" { item.paths = true } }; return true })
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok { return true }
				name := ""
				switch target := call.Fun.(type) { case *ast.Ident: name=target.Name; case *ast.SelectorExpr: name=target.Sel.Name }
				item.calls[name] = true
				if ((strings.HasPrefix(name,"Get") || strings.HasPrefix(name,"Count")) && strings.Contains(name,"Edge")) || name == "SeekRelationships" || name == "RelationshipCount" { item.io = true }
				return true
			})
			// StorageExecutor's parsed-pattern readers have unique names. Keep
			// owner methods separately so their names form explicit graph cuts.
			key := item.name
			if item.owner { key = "program."+key }
			functions[key] = item
		}
	}
	ownerNames := map[string]bool{}
	for _, fn := range functions { if fn.owner { ownerNames[fn.name]=true } }
	require.NotEmpty(t, ownerNames, "no program execution boundary")
	// Compute relationship I/O reachability with program methods as graph
	// cuts. Merely reaching the owner is not an independent read kernel.
	for changed := true; changed; {
		changed=false
		for _, fn := range functions {
			if fn.owner || fn.io { continue }
			for callee := range fn.calls {
				if next:=functions[callee]; next!=nil && !ownerNames[callee] && next.io { fn.io=true; changed=true; break }
			}
		}
	}
	readers := map[string]bool{}
	for key, fn := range functions { if fn.typed && (fn.io || fn.paths) && !fn.owner { readers[key]=true } }
	// The internal kernel closure is derived from the program's actual calls,
	// not a maintained list of clauses, functions or file-name exemptions.
	kernel := map[string]bool{}
	var include func(string)
	include=func(name string) { if !readers[name] || kernel[name] { return }; kernel[name]=true; for next:=range functions[name].calls { include(next) } }
	for _, fn:=range functions { if fn.owner { for callee:=range fn.calls { include(callee) } } }
	require.NotEmpty(t, kernel)
	var population []string
	for name:=range kernel { population=append(population,functions[name].position+" "+name) }
	sort.Strings(population)
	t.Logf("program-owned relationship read kernels (%d):\n%s",len(population),strings.Join(population,"\n"))
	for name, fn := range functions {
		for callee:=range fn.calls {
			if readers[callee] && !fn.owner && !kernel[name] {
				t.Errorf("%s: %s bypasses relationshipAccessProgram execution boundary via %s",fn.position,name,callee)
			}
			if callee == "relationshipAccessFor" && name != "relationshipExecutionFor" && name != "relationshipLocalAccessSites" {
				t.Errorf("%s: independent relationship plan consumer %s",fn.position,name)
			}
		}
	}
}
