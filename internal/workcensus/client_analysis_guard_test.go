package workcensus

// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// censusClients are the packages that consume the engine's analyses.
var censusClients = []string{"./testing/capturecensus", "./testing/storagecensus"}

// clientNilParsers returns every non-test function of the loaded client
// packages that parses a nil comparison itself: it inspects an *ssa.BinOp,
// uses token.EQL or token.NEQ, and calls (*ssa.Const).IsNil. (An *ssa.BinOp
// alone is ordinary value analysis, such as string concatenation.) Nil comparisons, nil tests, call success and
// known-non-nil facts each have one implementation in the engine
// (NilComparison, NilSuccessEdge, NonNilAt, SuccessfulCallEdge); a client that
// re-derives them is a second implementation.
func clientNilParsers(pkgs []*packages.Package) []string {
	const ssaPath = "golang.org/x/tools/go/ssa"
	isSSA := func(t types.Type, name string) bool {
		if ptr, ok := t.(*types.Pointer); ok {
			t = ptr.Elem()
		}
		named, ok := types.Unalias(t).(*types.Named)
		return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == ssaPath && named.Obj().Name() == name
	}
	var out []string
	for _, pkg := range pkgs {
		for i, file := range pkg.Syntax {
			name := pkg.CompiledGoFiles[i]
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				binOp, isNil, equality := false, false, false
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					switch n := n.(type) {
					case *ast.TypeAssertExpr:
						if n.Type != nil && isSSA(pkg.TypesInfo.TypeOf(n.Type), "BinOp") {
							binOp = true
						}
					case *ast.CaseClause:
						for _, e := range n.List {
							if isSSA(pkg.TypesInfo.TypeOf(e), "BinOp") {
								binOp = true
							}
						}
					case *ast.SelectorExpr:
						if n.Sel.Name == "EQL" || n.Sel.Name == "NEQ" {
							if obj := pkg.TypesInfo.Uses[n.Sel]; obj != nil && obj.Pkg() != nil && obj.Pkg().Path() == "go/token" {
								equality = true
							}
						}
						if n.Sel.Name == "IsNil" {
							if sel := pkg.TypesInfo.Selections[n]; sel != nil && isSSA(sel.Recv(), "Const") {
								isNil = true
							}
						}
					}
					return true
				})
				if binOp && isNil && equality {
					out = append(out, pkg.PkgPath+"."+fn.Name.Name+" ("+filepath.Base(name)+")")
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

func loadCensusClients(t *testing.T, overlay map[string][]byte) []*packages.Package {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps, Dir: root, BuildFlags: []string{"-tags=noui,nolocalllm"}, Overlay: overlay}
	pkgs, err := packages.Load(cfg, censusClients...)
	if err != nil {
		t.Fatal(err)
	}
	if len(pkgs) != len(censusClients) {
		t.Fatalf("loaded %d census client packages, want %d", len(pkgs), len(censusClients))
	}
	for _, pkg := range pkgs {
		if len(pkg.Errors) > 0 {
			t.Fatalf("%s: %v", pkg.PkgPath, pkg.Errors)
		}
	}
	return pkgs
}

// The census clients reuse the engine's nil-comparison and call-success
// analyses; none implements its own.
func TestCensusClientsHaveNoPrivateNilAnalysis(t *testing.T) {
	if found := clientNilParsers(loadCensusClients(t, nil)); len(found) != 0 {
		t.Fatalf("census clients parse nil comparisons themselves (use the engine's analyses): %v", found)
	}
}

// Planted control: a client copy of the deleted private call-success helper
// (the storage census's former successEdge) is reported.
func TestCensusClientsGuardPlant(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	plant := filepath.Join(root, "testing", "capturecensus", "zz_guard_plant.go")
	src := []byte(`package capturecensus

import (
	"go/token"

	"golang.org/x/tools/go/ssa"
)

func plantedSuccessEdge(e ssa.Value) (*ssa.If, *ssa.BasicBlock) {
	for _, r := range *e.Referrers() {
		cmp, ok := r.(*ssa.BinOp)
		if !ok || (cmp.Op != token.NEQ && cmp.Op != token.EQL) {
			continue
		}
		if c, ok := cmp.Y.(*ssa.Const); !ok || !c.IsNil() {
			continue
		}
		for _, u := range *cmp.Referrers() {
			if iff, ok := u.(*ssa.If); ok {
				return iff, iff.Block().Succs[1]
			}
		}
	}
	return nil, nil
}
`)
	found := clientNilParsers(loadCensusClients(t, map[string][]byte{plant: src}))
	if len(found) != 1 || !strings.Contains(found[0], "plantedSuccessEdge") {
		t.Fatalf("planted private nil analysis not reported exactly: %v", found)
	}
}
