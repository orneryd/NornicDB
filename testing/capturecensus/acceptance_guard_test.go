package capturecensus

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

// recorderBypasses returns every non-test function of the capture census that
// admits a finding without the one recorder: it sets a ContextSite's Reason,
// or appends to an []Acceptance, outside the acceptanceLog methods.
func recorderBypasses(pkg *packages.Package) []string {
	named := func(t types.Type, name string) bool {
		if t == nil {
			return false
		}
		if ptr, ok := t.(*types.Pointer); ok {
			t = ptr.Elem()
		}
		n, ok := types.Unalias(t).(*types.Named)
		return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == pkg.PkgPath && n.Obj().Name() == name
	}
	var out []string
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
			if fn.Recv != nil && len(fn.Recv.List) == 1 && named(pkg.TypesInfo.TypeOf(fn.Recv.List[0].Type), "acceptanceLog") {
				continue // the recorder itself
			}
			bypass := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.AssignStmt:
					for _, lhs := range n.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "Reason" && named(pkg.TypesInfo.TypeOf(sel.X), "ContextSite") {
							bypass = true
						}
					}
				case *ast.CallExpr:
					if id, ok := n.Fun.(*ast.Ident); ok && id.Name == "append" && len(n.Args) > 0 {
						if slice, ok := pkg.TypesInfo.TypeOf(n.Args[0]).(*types.Slice); ok && named(slice.Elem(), "Acceptance") {
							bypass = true
						}
					}
				}
				return true
			})
			if bypass {
				out = append(out, fn.Name.Name+" ("+filepath.Base(name)+")")
			}
		}
	}
	sort.Strings(out)
	return out
}

func loadCaptureCensus(t *testing.T, overlay map[string][]byte) *packages.Package {
	t.Helper()
	dir, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps, Dir: dir, BuildFlags: []string{"-tags=noui,nolocalllm"}, Overlay: overlay}
	pkgs, err := packages.Load(cfg, ".")
	if err != nil {
		t.Fatal(err)
	}
	if len(pkgs) != 1 || len(pkgs[0].Errors) > 0 {
		t.Fatalf("load capture census: %v", pkgs)
	}
	return pkgs[0]
}

// Every admission of the capture census goes through its one recorder.
func TestCaptureCensusAdmitsOnlyThroughTheRecorder(t *testing.T) {
	if found := recorderBypasses(loadCaptureCensus(t, nil)); len(found) != 0 {
		t.Fatalf("admissions outside the acceptance recorder: %v", found)
	}
}

// Planted control: a function that labels a site itself is reported.
func TestCaptureCensusRecorderGuardPlant(t *testing.T) {
	dir, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	plant := filepath.Join(dir, "zz_recorder_plant.go")
	src := []byte(`package capturecensus

func plantedSilentAdmission(site *ContextSite) { site.Reason = "trusted" }
`)
	found := recorderBypasses(loadCaptureCensus(t, map[string][]byte{plant: src}))
	if len(found) != 1 || !strings.Contains(found[0], "plantedSilentAdmission") {
		t.Fatalf("planted admission outside the recorder not reported exactly: %v", found)
	}
}
