package workcensus

import (
	"fmt"
	"go/token"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/callgraph/cha"
	"golang.org/x/tools/go/callgraph/vta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

// Program is the engine's read-only, process-local analysis view. Domain clients
// may analyze effects, method sets or def/use over these loaded objects without
// another loader, SSA builder, call graph or generic source traversal. Functions
// includes generic instances; clients retain domain-specific instance policies.
type Program struct {
	Root      string
	FileSet   *token.FileSet
	Packages  []*packages.Package
	SSA       *ssa.Program
	Functions []*ssa.Function
	CallGraph *callgraph.Graph
}

// LoadProgram owns generic package loading, SSA construction and VTA/CHA. It
// performs no domain classification. SSA-effect clients can use this view
// without requesting an additional syntax inventory; Extract uses this loader.
func LoadProgram(c Config) (*Program, error) {
	if c.SourceOnly {
		return nil, fmt.Errorf("source-only extraction has no SSA program; use Extract")
	}
	root, err := filepath.Abs(c.Dir)
	if err != nil {
		return nil, err
	}
	if len(c.Patterns) == 0 {
		return nil, fmt.Errorf("Go program view requires package patterns")
	}
	pkgs, _, err := loadPackages(c, root)
	if err != nil {
		return nil, err
	}
	prog, _ := ssautil.Packages(pkgs, ssa.InstantiateGenerics)
	prog.Build()
	all := ssautil.AllFunctions(prog)
	result := &Program{Root: root, FileSet: prog.Fset, Packages: pkgs, SSA: prog, CallGraph: vta.CallGraph(all, cha.CallGraph(prog))}
	for fn := range all {
		result.Functions = append(result.Functions, fn)
	}
	sort.Slice(result.Functions, func(i, j int) bool {
		a, b := result.Functions[i], result.Functions[j]
		if a.String() != b.String() {
			return a.String() < b.String()
		}
		return a.Pos() < b.Pos()
	})
	return result, nil
}

// loadPackages is the single typed loader for both extraction modes.
func loadPackages(c Config, root string) ([]*packages.Package, *token.FileSet, error) {
	fset := token.NewFileSet()
	pkgs, err := packages.Load(&packages.Config{Fset: fset, Mode: packages.LoadSyntax | c.LoadMode, Dir: root, BuildFlags: c.BuildFlags, Tests: c.Tests, Env: c.Env, Overlay: c.Overlay}, c.Patterns...)
	if err != nil {
		return nil, nil, err
	}
	var loadErrors []string
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, err := range p.Errors {
			loadErrors = append(loadErrors, fmt.Sprintf("%s: %v", p.PkgPath, err))
		}
	})
	if len(loadErrors) > 0 {
		sort.Strings(loadErrors)
		return nil, nil, fmt.Errorf("source does not type-check: %s", strings.Join(loadErrors, "\n"))
	}
	return pkgs, fset, nil
}
