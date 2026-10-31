package workcensus

// Context-owner convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"fmt"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/callgraph"
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

	nilFlow *nilFlow        // MayBeNil's memo; a Program is queried by one goroutine
	// dynamicTargets is CallTargets' memo of DynamicCallTargets.
	dynamicTargets map[ssa.CallInstruction][]*ssa.Function
	load    packages.Config // the load settings, reused to read a dependency's bodies on demand
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
	all := sourceFunctions(prog, pkgs)
	// VTA's nil initial graph constructs its CHA candidates from this exact
	// function population. cha.CallGraph(prog) uses AllFunctions internally,
	// which would again omit the otherwise-unreferenced source methods.
	result := &Program{Root: root, FileSet: prog.Fset, Packages: pkgs, SSA: prog, CallGraph: vta.CallGraph(all, nil), load: packages.Config{Dir: root, BuildFlags: c.BuildFlags, Env: c.Env}}
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

// sourceFunctions retains linker-style instances/wrappers AND every function
// defined in loaded, typed source. AllFunctions alone skips methods on generic
// types and unreferenced methods of unexported types. An all-source declaration
// must be queryable as a root without first adding an artificial invocation.
func sourceFunctions(prog *ssa.Program, pkgs []*packages.Package) map[*ssa.Function]bool {
	all := ssautil.AllFunctions(prog)
	var queue []*ssa.Function
	for fn := range all {
		queue = append(queue, fn)
	}
	for _, pkg := range pkgs {
		if pkg.TypesInfo == nil {
			continue
		}
		for _, object := range pkg.TypesInfo.Defs {
			if object, ok := object.(*types.Func); ok {
				if fn := prog.FuncValue(object); fn != nil && !all[fn] {
					all[fn] = true
					queue = append(queue, fn)
				}
			}
		}
	}
	// Reuse the engine's generic successor enumeration for lexical closures
	// and compiler-produced function operands; no client source walk is needed.
	view := Program{}
	for len(queue) > 0 {
		fn := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		for _, next := range view.functionSuccessors(fn) {
			if !all[next] {
				all[next] = true
				queue = append(queue, next)
			}
		}
	}
	return all
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
