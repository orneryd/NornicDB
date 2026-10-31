// Author: Codex (implementation sub-agent), 2026-10-26.
package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
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

type function struct {
	SSA        *ssa.Function
	Pkg        *packages.Package
	Body       *ast.BlockStmt
	Site       Site
	Reads      map[string]bool
	Inputs     map[string]bool
	Result     string
	ResultType *types.Tuple
}

type analysis struct {
	opt               Options
	owners            Owners
	pkgs              []*packages.Package
	prog              *ssa.Program
	graph             *callgraph.Graph
	functions         []*function
	bySSA             map[*ssa.Function]*function
	byPos             map[token.Pos]*function
	packageByPath     map[string]*packages.Package
	contractInventory []Contract
	graphIndex        map[*ssa.Function]int
	reachCache        map[*ssa.Function][]uint64
	report            Report
}

func typeKey(t types.Type) string {
	return types.TypeString(t, func(p *types.Package) string { return p.Path() })
}

func load(opt Options, owners Owners) (*analysis, error) {
	setStage("load")
	abs, err := filepath.Abs(opt.Tree)
	if err != nil {
		return nil, err
	}
	opt.Tree = abs
	// -mod=readonly prevents package loading from repairing the input module. No product code executes.
	env := append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0", "GOFLAGS=-mod=readonly -p=4")
	// Dependencies need exported type information, not retained ASTs: all input packages
	// are roots and get full syntax/types. This avoids loading stdlib/dependency bodies.
	cfg := &packages.Config{Dir: abs, Mode: packages.LoadSyntax | packages.NeedModule, Tests: false, Env: env, BuildFlags: []string{"-tags=" + opt.Tags}}
	pkgs, err := packages.Load(cfg, strings.Split(opt.Patterns, ",")...)
	if err != nil {
		return nil, err
	}
	errs := []string{}
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			errs = append(errs, e.Error())
		}
	})
	if len(errs) > 0 {
		sort.Strings(errs)
		return nil, fmt.Errorf("type checking failed (no partial report):\n%s", strings.Join(errs, "\n"))
	}
	if len(pkgs) == 0 {
		return nil, fmt.Errorf("no packages matched %s", opt.Patterns)
	}
	a := &analysis{opt: opt, owners: owners, pkgs: pkgs, bySSA: map[*ssa.Function]*function{}, byPos: map[token.Pos]*function{}, packageByPath: map[string]*packages.Package{}}
	a.report = Report{Author: author, Schema: 1, Options: opt, Algorithm: "SSA VTA refined from CHA; input-package bodies", LoadedFiles: []string{}, ExcludedFiles: []string{}, Limits: []string{
		"Candidates require human disposition; static similarity does not establish a product defect.",
		"Production packages only; tests excluded. CGO_ENABLED=0 and the recorded build tags select one platform/build. Excluded files are enumerated.",
		"SSA bodies cover loaded input packages; dependency-only bodies, reflection, unsafe aliasing and assembly are outside the call graph. Dynamic edges are conservative VTA candidates.",
		"Deciders require equal result tuples, overlapping typed inputs/SSA receiver reads, and a shared result-use consumer (including returned-value relay paths); no semantic equivalence is claimed.",
		"Writer independence excludes pairs connected by a static call path; Store addresses trace SSA field/global aliases, not a whole-program heap/points-to model. Map/slice mutation is not a Store to its containing field.",
		"Text provenance follows SSA values and interprocedural arguments/returns seeded by query, statement and name identifiers; opaque dependency internals and dynamically constructed names can be missed.",
		"Clone exact hashes cover all function bodies. Near matching is heuristic: >=0.90 token edit similarity, >=40 tokens and >=3 shared 12-token shingles with frequency <=128 exact groups; common-only/shifted near clones may be missed. All sliding eight-statement windows are compared.",
	}}
	loaded := map[string]bool{}
	contents := map[string][]byte{}
	for _, p := range pkgs {
		a.packageByPath[p.PkgPath] = p
		for _, file := range p.CompiledGoFiles {
			rel, e := filepath.Rel(abs, file)
			if e != nil || strings.HasPrefix(rel, "..") {
				continue
			}
			rel = filepath.ToSlash(rel)
			loaded[rel] = true
			data, e := os.ReadFile(file)
			if e != nil {
				return nil, e
			}
			contents[rel] = data
		}
	}
	for file := range loaded {
		a.report.LoadedFiles = append(a.report.LoadedFiles, file)
	}
	sort.Strings(a.report.LoadedFiles)
	var source strings.Builder
	for _, file := range a.report.LoadedFiles {
		fmt.Fprintf(&source, "%s\x00%s\x00", file, contents[file])
	}
	a.report.SourceSHA256 = digest(source.String())
	// Enumerate source omitted by build constraints; skip hidden, nested-module and generated dependency trees.
	err = filepath.WalkDir(abs, func(file string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			if file != abs {
				if strings.HasPrefix(d.Name(), ".") || d.Name() == "vendor" || d.Name() == "node_modules" {
					return filepath.SkipDir
				}
				if _, e := os.Stat(filepath.Join(file, "go.mod")); e == nil {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if strings.HasSuffix(file, ".go") {
			rel, _ := filepath.Rel(abs, file)
			rel = filepath.ToSlash(rel)
			if !loaded[rel] {
				a.report.ExcludedFiles = append(a.report.ExcludedFiles, rel)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(a.report.ExcludedFiles)
	fmt.Fprintln(os.Stderr, "divergence: type checked", len(pkgs), "packages; building SSA")
	setStage("ssa")
	// Unlike AllPackages, Packages builds bodies only for the requested source. All ./... packages are roots.
	a.prog, _ = ssautil.Packages(pkgs, ssa.InstantiateGenerics|ssa.GlobalDebug)
	a.prog.Build()
	funcs := ssautil.AllFunctions(a.prog)
	base := cha.CallGraph(a.prog)
	for f := range base.Nodes {
		if f != nil {
			funcs[f] = true
		}
	}
	setStage("callgraph")
	a.graph = vta.CallGraph(funcs, base)
	a.graphIndex = map[*ssa.Function]int{}
	a.reachCache = map[*ssa.Function][]uint64{}
	for f := range a.graph.Nodes {
		a.graphIndex[f] = len(a.graphIndex)
	}
	for f := range funcs {
		if f == nil || f.Pkg == nil {
			continue
		}
		p := a.packageByPath[f.Pkg.Pkg.Path()]
		if p == nil {
			continue
		}
		var body *ast.BlockStmt
		switch n := f.Syntax().(type) {
		case *ast.FuncDecl:
			body = n.Body
		case *ast.FuncLit:
			body = n.Body
		}
		if body == nil || f.Synthetic != "" {
			continue
		}
		// Generic instantiations share syntax; report one source function, retaining graph wrappers as bridges.
		if _, ok := a.byPos[body.Pos()]; ok {
			continue
		}
		fn := &function{SSA: f, Pkg: p, Body: body, Site: a.site(p.PkgPath, f.String(), f.Pos(), "function"), Reads: map[string]bool{}, Inputs: map[string]bool{}}
		for i := 0; i < f.Signature.Params().Len(); i++ {
			fn.Inputs[typeKey(types.Unalias(f.Signature.Params().At(i).Type()))] = true
		}
		if f.Signature.Results().Len() > 0 {
			fn.Result = typeKey(f.Signature.Results())
		}
		a.functions = append(a.functions, fn)
		a.bySSA[f] = fn
		a.byPos[body.Pos()] = fn
	}
	sort.Slice(a.functions, func(i, j int) bool { return siteKey(a.functions[i].Site) < siteKey(a.functions[j].Site) })
	// Compiler type identity ignores return-variable names and alias spellings.
	// Intern actual tuples; rendered type strings are display text, never equality.
	resultClasses := map[int][]*types.Tuple{}
	for _, fn := range a.functions {
		tuple := fn.SSA.Signature.Results()
		if tuple.Len() == 0 {
			continue
		}
		for _, prior := range resultClasses[tuple.Len()] {
			if types.Identical(tuple, prior) {
				fn.ResultType = prior
				break
			}
		}
		if fn.ResultType == nil {
			fn.ResultType = tuple
			resultClasses[tuple.Len()] = append(resultClasses[tuple.Len()], tuple)
		}
		parts := []string{}
		for i := 0; i < fn.ResultType.Len(); i++ {
			parts = append(parts, typeKey(types.Unalias(fn.ResultType.At(i).Type())))
		}
		fn.Result = "(" + strings.Join(parts, ", ") + ")"
	}
	a.report.Packages = len(pkgs)
	a.report.Functions = len(a.functions)
	return a, nil
}

func (a *analysis) site(pkg, fn string, pos token.Pos, role string) Site {
	p := a.prog.Fset.Position(pos)
	rel, e := filepath.Rel(a.opt.Tree, p.Filename)
	if e != nil {
		rel = p.Filename
	}
	return Site{pkg, filepath.ToSlash(rel), p.Line, fn, role}
}

func (a *analysis) canonical(f *ssa.Function) *function {
	if fn := a.bySSA[f]; fn != nil {
		return fn
	}
	if f == nil {
		return nil
	}
	switch n := f.Syntax().(type) {
	case *ast.FuncDecl:
		return a.byPos[n.Body.Pos()]
	case *ast.FuncLit:
		return a.byPos[n.Body.Pos()]
	}
	return nil
}

func (a *analysis) run() Report {
	fmt.Fprintln(os.Stderr, "divergence: scanning", len(a.functions), "functions")
	selected, _ := selectSections(a.opt.Sections)
	sections := make([][]Finding, 6)
	if selected[1] || selected[2] {
		name := "parallel_deciders"
		if selected[2] {
			name = "shared_state_writers"
		}
		setStage(name)
		fmt.Fprintln(os.Stderr, "divergence:", name, "(SSA state facts)")
		sections[2] = a.stateFacts(selected[2])
	}
	for _, i := range []int{0, 1, 3, 4, 5} {
		if !selected[i] {
			continue
		}
		setStage(sectionNames[i])
		fmt.Fprintln(os.Stderr, "divergence:", sectionNames[i])
		switch i {
		case 0:
			sections[i] = a.clones()
		case 1:
			sections[i] = a.deciders()
		case 3:
			sections[i] = a.contracts()
		case 4:
			sections[i] = a.text()
		case 5:
			sections[i] = a.contextLoss()
		}
	}
	for i, findings := range sections {
		if !selected[i] {
			continue
		}
		s := finishSection(sectionNames[i], findings, a.opt.Focus)
		if i == 3 {
			for _, c := range a.contractInventory {
				keep := a.opt.Focus == "" || matches(c.Owner.Package, a.opt.Focus)
				for _, s := range c.Implementations {
					if matches(s.Package, a.opt.Focus) {
						keep = true
					}
				}
				if keep {
					s.Inventory = append(s.Inventory, c)
				}
			}
		}
		a.report.Sections = append(a.report.Sections, s)
	}
	return a.report
}
