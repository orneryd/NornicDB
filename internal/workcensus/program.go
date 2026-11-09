package workcensus

// Context-owner convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

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

	nonNilResults map[*ssa.Function]map[int]string // validated client-reviewed result contracts; nil flow only
	nilFlow       *nilFlow                         // MayBeNil's memo; a Program is queried by one goroutine
	guardedFields map[*types.Var]bool              // complete private inline-field use proof
	// dynamicTargets is CallTargets' memo of DynamicCallTargets.
	dynamicTargets map[ssa.CallInstruction][]*ssa.Function
	// channelFamilies is channelFamilyOf's memo of one exact element type's
	// closed channel population.
	channelFamilies map[types.Type]*channelFamily
	sources         *loadedSources
	load            packages.Config // the load settings, reused to read a dependency's bodies on demand
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
	c.BuildFlags = append([]string(nil), c.BuildFlags...)
	c.Env = append([]string(nil), c.Env...)
	// Keep the bytes used by the compiler independent of caller-owned maps and slices.
	if c.Overlay != nil {
		overlay := make(map[string][]byte, len(c.Overlay))
		for path, raw := range c.Overlay {
			overlay[path] = bytes.Clone(raw)
		}
		c.Overlay = overlay
	}
	pkgs, _, sources, err := loadPackages(c, root)
	if err != nil {
		return nil, err
	}
	prog, _ := ssautil.Packages(pkgs, ssa.InstantiateGenerics)
	prog.Build()
	all := sourceFunctions(prog, pkgs)
	// VTA's nil initial graph constructs its CHA candidates from this exact
	// function population. cha.CallGraph(prog) uses AllFunctions internally,
	// which would again omit the otherwise-unreferenced source methods.
	result := &Program{Root: root, FileSet: prog.Fset, Packages: pkgs, SSA: prog, CallGraph: vta.CallGraph(all, nil), load: packages.Config{Dir: root, BuildFlags: c.BuildFlags, Env: c.Env, Overlay: c.Overlay, ParseFile: sources.parse}, sources: sources}
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

// ModuleConfig returns the separately captured root go.mod metadata from loading.
// Module metadata is not a Go parser input. A missing module remains an error,
// rather than an empty successful attestation.
func (p *Program) ModuleConfig() ([]byte, error) {
	if p.sources == nil {
		return nil, fmt.Errorf("module config was not loaded")
	}
	return bytes.Clone(p.sources.module), p.sources.moduleErr
}

// LoadedSource returns an owned copy of the exact bytes passed to the parser
// for a loaded file, including overlay and on-demand dependency source. It never
// substitutes current disk bytes for a compiled body.
func (p *Program) LoadedSource(path string) ([]byte, error) {
	if p.sources != nil {
		p.sources.mu.Lock()
		raw, ok := p.sources.files[path]
		p.sources.mu.Unlock()
		if ok {
			return bytes.Clone(raw), nil
		}
	}
	return nil, &os.PathError{Op: "loaded source", Path: path, Err: os.ErrNotExist}
}

// loadedSources owns parser inputs. packages can invoke ParseFile concurrently.
// Package variants may parse one file more than once; differing bytes must fail
// instead of assigning one source digest to inconsistent compiled bodies.
type loadedSources struct {
	mu        sync.Mutex
	files     map[string][]byte
	module    []byte
	moduleErr error
}

func (s *loadedSources) parse(fset *token.FileSet, path string, raw []byte) (*ast.File, error) {
	owned := bytes.Clone(raw)
	s.mu.Lock()
	if prior, ok := s.files[path]; ok && !bytes.Equal(prior, owned) {
		s.mu.Unlock()
		return nil, fmt.Errorf("source changed while loading %s", path)
	}
	s.files[path] = owned
	s.mu.Unlock()
	// Match go/packages' default parser flags, including object resolution.
	return parser.ParseFile(fset, path, owned, parser.AllErrors|parser.ParseComments)
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
func loadPackages(c Config, root string) ([]*packages.Package, *token.FileSet, *loadedSources, error) {
	sources := &loadedSources{files: map[string][]byte{}}
	sources.module, sources.moduleErr = os.ReadFile(filepath.Join(root, "go.mod"))
	fset := token.NewFileSet()
	pkgs, err := packages.Load(&packages.Config{Fset: fset, Mode: packages.LoadSyntax | c.LoadMode, Dir: root, BuildFlags: c.BuildFlags, Tests: c.Tests, Env: c.Env, Overlay: c.Overlay, ParseFile: sources.parse}, c.Patterns...)
	if err != nil {
		return nil, nil, nil, err
	}
	var loadErrors []string
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, err := range p.Errors {
			loadErrors = append(loadErrors, fmt.Sprintf("%s: %v", p.PkgPath, err))
		}
	})
	if len(loadErrors) > 0 {
		sort.Strings(loadErrors)
		return nil, nil, nil, fmt.Errorf("source does not type-check: %s", strings.Join(loadErrors, "\n"))
	}
	return pkgs, fset, sources, nil
}
