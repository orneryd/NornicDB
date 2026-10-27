// Package storagecensus builds the census of every write of stored entity
// state in the module: each Badger write, classified by the key it writes,
// and each change of a field that holds entity state, counts or quota; and
// it checks that each effect runs under the acquisition its class requires,
// on every path from an entry point. The census is generated from the fully
// type-checked module as SSA, with a VTA call graph
// (docs/generated/stored-entity-census.json and .md); its tests fail when the
// artifact is stale, when the module does not type-check, or when an effect
// escapes its acquisition.
package storagecensus

import (
	"fmt"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
)

// Module is the import path prefix of the analysed module.
const Module = "github.com/orneryd/nornicdb/"

// program is the type-checked module as SSA, with its call graph.
type program struct {
	root  string // absolute module root
	fset  *token.FileSet
	ssa   *ssa.Program
	funcs []*ssa.Function // every function with a body in the module, sorted
	cg    *callgraph.Graph
	// siteCallees is the call graph's targets of each call site, in a
	// stable order. It is built once, after the graph is final (see
	// indexCallTargets): the graph never changes after load, so every
	// reader shares this index instead of scanning the graph per call.
	siteCallees map[ssa.CallInstruction][]*ssa.Function
}

// load type-checks every package of the module rooted at root for
// linux/amd64 with the default build tags (noui, nolocalllm), builds it as
// SSA and computes its call graph. overlay replaces or adds files (absolute
// path to contents); the mutation tests use it. Dependencies outside the
// module are typed from export data and have no bodies. Any type error fails
// the load: a census of code that does not build proves nothing.
func load(root string, overlay map[string][]byte) (*program, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	loaded, err := workcensus.LoadProgram(workcensus.Config{
		Dir: abs, Patterns: []string{"./..."},
		BuildFlags: []string{"-tags=noui,nolocalllm"}, LoadMode: packages.NeedTypesSizes,
		Env:     append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0", "GOFLAGS=-mod=mod"),
		Overlay: overlay,
	})
	if err != nil {
		return nil, err
	}
	p := &program{root: loaded.Root, fset: loaded.FileSet, ssa: loaded.SSA, cg: loaded.CallGraph}
	for _, fn := range loaded.Functions {
		// A generic function is analysed in its instantiations, not as
		// written (its calls through type parameters resolve only there).
		top := fn
		for top.Parent() != nil {
			top = top.Parent()
		}
		if top.TypeParams().Len() > 0 && len(top.TypeArgs()) == 0 {
			continue
		}
		if fn.Blocks != nil && inModule(fn) {
			p.funcs = append(p.funcs, fn)
		}
	}
	sort.Slice(p.funcs, func(i, j int) bool { return p.funcName(p.funcs[i]) < p.funcName(p.funcs[j]) })
	p.indexCallTargets()
	return p, nil
}

// callTargets are the functions the call graph gives for one dynamic call site, in
// stable order. An empty result is unknown: no caller may read it as "calls
// nothing".
func (p *program) callTargets(site ssa.CallInstruction) []*ssa.Function {
	return p.siteCallees[site]
}

// indexCallTargets takes the engine's per-site call targets
// (workcensus.DynamicCallTargets) in the census's own stable order. It is built
// once here, before the program is shared, so readers need no synchronization
// and no per-call graph scan.
func (p *program) indexCallTargets() {
	p.siteCallees = workcensus.DynamicCallTargets(p.cg)
	for site, callees := range p.siteCallees {
		sort.Slice(callees, func(i, j int) bool { return p.funcName(callees[i]) < p.funcName(callees[j]) })
		p.siteCallees[site] = callees
	}
}

// inModule reports whether fn belongs to a package of the module (a closure
// or an instantiation belongs to its parent's package).
func inModule(fn *ssa.Function) bool {
	for fn.Parent() != nil {
		fn = fn.Parent()
	}
	if fn.Origin() != nil {
		fn = fn.Origin()
	}
	if fn.Pkg == nil {
		// A synthetic wrapper ($bound for a method value, $thunk for a
		// method expression) belongs to the method it wraps.
		if obj := fn.Object(); obj != nil && obj.Pkg() != nil {
			return strings.HasPrefix(obj.Pkg().Path()+"/", Module)
		}
		return false
	}
	return strings.HasPrefix(fn.Pkg.Pkg.Path()+"/", Module)
}

// funcName names fn independently of the checkout: package path relative to
// the module, receiver type and name ("pkg/storage.BadgerEngine.CreateNode",
// "pkg/storage.BadgerEngine.UpdateNode$1" for a closure).
func (p *program) funcName(fn *ssa.Function) string {
	s := strings.ReplaceAll(fn.String(), Module, "")
	if strings.HasPrefix(s, "(") {
		if end := strings.Index(s, ")"); end > 0 {
			s = strings.TrimPrefix(s[1:end], "*") + s[end+1:]
		}
	}
	return s
}

// pos is position, relative to the module root, as "file:line".
func (p *program) pos(pos token.Pos) string {
	if !pos.IsValid() {
		return "-"
	}
	at := p.fset.Position(pos)
	name := at.Filename
	if rel, err := filepath.Rel(p.root, name); err == nil {
		name = filepath.ToSlash(rel)
	}
	return fmt.Sprintf("%s:%d", name, at.Line)
}

// methodIdentity follows a compiler-produced bound method wrapper to the
// original method only when its object, receiver type and tail-call receiver
// agree. A method with the same spelling on another type remains distinct.
func (p *program) methodIdentity(fn *ssa.Function) string {
	if fn.Synthetic != "" && len(fn.FreeVars) == 1 {
		object, ok := fn.Object().(*types.Func)
		if ok {
			signature, ok := object.Type().(*types.Signature)
			if ok && signature.Recv() != nil && types.Identical(signature.Recv().Type(), fn.FreeVars[0].Type()) {
				for _, block := range fn.Blocks {
					for _, instruction := range block.Instrs {
						call, ok := instruction.(*ssa.Call)
						if !ok {
							continue
						}
						callee := call.Common().StaticCallee()
						if callee != nil && callee.Object() == object && len(call.Common().Args) > 0 && call.Common().Args[0] == fn.FreeVars[0] {
							return p.funcName(callee)
						}
					}
				}
			}
		}
	}
	return p.funcName(fn)
}
