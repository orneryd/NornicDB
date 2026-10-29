// NOR-160 lane (Claude), 29 October 2026. Analysis-only client, not product code.
// Convergence enumeration over the maintained workcensus typed/SSA loader: proves
// that each caller-copy behaviour has one owner and no divergent path remains.
// No loader, SSA builder or call graph is copied; no product code executes.
package main

import (
	"encoding/json"
	"fmt"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

const module = "github.com/orneryd/nornicdb"

type site struct {
	Func   string `json:"func"`
	File   string `json:"file"`
	Line   int    `json:"line"`
	Detail string `json:"detail"`
}

type report struct {
	Author   string            `json:"author"`
	Target   string            `json:"target"`
	Loader   string            `json:"loader"`
	Scope    string            `json:"scope"`
	Funcs    int               `json:"functions_loaded"`
	Stores   map[string][]site `json:"retained_store_access"`
	Buffer   map[string][]site `json:"buffer_selector_callers"`
	Copiers  []site            `json:"copy_or_projection_implementations"`
	Recopies []site            `json:"copies_of_owned_values"`
}

var tree string

func inScope(fn *ssa.Function) bool {
	if fn == nil || fn.Pkg == nil {
		// Anonymous functions report their parent's package.
		if fn != nil && fn.Parent() != nil {
			return inScope(fn.Parent())
		}
		return false
	}
	path := fn.Pkg.Pkg.Path()
	if path != module+"/pkg/storage" && path != module+"/pkg/cypher" {
		return false
	}
	file := position(fn, fn.Pos()).Filename
	return file != "" && !strings.HasSuffix(file, "_test.go")
}

func position(fn *ssa.Function, pos token.Pos) token.Position {
	return fn.Prog.Fset.Position(pos)
}

func at(fn *ssa.Function, pos token.Pos, detail string) site {
	if !pos.IsValid() {
		pos = fn.Pos()
	}
	p := position(fn, pos)
	file, _ := filepath.Rel(tree, p.Filename)
	return site{Func: fn.String(), File: filepath.ToSlash(file), Line: p.Line, Detail: detail}
}

func named(t types.Type) (pkg, name string) {
	for {
		switch x := t.(type) {
		case *types.Pointer:
			t = x.Elem()
			continue
		case *types.Alias:
			t = types.Unalias(x)
			continue
		case *types.Named:
			if x.Obj().Pkg() == nil {
				return "", x.Obj().Name()
			}
			return x.Obj().Pkg().Path(), x.Obj().Name()
		}
		return "", ""
	}
}

func isEntity(t types.Type) bool {
	pkg, name := named(t)
	return pkg == module+"/pkg/storage" && (name == "Node" || name == "Edge")
}

func entityBearing(t types.Type) bool {
	switch x := types.Unalias(t).(type) {
	case *types.Pointer:
		return isEntity(x)
	case *types.Slice:
		return entityBearing(x.Elem())
	case *types.Tuple:
		for i := 0; i < x.Len(); i++ {
			if entityBearing(x.At(i).Type()) {
				return true
			}
		}
	}
	return isEntity(t)
}

// Retained stores and their sole sanctioned readers.
var stores = map[string]bool{"edgeCache": true, "edgeCacheVersions": true, "edgeTypeCache": true}

func storeAccess(fn *ssa.Function, out map[string][]site) {
	for _, block := range fn.Blocks {
		for _, instruction := range block.Instrs {
			field, ok := instruction.(*ssa.FieldAddr)
			if !ok {
				continue
			}
			pkg, owner := named(field.X.Type())
			if pkg != module+"/pkg/storage" || owner != "BadgerEngine" {
				continue
			}
			st := types.Unalias(field.X.Type()).(*types.Pointer).Elem().Underlying().(*types.Struct)
			name := st.Field(field.Field).Name()
			if !stores[name] {
				continue
			}
			for _, use := range *field.Referrers() {
				switch u := use.(type) {
				case *ssa.Store:
					out[name] = append(out[name], at(fn, u.Pos(), "replace"))
				case *ssa.UnOp:
					for _, loaded := range *u.Referrers() {
						kind := "other-use"
						switch l := loaded.(type) {
						case *ssa.Lookup:
							kind = "read:lookup"
						case *ssa.Range:
							kind = "read:range"
						case *ssa.MapUpdate:
							kind = "write"
						case *ssa.Call:
							if b, ok := l.Call.Value.(*ssa.Builtin); ok {
								kind = "builtin:" + b.Name()
							}
						}
						out[name] = append(out[name], at(fn, loaded.Pos(), kind))
					}
				}
			}
		}
	}
}

// Buffered image selectors: every unexported *Locked WriteBehindBuffer method
// that returns node/edge images (or buffered entity state).
func selector(callee *ssa.Function) bool {
	if callee == nil || callee.Signature.Recv() == nil {
		return false
	}
	pkg, recv := named(callee.Signature.Recv().Type())
	if pkg != module+"/pkg/storage" || recv != "WriteBehindBuffer" || !strings.HasSuffix(callee.Name(), "Locked") {
		return false
	}
	if callee.Name() == "entityStateLocked" {
		return true
	}
	return entityBearing(callee.Signature.Results())
}

func callee(call ssa.CallInstruction) *ssa.Function {
	common := call.Common()
	if common.IsInvoke() {
		return nil
	}
	switch v := common.Value.(type) {
	case *ssa.Function:
		return v
	case *ssa.MakeClosure:
		f, _ := v.Fn.(*ssa.Function)
		return f
	}
	return nil
}

func origin(fn *ssa.Function) *ssa.Function {
	if fn != nil && fn.Origin() != nil {
		return fn.Origin()
	}
	return fn
}

func bufferCallers(fn *ssa.Function, out map[string][]site) {
	for _, block := range fn.Blocks {
		for _, instruction := range block.Instrs {
			call, ok := instruction.(ssa.CallInstruction)
			if !ok {
				continue
			}
			if c := callee(call); selector(c) {
				out[c.Name()] = append(out[c.Name()], at(fn, call.Pos(), "calls "+c.Name()))
			}
		}
	}
}

// A copy or projection implementation takes a node/edge and allocates a new one.
func copier(fn *ssa.Function) (string, bool) {
	takes := false
	for _, param := range fn.Params {
		if isEntity(param.Type()) {
			takes = true
		}
	}
	if !takes || !entityBearing(fn.Signature.Results()) {
		return "", false
	}
	for _, block := range fn.Blocks {
		for _, instruction := range block.Instrs {
			if alloc, ok := instruction.(*ssa.Alloc); ok && isEntity(alloc.Type()) {
				_, name := named(alloc.Type())
				return "allocates " + name, true
			}
		}
	}
	return "", false
}

// Calls that copy or project an entity, and the owners whose results are
// already caller-owned. A copy of such a result is a second copy.
var copies = map[string]bool{"copyNode": true, "copyNodeWithoutEmbeddings": true, "copyEdge": true, "copyImagesForCaller": true, "projectCachedNodeForRead": true, "edgeMatchCandidate": true, "CopyNode": true, "CopyEdge": true}
var owners = map[string]bool{"readBufferedEntities": true, "cacheLoadEdge": true}

func copyName(fn *ssa.Function) string {
	fn = origin(fn)
	if fn == nil || fn.Pkg == nil || fn.Pkg.Pkg.Path() != module+"/pkg/storage" || !copies[fn.Name()] {
		return ""
	}
	return fn.Name()
}

// ownedSource walks an argument back through value-preserving steps, closure
// bindings and local stores to the call that produced it.
func ownedSource(v ssa.Value, seen map[ssa.Value]bool, depth int) string {
	if v == nil || seen[v] || depth > 40 {
		return ""
	}
	seen[v] = true
	next := func(x ssa.Value) string { return ownedSource(x, seen, depth+1) }
	switch x := v.(type) {
	case *ssa.Call:
		c := origin(callee(x))
		if c != nil && c.Pkg != nil && c.Pkg.Pkg.Path() == module+"/pkg/storage" && owners[c.Name()] {
			return c.Name()
		}
		if c == nil && x.Call.IsInvoke() {
			if pkg, _ := named(x.Call.Value.Type()); pkg == module+"/pkg/storage" && entityBearing(x.Call.Signature().Results()) {
				return "public storage read " + x.Call.Method.Name()
			}
		}
		if c != nil && c.Signature.Recv() != nil && token.IsExported(c.Name()) && entityBearing(c.Signature.Results()) {
			if pkg, _ := named(c.Signature.Recv().Type()); pkg == module+"/pkg/storage" {
				return "public storage read " + c.Name()
			}
		}
	case *ssa.Extract:
		return next(x.Tuple)
	case *ssa.Index:
		return next(x.X)
	case *ssa.IndexAddr:
		return next(x.X)
	case *ssa.Field:
		return next(x.X)
	case *ssa.FieldAddr:
		return next(x.X)
	case *ssa.Slice:
		return next(x.X)
	case *ssa.ChangeType:
		return next(x.X)
	case *ssa.MakeInterface:
		return next(x.X)
	case *ssa.TypeAssert:
		return next(x.X)
	case *ssa.Phi:
		for _, edge := range x.Edges {
			if s := next(edge); s != "" {
				return s
			}
		}
	case *ssa.UnOp:
		if x.Op == token.MUL {
			// A load: the values stored to the same address, here or in closures.
			for _, use := range *x.X.Referrers() {
				if st, ok := use.(*ssa.Store); ok && st.Addr == x.X {
					if s := next(st.Val); s != "" {
						return s
					}
				}
			}
			return next(x.X)
		}
	case *ssa.FreeVar:
		parent := x.Parent()
		index := -1
		for i, fv := range parent.FreeVars {
			if fv == x {
				index = i
			}
		}
		if index < 0 || parent.Parent() == nil {
			return ""
		}
		for _, block := range parent.Parent().Blocks {
			for _, instruction := range block.Instrs {
				if mc, ok := instruction.(*ssa.MakeClosure); ok && mc.Fn == parent && index < len(mc.Bindings) {
					if s := next(mc.Bindings[index]); s != "" {
						return s
					}
				}
			}
		}
	case *ssa.Alloc:
		for _, use := range *x.Referrers() {
			if st, ok := use.(*ssa.Store); ok && st.Addr == x {
				if s := next(st.Val); s != "" {
					return s
				}
			}
		}
	}
	return ""
}

func recopies(fn *ssa.Function, out *[]site) {
	for _, block := range fn.Blocks {
		for _, instruction := range block.Instrs {
			call, ok := instruction.(ssa.CallInstruction)
			if !ok {
				continue
			}
			name := copyName(callee(call))
			if name == "" {
				continue
			}
			for _, arg := range call.Common().Args {
				if src := ownedSource(arg, map[ssa.Value]bool{}, 0); src != "" {
					*out = append(*out, at(fn, call.Pos(), name+" of "+src))
				}
			}
		}
	}
}

func targetTree(spec string) (string, func(), error) {
	if !strings.HasPrefix(spec, "git:") {
		return spec, func() {}, nil
	}
	sha := strings.TrimPrefix(spec, "git:")
	if len(sha) != 40 {
		return "", nil, fmt.Errorf("target must use a full commit SHA")
	}
	for _, c := range sha {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return "", nil, fmt.Errorf("invalid target SHA")
		}
	}
	dir, err := os.MkdirTemp("", "nor160-convergence-"+sha[:8]+"-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) } // exactly the directory MkdirTemp created
	archive := filepath.Join(dir, "source.tar")
	for _, argv := range [][]string{{"git", "archive", "--format=tar", "--output", archive, sha}, {"tar", "-xf", archive, "-C", dir}} {
		command := exec.Command(argv[0], argv[1:]...)
		command.Stderr = os.Stderr
		if err := command.Run(); err != nil {
			cleanup()
			return "", nil, err
		}
	}
	if err := os.Remove(archive); err != nil {
		cleanup()
		return "", nil, err
	}
	return dir, cleanup, nil
}

func main() {
	if len(os.Args) != 3 {
		panic("usage: nor160-convergence <tree|git:fullSHA> <output|->")
	}
	spec, output := os.Args[1], os.Args[2]
	var cleanup func()
	var err error
	tree, cleanup, err = targetTree(spec)
	if err != nil {
		panic(err)
	}
	defer cleanup()
	config := workcensus.Config{Dir: tree, Patterns: []string{"./pkg/storage/...", "./pkg/cypher/..."}, BuildFlags: []string{"-tags=noui,nolocalllm"}, Env: append(os.Environ(), "CGO_ENABLED=0", "GOWORK=off")}
	p, err := workcensus.LoadProgram(config)
	if err != nil {
		panic(err)
	}
	r := report{
		Author: "NOR-160 lane (Claude), 29 October 2026",
		Target: spec,
		Loader: "maintained internal/workcensus.LoadProgram (fixture base b013bdfd4)",
		Scope:  "non-test functions of pkg/storage and pkg/cypher, including closures and generic instances",
		Funcs:  len(p.Functions),
		Stores: map[string][]site{},
		Buffer: map[string][]site{},
	}
	for _, fn := range p.Functions {
		if !inScope(fn) {
			continue
		}
		storeAccess(fn, r.Stores)
		bufferCallers(fn, r.Buffer)
		if fn.Parent() == nil {
			if detail, ok := copier(fn); ok {
				r.Copiers = append(r.Copiers, at(fn, fn.Pos(), detail))
			}
		}
		recopies(fn, &r.Recopies)
	}
	order := func(xs []site) {
		sort.Slice(xs, func(i, j int) bool {
			if xs[i].File != xs[j].File {
				return xs[i].File < xs[j].File
			}
			if xs[i].Line != xs[j].Line {
				return xs[i].Line < xs[j].Line
			}
			return xs[i].Detail < xs[j].Detail
		})
	}
	for _, m := range []map[string][]site{r.Stores, r.Buffer} {
		for _, xs := range m {
			order(xs)
		}
	}
	order(r.Copiers)
	order(r.Recopies)
	out := os.Stdout
	if output != "-" {
		if out, err = os.Create(output); err != nil {
			panic(err)
		}
		defer out.Close()
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", " ")
	if err := encoder.Encode(r); err != nil {
		panic(err)
	}
}
