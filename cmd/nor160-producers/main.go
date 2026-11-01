// NOR-160 lane (Claude), 31 October 2026. Analysis-only program, not product code.
//
// The complete population of value-to-visitor producers in the compiled module:
// every call of a function value (not a static call and not an interface method)
// with a storage Node or Edge argument, clean ones included, each with what the
// code actually passes, traced through SSA and the VTA call graph.
//
// Origins of a passed value:
//
//	fresh       a struct allocated on this read (a decode or a copy)
//	held <name> read from a field or a package variable
//	unresolved  a shape this trace does not follow
//	external    a parameter of a function with no caller in the graph
//	truncated   the trace hit its budget
//
// The trace is path-insensitive: flag-guarded branches merge, so "lends" means a
// held value can reach the visitor on some path in the code, not on every entry.
// "fresh" says the struct is new; it does not judge fields it shares. Interface
// method calls with such an argument are listed apart, by method, unclassified.
//
// Usage: nor160-producers <package pattern>...
// Exit 1 if the known lend, VisitReadOnlyNodesByLabel, is not reported as a lend.
package main

import (
	"encoding/json"
	"fmt"
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

const (
	storagePath = "github.com/orneryd/nornicdb/pkg/storage"
	modulePath  = "github.com/orneryd/nornicdb/"
	traceBudget = 20000
)

type member struct {
	Function string   `json:"function"`
	Position string   `json:"position"`
	Argument string   `json:"argument"`
	Class    string   `json:"class"`
	Origins  []string `json:"origins"`
	Entries  []string `json:"exported_entries"`
}

type interfaceCall struct {
	Function string `json:"function"`
	Position string `json:"position"`
	Method   string `json:"method"`
}

type tracer struct {
	graph   *callgraph.Graph
	closure map[*ssa.Function][]*ssa.MakeClosure
	seen    map[ssa.Value]bool
	out     map[string]bool
}

func isEntityName(t types.Type) bool {
	named, ok := types.Unalias(t).(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != storagePath {
		return false
	}
	name := named.Obj().Name()
	return name == "Node" || name == "Edge"
}

// isEntity: a storage Node or Edge, or a pointer or slice of one.
func isEntity(t types.Type) bool {
	switch u := types.Unalias(t).(type) {
	case *types.Pointer:
		return isEntity(u.Elem())
	case *types.Slice:
		return isEntity(u.Elem())
	}
	return isEntityName(t)
}

func (t *tracer) add(origin string) { t.out[origin] = true }

// stores follows what is written into an allocation or a slice or array.
func (t *tracer) stores(v ssa.Value, field int) {
	refs := v.Referrers()
	if refs == nil {
		return
	}
	for _, ref := range *refs {
		switch r := ref.(type) {
		case *ssa.Store:
			if r.Addr == v {
				t.trace(r.Val)
			}
		case *ssa.IndexAddr:
			if r.X == v {
				t.stores(r, -1)
			}
		case *ssa.FieldAddr:
			if r.X == v && (field < 0 || r.Field == field) {
				t.stores(r, -1)
			}
		case *ssa.MapUpdate:
			if r.Map == v {
				t.trace(r.Value)
			}
		case *ssa.Slice:
			if r.X == v {
				t.stores(r, -1)
			}
		}
	}
}

func (t *tracer) results(call ssa.CallInstruction, index int) {
	node := t.graph.Nodes[call.Parent()]
	if node == nil {
		t.add("unresolved:no graph node")
		return
	}
	found := false
	for _, edge := range node.Out {
		if edge.Site != call {
			continue
		}
		found = true
		fn := edge.Callee.Func
		if fn.Blocks == nil {
			t.add("external:" + fn.String())
			continue
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				if ret, ok := ins.(*ssa.Return); ok && index < len(ret.Results) {
					t.trace(ret.Results[index])
				}
			}
		}
	}
	if !found {
		t.add("unresolved:call without a callee " + call.Common().Value.Name())
	}
}

func (t *tracer) trace(v ssa.Value) {
	if t.seen[v] {
		return
	}
	if len(t.seen) >= traceBudget {
		t.add("truncated")
		return
	}
	t.seen[v] = true
	switch x := v.(type) {
	case *ssa.Const:
		return
	case *ssa.Parameter:
		t.parameter(x)
	case *ssa.FreeVar:
		t.freeVar(x)
	case *ssa.Global:
		t.add("held global " + x.Name())
	case *ssa.Alloc:
		elem := x.Type().(*types.Pointer).Elem()
		if isEntityName(elem) {
			t.add("fresh")
			return
		}
		t.stores(x, -1)
	case *ssa.MakeSlice, *ssa.MakeMap:
		t.stores(x, -1)
	case *ssa.Phi:
		for _, edge := range x.Edges {
			t.trace(edge)
		}
	case *ssa.Call:
		if x.Call.Value != nil {
			if builtin, ok := x.Call.Value.(*ssa.Builtin); ok {
				if builtin.Name() == "append" {
					for _, arg := range x.Call.Args {
						t.trace(arg)
					}
				} else {
					t.add("unresolved:builtin " + builtin.Name())
				}
				return
			}
		}
		t.results(x, 0)
	case *ssa.Extract:
		switch tuple := x.Tuple.(type) {
		case *ssa.Call:
			t.results(tuple, x.Index)
		case *ssa.Lookup:
			t.trace(tuple)
		case *ssa.Next:
			t.trace(tuple.Iter)
		case *ssa.TypeAssert:
			t.trace(tuple.X)
		default:
			t.add(fmt.Sprintf("unresolved:extract %T", x.Tuple))
		}
	case *ssa.Range:
		t.trace(x.X)
	case *ssa.Lookup:
		t.trace(x.X)
	case *ssa.Index:
		t.trace(x.X)
	case *ssa.UnOp:
		if x.Op == token.MUL {
			t.load(x.X)
		} else {
			t.trace(x.X)
		}
	case *ssa.FieldAddr:
		t.load(x)
	case *ssa.IndexAddr:
		t.trace(x.X)
	case *ssa.Field:
		t.trace(x.X)
	case *ssa.Slice:
		t.trace(x.X)
	case *ssa.ChangeType:
		t.trace(x.X)
	case *ssa.ChangeInterface:
		t.trace(x.X)
	case *ssa.MakeInterface:
		t.trace(x.X)
	case *ssa.Convert:
		t.trace(x.X)
	case *ssa.TypeAssert:
		t.trace(x.X)
	case *ssa.SliceToArrayPointer:
		t.trace(x.X)
	default:
		t.add(fmt.Sprintf("unresolved:%T", v))
	}
}

// load is a read through an address: of a local, what was stored there; of a
// field of anything but a local struct, the field is held.
func (t *tracer) load(address ssa.Value) {
	switch a := address.(type) {
	case *ssa.Alloc:
		elem := a.Type().(*types.Pointer).Elem()
		if isEntityName(elem) {
			t.add("fresh")
			return
		}
		t.stores(a, -1)
	case *ssa.FieldAddr:
		if local, ok := a.X.(*ssa.Alloc); ok {
			t.stores(local, a.Field)
			return
		}
		structType := a.X.Type().Underlying().(*types.Pointer).Elem().Underlying().(*types.Struct)
		t.add("held field " + a.X.Type().String() + "." + structType.Field(a.Field).Name())
	case *ssa.IndexAddr:
		t.trace(a.X)
	case *ssa.Global:
		t.add("held global " + a.Name())
	case *ssa.FreeVar:
		t.freeVar(a)
	case *ssa.Parameter:
		t.parameter(a)
	case *ssa.Phi:
		for _, edge := range a.Edges {
			t.load(edge)
		}
	default:
		t.add(fmt.Sprintf("unresolved:load %T", address))
	}
}

func (t *tracer) parameter(p *ssa.Parameter) {
	fn := p.Parent()
	index := -1
	for i, param := range fn.Params {
		if param == p {
			index = i
		}
	}
	node := t.graph.Nodes[fn]
	callers := 0
	if node != nil {
		for _, edge := range node.In {
			if edge.Site == nil {
				continue
			}
			common := edge.Site.Common()
			position := index
			if common.IsInvoke() {
				position = index - 1
				if position < 0 {
					t.trace(common.Value)
					callers++
					continue
				}
			}
			if position >= 0 && position < len(common.Args) {
				t.trace(common.Args[position])
				callers++
			}
		}
	}
	if callers == 0 {
		t.add("external parameter of " + fn.String())
	}
}

func (t *tracer) freeVar(free *ssa.FreeVar) {
	fn := free.Parent()
	index := -1
	for i, candidate := range fn.FreeVars {
		if candidate == free {
			index = i
		}
	}
	closures := t.closure[fn]
	if len(closures) == 0 {
		t.add("unresolved:closure without a maker " + fn.String())
		return
	}
	for _, maker := range closures {
		binding := maker.Bindings[index]
		if _, addressed := binding.Type().(*types.Pointer); addressed {
			if _, captured := free.Type().(*types.Pointer); captured {
				t.load(binding)
				continue
			}
		}
		t.trace(binding)
	}
}

func classify(origins map[string]bool) string {
	held, fresh, other := false, false, false
	for origin := range origins {
		switch {
		case strings.HasPrefix(origin, "held"):
			held = true
		case origin == "fresh":
			fresh = true
		default:
			other = true
		}
	}
	switch {
	case held && (fresh || other):
		return "lends on some path"
	case held:
		return "lends"
	case other:
		return "open"
	case fresh:
		return "caller-owned"
	}
	return "open"
}

func exportedEntry(fn *ssa.Function) bool {
	return fn.Parent() == nil && token.IsExported(fn.Name())
}

// entries lists the exported functions that reach fn, through closures' makers
// and the graph's callers.
func entries(graph *callgraph.Graph, fn *ssa.Function) []string {
	seen := map[*ssa.Function]bool{}
	found := map[string]bool{}
	var walk func(*ssa.Function)
	walk = func(current *ssa.Function) {
		if current == nil || seen[current] {
			return
		}
		seen[current] = true
		if exportedEntry(current) {
			found[current.String()] = true
		}
		if parent := current.Parent(); parent != nil {
			walk(parent)
		}
		if node := graph.Nodes[current]; node != nil {
			for _, edge := range node.In {
				walk(edge.Caller.Func)
			}
		}
	}
	walk(fn)
	out := make([]string, 0, len(found))
	for name := range found {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func main() {
	root, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	cfg := &packages.Config{Mode: packages.LoadAllSyntax, Dir: root, BuildFlags: []string{"-tags=noui,nolocalllm"}}
	pkgs, err := packages.Load(cfg, os.Args[1:]...)
	if err != nil {
		panic(err)
	}
	loadErrors := packages.PrintErrors(pkgs)
	prog, _ := ssautil.AllPackages(pkgs, ssa.InstantiateGenerics)
	prog.Build()
	all := ssautil.AllFunctions(prog)
	graph := vta.CallGraph(all, cha.CallGraph(prog))

	closure := map[*ssa.Function][]*ssa.MakeClosure{}
	for fn := range all {
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				if maker, ok := ins.(*ssa.MakeClosure); ok {
					target := maker.Fn.(*ssa.Function)
					closure[target] = append(closure[target], maker)
				}
			}
		}
	}

	var members []member
	var interfaceCalls []interfaceCall
	functions := 0
	for fn := range all {
		if fn.Pkg == nil || !strings.HasPrefix(fn.Pkg.Pkg.Path()+"/", modulePath) || fn.Blocks == nil {
			continue
		}
		functions++
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok {
					continue
				}
				common := call.Common()
				if common.IsInvoke() {
					for _, arg := range common.Args {
						if isEntity(arg.Type()) {
							position := prog.Fset.Position(call.Pos())
							file, relErr := filepath.Rel(root, position.Filename)
							if relErr != nil {
								file = position.Filename
							}
							interfaceCalls = append(interfaceCalls, interfaceCall{
								Function: fn.String(),
								Position: fmt.Sprintf("%s:%d", filepath.ToSlash(file), position.Line),
								Method:   common.Value.Type().String() + "." + common.Method.Name(),
							})
							break
						}
					}
					continue
				}
				if common.StaticCallee() != nil {
					continue
				}
				if _, builtin := common.Value.(*ssa.Builtin); builtin {
					continue
				}
				for _, arg := range common.Args {
					if !isEntity(arg.Type()) {
						continue
					}
					t := &tracer{graph: graph, closure: closure, seen: map[ssa.Value]bool{}, out: map[string]bool{}}
					t.trace(arg)
					origins := make([]string, 0, len(t.out))
					for origin := range t.out {
						origins = append(origins, origin)
					}
					sort.Strings(origins)
					position := prog.Fset.Position(call.Pos())
					file, relErr := filepath.Rel(root, position.Filename)
					if relErr != nil {
						file = position.Filename
					}
					members = append(members, member{
						Function: fn.String(),
						Position: fmt.Sprintf("%s:%d", filepath.ToSlash(file), position.Line),
						Argument: arg.Type().String(),
						Class:    classify(t.out),
						Origins:  origins,
						Entries:  entries(graph, fn),
					})
				}
			}
		}
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].Position != members[j].Position {
			return members[i].Position < members[j].Position
		}
		return members[i].Function < members[j].Function
	})

	classes := map[string]int{}
	knownLend := false
	for _, m := range members {
		classes[m.Class]++
		if !strings.HasPrefix(m.Class, "lends") {
			continue
		}
		for _, entry := range m.Entries {
			if strings.HasSuffix(entry, "BadgerTransaction).VisitReadOnlyNodesByLabel") {
				knownLend = true
			}
		}
	}
	report := map[string]any{
		"load_errors":                      loadErrors,
		"functions_in_module":              functions,
		"producers":                        len(members),
		"classes":                          classes,
		"interface_calls":                  interfaceCalls,
		"control_known_lend_VisitReadOnly": knownLend,
		"members":                          members,
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", " ")
	if err := encoder.Encode(report); err != nil {
		panic(err)
	}
	if !knownLend || loadErrors > 0 {
		os.Exit(1)
	}
}
