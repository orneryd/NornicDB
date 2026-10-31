package storagecensus

import (
	"encoding/json"
	"fmt"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

// Author: #1548 implementation lane (Codex). This domain query uses the
// release's existing compiled loader and VTA edges. It does not edit, copy or
// replace the shared acquisition/callback analyzer. Argument flows are leads,
// not automatic ownership dispositions. Calls and returns must match at the
// original call site; another caller cannot inherit this argument's provenance.
type residualIngressEdge struct {
	to     ssa.Value
	reason string
}

type residualIngressBinding struct {
	actual map[ssa.Value]ssa.Value
	result ssa.Value
}

type residualIngressSink struct {
	Kind, Owner, Position string
}

type residualIngressRow struct {
	Boundary, Argument, Type, Position string
	Sinks                              []residualIngressSink
	Paths                              map[string][]string
}

func residualIngressQuery(p *program) []residualIngressRow {
	flow := map[ssa.Value][]residualIngressEdge{}
	delegates := map[ssa.Value][]residualIngressEdge{}
	bindings := map[*ssa.Function][]residualIngressBinding{}
	returns := map[*ssa.Function][]ssa.Value{}
	sinks := map[ssa.Value][]residualIngressSink{}
	// These identify the actual retained-state domains, not approved writers.
	states := map[string]bool{}
	for _, name := range []string{"BadgerEngine", "BadgerTransaction", "CommitBuffer", "WriteBehindBuffer", "BatchWriter", "WAL", "WALEngine", "PreparedEntityCommit", "MemoryEngine"} {
		states[Module+"pkg/storage."+name] = true
	}
	states[Module+"pkg/replication.ReplicatedEngine"] = true
	// Exact existing ownership calls terminate a borrowed-image flow. Every
	// such boundary is still emitted, so it cannot silently approve an API.
	owners := map[string]bool{}
	for _, name := range []string{"copyNode", "copyEdge", "CopyNode", "CopyEdge", "copyNodeWithoutEmbeddings", "copyNodeProjectedWithoutEmbeddings", "ownedProperties", "ownedMetadataValue", "canonicalPropertyValue", "codecImageProperties", "marshalWALRecoveryPayload", "encodeValue"} {
		owners["pkg/storage."+name] = true
	}
	var mutable func(types.Type, map[types.Type]bool) bool
	mutable = func(t types.Type, seen map[types.Type]bool) bool {
		t = types.Unalias(t)
		if seen[t] {
			return false
		}
		seen[t] = true
		if n, ok := t.(*types.Named); ok {
			if pkg := n.Obj().Pkg(); pkg != nil && states[pkg.Path()+"."+n.Obj().Name()] {
				return false // delegate/lifetime handle, not caller entity data
			}
		}
		switch t := t.Underlying().(type) {
		case *types.Pointer:
			return mutable(t.Elem(), seen)
		case *types.Map, *types.Slice:
			return true
		case *types.Interface:
			return t.NumMethods() == 0
		case *types.Array:
			return mutable(t.Elem(), seen)
		case *types.Tuple:
			for i := 0; i < t.Len(); i++ {
				if mutable(t.At(i).Type(), seen) {
					return true
				}
			}
		case *types.Struct:
			for i := 0; i < t.NumFields(); i++ {
				if mutable(t.Field(i).Type(), seen) {
					return true
				}
			}
		}
		return false
	}
	carry := func(v ssa.Value) bool {
		switch v.(type) {
		case *ssa.Range, *ssa.Next, *ssa.MakeClosure:
			return true // iterator transports the map's borrowed element values
		}
		return v != nil && mutable(v.Type(), map[types.Type]bool{})
	}
	add := func(from, to ssa.Value, reason string) {
		if carry(from) && carry(to) {
			flow[from] = append(flow[from], residualIngressEdge{to: to, reason: reason})
		}
	}
	delegate := func(from, to ssa.Value, reason string) {
		if carry(from) && carry(to) {
			delegates[from] = append(delegates[from], residualIngressEdge{to: to, reason: reason})
		}
	}

	var state func(ssa.Value, map[ssa.Value]bool) string
	state = func(v ssa.Value, seen map[ssa.Value]bool) string {
		if v == nil || seen[v] {
			return ""
		}
		seen[v] = true
		switch v := v.(type) {
		case *ssa.Global:
			return v.String()
		case *ssa.FieldAddr:
			t := types.Unalias(v.X.Type())
			if ptr, ok := t.(*types.Pointer); ok {
				t = types.Unalias(ptr.Elem())
			}
			if n, ok := t.(*types.Named); ok && n.Obj().Pkg() != nil {
				key := n.Obj().Pkg().Path() + "." + n.Obj().Name()
				if states[key] {
					st := t.Underlying().(*types.Struct)
					return key + "." + st.Field(v.Field).Name()
				}
			}
			return state(v.X, seen)
		case *ssa.UnOp:
			return state(v.X, seen)
		case *ssa.IndexAddr:
			return state(v.X, seen)
		case *ssa.Slice:
			return state(v.X, seen)
		case *ssa.Phi:
			for _, edge := range v.Edges {
				if s := state(edge, seen); s != "" {
					return s
				}
			}
		}
		return ""
	}
	// Public delegates are selected by compiled paths to actual state stores,
	// rather than expanding every public caller through other public owners.
	retains := map[*ssa.Function]bool{}
	var retainers []*ssa.Function
	for _, fn := range p.funcs {
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				var target ssa.Value
				switch ins := instruction.(type) {
				case *ssa.Store:
					target = ins.Addr
				case *ssa.MapUpdate:
					target = ins.Map
				}
				if target != nil && state(target, map[ssa.Value]bool{}) != "" && !retains[fn] {
					retains[fn] = true
					retainers = append(retainers, fn)
				}
			}
		}
	}
	for len(retainers) > 0 {
		fn := retainers[len(retainers)-1]
		retainers = retainers[:len(retainers)-1]
		if node := p.cg.Nodes[fn]; node != nil {
			for _, edge := range node.In {
				if caller := edge.Caller.Func; !retains[caller] {
					retains[caller] = true
					retainers = append(retainers, caller)
				}
			}
		}
	}
	for _, fn := range p.funcs {
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				switch ins := instruction.(type) {
				case *ssa.Return:
					for _, result := range ins.Results {
						if carry(result) {
							returns[fn] = append(returns[fn], result)
						}
					}
				case *ssa.Store:
					add(ins.Val, ins.Addr, "store/load alias")
					if field, ok := ins.Addr.(*ssa.FieldAddr); ok {
						add(ins.Val, field.X, "stored composite field")
					}
					if index, ok := ins.Addr.(*ssa.IndexAddr); ok {
						add(ins.Val, index.X, "stored slice element")
					}
					if owner := state(ins.Addr, map[ssa.Value]bool{}); owner != "" {
						sinks[ins.Val] = append(sinks[ins.Val], residualIngressSink{"retained-store", owner, p.pos(ins.Pos())})
					}
				case *ssa.MapUpdate:
					add(ins.Value, ins.Map, "stored map element")
					if owner := state(ins.Map, map[ssa.Value]bool{}); owner != "" {
						sinks[ins.Value] = append(sinks[ins.Value], residualIngressSink{"retained-map", owner, p.pos(ins.Pos())})
					}
				case *ssa.MakeClosure:
					if target, ok := ins.Fn.(*ssa.Function); ok {
						for i, binding := range ins.Bindings {
							delegate(binding, target.FreeVars[i], "closure binding")
							add(binding, ins, "retained closure capture")
						}
					}
				case *ssa.Call:
					if builtin, ok := ins.Common().Value.(*ssa.Builtin); ok && builtin.Name() == "append" {
						for _, arg := range ins.Common().Args {
							add(arg, ins, "append result")
						}
					}
				default:
					value, ok := instruction.(ssa.Value)
					if !ok {
						continue
					}
					// SSA data operations preserve aliases. Alloc/MakeMap/MakeSlice
					// introduce fresh storage and have no borrowed operands here.
					switch instruction.(type) {
					case *ssa.FieldAddr, *ssa.Field, *ssa.IndexAddr, *ssa.Index, *ssa.Lookup, *ssa.UnOp, *ssa.Slice, *ssa.Phi, *ssa.MakeInterface, *ssa.ChangeInterface, *ssa.ChangeType, *ssa.Convert, *ssa.TypeAssert, *ssa.Extract, *ssa.Range, *ssa.Next:
						for _, operand := range instruction.Operands(nil) {
							if operand != nil {
								add(*operand, value, "SSA data operand")
							}
						}
					}
				}
			}
		}
		if node := p.cg.Nodes[fn]; node != nil {
			for _, edge := range node.Out {
				if edge.Site == nil {
					continue
				}
				callee := edge.Callee.Func
				publicDelegate := false
				if callee != fn && callee.Parent() == nil && callee.Object() != nil && callee.Object().Exported() && callee.Object().Pkg() != nil {
					pkg := callee.Object().Pkg().Path()
					publicDelegate = strings.HasPrefix(pkg+"/", Module) && retains[callee]
				}
				args := append([]ssa.Value(nil), edge.Site.Common().Args...)
				if edge.Site.Common().IsInvoke() {
					args = append([]ssa.Value{edge.Site.Common().Value}, args...)
				}
				for i, arg := range args {
					if owners[p.funcName(callee)] {
						sinks[arg] = append(sinks[arg], residualIngressSink{"ownership-call", p.funcName(callee), p.pos(edge.Site.Pos())})
					} else if publicDelegate {
						// The callee is independently queried as an exported
						// boundary. Record the exact delegation instead of expanding
						// every higher public request through the entire database.
						sinks[arg] = append(sinks[arg], residualIngressSink{"public-delegate-call", p.funcName(callee), p.pos(edge.Site.Pos())})
					} else if i < len(callee.Params) {
						delegate(arg, callee.Params[i], "argument to "+p.funcName(callee))
					}
				}
				if owners[p.funcName(callee)] {
					continue
				}
				if call, ok := edge.Site.(*ssa.Call); ok {
					binding := residualIngressBinding{actual: map[ssa.Value]ssa.Value{}, result: call}
					for i, arg := range args {
						if i < len(callee.Params) {
							binding.actual[callee.Params[i]] = arg
						}
					}
					if closure, ok := edge.Site.Common().Value.(*ssa.MakeClosure); ok && closure.Fn == callee {
						for i, actual := range closure.Bindings {
							binding.actual[callee.FreeVars[i]] = actual
						}
					}
					bindings[callee] = append(bindings[callee], binding)
				}

			}
		}
	}
	// Local summaries are finite input-to-return/input relations. Applying a
	// relation to its original call site preserves caller identity without
	// enumerating call contexts or connecting a return to unrelated callers.
	relations := map[ssa.Value]map[ssa.Value]bool{}
	changed := true
	for changed {
		changed = false
		for _, fn := range p.funcs {
			if len(bindings[fn]) == 0 {
				continue
			}
			inputs := make([]ssa.Value, 0, len(fn.Params)+len(fn.FreeVars))
			for _, input := range fn.Params {
				if carry(input) {
					inputs = append(inputs, input)
				}
			}
			for _, input := range fn.FreeVars {
				if carry(input) {
					inputs = append(inputs, input)
				}
			}
			if len(inputs) < 2 && len(returns[fn]) == 0 {
				continue
			}
			for _, input := range inputs {
				reached := map[ssa.Value]bool{input: true}
				pending := []ssa.Value{input}
				for len(pending) > 0 {
					value := pending[len(pending)-1]
					pending = pending[:len(pending)-1]
					for _, edge := range flow[value] {
						if edge.to.Parent() == fn && !reached[edge.to] {
							reached[edge.to] = true
							pending = append(pending, edge.to)
						}
					}
				}
				var targets []ssa.Value
				for _, result := range returns[fn] {
					if reached[result] {
						targets = append(targets, nil)
						break
					}
				}
				for _, target := range inputs {
					if target != input && reached[target] {
						targets = append(targets, target)
					}
				}
				for _, target := range targets {
					if relations[input] == nil {
						relations[input] = map[ssa.Value]bool{}
					}
					if relations[input][target] {
						continue
					}
					relations[input][target] = true
					changed = true
					for _, binding := range bindings[fn] {
						actual, to := binding.actual[input], binding.result
						reason := "return alias at original call site"
						if target != nil {
							to = binding.actual[target]
							reason = "argument mutation at original call site"
						}
						if actual != nil && to != nil {
							add(actual, to, reason)
						}
					}
				}
			}
		}
	}
	for from, edges := range delegates {
		flow[from] = append(flow[from], edges...)
	}

	// A value with no path to any queried sink cannot contribute a matched
	// path either. Prune those dead branches before keeping call-site frames;
	// this changes neither the boundary set nor any ownership disposition.
	reverse := map[ssa.Value][]ssa.Value{}
	for from, edges := range flow {
		for _, edge := range edges {
			reverse[edge.to] = append(reverse[edge.to], from)
		}
	}
	mayReach := map[ssa.Value]bool{}
	var pending []ssa.Value
	for value := range sinks {
		mayReach[value] = true
		pending = append(pending, value)
	}
	for len(pending) > 0 {
		value := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		for _, from := range reverse[value] {
			if !mayReach[from] {
				mayReach[from] = true
				pending = append(pending, from)
			}
		}
	}
	var rows []residualIngressRow
	for _, fn := range p.funcs {
		if fn.Parent() != nil || fn.Object() == nil || !fn.Object().Exported() || fn.Pkg == nil || strings.HasPrefix(fn.Pkg.Pkg.Path()+"/", Module+"testing/") {
			continue
		}
		for i, param := range fn.Params {
			if fn.Signature.Recv() != nil && i == 0 || !mayReach[param] || !mutable(param.Type(), map[types.Type]bool{}) {
				continue
			}
			queue := []ssa.Value{param}
			seen := map[ssa.Value]bool{param: true}
			found := map[residualIngressSink]bool{}
			paths := map[string][]string{}
			previous := map[ssa.Value]ssa.Value{}
			reasons := map[ssa.Value]string{}
			for len(queue) > 0 {
				value := queue[0]
				queue = queue[1:]
				for _, sink := range sinks[value] {
					found[sink] = true
					key := sink.Kind + ":" + sink.Owner + ":" + sink.Position
					if _, exists := paths[key]; !exists {
						var reversed []string
						for step := value; step != param && step != nil; step = previous[step] {
							reversed = append(reversed, reasons[step]+" at "+p.pos(step.Pos()))
						}
						for j := len(reversed) - 1; j >= 0; j-- {
							paths[key] = append(paths[key], reversed[j])
						}
					}
				}
				for _, edge := range flow[value] {
					if mayReach[edge.to] && !seen[edge.to] {
						seen[edge.to] = true
						previous[edge.to], reasons[edge.to] = value, edge.reason
						queue = append(queue, edge.to)
					}
				}
			}

			if len(found) == 0 {
				continue
			}
			row := residualIngressRow{Boundary: p.funcName(fn), Argument: param.Name(), Type: param.Type().String(), Position: p.pos(fn.Pos()), Paths: paths}
			for sink := range found {
				row.Sinks = append(row.Sinks, sink)
			}
			sort.Slice(row.Sinks, func(i, j int) bool {
				a, b := row.Sinks[i], row.Sinks[j]
				return a.Kind+a.Owner+a.Position < b.Kind+b.Owner+b.Position
			})
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Boundary+rows[i].Argument < rows[j].Boundary+rows[j].Argument })
	return rows
}

func TestResidualExportedIngressPopulation(t *testing.T) {
	// The same single compiled load includes unsafe and owned controls outside
	// pkg/storage. They are reported separately from the product population.
	abs, err := filepath.Abs(root)
	require.NoError(t, err)
	path := filepath.Join(abs, "internal", "testfixture", "storageingress", "zz_residual_ingress.go")
	const plant = `package storageingress
import "github.com/orneryd/nornicdb/pkg/storage"
var retained *storage.Node
func RetainBorrowed(node *storage.Node) { retained = node }
func RetainOwned(node *storage.Node) { retained = storage.CopyNode(node) }
func pass(node *storage.Node) *storage.Node { return node }
func RetainViaReturn(node *storage.Node) { retained = pass(node) }
func IgnoreViaReturn(node *storage.Node) { _ = pass(node) }
`
	p, err := load(abs, map[string][]byte{path: []byte(plant)})
	require.NoError(t, err)
	rows := residualIngressQuery(p)
	unsafe, safeOwner, safeRetains := false, false, false
	returnRetains, ignoreRetains := false, false
	var product []residualIngressRow
	for _, row := range rows {
		if strings.Contains(row.Boundary, "internal/testfixture/storageingress.") {
			for _, sink := range row.Sinks {
				if strings.HasSuffix(row.Boundary, ".RetainBorrowed") && strings.HasPrefix(sink.Kind, "retained-") {
					unsafe = true
				}
				if strings.HasSuffix(row.Boundary, ".RetainOwned") {
					safeOwner = safeOwner || sink.Kind == "ownership-call"
					safeRetains = safeRetains || strings.HasPrefix(sink.Kind, "retained-")
				}
				if strings.HasSuffix(row.Boundary, ".RetainViaReturn") {
					returnRetains = returnRetains || strings.HasPrefix(sink.Kind, "retained-")
				}
				if strings.HasSuffix(row.Boundary, ".IgnoreViaReturn") {
					ignoreRetains = ignoreRetains || strings.HasPrefix(sink.Kind, "retained-")
				}
			}
			continue
		}
		product = append(product, row)
	}
	require.True(t, unsafe, "outside-package borrowed argument retention must be found")
	require.True(t, safeOwner, "owned control must name the existing copy owner")
	require.False(t, safeRetains, "the original borrowed control argument must stop at CopyNode")
	require.True(t, returnRetains, "borrowed data returned by a private delegate must reach its original caller's store")
	require.False(t, ignoreRetains, "another caller of that delegate must not inherit the first caller's store")
	data, err := json.MarshalIndent(struct {
		Author, Status string
		Members        []residualIngressRow
	}{"#1548 implementation lane (Codex)", "Compiled exported argument-flow candidates with finite parameter/return summaries at original call sites; ownership and public delegate calls explicit; no acquisition proof", product}, "", "  ")
	require.NoError(t, err)
	fmt.Printf("RESIDUAL-INGRESS-BEGIN\n%s\nRESIDUAL-INGRESS-END\n", data)
}
