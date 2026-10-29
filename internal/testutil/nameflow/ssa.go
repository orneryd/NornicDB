//go:build nameflowcensus

package nameflow

import (
	"fmt"
	"go/token"
	"go/types"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// ssaFlows supplements the all-source syntax interpretation with def/use from
// the shared engine's already loaded program. Source-only and inactive members
// remain candidates from the syntax pass. Neither pass classifies a member.
func (a *analysis) ssaFlows() {
	program := a.input.Program
	if program == nil {
		return
	}
	site := instructionSite(a.input)
	cell := func(v ssa.Value) string {
		if v == nil {
			return ""
		}
		parent := "global"
		if v.Parent() != nil {
			parent = v.Parent().String()
		}
		p := program.FileSet.Position(v.Pos())
		file, _ := filepath.Rel(program.Root, p.Filename)
		return fmt.Sprintf("ssa:%s:%s:%d:%s", parent, filepath.ToSlash(file), p.Offset, v.Name())
	}
	returnCell := func(fn *ssa.Function, slot int) string {
		return fmt.Sprintf("ssa-return:%s:%d", fn.String(), slot)
	}
	errorInterface := types.Universe.Lookup("error").Type().Underlying().(*types.Interface)
	diagnostics := map[types.Type]bool{}
	checkedDiagnostic := map[types.Type]bool{}
	diagnostic := func(typ types.Type) bool {
		if !checkedDiagnostic[typ] {
			diagnostics[typ] = types.Implements(typ, errorInterface)
			checkedDiagnostic[typ] = true
		}
		return diagnostics[typ]
	}
	namePayload := func(typ types.Type) bool {
		// An error/cancellation status is not a decoded symbolic name. Calls
		// receiving names still have sink obligations at their source sites.
		if diagnostic(typ) {
			return false
		}
		if basic, ok := typ.Underlying().(*types.Basic); ok {
			return basic.Info()&types.IsString != 0
		}
		return true
	}
	for _, root := range a.contracts.Fields {
		a.source("field:"+root.Type+"."+root.Field+root.Channel, root.Role, root.Type+"."+root.Field, "compiled field contract")
	}
	for _, fn := range program.Functions {
		if fn.Pkg == nil || (!strings.HasPrefix(fn.Pkg.Pkg.Path(), "github.com/orneryd/nornicdb") && fn.Pkg.Pkg.Path() != "command-line-arguments") {
			continue
		}
		if a.compiled {
			f := function{}
			for index := 0; index < fn.Signature.Results().Len(); index++ {
				f.returns = append(f.returns, returnCell(fn, index))
			}
			a.functions[fn.String()] = f
		}
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				pos := instruction.Pos()
				if value, ok := instruction.(ssa.Value); ok && !diagnostic(value.Type()) {
					typ := value.Type()
					if pointer, ok := typ.(*types.Pointer); ok {
						typ = pointer.Elem()
					}
					if named, ok := types.Unalias(typ).(*types.Named); ok {
						if structure, ok := named.Underlying().(*types.Struct); ok {
							for index := 0; index < structure.NumFields(); index++ {
								a.aggregateContent(declaredFieldKey(typ, index), cell(value), fn.String())
							}
						}
					}
				}
				switch n := instruction.(type) {
				case *ssa.MakeClosure:
					if target, ok := n.Fn.(*ssa.Function); ok {
						for index, binding := range n.Bindings {
							if index < len(target.FreeVars) {
								a.copy(cell(binding), cell(target.FreeVars[index]), "SSA closure binding", fn.String())
								a.copy(cell(binding)+"/memory", cell(target.FreeVars[index])+"/memory", "SSA captured storage", fn.String())
							}
						}
					}
				case *ssa.Range:
					a.copy(cell(n.X), cell(n), "SSA range collection", fn.String())
				case *ssa.Next:
					a.copy(cell(n.Iter)+"/key", cell(n)+"#1", "SSA range key", fn.String())
					a.copy(cell(n.Iter)+"/element", cell(n)+"#2", "SSA range value", fn.String())
				case *ssa.Phi:
					if diagnostic(n.Type()) {
						continue
					}
					for _, edge := range n.Edges {
						a.copy(cell(edge), cell(n), "SSA phi", fn.String())
					}
				case *ssa.ChangeType:
					if diagnostic(n.Type()) {
						continue
					}
					a.copy(cell(n.X), cell(n), "SSA change type", fn.String())
				case *ssa.Convert:
					if namePayload(n.Type()) {
						a.copy(cell(n.X), cell(n), "SSA conversion", fn.String())
					}
				case *ssa.MakeInterface:
					if diagnostic(n.Type()) {
						continue
					}
					a.copy(cell(n.X), cell(n), "SSA interface", fn.String())
				case *ssa.TypeAssert:
					if diagnostic(n.AssertedType) {
						continue
					}
					a.copy(cell(n.X), cell(n), "SSA assertion", fn.String())
					a.copy(cell(n.X), cell(n)+"#0", "SSA asserted result", fn.String())
				case *ssa.Extract:
					if diagnostic(n.Type()) {
						continue
					}
					a.copy(cell(n.Tuple)+fmt.Sprintf("#%d", n.Index), cell(n), "SSA tuple result", fn.String())
				case *ssa.Store:
					if diagnostic(n.Val.Type()) {
						continue
					}
					a.copy(cell(n.Val), cell(n.Addr)+"/memory", "SSA store", fn.String())
					if m := site(pos, "assign"); m.ID != "" {
						a.inputs[m.ID] = append(a.inputs[m.ID], cell(n.Val))
						a.kinds[m.ID] = "compiled value/field store"
					}
				case *ssa.UnOp:
					if diagnostic(n.Type()) {
						continue
					}
					if n.Op == token.MUL {
						a.copy(cell(n.X)+"/memory", cell(n), "SSA load", fn.String())
					}
				case *ssa.FieldAddr:
					key := declaredFieldKey(n.X.Type(), n.Field)
					if key != "" {
						a.copy(key, cell(n)+"/memory", "SSA field load address", fn.String())
						a.copy(cell(n)+"/memory", key, "SSA field store address", fn.String())
						for _, root := range a.contracts.Fields {
							if key == "field:"+root.Type+"."+root.Field {
								a.source(key+root.Channel, root.Role, root.Type+"."+root.Field, fn.String())
							}
						}
					}
				case *ssa.Field:
					key := declaredFieldKey(n.X.Type(), n.Field)
					if key != "" {
						a.copy(key, cell(n), "SSA field read", fn.String())
					}
				case *ssa.IndexAddr:
					a.copy(cell(n.X)+"/element", cell(n)+"/memory", "SSA element load address", fn.String())
					a.copy(cell(n)+"/memory", cell(n.X)+"/element", "SSA element store address", fn.String())
				case *ssa.Index:
					a.copy(cell(n.X)+"/element", cell(n), "SSA element read", fn.String())
				case *ssa.Slice:
					a.copy(cell(n.X), cell(n), "SSA slice", fn.String())
					if m := site(pos, "slice"); m.ID != "" {
						a.inputs[m.ID] = append(a.inputs[m.ID], cell(n.X))
						a.kinds[m.ID] = "compiled slice"
					}
				case *ssa.Lookup:
					a.copy(cell(n.X)+"/element", cell(n), "SSA map value", fn.String())
					a.copy(cell(n), cell(n)+"#0", "SSA map lookup value", fn.String())
					if m := site(pos, "index"); m.ID != "" {
						a.inputs[m.ID] = append(a.inputs[m.ID], cell(n.Index))
						a.kinds[m.ID] = "compiled key lookup"
					}
				case *ssa.MapUpdate:
					for _, kind := range []string{"index", "assign", "composite"} {
						if m := site(pos, kind); m.ID != "" {
							a.inputs[m.ID] = append(a.inputs[m.ID], cell(n.Key))
							a.kinds[m.ID] = "compiled map key write"
						}
					}
					a.add(cell(n.Key), cell(n.Map)+"/key", "SSA map key", fn.String())
					a.copy(cell(n.Value), cell(n.Map)+"/element", "SSA map value store", fn.String())
				case *ssa.BinOp:
					if n.Op == token.ADD && namePayload(n.Type()) {
						a.add(cell(n.X), cell(n), "SSA concatenation", fn.String())
						a.add(cell(n.Y), cell(n), "SSA concatenation", fn.String())
					}
					if m := site(pos, "binary"); m.ID != "" {
						a.inputs[m.ID] = append(a.inputs[m.ID], cell(n.X), cell(n.Y))
						a.kinds[m.ID] = "compiled comparison/concatenation"
					}
				case *ssa.Return:
					for i, value := range n.Results {
						if diagnostic(value.Type()) {
							continue
						}
						a.copy(cell(value), returnCell(fn, i), "SSA return", fn.String())
					}
				case ssa.CallInstruction:
					common := n.Common()
					m := site(pos, "call")
					if m.ID == "" {
						continue
					}
					a.kinds[m.ID] = "compiled call input/transform/emit"
					for _, arg := range common.Args {
						if diagnostic(arg.Type()) {
							continue
						}
						a.inputs[m.ID] = append(a.inputs[m.ID], cell(arg))
					}
					callValue := n.Value()
					if builtin, ok := common.Value.(*ssa.Builtin); ok {
						switch builtin.Name() {
						case "copy":
							if len(common.Args) == 2 {
								a.copy(cell(common.Args[1]), cell(common.Args[0]), "SSA copy destination contents", m.ID)
							}
						case "append":
							if callValue != nil {
								for _, arg := range common.Args {
									a.copy(cell(arg), cell(callValue), "SSA append contents", m.ID)
								}
							}
						}
					}
					barrier := false
					for _, root := range a.contracts.Calls {
						for _, target := range m.Callees {
							if target == root.Symbol && callValue != nil {
								output := cell(callValue)
								if _, tuple := callValue.Type().(*types.Tuple); tuple {
									output += fmt.Sprintf("#%d", root.Result)
								}
								a.source(output, root.Role, "SSA "+root.Symbol, m.ID)
								barrier = true
							}
						}
					}
					for _, writer := range a.contracts.Writers {
						for _, target := range m.Callees {
							barrier = barrier || target == writer
						}
					}
					var targets []*ssa.Function
					if target := common.StaticCallee(); target != nil {
						targets = append(targets, target)
					} else if node := program.CallGraph.Nodes[fn]; node != nil {
						for _, edge := range node.Out {
							if edge.Site == n {
								targets = append(targets, edge.Callee.Func)
							}
						}
					}
					if len(targets) == 0 {
						if _, builtin := common.Value.(*ssa.Builtin); !builtin {
							a.unknown[m.ID] = true
						}
					}
					for _, target := range targets {
						symbol := target.String()
						if object, ok := target.Object().(*types.Func); ok {
							symbol = object.FullName()
						}
						for _, root := range a.contracts.Calls {
							if symbol == root.Symbol && callValue != nil {
								output := cell(callValue)
								if _, tuple := callValue.Type().(*types.Tuple); tuple {
									output += fmt.Sprintf("#%d", root.Result)
								}
								a.source(output, root.Role, "compiled "+symbol, m.ID)
								barrier = true
							}
						}
						for _, writer := range a.contracts.Writers {
							barrier = barrier || symbol == writer
						}
						if !barrier && len(target.Blocks) == 0 && callValue != nil {
							for index := 0; index < target.Signature.Results().Len(); index++ {
								if !namePayload(target.Signature.Results().At(index).Type()) {
									continue
								}
								output := cell(callValue)
								if _, tuple := callValue.Type().(*types.Tuple); tuple {
									output += fmt.Sprintf("#%d", index)
								}
								for _, arg := range common.Args {
									a.copy(cell(arg), output, "SSA opaque name-capable result", m.ID)
								}
							}
						}
						for i, arg := range common.Args {
							if diagnostic(arg.Type()) {
								continue
							}
							if i < len(target.Params) {
								a.copy(cell(arg), cell(target.Params[i]), "SSA actual to parameter", m.ID)
							}
						}
						if !barrier && callValue != nil {
							for i := 0; i < target.Signature.Results().Len(); i++ {
								if diagnostic(target.Signature.Results().At(i).Type()) {
									continue
								}
								output := cell(callValue)
								if _, tuple := callValue.Type().(*types.Tuple); tuple {
									output += fmt.Sprintf("#%d", i)
								}
								a.copy(returnCell(target, i), output, "SSA result to caller", m.ID)
							}
						}
					}
				}
			}
		}
	}
}
