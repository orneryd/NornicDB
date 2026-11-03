package workcensus

import (
	"fmt"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// FieldProjection is the context-independent projection of an inline value
// field at a use. Values retain their real SSA provenance (no fabricated loads).
// Witness records zero/unknown or possibly nil inputs; Why records non-nil proof.
type FieldProjection struct {
	Values       []ssa.Value
	Witness, Why []string
	Unknown      bool
	Field        *types.Var // final inline field; conservative provenance on unknown writes
}

// ProjectFieldAt follows an exact inline struct index path in a value snapshot.
// Unknown calls, aliases and unsupported representations remain possible nil;
// this does not interpret pointer heaps or infer an external factory contract.
func (p *Program) ProjectFieldAt(value ssa.Value, path []int, at ssa.Instruction) FieldProjection {
	if p.nilFlow == nil {
		p.nilFlow = newNilFlow(p)
	}
	f := p.nilFlow
	f.trail = f.trail[:0]
	return f.projectFieldAt(value, path, at)
}

func (f *nilFlow) projectFieldAt(value ssa.Value, path []int, at ssa.Instruction) FieldProjection {
	p := f.p
	var out FieldProjection
	fieldType := value.Type()
	for _, index := range path {
		st, ok := types.Unalias(fieldType).Underlying().(*types.Struct)
		if !ok || index < 0 || index >= st.NumFields() {
			out.Unknown = true
			break
		}
		out.Field = st.Field(index)
		fieldType = out.Field.Type()
	}
	seen := map[string]bool{}
	var project func(ssa.Value, []int, ssa.Instruction)
	unknown := func(reason string) {
		out.Unknown = true
		if out.Witness == nil {
			out.Witness = []string{reason}
		}
	}
	project = func(v ssa.Value, path []int, use ssa.Instruction) {
		if len(path) == 0 {
			out.Values = append(out.Values, v)
			if w := f.use(v, use); w != nil && out.Witness == nil {
				out.Witness = append([]string(nil), w...)
			}
			return
		}
		key := fmt.Sprintf("%p/%v/%p", v, path, use)
		if seen[key] {
			unknown("unresolved cyclic inline field projection")
			return
		}
		seen[key] = true
		defer delete(seen, key)
		st, ok := types.Unalias(v.Type()).Underlying().(*types.Struct)
		if !ok || path[0] < 0 || path[0] >= st.NumFields() {
			unknown("unknown inline field projection")
			return
		}
		field := st.Field(path[0])
		name := "field " + field.Name() + " of " + v.Type().String()
		switch x := v.(type) {
		case *ssa.Const:
			if out.Witness == nil {
				out.Witness = []string{"zero " + name}
			}
		case *ssa.ChangeType:
			project(x.X, path, use)
		case *ssa.Field:
			project(x.X, append([]int{x.Field}, path...), use)
		case *ssa.Phi:
			if len(x.Edges) == 0 {
				unknown("unknown phi " + name)
			}
			for _, edge := range x.Edges {
				project(edge, path, use)
			}
		case *ssa.Parameter:
			index := sourceParameterIndex(x)
			matched := false
			for _, site := range p.SourceCallSites(x.Parent()) {
				if arg, compatible := sourceArgumentAt(x, index, site); compatible {
					matched = true
					project(arg, path, site)
				}
			}
			if !matched {
				unknown("unknown caller " + name)
			}
		case *ssa.Call:
			f.projectFieldReturns(x, 0, path, use, project, unknown)
		case *ssa.Extract:
			if call, ok := x.Tuple.(*ssa.Call); ok {
				f.projectFieldReturns(call, x.Index, path, use, project, unknown)
			} else if selection, ok := x.Tuple.(*ssa.Select); ok {
				// A receive case's value slot: the selected-state edge and the
				// closed channel population decide it, not the slot's type.
				f.projectSelectReceive(x, selection, path, use, project, unknown)
			} else {
				unknown("unknown result " + name)
			}
		case *ssa.UnOp:
			if x.Op != token.MUL {
				unknown("unknown load " + name)
				return
			}
			addr := x.X
			rest := path
			// Use only real field addresses. If a field was never addressed, its
			// enclosing whole-value definitions still determine the omitted value.
			for len(rest) > 0 {
				var next *ssa.FieldAddr
				if addr.Referrers() != nil {
					for _, ref := range *addr.Referrers() {
						if fa, ok := ref.(*ssa.FieldAddr); ok && fa.X == addr && fa.Field == rest[0] {
							next = fa
							break
						}
					}
				}
				if next == nil {
					break
				}
				addr = next
				rest = rest[1:]
			}
			var locationField *types.Var
			if fa, ok := addr.(*ssa.FieldAddr); ok {
				locationField = structField(fa.X.Type(), fa.Field)
			} else {
				locationField = field
			}
			defs := definitionsBefore(x.Block(), instructionIndex(x), addr, locationField)
			if len(defs) == 0 {
				unknown("unknown definitions of " + name)
			}
			for _, d := range defs {
				switch {
				case d.store != nil:
					project(d.store.Val, rest, use)
				case d.whole != nil:
					wholePath := d.wholePath
					if len(wholePath) == 0 {
						wholePath = []int{fieldIndex(d.whole.Val.Type(), locationField)}
					}
					project(d.whole.Val, append(append([]int(nil), wholePath...), rest...), use)
				case d.unknown:
					unknown("unknown write to " + name)
				case d.nonNilBranch && len(rest) == 0:
					// A nil guard proves validity, not request provenance.
					// Keep its real load for independent fresh-source taint.
					if d.tested != nil {
						out.Values = append(out.Values, d.tested)
					} else {
						out.Unknown = true
					}
					f.discharge("proof: non-nil branch of projected location")
				default:
					// Only a local allocation's initial contents are known
					// zero. A global/borrowed address retains unknown field
					// provenance even when no writer is visible in this body.
					root := addr
					for {
						fa, ok := root.(*ssa.FieldAddr)
						if !ok {
							break
						}
						root = fa.X
					}
					if _, local := root.(*ssa.Alloc); !local && !d.nilBranch {
						out.Unknown = true
					}
					if out.Witness == nil {
						out.Witness = []string{"zero or unknown " + name}
					}
				}
			}
		default:
			unknown("unknown representation of " + name)
		}
	}
	project(value, path, at)
	if out.Witness == nil {
		f.discharge("proof: exact inline value field projection")
		out.Why = dedupe(f.trail)
	}
	return out
}

func (f *nilFlow) projectFieldReturns(call *ssa.Call, result int, path []int, use ssa.Instruction, project func(ssa.Value, []int, ssa.Instruction), unknown func(string)) {
	// Conditional tuple knowledge belongs only to this invocation's actual
	// checked use, never the context-insensitive value/field memo. Blocks in
	// different functions cannot establish a dominance relationship.
	successOnly := use != nil && use.Block() != nil && use.Parent() == call.Parent() && errorChecked(call, use.Block())
	targets := f.p.CallTargets(call)
	if len(targets) == 0 {
		unknown("unknown inline field factory")
		return
	}
	for _, target := range targets {
		found := false
		for _, b := range target.Blocks {
			for _, ins := range b.Instrs {
				if ret, ok := ins.(*ssa.Return); ok && result < len(ret.Results) {
					// Reuse the source error proof: external-result/non-nil
					// conventions cannot exclude a possibly successful tuple.
					last := len(ret.Results) - 1
					if successOnly && result < last && isError(ret.Results[last].Type()) && newErrorProof(f).fact(ret.Results[last], ret).nonnil {
						f.discharge("proof: inline field return with source-proven non-nil error excluded by same-call success check")
						continue
					}
					found = true
					project(ret.Results[result], path, ret)
				}
			}
		}
		if !found {
			unknown("unknown inline field result of " + target.String())
		}
	}
}

// inlineAddressPath is the exact index path from an existing ancestor address
// to addr. Distinct reloads/aliases are not equated here.
func inlineAddressPath(addr, ancestor ssa.Value) ([]int, bool) {
	if addr == ancestor {
		return nil, true
	}
	fa, ok := addr.(*ssa.FieldAddr)
	if !ok {
		return nil, false
	}
	path, ok := inlineAddressPath(fa.X, ancestor)
	if !ok {
		return nil, false
	}
	return append(path, fa.Field), true
}

// InlineFieldPaths enumerates context-independent inline leaves matching leaf.
// Pointer/container contents are deliberately not followed.
func InlineFieldPaths(t types.Type, leaf func(types.Type) bool) [][]int {
	var out [][]int
	var visit func(types.Type, []int)
	visit = func(t types.Type, path []int) {
		if leaf(t) {
			out = append(out, append([]int(nil), path...))
			return
		}
		if st, ok := types.Unalias(t).Underlying().(*types.Struct); ok {
			for i := 0; i < st.NumFields(); i++ {
				visit(st.Field(i).Type(), append(append([]int(nil), path...), i))
			}
		}
	}
	visit(t, nil)
	return out
}

// InlineFieldName describes a previously enumerated inline index path.
func InlineFieldName(t types.Type, path []int) string {
	var names []string
	for _, i := range path {
		st, ok := types.Unalias(t).Underlying().(*types.Struct)
		if !ok || i < 0 || i >= st.NumFields() {
			return "unknown"
		}
		f := st.Field(i)
		names = append(names, f.Name())
		t = f.Type()
	}
	return strings.Join(names, ".")
}
