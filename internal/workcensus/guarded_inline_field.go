package workcensus

import (
	"go/token"
	"go/types"

	"golang.org/x/tools/go/ssa"
)

// GuardedInlineFieldUses proves that a private inline field is read only in a
// sibling's non-nil arm, and that every semantic use takes the field's own
// non-nil edge. It examines the complete loaded field-use population, including
// retained copies, rather than treating an omitted field as a valid context.
// Unknown projections, representations and outward escapes confer no proof.
func (p *Program) GuardedInlineFieldUses(value ssa.Value, path []int, projection FieldProjection) bool {
	if projection.Unknown || projection.Field == nil || len(path) != 1 {
		return false
	}
	named, ok := types.Unalias(value.Type()).(*types.Named)
	if !ok || named.Obj().Exported() || projection.Field.Exported() {
		return false
	}
	field := projection.Field
	if answer, found := p.guardedFields[field]; found {
		return answer
	}
	if p.guardedFields == nil {
		p.guardedFields = map[*types.Var]bool{}
	}
	p.guardedFields[field] = false
	proven := p.guardedInlineFieldUses(named, field)
	p.guardedFields[field] = proven
	return proven
}

func (p *Program) guardedInlineFieldUses(named *types.Named, field *types.Var) bool {
	reads := 0
	for _, fn := range p.Functions {
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				switch x := instruction.(type) {
				case *ssa.MakeInterface:
					if inlineContainsType(x.X.Type(), named, map[types.Type]bool{}) {
						return false
					}
				case *ssa.MakeClosure:
					for _, binding := range x.Bindings {
						if inlineContainsType(binding.Type(), named, map[types.Type]bool{}) {
							return false
						}
					}
				case *ssa.Convert:
					if inlineContainsType(x.X.Type(), named, map[types.Type]bool{}) {
						return false
					}
				case *ssa.ChangeType:
					if inlineContainsType(x.X.Type(), named, map[types.Type]bool{}) && !types.Identical(x.X.Type(), x.Type()) {
						return false
					}
				case *ssa.Return:
					for _, result := range x.Results {
						if inlineContainsType(result.Type(), named, map[types.Type]bool{}) && (token.IsExported(fn.Name()) || len(p.SourceCallSites(fn)) == 0) {
							return false
						}
					}
				case ssa.CallInstruction:
					for _, arg := range x.Common().Args {
						if !inlineContainsType(arg.Type(), named, map[types.Type]bool{}) {
							continue
						}
						if _, builtin := x.Common().Value.(*ssa.Builtin); builtin {
							continue
						}
						target := x.Common().StaticCallee()
						if _, synchronous := x.(*ssa.Call); !synchronous || target == nil {
							return false
						}
						if len(target.Blocks) == 0 && !p.inlineAggregateCopy(target) {
							return false
						}
						if _, pointer := arg.Type().Underlying().(*types.Pointer); pointer {
							return false
						}
					}
				case *ssa.Field:
					if structField(x.X.Type(), x.Field) == field {
						reads++
						if !guardedValueField(x) || !guardedFieldConsumers(x) {
							return false
						}
					}
				case *ssa.FieldAddr:
					if structField(x.X.Type(), x.Field) != field {
						continue
					}
					if !localInlineObject(x.X) || x.Referrers() == nil {
						return false
					}
					for _, use := range *x.Referrers() {
						switch use := use.(type) {
						case *ssa.DebugRef, *ssa.Store:
						case *ssa.UnOp:
							reads++
							if use.Op != token.MUL || !guardedAddressField(x, use) || !guardedFieldConsumers(use) {
								return false
							}
						default:
							return false
						}
					}
				}
			}
		}
	}
	return reads > 0
}

func (p *Program) inlineAggregateCopy(target *ssa.Function) bool {
	if p.nilFlow == nil {
		p.nilFlow = newNilFlow(p)
	}
	obj := target.Object()
	if obj == nil || obj.Pkg() == nil {
		return false
	}
	pkg := p.nilFlow.sourcePackage(obj.Pkg().Path())
	if pkg == nil {
		return false
	}
	body := pkg.Func(obj.Name())
	if body == nil || len(body.Blocks) == 0 || body.Recover != nil {
		return false
	}
	for _, block := range body.Blocks {
		for _, instruction := range block.Instrs {
			switch x := instruction.(type) {
			case *ssa.DebugRef, *ssa.If, *ssa.Jump, *ssa.BinOp, *ssa.Slice, *ssa.Return, *ssa.ChangeType:
			case *ssa.Alloc:
				ptr, ok := x.Type().Underlying().(*types.Pointer)
				if !ok {
					return false
				}
				array, ok := ptr.Elem().Underlying().(*types.Array)
				if !ok || array.Len() != 0 {
					return false
				}
			case *ssa.Call:
				builtin, ok := x.Common().Value.(*ssa.Builtin)
				if !ok || (builtin.Name() != "append" && builtin.Name() != "len" && builtin.Name() != "cap") {
					return false
				}
			default:
				return false
			}
		}
	}
	return true
}

func inlineContainsType(t, wanted types.Type, seen map[types.Type]bool) bool {
	if types.Identical(t, wanted) {
		return true
	}
	if seen[t] {
		return false
	}
	seen[t] = true
	switch x := t.Underlying().(type) {
	case *types.Pointer:
		return types.Identical(x.Elem(), wanted)
	case *types.Array:
		return inlineContainsType(x.Elem(), wanted, seen)
	case *types.Slice:
		return inlineContainsType(x.Elem(), wanted, seen)
	case *types.Chan:
		return inlineContainsType(x.Elem(), wanted, seen)
	case *types.Map:
		return inlineContainsType(x.Key(), wanted, seen) || inlineContainsType(x.Elem(), wanted, seen)
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if inlineContainsType(x.Field(i).Type(), wanted, seen) {
				return true
			}
		}
	}
	return false
}

func guardedAddressField(field *ssa.FieldAddr, load *ssa.UnOp) bool {
	if field.X.Referrers() == nil {
		return false
	}
	for _, ref := range *field.X.Referrers() {
		sibling, ok := ref.(*ssa.FieldAddr)
		if !ok || sibling.Field == field.Field {
			continue
		}
		defs := definitionsBefore(load.Block(), instructionIndex(load), sibling, structField(sibling.X.Type(), sibling.Field))
		if allNonNilBranches(defs) {
			return true
		}
	}
	return false
}

func guardedValueField(field *ssa.Field) bool {
	if field.X.Referrers() == nil {
		return false
	}
	for _, ref := range *field.X.Referrers() {
		sibling, ok := ref.(*ssa.Field)
		if ok && sibling.Field != field.Field && NonNilAt(sibling, field.Block()) {
			return true
		}
	}
	return false
}

func allNonNilBranches(defs []definition) bool {
	if len(defs) == 0 {
		return false
	}
	for _, d := range defs {
		if !d.nonNilBranch {
			return false
		}
	}
	return true
}

func guardedFieldConsumers(value ssa.Value) bool {
	if value.Referrers() == nil {
		return false
	}
	for _, use := range *value.Referrers() {
		switch use := use.(type) {
		case *ssa.DebugRef:
			continue
		case *ssa.BinOp:
			if tested, _, ok := NilComparison(use); ok && tested == value {
				continue
			}
		}
		if NonNilAt(value, use.Block()) {
			continue
		}
		load, ok := value.(*ssa.UnOp)
		if !ok {
			return false
		}
		addr, ok := load.X.(*ssa.FieldAddr)
		if !ok || !allNonNilBranches(reachingDefinitions(load, addr, structField(addr.X.Type(), addr.Field))) {
			return false
		}
	}
	return true
}
