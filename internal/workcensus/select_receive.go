package workcensus

// Selected-case receive projection author: PR #1693 event-select lane, 2 November 2026.

import (
	"fmt"
	"go/token"
	"go/types"
	"strconv"

	"golang.org/x/tools/go/ssa"
)

// This file adds one representation the inline field projection learned to read:
// the value the compiler extracts from a *ssa.Select receive case. A receive
// value slot is not a value the source ever named, so nothing in the def/use
// graph decides its provenance by itself. Two facts decide it, and both are
// needed together:
//
//   - The use runs only where that receive state was the chosen state. The
//     compiler guards each case with the selected index, and an unchosen
//     receive slot is the zero element, so a value slot read outside its own
//     selected-state edge carries the zero struct and can populate no field.
//   - The channel population that can deliver to that receive is finite and
//     closed. Every creation, store, capture, alias, send, receive and close of
//     a channel with that exact element type is enumerated from the loaded
//     source, so the sent values are a complete over-approximation of what the
//     receive can hold. A closed channel, an unenumerated producer, an escape
//     to code the program does not contain, or a reassignment from an unknown
//     source makes the population incomplete, and the answer stays unknown.
//
// Neither fact is a convention about the channel's type, its element's fields
// or the number of writers it has: an accepted family projects every enumerated
// send value's own field through the ordinary projection, so a zero, omitted or
// nil field in any producer keeps its witness.

// channelFamily is the closed population of channel values with one exact
// element type in the loaded program, together with every value sent on one. It
// is source evidence, not an acceptance: the first operation the population
// cannot account for makes the whole family unknown, and an unknown family
// demonstrates nothing about any receive.
type channelFamily struct {
	elem    types.Type
	creates int
	sends   []channelSend
	unknown string // the first source operation this population cannot account for
}

// channelSend is one enumerated send: the value snapshot at the site that
// queues it, never the receiver's later copy and never a union of field stores.
type channelSend struct {
	value ssa.Value
	at    ssa.Instruction
}

// channelFamilyOf enumerates, once per exact element type, every creation,
// store, capture, send, receive and close of a channel of that element type
// over the existing Program.Functions population. It is source-derived data for
// one query, with no registry, name allowance or acceptance path: an element
// type that any source operation cannot account for yields a family with an
// unknown reason, and the projection then keeps its unknown answer.
func (p *Program) channelFamilyOf(elem types.Type) *channelFamily {
	if fact, ok := p.channelFamilies[elem]; ok {
		return fact
	}
	fact := &channelFamily{elem: elem}
	if p.channelFamilies == nil {
		p.channelFamilies = map[types.Type]*channelFamily{}
	}
	p.channelFamilies[elem] = fact
	exposure := &channelExposure{elem: elem, memo: map[types.Type]bool{}, active: map[types.Type]bool{}}
	containment := &channelExposure{elem: elem, privateFields: true, memo: map[types.Type]bool{}, active: map[types.Type]bool{}}
	reject := func(fn *ssa.Function, pos token.Pos, reason string) {
		if fact.unknown != "" {
			return
		}
		fact.unknown = reason
		if fn != nil && pos.IsValid() {
			at := p.FileSet.PositionFor(pos, false)
			fact.unknown = fmt.Sprintf("%s:%d: %s", at.Filename, at.Line, reason)
		}
	}
	for _, fn := range p.Functions {
		if fn.Blocks == nil {
			continue
		}
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				if value, ok := instruction.(ssa.Value); ok && exposure.exposes(value.Type()) {
					switch definition := value.(type) {
					case *ssa.MakeChan:
						if types.Identical(channelElement(definition.Type()), elem) {
							fact.creates++
						} else {
							reject(fn, instruction.Pos(), "a channel of "+definition.Type().String()+" carries the family element type")
						}
					case *ssa.Alloc, *ssa.FieldAddr, *ssa.Field, *ssa.UnOp, *ssa.Phi, *ssa.ChangeType, *ssa.Convert:
						// An address, a load of one, or the same channel in
						// another representation: every use is classified.
					default:
						reject(fn, instruction.Pos(), fmt.Sprintf("a channel value from an unmodelled definition %T", value))
					}
				}
				if value, ok := instruction.(ssa.Value); ok && !exposure.exposes(value.Type()) && containment.exposes(value.Type()) {
					switch definition := value.(type) {
					case *ssa.Call:
						if reason := channelCallSeal(definition); reason != "" {
							reject(fn, instruction.Pos(), "a private channel carrier returned by "+reason)
						}
					case *ssa.Extract:
						if call, ok := definition.Tuple.(*ssa.Call); ok {
							if reason := channelCallSeal(call); reason != "" {
								reject(fn, instruction.Pos(), reason)
							}
						} else {
							reject(fn, instruction.Pos(), "a private channel carrier from an unproved tuple")
						}
					}
				}
				// Select states are distinct producers even when their channel
				// operand is the same SSA value. Enumerate states once, rather
				// than choosing the first matching state for every operand use.
				if selection, ok := instruction.(*ssa.Select); ok {
					for _, state := range selection.States {
						if state.Dir == types.SendOnly && types.Identical(channelElement(state.Chan.Type()), elem) {
							fact.sends = append(fact.sends, channelSend{value: state.Send, at: instruction})
						}
					}
				}
				for _, operand := range instruction.Operands(nil) {
					if operand == nil || *operand == nil {
						continue
					}
					if !exposure.exposes((*operand).Type()) {
						if containment.exposes((*operand).Type()) {
							if reason := hiddenChannelUse(instruction, *operand, containment); reason != "" {
								reject(fn, instruction.Pos(), reason)
							}
						}
						continue
					}
					send, reason := channelUse(instruction, *operand)
					if reason != "" {
						reject(fn, instruction.Pos(), reason)
						continue
					}
					if send != nil {
						fact.sends = append(fact.sends, *send)
					}
				}
			}
		}
	}
	return fact
}

// hiddenChannelUse checks actual carrier escapes even when an ordinary foreign
// caller cannot name its private field. Nameability seals typed field access,
// not interface/unsafe reflection or an escaping callback retaining the carrier.
func hiddenChannelUse(instruction ssa.Instruction, operand ssa.Value, containment *channelExposure) string {
	if global, ok := operand.(*ssa.Global); ok && global.Object() != nil && global.Object().Exported() {
		return "a private channel carrier in an exported package variable"
	}
	if parameter, ok := operand.(*ssa.Parameter); ok {
		if object := parameter.Parent().Object(); object != nil && object.Exported() {
			return "a private channel carrier supplied by an externally callable parameter"
		}
	}
	switch use := instruction.(type) {
	case *ssa.MakeInterface, *ssa.ChangeInterface, *ssa.TypeAssert:
		return "a private channel carrier converted to or from an interface"
	case *ssa.Convert:
		if !containment.exposes(use.Type()) {
			return "a private channel carrier converted to an opaque representation"
		}
	case *ssa.Call:
		return channelCallSeal(use)
	case *ssa.Go:
		return channelCallSeal(use)
	case *ssa.Defer:
		return channelCallSeal(use)
	case *ssa.Return:
		return "a private channel carrier returned from " + use.Parent().String()
	case *ssa.Panic:
		return "a private channel carrier passed to panic"
	case *ssa.Store:
		if use.Val == operand {
			return channelStoreSeal(use.Addr)
		}
	case *ssa.MapUpdate, *ssa.Send, *ssa.Select:
		return "a private channel carrier moved into a container or channel"
	case *ssa.MakeClosure:
		return channelClosureSeal(use, map[ssa.Value]bool{})
	case *ssa.DebugRef, *ssa.UnOp, *ssa.FieldAddr, *ssa.Field, *ssa.Phi, *ssa.ChangeType, *ssa.BinOp, *ssa.If:
		return ""
	default:
		return fmt.Sprintf("a private channel carrier used by an unmodelled instruction %T", instruction)
	}
	return ""
}

// A closure capture remains enumerated only while the function value is used
// directly by loaded calls. Storing, returning or passing it opaquely loses the
// complete carrier-use population just as escaping the carrier itself does.
func channelClosureSeal(value ssa.Value, seen map[ssa.Value]bool) string {
	if seen[value] {
		return ""
	}
	seen[value] = true
	if value.Referrers() == nil {
		return "a private channel carrier captured by an unenumerated closure"
	}
	for _, instruction := range *value.Referrers() {
		switch use := instruction.(type) {
		case *ssa.DebugRef:
		case ssa.CallInstruction:
			if use.Common().Value != value {
				return "a private channel carrier captured by a closure passed to another call"
			}
			if reason := channelCallSeal(use); reason != "" {
				return reason
			}
		case *ssa.ChangeType:
			if reason := channelClosureSeal(use, seen); reason != "" {
				return reason
			}
		default:
			return "a private channel carrier captured by an escaping closure"
		}
	}
	return ""
}

// channelUse classifies one operand use of a channel value. It returns the send
// this use performs, or the reason the population is not closed. Anything the
// analysis cannot follow (a foreign callee, a return, an interface conversion, a
// container element, a tuple member) is a boundary: the value may leave the
// enumerated source, so no send set is complete.
func channelUse(instruction ssa.Instruction, operand ssa.Value) (*channelSend, string) {
	switch held := operand.(type) {
	case *ssa.Parameter:
		// A caller supplies this channel, and the calls this analysis cannot
		// see are exactly the ones that could supply one it never enumerated.
		return nil, "a channel value held by a parameter of " + held.Parent().String()
	case *ssa.Const:
		return nil, "a constant channel value"
	case *ssa.Global:
		if object := held.Object(); object != nil && object.Exported() {
			return nil, "a channel in exported package variable " + object.Name()
		}
	}
	switch use := instruction.(type) {
	case *ssa.DebugRef, *ssa.Jump:
		return nil, ""
	case *ssa.Store:
		if operand == use.Addr {
			// Storing through the address moves no channel; the location's own
			// value and its container are classified where they are used.
			return nil, ""
		}
		return nil, channelStoreSeal(use.Addr)
	case *ssa.Send:
		if operand == use.Chan {
			return &channelSend{value: use.X, at: instruction}, ""
		}
		return nil, "a channel carried inside a sent element"
	case *ssa.Select:
		for _, state := range use.States {
			if state.Chan != operand {
				continue
			}
			// Producer values were enumerated per state, not per operand.
			return nil, ""
		}
		return nil, "a channel carried inside a selected element"
	case *ssa.MakeClosure:
		return nil, channelClosureSeal(use, map[ssa.Value]bool{})
	case *ssa.UnOp, *ssa.FieldAddr, *ssa.Field, *ssa.Phi, *ssa.ChangeType, *ssa.Convert, *ssa.BinOp, *ssa.If:
		// A load or receive, a field projection, a moved representation, a
		// capture bound into loaded source, or a nil test.
		return nil, ""
	case *ssa.Call:
		return nil, channelCallSeal(use)
	case *ssa.Go:
		return nil, channelCallSeal(use)
	case *ssa.Defer:
		return nil, channelCallSeal(use)
	case *ssa.Return:
		return nil, "a channel returned from " + use.Parent().String()
	case *ssa.MakeInterface, *ssa.ChangeInterface, *ssa.TypeAssert:
		return nil, "a channel value converted to or from an interface"
	case *ssa.Lookup, *ssa.Index, *ssa.IndexAddr, *ssa.Slice, *ssa.Range, *ssa.Next, *ssa.MapUpdate:
		return nil, "a channel stored in or read from a container element"
	case *ssa.Extract:
		return nil, "a channel value taken from a tuple"
	case *ssa.Panic:
		return nil, "a channel value passed to panic"
	default:
		return nil, fmt.Sprintf("a channel value used by an unmodelled instruction %T", instruction)
	}
}

// channelCallSeal accepts a call only when its callee's body is loaded source,
// so every use that callee makes of the channel is enumerated too. A builtin
// (close, len, cap), an external function or an unresolved target can hold,
// close or send on the channel outside the population, so it is a boundary.
func channelCallSeal(call ssa.CallInstruction) string {
	callee := call.Common().StaticCallee()
	if callee == nil || callee.Blocks == nil {
		return "a channel passed to a call whose body is not loaded source"
	}
	return ""
}

// channelStoreSeal accepts the memory a channel is stored into: a local cell, a
// field, or a package variable the program can name. The cell, field and
// container values are themselves channel-exposing values whose every use is
// classified, so an escape of the location is judged where it happens. An
// exported package variable is reachable by name from outside the program, and
// any other address (a container element, a caller-supplied pointer) has an
// owner this analysis cannot enumerate.
func channelStoreSeal(addr ssa.Value) string {
	switch target := addr.(type) {
	case *ssa.Alloc:
		return ""
	case *ssa.FieldAddr:
		root := target.X
		for {
			nested, ok := root.(*ssa.FieldAddr)
			if !ok {
				break
			}
			root = nested.X
		}
		if global, ok := root.(*ssa.Global); ok {
			return channelStoreSeal(global)
		}
		return ""
	case *ssa.Global:
		if object := target.Object(); object != nil && object.Exported() {
			return "a channel stored in exported package variable " + object.Name()
		}
		return ""
	default:
		return fmt.Sprintf("a channel stored through an unmodelled address %T", addr)
	}
}

// channelElement is the element type of a channel, or nil.
func channelElement(t types.Type) types.Type {
	if channel, ok := types.Unalias(t).Underlying().(*types.Chan); ok {
		return channel.Elem()
	}
	return nil
}

// channelExposure decides whether a value of a type can carry a channel of the
// family element type to code that can name it: directly, through an exported or
// promoted field, or through a container element. An unexported field is sealed
// - only source that can name that field can hold or replace the channel, and
// every such operation in the loaded program is enumerated - so a value of a
// type whose only route to the channel crosses one does not expose it in the
// nameability mode. Containment mode follows those same private fields so an
// opaque escape of their carrier is checked independently of field nameability.
type channelExposure struct {
	elem          types.Type
	privateFields bool // containment also follows private fields; nameability does not
	memo          map[types.Type]bool
	active        map[types.Type]bool
}

func (e *channelExposure) exposes(t types.Type) bool {
	clear(e.active)
	exposed, _ := e.walk(t)
	// The root traversal is graph reachability: each type's outgoing edges
	// need inspection once, even when the graph has cycles or shared subgraphs.
	// A negative completed root proves every visited type negative. On a
	// positive root only that root is cached; intermediate nodes can still
	// reach the target through an ancestor not yet explored at their visit.
	if !exposed {
		for visited := range e.active {
			e.memo[visited] = false
		}
	}
	e.memo[types.Unalias(t)] = exposed
	clear(e.active)
	return exposed
}

// walk traverses each type once for the current root. Its second result records
// a repeated node; only completed root results are memoized by exposes.
func (e *channelExposure) walk(t types.Type) (exposed, tainted bool) {
	if t == nil {
		return false, false
	}
	t = types.Unalias(t)
	if known, done := e.memo[t]; done {
		return known, false
	}
	if e.active[t] {
		return false, true
	}
	e.active[t] = true
	switch underlying := t.Underlying().(type) {
	case *types.Chan:
		if types.Identical(underlying.Elem(), e.elem) {
			return true, false
		}
		return e.walk(underlying.Elem())
	case *types.Pointer:
		return e.walk(underlying.Elem())
	case *types.Slice:
		return e.walk(underlying.Elem())
	case *types.Array:
		return e.walk(underlying.Elem())
	case *types.Map:
		key, keyCycle := e.walk(underlying.Key())
		value, valueCycle := e.walk(underlying.Elem())
		return key || value, keyCycle || valueCycle
	case *types.Tuple:
		for i := 0; i < underlying.Len(); i++ {
			field, cycle := e.walk(underlying.At(i).Type())
			tainted = tainted || cycle
			if field {
				return true, tainted
			}
		}
		return false, tainted
	case *types.Struct:
		for i := 0; i < underlying.NumFields(); i++ {
			field := underlying.Field(i)
			// A promoted field of an embedded type is nameable through the
			// outer type even when the embedded field itself is unexported.
			if !e.privateFields && !field.Exported() && !field.Anonymous() {
				continue
			}
			carried, cycle := e.walk(field.Type())
			tainted = tainted || cycle
			if carried {
				return true, tainted
			}
		}
		return false, tainted
	}
	// An interface names no concrete type; a conversion into one is the point
	// where its value is judged, and that conversion is classified as a use.
	return false, false
}

// projectSelectReceive projects a field of the value the compiler extracted
// from a select receive case. Both facts must hold at this use: the selected
// receive state dominates it, and every channel of that exact element type in
// the loaded program is accounted for, with every enumerated send projected.
func (f *nilFlow) projectSelectReceive(extract *ssa.Extract, selection *ssa.Select, path []int, use ssa.Instruction, project func(ssa.Value, []int, ssa.Instruction), unknown func(string)) {
	name := "value slot " + strconv.Itoa(extract.Index) + " of " + selection.String()
	if extract.Index < 2 {
		unknown("unknown " + name)
		return
	}
	receive := extract.Index - 2
	state, element := -1, types.Type(nil)
	for index, candidate := range selection.States {
		if candidate.Dir != types.RecvOnly {
			continue
		}
		if receive == 0 {
			state, element = index, channelElement(candidate.Chan.Type())
			break
		}
		receive--
	}
	if state < 0 || element == nil || !types.Identical(element, extract.Type()) {
		unknown("unknown " + name)
		return
	}
	if !selectedCaseDominates(selection, extract, state, use) {
		unknown("select receive of " + element.String() + " may not be the chosen case at this use")
		return
	}
	family := f.p.channelFamilyOf(element)
	if family.unknown != "" {
		unknown("unproved channel population of " + element.String() + ": " + family.unknown)
		return
	}
	if len(family.sends) == 0 {
		unknown("no enumerated send populates a channel of " + element.String())
		return
	}
	for _, send := range family.sends {
		project(send.value, path, send.at)
	}
	f.discharge(fmt.Sprintf("proof: %s selected receive case with a closed channel population of %d creates and %d sends and no close", element.String(), family.creates, len(family.sends)))
}

// selectedCaseDominates reports that every path reaching use passes through the
// block that runs only when the select chose state, so the receive value slot
// this use reads is that state's element and never an unchosen zero. Dominance
// is a property of the compiled control flow, not of source syntax, and a use in
// another function or before the case cannot be attributed to the case at all.
func selectedCaseDominates(selection *ssa.Select, extract *ssa.Extract, state int, use ssa.Instruction) bool {
	if use == nil || use.Block() == nil || extract.Parent() != use.Parent() {
		return false
	}
	chosen := selectCaseBlock(selection, state)
	if chosen == nil || !chosen.Dominates(extract.Block()) {
		return false
	}
	return chosen.Dominates(use.Block())
}

// selectCaseBlock returns the block the program enters only when the select's
// own index equals state: the equality successor of the comparison against the
// index component of that same select.
func selectCaseBlock(selection *ssa.Select, state int) *ssa.BasicBlock {
	function := selection.Parent()
	if function == nil {
		return nil
	}
	for _, block := range function.Blocks {
		if len(block.Instrs) == 0 || len(block.Succs) != 2 {
			continue
		}
		branch, ok := block.Instrs[len(block.Instrs)-1].(*ssa.If)
		if !ok {
			continue
		}
		condition, ok := branch.Cond.(*ssa.BinOp)
		if !ok {
			continue
		}
		index, constant := condition.X, condition.Y
		if !selectIndexIs(index, selection) {
			index, constant = constant, index
		}
		if !selectIndexIs(index, selection) {
			continue
		}
		value, ok := constant.(*ssa.Const)
		if !ok || value.Value == nil {
			continue
		}
		number, err := strconv.ParseInt(value.Value.ExactString(), 10, 64)
		if err != nil || number != int64(state) {
			continue
		}
		switch condition.Op {
		case token.EQL:
			return block.Succs[0]
		case token.NEQ:
			return block.Succs[1]
		}
	}
	return nil
}

// selectIndexIs reports that value is the index component of this exact select.
func selectIndexIs(value ssa.Value, selection *ssa.Select) bool {
	index, ok := value.(*ssa.Extract)
	return ok && index.Index == 0 && index.Tuple == selection
}
