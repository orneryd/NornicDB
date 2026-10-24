package storagecensus

import (
	"go/token"
	"go/types"
	"sort"

	"golang.org/x/tools/go/ssa"
)

// visibilityPrimitives decide whether a stored entity is hidden: decay and
// manual suppression. With the stored reads (the methods of
// storage.StoredEntityReader, which see hidden entities), they are the
// sources of a hidden-policy decision.
var visibilityPrimitives = map[string]bool{
	"pkg/storage.BadgerEngine.hidesNode":          true,
	"pkg/storage.BadgerEngine.hidesEdge":          true,
	"pkg/storage.BadgerEngine.manuallySuppressed": true,
}

// Decision is a branch on hidden state (or on a read that sees hidden
// entities) that decides whether ordered state is written: a hidden-ID
// disposition, a suppression change, an accounting choice.
type Decision struct {
	Func string `json:"func"`
	Pos  string `json:"pos"`
	// Gates is the write the branch decides: the instruction in Func and
	// the path from it to the Badger write.
	Gates string `json:"gates"`
}

type decider struct {
	a       *analysis
	stored  map[string]bool // StoredEntityReader method names
	reader  *types.Interface
	tainted map[*ssa.Function]bool
}

func (a *analysis) decisions() []Decision {
	d := &decider{a: a, stored: map[string]bool{}, tainted: map[*ssa.Function]bool{}}
	if pkg := a.p.ssa.ImportedPackage(Module + "pkg/storage"); pkg != nil {
		if tn, ok := pkg.Pkg.Scope().Lookup("StoredEntityReader").(*types.TypeName); ok {
			if it, ok := tn.Type().Underlying().(*types.Interface); ok {
				d.reader = it
				for i := 0; i < it.NumMethods(); i++ {
					d.stored[it.Method(i).Name()] = true
				}
			}
		}
	}
	// Returns that depend on a source make a function a source, to a fixed
	// point.
	for changed := true; changed; {
		changed = false
		for _, fn := range a.p.funcs {
			if d.tainted[fn] {
				continue
			}
			t := d.taint(fn)
			for _, b := range fn.Blocks {
				if ret, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return); ok {
					for _, v := range ret.Results {
						if t[v] {
							d.tainted[fn] = true
							changed = true
						}
					}
				}
			}
		}
	}
	var out []Decision
	for _, fn := range a.p.funcs {
		t := d.taint(fn)
		for _, b := range fn.Blocks {
			iff, ok := b.Instrs[len(b.Instrs)-1].(*ssa.If)
			if !ok || !t[iff.Cond] || d.propagatesError(iff) {
				continue
			}
			if loopBound(b) {
				continue // iterating over what a read returned decides nothing
			}
			if gate := d.gatesWrite(fn, b); gate != nil {
				out = append(out, Decision{Func: a.p.funcName(fn), Pos: a.p.pos(iff.Cond.Pos()), Gates: a.bodyPath(fn, gate)})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Func != out[j].Func {
			return out[i].Func < out[j].Func
		}
		return out[i].Pos < out[j].Pos
	})
	return out
}

// source reports whether a call returns hidden state or a read that sees
// hidden entities.
func (d *decider) source(c *ssa.CallCommon) bool {
	if c.IsInvoke() {
		return d.stored[c.Method.Name()] && d.reader != nil && types.Implements(c.Value.Type(), d.reader)
	}
	callee := c.StaticCallee()
	if callee == nil {
		return false
	}
	name := d.a.p.funcName(callee)
	if visibilityPrimitives[name] || d.tainted[callee] {
		return true
	}
	if recv := callee.Signature.Recv(); recv != nil && d.stored[callee.Name()] && d.reader != nil {
		return types.Implements(recv.Type(), d.reader)
	}
	return false
}

// taint is the values of fn that depend on a source.
func (d *decider) taint(fn *ssa.Function) map[ssa.Value]bool {
	t := map[ssa.Value]bool{}
	var work []ssa.Value
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			if call, ok := ins.(*ssa.Call); ok && d.source(call.Common()) {
				t[call] = true
				work = append(work, call)
			}
		}
	}
	for len(work) > 0 {
		v := work[len(work)-1]
		work = work[:len(work)-1]
		refs := v.Referrers()
		if refs == nil {
			continue
		}
		for _, ref := range *refs {
			var out ssa.Value
			switch r := ref.(type) {
			case *ssa.Extract, *ssa.BinOp, *ssa.UnOp, *ssa.Phi, *ssa.Field, *ssa.FieldAddr, *ssa.Convert,
				*ssa.ChangeType, *ssa.MakeInterface, *ssa.TypeAssert, *ssa.Slice, *ssa.Index, *ssa.IndexAddr, *ssa.Lookup:
				out = r.(ssa.Value)
			}
			if out != nil && !t[out] {
				t[out] = true
				work = append(work, out)
			}
		}
	}
	return t
}

// writes reports whether fn reaches a write of an entity body or of
// suppression state: the writes a hidden-ID disposition decides.
func (d *decider) writes(fn *ssa.Function) bool {
	return d.a.bodyWriters[fn]
}

func (d *decider) instrWrites(fn *ssa.Function, ins ssa.Instruction) bool {
	return d.a.bodyInstr(fn, ins)
}

// gatesWrite returns the instruction through which the branch at the end of
// b decides a write: one successor reaches it, within the iteration, and the
// other does not, and it leads to a write of an entity body or of
// suppression state. It returns nil when the branch decides none.
func (d *decider) gatesWrite(fn *ssa.Function, b *ssa.BasicBlock) ssa.Instruction {
	r0, r1 := reachable(b.Succs[0]), reachable(b.Succs[1])
	for _, blk := range fn.Blocks {
		// A write both successors reach is not decided by the branch.
		if r0[blk] == r1[blk] {
			continue
		}
		for _, ins := range blk.Instrs {
			if d.instrWrites(fn, ins) {
				return ins
			}
		}
	}
	return nil
}

// loopBound reports whether b is a loop header: the branch that ends it
// bounds an iteration (over what a read returned, say), and decides no
// write of its own.
func loopBound(b *ssa.BasicBlock) bool {
	for _, pred := range b.Preds {
		if b.Dominates(pred) {
			return true
		}
	}
	return false
}

// reachable is the set of blocks reachable from start, start included,
// within one iteration: loop back edges (to a block that dominates the edge's
// source) are not followed, so a decision inside a loop body still decides
// that iteration's write.
func reachable(start *ssa.BasicBlock) map[*ssa.BasicBlock]bool {
	seen := map[*ssa.BasicBlock]bool{}
	stack := []*ssa.BasicBlock{start}
	for len(stack) > 0 {
		b := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[b] {
			continue
		}
		seen[b] = true
		for _, s := range b.Succs {
			if !s.Dominates(b) {
				stack = append(stack, s)
			}
		}
	}
	return seen
}

// propagatesError reports a branch on a source's error that only returns
// that error: the operation fails with the read, it decides nothing.
func (d *decider) propagatesError(iff *ssa.If) bool {
	cmp, ok := iff.Cond.(*ssa.BinOp)
	if !ok || (cmp.Op != token.NEQ && cmp.Op != token.EQL) {
		return false
	}
	errv := cmp.X
	if c, ok := cmp.X.(*ssa.Const); ok && c.IsNil() {
		errv = cmp.Y
	} else if c, ok := cmp.Y.(*ssa.Const); !ok || !c.IsNil() {
		return false
	}
	switch errv.(type) {
	case *ssa.Extract, *ssa.Call: // (v, err) or a lone error result
	default:
		return false
	}
	branch := iff.Block().Succs[0]
	if cmp.Op == token.EQL {
		branch = iff.Block().Succs[1]
	}
	ret, ok := branch.Instrs[len(branch.Instrs)-1].(*ssa.Return)
	if !ok {
		return false
	}
	for _, v := range ret.Results {
		if v == errv {
			return true
		}
	}
	return false
}
