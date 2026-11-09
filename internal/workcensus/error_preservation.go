package workcensus

// Conditional error preservation is a source proof, independent of the nil
// flow's external-result conventions and reviewed provider contracts. Facts are
// scoped to one call/return; none enter the context-insensitive value memo.
import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/ssa"
)

type errorFact struct{ nonnil, zero, first bool }
type proofEdge struct{ from, to *ssa.BasicBlock }
type errorProof struct {
	flow      *nilFlow
	params    map[ssa.Value]errorFact
	cells     map[ssa.Value]bool
	preDefer  map[ssa.Value]bool
	reachable map[*ssa.BasicBlock]bool
	edges     map[proofEdge]bool
	active    map[ssa.Value]bool
	calls     map[*ssa.Function]bool
}

func newErrorProof(f *nilFlow) *errorProof {
	return &errorProof{flow: f, params: map[ssa.Value]errorFact{}, cells: map[ssa.Value]bool{}, preDefer: map[ssa.Value]bool{}, active: map[ssa.Value]bool{}, calls: map[*ssa.Function]bool{}}
}
func isError(t types.Type) bool { return types.Identical(t, types.Universe.Lookup("error").Type()) }
func integer(v ssa.Value, n int64) bool {
	c, ok := v.(*ssa.Const)
	if !ok || c.Value == nil || c.Value.Kind() != constant.Int {
		return false
	}
	i, ok := constant.Int64Val(c.Value)
	return ok && i == n
}
func (p *errorProof) fact(v ssa.Value, at ssa.Instruction) errorFact {
	if v == nil {
		return errorFact{}
	}
	if x, ok := p.params[v]; ok && (x.nonnil || x.zero) {
		return x
	}
	if at != nil && NonNilAt(v, at.Block()) {
		return errorFact{nonnil: true}
	}
	if p.active[v] {
		return errorFact{}
	}
	p.active[v] = true
	defer delete(p.active, v)
	switch v := v.(type) {
	case *ssa.Const:
		return errorFact{zero: v.IsNil()}
	case *ssa.Alloc, *ssa.MakeInterface, *ssa.MakeMap, *ssa.MakeChan, *ssa.MakeClosure, *ssa.Function, *ssa.FieldAddr, *ssa.IndexAddr:
		return errorFact{nonnil: true}
	case *ssa.ChangeInterface:
		return p.fact(v.X, at)
	case *ssa.ChangeType:
		return p.fact(v.X, at)
	case *ssa.Phi:
		any := false
		for i, a := range v.Edges {
			pred := v.Block().Preds[i]
			if p.edges != nil && !p.edges[proofEdge{pred, v.Block()}] {
				continue
			}
			any = true
			if !p.fact(a, pred.Instrs[len(pred.Instrs)-1]).nonnil {
				return errorFact{}
			}
		}
		return errorFact{nonnil: any}
	case *ssa.Call:
		return p.call(v, 0)
	case *ssa.Extract:
		if c, ok := v.Tuple.(*ssa.Call); ok {
			return p.call(c, v.Index)
		}
	case *ssa.UnOp:
		if v.Op != token.MUL {
			return errorFact{}
		}
		if p.cells[cellRoot(v.X)] {
			return errorFact{nonnil: true}
		}
		if idx, ok := v.X.(*ssa.IndexAddr); ok && integer(idx.Index, 0) {
			if p.first(idx.X, map[ssa.Value]bool{}).first {
				return errorFact{nonnil: true}
			}
		}
		if cell, ok := v.X.(*ssa.Alloc); ok && localCell(cell) {
			if len(capturedStores(cell)) > 0 && !p.preDefer[cell] {
				return errorFact{}
			}
			ds := definitionsBefore(v.Block(), instructionIndex(v), cell, nil)
			any := false
			for _, d := range ds {
				any = true
				if d.nonNilBranch {
					continue
				}
				if d.store == nil || !p.fact(d.store.Val, d.store).nonnil {
					return errorFact{}
				}
			}
			return errorFact{nonnil: any}
		}
	}
	return errorFact{}
}
func (p *errorProof) call(c *ssa.Call, index int) errorFact {
	targets := p.flow.p.CallTargets(c)
	if len(targets) == 0 {
		return errorFact{}
	}
	for _, fn := range targets {
		if p.calls[fn] {
			return errorFact{}
		}
		body := fn
		if len(body.Blocks) == 0 {
			body = p.flow.externalBody(fn)
		}
		if body == nil || len(body.Blocks) == 0 {
			return errorFact{}
		}
		child := newErrorProof(p.flow)
		for fn, b := range p.calls {
			child.calls[fn] = b
		}
		child.calls[fn] = true
		child.calls[body] = true
		for i, param := range body.Params {
			j := i - (len(body.Params) - len(c.Common().Args))
			if j < 0 || j >= len(c.Common().Args) {
				return errorFact{}
			}
			arg := c.Common().Args[j]
			if !types.AssignableTo(arg.Type(), param.Type()) {
				return errorFact{}
			}
			fact := p.fact(arg, c)
			if _, ok := param.Type().Underlying().(*types.Slice); ok {
				fact.first = p.first(arg, map[ssa.Value]bool{}).first && p.readOnly(arg, map[ssa.Value]bool{})
			}
			child.params[param] = fact
		}
		child.setReachable(body)
		found := false
		for _, b := range body.Blocks {
			if !child.reachable[b] {
				continue
			}
			ret, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return)
			if !ok {
				continue
			}
			found = true
			if index >= len(ret.Results) || !child.fact(ret.Results[index], ret).nonnil {
				return errorFact{}
			}
		}
		if !found {
			return errorFact{}
		}
	}
	for _, target := range targets {
		p.flow.discharge(fmt.Sprintf("proof: call-bound source error result %s #%d", target.String(), index))
	}
	return errorFact{nonnil: true}
}
func (p *errorProof) setReachable(fn *ssa.Function) {
	p.reachable = map[*ssa.BasicBlock]bool{}
	p.edges = map[proofEdge]bool{}
	for _, b := range fn.Blocks {
		p.reachable[b] = true
		for _, succ := range b.Succs {
			p.edges[proofEdge{b, succ}] = true
		}
	}
	for round := 0; round <= len(fn.Blocks); round++ {
		next := map[*ssa.BasicBlock]bool{}
		nextEdges := map[proofEdge]bool{}
		queue := []*ssa.BasicBlock{fn.Blocks[0]}
		if fn.Recover != nil && p.flow.recoverReachable(fn) {
			queue = append(queue, fn.Recover)
		}
		for len(queue) > 0 {
			b := queue[0]
			queue = queue[1:]
			if next[b] {
				continue
			}
			next[b] = true
			if len(b.Instrs) == 0 {
				continue
			}
			if iff, ok := b.Instrs[len(b.Instrs)-1].(*ssa.If); ok {
				if truth, known := p.condition(iff.Cond, iff); known {
					i := 1
					if truth {
						i = 0
					}
					nextEdges[proofEdge{b, b.Succs[i]}] = true
					queue = append(queue, b.Succs[i])
					continue
				}
			}
			for _, succ := range b.Succs {
				nextEdges[proofEdge{b, succ}] = true
			}
			queue = append(queue, b.Succs...)
		}
		same := len(next) == len(p.reachable) && len(nextEdges) == len(p.edges)
		if same {
			for b := range next {
				if !p.reachable[b] {
					same = false
				}
			}
		}
		if same {
			for edge := range nextEdges {
				if !p.edges[edge] {
					same = false
				}
			}
		}
		p.reachable = next
		p.edges = nextEdges
		if same {
			return
		}
	}
}
func (p *errorProof) condition(v ssa.Value, at ssa.Instruction) (bool, bool) {
	b, ok := v.(*ssa.BinOp)
	if !ok {
		return false, false
	}
	if tested, nilIndex, ok := NilComparison(v); ok {
		f := p.fact(tested, at)
		if f.nonnil {
			return nilIndex != 0, true
		}
		if f.zero {
			return nilIndex == 0, true
		}
	}
	if b.Op == token.EQL || b.Op == token.NEQ {
		var other ssa.Value
		if integer(b.X, 0) {
			other = b.Y
		} else if integer(b.Y, 0) {
			other = b.X
		}
		if other != nil {
			if c, ok := other.(*ssa.Call); ok {
				if builtin, ok := c.Common().Value.(*ssa.Builtin); ok && builtin.Name() == "len" && len(c.Common().Args) == 1 && p.first(c.Common().Args[0], map[ssa.Value]bool{}).first {
					return b.Op == token.NEQ, true
				}
			}
			if p.filteredCountGuard(b) {
				return b.Op == token.NEQ, true
			}
		}
	}
	return false, false
}

// first proves a retained nonnil element at index zero in a local append-only
// slice. Cyclic phi edges preserve the proposed invariant, but a real seed is
// required; ungrounded cycles never supply a proof.
func (p *errorProof) first(v ssa.Value, seen map[ssa.Value]bool) errorFact {
	if !p.readOnly(v, map[ssa.Value]bool{}) {
		return errorFact{}
	}
	return p.firstValue(v, seen)
}
func (p *errorProof) firstValue(v ssa.Value, seen map[ssa.Value]bool) errorFact {
	if f, ok := p.params[v]; ok {
		return errorFact{first: f.first}
	}
	if seen[v] {
		return errorFact{first: true}
	}
	seen[v] = true
	defer delete(seen, v)
	switch x := v.(type) {
	case *ssa.MakeSlice:
		return errorFact{zero: integer(x.Len, 0)}
	case *ssa.Slice:
		if x.Low != nil && !integer(x.Low, 0) || x.High != nil {
			return errorFact{}
		}
		a, ok := x.X.(*ssa.Alloc)
		if !ok {
			return p.firstValue(x.X, seen)
		}
		arr, ok := a.Type().Underlying().(*types.Pointer).Elem().Underlying().(*types.Array)
		if !ok || arr.Len() != 1 || a.Referrers() == nil {
			return errorFact{}
		}
		var store *ssa.Store
		for _, ref := range *a.Referrers() {
			switch ref := ref.(type) {
			case *ssa.IndexAddr:
				if !integer(ref.Index, 0) || ref.Referrers() == nil {
					return errorFact{}
				}
				for _, use := range *ref.Referrers() {
					s, ok := use.(*ssa.Store)
					if !ok || s.Addr != ref || store != nil {
						return errorFact{}
					}
					store = s
				}
			case *ssa.Slice:
				if ref != x {
					return errorFact{}
				}
			case *ssa.DebugRef:
			default:
				return errorFact{}
			}
		}
		if store == nil || !store.Block().Dominates(x.Block()) || store.Block() == x.Block() && instructionIndex(store) >= instructionIndex(x) {
			return errorFact{}
		}
		return errorFact{first: p.fact(store.Val, store).nonnil, nonnil: true}
	case *ssa.Call:
		b, ok := x.Common().Value.(*ssa.Builtin)
		if !ok || b.Name() != "append" || len(x.Common().Args) != 2 {
			return errorFact{}
		}
		left := p.firstValue(x.Common().Args[0], seen)
		if left.first {
			return left
		}
		if left.zero {
			return p.firstValue(x.Common().Args[1], seen)
		}
	case *ssa.Phi:
		seed := false
		any := false
		for i, a := range x.Edges {
			if p.edges != nil && !p.edges[proofEdge{x.Block().Preds[i], x.Block()}] {
				continue
			}
			any = true
			if seen[a] {
				continue
			}
			f := p.firstValue(a, seen)
			if !f.first {
				return errorFact{}
			}
			seed = seed || f.nonnil
		}
		return errorFact{first: any && seed, nonnil: seed}
	}
	return errorFact{}
}

// readOnly rejects element writes, escapes and unresolved consumers. Appending
// cannot overwrite existing elements. Other calls must have visible read-only
// parameter bodies; a builtin or function's spelling is not a contract.
func (p *errorProof) readOnly(v ssa.Value, seen map[ssa.Value]bool) bool {
	if seen[v] {
		return true
	}
	seen[v] = true
	if v.Referrers() == nil {
		return false
	}
	for _, r := range *v.Referrers() {
		switch r := r.(type) {
		case *ssa.Phi:
			if !p.readOnly(r, seen) {
				return false
			}
		case *ssa.Slice:
			if r.Low != nil && !integer(r.Low, 0) || r.High != nil || r.Max != nil {
				return false
			}
			if !p.readOnly(r, seen) {
				return false
			}
		case *ssa.IndexAddr:
			if r.Referrers() == nil {
				return false
			}
			for _, u := range *r.Referrers() {
				l, ok := u.(*ssa.UnOp)
				if !ok || l.Op != token.MUL {
					return false
				}
			}
		case *ssa.Call:
			if b, ok := r.Common().Value.(*ssa.Builtin); ok {
				if b.Name() == "len" || b.Name() == "cap" {
					continue
				}
				if b.Name() == "append" {
					if !p.readOnly(r, seen) {
						return false
					}
					continue
				}
				return false
			}
			ts := p.flow.p.CallTargets(r)
			if len(ts) == 0 {
				return false
			}
			for _, fn := range ts {
				body := fn
				if len(body.Blocks) == 0 {
					body = p.flow.externalBody(fn)
				}
				if body == nil {
					return false
				}
				for i, arg := range r.Common().Args {
					if arg != v {
						continue
					}
					j := i + len(body.Params) - len(r.Common().Args)
					if j < 0 || j >= len(body.Params) || !p.readOnly(body.Params[j], seen) {
						return false
					}
				}
			}
		case *ssa.DebugRef:
		default:
			return false
		}
	}
	return true
}

// filteredCountGuard verifies the complete leading source loop that counts
// nonnil range elements and tests zero immediately afterwards. It needs the
// exact supplied container fact; nonzero length by itself is insufficient.
func (p *errorProof) filteredCountGuard(cond *ssa.BinOp) bool {
	fn := cond.Parent()
	decl, ok := fn.Syntax().(*ast.FuncDecl)
	if !ok || decl.Body == nil || len(decl.Body.List) < 3 {
		return false
	}
	init, ok := decl.Body.List[0].(*ast.AssignStmt)
	if !ok || init.Tok != token.DEFINE || len(init.Lhs) != 1 || len(init.Rhs) != 1 {
		return false
	}
	n, ok := init.Lhs[0].(*ast.Ident)
	if !ok {
		return false
	}
	zero, ok := init.Rhs[0].(*ast.BasicLit)
	if !ok || zero.Kind != token.INT || zero.Value != "0" {
		return false
	}
	loop, ok := decl.Body.List[1].(*ast.RangeStmt)
	if !ok || loop.Tok != token.DEFINE || len(loop.Body.List) != 1 {
		return false
	}
	xs, ok := loop.X.(*ast.Ident)
	if !ok {
		return false
	}
	e, ok := loop.Value.(*ast.Ident)
	if !ok || e.Name == n.Name || xs.Name == n.Name {
		return false
	}
	if key, ok := loop.Key.(*ast.Ident); !ok || key.Name != "_" {
		return false
	}
	iff, ok := loop.Body.List[0].(*ast.IfStmt)
	if !ok || iff.Init != nil || iff.Else != nil || len(iff.Body.List) != 1 {
		return false
	}
	test, ok := iff.Cond.(*ast.BinaryExpr)
	if !ok || test.Op != token.NEQ {
		return false
	}
	a, okA := test.X.(*ast.Ident)
	b, okB := test.Y.(*ast.Ident)
	if !okA || !okB || a.Name != e.Name || b.Name != "nil" {
		return false
	}
	inc, ok := iff.Body.List[0].(*ast.IncDecStmt)
	if !ok || inc.Tok != token.INC {
		return false
	}
	id, ok := inc.X.(*ast.Ident)
	if !ok || id.Name != n.Name {
		return false
	}
	end, ok := decl.Body.List[2].(*ast.IfStmt)
	if !ok || end.Init != nil {
		return false
	}
	c, ok := end.Cond.(*ast.BinaryExpr)
	if !ok || c.OpPos != cond.Pos() {
		return false
	}
	left, ok := c.X.(*ast.Ident)
	if !ok || left.Name != n.Name {
		return false
	}
	z, ok := c.Y.(*ast.BasicLit)
	if !ok || z.Kind != token.INT || z.Value != "0" {
		return false
	}
	// Confirm the source's nil token is the actual typed nil constant, rather
	// than a shadowed identifier. Its SSA comparison position must match.
	typed := false
	for _, block := range fn.Blocks {
		for _, in := range block.Instrs {
			if op, ok := in.(*ssa.BinOp); ok && op.Pos() == test.OpPos {
				_, _, typed = NilComparison(op)
			}
		}
	}
	if !typed {
		return false
	}
	for _, param := range fn.Params {
		if param.Name() == xs.Name {
			return p.params[param].first
		}
	}
	return false
}
func cellRoot(v ssa.Value) ssa.Value {
	for {
		free, ok := v.(*ssa.FreeVar)
		if !ok {
			return v
		}
		next := freeVarBinding(free)
		if next == nil || next == v {
			return v
		}
		v = next
	}
}

// deferredErrorNonNil proves a single exact local named-result cell keeps a
// nonnil value through its registered deferred writers. It never initializes
// a zero cell, nor equates defer-registration contents with execution contents.
func (f *nilFlow) deferredErrorNonNil(load *ssa.UnOp) bool {
	if !isError(load.Type()) {
		return false
	}
	if f.errorPreservation == nil {
		f.errorPreservation = map[*ssa.UnOp]bool{}
	}
	if result, known := f.errorPreservation[load]; known {
		return result
	}
	result := f.proveDeferredErrorNonNil(load)
	f.errorPreservation[load] = result
	return result
}
func (f *nilFlow) proveDeferredErrorNonNil(load *ssa.UnOp) bool {
	if !isError(load.Type()) {
		return false
	}
	cell, ok := load.X.(*ssa.Alloc)
	if !ok || !localCell(cell) {
		return false
	}
	var rd *ssa.RunDefers
	for _, b := range load.Parent().Blocks {
		for _, ins := range b.Instrs {
			d, ok := ins.(*ssa.RunDefers)
			if ok && b.Dominates(load.Block()) && (b != load.Block() || instructionIndex(d) < instructionIndex(load)) {
				if rd != nil {
					return false
				}
				rd = d
			}
		}
	}
	if rd == nil {
		return false
	}
	// The proof ends at this actual read, not at RunDefers: any intervening
	// own store or call may invalidate the cell. Go's ordinary return sequence
	// has only direct frame loads, but require that sequence explicitly.
	if rd.Block() != load.Block() {
		return false
	}
	for _, ins := range load.Block().Instrs[instructionIndex(rd)+1 : instructionIndex(load)] {
		switch ins := ins.(type) {
		case *ssa.UnOp:
			if ins.Op != token.MUL {
				return false
			}
		case *ssa.DebugRef:
		default:
			return false
		}
	}
	if !deferredOnlyCell(cell, load.Parent(), map[ssa.Value]bool{}) {
		return false
	}
	p := newErrorProof(f)
	p.preDefer[cell] = true
	defs := definitionsBefore(rd.Block(), instructionIndex(rd), cell, nil)
	if len(defs) == 0 {
		return false
	}
	for _, d := range defs {
		if d.nonNilBranch {
			continue
		}
		if d.store == nil || !p.fact(d.store.Val, d.store).nonnil {
			return false
		}
	}
	p.cells[cell] = true
	for _, store := range capturedStores(cell) {
		if !p.fact(store.Val, store).nonnil {
			return false
		}
	}
	return true
}
func deferredOnlyCell(v ssa.Value, owner *ssa.Function, seen map[ssa.Value]bool) bool {
	if seen[v] {
		return true
	}
	seen[v] = true
	if v.Referrers() == nil {
		return false
	}
	for _, ref := range *v.Referrers() {
		cl, ok := ref.(*ssa.MakeClosure)
		if !ok {
			continue
		}
		fn, ok := cl.Fn.(*ssa.Function)
		if !ok || cl.Referrers() == nil {
			return false
		}
		for _, use := range *cl.Referrers() {
			switch use := use.(type) {
			case *ssa.Defer:
				if use.Common().Value != cl {
					return false
				}
			case *ssa.Call:
				if cl.Parent() == owner || use.Common().Value != cl {
					return false
				}
			case *ssa.DebugRef:
			default:
				return false
			}
		}
		for i, b := range cl.Bindings {
			if b == v && !deferredOnlyCell(fn.FreeVars[i], owner, seen) {
				return false
			}
		}
	}
	return true
}
