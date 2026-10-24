package storagecensus

import (
	"fmt"
	"go/types"
	"os"
	"sort"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// A write is one Badger write the census found, with the key space its key
// names (or why it names none) in one calling context.
type write struct {
	Space  string `json:"space"`
	Class  string `json:"class"`
	Origin string `json:"origin"` // the Badger call
	Func   string `json:"func"`   // the function that makes the Badger call
	Where  string `json:"where"`  // where the key was resolved, when not at the call
	Why    string `json:"why,omitempty"`
}

// req is a write of ordered state that a function makes, directly or
// through what it calls, and that no acquisition in it covers: every caller
// must cover it. chain is the path from the function to the Badger call.
type req struct {
	space, class string
	origin       string
	chain        []string
	// later marks a write a closure makes after its maker returned (one
	// that is stored, sent or started): no acquisition of the maker or its
	// callers covers it.
	later bool
}

func (r req) id() string { return r.space + "|" + r.origin }

// pend is a write whose key a function receives (a parameter, or a variable
// a closure captures): its callers resolve it. covered records that an
// acquisition on the way already covers it.
type pend struct {
	shape   keyShape
	origin  string
	chain   []string
	covered cover
	later   bool // as in req
}

func (p pend) id() string { return fmt.Sprintf("%v|%s|%d|%t", p.shape, p.origin, p.covered, p.later) }

type summary struct {
	reqs  map[string]req
	pends map[string]pend
}

// analysis is one census run over a program.
type analysis struct {
	p           *program
	k           *keys
	sums        map[*ssa.Function]*summary
	writes      map[string]write
	acq         *acquirer
	bodyMarks   map[ssa.Instruction]bool
	bodyWriters map[*ssa.Function]bool
	bodyVia     map[*ssa.Function]ssa.Instruction // why a function is in bodyWriters
	paramCovers map[*ssa.Parameter]cover          // see paramCover
	syncMemo    map[*ssa.Parameter]int
	syncClosure map[*ssa.MakeClosure]bool
	siteCallees map[ssa.CallInstruction][]*ssa.Function
	callers     map[*ssa.Function]map[*ssa.Function]bool
}

// badgerWrite reports whether a call writes Badger and returns the
// argument that holds its key (nil with whole set for writes of the whole
// store), or ok false.
func badgerWrite(c *ssa.CallCommon) (key ssa.Value, whole string, ok bool) {
	var fn *types.Func
	if c.IsInvoke() {
		fn = c.Method
	} else if callee := c.StaticCallee(); callee != nil {
		fn, _ = callee.Object().(*types.Func)
	}
	if fn == nil || fn.Pkg() == nil || !strings.HasPrefix(fn.Pkg().Path(), "github.com/dgraph-io/badger/") {
		return nil, "", false
	}
	recv := ""
	if sig, ok := fn.Type().(*types.Signature); ok && sig.Recv() != nil {
		recv = types.TypeString(sig.Recv().Type(), func(*types.Package) string { return "" })
	}
	args := c.Args
	if !c.IsInvoke() && len(args) > 0 && recv != "" {
		args = args[1:]
	}
	switch {
	case (recv == "*Txn" || recv == "*WriteBatch") && (fn.Name() == "Set" || fn.Name() == "Delete") && len(args) > 0:
		return args[0], "", true
	case (recv == "*Txn" || recv == "*WriteBatch") && fn.Name() == "SetEntry" && len(args) > 0:
		return entryKey(args[0]), "", true
	case recv == "*DB" && (fn.Name() == "DropAll" || fn.Name() == "Load" || fn.Name() == "DropPrefix"):
		return nil, "badger.DB." + fn.Name(), true
	}
	return nil, "", false
}

// entryKey is the key of a badger.Entry value: NewEntry's first argument,
// through With* methods that return their receiver.
func entryKey(v ssa.Value) ssa.Value {
	for i := 0; i < 8; i++ {
		call, ok := v.(*ssa.Call)
		if !ok {
			return v
		}
		callee := call.Common().StaticCallee()
		if callee == nil || len(call.Common().Args) == 0 {
			return v
		}
		if callee.Name() == "NewEntry" {
			return call.Common().Args[0]
		}
		if strings.HasPrefix(callee.Name(), "With") {
			v = call.Common().Args[0]
			continue
		}
		return v
	}
	return v
}

// callees are the functions a call site may call, its function values
// excepted: a closure or function value is accounted where it is made.
func (a *analysis) callees(fn *ssa.Function, site ssa.CallInstruction) []*ssa.Function {
	c := site.Common()
	if callee := c.StaticCallee(); callee != nil {
		if _, isClosure := c.Value.(*ssa.MakeClosure); isClosure {
			return nil // an immediately called closure is accounted at MakeClosure
		}
		return []*ssa.Function{callee}
	}
	if !c.IsInvoke() {
		return nil
	}
	if a.siteCallees == nil {
		a.siteCallees = map[ssa.CallInstruction][]*ssa.Function{}
		for _, node := range a.p.cg.Nodes {
			for _, e := range node.Out {
				if e.Site != nil && e.Callee.Func != nil && e.Site.Common().IsInvoke() {
					a.siteCallees[e.Site] = append(a.siteCallees[e.Site], e.Callee.Func)
				}
			}
		}
		for s, out := range a.siteCallees {
			sort.Slice(out, func(i, j int) bool { return a.p.funcName(out[i]) < a.p.funcName(out[j]) })
			a.siteCallees[s] = out
		}
	}
	return a.siteCallees[site]
}

// traceOrigin, from NORNICDB_CENSUS_TRACE (a Badger call's file:line), prints
// every resolution of that write: for diagnosing the census, not part of it.
var traceOrigin = os.Getenv("NORNICDB_CENSUS_TRACE")

func (a *analysis) resolve(shape keyShape, origin string, chain []string, covered cover, later bool, at *ssa.Function, where ssa.Instruction, s *summary) {
	for _, one := range expand(shape) {
		a.resolveOne(one, origin, chain, covered, later, at, where, s)
	}
}

func (a *analysis) resolveOne(shape keyShape, origin string, chain []string, covered cover, later bool, at *ssa.Function, where ssa.Instruction, s *summary) {
	if shape.nilKey && len(shape.bytes) == 0 {
		return // a nil key is never written
	}
	if shape.whole != "" {
		w := write{Space: shape.whole, Class: "whole-store", Origin: origin, Func: chainFunc(chain), Where: a.p.funcName(at) + " " + a.p.pos(where.Pos())}
		a.writes[w.Origin+"|"+w.Where+"|"+w.Space] = w
		if !covered.has(shape.whole) {
			s.add(req{space: shape.whole, class: "whole-store", origin: origin, chain: chain, later: later})
		}
		return
	}
	name, param, why := a.k.space(shape)
	if traceOrigin != "" && origin == traceOrigin {
		fmt.Printf("CENSUS-TRACE at %s %s shape=%+v name=%q param=%d why=%q covered=%d later=%t chain=%v\n",
			a.p.funcName(at), a.p.pos(where.Pos()), shape, name, param, why, covered, later, chain)
	}
	if shape.param >= 0 || shape.free >= 0 || (name == "" && (param >= 0 || shape.capturedByte())) {
		p := pend{shape: shape, origin: origin, chain: chain, covered: covered, later: later}
		s.pends[p.id()] = p
		return
	}
	class := spaceClasses[name]
	if name != "" && class == "" {
		why = "key space " + name + " has no class (spaceClasses)"
	}
	w := write{Space: name, Class: class, Origin: origin, Func: chainFunc(chain), Where: a.p.funcName(at) + " " + a.p.pos(where.Pos()), Why: why}
	if name == "" || class == "" {
		w.Space, w.Class = "", "unclassified"
	}
	if bodyClasses[class] {
		if a.bodyMarks == nil {
			a.bodyMarks = map[ssa.Instruction]bool{}
		}
		a.bodyMarks[where] = true
	}
	a.writes[w.Origin+"|"+w.Where+"|"+w.Space] = w
	if (orderedClasses[class] || w.Class == "unclassified") && !covered.has(w.Space) {
		r := req{space: w.Space, class: w.Class, origin: origin, chain: chain, later: later}
		if w.Class == "unclassified" {
			r.space = "?" + why
		}
		s.add(r)
	}
}

// add records r, keeping per write the shortest, then least, chain, so the
// census does not depend on the order it visits functions in.
func (s *summary) add(r req) {
	if old, ok := s.reqs[r.id()]; ok && !shorter(r.chain, old.chain) {
		return
	}
	s.reqs[r.id()] = r
}

func shorter(a, b []string) bool {
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	return strings.Join(a, "\x00") < strings.Join(b, "\x00")
}

func chainFunc(chain []string) string {
	if len(chain) == 0 {
		return ""
	}
	return chain[len(chain)-1]
}

// summarize recomputes fn's summary from its instructions and the current
// summaries of what it calls and the closures it makes.
func (a *analysis) summarize(fn *ssa.Function) *summary {
	s := &summary{reqs: map[string]req{}, pends: map[string]pend{}}
	name := a.p.funcName(fn)
	if exemptOwners[name] != "" || exemptOwners[outer(name)] != "" {
		return s
	}
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			switch ins := ins.(type) {
			case ssa.CallInstruction:
				c := ins.Common()
				if key, whole, ok := badgerWrite(c); ok {
					origin := a.p.pos(ins.Pos())
					chain := []string{name + " " + origin}
					covered := a.acq.covers(fn, ins)
					if whole != "" {
						a.resolveWhole(whole, origin, chain, covered, fn, ins, s)
						continue
					}
					for _, shape := range a.k.shapes(key, 0) {
						a.resolve(shape, origin, chain, covered, false, fn, ins, s)
					}
					continue
				}
				if _, isGo := ins.(*ssa.Go); isGo {
					continue // the started function is an entry of its own
				}
				args := c.Args
				if c.IsInvoke() {
					// An interface call's receiver is its callee's first parameter.
					args = append([]ssa.Value{c.Value}, c.Args...)
				}
				for _, callee := range a.callees(fn, ins) {
					cs := a.sums[callee]
					if cs == nil {
						continue
					}
					a.transfer(fn, ins, callee, cs, args, nil, s, 0)
				}
			case *ssa.MakeClosure:
				// The closure is accounted here, where its captured
				// variables are known (its Fn operand is no function value).
				closure := ins.Fn.(*ssa.Function)
				if cs := a.sums[closure]; cs != nil {
					if a.synchronous(ins) {
						a.transfer(fn, ins, closure, cs, nil, ins.Bindings, s, a.closureArgCover(fn, ins))
					} else {
						a.transferLater(fn, ins, closure, cs, s)
					}
				}
				continue
			}
			// A named function used as a value is accounted where it is used.
			for _, op := range ins.Operands(nil) {
				if op == nil || *op == nil {
					continue
				}
				f, ok := (*op).(*ssa.Function)
				if !ok || f.Blocks == nil {
					continue
				}
				if call, ok := ins.(ssa.CallInstruction); ok && call.Common().Value == f {
					continue // a direct call, handled above
				}
				if _, isGo := ins.(*ssa.Go); isGo {
					continue
				}
				if cs := a.sums[f]; cs != nil {
					a.transfer(fn, ins, f, cs, nil, nil, s, 0)
				}
			}
		}
	}
	return s
}

// transfer adds to s, for the instruction at in fn that runs callee, what
// callee requires: its requirements unless an acquisition covers at, and its
// pending writes with callee's parameters replaced by args (a call) or its
// captured variables by bindings (a closure).
func (a *analysis) transfer(fn *ssa.Function, at ssa.Instruction, callee *ssa.Function, cs *summary, args, bindings []ssa.Value, s *summary, extra cover) {
	covered := a.acq.covers(fn, at) | extra
	step := a.p.funcName(fn) + " " + a.p.pos(at.Pos())
	for _, r := range cs.reqs {
		if covered.has(r.space) && !r.later {
			continue
		}
		r.chain = append([]string{step}, r.chain...)
		s.add(r)
	}
	for _, p := range cs.pends {
		chain := append([]string{step}, p.chain...)
		var shapes []keyShape
		switch {
		case p.shape.param >= 0 && args != nil:
			if p.shape.param < len(args) {
				shapes = a.k.argShapes(p.shape, args[p.shape.param], 0)
			}
		case p.shape.free >= 0 && bindings != nil:
			if p.shape.free < len(bindings) {
				shapes = a.k.argShapes(p.shape, bindings[p.shape.free], 0)
			}
		case p.shape.param < 0 && p.shape.free < 0 && args != nil:
			shapes = a.k.substituteBytes(p.shape, args)
		case p.shape.param < 0 && p.shape.free < 0 && bindings != nil:
			shapes = []keyShape{a.k.closureBytes(p.shape, callee, bindings)}
		case p.shape.param >= 0 && bindings != nil:
			// A closure's parameter: the values its calls pass.
			shapes = a.closureParamShapes(p.shape, callee, fn)
		default:
			shapes = []keyShape{unknownShape("key reaches " + callee.String() + " as a value the census cannot follow")}
		}
		cov := p.covered | covered
		if p.later {
			cov = p.covered
		}
		for _, shape := range shapes {
			a.resolve(shape, p.origin, chain, cov, p.later, fn, at, s)
		}
	}
}

// closureParamShapes resolves a shape that is a parameter of closure over
// the arguments every call of the closure passes (the call graph), each
// resolved up to maker, the function that makes the closure.
func (a *analysis) closureParamShapes(s keyShape, closure, maker *ssa.Function) []keyShape {
	if s.param >= len(closure.Params) {
		return []keyShape{unknownShape("variadic closure parameter")}
	}
	args, why := a.k.paramArgs(closure.Params[s.param])
	if why != "" {
		return []keyShape{unknownShape(why)}
	}
	if len(args) == 0 {
		return []keyShape{unknownShape("key is a parameter of " + closure.String() + ", which only code outside the module calls")}
	}
	var out []keyShape
	for _, arg := range args {
		for _, r := range a.k.argShapes(s, arg, 0) {
			out = append(out, a.k.contextFree(r, parentOf(arg), maker, 0)...)
		}
	}
	return out
}

// transferLater adds to s, for a closure fn makes that runs after fn
// returned, the writes whose keys the closure captures: their spaces are
// known only here, from the values it captures, and as writes that run later
// no acquisition of fn or its callers covers them. The closure's other
// writes are checked where it runs, as an entry point of its own.
func (a *analysis) transferLater(fn *ssa.Function, mc *ssa.MakeClosure, closure *ssa.Function, cs *summary, s *summary) {
	step := a.p.funcName(fn) + " " + a.p.pos(mc.Pos())
	for _, p := range cs.pends {
		chain := append([]string{step}, p.chain...)
		var shapes []keyShape
		switch {
		case p.shape.free >= 0 && p.shape.free < len(mc.Bindings):
			shapes = a.k.argShapes(p.shape, mc.Bindings[p.shape.free], 0)
		case p.shape.param < 0 && p.shape.free < 0:
			shapes = []keyShape{a.k.closureBytes(p.shape, closure, mc.Bindings)}
		case p.shape.param >= 0:
			shapes = a.closureParamShapes(p.shape, closure, fn)
		default:
			shapes = []keyShape{unknownShape("key reaches " + closure.String() + ", a closure that runs later, as its parameter")}
		}
		for _, shape := range shapes {
			a.resolve(shape, p.origin, chain, p.covered, true, fn, mc, s)
		}
	}
}

func (a *analysis) resolveWhole(whole, origin string, chain []string, covered cover, fn *ssa.Function, at ssa.Instruction, s *summary) {
	w := write{Space: whole, Class: "whole-store", Origin: origin, Func: chainFunc(chain), Where: a.p.funcName(fn) + " " + a.p.pos(at.Pos())}
	a.writes[w.Origin+"|"+w.Where+"|"+w.Space] = w
	if !covered.has(whole) {
		s.add(req{space: whole, class: "whole-store", origin: origin, chain: chain})
	}
}

// substituteBytes replaces parameter bytes of a shape by the call's
// arguments.
func (k *keys) substituteBytes(s keyShape, args []ssa.Value) []keyShape {
	out := s
	out.bytes = append([]byteSrc(nil), s.bytes...)
	for i, b := range out.bytes {
		if !b.known && b.param >= 0 && b.param < len(args) {
			out.bytes[i] = k.byteOf(args[b.param])
		}
	}
	return []keyShape{out}
}

func equalSummary(x, y *summary) bool {
	if x == nil || y == nil {
		return x == y
	}
	if len(x.reqs) != len(y.reqs) || len(x.pends) != len(y.pends) {
		return false
	}
	for id, r := range x.reqs {
		o, ok := y.reqs[id]
		if !ok || strings.Join(r.chain, "\x00") != strings.Join(o.chain, "\x00") {
			return false
		}
	}
	for id := range x.pends {
		if _, ok := y.pends[id]; !ok {
			return false
		}
	}
	return true
}

// bodyClasses are the writes a hidden-ID disposition decides.
var bodyClasses = map[string]bool{"entity-body": true, "suppression": true}

// bodyInstr reports whether ins, in fn, leads to a write of an entity body
// or of suppression state: the key of one resolves there, or it calls (or
// makes) a function that does.
func (a *analysis) bodyInstr(fn *ssa.Function, ins ssa.Instruction) bool {
	if a.bodyMarks[ins] {
		return true
	}
	switch ins := ins.(type) {
	case ssa.CallInstruction:
		for _, callee := range a.callees(fn, ins) {
			if a.bodyWriters[callee] {
				return true
			}
		}
	case *ssa.MakeClosure:
		return a.bodyWriters[ins.Fn.(*ssa.Function)]
	}
	return false
}

// markBodyWriters computes, to a fixed point, the functions that lead to a
// write of an entity body or of suppression state.
func (a *analysis) markBodyWriters() {
	a.bodyWriters = map[*ssa.Function]bool{}
	a.bodyVia = map[*ssa.Function]ssa.Instruction{}
	for changed := true; changed; {
		changed = false
		for _, fn := range a.p.funcs {
			if a.bodyWriters[fn] {
				continue
			}
			for _, b := range fn.Blocks {
				for _, ins := range b.Instrs {
					if !a.bodyWriters[fn] && a.bodyInstr(fn, ins) {
						a.bodyWriters[fn] = true
						a.bodyVia[fn] = ins
						changed = true
					}
				}
			}
		}
	}
}

// bodyPath is the path from ins, in fn, to the write of an entity body or of
// suppression state it leads to, as markBodyWriters found it.
func (a *analysis) bodyPath(fn *ssa.Function, ins ssa.Instruction) string {
	var steps []string
	visited := map[*ssa.Function]bool{fn: true}
	for i := 0; i < 16 && ins != nil; i++ {
		steps = append(steps, a.p.funcName(fn)+" "+a.p.pos(ins.Pos()))
		if a.bodyMarks[ins] {
			break
		}
		var next *ssa.Function
		switch ins := ins.(type) {
		case ssa.CallInstruction:
			for _, callee := range a.callees(fn, ins) {
				if a.bodyWriters[callee] && !visited[callee] {
					next = callee
					break
				}
			}
		case *ssa.MakeClosure:
			next = ins.Fn.(*ssa.Function)
		}
		if next == nil {
			break
		}
		visited[next] = true
		fn, ins = next, a.bodyVia[next]
	}
	return strings.Join(steps, " -> ")
}

// fixpoint computes every function's summary.
func (a *analysis) fixpoint() {
	a.callers = map[*ssa.Function]map[*ssa.Function]bool{}
	link := func(callee, caller *ssa.Function) {
		if a.callers[callee] == nil {
			a.callers[callee] = map[*ssa.Function]bool{}
		}
		a.callers[callee][caller] = true
	}
	for _, fn := range a.p.funcs {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				if call, ok := ins.(ssa.CallInstruction); ok {
					for _, callee := range a.callees(fn, call) {
						link(callee, fn)
					}
				}
				if mc, ok := ins.(*ssa.MakeClosure); ok {
					link(mc.Fn.(*ssa.Function), fn)
				}
				for _, op := range ins.Operands(nil) {
					if op != nil && *op != nil {
						if f, ok := (*op).(*ssa.Function); ok {
							link(f, fn)
						}
					}
				}
			}
		}
	}
	queue := append([]*ssa.Function(nil), a.p.funcs...)
	queued := map[*ssa.Function]bool{}
	for _, fn := range queue {
		queued[fn] = true
	}
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		queued[fn] = false
		s := a.summarize(fn)
		if equalSummary(s, a.sums[fn]) {
			continue
		}
		a.sums[fn] = s
		for caller := range a.callers[fn] {
			if !queued[caller] {
				queued[caller] = true
				queue = append(queue, caller)
			}
		}
	}
}
