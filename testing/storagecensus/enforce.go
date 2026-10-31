package storagecensus

import (
	"go/token"
	"go/types"
	"sort"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// acquisition is a function whose successful call orders a write of stored
// entity state after every write-behind commit acknowledged before it, and
// holds that order until it is released. These are the only owners the
// census knows; nothing else becomes one by being listed or by its name.
type acquisition struct {
	reason string
	// errIndex is the result that reports failure: the hold exists only on
	// the branch where it is nil.
	errIndex int
	// releases are the functions that end the hold early when called (not
	// deferred); releaseValue is the result that is itself the release
	// function, or -1.
	releases     []string
	releaseValue int
}

var acquisitions = map[string]acquisition{
	"pkg/storage.BadgerEngine.orderAfterBufferedCommits": {
		reason:   "holds the write-order stripes of the written entities after draining their buffered commits",
		errIndex: 1, releases: []string{"pkg/storage.writeRelease[pkg/storage.bufferedOrderResources].release"}, releaseValue: -1,
	},
	"pkg/storage.BadgerEngine.orderCovering": {
		reason:   "orderAfterBufferedCommits over the written entities and every node the write reaches through stored state, taken again until it holds them all",
		errIndex: 0, releases: []string{"pkg/storage.writeRelease[pkg/storage.bufferedOrderResources].release"}, releaseValue: -1,
	},
	"pkg/storage.BadgerEngine.beginLifecycleWrite": {
		reason:   "holds the write barrier and every write-order stripe after draining the whole buffer",
		errIndex: 1, releases: []string{"pkg/storage.lifecycleWrite.finish"}, releaseValue: -1,
	},
	"pkg/storage.BadgerEngine.beginMaintenanceWrite": {
		reason:   "a lifecycle write (beginLifecycleWrite) that also fails the commit of a transaction open across it in its scope",
		errIndex: 1, releases: []string{"pkg/storage.lifecycleWrite.finish"}, releaseValue: -1,
	},
}

// lockAcquisitions are the locks that order the writes of some key spaces on
// their own: while one is held exclusively (Lock, not RLock), writes of its
// spaces are covered. The derived counts' base keys are rewritten only under
// their count lock (folds, repairs, rebuilds, the publication of a commit's
// deltas), which every count publication takes; their delta keys are
// appended by commits under the write order, one unique key per commit.
type lockAcquisition struct {
	reason string
	spaces map[string]bool
}

var lockAcquisitions = map[string]lockAcquisition{
	"pkg/storage.BadgerEngine.labelCountWriteMu": {
		reason: "orders every rewrite of the label counts' base and delta keys against count publication",
		spaces: map[string]bool{"prefixMVCCMetaLabelCount": true, "prefixMVCCMetaLabelCountDelta": true},
	},
	"pkg/storage.BadgerEngine.suppressionWriteMu": {
		reason: "orders every write of a stored manual suppression (a suppression set, ended or superseded by a lifecycle write)",
		spaces: map[string]bool{"prefixMVCCMetaManualSuppression": true},
	},
	"pkg/storage.BadgerEngine.edgeTypeCountWriteMu": {
		reason: "orders every rewrite of the relationship-type counts (scalar and positional) against count publication",
		spaces: map[string]bool{
			"prefixMVCCMetaEdgeTypeCount": true, "prefixMVCCMetaEdgeTypeCountDelta": true,
			"prefixMVCCMetaEdgeTypeStartLabelCount": true, "prefixMVCCMetaEdgeTypeEndLabelCount": true,
		},
	},
}

// constructors build an engine: from their start until they store the
// engine's write-behind buffer (BadgerEngine.writeBehind), nothing else holds
// the engine and no buffered commit exists, so their writes there are
// ordered. Each row names a function that exists.
var constructors = map[string]string{
	"pkg/storage.NewBadgerEngineWithOptions": "opens the store, runs the on-start migrations and count rebuilds, and only then creates the write-behind buffer and returns the engine",
}

// bufferStore reports whether st stores the engine's write-behind buffer.
func (q *acquirer) bufferStore(st *ssa.Store) bool {
	fa, ok := st.Addr.(*ssa.FieldAddr)
	if !ok {
		return false
	}
	name, _ := q.p.field(fa.X.Type(), fa.Field)
	return name == "pkg/storage.BadgerEngine.writeBehind"
}

// lockHelpers take (takes) or release a lock for their caller under a
// condition the census cannot see; each row says why every write that needs
// the lock runs only under that condition. A call of one counts as taking
// or releasing the lock there.
type lockHelper struct {
	lock     string
	takes    bool
	fallible bool
	reason   string
}

var lockHelpers = map[string]lockHelper{
	"pkg/storage.BadgerTransaction.acquireCommitPublicationLocked": {
		lock: "pkg/storage.BadgerEngine.edgeTypeCountWriteMu", takes: true, fallible: true,
		reason: "takes the lock whenever the transaction has positional count deltas; applyEdgeTypeLabelCountDeltasLocked writes nothing without them, and the commit's count repair runs only when that write failed",
	},
	"pkg/storage.BadgerTransaction.releaseEdgeTypeCountLockLocked": {
		lock: "pkg/storage.BadgerEngine.edgeTypeCountWriteMu", takes: false,
		reason: "releases what acquireCommitPublicationLocked took",
	},
}

// lockNames are lockAcquisitions' fields in a fixed order: lock i is the
// coverage bit 1<<(i+1).
var lockNames = func() []string {
	var out []string
	for name := range lockAcquisitions {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}()

// cover is what is held at an instruction: orderKind (the write order,
// which covers every key space) and one bit per lock acquisition.
type cover uint32

const orderKind cover = 1

// has reports whether c covers a write of space.
func (c cover) has(space string) bool {
	if c&orderKind != 0 {
		return true
	}
	for i, name := range lockNames {
		if c&(1<<(i+1)) != 0 && lockAcquisitions[name].spaces[space] {
			return true
		}
	}
	return false
}

// exemptOwners are functions whose writes of ordered state need no
// acquisition, each with the reason. Their own writes are not checked, so
// the list stays short and every row names a function that exists.
var exemptOwners = map[string]string{
	"pkg/storage.managedBadgerDB.rollbackSince":                         "undoes a large commit's own batches (every key written after its intent): when the commit fails, under the exclusive commit gate it holds; when the store is opened after a crash interrupted one (recoverInterruptedLargeCommit), before any engine, write-behind buffer or writer exists",
	"pkg/storage.BadgerEngine.compactDeletedEntries":                    "writes and drops only its own throw-away marker key (deleteCleanupMarkerKey, store metadata) so Badger compacts deleted entries; the drop holds the commit gate",
	"pkg/storage.migratePrefixToMsgpack":                                "converts the bodies of a raw Badger database (MigrateBadgerToMsgpack*: no BadgerEngine has it open, so there is no write-behind buffer or writer to order behind)",
	"pkg/storage.BadgerEngine.rebuildLabelIndex":                        "the background label-index backfill, run beside writes: the drop holds the commit gate, then each chunk reads its stored bodies and writes their entries in one conflict-checked Badger transaction, read again when a write committed over a record it read (forEachStoredRecordInChunks); every write keeps its own entries (TestIndexBackfillKeepsWritesLandingMidChunk)",
	"pkg/storage.BadgerEngine.rebuildRelationshipIndexes":               "the background between-index backfill or open-time case upgrade, using the same bounded rebuild kernel as rebuildLabelIndex",
	"pkg/storage.BadgerEngine.applyBufferedCommitsWithCountPublication": "the write-behind replay: applies acknowledged commits in their commit order, under the buffer's own order",
}

// acquirer answers which acquisitions cover an instruction.
type acquirer struct {
	p     *program
	cache map[flowKey]*flow
}

type flowKey struct {
	fn         *ssa.Function
	kind       int            // 0: the write order; i+1: lockNames[i]
	assumption flowAssumption // exact call-site/path proof
}

// flow is where in one function the write order is held: on the CFG edges
// where it starts (an acquisition's success branch, or the branch where the
// engine has no write-behind buffer, so there is nothing to order behind),
// and the instructions that end it (a release that is not deferred).
type flow struct {
	gen   map[[2]*ssa.BasicBlock]bool
	genAt map[ssa.Instruction]bool // a lock taken: held after the call
	kills map[ssa.Instruction]bool
	in    map[*ssa.BasicBlock]bool
}

func (q *acquirer) flow(fn *ssa.Function, kind int, assumption flowAssumption) *flow {
	key := flowKey{fn, kind, assumption}
	if f, ok := q.cache[key]; ok {
		return f
	}
	f := &flow{gen: map[[2]*ssa.BasicBlock]bool{}, genAt: map[ssa.Instruction]bool{}, kills: map[ssa.Instruction]bool{}}
	q.cache[key] = f
	var calls []*ssa.Call
	impossible := map[[2]*ssa.BasicBlock]bool{}
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			switch ins := ins.(type) {
			case *ssa.Call:
				calls = append(calls, ins)
			case *ssa.If:
				if yes, known := conditionKnown(ins.Cond, assumption); known {
					index := 0
					if yes {
						index = 1
					}
					impossible[[2]*ssa.BasicBlock{b, b.Succs[index]}] = true
				}
				if kind != 0 {
					continue
				}
				if succ := q.noBuffer(ins); succ != nil {
					f.gen[[2]*ssa.BasicBlock{ins.Block(), succ}] = true
				}
			}
		}
	}
	if kind != 0 {
		// A lock: Lock on the field takes it, a non-deferred Unlock ends it.
		field := lockNames[kind-1]
		for _, call := range calls {
			callee := call.Common().StaticCallee()
			if receiver, action, exclusive, fallible := q.mutexCallEffect(call); receiver != nil {
				takes := action == "lock" && exclusive
				releases := action == "unlock"
				if call.Common().IsInvoke() {
					releases = releases && call.Common().Method.Name() == "Unlock"
				} else {
					releases = releases && callee.Name() == "Unlock"
				}
				if takes || releases {
					if name := q.mutexField(receiver, assumption, 0); name == field {
						if takes {
							if fallible {
								if iff, succ := successEdge(call, 0); succ != nil {
									f.gen[[2]*ssa.BasicBlock{iff.Block(), succ}] = true
								}
							} else {
								f.genAt[call] = true
							}
						} else {
							f.kills[call] = true
						}
					}
				}
			}
			if callee := call.Common().StaticCallee(); callee != nil {
				if h, ok := lockHelpers[q.p.methodIdentity(callee)]; ok && h.lock == field {
					if h.takes {
						if h.fallible {
							if iff, succ := successEdge(call, 0); succ != nil {
								f.gen[[2]*ssa.BasicBlock{iff.Block(), succ}] = true
							}
						} else {
							f.genAt[call] = true
						}
					} else {
						f.kills[call] = true
					}
				}
			}
		}
		calls = nil
	}
	for _, call := range calls {
		callee := call.Common().StaticCallee()
		if callee == nil {
			continue
		}
		a, ok := acquisitions[q.p.funcName(callee)]
		if !ok {
			continue
		}
		if iff, succ := successEdge(call, a.errIndex); succ != nil {
			f.gen[[2]*ssa.BasicBlock{iff.Block(), succ}] = true
		}
		for _, rc := range calls {
			if c := rc.Common().StaticCallee(); c != nil {
				for _, name := range a.releases {
					if q.p.funcName(c) == name {
						f.kills[rc] = true
					}
				}
			}
			if a.releaseValue >= 0 {
				if ex, ok := rc.Common().Value.(*ssa.Extract); ok && ex.Tuple == call && ex.Index == a.releaseValue {
					f.kills[rc] = true
				}
			}
		}
	}
	// A constructor holds the order from its start until it creates the
	// write-behind buffer: nothing else holds the engine before, and no
	// buffered commit exists to order behind.
	_, constructor := constructors[q.p.funcName(fn)]
	constructor = constructor && kind == 0
	if constructor {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				if st, ok := ins.(*ssa.Store); ok && q.bufferStore(st) {
					f.kills[ins] = true
				}
			}
		}
	}
	if len(f.gen) == 0 && len(f.genAt) == 0 && !constructor {
		return f
	}
	// Must analysis: the order is held at a block's start when it is held
	// on every edge into it.
	f.in = map[*ssa.BasicBlock]bool{}
	possible := map[*ssa.BasicBlock]bool{}
	if len(fn.Blocks) != 0 {
		work := []*ssa.BasicBlock{fn.Blocks[0]}
		for len(work) > 0 {
			block := work[len(work)-1]
			work = work[:len(work)-1]
			if possible[block] {
				continue
			}
			possible[block] = true
			for _, next := range block.Succs {
				if !impossible[[2]*ssa.BasicBlock{block, next}] {
					work = append(work, next)
				}
			}
		}
	}
	for _, b := range fn.Blocks {
		f.in[b] = possible[b] && (b.Index != 0 || constructor)
	}
	if fn.Recover != nil {
		f.in[fn.Recover] = false
	}
	out := func(b *ssa.BasicBlock) bool {
		held := f.in[b]
		for _, ins := range b.Instrs {
			if f.genAt[ins] {
				held = true
			}
			if f.kills[ins] {
				held = false
			}
		}
		return held
	}
	for changed := true; changed; {
		changed = false
		for _, b := range fn.Blocks {
			if b.Index == 0 || b == fn.Recover || !possible[b] {
				continue
			}
			held := len(b.Preds) > 0
			for _, p := range b.Preds {
				if !possible[p] || impossible[[2]*ssa.BasicBlock{p, b}] {
					continue
				}
				if !f.gen[[2]*ssa.BasicBlock{p, b}] && !out(p) {
					held = false
					break
				}
			}
			if held != f.in[b] {
				f.in[b] = held
				changed = true
			}
		}
	}
	return f
}

// noBuffer returns the successor of an If that runs only when the engine
// has no write-behind buffer: a test of the field BadgerEngine.writeBehind
// against nil, directly, negated, or as the result of a function that
// returns that test (writeBehindEnabled).
func (q *acquirer) noBuffer(iff *ssa.If) *ssa.BasicBlock {
	present, ok := q.bufferTest(iff.Cond, 0)
	if !ok {
		return nil
	}
	if present {
		return iff.Block().Succs[1]
	}
	return iff.Block().Succs[0]
}

// bufferTest reports whether v is a test of the engine's write-behind
// buffer, and whether it is true when the buffer is present.
func (q *acquirer) bufferTest(v ssa.Value, depth int) (present, ok bool) {
	if depth > 4 {
		return false, false
	}
	switch v := v.(type) {
	case *ssa.UnOp:
		if v.Op == token.NOT {
			present, ok := q.bufferTest(v.X, depth+1)
			return !present, ok
		}
	case *ssa.Call:
		callee := v.Common().StaticCallee()
		if callee == nil || !inModule(callee) || len(callee.Blocks) != 1 {
			return false, false
		}
		ret, isRet := callee.Blocks[0].Instrs[len(callee.Blocks[0].Instrs)-1].(*ssa.Return)
		if !isRet || len(ret.Results) != 1 {
			return false, false
		}
		return q.bufferTest(ret.Results[0], depth+1)
	case *ssa.BinOp:
		if q.nilTest(v) {
			return v.Op == token.NEQ, true
		}
	}
	return false, false
}

// nilTest reports whether cmp compares BadgerEngine.writeBehind with nil.
func (q *acquirer) nilTest(cmp *ssa.BinOp) bool {
	if cmp.Op != token.NEQ && cmp.Op != token.EQL {
		return false
	}
	isField := func(v ssa.Value) bool {
		load, ok := v.(*ssa.UnOp)
		if !ok || load.Op != token.MUL {
			return false
		}
		fa, ok := load.X.(*ssa.FieldAddr)
		if !ok {
			return false
		}
		name, _ := q.p.field(fa.X.Type().Underlying().(*types.Pointer).Elem(), fa.Field)
		return name == "pkg/storage.BadgerEngine.writeBehind"
	}
	isNil := func(v ssa.Value) bool {
		c, ok := v.(*ssa.Const)
		return ok && c.IsNil()
	}
	return isField(cmp.X) && isNil(cmp.Y) || isField(cmp.Y) && isNil(cmp.X)
}

// successEdge is the branch taken when the call's result errIndex is nil:
// the If of "err != nil" (or "== nil") and its successor without an error.
func successEdge(call *ssa.Call, errIndex int) (*ssa.If, *ssa.BasicBlock) {
	// A call with one result (the error) is compared itself.
	if _, tuple := call.Type().(*types.Tuple); !tuple && errIndex == 0 {
		return nilCheck(call)
	}
	for _, ref := range *call.Referrers() {
		ex, ok := ref.(*ssa.Extract)
		if !ok || ex.Index != errIndex {
			continue
		}
		// The error, or (a named result a deferred closure captures lives
		// in a cell) a load of the cell it is stored in.
		errs := []ssa.Value{ex}
		for _, r := range *ex.Referrers() {
			if st, ok := r.(*ssa.Store); ok && st.Val == ex {
				if cell, ok := st.Addr.(*ssa.Alloc); ok {
					for _, u := range *cell.Referrers() {
						if load, ok := u.(*ssa.UnOp); ok && load.Op == token.MUL && st.Block().Dominates(load.Block()) {
							errs = append(errs, load)
						}
					}
				}
			}
		}
		for _, e := range errs {
			if iff, succ := nilCheck(e); succ != nil {
				return iff, succ
			}
		}
	}
	return nil, nil
}

// nilCheck is the If that compares e with nil and its successor taken when
// e is nil.
func nilCheck(e ssa.Value) (*ssa.If, *ssa.BasicBlock) {
	{
		ex := e
		for _, r := range *ex.Referrers() {
			cmp, ok := r.(*ssa.BinOp)
			if !ok || (cmp.Op != token.NEQ && cmp.Op != token.EQL) {
				continue
			}
			other := cmp.Y
			if cmp.Y == ex {
				other = cmp.X
			}
			if c, ok := other.(*ssa.Const); !ok || !c.IsNil() {
				continue
			}
			for _, u := range *cmp.Referrers() {
				iff, ok := u.(*ssa.If)
				if !ok || len(iff.Block().Succs) != 2 {
					continue
				}
				if cmp.Op == token.NEQ {
					return iff, iff.Block().Succs[1]
				}
				return iff, iff.Block().Succs[0]
			}
		}
	}
	return nil, nil
}

// covers reports whether the write order is held at ins on every path to
// it.
func (q *acquirer) covers(fn *ssa.Function, ins ssa.Instruction, facts ...flowAssumption) cover {
	var assumption flowAssumption
	if len(facts) > 0 {
		assumption = facts[0]
	}
	if assumption.selector.value == nil {
		selected := q.selectedValues(fn)
		for _, fact := range blockSelectors(fn, ins.Block(), nil) {
			if selected[fact.value] {
				assumption.selector = fact
				break
			}
		}
	}
	var c cover
	for kind := 0; kind <= len(lockNames); kind++ {
		if q.held(q.flow(fn, kind, assumption), ins) {
			c |= 1 << kind
		}
	}
	return c
}

// held reports whether f holds its acquisition when ins runs.
func (q *acquirer) held(f *flow, ins ssa.Instruction) bool {
	if f.in == nil {
		return false
	}
	held := f.in[ins.Block()]
	for _, x := range ins.Block().Instrs {
		if x == ins {
			return held
		}
		if f.genAt[x] {
			held = true
		}
		if f.kills[x] {
			held = false
		}
	}
	return false
}

// reaches reports whether, in one function, to can run after from.
func reaches(from, to ssa.Instruction) bool {
	if from.Block() == to.Block() && index(from) < index(to) {
		return true
	}
	seen := map[*ssa.BasicBlock]bool{}
	stack := append([]*ssa.BasicBlock(nil), from.Block().Succs...)
	for len(stack) > 0 {
		b := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[b] {
			continue
		}
		seen[b] = true
		if b == to.Block() {
			return true
		}
		stack = append(stack, b.Succs...)
	}
	return false
}

func index(ins ssa.Instruction) int {
	for i, x := range ins.Block().Instrs {
		if x == ins {
			return i
		}
	}
	return -1
}

// asyncCallees are functions outside the module that run a function they
// are given after they return.
var asyncCallees = map[string]bool{
	"(*golang.org/x/sync/errgroup.Group).Go": true, "(*golang.org/x/sync/errgroup.Group).TryGo": true,
	"(*sync.WaitGroup).Go": true, "time.AfterFunc": true, "runtime.SetFinalizer": true, "context.AfterFunc": true,
	"sync.OnceFunc": true, "sync.OnceValue": true, "sync.OnceValues": true,
}

// synchronous reports whether the closure mc makes runs only while its
// maker runs: it is called, deferred, or passed to a parameter that is
// itself synchronous. A closure that is stored, sent, returned or started
// is not: its writes run later, so it is an entry of its own.
func (a *analysis) synchronous(mc *ssa.MakeClosure) bool {
	if a.syncClosure == nil {
		a.syncClosure = map[*ssa.MakeClosure]bool{}
	}
	if ok, done := a.syncClosure[mc]; done {
		return ok
	}
	// A closure that reaches itself (one that captures the variable it is
	// stored in) counts as synchronous while its answer is computed.
	a.syncClosure[mc] = true
	ok := a.syncValue(mc, map[ssa.Value]bool{})
	a.syncClosure[mc] = ok
	return ok
}

func (a *analysis) syncValue(v ssa.Value, seen map[ssa.Value]bool) bool {
	if seen[v] {
		return true
	}
	seen[v] = true
	refs := v.Referrers()
	if refs == nil {
		return true
	}
	for _, ref := range *refs {
		switch r := ref.(type) {
		case *ssa.Call:
			c := r.Common()
			if c.Value == v {
				continue
			}
			if b, ok := c.Value.(*ssa.Builtin); ok {
				// len, cap and copy keep nothing; append's result holds v.
				if b.Name() == "append" && !a.syncValue(r, seen) {
					return false
				}
				continue
			}
			for i, arg := range c.Args {
				if arg != v {
					continue
				}
				if !a.syncParam(r, c, i) {
					return false
				}
			}
		case *ssa.Defer:
			continue
		case *ssa.ChangeType, *ssa.Convert, *ssa.IndexAddr, *ssa.FieldAddr, *ssa.Slice, *ssa.Index, *ssa.Lookup, *ssa.Extract:
			// Through a container: v (or the container holding it) is used
			// as the derived value is.
			if !a.syncValue(r.(ssa.Value), seen) {
				return false
			}
		case *ssa.UnOp:
			if r.Op == token.MUL && !a.syncValue(r, seen) { // a load of v's cell or element
				return false
			}
		case *ssa.Store:
			if r.Val != v {
				continue // v is the address stored into
			}
			// Stored into a value its function allocates: synchronous when
			// that value is.
			base := allocBase(r.Addr)
			if base == nil || !a.syncValue(base, seen) {
				return false
			}
		case *ssa.Phi:
			if !a.syncValue(r, seen) {
				return false
			}
		case *ssa.DebugRef:
			continue
		case *ssa.BinOp:
			// A comparison (fn == nil) keeps nothing.
			if r.Op != token.EQL && r.Op != token.NEQ {
				return false
			}
		case *ssa.MakeClosure:
			// Captured by a closure: synchronous when that closure is, and
			// uses the captured variable only synchronously.
			fn := r.Fn.(*ssa.Function)
			for i, b := range r.Bindings {
				if b == v && (i >= len(fn.FreeVars) || !a.synchronous(r) || !a.syncValue(fn.FreeVars[i], seen)) {
					return false
				}
			}
		default:
			return false
		}
	}
	return true
}

// allocBase is the allocation an element or field address belongs to, when
// its function allocates it, else nil.
func allocBase(addr ssa.Value) *ssa.Alloc {
	for i := 0; i < 16; i++ {
		switch x := addr.(type) {
		case *ssa.Alloc:
			return x
		case *ssa.IndexAddr:
			addr = x.X
		case *ssa.FieldAddr:
			addr = x.X
		default:
			return nil
		}
	}
	return nil
}

// syncParam reports whether argument i of call is used only synchronously
// by every function the call may reach.
func (a *analysis) syncParam(site ssa.CallInstruction, c *ssa.CallCommon, i int) bool {
	var callees []*ssa.Function
	if callee := c.StaticCallee(); callee != nil {
		callees = []*ssa.Function{callee}
	} else if c.IsInvoke() {
		callees = a.callees(site.Parent(), site)
	} else {
		// A function value (run(fn), a passed-in runner): every function
		// the call graph says it can be must keep fn synchronous.
		callees = a.p.callTargets(site)
	}
	if len(callees) == 0 {
		return false
	}
	for _, callee := range callees {
		if asyncCallees[callee.String()] {
			return false
		}
		if reviewedCallbackBoundary(a.p.funcName(callee), i, callee.Syntax(), a.p.fset) {
			continue
		}
		if callee.Blocks == nil {
			continue // outside the module and not known to defer it
		}
		idx := i
		if c.IsInvoke() {
			idx = i + 1 // the receiver is the method's first parameter
		}
		if idx >= len(callee.Params) {
			return false
		}
		if !a.syncParamOf(callee.Params[idx]) {
			return false
		}
	}
	return true
}

// syncParamOf memoizes syncValue for a parameter; a parameter whose answer
// is being computed (a recursive call passes it on) counts as synchronous.
func (a *analysis) syncParamOf(p *ssa.Parameter) bool {
	if a.syncMemo == nil {
		a.syncMemo = map[*ssa.Parameter]int{}
	}
	switch a.syncMemo[p] {
	case 1, 2:
		return true
	case 3:
		return false
	}
	a.syncMemo[p] = 1
	ok := a.syncValue(p, map[ssa.Value]bool{})
	a.syncMemo[p] = 3
	if ok {
		a.syncMemo[p] = 2
	}
	return ok
}

// closureArgCover is the coverage a synchronous closure fn makes at mc runs
// under because of where it goes: every call that receives it covers it
// there, or covers every use of the parameter it arrives in (an ordering
// wrapper: withOrderedEntityUpdate(e, closure)). It is the intersection over
// every use of mc; a use that is no such call contributes nothing.
func (a *analysis) closureArgCover(fn *ssa.Function, mc *ssa.MakeClosure, facts ...*ssa.Parameter) cover {
	var nonEmpty *ssa.Parameter
	if len(facts) > 0 {
		nonEmpty = facts[0]
	}
	refs := mc.Referrers()
	if refs == nil || len(*refs) == 0 {
		return 0
	}
	all := ^cover(0)
	for _, ref := range *refs {
		call, ok := ref.(*ssa.Call)
		if !ok {
			return 0
		}
		all &= a.argCover(fn, call, mc, nonEmpty)
	}
	return all
}

// argCover is the coverage v receives as an argument of call in fn: the
// call's own coverage, with every use the callees make of the parameter.
func (a *analysis) argCover(fn *ssa.Function, call *ssa.Call, v ssa.Value, nonEmpty *ssa.Parameter) cover {
	c := call.Common()
	if c.Value == v {
		return a.acq.covers(fn, call, flowAssumption{nonEmpty: nonEmpty})
	}
	var callees []*ssa.Function
	if callee := c.StaticCallee(); callee != nil {
		callees = []*ssa.Function{callee}
	} else if c.IsInvoke() {
		callees = a.callees(fn, call)
	} else {
		callees = a.p.callTargets(call)
	}
	inner := ^cover(0)
	if len(callees) == 0 {
		inner = 0
	}
	for i, arg := range c.Args {
		if arg != v {
			continue
		}
		for _, callee := range callees {
			idx := i
			if c.IsInvoke() {
				idx = i + 1
			}
			if callee.Blocks == nil || idx >= len(callee.Params) {
				inner = 0
				continue
			}
			var supplied *ssa.Parameter
			for j, value := range c.Args {
				parameter := j
				if c.IsInvoke() {
					parameter++
				}
				if parameter < len(callee.Params) && sliceNonEmpty(value, nonEmpty) {
					// One proven argument is enough for this specialization;
					// multiple or unknown arguments remain conservative.
					if supplied != nil {
						supplied = nil
						break
					}
					supplied = callee.Params[parameter]
				}
			}
			inner &= a.paramCover(callee, idx, supplied)
		}
	}
	return a.acq.covers(fn, call, flowAssumption{nonEmpty: nonEmpty}) | inner
}

// paramCover is the coverage every use of fn's parameter idx holds: the
// intersection over its calls and the calls it is passed to (argCover). A
// parameter whose answer is being computed (a recursion) counts as covered.
type parameterCoverKey struct {
	parameter *ssa.Parameter
	nonEmpty  *ssa.Parameter
}

func (a *analysis) paramCover(fn *ssa.Function, idx int, nonEmpty *ssa.Parameter) cover {
	parameter := fn.Params[idx]
	key := parameterCoverKey{parameter, nonEmpty}
	if a.paramCovers == nil {
		a.paramCovers = map[parameterCoverKey]cover{}
	}
	if c, ok := a.paramCovers[key]; ok {
		return c
	}
	a.paramCovers[key] = ^cover(0)
	all := a.parameterValueCover(fn, parameter, nonEmpty, map[ssa.Value]bool{})
	a.paramCovers[key] = all
	return all
}

// parameterValueCover follows callback aliases and captured local cells.
// Every use must be covered; escape, unknown stores and unsupported uses
// contribute no coverage, so a covered alias cannot bless another invocation.
func (a *analysis) parameterValueCover(fn *ssa.Function, value ssa.Value, nonEmpty *ssa.Parameter, seen map[ssa.Value]bool) cover {
	if seen[value] {
		return ^cover(0)
	}
	seen[value] = true
	refs := value.Referrers()
	if refs == nil || len(*refs) == 0 {
		return 0
	}
	all := ^cover(0)
	for _, ref := range *refs {
		switch ref := ref.(type) {
		case *ssa.Call:
			all &= a.argCover(fn, ref, value, nonEmpty)
		case *ssa.MakeClosure:
			if !a.synchronous(ref) {
				all = 0
			} else {
				all &= a.closureArgCover(fn, ref, nonEmpty)
			}
		case *ssa.Store:
			if ref.Val == value {
				if local := allocBase(ref.Addr); local != nil {
					all &= a.parameterValueCover(fn, local, nonEmpty, seen)
				} else {
					all = 0
				}
			}
		case *ssa.UnOp:
			if ref.Op == token.MUL {
				all &= a.parameterValueCover(fn, ref, nonEmpty, seen)
			} else {
				all = 0
			}
		case *ssa.Phi, *ssa.ChangeType, *ssa.Convert:
			all &= a.parameterValueCover(fn, ref.(ssa.Value), nonEmpty, seen)
		case *ssa.DebugRef:
		default:
			all = 0
		}
	}
	return all
}

// entry reports whether fn can be called from outside what the census
// follows: it is exported, an init or a main, or a closure that runs after
// its maker returns.
func (a *analysis) entry(fn *ssa.Function) bool {
	if fn.Parent() != nil {
		for _, b := range fn.Parent().Blocks {
			for _, ins := range b.Instrs {
				if mc, ok := ins.(*ssa.MakeClosure); ok && mc.Fn == fn {
					if !a.synchronous(mc) {
						return true
					}
				}
				if g, ok := ins.(*ssa.Go); ok {
					if mc, ok := g.Call.Value.(*ssa.MakeClosure); ok && mc.Fn == fn {
						return true
					}
				}
			}
		}
		return false
	}
	if fn.Object() != nil && fn.Object().Exported() {
		// A method of an unexported type is reached through the module's own
		// code only (its callers, or an interface the call graph resolves).
		if recv := fn.Signature.Recv(); recv != nil {
			t := recv.Type()
			if ptr, ok := t.(*types.Pointer); ok {
				t = ptr.Elem()
			}
			if named, ok := t.(*types.Named); ok && !named.Obj().Exported() {
				return false
			}
		}
		return true
	}
	if strings.HasPrefix(fn.Name(), "init") || fn.Name() == "main" && fn.Signature.Recv() == nil {
		return true
	}
	// An unexported function nothing in the module calls (nor makes a value
	// of, nor reaches through an interface) cannot run: it is no entry.
	return false
}

// sliceNonEmpty proves a literal variadic slice's length from its SSA array
// and bounds, or forwards the same proven parameter. Nil, empty and unknown
// slices establish no acquisition.
func sliceNonEmpty(value ssa.Value, parameter *ssa.Parameter) bool {
	if parameter != nil && value == parameter {
		return true
	}
	slice, ok := value.(*ssa.Slice)
	if !ok {
		return false
	}
	var high int64
	if slice.High != nil {
		var exact bool
		high, _, exact = integerConstant(slice.High)
		if !exact {
			return false
		}
	} else {
		pointer, ok := slice.X.Type().Underlying().(*types.Pointer)
		if !ok {
			return false
		}
		array, ok := pointer.Elem().Underlying().(*types.Array)
		if !ok {
			return false
		}
		high = array.Len()
	}
	var low int64
	if slice.Low != nil {
		var exact bool
		low, _, exact = integerConstant(slice.Low)
		if !exact {
			return false
		}
	}
	return high > low
}
