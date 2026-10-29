package capturecensus

// Author: storage-format-owners lane (Claude), #1693, 31 Oct 2026.

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go/token"
	"go/types"
	"os"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

// What a function holds where it creates a fresh context.
//
// The census reports every context a function holds, except values whose
// every possible runtime value is, on each execution that creates the fresh
// context, nil, a fresh context, or built only from those. It proves that by a
// fixed set of exclusions, each with a complete argument that needs no
// analysis of other functions, field or cell writes, or callers:
//
//	E1 a context.Background or context.TODO result: no request, by definition;
//	E2 a nil constant;
//	E3 a context parameter of the creating function that a nil test proves nil
//	   there (workcensus.ParameterNilAt): parameters never change, so it is nil
//	   on every execution that reaches the creation;
//	E4 a phi or interface conversion of excluded values: it is one of them;
//	E5 a context package derivation (context.WithValue, WithCancel, ...) of an
//	   excluded parent: by the package's contract it carries the parent's
//	   values and cancellation and nothing the caller holds besides;
//	E6 a load of a local variable whose address escapes nowhere, through any
//	   closure that captures it (workcensus.LocalCell), when every store into
//	   it anywhere (its function, or those closures) stores an excluded value:
//	   it holds only those values, or its zero value, nil;
//	E7 a result of a static call whose callee returns, at that result, only
//	   values these exclusions remove with no roots of its own (returnsOnlyFresh):
//	   a fact of the callee's body, so it holds for every invocation; a callee
//	   with any other return path, a parameter included, is held;
//	E8 on a caller walk, the caller's own context parameters that a nil test
//	   proves nil at every call it makes toward the function walked from (E3 at
//	   the call).
//
// An address of a context that is only written through or only loaded holds
// nothing beyond its loads, which are judged themselves; any other address is
// held. Everything else (a load of a field, global or reassigned variable, a
// call result, a receive, a type assertion, a parameter not proven nil) is
// held. A held value that is in fact not a request is over-reported: it fails,
// or carries a reviewed record (ReviewedHeldValue) pinned to the exact held
// values and the source of every function the review relied on.

// heldRequest describes a context held at a point of fn, or returns "" when
// none is (heldValues).
func heldRequest(fn *ssa.Function, at func(*ssa.Function) bool, own func(ssa.Value) bool) string {
	held := heldValues(fn, at, own)
	if len(held) == 0 {
		return ""
	}
	return held[0]
}

// heldValues lists every context held at a point of fn that the exclusions do
// not remove, in a stable form (heldDescription): the context parameters of
// fn and its enclosing functions, of the function literals in the same
// outermost function that at selects (those enclosing a walked call), and
// every context value, or held address, in the outermost function and its
// literals. own names the roots of E3 and E7.
func heldValues(fn *ssa.Function, at func(*ssa.Function) bool, own func(ssa.Value) bool) []string {
	excluded := func(v ssa.Value) bool { return excludedValue(v, own, map[ssa.Value]bool{}, 0) }
	var out []string
	params := func(f *ssa.Function) {
		for _, p := range f.Params {
			if isContext(p.Type()) && !excluded(p) {
				out = append(out, "parameter "+p.Name()+" of "+symbolOf(f))
			}
		}
	}
	for current := fn; current != nil; current = current.Parent() {
		params(current)
	}
	top := workcensus.OutermostFunction(fn)
	var scan func(*ssa.Function)
	scan = func(f *ssa.Function) {
		if f != top && at != nil && at(f) && !enclosingChain(fn, f) {
			params(f)
		}
		for _, block := range f.Blocks {
			for _, instr := range block.Instrs {
				v, ok := instr.(ssa.Value)
				if !ok || !heldContext(v) || excluded(v) {
					continue
				}
				out = append(out, heldDescription(v))
			}
		}
		for _, anon := range f.AnonFuncs {
			scan(anon)
		}
	}
	scan(top)
	return out
}

// enclosingChain reports that f is fn or encloses it (its parameters were
// listed already).
func enclosingChain(fn, f *ssa.Function) bool {
	for current := fn; current != nil; current = current.Parent() {
		if current == f {
			return true
		}
	}
	return false
}

// excludedValue applies E1-E8 (see above) to v.
func excludedValue(v ssa.Value, own func(ssa.Value) bool, visited map[ssa.Value]bool, depth int) bool {
	if own != nil && own(v) {
		return true
	}
	if freshContext(v) {
		return true
	}
	if visited[v] {
		return true
	}
	if depth > 32 {
		return false
	}
	visited[v] = true
	switch v := v.(type) {
	case *ssa.Const:
		return v.IsNil()
	case *ssa.Phi:
		for _, edge := range v.Edges {
			if !excludedValue(edge, own, visited, depth+1) {
				return false
			}
		}
		return len(v.Edges) > 0
	case *ssa.MakeInterface:
		return excludedValue(v.X, own, visited, depth+1)
	case *ssa.ChangeInterface:
		return excludedValue(v.X, own, visited, depth+1)
	case *ssa.Extract:
		if call, ok := v.Tuple.(*ssa.Call); ok && v.Index == 0 {
			if own != nil && own(call) {
				return true
			}
			if parent, ok := contextDerivationParent(call); ok {
				return excludedValue(parent, own, visited, depth+1)
			}
		}
		if call, ok := v.Tuple.(*ssa.Call); ok {
			return returnsOnlyFresh(call, v.Index, visited, depth+1)
		}
		return false
	case *ssa.Call:
		if parent, ok := contextDerivationParent(v); ok {
			return excludedValue(parent, own, visited, depth+1)
		}
		return returnsOnlyFresh(v, 0, visited, depth+1)
	case *ssa.UnOp:
		if v.Op != token.MUL {
			return false
		}
		cell := localCellOf(v.X)
		if cell == nil || !workcensus.LocalCell(cell) {
			return false
		}
		stores := workcensus.CapturedStores(cell)
		for _, ref := range *cell.Referrers() {
			if store, ok := ref.(*ssa.Store); ok && store.Addr == ssa.Value(cell) {
				stores = append(stores, store)
			}
		}
		for _, store := range stores {
			if !excludedValue(store.Val, own, visited, depth+1) {
				return false
			}
		}
		return true
	}
	return false
}

// returnsOnlyFresh applies E7 to result index of call: its static callee has a
// return, and every return's value at index is excluded with no roots. A cycle
// of such calls adds no other value.
func returnsOnlyFresh(call *ssa.Call, index int, visited map[ssa.Value]bool, depth int) bool {
	callee := call.Call.StaticCallee()
	if callee == nil {
		return false
	}
	returns := 0
	for _, block := range callee.Blocks {
		for _, instr := range block.Instrs {
			ret, ok := instr.(*ssa.Return)
			if !ok {
				continue
			}
			if index >= len(ret.Results) || !excludedValue(ret.Results[index], nil, visited, depth) {
				return false
			}
			returns++
		}
	}
	return returns > 0
}

// localCellOf resolves the address a load reads to the local variable it
// names: the variable itself, or, through closures' free variables at any
// depth, the variable they capture. It returns nil for any other address.
func localCellOf(addr ssa.Value) *ssa.Alloc {
	for depth := 0; depth < 16; depth++ {
		switch a := addr.(type) {
		case *ssa.Alloc:
			return a
		case *ssa.FreeVar:
			closure, index := workcensus.FreeVarClosure(a)
			if closure == nil {
				return nil
			}
			addr = closure.Bindings[index]
		default:
			return nil
		}
	}
	return nil
}

// contextDerivations are the context package's derivations (E5).
var contextDerivations = map[string]bool{
	"WithValue": true, "WithCancel": true, "WithCancelCause": true, "WithTimeout": true,
	"WithTimeoutCause": true, "WithDeadline": true, "WithDeadlineCause": true, "WithoutCancel": true,
}

// contextDerivationParent returns the parent of a static call of a context
// package derivation.
func contextDerivationParent(call *ssa.Call) (ssa.Value, bool) {
	if call.Call.IsInvoke() {
		return nil, false
	}
	callee := call.Call.StaticCallee()
	if callee == nil || callee.Pkg == nil || callee.Pkg.Pkg.Path() != "context" || !contextDerivations[callee.Name()] || len(call.Call.Args) == 0 {
		return nil, false
	}
	return call.Call.Args[0], true
}

// heldContext reports a value that may hold a context: a context value, or the
// address of a context variable or field with a use other than a store into
// it or a load from it (a local cell is judged through every closure that
// captures it, workcensus.LocalCell).
func heldContext(v ssa.Value) bool {
	ptr, pointer := types.Unalias(v.Type()).Underlying().(*types.Pointer)
	if !pointer {
		return isContext(v.Type())
	}
	if !isContext(ptr.Elem()) {
		return false
	}
	if cell, ok := v.(*ssa.Alloc); ok {
		return !workcensus.LocalCell(cell)
	}
	if v.Referrers() == nil {
		return false
	}
	for _, ref := range *v.Referrers() {
		switch ref := ref.(type) {
		case *ssa.Store:
			if ref.Addr != v {
				return true
			}
		case *ssa.UnOp:
			if ref.Op != token.MUL {
				return true
			}
		case *ssa.DebugRef:
		default:
			return true
		}
	}
	return false
}

// heldDescription names a held value stably, without SSA register names: its
// kind, what produced it (heldProducer) and its source line within its
// function, so two occurrences of the same producer stay distinct.
func heldDescription(v ssa.Value) string {
	what := heldProducer(v)
	if what == "" {
		what = strings.TrimPrefix(fmt.Sprintf("%T", v), "*ssa.")
	}
	fn := v.Parent()
	line := 0
	if fn != nil && v.Pos().IsValid() && fn.Pos().IsValid() {
		line = fn.Prog.Fset.Position(v.Pos()).Line - fn.Prog.Fset.Position(fn.Pos()).Line
	}
	return fmt.Sprintf("%s (+%d in %s)", what, line, symbolOf(fn))
}

// heldProducer names what produced a held value: "call <symbol>", "dynamic
// call of field <name> of <type>", "load of field <name> of <type>", "load of
// global <name>", with " result #i" for a tuple result.
func heldProducer(v ssa.Value) string {
	field := func(addr ssa.Value) string {
		fa, ok := addr.(*ssa.FieldAddr)
		if !ok {
			return ""
		}
		s, ok := types.Unalias(fa.X.Type()).Underlying().(*types.Pointer)
		if !ok {
			return ""
		}
		st, ok := s.Elem().Underlying().(*types.Struct)
		if !ok || fa.Field >= st.NumFields() {
			return ""
		}
		return "field " + st.Field(fa.Field).Name() + " of " + types.TypeString(fa.X.Type(), nil)
	}
	switch v := v.(type) {
	case *ssa.MakeInterface:
		return heldProducer(v.X)
	case *ssa.Extract:
		if p := heldProducer(v.Tuple); p != "" {
			return fmt.Sprintf("%s result #%d", p, v.Index)
		}
	case *ssa.Call:
		if callee := v.Call.StaticCallee(); callee != nil {
			return "call " + symbolOf(callee)
		}
		if v.Call.IsInvoke() {
			return "interface call " + v.Call.Method.FullName()
		}
		if load, ok := v.Call.Value.(*ssa.UnOp); ok && load.Op == token.MUL {
			if f := field(load.X); f != "" {
				return "dynamic call of " + f
			}
		}
		return "dynamic call"
	case *ssa.UnOp:
		if v.Op == token.MUL {
			if f := field(v.X); f != "" {
				return "load of " + f
			}
			if g, ok := v.X.(*ssa.Global); ok {
				return "load of global " + g.RelString(nil)
			}
			return "load of a variable"
		}
	}
	return ""
}

// encloses selects the function literals holding a call that may reach
// callee, directly or in a literal nested in them: their parameters are in
// scope at that call. A call may reach callee when it calls it statically, or
// dynamically with callee's method name (an interface call) or callee's
// signature (a function value).
func encloses(callee *ssa.Function) func(*ssa.Function) bool {
	var calls func(*ssa.Function) bool
	calls = func(f *ssa.Function) bool {
		for _, block := range f.Blocks {
			for _, instr := range block.Instrs {
				if call, ok := instr.(ssa.CallInstruction); ok && mayCall(call, callee) {
					return true
				}
			}
		}
		for _, anon := range f.AnonFuncs {
			if calls(anon) {
				return true
			}
		}
		return false
	}
	return calls
}

// mayCall reports a call that may reach callee: a static call of it, or a
// dynamic call with its method name (an interface call) or its signature (a
// function value).
func mayCall(call ssa.CallInstruction, callee *ssa.Function) bool {
	common := call.Common()
	if target := common.StaticCallee(); target != nil {
		return target == callee
	}
	if common.IsInvoke() {
		return common.Method.Name() == callee.Name()
	}
	sig, ok := common.Value.Type().Underlying().(*types.Signature)
	return ok && types.Identical(sig, callee.Signature)
}

// requestToward describes a context caller holds on its calls to callee, or
// "" when it holds none: heldRequest at those calls, with the caller's context
// parameters a nil test proves nil at every one of those calls excluded (E8).
// A parameter is never excluded when a call toward callee runs in a function
// literal.
func requestToward(caller, callee *ssa.Function) string {
	return requestTowardExcept(caller, callee, func(ssa.Value) bool { return false })
}

// reviewedHolder reports that caller holds, on its calls to callee, only what
// its reviewed record pins, and that the record is still current.
func reviewedHolder(p *workcensus.Program, caller, callee *ssa.Function, record ReviewedHeldValue, digests *sourceDigests) bool {
	if requestTowardExcept(caller, callee, reviewedRoots(record, func(ssa.Value) bool { return false })) != "" {
		return false
	}
	return reviewedHeldStale(p, caller, record, heldValues(caller, encloses(callee), nil), digests) == ""
}

// requestTowardExcept is requestToward with the values except names excluded.
func requestTowardExcept(caller, callee *ssa.Function, except func(ssa.Value) bool) string {
	nilParams := map[ssa.Value]bool{}
	inLiteral := false
	for _, anon := range caller.AnonFuncs {
		if encloses(callee)(anon) {
			inLiteral = true
		}
	}
	if !inLiteral {
		var blocks []*ssa.BasicBlock
		for _, block := range caller.Blocks {
			for _, instr := range block.Instrs {
				if call, ok := instr.(ssa.CallInstruction); ok && mayCall(call, callee) {
					blocks = append(blocks, block)
				}
			}
		}
		for _, param := range caller.Params {
			if !isContext(param.Type()) || len(blocks) == 0 {
				continue
			}
			everywhere := true
			for _, block := range blocks {
				if !workcensus.ParameterNilAt(param, block) {
					everywhere = false
				}
			}
			if everywhere {
				nilParams[param] = true
			}
		}
	}
	return heldRequest(caller, encloses(callee), func(v ssa.Value) bool { return nilParams[v] || except(v) })
}

// ownContextNilAt reports that fn has context parameters and every one is
// known nil at block.
func ownContextNilAt(fn *ssa.Function, block *ssa.BasicBlock) bool {
	params := 0
	for _, param := range fn.Params {
		if !isContext(param.Type()) {
			continue
		}
		params++
		if !workcensus.ParameterNilAt(param, block) {
			return false
		}
	}
	return params > 0
}

// creationRoots are the roots of E3 for a fresh context created at origin:
// origin itself and the creating function's context parameters proven nil
// there.
func creationRoots(origin ssa.Value, fn *ssa.Function) func(ssa.Value) bool {
	roots := map[ssa.Value]bool{origin: true}
	if instr, ok := origin.(ssa.Instruction); ok {
		for _, param := range fn.Params {
			if isContext(param.Type()) && workcensus.ParameterNilAt(param, instr.Block()) {
				roots[param] = true
			}
		}
	}
	return func(v ssa.Value) bool { return roots[v] }
}

// ReviewedHeldValue is a reviewed record for one function whose held contexts,
// at the fresh contexts it creates, are not requests by a reading the census
// cannot prove. It is pinned to what was reviewed: Held is the exact list of
// held values the reading covers (heldDescription: producer, line in the
// function, function); the census treats exactly those occurrences as
// excluded roots, so values built from them by E4-E6 are covered too, and
// fails the record when any of them is gone or anything else is held. Sources
// is the sha256 (sourceDigests.of) of the
// source of the function itself and of every function the reading relies on
// (producers, callbacks and their bindings), keyed by symbol; Modules pins
// library versions ("path version", as in go.mod) the reading relies on;
// Callers the exact caller set of named functions (a callback's binding
// sites); FieldWriters the exact set of functions storing a struct field
// ("<package path>.<Type>.<field>", a callback field or a wrapper's embedded
// context); and Methods the exact method set of a type ("<package
// path>.<Type>", each method "Name declared" or "Name promoted"), so a wrapper
// gaining or overriding a method is seen. The census accepts the site only
// when all of these still match; any change, an added or removed held value,
// an edited function, a new binding or writer, a changed method set or
// version, fails the record as stale, so it is reviewed again. Closure is the
// digest of everything those sources reach (sourceDigests.closure), so a change
// below a pinned function stales the record without that function being named.
// A record matching no site fails.
//
// EndsRouteAt names direct callers of the site at which its caller walk ends,
// each reviewed by reading: every call it makes passes its own context, or the
// route is one the record's reason covers. Each must be in Callers and in
// Sources or RouteSources, so a new caller or a changed one stales the record;
// any other caller is walked. TrackedRoutes names direct callers whose route is
// a filed finding, by its text: listed and counted as tracked defects, never
// cleared, and pinned the same way. RouteSources are source digests of those
// callers and the functions above them, outside Closure.
type ReviewedHeldValue struct {
	EndsRouteAt   []string
	TrackedRoutes map[string]string
	RouteSources  map[string]string
	Held          []string
	Sources       map[string]string
	Closure       string
	Modules       []string
	Callers       map[string][]string
	FieldWriters  map[string][]string
	Methods       map[string][]string
	Reason        string
	Approver      string
}

// reviewedHeldStale returns why record no longer matches the site of fn whose
// held values are held, or "" when every pinned fact still holds.
func reviewedHeldStale(p *workcensus.Program, fn *ssa.Function, record ReviewedHeldValue, held []string, digests *sourceDigests) string {
	var why []string
	present := map[string]int{}
	for _, h := range held {
		present[h]++
	}
	for _, h := range record.Held {
		if present[h] == 0 {
			why = append(why, fmt.Sprintf("pinned held value gone: %q (held now %q)", h, held))
			continue
		}
		present[h]--
	}
	site := symbolOf(workcensus.OutermostFunction(fn))
	if _, ok := record.Sources[site]; !ok {
		why = append(why, "the site's own source is not pinned: "+site+" "+digests.of(site))
	}
	symbols := make([]string, 0, len(record.Sources))
	for symbol := range record.Sources {
		symbols = append(symbols, symbol)
	}
	sort.Strings(symbols)
	for _, symbol := range symbols {
		if got := digests.of(symbol); got != record.Sources[symbol] {
			why = append(why, fmt.Sprintf("source of %s changed: now %s", symbol, got))
		}
	}
	routeSymbols := make([]string, 0, len(record.RouteSources))
	for symbol := range record.RouteSources {
		routeSymbols = append(routeSymbols, symbol)
	}
	sort.Strings(routeSymbols)
	for _, symbol := range routeSymbols {
		if got := digests.of(symbol); got != record.RouteSources[symbol] {
			why = append(why, fmt.Sprintf("route source of %s changed: now %s", symbol, got))
		}
	}
	if record.Closure != "" {
		if got := digests.closure(symbols); got != record.Closure {
			why = append(why, "static closure changed: now "+got)
		}
	}
	for _, module := range record.Modules {
		if !digests.requires(module) {
			why = append(why, "module requirement changed: "+module)
		}
	}
	compare := func(label string, pinned map[string][]string, compute func(string) []string) {
		keys := make([]string, 0, len(pinned))
		for key := range pinned {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			got := compute(key)
			want := append([]string(nil), pinned[key]...)
			sort.Strings(want)
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				why = append(why, fmt.Sprintf("%s %s changed: now %q", label, key, got))
			}
		}
	}
	compare("callers of", record.Callers, func(target string) []string { return callerSymbols(p, target) })
	compare("writers of", record.FieldWriters, func(field string) []string { return fieldWriters(p, field) })
	compare("methods of", record.Methods, func(typ string) []string { return methodSet(p, typ) })
	return strings.Join(why, "; ")
}

// fieldWriters lists the outermost functions storing the struct field named
// "<package path>.<Type>.<field>", by a store to its address (composite
// literals included).
func fieldWriters(p *workcensus.Program, key string) []string {
	seen := map[string]bool{}
	for _, fn := range p.Functions {
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				store, ok := instr.(*ssa.Store)
				if !ok {
					continue
				}
				fa, ok := store.Addr.(*ssa.FieldAddr)
				if !ok {
					continue
				}
				ptr, ok := types.Unalias(fa.X.Type()).Underlying().(*types.Pointer)
				if !ok {
					continue
				}
				named, ok := types.Unalias(ptr.Elem()).(*types.Named)
				if !ok || named.Obj().Pkg() == nil {
					continue
				}
				st, ok := named.Underlying().(*types.Struct)
				if !ok || fa.Field >= st.NumFields() {
					continue
				}
				if named.Obj().Pkg().Path()+"."+named.Obj().Name()+"."+st.Field(fa.Field).Name() == key {
					seen[symbolOf(workcensus.OutermostFunction(fn))] = true
				}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for symbol := range seen {
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

// methodSet lists the method set of a pointer to the type named
// "<package path>.<Type>", each "Name declared" or "Name promoted".
func methodSet(p *workcensus.Program, key string) []string {
	for _, fn := range p.Functions {
		if fn.Pkg == nil || !strings.HasPrefix(key, fn.Pkg.Pkg.Path()+".") {
			continue
		}
		obj, ok := fn.Pkg.Pkg.Scope().Lookup(strings.TrimPrefix(key, fn.Pkg.Pkg.Path()+".")).(*types.TypeName)
		if !ok {
			continue
		}
		mset := types.NewMethodSet(types.NewPointer(obj.Type()))
		out := make([]string, 0, mset.Len())
		for i := 0; i < mset.Len(); i++ {
			sel := mset.At(i)
			how := "promoted"
			if len(sel.Index()) == 1 {
				how = "declared"
			}
			out = append(out, sel.Obj().Name()+" "+how)
		}
		sort.Strings(out)
		return out
	}
	return []string{"missing"}
}

// reviewedRoots extends roots with the held values record pins, by their
// exact occurrence (heldDescription).
func reviewedRoots(record ReviewedHeldValue, roots func(ssa.Value) bool) func(ssa.Value) bool {
	pinned := map[string]bool{}
	for _, h := range record.Held {
		pinned[h] = true
	}
	return func(v ssa.Value) bool {
		if roots(v) {
			return true
		}
		if _, ok := v.(ssa.Instruction); !ok || len(pinned) == 0 {
			return false
		}
		return pinned[heldDescription(v)]
	}
}

// callerSymbols lists the outermost functions holding a call site of the
// function named target (the engine's SourceCallSites: static, interface and
// function-value calls).
func callerSymbols(p *workcensus.Program, target string) []string {
	seen := map[string]bool{}
	for _, fn := range p.Functions {
		if symbolOf(fn) != target {
			continue
		}
		for _, site := range p.SourceCallSites(fn) {
			seen[symbolOf(workcensus.OutermostFunction(site.Parent()))] = true
		}
	}
	out := make([]string, 0, len(seen))
	for symbol := range seen {
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

// sourceDigests computes sourceDigest per symbol, and reads the module's
// go.mod for Modules.
type sourceDigests struct {
	p      *workcensus.Program
	byName map[string]string
	files  map[string][]byte
	gomod  string
}

func newSourceDigests(p *workcensus.Program) *sourceDigests {
	d := &sourceDigests{p: p, byName: map[string]string{}, files: map[string][]byte{}}
	if data, err := os.ReadFile(p.Root + "/go.mod"); err == nil {
		d.gomod = strings.ReplaceAll(string(data), "\r\n", "\n")
	}
	return d
}

// of returns the sha256 of the source text of the function named symbol (its
// declaration, carriage returns removed), or "missing" when no function of the
// loaded module has that name and source.
func (d *sourceDigests) of(symbol string) string {
	if digest, ok := d.byName[symbol]; ok {
		return digest
	}
	digest := "missing"
	for _, fn := range d.p.Functions {
		if symbolOf(fn) != symbol || fn.Syntax() == nil {
			continue
		}
		node := fn.Syntax()
		start, end := d.p.FileSet.PositionFor(node.Pos(), false), d.p.FileSet.PositionFor(node.End(), false)
		data, ok := d.files[start.Filename]
		if !ok {
			data, _ = os.ReadFile(start.Filename)
			d.files[start.Filename] = data
		}
		if start.Offset < 0 || end.Offset > len(data) || start.Offset > end.Offset {
			continue
		}
		sum := sha256.Sum256([]byte(strings.ReplaceAll(string(data[start.Offset:end.Offset]), "\r", "")))
		digest = hex.EncodeToString(sum[:])
		break
	}
	d.byName[symbol] = digest
	return digest
}

// closure returns the digest of the source of every module function reachable
// from the functions named symbols by static calls and function values, each
// function with its literals. A dynamic call adds no target: its call text is
// in the digest, and its bindings are pinned by Callers and FieldWriters.
func (d *sourceDigests) closure(symbols []string) string {
	wanted := map[string]bool{}
	for _, symbol := range symbols {
		wanted[symbol] = true
	}
	seen := map[*ssa.Function]bool{}
	var queue []*ssa.Function
	add := func(fn *ssa.Function) {
		top := workcensus.OutermostFunction(fn)
		if top == nil || seen[top] || top.Syntax() == nil || !strings.HasPrefix(d.p.FileSet.PositionFor(top.Pos(), false).Filename, d.p.Root) {
			return
		}
		seen[top] = true
		queue = append(queue, top)
	}
	for _, fn := range d.p.Functions {
		if wanted[symbolOf(fn)] {
			add(fn)
		}
	}
	var scan func(*ssa.Function)
	scan = func(fn *ssa.Function) {
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				for _, operand := range instr.Operands(nil) {
					if operand != nil {
						if target, ok := (*operand).(*ssa.Function); ok {
							add(target)
						}
					}
				}
			}
		}
		for _, anon := range fn.AnonFuncs {
			scan(anon)
		}
	}
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		scan(fn)
	}
	lines := make([]string, 0, len(seen))
	for fn := range seen {
		lines = append(lines, symbolOf(fn)+" "+d.of(symbolOf(fn)))
	}
	sort.Strings(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(sum[:])
}

// requires reports a go.mod line requiring module ("path version").
func (d *sourceDigests) requires(module string) bool {
	for _, line := range strings.Split(d.gomod, "\n") {
		fields := strings.Fields(strings.TrimPrefix(strings.TrimSpace(line), "require "))
		if len(fields) >= 2 && fields[0]+" "+fields[1] == module {
			return true
		}
	}
	return false
}
