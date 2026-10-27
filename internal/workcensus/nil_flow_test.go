package workcensus

// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

// Each plant hands a context to sink; MayBeNil must report exactly the plants
// where a nil can arrive, through every flow it follows, and none of the
// guarded or real contexts.
func TestMayBeNilPlants(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "nil_flow.go"): []byte(`package fixture
import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)
func sink(ctx context.Context) error { return ctx.Err() }
type holder struct{ ctx context.Context }
func zeroField() error { h := &holder{}; return sink(h.ctx) }
func nilConstant() error { return sink(nil) }
func relay(ctx context.Context) error { return sink(ctx) }
func viaParam() error { return relay(nil) }
func realParam() error { return relay(context.Background()) }
func captured() error { var ctx context.Context; f := func() error { return sink(ctx) }; return f() }
func made() context.Context { return nil }
func viaResult() error { return sink(made()) }
func viaMap(m map[string]context.Context) error { return sink(m["x"]) }
func normalized(ctx context.Context) error { if ctx == nil { ctx = context.Background() }; return sink(ctx) }
func callsNormalized() error { return normalized(nil) }
func guarded(ctx context.Context) error { if ctx != nil { return sink(ctx) }; return nil }
func callsGuarded() error { return guarded(nil) }
func derived() error { ctx, cancel := context.WithCancel(context.Background()); defer cancel(); return sink(ctx) }
type reader struct{ props map[string]any }
func (r *reader) read(ctx context.Context, props map[string]any) error { _ = props; return sink(ctx) }
type readerAPI interface{ read(context.Context, map[string]any) error }
func viaMethodValue(r *reader) error { f := r.read; return f(context.Background(), r.props) }
func viaInterface(r readerAPI) error { return r.read(context.Background(), (&reader{}).props) }
func viaStaticMethod(r *reader) error { return r.read(context.Background(), r.props) }
type nilReader struct{}
type nilAPI interface{ readNil(context.Context, map[string]any) error }
func (nilReader) readNil(ctx context.Context, _ map[string]any) error { return sink(ctx) }
func viaInterfaceNil(r nilAPI) error { return r.readNil(nil, nil) }
func UseNilReader() error { return viaInterfaceNil(nilReader{}) }
type built struct{ ctx context.Context }
func newBuilt(ctx context.Context) built { return built{ctx: ctx} }
func (b *built) check() error { return sink(b.ctx) }
func viaWholeValue() error { b := newBuilt(context.Background()); return b.check() }
type ctxSource interface{ Ctx() context.Context }
type nilSource struct{}
func (nilSource) Ctx() context.Context { return nil }
func viaDynamicResult(s ctxSource) error { return sink(s.Ctx()) }
func UseNilSource() error { return viaDynamicResult(nilSource{}) }
type realAPI interface{ Real() context.Context }
type realSource struct{}
func (realSource) Real() context.Context { return context.Background() }
func viaRealResult(s realAPI) error { return sink(s.Real()) }
func UseRealSource() error { return viaRealResult(realSource{}) }
var makeNil = func() context.Context { return nil }
func viaFuncValue() error { return sink(makeNil()) }
func maybeCtx(fail bool) (context.Context, error) { if fail { return nil, errors.New("limit") }; return context.Background(), nil }
func viaCheckedError(fail bool) error { ctx, err := maybeCtx(fail); if err != nil { return err }; return sink(ctx) }
func viaIgnoredError(fail bool) error { ctx, _ := maybeCtx(fail); return sink(ctx) }
type laterHolder struct{ ctx context.Context }
func viaCapturedHolderSet() error { h := &laterHolder{}; f := func() { _ = h }; f(); h.ctx = context.Background(); return sink(h.ctx) }
type calledHolder struct{ ctx context.Context }
func viaCapturedHolderAfterCall() error { h := &calledHolder{}; f := func() { _ = h }; f(); h.ctx = context.Background(); f(); return sink(h.ctx) }
type movedHolder struct{ ctx context.Context }
func viaMutableHolder() error { h := &movedHolder{}; f := func() { _ = h }; f(); h.ctx = context.Background(); h = &movedHolder{}; return sink(h.ctx) }
func viaCellChecked(ctx context.Context, fail bool) (err error) {
	defer func() { _ = &ctx }()
	_ = sink(ctx)
	ctx, err = maybeCtx(fail)
	if err != nil {
		return err
	}
	return sink(ctx)
}
func callsCellChecked() error { return viaCellChecked(context.Background(), true) }
func viaOtherCell(fail bool) (e1, e2 error) {
	defer func() {}()
	var ctx context.Context
	ctx, e1 = maybeCtx(fail)
	if e2 != nil {
		return
	}
	e2 = sink(ctx)
	return
}
func viaOverwrittenCell(fail bool) (err error) {
	defer func() {}()
	var ctx context.Context
	ctx, err = maybeCtx(fail)
	err = nil
	if err != nil {
		return err
	}
	return sink(ctx)
}
type lazy struct{ ctx context.Context }
func (l *lazy) get() context.Context { if l.ctx == nil { l.ctx = context.Background() }; return l.ctx }
func viaLazy(l *lazy) error { return sink(l.get()) }
func UseLazy() error { return viaLazy(&lazy{}) }
type lazySkip struct{ ctx context.Context }
func (l *lazySkip) get(ok bool) context.Context { if l.ctx == nil { if ok { l.ctx = context.Background() } }; return l.ctx }
func viaLazySkip(l *lazySkip, ok bool) error { return sink(l.get(ok)) }
func UseLazySkip(ok bool) error { return viaLazySkip(&lazySkip{}, ok) }
type lazyLocked struct{ mu sync.Mutex; ctx context.Context }
func (l *lazyLocked) bind(ctx context.Context) context.Context {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ctx == nil {
		if ctx == nil {
			ctx = context.Background()
		}
		l.ctx, _ = context.WithCancel(ctx)
	}
	return l.ctx
}
func viaLazyLocked(l *lazyLocked) error { return sink(l.bind(nil)) }
func UseLazyLocked() error { return viaLazyLocked(&lazyLocked{}) }
type lazyRecovered struct{ ctx context.Context }
func (l *lazyRecovered) get(fail bool) context.Context {
	defer func() { _ = recover() }()
	if fail {
		panic("before the store")
	}
	if l.ctx == nil {
		l.ctx = context.Background()
	}
	return l.ctx
}
func viaLazyRecovered(l *lazyRecovered, fail bool) error { return sink(l.get(fail)) }
func UseLazyRecovered(fail bool) error { return viaLazyRecovered(&lazyRecovered{}, fail) }
type lazyDeferred struct{ ctx context.Context }
func (l *lazyDeferred) get(cleanup func()) context.Context {
	defer cleanup()
	if l.ctx == nil {
		l.ctx = context.Background()
	}
	return l.ctx
}
func viaLazyDeferred(l *lazyDeferred, cleanup func()) error { return sink(l.get(cleanup)) }
func UseLazyDeferred(cleanup func()) error { return viaLazyDeferred(&lazyDeferred{}, cleanup) }
type lazyReset struct{ ctx context.Context }
func (l *lazyReset) get() context.Context { if l.ctx == nil { l.ctx = context.Background() }; l.ctx = nil; return l.ctx }
func viaLazyReset(l *lazyReset) error { return sink(l.get()) }
func UseLazyReset() error { return viaLazyReset(&lazyReset{ctx: context.Background()}) }
type lazyCleared struct{ ctx context.Context }
func (l *lazyCleared) clear() { l.ctx = nil }
func (l *lazyCleared) get() context.Context { if l.ctx == nil { l.ctx = context.Background() }; l.clear(); return l.ctx }
func viaLazyCleared(l *lazyCleared) error { return sink(l.get()) }
func UseLazyCleared() error { return viaLazyCleared(&lazyCleared{ctx: context.Background()}) }
type lazyAliased struct{ ctx context.Context }
func (l *lazyAliased) get(other *lazyAliased) context.Context { if l.ctx == nil { l.ctx = context.Background() }; other.ctx = nil; return l.ctx }
func viaLazyAliased(l *lazyAliased) error { return sink(l.get(l)) }
func UseLazyAliased() error { return viaLazyAliased(&lazyAliased{ctx: context.Background()}) }
func viaCellSkipped(ok bool) error {
	var ctx context.Context
	_ = func() context.Context { return ctx }
	if ok {
		ctx = context.Background()
	}
	return sink(ctx)
}
type lazyExternalDefer struct{ n int32; ctx context.Context }
func (l *lazyExternalDefer) get() context.Context {
	defer atomic.AddInt32(&l.n, 1)
	if l.ctx == nil {
		l.ctx = context.Background()
	}
	return l.ctx
}
func viaLazyExternalDefer(l *lazyExternalDefer) error { return sink(l.get()) }
func UseLazyExternalDefer() error { return viaLazyExternalDefer(&lazyExternalDefer{}) }
type wrapped struct{ context.Context }
func viaWrappedReal() error { return sink(wrapped{Context: context.Background()}) }
func viaWrappedZero() error { return sink(wrapped{}) }
func viaWrappedParam(ctx context.Context) error { return sink(wrapped{Context: ctx}) }
func callsWrappedNil() error { return viaWrappedParam(nil) }
func viaWrappedReassigned(ctx context.Context) error { if ctx == nil { ctx = context.Background() }; ctx = wrapped{Context: ctx}; return sink(ctx) }
func callsWrappedReassigned() error { return viaWrappedReassigned(nil) }
func viaCellLater() error { var ctx context.Context; p := &ctx; _ = sink(*p); *p = context.Background(); return nil }
var _ = []any{zeroField, nilConstant, viaParam, realParam, captured, viaResult, viaMap, callsNormalized, callsGuarded, derived, viaMethodValue, viaInterface, viaStaticMethod, viaInterfaceNil, UseNilReader, viaWholeValue, UseNilSource, UseRealSource, viaFuncValue, viaCheckedError, viaIgnoredError, callsCellChecked, viaCellLater, viaOtherCell, viaOverwrittenCell, UseLazy, UseLazySkip, UseLazyLocked, UseLazyRecovered, UseLazyDeferred, UseLazyReset, UseLazyCleared, UseLazyAliased, viaCellSkipped, UseLazyExternalDefer, viaWrappedReal, viaWrappedZero, callsWrappedNil, callsWrappedReassigned}
`)}
	inv := mustExtract(t, c)
	got := map[string]string{}
	for _, fn := range inv.Program.Functions {
		if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" {
			continue
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "sink" {
					continue
				}
				if w := inv.Program.MayBeNil(call.Common().Args[0], call); w != nil {
					got[strings.TrimPrefix(OutermostFunction(fn).String(), "census.fixture.")] = strings.Join(w, " ")
				}
			}
		}
	}
	// (*reader).read receives only real contexts, by static, method-value and
	// interface calls, but its props argument is a zero field: mapping that
	// argument onto ctx (the receiver offset) would wrongly report it.
	// nilReader.readNil is reached with nil through its interface. A struct
	// built with a real context and assigned whole (viaWholeValue) is not nil.
	// A nil produced behind an interface method (viaDynamicResult) or a
	// function value (viaFuncValue) is reported; a real one (viaRealResult) is not.
	// A nil returned only with an error is reported when the error is ignored
	// (viaIgnoredError), not when the use follows "if err != nil { return }",
	// also through an address-taken local (viaCellChecked: the earlier use sees
	// only the real parameter, the later one only the checked success result).
	// A local read before its only store sees its zero value (viaCellLater).
	// An error tested through a cell counts only for the cell that received the
	// call's error and only if it is not overwritten before the test
	// (viaCellChecked silent; viaOtherCell and viaOverwrittenCell reported).
	// A field or cell read is judged by the definitions that reach it: lazy
	// initialization is non-nil (viaLazy silent) unless the nil branch can skip
	// the store (viaLazySkip), a later nil store overwrites it (viaLazyReset),
	// a call can clear it (viaLazyCleared) or a store through another reference
	// can (viaLazyAliased); a cell stored on one branch keeps its zero value on
	// the other (viaCellSkipped). All of these are reported; the plants that
	// set their field to a real context elsewhere show that the report comes
	// from the path, not from a zero allocation. A Recover block is
	// ignored only when no deferred call can recover: defer Unlock with lazy
	// init (viaLazyLocked) is silent; a deferred recover() with a panic before
	// the store (viaLazyRecovered) and a deferred func-typed parameter
	// (viaLazyDeferred) keep the Recover block's zero result and are reported.
	// A deferred callee from a package loaded without bodies is judged from its
	// package's source: sync's Unlock has no recover call (viaLazyLocked
	// silent); an assembly function has no Go body to read and may recover
	// (viaLazyExternalDefer reported).
	// A holder captured by a closure lives in a cell that every use loads
	// again. When the cell is stored once, those loads name one object: its
	// field set before the read is non-nil, directly (viaCapturedHolderSet)
	// or after a call (viaCapturedHolderAfterCall). A cell given a new object
	// after the store (viaMutableHolder) reads the new object's zero field.
	// A struct converted to an interface is never nil itself; the methods it
	// promotes forward to its embedded interface, which is judged: a real one
	// (viaWrappedReal) and a normalized one (viaWrappedReassigned, the
	// periodicBatchContext shape) are silent; a zero one (viaWrappedZero) and
	// a nil parameter wrapped (viaWrappedParam) are reported.
	for _, want := range []string{"zeroField", "nilConstant", "relay", "captured", "viaResult", "viaMap", "(census.fixture.nilReader).readNil", "viaDynamicResult", "viaFuncValue", "viaIgnoredError", "viaCellLater", "viaOtherCell", "viaOverwrittenCell", "viaLazySkip", "viaLazyRecovered", "viaLazyDeferred", "viaLazyReset", "viaLazyCleared", "viaLazyAliased", "viaCellSkipped", "viaLazyExternalDefer", "viaWrappedZero", "viaWrappedParam", "viaMutableHolder"} {
		if got[want] == "" {
			t.Errorf("%s: nil not reported; got %v", want, got)
		}
		delete(got, want)
	}
	if len(got) != 0 {
		t.Errorf("non-nil contexts reported as nil: %v", got)
	}
}

// The product witness: FabricTransaction.BindContext must be non-nil. Its
// normal return is cleared by reaching definitions; its Recover block by the
// recover-path rule (the only deferred call is sync's Unlock, read from sync's
// source). The log records which rule clears each return and each deferred
// callee judged from source.
func TestMayBeNilBindContextRules(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	p, err := LoadProgram(Config{Dir: root, Patterns: []string{"./pkg/fabric"}, BuildFlags: []string{"-tags=noui,nolocalllm"}})
	if err != nil {
		t.Fatal(err)
	}
	var bind *ssa.Function
	for _, fn := range p.Functions {
		if fn.Pkg != nil && fn.Pkg.Pkg.Path() == "github.com/orneryd/nornicdb/pkg/fabric" && fn.Name() == "BindContext" {
			bind = fn
		}
	}
	if bind == nil {
		t.Fatal("FabricTransaction.BindContext not loaded")
	}
	flow := newNilFlow(p)
	for _, block := range bind.Blocks {
		ret, ok := block.Instrs[len(block.Instrs)-1].(*ssa.Return)
		if !ok {
			continue
		}
		if block == bind.Recover {
			if flow.recoverReachable(bind) {
				t.Fatal("BindContext's Recover block must be unreachable: its only deferred call is Unlock")
			}
			t.Logf("BindContext Recover-block return: cleared by the recover-path rule")
			continue
		}
		if w := flow.use(ret.Results[0], ret); w != nil {
			t.Fatalf("BindContext normal return reported: %v", w)
		}
		t.Logf("BindContext normal return: cleared by reaching definitions (the nil branch stores context.WithCancel's result; the non-nil branch is known non-nil)")
	}
	for callee, verdict := range flow.external {
		t.Logf("deferred callee %s judged from its package's source: %s", callee, verdict)
	}
	if got := flow.external["(*sync.Mutex).Unlock"]; got != "no recover call in its body" {
		t.Fatalf("(*sync.Mutex).Unlock must be read from sync's source; got %q", got)
	}
}
