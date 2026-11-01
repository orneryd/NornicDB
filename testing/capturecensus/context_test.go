package capturecensus

// Author: storage-format-owners lane (Claude), #1693 takeover, 30 Oct 2026.

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

// Planted controls: a fresh context created where the function, an enclosing
// function, a direct caller or a caller several hops up in the package holds
// a request context must fail unless a cross-unit owner is recorded for it
// (then it is listed and counted). That includes a chain through a
// context-free exported function, as when a transaction's commit validates
// through the engine's exported reads. Fresh contexts at an exported boundary
// no caller in the package passes a request through, or in a context-free
// chain, are listed with their kind; a cross-unit record that matches no
// substitution fails. A fallback that reads a context from an argument
// field or a receiver slot holds that load: it is reported where it is
// created (the census does not prove what callers put there). A request
// holder that reaches a substitution only across the public API (another
// package's static call, or dynamic dispatch through Engine-like or optional
// interfaces) is unfulfilled Contract A against the tracked deferral, never an
// api-boundary acceptance; every cut edge is listed with its interface.
func TestCompiledContextSiteControls(t *testing.T) {
	c, p := fixture(t, `package fixture
import "context"
type Engine struct{}
func (e *Engine) capture(ctx context.Context) error { return ctx.Err() }
func (e *Engine) read(ctx context.Context) error { return e.capture(ctx) }
func (e *Engine) Boundary() error { return e.read(context.Background()) }
func (e *Engine) inScope(ctx context.Context) error { _ = ctx; return e.read(context.Background()) }
func (e *Engine) closure(ctx context.Context) error {
	f := func() error { return e.capture(context.TODO()) }
	_ = ctx
	return f()
}
func (e *Engine) derived() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = ctx
	return e.capture(context.Background())
}
func (e *Engine) unthreaded() error { return e.capture(context.Background()) }
func (e *Engine) Holder(ctx context.Context) error { _ = ctx; return e.unthreaded() }
func (e *Engine) quiet() error { return e.capture(context.Background()) }
func (e *Engine) Quiet() error { return e.quiet() }
func (e *Engine) Unrelated(ctx context.Context) error { _ = ctx; return context.Background().Err() }
func (e *Engine) deep() error { return e.capture(context.Background()) }
func (e *Engine) middle() error { return e.deep() }
func (e *Engine) Top(ctx context.Context) error { _ = ctx; return e.middle() }
func (e *Engine) owned() error { return e.capture(context.Background()) }
func (e *Engine) ownedMiddle() error { return e.owned() }
func (e *Engine) OwnedTop(ctx context.Context) error { _ = ctx; return e.ownedMiddle() }
func (e *Engine) StoredEdge() error { return e.read(context.Background()) }
type Tx struct{ e *Engine }
func (t *Tx) validateLocked() error { return t.e.StoredEdge() }
func (t *Tx) Commit(ctx context.Context) error { _ = ctx; return t.validateLocked() }
type plan struct{ ctx context.Context }
func (e *Engine) admit(p ...plan) error {
	var ctx context.Context
	if len(p) > 0 {
		ctx = p[0].ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return e.capture(ctx)
}
func (e *Engine) Plain() error { return e.admit() }
func (e *Engine) planned(ctx context.Context) error { return e.admit(plan{ctx: ctx}) }
func (e *Engine) dropped(ctx context.Context) error { _ = ctx; return e.admit() }
type loader interface{ Load() error }
func (e *Engine) Load() error { return e.read(context.Background()) }
func (e *Engine) viaLoader(ctx context.Context, l loader) error { _ = ctx; return l.Load() }
func (e *Engine) useLoader(ctx context.Context) error { return e.viaLoader(ctx, e) }
func (e *Engine) Read() error { return e.read(context.Background()) }
func (e *Engine) direct(ctx context.Context) error { _ = ctx; return e.Read() }
type sourceKey struct{}
func (e *Engine) Parse() error { ctx := context.WithValue(context.Background(), sourceKey{}, 1); return e.capture(ctx) }
func (e *Engine) External() error { return e.read(context.Background()) }
func (e *Engine) Write() error { return e.read(context.Background()) }
func (e *Engine) Lonely() error { return e.read(context.Background()) }
func (e *Engine) Reads(ctx context.Context) error { return e.read(ctx) }
type Store interface{ Put() error }
type writePlan struct {
	known   []string
	read    func() error
	batches map[string]int
	ctx     context.Context
}
type writeCtx struct {
	owner int
	ctx   context.Context
}
func (e *Engine) begin(id string, inherited *writeCtx, plans ...writePlan) error {
	var ctx context.Context
	if inherited != nil {
		ctx = inherited.ctx
	} else if len(plans) > 0 {
		ctx = plans[0].ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	request := ctx
	// Like beginEntityWrite's key admission, the closure captures plans, so
	// plans[0].ctx is read through the captured parameter's cell.
	admit := func() error {
		if len(plans) > 0 && plans[0].batches != nil {
			return nil
		}
		return e.capture(request)
	}
	_ = id
	return admit()
}
func (e *Engine) enqueue(ctx context.Context, id string) error { return e.begin(id, nil, writePlan{ctx: ctx}) }
func (e *Engine) erase(ctx context.Context, id string) error {
	return e.begin(id, nil, writePlan{read: func() error { return ctx.Err() }, ctx: ctx})
}
func (e *Engine) unplanned(ctx context.Context, id string) error { _ = ctx; return e.begin(id, nil, writePlan{known: []string{id}}) }
func (e *Engine) reassigned(ctx context.Context, id string) error {
	_ = ctx
	var plan writePlan
	plan = writePlan{known: []string{id}}
	return e.begin(id, nil, plan)
}
func (e *Engine) Shutdown() error { return e.begin("", &writeCtx{owner: 1, ctx: context.Background()}) }
func (e *Engine) beginCaptured(plans ...writePlan) error {
	var ctx context.Context
	if len(plans) > 0 {
		ctx = plans[0].ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	admit := func() error { return e.capture(ctx) }
	return admit()
}
func (e *Engine) timed(ctx context.Context) error {
	_ = ctx
	derived, cancel := context.WithCancel(context.Background())
	defer cancel()
	return e.capture(derived)
}
func fallbackCtx() context.Context { return context.Background() }
func (e *Engine) normalizedVia(ctx context.Context) error {
	if ctx == nil {
		ctx = fallbackCtx()
	}
	return e.capture(ctx)
}
func (e *Engine) heldVia(ctx context.Context) error { _ = ctx; return e.capture(fallbackCtx()) }
func (e *Engine) Backfill() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		_ = e.capture(ctx)
	}()
}
type slotTx struct {
	e   *Engine
	req context.Context
}
func (t *slotTx) request() context.Context {
	if t.req == nil {
		return context.Background()
	}
	return t.req
}
func (t *slotTx) prepare() error { return t.e.capture(t.request()) }
func (e *Engine) slotOwner(ctx context.Context) error { _ = ctx; t := &slotTx{e: e}; return t.prepare() }
var parkedRequest context.Context
func installRequest(dst *context.Context) { *dst = parkedRequest }
func (e *Engine) hiddenRequest() error {
	fresh := context.Background()
	held := fresh
	installRequest(&held)
	check := func() error { return held.Err() }
	if err := check(); err != nil {
		return err
	}
	return e.capture(fresh)
}
type slots struct{ a, b context.Context }
func (s *slots) pick(e *Engine) error {
	parent := s.a
	if parent == nil {
		parent = s.b
		if parent == nil {
			parent = context.Background()
		}
	}
	check := func() error { return parent.Err() }
	if err := check(); err != nil {
		return err
	}
	return e.capture(parent)
}
func (e *Engine) pickOwner(ctx context.Context) error { _ = ctx; s := &slots{}; return s.pick(e) }
var parked context.Context
func (e *Engine) closureStores() error {
	parent := context.Background()
	set := func() { parent = parked }
	set()
	return e.capture(parent)
}
func (e *Engine) capturedPlanned(ctx context.Context) error { return e.beginCaptured(writePlan{ctx: ctx}) }
func (e *Engine) capturedDropped(ctx context.Context) error { _ = ctx; return e.beginCaptured(writePlan{known: []string{"x"}}) }
var _ = []any{(*Engine).inScope, (*Engine).closure, (*Engine).derived, (*Engine).planned, (*Engine).dropped, (*Engine).useLoader, (*Engine).direct, (*Engine).enqueue, (*Engine).erase, (*Engine).unplanned, (*Engine).reassigned, (*Engine).capturedPlanned, (*Engine).capturedDropped, (*Engine).timed, (*Engine).normalizedVia, (*Engine).heldVia, (*Engine).slotOwner, (*Engine).hiddenRequest, (*Engine).pickOwner, (*Engine).closureStores}
`)
	require.NoError(t, os.MkdirAll(filepath.Join(c.Dir, "api"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(c.Dir, "api", "api.go"), []byte(`package api
import (
	"context"
	fixture "capture.fixture"
)
type Writer interface{ Write() error }
func Static(ctx context.Context, e *fixture.Engine) error { _ = ctx; return e.External() }
func Optional(ctx context.Context, w Writer) error { _ = ctx; return w.Write() }
func NoRequest(e *fixture.Engine) error { return e.Lonely() }
func UseOptional(ctx context.Context, e *fixture.Engine) error { return Optional(ctx, e) }
func outsideHelper(e *fixture.Engine) error { return e.Reads(context.Background()) }
type wrapper struct{ e *fixture.Engine }
func (w *wrapper) Put() error { return w.e.Reads(context.Background()) }
func UseStore(ctx context.Context, s fixture.Store) error { _ = ctx; return s.Put() }
func Wire(ctx context.Context, e *fixture.Engine) error { return UseStore(ctx, &wrapper{e: e}) }
func OutsideCaller(ctx context.Context, e *fixture.Engine) error { _ = ctx; return outsideHelper(e) }
`), 0600))
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
	p.DeferredPublicAPI = ScopeEntry{Reason: "tracked public API", Approver: "control"}
	p.CrossUnitOwners = map[string]CrossUnitOwner{
		"(*capture.fixture.Engine).owned": {Unit: "U-1", FixingPR: "#upper", Reason: "owned upstream", Approver: "control"},
		"(*capture.fixture.Engine).quiet": {Unit: "U-1", FixingPR: "#upper", Reason: "not needed", Approver: "control"},
		"(*capture.fixture.Engine).gone":  {Unit: "U-1", FixingPR: "#upper", Reason: "stale", Approver: "control"},
	}
	i, err := Extract(c, p)
	require.NoError(t, err)
	var flagged []string
	for _, v := range i.Violations {
		for _, fn := range []string{"inScope", "closure", "derived", "unthreaded", "deep", "quiet", "gone", "StoredEdge", "Read", "Load", "Parse", "admit", "begin", "beginCaptured"} {
			if strings.HasPrefix(v, "(*capture.fixture.Engine)."+fn+":") || strings.Contains(v, " (*capture.fixture.Engine)."+fn+" creates ") {
				flagged = append(flagged, fn)
			}
		}
	}
	sort.Strings(flagged)
	// quiet has no context up its chain, so its cross-unit record matches no
	// substitution; gone records a function that creates none.
	// A context derived only from a fresh one (derived) holds no request
	// (E1, E5). The argument-field fallbacks (admit, begin, beginCaptured)
	// hold the loaded field or reassigned variable: reported.
	require.Equal(t, []string{"Read", "StoredEdge", "admit", "begin", "beginCaptured", "closure", "deep", "gone", "inScope", "quiet", "unthreaded"}, flagged, "violations: %v", i.Violations)
	all := strings.Join(i.Violations, "\n")
	require.Contains(t, all, "(*capture.fixture.Engine).unthreaded <- (*capture.fixture.Engine).Holder")
	require.Contains(t, all, "(*capture.fixture.Engine).deep <- (*capture.fixture.Engine).middle <- (*capture.fixture.Engine).Top")
	require.Contains(t, all, "(*capture.fixture.Engine).StoredEdge <- (*capture.fixture.Tx).validateLocked <- (*capture.fixture.Tx).Commit")
	// The argument-field fallbacks are reported where they are created; no
	// caller is credited with supplying the field.
	require.Contains(t, all, " (*capture.fixture.Engine).admit creates context.Background: a request context is in scope; pass it")
	require.Contains(t, all, " (*capture.fixture.Engine).begin creates context.Background: a request context is in scope; pass it")
	require.NotContains(t, all, "leaves the context unset")
	// The fallback variable itself captured by a closure (a cell): the nil
	// analysis does not read the nil test on the cell as a guard, so the call
	// leaving the field unset fails as a nil flow from its literal (the
	// conservative direction), and the call setting it stays silent.
	require.NotContains(t, all, "capturedPlanned")
	require.Contains(t, all, "(*capture.fixture.Engine).beginCaptured -> (*capture.fixture.Engine).capture: a nil context can reach it: zero field ctx")
	// A fresh context reaching the reader only through a context.With*
	// derivation is still the fresh context, classified where it is created.
	require.Contains(t, all, " (*capture.fixture.Engine).timed creates context.Background: a request context is in scope; pass it")
	// A fallback helper reached by a request holder fails at that holder, a
	// nil normalizer calling it included (its parameter is a request on its
	// other branch). A function's own fresh context loaded by a closure it
	// starts (Backfill, an immutable cell) is that fresh context, not a
	// request.
	require.Contains(t, all, "capture.fixture.fallbackCtx <- (*capture.fixture.Engine).")
	require.NotContains(t, all, "(*capture.fixture.Engine).Backfill")
	// A receiver slot's fallback holds the slot it reads: reported where it
	// is created.
	require.Contains(t, all, " (*capture.fixture.slotTx).request creates context.Background: a request context is in scope; pass it")
	// A captured cell whose address escapes to a writer is not shown to hold
	// only the fresh context: the request it may hold is in scope.
	require.Contains(t, all, " (*capture.fixture.Engine).hiddenRequest creates context.Background: a request context is in scope; pass it")
	// Below the storage API, a request holder in another package substitutes
	// like one in storage: the walk stops only at storage's exported API, so a
	// helper's in-package caller holding a request fails, never unfulfilled.
	require.Contains(t, all, "capture.fixture/api.outsideHelper <- capture.fixture/api.OutsideCaller")
	// An implementation outside storage entered through a storage interface is
	// the same public API: unfulfilled, not a substitution below it.
	require.NotContains(t, all, "capture.fixture/api.wrapper).Put creates")
	// A local variable holding slots or a fallback (several stores) holds what
	// was stored: reported (pick), as is one a closure may store a request
	// into (closureStores).
	require.Contains(t, all, " (*capture.fixture.slots).pick creates context.Background: a request context is in scope; pass it")
	require.Contains(t, all, " (*capture.fixture.Engine).closureStores creates context.Background: a request context is in scope; pass it")
	// A request holder reaching the exported API only through an interface
	// (Load) is unfulfilled Contract A; called directly by a request holder in
	// the package it is a violation (Read). A context built only from fresh
	// contexts is not a request (Parse, derived).
	require.Contains(t, all, "(*capture.fixture.Engine).Read <- (*capture.fixture.Engine).direct")
	reasons := map[string]string{}
	for _, site := range i.ContextSites {
		if site.Reason != "" {
			reasons[site.Function] = site.Reason
		}
	}
	require.Equal(t, map[string]string{
		"(*capture.fixture.Engine).Boundary": "api-boundary: exported API without a request context parameter, which no caller holding one in the module reaches",
		"(*capture.fixture.Engine).owned":    "cross-unit-owner: context at (*capture.fixture.Engine).owned <- (*capture.fixture.Engine).ownedMiddle <- (*capture.fixture.Engine).OwnedTop; fixed by #upper in merge unit U-1: owned upstream",
		"(*capture.fixture.Engine).quiet":    "no-request-chain: no request context in scope or up its caller chains in the module",
		"(*capture.fixture.Engine).derived":  "no-request-chain: no request context in scope or up its caller chains in the module",
		"(*capture.fixture.Engine).Parse":    "api-boundary: exported API without a request context parameter, which no caller holding one in the module reaches",
		"(*capture.fixture.Engine).Load":     "unfulfilled-contract-a: Contract A unfulfilled: request held at (*capture.fixture.Engine).viaLoader reaches it across the public API at (*capture.fixture.Engine).Load (dynamic dispatch through capture.fixture.loader.Load from capture.fixture); deferred: tracked public API",
		"(*capture.fixture.Engine).External": "unfulfilled-contract-a: Contract A unfulfilled: request held at capture.fixture/api.Static reaches it across the public API at (*capture.fixture.Engine).External (static call from capture.fixture/api); deferred: tracked public API",
		"(*capture.fixture.Engine).Write":    "unfulfilled-contract-a: Contract A unfulfilled: request held at capture.fixture/api.Optional reaches it across the public API at (*capture.fixture.Engine).Write (dynamic dispatch through capture.fixture/api.Writer.Write from capture.fixture/api); deferred: tracked public API",
		"(*capture.fixture.Engine).Backfill": "api-boundary: exported API without a request context parameter, which no caller holding one in the module reaches",
		"(*capture.fixture/api.wrapper).Put": "unfulfilled-contract-a: Contract A unfulfilled: request held at capture.fixture/api.UseStore reaches it across the public API at (*capture.fixture/api.wrapper).Put (dynamic dispatch through capture.fixture.Store.Put from capture.fixture/api); deferred: tracked public API",
		"(*capture.fixture.Engine).Lonely":   "api-boundary: exported API without a request context parameter, which no caller holding one in the module reaches",
		"(*capture.fixture.Engine).Shutdown": "api-boundary: exported API without a request context parameter, which no caller holding one in the module reaches",
	}, reasons)
	var crossings []string
	for _, crossing := range i.BoundaryCrossings {
		crossings = append(crossings, crossing.String())
	}
	require.Contains(t, crossings, "(*capture.fixture.Engine).Lonely <- capture.fixture/api.NoRequest (static call from capture.fixture/api): no request holder beyond it")
	require.Contains(t, crossings, "(*capture.fixture.Engine).Write <- capture.fixture/api.Optional (dynamic dispatch through capture.fixture/api.Writer.Write from capture.fixture/api): request held at capture.fixture/api.Optional")
	interfaces := CrossingInterfaces(i.BoundaryCrossings)
	require.NotZero(t, interfaces["capture.fixture/api.Writer.Write"], "an optional interface beyond Engine is in the population: %v", interfaces)
	require.NotZero(t, interfaces["capture.fixture.loader.Load"], "%v", interfaces)
	require.NotZero(t, interfaces["static"], "%v", interfaces)
	require.Equal(t, 4, AcceptanceCounts(i.Acceptances)[KindUnfulfilled])
	counts := AcceptanceCounts(i.Acceptances)
	require.Equal(t, 1, counts[KindCrossUnitOwner], "the cross-unit owner is counted on its own")
	require.Zero(t, counts[KindRecordedOwner])

	// Without a tracked deferral the unfulfilled sites fail the census.
	p.DeferredPublicAPI = ScopeEntry{}
	i, err = Extract(c, p)
	require.NoError(t, err)
	require.Contains(t, i.Violations, "4 substitutions reached only across the public API have no tracked deferral")
}

// Planted controls for a named owner context: its use on the owner's path is
// an owned-source acceptance; a fresh context created on that path, a use of
// the source off the path, and a use on the path where a request is held
// (publishHeld) all fail. An in-scope substitution with a cross-unit owner is
// listed and counted with that owner.
func TestCompiledContextSourceControls(t *testing.T) {
	c, p := fixture(t, `package fixture
import "context"
type Engine struct{ req context.Context }
func (e *Engine) capture(ctx context.Context) error { return ctx.Err() }
func drainContext() context.Context { return context.Background() }
func (e *Engine) drain() { e.publish(); e.stray(); e.publishHeld() }
func (e *Engine) publish() { _ = e.capture(drainContext()) }
func (e *Engine) publishHeld() {
	held := e.req
	if held.Err() != nil {
		return
	}
	_ = e.capture(drainContext())
}
func (e *Engine) stray() { _ = e.capture(context.Background()) }
func (e *Engine) Misuse() error { return e.capture(drainContext()) }
func (e *Engine) Applier(ctx context.Context) { _ = ctx; go func() { _ = e.capture(context.Background()) }() }
func (e *Engine) Drain() { e.drain() }
`)
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
	p.ContextSources = []ContextSource{{Source: "capture.fixture.drainContext", Root: workcensus.Root{Package: "capture.fixture", Name: "drain", Receiver: "Engine", Required: true}, Disposition: "drainer-owned", Reason: "not cancellable"}}
	p.CrossUnitOwners = map[string]CrossUnitOwner{
		"(*capture.fixture.Engine).Applier": {Unit: "U-acc", FixingPR: "#accepted", Reason: "accepted-command owner", Approver: "control"},
	}
	i, err := Extract(c, p)
	require.NoError(t, err)
	all := strings.Join(i.Violations, "\n")
	require.Contains(t, all, "(*capture.fixture.Engine).stray creates context.Background: a fresh context on the path of drain")
	require.Contains(t, all, "(*capture.fixture.Engine).Misuse -> (*capture.fixture.Engine).capture: capture.fixture.drainContext used outside the path of its owner")
	require.Contains(t, all, "(*capture.fixture.Engine).publishHeld -> (*capture.fixture.Engine).capture: capture.fixture.drainContext used where a request context is in scope; pass it")
	reasons := map[string]string{}
	for _, site := range i.ContextSites {
		if site.Reason != "" {
			reasons[site.Function] = site.Reason
		}
	}
	require.Equal(t, "owned-source: drainer-owned: not cancellable", reasons["(*capture.fixture.Engine).publish"])
	require.Equal(t, "cross-unit-owner: a request context is in scope; fixed by #accepted in merge unit U-acc: accepted-command owner", reasons["(*capture.fixture.Engine).Applier"])
	require.NotContains(t, all, "Engine).publish ->")
	require.NotContains(t, all, "Engine).Applier creates")
}

// Planted controls for nil contexts: passing a nil (a field left zero, a
// parameter chain, a constant, even into a function that normalizes it) fails
// with its witness; a nil normalized inside before use, or an accessor that
// never returns nil, does not. A variable whose address a closure hands to a
// writer (clearedInClosure) may hold anything that writer stores: its read is
// not judged by its visible stores alone.
func TestCompiledNilContextControls(t *testing.T) {
	c, p := fixture(t, `package fixture
import "context"
type Engine struct{}
type Tx struct{ e *Engine; commitCtx context.Context }
func (e *Engine) capture(ctx context.Context) error { return ctx.Err() }
func (e *Engine) history(ctx context.Context) error { return e.capture(ctx) }
func (tx *Tx) checkRaw() error { return tx.e.history(tx.commitCtx) }
func (tx *Tx) request() context.Context { if tx.commitCtx == nil { return context.Background() }; return tx.commitCtx }
func (tx *Tx) checkAccessor() error { return tx.e.history(tx.request()) }
func (e *Engine) normalize(ctx context.Context) error { if ctx == nil { ctx = context.Background() }; return e.capture(ctx) }
func (e *Engine) Normalized() error { return e.normalize(nil) }
func (e *Engine) Constant() error { return e.capture(nil) }
func NewTx(e *Engine) *Tx { return &Tx{e: e} }
type capturer interface{ capture(context.Context) error }
func ViaInterface(c capturer) error { return c.capture(nil) }
func UseCapturer(e *Engine) error { return ViaInterface(e) }
type provider interface{ Request() context.Context }
type nilProvider struct{}
func (nilProvider) Request() context.Context { return nil }
func (e *Engine) Provided(p provider) error { return e.capture(p.Request()) }
func UseProvider(e *Engine) error { return e.Provided(nilProvider{}) }
func clearCtx(dst *context.Context) { *dst = nil }
func (e *Engine) clearedInClosure(ctx context.Context) error {
	held := ctx
	wipe := func() { clearCtx(&held) }
	wipe()
	return e.capture(held)
}
var _ = []any{(*Tx).checkRaw, (*Tx).checkAccessor, (*Engine).clearedInClosure}
`)
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
	i, err := Extract(c, p)
	require.NoError(t, err)
	var flagged []string
	for _, v := range i.Violations {
		if strings.Contains(v, "a nil context can reach it") {
			flagged = append(flagged, v)
		}
	}
	all := strings.Join(flagged, "\n")
	require.Contains(t, all, "(*capture.fixture.Tx).checkRaw -> (*capture.fixture.Engine).history: a nil context can reach it: zero field commitCtx")
	require.Contains(t, all, "(*capture.fixture.Engine).Constant -> (*capture.fixture.Engine).capture: a nil context can reach it: nil")
	require.Contains(t, all, "(*capture.fixture.Engine).Normalized -> (*capture.fixture.Engine).normalize: a nil context can reach it: nil")
	// Audit #29: a nil through an interface call to an owner, and a nil
	// produced behind an interface method's result.
	require.Contains(t, all, "capture.fixture.ViaInterface -> a dynamic call to (*capture.fixture.Engine).capture: a nil context can reach it: nil")
	require.Contains(t, all, "(*capture.fixture.Engine).Provided -> (*capture.fixture.Engine).capture: a nil context can reach it: nil")
	require.Contains(t, all, "(*capture.fixture.Engine).clearedInClosure -> (*capture.fixture.Engine).capture: a nil context can reach it: the address of local held escapes to code that may write it")
	require.NotContains(t, all, "checkAccessor")
	require.NotContains(t, all, "(*capture.fixture.Engine).normalize -> ")
	require.Len(t, flagged, 7, "checkRaw's field, the history parameter it feeds, Constant's nil, Normalized's nil, the interface call's nil, the provider's nil result and the escaped variable: %v", flagged)
}

// Planted controls for the computed nil boundary: a context reader the native
// type reaches is in the family even when it reaches no listed owner (its nil
// fails); a reader reached only by another type is not (its nil is listed
// elsewhere); a function that only passes its context on, or compares it with
// nil, is not a reader.
func TestCompiledNilBoundaryControls(t *testing.T) {
	c, p := fixture(t, `package fixture
import "context"
type Engine struct{}
func (e *Engine) capture(ctx context.Context) error { return ctx.Err() }
func deadline(ctx context.Context) bool { _, ok := ctx.Deadline(); return ok }
func (e *Engine) Expires(ctx context.Context) bool { return deadline(ctx) }
func (e *Engine) Unset() bool { return deadline(nil) }
func passOn(ctx context.Context) bool { if ctx == nil { return false }; return deadline(ctx) }
func (e *Engine) Passes(ctx context.Context) bool { return passOn(ctx) }
type Remote struct{}
func remoteDeadline(ctx context.Context) bool { _, ok := ctx.Deadline(); return ok }
func (r *Remote) Unset() bool { return remoteDeadline(nil) }
`)
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
	i, err := Extract(c, p)
	require.NoError(t, err)
	require.Contains(t, i.NilContextBoundary, "capture.fixture.deadline", "a reader the native type reaches")
	require.Contains(t, i.NilContextBoundary, "(*capture.fixture.Engine).capture", "a listed owner")
	require.NotContains(t, i.NilContextBoundary, "capture.fixture.passOn", "passing on and nil tests are not reads")
	require.NotContains(t, i.NilContextBoundary, "capture.fixture.remoteDeadline", "a reader only another type reaches")
	all := strings.Join(i.Violations, "\n")
	require.Contains(t, all, "(*capture.fixture.Engine).Unset -> capture.fixture.deadline: a nil context can reach it")
	require.NotContains(t, all, "(*capture.fixture.Remote).Unset")
	require.Contains(t, strings.Join(i.NilContextsElsewhere, "\n"), "(*capture.fixture.Remote).Unset -> capture.fixture.remoteDeadline")
}

// Planted controls for reviewed nil dispositions: an entry approves exact uses
// of a witness root, each at one physical call site. An approved use is
// accepted and listed with its reason. Another use of the same root fails, also
// an identical call in the same function (it has its own site). An approved
// use, or a root, that matches no current flow fails as stale, and an entry
// naming no use fails.
func TestCompiledNilDispositionControls(t *testing.T) {
	c, p := fixture(t, `package fixture
import "context"
type Engine struct{}
func (e *Engine) capture(ctx context.Context) error { return ctx.Err() }
func (e *Engine) Constant() error { return e.capture(nil) }
func (e *Engine) Another() error { return e.capture(nil) }
func (e *Engine) Twice() error {
	_ = e.capture(nil)
	return e.capture(nil)
}
`)
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
	useOf := func(violation string) string {
		at := strings.Index(violation, "; use: ")
		if at < 0 {
			return ""
		}
		return strings.TrimSuffix(violation[at+len("; use: "):], "]")
	}

	first, err := Extract(c, p)
	require.NoError(t, err)
	uses := map[string][]string{}
	for _, v := range first.Violations {
		if use := useOf(v); use != "" {
			caller := use[strings.Index(use, " ")+1 : strings.Index(use, " -> ")]
			uses[caller] = append(uses[caller], use)
		}
	}
	require.Len(t, uses["(*capture.fixture.Engine).Constant"], 1)
	require.Len(t, uses["(*capture.fixture.Engine).Another"], 1)
	twice := uses["(*capture.fixture.Engine).Twice"]
	require.Len(t, twice, 2, "two identical calls in one function are two uses")
	require.NotEqual(t, twice[0], twice[1], "each use names its own call site")
	sort.Strings(twice)

	stale := strings.Replace(uses["(*capture.fixture.Engine).Constant"][0], "Constant", "Removed", 1)
	p.NilContextDispositions = map[string]NilDisposition{
		"nil context.Context": {Reason: "planted", Reviewer: "control", Uses: []string{uses["(*capture.fixture.Engine).Constant"][0], twice[0], stale}},
		"zero field gone":     {Reason: "stale", Reviewer: "control"},
	}
	second, err := Extract(c, p)
	require.NoError(t, err)
	all := strings.Join(second.Violations, "\n")
	require.Len(t, second.NilContextsDisposed, 2, "Constant's use and Twice's first call are accepted")
	for _, d := range second.NilContextsDisposed {
		require.Contains(t, d, "[disposition: planted; reviewer control]")
	}
	require.Contains(t, all, "use not approved by its disposition; use: "+uses["(*capture.fixture.Engine).Another"][0])
	require.Contains(t, all, "use not approved by its disposition; use: "+twice[1], "an identical call at another site inherits nothing")
	require.Contains(t, all, "nil-context disposition nil context.Context approved use matches no current flow: "+stale)
	require.Contains(t, all, "nil-context disposition zero field gone matches no current flow")
	require.Contains(t, all, "nil-context disposition zero field gone names no approved use")
	require.NotContains(t, all, "is not unique to one call site")
}

// Planted controls for the naive reconciliation: where the naive taint can
// bring nil to a boundary call and the nil analysis judges the value non-nil,
// the analysis's reason is counted as an acceptance: a model convention (an
// unmodelled channel receive) or a proof rule (a nil-test guard, or a value
// that is structurally non-nil, like executeInternal's withQueryParams wrapper).
func TestCompiledReconciliationControls(t *testing.T) {
	c, p := fixture(t, `package fixture
import "context"
type Engine struct{}
func (e *Engine) capture(ctx context.Context) error { return ctx.Err() }
func (e *Engine) Received(ch chan context.Context) error { return e.capture(<-ch) }
func (e *Engine) Guarded(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return e.capture(ctx)
}
func UseGuarded(e *Engine) error { return e.Guarded(nil) }
type paramsContext struct {
	context.Context
	params map[string]any
}
type box struct{ ctx context.Context }
func Reset(b *box) { b.ctx = nil }
func (e *Engine) Wrapped(ctx context.Context) error {
	b := &box{ctx: &paramsContext{Context: ctx}}
	return e.capture(b.ctx)
}
`)
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
	i, err := Extract(c, p)
	require.NoError(t, err)
	var keys []string
	for _, a := range i.Acceptances {
		if a.Kind == KindConvention || a.Kind == KindProof {
			keys = append(keys, a.Kind+" "+a.Key)
		}
	}
	all := strings.Join(keys, " | ")
	require.Contains(t, all, "model-convention convention: unmodelled value <-", "the channel receive is counted as a convention")
	require.Contains(t, all, "proof proof: nil-test guard", "the nil-test guard is counted as a proof")
	require.Contains(t, all, "proof proof: structurally non-nil *ssa.Alloc", "the wrapper's allocation is counted as a proof")
	require.NotContains(t, strings.Join(i.Violations, " | "), "without a recorded reason")
}

// Planted controls for the narrowing rules without a control above. Each
// rule removes exactly its own site, a site it does not cover still fails,
// and a record that matches nothing fails:
//   - out of contract, excluded package and tracked defect remove their site
//     (outside-boundary, tracked-defect); without the record the site is a
//     substitution, and an unmatched record is stale;
//   - a caller in an excluded package is no request holder, so the storage
//     API it calls is api-boundary rather than unfulfilled;
//   - a fallback reading an argument field holds that load (admitOne is
//     reported where it is created; no caller is credited);
//   - a caller whose only context is the callee's result holds no request,
//     so the walk continues to the real holder (helperCtx's chain reaches
//     UsesHelperTop through usesHelper).
func TestCompiledNarrowingRuleControls(t *testing.T) {
	c, p := fixture(t, `package fixture
import "context"
type Engine struct{}
func (e *Engine) capture(ctx context.Context) error { return ctx.Err() }
func (e *Engine) read(ctx context.Context) error { return e.capture(ctx) }
func (e *Engine) Reads(ctx context.Context) error { return e.read(ctx) }
func (e *Engine) Excl() error { return e.read(context.Background()) }
func (e *Engine) outOfScope(ctx context.Context) error { _ = ctx; return e.read(context.Background()) }
func (e *Engine) defective(ctx context.Context) error { _ = ctx; return e.read(context.Background()) }
type plan struct {
	ctx context.Context
	n   int
}
func (e *Engine) admitOne(p *plan) error {
	ctx := p.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return e.capture(ctx)
}
func (e *Engine) forwarder(p *plan) error { return e.admitOne(p) }
func (e *Engine) forwardHolder(ctx context.Context) error { _ = ctx; return e.forwarder(&plan{n: 1}) }
func (e *Engine) forwardPlanned(ctx context.Context) error { return e.forwarder(&plan{ctx: ctx, n: 1}) }
func helperCtx() context.Context { return context.Background() }
func (e *Engine) usesHelper() error { return e.capture(helperCtx()) }
func (e *Engine) UsesHelperTop(ctx context.Context) error { _ = ctx; return e.usesHelper() }
var _ = []any{(*Engine).outOfScope, (*Engine).defective, (*Engine).forwardHolder, (*Engine).forwardPlanned}
`)
	require.NoError(t, os.MkdirAll(filepath.Join(c.Dir, "tooling"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(c.Dir, "tooling", "tooling.go"), []byte(`package tooling
import (
	"context"
	fixture "capture.fixture"
)
func Hold(ctx context.Context, e *fixture.Engine) error { _ = ctx; return e.Excl() }
func Make(ctx context.Context, e *fixture.Engine) error { _ = ctx; return e.Reads(context.Background()) }
`), 0600))
	require.NoError(t, os.MkdirAll(filepath.Join(c.Dir, "authlike"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(c.Dir, "authlike", "authlike.go"), []byte(`package authlike
import (
	"context"
	fixture "capture.fixture"
)
func save(e *fixture.Engine) error { return e.Reads(context.Background()) }
func Authenticate(e *fixture.Engine) error { return save(e) }
`), 0600))
	require.NoError(t, os.MkdirAll(filepath.Join(c.Dir, "frontend"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(c.Dir, "frontend", "frontend.go"), []byte(`package frontend
import (
	"context"
	fixture "capture.fixture"
	"capture.fixture/authlike"
)
func Login(ctx context.Context, e *fixture.Engine) error { _ = ctx; return authlike.Authenticate(e) }
`), 0600))
	require.NoError(t, os.MkdirAll(filepath.Join(c.Dir, "entrylike"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(c.Dir, "entrylike", "entrylike.go"), []byte(`package entrylike
import (
	"context"
	fixture "capture.fixture"
)
func Redact(e *fixture.Engine) error { return e.Reads(context.Background()) }
func Other(e *fixture.Engine) error { return e.Reads(context.Background()) }
`), 0600))
	require.NoError(t, os.MkdirAll(filepath.Join(c.Dir, "logger"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(c.Dir, "logger", "logger.go"), []byte(`package logger
import (
	"context"
	fixture "capture.fixture"
	"capture.fixture/entrylike"
)
func Log(ctx context.Context, e *fixture.Engine) error { _ = ctx; _ = entrylike.Redact(e); return entrylike.Other(e) }
`), 0600))
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
	p.DeferredPublicAPI = ScopeEntry{Reason: "tracked public API", Approver: "control"}
	scoped := func(withRecords bool) Inventory {
		q := p
		q.OutOfContract, q.ExcludedPackages, q.TrackedDefects = map[string]ScopeEntry{}, map[string]ScopeEntry{}, map[string]ScopeEntry{}
		if withRecords {
			q.OutOfContract["(*capture.fixture.Engine).outOfScope"] = ScopeEntry{Reason: "ruled out", Approver: "control"}
			q.ExcludedPackages["capture.fixture/tooling"] = ScopeEntry{Reason: "test tooling", Approver: "control"}
			q.TrackedDefects["(*capture.fixture.Engine).defective"] = ScopeEntry{Reason: "filed", Approver: "control"}
			q.OutOfContract["(*capture.fixture.Engine).goneScope"] = ScopeEntry{Reason: "stale", Approver: "control"}
			q.PublicPackages = []string{"capture.fixture/authlike"}
			q.PublicEntries = []string{"capture.fixture/entrylike.Redact"}
		}
		i, err := Extract(c, q)
		require.NoError(t, err)
		return i
	}
	with := scoped(true)
	all := strings.Join(with.Violations, "\n")
	reasons := map[string]string{}
	for _, site := range with.ContextSites {
		reasons[site.Function] = site.Reason
	}
	require.Equal(t, "outside-boundary: out of contract: ruled out", reasons["(*capture.fixture.Engine).outOfScope"])
	require.Equal(t, "outside-boundary: excluded package: test tooling", reasons["capture.fixture/tooling.Make"])
	require.Equal(t, "tracked-defect: filed", reasons["(*capture.fixture.Engine).defective"])
	require.Equal(t, "api-boundary: exported API without a request context parameter, which no caller holding one in the module reaches", reasons["(*capture.fixture.Engine).Excl"], "a caller in an excluded package is no holder")
	require.True(t, strings.HasPrefix(reasons["capture.fixture/entrylike.Redact"], "unfulfilled-contract-a: Contract A unfulfilled: request held at capture.fixture/logger.Log reaches it across the public API at capture.fixture/entrylike.Redact"), "a listed public entry is the deferred boundary: %s", reasons["capture.fixture/entrylike.Redact"])
	require.Contains(t, all, " capture.fixture/entrylike.Other creates context.Background: a request context exists up the caller chain capture.fixture/entrylike.Other <- capture.fixture/logger.Log", "only the listed entry is a boundary, not its package")
	require.True(t, strings.HasPrefix(reasons["capture.fixture/authlike.save"], "unfulfilled-contract-a: Contract A unfulfilled: request held at capture.fixture/frontend.Login reaches it across the public API at capture.fixture/authlike.Authenticate"), "a declared public package's exported API is the deferred boundary: %s", reasons["capture.fixture/authlike.save"])
	require.Contains(t, all, "(*capture.fixture.Engine).goneScope: a ruled contract-scope entry matches no context site")
	require.Contains(t, all, " (*capture.fixture.Engine).admitOne creates context.Background: a request context is in scope; pass it")
	require.NotContains(t, all, "leaves the context unset")
	require.Contains(t, all, "capture.fixture.helperCtx <- (*capture.fixture.Engine).usesHelper <- (*capture.fixture.Engine).UsesHelperTop")
	for _, fn := range []string{"outOfScope", "defective", "Excl"} {
		require.NotContains(t, all, "(*capture.fixture.Engine)."+fn+" creates")
	}
	require.NotContains(t, all, "capture.fixture/tooling.Make creates")

	// Without the records each site is a substitution again, and the storage
	// API the excluded caller reaches is unfulfilled.
	without := scoped(false)
	all = strings.Join(without.Violations, "\n")
	require.Contains(t, all, " (*capture.fixture.Engine).outOfScope creates context.Background: a request context is in scope; pass it")
	require.Contains(t, all, " (*capture.fixture.Engine).defective creates context.Background: a request context is in scope; pass it")
	require.Contains(t, all, " capture.fixture/tooling.Make creates context.Background: a request context is in scope; pass it")
	require.Contains(t, all, "capture.fixture/authlike.save <- capture.fixture/authlike.Authenticate <- capture.fixture/frontend.Login", "an undeclared package's exported API is no boundary")
	require.Contains(t, all, " capture.fixture/entrylike.Redact creates context.Background: a request context exists up the caller chain capture.fixture/entrylike.Redact <- capture.fixture/logger.Log", "an unlisted entry is no boundary")
	reasons = map[string]string{}
	for _, site := range without.ContextSites {
		reasons[site.Function] = site.Reason
	}
	require.True(t, strings.HasPrefix(reasons["(*capture.fixture.Engine).Excl"], "unfulfilled-contract-a: Contract A unfulfilled: request held at capture.fixture/tooling.Hold"), "%s", reasons["(*capture.fixture.Engine).Excl"])
}

// Planted controls for the withdrawn drain boundary (R22): no owned source
// stops a request walk, whatever its source returns. A stored owner context
// (A) proves no lifetime by itself, and a fresh one (B) proves nothing: the
// walk from a listener's fresh context reaches the drain in both, and the
// drain, holding that context, is the holder named.
func TestCompiledDrainBoundaryControls(t *testing.T) {
	run := func(source string) Inventory {
		c, p := fixture(t, `package fixture
import "context"
type Engine struct {
	work     context.Context
	listener func() error
}
func (e *Engine) capture(ctx context.Context) error { return ctx.Err() }
func (e *Engine) workContext() context.Context { return e.work }
func freshContext() context.Context { return context.Background() }
func (e *Engine) drain() error { _ = e.capture(`+source+`()); return e.listener() }
func (e *Engine) Open() { e.listener = e.notifyHook }
func (e *Engine) notifyHook() error { return e.notify() }
func (e *Engine) notify() error { return e.capture(context.Background()) }
func (e *Engine) Write(ctx context.Context) error { _ = ctx; return e.drain() }
`)
		p.Capture = []string{"(*capture.fixture.Engine).capture"}
		p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
		symbol := "capture.fixture.freshContext"
		if source == "e.workContext" {
			symbol = "(*capture.fixture.Engine).workContext"
		}
		p.ContextSources = []ContextSource{{Source: symbol, Root: workcensus.Root{Package: "capture.fixture", Name: "drain", Receiver: "Engine", Required: true}, Disposition: "drain-owned", Reason: "control"}}
		i, err := Extract(c, p)
		require.NoError(t, err)
		return i
	}
	owned := run("e.workContext")
	require.Contains(t, strings.Join(owned.Violations, "\n"), "(*capture.fixture.Engine).notify <- (*capture.fixture.Engine).notifyHook <- (*capture.fixture.Engine).drain", "a drain on a stored owner context stops nothing")
	fresh := run("freshContext")
	require.Contains(t, strings.Join(fresh.Violations, "\n"), "(*capture.fixture.Engine).notify <- (*capture.fixture.Engine).notifyHook <- (*capture.fixture.Engine).drain", "a drain on context.Background stops nothing: the walk reaches the drain, which holds that context")
}
