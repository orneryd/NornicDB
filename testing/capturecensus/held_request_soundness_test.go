package capturecensus

// Author: storage-format-owners lane (Claude), #1693, 31 Oct 2026.

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Controls for what a function holds where it creates a fresh context
// (held.go). The census excludes only E1-E7; everything else a function holds
// is reported. So every shape that hid a held request from an earlier version
// of the census must fail, each exclusion still removes exactly its own shape,
// and the shapes the earlier versions excluded by wider proofs are now
// reported (over-reporting, the safe direction).
func TestCompiledHeldRequestSoundnessControls(t *testing.T) {
	c, p := fixture(t, `package fixture
import "context"
type Engine struct{ request context.Context }
func (e *Engine) capture(ctx context.Context) error { return ctx.Err() }
var parkedRequest context.Context
func installRequest(dst *context.Context) { *dst = parkedRequest }
func (e *Engine) hiddenThroughClosure() error {
	fresh := context.Background()
	held := fresh
	install := func() { installRequest(&held) }
	install()
	check := func() error { return held.Err() }
	if err := check(); err != nil {
		return err
	}
	return e.capture(fresh)
}
type slotTx struct {
	e   *Engine
	req context.Context
}
func (t *slotTx) installRequest() { t.req = parkedRequest }
func (t *slotTx) hiddenAfterReload() error {
	if t.req == nil {
		t.installRequest()
		held := t.req
		if err := held.Err(); err != nil {
			return err
		}
		return t.e.capture(context.Background())
	}
	return nil
}
func (e *Engine) selectedRequest(_ context.Context) context.Context { return e.request }
func (e *Engine) hiddenByHelper() error {
	fresh := context.Background()
	held := e.selectedRequest(fresh)
	if err := held.Err(); err != nil {
		return err
	}
	return e.capture(fresh)
}
func (e *Engine) normalizeWithHeldRequest(optional context.Context) error {
	if optional == nil {
		held := e.request
		if err := held.Err(); err != nil {
			return err
		}
		return e.capture(context.Background())
	}
	return e.capture(optional)
}
func heldFallback() context.Context { return context.Background() }
func (e *Engine) normalizeViaHeld(optional context.Context) error {
	if optional == nil {
		held := e.request
		if err := held.Err(); err != nil {
			return err
		}
		return e.capture(heldFallback())
	}
	return e.capture(optional)
}
func (e *Engine) Pick(a, b context.Context, chooseA bool) error {
	var selected context.Context
	if chooseA {
		selected = a
	} else {
		selected = b
	}
	defer func() {
		if selected != nil {
			_ = selected.Err()
		}
	}()
	if selected == nil {
		return e.capture(context.Background())
	}
	return e.capture(selected)
}
type plan struct {
	ctx context.Context
	n   int
}
func (e *Engine) admitPlan(p *plan) error {
	ctx := p.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return e.capture(ctx)
}
func (e *Engine) late(ctx context.Context) error {
	p := &plan{}
	err := e.admitPlan(p)
	p.ctx = ctx
	return err
}
type admitter interface{ admitPlan(*plan) error }
func invoke(ctx context.Context, target admitter) error { _ = ctx; return target.admitPlan(&plan{}) }
func (e *Engine) Dynamic(ctx context.Context) error { return invoke(ctx, e) }
func (e *Engine) inClosureParam() func(context.Context) error {
	return func(ctx context.Context) error { return e.capture(context.Background()) }
}
func (e *Engine) unthreadedInClosure() error { return e.capture(context.Background()) }
func (e *Engine) viaClosureParam() func(context.Context) error {
	return func(ctx context.Context) error { return e.unthreadedInClosure() }
}
func (e *Engine) ignored(_ context.Context) error { return e.capture(context.Background()) }
func peek(c *context.Context) error { return (*c).Err() }
func (t *slotTx) addressHeld() error {
	if err := peek(&t.req); err != nil {
		return err
	}
	return t.e.capture(context.Background())
}
func (e *Engine) normalizeOnly(optional context.Context) error {
	if optional == nil {
		optional = context.Background()
	}
	return e.capture(optional)
}
type sourceKey struct{}
func (e *Engine) derivedFresh() error {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), sourceKey{}, 1))
	defer cancel()
	return e.capture(ctx)
}
func (e *Engine) immutableCell() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		_ = e.capture(ctx)
	}()
}
func pairCtx() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) }
func (e *Engine) usesPair() error {
	ctx, cancel := pairCtx()
	defer cancel()
	return e.capture(ctx)
}
func (e *Engine) UsesPairTop(ctx context.Context) error { _ = ctx; return e.usesPair() }
func (e *Engine) unthreadedElsewhere() error { return e.capture(context.Background()) }
func (e *Engine) unrelatedClosure() (func(context.Context) error, error) {
	f := func(ctx context.Context) error { return ctx.Err() }
	return f, e.unthreadedElsewhere()
}
type writeCtx struct{ ctx context.Context }
func (e *Engine) consume(w *writeCtx) error { return e.capture(w.ctx) }
func (e *Engine) Literal() error { return e.consume(&writeCtx{ctx: context.Background()}) }
func (e *Engine) onlyFresh() error {
	fresh := context.Background()
	held := fresh
	reset := func() { held = fresh }
	reset()
	check := func() error { return held.Err() }
	if err := check(); err != nil {
		return err
	}
	return e.capture(fresh)
}
func (t *slotTx) unchangedReload() error {
	if t.req == nil {
		held := t.req
		_ = held
		return t.e.capture(context.Background())
	}
	return nil
}
func withSource(ctx context.Context) context.Context { return context.WithValue(ctx, sourceKey{}, 1) }
func (e *Engine) ParseVia() error { return e.capture(withSource(context.Background())) }
func plainFallback() context.Context { return context.Background() }
func (e *Engine) normalizeVia(optional context.Context) error {
	if optional == nil {
		optional = plainFallback()
	}
	return e.capture(optional)
}
func (e *Engine) supplied(ctx context.Context) error { return e.admitPlan(&plan{ctx: ctx, n: 1}) }
func fallbackForNil() context.Context { return context.Background() }
func (e *Engine) normalizeForNil(optional context.Context) error {
	if optional == nil {
		optional = fallbackForNil()
	}
	return e.capture(optional)
}
func (e *Engine) passesNil(ctx context.Context) error { _ = ctx; return e.normalizeForNil(nil) }
var foreign context.Context
func (e *Engine) foreignStore(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	set := func() { ctx = foreign }
	set()
	return e.capture(ctx)
}
func (e *Engine) deferredNormalize(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() { _ = ctx.Err() }()
	return e.capture(ctx)
}
var _ = []any{(*Engine).hiddenThroughClosure, (*slotTx).hiddenAfterReload, (*Engine).hiddenByHelper, (*Engine).normalizeWithHeldRequest, (*Engine).normalizeViaHeld, (*Engine).late, (*Engine).inClosureParam, (*Engine).viaClosureParam, (*Engine).ignored, (*slotTx).addressHeld, (*Engine).normalizeOnly, (*Engine).derivedFresh, (*Engine).immutableCell, (*Engine).unrelatedClosure, (*Engine).onlyFresh, (*slotTx).unchangedReload, (*Engine).normalizeVia, (*Engine).supplied, (*Engine).passesNil, (*Engine).foreignStore, (*Engine).deferredNormalize}
`)
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
	p.DeferredPublicAPI = ScopeEntry{Reason: "tracked public API", Approver: "control"}
	i, err := Extract(c, p)
	require.NoError(t, err)
	all := strings.Join(i.Violations, "\n")
	inScope := func(fn string) string {
		return " " + fn + " creates context.Background: a request context is in scope; pass it"
	}
	reasons := map[string]string{}
	for _, site := range i.ContextSites {
		if site.Reason != "" {
			reasons[site.Function] = site.Reason
		}
	}

	// Every shape that hid a held request from an earlier census version
	// fails: the 0070 review's four, the 3707 review's three, and the ones the
	// course change found.
	for fn, why := range map[string]string{
		"(*capture.fixture.Engine).hiddenThroughClosure":     "a closure writes the cell through its address",
		"(*capture.fixture.slotTx).hiddenAfterReload":        "a slot reloaded after a call that sets it",
		"(*capture.fixture.Engine).hiddenByHelper":           "a helper's result is not its argument",
		"(*capture.fixture.Engine).normalizeWithHeldRequest": "a nil parameter proves no other request absent",
		"(*capture.fixture.Engine).Pick":                     "one nil source of a variable proves nothing of the others",
		"(*capture.fixture.Engine).admitPlan":                "a fallback for an argument field holds the field: no caller is credited, so late writes and dynamic calls supply nothing",
		"(*capture.fixture.Engine).inClosureParam":           "a literal's request parameter",
		"(*capture.fixture.Engine).ignored":                  "an ignored blank request parameter",
		"(*capture.fixture.slotTx).addressHeld":              "an address handed to a reader",
		"(*capture.fixture.Engine).foreignStore":             "a closure stores a value that is not excluded into the variable",
	} {
		require.Contains(t, all, inScope(fn), why)
	}
	require.Contains(t, all, "capture.fixture.heldFallback <- (*capture.fixture.Engine).normalizeViaHeld", "a walk passes no nil normalizer holding another request")
	require.Contains(t, all, "capture.fixture.fallbackForNil <- (*capture.fixture.Engine).normalizeForNil <- (*capture.fixture.Engine).passesNil", "a nil normalizer is walked through, never pruned: its caller holding a request and passing nil substitutes it")
	require.Contains(t, all, "(*capture.fixture.Engine).unthreadedInClosure <- (*capture.fixture.Engine).viaClosureParam", "a literal's request parameter around the walked call")

	// Each exclusion removes exactly its own shape.
	noChain := "no-request-chain: no request context in scope or up its caller chains in the module"
	for fn, want := range map[string]string{
		"(*capture.fixture.Engine).normalizeOnly":     "no-request-chain: created on the nil branch of its own context parameter, holding nothing else: no request on this path", // E3, E4
		"(*capture.fixture.Engine).derivedFresh":      noChain,                                                                                                                   // E1, E5
		"(*capture.fixture.Engine).immutableCell":     noChain,
		"(*capture.fixture.Engine).onlyFresh":         noChain,                                                                                                                   // E6: every store is a fresh context
		"(*capture.fixture.Engine).deferredNormalize": "no-request-chain: created on the nil branch of its own context parameter, holding nothing else: no request on this path", // E3, E6: a deferred closure captures the normalized parameter
		"capture.fixture.plainFallback":               noChain,                                                                                                                   // E8: the normalizer's parameter is nil at its call, and nothing calls the normalizer                                                                                                                   // E5, E6
		"(*capture.fixture.Engine).Literal":           "api-boundary: exported API without a request context parameter, which no caller holding one in the module reaches",       // a write-only address
	} {
		require.Equal(t, want, reasons[fn], fn)
	}
	require.Contains(t, all, "capture.fixture.pairCtx <- (*capture.fixture.Engine).usesPair <- (*capture.fixture.Engine).UsesPairTop", "E7: a caller holding only a result its callee builds from fresh contexts holds no request")
	require.Equal(t, noChain, reasons["(*capture.fixture.Engine).unthreadedElsewhere"], "a literal's parameter away from the walked call is not in scope there")

	// Shapes earlier versions excluded by wider proofs are now reported.
	for fn, why := range map[string]string{
		"(*capture.fixture.slotTx).unchangedReload": "a field load holds the field",
		"(*capture.fixture.Engine).ParseVia":        "a helper result is held: no return provenance",
	} {
		require.Contains(t, all, inScope(fn), why)
	}
}

// Controls for the walk: a result is excluded by what its callee returns on
// every invocation, never because the call is the one walked; and an allocated
// holder hides no holder above it.
func TestCompiledWalkSoundnessControls(t *testing.T) {
	const engine = "type Engine struct{}\nfunc (e *Engine) capture(ctx context.Context) error { return ctx.Err() }\nvar parked context.Context\n"
	run := func(body string, records map[string]ReviewedHeldValue, cross map[string]CrossUnitOwner) Inventory {
		c, p := fixture(t, "package fixture\nimport \"context\"\n"+engine+body+"\n")
		p.Capture = []string{"(*capture.fixture.Engine).capture"}
		p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
		p.ReviewedHeldValues = records
		p.CrossUnitOwners = cross
		i, err := Extract(c, p)
		require.NoError(t, err)
		return i
	}
	digest := func(src string) string {
		sum := sha256.Sum256([]byte(src))
		return hex.EncodeToString(sum[:])
	}

	// Two calls of one accessor, its fallback legitimately reviewed: the first
	// result is a real request while the second is the fresh context.
	accessor := "func (l *lifecycle) commitContext() context.Context {\n\tif l.ctx == nil {\n\t\treturn context.Background()\n\t}\n\treturn l.ctx\n}"
	pair := "func (l *lifecycle) pair() (context.Context, error) {\n\tif l.ctx == nil {\n\t\treturn context.Background(), nil\n\t}\n\treturn l.ctx, nil\n}"
	lifecycle := "type lifecycle struct{ ctx context.Context }\n"
	record := func(name, src string) ReviewedHeldValue {
		symbol := "(*capture.fixture.lifecycle)." + name
		field := "load of field ctx of *capture.fixture.lifecycle"
		return ReviewedHeldValue{
			Held:     []string{field + " (+1 in " + symbol + ")", field + " (+4 in " + symbol + ")"},
			Sources:  map[string]string{symbol: digest(src)},
			Reason:   "the slot is nil on the fallback",
			Approver: "control",
		}
	}
	two := run(lifecycle+accessor+"\nfunc (e *Engine) useTwo(a, b *lifecycle) error {\n\theld := a.commitContext()\n\t_ = held.Err()\n\treturn e.capture(b.commitContext())\n}\nvar _ = (*Engine).useTwo\n",
		map[string]ReviewedHeldValue{"(*capture.fixture.lifecycle).commitContext": record("commitContext", accessor)}, nil)
	require.Contains(t, strings.Join(two.Violations, "\n"), "(*capture.fixture.lifecycle).commitContext <- (*capture.fixture.Engine).useTwo", "another invocation's result of the walked accessor is held")
	tuple := run(lifecycle+pair+"\nfunc (e *Engine) useTwoPair(a, b *lifecycle) error {\n\theld, _ := a.pair()\n\t_ = held.Err()\n\tother, _ := b.pair()\n\treturn e.capture(other)\n}\nvar _ = (*Engine).useTwoPair\n",
		map[string]ReviewedHeldValue{"(*capture.fixture.lifecycle).pair": record("pair", pair)}, nil)
	require.Contains(t, strings.Join(tuple.Violations, "\n"), "(*capture.fixture.lifecycle).pair <- (*capture.fixture.Engine).useTwoPair", "the same through a tuple result")

	// E7 removes a result only when every return of its callee is fresh.
	fresh := `func fresh() context.Context { return context.WithValue(context.Background(), parked, 1) }
func (e *Engine) holdsFresh() error { f := fresh(); _ = f; return e.capture(context.Background()) }
func cycleA(n int) context.Context { if n == 0 { return context.Background() }; return cycleB(n - 1) }
func cycleB(n int) context.Context { return cycleA(n) }
func (e *Engine) holdsCycle() error { f := cycleA(1); _ = f; return e.capture(context.Background()) }
var _ = []any{(*Engine).holdsFresh, (*Engine).holdsCycle}
`
	require.Empty(t, run(fresh, nil, nil).Violations, "fresh-only returns hold no request, on any invocation")
	mixed := `func mixed() context.Context { if parked != nil { return parked }; return context.Background() }
func (e *Engine) holdsMixed() error { f := mixed(); _ = f; return e.capture(context.Background()) }
func passes(ctx context.Context) context.Context { return ctx }
func (e *Engine) holdsPasses() error { f := passes(context.TODO()); _ = f; return e.capture(context.Background()) }
var _ = []any{(*Engine).holdsMixed, (*Engine).holdsPasses}
`
	all := strings.Join(run(mixed, nil, nil).Violations, "\n")
	require.Contains(t, all, " (*capture.fixture.Engine).holdsMixed creates context.Background: a request context is in scope; pass it", "one other return path is held")
	require.Contains(t, all, " (*capture.fixture.Engine).holdsPasses creates context.Background: a request context is in scope; pass it", "a returned parameter is held")

	// A tracked defect pinned to one occurrence covers only that one.
	trackedBody := "func (e *Engine) tracked() error {\n\t_ = e.capture(context.Background())\n\treturn e.capture(context.Background())\n}\nfunc (e *Engine) caller(ctx context.Context) error { _ = ctx; return e.tracked() }\nvar _ = (*Engine).caller\n"
	pinned := func(line string) Inventory {
		c, p := fixture(t, "package fixture\nimport \"context\"\n"+engine+trackedBody+"\n")
		p.Capture = []string{"(*capture.fixture.Engine).capture"}
		p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
		p.TrackedDefects = map[string]ScopeEntry{"(*capture.fixture.Engine).tracked": {Reason: "owned elsewhere", Approver: "control", Occurrences: []string{"call context.Background (+" + line + " in (*capture.fixture.Engine).tracked)"}}}
		i, err := Extract(c, p)
		require.NoError(t, err)
		return i
	}
	one := pinned("1")
	require.Equal(t, 1, AcceptanceCounts(one.Acceptances)[KindTrackedDefect], "the pinned occurrence stays listed and counted")
	require.Contains(t, strings.Join(one.Violations, "\n"), "(*capture.fixture.Engine).tracked <- (*capture.fixture.Engine).caller", "the other occurrence is still reported")
	require.Contains(t, strings.Join(pinned("9").Violations, "\n"), "(*capture.fixture.Engine).tracked: a ruled contract-scope entry matches no context site")

	// One report lists every holder the walk finds.
	both := run("func (e *Engine) shared() error { return e.capture(context.Background()) }\nfunc (e *Engine) mid(ctx context.Context) error { _ = ctx; return e.shared() }\nfunc (e *Engine) top(ctx context.Context) error { return e.mid(ctx) }\nvar _ = (*Engine).top\n", nil, nil)
	require.Contains(t, strings.Join(both.Violations, "\n"), "(*capture.fixture.Engine).shared <- (*capture.fixture.Engine).mid (also held at: (*capture.fixture.Engine).shared <- (*capture.fixture.Engine).mid <- (*capture.fixture.Engine).top)")
	require.Contains(t, strings.Join(both.Violations, "\n"), "[routes: (*capture.fixture.Engine).mid: held here | (*capture.fixture.Engine).mid <- (*capture.fixture.Engine).top]", "each direct caller lists every holder on or above it")

	// A holder with its own current reviewed record is listed and walked
	// past; the holder above it is reported, and a stale record is a holder.
	holderSrc := "func (e *Engine) reviewed() error {\n\t_ = parked.Err()\n\treturn e.leafReviewed()\n}"
	leafReviewed := "func (e *Engine) leafReviewed() error { return e.capture(context.Background()) }\n"
	parkedLoad := "load of global capture.fixture.parked (+1 in (*capture.fixture.Engine).reviewed)"
	holderRecord := ReviewedHeldValue{Held: []string{parkedLoad}, Sources: map[string]string{"(*capture.fixture.Engine).reviewed": digest(holderSrc)}, Reason: "reviewed", Approver: "control"}
	records := map[string]ReviewedHeldValue{"(*capture.fixture.Engine).reviewed": holderRecord}
	outerAbove := "func (e *Engine) outerReviewed(ctx context.Context) error { _ = ctx; return e.reviewed() }\nvar _ = (*Engine).outerReviewed\n"
	above2 := run(holderSrc+"\n"+leafReviewed+outerAbove, records, nil)
	require.Contains(t, strings.Join(above2.Violations, "\n"), "(*capture.fixture.Engine).leafReviewed <- (*capture.fixture.Engine).reviewed <- (*capture.fixture.Engine).outerReviewed", "a reviewed holder hides no holder above it")
	require.NotZero(t, AcceptanceCounts(above2.Acceptances)[KindReviewedHolder])
	staleHolder := holderRecord
	staleHolder.Sources = map[string]string{"(*capture.fixture.Engine).reviewed": digest(holderSrc + " ")}
	require.Contains(t, strings.Join(run(holderSrc+"\n"+leafReviewed+"var _ = (*Engine).reviewed\n", map[string]ReviewedHeldValue{"(*capture.fixture.Engine).reviewed": staleHolder}, nil).Violations, "\n"), "(*capture.fixture.Engine).leafReviewed <- (*capture.fixture.Engine).reviewed", "a stale record does not clear its holder")

	// A caller the record names ends its route; any other caller is walked.
	ended := "func (e *Engine) fallback(r request) error {\n\tctx := r.ctx\n\tif ctx == nil {\n\t\tctx = context.Background()\n\t}\n\treturn e.capture(ctx)\n}"
	endedBody := "type request struct{ ctx context.Context }\n" + ended + "\nfunc (e *Engine) passes(ctx context.Context) error { return e.fallback(request{ctx: ctx}) }\nfunc (e *Engine) drops(ctx context.Context) error { _ = ctx; return e.fallback(request{}) }\nvar _ = []any{(*Engine).passes, (*Engine).drops}\n"
	field := "load of field ctx of *capture.fixture.request (+1 in (*capture.fixture.Engine).fallback)"
	endRecord := ReviewedHeldValue{EndsRouteAt: []string{"(*capture.fixture.Engine).passes"}, Held: []string{field}, Sources: map[string]string{"(*capture.fixture.Engine).fallback": digest(ended)}, Reason: "named", Approver: "control"}
	endRun := strings.Join(run(endedBody, map[string]ReviewedHeldValue{"(*capture.fixture.Engine).fallback": endRecord}, nil).Violations, "\n")
	require.Contains(t, endRun, "(*capture.fixture.Engine).fallback <- (*capture.fixture.Engine).drops", "a caller the record does not name is reported")
	require.NotContains(t, endRun, "(*capture.fixture.Engine).fallback <- (*capture.fixture.Engine).passes")
	// A route filed as a finding is listed and counted, never a pass, and its
	// pinned source stales the record.
	tracked := endRecord
	tracked.TrackedRoutes = map[string]string{"(*capture.fixture.Engine).drops": "finding F"}
	trackedRun := run(endedBody, map[string]ReviewedHeldValue{"(*capture.fixture.Engine).fallback": tracked}, nil)
	require.Empty(t, trackedRun.Violations)
	require.Equal(t, 1, AcceptanceCounts(trackedRun.Acceptances)[KindTrackedDefect])
	pinnedRoute := tracked
	pinnedRoute.RouteSources = map[string]string{"(*capture.fixture.Engine).drops": "stale"}
	require.Contains(t, strings.Join(run(endedBody, map[string]ReviewedHeldValue{"(*capture.fixture.Engine).fallback": pinnedRoute}, nil).Violations, "\n"), "route source of (*capture.fixture.Engine).drops changed")

	// An allocated holder is listed, and walked past.
	leaf := "func (e *Engine) leaf() error { return e.capture(context.Background()) }\nfunc (e *Engine) allocated(ctx context.Context) error { _ = ctx; return e.leaf() }\n"
	owners := map[string]CrossUnitOwner{"(*capture.fixture.Engine).allocated": {Unit: "U-1", FixingPR: "#upper", Reason: "owned upstream", Approver: "control"}}
	above := run(leaf+"func (e *Engine) outer(ctx context.Context) error { return e.allocated(ctx) }\nvar _ = (*Engine).outer\n", nil, owners)
	require.Contains(t, strings.Join(above.Violations, "\n"), "(*capture.fixture.Engine).leaf <- (*capture.fixture.Engine).allocated <- (*capture.fixture.Engine).outer", "the unallocated holder above is reported")
	only := run(leaf+"var _ = (*Engine).allocated\n", nil, owners)
	require.Empty(t, only.Violations)
	require.Equal(t, 1, AcceptanceCounts(only.Acceptances)[KindCrossUnitOwner])
	for _, site := range only.ContextSites {
		if site.Function == "(*capture.fixture.Engine).leaf" {
			require.Contains(t, site.Reason, KindCrossUnitOwner+": ", "never reported as having no request")
		}
	}
}

// Adversarial control for R3: a package excluded as tooling holds no product
// request only while no product package imports it. A product package that
// imports it fails the census; the excluded package importing the product
// does not.
func TestCompiledExcludedPackageImportControl(t *testing.T) {
	c, p := fixture(t, `package fixture
import "context"
type Engine struct{}
func (e *Engine) capture(ctx context.Context) error { return ctx.Err() }
func (e *Engine) Reads(ctx context.Context) error { return e.capture(ctx) }
`)
	require.NoError(t, os.MkdirAll(filepath.Join(c.Dir, "tooling"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(c.Dir, "tooling", "tooling.go"), []byte(`package tooling
import (
	"context"
	fixture "capture.fixture"
)
func Make(e *fixture.Engine) error { return e.Reads(context.Background()) }
`), 0600))
	require.NoError(t, os.MkdirAll(filepath.Join(c.Dir, "product"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(c.Dir, "product", "product.go"), []byte(`package product
import (
	fixture "capture.fixture"
	"capture.fixture/tooling"
)
func Use(e *fixture.Engine) error { return tooling.Make(e) }
`), 0600))
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
	p.ExcludedPackages = map[string]ScopeEntry{"capture.fixture/tooling": {Reason: "test tooling", Approver: "control"}}
	i, err := Extract(c, p)
	require.NoError(t, err)
	all := strings.Join(i.Violations, "\n")
	require.Contains(t, all, "excluded package capture.fixture/tooling is imported by capture.fixture/product")
	require.NotContains(t, all, "is imported by capture.fixture/tooling")
	require.NotContains(t, all, "is imported by capture.fixture: ")
}

// Controls for reviewed held-value records, pinned to what was reviewed. A
// record naming the exact held values and the source of the site and of every
// function its reading relies on accepts the site as its own counted kind.
// Any change fails it as stale: the producer's body (wrap returning a stored
// request), the site's body, an added held value (a second call), a changed
// binding set of a callback, a changed library version; and a record
// matching no site fails.
func TestCompiledReviewedHeldValueControls(t *testing.T) {
	const (
		engine   = "type Engine struct{ request context.Context }\nfunc (e *Engine) capture(ctx context.Context) error { return ctx.Err() }\n"
		wrapped  = "type wrapped struct{ context.Context }\n"
		wrap     = "func wrap(ctx context.Context) context.Context { return wrapped{ctx} }"
		wrapLeak = "func wrap(ctx context.Context) context.Context {\n\tif parkedRequest != nil {\n\t\treturn wrapped{parkedRequest}\n\t}\n\treturn wrapped{ctx}\n}\nvar parkedRequest context.Context"
		recorded = "func (e *Engine) recorded(ctx context.Context) error {\n\tif ctx == nil {\n\t\tctx = context.Background()\n\t}\n\treturn e.capture(wrap(ctx))\n}"
		twice    = "func (e *Engine) recorded(ctx context.Context) error {\n\tif ctx == nil {\n\t\tctx = context.Background()\n\t}\n\t_ = wrap(ctx)\n\treturn e.capture(wrap(ctx))\n}"
		uses     = "var _ = []any{(*Engine).recorded}\n"
	)
	digest := func(src string) string {
		sum := sha256.Sum256([]byte(src))
		return hex.EncodeToString(sum[:])
	}
	held := []string{"call capture.fixture.wrap (+4 in (*capture.fixture.Engine).recorded)"}
	record := ReviewedHeldValue{
		Held:     held,
		Sources:  map[string]string{"(*capture.fixture.Engine).recorded": digest(recorded), "capture.fixture.wrap": digest(wrap)},
		Reason:   "wrap embeds its argument",
		Approver: "control",
	}
	run := func(body string, records map[string]ReviewedHeldValue) Inventory {
		c, p := fixture(t, "package fixture\nimport \"context\"\n"+engine+wrapped+body+"\n"+uses)
		p.Capture = []string{"(*capture.fixture.Engine).capture"}
		p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
		p.ReviewedHeldValues = records
		i, err := Extract(c, p)
		require.NoError(t, err)
		return i
	}
	reasonOf := func(i Inventory, fn string) string {
		for _, site := range i.ContextSites {
			if site.Function == fn {
				return site.Reason
			}
		}
		return ""
	}
	site := "(*capture.fixture.Engine).recorded"

	// Without a record the site fails; with the pinned record it is accepted
	// and counted.
	none := run(wrap+"\n"+recorded, nil)
	require.Contains(t, strings.Join(none.Violations, "\n"), " "+site+" creates context.Background: a request context is in scope; pass it")
	ok := run(wrap+"\n"+recorded, map[string]ReviewedHeldValue{site: record})
	require.Empty(t, ok.Violations)
	require.Equal(t, "no-request-chain: created on the nil branch of its own context parameter, holding nothing else: no request on this path", reasonOf(ok, site), "the record clears the held value; the site is then judged as usual")
	require.Equal(t, 1, AcceptanceCounts(ok.Acceptances)[KindReviewedHeld])

	stale := func(i Inventory, why string) {
		t.Helper()
		all := strings.Join(i.Violations, "\n")
		require.Contains(t, all, " "+site+" creates context.Background: reviewed held-value record is stale: ")
		require.Contains(t, all, why)
		require.Zero(t, AcceptanceCounts(i.Acceptances)[KindReviewedHeld])
	}
	// The producer now returns a stored request: its pinned source changed.
	stale(run(wrapLeak+"\n"+recorded, map[string]ReviewedHeldValue{site: record}), "source of capture.fixture.wrap changed")
	// A second call of the same producer: the site's source changed (and the
	// new call is held beyond the record).
	stale(run(wrap+"\n"+twice, map[string]ReviewedHeldValue{site: record}), "source of (*capture.fixture.Engine).recorded changed")
	// The same held values with the site's source pinned to the edited body:
	// the second call is held beyond the record.
	again := record
	again.Sources = map[string]string{site: digest(twice), "capture.fixture.wrap": digest(wrap)}
	stale(run(wrap+"\n"+twice, map[string]ReviewedHeldValue{site: again}), "held beyond the record")
	// A callback binding set changed.
	bound := record
	bound.Callers = map[string][]string{"capture.fixture.wrap": {"(*capture.fixture.Engine).recorded"}}
	require.Empty(t, run(wrap+"\n"+recorded, map[string]ReviewedHeldValue{site: bound}).Violations)
	stale(run("type viewer struct{ view func(context.Context) context.Context }\n"+wrap+"\n"+recorded+"\nfunc bindView(v *viewer) { v.view = wrap; _ = v.view(nil) }\nvar _ = bindView\n", map[string]ReviewedHeldValue{site: bound}), "callers of capture.fixture.wrap changed")
	// A pinned library version no longer required.
	versioned := record
	versioned.Modules = []string{"example.com/lib v1.0.0"}
	stale(run(wrap+"\n"+recorded, map[string]ReviewedHeldValue{site: versioned}), "module requirement changed: example.com/lib v1.0.0")
	// The wrapper's method set and the writers of its embedded context: a
	// declared override, or a second writer, fails the record.
	typed := record
	typed.Methods = map[string][]string{"capture.fixture.wrapped": {"Deadline promoted", "Done promoted", "Err promoted", "Value promoted"}}
	typed.FieldWriters = map[string][]string{"capture.fixture.wrapped.Context": {"capture.fixture.wrap"}}
	require.Empty(t, run(wrap+"\n"+recorded, map[string]ReviewedHeldValue{site: typed}).Violations)
	stale(run(wrap+"\n"+recorded+"\nfunc (w wrapped) Done() <-chan struct{} { return nil }\n", map[string]ReviewedHeldValue{site: typed}), "methods of capture.fixture.wrapped changed")
	stale(run(wrap+"\n"+recorded+"\nfunc other(c context.Context) context.Context { return wrapped{c} }\nvar _ = other\n", map[string]ReviewedHeldValue{site: typed}), "writers of capture.fixture.wrapped.Context changed")
	// A record clears only its held values: a caller holding a request is
	// still found by the walk.
	walked := "func (e *Engine) recordedWalk() error { return e.capture(wrap(context.Background())) }"
	walkRecord := ReviewedHeldValue{
		Held:     []string{"call capture.fixture.wrap (+0 in (*capture.fixture.Engine).recordedWalk)"},
		Sources:  map[string]string{"(*capture.fixture.Engine).recordedWalk": digest(walked), "capture.fixture.wrap": digest(wrap)},
		Reason:   "wrap embeds its argument",
		Approver: "control",
	}
	viaHolder := run(wrap+"\n"+recorded+"\n"+walked+"\nfunc (e *Engine) holdsRequest(ctx context.Context) error { _ = ctx; return e.recordedWalk() }\nvar _ = (*Engine).holdsRequest\n", map[string]ReviewedHeldValue{site: record, "(*capture.fixture.Engine).recordedWalk": walkRecord})
	require.Contains(t, strings.Join(viaHolder.Violations, "\n"), "(*capture.fixture.Engine).recordedWalk <- (*capture.fixture.Engine).holdsRequest")
	require.Equal(t, 2, AcceptanceCounts(viaHolder.Acceptances)[KindReviewedHeld])
	// A change below every pinned source stales the record through its closure.
	wrapVia := "func wrap(ctx context.Context) context.Context { return wrapped{inner(ctx)} }\n"
	inner := "func inner(ctx context.Context) context.Context { return ctx }"
	innerLeak := "func inner(ctx context.Context) context.Context {\n\tif parked != nil {\n\t\treturn parked\n\t}\n\treturn ctx\n}\nvar parked context.Context"
	via := record
	via.Sources = map[string]string{site: digest(recorded), "capture.fixture.wrap": digest(strings.TrimSuffix(wrapVia, "\n"))}
	via.Closure = "pending"
	const closureNow = "static closure changed: now "
	pending := strings.Join(run(wrapVia+inner+"\n"+recorded, map[string]ReviewedHeldValue{site: via}).Violations, "\n")
	require.Contains(t, pending, closureNow)
	via.Closure = pending[strings.Index(pending, closureNow)+len(closureNow):][:64]
	require.Empty(t, run(wrapVia+inner+"\n"+recorded, map[string]ReviewedHeldValue{site: via}).Violations)
	stale(run(wrapVia+innerLeak+"\n"+recorded, map[string]ReviewedHeldValue{site: via}), closureNow)
	// A record at a function creating no fresh context.
	gone := run(wrap+"\n"+recorded, map[string]ReviewedHeldValue{site: record, "capture.fixture.wrap": record})
	require.Contains(t, strings.Join(gone.Violations, "\n"), "capture.fixture.wrap: reviewed held-value record matches no site holding a context")
}
