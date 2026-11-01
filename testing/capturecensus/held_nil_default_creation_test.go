package capturecensus

// The generic nil-default creation shape and the shapes that must stay held,
// beside the argument-only call chains E9 now judges through (#144). The shape
// reproduced here is the one the parent-nil-default-input records report: a
// context parameter tested nil, a fresh context created on the nil branch only,
// and the same variable carrying the caller's request otherwise.

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

const nilDefaultFixture = `package fixture
import "context"
type Key struct{}
type Engine struct{ ctx context.Context }
func (e *Engine) capture(ctx context.Context) error { return ctx.Err() }
var parked context.Context
func store(dst *context.Context) { *dst = parked }
func pass(ctx context.Context) context.Context { return ctx }
func hop(ctx context.Context) context.Context { return pass(ctx) }
func storedResult(context.Context) context.Context { return parked }
func mixedResult(ctx context.Context, chooseStored bool) context.Context {
	if chooseStored {
		return parked
	}
	return ctx
}
func otherResult(a, b context.Context) context.Context { return b }
func dynResult(read func(context.Context) context.Context, ctx context.Context) context.Context { return read(ctx) }
func cycleA(ctx context.Context, n int) context.Context {
	if n == 0 {
		return ctx
	}
	return cycleB(ctx, n-1)
}
func cycleB(ctx context.Context, n int) context.Context { return cycleA(ctx, n) }
type lifetime struct{ ctx context.Context }
func (l *lifetime) Acquire(ctx context.Context) (context.Context, func(), bool) { return l.acquire(ctx, false) }
func (l *lifetime) acquire(ctx context.Context, request bool) (context.Context, func(), bool) {
	if request {
		return ctx, func() {}, false
	}
	return context.WithValue(ctx, Key{}, 1), func() {}, true
}
func (l *lifetime) Context() context.Context { return l.ctx }

func (e *Engine) guardDefault(optional context.Context) error {
	if optional == nil {
		optional = context.Background()
	}
	return e.capture(optional)
}
func (e *Engine) guardDefaultDerived(optional context.Context) error {
	if optional == nil {
		optional = context.Background()
	}
	callCtx, cancel := context.WithTimeout(optional, 0)
	defer cancel()
	return e.capture(callCtx)
}
func (e *Engine) guardDefaultCallee(optional context.Context) error {
	if optional == nil {
		optional = context.Background()
	}
	return e.capture(pass(optional))
}
func (e *Engine) guardDefaultChain(optional context.Context) error {
	if optional == nil {
		optional = context.Background()
	}
	return e.capture(hop(optional))
}
func (e *Engine) guardDefaultTupleChain(optional context.Context, l *lifetime) error {
	if optional == nil {
		optional = context.Background()
	}
	useCtx, release, ok := l.Acquire(optional)
	if !ok {
		return context.Canceled
	}
	defer release()
	return e.capture(useCtx)
}
func (e *Engine) guardDefaultDeferred(optional context.Context) error {
	if optional == nil {
		optional = context.Background()
	}
	defer func() { _ = optional.Err() }()
	return e.capture(optional)
}

func (e *Engine) replaceUnconditional(optional context.Context) error {
	optional = context.Background()
	return e.capture(optional)
}
func (e *Engine) replaceWrongVariable(other, request context.Context) error {
	if other == nil {
		request = context.Background()
	}
	_ = other
	return e.capture(request)
}
func (e *Engine) overwriteAlias(optional context.Context) error {
	if optional == nil {
		optional = context.Background()
	}
	store(&optional)
	return e.capture(optional)
}
func (e *Engine) mixedBranch(optional context.Context, chooseStored bool) error {
	ctx := optional
	if chooseStored {
		ctx = parked
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return e.capture(ctx)
}
func (e *Engine) nestedCallbackWritesCell(optional context.Context) error {
	if optional == nil {
		optional = context.Background()
	}
	set := func() { optional = parked }
	set()
	return e.capture(optional)
}
func (e *Engine) nestedCallbackHoldsRequest(optional context.Context) error {
	if optional == nil {
		optional = context.Background()
	}
	held := parked
	check := func(ctx context.Context) error {
		_ = held.Err()
		return e.capture(ctx)
	}
	return check(optional)
}
func (e *Engine) guardDefaultStoredResult(optional context.Context) error {
	if optional == nil {
		optional = context.Background()
	}
	if err := e.capture(optional); err != nil {
		return err
	}
	return e.capture(storedResult(optional))
}
func (e *Engine) guardDefaultMixedResult(optional context.Context) error {
	if optional == nil {
		optional = context.Background()
	}
	return e.capture(mixedResult(optional, parked != nil))
}
func (e *Engine) guardDefaultOtherParameter(optional, other context.Context) error {
	if optional == nil {
		optional = context.Background()
	}
	return e.capture(otherResult(optional, other))
}
func (e *Engine) guardDefaultDynamic(optional context.Context, read func(context.Context) context.Context) error {
	if optional == nil {
		optional = context.Background()
	}
	return e.capture(dynResult(read, optional))
}
func (e *Engine) guardDefaultCycle(optional context.Context) error {
	if optional == nil {
		optional = context.Background()
	}
	return e.capture(cycleA(optional, 2))
}
func (e *Engine) guardDefaultBeforeCreation(optional context.Context) error {
	early := pass(optional)
	if optional == nil {
		optional = context.Background()
	}
	_ = early.Err()
	return e.capture(optional)
}
var _ = []any{(*Engine).guardDefault, (*Engine).guardDefaultDerived, (*Engine).guardDefaultCallee,
	(*Engine).guardDefaultChain, (*Engine).guardDefaultTupleChain, (*Engine).guardDefaultDeferred,
	(*Engine).replaceUnconditional, (*Engine).replaceWrongVariable, (*Engine).overwriteAlias,
	(*Engine).mixedBranch, (*Engine).nestedCallbackWritesCell, (*Engine).nestedCallbackHoldsRequest,
	(*Engine).guardDefaultStoredResult, (*Engine).guardDefaultMixedResult, (*Engine).guardDefaultOtherParameter,
	(*Engine).guardDefaultDynamic, (*Engine).guardDefaultCycle, (*Engine).guardDefaultBeforeCreation}
`

const nilDefaultReason = "no-request-chain: created on the nil branch of its own context parameter, holding nothing else: no request on this path"

// censusReasons runs the census over body and returns each site's reason.
func censusReasons(t *testing.T, body string) (map[string]string, Inventory) {
	t.Helper()
	c, p := fixture(t, body)
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = append([]string(nil), p.Capture...)
	inv, err := Extract(c, p)
	require.NoError(t, err)
	reasons := map[string]string{}
	for _, site := range inv.ContextSites {
		reasons[site.Function] = site.Reason
	}
	return reasons, inv
}

// TestCompiledNilDefaultCreationShape reproduces the generic shape in its actual
// SSA form: the nil branch creates the fresh context and the non-nil branch
// reaches the work with the caller's value, so the creation is guarded by the
// function's own nil test and holds no request on the path that reaches it.
func TestCompiledNilDefaultCreationShape(t *testing.T) {
	c, _ := fixture(t, nilDefaultFixture)
	c.IncludeProgram = true
	rawConfig := c
	rawConfig.Selectors = []workcensus.Selector{{Node: "declaration", Kind: "declaration", Reachability: "all"}}
	raw, err := workcensus.Extract(rawConfig)
	require.NoError(t, err)
	var fn *ssa.Function
	for _, candidate := range raw.Program.Functions {
		if candidate.Parent() == nil && candidate.Synthetic == "" && candidate.Name() == "guardDefault" {
			fn = candidate
		}
	}
	require.NotNil(t, fn)
	require.Len(t, fn.Params, 2, "receiver and the context parameter")
	param := fn.Params[1]
	require.Len(t, fn.Blocks, 3, "entry, nil branch, join")

	var creation ssa.Value
	for _, block := range fn.Blocks {
		for _, instr := range block.Instrs {
			if value, ok := instr.(ssa.Value); ok && freshContext(value) {
				creation = value
			}
		}
	}
	require.NotNil(t, creation, "the shape creates a fresh context")
	iff, nilBranch := workcensus.NilSuccessEdge(param)
	require.NotNil(t, iff, "the shape tests its own context parameter nil")
	require.Same(t, nilBranch, creation.(ssa.Instruction).Block(), "the fresh context is created on the nil branch")
	require.True(t, workcensus.NilAt(param, creation.(ssa.Instruction).Block()))
	require.True(t, workcensus.ParameterNilAt(param, creation.(ssa.Instruction).Block()), "the parameter is nil where the fresh context is created")
	for _, successor := range iff.Block().Succs {
		if successor == nilBranch {
			continue
		}
		for _, instr := range successor.Instrs {
			if value, ok := instr.(ssa.Value); ok {
				require.False(t, freshContext(value), "the non-nil branch keeps the caller's context")
			}
		}
	}
	var merge *ssa.Phi
	for _, block := range fn.Blocks {
		for _, instr := range block.Instrs {
			if phi, ok := instr.(*ssa.Phi); ok && isContext(phi.Type()) {
				merge = phi
			}
		}
	}
	require.NotNil(t, merge, "the two paths join as a phi")
	require.Contains(t, merge.Edges, ssa.Value(param), "the non-nil edge carries the parameter itself")
	require.Contains(t, merge.Edges, creation, "the nil edge carries the fresh context")
}

// TestCompiledNilDefaultCreationCensus requires the guarded default to hold no
// request where it is created, and every wider shape to stay held.
func TestCompiledNilDefaultCreationCensus(t *testing.T) {
	reasons, inv := censusReasons(t, nilDefaultFixture)
	all := strings.Join(inv.Violations, "\n")
	inScope := func(fn string) string {
		return " " + fn + " creates context.Background: a request context is in scope; pass it"
	}
	for _, fn := range []string{
		"(*capture.fixture.Engine).guardDefault",
		"(*capture.fixture.Engine).guardDefaultDerived",
		"(*capture.fixture.Engine).guardDefaultCallee",
		"(*capture.fixture.Engine).guardDefaultChain",
		"(*capture.fixture.Engine).guardDefaultTupleChain",
		"(*capture.fixture.Engine).guardDefaultDeferred",
	} {
		require.Equal(t, nilDefaultReason, reasons[fn], fn)
		require.NotContains(t, all, inScope(fn), fn)
	}
	// The chain result is credited by E9's argument rule applied through a
	// static callee's own return, and the discharge records that proof.
	var chainProof bool
	for _, entry := range inv.Acceptances {
		if entry.Kind == KindProof && strings.Contains(entry.Key, "future argument-derived context result call (*capture.fixture.lifetime).Acquire") {
			chainProof = true
		}
	}
	require.True(t, chainProof, "the wrapper chain needs its exact argument-derived proof")
	// Every shape that holds a request of its own stays held.
	for fn, why := range map[string]string{
		"(*capture.fixture.Engine).replaceUnconditional":       "the caller's request is replaced unconditionally",
		"(*capture.fixture.Engine).replaceWrongVariable":       "a nil test of another parameter proves nothing of this one",
		"(*capture.fixture.Engine).overwriteAlias":             "an address handed to a writer hides a store",
		"(*capture.fixture.Engine).mixedBranch":                "one branch stores a request into the variable",
		"(*capture.fixture.Engine).nestedCallbackWritesCell":   "a closure stores a request into the variable",
		"(*capture.fixture.Engine).nestedCallbackHoldsRequest": "a closure holds a stored request of its own",
		"(*capture.fixture.Engine).guardDefaultStoredResult":   "the callee returns a stored request, not its argument",
		"(*capture.fixture.Engine).guardDefaultMixedResult":    "one return path of the callee is held",
		"(*capture.fixture.Engine).guardDefaultOtherParameter": "the callee returns a parameter the caller did not exclude",
		"(*capture.fixture.Engine).guardDefaultDynamic":        "a dynamic call has no static callee to judge",
		"(*capture.fixture.Engine).guardDefaultCycle":          "a chain that returns to itself proves nothing",
		"(*capture.fixture.Engine).guardDefaultBeforeCreation": "a call made before creation is judged by E7, which credits no argument",
	} {
		require.Contains(t, all, inScope(fn), why)
		require.Empty(t, reasons[fn], fn)
	}
}
