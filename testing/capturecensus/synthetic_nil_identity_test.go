package capturecensus

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

const syntheticNilIdentityFixture = `package fixture
import "context"
type Engine struct{}
func(e *Engine)capture(ctx context.Context)error{return ctx.Err()}
type Promoted struct{*Engine}
type Reader interface{capture(context.Context)error}
func invoke(f func(context.Context)error){_=f(nil)}
func invokeMethod(f func(Reader,context.Context)error,r Reader){_=f(r,nil)}
func Bound(e *Engine,p *Promoted,r Reader){invoke(e.capture);invoke(p.capture);invoke(r.capture);invokeMethod(Reader.capture,r)}
func Root(){e:=&Engine{};Bound(e,&Promoted{e},e)}
`

func TestSyntheticNilContextCallsHaveDistinctIdentities(t *testing.T) {
	c, p := fixture(t, syntheticNilIdentityFixture)
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = append([]string(nil), p.Capture...)
	identities := func(config workcensus.Config) ([]string, map[string][]string) {
		t.Helper()
		program, err := workcensus.LoadProgram(config)
		require.NoError(t, err)
		owners := map[*ssa.Function]string{}
		seen := map[string]bool{}
		byFunction := map[string][]string{}
		var keys []string
		for _, function := range program.Functions {
			for _, block := range function.Blocks {
				for _, instruction := range block.Instrs {
					call, ok := instruction.(ssa.CallInstruction)
					if !ok {
						continue
					}
					for argument, value := range call.Common().Args {
						if !isContext(value.Type()) {
							continue
						}
						key := nilContextSite(program, call, argument, owners)
						if call.Pos().IsValid() {
							pos := program.FileSet.PositionFor(call.Pos(), false)
							require.Equal(t, fmt.Sprintf("owner.go:%d:%d", pos.Line, pos.Column), key, "reviewed physical keys must remain unchanged")
							continue
						}
						require.False(t, seen[key], "different source-less SSA flows must never collapse: %s", key)
						seen[key] = true
						require.NotContains(t, key, config.Dir, "workspace addresses cannot enter a stable identity")
						keys = append(keys, key)
						byFunction[function.String()] = append(byFunction[function.String()], key)
					}
				}
			}
		}
		sort.Strings(keys)
		return keys, byFunction
	}
	first, functions := identities(c)
	require.GreaterOrEqual(t, len(first), 6, "retain bound, promoted and interface compiler bodies")
	bound := functions["(*capture.fixture.Engine).capture$bound"]
	require.Len(t, bound, 2, "the compiler emitted two same-named bound wrappers with distinct source carriers")
	require.NotEqual(t, bound[0], bound[1])
	require.Len(t, functions["(capture.fixture.Reader).capture$bound"], 1)
	require.Len(t, functions["(capture.fixture.Reader).capture$thunk"], 1)
	second, _ := identities(c)
	require.Equal(t, first, second, "independent SSA loads must reproduce the exact identities")
	relocated, _ := fixture(t, syntheticNilIdentityFixture)
	third, _ := identities(relocated)
	require.Equal(t, first, third, "moving the module must preserve source-relative compiler identity")
	facts, err := Extract(c, p)
	require.NoError(t, err)
	require.NotContains(t, strings.Join(facts.Violations, "\n"), "nil-context use key is not unique to one call site")
	flows := strings.Join(nilContextFlows(facts), "\n")
	require.Equal(t, 2, strings.Count(flows, "(*capture.fixture.Engine).capture -> (*capture.fixture.Engine).capture: a nil context can reach it"), "both same-named forwarding bodies must retain their nil findings")
	require.Equal(t, 2, strings.Count(flows, "(capture.fixture.Reader).capture -> a dynamic call to (*capture.fixture.Engine).capture: a nil context can reach it"), "bound and thunk interface bodies must both remain visible")
}

func TestGenericNilContextCallsHaveDistinctIdentities(t *testing.T) {
	const source = `package fixture
import "context"
type Engine struct{}
func(e *Engine)capture(ctx context.Context)error{return ctx.Err()}
func forward[T any](e *Engine, value T){_=e.capture(nil)}
func Root(){e:=&Engine{};forward(e,1);forward(e,"two")}
`
	identities := func() []string {
		t.Helper()
		config, policy := fixture(t, source)
		policy.Capture = []string{"(*capture.fixture.Engine).capture"}
		policy.ContextOwners = append([]string(nil), policy.Capture...)
		program, err := workcensus.LoadProgram(config)
		require.NoError(t, err)
		var keys []string
		for _, function := range program.Functions {
			if function.Origin() == nil || function.Origin().Name() != "forward" {
				continue
			}
			for _, block := range function.Blocks {
				for _, instruction := range block.Instrs {
					call, ok := instruction.(ssa.CallInstruction)
					if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "capture" {
						continue
					}
					keys = append(keys, nilContextSite(program, call, 1, map[*ssa.Function]string{}))
				}
			}
		}
		require.Len(t, keys, 2, "both compiled generic instantiations remain in the population")
		require.NotEqual(t, keys[0], keys[1], "the shared source position must not collapse distinct instantiations")
		for _, key := range keys {
			require.NotContains(t, key, config.Dir)
		}
		facts, err := Extract(config, policy)
		require.NoError(t, err)
		require.NotContains(t, strings.Join(facts.Violations, "\n"), "nil-context use key is not unique")
		require.Len(t, nilContextFlows(facts), 3, "retain the generic source body and both compiled unsafe instantiations")
		sort.Strings(keys)
		return keys
	}
	require.Equal(t, identities(), identities(), "independent relocated loads preserve exact instantiation identities")
}

func TestGenericReceiverAndClosureNilContextIdentities(t *testing.T) {
	const source = `package fixture
import "context"
type Engine struct{}
func(e *Engine)capture(ctx context.Context)error{return ctx.Err()}
type Box[T any] struct{}
func(Box[T])forward(e *Engine){_=e.capture(nil)}
func closure[T any](e *Engine){f:=func(){_=e.capture(nil)};f()}
func Root(){e:=&Engine{};Box[int]{}.forward(e);Box[string]{}.forward(e);closure[int](e);closure[string](e)}
`
	config, _ := fixture(t, source)
	program, err := workcensus.LoadProgram(config)
	require.NoError(t, err)
	seen := map[string]string{}
	for _, function := range program.Functions {
		for _, block := range function.Blocks {
			for _, instruction := range block.Instrs {
				call, ok := instruction.(ssa.CallInstruction)
				if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "capture" {
					continue
				}
				key := nilContextSite(program, call, 1, map[*ssa.Function]string{})
				require.NotContains(t, seen, key, "distinct compiled receiver/closure owners: %s vs %s", seen[key], function)
				seen[key] = function.String()
			}
		}
	}
	require.Len(t, seen, 6, "retain source and both instantiations of receiver and closure calls")
}
