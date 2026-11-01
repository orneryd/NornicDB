package capturecensus

import (
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestHeldCreationUsesAvailableValues(t *testing.T) {
	c, p := fixture(t, `package fixture
import "context"
type Engine struct{ ctx context.Context }
func(e *Engine) capture(ctx context.Context) error{return ctx.Err()}
func derive(ctx context.Context) context.Context{return ctx}
var unknownDerive func(context.Context) context.Context
func(e *Engine) futureUnknown()error{fresh:=context.Background();held:=unknownDerive(fresh);_ = held.Err();return e.capture(fresh)}
var parked context.Context
func storedResult(context.Context) context.Context {return parked}
func (e *Engine) futureStored()error{fresh:=context.Background();held:=storedResult(fresh);_ = held.Err();return e.capture(fresh)}
func (e *Engine) priorResult()error{held:=storedResult(nil);fresh:=context.Background();_ = held.Err();return e.capture(fresh)}
func (e *Engine) cycleResult(n int)error{for i:=0;i<n;i++{fresh:=context.Background();held:=derive(fresh);_ = held.Err();_ = e.capture(fresh)};return nil}
func (e *Engine) onlyFuture() error { fresh:=context.Background();later:=derive(fresh);return e.capture(later) }
func (e *Engine) nilDefault(ctx context.Context) error {if ctx==nil {ctx=context.Background()};later:=derive(ctx);return e.capture(later)}
func (e *Engine) liveCaller(ctx context.Context) error {fresh:=context.Background();_ = ctx.Err();return e.capture(fresh)}
func(e *Engine) priorHolder(ctx context.Context) error {held:=derive(ctx);fresh:=context.Background();_ = held.Err();return e.capture(fresh)}
func(e *Engine) captured(ctx context.Context) error {f:=func()error{fresh:=context.Background();_ = ctx.Err();return e.capture(fresh)};return f()}
func(e *Engine) unknownField() error {fresh:=context.Background();held:=e.ctx;_ = held.Err();return e.capture(fresh)}
var install func(*context.Context)
func(e *Engine) aliasWrite(ctx context.Context) error {held:=ctx;install(&held);fresh:=context.Background();_ = held.Err();return e.capture(fresh)}
func(e *Engine) mixed(ctx context.Context,ok bool) error {if ok{ctx=nil};fresh:=context.Background();_ = ctx;return e.capture(fresh)}
func(e *Engine) cycle(ctx context.Context,n int)error {var held context.Context;for i:=0;i<n;i++ {fresh:=context.Background();held=derive(ctx);_ = held.Err();_ = e.capture(fresh)};return nil}
var _=[]any{(*Engine).futureUnknown,(*Engine).futureStored,(*Engine).priorResult,(*Engine).cycleResult,(*Engine).onlyFuture,(*Engine).nilDefault,(*Engine).liveCaller,(*Engine).priorHolder,(*Engine).captured,(*Engine).unknownField,(*Engine).aliasWrite,(*Engine).mixed,(*Engine).cycle}
`)
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = p.Capture
	c.IncludeProgram = true
	rawConfig := c
	rawConfig.Selectors = []workcensus.Selector{{Node: "declaration", Kind: "declaration", Reachability: "all"}}
	raw, loadErr := workcensus.Extract(rawConfig)
	require.NoError(t, loadErr)
	for _, fn := range raw.Program.Functions {
		if fn.Name() == "onlyFuture" || fn.Name() == "nilDefault" {
			var b strings.Builder
			fn.WriteTo(&b)
			t.Logf("%s SSA\n%s", fn.Name(), b.String())
		}
	}
	inv, err := Extract(c, p)
	require.NoError(t, err)
	all := strings.Join(inv.Violations, "\n")
	t.Log(all)
	for _, name := range []string{"onlyFuture", "nilDefault"} {
		require.NotContains(t, all, " "+"(*capture.fixture.Engine)."+name+" creates context.Background: a request context is in scope", name)
	}
	for _, name := range []string{"liveCaller", "priorHolder", "captured", "unknownField", "aliasWrite", "mixed", "cycle", "futureStored", "priorResult", "cycleResult", "futureUnknown"} {
		require.Contains(t, all, "(*capture.fixture.Engine)."+name+" creates context.Background: a request context is in scope", name)
	}
}
