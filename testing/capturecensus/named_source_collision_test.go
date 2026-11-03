package capturecensus

import (
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestDistinctNamedSourcesAtOneCall(t *testing.T) {
	c, p := fixture(t, `package fixture
import "context"
type Engine struct{}
func first()context.Context{return context.Background()}
func second()context.Context{return context.Background()}
func(e *Engine) consume(a,b context.Context)error{if err:=a.Err();err!=nil{return err};return b.Err()}
func(e *Engine) run(mixed bool)error{if mixed{return e.consume(first(),second())};return e.consume(first(),first())}
func(e *Engine) other()error{return e.consume(second(),second())}
`)
	p.Capture = []string{"(*capture.fixture.Engine).consume"}
	p.ContextOwners = append([]string(nil), p.Capture...)
	p.ContextSources = []ContextSource{{Source: "capture.fixture.first", Root: workcensus.Root{Package: "capture.fixture", Name: "run", Receiver: "Engine", Required: true}, Disposition: "first-owned", Reason: "control"}, {Source: "capture.fixture.second", Root: workcensus.Root{Package: "capture.fixture", Name: "other", Receiver: "Engine", Required: true}, Disposition: "second-owned", Reason: "control"}}
	f, err := Extract(c, p)
	require.NoError(t, err)
	t.Logf("violations=%v", f.Violations)
	require.Contains(t, strings.Join(f.Violations, "\n"), "capture.fixture.second used outside the path of its owner other")
	require.Equal(t, 3, AcceptanceCounts(f.Acceptances)[KindOwnedSource], "same source twice at one call remains a single owned-source use")
	// With both sources owned by run, its mixed call accounts for both
	// distinct owners while its repeated-first call still accounts for one.
	p.ContextSources[1].Root.Name = "run"
	valid, err := Extract(c, p)
	require.NoError(t, err)
	runOwned := 0
	for _, a := range valid.Acceptances {
		if a.Kind == KindOwnedSource && strings.Contains(a.Key, ".run ->") {
			runOwned++
		}
	}
	require.Equal(t, 3, runOwned)
	for _, v := range valid.Violations {
		require.NotContains(t, v, ".run ->")
	}
}
