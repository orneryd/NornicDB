package capturecensus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A nil test proves only the tested immutable SSA value nil. It does not
// prove a second field load nil, or erase an unrelated request held alongside it.
func TestContextCreationNilGuardUsesTypedSSAValue(t *testing.T) {
	c, p := fixture(t, `package fixture
import "context"
type Engine struct { ctx context.Context }
func (e *Engine) capture(ctx context.Context) error { return ctx.Err() }
func (e *Engine) GuardedField() error {
 held:=e.ctx
 if held==nil { return e.capture(context.Background()) }
 return e.capture(held)
}
func (e *Engine) GuardedFieldOther(request context.Context) error {
 held:=e.ctx
 if held==nil { _=request; return e.capture(context.Background()) }
 return e.capture(held)
}
func (e *Engine) GuardedFieldReload() error {
 held:=e.ctx
 if held==nil { return e.capture(context.WithValue(context.Background(),"retained",e.ctx)) }
 return e.capture(held)
}
func (e *Engine) UnconditionalLoss(ctx context.Context) error {
 _=ctx
 return e.capture(context.Background())
}
func (e *Engine) FilteredJoin(ctx context.Context,bypass bool) error {
 if bypass { goto fallback }
 if ctx!=nil { return e.capture(ctx) }
 fallback:
 return e.capture(context.Background())
}
`)
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = append([]string(nil), p.Capture...)
	facts, err := Extract(c, p)
	require.NoError(t, err)
	all := strings.Join(facts.Violations, "\n")
	require.NotContains(t, all, "(*capture.fixture.Engine).GuardedField creates context.Background: a request context is in scope")
	require.Contains(t, all, "(*capture.fixture.Engine).GuardedFieldOther creates context.Background: a request context is in scope")
	require.Contains(t, all, "(*capture.fixture.Engine).GuardedFieldReload creates context.Background: a request context is in scope")
	require.Contains(t, all, "(*capture.fixture.Engine).UnconditionalLoss creates context.Background: a request context is in scope")
	require.Contains(t, all, "(*capture.fixture.Engine).FilteredJoin creates context.Background: a request context is in scope", "a join reached without executing the nil test cannot prove the request nil")
}
