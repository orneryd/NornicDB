package capturecensus

// Counterexamples from the 8d45 narrow review. Author: storage-format-owners
// lane (Claude), #1693, 31 October 2026.

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const routeEngine = "package fixture\nimport \"context\"\ntype Engine struct{}\nfunc (e *Engine) capture(ctx context.Context) error { return ctx.Err() }\ntype request struct{ ctx context.Context }\n"

func routeCensus(t *testing.T, body string, records map[string]ReviewedHeldValue) Inventory {
	t.Helper()
	c, p := fixture(t, routeEngine+body+"\n")
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
	p.ReviewedHeldValues = records
	i, err := Extract(c, p)
	require.NoError(t, err)
	return i
}

func routeDigest(src string) string {
	sum := sha256.Sum256([]byte(src))
	return hex.EncodeToString(sum[:])
}

// A method value of a public interface is the deferred public API, like the
// call it stands for: listed as unfulfilled with its holder, not a chain.
func TestCompiledMethodValueIsPublicCrossing(t *testing.T) {
	c, p := fixture(t, routeEngine+"type Store interface{ Put() error }\nfunc (e *Engine) Reads(ctx context.Context) error { return e.capture(ctx) }\nvar _ = (*Engine).Reads\n")
	require.NoError(t, os.MkdirAll(filepath.Join(c.Dir, "api"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(c.Dir, "api", "api.go"), []byte(`package api
import (
	"context"
	fixture "capture.fixture"
)
type wrapper struct{ e *fixture.Engine }
func (w *wrapper) Put() error { return w.e.Reads(context.Background()) }
func MethodValue(ctx context.Context, s fixture.Store) error { _ = ctx; put := s.Put; return put() }
func Wire(ctx context.Context, e *fixture.Engine) error { return MethodValue(ctx, &wrapper{e: e}) }
`), 0600))
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
	p.DeferredPublicAPI = ScopeEntry{Reason: "tracked public API", Approver: "control"}
	i, err := Extract(c, p)
	require.NoError(t, err)
	require.NotContains(t, strings.Join(i.Violations, "\n"), "(*capture.fixture/api.wrapper).Put <- ")
	require.NotZero(t, AcceptanceCounts(i.Acceptances)[KindUnfulfilled])
}

// A caller reached first through an edge where its parameter is proven nil is
// judged again on the edge where it is not.
func TestCounterexampleDiamondEdgeFacts(t *testing.T) {
	i := routeCensus(t, `func (e *Engine) leaf() error { return e.capture(context.Background()) }
func (e *Engine) a() error { return e.leaf() }
func (e *Engine) b() error { return e.leaf() }
func (e *Engine) route(ctx context.Context) error {
	if ctx == nil {
		return e.a()
	}
	return e.b()
}
var _ = (*Engine).route
`, nil)
	require.Contains(t, strings.Join(i.Violations, "\n"), "(*capture.fixture.Engine).leaf <- (*capture.fixture.Engine).b <- (*capture.fixture.Engine).route")
}

// A record whose pinned value is gone authorizes nothing, even when the
// function no longer holds any context.
func TestCounterexampleUnvalidatedRouteEnding(t *testing.T) {
	const fallback = "func (e *Engine) fallback(r request) error {\n\tctx := r.ctx\n\tif ctx == nil {\n\t\tctx = context.Background()\n\t}\n\treturn e.capture(context.WithValue(ctx, \"retained\", r.ctx))\n}"
	const edited = "func (e *Engine) fallback(r request) error {\n\tctx := context.Background()\n\t_ = r\n\treturn e.capture(ctx)\n}"
	const named = "\nfunc (e *Engine) passes(ctx context.Context) error { return e.fallback(request{ctx: ctx}) }\nvar _ = (*Engine).passes\n"
	symbol := "(*capture.fixture.Engine).fallback"
	record := routeRecord(fallback, symbol)
	require.Empty(t, routeCensus(t, fallback+named, map[string]ReviewedHeldValue{symbol: record}).Violations, "the current record ends the route")
	all := strings.Join(routeCensus(t, edited+named, map[string]ReviewedHeldValue{symbol: record}).Violations, "\n")
	require.Contains(t, all, "reviewed held-value record is stale", "the edited function must fail its record")
	require.Contains(t, all, "pinned held value gone")
}

// An ending does not hide a route into the functions above it that nothing
// establishes: the holder above an unnamed route is reported.
func TestCounterexampleEndingBeyondItsEvidence(t *testing.T) {
	const fallback = "func (e *Engine) fallback(r request) error {\n\tctx := r.ctx\n\tif ctx == nil {\n\t\tctx = context.Background()\n\t}\n\treturn e.capture(context.WithValue(ctx, \"retained\", r.ctx))\n}"
	const route = `
func (e *Engine) inner() error { return e.fallback(request{}) }
func (e *Engine) middle() error { return e.inner() }
func (e *Engine) outer() error { return e.middle() }
func (e *Engine) drain() error { return e.middle() }
func (e *Engine) newHolder(ctx context.Context) error { _ = ctx; return e.outer() }
var _ = []any{(*Engine).drain, (*Engine).newHolder}
`
	symbol := "(*capture.fixture.Engine).fallback"
	record := routeBoundaryRecord(fallback, symbol)
	all := strings.Join(routeCensus(t, fallback+route, map[string]ReviewedHeldValue{symbol: record}).Violations, "\n")
	require.Contains(t, all, "(*capture.fixture.Engine).newHolder", "a new request holder above an unnamed route is reported")
	require.NotContains(t, all, "(*capture.fixture.Engine).drain", "the named boundary ends its own route")
}
