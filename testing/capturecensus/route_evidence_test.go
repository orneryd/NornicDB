package capturecensus

// Counterexamples from the 99a7 narrow review. Author: storage-format-owners
// lane (Claude), #1693, 31 October 2026.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A named forwarding function does not hide a request holder above it.
func TestCounterexampleHolderAboveEnding(t *testing.T) {
	const accessor = "func (e *Engine) accessor(l *slot) context.Context {\n\tif l.ctx == nil {\n\t\treturn context.Background()\n\t}\n\treturn l.ctx\n}"
	symbol := "(*capture.fixture.Engine).accessor"
	field := "load of field ctx of *capture.fixture.slot"
	record := ReviewedHeldValue{
		EndsEdges: []RouteEdge{{Caller: "(*capture.fixture.Engine).forward", Callee: symbol, Reason: "takes no request"}},
		Held:      []string{field + " (+4 in " + symbol + ")"},
		Sources:   map[string]string{symbol: routeDigest(accessor), "(*capture.fixture.Engine).forward": routeDigest("func (e *Engine) forward(l *slot) error { return e.capture(e.accessor(l)) }")},
		Reason:    "the slot is nil on the fallback",
		Approver:  "control",
	}
	body := "type slot struct{ ctx context.Context }\n" + accessor + `
func (e *Engine) forward(l *slot) error { return e.capture(e.accessor(l)) }
func (e *Engine) requestCommit(ctx context.Context) error { _ = ctx; return e.forward(&slot{}) }
var _ = (*Engine).requestCommit
`
	all := strings.Join(routeCensus(t, body, map[string]ReviewedHeldValue{symbol: record}).Violations, "\n")
	require.Contains(t, all, "(*capture.fixture.Engine).requestCommit", "the holder above the named caller is reported")
}

// A named caller whose body changes to drop the request fails its old record.
func TestCounterexampleChangedBoundaryBody(t *testing.T) {
	const fallback = "func (e *Engine) fallback(r request) error {\n\tctx := r.ctx\n\tif ctx == nil {\n\t\tctx = context.Background()\n\t}\n\treturn e.capture(context.WithValue(ctx, \"retained\", r.ctx))\n}"
	symbol := "(*capture.fixture.Engine).fallback"
	const changed = "\nfunc (e *Engine) passes(ctx context.Context) error { _ = ctx; return e.fallback(request{}) }\nvar _ = (*Engine).passes\n"
	require.NotEmpty(t, routeCensus(t, fallback+changed, map[string]ReviewedHeldValue{symbol: routeRecord(fallback, symbol)}).Violations, "the named caller no longer passes its context")
}

// A same-name method value elsewhere in the caller does not turn a private
// interface call into the public API.
func TestCounterexampleUnrelatedMethodValue(t *testing.T) {
	c, p := fixture(t, routeEngine+"type Store interface{ Put() error }\nfunc (e *Engine) Reads(ctx context.Context) error { return e.capture(ctx) }\nvar _ = (*Engine).Reads\n")
	require.NoError(t, os.MkdirAll(filepath.Join(c.Dir, "api"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(c.Dir, "api", "api.go"), []byte(`package api
import (
	"context"
	fixture "capture.fixture"
)
type localStore interface{ Put(int) error }
type private struct{ e *fixture.Engine }
func (p *private) Put(n int) error { _ = n; return p.e.Reads(context.Background()) }
var Stored func() error
func Confused(ctx context.Context, public fixture.Store, local localStore) error {
	_ = ctx
	Stored = public.Put
	return local.Put(1)
}
func Wire(ctx context.Context, e *fixture.Engine, public fixture.Store) error { return Confused(ctx, public, &private{e: e}) }
`), 0600))
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = []string{"(*capture.fixture.Engine).capture"}
	p.DeferredPublicAPI = ScopeEntry{Reason: "tracked public API", Approver: "control"}
	i, err := Extract(c, p)
	require.NoError(t, err)
	require.Contains(t, strings.Join(i.Violations, "\n"), "(*capture.fixture/api.private).Put <- capture.fixture/api.Confused", "the private edge is walked")
}
