package capturecensus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

func TestReviewedResultContractSourceAndWriterControls(t *testing.T) {
	c, p := fixture(t, `package fixture
import("context";"sync")
type Engine struct{once sync.Once;ctx context.Context}
func NewEngine()*Engine{return &Engine{}}
func(e *Engine)init(){e.once.Do(func(){e.ctx=context.Background()})}
func(e *Engine)ownedContext()context.Context{e.init();return e.ctx}
func(e *Engine)capture(ctx context.Context)error{return ctx.Err()}
func(e *Engine)Direct()error{return e.capture(e.ownedContext())}
func(e *Engine)Raw()error{return e.capture(e.ctx)}
func(e *Engine)nullable(accepted bool)context.Context{if !accepted{return nil};return e.ownedContext()}
func(e *Engine)Optional(accepted bool)error{return e.capture(e.nullable(accepted))}
func(e *Engine)RequestLoss(request context.Context)error{_=request;return e.capture(context.Background())}
`)
	const symbol = "(*capture.fixture.Engine).ownedContext"
	p.Capture = []string{"(*capture.fixture.Engine).capture"}
	p.ContextOwners = append([]string(nil), p.Capture...)
	loaded := c
	loaded.IncludeProgram = true
	loaded.Selectors = []workcensus.Selector{{Node: "declaration", Kind: "declaration", Reachability: "all"}}
	inventory, err := workcensus.Extract(loaded)
	require.NoError(t, err)
	program := inventory.Program
	d := newSourceDigests(program)
	d.sourceFiles = inventory.Sources
	sources := reviewedPackageFunctions(program, "capture.fixture")
	review := ReviewedHeldValue{Sources: map[string]string{}, FieldWriters: map[string][]string{"capture.fixture.Engine.ctx": fieldWriters(program, "capture.fixture.Engine.ctx")}, Reason: "reviewed Once-backed init control", Approver: "planted test"}
	for _, source := range sources {
		review.Sources[source] = d.of(source)
	}
	review.Methods = map[string][]string{"capture.fixture.Engine": methodSet(program, "capture.fixture.Engine")}
	review.Closure = d.closure(sources)
	record := ReviewedNonNilResult{Index: 0, PackageFunctions: sources, PackageFiles: reviewedPackageSourceFiles(inventory.Sources, "capture.fixture"), Review: review}
	run := func(config workcensus.Config, key string, entry ReviewedNonNilResult) Inventory {
		t.Helper()
		policy := p
		policy.NonNilResults = map[string]ReviewedNonNilResult{key: entry}
		facts, err := Extract(config, policy)
		require.NoError(t, err)
		return facts
	}
	valid := run(c, symbol, record)
	validNil := strings.Join(nilContextFlows(valid), "\n")
	require.NotContains(t, validNil, ".Direct ->")
	require.Contains(t, validNil, ".Raw ->")
	require.Contains(t, validNil, ".Optional ->")
	require.Contains(t, strings.Join(valid.Violations, "\n"), ".RequestLoss creates context.Background: a request context is in scope")
	require.NotZero(t, AcceptanceCounts(valid.Acceptances)[KindReviewedResult], "a reviewed rule is counted separately from inferred proofs")
	controls := map[string]ReviewedNonNilResult{}
	stale := record
	stale.Review.Closure = "stale"
	controls["stale closure"] = stale
	writer := record
	writer.Review.FieldWriters = map[string][]string{"capture.fixture.Engine.ctx": {}}
	controls["wrong writer population"] = writer
	missing := record
	missing.Review.FieldWriters = nil
	controls["missing writer population"] = missing
	slot := record
	slot.Index = 1
	controls["wrong result index"] = slot
	for name, entry := range controls {
		t.Run(name, func(t *testing.T) {
			facts := run(c, symbol, entry)
			require.Contains(t, strings.Join(facts.Violations, "\n"), "non-nil result contract")
			require.Contains(t, strings.Join(nilContextFlows(facts), "\n"), ".Direct ->")
			require.Zero(t, AcceptanceCounts(facts.Acceptances)[KindReviewedResult])
		})
	}
	wrong := run(c, "(*capture.fixture.Engine).other", record)
	require.Contains(t, strings.Join(wrong.Violations, "\n"), "matches no exact source function")
	require.Contains(t, strings.Join(nilContextFlows(wrong), "\n"), ".Direct ->")
	added := c
	added.Overlay = map[string][]byte{filepath.Join(c.Dir, "new_writer.go"): []byte("package fixture\nfunc clearContext(e *Engine){e.ctx=nil}\n")}
	changed := run(added, symbol, record)
	require.Contains(t, strings.Join(changed.Violations, "\n"), "writers of capture.fixture.Engine.ctx changed")
	require.Contains(t, strings.Join(nilContextFlows(changed), "\n"), ".Direct ->")
	require.Zero(t, AcceptanceCounts(changed.Acceptances)[KindReviewedResult])
	aggregate := c
	aggregate.Overlay = map[string][]byte{filepath.Join(c.Dir, "new_aggregate_writer.go"): []byte("package fixture\nfunc resetWhole(e *Engine){*e=Engine{}}\n")}
	whole := run(aggregate, symbol, record)
	require.Contains(t, strings.Join(whole.Violations, "\n"), "provider package function population changed")
	require.Zero(t, AcceptanceCounts(whole.Acceptances)[KindReviewedResult], "a new whole-aggregate writer cannot inherit the former proof")
	anonymous := c
	anonymous.Overlay = map[string][]byte{filepath.Join(c.Dir, "anonymous_initializer.go"): []byte(`package fixture
var _ = func() bool {
 e:=NewEngine()
 e.init()
 *e=Engine{once:e.once}
 return true
}()
`)}
	initializer := run(anonymous, symbol, record)
	require.Zero(t, AcceptanceCounts(initializer.Acceptances)[KindReviewedResult], "an anonymous global initializer can poison the aggregate without introducing a declared function or direct ctx-field store")
	require.Contains(t, strings.Join(initializer.Violations, "\n"), "provider package source-file population/content changed")
	original, readErr := os.ReadFile(filepath.Join(c.Dir, "owner.go"))
	require.NoError(t, readErr)
	withinFile := c
	withinFile.Overlay = map[string][]byte{filepath.Join(c.Dir, "owner.go"): append(original, []byte(`
var _ = func() bool {
 e:=NewEngine()
 e.init()
 *e=Engine{once:e.once}
 return true
}()
`)...)}
	modified := run(withinFile, symbol, record)
	require.Zero(t, AcceptanceCounts(modified.Acceptances)[KindReviewedResult], "an initializer inserted into an existing source file must invalidate its content pin too")
	require.Contains(t, strings.Join(modified.Violations, "\n"), "provider package source-file population/content changed")
}
