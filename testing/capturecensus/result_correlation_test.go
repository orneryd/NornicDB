package capturecensus

// Nil-result correlation: a call whose context result is nil only on its error
// returns is safe for a caller that tests that same error before the context is
// captured. The census does not assume that pairing for any (context, error)
// shape: it reads every return path of the callee, and it applies the caller's
// nil test only at the capture, where the value leaves its function. These
// plants prove the guarded shape is silent for that reason and that every
// unproved variant still reports.

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const resultCorrelationFixture = `package fixture

import (
	"context"
	"errors"
)

var problemRefused = errors.New("refused")

type Engine struct{}

func (e *Engine) keep(q context.Context) error { return q.Err() }

// paired leaves its receiver nil only with a non-nil problem.
func (e *Engine) paired(q context.Context, ok bool) (context.Context, func(), error) {
	if err := q.Err(); err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, problemRefused
	}
	q, undo := context.WithCancel(q)
	return q, undo, nil
}

// lenient also returns nothing at all on a successful call.
func (e *Engine) lenient(q context.Context, give bool) (context.Context, func(), error) {
	if !give {
		return nil, nil, nil
	}
	q, undo := context.WithCancel(q)
	return q, undo, nil
}

// forwarded hands back the caller's own problem with no context at all.
func (e *Engine) forwarded(q context.Context, problem error) (context.Context, error) {
	return nil, problem
}

// misplaced puts the problem in the middle of its results.
func (e *Engine) misplaced(q context.Context, ok bool) (context.Context, error, func()) {
	if !ok {
		return nil, problemRefused, func() {}
	}
	q, undo := context.WithCancel(q)
	return q, nil, undo
}

// dispatch has one paired and one lenient implementation.
type dispatch interface {
	admit(context.Context, bool) (context.Context, error)
}

type pairedAdmit struct{}

func (pairedAdmit) admit(q context.Context, ok bool) (context.Context, error) {
	if !ok {
		return nil, problemRefused
	}
	owned, _ := context.WithCancel(q)
	return owned, nil
}

type lenientAdmit struct{}

func (lenientAdmit) admit(q context.Context, ok bool) (context.Context, error) {
	return nil, nil
}

func (e *Engine) carry(q context.Context, fn func() error) error {
	_ = e.keep(q)
	return fn()
}

func (e *Engine) clear(target *context.Context) { *target = nil }

// guarded is the reviewed shape: the accepted context is captured by a closure
// after the same call's problem tested nil, and the variable it travels in was
// already holding the request.
func (e *Engine) guarded(q context.Context, ok bool) error {
	q, undo, problem := e.paired(q, ok)
	if problem != nil {
		return problem
	}
	defer undo()
	return e.carry(q, func() error { return e.keep(q) })
}

// secondUse reassigns the same variable twice, each time to a checked result.
func (e *Engine) secondUse(q context.Context, ok bool) error {
	q, undo, problem := e.paired(q, ok)
	if problem != nil {
		return problem
	}
	defer undo()
	next, undoNext, later := e.paired(q, true)
	if later != nil {
		return later
	}
	defer undoNext()
	q = next
	return e.carry(q, func() error { return e.keep(q) })
}

func (e *Engine) ignored(q context.Context, ok bool) error {
	q, undo, _ := e.paired(q, ok)
	defer undo()
	return e.carry(q, func() error { return e.keep(q) })
}

func (e *Engine) failingBranch(q context.Context, ok bool) error {
	q, undo, problem := e.paired(q, ok)
	defer undo()
	if problem != nil {
		return e.carry(q, func() error { return e.keep(q) })
	}
	return e.carry(q, func() error { return e.keep(q) })
}

func (e *Engine) beforeTest(q context.Context, ok bool) error {
	q, undo, problem := e.paired(q, ok)
	defer undo()
	_ = e.carry(q, func() error { return e.keep(q) })
	if problem != nil {
		return problem
	}
	return nil
}

func (e *Engine) clearedProblem(q context.Context, ok bool) error {
	q, undo, problem := e.paired(q, ok)
	defer undo()
	problem = nil
	if problem != nil {
		return problem
	}
	return e.carry(q, func() error { return e.keep(q) })
}

func (e *Engine) lenientUse(q context.Context, give bool) error {
	q, undo, problem := e.lenient(q, give)
	if problem != nil {
		return problem
	}
	defer undo()
	return e.carry(q, func() error { return e.keep(q) })
}

func (e *Engine) forwardedUse(q context.Context, problem error) error {
	q, problem = e.forwarded(q, problem)
	if problem != nil {
		return problem
	}
	return e.carry(q, func() error { return e.keep(q) })
}

func (e *Engine) misplacedUse(q context.Context, ok bool) error {
	q, problem, undo := e.misplaced(q, ok)
	if problem != nil {
		return problem
	}
	defer undo()
	return e.carry(q, func() error { return e.keep(q) })
}

func (e *Engine) dispatchedUse(d dispatch, q context.Context, ok bool) error {
	q, problem := d.admit(q, ok)
	if problem != nil {
		return problem
	}
	return e.carry(q, func() error { return e.keep(q) })
}

func (e *Engine) mutatedCapture(q context.Context, ok bool) error {
	q, undo, problem := e.paired(q, ok)
	if problem != nil {
		return problem
	}
	defer undo()
	return e.carry(q, func() error { q = nil; return e.keep(q) })
}

func (e *Engine) escapedCapture(q context.Context, ok bool) error {
	q, undo, problem := e.paired(q, ok)
	if problem != nil {
		return problem
	}
	defer undo()
	e.clear(&q)
	return e.carry(q, func() error { return e.keep(q) })
}

func Root() {
	var e Engine
	_ = e.guarded(context.Background(), true)
	_ = e.secondUse(context.Background(), true)
	_ = e.forwardedUse(context.Background(), nil)
	_ = e.dispatchedUse(pairedAdmit{}, context.Background(), true)
	_ = e.dispatchedUse(lenientAdmit{}, context.Background(), true)
	_ = e.misplacedUse(context.Background(), true)
}
`

var nilFlowCaller = regexp.MustCompile(`^[^ ]+:\d+ (\S+) -> `)

// nilFlowCallers groups the reported nil flows by the outermost function of the
// call they reach. A captured value is reported at the call inside the closure,
// so its own function names the plant that produced it.
func nilFlowCallers(flows []string) map[string][]string {
	byCaller := map[string][]string{}
	for _, flow := range flows {
		caller := "?"
		if match := nilFlowCaller.FindStringSubmatch(flow); match != nil {
			caller = match[1]
		}
		byCaller[caller] = append(byCaller[caller], flow)
	}
	return byCaller
}

func TestNilResultCorrelationProvesTheCaptureAndKeepsEveryOtherFlow(t *testing.T) {
	c, p := fixture(t, resultCorrelationFixture)
	p.Capture = []string{"(*capture.fixture.Engine).keep"}
	p.ContextOwners = append([]string(nil), p.Capture...)
	facts, err := Extract(c, p)
	require.NoError(t, err)
	byCaller := nilFlowCallers(nilContextFlows(facts))
	// The two proved shapes are silent, and the record says why: the error
	// return was excluded by the caller's own test, and the capture, not the
	// store, is where the value leaves its function.
	for _, silent := range []string{"(*capture.fixture.Engine).guarded", "(*capture.fixture.Engine).secondUse"} {
		require.Empty(t, byCaller[silent], "%s: the checked result is captured only after its own error tested nil", silent)
	}
	proofs := map[string]bool{}
	for _, acceptance := range facts.Acceptances {
		if acceptance.Kind == KindProof {
			proofs[acceptance.Key] = true
		}
	}
	for _, silent := range []string{"guarded", "secondUse"} {
		require.True(t, proofs["proof: return with a non-nil error excluded by the caller's check"], "the callee's error returns must be excluded by the caller's check")
		require.True(t, proofs["proof: reaching store before the capture of q of (*capture.fixture.Engine)."+silent], "the capture must be judged at the reaching store, not at the store: %v", proofs)
	}
	// Every variant whose pairing is not proved keeps its own flow.
	expected := []string{
		"ignored",
		"failingBranch",
		"beforeTest",
		"clearedProblem",
		"lenientUse",
		"forwardedUse",
		"misplacedUse",
		"dispatchedUse",
		"mutatedCapture",
		"escapedCapture",
	}
	for _, control := range expected {
		caller := "(*capture.fixture.Engine)." + control
		require.NotEmpty(t, byCaller[caller], "%s: an unproved result pairing must keep reporting", control)
	}
	// carry's own parameter reports the consumers that did reach it, and no
	// other function is invented.
	reported := make([]string, 0, len(byCaller))
	for caller := range byCaller {
		reported = append(reported, strings.TrimPrefix(strings.TrimPrefix(caller, "(*capture.fixture.Engine)."), "capture.fixture."))
	}
	sort.Strings(reported)
	want := append([]string{"carry"}, expected...)
	sort.Strings(want)
	require.Equal(t, want, reported, "only the unproved plants report")
	// The forwarder's flow names the caller's own problem, not a convention
	// about errors.
	require.Contains(t, strings.Join(byCaller["(*capture.fixture.Engine).forwardedUse"], "\n"), "result of (*capture.fixture.Engine).forwarded")
	require.Contains(t, strings.Join(byCaller["(*capture.fixture.Engine).lenientUse"], "\n"), "result of (*capture.fixture.Engine).lenient")
	require.Contains(t, strings.Join(byCaller["(*capture.fixture.Engine).dispatchedUse"], "\n"), "lenientAdmit).admit")
	require.Contains(t, strings.Join(byCaller["(*capture.fixture.Engine).escapedCapture"], "\n"), "the address of captured q escapes")
	// Nothing in the fixture is waived by a disposition or an owner record.
	counts := AcceptanceCounts(facts.Acceptances)
	for _, kind := range ForbiddenAcceptanceKinds {
		require.Zero(t, counts[kind])
	}
}

// TestNilResultCorrelationRealSources reads the current provider and search
// lifetime owners in their own packages: the same-call error test before each
// capture is a recorded proof. Actual-source mutation controls ensure that
// removing the result/error correlation restores the finding.
func TestNilResultCorrelationRealSources(t *testing.T) {
	const (
		provider = "result of (*github.com/orneryd/nornicdb/pkg/nornicdb.DB).beginProviderUse"
		reading  = "result of (*github.com/orneryd/nornicdb/pkg/search.Service).beginRead"
	)
	c := storageNilContextConfig(t, "./pkg/storage", "./pkg/nornicdb", "./pkg/search")
	facts, err := Extract(c, StoragePolicy())
	require.NoError(t, err)
	flows := nilContextFlows(facts)
	require.Empty(t, flows, "all current nil flows in the corrected source population must be resolved")
	t.Logf("nil flows=%d outside the boundary=%d", len(flows), len(facts.NilContextsElsewhere))
	for _, flow := range flows {
		require.NotContains(t, flow, provider, "a nil provider result reaches the boundary")
		require.NotContains(t, flow, reading, "a nil read result reaches the boundary")
	}
	proofs := map[string]int{}
	for _, acceptance := range facts.Acceptances {
		if acceptance.Kind == KindProof {
			proofs[acceptance.Key] += acceptance.Count
		}
	}
	for _, capture := range []string{
		"(*github.com/orneryd/nornicdb/pkg/nornicdb.DB).SearchDatabase",
		"(*github.com/orneryd/nornicdb/pkg/nornicdb.DB).HybridSearchWithOptions",
		"(*github.com/orneryd/nornicdb/pkg/search.Service).BuildIndexes",
		"(*github.com/orneryd/nornicdb/pkg/search.Service).VectorSearchCandidates",
	} {
		require.NotZero(t, proofs["proof: reaching store before the capture of ctx of "+capture], "the capture in %s must be judged at its reaching store: %v", capture, proofs)
	}
	require.NotZero(t, proofs["proof: return with a non-nil error excluded by the caller's check"], "the callee's error returns must be excluded by the caller's check")
	// The same correlation governs nil flows of code that reaches no boundary
	// reader: they stay listed there.
	for _, finding := range facts.NilContextsElsewhere {
		require.NotContains(t, finding, provider)
		require.NotContains(t, finding, reading)
	}
}

func TestNilResultCorrelationActualSourceMutant(t *testing.T) {
	c := storageNilContextConfig(t, "./pkg/storage", "./pkg/search")
	path := filepath.Join(c.Dir, "pkg/search/lifetime.go")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	const before = "if !accepted {\n\t\treturn nil, nil, context.Canceled\n\t}"
	require.Equal(t, 1, strings.Count(string(raw), before), "actual admission-refusal anchor changed")
	c.Overlay = map[string][]byte{path: []byte(strings.Replace(string(raw), before, "if !accepted {\n\t\treturn nil, nil, nil\n\t}", 1))}
	facts, err := Extract(c, StoragePolicy())
	require.NoError(t, err)
	var unpaired []string
	for _, flow := range nilContextFlows(facts) {
		if strings.Contains(flow, "result of (*github.com/orneryd/nornicdb/pkg/search.Service).beginRead") {
			unpaired = append(unpaired, flow)
		}
	}
	require.NotEmpty(t, unpaired, "an actual successful nil admission must not inherit the checked-error proof")
}
