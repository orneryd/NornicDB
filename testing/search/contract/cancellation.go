package contract

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Cancellation contract, and what each layer proves
// =================================================
//
// Every route fixture (HTTP, gRPC, Cypher db.retrieve) pulls the same
// service-owned cursor, so this file owns the public cancellation contract
// once: the request context is canceled at a supported point, the pull returns
// context.Canceled, the page is empty (no partial page), the canceled attempt
// does not advance the retrieved depth, the resumed pull returns healthy rows,
// and a normal write can acquire the readers the canceled request released.
//
// Three supported cancellation points are reachable from a public request:
//
//	issuance         the caller's request is canceled while the cursor has an
//	                 unissued pending step. The step must not be issued at all:
//	                 the search producer never runs for it, so no retrieval is
//	                 spent and no depth is consumed.
//	producer_return  the caller-supplied search producer returns after the real
//	                 Service.Search ran; the attempt it produced is never
//	                 reported as a completed step, and the resumed pull issues
//	                 that same depth again.
//	completion       SearchOptions.Where, the supported per-request admission
//	                 callback, cancels while completeVectorSearch applies the
//	                 completion page's filter batches. Same depth and robustness
//	                 contract as producer_return.
//
// The layer boundary, stated exactly
// ----------------------------------
// pkg/search/lifetime.go derives a child cancel context per accepted read so
// service shutdown also stops in-flight scoring. That child does not forward
// its own Err/Done polls to the decorated parent, and no public option installs
// a caller-supplied scorer or vector getter. The private scoring sub-boundaries
// are therefore NOT reachable from a route fixture (the HTTP route also derives
// its own timeout context in requestTimeoutMiddleware, so even a direct
// parent-context read cannot be observed from a route):
//
//	not reachable publicly              proven instead as a component contract
//	----------------------------------  --------------------------------------
//	scoreStageBatch                     TestFinalScoringCancellation/stage
//	                                    TestFinalScoringCancellationFromAnyRead/stage
//	                                    TestQuantizedFinalScoringCancellationReleasesReader/stage
//	withVectorScoreReader (final)       TestFinalScoringCancellation/final
//	                                    TestFinalScoringCancellationFromAnyRead/final
//	                                    TestExactPoolCancellationIsBounded
//	                                    TestQuantizedFinalScoringCancellationReleasesReader/final
//	withVectorScoreReader (completion)  TestFinalScoringCancellation/completion
//	                                    TestFinalScoringCancellationFromAnyRead/completion
//	                                    TestQuantizedFinalScoringCancellationReleasesReader/completion
//	scoreAllNodes completion pool        TestExactPoolCancellationIsBounded
//	                                    TestQuantizedPoolCancellationIsBounded
//	                                    (no partial pool escapes, reader released once)
//	scoreAllNodes completion consumer    only runs after a completed pool, so no
//	                                    deterministic in-pipeline cancellation
//	                                    lands inside it; it is covered as "no
//	                                    partial completion page" by the
//	                                    completion cases above
//
// Those component tests cancel from the actual vector read and while the
// quantized graph reader is pinned; they assert that no partial pool escapes and
// that the released reader is not held. The earlier stack-decorated probes in
// this file named the four private scoring stages but never emitted cancellation
// inside them: their failures were unreachable sensor assertions, not a product
// defect, and they are not reported as passing public injection anywhere. Only
// the three reachable points above are asserted here.
//
// The stack predicate below is not a production seam and not a timing trick:
// the decorated context is an ordinary request context whose parent the test
// cancels, and a supported product callback identifies the stage it is running
// in. The cancellation itself is the product's own, delivered to the same
// context the public route passes to Service.

// completionStage is the only Where/rejection site the completion probe fires
// from; the same callback's calls in the overfetch pass simply reject the
// candidate.
const completionStage = ".completeVectorSearch"

// trigger cancels the request context once, the first time a probe observes the
// selected stage. It records the frame that fired for evidence.
type trigger struct {
	cancel  context.CancelFunc
	mu      sync.Mutex
	reached bool
	site    string
}

func newTrigger(cancel context.CancelFunc) *trigger { return &trigger{cancel: cancel} }

func (t *trigger) fire(site string) {
	t.mu.Lock()
	if t.reached {
		t.mu.Unlock()
		return
	}
	t.reached, t.site = true, site
	t.mu.Unlock()
	t.cancel()
}

func (t *trigger) fireAt(names []string, stage string) {
	if stackContains(names, stage) {
		t.fire(names[0])
	}
}

func (t *trigger) status() (bool, string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.reached, t.site
}

// callerFunctions names the calling stack, immediate caller first. It is how a
// supported product callback tells the stage that invoked it apart: one request
// reaches the same admission predicate from the overfetch pass and from the
// completion pass.
func callerFunctions(skip int) []string {
	pcs := make([]uintptr, 32)
	n := runtime.Callers(skip+1, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	var names []string
	for {
		frame, more := frames.Next()
		names = append(names, frame.Function)
		if !more {
			return names
		}
	}
}

func stackContains(names []string, fragment string) bool {
	for _, name := range names {
		if strings.Contains(name, fragment) {
			return true
		}
	}
	return false
}

// cancelScript is the caller-supplied search producer handed to the
// continuation. Disarmed, it scripts the two-row page the matrix needs. Armed,
// it leaves the real Service.Search result intact and only adds the supported
// cancellation point of the running probe.
type cancelScript struct {
	t       *testing.T
	f       Fixture
	query   []float32
	policy  Policy
	armed   bool
	where   bool
	returns bool
	trigger *trigger
	issued  []int
}

func (s *cancelScript) search(ctx context.Context, _ string, _ []float32, options *search.SearchOptions) (*search.SearchResponse, error) {
	s.issued = append(s.issued, options.Limit)
	opts := *search.DefaultSearchOptions()
	opts.Limit = options.Limit
	// Complete the page from the exact pool: the completion pass runs when the
	// overfetch budget ends before the target is filled.
	opts.CandidateTarget = 2
	s.policy.Options(&opts)
	if s.armed && s.where {
		opts.Where = func(*storage.Node) bool {
			s.trigger.fireAt(callerFunctions(2), completionStage)
			return false
		}
	}
	response, err := s.f.Service.Search(ctx, "", s.query, &opts)
	if err != nil {
		return nil, err
	}
	owned := *response
	response = &owned
	if !s.armed {
		require.GreaterOrEqual(s.t, len(response.Results), 2)
		response.Results = response.Results[:2]
	}
	// The script controls evidence, not retrieval: every step still ran the
	// real search above.
	response.RetrievalExhausted = false
	response.CandidateBudgetReached = false
	if s.armed && s.returns {
		s.trigger.fire("producer return")
	}
	return response, nil
}

// start begins the continuation with the script disarmed, so the cursor has one
// more result than its first page.
func (s *cancelScript) start(t *testing.T) *search.SearchContinuationPage {
	s.armed, s.where, s.returns = false, false, false
	first, err := s.f.Service.SearchTextContinuation(context.Background(), "matrix", &search.SearchOptions{Limit: 2}, search.SearchContinuationRequest{Owner: s.f.Owner, Database: s.f.Database, N: 1}, nil, nil, s.search, search.ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.True(t, first.HasMore, "the public pull must consume an actual cursor")
	require.Len(t, first.Results, 1)
	return first
}

func RunCancellation(t *testing.T, factory func(*testing.T, int, Policy) Fixture) {
	for _, policy := range Policies() {
		t.Run(policy.Name+"/cancel_at_boundary", func(t *testing.T) {
			f := factory(t, 130, policy)
			f.Service.SetIndexFlags(false, true)
			query := make([]float32, f.Service.VectorIndexDimensions())
			query[0] = 1
			for i := 0; i < 130; i++ {
				node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("cancel-%03d", i)), ChunkEmbeddings: [][]float32{query}}
				_, err := f.Engine.CreateNode(t.Context(), node)
				require.NoError(t, err)
				require.NoError(t, f.Service.IndexNode(node))
			}
			policy.Prepare(t, f.Service)

			for _, probe := range []struct {
				name string
				// arm installs the probe's supported cancellation point.
				arm func(*cancelScript)
				// beforePull cancels the caller's request while the pending
				// step is still unissued.
				beforePull bool
				// observed is whether the cancellation is emitted from a product
				// read, so the trigger must record that it ran.
				observed bool
				// issued is the exact issued-depth history when the canceled pull
				// returns; resumed is the prefix after the resumed pull.
				issued, resumed []int
			}{
				{
					name:       "issuance",
					arm:        func(s *cancelScript) { s.armed = true },
					beforePull: true,
					issued:     []int{2},
					resumed:    []int{2, 4},
				},
				{
					name:     "producer_return",
					arm:      func(s *cancelScript) { s.armed, s.returns = true, true },
					observed: true,
					issued:   []int{2, 4},
					resumed:  []int{2, 4, 4},
				},
				{
					name:     "completion_where",
					arm:      func(s *cancelScript) { s.armed, s.where = true, true },
					observed: true,
					issued:   []int{2, 4},
					resumed:  []int{2, 4, 4},
				},
			} {
				t.Run(probe.name, func(t *testing.T) {
					script := &cancelScript{t: t, f: f, query: query, policy: policy}
					first := script.start(t)
					root, cancel := context.WithCancel(context.Background())
					defer cancel()
					script.trigger = newTrigger(cancel)
					probe.arm(script)
					if probe.beforePull {
						cancel()
					}
					page, err := f.Pull(root, first.QID, 256)

					if probe.observed {
						reached, site := script.trigger.status()
						require.True(t, reached, "the public request must reach the selected cancellation point")
						t.Logf("cancellation emitted at %s", site)
					}
					require.ErrorIs(t, err, context.Canceled)
					require.Empty(t, page.IDs, "a canceled request must not return a partial page")
					require.Equal(t, probe.issued, script.issued, "a canceled step must not be issued or reported, and must not advance the retrieved depth")

					script.armed, script.where, script.returns = false, false, false
					resumed, err := f.Pull(context.Background(), first.QID, 16)
					require.NoError(t, err, "the cursor must keep serving after a canceled pull")
					require.NotEmpty(t, resumed.IDs, "healthy results must resume after cancellation")
					require.GreaterOrEqual(t, len(script.issued), len(probe.resumed))
					require.Equal(t, probe.resumed, script.issued[:len(probe.resumed)], "the resumed pull must issue the canceled depth")

					// A normal write must acquire the readers just released by
					// the canceled request. A timeout is evidence of a leak.
					done := make(chan error, 1)
					go func() {
						node, err := f.Engine.GetNode(t.Context(), "cancel-000")
						if err == nil {
							err = f.Service.IndexNode(node)
						}
						done <- err
					}()
					select {
					case err := <-done:
						require.NoError(t, err)
					case <-time.After(time.Second):
						t.Fatal("canceled scoring retained a reader needed by writes")
					}
				})
			}
		})
	}
}
