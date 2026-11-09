package contract

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A canceled full-depth attempt has not retrieved the allowed population. A
// retry through the same public cursor must actually issue that step again,
// then complete, rather than converting the failed attempt into ErrCapacity.
//
// The cancellation is real and public: the caller-owned SearchOptions.Where
// admission callback cancels the request context while the search's overfetch
// pass filters the depth-8 attempt's candidates. With a capped population the
// completion pass cannot run (an explicit candidate cap ends the overfetch
// budget before it), so the overfetch post-filter is the frame this attempt
// reaches; the completion frame is proven by RunCancellation's
// completion_where probe. The failed attempt leaves execution.completed at the
// previous depth, so the retry issues 8 again instead of 16 or a capacity
// error.
//
// Layered proof, as in cancellation.go: the private scoring readers are covered
// in package search (TestFinalScoringCancellation,
// TestQuantizedFinalScoringCancellationReleasesReader), not here.

// whereProbe records where the admission callback ran. Route handlers may keep
// running on their own goroutine past a canceled request, so the counters are
// shared with the test goroutine under a lock.
type whereProbe struct {
	mu     sync.Mutex
	calls  int
	frames map[string]int
}

func newWhereProbe() *whereProbe { return &whereProbe{frames: map[string]int{}} }

func (p *whereProbe) observe(names []string) {
	site := "vector post-filter"
	for _, name := range names {
		if strings.Contains(name, ".adaptiveVectorSearch") || strings.Contains(name, ".completeVectorSearch") {
			site = name
			break
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.frames[site]++
}

func (p *whereProbe) snapshot() (int, map[string]int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	frames := make(map[string]int, len(p.frames))
	for site, calls := range p.frames {
		frames[site] = calls
	}
	return p.calls, frames
}

func RunRetry(t *testing.T, factory func(*testing.T, int, Policy) Fixture) {
	for _, policy := range Policies() {
		t.Run(policy.Name+"/retry_cancelled_full_step", func(t *testing.T) {
			f := factory(t, CandidateCap, policy)
			f.Service.SetIndexFlags(false, true)
			query := make([]float32, f.Service.VectorIndexDimensions())
			query[0] = 1
			for i := 0; i < CandidateCap; i++ {
				node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("retry-%02d", i)), ChunkEmbeddings: [][]float32{query}}
				_, err := f.Engine.CreateNode(t.Context(), node)
				require.NoError(t, err)
				require.NoError(t, f.Service.IndexNode(node))
			}
			policy.Prepare(t, f.Service)
			var issued []int
			var cancel context.CancelFunc
			armed := false
			where := newWhereProbe()
			producer := func(ctx context.Context, _ string, _ []float32, options *search.SearchOptions) (*search.SearchResponse, error) {
				issued = append(issued, options.Limit)
				opts := *options
				policy.Options(&opts)
				if armed {
					opts.Where = func(context.Context, *storage.Node) (bool, error) {
						where.observe(callerFunctions(2))
						cancel()
						return false, nil
					}
				}
				response, err := f.Service.Search(ctx, "", query, &opts)
				if err != nil {
					return nil, err
				}
				owned := *response
				owned.RetrievalExhausted = false
				owned.CandidateBudgetReached = false
				return &owned, nil
			}
			request := search.SearchContinuationRequest{Owner: f.Owner, Database: f.Database, N: 1}
			first, err := f.Service.SearchTextContinuation(context.Background(), "matrix", &search.SearchOptions{Limit: 4, MaxCandidateLimit: CandidateCap}, request, nil, nil, producer, search.ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
			require.True(t, first.HasMore)
			root, rootCancel := context.WithCancel(context.Background())
			defer rootCancel()
			cancel = rootCancel
			armed = true
			page, err := f.Pull(root, first.QID, 16)
			calls, frames := where.snapshot()
			require.Greater(t, calls, 0, "the supported Where callback must run during the canceled attempt")
			t.Logf("the canceled attempt reached Where from %v", frames)
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, page.IDs)
			armed = false
			last, err := f.Pull(context.Background(), first.QID, 16)
			require.NoError(t, err, "a canceled attempt must not consume the successful retrieval depth")
			require.Equal(t, []int{4, 8, 8}, issued)
			require.False(t, last.More)
			require.Equal(t, search.SearchContinuationCandidateComplete, last.Completion)
			require.Len(t, last.IDs, CandidateCap-1)
			for _, id := range last.IDs {
				require.NotEqual(t, first.Results[0].ID, id)
			}
		})
	}
}
