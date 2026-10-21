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

// Cancel at a real boundary reached by the public request, without sleeps or
// production test hooks. The decorated context remains an ordinary canceled
// context; the stack predicate merely decides when to cancel its parent.
type boundaryContext struct {
	context.Context
	cancel          context.CancelFunc
	target, exclude string
	once            sync.Once
	reached         bool
}

func (c *boundaryContext) touch() {
	pcs := make([]uintptr, 32)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	found, excluded := false, false
	for {
		frame, more := frames.Next()
		found = found || strings.Contains(frame.Function, c.target)
		excluded = excluded || (c.exclude != "" && strings.Contains(frame.Function, c.exclude))
		if !more {
			break
		}
	}
	if found && !excluded {
		c.once.Do(func() { c.reached = true; c.cancel() })
	}
}
func (c *boundaryContext) Done() <-chan struct{} { c.touch(); return c.Context.Done() }
func (c *boundaryContext) Err() error            { c.touch(); return c.Context.Err() }

func RunCancellation(t *testing.T, factory func(*testing.T, int, Policy) Fixture) {
	for _, policy := range Policies() {
		t.Run(policy.Name+"/cancel_at_stage", func(t *testing.T) {
			f := factory(t, 130, policy)
			f.Service.SetIndexFlags(false, true)
			query := make([]float32, f.Service.VectorIndexDimensions())
			query[0] = 1
			for i := 0; i < 130; i++ {
				node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("cancel-%03d", i)), Properties: map[string]any{"eligible": "yes"}, ChunkEmbeddings: [][]float32{query}}
				_, err := f.Engine.CreateNode(node)
				require.NoError(t, err)
				require.NoError(t, f.Service.IndexNode(node))
			}
			policy.Prepare(t, f.Service)
			stages := []struct {
				name, target, exclude string
				completion            bool
			}{
				{"issued_step", "continuationExecution).issue", "", false},
				{"reported_step", "continuationExecution).observe", "", false},
				{"stage_batch", ".scoreStageBatch", "", false},
				{"final_reader", ".withVectorScoreReader", "", false},
				{"completion_pool", ".completeVectorSearch", "", true},
				{"completion_reader", ".withVectorScoreReader", "", true},
				{"completion_consumer", ".scoreAllNodes", ".scoreFinal", true},
			}
			for _, stage := range stages {
				t.Run(stage.name, func(t *testing.T) {
					armed := false
					var signal *boundaryContext
					var cancelRequest context.CancelFunc
					producer := func(ctx context.Context, _ string, _ []float32, options *search.SearchOptions) (*search.SearchResponse, error) {
						opts := *search.DefaultSearchOptions()
						opts.Limit = options.Limit
						opts.CandidateTarget = 2
						policy.Options(&opts)
						if armed && stage.completion {
							opts.Filters = map[string][]string{"eligible": {"absent"}}
						}
						if armed && stage.name != "issued_step" && stage.name != "reported_step" {
							signal.Context = ctx
							ctx = signal
						}
						response, err := f.Service.Search(ctx, "", query, &opts)
						if err != nil {
							return nil, err
						}
						owned := *response
						response = &owned
						if !armed {
							require.GreaterOrEqual(t, len(response.Results), 2)
							response.Results = response.Results[:2]
						}
						response.RetrievalExhausted = false
						response.CandidateBudgetReached = false
						if armed && stage.name == "reported_step" {
							signal.reached = true
							cancelRequest()
						}
						return response, nil
					}
					first, err := f.Service.SearchTextContinuation(context.Background(), "matrix", &search.SearchOptions{Limit: 2}, search.SearchContinuationRequest{Owner: f.Owner, Database: f.Database, N: 1}, nil, nil, producer, search.ChunkedSearchErrorPolicy{})
					require.NoError(t, err)
					require.True(t, first.HasMore)
					armed = true
					root, cancel := context.WithCancel(context.Background())
					defer cancel()
					signal = &boundaryContext{Context: root, cancel: cancel, target: stage.target, exclude: stage.exclude}
					cancelRequest = cancel
					if stage.name == "issued_step" {
						signal.reached = true
						cancel()
					}
					page, err := f.Pull(root, first.QID, 256)
					require.True(t, signal.reached, "the public request must reach the selected scoring/completion boundary")
					require.ErrorIs(t, err, context.Canceled)
					require.Empty(t, page.IDs)
					// A normal write must acquire the readers just released by the
					// canceled request. A timeout is evidence of a leaked lease.
					done := make(chan error, 1)
					go func() {
						node, e := f.Engine.GetNode("cancel-000")
						if e == nil {
							e = f.Service.IndexNode(node)
						}
						done <- e
					}()
					select {
					case e := <-done:
						require.NoError(t, e)
					case <-time.After(time.Second):
						t.Fatal("canceled scoring retained a reader needed by writes")
					}
				})
			}
		})
	}
}
