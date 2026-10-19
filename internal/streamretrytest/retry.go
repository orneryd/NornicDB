// Package streamretrytest checks the retry contract through public search paths.
package streamretrytest

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type Page struct {
	IDs  []string
	More bool
}

type Fixture struct {
	Service  *search.Service
	Engine   storage.Engine
	Owner    string
	Database string
	Pull     func(context.Context, string, int) (Page, error)
}

// Run cancels an expansion after a cursor has been issued. No page was produced
// or delivered by that attempt: the same qid must retry the same search depth.
func Run(t *testing.T, factory func(*testing.T) Fixture) {
	t.Helper()
	for _, failure := range []string{"cancelled", "deadline"} {
		t.Run(failure, func(t *testing.T) {
			f := factory(t)
			var results []search.SearchResult
			for _, id := range []string{"retry-a", "retry-b", "retry-c"} {
				_, err := f.Engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Properties: map[string]any{"retry_id": id}})
				require.NoError(t, err)
				results = append(results, search.SearchResult{ID: id, NodeID: storage.NodeID(id), Score: 1})
			}
			var issued []int
			armed := false
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			producer := func(step context.Context, _ string, _ []float32, options *search.SearchOptions) (*search.SearchResponse, error) {
				issued = append(issued, options.Limit)
				if armed {
					if failure == "cancelled" {
						cancel()
						return nil, step.Err()
					}
					// A search sub-step can time out while the outer request is live.
					return nil, fmt.Errorf("search step: %w", context.DeadlineExceeded)
				}
				count := min(options.Limit, len(results))
				return &search.SearchResponse{Results: results[:count], SearchMethod: "bm25", RetrievalExhausted: count == len(results)}, nil
			}
			first, err := f.Service.SearchTextContinuation(context.Background(), "retry", &search.SearchOptions{Limit: 2, MaxCandidateLimit: 4}, search.SearchContinuationRequest{Owner: f.Owner, Database: f.Database, N: 1}, nil, nil, producer, search.ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
			require.True(t, first.HasMore)
			require.NotEmpty(t, first.QID)
			armed = true
			_, err = f.Pull(ctx, first.QID, 2)
			require.Error(t, err)
			if failure == "cancelled" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.Contains(t, err.Error(), context.DeadlineExceeded.Error())
			}
			armed = false
			last, err := f.Pull(context.Background(), first.QID, 2)
			require.NoError(t, err, "request failure must preserve the issued qid")
			require.Equal(t, []int{2, 4, 4}, issued, "retry must repeat the unsuccessful depth")
			require.Equal(t, []string{"retry-b", "retry-c"}, last.IDs)
			require.False(t, last.More)
			_, err = f.Pull(context.Background(), first.QID, 2)
			require.Error(t, err, "successful EOF must still release the cursor")
		})
	}
}
