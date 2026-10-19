package search

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChunkedSearchCancellationDoesNotFallBack(t *testing.T) {
	for _, chunks := range [][]string{{"one"}, {"one", "two"}} {
		for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
			for _, stage := range []string{"embedding", "search"} {
				t.Run(fmt.Sprintf("%d/%s/%s", len(chunks), cause, stage), func(t *testing.T) {
					ctx := context.Background()
					if cause == context.DeadlineExceeded {
						var cancel context.CancelFunc
						ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
						defer cancel()
					}
					calls := 0
					response, err := SearchTextChunks(ctx, "query", nil,
						func(context.Context, string) ([]string, error) { return chunks, nil },
						func(context.Context, string) ([]float32, error) {
							if stage == "embedding" {
								return nil, fmt.Errorf("provider: %w", cause)
							}
							return []float32{1, 0}, nil
						},
						func(context.Context, string, []float32, *SearchOptions) (*SearchResponse, error) {
							calls++
							if stage == "search" && calls == 1 {
								return nil, fmt.Errorf("search: %w", cause)
							}
							return &SearchResponse{}, nil
						})
					require.ErrorIs(t, err, cause)
					require.Nil(t, response)
					if stage == "embedding" || cause == context.DeadlineExceeded {
						require.Zero(t, calls)
					} else {
						require.Equal(t, 1, calls)
					}
				})
			}
		}
	}
}
