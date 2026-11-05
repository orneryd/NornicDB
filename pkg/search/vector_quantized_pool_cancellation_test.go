package search

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestQuantizedPoolCancellationIsBounded(t *testing.T) {
	for _, cancelAt := range []int{1, 64, 65, 256} {
		t.Run(fmt.Sprint(cancelAt), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			visited, released := 0, 0
			reader := func(string) (float64, bool) {
				visited++
				if visited == cancelAt {
					cancel()
				}
				return 1, true
			}
			release := func() { released++ }
			scored, err := scoreVectorPool(ctx, nil, nil, reader, release, make([]Candidate, 256))
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, scored)
			require.GreaterOrEqual(t, visited, cancelAt)
			require.LessOrEqual(t, visited, cancelAt+63, "cancellation must stop the scan within its current batch")
			// The pinned graph reader is the pool's own lease: a canceled scan
			// must hand it back exactly once, whatever batch it stopped in.
			require.Equal(t, 1, released, "a canceled pool scan must release its pin exactly once")
		})
	}
}
