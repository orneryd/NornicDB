package resultstream

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProgressiveReadyPageDoesNotWaitForLookahead(t *testing.T) {
	allowExpansion := false
	calls := 0
	stream, err := NewProgressive(probeRows(10), false, 10, func(context.Context, int) ([][]any, bool, error) {
		calls++
		if !allowExpansion {
			return nil, false, errors.New("full ready page requested unnecessary lookahead")
		}
		return probeRows(10), true, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.Close()) })
	page, err := stream.Pull(t.Context(), 0, 10)
	require.NoError(t, err)
	require.Equal(t, probeRows(10), page.Rows)
	require.True(t, page.HasMore, "cursor is not yet exhausted")
	require.Nil(t, page.Total)
	require.Zero(t, calls)
	allowExpansion = true
	last, err := stream.Pull(t.Context(), 10, 10)
	require.NoError(t, err)
	require.Empty(t, last.Rows)
	require.False(t, last.HasMore)
	require.Equal(t, uint64(10), *last.Total)
	require.Equal(t, 1, calls)
	replay, err := stream.Pull(t.Context(), 0, 10)
	require.NoError(t, err)
	require.Equal(t, page.Rows, replay.Rows, "delivery rows remain stable after exhaustion is learned")
}

func TestProgressiveStopsExpandingOnceRequestedPageIsReady(t *testing.T) {
	calls := 0
	stream, err := NewProgressive(probeRows(10), false, 10, func(_ context.Context, depth int) ([][]any, bool, error) {
		calls++
		if calls > 1 {
			return nil, false, errors.New("continued past the fulfilled page")
		}
		return probeRows(depth), false, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.Close()) })
	page, err := stream.Pull(t.Context(), 0, 20)
	require.NoError(t, err)
	require.Len(t, page.Rows, 20)
	require.True(t, page.HasMore)
	require.Equal(t, 1, calls)
}
