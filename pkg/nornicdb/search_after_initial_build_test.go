package nornicdb

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// A search that reports the initial index build as still running is retried
// once after the build; any other error is returned as it is.
func TestSearchAfterInitialBuild(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	calls := 0
	want := &search.SearchResponse{Query: "q"}
	response, err := db.searchAfterInitialBuild(ctx, func() (*search.SearchResponse, error) {
		calls++
		if calls == 1 {
			return nil, search.ErrSearchIndexBuilding
		}
		return want, nil
	})
	require.NoError(t, err)
	require.Same(t, want, response)
	require.Equal(t, 2, calls)

	calls = 0
	other := errors.New("forced search failure")
	_, err = db.searchAfterInitialBuild(ctx, func() (*search.SearchResponse, error) {
		calls++
		return nil, other
	})
	require.ErrorIs(t, err, other)
	require.Equal(t, 1, calls)
}
