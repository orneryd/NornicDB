package search

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSearchContinuationBufferedPageReturnsDuringExpansion(t *testing.T) {
	t.Setenv(EnvSearchResultCacheEnabled, "false")
	population := newNOR406Population(t, 1000, 0)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var calls atomic.Int32
	search := population.searchFunc(nil)
	gated := func(ctx context.Context, query string, embedding []float32, options *SearchOptions) (*SearchResponse, error) {
		if calls.Add(1) == 2 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return search(ctx, query, embedding, options)
	}
	first, err := population.service.SearchTextContinuation(ctx, nor406QueryText, population.initialOptions(), SearchContinuationRequest{Owner: nor406ProbeOwner, N: nor406PageSize}, nil, fixedQueryEmbedding2D, gated, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.True(t, first.HasMore)
	replayToken := first.QID
	page := first
	for consumed := first.Returned; consumed < nor406InitialDepth; {
		page, err = population.pull(ctx, nil, nor406ProbeOwner, page.QID, nor406PageSize)
		require.NoError(t, err)
		require.Equal(t, nor406PageSize, page.Returned)
		consumed += page.Returned
	}
	require.Equal(t, int32(1), calls.Load(), "the last full buffered page must not run lookahead")
	require.True(t, page.HasMore)
	type outcome struct {
		page *SearchContinuationPage
		err  error
	}
	completed := make(chan outcome, 1)
	go func() {
		next, err := population.pull(ctx, nil, nor406ProbeOwner, page.QID, nor406PageSize)
		completed <- outcome{next, err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("expansion did not enter", ctx.Err())
	}
	// This request uses the initial token, whose next ten rows are already in
	// the replay buffer. Retrieval for a different position is still gated.
	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readyCancel()
	readyResult := make(chan outcome, 1)
	go func() {
		ready, err := population.pull(readyCtx, nil, nor406ProbeOwner, replayToken, nor406PageSize)
		readyResult <- outcome{ready, err}
	}()
	var readyPage *SearchContinuationPage
	select {
	case ready := <-readyResult:
		require.NoError(t, ready.err, "ready page must not wait for retrieval or its metadata lock")
		require.Equal(t, nor406PageSize, ready.page.Returned)
		readyPage = ready.page
	case <-readyCtx.Done():
		t.Fatal("ready page waited for unrelated retrieval", readyCtx.Err())
	}
	require.Equal(t, int32(2), calls.Load())
	unblock()
	select {
	case result := <-completed:
		require.NoError(t, result.err)
		require.Equal(t, nor406PageSize, result.page.Returned)
		replayed, replayErr := population.pull(ctx, nil, nor406ProbeOwner, replayToken, nor406PageSize)
		require.NoError(t, replayErr)
		require.Equal(t, readyPage.Position, replayed.Position)
		require.Len(t, replayed.Results, len(readyPage.Results))
		for index, result := range readyPage.Results {
			require.Equal(t, searchResultID(result), searchResultID(replayed.Results[index]), "replay position %d", index)
		}
		if result.page.HasMore {
			require.NoError(t, population.discard(ctx, nor406ProbeOwner, result.page.QID))
		}
	case <-ctx.Done():
		t.Fatal("expansion did not complete", ctx.Err())
	}
}
