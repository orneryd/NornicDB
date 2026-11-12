package search

// PROFILE ownership for the rerank provider boundary.
//
// search.Service is the only owner that invokes the configured Reranker, so the
// actual provider call is reported there and nowhere above it. The provider is
// an arbitrary implementation whose database access this caller cannot see, so
// its actual invocation stays explicitly unknown. These tests hold that
// boundary through the public RerankCandidates owner and through the
// memo-decided window a ranked continuation rebuilds: an invocation that did
// not happen is never reported as one.

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// profiledRerankService is one bounded service over a fresh engine; the caller
// installs the provider under test.
func profiledRerankService(t *testing.T) *Service {
	t.Helper()
	service := NewService(newNamespacedEngine(t), 2, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	return service
}

func rerankProviderCandidates() []RerankCandidate {
	return []RerankCandidate{
		{ID: "alpha", Content: "alpha text", Score: 0.9},
		{ID: "beta", Content: "beta text", Score: 0.2},
	}
}

// The public rerank owner reports exactly the one provider call it makes.
func TestRerankProviderReportsTheActualProviderCall(t *testing.T) {
	service := profiledRerankService(t)
	reranker := &scriptedWindowReranker{script: firstOnlyRerankResult}
	service.SetReranker(reranker)
	recorder := &fixtureowner.QueryReadObserver{}

	results, err := service.RerankCandidates(queryprofile.WithObserver(context.Background(), recorder), "query", rerankProviderCandidates(), nil)
	require.NoError(t, err)
	require.Len(t, results, 2, "the valid subset is completed from the submitted population")
	require.Equal(t, [][]string{{"alpha", "beta"}}, reranker.recorded(), "the provider ranked the submitted batch once")
	require.Equal(t, []string{"rerank provider"}, recorder.Callbacks())
}

// Every branch that returns before the provider call reports nothing: no
// configured reranker, a disabled one, an empty candidate list, and a
// submission with no rankable candidate.
func TestRerankProviderStaysSilentWhenNotInvoked(t *testing.T) {
	t.Run("no reranker configured", func(t *testing.T) {
		service := profiledRerankService(t)
		recorder := &fixtureowner.QueryReadObserver{}

		results, err := service.RerankCandidates(queryprofile.WithObserver(context.Background(), recorder), "query", rerankProviderCandidates(), nil)
		require.NoError(t, err)
		require.Len(t, results, 2)
		require.Empty(t, recorder.Callbacks())
	})

	t.Run("disabled reranker", func(t *testing.T) {
		service := profiledRerankService(t)
		service.SetReranker(NewCrossEncoder(nil))
		recorder := &fixtureowner.QueryReadObserver{}

		results, err := service.RerankCandidates(queryprofile.WithObserver(context.Background(), recorder), "query", rerankProviderCandidates(), nil)
		require.NoError(t, err)
		require.Len(t, results, 2)
		require.Empty(t, recorder.Callbacks(), "a provider nobody enabled was not invoked")
	})

	t.Run("no candidates", func(t *testing.T) {
		service := profiledRerankService(t)
		reranker := &scriptedWindowReranker{script: firstOnlyRerankResult}
		service.SetReranker(reranker)
		recorder := &fixtureowner.QueryReadObserver{}

		results, err := service.RerankCandidates(queryprofile.WithObserver(context.Background(), recorder), "query", nil, nil)
		require.NoError(t, err)
		require.Empty(t, results)
		require.Empty(t, reranker.recorded())
		require.Empty(t, recorder.Callbacks())
	})

	t.Run("no rankable candidate", func(t *testing.T) {
		service := profiledRerankService(t)
		reranker := &scriptedWindowReranker{script: firstOnlyRerankResult}
		service.SetReranker(reranker)
		recorder := &fixtureowner.QueryReadObserver{}
		candidates := []RerankCandidate{{ID: "alpha", Score: 0.9}, {ID: "beta", Score: 0.2}}

		results, err := service.RerankCandidates(queryprofile.WithObserver(context.Background(), recorder), "query", candidates, nil)
		require.NoError(t, err)
		require.Len(t, results, 2, "an eligibility decision keeps the population in original order")
		require.Empty(t, reranker.recorded())
		require.Empty(t, recorder.Callbacks())
	})
}

// A window whose candidates the memo already decided submits nothing, so the
// deeper pull of a ranked continuation records no provider invocation.
func TestRerankProviderStaysSilentForAMemoDecidedWindow(t *testing.T) {
	service := profiledRerankService(t)
	reranker := &scriptedWindowReranker{script: firstOnlyRerankResult}
	candidates := rerankProviderCandidates()
	memo := newRerankMemo()
	for _, candidate := range candidates {
		memo.putUnranked("query", candidate, false)
	}
	recorder := &fixtureowner.QueryReadObserver{}

	outcome := service.runRerank(queryprofile.WithObserver(context.Background(), recorder), reranker, "query", candidates, rerankRunOptions{
		providerLimit:      len(candidates),
		memo:               memo,
		continuationWindow: true,
		memoizedCandidates: len(candidates),
	})
	require.Empty(t, reranker.recorded(), "the retained decisions are the canonical result")
	require.Equal(t, []string{"alpha", "beta"}, rerankResultIDs(outcome.Results))
	require.Empty(t, recorder.Callbacks(), "no provider was invoked, so none is reported")
}

// A failed provider call is still an actual call: the owner reports the
// invocation and fails the outcome.
func TestRerankProviderFailureStillReportsTheCall(t *testing.T) {
	service := profiledRerankService(t)
	service.SetReranker(&testReranker{enabled: true, err: errors.New("provider unavailable")})
	recorder := &fixtureowner.QueryReadObserver{}

	results, err := service.RerankCandidates(queryprofile.WithObserver(context.Background(), recorder), "query", rerankProviderCandidates(), nil)
	require.ErrorContains(t, err, "rerank failed")
	require.Nil(t, results)
	require.Equal(t, []string{"rerank provider"}, recorder.Callbacks())
}
