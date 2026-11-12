package search

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestContinuationObservesActualFilterCallbacksInEligibilityOrder(t *testing.T) {
	failure := errors.New("authorization unavailable")
	for _, tc := range []struct {
		name                  string
		hidden, decay, denied bool
		failure               error
		want                  []string
	}{
		{name: "both", want: []string{"node decay filter", "node authorization callback"}},
		{name: "hidden", hidden: true},
		{name: "decayed", decay: true, want: []string{"node decay filter"}},
		{name: "denied", denied: true, want: []string{"node decay filter", "node authorization callback"}},
		{name: "failed", failure: failure, want: []string{"node decay filter", "node authorization callback"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observer := new(fixtureowner.QueryReadObserver)
			ctx := queryprofile.WithObserver(t.Context(), observer)
			node := &storage.Node{ID: "n"}
			node.SetRuntimeMetadata(storage.NodeRuntimeMetadata{VisibilitySuppressed: tc.hidden})
			var calls []string
			service := &Service{}
			allowed, err := service.continuationNodeEligible(ctx, node, DefaultSearchOptions(), nil,
				func(string) bool { calls = append(calls, "node decay filter"); return tc.decay },
				func(*storage.Node) (bool, error) {
					calls = append(calls, "node authorization callback")
					return !tc.denied, tc.failure
				})
			require.ErrorIs(t, err, tc.failure)
			require.Equal(t, !tc.hidden && !tc.decay && !tc.denied && tc.failure == nil, allowed)
			require.Equal(t, tc.want, calls)
			require.Equal(t, calls, observer.Callbacks())
		})
	}
	t.Run("no callback", func(t *testing.T) {
		observer := new(fixtureowner.QueryReadObserver)
		service := &Service{}
		allowed, err := service.continuationNodeEligible(queryprofile.WithObserver(t.Context(), observer), &storage.Node{ID: "n"}, DefaultSearchOptions(), nil, nil, nil)
		require.NoError(t, err)
		require.True(t, allowed)
		require.Empty(t, observer.Callbacks())
	})
}

func TestCompleteContinuationReportsEachActualAuthorizationInvocation(t *testing.T) {
	engine := storage.NewNamespacedEngine(continuationTestEngine(t), "nornic")
	for _, id := range []storage.NodeID{"a", "b", "c"} {
		_, err := engine.CreateNode(t.Context(), &storage.Node{ID: id})
		require.NoError(t, err)
	}
	service := NewService(engine, 3, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	observer := new(fixtureowner.QueryReadObserver)
	calls := 0
	decayCalls := 0
	service.SetNodeDecayFilter(func(string) bool { decayCalls++; return false })
	request := SearchContinuationRequest{Owner: "profile-callback", Database: "nornic", Mode: SearchContinuationID, N: 2,
		AuthorizeNode: func(*storage.Node) (bool, error) { calls++; return true, nil }}
	page, err := service.SearchTextContinuation(queryprofile.WithObserver(t.Context(), observer), "", DefaultSearchOptions(), request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Equal(t, 2, page.Returned)
	require.True(t, page.HasMore)
	require.Positive(t, calls)
	require.Positive(t, decayCalls)
	require.Len(t, observer.Callbacks(), calls+decayCalls)
	var authorizations, decays int
	for _, name := range observer.Callbacks() {
		switch name {
		case "node authorization callback":
			authorizations++
		case "node decay filter":
			decays++
		default:
			t.Fatalf("unexpected callback %q", name)
		}
	}
	require.Equal(t, calls, authorizations)
	require.Equal(t, decayCalls, decays)
	// A new request carries its own observer; previous callbacks are never replayed.
	nextObserver := new(fixtureowner.QueryReadObserver)
	before := calls
	beforeDecay := decayCalls
	request.QID = page.QID
	next, err := service.SearchTextContinuation(queryprofile.WithObserver(t.Context(), nextObserver), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Equal(t, 1, next.Returned)
	require.False(t, next.HasMore)
	require.Positive(t, calls-before)
	require.Len(t, nextObserver.Callbacks(), calls-before+decayCalls-beforeDecay)
}

func TestDecayFilterObservationsKeepResultsAndSkipAbsentWork(t *testing.T) {
	service := &Service{}
	observer := new(fixtureowner.QueryReadObserver)
	ctx := queryprofile.WithObserver(context.Background(), observer)
	input := []indexResult{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	require.Equal(t, input, service.filterDecayedCandidates(ctx, input))
	require.Empty(t, observer.Callbacks())
	service.SetNodeDecayFilter(func(id string) bool { return id == "b" })
	require.Empty(t, service.filterDecayedCandidates(ctx, nil))
	require.Empty(t, observer.Callbacks())
	result := service.filterDecayedCandidates(ctx, input)
	require.Equal(t, []indexResult{{ID: "a"}, {ID: "c"}}, result)
	require.Equal(t, []string{"node decay filter", "node decay filter", "node decay filter"}, observer.Callbacks())
}
