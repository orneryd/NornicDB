package search

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
	"time"
)

func policySearchFixture(t *testing.T) (*storage.BadgerEngine, storage.Engine, *Service) {
	t.Helper()
	be, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { be.Close() })
	schema := be.GetSchemaForNamespace("tenant")
	for _, edge := range []bool{false, true} {
		name, scope := "doc", knowledgepolicy.ScopeNode
		if edge {
			name, scope = "link", knowledgepolicy.ScopeEdge
		}
		require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: name, Scope: scope, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
		binding := knowledgepolicy.DecayProfileBinding{Name: name, ProfileRef: name}
		for _, prop := range []string{"body", "title", "description", "content", "text", "type"} {
			binding.PropertyRules = append(binding.PropertyRules, knowledgepolicy.DecayProfilePropertyRule{PropertyPath: prop, HalfLifeSeconds: 1})
		}
		if edge {
			binding.IsEdge = true
			binding.TargetEdgeType = "LINK"
		} else {
			binding.TargetLabels = []string{"Doc"}
		}
		require.NoError(t, schema.CreateDecayProfileBinding(binding))
	}
	engine := storage.NewNamespacedEngine(be, "tenant")
	old := time.Now().Add(-time.Hour)
	props := map[string]interface{}{"public": "needle visible", "body": "needle hidden", "title": "hidden title", "description": "hidden description", "content": "needle hidden content", "type": "hidden type"}
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err = engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: props, CreatedAt: old, UpdatedAt: old})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "LINK", Properties: props, CreatedAt: old, UpdatedAt: old}))
	svc := NewService(engine)
	t.Cleanup(func() { svc.Close() })
	svc.SetFulltextProperties([]string{"public", "body", "title", "description", "content", "type"})
	svc.SetSearchResultCachePolicy(10, time.Hour)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	return be, engine, svc
}
func assertPolicyProjectedSearchResult(t *testing.T, r SearchResult) {
	t.Helper()
	require.Equal(t, "needle visible", r.Properties["public"])
	for _, prop := range []string{"body", "title", "description", "content", "text", "type"} {
		require.NotContains(t, r.Properties, prop)
	}
	require.Empty(t, r.Title)
	require.Empty(t, r.Description)
	require.Empty(t, r.ContentPreview)
	for _, match := range r.Matches {
		require.NotContains(t, []string{"body", "title", "description", "content", "text", "type"}, match.Property)
	}
}
func TestNormalSearchProjectionBypassesWarmRawCache(t *testing.T) {
	be, engine, svc := policySearchFixture(t)
	ctx := context.Background()
	opts := DefaultSearchOptions()
	opts.Limit = 2
	warm, err := svc.Search(ctx, "needle", nil, opts)
	require.NoError(t, err)
	require.NotEmpty(t, warm.Results)
	require.Contains(t, warm.Results[0].Properties, "body")
	be.SetDecayEnabled(true)
	result, err := svc.Search(ctx, "needle", nil, opts)
	require.NoError(t, err)
	require.NotEmpty(t, result.Results)
	for _, r := range result.Results {
		assertPolicyProjectedSearchResult(t, r)
	}
	raw, err := engine.GetNode("a")
	require.NoError(t, err)
	require.Contains(t, raw.Properties, "body")
}
func TestNormalSearchProjectionCoversEnrichmentAndContinuation(t *testing.T) {
	be, engine, svc := policySearchFixture(t)
	be.SetDecayEnabled(true)
	opts := DefaultSearchOptions()
	opts.Limit = 1
	for _, results := range [][]SearchResult{mustSearchRead[[]SearchResult](t)(svc.enrichResults(context.Background(), []rrfResult{{ID: "a", RRFScore: 1}}, opts, nil, hitMatchQuery{text: "needle"})), mustSearchRead[[]SearchResult](t)(svc.enrichIndexResults(context.Background(), []indexResult{{ID: "a", Score: 1}}, opts, nil, hitMatchQuery{text: "needle"}))} {
		require.Len(t, results, 1)
		assertPolicyProjectedSearchResult(t, results[0])
	}
	results, err := hydrateContinuationResults(engine, []SearchResult{{ID: "a", NodeID: "a", Title: "stale hidden", Matches: []SearchMatch{{Kind: SearchMatchTerm, Property: "body", Term: "needle"}}}}, nil, newResultPropertyProjection(nil, nil))
	require.NoError(t, err)
	require.Len(t, results, 1)
	assertPolicyProjectedSearchResult(t, results[0])
}
func TestNormalRelationshipSearchProjectsProperties(t *testing.T) {
	be, _, svc := policySearchFixture(t)
	be.SetDecayEnabled(true)
	svc.EnableRelationshipSearch(nil)
	svc.StartRelationshipIndex()
	result, err := svc.SearchRelationships(context.Background(), "needle", nil, DefaultSearchOptions())
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	r := result.Results[0]
	assertPolicyProjectedSearchResult(t, r)
	require.NotContains(t, r.Relationship.Properties, "body")
}

func TestNormalContinuationPagesRefreshDecayProjection(t *testing.T) {
	be, _, svc := policySearchFixture(t)
	ctx := context.Background()
	req := SearchContinuationRequest{Owner: "alice", Database: "tenant", Mode: SearchContinuationID, N: 1}
	first, err := svc.SearchTextContinuation(ctx, "", nil, req, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.True(t, first.HasMore)
	require.Contains(t, first.Results[0].Properties, "body")
	be.SetDecayEnabled(true)
	req.QID = first.QID
	last, err := svc.SearchTextContinuation(ctx, "", nil, req, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, last.Results, 1)
	assertPolicyProjectedSearchResult(t, last.Results[0])
}
func TestNormalGroupedContinuationCannotExposeSuppressedGroupKey(t *testing.T) {
	be, engine, svc := policySearchFixture(t)
	ctx := context.Background()
	b, err := engine.GetNode("b")
	require.NoError(t, err)
	b.Properties["title"] = "second hidden"
	require.NoError(t, engine.UpdateNode(b))
	req := SearchContinuationRequest{Owner: "alice", Database: "tenant", Mode: SearchContinuationID, GroupBy: "title", N: 1}
	first, err := svc.SearchTextContinuation(ctx, "", nil, req, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.True(t, first.HasMore)
	be.SetDecayEnabled(true)
	req.QID = first.QID
	_, err = svc.SearchTextContinuation(ctx, "", nil, req, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.ErrorIs(t, err, resultstream.ErrInvalidated)
	req.QID = ""
	_, err = svc.SearchTextContinuation(ctx, "", nil, req, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.Error(t, err, "a suppressed property cannot build public group keys")
}
func TestNormalSearchInsideOwnedRevealAndWaitingWriter(t *testing.T) {
	be, engine, svc := policySearchFixture(t)
	be.SetDecayEnabled(true)
	reveal, end := storage.BeginQueryRevealScopeContext(context.Background(), engine, true)
	r, err := svc.Search(reveal, "needle", nil, DefaultSearchOptions())
	require.NoError(t, err)
	require.NotEmpty(t, r.Results)
	require.Contains(t, r.Results[0].Properties, "body")
	end()
	owner, release := storage.BeginQueryRevealScopeContext(context.Background(), engine, false)
	defer release()
	started, done := make(chan struct{}), make(chan struct{})
	go func() {
		close(started)
		_, finish := storage.BeginQueryRevealScopeContext(context.Background(), engine, true)
		finish()
		close(done)
	}()
	<-started
	time.Sleep(20 * time.Millisecond)
	type response struct {
		result *SearchResponse
		err    error
	}
	ch := make(chan response, 1)
	go func() {
		result, err := svc.Search(owner, "needle", nil, DefaultSearchOptions())
		ch <- response{result, err}
	}()
	select {
	case got := <-ch:
		require.NoError(t, got.err)
		require.NotEmpty(t, got.result.Results)
		assertPolicyProjectedSearchResult(t, got.result.Results[0])
		release()
	case <-time.After(time.Second):
		release()
		<-done
		t.Fatal("nested service reacquired an owned reveal mutex")
	}
	<-done
}
func TestVisibleSearchMatchesKeepsRetainedInputImmutable(t *testing.T) {
	matches := []SearchMatch{{Property: "public"}, {Property: "hidden"}, {Property: ""}, {Property: "hidden"}, {Property: "public"}}
	original := append([]SearchMatch(nil), matches...)
	filtered := visibleSearchMatches(matches, map[string]interface{}{"public": "ok"})
	require.Equal(t, []SearchMatch{{Property: "public"}, {Property: ""}, {Property: "public"}}, filtered)
	require.Equal(t, original, matches)
	visible := []SearchMatch{{Property: "public"}, {Property: ""}}
	require.Equal(t, reflect.ValueOf(visible).Pointer(), reflect.ValueOf(visibleSearchMatches(visible, map[string]interface{}{"public": 1})).Pointer())
	require.Nil(t, visibleSearchMatches(nil, nil))
}
