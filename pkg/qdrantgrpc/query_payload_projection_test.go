package qdrantgrpc

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func setupSimilarityPropertyPolicy(t *testing.T, be *storage.BadgerEngine, namespace, label string) {
	t.Helper()
	schema := be.GetSchemaForNamespace(namespace)
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "query", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: .1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "querybinding", ProfileRef: "query", TargetLabels: []string{label}, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "secret", HalfLifeSeconds: 1}}}))
	be.SetDecayEnabled(true)
}

func TestQdrantQueriesProjectSuppressedPayload(t *testing.T) {
	be, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer be.Close()
	dbm, err := multidb.NewDatabaseManager(be, nil)
	require.NoError(t, err)
	collections, err := NewDatabaseCollectionStore(dbm, nil)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, collections.Create(ctx, "tenant", 2, qpb.Distance_Cosine))
	engine, _, err := collections.Open(ctx, "tenant")
	require.NoError(t, err)
	setupSimilarityPropertyPolicy(t, be, "tenant", QdrantPointLabel)
	old := time.Now().Add(-time.Hour)
	_, err = engine.CreateNode(&storage.Node{ID: "qdrant:point:1", Labels: []string{QdrantPointLabel}, Properties: map[string]interface{}{"public": "yes", "secret": "hidden"}, NamedEmbeddings: map[string][]float32{"default": {1, 0}}, CreatedAt: old, UpdatedAt: old})
	require.NoError(t, err)
	service := NewPointsService(DefaultConfig(), collections, nil, nil, nil)
	payload := &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}}
	t.Run("search", func(t *testing.T) {
		resp, err := service.Search(ctx, &qpb.SearchPoints{CollectionName: "tenant", Vector: []float32{1, 0}, Limit: 1, WithPayload: payload})
		require.NoError(t, err)
		require.Len(t, resp.Result, 1)
		require.NotContains(t, resp.Result[0].Payload, "secret")
		require.Contains(t, resp.Result[0].Payload, "public")
	})
	t.Run("scroll", func(t *testing.T) {
		resp, err := service.Scroll(ctx, &qpb.ScrollPoints{CollectionName: "tenant", WithPayload: payload})
		require.NoError(t, err)
		require.Len(t, resp.Result, 1)
		require.NotContains(t, resp.Result[0].Payload, "secret")
		require.Contains(t, resp.Result[0].Payload, "public")
	})
	got, err := service.Get(ctx, &qpb.GetPoints{CollectionName: "tenant", Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Num{Num: 1}}}, WithPayload: payload})
	require.NoError(t, err)
	require.Len(t, got.Result, 1)
	require.Contains(t, got.Result[0].Payload, "secret", "direct-ID Get intentionally returns raw stored payload")
	raw, err := engine.GetNode("qdrant:point:1")
	require.NoError(t, err)
	require.Contains(t, raw.Properties, "secret")
}
