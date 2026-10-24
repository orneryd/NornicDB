package nornicdb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
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

func TestFindSimilarProjectsSuppressedProperties(t *testing.T) {
	be, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer be.Close()
	setupSimilarityPropertyPolicy(t, be, "tenant", "Doc")
	engine := storage.NewNamespacedEngine(be, "tenant")
	old := time.Now().Add(-time.Hour)
	for _, id := range []storage.NodeID{"root", "other"} {
		_, err = engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"public": "yes", "secret": "hidden"}, ChunkEmbeddings: [][]float32{{1, 0}}, CreatedAt: old, UpdatedAt: old})
		require.NoError(t, err)
	}
	db := &DB{storage: engine}
	results, err := db.FindSimilar(context.Background(), "root", 10)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.NotContains(t, results[0].Node.Properties, "secret")
	require.Equal(t, "yes", results[0].Node.Properties["public"])
	owner, endOwner := storage.BeginQueryRevealScopeContext(context.Background(), engine, false)
	writerStarted, writerDone := make(chan struct{}), make(chan struct{})
	go func() { close(writerStarted); release := be.BeginQueryRevealScope(true); release(); close(writerDone) }()
	<-writerStarted
	time.Sleep(20 * time.Millisecond) // Queue the writer before the nested query attempts its scope.
	type reply struct {
		results []*SearchResult
		err     error
	}
	done := make(chan reply, 1)
	go func() { results, err := db.FindSimilar(owner, "root", 10); done <- reply{results, err} }()
	select {
	case r := <-done:
		endOwner()
		<-writerDone
		require.NoError(t, r.err)
		require.Len(t, r.results, 1)
		require.NotContains(t, r.results[0].Node.Properties, "secret")
	case <-time.After(2 * time.Second):
		endOwner()
		<-writerDone
		<-done
		t.Fatal("nested similarity query recursively acquired a scope behind a waiting REVEAL")
	}
	raw, err := engine.GetNode("other")
	require.NoError(t, err)
	require.Equal(t, "hidden", raw.Properties["secret"])
}
