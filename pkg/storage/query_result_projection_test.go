package storage

import (
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestQueryResultProjectionDisabledAndNil(t *testing.T) {
	view, release := BeginQueryResultProjection(NewMemoryEngine())
	defer release()
	require.Nil(t, view.Node(nil))
	require.Nil(t, view.Edge(nil))
	require.Nil(t, view.NodeProperties(nil))
	require.Nil(t, view.EdgeProperties(nil))
	n := &Node{Properties: map[string]interface{}{"x": "raw"}}
	e := &Edge{Properties: n.Properties}
	require.Same(t, n, view.Node(n))
	require.Same(t, e, view.Edge(e))
	require.Zero(t, testing.AllocsPerRun(100, func() { view.Node(n); view.Edge(e) }))
}
func TestQueryResultProjectionNamespaceDetachedAndRevealIsolation(t *testing.T) {
	be, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer be.Close()
	schema := be.GetSchemaForNamespace("tenant")
	for _, edge := range []bool{false, true} {
		scope := knowledgepolicy.ScopeNode
		name := "doc"
		if edge {
			scope = knowledgepolicy.ScopeEdge
			name = "link"
		}
		require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: name, Scope: scope, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
		require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: name + "binding", ProfileRef: name, TargetLabels: []string{"Doc"}, TargetEdgeType: "LINK", IsEdge: edge, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "secret", HalfLifeSeconds: 1}}}))
	}
	be.SetDecayEnabled(true)
	engine := NewNamespacedEngine(be, "tenant")
	old := time.Now().Add(-time.Hour)
	n := &Node{ID: "a:b", Labels: []string{"Doc"}, Properties: map[string]interface{}{"public": "yes", "secret": "hidden"}, CreatedAt: old}
	e := &Edge{ID: "a:b", Type: "LINK", Properties: n.Properties, CreatedAt: old}
	view, release := BeginQueryResultProjection(engine)
	require.NotContains(t, view.Node(n).Properties, "secret")
	require.NotContains(t, view.Edge(e).Properties, "secret")
	require.Contains(t, n.Properties, "secret")
	require.Contains(t, e.Properties, "secret")
	require.NotContains(t, view.Node(&Node{ID: "tenant:a:b", Labels: n.Labels, Properties: n.Properties, CreatedAt: old, UpdatedAt: old}).Properties, "secret")
	started, entered := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() { close(started); end := be.BeginQueryRevealScope(true); close(entered); end(); close(done) }()
	<-started
	select {
	case <-entered:
		t.Fatal("REVEAL entered an active normal query projection")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	<-done
	end := be.BeginQueryRevealScope(true)
	require.Contains(t, NewQueryResultProjection(engine).Node(n).Properties, "secret")
	end()
}
