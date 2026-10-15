package resolvers

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Events remain whole-entity notifications, but normal subscription payloads
// must not inherit a mutation producer's REVEAL or the default database policy.
func TestSubscriptionPropertiesHonorSourcePolicyDuringReveal(t *testing.T) {
	db := testDB(t)
	manager := testDBManager(t, db)
	resolver := NewResolver(db, manager)
	t.Cleanup(resolver.EventBroker.Close)
	be, ok := storage.UnwrapEngine(db.GetStorage()).(*storage.BadgerEngine)
	require.True(t, ok)
	for _, namespace := range []string{manager.DefaultDatabaseName(), "foreign"} {
		schema := be.GetSchemaForNamespace(namespace)
		hidden := "secret"
		if namespace == "foreign" {
			hidden = "foreignSecret"
		}
		for _, edge := range []bool{false, true} {
			name, scope := "doc", knowledgepolicy.ScopeNode
			if edge {
				name, scope = "link", knowledgepolicy.ScopeEdge
			}
			require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: name, Scope: scope, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
			require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: name, ProfileRef: name, TargetLabels: []string{"Doc"}, TargetEdgeType: "LINK", IsEdge: edge, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: hidden, HalfLifeSeconds: 1}}}))
		}
	}
	be.SetDecayEnabled(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	created := resolver.EventBroker.SubscribeNodeCreated(ctx, nil)
	updated := resolver.EventBroker.SubscribeNodeUpdated(ctx, nil, nil)
	edgeCreated := resolver.EventBroker.SubscribeRelationshipCreated(ctx, nil)
	edgeUpdated := resolver.EventBroker.SubscribeRelationshipUpdated(ctx, nil, nil)
	old := time.Now().Add(-time.Hour)
	release := be.BeginQueryRevealScope(true)
	defer release()
	for _, namespace := range []string{manager.DefaultDatabaseName(), "foreign"} {
		engine := storage.NewNamespacedEngine(be, namespace)
		props := map[string]interface{}{"secret": "default private", "foreignSecret": "foreign private", "public": "visible"}
		hidden, retained := "secret", "foreignSecret"
		if namespace == "foreign" {
			hidden, retained = retained, hidden
		}
		for _, id := range []storage.NodeID{"a", "b"} {
			node := &storage.Node{ID: id, Labels: []string{"Doc"}, Properties: props, CreatedAt: old, UpdatedAt: old}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			select {
			case event := <-created:
				require.NotContains(t, event.Properties, hidden)
				expectedID := string(id)
				if namespace == "foreign" {
					expectedID = namespace + ":" + expectedID
				}
				require.Equal(t, expectedID, event.ID)
				require.Equal(t, props[retained], event.Properties[retained])
				require.Equal(t, "visible", event.Properties["public"])
			case <-time.After(time.Second):
				t.Fatal("missing synchronous created event")
			}
			require.NoError(t, engine.UpdateNode(node))
			select {
			case event := <-updated:
				require.NotContains(t, event.Properties, hidden)
			case <-time.After(time.Second):
				t.Fatal("missing synchronous updated event")
			}
			require.Contains(t, node.Properties, hidden)
			raw, err := engine.GetNode(id)
			require.NoError(t, err)
			require.Contains(t, raw.Properties, hidden)
		}
		edge := &storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "LINK", Properties: props, CreatedAt: old, UpdatedAt: old}
		require.NoError(t, engine.CreateEdge(edge))
		select {
		case event := <-edgeCreated:
			require.NotContains(t, event.Properties, hidden)
			require.Equal(t, props[retained], event.Properties[retained])
		case <-time.After(time.Second):
			t.Fatal("missing synchronous relationship created event")
		}
		require.NoError(t, engine.UpdateEdge(edge))
		select {
		case event := <-edgeUpdated:
			require.NotContains(t, event.Properties, hidden)
		case <-time.After(time.Second):
			t.Fatal("missing synchronous relationship updated event")
		}
		require.Contains(t, edge.Properties, hidden)
		raw, err := engine.GetEdge("ab")
		require.NoError(t, err)
		require.Contains(t, raw.Properties, hidden)
	}
}
