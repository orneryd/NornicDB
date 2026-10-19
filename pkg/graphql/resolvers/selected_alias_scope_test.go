package resolvers

import (
	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSelectedAliasScopeDirectReads(t *testing.T) {
	db := testDB(t)
	manager := testDBManager(t, db)
	require.NoError(t, manager.CreateAlias("selected_alias", "nornic"))
	_, err := db.GetStorage().CreateNode(&storage.Node{ID: "alias-seed", Labels: []string{"Seed"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}})
	require.NoError(t, err)
	resolver := NewResolver(db, manager)
	query := resolver.Query()
	allowed := false
	scope := auth.NewRequestDatabaseScopeWithSelectionCheck("selected_alias", map[string]string{"nornic": "nornic", "selected_alias": "nornic"}, func(selection string) bool {
		return selection == "nornic" || selection == "selected_alias" && allowed
	})
	ctx, cancel := context.WithCancel(auth.WithRequestDatabaseScope(context.Background(), scope))
	defer cancel()
	ctx = auth.WithRequestEntitlementResolver(ctx, func(string, auth.Permission) bool { return true })
	// Bind the alias before its first grant, and retain it across every phase.
	ch := resolver.EventBroker.SubscribeNodeCreatedInDatabase(ctx, "selected_alias", nil)
	for _, grant := range []bool{false, true, false, true} {
		allowed = grant
		calls := map[string]func() error{
			"search":     func() error { _, e := query.Search(ctx, "seed", nil); return e },
			"similarity": func() error { _, e := query.Similar(ctx, "alias-seed", nil, nil); return e },
			"stats":      func() error { _, e := query.Stats(ctx); return e },
			"cypher": func() error {
				alias := "selected_alias"
				_, e := query.Cypher(ctx, models.CypherInput{Statement: "MATCH (n) RETURN count(n)", Database: &alias})
				return e
			},
		}
		for name, call := range calls {
			err := call()
			if grant {
				require.NoError(t, err, name)
			} else {
				require.ErrorContains(t, err, "not allowed", name)
			}
		}
		resolver.EventBroker.PublishNodeCreatedInDatabase("nornic", &models.Node{ID: "event"})
		select {
		case event := <-ch:
			require.True(t, grant, "excluded alias received %s", event.ID)
		default:
			require.False(t, grant, "granted alias lost event")
		}
	}
}

func TestSelectedAliasSubscriptionsObserveRevocation(t *testing.T) {
	db := testDB(t)
	manager := testDBManager(t, db)
	require.NoError(t, manager.CreateAlias("selected_alias", "nornic"))
	resolver := NewResolver(db, manager)
	subscriptions := resolver.Subscription()
	allowed := false
	scope := auth.NewRequestDatabaseScopeWithSelectionCheck("selected_alias", map[string]string{"nornic": "nornic", "selected_alias": "nornic"}, func(selection string) bool { return selection == "nornic" || selection == "selected_alias" && allowed })
	ctx, cancel := context.WithCancel(auth.WithRequestDatabaseScope(context.Background(), scope))
	defer cancel()
	ctx = auth.WithRequestEntitlementResolver(ctx, func(string, auth.Permission) bool { return true })
	checks := []func() error{
		func() error { _, e := subscriptions.NodeCreated(ctx, nil); return e },
		func() error { _, e := subscriptions.NodeUpdated(ctx, nil, nil); return e },
		func() error { _, e := subscriptions.NodeDeleted(ctx, nil); return e },
		func() error { _, e := subscriptions.RelationshipCreated(ctx, nil); return e },
		func() error { _, e := subscriptions.RelationshipUpdated(ctx, nil, nil); return e },
		func() error { _, e := subscriptions.RelationshipDeleted(ctx, nil); return e },
	}
	for _, check := range checks {
		require.ErrorContains(t, check(), "not allowed")
	}
	allowed = true
	nc, e := subscriptions.NodeCreated(ctx, nil)
	require.NoError(t, e)
	nu, e := subscriptions.NodeUpdated(ctx, nil, nil)
	require.NoError(t, e)
	nd, e := subscriptions.NodeDeleted(ctx, nil)
	require.NoError(t, e)
	rc, e := subscriptions.RelationshipCreated(ctx, nil)
	require.NoError(t, e)
	ru, e := subscriptions.RelationshipUpdated(ctx, nil, nil)
	require.NoError(t, e)
	rd, e := subscriptions.RelationshipDeleted(ctx, nil)
	require.NoError(t, e)
	streams := []struct {
		name    string
		channel any
		publish func()
	}{
		{"node-created", nc, func() { resolver.EventBroker.PublishNodeCreatedInDatabase("nornic", &models.Node{ID: "event"}) }},
		{"node-updated", nu, func() { resolver.EventBroker.PublishNodeUpdatedInDatabase("nornic", &models.Node{ID: "event"}) }},
		{"node-deleted", nd, func() { resolver.EventBroker.PublishNodeDeletedInDatabase("nornic", "event") }},
		{"relationship-created", rc, func() {
			resolver.EventBroker.PublishRelationshipCreatedInDatabase("nornic", &models.Relationship{ID: "event"})
		}},
		{"relationship-updated", ru, func() {
			resolver.EventBroker.PublishRelationshipUpdatedInDatabase("nornic", &models.Relationship{ID: "event"})
		}},
		{"relationship-deleted", rd, func() { resolver.EventBroker.PublishRelationshipDeletedInDatabase("nornic", "event") }},
	}
	for _, grant := range []bool{true, false, true, false} {
		allowed = grant
		for _, stream := range streams {
			stream.publish()
			chosen, _, _ := reflect.Select([]reflect.SelectCase{{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(stream.channel)}, {Dir: reflect.SelectDefault}})
			require.Equal(t, grant, chosen == 0, "%s grant=%v", stream.name, grant)
		}
	}
}
