package resolvers

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func subscriptionTestContext(database string, mode auth.DatabaseAccessMode, access auth.ResolvedAccess) context.Context {
	ctx := auth.WithRequestDatabaseScope(context.Background(), auth.NewRequestDatabaseScope(database, map[string]string{database: database}))
	ctx = auth.WithRequestDatabaseAccessMode(ctx, mode)
	return withResolvedAccess(ctx, func(string) auth.ResolvedAccess { return access })
}

func TestGraphQLSubscriptionsRequireSelectedDatabaseRead(t *testing.T) {
	db := testDB(t)
	dbManager, err := multidb.NewDatabaseManager(t.Context(), db.GetBaseStorageForManager(), nil)
	require.NoError(t, err)
	require.NoError(t, dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "named"}))
	resolver := NewResolver(db, dbManager)
	subscriptions := &subscriptionResolver{Resolver: resolver}

	deniedCalls := []struct {
		name string
		call func(context.Context) error
	}{
		{"node created", func(ctx context.Context) error { _, err := subscriptions.subscriptionNodeCreated(ctx, nil); return err }},
		{"node updated", func(ctx context.Context) error {
			_, err := subscriptions.subscriptionNodeUpdated(ctx, nil, nil)
			return err
		}},
		{"node deleted", func(ctx context.Context) error { _, err := subscriptions.subscriptionNodeDeleted(ctx, nil); return err }},
		{"relationship created", func(ctx context.Context) error {
			_, err := subscriptions.subscriptionRelationshipCreated(ctx, nil)
			return err
		}},
		{"relationship updated", func(ctx context.Context) error {
			_, err := subscriptions.subscriptionRelationshipUpdated(ctx, nil, nil)
			return err
		}},
		{"relationship deleted", func(ctx context.Context) error {
			_, err := subscriptions.subscriptionRelationshipDeleted(ctx, nil)
			return err
		}},
	}
	for _, database := range []string{"nornic", "named"} {
		for _, principal := range []struct {
			name   string
			mode   auth.DatabaseAccessMode
			access auth.ResolvedAccess
		}{
			{name: "write only", mode: auth.FullDatabaseAccessMode, access: auth.ResolvedAccess{Write: true}},
			{name: "no access", mode: auth.DenyAllDatabaseAccessMode},
		} {
			for _, subscription := range deniedCalls {
				t.Run(database+"/"+principal.name+"/"+subscription.name, func(t *testing.T) {
					err := subscription.call(subscriptionTestContext(database, principal.mode, principal.access))
					require.ErrorContains(t, err, "read permission")
				})
			}
		}
	}

	defaultStorage, err := dbManager.GetStorage("nornic")
	require.NoError(t, err)
	namedStorage, err := dbManager.GetStorage("named")
	require.NoError(t, err)
	for _, engine := range []storage.Engine{defaultStorage, namedStorage} {
		_, err = engine.CreateNode(&storage.Node{ID: "left", Labels: []string{"Anchor"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&storage.Node{ID: "right", Labels: []string{"Anchor"}})
		require.NoError(t, err)
	}

	defaultCtx := subscriptionTestContext("nornic", auth.FullDatabaseAccessMode, auth.ResolvedAccess{Read: true})
	defaultCtx, defaultCancel := context.WithCancel(defaultCtx)
	defer defaultCancel()
	namedCtx, namedCancel := context.WithCancel(subscriptionTestContext("named", auth.FullDatabaseAccessMode, auth.ResolvedAccess{Read: true}))
	defer namedCancel()

	defaultNodes, err := subscriptions.subscriptionNodeCreated(defaultCtx, nil)
	require.NoError(t, err)
	namedNodes, err := subscriptions.subscriptionNodeCreated(namedCtx, nil)
	require.NoError(t, err)
	defaultRelationships, err := subscriptions.subscriptionRelationshipCreated(defaultCtx, nil)
	require.NoError(t, err)
	namedRelationships, err := subscriptions.subscriptionRelationshipCreated(namedCtx, nil)
	require.NoError(t, err)

	_, err = namedStorage.CreateNode(&storage.Node{
		ID: "named-event", Labels: []string{"Secret"}, Properties: map[string]any{"secret": "named database payload"},
	})
	require.NoError(t, err)
	requireNodeEvent(t, namedNodes, "named-event", "named database payload")
	requireNoNodeEvent(t, defaultNodes)
	require.NoError(t, namedStorage.CreateEdge(&storage.Edge{
		ID: "named-edge", StartNode: "left", EndNode: "right", Type: "SECRET", Properties: map[string]any{"secret": "named relationship payload"},
	}))
	requireRelationshipEvent(t, namedRelationships, "named-edge", "named relationship payload")
	requireNoRelationshipEvent(t, defaultRelationships)

	_, err = defaultStorage.CreateNode(&storage.Node{
		ID: "default-event", Labels: []string{"Visible"}, Properties: map[string]any{"secret": "default database payload"},
	})
	require.NoError(t, err)
	requireNodeEvent(t, defaultNodes, "default-event", "default database payload")
	requireNoNodeEvent(t, namedNodes)
	require.NoError(t, defaultStorage.CreateEdge(&storage.Edge{
		ID: "default-edge", StartNode: "left", EndNode: "right", Type: "VISIBLE", Properties: map[string]any{"secret": "default relationship payload"},
	}))
	requireRelationshipEvent(t, defaultRelationships, "default-edge", "default relationship payload")
	requireNoRelationshipEvent(t, namedRelationships)

	openCtx, openCancel := context.WithCancel(context.Background())
	defer openCancel()
	_, err = subscriptions.subscriptionNodeCreated(openCtx, nil)
	require.NoError(t, err, "authentication-disabled subscription remains available on the default database")
}

func requireNodeEvent(t *testing.T, ch <-chan *models.Node, id, secret string) {
	t.Helper()
	select {
	case event := <-ch:
		require.Equal(t, id, event.ID)
		require.Equal(t, secret, map[string]any(event.Properties)["secret"])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for node subscription event")
	}
}

func requireNoNodeEvent(t *testing.T, ch <-chan *models.Node) {
	t.Helper()
	select {
	case event := <-ch:
		t.Fatalf("received cross-database node event: %#v", event)
	case <-time.After(100 * time.Millisecond):
	}
}

func requireRelationshipEvent(t *testing.T, ch <-chan *models.Relationship, id, secret string) {
	t.Helper()
	select {
	case event := <-ch:
		require.Equal(t, id, event.ID)
		require.Equal(t, secret, map[string]any(event.Properties)["secret"])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for relationship subscription event")
	}
}

func requireNoRelationshipEvent(t *testing.T, ch <-chan *models.Relationship) {
	t.Helper()
	select {
	case event := <-ch:
		t.Fatalf("received cross-database relationship event: %#v", event)
	case <-time.After(100 * time.Millisecond):
	}
}
