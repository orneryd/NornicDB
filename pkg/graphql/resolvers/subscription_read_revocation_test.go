package resolvers

import (
	"github.com/orneryd/nornicdb/pkg/multidb"

	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionsRevalidateDatabaseRead(t *testing.T) {
	db := testDB(t)
	manager := testutil.DatabaseManager(t, db)
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "revocation_named"}))
	resolver := NewResolver(db, manager)
	t.Cleanup(resolver.EventBroker.Close)
	subscriptions := resolver.Subscription()
	broker := resolver.EventBroker
	cases := []struct {
		name      string
		subscribe func(context.Context) (any, error)
		publish   func(string, string)
	}{
		{"node-created", func(ctx context.Context) (any, error) { ch, err := subscriptions.NodeCreated(ctx, nil); return ch, err }, func(database, id string) { broker.PublishNodeCreatedInDatabase(database, &models.Node{ID: id}) }},
		{"node-updated", func(ctx context.Context) (any, error) {
			ch, err := subscriptions.NodeUpdated(ctx, nil, nil)
			return ch, err
		}, func(database, id string) { broker.PublishNodeUpdatedInDatabase(database, &models.Node{ID: id}) }},
		{"node-deleted", func(ctx context.Context) (any, error) { ch, err := subscriptions.NodeDeleted(ctx, nil); return ch, err }, func(database, id string) { broker.PublishNodeDeletedInDatabase(database, id) }},
		{"relationship-created", func(ctx context.Context) (any, error) {
			ch, err := subscriptions.RelationshipCreated(ctx, nil)
			return ch, err
		}, func(database, id string) {
			broker.PublishRelationshipCreatedInDatabase(database, &models.Relationship{ID: id})
		}},
		{"relationship-updated", func(ctx context.Context) (any, error) {
			ch, err := subscriptions.RelationshipUpdated(ctx, nil, nil)
			return ch, err
		}, func(database, id string) {
			broker.PublishRelationshipUpdatedInDatabase(database, &models.Relationship{ID: id})
		}},
		{"relationship-deleted", func(ctx context.Context) (any, error) {
			ch, err := subscriptions.RelationshipDeleted(ctx, nil)
			return ch, err
		}, func(database, id string) { broker.PublishRelationshipDeletedInDatabase(database, id) }},
	}
	for _, database := range []string{"nornic", "revocation_named"} {
		for _, test := range cases {
			t.Run(database+"/"+test.name, func(t *testing.T) {
				var readable atomic.Bool
				readable.Store(true)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				ctx = auth.WithRequestDatabaseScope(ctx, auth.NewRequestDatabaseScope(database, map[string]string{database: database}))
				var reads, writes atomic.Int64
				ctx = auth.WithRequestEntitlementResolver(ctx, func(selected string, permission auth.Permission) bool {
					if permission != auth.PermRead {
						writes.Add(1)
						return false
					}
					reads.Add(1)
					return selected == database && readable.Load()
				})
				channel, err := test.subscribe(ctx)
				require.NoError(t, err)
				receive := func(timeout time.Duration) (any, bool) {
					chosen, value, open := reflect.Select([]reflect.SelectCase{
						{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(channel)},
						{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(time.After(timeout))},
					})
					if chosen != 0 || !open {
						return nil, false
					}
					return value.Interface(), true
				}
				require.Zero(t, writes.Load(), "read admission queried write")
				before := reads.Load()
				test.publish("another-database", "unmatched")
				require.Equal(t, before, reads.Load(), "unmatched event queried permissions")
				test.publish(database, "before-revoke")
				require.Equal(t, before+1, reads.Load(), "one live read predicate per matching event")
				_, delivered := receive(time.Second)
				require.True(t, delivered, "allowed event was not delivered")
				readable.Store(false)
				test.publish(database, "after-revoke")
				event, delivered := receive(100 * time.Millisecond)
				require.False(t, delivered, "event delivered after read revocation: %v", event)
				readable.Store(true)
				test.publish(database, "after-restore")
				_, delivered = receive(time.Second)
				require.True(t, delivered, "restored permission was cached as refused")
				require.Zero(t, writes.Load(), "delivery queried write")
			})
		}
	}
}
