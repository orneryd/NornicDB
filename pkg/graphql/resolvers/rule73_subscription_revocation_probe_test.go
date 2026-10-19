//go:build rule73_subscription_revocation_probe

// Target overlay: PR #1467 head 590ca245 (or a later integrated head that
// includes its database-scoped subscription broker).
package resolvers

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/stretchr/testify/require"
)

// Temporary Rule 73 probe. The checked-in regression test belongs with the
// mandatory admission owner; this probe demonstrates the current lease gap.
func TestRule73ProbeSubscriptionRevalidatesReadAfterRevocation(t *testing.T) {
	db := testDB(t)
	manager := testDBManager(t, db)
	resolver := NewResolver(db, manager)
	t.Cleanup(resolver.EventBroker.Close)

	var canRead atomic.Bool
	canRead.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ctx = auth.WithRequestDatabaseScope(ctx, auth.NewRequestDatabaseScope("nornic", map[string]string{"nornic": "nornic"}))
	ctx = auth.WithRequestResolvedAccessResolver(ctx, func(database string) auth.ResolvedAccess {
		return auth.ResolvedAccess{Read: database == "nornic" && canRead.Load()}
	})

	ch, err := (&subscriptionResolver{resolver}).subscriptionNodeCreated(ctx, nil)
	require.NoError(t, err)
	resolver.EventBroker.PublishNodeCreatedInDatabase("nornic", &models.Node{ID: "before-revoke"})
	select {
	case event := <-ch:
		require.Equal(t, "before-revoke", event.ID)
	case <-time.After(time.Second):
		t.Fatal("allowed event was not delivered")
	}

	canRead.Store(false)
	resolver.EventBroker.PublishNodeCreatedInDatabase("nornic", &models.Node{ID: "secret-after-revoke"})
	select {
	case event := <-ch:
		t.Fatalf("event delivered after read revocation: %s", event.ID)
	case <-time.After(100 * time.Millisecond):
	}
}
