package resolvers

import (
	"context"
	"github.com/orneryd/nornicdb/internal/testutil"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Event Broker Tests
// =============================================================================

func TestEventBroker_SubscribeNodeCreated(t *testing.T) {
	broker := NewEventBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Subscribe to all node created events
	ch := broker.SubscribeNodeCreated(ctx, nil)

	// Publish an event
	node := &models.Node{
		ID:     "n1",
		Labels: []string{"Person"},
	}
	broker.PublishNodeCreated(node)

	// Verify event received
	select {
	case event := <-ch:
		assert.Equal(t, "n1", event.ID)
		assert.Equal(t, []string{"Person"}, event.Labels)
	case <-time.After(1 * time.Second):
		t.Fatal("Event not received")
	}
}

func TestEventBroker_SubscribeNodeCreated_WithLabelFilter(t *testing.T) {
	broker := NewEventBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Subscribe to Person nodes only
	ch := broker.SubscribeNodeCreated(ctx, []string{"Person"})

	// Publish a Person node - should receive
	personNode := &models.Node{
		ID:     "n1",
		Labels: []string{"Person"},
	}
	broker.PublishNodeCreated(personNode)

	select {
	case event := <-ch:
		assert.Equal(t, "n1", event.ID)
	case <-time.After(1 * time.Second):
		t.Fatal("Person node event not received")
	}

	// Publish a Company node - should NOT receive
	companyNode := &models.Node{
		ID:     "n2",
		Labels: []string{"Company"},
	}
	broker.PublishNodeCreated(companyNode)

	select {
	case <-ch:
		t.Fatal("Company node should not be received")
	case <-time.After(100 * time.Millisecond):
		// Good - no event received
	}
}

func TestEventBroker_SubscribeNodeUpdated(t *testing.T) {
	broker := NewEventBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Subscribe to all node updated events
	ch := broker.SubscribeNodeUpdated(ctx, nil, nil)

	node := &models.Node{
		ID:     "n1",
		Labels: []string{"Person"},
	}
	broker.PublishNodeUpdated(node)

	select {
	case event := <-ch:
		assert.Equal(t, "n1", event.ID)
	case <-time.After(1 * time.Second):
		t.Fatal("Event not received")
	}
}

func TestEventBroker_SubscribeNodeUpdated_WithIDFilter(t *testing.T) {
	broker := NewEventBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeID := "n1"
	ch := broker.SubscribeNodeUpdated(ctx, &nodeID, nil)

	// Publish update for matching ID
	node := &models.Node{ID: "n1", Labels: []string{"Person"}}
	broker.PublishNodeUpdated(node)

	select {
	case event := <-ch:
		assert.Equal(t, "n1", event.ID)
	case <-time.After(1 * time.Second):
		t.Fatal("Event not received")
	}

	// Publish update for different ID - should NOT receive
	otherNode := &models.Node{ID: "n2", Labels: []string{"Person"}}
	broker.PublishNodeUpdated(otherNode)

	select {
	case <-ch:
		t.Fatal("Should not receive event for different ID")
	case <-time.After(100 * time.Millisecond):
		// Good - no event received
	}
}

func TestEventBroker_SubscribeNodeDeleted(t *testing.T) {
	broker := NewEventBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := broker.SubscribeNodeDeleted(ctx, nil)

	broker.PublishNodeDeleted("n1")

	select {
	case nodeID := <-ch:
		assert.Equal(t, "n1", nodeID)
	case <-time.After(1 * time.Second):
		t.Fatal("Event not received")
	}
}

func TestEventBroker_SubscribeRelationshipCreated(t *testing.T) {
	broker := NewEventBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := broker.SubscribeRelationshipCreated(ctx, nil)

	rel := &models.Relationship{
		ID:   "r1",
		Type: "KNOWS",
	}
	broker.PublishRelationshipCreated(rel)

	select {
	case event := <-ch:
		assert.Equal(t, "r1", event.ID)
		assert.Equal(t, "KNOWS", event.Type)
	case <-time.After(1 * time.Second):
		t.Fatal("Event not received")
	}
}

func TestEventBroker_SubscribeRelationshipCreated_WithTypeFilter(t *testing.T) {
	broker := NewEventBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := broker.SubscribeRelationshipCreated(ctx, []string{"KNOWS"})

	// Publish KNOWS relationship - should receive
	knowsRel := &models.Relationship{
		ID:   "r1",
		Type: "KNOWS",
	}
	broker.PublishRelationshipCreated(knowsRel)

	select {
	case event := <-ch:
		assert.Equal(t, "KNOWS", event.Type)
	case <-time.After(1 * time.Second):
		t.Fatal("KNOWS relationship event not received")
	}

	// Publish LIKES relationship - should NOT receive
	likesRel := &models.Relationship{
		ID:   "r2",
		Type: "LIKES",
	}
	broker.PublishRelationshipCreated(likesRel)

	select {
	case <-ch:
		t.Fatal("LIKES relationship should not be received")
	case <-time.After(100 * time.Millisecond):
		// Good - no event received
	}
}

func TestEventBroker_SubscribeRelationshipUpdated(t *testing.T) {
	broker := NewEventBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := broker.SubscribeRelationshipUpdated(ctx, nil, nil)

	rel := &models.Relationship{
		ID:   "r1",
		Type: "KNOWS",
	}
	broker.PublishRelationshipUpdated(rel)

	select {
	case event := <-ch:
		assert.Equal(t, "r1", event.ID)
	case <-time.After(1 * time.Second):
		t.Fatal("Event not received")
	}
}

func TestEventBroker_SubscribeRelationshipDeleted(t *testing.T) {
	broker := NewEventBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := broker.SubscribeRelationshipDeleted(ctx, nil)

	broker.PublishRelationshipDeleted("r1")

	select {
	case relID := <-ch:
		assert.Equal(t, "r1", relID)
	case <-time.After(1 * time.Second):
		t.Fatal("Event not received")
	}
}

func TestEventBroker_MultipleSubscriptions(t *testing.T) {
	broker := NewEventBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create multiple subscriptions
	ch1 := broker.SubscribeNodeCreated(ctx, []string{"Person"})
	ch2 := broker.SubscribeNodeCreated(ctx, []string{"Company"})
	ch3 := broker.SubscribeNodeCreated(ctx, nil) // All nodes

	// Publish Person node
	personNode := &models.Node{
		ID:     "n1",
		Labels: []string{"Person"},
	}
	broker.PublishNodeCreated(personNode)

	// ch1 should receive (Person filter)
	select {
	case event := <-ch1:
		assert.Equal(t, "n1", event.ID)
	case <-time.After(1 * time.Second):
		t.Fatal("ch1 did not receive Person node")
	}

	// ch2 should NOT receive (Company filter)
	select {
	case <-ch2:
		t.Fatal("ch2 should not receive Person node")
	case <-time.After(100 * time.Millisecond):
		// Good
	}

	// ch3 should receive (no filter)
	select {
	case event := <-ch3:
		assert.Equal(t, "n1", event.ID)
	case <-time.After(1 * time.Second):
		t.Fatal("ch3 did not receive Person node")
	}

	cancel()
}

func TestEventBroker_ContextCancellation(t *testing.T) {
	broker := NewEventBroker()
	ctx, cancel := context.WithCancel(context.Background())

	ch := broker.SubscribeNodeCreated(ctx, nil)

	// Cancel context
	cancel()

	// Wait a bit for cleanup
	time.Sleep(50 * time.Millisecond)

	// Try to publish - channel should be closed
	node := &models.Node{ID: "n1", Labels: []string{"Person"}}
	broker.PublishNodeCreated(node)

	// Channel should be closed
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "Channel should be closed")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Channel should be closed after context cancellation")
	}
}

func TestEventBroker_Close(t *testing.T) {
	broker := NewEventBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := broker.SubscribeNodeCreated(ctx, nil)

	// Close broker
	broker.Close()

	// Channel should be closed
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "Channel should be closed")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Channel should be closed after broker.Close()")
	}
}

// =============================================================================
// Integration Tests: Storage Events → GraphQL Subscriptions
// =============================================================================

func TestSubscription_NodeCreated_Integration(t *testing.T) {
	db := testDB(t)
	dbManager := testutil.DatabaseManager(t, db)
	resolver := NewResolver(db, dbManager)

	require.NotNil(t, resolver.EventBroker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Subscribe to node created events
	ch := resolver.EventBroker.SubscribeNodeCreated(ctx, []string{"Person"})

	// Create a node via storage (this should trigger the event)
	storageEngine, err := resolver.getStorage()
	require.NoError(t, err)

	node := &storage.Node{
		ID:     storage.NodeID("n1"),
		Labels: []string{"Person"},
		Properties: map[string]interface{}{
			"name": "Alice",
		},
	}
	_, err = storageEngine.CreateNode(node)
	require.NoError(t, err)

	// Verify subscription received the event
	select {
	case event := <-ch:
		assert.Equal(t, "n1", event.ID)
		assert.Equal(t, []string{"Person"}, event.Labels)
		props := map[string]interface{}(event.Properties)
		assert.Equal(t, "Alice", props["name"])
	case <-time.After(2 * time.Second):
		t.Fatal("Subscription did not receive node created event")
	}
}

func TestSubscription_NodeUpdated_Integration(t *testing.T) {
	db := testDB(t)
	dbManager := testutil.DatabaseManager(t, db)
	resolver := NewResolver(db, dbManager)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a node first
	storageEngine, err := resolver.getStorage()
	require.NoError(t, err)

	node := &storage.Node{
		ID:     storage.NodeID("n1"),
		Labels: []string{"Person"},
		Properties: map[string]interface{}{
			"name": "Alice",
		},
	}
	_, err = storageEngine.CreateNode(node)
	require.NoError(t, err)

	// Subscribe to node updated events
	ch := resolver.EventBroker.SubscribeNodeUpdated(ctx, nil, nil)

	// Update the node
	node.Properties["age"] = 30
	err = storageEngine.UpdateNode(storage.NodeUpdateRequest{After: node})
	require.NoError(t, err)

	// Verify subscription received the event
	select {
	case event := <-ch:
		assert.Equal(t, "n1", event.ID)
		props := map[string]interface{}(event.Properties)
		assert.Equal(t, int64(30), props["age"])
		assert.Equal(t, 30, node.Properties["age"], "event normalization must not modify the caller input")
	case <-time.After(2 * time.Second):
		t.Fatal("Subscription did not receive node updated event")
	}
}

func TestSubscription_NodeDeleted_Integration(t *testing.T) {
	db := testDB(t)
	dbManager := testutil.DatabaseManager(t, db)
	resolver := NewResolver(db, dbManager)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a node first
	storageEngine, err := resolver.getStorage()
	require.NoError(t, err)

	node := &storage.Node{
		ID:     storage.NodeID("n1"),
		Labels: []string{"Person"},
	}
	_, err = storageEngine.CreateNode(node)
	require.NoError(t, err)

	// Subscribe to node deleted events
	ch := resolver.EventBroker.SubscribeNodeDeleted(ctx, nil)

	// Delete the node
	err = storageEngine.DeleteNode(storage.NodeID("n1"))
	require.NoError(t, err)

	// Verify subscription received the event
	select {
	case nodeID := <-ch:
		assert.Equal(t, "n1", nodeID)
	case <-time.After(2 * time.Second):
		t.Fatal("Subscription did not receive node deleted event")
	}
}

func TestSubscription_RelationshipCreated_Integration(t *testing.T) {
	db := testDB(t)
	dbManager := testutil.DatabaseManager(t, db)
	resolver := NewResolver(db, dbManager)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create nodes first
	storageEngine, err := resolver.getStorage()
	require.NoError(t, err)

	node1 := &storage.Node{
		ID:     storage.NodeID("n1"),
		Labels: []string{"Person"},
	}
	node2 := &storage.Node{
		ID:     storage.NodeID("n2"),
		Labels: []string{"Person"},
	}
	_, err = storageEngine.CreateNode(node1)
	require.NoError(t, err)
	_, err = storageEngine.CreateNode(node2)
	require.NoError(t, err)

	// Subscribe to relationship created events
	ch := resolver.EventBroker.SubscribeRelationshipCreated(ctx, []string{"KNOWS"})

	// Create a relationship
	edge := &storage.Edge{
		ID:        storage.EdgeID("r1"),
		Type:      "KNOWS",
		StartNode: storage.NodeID("n1"),
		EndNode:   storage.NodeID("n2"),
		Properties: map[string]interface{}{
			"since": "2020",
		},
	}
	err = storageEngine.CreateEdge(edge)
	require.NoError(t, err)

	// Verify subscription received the event
	select {
	case event := <-ch:
		assert.Equal(t, "r1", event.ID)
		assert.Equal(t, "KNOWS", event.Type)
		props := map[string]interface{}(event.Properties)
		assert.Equal(t, "2020", props["since"])
	case <-time.After(2 * time.Second):
		t.Fatal("Subscription did not receive relationship created event")
	}
}

func TestSubscription_RelationshipDeleted_Integration(t *testing.T) {
	db := testDB(t)
	dbManager := testutil.DatabaseManager(t, db)
	resolver := NewResolver(db, dbManager)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create nodes and relationship first
	storageEngine, err := resolver.getStorage()
	require.NoError(t, err)

	node1 := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Person"}}
	node2 := &storage.Node{ID: storage.NodeID("n2"), Labels: []string{"Person"}}
	_, err = storageEngine.CreateNode(node1)
	require.NoError(t, err)
	_, err = storageEngine.CreateNode(node2)
	require.NoError(t, err)

	edge := &storage.Edge{
		ID:        storage.EdgeID("r1"),
		Type:      "KNOWS",
		StartNode: storage.NodeID("n1"),
		EndNode:   storage.NodeID("n2"),
	}
	err = storageEngine.CreateEdge(edge)
	require.NoError(t, err)

	// Subscribe to relationship deleted events
	ch := resolver.EventBroker.SubscribeRelationshipDeleted(ctx, nil)

	// Delete the relationship
	err = storageEngine.DeleteEdge(storage.EdgeID("r1"))
	require.NoError(t, err)

	// Verify subscription received the event
	select {
	case relID := <-ch:
		assert.Equal(t, "r1", relID)
	case <-time.After(2 * time.Second):
		t.Fatal("Subscription did not receive relationship deleted event")
	}
}

// =============================================================================
// Subscription Resolver Tests
// =============================================================================

func TestSubscriptionResolver_NodeCreated(t *testing.T) {
	for _, brokerAvailable := range []bool{true, false} {
		name := "event broker available"
		if !brokerAvailable {
			name = "event broker missing"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			db := testDB(t)
			resolver := NewResolver(db, testutil.DatabaseManager(t, db))
			var labels []string
			if brokerAvailable {
				labels = []string{"Person"}
			} else {
				resolver.EventBroker = nil
			}
			subResolver := &subscriptionResolver{Resolver: resolver}
			ch, err := subResolver.subscriptionNodeCreated(ctx, labels)
			if !brokerAvailable {
				require.Error(t, err)
				assert.Nil(t, ch)
				assert.Contains(t, err.Error(), "event broker not initialized")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, ch)
			storageEngine, err := resolver.getStorage()
			require.NoError(t, err)
			_, err = storageEngine.CreateNode(&storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Person"}})
			require.NoError(t, err)
			select {
			case event := <-ch:
				assert.Equal(t, "n1", event.ID)
			case <-time.After(2 * time.Second):
				t.Fatal("Subscription did not receive event")
			}
		})
	}
}
