package resolvers

import (
	"context"
	"slices"
	"sync"

	"github.com/orneryd/nornicdb/pkg/auth"

	"github.com/orneryd/nornicdb/pkg/graphql/models"
)

// subscriptionInfo holds a channel and its filter parameters.
type subscriptionInfo struct {
	canRead func() bool
	ch subscriptionChannel
	database string
	labels []string
	types  []string
	id     *string
}

func matchesDatabase(subscriptionDatabase, eventDatabase string) bool {
	// Empty is the legacy broker-wide scope; production events carry a database.
	return subscriptionDatabase == "" || eventDatabase == "" || subscriptionDatabase == eventDatabase
}

// EventBroker is a thread-safe pub/sub system for GraphQL subscriptions.
// It manages subscription channels and filters events based on subscription criteria.
//
// Architecture:
//   - Each subscription type has a slice of subscriptionInfo
//   - Each subscriptionInfo contains the channel and filter parameters
//   - When event occurs, check all subscriptions and send to matching ones
//
// Thread Safety:
//   - Uses RWMutex for concurrent access
//   - Channels are buffered (size: 10) to prevent blocking
//   - Non-blocking publishes (drops events if channel full)
//   - Each channel owner serializes sending and closure, including stale snapshots
//
// Example:
//
//	broker := NewEventBroker()
//	ctx := context.Background()
//	ch := broker.SubscribeNodeCreated(ctx, []string{"Person"})
//	broker.PublishNodeCreated(node)
type EventBroker struct {
	// Node event subscriptions
	nodeCreatedSubs []subscriptionInfo
	nodeUpdatedSubs []subscriptionInfo
	nodeDeletedSubs []subscriptionInfo

	// Relationship event subscriptions
	relationshipCreatedSubs []subscriptionInfo
	relationshipUpdatedSubs []subscriptionInfo
	relationshipDeletedSubs []subscriptionInfo

	// Search stream subscriptions (future enhancement)
	searchStreamSubs []subscriptionInfo

	mu sync.RWMutex

	// afterSnapshot is a test barrier, set before starting publishers.
	afterSnapshot func()
}

// NewEventBroker creates a new event broker for GraphQL subscriptions.
func NewEventBroker() *EventBroker {
	return &EventBroker{
		nodeCreatedSubs:         make([]subscriptionInfo, 0),
		nodeUpdatedSubs:         make([]subscriptionInfo, 0),
		nodeDeletedSubs:         make([]subscriptionInfo, 0),
		relationshipCreatedSubs: make([]subscriptionInfo, 0),
		relationshipUpdatedSubs: make([]subscriptionInfo, 0),
		relationshipDeletedSubs: make([]subscriptionInfo, 0),
		searchStreamSubs:        make([]subscriptionInfo, 0),
	}
}

// matchesFilter checks if an event matches the subscription filter.
func matchesFilter(labels []string, types []string, id *string, eventLabels []string, eventType string, eventID string) bool {
	// ID filter takes precedence
	if id != nil && *id != "" {
		return eventID == *id
	}

	// Label filtering for nodes
	if len(labels) > 0 {
		if len(eventLabels) == 0 {
			return false
		}
		// Check if any subscription label matches any event label
		eventLabelSet := make(map[string]bool)
		for _, l := range eventLabels {
			eventLabelSet[l] = true
		}
		for _, filterLabel := range labels {
			if eventLabelSet[filterLabel] {
				return true
			}
		}
		return false
	}

	// Type filtering for relationships
	if len(types) > 0 {
		for _, filterType := range types {
			if eventType == filterType {
				return true
			}
		}
		return false
	}

	// No filter = match everything
	return true
}

// SubscribeNodeCreated subscribes to node created events.
// Returns a channel that will receive events for nodes matching the label filter.
// If labels is empty, receives all node created events.
func (b *EventBroker) SubscribeNodeCreated(ctx context.Context, labels []string) <-chan *models.Node {
	return b.SubscribeNodeCreatedInDatabase(ctx, "", labels)
}

func (b *EventBroker) SubscribeNodeCreatedInDatabase(ctx context.Context, database string, labels []string) <-chan *models.Node {
	database, canRead := auth.BindRequestDatabaseRead(ctx, database)
	ch := newSubscriptionOwner[*models.Node]() // Buffered to prevent blocking

	b.addSubscription(ctx, &b.nodeCreatedSubs, subscriptionInfo{
		canRead: canRead,
		ch:     ch,
		database: database,
		labels: labels,
	}, func() {
		b.unsubscribeNodeCreated(ch)
	})

	return ch.Events()
}

// SubscribeNodeUpdated subscribes to node updated events.
// Returns a channel that will receive events for nodes matching the ID or label filter.
func (b *EventBroker) SubscribeNodeUpdated(ctx context.Context, id *string, labels []string) <-chan *models.Node {
	return b.SubscribeNodeUpdatedInDatabase(ctx, "", id, labels)
}

func (b *EventBroker) SubscribeNodeUpdatedInDatabase(ctx context.Context, database string, id *string, labels []string) <-chan *models.Node {
	database, canRead := auth.BindRequestDatabaseRead(ctx, database)
	ch := newSubscriptionOwner[*models.Node]()

	b.addSubscription(ctx, &b.nodeUpdatedSubs, subscriptionInfo{
		canRead: canRead,
		ch:     ch,
		database: database,
		labels: labels,
		id:     id,
	}, func() {
		b.unsubscribeNodeUpdated(ch)
	})

	return ch.Events()
}

// SubscribeNodeDeleted subscribes to node deleted events.
// Returns a channel that will receive node IDs for deleted nodes matching the label filter.
func (b *EventBroker) SubscribeNodeDeleted(ctx context.Context, labels []string) <-chan string {
	return b.SubscribeNodeDeletedInDatabase(ctx, "", labels)
}

func (b *EventBroker) SubscribeNodeDeletedInDatabase(ctx context.Context, database string, labels []string) <-chan string {
	database, canRead := auth.BindRequestDatabaseRead(ctx, database)
	ch := newSubscriptionOwner[string]()

	b.addSubscription(ctx, &b.nodeDeletedSubs, subscriptionInfo{
		canRead: canRead,
		ch:     ch,
		database: database,
		labels: labels,
	}, func() {
		b.unsubscribeNodeDeleted(ch)
	})

	return ch.Events()
}

// SubscribeRelationshipCreated subscribes to relationship created events.
// Returns a channel that will receive events for relationships matching the type filter.
func (b *EventBroker) SubscribeRelationshipCreated(ctx context.Context, types []string) <-chan *models.Relationship {
	return b.SubscribeRelationshipCreatedInDatabase(ctx, "", types)
}

func (b *EventBroker) SubscribeRelationshipCreatedInDatabase(ctx context.Context, database string, types []string) <-chan *models.Relationship {
	database, canRead := auth.BindRequestDatabaseRead(ctx, database)
	ch := newSubscriptionOwner[*models.Relationship]()

	b.addSubscription(ctx, &b.relationshipCreatedSubs, subscriptionInfo{
		canRead: canRead,
		ch:    ch,
		database: database,
		types: types,
	}, func() {
		b.unsubscribeRelationshipCreated(ch)
	})

	return ch.Events()
}

// SubscribeRelationshipUpdated subscribes to relationship updated events.
// Returns a channel that will receive events for relationships matching the ID or type filter.
func (b *EventBroker) SubscribeRelationshipUpdated(ctx context.Context, id *string, types []string) <-chan *models.Relationship {
	return b.SubscribeRelationshipUpdatedInDatabase(ctx, "", id, types)
}

func (b *EventBroker) SubscribeRelationshipUpdatedInDatabase(ctx context.Context, database string, id *string, types []string) <-chan *models.Relationship {
	database, canRead := auth.BindRequestDatabaseRead(ctx, database)
	ch := newSubscriptionOwner[*models.Relationship]()

	b.addSubscription(ctx, &b.relationshipUpdatedSubs, subscriptionInfo{
		canRead: canRead,
		ch:    ch,
		database: database,
		types: types,
		id:    id,
	}, func() {
		b.unsubscribeRelationshipUpdated(ch)
	})

	return ch.Events()
}

// SubscribeRelationshipDeleted subscribes to relationship deleted events.
// Returns a channel that will receive relationship IDs for deleted relationships matching the type filter.
func (b *EventBroker) SubscribeRelationshipDeleted(ctx context.Context, types []string) <-chan string {
	return b.SubscribeRelationshipDeletedInDatabase(ctx, "", types)
}

func (b *EventBroker) SubscribeRelationshipDeletedInDatabase(ctx context.Context, database string, types []string) <-chan string {
	database, canRead := auth.BindRequestDatabaseRead(ctx, database)
	ch := newSubscriptionOwner[string]()

	b.addSubscription(ctx, &b.relationshipDeletedSubs, subscriptionInfo{
		canRead: canRead,
		ch:    ch,
		database: database,
		types: types,
	}, func() {
		b.unsubscribeRelationshipDeleted(ch)
	})

	return ch.Events()
}

// addSubscription registers the owner before starting its cancellation watcher.
// Closing the owner also ends the watcher, even for a background context.
func (b *EventBroker) addSubscription(ctx context.Context, subscriptions *[]subscriptionInfo, sub subscriptionInfo, unsubscribe func()) {
	b.mu.Lock()
	*subscriptions = append(*subscriptions, sub)
	b.mu.Unlock()
	sub.ch.watch(ctx, unsubscribe)
}

// snapshot copies the subscription list; entries retain their channel owner.
func (b *EventBroker) snapshot(subscriptions *[]subscriptionInfo) []subscriptionInfo {
	b.mu.RLock()
	subs := append([]subscriptionInfo(nil), (*subscriptions)...)
	b.mu.RUnlock()
	if b.afterSnapshot != nil {
		b.afterSnapshot()
	}
	return subs
}

// PublishNodeCreated publishes a node created event to all matching subscribers.
func (b *EventBroker) PublishNodeCreated(node *models.Node) {
	b.PublishNodeCreatedInDatabase("", node)
}

func (b *EventBroker) PublishNodeCreatedInDatabase(database string, node *models.Node) {
	if node == nil {
		return
	}

	subs := b.snapshot(&b.nodeCreatedSubs)

	// Publish to all matching subscriptions
	for _, sub := range subs {
		if matchesDatabase(sub.database, database) && matchesFilter(sub.labels, nil, nil, node.Labels, "", node.ID) && sub.canRead() {
			if ch, ok := sub.ch.(*subscriptionOwner[*models.Node]); ok {
				ch.TrySend(node)
			}
		}
	}
}

// PublishNodeUpdated publishes a node updated event to all matching subscribers.
func (b *EventBroker) PublishNodeUpdated(node *models.Node) {
	b.PublishNodeUpdatedInDatabase("", node)
}

func (b *EventBroker) PublishNodeUpdatedInDatabase(database string, node *models.Node) {
	if node == nil {
		return
	}

	subs := b.snapshot(&b.nodeUpdatedSubs)

	for _, sub := range subs {
		if matchesDatabase(sub.database, database) && matchesFilter(sub.labels, nil, sub.id, node.Labels, "", node.ID) && sub.canRead() {
			if ch, ok := sub.ch.(*subscriptionOwner[*models.Node]); ok {
				ch.TrySend(node)
			}
		}
	}
}

// PublishNodeDeleted publishes a node deleted event to all matching subscribers.
// Note: For deleted events, we only have the ID, so label filtering is not possible.
// We send to all subscriptions (label filters are ignored for deletions).
func (b *EventBroker) PublishNodeDeleted(nodeID string) {
	b.PublishNodeDeletedInDatabase("", nodeID)
}

func (b *EventBroker) PublishNodeDeletedInDatabase(database, nodeID string) {
	if nodeID == "" {
		return
	}

	subs := b.snapshot(&b.nodeDeletedSubs)

	// Send to all subscriptions (can't filter by labels without node data)
	for _, sub := range subs {
		if !matchesDatabase(sub.database, database) || !sub.canRead() {
			continue
		}
		if ch, ok := sub.ch.(*subscriptionOwner[string]); ok {
			ch.TrySend(nodeID)
		}
	}
}

// PublishRelationshipCreated publishes a relationship created event to all matching subscribers.
func (b *EventBroker) PublishRelationshipCreated(rel *models.Relationship) {
	b.PublishRelationshipCreatedInDatabase("", rel)
}

func (b *EventBroker) PublishRelationshipCreatedInDatabase(database string, rel *models.Relationship) {
	if rel == nil {
		return
	}

	subs := b.snapshot(&b.relationshipCreatedSubs)

	for _, sub := range subs {
		if matchesDatabase(sub.database, database) && matchesFilter(nil, sub.types, nil, nil, rel.Type, rel.ID) && sub.canRead() {
			if ch, ok := sub.ch.(*subscriptionOwner[*models.Relationship]); ok {
				ch.TrySend(rel)
			}
		}
	}
}

// PublishRelationshipUpdated publishes a relationship updated event to all matching subscribers.
func (b *EventBroker) PublishRelationshipUpdated(rel *models.Relationship) {
	b.PublishRelationshipUpdatedInDatabase("", rel)
}

func (b *EventBroker) PublishRelationshipUpdatedInDatabase(database string, rel *models.Relationship) {
	if rel == nil {
		return
	}

	subs := b.snapshot(&b.relationshipUpdatedSubs)

	for _, sub := range subs {
		if matchesDatabase(sub.database, database) && matchesFilter(nil, sub.types, sub.id, nil, rel.Type, rel.ID) && sub.canRead() {
			if ch, ok := sub.ch.(*subscriptionOwner[*models.Relationship]); ok {
				ch.TrySend(rel)
			}
		}
	}
}

// PublishRelationshipDeleted publishes a relationship deleted event to all matching subscribers.
// Note: For deleted events, we only have the ID, so type filtering is not possible.
// We send to all subscriptions (type filters are ignored for deletions).
func (b *EventBroker) PublishRelationshipDeleted(relID string) {
	b.PublishRelationshipDeletedInDatabase("", relID)
}

func (b *EventBroker) PublishRelationshipDeletedInDatabase(database, relID string) {
	if relID == "" {
		return
	}

	subs := b.snapshot(&b.relationshipDeletedSubs)

	// Send to all subscriptions (can't filter by type without relationship data)
	for _, sub := range subs {
		if !matchesDatabase(sub.database, database) || !sub.canRead() {
			continue
		}
		if ch, ok := sub.ch.(*subscriptionOwner[string]); ok {
			ch.TrySend(relID)
		}
	}
}

// Unsubscribe methods remove channels from subscriptions

func (b *EventBroker) unsubscribeNodeCreated(ch *subscriptionOwner[*models.Node]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, sub := range b.nodeCreatedSubs {
		if subCh, ok := sub.ch.(*subscriptionOwner[*models.Node]); ok && subCh == ch {
			b.nodeCreatedSubs = slices.Delete(b.nodeCreatedSubs, i, i+1)
			ch.Close()
			return
		}
	}
}

func (b *EventBroker) unsubscribeNodeUpdated(ch *subscriptionOwner[*models.Node]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, sub := range b.nodeUpdatedSubs {
		if subCh, ok := sub.ch.(*subscriptionOwner[*models.Node]); ok && subCh == ch {
			b.nodeUpdatedSubs = slices.Delete(b.nodeUpdatedSubs, i, i+1)
			ch.Close()
			return
		}
	}
}

func (b *EventBroker) unsubscribeNodeDeleted(ch *subscriptionOwner[string]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, sub := range b.nodeDeletedSubs {
		if subCh, ok := sub.ch.(*subscriptionOwner[string]); ok && subCh == ch {
			b.nodeDeletedSubs = slices.Delete(b.nodeDeletedSubs, i, i+1)
			ch.Close()
			return
		}
	}
}

func (b *EventBroker) unsubscribeRelationshipCreated(ch *subscriptionOwner[*models.Relationship]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, sub := range b.relationshipCreatedSubs {
		if subCh, ok := sub.ch.(*subscriptionOwner[*models.Relationship]); ok && subCh == ch {
			b.relationshipCreatedSubs = slices.Delete(b.relationshipCreatedSubs, i, i+1)
			ch.Close()
			return
		}
	}
}

func (b *EventBroker) unsubscribeRelationshipUpdated(ch *subscriptionOwner[*models.Relationship]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, sub := range b.relationshipUpdatedSubs {
		if subCh, ok := sub.ch.(*subscriptionOwner[*models.Relationship]); ok && subCh == ch {
			b.relationshipUpdatedSubs = slices.Delete(b.relationshipUpdatedSubs, i, i+1)
			ch.Close()
			return
		}
	}
}

func (b *EventBroker) unsubscribeRelationshipDeleted(ch *subscriptionOwner[string]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, sub := range b.relationshipDeletedSubs {
		if subCh, ok := sub.ch.(*subscriptionOwner[string]); ok && subCh == ch {
			b.relationshipDeletedSubs = slices.Delete(b.relationshipDeletedSubs, i, i+1)
			ch.Close()
			return
		}
	}
}

// Close closes all subscription channels and cleans up resources.
func (b *EventBroker) Close() {
	b.mu.Lock()

	// Close every owner, including channels retained by publisher snapshots.
	lists := [][]subscriptionInfo{
		b.nodeCreatedSubs, b.nodeUpdatedSubs, b.nodeDeletedSubs,
		b.relationshipCreatedSubs, b.relationshipUpdatedSubs, b.relationshipDeletedSubs,
		b.searchStreamSubs,
	}
	for _, subs := range lists {
		for _, sub := range subs {
			sub.ch.Close()
		}
	}

	// Clear slices
	b.nodeCreatedSubs = make([]subscriptionInfo, 0)
	b.nodeUpdatedSubs = make([]subscriptionInfo, 0)
	b.nodeDeletedSubs = make([]subscriptionInfo, 0)
	b.relationshipCreatedSubs = make([]subscriptionInfo, 0)
	b.relationshipUpdatedSubs = make([]subscriptionInfo, 0)
	b.relationshipDeletedSubs = make([]subscriptionInfo, 0)
	b.searchStreamSubs = make([]subscriptionInfo, 0)
	b.mu.Unlock()

	// A watcher may already be waiting for b.mu to unsubscribe. Join after
	// releasing that lock so shutdown cannot deadlock with cancellation.
	for _, subs := range lists {
		for _, sub := range subs {
			sub.ch.wait()
		}
	}
}
