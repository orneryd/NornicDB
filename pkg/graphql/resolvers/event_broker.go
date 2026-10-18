package resolvers

import (
	"context"
	"slices"
	"sync"

	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/util/eventchannel"
)

// subscriptionInfo holds a channel and its filter parameters.
type subscriptionInfo struct {
	ch     interface{ Close() } // Typed eventchannel owner
	labels []string
	types  []string
	id     *string
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
	ch := eventchannel.New[*models.Node](10) // Buffered to prevent blocking

	b.mu.Lock()
	b.nodeCreatedSubs = append(b.nodeCreatedSubs, subscriptionInfo{
		ch:     ch,
		labels: labels,
	})
	b.mu.Unlock()

	// Cleanup on context cancellation
	go func() {
		<-ctx.Done()
		b.unsubscribeNodeCreated(ch)
	}()

	return ch.Events()
}

// SubscribeNodeUpdated subscribes to node updated events.
// Returns a channel that will receive events for nodes matching the ID or label filter.
func (b *EventBroker) SubscribeNodeUpdated(ctx context.Context, id *string, labels []string) <-chan *models.Node {
	ch := eventchannel.New[*models.Node](10)

	b.mu.Lock()
	b.nodeUpdatedSubs = append(b.nodeUpdatedSubs, subscriptionInfo{
		ch:     ch,
		labels: labels,
		id:     id,
	})
	b.mu.Unlock()

	go func() {
		<-ctx.Done()
		b.unsubscribeNodeUpdated(ch)
	}()

	return ch.Events()
}

// SubscribeNodeDeleted subscribes to node deleted events.
// Returns a channel that will receive node IDs for deleted nodes matching the label filter.
func (b *EventBroker) SubscribeNodeDeleted(ctx context.Context, labels []string) <-chan string {
	ch := eventchannel.New[string](10)

	b.mu.Lock()
	b.nodeDeletedSubs = append(b.nodeDeletedSubs, subscriptionInfo{
		ch:     ch,
		labels: labels,
	})
	b.mu.Unlock()

	go func() {
		<-ctx.Done()
		b.unsubscribeNodeDeleted(ch)
	}()

	return ch.Events()
}

// SubscribeRelationshipCreated subscribes to relationship created events.
// Returns a channel that will receive events for relationships matching the type filter.
func (b *EventBroker) SubscribeRelationshipCreated(ctx context.Context, types []string) <-chan *models.Relationship {
	ch := eventchannel.New[*models.Relationship](10)

	b.mu.Lock()
	b.relationshipCreatedSubs = append(b.relationshipCreatedSubs, subscriptionInfo{
		ch:    ch,
		types: types,
	})
	b.mu.Unlock()

	go func() {
		<-ctx.Done()
		b.unsubscribeRelationshipCreated(ch)
	}()

	return ch.Events()
}

// SubscribeRelationshipUpdated subscribes to relationship updated events.
// Returns a channel that will receive events for relationships matching the ID or type filter.
func (b *EventBroker) SubscribeRelationshipUpdated(ctx context.Context, id *string, types []string) <-chan *models.Relationship {
	ch := eventchannel.New[*models.Relationship](10)

	b.mu.Lock()
	b.relationshipUpdatedSubs = append(b.relationshipUpdatedSubs, subscriptionInfo{
		ch:    ch,
		types: types,
		id:    id,
	})
	b.mu.Unlock()

	go func() {
		<-ctx.Done()
		b.unsubscribeRelationshipUpdated(ch)
	}()

	return ch.Events()
}

// SubscribeRelationshipDeleted subscribes to relationship deleted events.
// Returns a channel that will receive relationship IDs for deleted relationships matching the type filter.
func (b *EventBroker) SubscribeRelationshipDeleted(ctx context.Context, types []string) <-chan string {
	ch := eventchannel.New[string](10)

	b.mu.Lock()
	b.relationshipDeletedSubs = append(b.relationshipDeletedSubs, subscriptionInfo{
		ch:    ch,
		types: types,
	})
	b.mu.Unlock()

	go func() {
		<-ctx.Done()
		b.unsubscribeRelationshipDeleted(ch)
	}()

	return ch.Events()
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
	if node == nil {
		return
	}

	subs := b.snapshot(&b.nodeCreatedSubs)

	// Publish to all matching subscriptions
	for _, sub := range subs {
		if matchesFilter(sub.labels, nil, nil, node.Labels, "", node.ID) {
			if ch, ok := sub.ch.(*eventchannel.Channel[*models.Node]); ok {
				ch.TrySend(node)
			}
		}
	}
}

// PublishNodeUpdated publishes a node updated event to all matching subscribers.
func (b *EventBroker) PublishNodeUpdated(node *models.Node) {
	if node == nil {
		return
	}

	subs := b.snapshot(&b.nodeUpdatedSubs)

	for _, sub := range subs {
		if matchesFilter(sub.labels, nil, sub.id, node.Labels, "", node.ID) {
			if ch, ok := sub.ch.(*eventchannel.Channel[*models.Node]); ok {
				ch.TrySend(node)
			}
		}
	}
}

// PublishNodeDeleted publishes a node deleted event to all matching subscribers.
// Note: For deleted events, we only have the ID, so label filtering is not possible.
// We send to all subscriptions (label filters are ignored for deletions).
func (b *EventBroker) PublishNodeDeleted(nodeID string) {
	if nodeID == "" {
		return
	}

	subs := b.snapshot(&b.nodeDeletedSubs)

	// Send to all subscriptions (can't filter by labels without node data)
	for _, sub := range subs {
		if ch, ok := sub.ch.(*eventchannel.Channel[string]); ok {
			ch.TrySend(nodeID)
		}
	}
}

// PublishRelationshipCreated publishes a relationship created event to all matching subscribers.
func (b *EventBroker) PublishRelationshipCreated(rel *models.Relationship) {
	if rel == nil {
		return
	}

	subs := b.snapshot(&b.relationshipCreatedSubs)

	for _, sub := range subs {
		if matchesFilter(nil, sub.types, nil, nil, rel.Type, rel.ID) {
			if ch, ok := sub.ch.(*eventchannel.Channel[*models.Relationship]); ok {
				ch.TrySend(rel)
			}
		}
	}
}

// PublishRelationshipUpdated publishes a relationship updated event to all matching subscribers.
func (b *EventBroker) PublishRelationshipUpdated(rel *models.Relationship) {
	if rel == nil {
		return
	}

	subs := b.snapshot(&b.relationshipUpdatedSubs)

	for _, sub := range subs {
		if matchesFilter(nil, sub.types, sub.id, nil, rel.Type, rel.ID) {
			if ch, ok := sub.ch.(*eventchannel.Channel[*models.Relationship]); ok {
				ch.TrySend(rel)
			}
		}
	}
}

// PublishRelationshipDeleted publishes a relationship deleted event to all matching subscribers.
// Note: For deleted events, we only have the ID, so type filtering is not possible.
// We send to all subscriptions (type filters are ignored for deletions).
func (b *EventBroker) PublishRelationshipDeleted(relID string) {
	if relID == "" {
		return
	}

	subs := b.snapshot(&b.relationshipDeletedSubs)

	// Send to all subscriptions (can't filter by type without relationship data)
	for _, sub := range subs {
		if ch, ok := sub.ch.(*eventchannel.Channel[string]); ok {
			ch.TrySend(relID)
		}
	}
}

// Unsubscribe methods remove channels from subscriptions

func (b *EventBroker) unsubscribeNodeCreated(ch *eventchannel.Channel[*models.Node]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, sub := range b.nodeCreatedSubs {
		if subCh, ok := sub.ch.(*eventchannel.Channel[*models.Node]); ok && subCh == ch {
			b.nodeCreatedSubs = slices.Delete(b.nodeCreatedSubs, i, i+1)
			ch.Close()
			return
		}
	}
}

func (b *EventBroker) unsubscribeNodeUpdated(ch *eventchannel.Channel[*models.Node]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, sub := range b.nodeUpdatedSubs {
		if subCh, ok := sub.ch.(*eventchannel.Channel[*models.Node]); ok && subCh == ch {
			b.nodeUpdatedSubs = slices.Delete(b.nodeUpdatedSubs, i, i+1)
			ch.Close()
			return
		}
	}
}

func (b *EventBroker) unsubscribeNodeDeleted(ch *eventchannel.Channel[string]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, sub := range b.nodeDeletedSubs {
		if subCh, ok := sub.ch.(*eventchannel.Channel[string]); ok && subCh == ch {
			b.nodeDeletedSubs = slices.Delete(b.nodeDeletedSubs, i, i+1)
			ch.Close()
			return
		}
	}
}

func (b *EventBroker) unsubscribeRelationshipCreated(ch *eventchannel.Channel[*models.Relationship]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, sub := range b.relationshipCreatedSubs {
		if subCh, ok := sub.ch.(*eventchannel.Channel[*models.Relationship]); ok && subCh == ch {
			b.relationshipCreatedSubs = slices.Delete(b.relationshipCreatedSubs, i, i+1)
			ch.Close()
			return
		}
	}
}

func (b *EventBroker) unsubscribeRelationshipUpdated(ch *eventchannel.Channel[*models.Relationship]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, sub := range b.relationshipUpdatedSubs {
		if subCh, ok := sub.ch.(*eventchannel.Channel[*models.Relationship]); ok && subCh == ch {
			b.relationshipUpdatedSubs = slices.Delete(b.relationshipUpdatedSubs, i, i+1)
			ch.Close()
			return
		}
	}
}

func (b *EventBroker) unsubscribeRelationshipDeleted(ch *eventchannel.Channel[string]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, sub := range b.relationshipDeletedSubs {
		if subCh, ok := sub.ch.(*eventchannel.Channel[string]); ok && subCh == ch {
			b.relationshipDeletedSubs = slices.Delete(b.relationshipDeletedSubs, i, i+1)
			ch.Close()
			return
		}
	}
}

// Close closes all subscription channels and cleans up resources.
func (b *EventBroker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Close every owner, including channels retained by publisher snapshots.
	for _, subs := range [][]subscriptionInfo{
		b.nodeCreatedSubs, b.nodeUpdatedSubs, b.nodeDeletedSubs,
		b.relationshipCreatedSubs, b.relationshipUpdatedSubs, b.relationshipDeletedSubs,
		b.searchStreamSubs,
	} {
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
}
