package storage

import (
	"sort"
	"sync"
)

type schemaTokenOrder struct {
	labels                []string
	relationships         []string
	labelPositions        map[string]int
	relationshipPositions map[string]int
}

// schemaTokenSnapshot contains only names already accepted by the schema
// owner. Names are appended after persistence; replacement publishes a new
// snapshot so existing readers never observe partially replaced state.
type schemaTokenSnapshot struct {
	labels        sync.Map
	relationships sync.Map
}

// publishTokenSnapshotLocked runs while sm.mu is held by the schema owner.
// On replacement it constructs a new snapshot; otherwise it appends only the
// newly persisted names, avoiding a complete copy for each distinct token.
func (sm *SchemaManager) publishTokenSnapshotLocked(labels, relationships []string) {
	snapshot := sm.tokenSnapshot.Load()
	if snapshot == nil {
		snapshot = &schemaTokenSnapshot{}
		labels, relationships = sm.tokenOrder.labels, sm.tokenOrder.relationships
	}
	for _, name := range labels {
		snapshot.labels.Store(name, struct{}{})
	}
	for _, name := range relationships {
		snapshot.relationships.Store(name, struct{}{})
	}
	sm.tokenSnapshot.Store(snapshot)
}

func (snapshot *schemaTokenSnapshot) knows(labels, relationships []string) bool {
	for _, name := range labels {
		if _, ok := snapshot.labels.Load(name); !ok {
			return false
		}
	}
	for _, name := range relationships {
		if _, ok := snapshot.relationships.Load(name); !ok {
			return false
		}
	}
	return true
}

func newSchemaTokenOrder(labels, relationships []string) schemaTokenOrder {
	order := schemaTokenOrder{
		labelPositions:        make(map[string]int),
		relationshipPositions: make(map[string]int),
	}
	order.labels = appendTokenNames(order.labels, order.labelPositions, labels)
	order.relationships = appendTokenNames(order.relationships, order.relationshipPositions, relationships)
	return order
}

func appendTokenNames(names []string, positions map[string]int, added []string) []string {
	for _, name := range added {
		if _, exists := positions[name]; !exists {
			positions[name] = len(names)
			names = append(names, name)
		}
	}
	return names
}

// RegisterTokens allocates durable label and relationship-type positions.
// Allocations are retained when their last entity is deleted.
// Example: schema.RegisterTokens([]string{"Person"}, []string{"KNOWS"}).
func (sm *SchemaManager) RegisterTokens(labels, relationships []string) error {
	if len(labels) == 0 && len(relationships) == 0 {
		return nil
	}
	// Known names do not depend on an unrelated token's pending persistence.
	// The second load detects schema replacement while checking the snapshot;
	// additions leave previously published names valid even before they commit.
	if snapshot := sm.tokenSnapshot.Load(); snapshot != nil &&
		snapshot.knows(labels, relationships) &&
		sm.tokenSnapshot.Load() == snapshot {
		return nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.tokenOrder.labelPositions == nil {
		sm.tokenOrder = newSchemaTokenOrder(nil, nil)
	}
	labelCount, relationshipCount := len(sm.tokenOrder.labels), len(sm.tokenOrder.relationships)
	sm.tokenOrder.labels = appendTokenNames(sm.tokenOrder.labels, sm.tokenOrder.labelPositions, labels)
	sm.tokenOrder.relationships = appendTokenNames(sm.tokenOrder.relationships, sm.tokenOrder.relationshipPositions, relationships)
	if labelCount == len(sm.tokenOrder.labels) && relationshipCount == len(sm.tokenOrder.relationships) {
		return nil
	}
	if sm.persist != nil {
		if err := sm.persist(sm.exportDefinitionLocked()); err != nil {
			sm.tokenOrder = newSchemaTokenOrder(sm.tokenOrder.labels[:labelCount], sm.tokenOrder.relationships[:relationshipCount])
			return err
		}
	}
	sm.publishTokenSnapshotLocked(sm.tokenOrder.labels[labelCount:], sm.tokenOrder.relationships[relationshipCount:])
	return nil
}

// OrderTokens returns a copy of in-use names ordered by durable token position.
// Unknown legacy names follow known names in lexical order. Set relationships
// for relationship types, or leave it false for labels.
// Example: schema.OrderTokens([]string{"Person", "Company"}, false).
func (sm *SchemaManager) OrderTokens(names []string, relationships bool) []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	positions := sm.tokenOrder.labelPositions
	if relationships {
		positions = sm.tokenOrder.relationshipPositions
	}
	ordered := append([]string(nil), names...)
	sort.SliceStable(ordered, func(left, right int) bool {
		leftPosition, leftKnown := positions[ordered[left]]
		rightPosition, rightKnown := positions[ordered[right]]
		if leftKnown != rightKnown {
			return leftKnown
		}
		if leftKnown {
			return leftPosition < rightPosition
		}
		return ordered[left] < ordered[right]
	})
	return ordered
}
