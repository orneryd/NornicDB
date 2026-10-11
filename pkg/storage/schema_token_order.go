package storage

import "sort"

type schemaTokenOrder struct {
	labels                []string
	relationships         []string
	labelPositions        map[string]int
	relationshipPositions map[string]int
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
	// Fast path: token registration is per-node hot-path work, and the
	// common case is that every name is already known. Checking under a
	// SHARED lock keeps concurrent readers (constraint validation, unique-
	// value tracking) from serializing behind the write lock, which only
	// the first sight of a name ever needs.
	sm.mu.RLock()
	allKnown := len(sm.tokenOrder.labelPositions) > 0 && len(sm.tokenOrder.relationshipPositions) > 0
	if allKnown {
		for _, name := range labels {
			if _, ok := sm.tokenOrder.labelPositions[name]; !ok {
				allKnown = false
				break
			}
		}
	}
	if allKnown {
		for _, name := range relationships {
			if _, ok := sm.tokenOrder.relationshipPositions[name]; !ok {
				allKnown = false
				break
			}
		}
	}
	sm.mu.RUnlock()
	if allKnown {
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
