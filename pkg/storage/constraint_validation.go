// Package storage - Constraint validation when constraints are created.
package storage

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// ValidateConstraintOnCreation validates that all existing data satisfies the constraint.
// This is called when CREATE CONSTRAINT is executed, matching Neo4j behavior.
func (b *BadgerEngine) ValidateConstraintOnCreation(c Constraint) error {
	return ValidateConstraintOnCreationForEngine(b, c)
}

// ValidateConstraintOnCreationForEngine validates constraints using the Engine interface.
// This allows callers (like Cypher) to validate through wrapper engines (namespaced, WAL, etc.).
func ValidateConstraintOnCreationForEngine(engine Engine, c Constraint) error {
	if c.EffectiveEntityType() == ConstraintEntityRelationship {
		return validateRelationshipConstraintOnCreationForEngine(engine, c)
	}
	switch c.Type {
	case ConstraintUnique:
		return validateUniqueConstraintOnCreationWithEngine(engine, c)
	case ConstraintNodeKey:
		return validateNodeKeyConstraintOnCreationWithEngine(engine, c)
	case ConstraintExists:
		return validateExistenceConstraintOnCreationWithEngine(engine, c)
	case ConstraintTemporal:
		return validateTemporalConstraintOnCreationWithEngine(engine, c)
	case ConstraintDomain:
		return validateDomainConstraintOnCreationForEngine(engine, c)
	default:
		return fmt.Errorf("unknown constraint type: %s", c.Type)
	}
}

// RefreshUniqueConstraintValuesForEngine rebuilds single-property UNIQUE value
// caches from the engine after a schema mutation has been admitted.
func RefreshUniqueConstraintValuesForEngine(engine Engine, schema *SchemaManager) error {
	if engine == nil || schema == nil {
		return nil
	}

	schema.mu.RLock()
	uniqueConstraints := make([]*UniqueConstraint, 0, len(schema.uniqueConstraints))
	for _, uc := range schema.uniqueConstraints {
		uniqueConstraints = append(uniqueConstraints, uc)
	}
	schema.mu.RUnlock()
	if len(uniqueConstraints) == 0 {
		return nil
	}

	for _, uc := range uniqueConstraints {
		uc.mu.Lock()
		uc.values = make(map[interface{}]NodeID)
		uc.valuesAuthoritative = false
		uc.mu.Unlock()
	}

	nodes, err := engine.AllNodes()
	if err != nil {
		return fmt.Errorf("refresh unique constraint values: scan nodes: %w", err)
	}
	for _, node := range nodes {
		if node == nil {
			continue
		}
		storageNodeID := EnsureNodeIDDatabasePrefixForEngine(engine, node.ID)
		for _, label := range node.Labels {
			for propName, propValue := range node.Properties {
				if err := schema.CheckUniqueConstraint(label, propName, propValue, storageNodeID); err != nil {
					return fmt.Errorf("refresh unique constraint values: %w", err)
				}
				schema.RegisterUniqueValue(label, propName, propValue, storageNodeID)
			}
		}
	}

	for _, uc := range uniqueConstraints {
		uc.mu.Lock()
		uc.valuesAuthoritative = true
		uc.mu.Unlock()
	}
	return nil
}

// validateRelationshipConstraintOnCreationForEngine validates relationship constraints
// using the Engine interface. It scans all edges for violations.
func validateRelationshipConstraintOnCreationForEngine(engine Engine, c Constraint) error {
	edges, err := engine.AllEdges()
	if err != nil {
		return fmt.Errorf("scanning edges: %w", err)
	}

	switch c.Type {
	case ConstraintUnique:
		return validateRelUniquenessOnEdges(edges, c)
	case ConstraintExists:
		return validateRelExistenceOnEdges(edges, c)
	case ConstraintPropertyType:
		// Handled separately via PropertyTypeConstraint path
		return nil
	case ConstraintRelationshipKey:
		// Relationship key = existence + uniqueness on all key properties
		if err := validateRelExistenceOnEdges(edges, c); err != nil {
			return err
		}
		return validateRelCompositeUniquenessOnEdges(edges, c)
	case ConstraintTemporal:
		return validateRelTemporalOnCreationForEngine(edges, c)
	case ConstraintDomain:
		return validateRelDomainOnCreationForEngine(edges, c)
	case ConstraintCardinality:
		return validateCardinalityOnCreationForEngine(edges, c)
	case ConstraintPolicy:
		return validatePolicyOnCreationForEngine(engine, edges, c)
	default:
		return fmt.Errorf("unsupported relationship constraint type: %s", c.Type)
	}
}

// validateRelUniquenessOnEdges checks uniqueness for relationship properties.
func validateRelUniquenessOnEdges(edges []*Edge, c Constraint) error {
	if len(c.Properties) == 1 {
		property := c.Properties[0]
		seen := make(map[interface{}]EdgeID)
		for _, edge := range edges {
			if edge.Type != c.Label {
				continue
			}
			value := edge.Properties[property]
			if value == nil {
				continue
			}
			if existingID, found := seen[value]; found {
				return &ConstraintViolationError{
					Type:       ConstraintUnique,
					Label:      c.Label,
					Properties: []string{property},
					Message: fmt.Sprintf("Cannot create UNIQUE constraint on relationship: edges %s and %s both have %s=%v",
						existingID, edge.ID, property, value),
				}
			}
			seen[value] = edge.ID
		}
		return nil
	}
	return validateRelCompositeUniquenessOnEdges(edges, c)
}

// validateRelCompositeUniquenessOnEdges checks composite uniqueness for relationship properties.
func validateRelCompositeUniquenessOnEdges(edges []*Edge, c Constraint) error {
	type compositeKey string
	seen := make(map[compositeKey]EdgeID)
	for _, edge := range edges {
		if edge.Type != c.Label {
			continue
		}
		// Build composite key — skip if any property is nil
		allPresent := true
		parts := make([]string, len(c.Properties))
		for i, prop := range c.Properties {
			val := edge.Properties[prop]
			if val == nil {
				allPresent = false
				break
			}
			parts[i] = fmt.Sprintf("%v", val)
		}
		if !allPresent {
			continue
		}
		key := compositeKey(strings.Join(parts, "\x00"))
		if existingID, found := seen[key]; found {
			return &ConstraintViolationError{
				Type:       ConstraintUnique,
				Label:      c.Label,
				Properties: c.Properties,
				Message: fmt.Sprintf("Cannot create UNIQUE constraint on relationship: edges %s and %s have duplicate composite key %v",
					existingID, edge.ID, parts),
			}
		}
		seen[key] = edge.ID
	}
	return nil
}

// validateRelExistenceOnEdges checks existence for relationship properties.
func validateRelExistenceOnEdges(edges []*Edge, c Constraint) error {
	for _, edge := range edges {
		if edge.Type != c.Label {
			continue
		}
		for _, prop := range c.Properties {
			value := edge.Properties[prop]
			if value == nil {
				return &ConstraintViolationError{
					Type:       ConstraintExists,
					Label:      c.Label,
					Properties: []string{prop},
					Message: fmt.Sprintf("Cannot create constraint on relationship: edge %s is missing required property %s",
						edge.ID, prop),
				}
			}
		}
	}
	return nil
}

// temporalCompositeKey builds a composite key string from multiple key property values on an edge.
func temporalCompositeKey(edge *Edge, keyProps []string) (string, error) {
	parts := make([]string, len(keyProps))
	for i, prop := range keyProps {
		val := edge.Properties[prop]
		if val == nil {
			return "", fmt.Errorf("edge %s has null %s", edge.ID, prop)
		}
		parts[i] = fmt.Sprint(val)
	}
	return strings.Join(parts, "\x00"), nil
}

// validateRelTemporalOnCreationForEngine checks temporal no-overlap for relationship properties.
// Supports 3+ properties: the last 2 are always (valid_from, valid_to), everything before
// that forms a composite key (e.g., from_id, to_id, valid_from, valid_to).
func validateRelTemporalOnCreationForEngine(edges []*Edge, c Constraint) error {
	if len(c.Properties) < 3 {
		return fmt.Errorf("TEMPORAL constraint requires at least 3 properties (key..., valid_from, valid_to)")
	}

	keyProps := c.Properties[:len(c.Properties)-2]
	startProp := c.Properties[len(c.Properties)-2]
	endProp := c.Properties[len(c.Properties)-1]

	type edgeInterval struct {
		temporalInterval
		edgeID EdgeID
	}

	byKey := make(map[string][]edgeInterval)
	for _, edge := range edges {
		if edge.Type != c.Label {
			continue
		}
		key, err := temporalCompositeKey(edge, keyProps)
		if err != nil {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      c.Label,
				Properties: c.Properties,
				Message:    fmt.Sprintf("Cannot create TEMPORAL constraint: %s", err),
			}
		}

		start, ok := coerceTemporalTime(edge.Properties[startProp])
		if !ok {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      c.Label,
				Properties: c.Properties,
				Message:    fmt.Sprintf("Cannot create TEMPORAL constraint: edge %s has invalid %s", edge.ID, startProp),
			}
		}
		end, hasEnd := coerceTemporalTime(edge.Properties[endProp])

		byKey[key] = append(byKey[key], edgeInterval{
			temporalInterval: temporalInterval{start: start, end: end, hasEnd: hasEnd},
			edgeID:           edge.ID,
		})
	}

	for _, intervals := range byKey {
		sort.Slice(intervals, func(i, j int) bool {
			return intervals[i].start.Before(intervals[j].start)
		})
		for i := 1; i < len(intervals); i++ {
			prev := intervals[i-1]
			curr := intervals[i]
			if intervalsOverlap(prev.temporalInterval, curr.temporalInterval) {
				return &ConstraintViolationError{
					Type:       ConstraintTemporal,
					Label:      c.Label,
					Properties: c.Properties,
					Message: fmt.Sprintf("Cannot create TEMPORAL constraint: overlap between edges %s and %s",
						prev.edgeID, curr.edgeID),
				}
			}
		}
	}

	return nil
}

// isValueInAllowedList checks if a value matches any in the allowed values list.
func isValueInAllowedList(value interface{}, allowedValues []interface{}) bool {
	for _, allowed := range allowedValues {
		if compareValues(value, allowed) {
			return true
		}
	}
	return false
}

// validateDomainConstraintOnCreationForEngine validates that all existing nodes satisfy the domain constraint.
func validateDomainConstraintOnCreationForEngine(engine Engine, c Constraint) error {
	if len(c.Properties) != 1 {
		return fmt.Errorf("DOMAIN constraint requires exactly 1 property, got %d", len(c.Properties))
	}
	if len(c.AllowedValues) == 0 {
		return fmt.Errorf("DOMAIN constraint requires at least one allowed value")
	}

	property := c.Properties[0]

	nodes, err := engine.GetNodesByLabel(c.Label)
	if err != nil {
		return fmt.Errorf("scanning nodes: %w", err)
	}

	for _, node := range nodes {
		value := node.Properties[property]
		if value == nil {
			continue // NULL is valid for domain constraints
		}
		if !isValueInAllowedList(value, c.AllowedValues) {
			return &ConstraintViolationError{
				Type:       ConstraintDomain,
				Label:      c.Label,
				Properties: []string{property},
				Message: fmt.Sprintf("Cannot create DOMAIN constraint: node %s has %s=%v which is not in allowed values %v",
					node.ID, property, value, c.AllowedValues),
			}
		}
	}

	return nil
}

// validateRelDomainOnCreationForEngine validates that all existing edges satisfy the domain constraint.
func validateRelDomainOnCreationForEngine(edges []*Edge, c Constraint) error {
	if len(c.Properties) != 1 {
		return fmt.Errorf("DOMAIN constraint requires exactly 1 property, got %d", len(c.Properties))
	}
	if len(c.AllowedValues) == 0 {
		return fmt.Errorf("DOMAIN constraint requires at least one allowed value")
	}

	property := c.Properties[0]

	for _, edge := range edges {
		if edge.Type != c.Label {
			continue
		}
		value := edge.Properties[property]
		if value == nil {
			continue // NULL is valid for domain constraints
		}
		if !isValueInAllowedList(value, c.AllowedValues) {
			return &ConstraintViolationError{
				Type:       ConstraintDomain,
				Label:      c.Label,
				Properties: []string{property},
				Message: fmt.Sprintf("Cannot create DOMAIN constraint: edge %s has %s=%v which is not in allowed values %v",
					edge.ID, property, value, c.AllowedValues),
			}
		}
	}

	return nil
}

// validateCardinalityOnCreationForEngine checks that no node exceeds the max edge count.
func validateCardinalityOnCreationForEngine(edges []*Edge, c Constraint) error {
	counts := make(map[NodeID]int)
	for _, edge := range edges {
		if edge.Type != c.Label {
			continue
		}
		var anchor NodeID
		if c.Direction == "OUTGOING" {
			anchor = NodeID(edge.StartNode)
		} else {
			anchor = NodeID(edge.EndNode)
		}
		counts[anchor]++
	}
	for nodeID, count := range counts {
		if count > c.MaxCount {
			return &ConstraintViolationError{
				Type:  ConstraintCardinality,
				Label: c.Label,
				Message: fmt.Sprintf("Cannot create CARDINALITY constraint: node %s has %d %s %s edges, exceeding max count %d",
					nodeID, count, strings.ToLower(c.Direction), c.Label, c.MaxCount),
			}
		}
	}
	return nil
}

// validatePolicyOnCreationForEngine checks that all existing edges satisfy the policy.
func validatePolicyOnCreationForEngine(engine Engine, edges []*Edge, c Constraint) error {
	// For ALLOWED policies, gather the full set of ALLOWED policies for this relationship type
	// (existing ones from the schema plus the new one being created) and verify every edge
	// of this type is covered by at least one ALLOWED pair.
	var allowedSet []Constraint
	if c.PolicyMode == "ALLOWED" {
		schema := engine.GetSchema()
		if schema != nil {
			for _, existing := range schema.GetAllConstraints() {
				if existing.Type == ConstraintPolicy && existing.Label == c.Label && existing.PolicyMode == "ALLOWED" {
					allowedSet = append(allowedSet, existing)
				}
			}
		}
		// Add the new constraint being created (not yet in schema).
		allowedSet = append(allowedSet, c)
	}

	for _, edge := range edges {
		if edge.Type != c.Label {
			continue
		}
		srcNode, err := engine.GetNode(NodeID(edge.StartNode))
		if err != nil || srcNode == nil {
			continue
		}
		tgtNode, err := engine.GetNode(NodeID(edge.EndNode))
		if err != nil || tgtNode == nil {
			continue
		}

		if c.PolicyMode == "DISALLOWED" {
			srcHas := hasLabel(srcNode.Labels, c.SourceLabel)
			tgtHas := hasLabel(tgtNode.Labels, c.TargetLabel)
			if srcHas && tgtHas {
				return &ConstraintViolationError{
					Type:  ConstraintPolicy,
					Label: c.Label,
					Message: fmt.Sprintf("Cannot create DISALLOWED policy: edge %s connects %s (:%s) to %s (:%s) via :%s",
						edge.ID, edge.StartNode, c.SourceLabel, edge.EndNode, c.TargetLabel, c.Label),
				}
			}
		} else if c.PolicyMode == "ALLOWED" {
			// Check that this edge is covered by at least one ALLOWED pair in the full set.
			matched := false
			for _, ap := range allowedSet {
				if hasLabel(srcNode.Labels, ap.SourceLabel) && hasLabel(tgtNode.Labels, ap.TargetLabel) {
					matched = true
					break
				}
			}
			if !matched {
				return &ConstraintViolationError{
					Type:  ConstraintPolicy,
					Label: c.Label,
					Message: fmt.Sprintf("Cannot create ALLOWED policy: existing edge %s connects %s to %s via :%s, which is not covered by any ALLOWED pair",
						edge.ID, edge.StartNode, edge.EndNode, c.Label),
				}
			}
		}
	}
	return nil
}

// validateUniqueConstraintOnCreation checks all existing nodes for duplicates.
func validateUniqueConstraintOnCreationWithEngine(engine Engine, c Constraint) error {
	if len(c.Properties) != 1 {
		return fmt.Errorf("UNIQUE constraint requires exactly 1 property, got %d", len(c.Properties))
	}

	property := c.Properties[0]
	seen := make(map[interface{}]NodeID)

	// Scan all nodes with this label
	nodes, err := engine.GetNodesByLabel(c.Label)
	if err != nil {
		return fmt.Errorf("scanning nodes: %w", err)
	}

	for _, node := range nodes {
		value := node.Properties[property]
		if value == nil {
			continue // NULL values don't violate uniqueness
		}

		if existingNodeID, found := seen[value]; found {
			return &ConstraintViolationError{
				Type:       ConstraintUnique,
				Label:      c.Label,
				Properties: []string{property},
				Message: fmt.Sprintf("Cannot create UNIQUE constraint: nodes %s and %s both have %s=%v",
					existingNodeID, node.ID, property, value),
			}
		}

		seen[value] = node.ID
	}

	return nil
}

// validateNodeKeyConstraintOnCreation checks all existing nodes for duplicate composite keys.
func validateNodeKeyConstraintOnCreationWithEngine(engine Engine, c Constraint) error {
	if len(c.Properties) < 1 {
		return fmt.Errorf("NODE KEY constraint requires at least 1 property")
	}

	seen := make(map[string]NodeID) // composite key -> nodeID

	nodes, err := engine.GetNodesByLabel(c.Label)
	if err != nil {
		return fmt.Errorf("scanning nodes: %w", err)
	}

	for _, node := range nodes {
		// Extract all property values
		values := make([]interface{}, len(c.Properties))
		hasAllValues := true

		for i, prop := range c.Properties {
			val := node.Properties[prop]
			if val == nil {
				return &ConstraintViolationError{
					Type:       ConstraintNodeKey,
					Label:      c.Label,
					Properties: c.Properties,
					Message: fmt.Sprintf("Cannot create NODE KEY constraint: node %s has null value for property %s",
						node.ID, prop),
				}
			}
			values[i] = val
		}

		if !hasAllValues {
			continue
		}

		// Create composite key string
		compositeKey := fmt.Sprintf("%v", values)

		if existingNodeID, found := seen[compositeKey]; found {
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      c.Label,
				Properties: c.Properties,
				Message: fmt.Sprintf("Cannot create NODE KEY constraint: nodes %s and %s both have composite key %v=%v",
					existingNodeID, node.ID, c.Properties, values),
			}
		}

		seen[compositeKey] = node.ID
	}

	return nil
}

// validateExistenceConstraintOnCreation checks all existing nodes have the required property.
func validateExistenceConstraintOnCreationWithEngine(engine Engine, c Constraint) error {
	if len(c.Properties) != 1 {
		return fmt.Errorf("EXISTS constraint requires exactly 1 property, got %d", len(c.Properties))
	}

	property := c.Properties[0]

	nodes, err := engine.GetNodesByLabel(c.Label)
	if err != nil {
		return fmt.Errorf("scanning nodes: %w", err)
	}

	for _, node := range nodes {
		value := node.Properties[property]
		if value == nil {
			return &ConstraintViolationError{
				Type:       ConstraintExists,
				Label:      c.Label,
				Properties: []string{property},
				Message: fmt.Sprintf("Cannot create EXISTS constraint: node %s is missing required property %s",
					node.ID, property),
			}
		}
	}

	return nil
}

// validateTemporalConstraintOnCreationWithEngine enforces no-overlap for temporal intervals.
func validateTemporalConstraintOnCreationWithEngine(engine Engine, c Constraint) error {
	if len(c.Properties) != 3 {
		return fmt.Errorf("TEMPORAL constraint requires 3 properties (key, valid_from, valid_to)")
	}

	keyProp := c.Properties[0]
	startProp := c.Properties[1]
	endProp := c.Properties[2]

	nodes, err := engine.GetNodesByLabel(c.Label)
	if err != nil {
		return fmt.Errorf("scanning nodes: %w", err)
	}

	byKey := make(map[string][]temporalInterval)
	for _, node := range nodes {
		keyVal := node.Properties[keyProp]
		if keyVal == nil {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      c.Label,
				Properties: c.Properties,
				Message:    fmt.Sprintf("Cannot create TEMPORAL constraint: node %s has null %s", node.ID, keyProp),
			}
		}
		key := fmt.Sprint(keyVal)

		start, ok := coerceTemporalTime(node.Properties[startProp])
		if !ok {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      c.Label,
				Properties: c.Properties,
				Message:    fmt.Sprintf("Cannot create TEMPORAL constraint: node %s has invalid %s", node.ID, startProp),
			}
		}
		end, hasEnd := coerceTemporalTime(node.Properties[endProp])

		interval := temporalInterval{
			start:  start,
			end:    end,
			hasEnd: hasEnd,
			nodeID: node.ID,
		}
		byKey[key] = append(byKey[key], interval)
	}

	for _, intervals := range byKey {
		sort.Slice(intervals, func(i, j int) bool {
			return intervals[i].start.Before(intervals[j].start)
		})
		for i := 1; i < len(intervals); i++ {
			prev := intervals[i-1]
			curr := intervals[i]
			if intervalsOverlap(prev, curr) {
				return &ConstraintViolationError{
					Type:       ConstraintTemporal,
					Label:      c.Label,
					Properties: c.Properties,
					Message: fmt.Sprintf("Cannot create TEMPORAL constraint: overlap between %s and %s",
						prev.nodeID, curr.nodeID),
				}
			}
		}
	}

	return nil
}

// RelationshipConstraint represents a constraint on relationship properties.
type RelationshipConstraint struct {
	Name       string
	Type       ConstraintType
	RelType    string // Relationship type (e.g., "KNOWS", "FOLLOWS")
	Properties []string
}

// ValidateRelationshipConstraint validates relationship property constraints.
func (b *BadgerEngine) ValidateRelationshipConstraint(rc RelationshipConstraint) error {
	switch rc.Type {
	case ConstraintUnique:
		return b.validateUniqueRelationshipConstraint(rc)
	case ConstraintExists:
		return b.validateExistenceRelationshipConstraint(rc)
	default:
		return fmt.Errorf("unsupported relationship constraint type: %s", rc.Type)
	}
}

// validateUniqueRelationshipConstraint checks relationship property uniqueness.
func (b *BadgerEngine) validateUniqueRelationshipConstraint(rc RelationshipConstraint) error {
	if len(rc.Properties) != 1 {
		return fmt.Errorf("UNIQUE constraint on relationships requires exactly 1 property")
	}

	property := rc.Properties[0]
	seen := make(map[interface{}]EdgeID)

	// Scan all relationships of this type
	edges, err := b.AllEdges()
	if err != nil {
		return fmt.Errorf("scanning edges: %w", err)
	}

	for _, edge := range edges {
		if edge.Type != rc.RelType {
			continue
		}

		value := edge.Properties[property]
		if value == nil {
			continue
		}

		if existingEdgeID, found := seen[value]; found {
			return &ConstraintViolationError{
				Type:       ConstraintUnique,
				Label:      rc.RelType,
				Properties: []string{property},
				Message: fmt.Sprintf("Cannot create UNIQUE constraint on relationship: edges %s and %s both have %s=%v",
					existingEdgeID, edge.ID, property, value),
			}
		}

		seen[value] = edge.ID
	}

	return nil
}

// validateExistenceRelationshipConstraint checks required relationship properties.
func (b *BadgerEngine) validateExistenceRelationshipConstraint(rc RelationshipConstraint) error {
	if len(rc.Properties) != 1 {
		return fmt.Errorf("EXISTS constraint on relationships requires exactly 1 property")
	}

	property := rc.Properties[0]

	edges, err := b.AllEdges()
	if err != nil {
		return fmt.Errorf("scanning edges: %w", err)
	}

	for _, edge := range edges {
		if edge.Type != rc.RelType {
			continue
		}

		value := edge.Properties[property]
		if value == nil {
			return &ConstraintViolationError{
				Type:       ConstraintExists,
				Label:      rc.RelType,
				Properties: []string{property},
				Message: fmt.Sprintf("Cannot create EXISTS constraint on relationship: edge %s is missing required property %s",
					edge.ID, property),
			}
		}
	}

	return nil
}

// PropertyTypeConstraint represents a type constraint on properties.
type PropertyTypeConstraint struct {
	Name         string               `json:"name"`
	EntityType   ConstraintEntityType `json:"entity_type,omitempty"` // defaults to NODE when empty
	Label        string               `json:"label"`                 // label for nodes, relationship type for relationships
	Property     string               `json:"property"`
	ExpectedType PropertyType         `json:"expected_type"`
}

// EffectiveEntityType returns the entity type, defaulting to NODE for backward compatibility.
func (c PropertyTypeConstraint) EffectiveEntityType() ConstraintEntityType {
	if c.EntityType == "" {
		return ConstraintEntityNode
	}
	return c.EntityType
}

// PropertyType represents the expected type of a property.
type PropertyType string

const (
	PropertyTypeString   PropertyType = "STRING"
	PropertyTypeInteger  PropertyType = "INTEGER"
	PropertyTypeFloat    PropertyType = "FLOAT"
	PropertyTypeBoolean  PropertyType = "BOOLEAN"
	PropertyTypeDate     PropertyType = "DATE"
	PropertyTypeDateTime PropertyType = "DATETIME" // Legacy alias for zoned datetime
	// Neo4j temporal property type constraints.
	PropertyTypeZonedDateTime PropertyType = "ZONED DATETIME"
	PropertyTypeLocalDateTime PropertyType = "LOCAL DATETIME"
)

// ValidatePropertyType checks if a value matches the expected type.
// Handles JSON/MessagePack serialization quirks where integers become float64.
func ValidatePropertyType(value interface{}, expectedType PropertyType) error {
	if value == nil {
		return nil // NULL is valid for any type
	}

	switch expectedType {
	case PropertyTypeString:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected STRING, got %T", value)
		}
	case PropertyTypeInteger:
		switch v := value.(type) {
		case int, int32, int64:
			return nil
		case float64:
			// JSON/MessagePack deserializes integers as float64
			// Accept if it's a whole number
			if v == float64(int64(v)) {
				return nil
			}
			return fmt.Errorf("expected INTEGER, got %T", value)
		case float32:
			// Also check float32 for whole numbers
			if v == float32(int32(v)) {
				return nil
			}
			return fmt.Errorf("expected INTEGER, got %T", value)
		default:
			return fmt.Errorf("expected INTEGER, got %T", value)
		}
	case PropertyTypeFloat:
		switch value.(type) {
		case float32, float64:
			return nil
		default:
			return fmt.Errorf("expected FLOAT, got %T", value)
		}
	case PropertyTypeBoolean:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected BOOLEAN, got %T", value)
		}
	case PropertyTypeDate:
		switch v := value.(type) {
		case time.Time:
			return nil
		case string:
			if _, err := time.Parse("2006-01-02", strings.TrimSpace(v)); err == nil {
				return nil
			}
			return fmt.Errorf("expected DATE, got %T", value)
		default:
			return fmt.Errorf("expected DATE, got %T", value)
		}
	case PropertyTypeDateTime, PropertyTypeZonedDateTime:
		switch v := value.(type) {
		case time.Time:
			return nil
		case string:
			if isZonedDateTimeString(v) {
				return nil
			}
			return fmt.Errorf("expected ZONED DATETIME, got %T", value)
		default:
			return fmt.Errorf("expected ZONED DATETIME, got %T", value)
		}
	case PropertyTypeLocalDateTime:
		switch v := value.(type) {
		case string:
			if isLocalDateTimeString(v) {
				return nil
			}
			return fmt.Errorf("expected LOCAL DATETIME, got %T", value)
		default:
			return fmt.Errorf("expected LOCAL DATETIME, got %T", value)
		}
	default:
		return fmt.Errorf("unknown property type: %s", expectedType)
	}

	return nil
}

func isZonedDateTimeString(raw string) bool {
	s := strings.TrimSpace(strings.Trim(raw, "'\""))
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
	} {
		if _, err := time.Parse(layout, s); err == nil {
			return true
		}
	}
	return false
}

func isLocalDateTimeString(raw string) bool {
	s := strings.TrimSpace(strings.Trim(raw, "'\""))
	for _, layout := range []string{
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	} {
		if _, err := time.Parse(layout, s); err == nil {
			return true
		}
	}
	return false
}

// ValidatePropertyTypeConstraintOnCreation validates existing data against type constraint.
func (b *BadgerEngine) ValidatePropertyTypeConstraintOnCreation(ptc PropertyTypeConstraint) error {
	return ValidatePropertyTypeConstraintOnCreationForEngine(b, ptc)
}

// ValidatePropertyTypeConstraintOnCreationForEngine validates type constraints using Engine.
func ValidatePropertyTypeConstraintOnCreationForEngine(engine Engine, ptc PropertyTypeConstraint) error {
	if ptc.EffectiveEntityType() == ConstraintEntityRelationship {
		return validateRelPropertyTypeOnCreationForEngine(engine, ptc)
	}

	nodes, err := engine.GetNodesByLabel(ptc.Label)
	if err != nil {
		return fmt.Errorf("scanning nodes: %w", err)
	}

	for _, node := range nodes {
		value := node.Properties[ptc.Property]
		if err := ValidatePropertyType(value, ptc.ExpectedType); err != nil {
			return fmt.Errorf("node %s property %s: %w", node.ID, ptc.Property, err)
		}
	}

	return nil
}

// validateRelPropertyTypeOnCreationForEngine validates property type constraints on relationships.
func validateRelPropertyTypeOnCreationForEngine(engine Engine, ptc PropertyTypeConstraint) error {
	edges, err := engine.AllEdges()
	if err != nil {
		return fmt.Errorf("scanning edges: %w", err)
	}

	for _, edge := range edges {
		if edge.Type != ptc.Label {
			continue
		}
		value := edge.Properties[ptc.Property]
		if err := ValidatePropertyType(value, ptc.ExpectedType); err != nil {
			return fmt.Errorf("relationship %s property %s: %w", edge.ID, ptc.Property, err)
		}
	}

	return nil
}
