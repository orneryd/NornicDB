// Package storage - Constraint validation when constraints are created.
package storage

import (
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

func newLocalizedConstraintViolation(constraintType ConstraintType, label string, properties []string, message localization.Message, cause error) *ConstraintViolationError {
	return &ConstraintViolationError{
		Type:       constraintType,
		Label:      label,
		Properties: properties,
		Message:    message.Fallback,
		Cause:      localizedError(message, cause),
	}
}

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
		return localizedError(localization.StorageValidationUnknownConstraintType(string(c.Type)), nil)
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
		uc.valuesCacheComplete = false
		uc.mu.Unlock()
	}

	// Only the labels that carry a UNIQUE constraint are read, each through
	// the label-scoped stream with just the constrained properties decoded:
	// a refresh in one database never scans the whole server (AllNodes).
	propertiesByLabel := make(map[string][]string)
	labels := make([]string, 0)
	for _, uc := range uniqueConstraints {
		if _, ok := propertiesByLabel[uc.Label]; !ok {
			labels = append(labels, uc.Label)
		}
		if !slices.Contains(propertiesByLabel[uc.Label], uc.Property) {
			propertiesByLabel[uc.Label] = append(propertiesByLabel[uc.Label], uc.Property)
		}
	}
	sort.Strings(labels)
	for _, label := range labels {
		properties := propertiesByLabel[label]
		var violation error
		err := streamNodesByLabelForValidation(engine, label, properties, func(node *Node) error {
			if node == nil {
				return nil
			}
			// Pinned by docs/plans/consumer-pinned-error-contract-plan.md §2.3 and
			// TestRefreshUniqueConstraint_KeepsPrefixedIDs_UnderNamespacedEngine. Removing
			// EnsureNodeIDDatabasePrefixForEngine re-introduces a known false-UNIQUE failure
			// mode for namespaced engines (the cache holds unprefixed IDs while transaction
			// commit validation passes prefixed IDs).
			storageNodeID := EnsureNodeIDDatabasePrefixForEngine(engine, node.ID)
			for _, propName := range properties {
				propValue, ok := node.Properties[propName]
				if !ok {
					continue
				}
				if err := schema.CheckUniqueConstraint(label, propName, propValue, storageNodeID); err != nil {
					violation = localizedError(localization.StorageValidationRefreshUniqueFailed(err), err)
					return violation
				}
				schema.RegisterUniqueValue(label, propName, propValue, storageNodeID)
			}
			return nil
		})
		if violation != nil {
			return violation
		}
		if err != nil {
			return localizedError(localization.StorageValidationRefreshUniqueScanFailed(err), err)
		}
	}

	for _, uc := range uniqueConstraints {
		uc.mu.Lock()
		uc.valuesCacheComplete = true
		uc.mu.Unlock()
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
		return localizedError(localization.StorageValidationDomainPropertyCount(len(c.Properties)), nil)
	}
	if len(c.AllowedValues) == 0 {
		return localizedError(localization.StorageValidationDomainAllowedValuesRequired(), nil)
	}

	property := c.Properties[0]

	nodes, err := engine.GetNodesByLabel(c.Label)
	if err != nil {
		return localizedError(localization.StorageValidationScanNodesFailed(err), err)
	}

	for _, node := range nodes {
		value := node.Properties[property]
		if value == nil {
			continue // NULL is valid for domain constraints
		}
		if !isValueInAllowedList(value, c.AllowedValues) {
			message := localization.StorageValidationDomainNodeInvalid(string(node.ID), property, value, c.AllowedValues)
			return newLocalizedConstraintViolation(ConstraintDomain, c.Label, []string{property}, message, nil)
		}
	}

	return nil
}

// validateUniqueConstraintOnCreation checks all existing nodes for duplicates.
func validateUniqueConstraintOnCreationWithEngine(engine Engine, c Constraint) error {
	if len(c.Properties) > 1 {
		return validateNodeKeyConstraintOnCreationWithEngine(engine, c)
	}
	if len(c.Properties) != 1 {
		return localizedError(localization.StorageValidationUniquePropertyCount(len(c.Properties)), nil)
	}

	property := c.Properties[0]
	seen := make(map[interface{}]NodeID)

	// Scan all nodes with this label
	nodes, err := engine.GetNodesByLabel(c.Label)
	if err != nil {
		return localizedError(localization.StorageValidationScanNodesFailed(err), err)
	}

	for _, node := range nodes {
		value := node.Properties[property]
		if value == nil {
			continue // NULL values don't violate uniqueness
		}

		if existingNodeID, found := seen[value]; found {
			message := localization.StorageValidationNodeUniqueDuplicate(string(existingNodeID), string(node.ID), property, value)
			return newLocalizedConstraintViolation(ConstraintUnique, c.Label, []string{property}, message, nil)
		}

		seen[value] = node.ID
	}

	return nil
}

// validateNodeKeyConstraintOnCreation checks all existing nodes for duplicate composite keys.
func validateNodeKeyConstraintOnCreationWithEngine(engine Engine, c Constraint) error {
	if len(c.Properties) < 1 {
		return localizedError(localization.StorageValidationNodeKeyPropertyRequired(), nil)
	}

	seen := make(map[string][]*Node)

	nodes, err := engine.GetNodesByLabel(c.Label)
	if err != nil {
		return localizedError(localization.StorageValidationScanNodesFailed(err), err)
	}

	for _, node := range nodes {
		// Extract all property values
		values := make([]interface{}, len(c.Properties))
		keyValues := make([]interface{}, len(c.Properties))
		hasAllValues := true

		for i, prop := range c.Properties {
			val := node.Properties[prop]
			if val == nil {
				if c.Type == ConstraintUnique {
					hasAllValues = false
					break
				}
				message := localization.StorageValidationNodeKeyNullCreation(string(node.ID), prop)
				return newLocalizedConstraintViolation(ConstraintNodeKey, c.Label, c.Properties, message, nil)
			}
			values[i] = val
			keyValues[i] = val
			if numeric, ok := numericConstraintValue(val); ok {
				keyValues[i] = numeric
			}
		}

		if !hasAllValues {
			continue
		}

		key := fmt.Sprintf("%#v", keyValues)
		for _, existing := range seen[key] {
			match := true
			for index, property := range c.Properties {
				if !compareValues(existing.Properties[property], values[index]) {
					match = false
					break
				}
			}
			if match {
				message := localization.StorageValidationNodeKeyDuplicateCreation(string(existing.ID), string(node.ID), c.Properties, values)
				if c.Type == ConstraintUnique {
					message = localization.StorageValidationNodeCompositeKeyExisting(c.Properties, values, string(existing.ID))
				}
				return newLocalizedConstraintViolation(c.Type, c.Label, c.Properties, message, nil)
			}
		}
		seen[key] = append(seen[key], node)
	}

	return nil
}

// validateExistenceConstraintOnCreation checks all existing nodes have the required property.
func validateExistenceConstraintOnCreationWithEngine(engine Engine, c Constraint) error {
	if len(c.Properties) != 1 {
		return localizedError(localization.StorageValidationExistsPropertyCount(len(c.Properties)), nil)
	}

	property := c.Properties[0]

	nodes, err := engine.GetNodesByLabel(c.Label)
	if err != nil {
		return localizedError(localization.StorageValidationScanNodesFailed(err), err)
	}

	for _, node := range nodes {
		value := node.Properties[property]
		if value == nil {
			message := localization.StorageValidationNodeExistsMissingCreation(string(node.ID), property)
			return newLocalizedConstraintViolation(ConstraintExists, c.Label, []string{property}, message, nil)
		}
	}

	return nil
}

// validateTemporalConstraintOnCreationWithEngine enforces no-overlap for temporal intervals.
// Every property before the trailing (valid_from, valid_to) pair forms the
// grouping key (see temporalKeySpec).
func validateTemporalConstraintOnCreationWithEngine(engine Engine, c Constraint) error {
	spec, ok := splitTemporalKeySpec(c.Properties)
	if !ok {
		return localizedError(localization.StorageValidationTemporalPropertiesAtLeastThree(), nil)
	}

	nodes, err := engine.GetNodesByLabel(c.Label)
	if err != nil {
		return localizedError(localization.StorageValidationScanNodesFailed(err), err)
	}

	byKey := make(map[string][]temporalInterval)
	for _, node := range nodes {
		keyVal, missing := spec.keyValue(node.Properties)
		if missing != "" {
			message := localization.StorageValidationTemporalNodeKeyNullCreation(string(node.ID), missing)
			return newLocalizedConstraintViolation(ConstraintTemporal, c.Label, c.Properties, message, nil)
		}
		key := spec.groupString(keyVal)

		interval, ok := spec.interval(node.Properties)
		if !ok {
			message := localization.StorageValidationTemporalNodeInvalidCreation(string(node.ID), spec.startProp)
			return newLocalizedConstraintViolation(ConstraintTemporal, c.Label, c.Properties, message, nil)
		}
		interval.nodeID = node.ID
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
				message := localization.StorageValidationTemporalNodesOverlapCreation(string(prev.nodeID), string(curr.nodeID))
				return newLocalizedConstraintViolation(ConstraintTemporal, c.Label, c.Properties, message, nil)
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
		return localizedError(localization.StorageValidationRelationshipConstraintTypeUnsupported(string(rc.Type)), nil)
	}
}

// validateUniqueRelationshipConstraint checks relationship property uniqueness
// over the edges of rc.RelType only (streamed from the edge type index).
func (b *BadgerEngine) validateUniqueRelationshipConstraint(rc RelationshipConstraint) error {
	if len(rc.Properties) != 1 {
		return localizedError(localization.StorageValidationRelationshipUniquePropertyCount(), nil)
	}
	return runRelEdgeCheck(b, rc.RelType, newRelUniquenessCheck(Constraint{
		Type:       ConstraintUnique,
		Label:      rc.RelType,
		Properties: rc.Properties,
	}))
}

// validateExistenceRelationshipConstraint checks required relationship
// properties over the edges of rc.RelType only.
func (b *BadgerEngine) validateExistenceRelationshipConstraint(rc RelationshipConstraint) error {
	if len(rc.Properties) != 1 {
		return localizedError(localization.StorageValidationRelationshipExistsPropertyCount(), nil)
	}
	property := rc.Properties[0]
	return runRelEdgeCheck(b, rc.RelType, relEdgeCheck{visit: ofType(rc.RelType, func(edge *Edge) error {
		if edge.Properties[property] == nil {
			message := localization.StorageValidationRelationshipExistsMissing(string(edge.ID), property)
			return newLocalizedConstraintViolation(ConstraintExists, rc.RelType, []string{property}, message, nil)
		}
		return nil
	})})
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
			return localizedError(localization.StorageValidationExpectedType("STRING", fmt.Sprintf("%T", value)), nil)
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
			return localizedError(localization.StorageValidationExpectedType("INTEGER", fmt.Sprintf("%T", value)), nil)
		case float32:
			// Also check float32 for whole numbers
			if v == float32(int32(v)) {
				return nil
			}
			return localizedError(localization.StorageValidationExpectedType("INTEGER", fmt.Sprintf("%T", value)), nil)
		default:
			return localizedError(localization.StorageValidationExpectedType("INTEGER", fmt.Sprintf("%T", value)), nil)
		}
	case PropertyTypeFloat:
		switch value.(type) {
		case float32, float64:
			return nil
		default:
			return localizedError(localization.StorageValidationExpectedType("FLOAT", fmt.Sprintf("%T", value)), nil)
		}
	case PropertyTypeBoolean:
		if _, ok := value.(bool); !ok {
			return localizedError(localization.StorageValidationExpectedType("BOOLEAN", fmt.Sprintf("%T", value)), nil)
		}
	case PropertyTypeDate:
		if propertyValueKind(value) == "date" {
			return nil
		}
		switch v := value.(type) {
		case time.Time:
			return nil
		case string:
			if _, err := time.Parse("2006-01-02", strings.TrimSpace(v)); err == nil {
				return nil
			}
			return localizedError(localization.StorageValidationExpectedType("DATE", fmt.Sprintf("%T", value)), nil)
		default:
			return localizedError(localization.StorageValidationExpectedType("DATE", fmt.Sprintf("%T", value)), nil)
		}
	case PropertyTypeDateTime, PropertyTypeZonedDateTime:
		if propertyValueKind(value) == "zoned-date-time" {
			return nil
		}
		switch v := value.(type) {
		case time.Time:
			return nil
		case string:
			if isZonedDateTimeString(v) {
				return nil
			}
			return localizedError(localization.StorageValidationExpectedType("ZONED DATETIME", fmt.Sprintf("%T", value)), nil)
		default:
			return localizedError(localization.StorageValidationExpectedType("ZONED DATETIME", fmt.Sprintf("%T", value)), nil)
		}
	case PropertyTypeLocalDateTime:
		if propertyValueKind(value) == "local-date-time" {
			return nil
		}
		switch v := value.(type) {
		case string:
			if isLocalDateTimeString(v) {
				return nil
			}
			return localizedError(localization.StorageValidationExpectedType("LOCAL DATETIME", fmt.Sprintf("%T", value)), nil)
		default:
			return localizedError(localization.StorageValidationExpectedType("LOCAL DATETIME", fmt.Sprintf("%T", value)), nil)
		}
	default:
		if element, ok := ListElementPropertyType(expectedType); ok {
			return validateListPropertyType(value, expectedType, element)
		}
		return localizedError(localization.StorageValidationUnknownPropertyType(string(expectedType)), nil)
	}

	return nil
}

// ListPropertyType returns the canonical LIST<T NOT NULL> property type for a
// scalar element type.
func ListPropertyType(element PropertyType) PropertyType {
	return PropertyType("LIST<" + string(element) + " NOT NULL>")
}

// ListElementPropertyType reports the element type of a LIST<T NOT NULL>
// property type.
func ListElementPropertyType(pt PropertyType) (PropertyType, bool) {
	s := string(pt)
	if !strings.HasPrefix(s, "LIST<") || !strings.HasSuffix(s, " NOT NULL>") {
		return "", false
	}
	element := PropertyType(strings.TrimSuffix(strings.TrimPrefix(s, "LIST<"), " NOT NULL>"))
	if element == "" {
		return "", false
	}
	return element, true
}

func validateListPropertyType(value interface{}, listType, element PropertyType) error {
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return localizedError(localization.StorageValidationExpectedType(string(listType), fmt.Sprintf("%T", value)), nil)
	}
	for i := 0; i < rv.Len(); i++ {
		item := rv.Index(i).Interface()
		if item == nil {
			return localizedError(localization.StorageValidationExpectedType(string(listType), "list containing null"), nil)
		}
		if err := ValidatePropertyType(item, element); err != nil {
			return localizedError(localization.StorageValidationExpectedType(string(listType), fmt.Sprintf("list containing %T", item)), nil)
		}
	}
	return nil
}

func propertyValueKind(value interface{}) string {
	typed, ok := value.(TypedPropertyValue)
	if !ok {
		return ""
	}
	return typed.PropertyValueKind()
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
		return localizedError(localization.StorageValidationScanNodesFailed(err), err)
	}

	for _, node := range nodes {
		value := node.Properties[ptc.Property]
		if err := ValidatePropertyType(value, ptc.ExpectedType); err != nil {
			return localizedError(localization.StorageValidationNodePropertyInvalid(string(node.ID), ptc.Property, err), err)
		}
	}

	return nil
}

// validateRelPropertyTypeOnCreationForEngine validates property type
// constraints on relationships, streaming only the edges of ptc.Label.
func validateRelPropertyTypeOnCreationForEngine(engine Engine, ptc PropertyTypeConstraint) error {
	return runRelEdgeCheck(engine, ptc.Label, newRelPropertyTypeCheck(ptc))
}
