package storage

import "fmt"

// IndexKind identifies the durable kind of a schema property index.
type IndexKind string

const (
	// IndexKindRange is the default property-index kind, including legacy definitions.
	IndexKindRange IndexKind = "RANGE"
	// IndexKindText identifies a single-property text index definition.
	IndexKindText IndexKind = "TEXT"
	// IndexKindPoint identifies a single-property spatial index definition.
	IndexKindPoint IndexKind = "POINT"
)

func (idx *RangeIndex) effectiveKind() IndexKind {
	if idx.Kind == "" {
		return IndexKindRange
	}
	return idx.Kind
}

// AddTypedIndexForEntity admits a durable TEXT or POINT schema definition.
// Query execution retains its existing scan fallback; this method does not
// introduce a dedicated text-search or spatial acceleration engine.
//
// Example:
//
//	schema.AddTypedIndexForEntity(IndexKindText, "person_name", "Person", []string{"name"}, ConstraintEntityNode)
func (sm *SchemaManager) AddTypedIndexForEntity(kind IndexKind, name, label string, properties []string, entityType ConstraintEntityType) error {
	if kind != IndexKindText && kind != IndexKindPoint {
		return fmt.Errorf("unsupported typed index kind %q", kind)
	}
	if len(properties) != 1 {
		return fmt.Errorf("%s indexes require exactly one property", kind)
	}
	return sm.addIndexForEntity(kind, name, label, properties, entityType)
}
