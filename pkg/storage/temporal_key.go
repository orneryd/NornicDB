package storage

import (
	"fmt"
	"strings"
)

// temporalKeyPropSeparator joins the names of a composite temporal grouping
// key inside temporal index descriptors. Unit separator (0x1f) is not a
// character any parsed identifier carries, so a joined name never collides
// with a single-property key name.
const temporalKeyPropSeparator = "\x1f"

// temporalKeySpec splits the properties of a TEMPORAL NO OVERLAP constraint
// into the grouping key and the validity window. The last two properties are
// always (valid_from, valid_to); every property before them forms the
// grouping key, so a constraint has one or more key properties.
//
// Node and relationship constraints share this layout:
//
//	(n.fact_key, n.valid_from, n.valid_to)          // single key
//	(n.tenant, n.fact_key, n.valid_from, n.valid_to) // composite key
//
// Single-key constraints keep their historical key representation (the raw
// property value) so persisted temporal indexes stay byte-identical; composite
// keys use a []interface{} of the key values in property order.
type temporalKeySpec struct {
	keyProps  []string
	startProp string
	endProp   string
}

// splitTemporalKeySpec returns the key/window split of temporal constraint
// properties, or false when fewer than three properties are given.
func splitTemporalKeySpec(properties []string) (temporalKeySpec, bool) {
	n := len(properties)
	if n < 3 {
		return temporalKeySpec{}, false
	}
	return temporalKeySpec{
		keyProps:  properties[:n-2],
		startProp: properties[n-2],
		endProp:   properties[n-1],
	}, true
}

func (s temporalKeySpec) composite() bool { return len(s.keyProps) > 1 }

// keyValue extracts the grouping key from props. It returns the name of the
// first null/absent key property when the key is incomplete (a violation:
// TEMPORAL key properties cannot be null).
func (s temporalKeySpec) keyValue(props map[string]interface{}) (interface{}, string) {
	if !s.composite() {
		value := props[s.keyProps[0]]
		if value == nil {
			return nil, s.keyProps[0]
		}
		return value, ""
	}
	values := make([]interface{}, len(s.keyProps))
	for i, prop := range s.keyProps {
		values[i] = props[prop]
		if values[i] == nil {
			return nil, prop
		}
	}
	return values, ""
}

// keyEqual reports whether two key values produced by keyValue are equal,
// using constraint value semantics (numeric cross-type equality).
func (s temporalKeySpec) keyEqual(a, b interface{}) bool {
	if a == nil || b == nil {
		return false
	}
	if !s.composite() {
		return compareValues(a, b)
	}
	av, aok := a.([]interface{})
	bv, bok := b.([]interface{})
	if !aok || !bok || len(av) != len(bv) || len(av) != len(s.keyProps) {
		return false
	}
	for i := range av {
		if av[i] == nil || bv[i] == nil || !compareValues(av[i], bv[i]) {
			return false
		}
	}
	return true
}

// keyMatches reports whether props carry the given key value.
func (s temporalKeySpec) keyMatches(props map[string]interface{}, keyValue interface{}) bool {
	other, missing := s.keyValue(props)
	return missing == "" && s.keyEqual(other, keyValue)
}

// keyLabel names the key for error messages ("fact_key" or "tenant, fact_key").
func (s temporalKeySpec) keyLabel() string {
	return strings.Join(s.keyProps, ", ")
}

// indexKeyProp is the key property name stored in temporal index keys.
func (s temporalKeySpec) indexKeyProp() string {
	return strings.Join(s.keyProps, temporalKeyPropSeparator)
}

// keyHash hashes a key value for temporal index keys. Single keys hash the raw
// value exactly as before composite keys existed.
func (s temporalKeySpec) keyHash(keyValue interface{}) string {
	if values, ok := keyValue.([]interface{}); ok && s.composite() {
		return constraintCompositeKey(values)
	}
	return constraintValueKey(keyValue)
}

// groupString renders a key value as a map key for creation-time validation.
func (s temporalKeySpec) groupString(keyValue interface{}) string {
	values, ok := keyValue.([]interface{})
	if !ok || !s.composite() {
		return fmt.Sprint(keyValue)
	}
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprint(v)
	}
	return strings.Join(parts, "\x00")
}

// interval reads the validity window from props; ok is false when the start
// property is missing or not a temporal value.
func (s temporalKeySpec) interval(props map[string]interface{}) (temporalInterval, bool) {
	start, ok := coerceTemporalTime(props[s.startProp])
	if !ok {
		return temporalInterval{}, false
	}
	end, hasEnd := coerceTemporalTime(props[s.endProp])
	return temporalInterval{start: start, end: end, hasEnd: hasEnd}, true
}
