package storage

import (
	"math"
)

// Only integers that lose information through the legacy float64 index key
// need a distinct UNIQUE-cache key. The cache is in-memory and rebuilt; this
// does not alter any durable property-index encoding or ordinary small key.
type exactConstraintIntegerKey struct {
	negative  bool
	magnitude uint64
}

func uniqueConstraintValueKey(value interface{}) (interface{}, bool) {
	switch n := value.(type) {
	case int:
		return exactConstraintSignedKey(int64(n), value)
	case int64:
		return exactConstraintSignedKey(n, value)
	case uint:
		return exactConstraintUnsignedKey(uint64(n), value)
	case uint64:
		return exactConstraintUnsignedKey(n, value)
	}
	return indexValueKey(value)
}
func exactConstraintSignedKey(n int64, original interface{}) (interface{}, bool) {
	if !constraintIntegerFloatEqual(n, float64(n)) {
		if n < 0 {
			return exactConstraintIntegerKey{negative: true, magnitude: uint64(-(n + 1)) + 1}, true
		}
		return exactConstraintIntegerKey{magnitude: uint64(n)}, true
	}
	return indexValueKey(original)
}
func exactConstraintUnsignedKey(n uint64, original interface{}) (interface{}, bool) {
	if !constraintUnsignedFloatEqual(n, float64(n)) {
		return exactConstraintIntegerKey{magnitude: n}, true
	}
	return indexValueKey(original)
}

// The generic pending index intentionally retains its existing property-index
// keys. At the precision boundary, filter its candidates against their actual
// buffered values instead of trusting a rounded numeric key as identity.
func (v pendingWriteView) uniqueValueMatches(out []NodeID, label, property string, value, valueKey interface{}) []NodeID {
	if v.index == nil {
		return out
	}
	legacy := valueKey
	if _, exact := valueKey.(exactConstraintIntegerKey); exact {
		legacy, _ = indexValueKey(value)
	}
	numeric, isNumeric := numericConstraintValue(value)
	if !isNumeric || math.Abs(numeric) < 0x1p53 {
		return v.valueMatches(out, label, property, legacy)
	}
	reader, ok := v.owner.(interface{ newestNodeLocked(NodeID) (*Node, bool) })
	if !ok {
		return v.valueMatches(out, label, property, legacy)
	}
	for _, id := range v.valueMatchSet(label, property, legacy) {
		if !v.inNamespace(id) {
			continue
		}
		if node, exists := reader.newestNodeLocked(id); exists && constraintCreationValueEqual(node.Properties[property], value) {
			out = append(out, id)
		}
	}
	return out
}
