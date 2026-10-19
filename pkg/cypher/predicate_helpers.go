package cypher

import (
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"reflect"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func getNodePropertyValue(node *storage.Node, propName string) (any, bool) {
	if node == nil {
		return nil, false
	}
	if propName == "has_embedding" {
		return nodeHasEmbeddingValue(node), true
	}
	val, ok := node.Properties[propName]
	return val, ok
}

// nodeHasEmbeddingValue is n.has_embedding: the flag the managed embedding
// system keeps in EmbedMeta, else whether the node has chunk embeddings,
// counted from its metadata when a query read left the vectors in storage
// (storage.Node.HasChunkEmbeddings).
func nodeHasEmbeddingValue(node *storage.Node) any {
	if val, ok := node.EmbedMeta["has_embedding"]; ok {
		return val
	}
	return node.HasChunkEmbeddings()
}

func getBindingNodeValue(node *storage.Node, propName string) (any, bool) {
	if node == nil {
		return nil, false
	}
	if propName == "id" {
		if val, ok := node.Properties["id"]; ok {
			return val, true
		}
		return string(node.ID), true
	}
	return getNodePropertyValue(node, propName)
}

func buildComparableMembershipIndex(items []interface{}) (map[interface{}]struct{}, []interface{}) {
	return buildComparableMembershipIndexWithCancellation(items, nil)
}

func buildComparableMembershipIndexWithCancellation(items []interface{}, probe *pipelineCancellation) (map[interface{}]struct{}, []interface{}) {
	comparableSet := make(map[interface{}]struct{}, len(items))
	nonComparable := make([]interface{}, 0)
	for _, item := range items {
		if probe != nil && !probe.check() {
			return nil, nil
		}
		if item == nil {
			continue
		}
		if isComparableValue(item) {
			// A string's native hash scans all of it: its length is charged
			// to the probe (chargeText).
			if text, ok := item.(string); ok && probe != nil && !probe.chargeText(len(text)) {
				return nil, nil
			}
			comparableSet[membershipKey(item)] = struct{}{}
			continue
		}
		nonComparable = append(nonComparable, item)
	}
	return comparableSet, nonComparable
}

func evaluateComparableMembership(actual interface{}, comparableSet map[interface{}]struct{}, nonComparable []interface{}, equals func(interface{}, interface{}) bool) bool {
	return indexedMembershipTruth(actual, comparableSet, nonComparable, false, func(left, right interface{}) interface{} {
		return equals(left, right)
	}) == truthTrue
}

// membershipKey is the key of a comparable value in a membership index
// (buildComparableMembershipIndex). Cypher compares numbers by value (1 = 1.0,
// an integer parameter that arrives as a float over HTTP equals the stored
// integer), so every integer type, and every whole-valued float in int64
// range, keys as int64; other floats key as float64. Integers keep their
// exact value, including above 2^53.
func membershipKey(v interface{}) interface{} {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int16:
		return int64(n)
	case int8:
		return int64(n)
	case uint32:
		return int64(n)
	case uint16:
		return int64(n)
	case uint8:
		return int64(n)
	case uint:
		if uint64(n) <= math.MaxInt64 {
			return int64(n)
		}
	case uint64:
		if n <= math.MaxInt64 {
			return int64(n)
		}
	case float64:
		return floatMembershipKey(n)
	case float32:
		return floatMembershipKey(float64(n))
	}
	return v
}

func floatMembershipKey(f float64) interface{} {
	if f == math.Trunc(f) && f >= math.MinInt64 && f < math.MaxInt64 {
		return int64(f)
	}
	return f
}

func isComparableValue(v interface{}) bool {
	if v == nil {
		return false
	}
	// Graph pointers are Go-comparable, but their addresses are not Cypher
	// identities. Leave them for the equality owner's non-indexed comparisons.
	switch v.(type) {
	case string, bool:
		return true
	case *storage.Node, *storage.Edge:
		return false
	}
	return reflect.TypeOf(v).Comparable()
}
