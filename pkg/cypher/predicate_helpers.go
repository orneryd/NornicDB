package cypher

import (
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
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
	comparableSet := make(map[interface{}]struct{}, len(items))
	nonComparable := make([]interface{}, 0)
	for _, item := range items {
		if item == nil {
			continue
		}
		if isComparableValue(item) {
			comparableSet[membershipKey(item)] = struct{}{}
			continue
		}
		nonComparable = append(nonComparable, item)
	}
	return comparableSet, nonComparable
}

func evaluateComparableMembership(actual interface{}, comparableSet map[interface{}]struct{}, nonComparable []interface{}, equals func(interface{}, interface{}) bool) bool {
	if actual == nil {
		return false
	}
	if isComparableValue(actual) {
		if _, hit := comparableSet[membershipKey(actual)]; hit {
			return true
		}
	}
	for _, item := range nonComparable {
		if equals(actual, item) {
			return true
		}
	}
	return false
}

// membershipKey is the key of a comparable value in a membership index
// (buildComparableMembershipIndex). Cypher compares numbers by value (1 = 1.0,
// an integer parameter that arrives as a float over HTTP equals the stored
// integer), so every integer type, and every whole-valued float in int64
// range, keys as int64; other floats key as float64. Integers keep their
// exact value, including above 2^53.
func membershipKey(v interface{}) interface{} {
	if integer, ok := valueconvert.IntegerValue(v); ok {
		return integer
	}
	if unsigned, ok := valueconvert.UnsignedInteger(v); ok {
		return unsigned
	}
	if floating, ok := valueconvert.FloatValue(v); ok {
		return floatMembershipKey(floating)
	}
	return v
}

func floatMembershipKey(f float64) interface{} {
	if integer, ok := valueconvert.Int64(f, valueconvert.IntegerFloat|valueconvert.IntegerWholeFloat); ok {
		return integer
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
	case *storage.Node, *storage.Edge:
		return false
	}
	return reflect.TypeOf(v).Comparable()
}
