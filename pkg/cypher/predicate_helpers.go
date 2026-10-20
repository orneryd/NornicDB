package cypher

import (
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
	return indexedMembershipTruth(actual, comparableSet, nonComparable, false, func(left, right interface{}) interface{} {
		return equals(left, right)
	}) == truthTrue
}

// membershipKey is the key of a comparable value in a membership index
// (buildComparableMembershipIndex). The storage owner supplies numeric identity,
// including mixed signed/unsigned/float values and integers above 2^53.
func membershipKey(v interface{}) interface{} {
	if key, numeric := storage.NumericValueKey(v); numeric {
		return key
	}
	return v
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
