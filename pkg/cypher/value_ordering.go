package cypher

import (
	"math"
	"slices"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Ordering of maps, nodes, relationships and paths, shared by the comparison
// operators (< <= > >=) and ORDER BY so both order these values the same way,
// as Neo4j 5.26 does (#907):
//
//   - a map before another with more keys; between maps of as many keys, by
//     their sorted key names compared in order; then by their values in key
//     order;
//   - nodes, and relationships, by id;
//   - paths element by element: start node, first relationship, next node, …
//
// The comparison operators compare the values inside with comparability
// (two values that can't be ordered make the maps unordered: {a: 1} <
// {a: 'x'} is null); ORDER BY with orderability, which orders every value.
// Neo4j orders nodes and relationships by its internal ids, which follow
// creation order; NornicDB's ids don't, so the order between two different
// nodes is NornicDB's own, as it is for ORDER BY.

// cypherPathElements returns a path's nodes and relationships alternating,
// start node first.
func cypherPathElements(value interface{}) ([]interface{}, bool) {
	object, isMap := toStringAnyMap(value)
	return cypherPathElementsOf(value, object, isMap)
}

// cypherPathElementsOf is cypherPathElements for a value already converted
// with toStringAnyMap, so a map is converted only once.
func cypherPathElementsOf(value interface{}, object map[string]interface{}, isMap bool) ([]interface{}, bool) {
	if remote, ok := value.(storage.RemotePathValue); ok {
		if _, _, valid := remote.Parts(); !valid {
			return nil, false
		}
		elements := make([]interface{}, len(remote))
		for i, entity := range remote {
			elements[i] = entity
		}
		return elements, true
	}

	path, ok := cypherPathResultOf(value, object, isMap)
	if !ok {
		return nil, false
	}
	elements := make([]interface{}, 0, len(path.Nodes)+len(path.Relationships))
	for index, node := range path.Nodes {
		elements = append(elements, node)
		if index < len(path.Relationships) {
			elements = append(elements, path.Relationships[index])
		}
	}
	return elements, true
}

// cypherPathResultOf recognizes an actual path carrier, rather than a user map
// with coincidentally named keys such as length or _pathResult.
func cypherPathResultOf(value interface{}, object map[string]interface{}, isMap bool) (PathResult, bool) {
	var path PathResult
	switch typed := value.(type) {
	case PathResult:
		path = typed
	case *PathResult:
		if typed == nil {
			return PathResult{}, false
		}
		path = *typed
	default:
		if !isMap {
			return PathResult{}, false
		}
		switch inner := object["_pathResult"].(type) {
		case PathResult:
			path = inner
		case *PathResult:
			if inner == nil {
				return PathResult{}, false
			}
			path = *inner
		default:
			return PathResult{}, false
		}
	}
	return path, true
}

// compareCypherStructuredValues orders two values when either is a map, node,
// relationship or path. handled is false when neither is; comparable is false
// for values of different kinds and for maps holding values compareValue
// can't order.
func compareCypherStructuredValues(left, right interface{}, compareValue func(a, b interface{}) (int, bool)) (order int, comparable bool, handled bool) {
	switch leftValue := left.(type) {
	case storage.RemoteEntityValue:
		rightValue, ok := right.(storage.RemoteEntityValue)
		if !ok || cypherValueKindOf(leftValue) != cypherValueKindOf(rightValue) {
			return 0, false, true
		}
		leftElement, _ := leftValue["elementId"].(string)
		rightElement, _ := rightValue["elementId"].(string)
		if leftElement == rightElement {
			return 0, true, true
		}
		leftID, _, _ := leftValue.LegacyIDs()
		rightID, _, _ := rightValue.LegacyIDs()
		if leftID >= 0 && rightID >= 0 {
			if leftID < rightID {
				return -1, true, true
			}
			if leftID > rightID {
				return 1, true, true
			}
		}
		return compareStrings(leftElement, rightElement), true, true
	case *storage.Node:
		rightValue, ok := right.(*storage.Node)
		if !ok || leftValue == nil || rightValue == nil {
			return 0, false, true
		}
		return compareStrings(string(leftValue.ID), string(rightValue.ID)), true, true
	case *storage.Edge:
		rightValue, ok := right.(*storage.Edge)
		if !ok || leftValue == nil || rightValue == nil {
			return 0, false, true
		}
		return compareStrings(string(leftValue.ID), string(rightValue.ID)), true, true
	}
	switch right.(type) {
	case *storage.Node, *storage.Edge, storage.RemoteEntityValue:
		return 0, false, true
	}
	leftMap, leftIsMap := toStringAnyMap(left)
	rightMap, rightIsMap := toStringAnyMap(right)
	leftPath, leftIsPath := cypherPathElementsOf(left, leftMap, leftIsMap)
	rightPath, rightIsPath := cypherPathElementsOf(right, rightMap, rightIsMap)
	if leftIsPath || rightIsPath {
		if !leftIsPath || !rightIsPath {
			return 0, false, true
		}
		for index := 0; index < len(leftPath) && index < len(rightPath); index++ {
			if order, _, _ := compareCypherStructuredValues(leftPath[index], rightPath[index], compareValue); order != 0 {
				return order, true, true
			}
		}
		return compareOrderedInts(len(leftPath), len(rightPath)), true, true
	}
	if leftIsMap || rightIsMap {
		if !leftIsMap || !rightIsMap {
			return 0, false, true
		}
		order, comparable := compareCypherMaps(leftMap, rightMap, compareValue)
		return order, comparable, true
	}
	return 0, false, false
}

// compareCypherMaps orders two maps: fewer keys first, then the sorted key
// names compared in order, then the values in key order by compareValue.
func compareCypherMaps(left, right map[string]interface{}, compareValue func(a, b interface{}) (int, bool)) (int, bool) {
	if order := compareOrderedInts(len(left), len(right)); order != 0 {
		return order, true
	}
	// Small maps sort their keys on the stack.
	var leftBuffer, rightBuffer [8]string
	leftKeys, rightKeys := appendSortedMapKeys(leftBuffer[:0], left), appendSortedMapKeys(rightBuffer[:0], right)
	for index := range leftKeys {
		if order := compareStrings(leftKeys[index], rightKeys[index]); order != 0 {
			return order, true
		}
	}
	for _, key := range leftKeys {
		order, comparable := compareValue(left[key], right[key])
		if !comparable {
			return 0, false
		}
		if order != 0 {
			return order, true
		}
	}
	return 0, true
}

func appendSortedMapKeys(keys []string, object map[string]interface{}) []string {
	for key := range object {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func compareStrings(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	}
	return 0
}

// compareCypherComparableValues compares two values inside a map or list for
// the comparison operators: equal values are 0, including values with no
// order (two equal durations); values that are neither ordered nor equal
// (null, or of different kinds) are not comparable.
func compareCypherComparableValues(left, right interface{}) (int, bool) {
	if order, comparable := compareCypherOrderedValues(left, right); comparable {
		return order, true
	}
	if equal, known := cypherEquality(left, right).(bool); known && equal {
		return 0, true
	}
	return 0, false
}

// compareSortableValues is compareValuesForSort as a comparison that always
// orders, for compareCypherStructuredValues under ORDER BY.
func compareSortableValues(left, right interface{}) (int, bool) {
	return compareValuesForSort(left, right), true
}

// cypherSortRank implements the openCypher comparability order used by ORDER
// BY: maps, nodes, relationships, lists, paths, strings, booleans, numbers,
// NaN, and null. Null is handled before this function.
// cypherSortRank is a value's position in Neo4j's orderability across types:
// map < node < relationship < list < path < point < zoned datetime < local
// datetime < date < zoned time < local time < duration < string < boolean <
// number < NaN (#817, #837).

func cypherSortRank(value interface{}) int {
	if _, ok := pointValue(value); ok {
		return 6
	}
	if kind, _, ok := temporalOrderParts(value); ok {
		return temporalSortRanks[kind]
	}
	switch cypherValueKindOf(value) {
	case valueKindMap:
		return 0
	case valueKindNode:
		return 1
	case valueKindRelationship:
		return 2
	case valueKindList:
		return 3
	case valueKindPath:
		return 4
	case valueKindVector:
		return 5
	case valueKindDuration:
		return 12
	case valueKindString:
		return 13
	case valueKindBoolean:
		return 14
	case valueKindUUID:
		return 15
	}
	if number, ok := strictNumericValue(value); ok {
		if math.IsNaN(number) {
			return 17
		}
		return 16
	}
	return 20
}

// temporalSortRanks are the temporal kinds' positions in cypherSortRank.
var temporalSortRanks = map[string]int{"datetime": 7, "localdatetime": 8, "date": 9, "time": 10, "localtime": 11}

func cypherSortList(value interface{}) ([]interface{}, bool) { return cypherListValue(value) }
