package cypher

import (
	"context"
	"math"

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
	if leftKey, leftEntity := cypherEntityOrderingKey(left); leftEntity {
		rightKey, rightEntity := cypherEntityOrderingKey(right)
		if !rightEntity || leftKey.kind != rightKey.kind || leftKey.kind == valueKindNull {
			return 0, false, true
		}
		// A total sort key does not manufacture relational comparability
		// across ID domains, or for a nil native entity carrier.
		comparable := leftKey.carrier == rightKey.carrier && leftKey.carrier != 0
		if order := compareOrderedInts(leftKey.carrier, rightKey.carrier); order != 0 {
			return order, comparable, true
		}
		if leftKey.legacy < rightKey.legacy {
			return -1, comparable, true
		}
		if leftKey.legacy > rightKey.legacy {
			return 1, comparable, true
		}
		return compareStrings(leftKey.identity, rightKey.identity), comparable, true
	}
	if _, rightEntity := cypherEntityOrderingKey(right); rightEntity {
		return 0, false, true
	}
	leftMap, leftIsMap := toStringAnyMap(left)
	rightMap, rightIsMap := toStringAnyMap(right)
	leftPath, leftIsPath := cypherPathElementsOf(left, leftMap, leftIsMap)
	rightPath, rightIsPath := cypherPathElementsOf(right, rightMap, rightIsMap)
	leftPathCarrier := leftIsPath || cypherValueKindOf(left) == valueKindPath
	rightPathCarrier := rightIsPath || cypherValueKindOf(right) == valueKindPath
	if leftPathCarrier || rightPathCarrier {
		if !leftPathCarrier || !rightPathCarrier {
			return 0, false, true
		}
		if !leftIsPath || !rightIsPath {
			// Invalid/nil path carriers must not sort equal to every valid
			// path. They remain incomparable to comparison operators.
			leftValid, rightValid := 0, 0
			if leftIsPath {
				leftValid = 1
			}
			if rightIsPath {
				rightValid = 1
			}
			return compareOrderedInts(leftValid, rightValid), false, true
		}
		for index := 0; index < len(leftPath) && index < len(rightPath); index++ {
			if order, comparable := compareValue(leftPath[index], rightPath[index]); !comparable || order != 0 {
				return order, comparable, true
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

// cypherEntityOrderKey is a per-value key, never a pairwise fallback. Native
// IDs and remote numeric IDs belong to different identity domains. Remote
// carriers without a legacy numeric ID form a further domain ordered by
// element ID. Mixing numeric and element-only comparisons pairwise would
// violate transitivity. Properties never participate in entity ordering.
// Typed nil native carriers are distinguished from every real native entity.
type cypherEntityOrderKey struct {
	kind     cypherValueKind
	carrier  int
	legacy   int64
	identity string
}

func cypherEntityOrderingKey(value interface{}) (cypherEntityOrderKey, bool) {
	switch entity := value.(type) {
	case *storage.Node:
		key := cypherEntityOrderKey{kind: valueKindNode}
		if entity != nil {
			key.carrier = 1
			key.identity = string(entity.ID)
		}
		return key, true
	case *storage.Edge:
		key := cypherEntityOrderKey{kind: valueKindRelationship}
		if entity != nil {
			key.carrier = 1
			key.identity = string(entity.ID)
		}
		return key, true
	case storage.RemoteEntityValue:
		element, _ := entity["elementId"].(string)
		key := cypherEntityOrderKey{kind: cypherValueKindOf(entity), carrier: 2, identity: element}
		if id, _, _ := entity.LegacyIDs(); id >= 0 {
			key.carrier = 3
			key.legacy = id
		}
		return key, true
	}
	return cypherEntityOrderKey{}, false
}

// compareCypherMaps orders two maps: fewer keys first, then the sorted key
// names compared in order, then the values in key order by compareValue.
// It finds that first difference in one pass over each map, without sorting
// either: with as many keys, the smallest key in only one map decides the
// names (the map holding it is first), and with the same keys the smallest
// key whose values differ or don't compare decides the values.
func compareCypherMaps(left, right map[string]interface{}, compareValue func(a, b interface{}) (int, bool)) (int, bool) {
	if order := compareOrderedInts(len(left), len(right)); order != 0 {
		return order, true
	}
	unshared, order := "", 0
	for key := range left {
		if _, shared := right[key]; !shared && (order == 0 || key < unshared) {
			unshared, order = key, -1
		}
	}
	for key := range right {
		if _, shared := left[key]; !shared && (order == 0 || key < unshared) {
			unshared, order = key, 1
		}
	}
	if order != 0 {
		return order, true
	}
	deciding, decided, comparable := "", false, true
	for key, value := range left {
		if decided && key > deciding {
			continue
		}
		if valueOrder, valueComparable := compareValue(value, right[key]); !valueComparable || valueOrder != 0 {
			deciding, decided, order, comparable = key, true, valueOrder, valueComparable
		}
	}
	if !comparable {
		return 0, false
	}
	return order, true
}

// sortedMapKeys is object's keys in order, sorted through the bulk owner
// under the statement (ctx; nil: none).
func sortedMapKeys(ctx context.Context, object map[string]interface{}) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_ = sortStrings(ctx, keys)
	return keys
}

// statementOfContexts is the statement among a call's optional contexts.
func statementOfContexts(contexts []context.Context) context.Context {
	if len(contexts) > 0 {
		return contexts[0]
	}
	return nil
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
