package cypher

import (
	"bytes"
	"hash/maphash"
	"math"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type cypherEncodedKey string
type cypherNaNKey uint64
type cypherEmptyTupleKey struct{}
type cypherEntityKey struct {
	kind byte
	id   string
}

// cypherComparableKey normalizes scalars without formatting numbers or
// copying strings. Containers use structural hashing below. Uncommon scalar
// values retain the canonical type/value encoding.
func cypherComparableKey(value interface{}, contract cypherComparisonContract) (interface{}, bool) {
	switch value := value.(type) {
	case nil, string, bool, int64:
		return value, true
	case *storage.Node:
		if value == nil {
			return nil, true
		}
		return cypherEntityKey{'n', string(value.ID)}, true
	case *storage.Edge:
		if value == nil {
			return nil, true
		}
		return cypherEntityKey{'r', string(value.ID)}, true
	case storage.RemoteEntityValue:
		if value == nil {
			return nil, true
		}
		return cypherEncodedKey(cypherValueKey(value, contract)), true
	case storage.RemotePathValue:
		if value == nil {
			return nil, true
		}
		return cypherEncodedKey(cypherValueKey(value, contract)), true
	case map[string]interface{}:
		// A genuine path wrapper keys by its ordered entities, as a native
		// path does; any other map, including a forged _pathResult, is
		// structural.
		if cypherValueKindOf(value) == valueKindPath {
			return cypherEncodedKey(cypherValueKey(value, contract)), true
		}
		return nil, false
	case []byte, []interface{}:
		return nil, false
	}
	if signed, ok := cypherSignedInteger(value); ok {
		return signed, true
	}
	if unsigned, ok := cypherUnsignedInteger(value); ok {
		if unsigned <= math.MaxInt64 {
			return int64(unsigned), true
		}
		return unsigned, true
	}
	if number, ok := cypherFloatValue(value); ok {
		if math.IsNaN(number) {
			return cypherNaNKey(nanKeys.Add(1)), true
		}
		if number == math.Trunc(number) {
			if number >= -0x1p63 && number < 0x1p63 {
				return int64(number), true
			}
			if number >= 0x1p63 && number < 0x1p64 {
				return uint64(number), true
			}
			// Outside the integer types' ranges, only another float can
			// have this exact value; keep the float itself as its key.
		}
		return number, true
	}
	if cypherValueKindOf(value) == valueKindPath {
		return cypherEncodedKey(cypherValueKey(value, contract)), true
	}
	if _, list := cypherListValue(value); list {
		return nil, false
	}
	if _, fields := toStringAnyMap(value); fields {
		return nil, false
	}
	return cypherEncodedKey(cypherValueKey(value, contract)), true
}

var cypherTupleHashSeed = maphash.MakeSeed()

func mixCypherHash(hash, value uint64) uint64 {
	return (hash ^ value) * 1099511628211
}

// cypherEquivalenceHash never serializes lists or maps. Map entry hashes are
// combined commutatively, independently of insertion/iteration order. A hash
// only selects a bucket; cypherValuesEquivalent checks collisions.
func cypherValueHash(value interface{}, contract cypherComparisonContract) uint64 {
	if key, scalar := cypherComparableKey(value, contract); scalar {
		return maphash.Comparable(cypherTupleHashSeed, key)
	}
	if value, bytes := value.([]byte); bytes {
		return mixCypherHash('X', maphash.Bytes(cypherTupleHashSeed, value))
	}
	if list, ok := cypherListValue(value); ok {
		hash := mixCypherHash('L', uint64(len(list)))
		for _, item := range list {
			hash = mixCypherHash(hash, cypherValueHash(item, contract))
		}
		return hash
	}
	fields, _ := toStringAnyMap(value)
	hash := mixCypherHash('M', uint64(len(fields)))
	for name, item := range fields {
		hash += mixCypherHash(maphash.String(cypherTupleHashSeed, name), cypherValueHash(item, contract))
	}
	return hash
}

// cypherValuesEquivalent is DISTINCT's structural equality: null equals
// null, numbers compare exactly across types, and each NaN remains separate.
func cypherValuesMatch(left, right interface{}, contract cypherComparisonContract) bool {
	leftKey, leftScalar := cypherComparableKey(left, contract)
	rightKey, rightScalar := cypherComparableKey(right, contract)
	if leftScalar || rightScalar {
		return leftScalar && rightScalar && leftKey == rightKey
	}
	leftBytes, leftIsBytes := left.([]byte)
	rightBytes, rightIsBytes := right.([]byte)
	if leftIsBytes || rightIsBytes {
		return leftIsBytes && rightIsBytes && bytes.Equal(leftBytes, rightBytes)
	}
	leftList, leftIsList := cypherListValue(left)
	rightList, rightIsList := cypherListValue(right)
	if leftIsList || rightIsList {
		if !leftIsList || !rightIsList || len(leftList) != len(rightList) {
			return false
		}
		for i, item := range leftList {
			if !cypherValuesMatch(item, rightList[i], contract) {
				return false
			}
		}
		return true
	}
	leftMap, _ := toStringAnyMap(left)
	rightMap, _ := toStringAnyMap(right)
	if len(leftMap) != len(rightMap) {
		return false
	}
	for name, item := range leftMap {
		other, exists := rightMap[name]
		if !exists || !cypherValuesMatch(item, other, contract) {
			return false
		}
	}
	return true
}

type cypherTupleEntry struct {
	values   []interface{}
	position int
}

// cypherValueIndex owns equivalence lookup for DISTINCT, grouping, collection
// deduplication and UNION. Scalar keys use Go's collision-checked hashing;
// structural tuples check every value in a bucket.
type cypherValueIndex struct {
	single map[interface{}]int
	tuples map[uint64][]cypherTupleEntry
	count  int
}

func (index *cypherValueIndex) addByContract(value interface{}, contract cypherComparisonContract) (position int, existed bool) {
	key, scalar := cypherComparableKey(value, contract)
	if !scalar {
		return index.addTupleValuesByContract(cypherValueHash(value, contract), []interface{}{value}, contract)
	}
	return index.addComparable(key)
}

func (index *cypherValueIndex) addComparable(key interface{}) (position int, existed bool) {
	if position, existed = index.single[key]; existed {
		return position, true
	}
	if index.single == nil {
		index.single = make(map[interface{}]int)
	}
	position = index.count
	index.single[key] = position
	index.count++
	return position, false
}

func (index *cypherValueIndex) addTupleByContract(values []interface{}, contract cypherComparisonContract) (int, bool) {
	if len(values) == 0 {
		return index.addComparable(cypherEmptyTupleKey{})
	}
	if len(values) == 1 {
		return index.addByContract(values[0], contract)
	}
	hash := uint64(len(values))
	for _, value := range values {
		hash = mixCypherHash(hash, cypherValueHash(value, contract))
	}
	return index.addTupleValuesByContract(hash, values, contract)
}

func (index *cypherValueIndex) addTupleValuesByContract(hash uint64, values []interface{}, contract cypherComparisonContract) (int, bool) {
	for _, candidate := range index.tuples[hash] {
		if len(candidate.values) != len(values) {
			continue
		}
		equal := true
		for i, value := range values {
			if !cypherValuesMatch(value, candidate.values[i], contract) {
				equal = false
				break
			}
		}
		if equal {
			return candidate.position, true
		}
	}
	if index.tuples == nil {
		index.tuples = make(map[uint64][]cypherTupleEntry)
	}
	position := index.count
	index.tuples[hash] = append(index.tuples[hash], cypherTupleEntry{append([]interface{}(nil), values...), position})
	index.count++
	return position, false
}
