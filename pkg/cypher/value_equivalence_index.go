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

func cypherComparableKey(value interface{}, checkpoint *materializedRowCheckpoint) (interface{}, bool) {
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
		return cypherEncodedKey(cypherEquivalenceKeyWithCheckpoint(value, checkpoint)), true
	case storage.RemotePathValue:
		if value == nil {
			return nil, true
		}
		return cypherEncodedKey(cypherEquivalenceKeyWithCheckpoint(value, checkpoint)), true
	case map[string]interface{}:
		// A genuine path wrapper keys by its ordered entities, as a native
		// path does; any other map, including a forged _pathResult, is
		// structural.
		if cypherValueKindOf(value) == valueKindPath {
			return cypherEncodedKey(cypherEquivalenceKeyWithCheckpoint(value, checkpoint)), true
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
		return cypherEncodedKey(cypherEquivalenceKeyWithCheckpoint(value, checkpoint)), true
	}
	if _, list := cypherListValue(value); list {
		return nil, false
	}
	if _, fields := toStringAnyMap(value); fields {
		return nil, false
	}
	return cypherEncodedKey(cypherEquivalenceKeyWithCheckpoint(value, checkpoint)), true
}

var cypherTupleHashSeed = maphash.MakeSeed()

func mixCypherHash(hash, value uint64) uint64 {
	return (hash ^ value) * 1099511628211
}

// cypherEquivalenceHash never serializes lists or maps. Map entry hashes are
// combined commutatively, independently of insertion/iteration order. A hash
// only selects a bucket; cypherValuesEquivalent checks collisions.
func cypherEquivalenceHash(value interface{}) uint64 {
	return cypherEquivalenceHashWithCheckpoint(value, nil)
}

func cypherEquivalenceHashWithCheckpoint(value interface{}, checkpoint *materializedRowCheckpoint) uint64 {
	if checkpoint != nil && checkpoint.due() && checkpoint.canceled() {
		panic(rowKeyCancellation{})
	}
	if key, scalar := cypherComparableKey(value, checkpoint); scalar {
		return maphash.Comparable(cypherTupleHashSeed, key)
	}
	if value, bytes := value.([]byte); bytes {
		return mixCypherHash('X', maphash.Bytes(cypherTupleHashSeed, value))
	}
	if list, ok := cypherListValue(value); ok {
		hash := mixCypherHash('L', uint64(len(list)))
		for _, item := range list {
			hash = mixCypherHash(hash, cypherEquivalenceHashWithCheckpoint(item, checkpoint))
		}
		return hash
	}
	fields, _ := toStringAnyMap(value)
	hash := mixCypherHash('M', uint64(len(fields)))
	for name, item := range fields {
		hash += mixCypherHash(maphash.String(cypherTupleHashSeed, name), cypherEquivalenceHashWithCheckpoint(item, checkpoint))
	}
	return hash
}

// cypherValuesEquivalent is DISTINCT's structural equality: null equals
// null, numbers compare exactly across types, and each NaN remains separate.

func cypherValuesEquivalent(left, right interface{}, checkpoint *materializedRowCheckpoint) bool {
	if checkpoint != nil && checkpoint.due() && checkpoint.canceled() {
		panic(rowKeyCancellation{})
	}
	leftKey, leftScalar := cypherComparableKey(left, checkpoint)
	rightKey, rightScalar := cypherComparableKey(right, checkpoint)
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
			if !cypherValuesEquivalent(item, rightList[i], checkpoint) {
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
		if !exists || !cypherValuesEquivalent(item, other, checkpoint) {
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

func (index *cypherValueIndex) add(value interface{}) (position int, existed bool) {
	return index.addWithCheckpoint(value, nil)
}

func (index *cypherValueIndex) addWithCheckpoint(value interface{}, checkpoint *materializedRowCheckpoint) (position int, existed bool) {
	key, scalar := cypherComparableKey(value, checkpoint)
	if !scalar {
		return index.addTupleValuesWithCheckpoint(cypherEquivalenceHashWithCheckpoint(value, checkpoint), []interface{}{value}, checkpoint)
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

func (index *cypherValueIndex) addTuple(values []interface{}) (int, bool) {
	return index.addTupleWithCheckpoint(values, nil)
}

func (index *cypherValueIndex) addTupleWithCheckpoint(values []interface{}, checkpoint *materializedRowCheckpoint) (int, bool) {
	if len(values) == 0 {
		return index.addComparable(cypherEmptyTupleKey{})
	}
	if len(values) == 1 {
		return index.addWithCheckpoint(values[0], checkpoint)
	}
	hash := uint64(len(values))
	for _, value := range values {
		hash = mixCypherHash(hash, cypherEquivalenceHashWithCheckpoint(value, checkpoint))
	}
	return index.addTupleValuesWithCheckpoint(hash, values, checkpoint)
}

func (index *cypherValueIndex) addTupleValues(hash uint64, values []interface{}) (int, bool) {
	return index.addTupleValuesWithCheckpoint(hash, values, nil)
}

func (index *cypherValueIndex) addTupleValuesWithCheckpoint(hash uint64, values []interface{}, checkpoint *materializedRowCheckpoint) (int, bool) {
	for _, candidate := range index.tuples[hash] {
		if checkpoint != nil && checkpoint.due() && checkpoint.canceled() {
			panic(rowKeyCancellation{})
		}
		if len(candidate.values) != len(values) {
			continue
		}
		equal := true
		for i, value := range values {
			if !cypherValuesEquivalent(value, candidate.values[i], checkpoint) {
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
