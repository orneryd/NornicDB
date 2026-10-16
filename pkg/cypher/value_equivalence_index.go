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
func cypherComparableKey(value interface{}) (interface{}, bool) {
	return cypherComparableKeyWithCancellation(value, nil)
}

// cypherComparableKeyWithCancellation is cypherComparableKey whose encoded
// keys stop when probe is cancelled; the result is then unused.
func cypherComparableKeyWithCancellation(value interface{}, probe *pipelineCancellation) (interface{}, bool) {
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
		return cypherEncodedKey(cypherEquivalenceKeyWithCancellation(value, probe)), true
	case storage.RemotePathValue:
		if value == nil {
			return nil, true
		}
		return cypherEncodedKey(cypherEquivalenceKeyWithCancellation(value, probe)), true
	case map[string]interface{}:
		// A genuine path wrapper keys by its ordered entities, as a native
		// path does; any other map, including a forged _pathResult, is
		// structural.
		if cypherValueKindOf(value) == valueKindPath {
			return cypherEncodedKey(cypherEquivalenceKeyWithCancellation(value, probe)), true
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
		return cypherEncodedKey(cypherEquivalenceKeyWithCancellation(value, probe)), true
	}
	if _, list := cypherListValueWithCancellation(value, probe); list {
		return nil, false
	}
	if _, fields := toStringAnyMapWithCancellation(value, probe); fields {
		return nil, false
	}
	return cypherEncodedKey(cypherEquivalenceKeyWithCancellation(value, probe)), true
}

var cypherTupleHashSeed = maphash.MakeSeed()

func mixCypherHash(hash, value uint64) uint64 {
	return (hash ^ value) * 1099511628211
}

// cypherEquivalenceHash never serializes lists or maps. Map entry hashes are
// combined commutatively, independently of insertion/iteration order. A hash
// only selects a bucket; cypherValuesEquivalent checks collisions.
func cypherEquivalenceHash(value interface{}) uint64 {
	return cypherEquivalenceHashWithCancellation(value, nil)
}

// cypherEquivalenceHashWithCancellation checks probe once per contained
// value; a cancelled hash is unused.
func cypherEquivalenceHashWithCancellation(value interface{}, probe *pipelineCancellation) uint64 {
	if probe != nil && !probe.check() {
		return 0
	}
	if key, scalar := cypherComparableKeyWithCancellation(value, probe); scalar {
		return maphash.Comparable(cypherTupleHashSeed, key)
	}
	if value, bytes := value.([]byte); bytes {
		return mixCypherHash('X', maphash.Bytes(cypherTupleHashSeed, value))
	}
	if list, ok := cypherListValueWithCancellation(value, probe); ok {
		hash := mixCypherHash('L', uint64(len(list)))
		for _, item := range list {
			if probe != nil && probe.err != nil {
				return 0
			}
			hash = mixCypherHash(hash, cypherEquivalenceHashWithCancellation(item, probe))
		}
		return hash
	}
	fields, _ := toStringAnyMapWithCancellation(value, probe)
	hash := mixCypherHash('M', uint64(len(fields)))
	for name, item := range fields {
		if probe != nil && probe.err != nil {
			return 0
		}
		hash += mixCypherHash(maphash.String(cypherTupleHashSeed, name), cypherEquivalenceHashWithCancellation(item, probe))
	}
	return hash
}

// cypherValuesEquivalent is DISTINCT's structural equality: null equals
// null, numbers compare exactly across types, and each NaN remains separate.
func cypherValuesEquivalent(left, right interface{}) bool {
	return cypherValuesEquivalentWithCancellation(left, right, nil)
}

// cypherValuesEquivalentWithCancellation checks probe once per compared
// value; a cancelled comparison is false and unused.
func cypherValuesEquivalentWithCancellation(left, right interface{}, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	leftKey, leftScalar := cypherComparableKeyWithCancellation(left, probe)
	rightKey, rightScalar := cypherComparableKeyWithCancellation(right, probe)
	if leftScalar || rightScalar {
		return leftScalar && rightScalar && leftKey == rightKey
	}
	leftBytes, leftIsBytes := left.([]byte)
	rightBytes, rightIsBytes := right.([]byte)
	if leftIsBytes || rightIsBytes {
		return leftIsBytes && rightIsBytes && bytes.Equal(leftBytes, rightBytes)
	}
	leftList, leftIsList := cypherListValueWithCancellation(left, probe)
	rightList, rightIsList := cypherListValueWithCancellation(right, probe)
	if leftIsList || rightIsList {
		if !leftIsList || !rightIsList || len(leftList) != len(rightList) {
			return false
		}
		for i, item := range leftList {
			if !cypherValuesEquivalentWithCancellation(item, rightList[i], probe) {
				return false
			}
		}
		return true
	}
	leftMap, _ := toStringAnyMapWithCancellation(left, probe)
	rightMap, _ := toStringAnyMapWithCancellation(right, probe)
	if len(leftMap) != len(rightMap) {
		return false
	}
	for name, item := range leftMap {
		other, exists := rightMap[name]
		if !exists || !cypherValuesEquivalentWithCancellation(item, other, probe) {
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
	// probe, when set, is checked while values are hashed and compared; a
	// cancelled add stores nothing and reports a new position the caller
	// never uses, because it stops on probe.err.
	probe *pipelineCancellation
}

func (index *cypherValueIndex) add(value interface{}) (position int, existed bool) {
	key, scalar := cypherComparableKeyWithCancellation(value, index.probe)
	if index.cancelled() {
		return index.count, false
	}
	if !scalar {
		return index.addTupleValues(cypherEquivalenceHashWithCancellation(value, index.probe), []interface{}{value})
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
	if len(values) == 0 {
		return index.addComparable(cypherEmptyTupleKey{})
	}
	if len(values) == 1 {
		return index.add(values[0])
	}
	hash := uint64(len(values))
	for _, value := range values {
		hash = mixCypherHash(hash, cypherEquivalenceHashWithCancellation(value, index.probe))
		if index.cancelled() {
			return index.count, false
		}
	}
	return index.addTupleValues(hash, values)
}

func (index *cypherValueIndex) addTupleValues(hash uint64, values []interface{}) (int, bool) {
	if index.cancelled() {
		return index.count, false
	}
	for _, candidate := range index.tuples[hash] {
		if len(candidate.values) != len(values) {
			continue
		}
		equal := true
		for i, value := range values {
			if !cypherValuesEquivalentWithCancellation(value, candidate.values[i], index.probe) {
				equal = false
				break
			}
		}
		if index.cancelled() {
			return index.count, false
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

func (index *cypherValueIndex) cancelled() bool {
	return index.probe != nil && index.probe.err != nil
}
