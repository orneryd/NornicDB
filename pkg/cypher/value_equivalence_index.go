package cypher

import (
	"bytes"
	"context"
	"hash/maphash"
	"math"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type cypherEncodedKey string
type cypherNaNKey uint64
type cypherEntityKey struct {
	kind byte
	id   string
}

// cypherComparableKey normalizes scalars without formatting numbers or
// copying strings. Containers use structural hashing below. Uncommon scalar
// values retain the canonical type/value encoding. Numbers and lists are read
// by the shared input readers.
func cypherComparableKey(value interface{}, contract cypherComparisonContract) (interface{}, bool) {
	switch value := value.(type) {
	case nil, string, bool:
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
	}
	if number, numeric := valueconvert.ReadNumber(value); numeric {
		return cypherNumberKey(value, number), true
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

// cypherNumberKey keys a number by its exact value, so 1, 1.0 and uint8(1)
// are one key and each NaN is its own. An int64 keeps its existing box.
func cypherNumberKey(value interface{}, number valueconvert.Number) interface{} {
	if signed, ok := number.Signed(); ok {
		if _, boxed := value.(int64); boxed {
			return value
		}
		return signed
	}
	if unsigned, ok := number.Unsigned(); ok {
		if unsigned <= math.MaxInt64 {
			return int64(unsigned)
		}
		return unsigned
	}
	floating, _ := number.Float()
	if math.IsNaN(floating) {
		return cypherNaNKey(nanKeys.Add(1))
	}
	if floating == math.Trunc(floating) {
		if floating >= -0x1p63 && floating < 0x1p63 {
			return int64(floating)
		}
		if floating >= 0x1p63 && floating < 0x1p64 {
			return uint64(floating)
		}
		// Outside the integer types' ranges, only another float can have
		// this exact value; keep the float itself as its key.
	}
	return floating
}

var cypherTupleHashSeed = maphash.MakeSeed()

func mixCypherHash(hash, value uint64) uint64 {
	return (hash ^ value) * 1099511628211
}

// cypherValueHash never serializes lists or maps. Map entry hashes are
// combined commutatively, independently of insertion/iteration order. A hash
// only selects a bucket; cypherValuesMatch checks collisions under the same
// explicitly selected contract, including nested temporal values.
func cypherValueHash(value interface{}, contract cypherComparisonContract) uint64 {
	return cypherValueHashWithCheckpoint(value, nil, contract)
}

// cypherValueHashWithCheckpoint is cypherValueHash counting each nested list
// item and map field against checkpoint (nil: none); a cancellation stops
// the walk with the owner's check, as the DISTINCT key traversal does.
func cypherValueHashWithCheckpoint(value interface{}, checkpoint *materializedRowCheckpoint, contract cypherComparisonContract) uint64 {
	if key, scalar := cypherComparableKey(value, contract); scalar {
		return maphash.Comparable(cypherTupleHashSeed, key)
	}
	if value, bytes := value.([]byte); bytes {
		return mixCypherHash('X', maphash.Bytes(cypherTupleHashSeed, value))
	}
	if list, ok := cypherListValue(value); ok {
		hash := mixCypherHash('L', uint64(len(list)))
		for _, item := range list {
			checkpoint.Check()
			hash = mixCypherHash(hash, cypherValueHashWithCheckpoint(item, checkpoint, contract))
		}
		return hash
	}
	fields, _ := toStringAnyMap(value)
	hash := mixCypherHash('M', uint64(len(fields)))
	for name, item := range fields {
		checkpoint.Check()
		hash += mixCypherHash(maphash.String(cypherTupleHashSeed, name), cypherValueHashWithCheckpoint(item, checkpoint, contract))
	}
	return hash
}

// cypherValuesMatch compares structural values under the caller's contract:
// null equals null, numbers compare exactly, and each NaN remains separate.
func cypherValuesMatch(left, right interface{}, contract cypherComparisonContract) bool {
	return cypherValuesMatchWithCheckpoint(left, right, nil, contract)
}

// cypherValuesMatchWithCheckpoint is cypherValuesMatch observing checkpoint
// (nil: none) as cypherValueHashWithCheckpoint does.
func cypherValuesMatchWithCheckpoint(left, right interface{}, checkpoint *materializedRowCheckpoint, contract cypherComparisonContract) bool {
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
			checkpoint.Check()
			if !cypherValuesMatchWithCheckpoint(item, rightList[i], checkpoint, contract) {
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
		checkpoint.Check()
		other, exists := rightMap[name]
		if !exists || !cypherValuesMatchWithCheckpoint(item, other, checkpoint, contract) {
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

	emptyOrdinal int // Position plus one; zero means the empty tuple is unseen.

	// checkpoint observes the statement while the index hashes or compares
	// the items and fields of a nested list or map (observeCancellation arms
	// it; unarmed, the index observes nothing). A cancellation stops the walk
	// with the owner's stop, which a boundary recovers, so one
	// large value stops as well as many rows.
	checkpoint materializedRowCheckpoint
}

// observeCancellation arms the index's checkpoint with the statement.
func (index *cypherValueIndex) observeCancellation(ctx context.Context) {
	index.checkpoint = bulk.NewCheckpoint(ctx)
}

// walkCheckpoint is the armed checkpoint, or nil.
func (index *cypherValueIndex) walkCheckpoint() *materializedRowCheckpoint {
	if index.checkpoint == (bulk.Checkpoint{}) {
		return nil
	}
	return &index.checkpoint
}

func (index *cypherValueIndex) addByContract(value interface{}, contract cypherComparisonContract) (position int, existed bool) {
	key, scalar := cypherComparableKey(value, contract)
	if !scalar {
		return index.addTupleValuesByContract(cypherValueHashWithCheckpoint(value, index.walkCheckpoint(), contract), []interface{}{value}, contract)
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
		// Zero-column tuples have one equivalence class. Keep its position in
		// the index itself; global aggregation must not hash a synthetic key
		// or allocate a map merely to find that same class for every input row.
		if index.emptyOrdinal != 0 {
			return index.emptyOrdinal - 1, true
		}
		index.count++
		index.emptyOrdinal = index.count
		return index.emptyOrdinal - 1, false
	}
	if len(values) == 1 {
		return index.addByContract(values[0], contract)
	}
	hash := uint64(len(values))
	for _, value := range values {
		hash = mixCypherHash(hash, cypherValueHashWithCheckpoint(value, index.walkCheckpoint(), contract))
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
			if !cypherValuesMatchWithCheckpoint(value, candidate.values[i], index.walkCheckpoint(), contract) {
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
