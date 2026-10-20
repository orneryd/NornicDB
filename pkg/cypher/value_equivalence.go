package cypher

import (
	"encoding/binary"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Canonical value keys encode the explicitly selected comparison contract.
// Execution uses cypherValueIndex's native scalar keys and
// collision-checked structural hashes instead of serializing containers.
// Two values are equivalent exactly when their encoded keys are equal.
// Equivalence has its own contract, including named-zone overlap identity:
//   - numbers are equivalent when their exact values are, whatever their
//     types: 1 ≡ 1.0 and -0.0 ≡ 0, while 9007199254740993 and
//     9007199254740992.0 stay apart, as they are unequal;
//   - lists and maps are equivalent element by element (map keys in any
//     order);
//   - nodes and relationships are equivalent when they are the same entity;
//   - NaN is equivalent to nothing, not even another NaN, as in Neo4j 5.26:
//     two NaNs are two DISTINCT values, two groups and two UNION rows. (Neo4j
//     counts the same bound NaN repeated once, by object identity; values here
//     have no identity, so every NaN is apart.)
//   - paths are equivalent by their ordered entity identities;
//   - temporal values use the canonical equivalence parts (overlap offsets retained);
//   - other values (including spatial values) keep their Go type and value.

// cypherValueKey serializes the requested comparison contract recursively.
func cypherValueKey(value interface{}, contract cypherComparisonContract) string {
	// The common single values get their key in one allocation; the rest are
	// written by appendValueKey.
	switch typed := value.(type) {
	case nil:
		return "N"
	case string:
		return "S" + typed
	case *storage.Node:
		if typed != nil {
			return "n:" + typed.IdentityKey()
		}
		return "N"
	case *storage.Edge:
		if typed != nil {
			return "r:" + typed.IdentityKey()
		}
		return "N"
	case storage.RemotePathValue:
		if typed == nil {
			return "N"
		}
	case storage.RemoteEntityValue:
		if typed == nil {
			return "N"
		}
		element := typed.IdentityKey()
		if cypherValueKindOf(typed) == valueKindRelationship {
			return "rr:" + element
		}
		return "rn:" + element
	case int64:
		return "I" + strconv.FormatInt(typed, 10)
	}
	var key strings.Builder
	appendValueKey(&key, value, cypherNumericValues, contract)
	return key.String()
}

// apoc.map.clean uses Java-object removal values: floating and integral
// representations stay distinct, recursively. Share structural encoding and
// temporal normalization with ordinary Cypher keys instead of reimplementing
// equality at this protocol boundary.
func cypherValueKeyWithNumericPolicy(value interface{}, numericPolicy cypherNumericComparisonPolicy, contract cypherComparisonContract) string {
	if numericPolicy == cypherNumericValues {
		return cypherValueKey(value, contract)
	}
	_, floating := value.(float64)
	_, list := cypherListValue(value)
	_, fields := toStringAnyMap(value)
	if !floating && !list && !fields {
		return cypherValueKey(value, contract)
	}
	var key strings.Builder
	appendValueKey(&key, value, numericPolicy, contract)
	return key.String()
}

// appendValueKey writes the key for the selected comparison contract.
// A key starts with one letter naming its kind; a list or map element's key
// is preceded by its length, so a composite key has one reading.
func appendValueKey(key *strings.Builder, value interface{}, numericPolicy cypherNumericComparisonPolicy, contract cypherComparisonContract) {
	switch typed := value.(type) {
	case bool:
		if typed {
			key.WriteString("Bt")
		} else {
			key.WriteString("Bf")
		}
		return
	case []byte:
		key.WriteByte('X')
		key.Write(typed)
		return
	}
	if signed, ok := cypherSignedInteger(value); ok {
		key.WriteByte('I')
		key.WriteString(strconv.FormatInt(signed, 10))
		return
	}
	if unsigned, ok := cypherUnsignedInteger(value); ok {
		key.WriteByte('I')
		key.WriteString(strconv.FormatUint(unsigned, 10))
		return
	}
	if number, ok := cypherFloatValue(value); ok {
		if _, floatKind := value.(float64); floatKind && numericPolicy == cypherNumericKinds {
			key.WriteString("F64:")
		}
		appendFloatEquivalenceKey(key, number)
		return
	}
	if cypherValueKindOf(value) == valueKindPath {
		if elements, valid := cypherPathElements(value); valid {
			key.WriteByte('P')
			key.WriteString(strconv.Itoa(len(elements)))
			for _, entity := range elements {
				appendNestedValueKey(key, entity, numericPolicy, contract)
			}
			return
		}
	}
	if list, ok := cypherListValue(value); ok {
		key.WriteByte('L')
		key.WriteString(strconv.Itoa(len(list)))
		for _, item := range list {
			appendNestedValueKey(key, item, numericPolicy, contract)
		}
		return
	}
	if fields, ok := toStringAnyMap(value); ok {
		names := make([]string, 0, len(fields))
		for name := range fields {
			names = append(names, name)
		}
		sort.Strings(names)
		key.WriteByte('M')
		key.WriteString(strconv.Itoa(len(names)))
		for _, name := range names {
			key.WriteByte('|')
			key.WriteString(strconv.Itoa(len(name)))
			key.WriteByte(':')
			key.WriteString(name)
			appendNestedValueKey(key, fields[name], numericPolicy, contract)
		}
		return
	}
	if parts, temporal := temporalIdentityParts(value, contract); temporal {
		appendTemporalValueKey(key, parts)
		return
	}
	key.WriteByte('T')
	key.WriteString(fmt.Sprintf("%T:%#v", value, value))
}

// Encoded keys are needed by older grouping paths and nested encoded values.
// Serialize the same typed identity without reflection or per-field formatting.
func appendTemporalValueKey(key *strings.Builder, parts temporalValueKey) {
	var buffer [1 + 7*8 + 8]byte
	buffer[0] = parts.kind
	for i, part := range parts.parts {
		binary.LittleEndian.PutUint64(buffer[1+i*8:], uint64(part))
	}
	binary.LittleEndian.PutUint64(buffer[1+7*8:], uint64(parts.offset))
	key.WriteByte('T')
	key.Write(buffer[:])
	key.WriteString(parts.zone)
}

// appendNestedValueKey writes a list or map element's key preceded by
// its length.
func appendNestedValueKey(key *strings.Builder, value interface{}, numericPolicy cypherNumericComparisonPolicy, contract cypherComparisonContract) {
	var nested string
	if numericPolicy == cypherNumericKinds {
		nested = cypherValueKeyWithNumericPolicy(value, numericPolicy, contract)
	} else {
		nested = cypherValueKey(value, contract)
	}
	key.WriteByte('|')
	key.WriteString(strconv.Itoa(len(nested)))
	key.WriteByte(':')
	key.WriteString(nested)
}

// nanKeys numbers NaN keys, so no two NaNs share one.
var nanKeys atomic.Uint64

// appendFloatEquivalenceKey writes a float's key: a whole number has the key
// of the integer with its exact value (so 1.0 ≡ 1 and -0.0 ≡ 0), any other
// float its shortest decimal form, and each NaN a key of its own.
func appendFloatEquivalenceKey(key *strings.Builder, number float64) {
	switch {
	case math.IsNaN(number):
		key.WriteString("FNaN#")
		key.WriteString(strconv.FormatUint(nanKeys.Add(1), 10))
	case math.IsInf(number, 0) || number != math.Trunc(number):
		key.WriteByte('F')
		key.WriteString(strconv.FormatFloat(number, 'g', -1, 64))
	case number >= math.MinInt64 && number < math.MaxInt64:
		key.WriteByte('I')
		key.WriteString(strconv.FormatInt(int64(number), 10))
	default:
		key.WriteByte('I')
		key.WriteString(new(big.Float).SetFloat64(number).Text('f', 0))
	}
}

// cypherValueSet stores values under each operation's explicit contract,
// for the apoc.coll functions that compare items as APOC's sets of AnyValues
// do: 1 and 1.0 are one item, a null is one item, and every NaN is its own.
type cypherValueSet struct {
	// Every operation receives an explicit comparison contract.
	encoded  map[string]int
	temporal map[temporalValueKey]int
	count    int
}

// add adds value; existed reports an equivalent value already there.
func (set *cypherValueSet) addByContract(value interface{}, contract cypherComparisonContract) (existed bool) {
	_, existed = set.addPositionByContract(value, contract)
	return existed
}

// addPosition shares first-occurrence lookup with frequencies. Typed temporal
// keys avoid allocating a serialized key for every scalar item.
func (set *cypherValueSet) addPositionByContract(value interface{}, contract cypherComparisonContract) (position int, existed bool) {
	// Ordinary scalars need no temporal extraction or large key return value.
	switch value.(type) {
	case nil, string, bool, int64, float64:
	default:
		if key, temporal := temporalIdentityParts(value, contract); temporal {
			if position, existed = set.temporal[key]; existed {
				return position, true
			}
			if set.temporal == nil {
				set.temporal = make(map[temporalValueKey]int)
			}
			position = set.count
			set.temporal[key] = position
			set.count++
			return position, false
		}
	}
	key := cypherValueKey(value, contract)
	if position, existed = set.encoded[key]; existed {
		return position, true
	}
	if set.encoded == nil {
		set.encoded = make(map[string]int)
	}
	position = set.count
	set.encoded[key] = position
	set.count++
	return position, false
}

// has reports whether the set holds a value equivalent to value.
func (set *cypherValueSet) hasByContract(value interface{}, contract cypherComparisonContract) bool {
	if key, temporal := temporalIdentityParts(value, contract); temporal {
		_, found := set.temporal[key]
		return found
	}
	_, found := set.encoded[cypherValueKey(value, contract)]
	return found
}

// cypherValueEquals is APOC's value equality (Util.valueEquals, used by
// apoc.coll.contains, containsAll, indexOf, occurrences and split):
// value equality, excluding a top-level null. Nested values keep that contract.
func cypherValueEquals(left, right interface{}) bool {
	if equal, temporal := compareTemporalByContract(left, right, cypherValueEqualityContract); temporal {
		return equal
	}
	return left != nil && right != nil && cypherValueKey(left, cypherValueEqualityContract) == cypherValueKey(right, cypherValueEqualityContract)
}
