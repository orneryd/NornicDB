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

// cypherEquivalenceKey is value's key under Cypher equivalence, the sameness
// DISTINCT, grouping keys, UNION and count/collect(DISTINCT …) use: two values
// are equivalent exactly when their keys are equal. Equivalence is equality
// (cypherEquality) except that null is equivalent to null:
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
//   - temporal values use the same canonical parts as temporal equality;
//   - other values (including spatial values) keep their Go type and value.
func cypherEquivalenceKey(value interface{}) string {
	// The common single values get their key in one allocation; the rest are
	// written by appendEquivalenceKey.
	switch typed := value.(type) {
	case nil:
		return "N"
	case string:
		return "S" + typed
	case *storage.Node:
		if typed != nil {
			return "n:" + string(typed.ID)
		}
		return "N"
	case *storage.Edge:
		if typed != nil {
			return "r:" + string(typed.ID)
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
		element, _ := typed["elementId"].(string)
		if cypherValueKindOf(typed) == valueKindRelationship {
			return "rr:" + element
		}
		return "rn:" + element
	case int64:
		return "I" + strconv.FormatInt(typed, 10)
	}
	var key strings.Builder
	appendEquivalenceKey(&key, value)
	return key.String()
}

// appendEquivalenceKey writes value's equivalence key (cypherEquivalenceKey).
// A key starts with one letter naming its kind; a list or map element's key
// is preceded by its length, so a composite key has one reading.
func appendEquivalenceKey(key *strings.Builder, value interface{}) {
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
		appendFloatEquivalenceKey(key, number)
		return
	}
	if cypherValueKindOf(value) == valueKindPath {
		if elements, valid := cypherPathElements(value); valid {
			key.WriteByte('P')
			key.WriteString(strconv.Itoa(len(elements)))
			for _, entity := range elements {
				appendNestedEquivalenceKey(key, entity)
			}
			return
		}
	}
	if list, ok := cypherListValue(value); ok {
		key.WriteByte('L')
		key.WriteString(strconv.Itoa(len(list)))
		for _, item := range list {
			appendNestedEquivalenceKey(key, item)
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
			appendNestedEquivalenceKey(key, fields[name])
		}
		return
	}
	if parts, temporal := temporalEqualityParts(value); temporal {
		appendTemporalEquivalenceKey(key, parts)
		return
	}
	key.WriteByte('T')
	key.WriteString(fmt.Sprintf("%T:%#v", value, value))
}

// Encoded keys are needed by older grouping paths and nested encoded values.
// Serialize the same typed identity without reflection or per-field formatting.
func appendTemporalEquivalenceKey(key *strings.Builder, parts temporalEqualityKey) {
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

// appendNestedEquivalenceKey writes a list or map element's key preceded by
// its length.
func appendNestedEquivalenceKey(key *strings.Builder, value interface{}) {
	nested := cypherEquivalenceKey(value)
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

// cypherValueSet is a set of values under equivalence (cypherEquivalenceKey),
// for the apoc.coll functions that compare items as APOC's sets of AnyValues
// do: 1 and 1.0 are one item, a null is one item, and every NaN is its own.
type cypherValueSet struct {
	encoded  map[string]int
	temporal map[temporalEqualityKey]int
	count    int
}

// add adds value; existed reports an equivalent value already there.
func (set *cypherValueSet) add(value interface{}) (existed bool) {
	_, existed = set.addPosition(value)
	return existed
}

// addPosition shares first-occurrence lookup with frequencies. Typed temporal
// keys avoid allocating a serialized key for every scalar item.
func (set *cypherValueSet) addPosition(value interface{}) (position int, existed bool) {
	if key, temporal := temporalEqualityParts(value); temporal {
		if position, existed = set.temporal[key]; existed {
			return position, true
		}
		if set.temporal == nil {
			set.temporal = make(map[temporalEqualityKey]int)
		}
		position = set.count
		set.temporal[key] = position
	} else {
		key := cypherEquivalenceKey(value)
		if position, existed = set.encoded[key]; existed {
			return position, true
		}
		if set.encoded == nil {
			set.encoded = make(map[string]int)
		}
		position = set.count
		set.encoded[key] = position
	}
	set.count++
	return position, false
}

// has reports whether the set holds a value equivalent to value.
func (set *cypherValueSet) has(value interface{}) bool {
	if key, temporal := temporalEqualityParts(value); temporal {
		_, found := set.temporal[key]
		return found
	}
	_, found := set.encoded[cypherEquivalenceKey(value)]
	return found
}

// cypherValueEquals is APOC's value equality (Util.valueEquals, used by
// apoc.coll.contains, containsAll, indexOf, occurrences and split):
// equivalence, except that a null equals nothing.
func cypherValueEquals(left, right interface{}) bool {
	if equal, temporal := compareTemporalValues(left, right); temporal {
		return equal
	}
	return left != nil && right != nil && cypherEquivalenceKey(left) == cypherEquivalenceKey(right)
}
