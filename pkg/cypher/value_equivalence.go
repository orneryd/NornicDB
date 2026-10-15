package cypher

import (
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
//   - any other value (temporal, spatial, path) keeps its Go type and value.
func cypherEquivalenceKey(value interface{}) string {
	return cypherEquivalenceKeyWithCancellation(value, nil)
}

func cypherEquivalenceKeyWithCancellation(value interface{}, probe *pipelineCancellation) string {
	if probe != nil && !probe.check() {
		return ""
	}
	// The common single values get their key in one allocation; the rest are
	// written by appendEquivalenceKey.
	switch typed := value.(type) {
	case nil:
		return "N"
	case string:
		if probe == nil || len(typed) <= 4096 {
			return "S" + typed
		}
		var key strings.Builder
		key.WriteByte('S')
		if !appendPipelineString(&key, typed, probe) {
			return ""
		}
		return key.String()
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
	case int64:
		return "I" + strconv.FormatInt(typed, 10)
	}
	var key strings.Builder
	appendEquivalenceKey(&key, value, probe)
	return key.String()
}

// appendEquivalenceKey writes value's equivalence key (cypherEquivalenceKey).
// A key starts with one letter naming its kind; a list or map element's key
// is preceded by its length, so a composite key has one reading.
func appendEquivalenceKey(key *strings.Builder, value interface{}, probe *pipelineCancellation) {
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
		for start := 0; start < len(typed); start += 4096 {
			if probe != nil && !probe.check() {
				return
			}
			key.Write(typed[start:min(start+4096, len(typed))])
		}
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
	if list, ok := cypherListValueWithCancellation(value, probe); ok {
		key.WriteByte('L')
		key.WriteString(strconv.Itoa(len(list)))
		for _, item := range list {
			if probe != nil && !probe.check() {
				return
			}
			appendNestedEquivalenceKey(key, item, probe)
			if probe != nil && probe.err != nil {
				return
			}
		}
		return
	}
	if fields, ok := toStringAnyMapWithCancellation(value, probe); ok {
		names := make([]string, 0, len(fields))
		largeName := false
		for name := range fields {
			if probe != nil && !probe.check() {
				return
			}
			names = append(names, name)
			largeName = largeName || len(name) > 4096
		}
		if probe == nil || len(names) <= pipelineCancellationInterval && !largeName {
			sort.Strings(names)
		} else if !probe.sort(names, func(i, j int) bool { return comparePipelineStrings(names[i], names[j], probe) < 0 }, false) {
			return
		}
		key.WriteByte('M')
		key.WriteString(strconv.Itoa(len(names)))
		for _, name := range names {
			if probe != nil && !probe.check() {
				return
			}
			key.WriteByte('|')
			key.WriteString(strconv.Itoa(len(name)))
			key.WriteByte(':')
			if !appendPipelineString(key, name, probe) {
				return
			}
			appendNestedEquivalenceKey(key, fields[name], probe)
			if probe != nil && probe.err != nil {
				return
			}
		}
		return
	}
	if probe != nil && probe.err != nil {
		return
	}
	key.WriteByte('T')
	appendEquivalenceFallback(key, value, probe)
}

// appendNestedEquivalenceKey writes a list or map element's key preceded by
// its length.
func appendNestedEquivalenceKey(key *strings.Builder, value interface{}, probe *pipelineCancellation) {
	nested := cypherEquivalenceKeyWithCancellation(value, probe)
	if probe != nil && probe.err != nil {
		return
	}
	key.WriteByte('|')
	key.WriteString(strconv.Itoa(len(nested)))
	key.WriteByte(':')
	appendPipelineString(key, nested, probe)
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
