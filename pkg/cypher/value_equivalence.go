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

// cypherEquivalenceKey is value's deterministic encoded key under Cypher
// equivalence. Execution uses cypherValueIndex's native scalar keys and
// collision-checked structural hashes instead of serializing containers.
// Two values are equivalent exactly when their encoded keys are equal.
// Equivalence is equality
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
//   - any other value (temporal, spatial) keeps its Go type and value.
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
		if !appendPipelineByte(&key, 'S', probe) {
			return ""
		}

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
			if !appendPipelineString(key, "Bt", probe) {
				return
			}
		} else {
			if !appendPipelineString(key, "Bf", probe) {
				return
			}
		}
		return
	case []byte:
		if !appendPipelineByte(key, 'X', probe) {
			return
		}
		for start := 0; start < len(typed); start += 4096 {
			if probe != nil && !probe.check() {
				return
			}
			if !appendPipelineBytes(key, typed[start:min(start+4096, len(typed))], probe) {
				return
			}
		}
		return
	}
	if signed, ok := cypherSignedInteger(value); ok {
		if !appendPipelineByte(key, 'I', probe) {
			return
		}
		if !appendPipelineString(key, strconv.FormatInt(signed, 10), probe) {
			return
		}
		return
	}
	if unsigned, ok := cypherUnsignedInteger(value); ok {
		if !appendPipelineByte(key, 'I', probe) {
			return
		}
		if !appendPipelineString(key, strconv.FormatUint(unsigned, 10), probe) {
			return
		}
		return
	}
	if number, ok := cypherFloatValue(value); ok {
		appendFloatEquivalenceKeyWithCancellation(key, number, probe)
		return
	}
	if cypherValueKindOf(value) == valueKindPath {
		object, isMap := toStringAnyMapWithCancellation(value, probe)
		elements, valid := cypherPathElementsOfWithCancellation(value, object, isMap, probe)
		if probe != nil && probe.err != nil {
			return
		}
		if valid {
			if !appendPipelineByte(key, 'P', probe) {
				return
			}
			if !appendPipelineString(key, strconv.Itoa(len(elements)), probe) {
				return
			}
			for _, entity := range elements {
				if probe != nil && !probe.check() {
					return
				}
				appendNestedEquivalenceKey(key, entity, probe)
				if probe != nil && probe.err != nil {
					return
				}
			}
			return
		}
	}
	if list, ok := cypherListValueWithCancellation(value, probe); ok {
		if !appendPipelineByte(key, 'L', probe) {
			return
		}
		if !appendPipelineString(key, strconv.Itoa(len(list)), probe) {
			return
		}
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
		names := make([]string, 0, pipelineInitialCapacity(len(fields), probe))
		largeName := false
		for name := range fields {
			if probe != nil && !probe.check() {
				return
			}
			names = appendPipelineItem(names, name, probe)
			if probe != nil && probe.err != nil {
				return
			}

			largeName = largeName || len(name) > 4096
		}
		if probe == nil || len(names) <= pipelineCancellationInterval && !largeName {
			sort.Strings(names)
		} else if !probe.sort(names, func(i, j int) bool { return comparePipelineStrings(names[i], names[j], probe) < 0 }, false) {
			return
		}
		if !appendPipelineByte(key, 'M', probe) {
			return
		}
		if !appendPipelineString(key, strconv.Itoa(len(names)), probe) {
			return
		}
		for _, name := range names {
			if probe != nil && !probe.check() {
				return
			}
			if !appendPipelineByte(key, '|', probe) {
				return
			}
			if !appendPipelineString(key, strconv.Itoa(len(name)), probe) {
				return
			}
			if !appendPipelineByte(key, ':', probe) {
				return
			}
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
	if !appendPipelineByte(key, 'T', probe) {
		return
	}
	appendEquivalenceFallback(key, value, probe)
}

// appendNestedEquivalenceKey writes a list or map element's key preceded by
// its length.
func appendNestedEquivalenceKey(key *strings.Builder, value interface{}, probe *pipelineCancellation) {
	nested := cypherEquivalenceKeyWithCancellation(value, probe)
	if probe != nil && probe.err != nil {
		return
	}
	if !appendPipelineByte(key, '|', probe) {
		return
	}
	if !appendPipelineString(key, strconv.Itoa(len(nested)), probe) {
		return
	}
	if !appendPipelineByte(key, ':', probe) {
		return
	}
	appendPipelineString(key, nested, probe)
}

// nanKeys numbers NaN keys, so no two NaNs share one.
var nanKeys atomic.Uint64

// appendFloatEquivalenceKey writes a float's key: a whole number has the key
// of the integer with its exact value (so 1.0 ≡ 1 and -0.0 ≡ 0), any other
// float its shortest decimal form, and each NaN a key of its own.
func appendFloatEquivalenceKey(key *strings.Builder, number float64) {
	appendFloatEquivalenceKeyWithCancellation(key, number, nil)
}

func appendFloatEquivalenceKeyWithCancellation(key *strings.Builder, number float64, probe *pipelineCancellation) {
	switch {
	case math.IsNaN(number):
		if !appendPipelineString(key, "FNaN#", probe) {
			return
		}
		if !appendPipelineString(key, strconv.FormatUint(nanKeys.Add(1), 10), probe) {
			return
		}
	case math.IsInf(number, 0) || number != math.Trunc(number):
		if !appendPipelineByte(key, 'F', probe) {
			return
		}
		if !appendPipelineString(key, strconv.FormatFloat(number, 'g', -1, 64), probe) {
			return
		}
	case number >= math.MinInt64 && number < math.MaxInt64:
		if !appendPipelineByte(key, 'I', probe) {
			return
		}
		if !appendPipelineString(key, strconv.FormatInt(int64(number), 10), probe) {
			return
		}
	default:
		if !appendPipelineByte(key, 'I', probe) {
			return
		}
		if !appendPipelineString(key, new(big.Float).SetFloat64(number).Text('f', 0), probe) {
			return
		}
	}
}
