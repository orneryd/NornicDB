package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NOR-74: the public apoc.coll set and lookup functions compare items by
// Cypher value equality (pkg/cypher/value_equivalence.go), on the built-in
// route and on the shipped APOC plugin route, on both statement parsers
// (config.ParserTypeNornic and config.ParserTypeANTLR) and on both row value
// evaluators (compiled program and expression text).
//
// Contracts pinned here (APOC 5.26.0 core apoc/coll/Coll.java, apoc/util/Util.java):
//   - toSet, union, intersection and subtract hold sets of AnyValues
//     (ValueUtils.of into a HashSet): 1 and 1.0 are one item, lists and maps
//     compare element by element, a null is one item, every NaN its own;
//   - contains, containsAll, containsAny, indexOf and occurrences use
//     Util.valueEquals: the same equality, except that a null equals nothing.
//
// The two null contracts stay distinct; neither collapses into the other.

// apocCollNumericEqualityCase is one single-column RETURN of the matrix.
type apocCollNumericEqualityCase struct {
	query string
	want  interface{}
}

// apocCollNumericEqualityCases spans the acceptance reproduction of NOR-74,
// numeric integer/float equivalence, nested maps and lists, adjacent large
// integers, the null and NaN controls, and ordinary value controls.
func apocCollNumericEqualityCases() []apocCollNumericEqualityCase {
	list := func(items ...interface{}) []interface{} { return items }
	object := func(fields map[string]interface{}) map[string]interface{} { return fields }
	return []apocCollNumericEqualityCase{
		// NOR-74 acceptance: the reported reproduction and its result.
		{"RETURN size(apoc.coll.toSet([1, 1.0, 2, 2.0, 2.5])) AS v", int64(3)},
		{"RETURN apoc.coll.toSet([1, 1.0, 2, 2.0, 2.5]) AS v", list(int64(1), int64(2), float64(2.5))},
		{"RETURN apoc.coll.toSet([1.0, 1, 2.0, 2]) AS v", list(float64(1), float64(2))},

		// Numeric equivalence in every set and lookup consumer.
		{"RETURN apoc.coll.union([1, 2], [2.0, 3]) AS v", list(int64(1), int64(2), int64(3))},
		{"RETURN apoc.coll.union([1, 2], [2.0, 3, '3']) AS v", list(int64(1), int64(2), int64(3), "3")},
		{"RETURN apoc.coll.intersection([1, 2.0, '3', [4], 1], [2, 1.0, [4.0]]) AS v", list(int64(1), float64(2), list(int64(4)))},
		{"RETURN apoc.coll.subtract([1, 1, 2, 3.0, [1], '3'], [3, [1.0]]) AS v", list(int64(1), int64(2), "3")},
		{"RETURN apoc.coll.contains([1.0], 1) AS v", true},
		{"RETURN apoc.coll.contains([1, 2], 2.0) AS v", true},
		{"RETURN apoc.coll.contains([[1]], [1.0]) AS v", true},
		{"RETURN apoc.coll.contains(['1'], 1) AS v", false},
		{"RETURN apoc.coll.containsAll([1, 1.0], [1.0]) AS v", true},
		{"RETURN apoc.coll.containsAll([1, 2], [2.0, 1]) AS v", true},
		{"RETURN apoc.coll.containsAny([1, 2], [3, 2.0]) AS v", true},
		{"RETURN apoc.coll.indexOf([1.0, 2], 1) AS v", int64(0)},
		{"RETURN apoc.coll.indexOf([1, 2, 3], 2.0) AS v", int64(1)},
		{"RETURN apoc.coll.occurrences([1, 1.0, 1], 1.0) AS v", int64(3)},
		{"RETURN apoc.coll.occurrences([1, 1.0, '1', 2], 1) AS v", int64(2)},

		// Nested lists and maps: set, membership, and removal.
		{"RETURN apoc.coll.toSet([[1], ['1'], [1]]) AS v", list(list(int64(1)), list("1"))},
		{"RETURN apoc.coll.toSet([[1], [1.0], [{a: 1}], [{a: 1.0}]]) AS v", list(list(int64(1)), list(object(map[string]interface{}{"a": int64(1)})))},
		{"RETURN apoc.coll.toSet([{a: 1}, {a: '1'}, {a: 1.0}]) AS v", list(object(map[string]interface{}{"a": int64(1)}), object(map[string]interface{}{"a": "1"}))},
		{"RETURN apoc.coll.toSet([{a: [1, null]}, {a: [1.0, null]}]) AS v", list(object(map[string]interface{}{"a": list(int64(1), nil)}))},
		{"RETURN size(apoc.coll.toSet([[{a: 1}], [{a: 1.0}], [{a: '1'}]])) AS v", int64(2)},
		{"RETURN apoc.coll.contains([{a: [1]}], {a: [1.0]}) AS v", true},
		{"RETURN apoc.coll.indexOf([{a: [1.0]}], {a: [1]}) AS v", int64(0)},
		{"RETURN apoc.coll.intersection([{a: 1}, {b: 2}], [{a: 1.0}]) AS v", list(object(map[string]interface{}{"a": int64(1)}))},
		{"RETURN apoc.coll.union([[1]], [[1.0], [2]]) AS v", list(list(int64(1)), list(int64(2)))},
		{"RETURN apoc.coll.subtract([{a: 1}, {a: '1'}, {a: 1.0}], [{a: 1.0}]) AS v", list(object(map[string]interface{}{"a": "1"}))},
		{"RETURN apoc.coll.subtract([[1, 2], [1.0, 2], [1, '2']], [[1.0, 2.0]]) AS v", list(list(int64(1), "2"))},

		// Adjacent large integers stay apart; the exactly representable
		// float 9007199254740992.0 equals the same integer.
		{"RETURN apoc.coll.toSet([9007199254740992, 9007199254740993, 9007199254740992.0]) AS v", list(int64(9007199254740992), int64(9007199254740993))},
		{"RETURN size(apoc.coll.toSet([9007199254740993, 9007199254740992.0])) AS v", int64(2)},
		{"RETURN apoc.coll.contains([9007199254740993], 9007199254740992.0) AS v", false},
		{"RETURN apoc.coll.contains([9007199254740992], 9007199254740992.0) AS v", true},
		{"RETURN apoc.coll.indexOf([9007199254740993, 9007199254740992], 9007199254740992.0) AS v", int64(1)},
		{"RETURN apoc.coll.occurrences([9007199254740993, 9007199254740992], 9007199254740992.0) AS v", int64(1)},

		// A null is one set item and is in no list.
		{"RETURN apoc.coll.toSet([null, null]) AS v", list(nil)},
		{"RETURN apoc.coll.toSet([null, 1, null]) AS v", list(nil, int64(1))},
		{"RETURN apoc.coll.toSet(['1', 1, null, null]) AS v", list("1", int64(1), nil)},
		{"RETURN size(apoc.coll.toSet([[1, null], [1.0, null]])) AS v", int64(1)},
		{"RETURN apoc.coll.subtract([null, 1, null], [null]) AS v", list(int64(1))},
		{"RETURN apoc.coll.contains([null], null) AS v", false},
		{"RETURN apoc.coll.indexOf([null], null) AS v", int64(-1)},
		{"RETURN apoc.coll.occurrences([null, null], null) AS v", int64(0)},
		{"RETURN apoc.coll.containsAll([null, 1], [null]) AS v", false},
		{"RETURN apoc.coll.containsAny([null], [null]) AS v", false},

		// Every NaN is its own set item and equals nothing in a lookup.
		{"RETURN size(apoc.coll.toSet([0.0 / 0.0, 0.0 / 0.0])) AS v", int64(2)},
		{"RETURN size(apoc.coll.toSet([null, 0.0 / 0.0])) AS v", int64(2)},
		{"RETURN apoc.coll.contains([0.0 / 0.0], 0.0 / 0.0) AS v", false},
		{"RETURN apoc.coll.indexOf([0.0 / 0.0], 0.0 / 0.0) AS v", int64(-1)},
		{"RETURN apoc.coll.occurrences([0.0 / 0.0, 0.0 / 0.0], 0.0 / 0.0) AS v", int64(0)},

		// Ordinary controls: distinct types and values stay apart.
		{"RETURN apoc.coll.toSet([1, '1', true, true, 2, 2.5, 2.5]) AS v", list(int64(1), "1", true, int64(2), float64(2.5))},
		{"RETURN apoc.coll.contains([2], 2.5) AS v", false},
		{"RETURN apoc.coll.contains([1.5], 1.5) AS v", true},
		{"RETURN apoc.coll.indexOf(['1'], 1) AS v", int64(-1)},
		{"RETURN apoc.coll.occurrences([2, 2.5, 2], 2) AS v", int64(2)},
		{"RETURN apoc.coll.toSet([]) AS v", []interface{}{}},
	}
}

// apocCollNullContractQuery reads the set contract (a null is one item) and
// the membership contract (a null equals nothing) in one row, so neither can
// drift into the other.
const apocCollNullContractQuery = `RETURN size(apoc.coll.toSet([null, null])) AS setSize,
       apoc.coll.contains([null], null) AS contained,
       apoc.coll.indexOf([null], null) AS position,
       apoc.coll.occurrences([null, null], null) AS counted,
       apoc.coll.containsAll([null], [null]) AS allOf,
       apoc.coll.containsAny([null], [null]) AS anyOf,
       size(apoc.coll.toSet([0.0 / 0.0, 0.0 / 0.0])) AS nanSetSize`

func apocCollNullContractRow() []interface{} {
	return []interface{}{int64(1), false, int64(-1), int64(0), false, false, int64(2)}
}

// newApocCollNumericEqualityExecutor builds the public query route: the
// storage executor customers' statements reach.
func newApocCollNumericEqualityExecutor(t *testing.T, namespace string) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), namespace), QueryCachePolicy{MaxEntries: 0, TTL: 0})
}

// runApocCollNumericEqualityMatrix executes the matrix on both statement
// parsers and both row value evaluators, restoring both globals afterwards.
// Every case is asserted, so one deviation does not hide the rest.
func runApocCollNumericEqualityMatrix(t *testing.T, exec *StorageExecutor) {
	t.Helper()
	ctx := context.Background()
	previousParser := config.GetParserType()
	previousPrograms := rowValueProgramsDisabled.Swap(false)
	t.Cleanup(func() {
		config.SetParserType(previousParser)
		rowValueProgramsDisabled.Store(previousPrograms)
	})
	cases := apocCollNumericEqualityCases()
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		for _, programs := range []bool{true, false} {
			t.Run(fmt.Sprintf("parser=%s/programs=%t", parser, programs), func(t *testing.T) {
				config.SetParserType(parser)
				rowValueProgramsDisabled.Store(!programs)
				require.Equal(t, parser, config.GetParserType())

				for _, tc := range cases {
					result, err := exec.Execute(ctx, tc.query, nil)
					require.NoError(t, err, tc.query)
					require.Len(t, result.Rows, 1, tc.query)
					assert.Equal(t, tc.want, result.Rows[0][0], tc.query)
				}

				result, err := exec.Execute(ctx, apocCollNullContractQuery, nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				assert.Equal(t, apocCollNullContractRow(), result.Rows[0])
			})
		}
	}
}

// TestApocCollNumericEqualityBuiltInRoute pins the built-in apoc.coll
// functions, which answer with no plugin loaded.
func TestApocCollNumericEqualityBuiltInRoute(t *testing.T) {
	exec := newApocCollNumericEqualityExecutor(t, "apoc_coll_numeric_equality_builtin")
	runApocCollNumericEqualityMatrix(t, exec)
}

// TestApocCollNumericEqualityShippedPluginHandlerRoute installs the handlers
// the shipped APOC plugin (apoc.so) exports and runs the same matrix through
// the plugin-first dispatcher. The plugin exports none of these names (they
// are built-in, apoc.PluginFunctionNames), so the built-ins must answer with
// APOC equality rather than the plugin's printed-text copies.
func TestApocCollNumericEqualityShippedPluginHandlerRoute(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t))
	exec := newApocCollNumericEqualityExecutor(t, "apoc_coll_numeric_equality_plugin")
	runApocCollNumericEqualityMatrix(t, exec)
}

// TestApocCollNumericRemovalShippedPluginRoute covers apoc.coll.removeAll,
// the removal function the shipped plugin exports itself (it is not in the
// built-in catalog). Its documented contract is value-based: every element
// equal to the given value is removed (apoc/coll/coll.go RemoveAll). NOR-74
// requires that equality to be Cypher equality, nested values included.
func TestApocCollNumericRemovalShippedPluginRoute(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t))
	exec := newApocCollNumericEqualityExecutor(t, "apoc_coll_numeric_removal_plugin")
	ctx := context.Background()
	previousParser := config.GetParserType()
	previousPrograms := rowValueProgramsDisabled.Swap(false)
	t.Cleanup(func() {
		config.SetParserType(previousParser)
		rowValueProgramsDisabled.Store(previousPrograms)
	})
	list := func(items ...interface{}) []interface{} { return items }
	object := func(fields map[string]interface{}) map[string]interface{} { return fields }
	cases := []apocCollNumericEqualityCase{
		{"RETURN apoc.coll.removeAll([1, 1.0, 2], 1.0) AS v", list(int64(2))},
		{"RETURN apoc.coll.removeAll([{a: 1}, {a: '1'}, {a: 1.0}], {a: 1.0}) AS v", list(object(map[string]interface{}{"a": "1"}))},
		{"RETURN apoc.coll.removeAll([[1], [1.0], [2]], [1]) AS v", list(list(int64(2)))},
		{"RETURN apoc.coll.removeAll([9007199254740993, 9007199254740992], 9007199254740992.0) AS v", list(int64(9007199254740993))},
		{"RETURN apoc.coll.removeAll([1, '1', 2, 1], 1) AS v", list("1", int64(2))},
	}
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		for _, programs := range []bool{true, false} {
			t.Run(fmt.Sprintf("parser=%s/programs=%t", parser, programs), func(t *testing.T) {
				config.SetParserType(parser)
				rowValueProgramsDisabled.Store(!programs)
				for _, tc := range cases {
					result, err := exec.Execute(ctx, tc.query, nil)
					require.NoError(t, err, tc.query)
					require.Len(t, result.Rows, 1, tc.query)
					assert.Equal(t, tc.want, result.Rows[0][0], tc.query)
				}
			})
		}
	}
}
