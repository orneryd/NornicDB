package nornicdb

// NOR-74, second public matrix: the apoc.coll deduplication, difference,
// frequency and removal functions that only the shipped APOC plugin exports.
// apoc.coll.containsDuplicates, .duplicates, .duplicatesWithCount,
// .dropDuplicateNeighbors, .different, .frequenciesAsMap and the scalar-value
// .removeAll are not in the engine's built-in catalog (pkg/apocmeta), so these
// checks load the bundled apoc.so from apoc/plugin-src/apoc the way the images
// do and run on both statement parsers.
//
// The item equality asserted here is the one pkg/cypher/value_equivalence.go
// documents for the apoc.coll family, and that the first NOR-74 matrix pins
// for toSet, union, intersection, subtract, contains, containsAll, containsAny,
// indexOf and occurrences:
//   - numbers are equivalent when their exact values are: 1 ≡ 1.0 and
//     9007199254740992 ≡ 9007199254740992.0, while 9007199254740993 and
//     9007199254740992.0 stay apart;
//   - lists and maps are equivalent element by element;
//   - a null is one item in a set;
//   - NaN is equivalent to nothing, not even another NaN.
//
// APOC 5.26.0 (core/src/main/java/apoc/coll/Coll.java) reaches the same answer
// by value, never by printed text: containsDuplicates, duplicates,
// duplicatesWithCount and different normalise every item with ValueUtils.of
// into a HashSet/LinkedHashMap, dropDuplicateNeighbors compares neighbours with
// Util.valueEquals, and frequenciesAsMap keys its counts by the item's text.
// A comparison of printed text would make 1 and '1' one item and would miss an
// integer next to the same float.
//
// apoc.coll.removeAll is NornicDB's documented scalar-value form,
// removeAll(list, value) (apoc/apoc.go, apoc/coll/coll.go). APOC 5.26.0's
// removeAll(list1, list2) is a different, two-list contract; this matrix does
// not assert list-versus-list parity.
//
// apoc.coll.duplicates and apoc.coll.duplicatesWithCount return their entries in
// an order NornicDB does not declare, so those checks compare contents.

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// apocCollDedupCase is one single-column RETURN of a plugin-only apoc.coll
// function and the value the family's equality gives it.
type apocCollDedupCase struct {
	name  string
	query string
	want  interface{}
}

// apocCollDedupList builds an expected list; an empty list is [] and not null.
func apocCollDedupList(items ...interface{}) []interface{} {
	return append([]interface{}{}, items...)
}

// apocCollDedupObject builds an expected map.
func apocCollDedupObject(fields map[string]interface{}) map[string]interface{} {
	return fields
}

// apocCollDedupPluginDB builds and loads the shipped APOC plugin and opens a
// database over the plugin directory, restoring the process-wide plugin and
// parser state when the test ends.
func apocCollDedupPluginDB(t *testing.T) *DB {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Go buildmode=plugin is unsupported on Windows")
	}
	if testing.Short() {
		t.Skip("builds the APOC plugin")
	}
	// The plugin runtime loads the bundled plugin once per test process, so the
	// build is shared with the other checks that need the real apoc.so.
	plugins := bundledAPOCPluginDir(t)

	resetPluginTestState()
	lookup, list := cypher.PluginFunctionLookup, cypher.PluginFunctionList
	t.Cleanup(func() {
		cypher.PluginFunctionLookup, cypher.PluginFunctionList = lookup, list
		resetPluginTestState()
	})
	// The parser type is a suite-wide flag; these checks restore it.
	previousParser := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previousParser) })

	cfg := DefaultConfig()
	cfg.Server.PluginsDir = plugins
	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// The plugin is loaded: a function only it exports answers.
	result, err := db.ExecuteCypher(context.Background(), "RETURN apoc.coll.containsDuplicates([1,2,3,2]) AS v", nil)
	require.NoError(t, err, "the shipped plugin must answer apoc.coll.containsDuplicates")
	require.Len(t, result.Rows, 1)
	require.Equal(t, true, result.Rows[0][0])
	return db
}

// apocCollDedupForEachParser runs body once per statement parser.
func apocCollDedupForEachParser(t *testing.T, body func(t *testing.T)) {
	t.Helper()
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run("parser="+parser, func(t *testing.T) {
			config.SetParserType(parser)
			require.Equal(t, parser, config.GetParserType())
			body(t)
		})
	}
}

// apocCollDedupCheckExact asserts each case's single-row value exactly.
func apocCollDedupCheckExact(t *testing.T, db *DB, cases []apocCollDedupCase) {
	t.Helper()
	ctx := context.Background()
	apocCollDedupForEachParser(t, func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := db.ExecuteCypher(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Len(t, result.Rows, 1, tc.query)
				// Assert every case so one deviation does not hide the rest.
				assert.Equal(t, tc.want, result.Rows[0][0], tc.query)
			})
		}
	})
}

// apocCollDedupItemKey names a value by its Go type and text. It lets the checks
// compare an unordered result list; it says nothing about how the functions
// themselves compare items.
func apocCollDedupItemKey(value interface{}) string {
	return fmt.Sprintf("%T=%#v", value, value)
}

// apocCollDedupSortedKeys renders a value list as sorted type-and-text keys.
func apocCollDedupSortedKeys(values []interface{}) []string {
	keys := make([]string, 0, len(values))
	for _, value := range values {
		keys = append(keys, apocCollDedupItemKey(value))
	}
	sort.Strings(keys)
	return keys
}

// apocCollDedupCheckUnordered asserts each case's single-row list as a
// multiset, because the function declares no entry order.
func apocCollDedupCheckUnordered(t *testing.T, db *DB, cases []apocCollDedupCase) {
	t.Helper()
	ctx := context.Background()
	apocCollDedupForEachParser(t, func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				wantValues, ok := tc.want.([]interface{})
				require.True(t, ok, "%s: want %T is not a list", tc.query, tc.want)
				result, err := db.ExecuteCypher(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Len(t, result.Rows, 1, tc.query)
				gotValues, ok := result.Rows[0][0].([]interface{})
				require.True(t, ok, "%s: got %T", tc.query, result.Rows[0][0])
				assert.Equal(t, apocCollDedupSortedKeys(wantValues), apocCollDedupSortedKeys(gotValues), tc.query)
			})
		}
	})
}

// apocCollDedupCount is one {item, count} entry a check expects.
type apocCollDedupCount struct {
	item  interface{}
	count int64
}

// apocCollDedupExpectedCounts renders expected {item, count} entries as counts
// keyed by the item, so entry order cannot affect the comparison.
func apocCollDedupExpectedCounts(entries ...apocCollDedupCount) map[string]int64 {
	counts := make(map[string]int64, len(entries))
	for _, entry := range entries {
		counts[apocCollDedupItemKey(entry.item)] = entry.count
	}
	return counts
}

// apocCollDedupCountsByItem reads a duplicatesWithCount result as counts keyed
// by the item. The plugin builds its own {item, count} maps with a Go int, so
// both int and INTEGER (int64) are the same count here; a count of any other
// type is a failure.
func apocCollDedupCountsByItem(t *testing.T, value interface{}) map[string]int64 {
	t.Helper()
	entries, ok := value.([]interface{})
	require.True(t, ok, "expected a list of {item, count} entries, got %T", value)
	counts := make(map[string]int64, len(entries))
	for _, entry := range entries {
		fields, ok := entry.(map[string]interface{})
		require.True(t, ok, "expected a {item, count} map, got %T", entry)
		item, present := fields["item"]
		require.True(t, present, "entry without item: %v", fields)
		count, present := fields["count"]
		require.True(t, present, "entry without count: %v", fields)
		switch typed := count.(type) {
		case int64:
			counts[apocCollDedupItemKey(item)] = typed
		case int:
			counts[apocCollDedupItemKey(item)] = int64(typed)
		default:
			require.FailNowf(t, "unexpected count type", "count of %v is %T", item, count)
		}
	}
	return counts
}

// apocCollDedupCheckCounts asserts each case's single-row list as counts keyed
// by item, because the function declares no entry order.
func apocCollDedupCheckCounts(t *testing.T, db *DB, cases []apocCollDedupCase) {
	t.Helper()
	ctx := context.Background()
	apocCollDedupForEachParser(t, func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				want, ok := tc.want.(map[string]int64)
				require.True(t, ok, "%s: want %T is not counts", tc.query, tc.want)
				result, err := db.ExecuteCypher(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Len(t, result.Rows, 1, tc.query)
				assert.Equal(t, want, apocCollDedupCountsByItem(t, result.Rows[0][0]), tc.query)
			})
		}
	})
}

// TestBundledAPOCPluginCollDedupContainsDuplicates pins
// apoc.coll.containsDuplicates on the plugin route.
func TestBundledAPOCPluginCollDedupContainsDuplicates(t *testing.T) {
	db := apocCollDedupPluginDB(t)
	apocCollDedupCheckExact(t, db, []apocCollDedupCase{
		// Rows this function documents in apoc/coll/coll.go.
		{"documented duplicates", `RETURN apoc.coll.containsDuplicates([1,2,3,2,4]) AS v`, true},
		{"documented distinct values", `RETURN apoc.coll.containsDuplicates([1,2,3,4,5]) AS v`, false},

		// Empty and single-item lists.
		{"empty list", `RETURN apoc.coll.containsDuplicates([]) AS v`, false},
		{"single item", `RETURN apoc.coll.containsDuplicates([1]) AS v`, false},
		{"repeated text", `RETURN apoc.coll.containsDuplicates(['a','a']) AS v`, true},

		// Items compare as Cypher values, never as printed text.
		{"integer and float of one value", `RETURN apoc.coll.containsDuplicates([1, 1.0]) AS v`, true},
		{"number and its text", `RETURN apoc.coll.containsDuplicates([1, '1']) AS v`, false},
		{"boolean and its text", `RETURN apoc.coll.containsDuplicates([true, 'true']) AS v`, false},
		{"integer and boolean", `RETURN apoc.coll.containsDuplicates([1, true]) AS v`, false},
		{"nested lists of one value", `RETURN apoc.coll.containsDuplicates([[1], [1.0]]) AS v`, true},
		{"nested list and its text", `RETURN apoc.coll.containsDuplicates([[1], ['1']]) AS v`, false},
		{"nested maps of one value", `RETURN apoc.coll.containsDuplicates([{a: 1}, {a: 1.0}]) AS v`, true},
		{"nested map and its text", `RETURN apoc.coll.containsDuplicates([{a: 1}, {a: '1'}]) AS v`, false},
		{"nested map and a different key", `RETURN apoc.coll.containsDuplicates([{a: 1}, {b: 1}]) AS v`, false},

		// Adjacent large integers stay apart; the exactly representable float
		// 9007199254740992.0 is that same number.
		{"large integer and its exact float", `RETURN apoc.coll.containsDuplicates([9007199254740992, 9007199254740992.0]) AS v`, true},
		{"adjacent large integers", `RETURN apoc.coll.containsDuplicates([9007199254740992, 9007199254740993]) AS v`, false},
		{"adjacent large integer and float", `RETURN apoc.coll.containsDuplicates([9007199254740993, 9007199254740992.0]) AS v`, false},

		// A null is one item in the family's sets; every NaN is its own.
		{"repeated null", `RETURN apoc.coll.containsDuplicates([null, null]) AS v`, true},
		{"null beside a value", `RETURN apoc.coll.containsDuplicates([null, 1]) AS v`, false},
		{"repeated NaN", `RETURN apoc.coll.containsDuplicates([0.0 / 0.0, 0.0 / 0.0]) AS v`, false},
	})
}

// TestBundledAPOCPluginCollDedupDuplicates pins apoc.coll.duplicates on the
// plugin route.
func TestBundledAPOCPluginCollDedupDuplicates(t *testing.T) {
	db := apocCollDedupPluginDB(t)
	list := apocCollDedupList
	object := apocCollDedupObject
	apocCollDedupCheckUnordered(t, db, []apocCollDedupCase{
		// The documented row and the no-duplicate answers.
		{"documented duplicates", `RETURN apoc.coll.duplicates([1,2,3,2,4,3,5]) AS v`, list(int64(2), int64(3))},
		{"distinct values", `RETURN apoc.coll.duplicates([1,2,3]) AS v`, list()},
		{"empty list", `RETURN apoc.coll.duplicates([]) AS v`, list()},
		{"single item", `RETURN apoc.coll.duplicates([1]) AS v`, list()},

		// Numeric equivalence, not printed text.
		{"integer and float of one value", `RETURN apoc.coll.duplicates([1, 1.0]) AS v`, list(int64(1))},
		{"first occurrence is reported", `RETURN apoc.coll.duplicates([1.0, 1]) AS v`, list(float64(1))},
		{"number and its text", `RETURN apoc.coll.duplicates([1, '1']) AS v`, list()},
		{"boolean and its text", `RETURN apoc.coll.duplicates([true, 'true']) AS v`, list()},

		// Nested values compare element by element.
		{"nested lists of one value", `RETURN apoc.coll.duplicates([[1], [1.0]]) AS v`, list(list(int64(1)))},
		{"nested list and its text", `RETURN apoc.coll.duplicates([[1], ['1']]) AS v`, list()},
		{"nested maps of one value", `RETURN apoc.coll.duplicates([{a: 1}, {a: 1.0}]) AS v`, list(object(map[string]interface{}{"a": int64(1)}))},
		{"nested map and its text", `RETURN apoc.coll.duplicates([{a: 1}, {a: '1'}]) AS v`, list()},

		// Adjacent large integers.
		{"large integer and its exact float", `RETURN apoc.coll.duplicates([9007199254740992, 9007199254740992.0]) AS v`, list(int64(9007199254740992))},
		{"adjacent large integers", `RETURN apoc.coll.duplicates([9007199254740992, 9007199254740993]) AS v`, list()},
		{"adjacent large integer and float", `RETURN apoc.coll.duplicates([9007199254740993, 9007199254740992.0]) AS v`, list()},

		// A null is one set item and is reported once; every NaN is its own.
		{"repeated null", `RETURN apoc.coll.duplicates([null, null, 1]) AS v`, list(nil)},
		{"repeated NaN", `RETURN apoc.coll.duplicates([0.0 / 0.0, 0.0 / 0.0]) AS v`, list()},
	})
}

// TestBundledAPOCPluginCollDedupDuplicatesWithCount pins
// apoc.coll.duplicatesWithCount on the plugin route.
func TestBundledAPOCPluginCollDedupDuplicatesWithCount(t *testing.T) {
	db := apocCollDedupPluginDB(t)
	object := apocCollDedupObject
	apocCollDedupCheckCounts(t, db, []apocCollDedupCase{
		// The documented row.
		{"documented duplicates", `RETURN apoc.coll.duplicatesWithCount([1,2,3,2,4,3,3]) AS v`,
			apocCollDedupExpectedCounts(apocCollDedupCount{item: int64(2), count: 2}, apocCollDedupCount{item: int64(3), count: 3})},
		{"distinct values", `RETURN apoc.coll.duplicatesWithCount([1,2,3]) AS v`, apocCollDedupExpectedCounts()},
		{"empty list", `RETURN apoc.coll.duplicatesWithCount([]) AS v`, apocCollDedupExpectedCounts()},

		// Numeric equivalence, not printed text.
		{"integer and float count together", `RETURN apoc.coll.duplicatesWithCount([1, 1.0, 1]) AS v`,
			apocCollDedupExpectedCounts(apocCollDedupCount{item: int64(1), count: 3})},
		{"number and its text stay apart", `RETURN apoc.coll.duplicatesWithCount([1, '1', 1]) AS v`,
			apocCollDedupExpectedCounts(apocCollDedupCount{item: int64(1), count: 2})},

		// Nested values compare element by element.
		{"nested maps of one value", `RETURN apoc.coll.duplicatesWithCount([{a: 1}, {a: 1.0}]) AS v`,
			apocCollDedupExpectedCounts(apocCollDedupCount{item: object(map[string]interface{}{"a": int64(1)}), count: 2})},
		{"nested map and its text stay apart", `RETURN apoc.coll.duplicatesWithCount([{a: 1}, {a: '1'}]) AS v`,
			apocCollDedupExpectedCounts()},

		// Adjacent large integers.
		{"large integer and its exact float", `RETURN apoc.coll.duplicatesWithCount([9007199254740992, 9007199254740992.0]) AS v`,
			apocCollDedupExpectedCounts(apocCollDedupCount{item: int64(9007199254740992), count: 2})},
		{"adjacent large integer and float stay apart", `RETURN apoc.coll.duplicatesWithCount([9007199254740993, 9007199254740992.0]) AS v`,
			apocCollDedupExpectedCounts()},

		// A null is one set item; every NaN is its own.
		{"repeated null", `RETURN apoc.coll.duplicatesWithCount([null, null]) AS v`,
			apocCollDedupExpectedCounts(apocCollDedupCount{item: nil, count: 2})},
		{"repeated NaN", `RETURN apoc.coll.duplicatesWithCount([0.0 / 0.0, 0.0 / 0.0]) AS v`, apocCollDedupExpectedCounts()},
	})
}

// TestBundledAPOCPluginCollDedupDropDuplicateNeighbors pins
// apoc.coll.dropDuplicateNeighbors on the plugin route: consecutive neighbours
// collapse when they are equal values, in order.
func TestBundledAPOCPluginCollDedupDropDuplicateNeighbors(t *testing.T) {
	db := apocCollDedupPluginDB(t)
	list := apocCollDedupList
	object := apocCollDedupObject
	apocCollDedupCheckExact(t, db, []apocCollDedupCase{
		// The documented row and the ordinary controls.
		{"documented neighbours", `RETURN apoc.coll.dropDuplicateNeighbors([1,1,2,2,3,3,2,2]) AS v`, list(int64(1), int64(2), int64(3), int64(2))},
		{"empty list", `RETURN apoc.coll.dropDuplicateNeighbors([]) AS v`, list()},
		{"non-adjacent repeats stay", `RETURN apoc.coll.dropDuplicateNeighbors([1,2,1]) AS v`, list(int64(1), int64(2), int64(1))},

		// Neighbours compare as Cypher values, never as printed text.
		{"integer and float neighbours", `RETURN apoc.coll.dropDuplicateNeighbors([1, 1.0, 2]) AS v`, list(int64(1), int64(2))},
		{"number and its text neighbours", `RETURN apoc.coll.dropDuplicateNeighbors([1, '1', 1]) AS v`, list(int64(1), "1", int64(1))},
		{"boolean and its text neighbours", `RETURN apoc.coll.dropDuplicateNeighbors([true, 'true']) AS v`, list(true, "true")},
		{"nested list neighbours", `RETURN apoc.coll.dropDuplicateNeighbors([[1], [1.0], [2]]) AS v`, list(list(int64(1)), list(int64(2)))},
		{"nested map neighbours", `RETURN apoc.coll.dropDuplicateNeighbors([{a: 1}, {a: 1.0}]) AS v`, list(object(map[string]interface{}{"a": int64(1)}))},
		{"nested map and its text neighbours", `RETURN apoc.coll.dropDuplicateNeighbors([{a: 1}, {a: '1'}]) AS v`, list(object(map[string]interface{}{"a": int64(1)}), object(map[string]interface{}{"a": "1"}))},

		// Adjacent large integers.
		{"large integer and its exact float", `RETURN apoc.coll.dropDuplicateNeighbors([9007199254740992, 9007199254740992.0]) AS v`, list(int64(9007199254740992))},
		{"adjacent large integer and float stay", `RETURN apoc.coll.dropDuplicateNeighbors([9007199254740993, 9007199254740992.0]) AS v`, list(int64(9007199254740993), float64(9007199254740992))},

		// A null is one item; every NaN is its own.
		{"repeated null", `RETURN apoc.coll.dropDuplicateNeighbors([null, null, 1]) AS v`, list(nil, int64(1))},
		{"null neighbours", `RETURN apoc.coll.dropDuplicateNeighbors([1, null, null, 2]) AS v`, list(int64(1), nil, int64(2))},
		{"repeated NaN stays", `RETURN size(apoc.coll.dropDuplicateNeighbors([0.0 / 0.0, 0.0 / 0.0])) AS v`, int64(2)},
	})
}

// TestBundledAPOCPluginCollDifferent pins apoc.coll.different on the plugin
// route: the elements of the first list that are not in the second, in order
// and with their multiplicity, compared as Cypher values.
func TestBundledAPOCPluginCollDifferent(t *testing.T) {
	db := apocCollDedupPluginDB(t)
	list := apocCollDedupList
	object := apocCollDedupObject
	apocCollDedupCheckExact(t, db, []apocCollDedupCase{
		// The documented row.
		{"documented difference", `RETURN apoc.coll.different([1,2,3], [2,4]) AS v`, list(int64(1), int64(3))},
		{"empty second list", `RETURN apoc.coll.different([1,2], []) AS v`, list(int64(1), int64(2))},
		{"empty first list", `RETURN apoc.coll.different([], [1]) AS v`, list()},

		// Removal follows Cypher equality, never printed text.
		{"number removes its float", `RETURN apoc.coll.different([1,2,3], [2.0]) AS v`, list(int64(1), int64(3))},
		{"number keeps its text", `RETURN apoc.coll.different([1, '1'], [1]) AS v`, list("1")},
		{"boolean keeps its text", `RETURN apoc.coll.different([true, 'true'], [true]) AS v`, list("true")},
		{"multiplicity is kept", `RETURN apoc.coll.different([1,1,2], [2]) AS v`, list(int64(1), int64(1))},
		{"nested list removed by its numeric twin", `RETURN apoc.coll.different([[1], [1.0], [2]], [[1]]) AS v`, list(list(int64(2)))},
		{"nested map keeps its text", `RETURN apoc.coll.different([{a: 1}, {a: '1'}], [{a: 1.0}]) AS v`, list(object(map[string]interface{}{"a": "1"}))},

		// Adjacent large integers.
		{"exact float removes the large integer", `RETURN apoc.coll.different([9007199254740992, 9007199254740993], [9007199254740992.0]) AS v`, list(int64(9007199254740993))},
		{"inexact float removes nothing", `RETURN apoc.coll.different([9007199254740993, 9007199254740992.0], [9007199254740993]) AS v`, list(float64(9007199254740992))},

		// A null is one value to remove; every NaN is its own.
		{"null removes both nulls", `RETURN apoc.coll.different([null, 1, null], [null]) AS v`, list(int64(1))},
		{"NaN removes nothing", `RETURN size(apoc.coll.different([0.0 / 0.0, 1], [0.0 / 0.0])) AS v`, int64(2)},
	})
}

// TestBundledAPOCPluginCollFrequenciesAsMap pins apoc.coll.frequenciesAsMap on
// the plugin route: equal numbers count together, as the v1.5.0 changelog states
// for the built-in and the plugin (#1164), and the counts are keyed by the
// item's text.
func TestBundledAPOCPluginCollFrequenciesAsMap(t *testing.T) {
	db := apocCollDedupPluginDB(t)
	frequencies := func(counts map[string]interface{}) map[string]interface{} { return counts }
	apocCollDedupCheckExact(t, db, []apocCollDedupCase{
		// The documented row and the empty list.
		{"documented frequencies", `RETURN apoc.coll.frequenciesAsMap([1,2,2,3,3,3]) AS v`,
			frequencies(map[string]interface{}{"1": int64(1), "2": int64(2), "3": int64(3)})},
		{"empty list", `RETURN apoc.coll.frequenciesAsMap([]) AS v`, frequencies(map[string]interface{}{})},

		// Equal numbers count together, whatever their type.
		{"integer and float count together", `RETURN apoc.coll.frequenciesAsMap([1, 1.0, 2, 2.0, 2.5]) AS v`,
			frequencies(map[string]interface{}{"1": int64(2), "2": int64(2), "2.5": int64(1)})},
		{"nested lists count together", `RETURN apoc.coll.frequenciesAsMap([[1], [1.0]]) AS v`,
			frequencies(map[string]interface{}{"[1]": int64(2)})},

		// Adjacent large integers stay apart; the exactly representable float
		// 9007199254740992.0 is the same number.
		{"adjacent large integers", `RETURN apoc.coll.frequenciesAsMap([9007199254740992, 9007199254740993]) AS v`,
			frequencies(map[string]interface{}{"9007199254740992": int64(1), "9007199254740993": int64(1)})},
		{"large integer and its exact float", `RETURN apoc.coll.frequenciesAsMap([9007199254740992, 9007199254740992.0]) AS v`,
			frequencies(map[string]interface{}{"9007199254740992": int64(2)})},

		// The rendered-key API keeps one entry for repeated null or NaN.
		{"null list", `RETURN apoc.coll.frequenciesAsMap(null) AS v`, frequencies(map[string]interface{}{})},
		{"repeated null is one entry", `RETURN size(keys(apoc.coll.frequenciesAsMap([null, null, 1]))) AS v`, int64(2)},
		{"repeated NaN counts under its rendered key", `RETURN apoc.coll.frequenciesAsMap([0.0 / 0.0, 0.0 / 0.0]) AS v`,
			frequencies(map[string]interface{}{"NaN": int64(2)})},
	})
}

// TestBundledAPOCPluginCollRemoveAll exercises the plugin's scalar removal
// contract, including null values and nested exact numeric equality.
func TestBundledAPOCPluginCollRemoveAll(t *testing.T) {
	db := apocCollDedupPluginDB(t)
	list := apocCollDedupList
	apocCollDedupCheckExact(t, db, []apocCollDedupCase{
		{"null value removes every null", `RETURN apoc.coll.removeAll([null, 1, null], null) AS v`, list(int64(1))},
		{"null value empties a null list", `RETURN apoc.coll.removeAll([null, null], null) AS v`, list()},
		{"null value keeps values", `RETURN apoc.coll.removeAll([1, 2], null) AS v`, list(int64(1), int64(2))},
		{"null keys the text boundary", `RETURN apoc.coll.removeAll(['null', null], null) AS v`, list("null")},
		{"value keeps nulls it does not match", `RETURN apoc.coll.removeAll([null, 1, 2], 1) AS v`, list(nil, int64(2))},
		{"number removes its float beside nulls", `RETURN apoc.coll.removeAll([null, 1, null, 1.0], 1) AS v`, list(nil, nil)},
		{"nested numeric lists", `RETURN apoc.coll.removeAll([[1], [1.0], [2]], [1]) AS v`, list(list(int64(2)))},
		{"nested numeric maps keep strings", `RETURN apoc.coll.removeAll([{a: 1}, {a: '1'}, {a: 1.0}], {a: 1.0}) AS v`,
			list(map[string]interface{}{"a": "1"})},
		{"exact large integer", `RETURN apoc.coll.removeAll([9007199254740993, 9007199254740992], 9007199254740992.0) AS v`,
			list(int64(9007199254740993))},
	})
}
