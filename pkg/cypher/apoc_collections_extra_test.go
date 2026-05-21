package cypher

import (
	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestApocCollections_AvgMinMax(t *testing.T) {
	vals := []interface{}{int64(1), int64(3), int64(2)}
	assert.Equal(t, 2.0, apocCollAvg(vals))
	assert.Equal(t, int64(1), apocCollMin(vals))
	assert.Equal(t, int64(3), apocCollMax(vals))

	assert.Nil(t, apocCollAvg([]interface{}{}))
	assert.Nil(t, apocCollMin([]interface{}{}))
	assert.Nil(t, apocCollMax([]interface{}{}))
	assert.Nil(t, apocCollAvg("not-a-list"))
}

func TestApocCollections_SortAndReverse(t *testing.T) {
	vals := []interface{}{int64(3), int64(1), int64(2)}
	assert.Equal(t, []interface{}{int64(1), int64(2), int64(3)}, apocCollSort(vals))
	assert.Equal(t, []interface{}{int64(2), int64(1), int64(3)}, apocCollReverse([]interface{}{int64(3), int64(1), int64(2)}))
	assert.Nil(t, apocCollSort("x"))
	assert.Nil(t, apocCollReverse("x"))
}

func TestApocCollections_SortNodesAndProperty(t *testing.T) {
	n1 := &storage.Node{Properties: map[string]interface{}{"age": int64(40)}}
	n2 := &storage.Node{Properties: map[string]interface{}{"age": int64(20)}}
	n3 := map[string]interface{}{"properties": map[string]interface{}{"age": int64(30)}}

	sorted := apocCollSortNodes([]interface{}{n1, n2, n3}, "age")
	assert.Len(t, sorted, 3)
	assert.Equal(t, int64(20), getNodeProperty(sorted[0], "age"))
	assert.Equal(t, int64(30), getNodeProperty(sorted[1], "age"))
	assert.Equal(t, int64(40), getNodeProperty(sorted[2], "age"))
	assert.Nil(t, apocCollSortNodes("x", "age"))
	assert.Nil(t, getNodeProperty("x", "age"))
}

func TestApocCollections_UnionIntersectSubtract(t *testing.T) {
	a := []interface{}{int64(1), int64(2), int64(2)}
	b := []interface{}{int64(2), int64(3)}

	assert.Equal(t, []interface{}{int64(1), int64(2), int64(2), int64(2), int64(3)}, apocCollUnionAll(a, b))
	assert.Equal(t, []interface{}{int64(1), int64(2), int64(3)}, apocCollUnion(a, b))
	assert.Equal(t, []interface{}{int64(2)}, apocCollIntersection(a, b))
	assert.Equal(t, []interface{}{int64(1)}, apocCollSubtract(a, b))
	assert.Empty(t, apocCollIntersection("x", b))
	assert.Empty(t, apocCollSubtract(a, "x"))
}

func TestApocCollections_ContainsAndIndex(t *testing.T) {
	list := []interface{}{"a", "b", "c", "b"}
	assert.True(t, apocCollContains(list, "b"))
	assert.False(t, apocCollContains(list, "z"))
	assert.True(t, apocCollContainsAll(list, []interface{}{"a", "b"}))
	assert.False(t, apocCollContainsAll(list, []interface{}{"a", "z"}))
	assert.True(t, apocCollContainsAny(list, []interface{}{"z", "b"}))
	assert.False(t, apocCollContainsAny(list, []interface{}{"z", "y"}))
	assert.EqualValues(t, 1, apocCollIndexOf(list, "b"))
	assert.EqualValues(t, -1, apocCollIndexOf(list, "z"))
	assert.False(t, apocCollContains("x", "y"))
	assert.False(t, apocCollContainsAll("x", list))
	assert.False(t, apocCollContainsAny(list, "x"))
	assert.EqualValues(t, -1, apocCollIndexOf("x", "y"))
}

func TestApocCollections_SplitPartitionPairsZip(t *testing.T) {
	list := []interface{}{int64(1), int64(0), int64(2), int64(0), int64(3)}
	split := apocCollSplit(list, int64(0))
	assert.Len(t, split, 3)
	assert.Equal(t, []interface{}{int64(1)}, split[0])

	parts := apocCollPartition([]interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)}, int64(2))
	assert.Len(t, parts, 3)
	assert.Nil(t, apocCollPartition(list, int64(0)))
	assert.Nil(t, apocCollPartition("x", int64(2)))

	pairs := apocCollPairs([]interface{}{"a", "b"})
	assert.Equal(t, []interface{}{[]interface{}{"a", "b"}, []interface{}{"b", nil}}, pairs)
	assert.Nil(t, apocCollPairs("x"))

	zipped := apocCollZip([]interface{}{1, 2, 3}, []interface{}{"a", "b"})
	assert.Equal(t, []interface{}{[]interface{}{1, "a"}, []interface{}{2, "b"}}, zipped)
	assert.Nil(t, apocCollZip("x", []interface{}{}))
}

func TestApocCollections_FrequenciesAndOccurrences(t *testing.T) {
	vals := []interface{}{"a", "b", "a", int64(1), int64(1)}
	freq := apocCollFrequencies(vals)
	assert.EqualValues(t, 2, freq["a"])
	assert.EqualValues(t, 1, freq["b"])
	assert.EqualValues(t, 2, freq["1"])
	assert.Empty(t, apocCollFrequencies("x"))

	assert.EqualValues(t, 2, apocCollOccurrences(vals, "a"))
	assert.EqualValues(t, 0, apocCollOccurrences(vals, "z"))
	assert.EqualValues(t, 0, apocCollOccurrences("x", "a"))
}

func TestApocCollections_FlattenListVariants(t *testing.T) {
	nested := []interface{}{int64(1), []interface{}{int64(2), []interface{}{int64(3)}}, int64(4)}
	assert.Equal(t, []interface{}{int64(1), int64(2), int64(3), int64(4)}, flattenList(nested))
	assert.Equal(t, []interface{}{"a", "b"}, flattenList([]string{"a", "b"}))
	assert.Equal(t, []interface{}{int64(5)}, flattenList(int64(5)))
}

func TestApocCollections_FromPairsAndFromLists(t *testing.T) {
	pairs := []interface{}{
		[]interface{}{"name", "alice"},
		[]interface{}{int64(42), "num-key"},
		[]interface{}{"age"}, // invalid pair ignored
		"bad",                // invalid item ignored
	}
	m := fromPairs(pairs)
	assert.Equal(t, "alice", m["name"])
	assert.Equal(t, "num-key", m["42"])
	assert.Empty(t, fromPairs("x"))

	m2 := fromLists([]interface{}{"a", int64(2)}, []interface{}{"x", "y", "z"})
	assert.Equal(t, "x", m2["a"])
	assert.Equal(t, "y", m2["2"])
	assert.Empty(t, fromLists("x", []interface{}{}))
}

func TestApocCollections_GetCypherType(t *testing.T) {
	assert.Equal(t, "NULL", getCypherType(nil))
	assert.Equal(t, "BOOLEAN", getCypherType(true))
	assert.Equal(t, "INTEGER", getCypherType(int64(1)))
	assert.Equal(t, "FLOAT", getCypherType(1.5))
	assert.Equal(t, "STRING", getCypherType("x"))
	assert.Equal(t, "LIST", getCypherType([]interface{}{1}))
	assert.Equal(t, "LIST", getCypherType([]string{"a"}))
	assert.Equal(t, "MAP", getCypherType(map[string]interface{}{"k": "v"}))
	assert.Equal(t, "NODE", getCypherType(&storage.Node{}))
	assert.Equal(t, "RELATIONSHIP", getCypherType(&storage.Edge{}))
	assert.Equal(t, "DURATION", getCypherType(&CypherDuration{}))
	assert.Equal(t, "ANY", getCypherType(struct{}{}))
}

func TestApocCollections_DispatchBranches_FromExpressionEvaluator(t *testing.T) {
	e := setupTestExecutor(t)
	n1 := createTestNode(t, e, "apoc-a", []string{"Person"}, map[string]interface{}{"name": "bob", "age": int64(40)})
	n2 := createTestNode(t, e, "apoc-b", []string{"Person"}, map[string]interface{}{"name": "alice", "age": int64(20)})
	nodes := map[string]*storage.Node{"n1": n1, "n2": n2}
	ctx := context.Background()

	assert.Equal(t, "a|b|c", e.evaluateExpressionWithContext(ctx, "apoc.text.join(['a','b','c'], '|')", nodes, nil))
	assert.Nil(t, e.evaluateExpressionWithContext(ctx, "apoc.text.join(['a'])", nodes, nil))

	assert.Equal(t, float64(2), e.evaluateExpressionWithContext(ctx, "apoc.coll.avg([1,2,3])", nodes, nil))
	assert.Equal(t, int64(1), e.evaluateExpressionWithContext(ctx, "apoc.coll.min([3,1,2])", nodes, nil))
	assert.Equal(t, int64(3), e.evaluateExpressionWithContext(ctx, "apoc.coll.max([3,1,2])", nodes, nil))
	assert.True(t, reflect.DeepEqual([]interface{}{int64(1), int64(2), int64(3)}, e.evaluateExpressionWithContext(ctx, "apoc.coll.sort([3,1,2])", nodes, nil)))
	sortedNodes, ok := e.evaluateExpressionWithContext(ctx, "apoc.coll.sortNodes([n1,n2], 'age')", nodes, nil).([]interface{})
	assert.True(t, ok)
	assert.NotNil(t, sortedNodes)
	assert.Nil(t, e.evaluateExpressionWithContext(ctx, "apoc.coll.sortNodes([n1])", nodes, nil))

	assert.True(t, reflect.DeepEqual([]interface{}{int64(1), int64(2), int64(3)}, e.evaluateExpressionWithContext(ctx, "apoc.coll.union([1,2],[2,3])", nodes, nil)))
	assert.Nil(t, e.evaluateExpressionWithContext(ctx, "apoc.coll.union([1])", nodes, nil))
	assert.True(t, reflect.DeepEqual([]interface{}{int64(1), int64(2), int64(2), int64(3)}, e.evaluateExpressionWithContext(ctx, "apoc.coll.unionAll([1,2],[2,3])", nodes, nil)))
	assert.Nil(t, e.evaluateExpressionWithContext(ctx, "apoc.coll.unionAll([1])", nodes, nil))
	assert.True(t, reflect.DeepEqual([]interface{}{int64(2)}, e.evaluateExpressionWithContext(ctx, "apoc.coll.intersection([1,2],[2,3])", nodes, nil)))
	assert.Nil(t, e.evaluateExpressionWithContext(ctx, "apoc.coll.intersection([1])", nodes, nil))
	subtracted, ok := e.evaluateExpressionWithContext(ctx, "apoc.coll.subtract([1,2,3],[2])", nodes, nil).([]interface{})
	assert.True(t, ok)
	_ = subtracted
	assert.Nil(t, e.evaluateExpressionWithContext(ctx, "apoc.coll.subtract([1])", nodes, nil))

	_, ok = e.evaluateExpressionWithContext(ctx, "apoc.coll.contains(['a','b','c'],'b')", nodes, nil).(bool)
	assert.True(t, ok)
	assert.Equal(t, false, e.evaluateExpressionWithContext(ctx, "apoc.coll.contains([1,2,3])", nodes, nil))
	assert.Equal(t, true, e.evaluateExpressionWithContext(ctx, "apoc.coll.containsAll([1,2,3],[2,3])", nodes, nil))
	assert.Equal(t, false, e.evaluateExpressionWithContext(ctx, "apoc.coll.containsAll([1,2,3])", nodes, nil))
	assert.Equal(t, true, e.evaluateExpressionWithContext(ctx, "apoc.coll.containsAny([1,2,3],[5,3])", nodes, nil))
	assert.Equal(t, false, e.evaluateExpressionWithContext(ctx, "apoc.coll.containsAny([1,2,3])", nodes, nil))
	assert.Equal(t, int64(1), e.evaluateExpressionWithContext(ctx, "apoc.coll.indexOf([1,2,3],2)", nodes, nil))
	assert.Equal(t, int64(-1), e.evaluateExpressionWithContext(ctx, "apoc.coll.indexOf([1,2,3])", nodes, nil))
	assert.NotNil(t, e.evaluateExpressionWithContext(ctx, "apoc.coll.split([1,2,3],2)", nodes, nil))
	assert.Nil(t, e.evaluateExpressionWithContext(ctx, "apoc.coll.split([1,2,3])", nodes, nil))
	assert.NotNil(t, e.evaluateExpressionWithContext(ctx, "apoc.coll.partition([1,2,3,4],2)", nodes, nil))
	assert.Nil(t, e.evaluateExpressionWithContext(ctx, "apoc.coll.partition([1,2,3,4])", nodes, nil))
	assert.NotNil(t, e.evaluateExpressionWithContext(ctx, "apoc.coll.zip([1,2],[3,4])", nodes, nil))
	assert.Nil(t, e.evaluateExpressionWithContext(ctx, "apoc.coll.zip([1,2])", nodes, nil))
	assert.Equal(t, int64(2), e.evaluateExpressionWithContext(ctx, "apoc.coll.occurrences([1,2,2],2)", nodes, nil))
	assert.Equal(t, int64(0), e.evaluateExpressionWithContext(ctx, "apoc.coll.occurrences([1,2,2])", nodes, nil))

	assert.Nil(t, e.evaluateExpressionWithContext(ctx, "apoc.convert.fromJsonMap(123)", nodes, nil))
	assert.Nil(t, e.evaluateExpressionWithContext(ctx, "apoc.convert.fromJsonList(123)", nodes, nil))
	assert.Equal(t, false, e.evaluateExpressionWithContext(ctx, "apoc.meta.isType(1,2)", nodes, nil))
	assert.Equal(t, false, e.evaluateExpressionWithContext(ctx, "apoc.meta.isType(1)", nodes, nil))
	assert.Equal(t, map[string]interface{}{}, e.evaluateExpressionWithContext(ctx, "apoc.map.merge({a:1})", nodes, nil))
	assert.Nil(t, e.evaluateExpressionWithContext(ctx, "apoc.map.fromLists(['a'])", nodes, nil))
}
