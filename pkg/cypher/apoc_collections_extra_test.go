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
	n3 := storage.RemoteEntityValue{"properties": map[string]interface{}{"age": int64(30)}}

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
	assert.Equal(t, []interface{}{
		map[string]interface{}{"item": "a", "count": int64(2)},
		map[string]interface{}{"item": "b", "count": int64(1)},
		map[string]interface{}{"item": int64(1), "count": int64(2)},
	}, apocCollFrequencies(vals))
	assert.Empty(t, apocCollFrequencies("x"))

	assert.EqualValues(t, 2, apocCollOccurrences(vals, "a"))
	assert.EqualValues(t, 0, apocCollOccurrences(vals, "z"))
	assert.EqualValues(t, 0, apocCollOccurrences("x", "a"))
}

func TestApocCollections_FlattenListVariants(t *testing.T) {
	nested := []interface{}{int64(1), []interface{}{int64(2), []interface{}{int64(3)}}, int64(4)}
	assert.Equal(t, []interface{}{int64(1), int64(2), int64(3), int64(4)}, flattenList(nested, true))
	assert.Equal(t, []interface{}{int64(1), int64(2), []interface{}{int64(3)}, int64(4)}, flattenList(nested, false))
	assert.Equal(t, []interface{}{"a", "b"}, flattenList([]string{"a", "b"}, false))
	assert.Equal(t, []interface{}{int64(5)}, flattenList(int64(5), false))
	assert.Equal(t, []interface{}{}, flattenList(nil, false))
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
	assert.Equal(t, "NULL", apocValueTypeName(nil))
	assert.Equal(t, "BOOLEAN", apocValueTypeName(true))
	assert.Equal(t, "INTEGER", apocValueTypeName(int64(1)))
	assert.Equal(t, "FLOAT", apocValueTypeName(1.5))
	assert.Equal(t, "STRING", apocValueTypeName("x"))
	assert.Equal(t, "LIST", apocValueTypeName([]interface{}{1}))
	assert.Equal(t, "LIST", apocValueTypeName([]string{"a"}))
	assert.Equal(t, "MAP", apocValueTypeName(map[string]interface{}{"k": "v"}))
	assert.Equal(t, "NODE", apocValueTypeName(&storage.Node{}))
	assert.Equal(t, "RELATIONSHIP", apocValueTypeName(&storage.Edge{}))
	assert.Equal(t, "DURATION", apocValueTypeName(&CypherDuration{}))
	assert.Equal(t, "ANY", apocValueTypeName(struct{}{}))
}

func TestApocCollections_DispatchBranches_FromExpressionEvaluator(t *testing.T) {
	e := setupTestExecutor(t)
	n1 := createTestNode(t, e, "apoc-a", []string{"Person"}, map[string]interface{}{"name": "bob", "age": int64(40)})
	n2 := createTestNode(t, e, "apoc-b", []string{"Person"}, map[string]interface{}{"name": "alice", "age": int64(20)})
	nodes := map[string]*storage.Node{"n1": n1, "n2": n2}
	ctx := context.Background()

	fixtureAdmission17Context, fixtureAdmission17 := expressionTestContext(ctx, "apoc.text.join(['a','b','c'], '|')")
	_ = fixtureAdmission17
	assert.Equal(t, "a|b|c", e.evaluateExpressionWithContext(fixtureAdmission17, fixtureAdmission17Context, "apoc.text.join(['a','b','c'], '|')", nodes, nil))
	fixtureAdmission18Context, fixtureAdmission18 := expressionTestContext(ctx, "apoc.text.join(['a'])")
	_ = fixtureAdmission18
	assert.Nil(t, e.evaluateExpressionWithContext(fixtureAdmission18, fixtureAdmission18Context, "apoc.text.join(['a'])", nodes, nil))

	fixtureAdmission19Context, fixtureAdmission19 := expressionTestContext(ctx, "apoc.coll.avg([1,2,3])")
	_ = fixtureAdmission19
	assert.Equal(t, float64(2), e.evaluateExpressionWithContext(fixtureAdmission19, fixtureAdmission19Context, "apoc.coll.avg([1,2,3])", nodes, nil))
	fixtureAdmission20Context, fixtureAdmission20 := expressionTestContext(ctx, "apoc.coll.min([3,1,2])")
	_ = fixtureAdmission20
	assert.Equal(t, int64(1), e.evaluateExpressionWithContext(fixtureAdmission20, fixtureAdmission20Context, "apoc.coll.min([3,1,2])", nodes, nil))
	fixtureAdmission21Context, fixtureAdmission21 := expressionTestContext(ctx, "apoc.coll.max([3,1,2])")
	_ = fixtureAdmission21
	assert.Equal(t, int64(3), e.evaluateExpressionWithContext(fixtureAdmission21, fixtureAdmission21Context, "apoc.coll.max([3,1,2])", nodes, nil))
	fixtureAdmission22Context, fixtureAdmission22 := expressionTestContext(ctx, "apoc.coll.sort([3,1,2])")
	_ = fixtureAdmission22
	assert.True(t, reflect.DeepEqual([]interface{}{int64(1), int64(2), int64(3)}, e.evaluateExpressionWithContext(fixtureAdmission22, fixtureAdmission22Context, "apoc.coll.sort([3,1,2])", nodes, nil)))
	fixtureAdmission23Context, fixtureAdmission23 := expressionTestContext(ctx, "apoc.coll.sortNodes([n1,n2], 'age')")
	_ = fixtureAdmission23
	sortedNodes, ok := e.evaluateExpressionWithContext(fixtureAdmission23, fixtureAdmission23Context, "apoc.coll.sortNodes([n1,n2], 'age')", nodes, nil).([]interface{})
	assert.True(t, ok)
	assert.NotNil(t, sortedNodes)
	fixtureAdmission24Context, fixtureAdmission24 := expressionTestContext(ctx, "apoc.coll.sortNodes([n1])")
	_ = fixtureAdmission24
	assert.Nil(t, e.evaluateExpressionWithContext(fixtureAdmission24, fixtureAdmission24Context, "apoc.coll.sortNodes([n1])", nodes, nil))

	fixtureAdmission25Context, fixtureAdmission25 := expressionTestContext(ctx, "apoc.coll.union([1,2],[2,3])")
	_ = fixtureAdmission25
	assert.True(t, reflect.DeepEqual([]interface{}{int64(1), int64(2), int64(3)}, e.evaluateExpressionWithContext(fixtureAdmission25, fixtureAdmission25Context, "apoc.coll.union([1,2],[2,3])", nodes, nil)))
	fixtureAdmission26Context, fixtureAdmission26 := expressionTestContext(ctx, "apoc.coll.union([1])")
	_ = fixtureAdmission26
	assert.Nil(t, e.evaluateExpressionWithContext(fixtureAdmission26, fixtureAdmission26Context, "apoc.coll.union([1])", nodes, nil))
	fixtureAdmission27Context, fixtureAdmission27 := expressionTestContext(ctx, "apoc.coll.unionAll([1,2],[2,3])")
	_ = fixtureAdmission27
	assert.True(t, reflect.DeepEqual([]interface{}{int64(1), int64(2), int64(2), int64(3)}, e.evaluateExpressionWithContext(fixtureAdmission27, fixtureAdmission27Context, "apoc.coll.unionAll([1,2],[2,3])", nodes, nil)))
	fixtureAdmission28Context, fixtureAdmission28 := expressionTestContext(ctx, "apoc.coll.unionAll([1])")
	_ = fixtureAdmission28
	assert.Nil(t, e.evaluateExpressionWithContext(fixtureAdmission28, fixtureAdmission28Context, "apoc.coll.unionAll([1])", nodes, nil))
	fixtureAdmission29Context, fixtureAdmission29 := expressionTestContext(ctx, "apoc.coll.intersection([1,2],[2,3])")
	_ = fixtureAdmission29
	assert.True(t, reflect.DeepEqual([]interface{}{int64(2)}, e.evaluateExpressionWithContext(fixtureAdmission29, fixtureAdmission29Context, "apoc.coll.intersection([1,2],[2,3])", nodes, nil)))
	fixtureAdmission30Context, fixtureAdmission30 := expressionTestContext(ctx, "apoc.coll.intersection([1])")
	_ = fixtureAdmission30
	assert.Nil(t, e.evaluateExpressionWithContext(fixtureAdmission30, fixtureAdmission30Context, "apoc.coll.intersection([1])", nodes, nil))
	fixtureAdmission31Context, fixtureAdmission31 := expressionTestContext(ctx, "apoc.coll.subtract([1,2,3],[2])")
	_ = fixtureAdmission31
	subtracted, ok := e.evaluateExpressionWithContext(fixtureAdmission31, fixtureAdmission31Context, "apoc.coll.subtract([1,2,3],[2])", nodes, nil).([]interface{})
	assert.True(t, ok)
	_ = subtracted
	fixtureAdmission32Context, fixtureAdmission32 := expressionTestContext(ctx, "apoc.coll.subtract([1])")
	_ = fixtureAdmission32
	assert.Nil(t, e.evaluateExpressionWithContext(fixtureAdmission32, fixtureAdmission32Context, "apoc.coll.subtract([1])", nodes, nil))

	fixtureAdmission33Context, fixtureAdmission33 := expressionTestContext(ctx, "apoc.coll.contains(['a','b','c'],'b')")
	_ = fixtureAdmission33
	_, ok = e.evaluateExpressionWithContext(fixtureAdmission33, fixtureAdmission33Context, "apoc.coll.contains(['a','b','c'],'b')", nodes, nil).(bool)
	assert.True(t, ok)
	fixtureAdmission34Context, fixtureAdmission34 := expressionTestContext(ctx, "apoc.coll.contains([1,2,3])")
	_ = fixtureAdmission34
	assert.Equal(t, false, e.evaluateExpressionWithContext(fixtureAdmission34, fixtureAdmission34Context, "apoc.coll.contains([1,2,3])", nodes, nil))
	fixtureAdmission35Context, fixtureAdmission35 := expressionTestContext(ctx, "apoc.coll.containsAll([1,2,3],[2,3])")
	_ = fixtureAdmission35
	assert.Equal(t, true, e.evaluateExpressionWithContext(fixtureAdmission35, fixtureAdmission35Context, "apoc.coll.containsAll([1,2,3],[2,3])", nodes, nil))
	fixtureAdmission36Context, fixtureAdmission36 := expressionTestContext(ctx, "apoc.coll.containsAll([1,2,3])")
	_ = fixtureAdmission36
	assert.Equal(t, false, e.evaluateExpressionWithContext(fixtureAdmission36, fixtureAdmission36Context, "apoc.coll.containsAll([1,2,3])", nodes, nil))
	fixtureAdmission37Context, fixtureAdmission37 := expressionTestContext(ctx, "apoc.coll.containsAny([1,2,3],[5,3])")
	_ = fixtureAdmission37
	assert.Equal(t, true, e.evaluateExpressionWithContext(fixtureAdmission37, fixtureAdmission37Context, "apoc.coll.containsAny([1,2,3],[5,3])", nodes, nil))
	fixtureAdmission38Context, fixtureAdmission38 := expressionTestContext(ctx, "apoc.coll.containsAny([1,2,3])")
	_ = fixtureAdmission38
	assert.Equal(t, false, e.evaluateExpressionWithContext(fixtureAdmission38, fixtureAdmission38Context, "apoc.coll.containsAny([1,2,3])", nodes, nil))
	fixtureAdmission39Context, fixtureAdmission39 := expressionTestContext(ctx, "apoc.coll.indexOf([1,2,3],2)")
	_ = fixtureAdmission39
	assert.Equal(t, int64(1), e.evaluateExpressionWithContext(fixtureAdmission39, fixtureAdmission39Context, "apoc.coll.indexOf([1,2,3],2)", nodes, nil))
	fixtureAdmission40Context, fixtureAdmission40 := expressionTestContext(ctx, "apoc.coll.indexOf([1,2,3])")
	_ = fixtureAdmission40
	assert.Equal(t, int64(-1), e.evaluateExpressionWithContext(fixtureAdmission40, fixtureAdmission40Context, "apoc.coll.indexOf([1,2,3])", nodes, nil))
	fixtureAdmission41Context, fixtureAdmission41 := expressionTestContext(ctx, "apoc.coll.split([1,2,3],2)")
	_ = fixtureAdmission41
	assert.NotNil(t, e.evaluateExpressionWithContext(fixtureAdmission41, fixtureAdmission41Context, "apoc.coll.split([1,2,3],2)", nodes, nil))
	fixtureAdmission42Context, fixtureAdmission42 := expressionTestContext(ctx, "apoc.coll.split([1,2,3])")
	_ = fixtureAdmission42
	assert.Nil(t, e.evaluateExpressionWithContext(fixtureAdmission42, fixtureAdmission42Context, "apoc.coll.split([1,2,3])", nodes, nil))
	fixtureAdmission43Context, fixtureAdmission43 := expressionTestContext(ctx, "apoc.coll.partition([1,2,3,4],2)")
	_ = fixtureAdmission43
	assert.NotNil(t, e.evaluateExpressionWithContext(fixtureAdmission43, fixtureAdmission43Context, "apoc.coll.partition([1,2,3,4],2)", nodes, nil))
	fixtureAdmission44Context, fixtureAdmission44 := expressionTestContext(ctx, "apoc.coll.partition([1,2,3,4])")
	_ = fixtureAdmission44
	assert.Nil(t, e.evaluateExpressionWithContext(fixtureAdmission44, fixtureAdmission44Context, "apoc.coll.partition([1,2,3,4])", nodes, nil))
	fixtureAdmission45Context, fixtureAdmission45 := expressionTestContext(ctx, "apoc.coll.zip([1,2],[3,4])")
	_ = fixtureAdmission45
	assert.NotNil(t, e.evaluateExpressionWithContext(fixtureAdmission45, fixtureAdmission45Context, "apoc.coll.zip([1,2],[3,4])", nodes, nil))
	fixtureAdmission46Context, fixtureAdmission46 := expressionTestContext(ctx, "apoc.coll.zip([1,2])")
	_ = fixtureAdmission46
	assert.Nil(t, e.evaluateExpressionWithContext(fixtureAdmission46, fixtureAdmission46Context, "apoc.coll.zip([1,2])", nodes, nil))
	fixtureAdmission47Context, fixtureAdmission47 := expressionTestContext(ctx, "apoc.coll.occurrences([1,2,2],2)")
	_ = fixtureAdmission47
	assert.Equal(t, int64(2), e.evaluateExpressionWithContext(fixtureAdmission47, fixtureAdmission47Context, "apoc.coll.occurrences([1,2,2],2)", nodes, nil))
	fixtureAdmission48Context, fixtureAdmission48 := expressionTestContext(ctx, "apoc.coll.occurrences([1,2,2])")
	_ = fixtureAdmission48
	assert.Equal(t, int64(0), e.evaluateExpressionWithContext(fixtureAdmission48, fixtureAdmission48Context, "apoc.coll.occurrences([1,2,2])", nodes, nil))

	fixtureAdmission49Context, fixtureAdmission49 := expressionTestContext(ctx, "apoc.convert.fromJsonMap(123)")
	_ = fixtureAdmission49
	assert.Nil(t, e.evaluateExpressionWithContext(fixtureAdmission49, fixtureAdmission49Context, "apoc.convert.fromJsonMap(123)", nodes, nil))
	fixtureAdmission50Context, fixtureAdmission50 := expressionTestContext(ctx, "apoc.convert.fromJsonList(123)")
	_ = fixtureAdmission50
	assert.Nil(t, e.evaluateExpressionWithContext(fixtureAdmission50, fixtureAdmission50Context, "apoc.convert.fromJsonList(123)", nodes, nil))
	fixtureAdmission51Context, fixtureAdmission51 := expressionTestContext(ctx, "apoc.meta.isType(1,2)")
	_ = fixtureAdmission51
	assert.Equal(t, false, e.evaluateExpressionWithContext(fixtureAdmission51, fixtureAdmission51Context, "apoc.meta.isType(1,2)", nodes, nil))
	fixtureAdmission52Context, fixtureAdmission52 := expressionTestContext(ctx, "apoc.meta.isType(1)")
	_ = fixtureAdmission52
	assert.Equal(t, false, e.evaluateExpressionWithContext(fixtureAdmission52, fixtureAdmission52Context, "apoc.meta.isType(1)", nodes, nil))
	fixtureAdmission53Context, fixtureAdmission53 := expressionTestContext(ctx, "apoc.map.merge({a:1})")
	_ = fixtureAdmission53
	assert.Equal(t, map[string]interface{}{}, e.evaluateExpressionWithContext(fixtureAdmission53, fixtureAdmission53Context, "apoc.map.merge({a:1})", nodes, nil))
	fixtureAdmission54Context, fixtureAdmission54 := expressionTestContext(ctx, "apoc.map.fromLists(['a'])")
	_ = fixtureAdmission54
	assert.Nil(t, e.evaluateExpressionWithContext(fixtureAdmission54, fixtureAdmission54Context, "apoc.map.fromLists(['a'])", nodes, nil))
}
