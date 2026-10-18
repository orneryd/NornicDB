package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"reflect"
	"strings"
	"testing"
)

func TestPipelineFallbackKeyKinds(t *testing.T) {
	type privateFields struct {
		flag      bool
		number    int
		unsigned  uint
		fraction  float64
		imaginary complex128
		channel   chan int
		function  func()
	}
	for _, value := range []interface{}{
		struct{ Value interface{} }{}, struct{ Map map[int]string }{},
		privateFields{true, -2, 3, 1.25, complex(1, 2), make(chan int), func() {}},
	} {
		var out strings.Builder
		probe := pipelineCancellation{ctx: context.Background()}
		appendEquivalenceGoValue(&out, reflect.ValueOf(value), &probe, false)
		require.Equal(t, fmt.Sprintf("%#v", value), out.String())
	}
	var out strings.Builder
	appendEquivalenceGoValue(&out, reflect.Value{}, nil, false)
	require.Equal(t, "<nil>", out.String())
	pointer := new(int)
	require.Zero(t, compareEquivalenceGoMapKeys(reflect.ValueOf(pointer), reflect.ValueOf(pointer), nil))
	require.Zero(t, compareEquivalenceGoMapKeys(reflect.ValueOf([1]int{1}), reflect.ValueOf([1]int{1}), nil))
	require.Zero(t, compareEquivalenceGoMapKeys(reflect.ValueOf(struct{ X int }{1}), reflect.ValueOf(struct{ X int }{1}), nil))
}

func TestPipelineContextFreeReadersPreserveResults(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	row := map[string]interface{}{"items": []interface{}{int64(1), int64(2)}}
	value, ok, err := e.evaluateRowCaseExpression("CASE 1 WHEN 1 THEN 2 ELSE 0 END", row)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(2), value)
	_, err = evaluateListAccessFunction("reverse", row["items"])
	require.NoError(t, err)
	require.Equal(t, 0, cypherSortRank(map[string]interface{}{}))
	_, _, _ = compareCypherStructuredValues(int64(1), int64(2), func(a, b interface{}) (int, bool) { return 0, true })
	_, _ = compareCypherMaps(map[string]interface{}{}, map[string]interface{}{}, func(a, b interface{}) (int, bool) { return 0, true })
	_, _ = evaluateStaticListForPipeline("[1,2]", nil)
	edge := &storage.Edge{ID: "e", StartNode: "a", EndNode: "b"}
	list, ok := pipelineRelationshipList([]interface{}{edge})
	require.True(t, ok)
	require.Len(t, list, 1)
	require.False(t, relationshipListReusesEdge(list))
	require.Equal(t, [][2]storage.NodeID{{"a", "b"}}, traceRelationshipList(list, "outgoing"))
	value, ok = rowMembershipOfValues(int64(1), []interface{}{int64(1)}, false)
	require.True(t, ok)
	require.Equal(t, true, value)
	require.Equal(t, "�x", padRowString("\xff", 2, "x", false))
}
