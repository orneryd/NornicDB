package apoc

import (
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/apoc/meta"
	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

func TestGraphSampleRetirementPreservesCanonicalGraph(t *testing.T) {
	backend := storage.NewInMemoryStorage()
	first, err := backend.CreateNode(t.Context(), []string{"Person"}, nil)
	require.NoError(t, err)
	second, err := backend.CreateNode(t.Context(), []string{"Person"}, nil)
	require.NoError(t, err)
	_, err = backend.CreateRelationship(t.Context(), first.ID, second.ID, "KNOWS", nil)
	require.NoError(t, err)
	require.NoError(t, Initialize(backend, nil))
	require.NotContains(t, List(), "apoc.meta.graphSample")
	require.NotContains(t, PluginFunctionNames(), "apoc.meta.graphSample")
	_, exists := reflect.TypeOf(meta.New(backend)).MethodByName("GraphSample")
	require.False(t, exists, "retire the misleading Go helper rather than forwarding it")
	_, err = Call("apoc.meta.graphSample", []interface{}{1})
	require.Error(t, err)
	want, apocCallError1 := meta.New(backend).Graph(t.Context())
	if apocCallError1 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError1)
	}

	require.Equal(t, 2, want["nodes"])
	require.Equal(t, 1, want["relationships"])
	require.Equal(t, map[string]int{"Person": 2}, want["labels"])
	require.Equal(t, map[string]int{"KNOWS": 1}, want["relTypes"])
	require.Contains(t, apocRegisteredHandler(t, "apoc.meta.graph").Symbol(), "/apoc/meta.(*BoundFunctions).Graph")
}
