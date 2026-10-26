package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRemotePathRetainsCypherPathSemantics(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()
	a := storage.RemoteEntityValue{"elementId": "4:source:a", "labels": []interface{}{"Doc"}, "properties": map[string]interface{}{}}
	b := storage.RemoteEntityValue{"elementId": "4:source:b", "labels": []interface{}{"Doc"}, "properties": map[string]interface{}{}}
	r := storage.RemoteEntityValue{"elementId": "5:source:r", "type": "LINKS", "startNodeElementId": "4:source:a", "endNodeElementId": "4:source:b", "properties": map[string]interface{}{}}
	for _, tc := range []struct {
		name   string
		path   storage.RemotePathValue
		length interface{}
		nodes  interface{}
		rels   interface{}
	}{
		{"one hop", storage.RemotePathValue{a, r, b}, int64(1), []interface{}{a, b}, []interface{}{r}},
		{"zero hop", storage.RemotePathValue{a}, int64(0), []interface{}{a}, []interface{}{}},
		{"null", nil, nil, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, expr := range []struct {
				query string
				want  interface{}
			}{{"length(p)", tc.length}, {"nodes(p)", tc.nodes}, {"relationships(p)", tc.rels}} {
				result, err := e.Execute(ctx, "WITH $p AS p RETURN "+expr.query+" AS v", map[string]interface{}{"p": tc.path})
				require.NoError(t, err, expr.query)
				require.Equal(t, [][]interface{}{{expr.want}}, result.Rows, expr.query)
			}
		})
	}
	p := storage.RemotePathValue{a, r, b}
	for _, expr := range []string{"size(p)", "all(x IN p WHERE true)"} {
		_, err := e.Execute(ctx, "WITH $p AS p RETURN "+expr+" AS v", map[string]interface{}{"p": p})
		require.Error(t, err, expr)
	}
}

func TestRemotePathContextReadersUseSharedParts(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	a := storage.RemoteEntityValue{"elementId": "a", "labels": []interface{}{}, "properties": map[string]interface{}{}}
	b := storage.RemoteEntityValue{"elementId": "b", "labels": []interface{}{}, "properties": map[string]interface{}{}}
	r := storage.RemoteEntityValue{"elementId": "r", "type": "R", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{}}
	path := storage.RemotePathValue{a, r, b}
	for _, tc := range []struct {
		expr string
		want interface{}
	}{{"length(p)", int64(1)}, {"nodes(p)", []interface{}{a, b}}, {"relationships(p)", []interface{}{r}}} {
		t.Run(tc.expr, func(t *testing.T) {
			require.Equal(t, tc.want, e.evaluateExpressionFromValuesContext(context.Background(), originalBraceView(tc.expr), map[string]interface{}{"p": path}))
		})
	}
}

func TestMaterializedRemotePathVisitsSourceEntities(t *testing.T) {
	a := storage.RemoteEntityValue{"elementId": "a", "labels": []interface{}{}, "properties": map[string]interface{}{}}
	b := storage.RemoteEntityValue{"elementId": "b", "labels": []interface{}{}, "properties": map[string]interface{}{}}
	r := storage.RemoteEntityValue{"elementId": "r", "type": "R", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{}}
	recorder := &recordingAccessRecorder{}
	require.True(t, materializedValue(recorder, storage.RemotePathValue{a, r, b}))
	require.Equal(t, []string{"a", "r", "b"}, recorder.ids)
}

func TestRemotePathHelperNullAndInvalidControls(t *testing.T) {
	for _, function := range []string{"length", "nodes", "relationships"} {
		value, err := remotePathFunctionValue(function, nil)
		require.NoError(t, err)
		require.Nil(t, value)
		_, err = remotePathFunctionValue(function, storage.RemotePathValue{})
		require.Error(t, err)
	}
	node := storage.RemoteEntityValue{"elementId": "a"}
	path := storage.RemotePathValue{node}
	require.Equal(t, []interface{}{path}, coerceToUnwindItems(path))
}
