package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPluginNilMapResultPreservesProperties(t *testing.T) {
	type namedMap map[string]interface{}
	// Exercise common interface-return and reflected map-return dispatch,
	// including named maps and map/error signatures, independently of APOC.
	handlers := map[string]interface{}{
		"zz.nilMapCommon": func() interface{} { return map[string]interface{}(nil) },
		"zz.nilMapTyped":  func() map[string]interface{} { return nil },
		"zz.nilMapNamed":  func() namedMap { return nil },
		"zz.nilMapError":  func() (map[string]interface{}, error) { return nil, nil },
		"zz.emptyMap":     func() map[string]interface{} { return map[string]interface{}{} },
	}
	withPluginFunctions(t, handlers)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nil_map_result"))
	for name := range handlers {
		t.Run(name, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), "RETURN "+name+"()", nil)
			require.NoError(t, err)
			if name == "zz.emptyMap" {
				require.Equal(t, map[string]interface{}{}, result.Rows[0][0])
			} else {
				require.Nil(t, result.Rows[0][0])
			}
			_, err = exec.Execute(context.Background(), "CREATE (:NilMapResult {keep:1})", nil)
			require.NoError(t, err)
			_, err = exec.Execute(context.Background(), "MATCH (n:NilMapResult) SET n = "+name+"()", nil)
			after, readErr := exec.Execute(context.Background(), "MATCH (n:NilMapResult) RETURN properties(n)", nil)
			require.NoError(t, readErr)
			if name == "zz.emptyMap" {
				require.NoError(t, err)
				require.Empty(t, after.Rows[0][0])
			} else {
				require.Error(t, err)
				require.Equal(t, map[string]interface{}{"keep": int64(1)}, after.Rows[0][0])
			}
			_, err = exec.Execute(context.Background(), "MATCH (n:NilMapResult) DETACH DELETE n", nil)
			require.NoError(t, err)
		})
	}
}
