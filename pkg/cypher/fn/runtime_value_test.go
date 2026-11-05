package fn

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRuntimeValueNullMapContract(t *testing.T) {
	type namedMap map[string]interface{}
	for _, value := range []interface{}{nil, map[string]interface{}(nil), map[interface{}]interface{}(nil), namedMap(nil), (*storage.Node)(nil), (*storage.Edge)(nil), storage.RemoteEntityValue(nil), storage.RemotePathValue(nil)} {
		require.Nil(t, RuntimeValue(value), "%T", value)
	}
	for _, value := range []interface{}{map[string]interface{}{}, map[interface{}]interface{}{}, namedMap{}, &storage.Node{}, &storage.Edge{}, storage.RemoteEntityValue{"elementId": "remote"}, []interface{}(nil)} {
		require.Equal(t, value, RuntimeValue(value), "%T", value)
	}
}
