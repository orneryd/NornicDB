// Author: Codex NOR-184 lane. Preserve the root adapter's existing input policy.
package math_test

import (
	"github.com/orneryd/nornicdb/apoc"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAPOCScalarAdapterPolicyNOR184(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	value, err := apoc.Call("apoc.math.ceil", []interface{}{2.5})
	require.NoError(t, err)
	require.Equal(t, float64(3), value)
	_, err = apoc.Call("apoc.math.ceil", []interface{}{"2.5"})
	require.Error(t, err, "root Call retains numeric-only float binding")
	_, err = apoc.Call("apoc.math.factorial", []interface{}{int64(3)})
	require.Error(t, err, "unrelated unsupported root signatures must not become executable")
}
