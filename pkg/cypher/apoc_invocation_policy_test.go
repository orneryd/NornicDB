// Author: Codex NOR-184 lane. Preserve scalar fast adapters and typed dispatch.
package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/apoc"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"unsafe"
)

func TestAPOCScalarInvocationPolicyNOR184(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	handlers := map[string]interface{}{}
	for _, name := range []string{"ceil", "factorial"} {
		info, ok := apoc.GetInfo("apoc.math." + name)
		require.True(t, ok)
		handlers["nor184.legacy_"+name] = info.Handler
	}
	withPluginFunctions(t, handlers)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor184_scalar_policy"))
	for _, entry := range []struct {
		query string
		want  interface{}
	}{
		{"RETURN nor184.legacy_ceil('2.5') AS v", float64(3)},
		{"RETURN nor184.legacy_factorial(3) AS v", int64(6)},
	} {
		result, err := exec.Execute(context.Background(), entry.query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{entry.want}}, result.Rows)
	}
}

func TestAPOCQueryValueDomainNOR184(t *testing.T) {
	invoked := false
	withPluginFunctions(t, map[string]interface{}{
		"nor184.capability":         func() func() { invoked = true; return func() {} },
		"nor184.channel":            func() chan int { invoked = true; return nil },
		"nor184.pointer":            func() unsafe.Pointer { invoked = true; return nil },
		"nor184.checked_capability": func() (func(), error) { invoked = true; return nil, nil },
	})
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor184_value_domain"))
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, name := range []string{"capability", "channel", "pointer", "checked_capability"} {
			for _, query := range []string{"RETURN nor184." + name + "() AS v", "UNWIND [1] AS n WITH n RETURN nor184." + name + "() AS v"} {
				t.Run(query, func(t *testing.T) {
					_, err := exec.Execute(context.Background(), query, nil)
					require.Error(t, err)
					require.False(t, invoked, "a query must reject an unrepresentable declared result before invoking its producer")
				})
			}
		}
	}
}
